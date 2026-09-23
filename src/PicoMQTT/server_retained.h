#pragma once

#include <Arduino.h>

#include <memory>
#include <new>

#include "buffer_client.h"
#include "config.h"
#include "debug.h"
#include "incoming_packet.h"
#include "server.h"

namespace PicoMQTT {

template <typename BaseServer>
class RetainedMessagesServer : public BaseServer {
public:
    using BaseServer::BaseServer;
    using BaseServer::publish;
    using BaseServer::publish_P;
    using BaseServer::subscribe;

    RetainedMessagesServer(const RetainedMessagesServer &) = delete;
    RetainedMessagesServer & operator=(const RetainedMessagesServer &) = delete;

    virtual ~RetainedMessagesServer() {
        while (retained_messages) {
            RetainedMessage * next = retained_messages->next;
            delete retained_messages;
            retained_messages = next;
        }
    }

    virtual bool publish(const char * topic, const void * payload,
                         const size_t payload_size, uint8_t qos = 0,
                         bool retain = false,
                         uint16_t message_id = 0) override {
        TRACE_FUNCTION;
        if (retain) {
            retain_message(topic, payload, payload_size, false);
        }
        return BaseServer::publish(topic, payload, payload_size, qos, retain,
                                   message_id);
    }

    virtual bool publish_P(const char * topic, PGM_P payload,
                           const size_t payload_size, uint8_t qos = 0,
                           bool retain = false,
                           uint16_t message_id = 0) override {
        TRACE_FUNCTION;
        if (retain) {
            retain_message(topic, payload, payload_size, true);
        }
        return BaseServer::publish_P(topic, payload, payload_size, qos, retain,
                                     message_id);
    }

    virtual Subscriber::SubscriptionId subscribe(
        const String & topic_filter,
        SubscribedMessageListener::MessageCallback callback) override {
        TRACE_FUNCTION;
        const auto id =
            BaseServer::subscribe(topic_filter, std::move(callback));
        if (id) {
            deliver_retained_locally(topic_filter.c_str());
        }
        return id;
    }

    size_t get_retained_message_count() const { return retained_count; }

    void clear_retained_messages() {
        TRACE_FUNCTION;
        for (RetainedMessage * m = retained_messages; m; m = m->next) {
            m->erased = true;
        }
        retained_count = 0;
        purge_erased();
    }

protected:
    struct RetainedMessage {
        RetainedMessage(const char * topic, std::unique_ptr<uint8_t[]> payload,
                        size_t size)
            : topic(topic),
              payload(std::move(payload)),
              size(size),
              erased(false),
              next(nullptr) {}
        RetainedMessage(const RetainedMessage &) = delete;
        RetainedMessage & operator=(const RetainedMessage &) = delete;

        const String topic;
        const std::unique_ptr<uint8_t[]> payload;
        const size_t size;
        bool erased;
        RetainedMessage * next;
    };

    class CapturingPacket : public IncomingPacket {
    public:
        CapturingPacket(IncomingPacket & source, uint8_t * buffer)
            : IncomingPacket(IncomingPacket::PUBLISH, source.get_flags(),
                             source.get_remaining_size(), source),
              buffer(buffer) {
            TRACE_FUNCTION;
        }

        virtual int read(uint8_t * buf, size_t size) override {
            TRACE_FUNCTION;
            const size_t offset = pos;
            const int ret = IncomingPacket::read(buf, size);
            if (buffer && (ret > 0)) {
                memcpy(buffer + offset, buf, ret);
            }
            return ret;
        }

        virtual int read() override {
            TRACE_FUNCTION;
            const size_t offset = pos;
            const int ret = IncomingPacket::read();
            if (buffer && (ret >= 0)) {
                buffer[offset] = ret;
            }
            return ret;
        }

        bool read_remaining() {
            TRACE_FUNCTION;
            uint8_t chunk[32];
            while (get_remaining_size()) {
                const size_t remaining = get_remaining_size();
                const size_t chunk_size =
                    remaining < sizeof(chunk) ? remaining : sizeof(chunk);
                if (read(chunk, chunk_size) <= 0) {
                    return false;
                }
            }
            return true;
        }

    protected:
        uint8_t * const buffer;
    };

    virtual void on_message(const char * topic,
                            IncomingPacket & packet) override {
        TRACE_FUNCTION;
        if (!(packet.get_flags() & RETAIN_FLAG)) {
            BaseServer::on_message(topic, packet);
            return;
        }

        const size_t payload_size = packet.get_remaining_size();
        std::unique_ptr<uint8_t[]> payload;
        if (payload_size && can_retain(topic, payload_size)) {
            payload.reset(new (std::nothrow) uint8_t[payload_size]);
        }

        bool complete;
        {
            CapturingPacket capturing_packet(packet, payload.get());
            BaseServer::on_message(topic, capturing_packet);
            complete = capturing_packet.read_remaining();
        }

        if (!complete) {
            return;
        }

        erase_retained(topic);
        if (payload) {
            insert_retained(topic, std::move(payload), payload_size);
        }
    }

    virtual void on_subscribed(Server::Client & client,
                               const char * topic_filter) override {
        TRACE_FUNCTION;
        BaseServer::on_subscribed(client, topic_filter);
        for (const RetainedMessage * m = retained_messages; m; m = m->next) {
            if (m->erased ||
                !Subscriber::topic_matches(topic_filter, m->topic.c_str())) {
                continue;
            }
            Publisher::Publish publish(*this, client.get_print(),
                                       m->topic.c_str(), m->size, 0, true);
            publish.write(m->payload.get(), m->size);
            publish.send();
        }
    }

    void deliver_retained_locally(const char * topic_filter) {
        TRACE_FUNCTION;
        // callbacks may erase retained messages, defer freeing them
        ++delivering;
        for (const RetainedMessage * m = retained_messages; m; m = m->next) {
            if (m->erased ||
                !Subscriber::topic_matches(topic_filter, m->topic.c_str())) {
                continue;
            }
            // callbacks receive a mutable topic
            String topic = m->topic;
            BufferClient buffer(m->payload.get());
            IncomingPacket packet(IncomingPacket::PUBLISH, RETAIN_FLAG, m->size,
                                  buffer);
            this->fire_message_callbacks(topic.c_str(), packet);
        }
        --delivering;
        purge_erased();
    }

    void retain_message(const char * topic, const void * payload,
                        size_t payload_size, bool progmem) {
        TRACE_FUNCTION;
        erase_retained(topic);
        if (!payload_size || !can_retain(topic, payload_size)) {
            return;
        }
        std::unique_ptr<uint8_t[]> copy(new (std::nothrow)
                                            uint8_t[payload_size]);
        if (!copy) {
            return;
        }
        if (progmem) {
            memcpy_P(copy.get(), payload, payload_size);
        } else {
            memcpy(copy.get(), payload, payload_size);
        }
        insert_retained(topic, std::move(copy), payload_size);
    }

    bool can_retain(const char * topic, size_t payload_size) const {
        if (payload_size > PICOMQTT_MAX_RETAINED_MESSAGE_SIZE) {
            return false;
        }
        if (retained_count < PICOMQTT_MAX_RETAINED_MESSAGES) {
            return true;
        }
        for (const RetainedMessage * m = retained_messages; m; m = m->next) {
            if (!m->erased && (m->topic == topic)) {
                return true;
            }
        }
        return false;
    }

    void insert_retained(const char * topic, std::unique_ptr<uint8_t[]> payload,
                         size_t size) {
        TRACE_FUNCTION;
        RetainedMessage * m =
            new (std::nothrow) RetainedMessage(topic, std::move(payload), size);
        if (!m) {
            return;
        }
        m->next = retained_messages;
        retained_messages = m;
        ++retained_count;
    }

    void erase_retained(const char * topic) {
        TRACE_FUNCTION;
        for (RetainedMessage * m = retained_messages; m; m = m->next) {
            if (!m->erased && (m->topic == topic)) {
                m->erased = true;
                --retained_count;
                break;
            }
        }
        purge_erased();
    }

    void purge_erased() {
        TRACE_FUNCTION;
        if (delivering) {
            return;
        }
        RetainedMessage ** current = &retained_messages;
        while (*current) {
            RetainedMessage * m = *current;
            if (m->erased) {
                *current = m->next;
                delete m;
            } else {
                current = &m->next;
            }
        }
    }

    static const uint8_t RETAIN_FLAG = 0b0001;

    RetainedMessage * retained_messages = nullptr;
    size_t retained_count = 0;
    unsigned int delivering = 0;
};

}  // namespace PicoMQTT
