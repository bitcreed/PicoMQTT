#pragma once

#include "server.h"
#include <Arduino.h>
#include <functional>
#include <unordered_map>
#include <vector>

namespace std {
template <>
struct hash<String> {
    size_t operator()(const String& key) const {
        return hash<std::string>{}(std::string(key.c_str(), key.length()));
    }
};
}

namespace PicoMQTT {

template <typename BaseServer>
class RetainedMessagesServer : public BaseServer {
public:
    using BaseServer::BaseServer;

protected:
    // Retained message struct
    struct RetainedMessage {
        public:
            RetainedMessage() = delete;
            RetainedMessage(const std::vector<uint8_t> && payload_data, uint8_t qos_level)
                : payload(std::move(payload_data)), qos(qos_level) {}

        private:
            std::vector<uint8_t> payload;
            uint8_t qos;
    };

    std::unordered_map<String, RetainedMessage> retained_messages; // topic → retained message

    // Client mixin to handle sending retained messages on subscribe
    template <typename BaseClient>
    class ClientRetainedMixin : public BaseClient {
    public:
        using BaseClient::BaseClient;
        using BaseClient::topic_matches;

        virtual void on_subscribe(IncomingPacket & packet) override {
            BaseClient::on_subscribe(packet);

            // After subscribing, send retained messages for matched topics
            for (const auto & sub : this->subscriptions) {
                const char * topic_filter = sub.c_str();
                for (auto & retained : this->server.retained_messages) {
                    const char * ret_topic = retained.first.c_str();
                    const auto & retained_msg = retained.second;
                    if (topic_matches(topic_filter, ret_topic)) {
                        auto pub = Publish(*this, PrintMux(this->get_print()), ret_topic,
                                           retained_msg.payload.size(), 0, true, 0);
                        pub.write(retained_msg.payload.data(), retained_msg.payload.size());
                        pub.send();
                    }
                }
            }
        }
    };

    using Client = ClientRetainedMixin<typename BaseServer::Client>;

    // Override Server::on_message to store retained messages
    void on_message(const char * topic, IncomingPacket & packet) override {
        TRACE_FUNCTION
        const bool retain = packet.get_flags() & 0b1;
        if (retain) {
            const uint8_t qos = (packet.get_flags() >> 1) & 0b11;

            std::vector<uint8_t> payload;
            payload.reserve(packet.get_remaining_size());
            uint8_t byte;
            while (byte = packet.read_u8()) {
                payload.push_back(byte);
            }

            if (payload.empty()) {
                retained_messages.erase(topic);
            } else {
                retained_messages.insert_or_assign(topic, RetainedMessage(std::move(payload), qos));
            }
        }

        BaseServer::on_message(topic, packet);
    }
};

} // namespace PicoMQTT
