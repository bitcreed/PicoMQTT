#pragma once
#include "server.h"
#include <unordered_map>
#include <vector>

namespace PicoMQTT {

template <typename BaseServer>
class RetainedMessagesServer : public BaseServer {
public:
    using BaseServer::BaseServer;

protected:
    using ClientType = typename BaseServer::Client;
    struct RetainedMessage {
        const std::vector<uint8_t> payload;
        const uint8_t qos;

        RetainedMessage(std::vector<uint8_t>&& payload_data, uint8_t qos_level)
            : payload(std::move(payload_data)), qos(qos_level) {}
    };

    std::unordered_map<std::string, RetainedMessage> retained_messages;

    void on_subscribe(const char* client_id, const char* topic) override {
        TRACE_FUNCTION
        BaseServer::on_subscribe(client_id, topic);
        for (auto& client_ptr : this->clients) {
            if (strcmp(client_ptr->get_client_id(), client_id) == 0) {
                for (auto& retained : retained_messages) {
                    const auto& retained_topic = retained.first;
                    const char* ret_topic = retained_topic.c_str();
                    const auto& retained_msg = retained.second;
                    if (topic_matches(topic, ret_topic)) {
                        auto pub = Publish(*this, PrintMux(client_ptr->get_print()), ret_topic,
                                           retained_msg.payload.size(), 0, true, 0);
                        pub.write(retained_msg.payload.data(), retained_msg.payload.size());
                        pub.send();
                    }
                }
                break;
            }
        }
    }

    void on_message(const char* topic, IncomingPacket& packet) override {
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

}
