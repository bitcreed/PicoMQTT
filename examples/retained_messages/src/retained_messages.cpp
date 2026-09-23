#include <PicoMQTT.h>

// Broker which stores retained messages in memory.  Clients subscribing to
// picomqtt/# will immediately receive the last retained message published to
// each matching topic.
PicoMQTT::RetainedMessagesServer<PicoMQTT::ServerLocalSubscribe> mqtt;
unsigned long last_publish;

static const char flash_string[] PROGMEM = "Retained message from flash.";

void setup() {
    // Setup serial
    Serial.begin(115200);

    // Connect to WiFi
    WiFi.mode(WIFI_STA);
    Serial.printf("Connecting to WiFi %s\n", WIFI_SSID);
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    while (WiFi.status() != WL_CONNECTED) {
        delay(1000);
    }
    Serial.printf("WiFi connected, IP: %s\n",
                  WiFi.localIP().toString().c_str());

    mqtt.begin();

    // Retain messages published locally
    mqtt.publish("picomqtt/status", "online", 0, true);
    mqtt.publish_P("picomqtt/flash", flash_string, 0, true);

    // Local subscriptions receive matching retained messages immediately
    mqtt.subscribe("picomqtt/#", [](const char * topic, const char * payload) {
        Serial.printf("Message in topic '%s': %s\n", topic, payload);
    });
}

void loop() {
    mqtt.loop();

    if (millis() - last_publish >= 5000) {
        // Retain the latest uptime, new subscribers will receive it right away
        mqtt.publish("picomqtt/uptime", String(millis() / 1000), 0, true);
        Serial.printf("Retained messages stored: %u\n",
                      (unsigned int)mqtt.get_retained_message_count());
        last_publish = millis();
    }
}
