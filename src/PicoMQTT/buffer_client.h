#pragma once

#include <Arduino.h>
#include <Client.h>

#include <limits>

#include "config.h"
#include "debug.h"

namespace PicoMQTT {

// Client which reads data from a memory buffer.  Used to pass locally
// published payloads to callbacks expecting an IncomingPacket.
class BufferClient : public ::Client {
public:
    BufferClient(const void * ptr) : ptr((const char *)ptr) { TRACE_FUNCTION; }

    // these methods are nop dummies
    virtual int connect(IPAddress ip, uint16_t port) override final {
        TRACE_FUNCTION;
        return 0;
    }
    virtual int connect(const char * host, uint16_t port) override final {
        TRACE_FUNCTION;
        return 0;
    }
#ifdef PICOMQTT_EXTRA_CONNECT_METHODS
    virtual int connect(IPAddress ip, uint16_t port,
                        int32_t timeout) override final {
        TRACE_FUNCTION;
        return 0;
    }
    virtual int connect(const char * host, uint16_t port,
                        int32_t timeout) override final {
        TRACE_FUNCTION;
        return 0;
    }
#endif
    virtual size_t write(const uint8_t * buffer, size_t size) override final {
        TRACE_FUNCTION;
        return 0;
    }
    virtual size_t write(uint8_t value) override final {
        TRACE_FUNCTION;
        return 0;
    }
    virtual void flush() override final { TRACE_FUNCTION; }
    virtual void stop() override final { TRACE_FUNCTION; }

    // these methods are in jasager mode
    virtual int available() override final {
        TRACE_FUNCTION;
        return std::numeric_limits<int>::max();
    }
    virtual operator bool() override final {
        TRACE_FUNCTION;
        return true;
    }
    virtual uint8_t connected() override final {
        TRACE_FUNCTION;
        return true;
    }

    // actual reads implemented here
    virtual int read(uint8_t * buf, size_t size) override {
        memcpy(buf, ptr, size);
        ptr += size;
        return size;
    }

    virtual int read() override final {
        TRACE_FUNCTION;
        uint8_t ret;
        read(&ret, 1);
        return ret;
    }

    virtual int peek() override final {
        TRACE_FUNCTION;
        const int ret = read();
        --ptr;
        return ret;
    }

protected:
    const char * ptr;
};

// BufferClient reading from PROGMEM.
class BufferClientP : public BufferClient {
public:
    using BufferClient::BufferClient;

    virtual int read(uint8_t * buf, size_t size) override {
        memcpy_P(buf, ptr, size);
        ptr += size;
        return size;
    }
};

}  // namespace PicoMQTT
