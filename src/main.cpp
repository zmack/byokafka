#include <arpa/inet.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <netdb.h>
#include <optional>
#include <span>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

class Serializable {
    public:
        virtual std::vector<uint8_t> serialize() const = 0;
        virtual ~Serializable() = default;
};

class NetworkBuffer {
    std::vector<uint8_t> buffer;

    public:
        NetworkBuffer(): buffer() {}

        size_t push_back(uint16_t value) {
            auto network_order = htons(value);
            const uint8_t* bytes = reinterpret_cast<const uint8_t*>(&network_order);
            buffer.insert(buffer.end(), bytes, bytes + sizeof(uint16_t));
            return sizeof(uint16_t);
        }

        size_t push_back(uint32_t value) {
            auto network_order = htonl(value);
            const uint8_t* bytes = reinterpret_cast<const uint8_t*>(&network_order);
            buffer.insert(buffer.end(), bytes, bytes + sizeof(uint32_t));
            return sizeof(uint32_t);
        }

        size_t push_back(uint8_t value) {
            buffer.push_back(value);
            return sizeof(uint32_t);
        }

        size_t push_back_varlong(uint32_t value) {
            auto n = value;
            do {
                unsigned char c = n & 0x7F;
                if ((n >>= 7) > 0) {
                    c |= 0x80;
                }
                buffer.push_back(c);
            } while (n > 0);

            return sizeof(value);
        }

        const std::vector<uint8_t> get() const {
            return buffer;
        }

        void inspect() {
            for (auto byte : buffer) {
                std::cout << std::hex << (int) byte << " ";
            }
            std::cout << std::dec << std::endl;
        }
};

struct RequestHeader {
    uint16_t request_api_key;
    uint16_t request_api_version;
    uint32_t correlation_id;
    std::optional<std::string> client_id;
    std::vector<uint8_t> tag_buffer;

    void to_host_order() {
        request_api_key = ntohs(request_api_key);
        request_api_version = ntohs(request_api_version);
        correlation_id = ntohl(correlation_id);
    }
};

class ErrorResponse : public Serializable {
    uint16_t error_code;

public:
    ErrorResponse(uint16_t error_code): error_code(error_code) {}

    std::vector<uint8_t> serialize() const override {
        auto buffer = NetworkBuffer{};
        buffer.push_back(error_code);
        return buffer.get();
    }
};

struct APIVersionsV4APIKeys {
    uint16_t api_key;
    uint16_t min_version;
    uint16_t max_version;
};

class APIVersionsResponseV4 : public Serializable {
    uint16_t error_code;
    std::vector<APIVersionsV4APIKeys> api_keys;
    uint32_t throttle_time_ms;

public:
    APIVersionsResponseV4(uint16_t error_code, uint32_t throttle_time_ms):
        error_code(error_code), throttle_time_ms(throttle_time_ms), api_keys({}) {}

    std::vector<uint8_t> serialize() const override {
        auto versions = APIVersionsV4APIKeys{18, 4, 4};
        std::cout << "Serializing error_code(" << versions.api_key << ")"
                << " throttle(" << throttle_time_ms << ")" << std::endl;

        auto buffer = NetworkBuffer{};
        buffer.push_back(error_code);
        buffer.push_back_varlong((uint32_t) 2);
        buffer.push_back(versions.api_key);
        buffer.push_back(versions.min_version);
        buffer.push_back(versions.max_version);
        buffer.push_back((uint8_t) 0);
        buffer.push_back(throttle_time_ms);
        buffer.push_back((uint8_t) 0);
        buffer.inspect();
        return buffer.get();
    }
};

bool sendHeader(int client_fd, uint32_t correlation_id, const std::vector<uint8_t> &additional_data) {
    uint bufferSize = sizeof(int32_t) + sizeof(uint32_t) + additional_data.size();
    char buffer[bufferSize];

    uint32_t c_id = htonl(correlation_id);
    uint32_t headerSize = htonl(bufferSize - sizeof(int32_t));
    std::cout << "Header size: " << headerSize << std::endl;

    memset(buffer, 0, sizeof(buffer));

    memcpy(buffer, &headerSize, sizeof(headerSize));
    memcpy(buffer + sizeof(int32_t), &c_id, sizeof(uint32_t));

    if (!additional_data.empty()) {
        memcpy(buffer + sizeof(int32_t) + sizeof(uint32_t), additional_data.data(), additional_data.size());
    }

    auto bytesSent = send(client_fd, &buffer, bufferSize, 0);

    if (bytesSent != sizeof(buffer)) {
        std::cerr << "Failed to send header" << std::endl;
        return false;
    } else {
        std::cout << "Sent " << bytesSent << " bytes " << std::endl;
    }
    return true;
}

std::vector<uint8_t> generateResponse(const RequestHeader& request_header) {
    std::cout << "Generating response for " << request_header.request_api_key << std::endl;

    if (request_header.request_api_key == 18 && request_header.request_api_version <= 4) {
        auto api_versions_response = std::make_shared<APIVersionsResponseV4>(
            APIVersionsResponseV4{0, 13}
        );

        auto serialized_response = api_versions_response->serialize();

        std::cout << "Response size: " << serialized_response.size() << std::endl;

        return serialized_response;
    } else {
        auto error_response = std::make_shared<ErrorResponse>(ErrorResponse{35});

        auto response = error_response->serialize();
        return response;
    }
}

int main(int argc, char* argv[]) {
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket: " << std::endl;
        return 1;
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        close(server_fd);
        std::cerr << "setsockopt failed: " << std::endl;
        return 1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
        close(server_fd);
        std::cerr << "Failed to bind to port 9092" << std::endl;
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        close(server_fd);
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";

    struct sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    std::cerr << "Logs from your program will appear here!\n";

    // Uncomment this block to pass the first stage
    int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr*>(&client_addr), &client_addr_len);
    std::cout << "Client connected\n";

    uint32_t message_size;
    RequestHeader request_header;

    recv(client_fd, &message_size, sizeof(message_size), 0);
    recv(client_fd, &request_header, ntohl(message_size), 0);
    request_header.to_host_order();

    std::cout << sizeof(request_header) << std::endl;
    std::cout << "Correlation ID: " << request_header.correlation_id << std::endl;
    std::cout << "Message Client: " << request_header.client_id.value_or("None") << std::endl;
    std::cout << "Request API Key: " << request_header.request_api_key << std::endl;
    std::cout << "Request API Version: " << request_header.request_api_version << std::endl;

    auto response = generateResponse(request_header);
    std::cout << "Response size: " << response.size() << std::endl;
    sendHeader(client_fd, request_header.correlation_id, response);

    close(client_fd);
    close(server_fd);
    return 0;
}
