#include <arpa/inet.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
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
        virtual void deserialize(std::span<const std::byte> buffer) = 0;
        virtual ~Serializable() = default;
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

struct ErrorResponse {
    uint16_t error_code;
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
    APIVersionsResponseV4(uint16_t error_code,
        uint32_t throttle_time_ms):
    error_code(error_code), throttle_time_ms(throttle_time_ms), api_keys({}) {}

    std::vector<uint8_t> serialize() const override {
        std::vector<uint8_t> buffer;
        buffer.reserve(sizeof(error_code) + sizeof(throttle_time_ms));
        buffer.push_back(htons(error_code));
        buffer.push_back(htonl(throttle_time_ms));
        return buffer;
    }

    void deserialize(std::span<const std::byte> buffer) override {
    }
};

bool sendHeader(int client_fd, uint32_t correlation_id, std::span<const std::byte> additional_data) {
    uint bufferSize = sizeof(int32_t) + sizeof(uint32_t) + additional_data.size();
    char buffer[bufferSize];

    uint32_t header = htonl(correlation_id);
    uint32_t headerSize = htonl(sizeof(header));
    memset(buffer, 0, sizeof(buffer));
    memcpy(buffer, &headerSize, sizeof(headerSize));
    memcpy(buffer + sizeof(int32_t), &header, sizeof(uint32_t));
    if (!additional_data.empty()) {
        memcpy(buffer + sizeof(int32_t) + sizeof(uint32_t), additional_data.data(), additional_data.size());
    }
    memcpy(buffer + sizeof(int32_t), &header, sizeof(uint32_t));

    auto bytesSent = send(client_fd, &buffer, bufferSize, 0);

    if (bytesSent != sizeof(buffer)) {
        std::cerr << "Failed to send header" << std::endl;
        return false;
    } else {
        std::cout << "Sent " << bytesSent << " bytes " << std::endl;
    }
    return true;
}

std::span<const std::byte> generateResponse(const RequestHeader& request_header) {
    std::cout << "Generating response for " << request_header.request_api_key << std::endl;

    if (request_header.request_api_key == 18) {
        auto api_versions_response = std::make_shared<APIVersionsResponseV4>(
            APIVersionsResponseV4{0, 0}
        );

        auto serialized_response = api_versions_response->serialize();

        auto response = std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(&serialized_response),
            sizeof(serialized_response)
        );

        return response;
    } else {
        auto error_response = std::make_shared<ErrorResponse>(ErrorResponse{htons(35)});

        auto response = std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(&error_response),
            sizeof(error_response)
        );

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
    std::cout << "Request API Key: " << request_header.request_api_key << std::endl;
    std::cout << "Request API Version: " << request_header.request_api_version << std::endl;

    auto response = generateResponse(request_header);
    sendHeader(client_fd, request_header.correlation_id, response);

    close(client_fd);
    close(server_fd);
    return 0;
}
