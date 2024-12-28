#include <cstdint>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>

bool sendHeader(int client_fd, uint32_t correlationId) {
    uint bufferSize = sizeof(int32_t) + sizeof(uint32_t);
    char buffer[bufferSize];

    uint32_t header = htonl(correlationId);
    uint32_t headerSize = htonl(sizeof(header));
    memset(buffer, 0, sizeof(buffer));
    memcpy(buffer, &headerSize, sizeof(headerSize));
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
    sendHeader(client_fd, 7);

    close(client_fd);
    close(server_fd);
    return 0;
}
