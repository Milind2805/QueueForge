#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(7000);

    bind(server_fd, (sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 16);
    std::cout << "Broker listening on port 7000\n";

    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        char buf[1024];
        ssize_t n;
        while ((n = read(client_fd, buf, sizeof(buf))) > 0) {
            write(client_fd, buf, n);  // echo back
        }
        close(client_fd);
    }
}