#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <thread>

static std::string read_line(int fd, std::string& buf) {
    while (true) {
        size_t pos = buf.find('\n');
        if (pos != std::string::npos) {
            std::string line = buf.substr(0, pos);
            buf.erase(0, pos + 1);
            return line;
        }
        char tmp[1024];
        ssize_t n = read(fd, tmp, sizeof(tmp));
        if (n <= 0) return "";  // broker closed the connection
        buf.append(tmp, n);
    }
}

static void send_line(int fd, const std::string& s) {
    std::string msg = s + "\n";
    send(fd, msg.data(), msg.size(), MSG_NOSIGNAL);
}

int main(int argc, char** argv) {
    std::string name = argc > 1 ? argv[1] : "1";

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(7000);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "could not connect to broker\n";
        return 1;
    }

    std::mt19937 rng{std::random_device{}()};
    std::uniform_int_distribution<int> work_ms(100, 300);
    std::uniform_int_distribution<int> pct(1, 100);

    std::string buf;
    while (true) {
        send_line(fd, "FETCH");
        std::string reply = read_line(fd, buf);
        if (reply.empty()) break;  // broker went away

        if (reply == "EMPTY") {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            continue;
        }

        std::istringstream in(reply);
        std::string tag, type;
        uint64_t id;
        in >> tag >> id >> type;
        if (type == "slow") std::this_thread::sleep_for(std::chrono::seconds(10));

        std::this_thread::sleep_for(std::chrono::milliseconds(work_ms(rng)));

        bool ok = pct(rng) > 20;  // pretend 20% of jobs fail
        send_line(fd, (ok ? "ACK " : "FAIL ") + std::to_string(id));
        read_line(fd, buf);  // read the "OK"
        std::cout << "[worker " << name << "] job " << id << " " << type
                  << (ok ? " done\n" : " failed\n");
    }
    close(fd);
    return 0;
}