// Load test: many producers submit jobs, then many consumers drain them.
// Usage: ./build/loadtest <producers> <jobs_per_producer> <consumers>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

static int connect_broker() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(7000);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "could not connect to broker on port 7000\n";
        std::exit(1);
    }
    return fd;
}

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
        if (n <= 0) return "";
        buf.append(tmp, n);
    }
}

static std::string request(int fd, std::string& buf, const std::string& cmd) {
    std::string msg = cmd + "\n";
    send(fd, msg.data(), msg.size(), MSG_NOSIGNAL);
    return read_line(fd, buf);
}

static double pct(std::vector<double>& v, double p) {
    if (v.empty()) return 0;
    size_t i = static_cast<size_t>(p * (v.size() - 1));
    return v[i];
}

int main(int argc, char** argv) {
    int producers = argc > 1 ? std::atoi(argv[1]) : 4;
    int per_producer = argc > 2 ? std::atoi(argv[2]) : 1000;
    int consumers = argc > 3 ? std::atoi(argv[3]) : 4;
    long total = static_cast<long>(producers) * per_producer;

    // ---- Phase 1: submit ----
    std::vector<std::vector<double>> lat(producers);
    std::vector<std::thread> threads;
    auto t0 = Clock::now();
    for (int p = 0; p < producers; ++p) {
        threads.emplace_back([&, p] {
            int fd = connect_broker();
            std::string buf;
            lat[p].reserve(per_producer);
            for (int i = 0; i < per_producer; ++i) {
                auto s = Clock::now();
                request(fd, buf, "SUBMIT 1 load payload");
                lat[p].push_back(std::chrono::duration<double, std::milli>(Clock::now() - s).count());
            }
            close(fd);
        });
    }
    for (auto& t : threads) t.join();
    double submit_s = std::chrono::duration<double>(Clock::now() - t0).count();

    std::vector<double> all;
    for (auto& v : lat) all.insert(all.end(), v.begin(), v.end());
    std::sort(all.begin(), all.end());

    // ---- Phase 2: consume ----
    std::atomic<long> consumed{0};
    threads.clear();
    t0 = Clock::now();
    for (int c = 0; c < consumers; ++c) {
        threads.emplace_back([&] {
            int fd = connect_broker();
            std::string buf;
            while (true) {
                std::string reply = request(fd, buf, "FETCH");
                if (reply.empty() || reply == "EMPTY") break;
                // reply looks like: JOB <id> <type> <payload>
                size_t a = reply.find(' ');
                size_t b = reply.find(' ', a + 1);
                std::string id = reply.substr(a + 1, b - a - 1);
                request(fd, buf, "ACK " + id);
                consumed++;
            }
            close(fd);
        });
    }
    for (auto& t : threads) t.join();
    double consume_s = std::chrono::duration<double>(Clock::now() - t0).count();

    int fd = connect_broker();
    std::string buf;
    std::string stats = request(fd, buf, "STATS");
    close(fd);

    std::cout << "---- QueueForge load test ----\n";
    std::cout << producers << " producers x " << per_producer << " jobs = " << total << " jobs, "
              << consumers << " consumers\n";
    std::cout << "SUBMIT : " << static_cast<long>(total / submit_s) << " jobs/sec"
              << "   latency p50 " << pct(all, 0.50) << " ms, p99 " << pct(all, 0.99) << " ms\n";
    std::cout << "CONSUME: " << static_cast<long>(consumed.load() / consume_s) << " jobs/sec"
              << "   (" << consumed.load() << " jobs fetched + acked)\n";
    std::cout << "Final  : " << stats << "\n";
    return consumed.load() == total ? 0 : 2;
}