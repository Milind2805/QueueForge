#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <functional>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>

#include "queueforge/job_queue.hpp"

using namespace qf;

class Broker {
public:
    std::string handle(const std::string& line) {
        std::istringstream in(line);
        std::string cmd;
        in >> cmd;

        if (cmd == "SUBMIT") {
            Job job;
            if (!(in >> job.priority >> job.type))
                return "ERR usage: SUBMIT <priority> <type> [payload]\n";
            std::getline(in, job.payload);
            if (!job.payload.empty() && job.payload[0] == ' ') job.payload.erase(0, 1);
            job.id = next_id_++;
            uint64_t id = job.id;
            queue_.push(std::move(job));
            return "OK " + std::to_string(id) + "\n";
        }

        if (cmd == "FETCH") {
            auto job = queue_.pop(std::chrono::milliseconds(0));
            if (!job) return "EMPTY\n";
            job->state = JobState::Running;
            job->attempts++;
            std::string reply = "JOB " + std::to_string(job->id) + " " +
                                job->type + " " + job->payload + "\n";
            std::lock_guard<std::mutex> lock(mu_);
            running_[job->id] = *job;
            return reply;
        }

        if (cmd == "ACK" || cmd == "FAIL") {
            uint64_t id;
            if (!(in >> id)) return "ERR usage: " + cmd + " <id>\n";
            std::lock_guard<std::mutex> lock(mu_);
            auto it = running_.find(id);
            if (it == running_.end()) return "ERR unknown job\n";
            Job job = it->second;
            running_.erase(it);
            if (cmd == "ACK") {
                done_++;
            } else if (job.attempts >= job.max_attempts) {
                dead_++;
            } else {
                queue_.push(std::move(job));
            }
            return "OK\n";
        }

        if (cmd == "STATS") {
            std::lock_guard<std::mutex> lock(mu_);
            return "queued=" + std::to_string(queue_.size()) +
                   " running=" + std::to_string(running_.size()) +
                   " done=" + std::to_string(done_) +
                   " dead=" + std::to_string(dead_) + "\n";
        }

        return "ERR unknown command\n";
    }

private:
    JobQueue queue_;
    std::mutex mu_;  // protects running_, done_, dead_
    std::unordered_map<uint64_t, Job> running_;
    std::atomic<uint64_t> next_id_{1};
    int done_ = 0;
    int dead_ = 0;
};

void serve_client(int fd, Broker& broker) {
    std::string buf;
    char tmp[1024];
    ssize_t n;
    while ((n = read(fd, tmp, sizeof(tmp))) > 0) {
        buf.append(tmp, n);
        size_t pos;
        while ((pos = buf.find('\n')) != std::string::npos) {
            std::string line = buf.substr(0, pos);
            buf.erase(0, pos + 1);
            if (!line.empty() && line.back() == '\r') line.pop_back();
            if (line.empty()) continue;
            std::string reply = broker.handle(line);
            send(fd, reply.data(), reply.size(), MSG_NOSIGNAL);
        }
    }
    close(fd);
}

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(7000);

    if (bind(server_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "bind failed (is the port already in use?)\n";
        return 1;
    }
    listen(server_fd, 16);
    std::cout << "Broker listening on port 7000\n";

    Broker broker;
    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd < 0) continue;
        std::thread(serve_client, client_fd, std::ref(broker)).detach();
    }
}