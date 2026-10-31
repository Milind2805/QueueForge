#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>

#include "queueforge/job_queue.hpp"
#include "queueforge/wal.hpp"

using namespace qf;
using Clock = std::chrono::steady_clock;

class Broker {
public:
    Broker(Wal& wal, uint64_t first_id) : wal_(wal), next_id_(first_id) {}

    // Put jobs recovered from the log back where they belong.
    void restore(const std::vector<WalJob>& jobs) {
        std::lock_guard<std::mutex> lock(mu_);
        int64_t now = wall_ms();
        for (const auto& w : jobs) {
            Job job;
            job.id = w.id;
            job.priority = w.priority;
            job.type = w.type;
            job.payload = w.payload;
            if (w.run_at_ms > now) {
                delayed_.emplace(Clock::now() + std::chrono::milliseconds(w.run_at_ms - now),
                                 std::move(job));
            } else {
                queue_.push(std::move(job));
            }
        }
    }

    std::string handle(const std::string& line) {
        std::istringstream in(line);
        std::string cmd;
        in >> cmd;

        if (cmd == "SUBMIT") {
            Job job;
            if (!(in >> job.priority >> job.type))
                return "ERR usage: SUBMIT <priority> <type> [payload]\n";
            read_payload(in, job);
            job.id = next_id_++;
            uint64_t id = job.id;
            wal_.log_submit(to_wal(job, 0));  // on disk BEFORE we say OK
            queue_.push(std::move(job));
            return "OK " + std::to_string(id) + "\n";
        }

        if (cmd == "SUBMIT_AT") {
            int delay_s;
            Job job;
            if (!(in >> delay_s >> job.priority >> job.type) || delay_s < 0)
                return "ERR usage: SUBMIT_AT <delay_s> <priority> <type> [payload]\n";
            read_payload(in, job);
            job.id = next_id_++;
            uint64_t id = job.id;
            wal_.log_submit(to_wal(job, wall_ms() + delay_s * 1000LL));
            std::lock_guard<std::mutex> lock(mu_);
            delayed_.emplace(Clock::now() + std::chrono::seconds(delay_s), std::move(job));
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
            running_[job->id] = Running{*job, Clock::now() + kVisibilityTimeout};
            return reply;
        }

        if (cmd == "ACK" || cmd == "FAIL") {
            uint64_t id;
            if (!(in >> id)) return "ERR usage: " + cmd + " <id>\n";
            std::lock_guard<std::mutex> lock(mu_);
            auto it = running_.find(id);
            if (it == running_.end()) return "ERR unknown job\n";
            Job job = it->second.job;
            running_.erase(it);
            if (cmd == "ACK") {
                wal_.log_ack(id);
                done_++;
            } else if (job.attempts >= job.max_attempts) {
                wal_.log_dead(id);
                dead_++;
            } else {
                queue_.push(std::move(job));
            }
            return "OK\n";
        }

        if (cmd == "STATS") {
            std::lock_guard<std::mutex> lock(mu_);
            return "queued=" + std::to_string(queue_.size()) +
                   " delayed=" + std::to_string(delayed_.size()) +
                   " running=" + std::to_string(running_.size()) +
                   " done=" + std::to_string(done_) +
                   " dead=" + std::to_string(dead_) + "\n";
        }

        return "ERR unknown command\n";
    }

    // Called by the timer thread twice a second.
    void tick() {
        auto now = Clock::now();
        std::lock_guard<std::mutex> lock(mu_);

        while (!delayed_.empty() && delayed_.begin()->first <= now) {
            queue_.push(std::move(delayed_.begin()->second));
            delayed_.erase(delayed_.begin());
        }

        for (auto it = running_.begin(); it != running_.end();) {
            if (it->second.deadline <= now) {
                Job job = it->second.job;
                it = running_.erase(it);
                std::cout << "[broker] job " << job.id << " timed out\n";
                if (job.attempts >= job.max_attempts) {
                    wal_.log_dead(job.id);
                    dead_++;
                } else {
                    queue_.push(std::move(job));
                }
            } else {
                ++it;
            }
        }
    }

private:
    struct Running {
        Job job;
        Clock::time_point deadline;
    };
    // Must be longer than the slowest job, or healthy jobs get taken back.
    static constexpr std::chrono::seconds kVisibilityTimeout{15};

    static void read_payload(std::istringstream& in, Job& job) {
        std::getline(in, job.payload);
        if (!job.payload.empty() && job.payload[0] == ' ') job.payload.erase(0, 1);
    }

    static WalJob to_wal(const Job& j, int64_t run_at_ms) {
        WalJob w;
        w.id = j.id;
        w.priority = j.priority;
        w.run_at_ms = run_at_ms;
        w.type = j.type;
        w.payload = j.payload;
        return w;
    }

    Wal& wal_;
    JobQueue queue_;
    std::mutex mu_;  // protects delayed_, running_, done_, dead_
    std::multimap<Clock::time_point, Job> delayed_;
    std::unordered_map<uint64_t, Running> running_;
    std::atomic<uint64_t> next_id_;
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
    // 1. Recover state from the log before accepting any connection.
    Wal wal("queueforge.wal");
    uint64_t max_id = 0;
    std::vector<WalJob> live = wal.recover(max_id);
    if (!wal.compact_and_open(live)) {
        std::cerr << "cannot open queueforge.wal\n";
        return 1;
    }
    Broker broker(wal, max_id + 1);
    broker.restore(live);
    std::cout << "Recovered " << live.size() << " job(s) from queueforge.wal\n";

    // 2. Start listening.
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

    std::thread([&broker] {
        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            broker.tick();
        }
    }).detach();

    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd < 0) continue;
        std::thread(serve_client, client_fd, std::ref(broker)).detach();
    }
}