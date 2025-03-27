#include <zmq.hpp>
#include <iostream>
#include <sys/resource.h>
#include <signal.h>
#include <thread>
#include <chrono>
#include <execinfo.h>
#include <zlib.h>
#include "../include/ring_buffer.h"
#include "telemetry.h"

class Agent {
    zmq::context_t context{1};
    zmq::socket_t socket{context, ZMQ_REQ};
    std::string node_id;
    RingBuffer<Telemetry, 60> stats; // 1-min window

    std::string serialize(const Telemetry& t) {
        std::ostringstream oss;
        oss << node_id << ":" << t.memory_mb << ":" << t.cpu_time << ":" << t.io_blocks 
            << ":" << (t.is_anomaly ? "ANOMALY" : "") << ":" << compress_string(t.stack_trace);
        return oss.str();
    }

public:
    Agent() : node_id(std::getenv("NODE_ID") ? std::getenv("NODE_ID") : "node1") {
        socket.connect("tcp://ingestion:5556");
    }

    void run() {
        signal(SIGSEGV, [](int sig) {
            Telemetry t{0, 0, 0, false, Agent::get_stack_trace()};
            Agent().report("NODE_CRASH", t);
            exit(1);
        });

        while (true) {
            Telemetry t = collect_telemetry();
            stats.push(t);
            t.is_anomaly = detect_anomaly();
            report(t.memory_mb > 1000 ? "MEMORY_EXCEEDED" : "MEMORY", t);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }

private:
    Telemetry collect_telemetry() {
        struct rusage usage;
        getrusage(RUSAGE_SELF, &usage);
        return {usage.ru_maxrss / 1024.0, 
                usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6, 
                usage.ru_oublock, false, ""};
    }

    bool detect_anomaly() {
        std::vector<double> window;
        for (size_t i = 0; i < stats.size(); i++) window.push_back(stats[i].memory_mb);
        if (window.size() < 10) return false;
        double mean = std::accumulate(window.begin(), window.end(), 0.0) / window.size();
        double stddev = std::sqrt(std::accumulate(window.begin(), window.end(), 0.0,
            [mean](double a, double b) { return a + (b - mean) * (b - mean); }) / window.size());
        return stddev > 0 && std::abs(stats.back().memory_mb - mean) > 3 * stddev;
    }

    void report(const std::string& event, const Telemetry& t) {
        std::string msg = event + ":" + serialize(t);
        zmq::message_t request(msg.c_str(), msg.size());
        socket.send(request, zmq::send_flags::none);
        zmq::message_t reply;
        socket.recv(reply);
    }

    static std::string get_stack_trace() {
        void* array[20];
        size_t size = backtrace(array, 20);
        char** strings = backtrace_symbols(array, size);
        std::string result;
        for (size_t i = 0; i < size; i++) result += strings[i] + std::string(";");
        free(strings);
        return compress_string(result);
    }

    static std::string compress_string(const std::string& str) {
        z_stream zs{};
        deflateInit(&zs, Z_BEST_SPEED);
        zs.next_in = (Bytef*)str.data();
        zs.avail_in = str.size();
        std::string out(1024, 0);
        zs.next_out = (Bytef*)out.data();
        zs.avail_out = out.size();
        deflate(&zs, Z_FINISH);
        deflateEnd(&zs);
        return out.substr(0, zs.total_out);
    }
};

int main() {
    Agent agent;
    agent.run();
    return 0;
}