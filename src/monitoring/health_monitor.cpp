#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <string>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <grpcpp/grpcpp.h>
#include "health_monitor.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

// Configuration parameters
struct MonitorConfig {
    std::string node_id;
    std::string sentinel_address;
    int heartbeat_interval_ms = 1000;
    int resource_check_interval_ms = 5000;
    int log_check_interval_ms = 10000;
    std::vector<std::string> processes_to_monitor;
};

// Health metrics collected by the agent
struct HealthMetrics {
    double cpu_usage_percent;
    double memory_usage_percent;
    double disk_usage_percent;
    double network_throughput_mbps;
    std::unordered_map<std::string, bool> process_status;
    std::vector<std::string> recent_errors;
    std::chrono::system_clock::time_point timestamp;
};

class HealthMonitorAgent {
public:
    HealthMonitorAgent(const MonitorConfig& config) 
        : config_(config), 
          running_(false),
          latest_metrics_{} {
    }

    ~HealthMonitorAgent() {
        stop();
    }

    void start() {
        if (running_.load()) return;
        
        running_.store(true);
        
        // Start monitoring threads
        heartbeat_thread_ = std::thread(&HealthMonitorAgent::heartbeat_loop, this);
        resource_monitor_thread_ = std::thread(&HealthMonitorAgent::monitor_resources, this);
        log_analyzer_thread_ = std::thread(&HealthMonitorAgent::analyze_logs, this);
        
        std::cout << "Health Monitor Agent started on node: " << config_.node_id << std::endl;
    }

    void stop() {
        if (!running_.load()) return;
        
        running_.store(false);
        cv_.notify_all();
        
        if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
        if (resource_monitor_thread_.joinable()) resource_monitor_thread_.join();
        if (log_analyzer_thread_.joinable()) log_analyzer_thread_.join();
        
        std::cout << "Health Monitor Agent stopped" << std::endl;
    }

    HealthMetrics get_latest_metrics() const {
        std::lock_guard<std::mutex> lock(metrics_mutex_);
        return latest_metrics_;
    }

private:
    // Send regular heartbeats to the central Sentinel
    void heartbeat_loop() {
        while (running_.load()) {
            try {
                send_heartbeat();
            } catch (const std::exception& e) {
                std::cerr << "Error in heartbeat: " << e.what() << std::endl;
            }
            
            // Sleep with interruption support
            std::unique_lock<std::mutex> lock(cv_mutex_);
            cv_.wait_for(lock, 
                std::chrono::milliseconds(config_.heartbeat_interval_ms), 
                [this]() { return !running_.load(); });
        }
    }

    // Monitor system resources (CPU, memory, disk, network)
    void monitor_resources() {
        while (running_.load()) {
            try {
                collect_resource_metrics();
                check_process_health();
                report_metrics();
            } catch (const std::exception& e) {
                std::cerr << "Error monitoring resources: " << e.what() << std::endl;
            }
            
            std::unique_lock<std::mutex> lock(cv_mutex_);
            cv_.wait_for(lock, 
                std::chrono::milliseconds(config_.resource_check_interval_ms), 
                [this]() { return !running_.load(); });
        }
    }

    // Analyze system and application logs for errors
    void analyze_logs() {
        while (running_.load()) {
            try {
                check_system_logs();
                check_application_logs();
            } catch (const std::exception& e) {
                std::cerr << "Error analyzing logs: " << e.what() << std::endl;
            }
            
            std::unique_lock<std::mutex> lock(cv_mutex_);
            cv_.wait_for(lock, 
                std::chrono::milliseconds(config_.log_check_interval_ms), 
                [this]() { return !running_.load(); });
        }
    }

    // Send heartbeat signal to the central monitoring system
    void send_heartbeat() {
        // In a real implementation, this would use gRPC to send a heartbeat message
        std::cout << "Sending heartbeat from node " << config_.node_id << std::endl;
    }

    // Collect CPU, memory, disk, and network metrics
    void collect_resource_metrics() {
        std::lock_guard<std::mutex> lock(metrics_mutex_);
        
        // In a real implementation, these would use system calls to get actual metrics
        latest_metrics_.cpu_usage_percent = get_cpu_usage();
        latest_metrics_.memory_usage_percent = get_memory_usage();
        latest_metrics_.disk_usage_percent = get_disk_usage();
        latest_metrics_.network_throughput_mbps = get_network_throughput();
        latest_metrics_.timestamp = std::chrono::system_clock::now();
    }

    // Check if monitored processes are running
    void check_process_health() {
        std::lock_guard<std::mutex> lock(metrics_mutex_);
        
        for (const auto& process : config_.processes_to_monitor) {
            latest_metrics_.process_status[process] = is_process_running(process);
        }
    }

    // Check system logs for errors
    void check_system_logs() {
        std::lock_guard<std::mutex> lock(metrics_mutex_);
        
        // In a real implementation, this would parse system logs
        // For now, we'll just add a placeholder error detection
        std::vector<std::string> new_errors = parse_system_logs();
        latest_metrics_.recent_errors.insert(
            latest_metrics_.recent_errors.end(),
            new_errors.begin(),
            new_errors.end()
        );
    }

    // Check application-specific logs
    void check_application_logs() {
        std::lock_guard<std::mutex> lock(metrics_mutex_);
        
        // In a real implementation, this would parse application logs
        std::vector<std::string> new_errors = parse_application_logs();
        latest_metrics_.recent_errors.insert(
            latest_metrics_.recent_errors.end(),
            new_errors.begin(),
            new_errors.end()
        );
    }

    // Send collected metrics to the central monitoring system
    void report_metrics() {
        HealthMetrics metrics = get_latest_metrics();
        
        // In a real implementation, this would use gRPC to send metrics
        std::cout << "Reporting metrics from node " << config_.node_id << ":" << std::endl;
        std::cout << "  CPU: " << metrics.cpu_usage_percent << "%" << std::endl;
        std::cout << "  Memory: " << metrics.memory_usage_percent << "%" << std::endl;
        std::cout << "  Disk: " << metrics.disk_usage_percent << "%" << std::endl;
        std::cout << "  Network: " << metrics.network_throughput_mbps << " Mbps" << std::endl;
    }

    // Helper methods to get actual system metrics (placeholder implementations)
    double get_cpu_usage() { return 30.0 + (rand() % 40); }
    double get_memory_usage() { return 40.0 + (rand() % 30); }
    double get_disk_usage() { return 50.0 + (rand() % 20); }
    double get_network_throughput() { return 100.0 + (rand() % 900); }
    bool is_process_running(const std::string& process) { return (rand() % 100) < 95; }
    std::vector<std::string> parse_system_logs() { 
        std::vector<std::string> errors;
        if (rand() % 100 < 5) {
            errors.push_back("SYSTEM: High disk I/O latency detected");
        }
        return errors;
    }
    std::vector<std::string> parse_application_logs() { 
        std::vector<std::string> errors;
        if (rand() % 100 < 3) {
            errors.push_back("APP: Connection timeout to database");
        }
        return errors;
    }

private:
    MonitorConfig config_;
    std::atomic<bool> running_;
    HealthMetrics latest_metrics_;
    
    std::thread heartbeat_thread_;
    std::thread resource_monitor_thread_;
    std::thread log_analyzer_thread_;
    
    mutable std::mutex metrics_mutex_;
    std::mutex cv_mutex_;
    std::condition_variable cv_;
};

// Simple command-line interface to test the agent
int main(int argc, char* argv[]) {
    MonitorConfig config;
    config.node_id = "node-1";
    config.sentinel_address = "localhost:50051";
    config.heartbeat_interval_ms = 1000;
    config.resource_check_interval_ms = 5000;
    config.log_check_interval_ms = 10000;
    config.processes_to_monitor = {"compute_task", "database", "web_server"};
    
    HealthMonitorAgent agent(config);
    agent.start();
    
    std::cout << "Health Monitor Agent running. Press enter to stop." << std::endl;
    std::cin.get();
    
    agent.stop();
    return 0;
}