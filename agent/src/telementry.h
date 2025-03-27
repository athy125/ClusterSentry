#ifndef TELEMETRY_H
#define TELEMETRY_H

struct Telemetry {
    double memory_mb;
    double cpu_time;
    uint64_t io_blocks;
    bool is_anomaly;
    std::string stack_trace;
};

#endif // TELEMETRY_H