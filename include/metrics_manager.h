// metrics_manager.h
#pragma once
#include <atomic>

class MetricsManager {
public:
    static MetricsManager& instance() {
        static MetricsManager m;
        return m;
    }

    void incr_rpc_calls() {
        rpc_call_count.fetch_add(1, std::memory_order_relaxed);
    }

    void incr_concurrent() {
        concurrent_rpc.fetch_add(1, std::memory_order_relaxed);
    }

    void decr_concurrent() {
        concurrent_rpc.fetch_sub(1, std::memory_order_relaxed);
    }

    int64_t get_rpc_calls() const {
        return rpc_call_count.load(std::memory_order_relaxed);
    }

    int64_t get_concurrent() const {
        return concurrent_rpc.load(std::memory_order_relaxed);
    }

private:
    MetricsManager() : rpc_call_count(0), concurrent_rpc(0) {}
    std::atomic<int64_t> rpc_call_count;
    std::atomic<int64_t> concurrent_rpc;
};
