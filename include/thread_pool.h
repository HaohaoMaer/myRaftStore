#pragma once
#include <functional>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include "mini_coroutine.h"  // 包含 Scheduler 和 Coroutine 定义

class ThreadPool {
public:
    ThreadPool(int thread_count);
    ~ThreadPool();

    void add_task(std::function<void()> func);

private:
    void worker_loop();

    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex mtx;
    std::condition_variable cv;
    std::atomic<bool> stop;
};
