#include "thread_pool.h"
#include "my_log.h"

class ThreadLocalGuard {
public:
    ~ThreadLocalGuard() {
        myLog::get_instance()->flush_local_buffer();
        LOG_INFO("[Thread] Local logs flushed");
    }
};

ThreadPool::ThreadPool(int thread_count) : stop(false) {
    for (int i = 0; i < thread_count; ++i) {
        workers.emplace_back([this]() { this->worker_loop(); });
    }
}

ThreadPool::~ThreadPool() {
    // 步骤1: 设置停止标志
    {
        std::lock_guard<std::mutex> lock(mtx);
        stop = true; // 必须设置停止标志
    }
    
    // 步骤2: 唤醒所有可能阻塞的线程
    cv.notify_all();
    
    // 步骤3: 等待所有工作线程退出
    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join(); // 确保线程正常退出
        }
    }
}

void ThreadPool::add_task(std::function<void()> func) {
    {
        std::lock_guard<std::mutex> lock(mtx);
        tasks.push(func);
    }
    cv.notify_all();
}

void ThreadPool::worker_loop() {
    LOG_INFO("[Thread] worker loop started");
    ThreadLocalGuard guard;
    while (true) {

        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait(lock, [&]() { return stop || !tasks.empty(); });

            if (stop && tasks.empty()) {
                break;  // 所有任务完成，准备退出
            }

            // 从队列中获取任务
            if (!tasks.empty()) {
                task = std::move(tasks.front());
                tasks.pop();
            }
        }

        // 将任务加入调度器
        if (task) {
            scheduler.add_coroutine(std::move(task));
            LOG_INFO("[Thread] added task to scheduler");
            scheduler.run();  // 调度协程
        }
    }
}




