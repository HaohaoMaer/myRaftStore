#pragma once

#include <ucontext.h>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <deque>
#include <memory>
#include <functional>
#include <atomic>
#include <iostream>

// 协程栈大小
constexpr int STACK_SIZE = 1024 * 64;

class Coroutine {
public:
    enum class State { READY = 0, RUNNING, BLOCKED, FINISHED };
    using Func = std::function<void()>;

    Coroutine(Func f);

    static void trampoline(Coroutine* co);

    ucontext_t ctx;
    char stack[STACK_SIZE];
    State state;
    Func func;
};

class Scheduler {
public:
    Scheduler();

    int create_coroutine(Coroutine::Func f);
    void add_coroutine(std::function<void()> func);
    void run();
    void yield_current();
    void block_current();            // 阻塞当前协程
    void wake(int coroutine_id);     // 唤醒某个协程

private:
    ucontext_t main_ctx;
    // std::mutex mutex;
    // std::condition_variable cond_var;
    std::vector<std::unique_ptr<Coroutine>> coroutines;
    std::deque<int> runnable_indices;
    std::atomic<bool> stop_flag{false};
    int current;
};

extern thread_local Scheduler scheduler;
void yield();
void block();
