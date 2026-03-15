#include "mini_coroutine.h"

Coroutine::Coroutine(Func f) : func(std::move(f)), state(Coroutine::State::READY) {
    getcontext(&ctx);
    ctx.uc_stack.ss_sp = stack;
    ctx.uc_stack.ss_size = STACK_SIZE;
    ctx.uc_link = nullptr;
    makecontext(&ctx, (void(*)())trampoline, 1, this);
}

void Coroutine::trampoline(Coroutine* co) {
    co->state = Coroutine::State::RUNNING;
    co->func();
    co->state = Coroutine::State::FINISHED;
    yield();
}

Scheduler::Scheduler() : current(-1) {
    getcontext(&main_ctx);
}

void Scheduler::add_coroutine(std::function<void()> func) {
    int id = coroutines.size();
    coroutines.emplace_back(std::make_unique<Coroutine>(func));
    // std::lock_guard<std::mutex> lock(mutex);
    runnable_indices.push_back(id);
    // cond_var.notify_one();
}

void Scheduler::run() {
    while (!stop_flag && !runnable_indices.empty()) {
        int coro_id = -1;
        {
            // std::unique_lock<std::mutex> lock(mutex);
            // cond_var.wait(lock, [&]() { 
            //     return !runnable_indices.empty() || stop_flag; 
            // });
            coro_id = runnable_indices.front();
            runnable_indices.pop_front();
        }

        current = coro_id;
        Coroutine* co = coroutines[coro_id].get();
        if (co->state == Coroutine::State::FINISHED || co->state == Coroutine::State::BLOCKED) {
            continue;
        }

        swapcontext(&main_ctx, &co->ctx);

        if (co->state == Coroutine::State::READY || co->state == Coroutine::State::RUNNING) {
            // std::lock_guard<std::mutex> lock(mutex);
            runnable_indices.push_back(coro_id);
        }
    }

    //std::cout << "[scheduler] all coroutines finished.\n";
}


void Scheduler::yield_current() {
    if (current >= 0 && current < (int)coroutines.size()) {
        Coroutine* co = coroutines[current].get();
        if (co->state != Coroutine::State::FINISHED && co->state != Coroutine::State::BLOCKED)
            co->state = Coroutine::State::READY;
        swapcontext(&co->ctx, &main_ctx);
    }
}

void Scheduler::block_current() {
    if (current >= 0 && current < (int)coroutines.size()) {
        coroutines[current]->state = Coroutine::State::BLOCKED;
        swapcontext(&coroutines[current]->ctx, &main_ctx);
    }
}

void Scheduler::wake(int coroutine_id) {
    if (coroutine_id >= 0 && coroutine_id < (int)coroutines.size()) {
        Coroutine* co = coroutines[coroutine_id].get();
        if (co->state == Coroutine::State::BLOCKED) {
            co->state = Coroutine::State::READY;
            runnable_indices.push_back(coroutine_id);
        }
    }
}

thread_local  Scheduler scheduler;

void yield() {
    scheduler.yield_current();
}

void block() {
    scheduler.block_current();
}
