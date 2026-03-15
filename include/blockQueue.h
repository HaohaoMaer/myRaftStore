// 修改后的 block_queue.h 文件

#ifndef BLOCK_QUEUE_H
#define BLOCK_QUEUE_H

#include <iostream>
#include <stdlib.h>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <string>
#include <sys/time.h>

template <typename T>
class block_queue {
public:
    block_queue(int max_size = 1000) {
        if (max_size <= 0) {
            exit(-1);
        }

        m_max_size = max_size;
        m_array = new T[max_size];
        m_size = 0;
        m_front = 0;
        m_back = -1;
    }

    ~block_queue() {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_array != nullptr) {
            delete[] m_array;
            m_array = nullptr;
        }
    }

    bool full() {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_size >= m_max_size;
    }

    bool empty() {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_size == 0;
    }

    bool front(T& value) {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_size == 0) {
            return false;
        }
        value = m_array[m_front];
        return true;
    }

    bool back(T& value) {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_size == 0) {
            return false;
        }
        value = m_array[m_back];
        return true;
    }

    bool pop(T& value) {
        std::unique_lock<std::mutex> lock(m_mutex);

        if (m_size <= 0) {
            return false; // 非阻塞模式
        }
        value = m_array[m_front];
        m_front = (m_front + 1) % m_max_size;
        --m_size;

        return true;
    }

    bool push(const T& value) {
        std::unique_lock<std::mutex> lock(m_mutex);

        if (m_size >= m_max_size) {
            m_cond.notify_all();
            return false;
        }

        m_back = (m_back + 1) % m_max_size;
        m_array[m_back] = value;
        ++m_size;

        m_cond.notify_one();

        return true;
    }

    int size() {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_size;
    }

    int max_size() {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_max_size;
    }

private:
    mutable std::mutex m_mutex;
    std::condition_variable m_cond;
    int m_max_size;
    int m_size;
    int m_front;
    int m_back;
    T* m_array;
};

namespace ThreadLocalBuffers {
    extern thread_local std::vector<std::string> local_buffer; // 线程本地缓冲
    constexpr size_t BUFFER_SIZE = 100; // 本地缓冲阈值
}

extern std::shared_ptr<block_queue<std::string>> global_log_queue; // 全局队列
extern std::mutex global_queue_mutex; // 全局队列互斥锁

#endif // BLOCK_QUEUE_H
