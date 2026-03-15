#include "blockQueue.h"

namespace ThreadLocalBuffers {
    thread_local std::vector<std::string> local_buffer; // 线程本地缓冲
}

std::shared_ptr<block_queue<std::string>> global_log_queue; // 全局队列
std::mutex global_queue_mutex; // 全局队列互斥锁