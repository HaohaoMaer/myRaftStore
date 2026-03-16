#ifndef MY_LOG_H
#define MY_LOG_H

#include <stdio.h>
#include <iostream>
#include <string>
#include <cstring>
#include <stdarg.h>
#include <thread>
#include <mutex>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <fstream>
#include <cstdarg>
#include <memory>
#include <atomic>
#include "blockQueue.h"

class myLog
{
public:
    static myLog *get_instance()
    {
        static myLog instance;
        return &instance;
    }

    void flush_log_thread() {
        get_instance()->async_write_log();
    }

    bool is_open() const {
        return m_close_log == 0;
    }    

    enum class LogLevel {
        DEBUG = 0,
        INFO = 1,
        WARN = 2,
        ERROR = 3
    };

    bool init(const char *fileName, int close_log, int log_buf_size = 8192,
         int split_lines = 5000000, int max_queue_size = 0);

    void write_log(LogLevel level, const std::string& message);

    void flush_local_buffer();

    void flush(void);

private:
    myLog();
    virtual ~myLog();

    void async_write_log();

    char dir_name[128];
    char log_name[128];
    int m_split_lines;
    int m_log_buf_size;
    long long m_count;
    int m_today;
    int m_fd{-1};       // 低级文件描述符，write() 直接进内核 page cache
    int m_close_log{1}; // 默认关闭（1=closed），init() 后置为 0
    char *m_buf;
    // block_queue<std::string> *m_log_queue; //阻塞队列
    bool m_is_async;
    std::mutex m_mutex;
    std::atomic<bool> m_stop_flag{false};
    std::thread m_log_thread;
};

#define LOG_DEBUG(msg) \
    if (myLog::get_instance()->is_open()) { \
        myLog::get_instance()->write_log(myLog::LogLevel::DEBUG, msg); \
        if (ThreadLocalBuffers::local_buffer.size() >= ThreadLocalBuffers::BUFFER_SIZE/2) {\
            myLog::get_instance()->flush_local_buffer(); \
        }\
    }

#define LOG_INFO(msg) \
if (myLog::get_instance()->is_open()) { \
    myLog::get_instance()->write_log(myLog::LogLevel::INFO, msg); \
    if (ThreadLocalBuffers::local_buffer.size() >= ThreadLocalBuffers::BUFFER_SIZE/2) {\
        myLog::get_instance()->flush_local_buffer(); \
    }\
}

#define LOG_WARN(msg) \
if (myLog::get_instance()->is_open()) { \
    myLog::get_instance()->write_log(myLog::LogLevel::WARN, msg); \
    if (ThreadLocalBuffers::local_buffer.size() >= ThreadLocalBuffers::BUFFER_SIZE/2) {\
        myLog::get_instance()->flush_local_buffer(); \
    }\
}

#define LOG_ERROR(msg) \
if (myLog::get_instance()->is_open()) { \
    myLog::get_instance()->write_log(myLog::LogLevel::ERROR, msg); \
    if (ThreadLocalBuffers::local_buffer.size() >= ThreadLocalBuffers::BUFFER_SIZE/2) {\
        myLog::get_instance()->flush_local_buffer(); \
    }\
}

// #define LOG_DEBUG(msg) if (myLog::get_instance()->is_open()) { myLog::get_instance()->write_log(myLog::LogLevel::DEBUG, msg); }
// #define LOG_INFO(msg)  if (myLog::get_instance()->is_open()) { myLog::get_instance()->write_log(myLog::LogLevel::INFO, msg); }
// #define LOG_WARN(msg)  if (myLog::get_instance()->is_open()) { myLog::get_instance()->write_log(myLog::LogLevel::WARN, msg); }
// #define LOG_ERROR(msg) if (myLog::get_instance()->is_open()) { myLog::get_instance()->write_log(myLog::LogLevel::ERROR, msg); }


#endif