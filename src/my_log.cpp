#include "my_log.h"
#include <fcntl.h>

myLog::myLog()
{
    m_count = 0;
    m_stop_flag = false;
}

myLog::~myLog()
{
    m_stop_flag = true;

    // 通知日志线程退出
    if (m_log_thread.joinable()) {
        m_log_thread.join();
    }

    // 写入队列剩余日志
    {
        std::lock_guard<std::mutex> lock(global_queue_mutex);
        std::string log;
        while (global_log_queue->pop(log)) {
            ::write(m_fd, log.c_str(), log.size());
        }
    }

    if (m_fd != -1) {
        close(m_fd);
        m_fd = -1;
        std::cout << "Log file closed." << std::endl;
    }
}

void myLog::flush_local_buffer() {
    std::lock_guard<std::mutex> lock(global_queue_mutex);
    for (auto& log : ThreadLocalBuffers::local_buffer) {
        global_log_queue->push(std::move(log));
    }
    ThreadLocalBuffers::local_buffer.clear();
}


bool myLog::init(const char *file_name, int close_log, int log_buf_size,
                 int split_lines, int max_queue_size)
{
    global_log_queue = std::make_shared<block_queue<std::string>>(max_queue_size);

    m_close_log = close_log;
    m_log_buf_size = log_buf_size;
    m_buf = new char[m_log_buf_size];
    memset(m_buf, '\0', m_log_buf_size);
    m_split_lines = split_lines;

    time_t t = time(NULL);
    struct tm *sys_tm = localtime(&t);
    struct tm my_tm = *sys_tm;

    const char *p = strrchr(file_name, '/');
    char log_full_name[256] = {0};
    if (p == NULL) {
        snprintf(log_full_name, 256, "%d_%02d_%02d_%s",
                 my_tm.tm_year + 1900, my_tm.tm_mon + 1, my_tm.tm_mday, file_name);
    } else {
        strcpy(log_name, p + 1);
        strncpy(dir_name, file_name, p - file_name + 1);
        snprintf(log_full_name, 256, "%s%d_%02d_%02d_%s",
                 dir_name, my_tm.tm_year + 1900, my_tm.tm_mon + 1, my_tm.tm_mday, log_name);
    }

    m_today = my_tm.tm_mday;
    m_fd = open(log_full_name, O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (m_fd == -1) {
        std::cerr << "Failed to open log file: " << log_full_name << std::endl;
        return false;
    }

    // 启动异步日志线程
    m_log_thread = std::thread(&myLog::async_write_log, this);

    std::cout << "Log file initialized: " << log_full_name << std::endl;

    return true;
}

void myLog::write_log(LogLevel level, const std::string &message)
{
    using namespace std::chrono;

    auto now = system_clock::now();
    auto in_time_t = system_clock::to_time_t(now);
    auto micro = duration_cast<microseconds>(now.time_since_epoch()).count() % 1000000;
    std::tm tm_time = *std::localtime(&in_time_t);

    static const char *level_str[] = {"[debug]:", "[info]:", "[warn]:", "[erro]:"};
    const char *level_tag = level_str[static_cast<int>(level)];

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_count++;

        if (m_today != tm_time.tm_mday || m_count % m_split_lines == 0) {
            close(m_fd);

            std::ostringstream oss;
            oss << std::setfill('0') << std::setw(4) << tm_time.tm_year + 1900 << "_"
                << std::setw(2) << tm_time.tm_mon + 1 << "_"
                << std::setw(2) << tm_time.tm_mday << "_";

            std::string tail = oss.str();
            std::string new_log;
            if (m_today != tm_time.tm_mday) {
                new_log = dir_name + tail + log_name;
                m_today = tm_time.tm_mday;
                m_count = 0;
            } else {
                new_log = dir_name + tail + log_name + "." + std::to_string(m_count / m_split_lines);
            }

            m_fd = open(new_log.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
        }
    }

    std::ostringstream log_stream;
    log_stream << std::setfill('0')
               << (tm_time.tm_year + 1900) << "-"
               << std::setw(2) << (tm_time.tm_mon + 1) << "-"
               << std::setw(2) << tm_time.tm_mday << " "
               << std::setw(2) << tm_time.tm_hour << ":"
               << std::setw(2) << tm_time.tm_min << ":"
               << std::setw(2) << tm_time.tm_sec << "."
               << std::setw(6) << micro << " "
               << level_tag << " "
               << message << "\n";

    std::string log_str = log_stream.str();
    ThreadLocalBuffers::local_buffer.emplace_back(std::move(log_str));

    if (ThreadLocalBuffers::local_buffer.size() >= ThreadLocalBuffers::BUFFER_SIZE) {
        std::lock_guard<std::mutex> lock(global_queue_mutex);
        for (auto &log : ThreadLocalBuffers::local_buffer) {
            global_log_queue->push(std::move(log));
        }
        ThreadLocalBuffers::local_buffer.clear();
    }
}

void myLog::async_write_log()
{
    std::vector<std::string> batch;
    while (!m_stop_flag.load()) {
        {
            std::lock_guard<std::mutex> lock(global_queue_mutex);
            std::string log;
            while (global_log_queue->pop(log)) {
                batch.push_back(std::move(log));
            }
        }

        if (!batch.empty()) {
            std::lock_guard<std::mutex> lock(m_mutex);
            for (auto &msg : batch) {
                ::write(m_fd, msg.c_str(), msg.size());
            }
            // write() 已直接进内核 page cache，无需 userspace flush
            batch.clear();
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

void myLog::flush()
{
    // write() 直接进内核 page cache，无需 userspace flush；保留接口供兼容调用
}
