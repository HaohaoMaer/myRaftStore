#include "connection_pool.h"
#include "my_log.h"
#include <cstring>
#include <sys/select.h>

namespace myrpc {

// ── 超时常量（与 my_rpc.cpp 保持一致） ─────────────────────────────────────
static constexpr int kCpConnectTimeoutMs = 3000;
static constexpr int kCpRwTimeoutMs      = 5000;

// ── 析构：关闭所有空闲连接 ────────────────────────────────────────────────
ConnectionPool::~ConnectionPool() {
    std::lock_guard<std::mutex> lock(mtx_);
    for (auto& [key, ep] : pools_) {
        while (!ep.idle_fds.empty()) {
            close(ep.idle_fds.front());
            ep.idle_fds.pop();
        }
    }
}

void ConnectionPool::set_max_per_endpoint(int n) {
    std::lock_guard<std::mutex> lock(mtx_);
    max_per_endpoint_ = n;
}

// ── acquire ──────────────────────────────────────────────────────────────────
int ConnectionPool::acquire(const std::string& ip, int port) {
    std::string key = make_key(ip, port);

    // 尝试从空闲队列取出一个健康连接（锁外做健康检测，避免长时间持锁）
    while (true) {
        int fd = -1;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto& ep = pools_[key];
            if (ep.idle_fds.empty()) break;
            fd = ep.idle_fds.front();
            ep.idle_fds.pop();
            // total 不变：fd 从 idle 变为 in-use，仍算作已分配
        }

        if (is_alive(fd)) return fd;  // 复用成功

        // 连接失效，废弃并继续尝试下一个
        close(fd);
        {
            std::lock_guard<std::mutex> lock(mtx_);
            pools_[key].total--;
        }
    }

    // 无可用空闲连接，尝试新建
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto& ep = pools_[key];
        if (ep.total >= max_per_endpoint_) {
            LOG_WARN("[Pool] Connection limit reached for " + key);
            return -1;
        }
        ep.total++;  // 预占名额
    }

    int fd = make_connection(ip, port);
    if (fd == -1) {
        std::lock_guard<std::mutex> lock(mtx_);
        pools_[key].total--;  // 新建失败，归还名额
        return -1;
    }
    return fd;
}

// ── release ──────────────────────────────────────────────────────────────────
void ConnectionPool::release(const std::string& ip, int port, int fd) {
    std::string key = make_key(ip, port);
    bool should_close = false;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto& ep = pools_[key];
        if (static_cast<int>(ep.idle_fds.size()) < max_per_endpoint_) {
            ep.idle_fds.push(fd);
        } else {
            ep.total--;
            should_close = true;
        }
    }
    if (should_close) close(fd);
}

// ── discard ──────────────────────────────────────────────────────────────────
void ConnectionPool::discard(const std::string& ip, int port, int fd) {
    close(fd);
    std::lock_guard<std::mutex> lock(mtx_);
    pools_[make_key(ip, port)].total--;
}

// ── make_connection ───────────────────────────────────────────────────────────
int ConnectionPool::make_connection(const std::string& ip, int port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) return -1;

    // 设置读写超时
    struct timeval tv;
    tv.tv_sec  = kCpRwTimeoutMs / 1000;
    tv.tv_usec = (kCpRwTimeoutMs % 1000) * 1000;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    // 非阻塞 connect + select 超时
    fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFL) | O_NONBLOCK);

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(port);
    addr.sin_addr.s_addr = inet_addr(ip.c_str());

    int ret = connect(sockfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (ret < 0 && errno != EINPROGRESS) {
        close(sockfd);
        return -1;
    }

    if (ret != 0) {
        fd_set wfds;
        FD_ZERO(&wfds);
        FD_SET(sockfd, &wfds);
        struct timeval conn_tv;
        conn_tv.tv_sec  = kCpConnectTimeoutMs / 1000;
        conn_tv.tv_usec = (kCpConnectTimeoutMs % 1000) * 1000;
        if (select(sockfd + 1, nullptr, &wfds, nullptr, &conn_tv) <= 0) {
            LOG_ERROR("[Pool] connect timeout to " + ip + ":" + std::to_string(port));
            close(sockfd);
            return -1;
        }
        int err = 0;
        socklen_t errlen = sizeof(err);
        getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &err, &errlen);
        if (err != 0) {
            LOG_ERROR("[Pool] connect failed: " + std::string(strerror(err)));
            close(sockfd);
            return -1;
        }
    }

    // 恢复阻塞模式（调用方期望阻塞 fd）
    fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFL) & ~O_NONBLOCK);
    return sockfd;
}

// ── is_alive ─────────────────────────────────────────────────────────────────
bool ConnectionPool::is_alive(int fd) {
    char buf[1];
    ssize_t n = recv(fd, buf, 1, MSG_PEEK | MSG_DONTWAIT);
    if (n == 0) return false;                                           // EOF
    if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) return true;  // 健康
    return false;                                                       // 其他错误
}

} // namespace myrpc
