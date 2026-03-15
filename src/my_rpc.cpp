#include "my_rpc.h"
#include <cstring>

namespace myrpc {

// ── 常量 ────────────────────────────────────────────────────────────────────
static constexpr int      kMaxConnections = 1000;

// ── 内部辅助：循环读取恰好 n 字节 ─────────────────────────────────────────
static bool read_n(int fd, char* buf, size_t n) {
    size_t total = 0;
    while (total < n) {
        ssize_t r = read(fd, buf + total, n - total);
        if (r <= 0) return false;
        total += static_cast<size_t>(r);
    }
    return true;
}

// ── 内部辅助：循环写出所有字节 ────────────────────────────────────────────
static bool write_all(int fd, const char* buf, size_t n) {
    size_t total = 0;
    while (total < n) {
        ssize_t w = write(fd, buf + total, n - total);
        if (w <= 0) return false;
        total += static_cast<size_t>(w);
    }
    return true;
}

// ────────────────────────────────────────────────────────────────────────────
//  RpcServer
// ────────────────────────────────────────────────────────────────────────────

void RpcServer::init(int close_log, int log_write, int thread_num) {
    m_close_log  = close_log;
    m_log_write  = log_write;
    m_thread_num = thread_num;
}

RpcServer::~RpcServer() {
    if (m_threadpool) {
        m_threadpool->~ThreadPool();
    }
    if (epfd_ != -1)     { close(epfd_);     }
    if (server_fd_ != -1){ close(server_fd_); }
}

void RpcServer::log_write() {}

void RpcServer::init_thread_pool() {
    m_threadpool = new ThreadPool(m_thread_num);
}

void RpcServer::register_service(google::protobuf::Service* service) {
    const google::protobuf::ServiceDescriptor* sd = service->GetDescriptor();
    for (int i = 0; i < sd->method_count(); ++i) {
        const google::protobuf::MethodDescriptor* md = sd->method(i);
        std::string full_name = service->GetDescriptor()->full_name() + "." + md->name();
        MethodProperty mp;
        mp.service = service;
        mp.method  = md;
        handlers_[full_name] = mp;
        std::cout << "[Server] Registered service: " << full_name << std::endl;
        LOG_INFO(std::string("[Server] Registered service: ") + full_name);
    }
}

void RpcServer::start() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ == -1) {
        perror("socket");
        return;
    }
    fcntl(server_fd_, F_SETFL, fcntl(server_fd_, F_GETFL) | O_NONBLOCK);

    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(port_);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd_, (sockaddr*)&addr, sizeof(addr)) == -1) {
        perror("bind");
        return;
    }
    if (listen(server_fd_, 10) == -1) {
        perror("listen");
        return;
    }
    LOG_INFO(std::string("[Server] Listening on port ") + std::to_string(port_));

    // epfd_ 提升为成员变量，供 handle_client() 重新注册 fd
    epfd_ = epoll_create1(0);
    epoll_event ev{}, events[100];
    ev.events  = EPOLLIN;
    ev.data.fd = server_fd_;
    epoll_ctl(epfd_, EPOLL_CTL_ADD, server_fd_, &ev);

    while (true) {
        int nfds = epoll_wait(epfd_, events, 100, -1);
        for (int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;
            if (fd == server_fd_) {
                // 接受所有待连接
                while (true) {
                    int client_fd = accept(server_fd_, nullptr, nullptr);
                    if (client_fd < 0) break;

                    // 连接数限制
                    if (active_connections_.load(std::memory_order_relaxed) >= kMaxConnections) {
                        LOG_WARN("[Server] Connection limit reached, rejecting new connection");
                        close(client_fd);
                        continue;
                    }
                    active_connections_.fetch_add(1, std::memory_order_relaxed);

                    fcntl(client_fd, F_SETFL, fcntl(client_fd, F_GETFL) | O_NONBLOCK);

                    // 使用 EPOLLONESHOT：触发一次后自动 disarm，防止多 worker 并发处理同一 fd
                    epoll_event client_ev{};
                    client_ev.events  = EPOLLIN | EPOLLONESHOT;
                    client_ev.data.fd = client_fd;
                    epoll_ctl(epfd_, EPOLL_CTL_ADD, client_fd, &client_ev);
                }
            } else {
                // EPOLLONESHOT 触发后已自动 disarm，直接交给线程池
                m_threadpool->add_task([this, fd]() {
                    handle_client(fd);
                });
            }
        }
    }
    myLog::get_instance()->flush_local_buffer();
}

void RpcServer::handle_client(int fd) {
    MetricsManager::instance().incr_rpc_calls();
    MetricsManager::instance().incr_concurrent();

    // fd 由 epoll 线程以非阻塞模式接受，在 worker 线程中改回阻塞模式
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) & ~O_NONBLOCK);

    // 读取 8 字节结构化 header：[4B method_name_len][4B args_len]
    char hdr[8];
    if (!read_n(fd, hdr, 8)) {
        LOG_ERROR("[Server] Failed to read header");
        close(fd);
        active_connections_.fetch_sub(1, std::memory_order_relaxed);
        MetricsManager::instance().decr_concurrent();
        return;
    }

    uint32_t method_name_len = ntohl(*reinterpret_cast<uint32_t*>(hdr));
    uint32_t args_len        = ntohl(*reinterpret_cast<uint32_t*>(hdr + 4));

    static constexpr uint32_t kMaxMsgSize = 4 * 1024 * 1024;
    if (method_name_len == 0 || method_name_len > kMaxMsgSize || args_len > kMaxMsgSize) {
        LOG_ERROR("[Server] Invalid header: method_name_len=" + std::to_string(method_name_len)
                  + " args_len=" + std::to_string(args_len));
        close(fd);
        active_connections_.fetch_sub(1, std::memory_order_relaxed);
        MetricsManager::instance().decr_concurrent();
        return;
    }

    std::string full_name(method_name_len, '\0');
    if (!read_n(fd, &full_name[0], method_name_len)) {
        LOG_ERROR("[Server] Failed to read method name");
        close(fd);
        active_connections_.fetch_sub(1, std::memory_order_relaxed);
        MetricsManager::instance().decr_concurrent();
        return;
    }

    std::string args(args_len, '\0');
    if (args_len > 0 && !read_n(fd, &args[0], args_len)) {
        LOG_ERROR("[Server] Failed to read args");
        close(fd);
        active_connections_.fetch_sub(1, std::memory_order_relaxed);
        MetricsManager::instance().decr_concurrent();
        return;
    }

    // 方法分发
    std::string result;
    auto it = handlers_.find(full_name);
    if (it != handlers_.end()) {
        MethodProperty& mp = it->second;
        google::protobuf::Service* service = mp.service;
        const google::protobuf::MethodDescriptor* method = mp.method;

        const google::protobuf::Message* req_proto = &service->GetRequestPrototype(method);
        const google::protobuf::Message* res_proto = &service->GetResponsePrototype(method);

        std::unique_ptr<google::protobuf::Message> request(req_proto->New());
        std::unique_ptr<google::protobuf::Message> response(res_proto->New());

        if (!request->ParseFromString(args)) {
            result = "Failed to parse request";
        } else {
            google::protobuf::Closure* done =
                google::protobuf::NewPermanentCallback([]() {});
            LOG_INFO(std::string("[Server] Calling method: ") + full_name);
            service->CallMethod(method, nullptr, request.get(), response.get(), done);
            LOG_INFO(std::string("[Server] Method call completed: ") + full_name);
            if (!response->SerializeToString(&result)) {
                result = "Failed to serialize response";
            }
        }
    } else {
        result = "Method not found";
    }

    // 发送响应：[4B response_len][response bytes]
    uint32_t resp_len_net = htonl(static_cast<uint32_t>(result.size()));
    bool write_ok = write_all(fd, reinterpret_cast<char*>(&resp_len_net), 4)
                 && write_all(fd, result.c_str(), result.size());

    if (write_ok) {
        // 持久连接：切回非阻塞模式，重新注册到 epoll（EPOLLONESHOT 需要 MOD 重新 arm）
        fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
        epoll_event ev{};
        ev.events  = EPOLLIN | EPOLLONESHOT;
        ev.data.fd = fd;
        if (epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) == 0) {
            // 连接保持，active_connections_ 不递减
            MetricsManager::instance().decr_concurrent();
            return;
        }
        // epoll_ctl 失败（fd 已无效），关闭连接
        LOG_ERROR("[Server] epoll_ctl MOD failed for fd " + std::to_string(fd));
    } else {
        LOG_ERROR("[Server] Failed to send response for: " + full_name);
    }

    close(fd);
    active_connections_.fetch_sub(1, std::memory_order_relaxed);
    MetricsManager::instance().decr_concurrent();
}

// ────────────────────────────────────────────────────────────────────────────
//  RpcClient
// ────────────────────────────────────────────────────────────────────────────

void RpcClient::init() {
    m_threadpool = new ThreadPool(thread_num_);
}

std::string RpcClient::call(const std::string& ip, int port,
                            const std::string& full_method_name,
                            const std::string& args) {
    ConnectionPool& pool = ConnectionPool::instance();

    // 至多重试 1 次（处理 is_alive 通过但实际发送时连接已断的窗口期）
    for (int attempt = 0; attempt < 2; ++attempt) {
        int sockfd = pool.acquire(ip, port);
        if (sockfd == -1) {
            LOG_ERROR("[Client] ConnectionPool exhausted for "
                      + ip + ":" + std::to_string(port));
            return "";
        }

        // 发送请求：[4B name_len][4B args_len][method_name][args]
        uint32_t name_len_net = htonl(static_cast<uint32_t>(full_method_name.size()));
        uint32_t args_len_net = htonl(static_cast<uint32_t>(args.size()));
        char req_hdr[8];
        memcpy(req_hdr,     &name_len_net, 4);
        memcpy(req_hdr + 4, &args_len_net, 4);

        // 接收响应
        char resp_hdr[4];
        uint32_t resp_len = 0;
        std::string response;

        bool ok = write_all(sockfd, req_hdr, 8)
               && write_all(sockfd, full_method_name.c_str(), full_method_name.size())
               && write_all(sockfd, args.c_str(), args.size())
               && read_n(sockfd, resp_hdr, 4)
               && [&]() {
                      resp_len = ntohl(*reinterpret_cast<uint32_t*>(resp_hdr));
                      static constexpr uint32_t kMax = 4 * 1024 * 1024;
                      if (resp_len == 0 || resp_len > kMax) return false;
                      response.resize(resp_len);
                      return read_n(sockfd, &response[0], resp_len);
                  }();

        if (ok) {
            pool.release(ip, port, sockfd);
            return response;
        }

        // 发送或接收失败，废弃连接并重试
        pool.discard(ip, port, sockfd);
        if (attempt == 0) {
            LOG_WARN("[Client] RPC call failed, retrying: " + full_method_name);
        }
    }

    LOG_ERROR("[Client] call failed after retry: " + full_method_name);
    return "";
}

void RpcClient::call_async(const std::string& ip, int port,
                           const std::string& full_method_name,
                           const std::string& args,
                           std::function<void(const std::string&)> callback) {
    m_threadpool->add_task([this, ip, port, full_method_name, args, callback]() {
        std::string resp = this->call(ip, port, full_method_name, args);
        callback(resp);
    });
}

} // namespace myrpc
