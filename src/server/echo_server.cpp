#include "event_loop.h"
#include "tcp_connection.h"
#include "thread_pool.h"
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <memory>
#include <sys/epoll.h>
#include <vector>
#include <signal.h>
#include <atomic>

using namespace server;

// 全局标志，用于通知程序退出
std::atomic<bool> g_running(true);

// 信号处理函数（这就是你要的那个函数）
void signalHandler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        std::cout << "\nReceived signal " << sig << ", shutting down..." << std::endl;
        g_running = false;
    }
}

void onMessage(const std::vector<char>& data, TcpConnection* conn) {
    std::string msg(data.begin(), data.end());
    
    // 打印客户端 IP 和端口
    std::cout << "[" << conn->getClientIP() << ":" << conn->getClientPort() << "] ";
    std::cout << "Recv: " << msg << std::endl;
    
    // 可选：回复时带上客户端信息
    std::string response = "[" + conn->getClientIP() + ":" + std::to_string(conn->getClientPort()) + "] Echo: " + msg;
    conn->send(response.c_str(), response.size());
}

class SimpleConnectionManager {
public:
    void add(int fd, std::shared_ptr<TcpConnection> conn) {
        conns_[fd] = conn;
    }
    
    void remove(int fd) {
        conns_.erase(fd);
    }
    
private:
    std::unordered_map<int, std::shared_ptr<TcpConnection>> conns_;
};

class ConnectionManager {
public:
    void add(int fd, std::shared_ptr<TcpConnection> conn) {
        connections_[fd] = conn;
    }
    
    void remove(int fd) {
        connections_.erase(fd);
    }
    
    std::shared_ptr<TcpConnection> get(int fd) {
        auto it = connections_.find(fd);
        if (it != connections_.end()) {
            return it->second;
        }
        return nullptr;
    }
    
private:
    std::unordered_map<int, std::shared_ptr<TcpConnection>> connections_;
};

int main() {
    // 注册信号处理函数
    signal(SIGINT, signalHandler);   // Ctrl+C 会触发 SIGINT
    signal(SIGTERM, signalHandler);  // kill 命令会触发 SIGTERM
    signal(SIGPIPE, SIG_IGN);        // 忽略 SIGPIPE，防止写已关闭连接时崩溃
    
    const int port = 8888;
    
    int listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (listen_fd == -1) {
        perror("socket");
        return 1;
    }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        perror("bind");
        close(listen_fd);
        return 1;
    }

    if (listen(listen_fd, 128) == -1) {
        perror("listen");
        close(listen_fd);
        return 1;
    }

    EventLoop loop;
    ThreadPool pool(4);
    ConnectionManager conn_manager;

    int epfd = loop.getEpollFd();
    epoll_event ev;
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = listen_fd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev) == -1) {
        perror("epoll_ctl add listen");
        close(listen_fd);
        return 1;
    }

    std::cout << "Echo Server running on port " << port << std::endl;
    std::cout << "Press Ctrl+C to stop" << std::endl;

    const int MAX_EVENTS = 1024;
    std::vector<epoll_event> events(MAX_EVENTS);
    
    while (g_running) {  // 检查全局标志，而不是死循环
        int nfds = epoll_wait(epfd, events.data(), MAX_EVENTS, 1000);  // 1秒超时，便于检查退出标志
        
        if (nfds == -1) {
            if (errno == EINTR) continue;  // 被信号中断，继续
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < nfds; ++i) {
            if (events[i].data.fd == listen_fd) {
                while (true) {
                    struct sockaddr_in client_addr;
                    socklen_t len = sizeof(client_addr);
                    int client_fd = accept(listen_fd, (struct sockaddr*)&client_addr, &len);
                    if (client_fd == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        perror("accept");
                        break;
                    }
                    
                    int flags = fcntl(client_fd, F_GETFL, 0);
                    fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);
                    
                    auto conn = std::make_shared<TcpConnection>(client_fd, &loop, &pool);
                    conn->setMessageCallback(onMessage);
                    
                    epoll_event conn_ev;
                    conn_ev.events = EPOLLIN | EPOLLET;
                    conn_ev.data.ptr = conn.get();
                    if (epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &conn_ev) == -1) {
                        perror("epoll_ctl add connection");
                        continue;
                    }
                    
                    conn_manager.add(client_fd, conn);
                    
                    char ip[INET_ADDRSTRLEN];
                    inet_ntop(AF_INET, &client_addr.sin_addr, ip, sizeof(ip));
                    std::cout << "New connection from " << ip << ":" << ntohs(client_addr.sin_port) << std::endl;
                }
            } else {
                auto* conn = static_cast<TcpConnection*>(events[i].data.ptr);
                if (events[i].events & EPOLLIN) {
                    conn->handleRead();
                }
                if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                    conn->handleClose();
                    conn_manager.remove(conn->fd());
                }
            }
        }
    }

    std::cout << "Shutting down..." << std::endl;
    close(listen_fd);
    return 0;
}