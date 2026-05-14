#include "tcp_connection.h"
#include "event_loop.h"
#include "thread_pool.h"
#include <unistd.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <iostream>
#include <errno.h>
#include <cstring>
#include <arpa/inet.h>  // 添加这个头文件（用于 inet_ntop）

namespace server {

TcpConnection::TcpConnection(int fd, EventLoop* loop, ThreadPool* pool)
    : fd_(fd), loop_(loop), pool_(pool), closed_(false) {
    
    // 获取客户端地址信息
    struct sockaddr_in client_addr;
    socklen_t addr_len = sizeof(client_addr);
    if (getpeername(fd_, (struct sockaddr*)&client_addr, &addr_len) == 0) {
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, ip, sizeof(ip));
        client_ip_ = ip;
        client_port_ = ntohs(client_addr.sin_port);
    } else {
        client_ip_ = "unknown";
        client_port_ = 0;
    }
    
    // 设为非阻塞
    int flags = fcntl(fd_, F_GETFL, 0);
    fcntl(fd_, F_SETFL, flags | O_NONBLOCK);
}

TcpConnection::~TcpConnection() {
    if (fd_ != -1) close(fd_);
}

void TcpConnection::handleRead() {
    char buf[4096];
    while (true) {
        ssize_t n = read(fd_, buf, sizeof(buf));
        if (n == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            handleClose();
            return;
        } else if (n == 0) {
            handleClose();
            return;
        }
        read_buf_.insert(read_buf_.end(), buf, buf + n);
    }

    if (read_buf_.empty()) return;

    // 将业务处理交给线程池
    auto self = shared_from_this();
    pool_->submit([self, this]() {
        if (message_cb_) {
            message_cb_(read_buf_, this);
        }
        read_buf_.clear();
    });
}

void TcpConnection::handleClose() {
    if (closed_) return;
    closed_ = true;
    loop_->updateConnection(this, EPOLL_CTL_DEL);
    closeConnection();
}

void TcpConnection::send(const char* data, size_t len) {
    // 简化版：同步发送，阻塞式。实际应加入写缓冲 + epollout
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = write(fd_, data + sent, len - sent);
        if (n == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 这里应注册 EPOLLOUT 等待可写
                // 作业：完善非阻塞发送
                return;
            }
            handleClose();
            return;
        }
        sent += n;
    }
}

void TcpConnection::closeConnection() {
    if (fd_ != -1) {
        close(fd_);
        fd_ = -1;
    }
}

// handleWrite 空实现，留作业
void TcpConnection::handleWrite() {}

} // namespace server