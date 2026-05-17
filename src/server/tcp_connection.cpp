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

// 新增：写缓冲区
void TcpConnection::writeToBuffer(const char* data, size_t len) {
    std::vector<char> buffer(data, data + len);
    write_buffer_.push(std::move(buffer));
}

// 新增：启用写事件（注册 EPOLLOUT）
void TcpConnection::enableWriting() {
    if (writing_) return;
    writing_ = true;
    loop_->updateConnection(this, EPOLL_CTL_MOD, WRITE_EVENT);  
    // 需要修改 EventLoop::updateConnection 支持事件类型
}

// 新增：禁用写事件
void TcpConnection::disableWriting() {
    if (!writing_) return;
    writing_ = false;
    loop_->updateConnection(this, EPOLL_CTL_MOD, READ_EVENT);
}

// 核心：非阻塞 send
void TcpConnection::send(const char* data, size_t len) {
    // 1. 先尝试直接发送
    size_t sent = 0;
    if (write_buffer_.empty()) {  // 如果写缓冲区为空，直接写
        ssize_t n = write(fd_, data, len);
        if (n == -1) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                handleClose();
                return;
            }
            // 缓冲区满，需要加入写队列
            sent = 0;
        } else if (n >= 0) {
            sent = n;
            if (sent == len) {
                // 全部发送完成，不需要加入队列
                return;
            }
        }
    }
    
    // 2. 没发完或缓冲区满，剩余数据加入写队列
    if (sent < len) {
        writeToBuffer(data + sent, len - sent);
    }
    
    // 3. 启用 EPOLLOUT 事件，等待可写
    enableWriting();
}

// 新增：handleWrite 实现
void TcpConnection::handleWrite() {
    if (write_buffer_.empty()) {
        // 没有待写数据，禁用写事件
        disableWriting();
        return;
    }
    
    // 循环发送队列中的数据
    while (!write_buffer_.empty()) {
        std::vector<char>& buffer = write_buffer_.front();
        ssize_t n = write(fd_, buffer.data(), buffer.size());
        
        if (n == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 缓冲区又满了，下次继续
                return;
            }
            // 出错了
            handleClose();
            return;
        }
        
        if (n < buffer.size()) {
            // 发送了一部分，没发完，保留剩余部分
            std::vector<char> remaining(buffer.begin() + n, buffer.end());
            write_buffer_.pop();
            write_buffer_.push(std::move(remaining));
            return;
        }
        
        // 这个包发完了
        write_buffer_.pop();
    }
    
    // 所有数据都发完了，禁用写事件
    disableWriting();
}

void TcpConnection::handleClose() {
    if (closed_) return;
    closed_ = true;
    loop_->updateConnection(this, EPOLL_CTL_DEL);
    closeConnection();
}


void TcpConnection::closeConnection() {
    if (fd_ != -1) {
        close(fd_);
        fd_ = -1;
    }
}

} // namespace server