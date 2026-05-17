#pragma once

#include <memory>
#include <functional>
#include <vector>
#include <queue>
#include "../common/noncopyable.h"

namespace server {

class EventLoop;
class ThreadPool;

class TcpConnection : public std::enable_shared_from_this<TcpConnection>,
                      public common::noncopyable {
public:
    using MessageCallback = std::function<void(const std::vector<char>&, TcpConnection*)>;

    TcpConnection(int fd, EventLoop* loop, ThreadPool* pool);
    ~TcpConnection();

    int fd() const { return fd_; }
    void handleRead();
    void handleWrite();   // 留作扩展
    void handleClose();

    void send(const char* data, size_t len);
    void setMessageCallback(MessageCallback cb) { message_cb_ = std::move(cb); }

    void shutdown();
    
    std::string getClientIP() const { return client_ip_; }
    uint16_t getClientPort() const { return client_port_; }

private:
    void closeConnection();
    void enableWriting();   // 注册 EPOLLOUT 事件
    void disableWriting();  // 注销 EPOLLOUT 事件
    void writeToBuffer(const char* data, size_t len);  // 写缓冲区

    int fd_;
    EventLoop* loop_;
    ThreadPool* pool_;
    MessageCallback message_cb_;

    std::vector<char> read_buf_;
    std::queue<std::vector<char>> write_buffer_;  // 写缓冲区队列（多个包）
    bool writing_;  // 是否正在等待 EPOLLOUT
    bool closed_;
    
    // 添加写事件标志
    static const int READ_EVENT = 0x01;
    static const int WRITE_EVENT = 0x02;

    std::string client_ip_;   // 客户端 IP
    uint16_t client_port_;    // 客户端端口
};

} // namespace server