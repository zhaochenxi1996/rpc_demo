#pragma once

#include <memory>
#include <functional>
#include <vector>
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

private:
    void closeConnection();

    int fd_;
    EventLoop* loop_;
    ThreadPool* pool_;
    MessageCallback message_cb_;

    std::vector<char> read_buf_;
    bool closed_;
};

} // namespace server