#pragma once

#include <memory>
#include <functional>
#include <unordered_map>
#include <vector>
#include "../common/noncopyable.h"

struct epoll_event;

namespace server {

class TcpConnection;

class EventLoop : public common::noncopyable {
public:
    using Callback = std::function<void(TcpConnection*, uint32_t events)>;

    EventLoop();
    ~EventLoop();

    void loop();
    void stop();

    void updateConnection(TcpConnection* conn, int operation);
    void addTimer(std::function<void()> cb, int timeout_ms);
    
    // 添加公有方法：获取 epoll fd（用于添加 listen fd）
    int getEpollFd() const { return epfd_; }

    bool isStopped() const { return stop_; }
    void setStop(bool s) { stop_ = s; }

private:
    void handleEvent(const epoll_event& ev);

    int epfd_;
    int eventfd_;
    bool stop_;
    std::vector<epoll_event> events_;
    std::unordered_map<int, Callback> callbacks_;
};

} // namespace server