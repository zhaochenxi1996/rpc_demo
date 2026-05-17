#pragma once

#include <memory>
#include <functional>
#include <unordered_map>
#include <vector>
#include "../common/noncopyable.h"

// 添加事件类型常量
enum EventType {
    EVENT_READ = 0x01,
    EVENT_WRITE = 0x02,
    EVENT_RW = 0x03
};

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

    void updateConnection(TcpConnection* conn, int operation, int events = EVENT_READ);
    
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