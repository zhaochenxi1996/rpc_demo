#include "event_loop.h"
#include "tcp_connection.h"
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <cassert>
#include <cstring>
#include <cerrno>

namespace server {

EventLoop::EventLoop() : stop_(false) {
    epfd_ = epoll_create1(0);
    assert(epfd_ != -1);

    eventfd_ = eventfd(0, EFD_NONBLOCK);
    assert(eventfd_ != -1);

    epoll_event ev;
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = eventfd_;
    epoll_ctl(epfd_, EPOLL_CTL_ADD, eventfd_, &ev);
}

EventLoop::~EventLoop() {
    close(epfd_);
    close(eventfd_);
}

void EventLoop::loop() {
    const int MAX_EVENTS = 1024;
    events_.resize(MAX_EVENTS);

    while (!stop_) {
        int nfds = epoll_wait(epfd_, events_.data(), MAX_EVENTS, -1);
        if (nfds == -1) {
            if (errno == EINTR) continue;
            break;
        }

        for (int i = 0; i < nfds; ++i) {
            if (events_[i].data.fd == eventfd_) {
                uint64_t val;
                read(eventfd_, &val, sizeof(val));
                continue;
            }
            handleEvent(events_[i]);
        }
    }
}

void EventLoop::stop() {
    stop_ = true;
    uint64_t val = 1;
    write(eventfd_, &val, sizeof(val));
}

void EventLoop::updateConnection(TcpConnection* conn, int operation, int events) {
    epoll_event ev;
    // 根据 events 参数设置 epoll 事件
    ev.events = 0;
    if (events & EVENT_READ) ev.events |= (EPOLLIN | EPOLLET);
    if (events & EVENT_WRITE) ev.events |= (EPOLLOUT | EPOLLET);
    
    ev.data.ptr = conn;

    if (operation == EPOLL_CTL_ADD || operation == EPOLL_CTL_MOD) {
        // 回调函数保持不变
        callbacks_[conn->fd()] = [this](TcpConnection* c, uint32_t epoll_events) {
            if (epoll_events & EPOLLIN) c->handleRead();
            if (epoll_events & EPOLLOUT) c->handleWrite();
            if (epoll_events & (EPOLLERR | EPOLLHUP)) c->handleClose();
        };
    } else if (operation == EPOLL_CTL_DEL) {
        callbacks_.erase(conn->fd());
    }
    
    epoll_ctl(epfd_, operation, conn->fd(), &ev);
}

void EventLoop::handleEvent(const epoll_event& ev) {
    auto* conn = static_cast<TcpConnection*>(ev.data.ptr);
    auto it = callbacks_.find(conn->fd());
    if (it != callbacks_.end()) {
        it->second(conn, ev.events);
    }
}

void EventLoop::addTimer(std::function<void()> cb, int timeout_ms) {
    // 简化：这里不做真正的定时器，留作扩展
    (void)cb;
    (void)timeout_ms;
}

} // namespace server