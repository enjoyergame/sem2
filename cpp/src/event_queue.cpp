#include "p2pcore/event_queue.hpp"

#include <utility>

namespace p2p {

void EventQueue::push(Event ev) {
    {
        std::lock_guard lock(mutex_);
        queue_.push_back(std::move(ev));
    }
    cv_.notify_one();
}

std::vector<Event> EventQueue::poll(std::size_t max) {
    std::lock_guard lock(mutex_);
    return take_locked(max);
}

std::vector<Event> EventQueue::wait(std::chrono::milliseconds timeout, std::size_t max) {
    std::unique_lock lock(mutex_);
    cv_.wait_for(lock, timeout, [this] { return !queue_.empty() || !open_; });
    return take_locked(max);
}

void EventQueue::open() {
    std::lock_guard lock(mutex_);
    open_ = true;
}

void EventQueue::close() {
    {
        std::lock_guard lock(mutex_);
        open_ = false;
    }
    cv_.notify_all();
}

std::size_t EventQueue::size() const {
    std::lock_guard lock(mutex_);
    return queue_.size();
}

std::vector<Event> EventQueue::take_locked(std::size_t max) {
    std::vector<Event> out;
    const std::size_t n = queue_.size() < max ? queue_.size() : max;
    out.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        out.push_back(std::move(queue_.front()));
        queue_.pop_front();
    }
    return out;
}

}  // namespace p2p
