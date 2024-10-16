//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/2.
// src/include/common/channel.h
//
//===-----------------------------------------------------


#pragma once

#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT
#include <queue>
#include <utility>

namespace distribution_lsh {
/**
 * Channels allow for safe sharing of data between threads. This is a multi-producer multi-consumer channel.
 */
template<class T>
class Channel {
 public:
  Channel() = default;
  ~Channel() = default;

  /**
   * @brief Inserts an element into a shared queue.
   *
   * @param element The element to be inserted.
   */
  void Put(T element) {
    std::unique_lock<std::mutex> lk(m_);
    q_.push(std::move(element));
    lk.unlock();
    cv_.notify_all();
  }

  /**
   * @brief Gets an element from the shared queue. If the queue is empty, blocks until an element is available.
   */
  auto Get() -> T {
    std::unique_lock<std::mutex> lk(m_);
    cv_.wait(lk, [&]() { return !q_.empty(); });
    T element = std::move(q_.front());
    q_.pop();
    return element;
  }

 private:
  std::mutex m_;
  std::condition_variable cv_;
  std::queue<T> q_;
};

}// namespace distribution_lsh
