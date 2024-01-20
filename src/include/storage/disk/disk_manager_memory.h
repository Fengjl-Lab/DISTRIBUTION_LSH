//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/5.
// src/include/storage/disk/disk_manager_memory.h
//
//===-----------------------------------------------------

#pragma once

#include <array>
#include <chrono>  // NOLINT
#include <cstring>
#include <fstream>
#include <future>  // NOLINT
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include <common/config.h>
#include <common/exception.h>
#include <common/logger.h>
#include <fmt/core.h>
#include <storage/disk/disk_manager.h>

namespace distribution_lsh {
/**
 * DiskManagerMemory replicates the utility of DiskManager on memory. It is primarily used for
 * data structure performance testing.
 */
class DiskManagerMemory : public DiskManager {
 public:
  explicit DiskManagerMemory(size_t pages);

  ~DiskManagerMemory() override { delete[] memory_; }

  /**
   * Write a page to the database file.
   * @param page_id id of the page
   * @param page_data raw page data
   */
  void WritePage(page_id_t page_id, const char *page_data) override;

  /**
   * Read a page from the database file.
   * @param page_id id of the page
   * @param[out] page_data output buffer
   */
  void ReadPage(page_id_t page_id, char *page_data) override;

 private:
  char *memory_;
};

/**
 * DiskManagerMemory replicates the utility of DiskManager on memory. It is primarily used for
 * data structure performance testing.
 */
class DiskManagerUnlimitedMemory : public DiskManager {
 public:
  DiskManagerUnlimitedMemory() { std::fill(recent_access_.begin(), recent_access_.end(), -1); }

  /**
   * Write a page to the database file.
   * @param page_id id of the page
   * @param page_data raw page data
   */
  void WritePage(page_id_t page_id, const char *page_data) override {
    ProcessLatency(page_id);

    std::unique_lock<std::mutex> l(mutex_);
    if (!thread_id_.has_value()) {
      thread_id_ = std::this_thread::get_id();
    }
    if (page_id >= static_cast<int>(data_.size())) {
      data_.resize(page_id + 1);
    }
    if (data_[page_id] == nullptr) {
      data_[page_id] = std::make_shared<ProtectedPage>();
    }
    std::shared_ptr<ProtectedPage> ptr = data_[page_id];
    std::unique_lock<std::shared_mutex> l_page(ptr->second);
    l.unlock();

    memcpy(ptr->first.data(), page_data, DISTRIBUTION_LSH_PAGE_SIZE);

    PostProcessLatency(page_id);
  }

  /**
   * Read a page from the database file.
   * @param page_id id of the page
   * @param[out] page_data output buffer
   */
  void ReadPage(page_id_t page_id, char *page_data) override {
    ProcessLatency(page_id);

    std::unique_lock<std::mutex> l(mutex_);
    if (!thread_id_.has_value()) {
      thread_id_ = std::this_thread::get_id();
    }
    if (page_id >= static_cast<int>(data_.size()) || page_id < 0) {
      fmt::println(stderr, "page {} not in range", page_id);
      std::terminate();
    }
    if (data_[page_id] == nullptr) {
      fmt::println(stderr, "page {} not exist", page_id);
      std::terminate();
    }
    std::shared_ptr<ProtectedPage> ptr = data_[page_id];
    std::shared_lock<std::shared_mutex> l_page(ptr->second);
    l.unlock();

    memcpy(page_data, ptr->first.data(), DISTRIBUTION_LSH_PAGE_SIZE);

    PostProcessLatency(page_id);
  }

  void ProcessLatency(page_id_t page_id) {
    uint64_t sleep_micro_sec = 1000;  // for random access, 1ms latency
    if (latency_simulator_enabled_) {
      std::unique_lock<std::mutex> lck(latency_processor_mutex_);
      for (auto &recent_page_id : recent_access_) {
        if ((recent_page_id & (~0x3)) == (page_id & (~0x3))) {
          sleep_micro_sec = 100;  // for access in the same "block", 0.1ms latency
          break;
        }
        if (page_id >= recent_page_id && page_id <= recent_page_id + 3) {
          sleep_micro_sec = 100;  // for sequential access, 0.1ms latency
          break;
        }
      }
      lck.unlock();
      std::this_thread::sleep_for(std::chrono::microseconds(sleep_micro_sec));
    }
  }

  void PostProcessLatency(page_id_t page_id) {
    if (latency_simulator_enabled_) {
      std::scoped_lock<std::mutex> lck(latency_processor_mutex_);
      recent_access_[access_ptr_] = page_id;
      access_ptr_ = (access_ptr_ + 1) % recent_access_.size();
    }
  }

  void EnableLatencySimulator(bool enabled) { latency_simulator_enabled_ = enabled; }

  auto GetLastReadThreadAndClear() -> std::optional<std::thread::id> {
    std::unique_lock<std::mutex> lck(mutex_);
    auto t = thread_id_;
    thread_id_ = std::nullopt;
    return t;
  }

 private:
  bool latency_simulator_enabled_{false};

  std::mutex latency_processor_mutex_;
  std::array<page_id_t, 4> recent_access_;
  uint64_t access_ptr_{0};

  using Page = std::array<char, DISTRIBUTION_LSH_PAGE_SIZE>;
  using ProtectedPage = std::pair<Page, std::shared_mutex>;

  std::mutex mutex_;
  std::optional<std::thread::id> thread_id_;
  std::vector<std::shared_ptr<ProtectedPage>> data_;
};

} // namespace distribution_lsh
