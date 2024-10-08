//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/2.
// src/include/storage/disk/disk_manager.h
//
//===-----------------------------------------------------

#pragma once

#include <atomic>
#include <fstream>
#include <future>
#include <mutex>
#include <string>

#include <common/config.h>

namespace distribution_lsh {
/**
 * DiskManager takes care of the allocation and de-allocation of pages.
 */
class DiskManager {
  friend class Monitor;
 public:
  /**
   * Creates a new disk manager that writes to the specified data file.
   * @param data_file the file name of data file
   */
  explicit DiskManager(const std::string &db_file);

  DiskManager() = default;

  virtual ~DiskManager() = default;

  /**
   * Shut down the disk manager and close the file sources.
   */
  void ShutDown();

  /**
   * Write a page to the data file
   * @param page_id id of the page
   * @param page_data raw page data
   */
  virtual void WritePage(page_id_t page_id, const char *page_data);

  /**
   * Read a page from the data file
   * @param page_id id of the page
   * @param[out] page data output buffer
   */
  virtual void ReadPage(page_id_t page_id, char *page_data);

  /**
   * Flush the entire log buffer into disk.
   * @param log_data raw log data
   * @param size size of log entry
   */
  void WriteLog(char *log_data, int size);

  /**
   *  Read a log entry from the log file.
   * @param[out] log_data output buffer
   * @param size size of the log entry
   * @param offset offset of the log entry in the file
   * @return true if the read was successful, false otherwise
   */
  auto ReadLog(char *log_data, int size, int offset) -> bool;

  /** @return the number of disk flushes */
  auto GetNumFlushes() const -> int;

  /** @return true iff the in-memory content has not been flushed yet */
  auto GetFlushState() const -> bool;

  /** @return the number of disk writes */
  auto GetNumWrites() const -> int;

  /**
   * Sets the future which is used to check for non-blocking flushes.
   * @param f the non-blocking flush check
   */
  inline void SetFlushLogFuture(std::future<void> *f) { flush_log_f_ = f; }

  /** Checks if the non-blocking flush future was set. */
  inline auto HasFlushLogFuture() -> bool { return flush_log_f_ != nullptr; }

 protected:
  auto GetFileSize(const std::string &file_name) -> int;
  // stream to write log file
  std::fstream log_io_;
  std::string log_name_;
  // stream to write data file;
  std::fstream data_io_;
  std::string file_name_;
  int num_flushes_{0};
  int num_writes_{0};
  bool flush_log_{false};
  std::future<void> *flush_log_f_{nullptr};
  // With multiple buffer pool instances, need to protect file access
  std::mutex data_io_latch_;
};
} // namespace distribution_lsh
