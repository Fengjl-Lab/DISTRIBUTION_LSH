//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/2.
// src/storage/disk/disk_manager.cpp
//
//===-----------------------------------------------------

#include <sys/stat.h>
#include <cassert>
#include <cstring>
#include <iostream>
#include <mutex>  // NOLINT
#include <string>
#include <thread>  // NOLINT

#include <common/exception.h>
#include <common/logger.h>
#include <storage/disk/disk_manager.h>

namespace distribution_lsh {
static char *buffer_used;

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::string::size_type n = file_name_.rfind('.');
  if (n == std::string::npos) {
    LOG_DEBUG("Wrong file format");
    return;
  }
#ifdef LOG_FILE_NEEDED
  log_name_ = file_name_.substr(0, n) + ".log";
  
  log_io_.open(log_name_, std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
  // directory or file does not exist
  if (!log_io_.is_open()) {
    log_io_.clear();
    // create a new file
    log_io_.open(log_name_, std::ios::binary | std::ios::trunc | std::ios::out | std::ios::in);
    if (!log_io_.is_open()) {
      throw Exception("can't open dblog file");
    }
  }
#endif

  std::scoped_lock scoped_data_io_latch(data_io_latch_);
  data_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!data_io_.is_open()) {
    data_io_.clear();
    // create a new file
    data_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out | std::ios::in);
    if (!data_io_.is_open()) {
      throw Exception("can't open db file");
    }
  }
  buffer_used = nullptr;
}

void DiskManager::ShutDown() {
  {
    std::scoped_lock scoped_data_io_latch(data_io_latch_);
    data_io_.close();
  }
  log_io_.close();
}

void DiskManager::WritePage(distribution_lsh::page_id_t page_id, const char *page_data) {
  std::scoped_lock scoped_data_io_latch(data_io_latch_);
  size_t offset = static_cast<size_t>(page_id) * DISTRIBUTION_LSH_PAGE_SIZE;
  // set write cursor to offset
  num_writes_ += 1;
  data_io_.seekp(offset);
  data_io_.write(page_data, DISTRIBUTION_LSH_PAGE_SIZE);
  // check for I/O error
  if (data_io_.bad()) {
    LOG_DEBUG("I/O error while writting");
  }
  // needs to flush to keep disk file in sync
  data_io_.flush();
}

void DiskManager::ReadPage(page_id_t page_id, char *page_data) {
  std::scoped_lock scoped_data_io_latch(data_io_latch_);
  int offset = page_id * DISTRIBUTION_LSH_PAGE_SIZE;
  // check if read beyond file length
  if (offset > GetFileSize(file_name_)) {
    LOG_DEBUG("I/O error reading past end of file");
  } else {
    // set read cursor to offset
    data_io_.seekp(offset);
    data_io_.read(page_data, DISTRIBUTION_LSH_PAGE_SIZE);
    if (data_io_.bad()) {
      LOG_DEBUG("I/O error while reading");
      return;
    }
    // if file ends before reading BUSTUB_PAGE_SIZE
    int read_count = data_io_.gcount();
    if (read_count < DISTRIBUTION_LSH_PAGE_SIZE) {
      LOG_DEBUG("Read less than a page");
      data_io_.clear();
      memset(page_data + read_count, 0, DISTRIBUTION_LSH_PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WriteLog(char *log_data, int size) {
  // enforce swap log buffer
  assert(log_data != buffer_used);
  buffer_used = log_data;

  if (size == 0) {  // no effect on num_flushes_ if log buffer is empty
    return;
  }

  flush_log_ = true;

  if (flush_log_f_ != nullptr) {
    // used for checking non-blocking flushing
    assert(flush_log_f_->wait_for(std::chrono::seconds(10)) == std::future_status::ready);
  }

  num_flushes_ += 1;
  // sequence write
  log_io_.write(log_data, size);

  // check for I/O error
  if (log_io_.bad()) {
    LOG_DEBUG("I/O error while writing log");
    return;
  }
  // needs to flush to keep disk file in sync
  log_io_.flush();
  flush_log_ = false;
}


auto DiskManager::ReadLog(char *log_data, int size, int offset) -> bool {
  if (offset >= GetFileSize(log_name_)) {
    // LOG_DEBUG("end of log file");
    // LOG_DEBUG("file size is %d", GetFileSize(log_name_));
    return false;
  }
  log_io_.seekp(offset);
  log_io_.read(log_data, size);

  if (log_io_.bad()) {
    LOG_DEBUG("I/O error while reading log");
    return false;
  }
  // if log file ends before reading "size"
  int read_count = log_io_.gcount();
  if (read_count < size) {
    log_io_.clear();
    memset(log_data + read_count, 0, size - read_count);
  }

  return true;
}

/**
 * Returns number of flushes made so far
 */
auto DiskManager::GetNumFlushes() const -> int { return num_flushes_; }

/**
 * Returns number of Writes made so far
 */
auto DiskManager::GetNumWrites() const -> int { return num_writes_; }

/**
 * Returns true if the log is currently being flushed
 */
auto DiskManager::GetFlushState() const -> bool { return flush_log_; }

/**
 * Private helper function to get disk file size
 */
auto DiskManager::GetFileSize(const std::string &file_name) -> int {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? static_cast<int>(stat_buf.st_size) : -1;
}

}// namespace distribution_lsh