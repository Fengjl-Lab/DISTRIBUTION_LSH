//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/16.
// src/include/file/monitor.h
//
//===-----------------------------------------------------

#pragma once

#include <map>
#include <filesystem>
#include <random>
#include <mutex>

#include <common/config.h>
#include <common/logger.h>
#include <common/util/file.h>
#include <buffer/buffer_pool_manager.h>
#include <storage/page/header_page.h>

namespace distribution_lsh {

/**
 * @brief file monitor which responsible for allocate and scan file name in certain directory
 */
class Monitor {
 public:
  explicit Monitor() = default;

  /**
   * @brief list target files in target directory and print their information
   */
  virtual void List() = 0;

 protected:

  /**
   * @brief generate a random file identification
   * It need to detect if there has existing file with the same file identification
   * if collision happens, it will generate a new file identification
   * @return buffer pool manager of new file
   * @attention one file can only correspond to one buffer pool manager
   */
  auto GenerateFileIdentification(const std::string &directory_name, FileType type) -> file_id_t ;

  /**
   * Get next page id to initiate buffer pool manager
   * @param manager disk manager to get next page
   * @param file_name target file path
   * @return
   */
  static auto GetNextPageId(DiskManager *manager, const std::string &file_name) -> page_id_t {
    return manager->GetFileSize(file_name) %  DISTRIBUTION_LSH_PAGE_SIZE == 0 ?  manager->GetFileSize(file_name) /  DISTRIBUTION_LSH_PAGE_SIZE : manager->GetFileSize(file_name) / DISTRIBUTION_LSH_PAGE_SIZE + 1;
  }
};
} // namespace distribution_lsh
