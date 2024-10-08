//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/24.
// src/include/storage/index/relation_manager.h
//
//===-----------------------------------------------------

#pragma once

#include <mutex>
#include <optional>

#include <common/config.h>
#include <common/logger.h>
#include <common/exception.h>
#include <buffer/buffer_pool_manager.h>
#include <storage/page/relation/relation_header_page.h>
#include <storage/page/relation/relation_data_page.h>

namespace distribution_lsh {

#define RELATION_MANAGER_TYPE RelationManager<ValueType>

struct RelationContext {
  std::optional<WritePageGuard> header_page_{std::nullopt};
  std::deque<ReadPageGuard> read_set_;
  std::deque<WritePageGuard> write_set_;
};

RELATION_TEMPLATE
class RelationManager {
  using DataPage = RelationDataPage<ValueType>;
 public:
  RelationManager() = delete;
  RelationManager(std::string manager_name, std::string directory_name, std::shared_ptr<BufferPoolManager> bpm, RelationFileType type, file_id_t file_id = INVALID_FILE_ID, page_id_t header_page_id = INVALID_PAGE_ID, int data_max_size = RELATION_DATA_PAGE_SIZE);

  /**
   *
   * @return relation file is empty or not
   */
  auto IsEmpty(RelationContext *ctx = nullptr, bool is_read = true) -> bool;

  /**
   *
   * @param value
   * @param index
   * @return
   */
  auto Insert(ValueType value, int *index) -> bool;

  /**
   *
   * @param data_page_id
   * @param slot
   * @return
   */
  auto Delete(page_id_t data_page_id, int slot) -> bool;

  /**
   *
   * @param data_page_id
   * @param slot
   * @return
   */
  auto Get(page_id_t data_page_id, int slot) -> ValueType;

  /**
   * @param index
   * @param data_page_id
   * @param slot
   * @return
   */
  auto Get(int index, page_id_t *data_page_id, int *slot) -> ValueType;

  [[nodiscard]] auto GetType() const -> RelationFileType { return type_; }

 private:
  std::string manager_name_;
  std::string directory_name_;
  std::shared_ptr<BufferPoolManager> bpm_;
  RelationFileType type_;
  file_id_t file_id_;
  page_id_t header_page_id_;
  int data_page_max_size_;
  std::mutex lock_;
};
} // namespace distribution_lsh
