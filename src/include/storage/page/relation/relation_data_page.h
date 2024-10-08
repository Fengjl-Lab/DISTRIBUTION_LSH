//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/24.
// src/include/storage/page/relation/relation_directory_page.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>
#include <storage/page/data_page.h>

namespace distribution_lsh {

#define RELATION_DATA_PAGE_HEADER_SIZE (12 + COMMON_DATA_PAGE_HEADER_SIZE)
#define RELATION_TEMPLATE template<typename ValueType>
#define RELATION_TYPE RelationDataPage<ValueType>
#define RELATION_DATA_PAGE_SIZE ((DISTRIBUTION_LSH_PAGE_SIZE - RELATION_DATA_PAGE_HEADER_SIZE) / sizeof(ValueType))

RELATION_TEMPLATE
class RelationManager;
/**
 * from random line to b plus tree
 */
using RandomLineFileToBPlusTreeFileMap = struct RandomLineFileToBPlusTreeFileMap {
  file_id_t random_line_file_id_{INVALID_FILE_ID};
  page_id_t random_line_directory_page_id_{INVALID_PAGE_ID};
  int random_line_slot_{INVALID_SLOT};
  file_id_t b_plus_tree_file_id_{INVALID_FILE_ID};
  file_id_t data_set_file_id_{INVALID_FILE_ID};
};

/**
 * from training set to testing set
 */
using TrainingSetToTestingSetMap = struct TrainingSetToTestingSetMap {
  file_id_t training_set_file_id_{INVALID_FILE_ID};
  file_id_t testing_set_file_id_{INVALID_FILE_ID};
};

union RandomLineFileToBPlusTreeFileUnion {
  int next_null_slot_{NULL_SLOT_END};
  RandomLineFileToBPlusTreeFileMap map_;
};

union TrainingSetToTestingSetUnion {
  int next_null_slot_{NULL_SLOT_END};
  TrainingSetToTestingSetMap map_;
};

RELATION_TEMPLATE
class RelationDataPage : public DataPage {
  friend class RelationManager<ValueType>;
 public:
  RelationDataPage() = delete;
  RelationDataPage(RelationDataPage &&) = delete;

  void Init(int max_size = RELATION_DATA_PAGE_SIZE);

  [[nodiscard]] auto GetSize() const -> int;
  void SetSize(int size);

  [[nodiscard]] auto GetMaxSize() const -> int;
  void SetMaxSize(int max_size);

  [[nodiscard]] auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);

  [[nodiscard]] auto GetNullSlotStart() const -> int;
  void  SetNullSlotStart(int null_slot_start);

  [[nodiscard]] auto GetEndOfArray() const -> int;
  void SetEndOfArray(int end_of_array);

  void IncreaseSize(int amount);

  /**
  * Insert an entry into the page
  * @param data_page_id new data page start
  * @return successfully insert or not
  */
  auto Insert(ValueType value, int *index) -> bool;

  /**
   * Delete an entry into the page
   */
  auto Delete(int index) -> bool;

  /**
   * Get specific data in slot
   */
  auto Get(int index) const -> ValueType;

 private:
  int size_;
  int max_size_;
  page_id_t next_page_id_{INVALID_PAGE_ID};
  int null_slot_start_{0};
  int end_of_array_{0};
  ValueType array_[0];
};

} // namespace distribution_lsh
