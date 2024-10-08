//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/4/10.
// src/include/storage/page/random_line/random_line_directory_page.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>
#include <common/exception.h>
#include <storage/page/random_line/random_line_page.h>

namespace distribution_lsh {

#define RANDOM_LINE_DIRECTORY_PAGE_HEADER_SIZE (8 + RANDOM_LINE_PAGE_HEADER_SIZE)
#define RANDOM_LINE_DIRECTORY_PAGE_SIZE ((DISTRIBUTION_LSH_PAGE_SIZE - RANDOM_LINE_DIRECTORY_PAGE_HEADER_SIZE)/ sizeof(page_id_t))

auto constexpr GetRandomLineDirectoryPageSize() {
  return static_cast<int>((DISTRIBUTION_LSH_PAGE_SIZE - RANDOM_LINE_DIRECTORY_PAGE_HEADER_SIZE) / sizeof(page_id_t));
}

class RandomLineDirectoryPage : public RandomLinePage {
  template<class RandomLineValueType>
  friend class RandomLineManager;
 public:
  RandomLineDirectoryPage() = delete;
  RandomLineDirectoryPage(const RandomLineDirectoryPage &other) = delete;

  /**
  * Init the directory page
  * @param max_size data the page can max hold
  */
  void Init(int max_size = GetRandomLineDirectoryPageSize());

  /**
   * Insert an entry into the page
   * @param data_page_id new data page start
   * @return successfully insert or not
   */
  auto Insert(page_id_t data_page_id, int *index) -> bool;

  /**
   * Delete an entry into the page
   */
  auto Delete(int index) -> bool;

  /**
   * Print the information of the page
   */
  auto ToString() -> std::string override;

  /**
   * Value of input index
   */
  auto IndexAt(int index) const -> page_id_t;

  auto GetNullSlotStart() const -> int;
  void  SetNullSlotStart(int null_slot_start);

  auto GetEndOfArray() const -> int;
  void SetEndOfArray(int end_of_array);

 private:
  int null_slot_start_{0};
  int end_of_array_{0};
  page_id_t array_[0];
};


} // namespace distribution_lsh
