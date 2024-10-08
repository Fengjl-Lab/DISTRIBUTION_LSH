//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2023/11/17.
// src/include/storage/page/b_plus_tree_page.h
//
//===-----------------------------------------------------

#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include <buffer/buffer_pool_manager.h>
#include <storage/page/data_page.h>

namespace distribution_lsh {

#define MappingType std::pair<BPlusTreeKeyType, BPlusTreeValueType>

#define INDEX_TEMPLATE_ARGUMENTS template <typename BPlusTreeKeyType, typename BPlusTreeValueType>

// define page type enum
enum class IndexPageType { INVALID_ID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE};


class BPlusTreePage : public DataPage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreePage() = delete;
  BPlusTreePage(const BPlusTreePage &other) = delete;
  ~BPlusTreePage() = delete;

  [[nodiscard]] auto IsLeafPage() const -> bool;
  void SetPageType(IndexPageType page_type);

  [[nodiscard]] auto GetSize() const -> int;
  void SetSize(int size);
  void IncreaseSize(int amount);

  [[nodiscard]] auto GetMaxSize() const -> int;
  void SetMaxSize(int max_size);
  [[nodiscard]] auto GetMinSize() const -> int;

 private:
  // Member variables, attributes that both internal and leaf page share
  IndexPageType page_type_;
  int size_;
  int max_size_;
};

}// namespace distribution_lsh


