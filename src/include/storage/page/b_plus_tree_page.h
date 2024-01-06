//===----------------------------------------------------
//                          QALSH
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

namespace qalsh {

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>

// define page type enum
enum class IndexPageType { INVALID_ID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE};


class BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreePage() = delete;
  BPlusTreePage(const BPlusTreePage &other) = delete;
  ~BPlusTreePage() = delete;

  auto IsLeafPage() const -> bool;
  void SetPageType(IndexPageType page_type);

  auto GetSize() const -> int;
  void SetSize(int size);
  void IncreaseSize(int amount);

  auto GetMaxSize() const -> int;
  void SetMaxSize(int max_size);
  auto GetMinSize() const -> int;

 private:
  // Member variables, attributes that both internal and leaf page share
  IndexPageType page_type_ __attribute__((__unused__));
  int size_ __attribute__((__unused__));
  int max_size_ __attribute__((__unused__));
};

}// namespace qalsh


