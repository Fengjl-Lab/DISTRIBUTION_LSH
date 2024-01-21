//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/2.
// src/include/storage/index/b_plus_tree.h
//
//===-----------------------------------------------------

#pragma once

#include <algorithm>
#include <deque>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>

#include <common/config.h>
#include <common/macro.h>
#include <common/rid.h>
#include <buffer/buffer_pool_manager.h>
#include <storage/page/b_plus_tree_page.h>
#include <storage/page/b_plus_tree_leaf_page.h>
#include <storage/page/b_plus_tree_internal_page.h>
#include <storage/page/b_plus_tree_header_page.h>

namespace distribution_lsh {

/**
 * @brief Definition of the Context class.
 *
 * This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
*/
class Context {
 public:
  /**
   *  When you insert into / remove from the B+ tree, store the write guard of header page here.
   *  Remember to drop the header page guard and set it to nullopt when you want to unlock all.
   */
   // header page guard, when you need to edit the tree, this element should be existed.
   std::optional<WritePageGuard> header_page_{std::nullopt};

   // root page id
   page_id_t root_page_id_{INVALID_PAGE_ID};

   // read latch set for the operation
   std::deque<ReadPageGuard> read_set_;

   // write latch set for the operation
   std::deque<WritePageGuard> write_set_;

   auto IsRootPage(page_id_t page_id) -> bool { return root_page_id_ == page_id; }
};

#define B_PLUS_TREE_TYPE BPlusTree<KeyType, ValueType>
/**
 * @breif the exact operation data structure for B+ tree
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType>;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     int leaf_max_size = LEAF_PAGE_SIZE,
                     int internal_max_size = INTERNAL_PAGE_SIZE);

  // Judge if the B+ tree is empty
  auto IsEmpty() -> bool;

  // Get root page id
  auto GetRootPageId() -> page_id_t;

  // Get value from certain input key
  auto Get(KeyType &key, std::vector<ValueType> *result) -> bool;

  // Insert value to the B+ tree
  auto Insert(KeyType &key, ValueType &value) -> bool;

  // Delete value to the B+ tree
  auto Delete(KeyType &key) -> bool;

  // Update value to the B+ tree
  auto UpDate(KeyType &key, ValueType &value) -> bool;

  // Range read for c-ANN
  auto RangeRead(KeyType &lkey, KeyType &rkey, std::vector<ValueType> *result) -> bool;

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

 private:
  // member variable
  std::string index_name_;
  BufferPoolManager *bpm_;
  std::vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;
  constexpr static float TEMP_SLOT_VACANCY = -1;
};
}