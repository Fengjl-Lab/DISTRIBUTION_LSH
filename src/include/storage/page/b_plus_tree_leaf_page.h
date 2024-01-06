//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2023/11/25.
// src/include/storage/page/b_plus_tree_leaf_page.h
//
//===-----------------------------------------------------

#pragma once

#include <string>
#include <utility>
#include <vector>

#include <common/config.h>
#include <storage/page/b_plus_tree_page.h>

namespace qalsh {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KetComparator>
#define LEAF_PAGE_HEADER_SIZE 16
#define LEAF_PAGE_SIZE ((QALSH_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  /**
   * After creating a new leaf page from buffer pool, must call initialize
   * method to set default values
   * @param max_size Max size of the leaf node
   */
  void Init(int max_size = LEAF_PAGE_SIZE);

  // Helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;

  /**
   * @brief For test only return a string representing all keys in
   * this leaf page formatted as "(key1,key2,key3,...)"
   *
   * @return The string representation of all keys in the current internal page
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    for (int i = 0; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[0];
};
}

