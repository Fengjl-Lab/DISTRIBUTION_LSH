//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2023/11/25.
// src/include/storage/page/b_plus_tree_leaf_page.h
//
//===-----------------------------------------------------

#pragma once

#include <string>
#include <utility>
#include <vector>

#include <common/config.h>
#include <storage/page/b_plus_tree/b_plus_tree_page.h>

namespace distribution_lsh {

INDEX_TEMPLATE_ARGUMENTS
class BPlusTree;

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<BPlusTreeKeyType, BPlusTreeValueType>
#define LEAF_PAGE_HEADER_SIZE (16 + COMMON_DATA_PAGE_HEADER_SIZE)
#define LEAF_PAGE_SIZE ((DISTRIBUTION_LSH_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

template <typename BPlusTreeKeyType, typename BPlusTreeValueType>
auto constexpr GetLeafPageSize() {
  if constexpr ((std::is_integral_v<BPlusTreeKeyType> || std::is_floating_point_v<BPlusTreeKeyType>)
      && has_rid_feature<BPlusTreeValueType>::value) {
    return (DISTRIBUTION_LSH_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof (MappingType) - 1;
  } else {
    static_assert(false, "B plus tree template is not valid");
  }
}

/**
 * Store indexed key and record id ( record id = page id combined with slot id )
 * together within leaf page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 * -----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)  |
 * -----------------------------------------------------------------------
 *
 * Header format (size in byte, 16 bytes in total):
 * -----------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) | NextPageId (4) | ... |
 * -----------------------------------------------------------------------
 */

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
  void Init(int max_size = GetLeafPageSize<BPlusTreeKeyType, BPlusTreeValueType>());

  // Helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> BPlusTreeKeyType;

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
      BPlusTreeKeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[0];
  friend class BPlusTree<BPlusTreeKeyType, BPlusTreeValueType>;
};
}// namespace distribution_lsh

