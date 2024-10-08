//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2023/11/17.
// src/include/storage/page/b_plus_tree_internal_page.h
//
//===-----------------------------------------------------

#pragma once

#include <queue>
#include <string>
#include <type_traits>

#include <storage/page/b_plus_tree/b_plus_tree_page.h>
#include <common/rid.h>

namespace distribution_lsh
{

INDEX_TEMPLATE_ARGUMENTS
class BPlusTree;

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<BPlusTreeKeyType, BPlusTreeValueType>
#define INTERNAL_PAGE_HEADER_SIZE (12 + COMMON_DATA_PAGE_HEADER_SIZE)
#define INTERNAL_PAGE_SIZE ((DISTRIBUTION_LSH_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)) - 1)

template <typename BPlusTreeKeyType, typename BPlusTreeValueType>
auto constexpr GetInternalPageSize() {
  if constexpr ((std::is_integral_v<BPlusTreeKeyType> || std::is_floating_point_v<BPlusTreeKeyType>)
  && has_rid_feature<BPlusTreeValueType>::value) {
    return (DISTRIBUTION_LSH_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / sizeof (MappingType) - 1;
  } else {
    static_assert(false, "B plus tree template is not valid");
  }
}

/**
 * Store `n` indexed keys and `n + 1` child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: Since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search / lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 * ----------------------------------------------------------------------------------
 * | HEADER | KEY(1) + PAGE_ID(1) | KEY(2) + PAGE_ID(2) | ... | KEY(n) + PAGE_ID(n) |
 * ----------------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeInternalPage() = delete;
  BPlusTreeInternalPage(const BPlusTreeInternalPage &other) = delete;

  /**
   * Writes the necessary header information to a newly created page, must be called after
   * the creation of a new page to make a valid `BPlusTreeInternalPage`
   * @param max_size Maximal size of the page
   */
  void Init(int max_size = GetInternalPageSize<BPlusTreeKeyType, BPlusTreeValueType>());

  /**
   * @param index The index of the key to get. Index must be non-zero.
   * @return Key at index
   */
  auto KeyAt(int index) const -> BPlusTreeKeyType;

  /**
   * @param index The index of the key to set. Index must be non-zero.
   * @param key The new value for key
   */
  void SetKeyAt(int index, const BPlusTreeKeyType &key);

  /**
   * Binary Search
   * @param value The value to search for
   * @return The index that corresponds to the specified value
   */
  auto ValueIndex(const BPlusTreeValueType &value) const -> int;

  /**
   * @param index The index to search for
   * @return The value at the index
   */
  auto ValueAt(int index) const -> BPlusTreeValueType;

  /**
   * @brief For test only, return a string representing all keys in
   * this internal page, formatted as "(key1,key2,key3,...)"
   *
   * @return The string representation of all keys in the current internal page
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    // First key of internal page is always invalid
    for (int i = 1; i < GetSize(); i++) {
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
  // Flexible array member for page data.
  MappingType array_[0];
  friend class BPlusTree<BPlusTreeKeyType, RID>;
};


} // namespace distribution_lsh


