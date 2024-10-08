//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2034/11/24.
// src/storage/page/b_plus_tree_leaf_page.cpp
//
//===-----------------------------------------------------

#include <sstream>

#include "include/common/exception.h"
#include "include/common/rid.h"
#include "include/storage/page/b_plus_tree/b_plus_tree_leaf_page.h"

namespace distribution_lsh {
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Set/Get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(distribution_lsh::page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * find and return the key associated with input "index"
 * (a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> BPlusTreeKeyType {
  if (index >= GetSize()) {
    return -1;
  }

  return array_[index].first;
}

template class BPlusTreeLeafPage<float, RID>;
template class BPlusTreeLeafPage<double, RID>;
} // namespace distribution_lsh