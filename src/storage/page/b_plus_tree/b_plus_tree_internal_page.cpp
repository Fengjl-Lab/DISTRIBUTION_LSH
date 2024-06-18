//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2034/11/25.
// src/storage/page/b_plus_tree_internal_page.cpp
//
//===-----------------------------------------------------

#include "include/storage/page/b_plus_tree/b_plus_tree_internal_page.h"

namespace distribution_lsh {

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  if (index <= 0 || index > GetSize()) {
    return -1;
  }

  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index <= 0 || index > GetMaxSize()) {
    return;
  }

  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); ++i) {
    if (array_[i].second == value) {
      return i;
    }
  }

  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index < 0 || index > GetSize()) {
    return -1;
  }

  return array_[index].second;
}

template class BPlusTreeInternalPage<float, page_id_t >;
} // namespace distribution_lsh