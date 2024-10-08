//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2034/11/25.
// src/storage/page/b_plus_tree_page.cpp
//
//===-----------------------------------------------------

#include "include/storage/page/b_plus_tree/b_plus_tree_page.h"

namespace distribution_lsh {
/**
 * Set page type
 */
auto BPlusTreePage::IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }
void BPlusTreePage::SetPageType(distribution_lsh::IndexPageType page_type) { page_type_ = page_type; }

/**
 * Get information from the class
 */
auto BPlusTreePage::GetSize() const -> int { return size_; }
void BPlusTreePage::SetSize(int size) { size_ = size; }
void BPlusTreePage::IncreaseSize(int amount) { size_ += amount; }

/**
 * Set/Get max size
 */
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }
void BPlusTreePage::SetMaxSize(int max_size) { max_size_ = max_size; }

/**
 * Get min size
 */
auto BPlusTreePage::GetMinSize() const -> int { return max_size_/2; }

} // namespace distribution_lsh