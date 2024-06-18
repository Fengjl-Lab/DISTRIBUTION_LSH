//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/4/11.
// src/storage/page/random_line/random_line_page.cpp
//
//===-----------------------------------------------------


#include <storage/page/random_line/random_line_page.h>

namespace distribution_lsh {

auto RandomLinePage::GetPageType() const -> RandomLinePageType { return type_; }

void RandomLinePage::SetPageType(distribution_lsh::RandomLinePageType type) { type_ = type; }

auto RandomLinePage::GetSize() const -> int { return size_; }

void RandomLinePage::SetSize(int size) { size_ = size; }

auto RandomLinePage::GetMaxSize() const -> int { return max_size_; }

void RandomLinePage::SetMaxSize(int max_size) { max_size_ = max_size; }

auto RandomLinePage::GetNextPageId() const -> page_id_t { return next_page_id_; }

void RandomLinePage::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

void RandomLinePage::IncreaseSize(int amount) { size_ += amount; }
} // namespace distribution_lsh