//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/18.
// src/storage/page/dataset/distribution_dataset_page.cpp
//
//===-----------------------------------------------------

#include <storage/page/dataset/distribution_dataset_page.h>

namespace distribution_lsh {

auto DistributionDataSetPage::IsDirectoryPage() const -> bool { return page_type_ == DistributionDatatSetPageType::DIRECTORY_PAGE; }
void DistributionDataSetPage::SetPageType(DistributionDatatSetPageType page_type) { page_type_ = page_type; }

auto DistributionDataSetPage::GetSize() const -> int { return size_; }
void DistributionDataSetPage::SetSize(int size) { size_ = size; }

auto DistributionDataSetPage::GetMaxSize() const -> int { return max_size_; }
void DistributionDataSetPage::SetMaxSize(int max_size) { max_size_ = max_size; }

auto DistributionDataSetPage::GetNextPageId() const -> page_id_t { return next_page_id_; }
void DistributionDataSetPage::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

void DistributionDataSetPage::IncreaseSize(int amount) { size_ += amount; }

} // namespace distribution_lsh