//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/18.
// src/storage/page/dataset/distribution_datatset_header_page.cpp
//
//===-----------------------------------------------------

#include <storage/page/dataset/distribution_dataset_header_page.h>

namespace distribution_lsh {

auto DistributionDataSetHeaderPage::GetDataSetType() const -> DataSetType { return data_set_type_; }
void DistributionDataSetHeaderPage::SetDataSetType(DataSetType data_set_type) { data_set_type_ = data_set_type; }

auto DistributionDataSetHeaderPage::GetNormalizationType() const -> NormalizationType { return normalization_type_; }
void DistributionDataSetHeaderPage::SetNormalizationType(NormalizationType normalization_type) { normalization_type_ = normalization_type; }

auto DistributionDataSetHeaderPage::GetDimension() const -> int { return dimension_; }
void DistributionDataSetHeaderPage::SetDimension(int dimension) { dimension_ = dimension; }

auto DistributionDataSetHeaderPage::IsEmpty() const -> bool { return directory_start_page_id_ == INVALID_PAGE_ID || directory_start_page_id_ == HEADER_PAGE_ID; }
void DistributionDataSetHeaderPage::SetDirectoryId(page_id_t directory_start_page_id) { directory_start_page_id_ = directory_start_page_id; }
} // namespace distribution_lsh