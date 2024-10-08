//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/4/11.
// src/storage/page/random_line/random_line_header_page.cpp
//
//===-----------------------------------------------------

#include <storage/page/random_line/random_line_header_page.h>

namespace distribution_lsh {
void RandomLineHeaderPage::Init(distribution_lsh::file_id_t file_id,
                                int dimension,
                                RandomLineDistributionType distribution_type,
                                RandomLineNormalizationType normalization_type,
                                float epsilon,
                                distribution_lsh::page_id_t average_random_line_page_id,
                                distribution_lsh::page_id_t data_page_start_page_id) {
  SetFileIdentification(file_id);
  SetDimension(dimension);
  SetDistributionType(distribution_type);
  SetNormalizationType(normalization_type);
  SetEpsilon(epsilon);
  SetAverageRandomLinePageId(average_random_line_page_id);
  SetDirectoryPageStartPageId(data_page_start_page_id);
}

auto RandomLineHeaderPage::IsEmpty() const -> bool {
  return directory_page_start_page_id_ == INVALID_PAGE_ID || directory_page_start_page_id_ == HEADER_PAGE_ID;
}

auto RandomLineHeaderPage::GetDistributionType() const -> RandomLineDistributionType { return distribution_type_; }
void RandomLineHeaderPage::SetDistributionType(RandomLineDistributionType distribution_type) { distribution_type_ = distribution_type; }

auto RandomLineHeaderPage::GetNormalizationType() const -> RandomLineNormalizationType { return normalization_type_; }
void RandomLineHeaderPage::SetNormalizationType(RandomLineNormalizationType normalization_type) { normalization_type_ = normalization_type; }

auto RandomLineHeaderPage::GetDimension() const -> int { return dimension_; }
void RandomLineHeaderPage::SetDimension(int dimension) { dimension_ = dimension; }

auto RandomLineHeaderPage::GetEpsilon() const -> float { return epsilon_; }
void RandomLineHeaderPage::SetEpsilon(float epsilon) { epsilon_ = epsilon; }

auto RandomLineHeaderPage::GetAverageRandomLinePageId() const -> page_id_t { return average_random_line_page_id_; }
void RandomLineHeaderPage::SetAverageRandomLinePageId(page_id_t average_random_line_page_id) { average_random_line_page_id_ = average_random_line_page_id; }

auto RandomLineHeaderPage::GetDirectoryPageStartPageId() const -> page_id_t { return directory_page_start_page_id_; }
void RandomLineHeaderPage::SetDirectoryPageStartPageId(page_id_t directory_page_start_page_id) { directory_page_start_page_id_ = directory_page_start_page_id; }
}// namespace distribution_lsh