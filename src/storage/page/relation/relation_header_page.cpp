//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/24.
// src/storage/page/relation/relation_page.cpp
//
//===-----------------------------------------------------

#include <storage/page/relation/relation_header_page.h>

namespace distribution_lsh {

auto RelationHeaderPage::GetRelationFileType() const -> RelationFileType { return type_; }
auto RelationHeaderPage::IsEmpty() const -> bool {
  return data_page_start_id_ == HEADER_PAGE_ID || data_page_start_id_ == INVALID_PAGE_ID;
}

void RelationHeaderPage::SetRelationFileType(distribution_lsh::RelationFileType type) { type_ = type; }
} // namespace distribution_lsh