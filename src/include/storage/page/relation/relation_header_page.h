//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/24.
// src/include/storage/page/relation/relation_header_page.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>
#include <storage/page/header_page.h>

namespace distribution_lsh {

enum class RelationFileType : std::uint8_t {INVALID_RELATION_PAGE_TYPE = 0, RANDOM_LINE_TO_B_PLUS_TREE, TRAINING_SET_TO_TESTING_SET};

/**
 * @brief class that stor the relationship between different file
 */
class RelationHeaderPage : public HeaderPage {
  template<typename ValueType>
  friend class RelationManager;
 public:
  RelationHeaderPage() = delete;
  RelationHeaderPage(RelationHeaderPage &&) = delete;

  /**
   * Getter and Setter methods
   */
   auto GetRelationFileType() const -> RelationFileType;
   auto IsEmpty() const -> bool;

   void SetRelationFileType(RelationFileType type);

 private:
  RelationFileType type_;
  page_id_t data_page_start_id_;
};

} // namespace distribution_lsh
