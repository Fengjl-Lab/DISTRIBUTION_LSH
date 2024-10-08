//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2024/1/20.
// src/include/storage/page/random_line_header_page.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>
#include <storage/page/header_page.h>

namespace distribution_lsh {

enum class RandomLineDistributionType : std::uint8_t {
  INVALID_DISTRIBUTION_TYPE = 0,
  CAUCHY = static_cast<std::uint8_t>(1 << 1),
  GAUSSIAN = static_cast<std::uint8_t>(1 << 2)
};

enum class RandomLineNormalizationType : std::uint8_t {
  INVALID_NORMALIZATION_TYPE = 0,
  NONE = static_cast<std::uint8_t>(1 << 1),
  L1_NORM = static_cast<uint8_t>(1 << 2),
  L2_NORM = static_cast<std::uint8_t>(1 << 3)
};


inline auto RandomLineDistributionTypeToString(RandomLineDistributionType distribution_type) -> std::string {
  switch (distribution_type) {
    case RandomLineDistributionType::INVALID_DISTRIBUTION_TYPE: return "INVALID DISTRIBUTION TYPE";
    case RandomLineDistributionType::CAUCHY: return "CAUCHY DISTRIBUTION TYPE";
    case RandomLineDistributionType::GAUSSIAN: return "GAUSSIAN DISTRIBUTION TYPE";
    default: return "UNSUPPORTED DISTRIBUTION TYPE";
  }
}

inline auto RandomLineNormalizationTypeToString(RandomLineNormalizationType normalization_type) -> std::string {
  switch (normalization_type) {
    case RandomLineNormalizationType::INVALID_NORMALIZATION_TYPE: return "INVALID NORMALIZATION TYPE";
    case RandomLineNormalizationType::NONE: return "NONE NORMALIZATION TYPE";
    case RandomLineNormalizationType::L1_NORM: return "L1_NORM NORMALIZATION TYPE";
    case RandomLineNormalizationType::L2_NORM: return "L2_NORM NORMALIZATION TYPE";
    default: return "UNSUPPORTED NORMALIZATION TYPE";
  }
}


/**
* @breif page that contain the start page of each random line
*/
class RandomLineHeaderPage : HeaderPage {
  template<typename RandomLineValueType>
  friend class RandomLineManager;
 public:
  RandomLineHeaderPage() = delete;
  RandomLineHeaderPage(const RandomLineHeaderPage &other) = delete;

  void Init(file_id_t file_id, int dimension, RandomLineDistributionType distribution_type, RandomLineNormalizationType normalization_type, float epsilon, page_id_t average_random_line_page_id, page_id_t data_page_start_id);

  [[nodiscard]] auto IsEmpty() const -> bool;

  [[nodiscard]] auto GetDistributionType() const -> RandomLineDistributionType;
  void SetDistributionType(RandomLineDistributionType distribution_type);

  [[nodiscard]] auto GetNormalizationType() const -> RandomLineNormalizationType;
  void SetNormalizationType(RandomLineNormalizationType normalization_type);

  [[nodiscard]] auto GetDimension() const -> int;
  void SetDimension(int dimension);

  [[nodiscard]] auto GetEpsilon() const -> float;
  void SetEpsilon(float epsilon);

  [[nodiscard]] auto GetAverageRandomLinePageId() const -> page_id_t;
  void SetAverageRandomLinePageId(page_id_t average_random_line_page_id);

  [[nodiscard]] auto GetDirectoryPageStartPageId() const -> page_id_t;
  void SetDirectoryPageStartPageId(page_id_t directory_page_start_page_id);

 private:
  int dimension_{0};
  RandomLineDistributionType distribution_type_{RandomLineDistributionType::INVALID_DISTRIBUTION_TYPE};
  RandomLineNormalizationType normalization_type_{RandomLineNormalizationType::INVALID_NORMALIZATION_TYPE};
  float epsilon_{EPSILON};
  page_id_t average_random_line_page_id_{INVALID_PAGE_ID};
  page_id_t directory_page_start_page_id_{INVALID_PAGE_ID};
};

}  // namespace distribution_lsh

