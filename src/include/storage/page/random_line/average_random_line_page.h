//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/4/11.
// src/include/storage/page/random_line/random_line_average_page.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>
#include <storage/page/random_line/random_line_page.h>

namespace distribution_lsh {

#define AVERAGE_RANDOM_LINE_TYPE AverageRandomLinePage<RandomLineValueType>
#define AVERAGE_RANDOM_LINE_PAGE_HEADER_SIZE RANDOM_LINE_PAGE_HEADER_SIZE
#define AVERAGE_RANDOM_LINE_PAGE_SIZE ((DISTRIBUTION_LSH_PAGE_SIZE - AVERAGE_RANDOM_LINE_PAGE_HEADER_SIZE)/ sizeof(RandomLineValueType))

template <typename RandomLineValueType>
auto constexpr GetAverageRandomLinePageSize() {
  if constexpr ((std::is_integral_v<RandomLineValueType> || std::is_floating_point_v<RandomLineValueType>)) {
    return (DISTRIBUTION_LSH_PAGE_SIZE - AVERAGE_RANDOM_LINE_PAGE_HEADER_SIZE) / sizeof(RandomLineValueType);
  } else {
    static_assert(false, "Average random line page template is not valid");
  }
}

RANDOM_LINE_TEMPLATE
class RandomLineManager;

RANDOM_LINE_TEMPLATE
class AverageRandomLinePage : public RandomLinePage {
  friend class RandomLineManager<RandomLineValueType>;
 public:
  AverageRandomLinePage() = delete;
  AverageRandomLinePage(const AverageRandomLinePage &other) = delete;

  void Init(int max_size = GetAverageRandomLinePageSize<RandomLineValueType>());

  auto ToString() -> std::string override;

 private:
  RandomLineValueType array_[0];
};
} // namespace distribution_lsh
