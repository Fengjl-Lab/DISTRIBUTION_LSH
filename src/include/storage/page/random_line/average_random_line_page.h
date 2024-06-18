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

#define AVERAGE_RANDOM_LINE_TYPE AverageRandomLinePage<ValueType>
#define AVERAGE_RANDOM_LINE_PAGE_HEADER_SIZE RANDOM_LINE_PAGE_HEADER_SIZE
#define AVERAGE_RANDOM_LINE_PAGE_SIZE ((DISTRIBUTION_LSH_PAGE_SIZE - AVERAGE_RANDOM_LINE_PAGE_HEADER_SIZE)/ sizeof(ValueType))

RANDOM_LINE_TEMPLATE
class RandomLineManager;

RANDOM_LINE_TEMPLATE
class AverageRandomLinePage : public RandomLinePage {
  friend class RandomLineManager<ValueType>;
 public:
  AverageRandomLinePage() = delete;
  AverageRandomLinePage(const AverageRandomLinePage &other) = delete;

  void Init(int max_size = AVERAGE_RANDOM_LINE_PAGE_SIZE);

  auto ToString() -> std::string override;

 private:
  ValueType array_[0];
};
} // namespace distribution_lsh
