//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/4/11.
// src/include/storage/page/random_line/random_line_data_page.h
//
//===-----------------------------------------------------

#include <common/config.h>
#include <storage/page/random_line/random_line_page.h>

namespace distribution_lsh {

#define RANDOM_LINE_DATA_PAGE_TYPE RandomLineDataPage<ValueType>
#define RANDOM_LINE_DATA_PAGE_HEADER_SIZE RANDOM_LINE_PAGE_HEADER_SIZE
#define RANDOM_LINE_DATA_PAGE_SIZE ((DISTRIBUTION_LSH_PAGE_SIZE - RANDOM_LINE_DATA_PAGE_HEADER_SIZE)/ sizeof(ValueType))

RANDOM_LINE_TEMPLATE
class RandomLineManager;

RANDOM_LINE_TEMPLATE
class RandomLineDataPage : public RandomLinePage {
  friend class RandomLineManager<ValueType>;
 public:
  RandomLineDataPage() = delete;
  RandomLineDataPage(const RandomLineDataPage &other) = delete;

  void Init(int max_size = RANDOM_LINE_DATA_PAGE_SIZE);

  auto ToString() -> std::string override;

 private:
  ValueType array_[0];
};
} // namespace distribution_lsh
