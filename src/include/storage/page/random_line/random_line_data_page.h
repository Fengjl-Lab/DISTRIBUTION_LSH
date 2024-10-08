//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/4/11.
// src/include/storage/page/random_line/random_line_data_page.h
//
//===-----------------------------------------------------

#include <common/config.h>
#include <storage/page/random_line/random_line_page.h>

namespace distribution_lsh {

#define RANDOM_LINE_DATA_PAGE_TYPE RandomLineDataPage<RandomLineValueType>
#define RANDOM_LINE_DATA_PAGE_HEADER_SIZE RANDOM_LINE_PAGE_HEADER_SIZE
#define RANDOM_LINE_DATA_PAGE_SIZE ((DISTRIBUTION_LSH_PAGE_SIZE - RANDOM_LINE_DATA_PAGE_HEADER_SIZE)/ sizeof(RandomLineValueType))

template <typename RandomLineValueType>
auto constexpr GetRandomLineDataPageSize() {
  if constexpr ((std::is_integral_v<RandomLineValueType> || std::is_floating_point_v<RandomLineValueType>)) {
    return (DISTRIBUTION_LSH_PAGE_SIZE - RANDOM_LINE_DATA_PAGE_HEADER_SIZE) / sizeof(RandomLineValueType);
  } else {
    static_assert(false, "Random line data page template is not valid");
  }
}

RANDOM_LINE_TEMPLATE
class RandomLineManager;

RANDOM_LINE_TEMPLATE
class RandomLineDataPage : public RandomLinePage {
  friend class RandomLineManager<RandomLineValueType>;
 public:
  RandomLineDataPage() = delete;
  RandomLineDataPage(const RandomLineDataPage &other) = delete;

  void Init(int max_size = GetRandomLineDataPageSize<RandomLineValueType>());

  auto ToString() -> std::string override;

 private:
  RandomLineValueType array_[0];
};
} // namespace distribution_lsh
