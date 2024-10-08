//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/4/11.
// src/storage/page/random_line/average_random_line_page.cpp
//
//===-----------------------------------------------------


#include <storage/page/random_line/average_random_line_page.h>

namespace distribution_lsh {
RANDOM_LINE_TEMPLATE
void AVERAGE_RANDOM_LINE_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(RandomLinePageType::AVERAGE_RANDOM_LINE_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
}

RANDOM_LINE_TEMPLATE
auto AVERAGE_RANDOM_LINE_TYPE::ToString() -> std::string {
  return fmt::format("average random line page(next page id={})",
                      GetNextPageId());
}

template class AverageRandomLinePage<int>;
template class AverageRandomLinePage<float>;
template class AverageRandomLinePage<double>;
} // namespace distribution_lsh