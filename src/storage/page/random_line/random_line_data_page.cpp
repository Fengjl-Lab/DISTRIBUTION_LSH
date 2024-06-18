//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/4/11.
// src/storage/page/random_line/random_line_data_page.cpp
//
//===-----------------------------------------------------

#include <storage/page/random_line/random_line_data_page.h>

namespace distribution_lsh {

RANDOM_LINE_TEMPLATE
void RANDOM_LINE_DATA_PAGE_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(RandomLinePageType::DATA_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
}

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_DATA_PAGE_TYPE::ToString() -> std::string {
  return fmt::format("average random line page(next page id={})",
                     GetNextPageId());
}

template class RandomLineDataPage<int>;
template class RandomLineDataPage<float>;
template class RandomLineDataPage<double>;
} // namespace distribution_lsh