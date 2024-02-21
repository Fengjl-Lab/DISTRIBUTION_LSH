//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/21.
// src/include/random/random_line_manager.h
//
//===-----------------------------------------------------


#pragma once

#include <common/config.h>
#include <buffer/buffer_pool_manager.h>
#include <storage/page/random_line_header_page.h>
#include <storage/page/random_line_page.h>
#include <storage/page/page_guard.h>
#include <random/random_line_generator.h>


namespace distribution_lsh {

class RandomLineContext {
 public:
  std::deque<ReadPageGuard> read_set_;
  std::deque<WritePageGuard> write_set_;
};

/** @breif class that controls the random lines
 */
class RandomLineManager {
  using FloatRandomLinePage = RandomLinePage<float>;
 public:
  RandomLineManager(std::string manager_name, BufferPoolManager *bpm, RandomLineGenerator *rlg, page_id_t  header_page_id, int dimension_, float epsilon = EPSILON);

  /** Judge if the random line group is empty*/
  auto IsEmpty() -> bool;

  /** Obtain the new random line group */
  auto GenerateRandomLineGroup(int group_size) -> bool;

  /** Compute inner product by header page id and slot number */
  auto InnerProduct(page_id_t header_page_id, int slot, const float* outer_array) -> float;

  /** Print random line group */
  void PrintRandomLineGroup();

  /* Getter and Setter method for size and max size */
  auto GetSize() -> int  {
    if (IsEmpty()) {
      return 0;
    }

    auto size = 0;
    RandomLineContext header_page_ctx;
    auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
    header_page_ctx.read_set_.emplace_back(std::move(header_page_guard));
    auto header_page = header_page_ctx.read_set_.back().template As<RandomLineHeaderPage>();
    size += header_page->GetSize();

    while (header_page->GetNextPageId() != INVALID_PAGE_ID) {
      size += header_page->GetSize();
      auto next_header_page_guard = bpm_->FetchPageRead(header_page->GetNextPageId());
      header_page_ctx.read_set_.emplace_back(std::move(next_header_page_guard));
      header_page = header_page_ctx.read_set_.back().As<RandomLineHeaderPage>();
    }

    return size;
  }

 private:

  /** Calculate the inner product of two random line*/
  auto InnerProductByPageId(page_id_t random_line_page_id, const float* outer_array) -> float;

  /** Store a random line with needed page*/
  auto StoreAverageRandomLine(float *array, RandomLineHeaderPage *header_page) -> bool;

  /** Store a random line with called page*/
  auto Store(float *array, RandomLineHeaderPage *header_page) -> bool;

  /** Update average random line */
  void UpdateAverageRandomLine(page_id_t average_random_line_page_id, page_id_t new_random_line_page_id, RandomLineHeaderPage *header_page);

  /** Print a random line with called page*/
  void PrintRandomLine(page_id_t random_line_page_id);

  std::string manager_name_;
  BufferPoolManager *bpm_;
  page_id_t header_page_id_;
  int dimension_;
  float epsilon_{EPSILON};
  RandomLineGenerator *rlg_{nullptr};
};

} // namespace distribution_lsh