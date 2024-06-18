//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/21.
// src/include/random/random_line_manager.h
//
//===-----------------------------------------------------


#pragma once

#include <mutex>

#include <common/config.h>
#include <common/logger.h>
#include <common/exception.h>
#include <buffer/buffer_pool_manager.h>
#include <storage/page/random_line/random_line_header_page.h>
#include <storage/page/random_line/random_line_page.h>
#include <storage/page/random_line/random_line_data_page.h>
#include <storage/page/random_line/average_random_line_page.h>
#include <storage/page/random_line/random_line_directory_page.h>
#include <storage/page/page_guard.h>
#include <random/random_line_generator.h>

#include <fmt/color.h>
#include <fmt/format.h>

namespace distribution_lsh {

#define RANDOM_LINE_MANAGER_TYPE RandomLineManager<ValueType>

class RandomLineContext {
 public:
  /**
   * When execute some write operation on the file, it need to contain the header_page
   */
  std::optional<WritePageGuard> header_page_{std::nullopt};
  std::deque<ReadPageGuard> read_set_;
  std::deque<WritePageGuard> write_set_;
};

/**
 * @breif class that controls the random lines
 */
RANDOM_LINE_TEMPLATE
class RandomLineManager {
 public:
  explicit RandomLineManager(std::string manager_name,
                             file_id_t file_id,
                             std::shared_ptr<BufferPoolManager> bpm,
                             std::shared_ptr<RandomLineGenerator<ValueType>> rlg,
                             page_id_t header_page_id,
                             int dimension_,
                             int directory_page_max_size,
                             int data_page_max_size,
                             RandomLineDistributionType distribution_type,
                             RandomLineNormalizationType normalization_type,
                             float epsilon = EPSILON);

  /** Judge if the random line group is empty*/
  auto IsEmpty(RandomLineContext *ctx = nullptr) -> bool;

  /** Obtain the new random line group */
  auto GenerateRandomLineGroup(int group_size) -> bool;

  /** Compute inner product by header page id and slot number */
  auto InnerProduct(int index, page_id_t *directory_page_id, int *slot, std::shared_ptr<ValueType[]> outer_array) -> ValueType;

  /** random line group information*/
  auto RandomLineGroupInformation() -> std::string;

  /** Getter and Setter method for attribution */
  auto GetEpsilon() -> float;
  auto GetDimension() -> int;
  auto GetDirectoryPageMaxSize() -> int;
  auto GetDataPageMaxSize() -> int;
  auto GetDistributionType() -> RandomLineDistributionType;
  auto GetNormalizationType() -> RandomLineNormalizationType;
  auto GetSize(RandomLineContext *ctx = nullptr) -> int;

  /** Information of the random line manager */
  auto ToString() -> std::string;

  /**
   * Delete an entry in specific directory page
   * @param directory_page_id
   * @param slot
   * @return delete success or not
   */
  auto Delete(page_id_t directory_page_id, int slot) -> bool;

 private:

  /** Calculate the inner product of two random line */
  auto InnerProduct(page_id_t random_line_page_start_id, std::shared_ptr<ValueType[]> outer_array) -> ValueType;

  /** Store a random line with needed page*/
  auto StoreAverageRandomLine(std::shared_ptr<ValueType[]> array, RandomLineContext *ctx = nullptr) -> bool;

  /** Store a random line with called page*/
  auto Store(std::shared_ptr<ValueType[]> array, RandomLineContext *ctx = nullptr) -> bool;

  /** Update average random line */
  void UpdateAverageRandomLine(std::shared_ptr<ValueType[]> array, RandomLineContext *ctx = nullptr);

  /** A random line with called page*/
  auto RandomLineInformation(page_id_t random_line_page_id) -> std::string;

  std::string manager_name_;
  file_id_t file_id_{INVALID_FILE_ID};
  std::shared_ptr<BufferPoolManager> bpm_{nullptr};
  std::shared_ptr<RandomLineGenerator<ValueType>> rlg_{nullptr};
  page_id_t header_page_id_{INVALID_PAGE_ID};
  int dimension_;
  int directory_page_max_size_{RANDOM_LINE_DIRECTORY_PAGE_SIZE};
  int data_page_max_size_{RANDOM_LINE_DATA_PAGE_SIZE};
  RandomLineDistributionType distribution_type_{RandomLineDistributionType::INVALID_DISTRIBUTION_TYPE};
  RandomLineNormalizationType normalization_type_{RandomLineNormalizationType::INVALID_NORMALIZATION_TYPE};
  float epsilon_{EPSILON};
  std::mutex latch_;     // for empty page verification
};

} // namespace distribution_lsh