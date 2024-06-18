//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2023/12/24.
// src/include/dataset/distribution/distribution_dataset.h
//
//===-----------------------------------------------------

#pragma once

#include <memory>

#include <fmt/color.h>
#include <fmt/format.h>

#include <common/config.h>
#include <common/exception.h>
#include <common/rid.h>
#include <buffer/buffer_pool_manager.h>
#include <dataset/distribution/distribution_dataset_processor.h>
#include <storage/page/dataset/distribution_dataset_header_page.h>
#include <storage/page/dataset/distribution_dataset_directory_page.h>
#include <storage/page/dataset/distribution_dataset_generation_header_page.h>
#include <storage/page/dataset/distribution_dataset_mnist_header_page.h>
#include <storage/page/dataset/distribution_dataset_cifar10_header_page.h>
#include <storage/page/dataset/distribution_dataset_data_page.h>

namespace distribution_lsh {

#define DISTRIBUTION_DATASET_MANAGER_TYPE DistributionDataSetManager<ValueType>

struct DistributionDataSetContext {
  std::deque<ReadPageGuard> read_set_;    // read set for distribution dataset
  std::deque<WritePageGuard> write_set_;  // write set for distribution dataset
};

DISTRIBUTION_DATASET_TEMPLATE
class DistributionDataSetMonitor;

DISTRIBUTION_DATASET_TEMPLATE
class DistributionDataSetManager {
  friend class DistributionDataSetMonitor<ValueType>;
  using DataPage = DistributionDataSetDataPage<ValueType>;
 public:
  explicit DistributionDataSetManager(std::string manager_name,
                             DataSetType data_set_type,
                             DistributionType distribution_type,
                             NormalizationType normalization_type,
                             std::shared_ptr<BufferPoolManager> training_set_bpm,
                             std::shared_ptr<BufferPoolManager> testing_set_bpm,
                             std::unique_ptr<DistributionDatasetProcessor<ValueType>> ddp,
                             page_id_t training_set_header_page_id,
                             page_id_t testing_set_header_page_id,
                             int dimension,
                             std::shared_ptr<float [2]> params,
                             std::string directory_name,
                             file_id_t training_set_file_id,
                             file_id_t testing_set_file_id,
                             int directory_page_max_size = DISTRIBUTION_DATASET_DIRECTORY_PAGE_SIZE,
                             int data_page_max_size = DISTRIBUTION_DATASET_DATA_PAGE_SIZE);

  /** Judge if the distribution dataset is empty*/
  auto IsEmpty() -> bool;

  /**
   * @brief Assert that test set and training set are complement, which means they can be a pair.
   */
   auto IsTrainingSetMatchTestingSet() -> bool;

  /** Generate the new distribution dataset */
  auto GenerateDistributionDataset(int size, float ratio) -> bool;

  /** Get method */
  auto GetDataSetType() -> DataSetType { return data_set_type_; };
  auto GetDistributionType() -> DistributionType { return distribution_type_; };
  auto GetNormalizationType() -> NormalizationType { return normalization_type_; };
  auto GetDimension() -> int { return dimension_; };
  auto GetSize(bool is_training_set) -> int;

  /** Get single distribution data
   * @brief get data by directory page id and its logical slot
   * */
  auto GetDistributionData(bool is_training_set, page_id_t directory_page_id, int index) -> std::shared_ptr<ValueType[]>;

  /**
   * @param is_training_set set type
   * @param index total index of the data
   * @return distribution data
   */
  auto GetDistributionData(bool is_training_set, int index, page_id_t *directory_page_id, int *slot) -> std::shared_ptr<ValueType[]>;

  /**
   * @param is_training_set set type
   * @param directory_page_id directory page id
   * @param index logical index
   * @return
   */
  auto Delete(bool is_training_set, page_id_t directory_page_id, int index) -> bool;

  /** Information of the distribution dataset */
  auto ToString() -> std::string;

  /** Store a distribution data into page */
  auto Store(bool is_training_set, ValueType *distribution) -> RID;

 private:

  std::string manager_name_;
  DataSetType data_set_type_;
  DistributionType distribution_type_;
  NormalizationType normalization_type_;
  std::shared_ptr<BufferPoolManager> training_set_bpm_;
  std::shared_ptr<BufferPoolManager> testing_set_bpm_;
  std::unique_ptr<DistributionDatasetProcessor<ValueType>> ddp_;

  page_id_t training_set_header_page_id_{INVALID_PAGE_ID};
  page_id_t testing_set_header_page_id_{INVALID_PAGE_ID};
  int dimension_;
  std::shared_ptr<float [2]>params_;       // Mention: not assign bit on it. Only use page data / unique_ptr out-scope(unique_ptr.get())

  std::string directory_name_;
  int directory_page_max_size_;
  int data_page_max_size_;

  file_id_t training_set_file_id_;
  file_id_t testing_set_file_id_;

  std::mutex empty_mutex_;
};

} // namespace distribution_lsh