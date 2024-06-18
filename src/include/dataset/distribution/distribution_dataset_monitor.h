//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/13.
// src/include/dataset/distribution/distribution_dataset_monitor.h
//
//===-----------------------------------------------------

#pragma once

#include <filesystem>
#include <memory>
#include <map>
#include <mutex>
#include <tuple>

#include <common/util/file.h>
#include <dataset/distribution/distribution_dataset_manager.h>
#include <file/monitor.h>
#include <storage/page/relation/relation_header_page.h>
#include <storage/index/relation_manager.h>

#include <indicators/cursor_control.hpp>
#include <indicators/progress_bar.hpp>
#include <indicators/progress_spinner.hpp>

namespace distribution_lsh {

#define DISTRIBUTION_DATASET_MONITOR_TYPE DistributionDataSetMonitor<ValueType>

/**
 * Class that monitors the distribution dataset.
 */
DISTRIBUTION_DATASET_TEMPLATE
class DistributionDataSetMonitor : public Monitor {
 public:
  /**
  * Constructor.
  * @param directory_name The directory name of the distribution dataset.
  */
  explicit DistributionDataSetMonitor(
      std::string raw_data_directory_name,
      std::string training_set_directory_name,
      std::string testing_set_directory_name,
      std::string relation_directory_name,
      int k = 16,
      int pool_size = 50,
      int directory_page_max_size = DISTRIBUTION_DATASET_DIRECTORY_PAGE_SIZE,
      int data_page_max_size = DISTRIBUTION_DATASET_DATA_PAGE_SIZE);

  virtual ~DistributionDataSetMonitor() = default;

  /**
   * Get specific dataset manager index
   * @param size total size of the dataset
   * @param ratio training set and testing set ratio
   * @return the index target dataset manager corresponding to the input constriant
   */
  auto GetDataSetIndex(DataSetType data_set_type,
                       DistributionType distribution_type,
                       NormalizationType normalization_type,
                       int dimension,
                       float param1,
                       float param2,
                       int size,
                       float ratio) -> int;

  /**
   * Get specific data by directory page id and index
   * @param manager_index
   * @param is_training_set
   * @param directory_page_id
   * @param index
   * @return
   */
  auto GetDistributionData(int manager_index, bool is_training_set, page_id_t directory_page_id, int index) -> std::shared_ptr<ValueType[]>;


  /**
   * Get specific data by global index
   * @param manager_index distribution dataset manager index
   * @param index global index of the data
   * @param[return] directory_page_id target data location
   * @param[return] slot logical slot of the data
   * @return data from specific global index, if the location is invalid, we will set the directory_page_id to INVALID and slot to -1
   */
  auto GetDistributionData(int manager_index,
                           bool is_training_set,
                           int index,
                           page_id_t *directory_page_id,
                           int *slot) -> std::shared_ptr<ValueType[]>;

  /**
   * Delete a data in dataset
   * @param manager_index  distribution dataset manager index
   * @param is_training_set
   * @param directory_page_id
   * @param index
   * @return delete data content
   */
  auto DeleteDistributionData(int manager_index, bool is_training_set, page_id_t directory_page_id, int index) -> std::shared_ptr<ValueType[]>;

  void List() override;

 private:
  /**
   * @brief Generate a new manager to generate and manage dataset
   */
  auto GenerateDataSet(DataSetType data_set_type,
                       DistributionType distribution_type,
                       NormalizationType normalization_type,
                       int dimension,
                       float param1,
                       float param2,
                       int size,
                       float ratio) -> int;

  std::string raw_data_directory_name_;
  std::string training_set_directory_name_;
  std::string testing_set_directory_name_;
  std::string relation_directory_name_;
  int k_;
  int pool_size_;
  int directory_page_max_size_;
  int data_page_max_size_;
  std::mutex latch_;
  std::atomic<size_t> current_index_{0};
  std::map<file_id_t, std::shared_ptr<BufferPoolManager>> training_set_bpms_;
  std::map<file_id_t, std::shared_ptr<BufferPoolManager>> testing_set_bpms_;
  std::map<file_id_t, std::shared_ptr<RelationManager<TrainingSetToTestingSetUnion>>> relation_managers_;
  std::map<size_t, std::unique_ptr<DistributionDataSetManager<ValueType>>> dataset_managers_;
};
} // namespace distribution_lsh
