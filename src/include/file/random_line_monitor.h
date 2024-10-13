//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/7/10.
// src/include/file/random_line_monitor.h
//
//===-----------------------------------------------------

#pragma once

#include <filesystem>
#include <memory>
#include <map>
#include <unordered_map>
#include <mutex>
#include <tuple>
#include <utility>

#include <common/util/file.h>
#include <storage/index/random_line_manager.h>
#include <storage/index/b_plus_tree.h>
#include <file/monitor.h>
#include <storage/page/relation/relation_header_page.h>
#include <storage/index/relation_manager.h>

#include <indicators/cursor_control.hpp>
#include <indicators/progress_bar.hpp>
#include <indicators/progress_spinner.hpp>

namespace distribution_lsh {

#define RANDOM_LINE_MONITOR_TEMPLATE template <typename ValueType>
#define RANDOM_LINE_MONITOR_TYPE RandomLineMonitor<ValueType>

/**
 * Class that monitors the random line and b+ tree.
 */
RANDOM_LINE_MONITOR_TEMPLATE
class RandomLineMonitor : public Monitor {
  using RandomLineValueType = ValueType;
  using BPlusTreeKeyType = ValueType;
  using BPlusTreeValueType = RID;
 public:
    explicit RandomLineMonitor(
      std::string b_plus_tree_directory_name,
      std::string random_line_directory_name,
      std::string relation_directory_name,
      int k = 16,
      int pool_size = 50,
      int b_plus_tree_internal_max_size = GetInternalPageSize<BPlusTreeKeyType, BPlusTreeValueType>(),
      int b_plus_tree_leaf_max_size = GetLeafPageSize<BPlusTreeKeyType, BPlusTreeValueType>(),
      int random_line_directory_page_max_size = GetRandomLineDirectoryPageSize(),
      int random_line_data_page_max_size = GetRandomLineDataPageSize<RandomLineValueType>());

  virtual ~RandomLineMonitor() = default;

  /**
   * Random projection function for training set
   * @param dimension dimension of the input data
   * @param data the real data
   * @param distribution_type random line group distribution type
   * @param normalization_type random line group normalization type
   * @param epsilon random line group epsilon, greater than 0 is valid
   * @param random_line_size random line group size
   * @param training_set_file_id training set file id corresponding to the data
   * @return
   */
  auto RandomProjection(
      int dimension,
      std::shared_ptr<RandomLineValueType[] > data,
      std::shared_ptr<std::vector<RID>> data_rids,
      RandomLineDistributionType distribution_type,
      RandomLineNormalizationType normalization_type,
      float epsilon,
      int random_line_size,
      file_id_t training_set_file_id) -> std::shared_ptr<std::vector<std::vector<std::pair<file_id_t, RID>>>>;

#ifdef BY_PASS_ACCESS_RANDOM_LINE_MANAGER
  /**
   *
   * @param training_set_file_id
   * @param distribution_type
   * @param normalization_type
   * @param epsilon
   * @param random_line_rid
   * @param random_projection_value
   * @param radius
   * @return
   */
  auto GetConstituencyPoints(
       file_id_t random_line_file_id,
       RID random_line_rid,
         std::shared_ptr<RandomLineValueType[] > query,
       int radius) ->std::shared_ptr<std::vector<RID>>;
#endif

  
  void List() override;

 private:


  std::string b_plus_tree_directory_name_;
  std::string random_line_directory_name_;
  std::string relation_directory_name_;
  int k_;
  int pool_size_;
  int b_plus_tree_internal_max_size_;
  int b_plus_tree_leaf_max_size_;
  int random_line_directory_page_max_size_;
  int random_line_data_page_max_size_;
  std::mutex latch_;
  std::atomic<size_t> current_index_{0};
  std::map<file_id_t, std::shared_ptr<BufferPoolManager>> b_plus_tree_bpms_;
  std::map<file_id_t, std::shared_ptr<BufferPoolManager>> random_line_bpms_;
  std::map<file_id_t, std::shared_ptr<RelationManager<RandomLineFileToBPlusTreeFileUnion>>> relation_managers_;
  std::map<std::pair<file_id_t /* random  line file id*/, RID>, std::shared_ptr<BPlusTree<BPlusTreeKeyType, BPlusTreeValueType>>> b_plus_trees_;
  std::map<std::tuple<file_id_t /* training set file id*/, RandomLineDistributionType, RandomLineNormalizationType, int>, std::shared_ptr<RandomLineManager<RandomLineValueType>>> random_line_managers_;
  std::unordered_map<file_id_t /* random line manager file id*/,  std::shared_ptr<RandomLineManager<RandomLineValueType>>> by_pass_random_line_managers_;
};

} // namespace distribution_lsh
