//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2023/12/24.
// src/include/dataset/distribution/distribution_dataset.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>
#include <buffer/buffer_pool_manager.h>
#include <storage/page/distribution_dataset_header_page.h>
#include <storage/page/distribution_dataset_page.h>

namespace distribution_lsh {

class DistributionDataSetManager {
 public:
  DistributionDataSetManager() = delete;

 private:
  std::string manager_name_;
  BufferPoolManager *bpm_;
  page_id_t header_page_id_;
  int dimension_;
  float epsilon_{EPSILON};
};
} // namespace distribution_lsh