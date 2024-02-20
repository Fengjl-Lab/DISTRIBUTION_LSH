//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2024/1/21.
// src/include/random/random_line_manager.h
//
//===-----------------------------------------------------


#pragma once

#include <common/config.h>
#include <buffer/buffer_pool_manager.h>
#include <storage/page/random_line_header_page.h>
#include <storage/page/random_line_page.h>

namespace distribution_lsh {

/** @breif class that controls the random lines
 */
class RandomLineManager {
  using FloatRandomLinePage = RandomLinePage<float>;
 public:
  RandomLineManager(std::string manager_name, BufferPoolManager bpm, page_id_t  header_page_id);
  ~RandomLineManager() = delete;

  
 private:
  std::string manager_name_;
  BufferPoolManager *bpm_;
  page_id_t header_page_id_;
};
} // namespace distribution_lsh