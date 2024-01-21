//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2024/1/20.
// src/include/storage/page/random_line_header_page.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>

namespace distribution_lsh {

/**
* @breif page that contain the start page of each random line
*/
class RandomLineHeaderPage {
 public:
  RandomLineHeaderPage() = delete;
  RandomLineHeaderPage(const RandomLineHeaderPage &other) = delete;

  int size_{0};
  page_id_t random_line_pages_start_[0];
};

}  // namespace distribution_lsh

