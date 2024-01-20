//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2023/11/17.
// src/include/storage/page/b_plus_tree_header_page.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>

namespace distribution_lsh {

/**
 * The header page is just used to retrieve the root page,
 * preventing potential race condition under concurrent environment. 
*/
class BPlusTreeHeaderPage {
 public:
  /** Delete all constructor / destructor to ensure memory safety */
  BPlusTreeHeaderPage() = delete;

  BPlusTreeHeaderPage(const BPlusTreeHeaderPage &other) = delete;

  page_id_t root_page_id_{INVALID_PAGE_ID};
};

}  // namespace distribution_lsh
