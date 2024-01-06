//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2024/1/2.
// src/include/storage/index/b_plus_tree.h
//
//===-----------------------------------------------------

#pragma once

#include <algorithm>
#include <deque>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>

#include <common/config.h>
#include <common/macro.h>

namespace qalsh {

/**
 * @brief Definition of the Context class.
 *
 * This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
*/
class Context {
 public:
  /**
   *  When you insert into / remove from the B+ tree, store the write guard of header page here.
   *  Remember to drop the header page guard and set it to nullopt when you want to unlock all.
   */

};

}