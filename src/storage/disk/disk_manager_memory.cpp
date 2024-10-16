//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/5.
// src/storage/disk/disk_manager_memory.cpp
//
//===-----------------------------------------------------

#include <storage/disk/disk_manager_memory.h>

#include <cassert>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>  // NOLINT

#include <common/exception.h>
#include <common/logger.h>

namespace distribution_lsh {

/**
 * Constructor: used for memory based manager
 */
DiskManagerMemory::DiskManagerMemory(size_t pages) { memory_ = new char[pages * DISTRIBUTION_LSH_PAGE_SIZE]; }

/**
 * Write the contents of the specified page into disk file
 */
void DiskManagerMemory::WritePage(page_id_t page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(page_id) * DISTRIBUTION_LSH_PAGE_SIZE;
  // set write cursor to offset
  num_writes_ += 1;
  memcpy(memory_ + offset, page_data, DISTRIBUTION_LSH_PAGE_SIZE);
}

/**
 * Read the contents of the specified page into the given memory area
 */
void DiskManagerMemory::ReadPage(page_id_t page_id, char *page_data) {
  int64_t offset = static_cast<int64_t>(page_id) * DISTRIBUTION_LSH_PAGE_SIZE;
  memcpy(page_data, memory_ + offset, DISTRIBUTION_LSH_PAGE_SIZE);
}

} // namespace distribution_lsh