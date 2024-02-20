//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/2.
// src/include/common/config.h
//
//===-----------------------------------------------------

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

namespace distribution_lsh {

/** Cycle detection is performed every CYCLE_DETECTION_INTERVAL milliseconds. */
extern std::chrono::milliseconds cycle_detection_interval;

/** True if logging should be enabled, false otherwise.*/
extern std::atomic<bool> enable_logging;
/** If ENABLE_LOGGING is true, the log should be flushed to disk every LOG_TIMEOUT. */
extern std::chrono::duration<int64_t> log_timeout;

static const int INVALID_PAGE_ID = -1;                                                        // invalid page id
static const int INVALID_TXN_ID = -1;                                                         // invalid transaction id
static const int INVALID_LSN = -1;                                                            // invalid log sequence number
static const int HEADER_PAGE_ID = 0;                                                          // the header page id
static const int DISTRIBUTION_LSH_PAGE_SIZE = 4096;                                           // size of a data page in byte
static const int BUFFER_POOL_SIZE = 10;                                                       // size of buffer pool
static const int LOG_BUFFER_SIZE = ((BUFFER_POOL_SIZE + 1) * DISTRIBUTION_LSH_PAGE_SIZE);     // size of a log buffer in byte
static const int BUCKET_SIZE = 50;                                                            // size of extendable hash bucket
static const int LRUK_REPLACER_K = 10;                                                        // lookback window for lru-k replacer
static const float EPSILON = 0.1;                                                              // epsilon for generating random line
static const int RANDOM_LINE_GROUP_MAX_SIZE = 1000;                                           // max size of random line group

using frame_id_t = int32_t;     // frame id type
using page_id_t = int32_t;      // page id type
using txn_id_t = int32_t;       // transaction id type
using lsn_t = int32_t;          // log sequence number
using oid_t = uint16_t;

static const int VARCHAR_DEFAULT_LENGTH = 128;
} // namespace distribution_lsh