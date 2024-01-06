//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2024/1/3.
// src/include/recovery/log_record.h
//
//===-----------------------------------------------------

#pragma once

#include <cassert>
#include <string>

#include <common/config.h>

namespace qalsh {
/** The type of the log record. */
enum class LogRecordType {
  INVALID = 0,
  INSERT,
  MARKDELETE,
  APPLDELETE,
  ROLLBACKDELETE,
  UPDATE,
  BEGIN,
  COMMIT,
  ABORT,
  NEWPAGE,
};

class LogRecord {
  friend class LogManager;
};
}// namespace qalsh