//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/11/25.
// src/include/common/macro.h
//
//===-----------------------------------------------------

#pragma once

#include <cassert>
#include <exception>
#include <stdexcept>

namespace distribution_lsh {

#define DISTRIBUTION_LSH_ASSERT(expr, message)  assert((expr) && (message))

#define UNIMPLEMENTED(message) throw std::logic_error(message)

#define DISTRIBUTION_LSH_ENSURE(expr, message)                  \
  if (!(expr)) {                                      \
    std::cerr << "ERROR: " << (message) << "\n"; \
    std::terminate();                                 \
  }

#define UNREACHABLE(message) throw std::logic_error(message)

// Macro's to disable copying and moving
#define DISALLOW_COPY(cname) \
          cname(const cname &) = delete;                    /* NOLINT */ \
          auto operator=(const cname & ) -> cname & = delete;   /* NOLINT */

#define DISALLOW_MOVE(cname) \
    cname(cname &&) = delete;       /* NOLINT */ \
    auto operator=(cname &&)->cname & = delete;      /* NOLINT */

#define DISALLOW_COPY_AND_MOVE(cname) \
    DISALLOW_COPY(cname);             \
    DISALLOW_MOVE(cname);

}  // namespace distribution_lsh

