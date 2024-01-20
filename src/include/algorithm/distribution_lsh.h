//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2023/11/25.
// src/include/algorithm/distribution_lsh.h
//
//===-----------------------------------------------------
#pragma once

#include <algorithm>
#include <string>
#include <vector>

#include <common/exception.h>
#include <common/config.h>

namespace DISTRIBUTION_LSH {

template<class DType>
class DISTRIBUTION_LSH {
 public:
    DISTRIBUTION_LSH() = delete;
 private:
    /** point data */
    int32_t n_pts;          // number of points
    int16_t dim_;          // data dimension
    /** lp distance */
    float p_;               // l_p distance, p in (0,2]
    float zeta_;            // symmetric factor of p-stable distribution.
    /** approximate factor */
    float c_;               // approximate ratio
    /** index phase */
    std::string path_;     // index path
    // const int * index;

    /** algorithm parameter */
    float w_;               // bucket width
    int32_t m_;            // number of has tables
    int32_t l_;            // collision threshold
    uint64_t dist_io_;     // io for computing distance
    uint64_t page_io_;     // io for scanning pages
};

}

