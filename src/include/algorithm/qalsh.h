//
// Created by chenjunhao on 2023/11/25.
//
#pragma once

#include <algorithm>
#include <string>
#include <vector>

namespace QALSH {

template<class DType>
class QALSH {
 public:
    QALSH() = delete;
 private:
    /** point data */
    int32_t n_pts;      // number of points
    int16_t dim_;        // data dimension

};

}

