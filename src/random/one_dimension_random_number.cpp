//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2023/12/25.
// src/random/one_dimension_random_number.cpp
//
//===-----------------------------------------------------

#include <random/one_dimension_random_number.h>

namespace qalsh {
    template<typename T>
    auto OneDimensionRandomNumber<T>::GenerateUnitDistribution(int min, int max) -> T {
        auto lower_bound = std::numeric_limits<T>::min();
        auto upper_bound = std::numeric_limits<T>::max();
        static_assert((min > lower_bound && max < upper_bound));

        std::random_device rd;
        std::mt19937 gen(rd());
   }
} // namespace qalsh