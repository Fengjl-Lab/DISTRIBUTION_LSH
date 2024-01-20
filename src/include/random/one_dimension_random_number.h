//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/11/30.
// src/include/random/one_dimension_random_number.h
//
//===-----------------------------------------------------

#pragma once

#include <random>

namespace distribution_lsh {
    /**
     * @brief class used to generating random variable
     * */
    template<typename T>
    class OneDimensionRandomNumber {
    public:
        /**
         * Constructor for produce a one-dimension random number*/
        OneDimensionRandomNumber() = default;

        ~OneDimensionRandomNumber() = delete;

        /**
         * Gaussian distribution: mu & sigma
         * */
        static auto GenerateGaussianDistribution(int mu, int sigma) -> T;

        /**
         * Cauchy distribution
         */
        static auto GenerateCauchyDistrbution() -> T;

        /**
         * Unit distribution
         */
        static auto GenerateUnitDistribution(int min, int max) -> T;
    };
} // namespace distribution_lsh
