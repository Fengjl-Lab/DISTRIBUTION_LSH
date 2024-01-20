//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2023/12/24.
// src/include/dataset/distribution/distribution_dataset.h
//
//===-----------------------------------------------------

#pragma once
#include <fstream>
#include <mutex>

namespace distribution_lsh {

    template<typename type>
    class DistributionDataSet {
    public:
        DistributionDataSet() = delete;

    private:
        // Dimension of the data
        size_t dimension_;
        // Number of data
        size_t n_;
        // File to write-in
        std::fstream data_file_;
        std::string file_name_;
        // Protect data access
        std::mutex data_latch_;
    };
} // namespace distribution_lsh