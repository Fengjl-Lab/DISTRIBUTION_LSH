//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/18.
// src/storage/page/dataset/distribution_dataset_mnist_header_page.cpp
//
//===-----------------------------------------------------

#include <storage/page/dataset/distribution_dataset_mnist_header_page.h>

namespace distribution_lsh {

void DistributionDataSetMNISTHeaderPage::Init(
    file_id_t file_id,
    NormalizationType normalization_type,
    page_id_t directory_start_page_id) {
  SetFileIdentification(file_id);
  SetDataSetType(DataSetType::MNIST);
  SetNormalizationType(normalization_type);
  SetDimension(784);        // MNIST dataset has 784 features
  SetDirectoryId(directory_start_page_id);
}

} // namespace distribution_lsh