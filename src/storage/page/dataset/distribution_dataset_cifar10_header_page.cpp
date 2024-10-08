//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/18.
// src/storage/page/dataset/distribution_dataset_cifar10_header_page.cpp
//
//===-----------------------------------------------------

#include <storage/page/dataset/distribution_dataset_cifar10_header_page.h>

namespace distribution_lsh {

void DistributionDataSetCIFAR10HeaderPage::Init(
    file_id_t file_id,
    NormalizationType normalization_type,
    page_id_t directory_start_page_id) {
  SetFileIdentification(file_id);
  SetDataSetType(DataSetType::CIFAR10);
  SetNormalizationType(normalization_type);
  SetDimension(32 * 32 * 3);        // CIFAR10 dataset has 32 * 32 * 3 = 3072 features
  SetDirectoryId(directory_start_page_id);
}

} // namespace distribution_lsh