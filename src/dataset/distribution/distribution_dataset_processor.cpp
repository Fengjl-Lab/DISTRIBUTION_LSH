//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/25.
// src/dataset/distribution/distribution_dataset_processor.cpp
//
//===-----------------------------------------------------

#include <cmath>
#include <fstream>
#include <sstream>

#include <dataset/distribution/distribution_dataset_processor.h>

namespace distribution_lsh {

DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_PROCESSOR_TYPE::GenerationDistributionDataset(int dimension,
                                                                 int size,
                                                                 distribution_lsh::DistributionType distribution_type,
                                                                 distribution_lsh::NormalizationType normalization_type,
                                                                 float *param) -> std::unique_ptr<ValueType[]>  {
  std::unique_ptr<ValueType[]> distribution_dataset(new ValueType[size * dimension]);
  std::random_device rd;
  std::mt19937 gen(rd());


  // Generate raw data
  // Uniform distribution
  if (distribution_type == DistributionType::UNIFORM) {
    std::uniform_real_distribution<> dis(param[0], param[1]);
    for (int i = 0; i < size; i++) {
      for (int j = 0; j < dimension; j++) {
        distribution_dataset[i * dimension + j] = static_cast<ValueType>(dis(gen));
      }
    }
  }
    // Gaussian distribution
  else if (distribution_type == DistributionType::GAUSSIAN) {
    std::normal_distribution<> dis(param[0], param[1]);
    for (int i = 0; i < size; i++) {
      for (int j = 0; j < dimension; j++) {
        distribution_dataset[i * dimension + j] = static_cast<ValueType>(dis(gen));
      }
    }
  }
    // Cauchy distribution
  else if (distribution_type == DistributionType::CAUCHY) {
    std::cauchy_distribution<> dis(param[0], param[1]);
    for (int i = 0; i < size; i++) {
      for (int j = 0; j < dimension; j++) {
        distribution_dataset[i * dimension + j] = static_cast<ValueType>(dis(gen));
      }
    }
  } else {
    throw Exception("Distribution type not supported");
  }

  // Normalization phase
  Normalize(distribution_dataset.get(), size, dimension, normalization_type);

  return distribution_dataset;
}


DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_PROCESSOR_TYPE::MNISTDistributionDataset(int size,
                                                            const std::string& directory_name_,
                                                            distribution_lsh::NormalizationType normalization_type) -> std::unique_ptr<ValueType[]> {
  std::unique_ptr<ValueType[]> distribution_dataset(new ValueType[size * 784]); // MNIST dataset is 28x28, so dimension is 784

  // Open target file
  std::string directory_name = directory_name_ + "/mnist_train.csv";
  std::ifstream ifs(directory_name, std::ios::in);
  if (!ifs.is_open()) {
    throw Exception("Cannot open file");
  }

  // Read data from file
  std::string line;
  auto current_row = -1;
  while (std::getline(ifs, line) && current_row < size) {
    std::stringstream ss(line);
    std::string cell;

    auto current_col = 1;
    while (std::getline(ss, cell, ',') && current_row != -1) {
      if (current_col > 784) {
        continue;
      }

      distribution_dataset[current_row * 784 + current_col - 1] = static_cast<ValueType>(std::stod(cell));
      current_col++;
    }

    current_row ++;
  }

  // Normalization phase
  Normalize(distribution_dataset.get(), size, 784, normalization_type);

  // Close file
  ifs.close();

  return distribution_dataset;
}

DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_PROCESSOR_TYPE::CIFAR10DistributionDataset(int size,
                                                              std::string directory_name_,
                                                              distribution_lsh::NormalizationType normalization_type) -> std::unique_ptr<ValueType[]> {
  std::unique_ptr<ValueType[]> distribution_dataset(new ValueType[size * 32 * 32 * 3]); // CIFAR dataset dimension is 32*32*3


  return distribution_dataset;
}

DISTRIBUTION_DATASET_TEMPLATE
void DISTRIBUTION_DATASET_PROCESSOR_TYPE::Normalize(float *distribution_dataset,
                                             int size,
                                             int dimension,
                                             distribution_lsh::NormalizationType normalization_type) {
  // Normalize raw data into [0, 1]
  // Softmax normalization
  // Determine the appropriate SIMD instruction set
  if (normalization_type == NormalizationType::SOFTMAX) {
    for (int i = 0; i < size; i++) {
      // TODO : Use SIMD to accelerate this part
      for (int j = 0; j < dimension; j++) {
        distribution_dataset[i * dimension + j] = std::exp(distribution_dataset[i * dimension + j]);
      }
      auto sum = static_cast<ValueType>(std::accumulate(distribution_dataset + i * dimension,
                                                    distribution_dataset + (i + 1) * dimension,
                                                    0.0));
      for (int j = 0; j < dimension; j++) {
        distribution_dataset[i * dimension + j] /= sum;
      }
    }
  }
    // Min-max normalization
  else if (normalization_type == NormalizationType::MIN_MAX) {
    for (int i = 0; i < size; i++) {
      // TODO : Use SIMD to accelerate this part
      auto min_value = 0.0F;
      for (int j = 0; j < dimension; j++) {
        if (distribution_dataset[i * dimension + j] < min_value) {
          min_value = distribution_dataset[i * dimension + j];
        }
      }
      auto sum = static_cast<ValueType>(std::accumulate(distribution_dataset + i * dimension,
                                                    distribution_dataset + (i + 1) * dimension,
                                                    0.0));
      sum += static_cast<ValueType>(std::abs(min_value)) * static_cast<ValueType>(dimension);
      for (int j = 0; j < dimension; j++) {
        distribution_dataset[i * dimension + j] = (distribution_dataset[i * dimension + j] + static_cast<ValueType>(std::abs(min_value))) / sum;
      }
    }
  }
    // Exception
  else {
    throw Exception("Normalization type not supported");
  }
}


template class DistributionDatasetProcessor<float>;
} // namespace distribution_lsh