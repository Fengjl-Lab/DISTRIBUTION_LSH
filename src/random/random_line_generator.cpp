//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2024/1/21.
// src/random/random_line_generator.cpp
//
//===-----------------------------------------------------

#include <random/random_line_generator.h>
#include <common/logger.h>

#include <omp.h>
#include <random>

namespace distribution_lsh {

template<typename ValueType>
auto RandomLineGenerator<ValueType>::GenerateRandomLine(RandomLineDistributionType distribution_type, RandomLineNormalizationType normalization_type, int dimension) -> std::shared_ptr<ValueType[]> {
  std::shared_ptr<ValueType[]> data (new ValueType[dimension]);
  std::random_device rd;
  std::mt19937 generator(rd());

  switch (distribution_type) {
    case RandomLineDistributionType::INVALID_DISTRIBUTION_TYPE : {
      LOG_DEBUG("Invalid distribution type");
      return nullptr;
    }
    case RandomLineDistributionType::CAUCHY : {
      std::cauchy_distribution<> distribution(0.0, 1.0);
      for (int i = 0; i < dimension; ++i) {
        data[i] = static_cast<ValueType>(distribution(generator));
      }
      break;
    }
    case RandomLineDistributionType::GAUSSIAN : {
      std::normal_distribution<> distribution(0.0, 1.0);
      for (int i = 0; i < dimension; ++i) {
        data[i] = static_cast<ValueType>(distribution(generator));
      }
      break;
    }
    default: {
      LOG_DEBUG("Unsupported distribution type");
      return nullptr;
    }
  }

  return Normalization(data, normalization_type, dimension);
}

template<typename ValueType>
auto RandomLineGenerator<ValueType>::Normalization(std::shared_ptr<ValueType[]> data, RandomLineNormalizationType normalization_type, int dimension) -> std::shared_ptr<ValueType[]> {
  switch (normalization_type) {
    case RandomLineNormalizationType::INVALID_NORMALIZATION_TYPE : {
      LOG_DEBUG("Invalid normalization type");
      return nullptr;
    }
    case RandomLineNormalizationType::NONE : {
      return data;
    }
    case RandomLineNormalizationType::L1_NORM: {
      auto norm = 0.0F;

#pragma omp parallel for reduction(+:norm) default(none) shared(data, dimension)
      for (int i = 0; i < dimension; ++i) {
        norm += std::abs(data[i]);
      }

      if (std::abs(norm - 0.0F) < 1E-5) {
        return data;
      }

#pragma omp parallel default(none) shared(data, dimension, norm)
      for (int i = 0; i < dimension; ++i) {
        data[i] /= norm;
      }
      return data;
    }
    case RandomLineNormalizationType::L2_NORM: {
      auto norm = 0.0F;

#pragma omp parallel for reduction(+:norm) default(none) shared(data, dimension)
      for (int i = 0; i < dimension; ++i) {
        norm += data[i] * data[i];
      }

      norm = std::sqrt(norm);

#pragma omp parallel default(none) shared(data, dimension, norm)
      for (int i = 0; i < dimension; ++i) {
        data[i] /= norm;
      }
      return data;
    }
    default: {
      LOG_DEBUG("Unsupported normalization type");
      return nullptr;
    }
  }
}

template class RandomLineGenerator<int>;
template class RandomLineGenerator<float>;
template class RandomLineGenerator<double>;
} // namespace distribution_lsh