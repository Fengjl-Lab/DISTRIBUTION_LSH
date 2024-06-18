//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/18.
// src/storage/page/dataset/distribution_dataset_generation_header_page.cpp
//
//===-----------------------------------------------------

#include <storage/page/dataset/distribution_dataset_generation_header_page.h>

namespace distribution_lsh {

void DistributionDataSetGenerationHeaderPage::Init(
    file_id_t file_id,
    DistributionType distribution_type,
    NormalizationType normalization_type,
    float param1,
    float param2,
    int dimension,
    page_id_t directory_start_page_id) {
  SetFileIdentification(file_id);
  SetDataSetType(DataSetType::GENERATION);
  SetDistributionType(distribution_type);
  SetNormalizationType(normalization_type);
  SetParameter(param1, param2);
  SetDimension(dimension);
  SetDirectoryId(directory_start_page_id);
}

auto DistributionDataSetGenerationHeaderPage::GetDistributionType() const -> DistributionType { return distribution_type_; }
void DistributionDataSetGenerationHeaderPage::SetDistributionType(DistributionType distribution_type) {
  distribution_type_ = distribution_type;
}

auto DistributionDataSetGenerationHeaderPage::GetParameter() const -> std::shared_ptr<float [2]> {
  auto parameter = std::make_shared<float [2]>();
  parameter[0] = parameter_[0];
  parameter[1] = parameter_[1];
  return parameter;
}

void DistributionDataSetGenerationHeaderPage::SetParameter(float param1, float param2) {
  switch (distribution_type_) {
    case DistributionType::INVALID_DISTRIBUTION_TYPE:
      throw Exception(ExceptionType::INVALID,
                      "Input parameter is illegal.");
    case DistributionType::UNIFORM: {
      if (param1 >= param2) {
        throw Exception(ExceptionType::INVALID, "Input parameter is illegal.(param1 should less than param2)");
      }
      break;
    }
    case DistributionType::GAUSSIAN:
    case DistributionType::CAUCHY: {
      if (param2 <= 0) {
        throw Exception(ExceptionType::INVALID, "Input parameter is illegal.(param2 should larger than zero)");
      }
      break;
    }
    default: throw Exception(ExceptionType::INVALID, "Invalid distribution type");
  }

  // Once set the parameter is fixed
  parameter_[0] = param1;
  parameter_[1] = param2;
}

} // namespace distribution_lsh