//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/2/22.
// src/dataset/distribution/distribution_dataset.cpp
//
//===-----------------------------------------------------


#include <argparse/argparse.hpp>
#include <fmt/core.h>
#include <magic_enum/magic_enum.hpp>

#include <dataset/distribution/distribution_dataset_processor.h>
#include <dataset/distribution/distribution_dataset_manager.h>
#include <dataset/distribution/distribution_dataset_monitor.h>

using distribution_lsh::DataSetType;
using distribution_lsh::DistributionType;
using distribution_lsh::NormalizationType;
using distribution_lsh::DistributionDatasetProcessor;
using distribution_lsh::DistributionDataSetManager;
using distribution_lsh::DistributionDataSetMonitor;

auto  main(int argc, char** argv) -> int {

  argparse::ArgumentParser program("distribution_dataset");
  // Basic arguments for constructing dataset
  program.add_argument("--data-set-type", "-ds").default_value(std::string {"generation"}).help("data set type of the dataset(generation, mnist, cifar10)");
  program.add_argument("--distribution-type", "-dt").default_value(std::string {"uniform"}).help("distribution type of the dataset(uniform, cauchy, gaussian)");
  program.add_argument("--normalization-type", "-n").default_value(std::string {"softmax"}).help("normalization type of the dataset(softmax, minimax)");
  program.add_argument("--dimension", "-d").help("dimension of the dataset");
  program.add_argument("--params", "-p").nargs(2).default_value(std::vector<float>{0.0F, 1.0F}).scan<'g', float>().help("parameters of the dataset");
  program.add_argument("--size", "-s").help("size of the dataset");
  program.add_argument("--ratio", "-r").help("training set and testing set ratio of the dataset");
  program.add_argument("--input-dir", "-i").help("raw data directory of the dataset(in the case of mnist and cifar10)");
  program.add_argument("--output-dir", "-o").default_value(std::string {"./dataset"}).help("output directory of the dataset");

  // Arguments for buffer pool manager
  program.add_argument("--lru-k", "-k").default_value(16).help("lru-k size of dataset");
  program.add_argument("--buffer-pool-size", "-b").default_value(50).help("buffer pool size of dataset");

  // Arguments for page
  program.add_argument("--directory-page-size", "-dps").default_value(1000).help("directory page size of dataset");
  program.add_argument("--data-page-size", "-dps").default_value(1000).help("data page size of dataset");

  try {
    program.parse_args(argc, argv);
  } catch (const std::runtime_error &err) {
    std::cerr << err.what() << "\n";
    std::cerr << program;
    return 1;
  }

  // Data set type : generation, mnist, cifar10
  auto data_set_type = magic_enum::enum_cast<DataSetType>(program.get<std::string>("--data-set-type"), magic_enum::case_insensitive);
  if (!data_set_type.has_value() || data_set_type.value() == distribution_lsh::DataSetType::INVALID_DATA_SET_TYPE) {
    std::cerr << "Invalid data set type!\n";
    return 1;
  }

  // Distribution type : uniform, cauchy, gaussian
  auto distribution_type = magic_enum::enum_cast<DistributionType>(program.get<std::string>("--distribution-type"), magic_enum::case_insensitive);
  if (!distribution_type.has_value() || distribution_type.value() == distribution_lsh::DistributionType::INVALID_DISTRIBUTION_TYPE) {
    std::cerr << "Invalid distribution type!\n";
    return 1;
  }

  // Normalization type : softmax, minimax
  auto normalization_type = magic_enum::enum_cast<NormalizationType>(program.get<std::string>("--normalization-type"), magic_enum::case_insensitive);
  if (!normalization_type.has_value() || normalization_type.value() == distribution_lsh::NormalizationType::INVALID_NORMALIZATION_TYPE) {
    std::cerr << "Invalid normalization type!\n";
    return 1;
  }

  // Dimension of dataset
  if (!program.present("--dimension")) {
    std::cerr << "Size of dataset is required!\n";
    return 1;
  }
  auto dimension = std::stoi(program.get("--dimension"));

  // Parameters of dataset
  auto params_vector = program.get<std::vector<float>>("--params");
  auto parameters = std::make_shared<float []>(2);
  parameters[0] = params_vector[0];
  parameters[1] = params_vector[1];

  // Size of dataset
  if (!program.present("--size")) {
    std::cerr << "Size of dataset is required!\n";
    return 1;
  }
  auto size = std::stoi(program.get("--size"));

  // Ratio of training set and testing set
  if (!program.present("--ratio")) {
    std::cerr << "Ratio of training set and testing set is required!\n";
    return 1;
  }
  auto ratio = std::stof(program.get("--ratio"));

  // Input directory of dataset
  if (!program.present("--input-dir") && data_set_type != DataSetType::GENERATION) {
    std::cerr << "Input directory is required in case of mnist and cifar10!\n";
    return 1;
  }
  auto input_directory = program.present("--input-dir")? program.get<std::string>("--input-dir") : std::string {};

  // Output directory of dataset
  auto output_directory = program.get<std::string>("--output-dir");
  auto training_set_directory = fmt::format("{}/training_set", output_directory);
  auto testing_set_directory = fmt::format("{}/testing_set", output_directory);
  auto relation_directory = fmt::format("{}/relation", output_directory);

  // Buffer pool arguments
  auto lru_k = std::stoi(program.get("--lru-k"));
  auto buffer_pool_size = std::stoi(program.get("--buffer-pool-size"));

  // Page size
  auto directory_page_size = program.get<int>("--directory-page-size");
  auto data_page_size = program.get<int>("--data-page-size");

  // Create dataset monitor
  auto monitor = std::make_shared<DistributionDataSetMonitor<float>>(input_directory, training_set_directory, testing_set_directory, relation_directory, lru_k, buffer_pool_size, directory_page_size, data_page_size);
  monitor->GetDataSetIndex(data_set_type.value(), distribution_type.value(), normalization_type.value(), dimension, parameters[0], parameters[1], size, ratio);
  monitor->List();
  return 0;
}

