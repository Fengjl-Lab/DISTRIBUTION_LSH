//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/7/10.
// src/file/random_line_monitor.cpp
//
//===-----------------------------------------------------

#include <string_view>

#include <fmt/format.h>

#include <file/random_line_monitor.h>

namespace distribution_lsh {

using namespace indicators;

RANDOM_LINE_MONITOR_TEMPLATE
RANDOM_LINE_MONITOR_TYPE::RandomLineMonitor(
    std::string b_plus_tree_directory_name,
    std::string random_line_directory_name,
    std::string relation_directory_name,
    int k,
    int pool_size,
    int b_plus_tree_internal_max_size,
    int b_plus_tree_leaf_max_size,
    int random_line_directory_page_max_size,
    int random_line_data_page_max_size)
    : b_plus_tree_directory_name_(std::move(b_plus_tree_directory_name)),
      random_line_directory_name_(std::move(random_line_directory_name)),
      relation_directory_name_(std::move(relation_directory_name)),
      k_(k),
      pool_size_(pool_size),
      b_plus_tree_internal_max_size_(b_plus_tree_internal_max_size),
      b_plus_tree_leaf_max_size_(b_plus_tree_leaf_max_size),
      random_line_directory_page_max_size_(random_line_directory_page_max_size),
      random_line_data_page_max_size_(random_line_data_page_max_size) {

  // Indicators progress bar presentation
  // Progress bar for reading
  ProgressBar random_lines_process{
      option::BarWidth{50},
      option::ForegroundColor{Color::cyan},
      option::ShowElapsedTime{true},
      option::ShowRemainingTime{true},
      option::PrefixText{"Random Lines Loading"},
      indicators::option::FontStyles{
          std::vector<indicators::FontStyle>{indicators::FontStyle::bold}}
  };

  ProgressBar b_plus_tree_process{
      option::BarWidth{50},
      option::ForegroundColor{Color::red},
      option::ShowElapsedTime{true},
      option::ShowRemainingTime{true},
      option::PrefixText{"B+ Tree Loading"},
      indicators::option::FontStyles{
          std::vector<indicators::FontStyle>{indicators::FontStyle::bold}}
  };

  ProgressBar relation_process{
      option::BarWidth{50},
      option::ForegroundColor{Color::grey},
      option::ShowElapsedTime{true},
      option::ShowRemainingTime{true},
      option::PrefixText{"Relation Files Reading"},
      indicators::option::FontStyles{
          std::vector<indicators::FontStyle>{indicators::FontStyle::bold}}
  };

  // Load from random line directory and prepare bpms
  if (!std::filesystem::exists(std::filesystem::path{random_line_directory_name_})) {
    std::filesystem::create_directories(std::filesystem::path{random_line_directory_name_});
  }

  auto random_lines_size = std::distance(std::filesystem::recursive_directory_iterator{std::filesystem::path{
      random_line_directory_name_}}, std::filesystem::recursive_directory_iterator{});
  std::atomic<int> random_lines_index = 0;
  random_lines_process.set_option(option::MaxProgress{random_lines_size});

  std::ranges::for_each(
      std::filesystem::directory_iterator{std::filesystem::path{random_line_directory_name_}},
      [&](const auto &entry) {
        if (std::string_view{FileFullExTension(entry.path().filename())} == RANDOM_LINE_FILE_SUFFIX) {
          auto disk_manager = std::make_shared<DiskManager>(entry.path().string());
          auto next_page_id = GetNextPageId(disk_manager.get(), entry.path().string());
          auto bpm = std::make_shared<BufferPoolManager>(pool_size_, disk_manager, k_, nullptr, next_page_id);
          random_line_bpms_.insert({static_cast<file_id_t >(std::stoull(entry.path().filename())), bpm});

          random_lines_process.tick();
          random_lines_index++;
          random_lines_process.set_option(option::PrefixText{
              std::to_string(random_lines_index) + "/" + std::to_string(random_lines_size)});
        }
      }
  );

  if (random_lines_index == random_lines_size && random_lines_size != 0) {
    random_lines_process.mark_as_completed();
  }

  // Load from b+ tree directory and prepare bpms
  if (!std::filesystem::exists(std::filesystem::path{b_plus_tree_directory_name_})) {
    std::filesystem::create_directories(std::filesystem::path{b_plus_tree_directory_name_});
  }

  auto b_plus_trees_size =
      std::distance(std::filesystem::recursive_directory_iterator{std::filesystem::path{b_plus_tree_directory_name_}},
                    std::filesystem::recursive_directory_iterator{});
  std::atomic<int> b_plus_trees_index = 0;

  std::ranges::for_each(
      std::filesystem::directory_iterator{std::filesystem::path{b_plus_tree_directory_name_}},
      [&](const auto &entry) {
        if (std::string_view{FileFullExTension(entry.path().filename())} == B_PLUS_TREE_FILE_SUFFIX) {
          auto disk_manager = std::make_shared<DiskManager>(entry.path().string());
          auto next_page_id = GetNextPageId(disk_manager.get(), entry.path().string());
          auto bpm = std::make_shared<BufferPoolManager>(pool_size_, disk_manager, k_, nullptr, next_page_id);
          b_plus_tree_bpms_.insert({static_cast<file_id_t >(std::stoull(entry.path().filename())), bpm});
        }

        b_plus_tree_process.tick();
        b_plus_trees_index++;
        b_plus_tree_process.set_option(option::PrefixText{
            std::to_string(b_plus_trees_index) + "/" + std::to_string(b_plus_trees_size)});
      }
  );

  if (b_plus_trees_index == b_plus_trees_size && b_plus_trees_size != 0) {
    b_plus_tree_process.mark_as_completed();
  }

  // Read from relation file directory and prepare bpm
  if (!std::filesystem::exists(std::filesystem::path{relation_directory_name_})) {
    std::filesystem::create_directories(relation_directory_name_);
  }
  auto relation_directory_size =
      std::distance(std::filesystem::recursive_directory_iterator{std::filesystem::path{relation_directory_name_}},
                    std::filesystem::recursive_directory_iterator{}) / 2;
  std::atomic<int> relation_index{0};
  relation_process.set_option(option::MaxProgress{relation_directory_size});

  std::ranges::for_each(
      std::filesystem::directory_iterator{std::filesystem::path{relation_directory_name_}},
      [&](const auto &entry) {
        if (std::string_view{FileFullExTension(entry.path().filename())} == RELATION_FILE_SUFFIX) {
          auto disk_manager = std::make_shared<DiskManager>(entry.path().string());
          auto next_page_id = GetNextPageId(disk_manager.get(), entry.path().string());
          auto bpm = std::make_shared<BufferPoolManager>(pool_size_, disk_manager, k_, nullptr, next_page_id);
          auto relation_manager = std::make_shared<RelationManager<RandomLineFileToBPlusTreeFileUnion>>(
              "relation manager",
              relation_directory_name_,
              bpm,
              RelationFileType::RANDOM_LINE_TO_B_PLUS_TREE,
              INVALID_FILE_ID,
              HEADER_PAGE_ID);
          if (relation_manager->GetType() != RelationFileType::RANDOM_LINE_TO_B_PLUS_TREE) {
            return;
          }

          relation_managers_.insert({static_cast<file_id_t >(std::stoull(entry.path().filename())), relation_manager});

          relation_process.tick();
          relation_index++;
          relation_process.set_option(option::PostfixText{
              std::to_string(relation_index) + "/" + std::to_string(relation_directory_size) + +" completed."});
        }
      }
  );

  if (relation_index == relation_directory_size && relation_index != 0) {
    relation_process.mark_as_completed();
  }

  // constructing b plus tree and random line manager according to the relation file
  for (const auto &[relation_file_id, relation_manager] : relation_managers_) {
    while (true) {
      try {
        auto data_page_id = INVALID_PAGE_ID;
        auto slot = NULL_SLOT_END;
        auto relation_record = relation_manager->Get(current_index_, &data_page_id, &slot);

        if (relation_record.next_null_slot_ == NULL_SLOT_END) {
          break;
        }

        if (relation_record.next_null_slot_ == INVALID_SLOT_VALUE) {
          current_index_++;
          continue;
        }

        // According to the relation record to find specific random line and b plus tree
        if (random_line_bpms_.find(relation_record.map_.random_line_file_id_) == random_line_bpms_.end()
            || b_plus_tree_bpms_.find(relation_record.map_.b_plus_tree_file_id_) == b_plus_tree_bpms_.end()) {
          fmt::println("Random line file/B plus tree file not found(random line file id {}, b plus tree file id {})",
                       relation_record.map_.random_line_file_id_,
                       relation_record.map_.b_plus_tree_file_id_);

          relation_manager->Delete(data_page_id, slot);
          // Just remove the b plus tree file
          std::filesystem::remove(std::filesystem::path{
              b_plus_tree_directory_name_ + "/" + std::to_string(relation_record.map_.b_plus_tree_file_id_)
                  + B_PLUS_TREE_FILE_SUFFIX}) ? fmt::print("B plus tree file {} has successfully deleted\n",
                                                           relation_record.map_.b_plus_tree_file_id_) : fmt::print(
              "B plus tree file not found\n");
          current_index_++;
          continue;
        }


        // Constructing random line manager, use data set file as identifier
        auto rlg = std::make_shared<RandomLineGenerator<RandomLineValueType >>();
        auto random_line_manager =
            std::make_shared<RandomLineManager<RandomLineValueType>>(
                std::to_string(relation_record.map_.random_line_file_id_),
                relation_record.map_.random_line_file_id_,
                random_line_bpms_[relation_record.map_.random_line_file_id_],
                rlg,
                HEADER_PAGE_ID,
                INVALID_DIMENSION,
                random_line_directory_page_max_size_,
                random_line_data_page_max_size_,
                RandomLineDistributionType::INVALID_DISTRIBUTION_TYPE,
                RandomLineNormalizationType::INVALID_NORMALIZATION_TYPE,
                EPSILON);
        random_line_managers_.insert({{relation_record.map_.data_set_file_id_,
                                       random_line_manager->GetDistributionType(),
                                       random_line_manager->GetNormalizationType(),
                                       static_cast<int>(random_line_manager->GetEpsilon())},
                                      random_line_manager});

#ifdef BY_PASS_ACCESS_RANDOM_LINE_MANAGER
        by_pass_random_line_managers_.insert({relation_record.map_.random_line_file_id_, random_line_manager});
#endif
        // Constructing B Plus Tree, use random line file id concat its directory id and slot as identifier
        b_plus_trees_[{relation_record.map_.random_line_file_id_, {relation_record.map_.random_line_directory_page_id_,
                                                                   static_cast<uint32_t >(relation_record.map_.random_line_slot_)}}] =
            std::make_shared<BPlusTree<BPlusTreeKeyType, BPlusTreeValueType>>(
                std::to_string(relation_record.map_.random_line_file_id_) + "-"
                    + std::to_string(relation_record.map_.random_line_directory_page_id_) + "-"
                    + std::to_string(relation_record.map_.random_line_slot_),
                HEADER_PAGE_ID,
                b_plus_tree_bpms_[relation_record.map_.b_plus_tree_file_id_],
                b_plus_tree_leaf_max_size_,
                b_plus_tree_internal_max_size_);

        current_index_++;
      } catch (Exception &exception) {
        current_index_++;
        break;
      }
    }
  }

  if (relation_managers_.empty()) {
    auto relation_file_id = GenerateFileIdentification(relation_directory_name_, FileType::RELATION_FILE);
    auto relation_disk_manager = std::make_shared<DiskManager>(
        relation_directory_name_ + "/" + std::to_string(relation_file_id) + RELATION_FILE_SUFFIX);
    auto relation_bpm = std::make_shared<BufferPoolManager>(pool_size_, relation_disk_manager, k_, nullptr);
    auto relation_manager = std::make_shared<RelationManager<RandomLineFileToBPlusTreeFileUnion>>(
        "relation manager-" + std::to_string(relation_file_id),
        relation_directory_name_,
        relation_bpm,
        RelationFileType::RANDOM_LINE_TO_B_PLUS_TREE,
        relation_file_id);
    relation_managers_.insert({relation_file_id, relation_manager});
  }
}

RANDOM_LINE_MONITOR_TEMPLATE
auto RANDOM_LINE_MONITOR_TYPE::RandomProjection(
    int dimension,
    std::shared_ptr<RandomLineValueType[]> data,
    RandomLineDistributionType distribution_type,
    RandomLineNormalizationType normalization_type,
    float epsilon,
    int random_line_size,
    file_id_t training_set_file_id,
    RID data_rid) -> std::shared_ptr<std::pair<file_id_t, RID>[]> {
  // If the random line manager not exists
  {
    std::scoped_lock<std::mutex> lock(latch_);
    if (random_line_managers_.find(std::tuple(training_set_file_id,
                                              distribution_type,
                                              normalization_type,
                                              static_cast<int>(epsilon))) == random_line_managers_.end()) {
      // Prepare buffer pool manager
      auto random_line_file_id = GenerateFileIdentification(random_line_directory_name_, FileType::RANDOM_LINE_FILE);
      auto random_line_disk_manager = std::make_shared<DiskManager>(
          random_line_directory_name_ + "/" + std::to_string(random_line_file_id) + RANDOM_LINE_FILE_SUFFIX);
      auto random_line_next_page_id = GetNextPageId(random_line_disk_manager.get(),
                                                    random_line_directory_name_ + "/"
                                                        + std::to_string(random_line_file_id)
                                                        + RANDOM_LINE_FILE_SUFFIX);
      auto random_line_bpm = std::make_shared<BufferPoolManager>(pool_size_,
                                                                 random_line_disk_manager,
                                                                 k_,
                                                                 nullptr,
                                                                 random_line_next_page_id);
      // Prepare random line generator
      auto rlg = std::make_shared<RandomLineGenerator<RandomLineValueType>>();
      random_line_bpms_.insert({random_line_file_id, random_line_bpm});
      auto rlm = std::make_shared<RandomLineManager<RandomLineValueType>>(
          std::to_string(random_line_file_id),
          random_line_file_id,
          random_line_bpm,
          rlg,
          HEADER_PAGE_ID,
          dimension,
          random_line_directory_page_max_size_,
          random_line_data_page_max_size_,
          distribution_type,
          normalization_type,
          static_cast<int>(epsilon));
      random_line_managers_.insert({{training_set_file_id, distribution_type, normalization_type, epsilon}, rlm});
#ifdef BY_PASS_ACCESS_RANDOM_LINE_MANAGER
      by_pass_random_line_managers_.insert({random_line_file_id, rlm});
#endif
    }
  }

  auto random_line_manager =
      random_line_managers_[{training_set_file_id, distribution_type, normalization_type, static_cast<int>(epsilon)}];
  // Generate more random line for request
  if (random_line_manager->GetSize() < random_line_size) {
    random_line_manager->GenerateRandomLineGroup(random_line_size - random_line_manager->GetSize());
  }

  std::shared_ptr<std::pair<file_id_t, RID>[]> result = std::make_shared<std::pair<file_id_t, RID>[]>(random_line_size);
  // Calculate the random projection value and insert it into the b plus tree
  for (auto current_index = 0; current_index < random_line_size; current_index++) {
    auto random_line_directory_page_id = INVALID_PAGE_ID;
    auto random_line_slot = INVALID_SLOT;
    auto random_projection_value =
        random_line_manager->InnerProduct(current_index, &random_line_directory_page_id, &random_line_slot, data);

    {
      std::scoped_lock<std::mutex> lock(latch_);
      if (b_plus_trees_.find({random_line_manager->GetFileId(), RID(random_line_directory_page_id, random_line_slot)})
          == b_plus_trees_.end()) {
        auto b_plus_tree_file_id = GenerateFileIdentification(b_plus_tree_directory_name_, FileType::B_PLUS_TREE_FILE);
        auto b_plus_tree_disk_manager = std::make_shared<DiskManager>(
            b_plus_tree_directory_name_ + "/" + std::to_string(b_plus_tree_file_id)
                + B_PLUS_TREE_FILE_SUFFIX);
        auto b_plus_tree_next_page_id = GetNextPageId(b_plus_tree_disk_manager.get(),
                                                      b_plus_tree_directory_name_ + "/"
                                                          + std::to_string(b_plus_tree_file_id)
                                                          + B_PLUS_TREE_FILE_SUFFIX);
        auto b_plus_tree_bpm = std::make_shared<BufferPoolManager>(
            pool_size_,
            b_plus_tree_disk_manager,
            k_,
            nullptr,
            b_plus_tree_next_page_id);
        b_plus_tree_bpms_.insert({b_plus_tree_file_id, b_plus_tree_bpm});
        b_plus_trees_.insert({{random_line_manager->GetFileId(),
                               RID(random_line_directory_page_id, random_line_slot)},
                              std::make_shared<BPlusTree<BPlusTreeKeyType, BPlusTreeValueType>>(
                                  std::to_string(random_line_manager->GetFileId()) + "-"
                                      + std::to_string(random_line_directory_page_id) + "-"
                                      + std::to_string(random_line_slot),
                                  HEADER_PAGE_ID,
                                  b_plus_tree_bpms_[b_plus_tree_file_id],
                                  b_plus_tree_leaf_max_size_,
                                  b_plus_tree_internal_max_size_)});

        // Insert into the relation page
        auto index{0};
        relation_managers_.begin()->second->Insert({.map_{random_line_manager->GetFileId(),
                                                          random_line_directory_page_id, random_line_slot,
                                                          b_plus_tree_file_id, training_set_file_id}}, &index);
      }
    }

    auto b_plus_tree =
        b_plus_trees_[{random_line_manager->GetFileId(), RID(random_line_directory_page_id, random_line_slot)}];
    b_plus_tree->Insert(std::move(random_projection_value), data_rid);
    result[current_index] =
        {random_line_manager->GetFileId(), {random_line_directory_page_id, static_cast<uint32_t>(random_line_slot)}};
  }

  return result;
}

RANDOM_LINE_MONITOR_TEMPLATE
void RANDOM_LINE_MONITOR_TYPE::List() {
  fmt::println("RANDOM LINES INFORMATION");
  for (const auto &rlm : random_line_managers_) {
    fmt::print("{}", rlm.second->ToString());
    fmt::print("-------------------------Corresponding B PLUS TREE INFORMATION------------------------\n");
    auto random_line_rids = std::make_shared<std::vector<RID>>();
    rlm.second->GetSize(nullptr, random_line_rids);
    for (const auto &random_line_rid : *random_line_rids) {
      auto b_plus_tree =
          b_plus_trees_[{rlm.second->GetFileId(), random_line_rid}];
      fmt::print("B PLUS TREE(random line file id: {}, rid: {})", rlm.second->GetFileId(), random_line_rid.Get());
      std::cout << b_plus_tree->ToString();
    }
  }
}

#ifdef BY_PASS_ACCESS_RANDOM_LINE_MANAGER
RANDOM_LINE_MONITOR_TEMPLATE
auto RANDOM_LINE_MONITOR_TYPE::GetConstituencyPoints(
    file_id_t random_line_file_id,
    RID random_line_rid,
    std::shared_ptr<RandomLineValueType[]> query,
    int radius) -> std::shared_ptr<std::vector<RID>> {
  if (by_pass_random_line_managers_.find(random_line_file_id) == by_pass_random_line_managers_.end()) {
    throw new Exception(ExceptionType::INVALID_ARGUMENT, "Invalid random line file id");
  }

  auto random_projection_value =
      by_pass_random_line_managers_[random_line_file_id]->InnerProduct(random_line_rid.GetPageId(),
                                                                       random_line_rid.GetSlotNum(),
                                                                       query);

  if (b_plus_trees_.find({random_line_file_id, random_line_rid}) == b_plus_trees_.end()) {
    throw new Exception(ExceptionType::INVALID_ARGUMENT, "Invalid random line rid, not related b plus tree");
  }

  auto constituencys = std::make_shared<std::vector<RID>>();
  auto b_plus_tree = b_plus_trees_[{random_line_file_id, random_line_rid}];
  b_plus_tree->RangeRead(random_projection_value - radius, random_projection_value + radius, constituencys.get());
  return constituencys;
}
#endif

template
class RandomLineMonitor<float>;
} // namespace distribution_lsh