//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/2/19.
// test/storage/random_line_page_test.cpp
//
//===-----------------------------------------------------

#include <memory>
#include <stdexcept>

#include <buffer/buffer_pool_manager.h>
#include <random/random_line_generator.h>
#include <storage/index/random_line_manager.h>
#include <storage/disk/disk_manager_memory.h>
#include <gtest/gtest.h>

namespace distribution_lsh {

class RandomLineManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto file_id = GetHashValue("random line manager");
    auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
    auto bpm = std::make_shared<BufferPoolManager>(5, disk_manager);
    rlg_ = std::make_shared<RandomLineGenerator<float>>();
    auto header_page_id = INVALID_PAGE_ID;
    dimension_ = 100;
    auto directory_page_max_size = 10;
    auto data_page_max_size = 10;
    auto epsilon = 100.0F;

    rlm_ = std::make_shared<RandomLineManager<float>>("random line manager",
                                                      file_id,
                                                      bpm,
                                                      rlg_,
                                                      header_page_id,
                                                      dimension_,
                                                      directory_page_max_size,
                                                      data_page_max_size,
                                                      RandomLineDistributionType::GAUSSIAN,
                                                      RandomLineNormalizationType::NONE,
                                                      epsilon);
  }

  void TearDown() override {}

  int dimension_;
  std::shared_ptr<RandomLineGenerator<float>> rlg_;
  std::shared_ptr<RandomLineManager<float>> rlm_;
};

TEST_F(RandomLineManagerTest, GenerationTest) {
  // Test if the current random line group is empty
  ASSERT_TRUE(rlm_->IsEmpty());

  // Test generation process
  ASSERT_TRUE(rlm_->GenerateRandomLineGroup(100));
  ASSERT_EQ(rlm_->GetSize(), 100);
  std::cout << rlm_->ToString() << "\n";
  std::cout << rlm_->RandomLineGroupInformation();
}

TEST_F(RandomLineManagerTest, InnerProductTest) {
  // Test if the current random line group is empty
  ASSERT_TRUE(rlm_->IsEmpty());

  // Test generation process
  ASSERT_TRUE(rlm_->GenerateRandomLineGroup(25));
  ASSERT_EQ(rlm_->GetSize(), 25);

  // Test inner product
  auto outer_array =
      rlg_->GenerateRandomLine(RandomLineDistributionType::GAUSSIAN, RandomLineNormalizationType::NONE, dimension_);
  auto directory_page_id = INVALID_PAGE_ID;
  auto slot = INVALID_SLOT_VALUE;


  // case 1 : index is 0
  auto inner_product = rlm_->InnerProduct(0, &directory_page_id, &slot, outer_array);
  ASSERT_NE(directory_page_id, INVALID_PAGE_ID);
  ASSERT_EQ(slot, 0);
  std::cout << inner_product << "\n";

  // case 2: index is invalid
  EXPECT_THROW(rlm_->InnerProduct(-1, &directory_page_id, &slot, outer_array), std::runtime_error);

  // case 3: index in other directory page
  inner_product = rlm_->InnerProduct(21, &directory_page_id, &slot, outer_array);
  ASSERT_NE(directory_page_id, INVALID_PAGE_ID);
  ASSERT_EQ(slot, 1);
  std::cout << inner_product << "\n";
}

TEST_F(RandomLineManagerTest, DeleteTest) {
  // Test if the current random line group is empty
  ASSERT_TRUE(rlm_->IsEmpty());

  // Test generation process
  ASSERT_TRUE(rlm_->GenerateRandomLineGroup(25));
  ASSERT_EQ(rlm_->GetSize(), 25);

  // Test delete Process
  std::vector<int> delete_array({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

  // Delete the data in first page
  for (int i : delete_array) {
    auto size = rlm_->GetSize();
    EXPECT_TRUE(rlm_->Delete(1, i));
    EXPECT_TRUE(rlm_->GetSize() == size - 1);
  }
}

} // namespace distribution_lsh
