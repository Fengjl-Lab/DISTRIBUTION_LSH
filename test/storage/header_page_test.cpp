//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/9/18.
// test/storage/header_page_test.cpp
//
//===-----------------------------------------------------

#include <common/util/file.h>
#include <common/config.h>
#include <storage/page/header_page.h>

#include <gtest/gtest.h>

namespace distribution_lsh {
TEST(HEADER_PAGE_TEST, GenerationTest) {
  char header_page_buffer[DISTRIBUTION_LSH_PAGE_SIZE] = {0};
  HeaderPage* header_page = reinterpret_cast<HeaderPage *>(header_page_buffer);
  header_page->SetFileIdentification(FileType::B_PLUS_TREE_FILE, 0x1000);
  EXPECT_EQ(header_page->GetFileType(), FileType::B_PLUS_TREE_FILE);

  header_page->SetFileIdentification(FileType::RANDOM_LINE_FILE, 0x1000);
  EXPECT_EQ(header_page->GetFileType(), FileType::RANDOM_LINE_FILE);

  header_page->SetFileIdentification(FileType::DISTRIBUTION_DATASET_FILE, 0x1000);
  EXPECT_EQ(header_page->GetFileType(), FileType::DISTRIBUTION_DATASET_FILE);

  header_page->SetFileIdentification(FileType::RELATION_FILE, 0x1000);
  EXPECT_EQ(header_page->GetFileType(), FileType::RELATION_FILE);
}

} // namespace distribution_lsh
