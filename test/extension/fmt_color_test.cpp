//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/2/19.
// test/extension/fmt_color_test.cpp
//
//===-----------------------------------------------------

#include <fmt/core.h>
#include <fmt/color.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

TEST(FmtTest, ColorTest) {
  fmt::print("\033[31mThis is red text.\033[0m\n");       // 红色文本
  fmt::print("\033[32mThis is green text.\033[0m\n");     // 绿色文本
  fmt::print("\033[33mThis is yellow text.\033[0m\n");    // 黄色文本
  fmt::print("\033[34mThis is blue text.\033[0m\n");      // 蓝色文本
  fmt::print("\033[35mThis is magenta text.\033[0m\n");   // 洋红文本
  fmt::print("\033[36mThis is cyan text.\033[0m\n");      // 青色文本
  fmt::print("\033[1mThis is bold text.\033[0m\n");       // 粗体文本
  fmt::print("\033[4mThis is underlined text.\033[0m\n"); // 下划线文本

  auto result = fmt::format("The sky is {}\n", fmt::format(fg(fmt::color::royal_blue), "blue"));
  fmt::print("{}", result);
  fmt::print("Bold style is {}\n", fmt::format(fmt::emphasis::bold, "bold"));
}