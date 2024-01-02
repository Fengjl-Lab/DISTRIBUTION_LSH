//
// Created by chenjunhao on 2023/11/24.
//

#include <common/exception.h>
#include <common/macro.h>
#include <fmt/ranges.h>
#include <fmt/core.h>
#include <vector>

auto main(int argc, char** argv) -> int {
    std::vector<int> arr = {1, 2, 3};

    fmt::print("arr is {}\n", arr);
    fmt::print("The price of {} is {} Yuan/kg", "apple", 4.2);

    return 0;
}