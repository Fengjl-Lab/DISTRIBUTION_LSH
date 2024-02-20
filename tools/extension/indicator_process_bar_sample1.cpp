//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/2/19.
// tools/extension/indicator_process_bar.cpp
//
//===-----------------------------------------------------

#include <indicators/progress_bar.hpp>
#include <thread>
#include <chrono>

auto main() -> int {
  using namespace indicators;
  ProgressBar bar{
      option::BarWidth{100},
      option::Start{"["},
      option::Fill{"="},
      option::Lead{">"},
      option::Remainder{" "},
      option::End{"]"},
      option::PostfixText{"Extracting Archive"},
      option::ForegroundColor{Color::red},
      option::FontStyles{std::vector<FontStyle>{FontStyle::bold}}
  };

  // Update bar state
  while (true) {
    bar.tick();
    if (bar.is_completed()) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  return 0;
}