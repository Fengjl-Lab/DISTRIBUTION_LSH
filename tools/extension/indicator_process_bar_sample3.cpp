//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/2/19.
// tools/extension/indicator_process_bar_sample3.cpp
//
//===-----------------------------------------------------

#include <chrono>
#include <indicators/cursor_control.hpp>
#include <indicators/progress_bar.hpp>
#include <thread>

auto main() -> int {
  using namespace indicators;

  // Hide cursor
  show_console_cursor(false);

  indicators::ProgressBar bar{
      option::BarWidth{50},
      option::Start{" ["},
      option::Fill{"█"},
      option::Lead{"█"},
      option::Remainder{"-"},
      option::End{"]"},
      option::PrefixText{"Training Gaze Network 👀"},
      option::ForegroundColor{Color::yellow},
      option::ShowElapsedTime{true},
      option::ShowRemainingTime{true},
      option::FontStyles{std::vector<FontStyle>{FontStyle::bold}}
  };

  // Update bar state
  while (true) {
    bar.tick();
    if (bar.is_completed())
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }

  // Show cursor
  show_console_cursor(true);

  return 0;
}