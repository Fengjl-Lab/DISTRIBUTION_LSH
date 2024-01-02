//
// Created by chenjunhao on 2023/11/29.
//

#include <metric/io_monitor.h>

auto main() -> int {
    std::string process_name = "gen_distribution";
    qalsh::IOMonitor monitor(60, 60, 5);
    monitor.StartMonitoring(process_name);

    std::chrono::seconds sleep_duration(120);
    std::this_thread::sleep_for(sleep_duration);

    monitor.StopMonitoring();
    return 0;
}