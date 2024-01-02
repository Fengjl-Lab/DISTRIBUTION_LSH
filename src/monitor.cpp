//
// Created by chenjunhao on 2023/11/29.
//

#include <metric/io_monitor.h>

int main() {
    std::string processName = "test_io.o";
    QALSH::IOMonitor monitor(60);
    monitor.StartMonitoring(processName);

    std::chrono::seconds sleepDuration(100);
    std::this_thread::sleep_for(sleepDuration);

    monitor.StopMonitoring();
    return 0;
}