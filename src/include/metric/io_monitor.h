//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2023/11/26.
// src/include/metric/io_monitor.h
//
//===-----------------------------------------------------

#pragma once

#include <iostream>
#include <thread>
#include <sys/types.h>
#include <sys/proc_info.h>
#include <filesystem>
#include <fstream>
#include <unistd.h>

#include <string>
#include <vector>
#include <chrono>
#include <algorithm>
#include <unordered_map>

#include <common/exception.h>


namespace qalsh {

    class IOMonitor {
    public:
        /**
         * delete the default constructor for memory-safety
         * */
        IOMonitor() = delete;

        explicit IOMonitor(int timeOutForSearch, int monitorDuration, int sleepDuration) : time_out_for_search_(
                timeOutForSearch), monitor_duration_(monitorDuration), sleep_duration_(sleepDuration),
                                                                                           is_monitoring_(false) {}

        /**
         * start a thread for monitoring
         * */
        void StartMonitoring(const std::string &processName);

        /**
         * stop the monitoring thread
         * */
        void StopMonitoring();

    private:
        /**
         * @brief get target process PID by its program name
         * @param processName : the program which process executing
         * @param timeOutSeconds : time for searching the target process
         * */
        static auto GetProcessIdByName(const std::string &processName, int timeOutSeconds) -> pid_t;

        /**
         * @brief get the IO data of target process
         * @param pid : pid of target process
         * */
        static auto GetProcessIOData(const int &pid) -> std::unordered_map<std::string, std::string>;

        /**
         * @brief format the transferred bytes in different metric
         * */
        static auto FormatBytes(uint64_t bytes) -> std::string;

        /**
         * @brief format the transferred blocks in different metric
         * */
        static auto FormatBlocks(uint64_t blocks) -> std::string;

        [[maybe_unused]] static auto GetFileDescriptorIOCount(const int &fd) -> size_t;

        /**
         * @brief thread execution function
         * */
        void Monitor(const std::string &processName) const;

        int time_out_for_search_ = 60;      // time out for searching the pid
        int monitor_duration_ = 1800;     // time for monitoring the target process
        int sleep_duration_ = 10;         // sleep time between each target
        bool is_monitoring_;              // flag bit
        std::thread monitor_thread_;      // monitor thread
    };

}  // namespace qalsh