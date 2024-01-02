//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2023/11/26.
// src/metric/io_monitor.cpp
//
//===-----------------------------------------------------

#include <metric/io_monitor.h>

#ifdef __APPLE__

#include <libproc.h>

#endif

namespace qalsh {
    void IOMonitor::StartMonitoring(const std::string &processName) {
        if (!isMonitoring) {
            isMonitoring = true;
            monitorThread = std::thread(&IOMonitor::Monitor, this, processName);
        }
    }

    void IOMonitor::StopMonitoring() {
        if (isMonitoring) {
            isMonitoring = false;
            if (monitorThread.joinable()) {
                monitorThread.join();
            }
        }
    }


    auto IOMonitor::GetProcessIdByName(const std::string &processName, int timeOutSeconds) -> pid_t {
        std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
        int target_pid = -1;

        while (target_pid == -1) {
#ifdef __APPLE__
            std::vector<proc_bsdinfo> process_info_list;

            int proc_count = proc_listallpids(nullptr, 0); // Earn the number of processes
            auto pids = new pid_t[proc_count];
            proc_listallpids(pids, proc_count);

            for (int i = 0; i < proc_count; i++) {
                char path_buffer[PROC_PIDPATHINFO_MAXSIZE];
                struct proc_bsdinfo info{};
                pid_t pid = pids[i];
                if (proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &info, PROC_PIDTBSDINFO_SIZE) > 0) {
                    // Access the process path
                    if (proc_pidpath(pid, path_buffer, sizeof(path_buffer)) > 0) {
                        std::string process_path(path_buffer);
                        std::size_t found = process_path.rfind('/');

                        if (found != std::string::npos) {
                            std::string process_name_from_path = process_path.substr(found + 1);
                            // Match the process name
                            if (process_name_from_path == processName) {
                                target_pid = info.pbi_pid;
                                break;
                            }
                        }
                    }
                }
            }
            delete[]pids;
#elif defined(__LINUX__)
            std::filesystem::path procPath("/proc");
        std::filesystem::directory_iterator endIterator;
        for (const auto& entry : std::filesystem::directory_iterator(procPath)) {
            if (entry.is_directory()) {
                std::string fileName = entry.path().filename().string();

                if (std::all_of(fileName.begin(), fileName.end(), isdigit)) {
                    std::filesystem::path cmdlinePath = entry.path() / "cmdline";
                    if (std::filesystem::exists(cmdlinePath)) {
                        std::ifstream cmdlineFile(cmdlinePath);
                        std::string cmdline;
                        std::getline(cmdlineFile, cmdline);

                        // extract the file name
                        std::size_t pos = cmdline.find_last_of('/');
                        std::string currentProcessName = cmdline.substr(pos + 1);

                        if (currentProcessName == processName) {
                            targetPid = std::stoi(fileName);
                            break;
                        }
                    }
                }
            }
        }

#endif
            // Test if time out or not
            std::chrono::steady_clock::time_point current_time = std::chrono::steady_clock::now();
            std::chrono::seconds elapsed_time = std::chrono::duration_cast<std::chrono::seconds>(
                    current_time - start_time);
            if (elapsed_time.count() >= timeOutSeconds) {
                // fail
                return -1;
            }
            // Sleep for 1 second
            std::chrono::seconds sleep_duration(1);
            std::this_thread::sleep_for(sleep_duration);
        }
        // success
        return target_pid;
    }

    auto IOMonitor::GetProcessIOData(const int &pid) -> std::unordered_map<std::string, std::string> {
        std::unordered_map<std::string, std::string> io_data;
#ifdef __APPLE__
        uint64_t total_io_count = 0;
        int buffer_size;
        try {
            buffer_size = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, nullptr, 0);
            if (buffer_size <= 0) {
                throw Exception(ExceptionType::EXECUTION, "Target process fd list not found", false);
            }
        } catch (Exception &e) {
            std::cout << Exception::ExceptionTypeToString(e.GetType()) << " " << e.what() << '\n';
            exit(1);
        }

        // Create buffer
        auto buffer = new char[buffer_size];
        try {
            if (proc_pidinfo(pid, PROC_PIDLISTFDS, 0, buffer, buffer_size) <= 0) {
                throw Exception(ExceptionType::EXECUTION, "Access target process fd list not found", false);
            }
        } catch (Exception &e) {
            std::cout << Exception::ExceptionTypeToString(e.GetType()) << " " << e.what() << '\n';
            exit(1);
        }
        std::vector<struct proc_fdinfo> fds_info(reinterpret_cast<struct proc_fdinfo *>(buffer),
                                                 reinterpret_cast<struct proc_fdinfo *>(buffer + buffer_size));
        for (auto fd: fds_info) {
            // Exclude the stdin, stdout and stderr
            if (fd.proc_fdtype == PROX_FDTYPE_VNODE &&
                (fd.proc_fd == STDIN_FILENO || fd.proc_fd == STDOUT_FILENO || fd.proc_fd == STDERR_FILENO)) {
                continue;
            }

            total_io_count += lseek(fd.proc_fd, 0, SEEK_END);
        }

        delete[]buffer;
        io_data["Total Count"] = std::to_string(total_io_count);

#elif defined(__LINUX__)
        std::ifstream file("/proc/" + std::to_string(pid) + "/io");

    if (file.is_open()) {
        std::string line;
        while (std::getline(file, line)) {
            size_t delimiterPos = line.find(':');
            if (delimiterPos != std::string::npos) {
                std::string key = line.substr(0, delimiterPos);
                std::string value = line.substr(delimiterPos + 1);
                ioData[key] = value;
            }
        }
    }
#endif
        return io_data;
    }

    auto IOMonitor::FormatBytes(uint64_t bytes) -> std::string {
        const std::string suffixes[] = {"B", "KB", "MB", "GB", "TB"};
        int suffix_index = 0;
        auto size = static_cast<double>(bytes);
        while (size >= 1024 && suffix_index < sizeof(suffixes) / sizeof(suffixes[0])) {
            size /= 1024;
            suffix_index++;
        }

        char buffer[20];
        snprintf(buffer, sizeof(buffer), "%.2f %s", size, suffixes[suffix_index].c_str());
        return buffer;
    }

    auto IOMonitor::FormatBlocks(uint64_t blocks) -> std::string {
        return std::to_string(blocks) + " blocks";
    }

    [[maybe_unused]] auto IOMonitor::GetFileDescriptorIOCount(const int &fd) -> size_t {
        struct stat stat_buf{};
        if (fstat(fd, &stat_buf) == 0) {
            return stat_buf.st_size;
        }

        return 0;
    }


    void IOMonitor::Monitor(const std::string &processName) const {
        pid_t target_id = GetProcessIdByName(processName, timeOutForSearch);
        if (target_id == -1) {
            exit(1);
        }
        std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();

        while (true) {
            std::unordered_map<std::string, std::string> io_data = GetProcessIOData(target_id);
            int sector_size = getpagesize();
#ifdef __APPLE__
            if (io_data.count("Total Count") != 0U) {
                uint64_t bytes_transferred = std::stoull(io_data["Total Count"]);
                std::cout << "I/O info for PID:" << target_id << " (program name:" << processName << ")\n";
                std::cout << "Blocks transferred: " << FormatBlocks(bytes_transferred / sector_size) << '\n';
                std::cout << "Bytes transferred: " << FormatBytes(bytes_transferred) << '\n';
            }
#elif defined(__LINUX__)
            if (ioData.count("rchar") && ioData.count("wchar") && ioData.count("blkio.sectors")) {
            int bytesRead = std::stoi(ioData["rchar"]);
            int byteWritten = std::stoi(ioData["wchar"]);
            int blockTransferred = (bytesRead + byteWritten) / sectorSize;

            std::cout << "I/O info for PID:" << targetId << "(program name:" << processName << ")\n";
            std::cout << "Blocks transferred: " << FormatBlocks(blockTransferred) << std::endl;
            std::cout << "Bytes transferred: " << FormatBytes(blockTransferred * sectorSize) << std::endl;
        }
#endif

            std::chrono::steady_clock::time_point current_time = std::chrono::steady_clock::now();
            std::chrono::seconds elapsed_time = std::chrono::duration_cast<std::chrono::seconds>(
                    current_time - start_time);
            if (elapsed_time.count() > monitorDuration) { break;
}

            // Sleep
            std::chrono::seconds thread_sleep_duration(this->sleepDuration);
            std::this_thread::sleep_for(thread_sleep_duration);
        }
    }

} // namespace QALSH
