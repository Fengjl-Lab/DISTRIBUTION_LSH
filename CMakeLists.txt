cmake_minimum_required(VERSION 3.20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(DISTRIBUTION_LSH_refactor_version
        VERSION 2024.1
        DESCRIPTION "distribution_lsh c-ANNs algorithm"
        LANGUAGES C CXX
)

if (NOT CMAKE_BUILD_TYPE  AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to `Debug` as none was specified.")
    set(CMAKE_BUILD_TYPE "Debug")
endif ()

if (EMSCRIPTEN)
    add_compile_definitions(-fexceptions)
    add_link_options(-fexceptions)
endif ()

# People keep running CMake in the wrong folder, completely nuking their project or creating weird bugs.
# This checks if you're running CMake from a folder that already has CMakeLists.txt.
# Importantly, this catches the common case of running it from the root directory.
file(TO_CMAKE_PATH "${PROJECT_BINARY_DIR}/CMakeLists.txt" PATH_TO_CMAKELISTS_TXT)

if(EXISTS "${PATH_TO_CMAKELISTS_TXT}")
    message(FATAL_ERROR "Run CMake from a build subdirectory! \"mkdir build ; cd build ; cmake ..\" \
    Some junk files were created in this folder (CMakeCache.txt, CMakeFiles); you should delete those.")
endif()

#set(CMAKE_C_COMPILER "/usr/local/opt/llvm/bin/clang")
#set(CMAKE_CXX_COMPILER "/usr/local/opt/llvm/bin/clang++")
set(CMAKE_C_COMPILER "/usr/local/bin/gcc-13")
set(CMAKE_CXX_COMPILER "/usr/local/bin/g++-13")

# #############################################
# DEPENDENCIES
# #############################################

# CTest
enable_testing()


# Output directory.
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# add new flag
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fopenmp")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")

# Includes.
set(DISTRIBUTION_LSH_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
set(DISTRIBUTION_LSH_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test/include)
set(DISTRIBUTION_LSH_THIRD_PARTY_INCLUDE_DIR
        ${PROJECT_SOURCE_DIR}/third_party
        ${PROJECT_SOURCE_DIR}/third_party/fmt/include
        # ${PROJECT_SOURCE_DIR}/third_party/libpg_query/include
        ${PROJECT_SOURCE_DIR}/third_party/argparse/include
        ${PROJECT_SOURCE_DIR}/third_party/cpp_random_distributions
        ${PROJECT_SOURCE_DIR}/third_party/backward-cpp/include
)

include_directories(${DISTRIBUTION_LSH_SRC_INCLUDE_DIR} ${DISTRIBUTION_LSH_TEST_INCLUDE_DIR} ${DISTRIBUTION_LSH_THIRD_PARTY_INCLUDE_DIR})
include_directories(BEFORE src) # This is needed for gtest.

add_subdirectory(third_party)
add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(tools)
