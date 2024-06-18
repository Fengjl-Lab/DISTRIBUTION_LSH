cmake_minimum_required(VERSION 3.20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(DISTRIBUTION_LSH
        VERSION 2024.1
        DESCRIPTION "distribution_lsh c-ANNs algorithm"
        LANGUAGES C CXX
)

if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to `Debug` as none was specified.")
    set(CMAKE_BUILD_TYPE "Debug")
endif ()

if (EMSCRIPTEN)
    add_compile_definitions(-fexceptions)
    add_link_options(-fexceptions)
endif ()

# Compiler flags.
# Debug flags.
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -Wextra -Werror")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wno-unused-parameter -Wno-attributes") # TODO: remove
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb -fsanitize=address -fno-omit-frame-pointer -fno-optimize-sibling-calls -fopenmp")

# Release flags.
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -fopenmp")

# People keep running CMake in the wrong folder, completely nuking their project or creating weird bugs.
# This checks if you're running CMake from a folder that already has CMakeLists.txt.
# Importantly, this catches the common case of running it from the root directory.
file(TO_CMAKE_PATH "${PROJECT_BINARY_DIR}/CMakeLists.txt" PATH_TO_CMAKELISTS_TXT)

if(EXISTS "${PATH_TO_CMAKELISTS_TXT}")
    message(FATAL_ERROR "Run CMake from a build subdirectory! \"mkdir build ; cd build ; cmake ..\" \
    Some junk files were created in this folder (CMakeCache.txt, CMakeFiles); you should delete those.")
endif()

# #############################################
# DEPENDENCIES
# #############################################

# CTest
enable_testing()


# Output directory.
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Includes.
set(DISTRIBUTION_LSH_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
set(DISTRIBUTION_LSH_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test/include)
set(DISTRIBUTION_LSH_THIRD_PARTY_INCLUDE_DIR
        ${PROJECT_SOURCE_DIR}/third_party
        ${PROJECT_SOURCE_DIR}/third_party/fmt/include
        ${PROJECT_SOURCE_DIR}/third_party/argparse/include
        ${PROJECT_SOURCE_DIR}/third_party/cpp_random_distributions
        ${PROJECT_SOURCE_DIR}/third_party/backward-cpp/include
        ${PROJECT_SOURCE_DIR}/third_party/indicators/include
        ${PROJECT_SOURCE_DIR}/third_party/tabulate/include
        ${PROJECT_SOURCE_DIR}/third_party/highway/hwy
        ${PROJECT_SOURCE_DIR}/third_party/libsimdpp
        ${PROJECT_SOURCE_DIR}/third_party/magic_enum/include
)

include_directories(${DISTRIBUTION_LSH_SRC_INCLUDE_DIR} ${DISTRIBUTION_LSH_TEST_INCLUDE_DIR} ${DISTRIBUTION_LSH_THIRD_PARTY_INCLUDE_DIR})
include_directories(BEFORE src) # This is needed for gtest.

add_subdirectory(third_party)
add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(tools)

# debug log level
add_definitions(LOG_LEVEL=200)
