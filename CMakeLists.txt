cmake_minimum_required(VERSION 3.20)
project(QALSH_refactor_version)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

#set(CMAKE_C_COMPILER "/usr/local/opt/llvm/bin/clang")
#set(CMAKE_CXX_COMPILER "/usr/local/opt/llvm/bin/clang++")
set(CMAKE_C_COMPILER "/usr/local/bin/gcc-13")
set(CMAKE_CXX_COMPILER "/usr/local/bin/g++-13")

# Output directory.
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# add new flag
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fopenmp")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")

# Includes.
set(QALSH_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
include_directories(${QALSH_SRC_INCLUDE_DIR})

add_subdirectory(src)
add_subdirectory(tests)
add_subdirectory(third_party)
