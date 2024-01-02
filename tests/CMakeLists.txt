include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

add_executable(DistributionTests DistributionTests.cpp)

target_link_libraries(DistributionTests QALSH_static)
target_link_libraries(DistributionTests gtest gtest_main)
target_link_libraries(DistributionTests fmt)