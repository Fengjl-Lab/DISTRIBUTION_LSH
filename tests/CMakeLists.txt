include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

add_executable(DistributionTests DistributionTests.cpp)

target_link_libraries(DistributionTests GenDistribution)
target_link_libraries(DistributionTests gtest gtest_main)