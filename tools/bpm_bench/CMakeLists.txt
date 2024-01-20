set(BPM_BENCH_SOURCES bpm_bench.cpp)
add_executable(bpm-bench ${BPM_BENCH_SOURCES} "${PROJECT_SOURCE_DIR}/tools/backtrace.cpp")
add_backward(bpm-bench)

target_link_libraries(bpm-bench distribution_lsh)
set_target_properties(bpm-bench PROPERTIES OUTPUT_NAME distribution_lsh-bpm-bench)