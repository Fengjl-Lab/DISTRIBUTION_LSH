set(INDICATOR_PROCESS_BARS_SAMPLE1 indicator_process_bar_sample1.cpp)
add_executable(indicator-process-bar-sample1 ${INDICATOR_PROCESS_BARS_SAMPLE1} "${PROJECT_SOURCE_DIR}/tools/backtrace.cpp")
add_backward(indicator-process-bar-sample1)

target_link_libraries(indicator-process-bar-sample1 indicators)
set_target_properties(indicator-process-bar-sample1 PROPERTIES OUTPUT_NAME distribution_lsh-indicator-process-bar-sample1)

set(INDICATOR_PROCESS_BARS_SAMPLE2 indicator_process_bar_sample2.cpp)
add_executable(indicator-process-bar-sample2 ${INDICATOR_PROCESS_BARS_SAMPLE2} "${PROJECT_SOURCE_DIR}/tools/backtrace.cpp")
add_backward(indicator-process-bar-sample2)

target_link_libraries(indicator-process-bar-sample2 indicators)
set_target_properties(indicator-process-bar-sample2 PROPERTIES OUTPUT_NAME distribution_lsh-indicator-process-bar-sample2)

set(INDICATOR_PROCESS_BARS_SAMPLE3 indicator_process_bar_sample3.cpp)
add_executable(indicator-process-bar-sample3 ${INDICATOR_PROCESS_BARS_SAMPLE3} "${PROJECT_SOURCE_DIR}/tools/backtrace.cpp")
add_backward(indicator-process-bar-sample3)

target_link_libraries(indicator-process-bar-sample3 indicators)
set_target_properties(indicator-process-bar-sample3 PROPERTIES OUTPUT_NAME distribution_lsh-indicator-process-bar-sample3)

set(INDICATOR_PROCESS_BARS_MULTIPROCESS indicator_process_bar_multi_process.cpp)
add_executable(indicator-process-bar-multiprocess ${INDICATOR_PROCESS_BARS_MULTIPROCESS} "${PROJECT_SOURCE_DIR}/tools/backtrace.cpp")
add_backward(indicator-process-bar-multiprocess)

target_link_libraries(indicator-process-bar-multiprocess indicators)
set_target_properties(indicator-process-bar-multiprocess PROPERTIES OUTPUT_NAME distribution_lsh-indicator-process-bar-multiprocess)

set(TABULATE_SAMPLE1 tabulate_sample1.cpp)
add_executable(tabulate_sample1 ${TABULATE_SAMPLE1} "${PROJECT_SOURCE_DIR}/tools/backtrace.cpp")
add_backward(tabulate_sample1)

target_link_libraries(tabulate_sample1 tabulate)
set_target_properties(tabulate_sample1 PROPERTIES OUTPUT_NAME distribution_lsh-tabulate-sample1)