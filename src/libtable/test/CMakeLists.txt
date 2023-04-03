add_executable(test_table_api EXCLUDE_FROM_ALL ob_batch_execute_test.cpp)
target_link_libraries(test_table_api PRIVATE obtable gtest)
target_include_directories(test_table_api PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
