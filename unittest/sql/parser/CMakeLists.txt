ob_unittest(test_parser_perf)
sql_unittest(test_parser)
sql_unittest(test_multi_parser)

add_executable(test_sql_fast_parser test_sql_fast_parser.cpp)
target_link_libraries(test_sql_fast_parser
  PRIVATE
  gtest
  gmock
  ob_sql_proxy_parser_static
  -static-libstdc++
)
disable_pch(test_sql_fast_parser)
target_include_directories(test_sql_fast_parser
  PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/ob_unittest
)
