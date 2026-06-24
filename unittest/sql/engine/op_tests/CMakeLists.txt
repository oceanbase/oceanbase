# Operator Unit Test Framework
# This directory contains the operator unit test framework and tests for various operators.

# Copy test_data directory to build directory at configure time
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
if(EXISTS ${CMAKE_CURRENT_BINARY_DIR}/test_data)
  file(REMOVE_RECURSE ${CMAKE_CURRENT_BINARY_DIR}/test_data)
endif()
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test_data
     DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

# Also copy to build root for tests run from that directory
if(EXISTS ${CMAKE_BINARY_DIR}/test_data)
  file(REMOVE_RECURSE ${CMAKE_BINARY_DIR}/test_data)
endif()
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test_data
     DESTINATION ${CMAKE_BINARY_DIR})

# Phase 1: Core infrastructure tests
sql_unittest(test_material_op
  test_material_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# SUM(decint)->int128 NEON/builtin SIMD fast-path tests
sql_unittest(test_sum_int128_neon_op
  test_sum_int128_neon_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Phase 2: OpTestEngine unit tests
sql_unittest(test_op_test_engine
  test_op_test_engine.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Phase 2: Limit operator tests
sql_unittest(test_limit_op
  test_limit_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Phase 2: Sort operator tests
sql_unittest(test_sort_op
  test_sort_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Phase 2: Scalar Aggregate operator tests
sql_unittest(test_scalar_agg_op
  test_scalar_agg_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Phase 3: Window Function operator tests
sql_unittest(test_window_function_op
  test_window_function_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Phase 4: MergeGroupBy operator tests
sql_unittest(test_merge_groupby_op
  test_merge_groupby_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# MergeGroupBy: rollup + distinct aggregate + HAVING IS UNKNOWN (captured
# from production SQL via tracing(21,22) hint).
sql_unittest(test_merge_groupby_rollup_distinct_op
  test_merge_groupby_rollup_distinct_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Phase 4: HashGroupBy operator tests
sql_unittest(test_hash_groupby_op
  test_hash_groupby_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Phase 5: Expand operator tests
sql_unittest(test_expand_op
  test_expand_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Phase 6: Merge Join operator tests
sql_unittest(test_merge_join_op
  test_merge_join_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Phase 7: Type coverage tests
sql_unittest(test_type_coverage
  test_type_coverage.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Phase 8: Hash Join operator tests
sql_unittest(test_hash_join_op
  test_hash_join_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Phase 8: Nested Loop Join operator tests
sql_unittest(test_nested_loop_join_op
  test_nested_loop_join_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# avgWeighted 表达式单测
sql_unittest(test_avg_weighted_op
  test_avg_weighted_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Phase 8: Window Function datahub tests
# NOTE: Disabled due to `#define private public` macro conflict in ob_op_test_datahub.h
# which pollutes the namespace when included alongside other OceanBase headers.
# To re-enable: isolate datahub includes in a separate compilation unit.
# sql_unittest(test_window_function_datahub
#   test_window_function_datahub.cpp
#   ob_op_test_engine.cpp
#   ob_op_test_data_source.cpp
# )


# STRING_AGG aggregate function tests
sql_unittest(test_string_agg_op
  test_string_agg_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)
# Phase 3: Partition Sort operator tests
sql_unittest(test_partition_sort_op
  test_partition_sort_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Phase 9: Framework extensions tests
sql_unittest(test_framework_extensions
  test_framework_extensions.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# CASE WHEN expression tests
sql_unittest(test_case_expr
  test_case_expr.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# CASE WHEN coverage: divisor-zero lazy-eval regression + optimization paths (large data)
sql_unittest(test_case_expr_coverage_op
  test_case_expr_coverage_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Expression vectorization 1.0 / 2.0 dual-format tests
sql_unittest(test_expr_dual_format
  test_expr_dual_format.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# date_format() buffer overflow regression tests
sql_unittest(test_date_format_expr
  test_date_format_expr.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)
# Phase 10: Hash Distinct operator tests
sql_unittest(test_hash_distinct_op
  test_hash_distinct_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Decimal encoding bug regression test
sql_unittest(test_decimal_encoding_bug
  test_decimal_encoding_bug.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Phase 10: Merge Distinct operator tests
sql_unittest(test_merge_distinct_op
  test_merge_distinct_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# DATE_FORMAT fast path optimization tests
sql_unittest(test_date_format_fast_path
  test_date_format_fast_path.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# Decimal int cast to int tests
sql_unittest(test_decimalint_cast_to_int
  test_decimalint_cast_to_int.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)

# CASE expr: else_is_uniform_const dispatch + cmp_ws fast string comparison
sql_unittest(test_case_expr_op
  test_case_expr_op.cpp
  ob_op_test_engine.cpp
  ob_op_test_data_source.cpp
)