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