function(rewrite_unittest case)
  sql_unittest(${ARGV})
  target_sources(${case} PRIVATE test_trans_utils.h test_trans_utils.cpp)
endfunction()

rewrite_unittest(test_query_range)
rewrite_unittest(test_transformer)
