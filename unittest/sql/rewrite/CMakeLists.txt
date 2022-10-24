function(rewrite_unittest case)
  sql_unittest(${ARGV})
  target_sources(${case} PRIVATE)
endfunction()

rewrite_unittest(test_query_range)
# Fix me: disable for now
#rewrite_unittest(test_transformer)
