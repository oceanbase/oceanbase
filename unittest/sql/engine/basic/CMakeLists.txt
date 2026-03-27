#ob_unittest(test_ra_row_store)
sql_unittest(test_ra_row_store_projector)
sql_unittest(test_chunk_row_store)
sql_unittest(test_chunk_datum_store)
sql_unittest(test_consistent_hashing_load_balancer)
sql_unittest(test_ob_limit_vec_op)
target_sources(test_ob_limit_vec_op PRIVATE
  utils/expr_maker.cpp
  utils/mock_child.cpp
  utils/test_op_base.cpp)
