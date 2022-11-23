#function(join_unittest case)
#  ob_unittest(${ARGV})
#  target_sources(${case} PRIVATE ob_join_fake_table.h ob_join_fake_table.cpp)
#endfunction()
#
#join_unittest(ob_merge_join_test)
##join_unittest(ob_nested_loop_join_test)
#join_unittest(ob_hash_join_test)
#ob_unittest(farm_tmp_disabled_test_hash_join_dump test_hash_join_dump.cpp join_data_generator.h)
