#ob_unittest(test_postfix_expression)
#sql_unittest(bit_type_test)
#sql_unittest(ob_expr_type_to_str_test)
#sql_unittest(ob_expr_equal_test)
#sql_unittest(ob_expr_res_type_map_test)
#sql_unittest(ob_expr_operator_factory_test)
sql_unittest(ob_geo_expr_utils_test)
sql_unittest(test_gis_dispatcher test_gis_dispatcher.cpp ob_geo_func_testx.cpp ob_geo_func_testy.cpp)
sql_unittest(test_expr_relation_map)

# engine_expr_test_lrpad_SOURCES=engine/expr/ob_expr_lrpad_test.cpp
#ob_postfix_expression_test_SOURCES = ob_postfix_expression_test.cpp
#my_allocator_SOURCES = my_allocator.cpp
#ob_expr_str_to_date_test_SOURCES=engine/expr/ob_expr_str_to_date_test.cpp
#ob_expr_date_format_test_SOURCES=engine/expr/ob_expr_date_format_test.cpp
#ob_expr_date_add_test_SOURCES=engine/expr/ob_expr_date_add_test.cpp
#ob_expr_date_sub_test_SOURCES=engine/expr/ob_expr_date_sub_test.cpp
#engine_expr_ob_expr_extract_test_SOURCES=engine/expr/ob_expr_extract_test.cpp ${pub_source}
#ob_expr_bit_neg_test_SOURCES=engine/expr/ob_expr_bit_neg_test.cpp ${pub_source}
#ob_expr_nvl_test_SOURCES=engine/expr/ob_expr_nvl_test.cpp
#engine_expr_ob_expr_like_test_SOURCES=engine/expr/ob_expr_like_test.cpp ${pub_source}
#ob_expr_regexp_test_SOURCES=engine/expr/ob_expr_regexp_test.cpp
#ob_expr_trim_test_SOURCES=engine/expr/ob_expr_trim_test.cpp
#engine_expr_ob_expr_substr_test_SOURCES=engine/expr/ob_expr_substr_test.cpp ${pub_source}
#engine_expr_ob_expr_replace_test_SOURCES=engine/expr/ob_expr_replace_test.cpp ${pub_source}
#ob_expr_substring_index_test_SOURCES=engine/expr/ob_expr_substring_index_test.cpp
#ob_expr_md5_test_SOURCES=engine/expr/ob_expr_md5_test.cpp
#engine_expr_ob_expr_insert_test_SOURCES=engine/expr/ob_expr_insert_test.cpp ${pub_source}
#engine_expr_ob_expr_length_test_SOURCES=engine/expr/ob_expr_length_test.cpp ${pub_source}
#engine_expr_ob_expr_hex_test_SOURCES=engine/expr/ob_expr_hex_test.cpp ${pub_source}
#ob_expr_sign_test_SOURCES=engine/expr/ob_expr_sign_test.cpp
#ob_expr_unhex_test_SOURCES=engine/expr/ob_expr_unhex_test.cpp
#engine_expr_ob_expr_concat_test_SOURCES=engine/expr/ob_expr_concat_test.cpp ${pub_source}
#ob_expr_ip2int_test_SOURCES=engine/expr/ob_expr_ip2int_test.cpp
#ob_expr_int2ip_test_SOURCES=engine/expr/ob_expr_int2ip_test.cpp
#ob_expr_upper_test_SOURCES=engine/expr/ob_expr_upper_test.cpp
#engine_expr_ob_expr_lower_test_SOURCES=engine/expr/ob_expr_lower_test.cpp ${pub_source}
#ob_expr_case_test_SOURCES=engine/expr/ob_expr_case_test.cpp
#ob_expr_least_test_SOURCES=engine/expr/ob_expr_least_test.cpp
#ob_expr_greatest_test_SOURCES=engine/expr/ob_expr_greatest_test.cpp
#ob_expr_coalesce_test_SOURCES=engine/expr/ob_expr_coalesce_test.cpp
#engine_expr_ob_expr_greater_than_test_SOURCES=engine/expr/ob_expr_greater_than_test.cpp ${pub_source}
#engine_expr_ob_expr_equal_test_SOURCES=engine/expr/ob_expr_equal_test.cpp ${pub_source}
#engine_expr_ob_expr_less_equal_test_SOURCES=engine/expr/ob_expr_less_equal_test.cpp ${pub_source}
#engine_expr_ob_expr_less_than_test_SOURCES=engine/expr/ob_expr_less_than_test.cpp ${pub_source}
#engine_expr_ob_expr_greater_equal_test_SOURCES=engine/expr/ob_expr_greater_equal_test.cpp ${pub_source}
#engine_expr_ob_expr_not_equal_test_SOURCES=engine/expr/ob_expr_not_equal_test.cpp ${pub_source}
#engine_expr_ob_expr_add_test_SOURCES=engine/expr/ob_expr_add_test.cpp ${pub_source}
#engine_expr_ob_expr_minus_test_SOURCES=engine/expr/ob_expr_minus_test.cpp ${pub_source}
#engine_expr_ob_expr_mul_test_SOURCES=engine/expr/ob_expr_mul_test.cpp ${pub_source}
#engine_expr_ob_expr_div_test_SOURCES=engine/expr/ob_expr_div_test.cpp ${pub_source}
#ob_expr_add_test_SOURCES=engine/expr/ob_expr_add_test.cpp
#ob_expr_minus_test_SOURCES=engine/expr/ob_expr_minus_test.cpp
#ob_expr_mul_test_SOURCES=engine/expr/ob_expr_mul_test.cpp
#ob_expr_div_test_SOURCES=engine/expr/ob_expr_div_test.cpp
#engine_expr_ob_expr_mod_test_SOURCES=engine/expr/ob_expr_mod_test.cpp ${pub_source}
#ob_expr_neg_test_SOURCES=engine/expr/ob_expr_neg_test.cpp
#ob_expr_abs_test_SOURCES=engine/expr/ob_expr_abs_test.cpp
#ob_expr_not_test_SOURCES=engine/expr/ob_expr_not_test.cpp
#engine_expr_ob_expr_int_div_test_SOURCES=engine/expr/ob_expr_int_div_test.cpp ${pub_source}
#ob_expr_and_test_SOURCES=engine/expr/ob_expr_and_test.cpp
#engine_expr_ob_expr_is_test_SOURCES=engine/expr/ob_expr_is_test.cpp ${pub_source}
#engine_expr_ob_expr_is_not_test_SOURCES=engine/expr/ob_expr_is_not_test.cpp ${pub_source}
#ob_expr_cast_test_SOURCES=engine/expr/ob_expr_cast_test.cpp
#ob_expr_between_test_SOURCES=engine/expr/ob_expr_between_test.cpp
#ob_expr_not_between_test_SOURCES=engine/expr/ob_expr_not_between_test.cpp
#engine_expr_ob_expr_in_test_SOURCES=engine/expr/ob_expr_in_test.cpp ${pub_source}
#ob_expr_repeat_test_SOURCES=engine/expr/ob_expr_repeat_test.cpp
#ob_expr_concat_ws_test_SOURCES=engine/expr/ob_expr_concat_ws_test.cpp
#engine_expr_ob_expr_instr_test_SOURCES=engine/expr/ob_expr_instr_test.cpp ${pub_source}
#engine_expr_ob_expr_locate_test_SOURCES=engine/expr/ob_expr_locate_test.cpp ${pub_source}
#engine_expr_ob_expr_conv_test_SOURCES=engine/expr/ob_expr_conv_test.cpp ${pub_source}
#engine_expr_ob_expr_quote_test_SOURCES=engine/expr/ob_expr_quote_test.cpp ${pub_source}
#engine_expr_ob_expr_arithmatic_test_SOURCES=engine/expr/ob_expr_arithmatic_test.cpp ${pub_source}
#engine_expr_ob_expr_compare_test_SOURCES=engine/expr/ob_expr_compare_test.cpp ${pub_source}
#engine_expr_ob_expr_lnnvl_test_SOURCES=engine/expr/ob_expr_lnnvl_test.cpp ${pub_source}
#engine_expr_ob_expr_reverse_test_SOURCES=engine/expr/ob_expr_reverse_test.cpp ${pub_source}
#engine_expr_ob_expr_right_test_SOURCES=engine/expr/ob_expr_right_test.cpp ${pub_source}
#engine_expr_ob_expr_rpad_test_SOURCES=engine/expr/ob_expr_rpad_test.cpp ${pub_source}
#engine_expr_test_postfix_expression_SOURCES=engine/expr/test_postfix_expression.cpp
