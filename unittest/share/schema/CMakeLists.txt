function(schema_unittest case)
  ob_unittest(${ARGV})
  target_sources(${case}
    PRIVATE
    mock_schema_service.h
    schema_test_utils.h
    mock_multi_version_schema_service_for_fallback.h)
endfunction()

#schema_unittest(test_schema_service_rpc_proxy)
#schema_unittest(test_schema_struct)
#schema_unittest(test_table_schema)
#schema_unittest(test_part_mg)
#schema_unittest(test_table_iter)
#schema_unittest(test_server_schema_service)
#schema_unittest(test_ddl)
#schema_unittest(test_schema_getter_guard)
#schema_unittest(test_schema_utils)
#schema_unittest(test_schema_cache)
#schema_unittest(test_schema_mgr)
#schema_unittest(test_outline_mgr)
#schema_unittest(test_priv_mgr)
#schema_unittest(test_schema_mgr_cache)
#schema_unittest(test_schema_mem_mgr)
#schema_unittest(test_fallback_schema_mgr)
#schema_unittest(test_outline_info)
schema_unittest(test_table_dml_param)
