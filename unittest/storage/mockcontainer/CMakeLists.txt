add_library(mockcontainer SHARED
  ob_restore_schema.h               ob_restore_schema.cpp
  mock_ob_location_cache.h          mock_ob_location_cache.cpp
  mock_ob_schema_service.h          mock_ob_schema_service.cpp
  mock_ob_server.h                  mock_ob_server.cpp
  mock_ob_partition_service.h
  mock_misc_for_ps_test.h
  mock_ob_partition.h               mock_ob_partition_storage.h
  mock_ob_iterator.h                mock_ob_iterator.cpp
  ob_dml_check.h                    ob_dml_check.cpp
  mock_ob_trans_service.h           mock_ob_end_trans_callback.h
  )
target_link_libraries(mockcontainer PUBLIC oceanbase)
disable_pch(mockcontainer)
