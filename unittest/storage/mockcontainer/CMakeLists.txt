add_library(mockcontainer SHARED
  ob_restore_schema.h               ob_restore_schema.cpp
  mock_ob_schema_service.h          mock_ob_schema_service.cpp
  mock_ob_server.h                  mock_ob_server.cpp
  mock_ob_iterator.h                mock_ob_iterator.cpp
  mock_ob_end_trans_callback.h
  )
target_link_libraries(mockcontainer PUBLIC oceanbase)
