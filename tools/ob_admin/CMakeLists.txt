add_executable(ob_admin 
  clog_tool/cmd_args_parser.h
  clog_tool/ob_admin_clog_v2_executor.cpp
  clog_tool/ob_admin_clog_v2_executor.h
  clog_tool/ob_func_utils.cpp
  clog_tool/ob_func_utils.h
  clog_tool/ob_ilog_entry_parser.cpp
  clog_tool/ob_ilog_entry_parser.h
  clog_tool/ob_log_entry_filter.cpp
  clog_tool/ob_log_entry_filter.h
  clog_tool/ob_log_entry_parser.cpp
  clog_tool/ob_log_entry_parser.h
  usec_tool/ob_admin_usec_executor.cpp
  usec_tool/ob_admin_usec_executor.h
  ob_admin_executor.h
  ob_admin_executor.cpp
  main.cpp)

set(LGPL_DEPS "-L${DEP_DIR}/lib/mariadb -lmariadb -laio")
if (OB_STATIC_LINK_LGPL_DEPS)
   set(LGPL_DEPS "-L${DEP_DIR}/lib/mariadb -l:libmariadbclient.a -l:libaio.a")
endif()

target_link_libraries(ob_admin
  PRIVATE
  oceanbase_static
  -static-libgcc
  -static-libstdc++
  ${LGPL_DEPS}
  )


target_include_directories(ob_admin
  PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR})