function(rpc_server_test case)
  if(ARGC EQUAL 1)
    add_executable(${case} ${case}.cpp)
  else()
    add_executable(${ARGV})
  endif()

  target_include_directories(${case} PRIVATE)

  target_link_libraries(${case}
    PRIVATE
    -Wl,--start-group
    oceanbase_static
    ob_sql_static
    ob_storage_static
    ${ob_close_modules_static_name}
    -Wl,--end-group
    ${STATIC_LIBGCC_FLAGS}
    ${STATIC_LIBSTDCXX_FLAGS})
endfunction()

rpc_server_test(easy_server_basic)
rpc_server_test(easy_server_worker)

