add_library(sql_ut_base OBJECT
  test_sql_utils.h
  test_sql_utils.cpp
  optimizer/ob_mock_part_mgr.h
  optimizer/test_optimizer_utils.h
  optimizer/test_optimizer_utils.cpp
  )
target_link_libraries(sql_ut_base PUBLIC mockcontainer)

function(sql_unittest case)
  ob_unittest(${ARGV})
  target_link_libraries(${case} PRIVATE sql_ut_base)
endfunction()

add_subdirectory(optimizer)
add_subdirectory(common)
add_subdirectory(parser)
add_subdirectory(resolver)
add_subdirectory(code_generator)
add_subdirectory(plan_cache)
add_subdirectory(engine)
add_subdirectory(rewrite)
add_subdirectory(session)
add_subdirectory(module)
add_subdirectory(monitor)
add_subdirectory(dtl)
