function(add_hybrid_search_test case)
  ob_unittest(${case})
  target_include_directories(${case} PRIVATE
    ${CMAKE_SOURCE_DIR}/mittest/simple_server
  )
  target_link_libraries(${case} PRIVATE
    observer_test
    mit_env
  )
endfunction()

add_hybrid_search_test(test_scalar_query)
add_hybrid_search_test(test_boolean_query)
