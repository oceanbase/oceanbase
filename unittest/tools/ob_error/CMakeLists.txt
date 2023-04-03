ob_unittest(test_ob_error)

target_link_libraries(test_ob_error PRIVATE oberror)
target_include_directories(test_ob_error
 PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/unittest ${CMAKE_SOURCE_DIR}/deps/oblib/unittest
 ${CMAKE_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/deps/oblib/src)