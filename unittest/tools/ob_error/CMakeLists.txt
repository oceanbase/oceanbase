add_executable(test_ob_error test_ob_error.cpp)
add_test(test_ob_error test_ob_error)
target_link_libraries(test_ob_error PRIVATE oberror ${DEP_DIR}/lib/libgtest.a ${DEP_DIR}/lib/libgmock.a -lpthread)
target_include_directories(test_ob_error
 PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/unittest ${CMAKE_SOURCE_DIR}/deps/oblib/unittest ${DEP_DIR}/include
 ${CMAKE_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/deps/oblib/src)