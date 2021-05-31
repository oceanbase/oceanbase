add_library(oblib_testbase INTERFACE)
target_include_directories(oblib_testbase INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(oblib_testbase INTERFACE -lgmock -lgtest)

function(oblib_addtest mainfile)
  get_filename_component(testname ${mainfile} NAME_WE)
  add_executable(${testname} ${ARGV})
  target_link_libraries(${testname} PRIVATE oblib oblib_testbase -static-libgcc -static-libstdc++)
endfunction()

add_subdirectory(lib)
add_subdirectory(rpc)
add_subdirectory(common)
