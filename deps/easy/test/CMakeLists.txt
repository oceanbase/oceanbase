set(EASY_TEST_ROOT ${CMAKE_CURRENT_SOURCE_DIR})

function(easy_test case)
  get_property(EASY_INCLUDE_DIRS GLOBAL PROPERTY "EASY_INCLUDE_DIRS")
  add_library(${case}_base OBJECT ${ARGN})

  target_include_directories(
    ${case}_base PUBLIC
    ${EASY_TEST_ROOT}
  )

  target_link_libraries(${case}_base
    PUBLIC
    easy_base
  )

  target_compile_features(${case}_base PUBLIC cxx_std_11)

  add_executable(${case} ${case}.c)

  target_link_libraries(${case}
    PRIVATE
    ${case}_base
    pthread m dl easy
  )
  add_test(${case} ${case})
endfunction()

add_subdirectory(include)
add_subdirectory(io)
add_subdirectory(memory)
add_subdirectory(packet)
add_subdirectory(thread)
add_subdirectory(util)
