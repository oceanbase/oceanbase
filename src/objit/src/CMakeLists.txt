add_library(objit_base INTERFACE)
target_include_directories(objit_base INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
  )

# GLOB all source files according to its suffix.
file(GLOB_RECURSE objit_SRC "*.h" "*.cpp" "${CMAKE_CURRENT_SROUCE_DIR}/../include/*.h")

add_definitions(-std=gnu++14)
add_library(objit_objects OBJECT ${objit_SRC})
target_link_libraries(objit_objects PRIVATE oblib_base_base ob_share objit_base)

# Define a static library target named OBJIT_RAW.
add_library(objit STATIC dummy.cpp)

target_link_libraries(objit
  PUBLIC
  objit_objects objit_base oblib "${llvm_libs}"
  INTERFACE
  -lrt -ldl -lpthread -lm
)
