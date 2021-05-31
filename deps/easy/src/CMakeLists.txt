function(easy_add_objects target)
  FOREACH(item ${ARGN})
    if(item MATCHES ".*\.h$")
      list(APPEND easy_headers "${target}/${item}")
    else()
      list(APPEND easy_objects "${target}/${item}")
    endif()
  ENDFOREACH(item)
  set(easy_headers "${easy_headers}" CACHE INTERNAL easy_headers)
  set(easy_objects "${easy_objects}" CACHE INTERNAL easy_objects)
endfunction()
set(easy_headers "" CACHE INTERNAL easy_headers)
set(easy_objects "" CACHE INTERNAL easy_objects)

add_subdirectory(io)
add_subdirectory(memory)
add_subdirectory(packet)
add_subdirectory(thread)
add_subdirectory(util)

add_library(
  easy STATIC
  ${easy_objects}
)

target_link_libraries(
  easy PRIVATE
  easy_base
)
