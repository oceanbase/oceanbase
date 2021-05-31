add_library(ob_base INTERFACE)
target_include_directories(
  ob_base INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_features(ob_base INTERFACE cxx_std_11)
target_link_libraries(ob_base INTERFACE oblib_base)
target_compile_definitions(ob_base INTERFACE
  PACKAGE_NAME="${PROJECT_NAME}"
  PACKAGE_VERSION="${PROJECT_VERSION}"
  PACKAGE_STRING="${PROJECT_NAME} ${PROJECT_VERSION}"
  RELEASEID="${OB_RELEASEID}"
  )
target_compile_options(ob_base INTERFACE
  $<$<COMPILE_LANGUAGE:CXX>:-Wno-invalid-offsetof>)

#PCH
if (OB_ENABLE_SERVER_PCH)
  add_library(server_pch OBJECT share/dummy.cpp)
  target_link_libraries(server_pch oblib_base)
  set(ob_server_pchs "" CACHE INTERNAL "ob server precompiled headers")
endif()

function(ob_server_add_pchs rpath)
  if (OB_ENABLE_SERVER_PCH)
    FOREACH(item ${ARGN})
      list(APPEND ob_server_pchs ${rpath}/${item})
    ENDFOREACH(item)
    set(ob_server_pchs ${ob_server_pchs} CACHE INTERNAL "ob server precompiled headers")
  endif()
endfunction()

function(enable_server_pch target)
  if (OB_ENABLE_SERVER_PCH)
    target_precompile_headers(${target} REUSE_FROM server_pch)
  endif()
endfunction()

set(ob_objects "" CACHE INTERNAL "observer object library list")
function(ob_server_add_target target)
  message(STATUS "observer_add_target ${target}")
  ob_add_target(${ARGV})
  target_link_libraries(${target} PUBLIC ob_base)
  list(APPEND ob_objects ${target})
  set(ob_objects "${ob_objects}" CACHE INTERNAL "observer object library list")
  enable_server_pch(${target})
endfunction()

add_subdirectory(share)
add_subdirectory(sql)
add_subdirectory(rootserver)
add_subdirectory(clog)
add_subdirectory(archive)
add_subdirectory(election)
add_subdirectory(storage)
add_subdirectory(observer)

if (OB_ENABLE_SERVER_PCH)
  target_precompile_headers(ob_base INTERFACE ${ob_server_pchs})
endif()
