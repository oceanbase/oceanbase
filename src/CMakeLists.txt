add_library(ob_base_without_pass INTERFACE)
add_library(ob_base INTERFACE)
target_include_directories(
  ob_base INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/plugin/include)

target_compile_features(ob_base_without_pass INTERFACE cxx_std_11)
target_link_libraries(ob_base_without_pass INTERFACE oblib_base_without_pass objit_base
  ${OB_RELRO_FLAG} -Wl,-T,${CMAKE_SOURCE_DIR}/rpm/ld.lds)
target_compile_definitions(ob_base_without_pass INTERFACE
  PACKAGE_NAME="${PROJECT_NAME}"
  PACKAGE_VERSION="${PROJECT_VERSION}"
  PACKAGE_STRING="${PROJECT_NAME} ${PROJECT_VERSION}"
  # change to real release ID.
  RELEASEID="${BUILD_NUMBER}"
  )
target_compile_options(ob_base_without_pass INTERFACE
  $<$<COMPILE_LANGUAGE:CXX>:-Wno-invalid-offsetof>)

target_link_libraries(ob_base INTERFACE ob_base_without_pass oblib_pass)
function(ob_server_add_target target)
  message(STATUS "observer_add_target ${target}")
  ob_add_object_target(${target})
  target_link_libraries(${target} PUBLIC ob_base)
  list(APPEND ob_objects ${target})
  set(ob_objects "${ob_objects}" CACHE INTERNAL "observer object library list")
endfunction()

add_subdirectory(share)
add_subdirectory(sql)
add_subdirectory(pl)
add_subdirectory(rootserver)
add_subdirectory(logservice)
add_subdirectory(storage)
add_subdirectory(diagnose)
if (BUILD_CDC_ONLY)
  add_subdirectory(observer EXCLUDE_FROM_ALL)
  add_subdirectory(libtable EXCLUDE_FROM_ALL)
else()
  add_subdirectory(observer)
  add_subdirectory(libtable)
endif()
add_subdirectory(plugin)
