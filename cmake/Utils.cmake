if(NOT OB_BUILD_OPENSOURCE)
include(cmake/Err.cmake)
endif()

macro(ob_define VAR DEFAULT)
  if (NOT DEFINED ${VAR})
    set(${VAR} ${DEFAULT})
  endif()
endmacro()

function(ob_replace_in_file INFILE OUTFILE MATCH-STRING REPLACE-STRING)
  file(READ ${INFILE} CONTENT)
  string(REPLACE ${MATCH-STRING} ${REPLACE-STRING} NEW-CONTENT ${CONTENT})
  file(WRITE ${OUTFILE} ${NEW-CONTENT})
endfunction()

function(ob_set_subtarget target group)

  list(LENGTH ${target}_cache_objects_ CCLS_TARGET_CURRENT_LENGTH)

  # 需要参与编译的源文件列表
  set(ARGN_NEED_LIST "")

  FOREACH(item ${ARGN})
    list(APPEND ARGN_NEED_LIST ${item})
  ENDFOREACH(item)

  list(APPEND "${target}_cache_objects_" ${ARGN_NEED_LIST})
  set("${target}_cache_objects_" ${${target}_cache_objects_} PARENT_SCOPE)

  if (OB_CMAKE_RULES_CHECK)
    FOREACH(item ${ARGN_NEED_LIST})
      # [E1001] Header files are not allowed in CMakeLists.txt
      string(REGEX MATCHALL "^.*\.h$" MATCH_OUTPUT ${item})
      if(MATCH_OUTPUT)
        message(FATAL_ERROR "\n${E1001}\n不允许把头文件${item}写到CMakeLists.txt文件中\n")
      endif()
    ENDFOREACH(item)
  endif()

  # diable global unity build
  if (NOT OB_ENABLE_UNITY)
    return()
  endif()

  # ALONE group will not join unity build
  if(group STREQUAL "ALONE")
    return()
  endif()

  if (NOT OB_BUILD_CCLS)
    set(i 0)
    set(group_id 0)
  else()
    # ccls构建，将更改分组方法，是以target为单位，而不是以group为单元
    set(i ${CCLS_TARGET_CURRENT_LENGTH})
    math(EXPR group_id "(${i} / ${OB_MAX_UNITY_BATCH_SIZE})")
  endif()

  set(ob_sub_objects "")
  FOREACH(item ${ARGN_NEED_LIST})
    math(EXPR i "(${i} + 1) % ${OB_MAX_UNITY_BATCH_SIZE}")
    list(APPEND ob_sub_objects ${item})
    if (${i} EQUAL 0)
      if (NOT OB_BUILD_CCLS)
        set_source_files_properties(${ob_sub_objects} PROPERTIES UNITY_GROUP "${target}_${group}/${group_id}")
      else()
        set_source_files_properties(${ob_sub_objects} PROPERTIES UNITY_GROUP "${target}/${group_id}")
      endif()
      math(EXPR group_id "${group_id} + 1")
      set(ob_sub_objects "")
    endif()
  ENDFOREACH(item)

  if (${i} GREATER 0)
    if (NOT OB_BUILD_CCLS)
      set_source_files_properties(${ob_sub_objects} PROPERTIES UNITY_GROUP "${target}_${group}/${group_id}")
    else()
      set_source_files_properties(${ob_sub_objects} PROPERTIES UNITY_GROUP "${target}/${group_id}")
    endif()
  endif()

endfunction()

function (check_need_build_unity_target target need_build)
  list(LENGTH ${target}_cache_objects_ TARGET_LENGTH)
  if (TARGET_LENGTH EQUAL 0)
    set(${need_build} FALSE PARENT_SCOPE)
  else()
    set(${need_build} TRUE PARENT_SCOPE)
  endif()
endfunction()


set(unity_after [[
#ifdef USING_LOG_PREFIX
#undef USING_LOG_PREFIX
#endif
]])

function(config_target_unity target)
  if (OB_ENABLE_UNITY)
    set_target_properties(${target} PROPERTIES UNITY_BUILD ON)
    set_target_properties(${target} PROPERTIES UNITY_BUILD_CODE_AFTER_INCLUDE "${unity_after}")
    set_target_properties(${target} PROPERTIES UNITY_BUILD_MODE GROUP)
  endif()
endfunction()

function(config_ccls_flag target)
  if (OB_BUILD_CCLS)
    target_compile_definitions(${target} PRIVATE CCLS_LASY_OFF)
  endif()
endfunction()

function(config_remove_coverage_flag target)
  # 针对于特定的目标，由于某种写法会命中clang的DAG解析的bug，将少量文件不参与coverage编译
  if (WITH_COVERAGE)
    get_target_property(EXTLIB_COMPILE_FLAGS ${target} COMPILE_OPTIONS)
    list(REMOVE_ITEM EXTLIB_COMPILE_FLAGS ${CMAKE_COVERAGE_COMPILE_OPTIONS})
    set_target_properties(${target} PROPERTIES COMPILE_OPTIONS "${EXTLIB_COMPILE_FLAGS}")
  endif()
endfunction()

function(ob_add_object_target target)
  add_library(${target} OBJECT "${${target}_cache_objects_}")
  config_target_unity(${target})
  config_ccls_flag(${target})
endfunction()

function(ob_lib_add_target target)
  message(STATUS "ob_lib_add_target ${target}")
  ob_add_object_target(${target})
  target_link_libraries(${target} PUBLIC oblib_base)
  list(APPEND oblib_object_libraries ${target})
  set(oblib_object_libraries "${oblib_object_libraries}" CACHE INTERNAL "observer library list")
  config_ccls_flag(${target})
endfunction()

function(ob_add_new_object_target target target_objects_list)
  message(STATUS "ob_add_new_object_target ${target}")
  add_library(${target} OBJECT EXCLUDE_FROM_ALL "${${target_objects_list}_cache_objects_}")
  config_target_unity(${target})
  config_ccls_flag(${target})
endfunction()
