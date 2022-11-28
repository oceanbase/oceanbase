ob_set_subtarget(obcdc_tailf_object_list common
  main.cpp
  ob_binlog_record_printer.cpp
  obcdc_main.cpp
)

# obcdc_tailf
add_executable(obcdc_tailf)
ob_add_new_object_target(obcdc_tailf_objects obcdc_tailf_object_list)
target_link_libraries(obcdc_tailf_objects PRIVATE obcdc)
target_link_libraries(obcdc_tailf PRIVATE obcdc_tailf_objects)

# obcdc_tailf_static
add_executable(obcdc_tailf_static EXCLUDE_FROM_ALL)
ob_add_new_object_target(obcdc_tailf_objects_static obcdc_tailf_object_list)
target_link_libraries(obcdc_tailf_objects_static PRIVATE obcdc_static)
target_link_libraries(obcdc_tailf_static PRIVATE obcdc_tailf_objects_static)

if (OB_BUILD_CDC)
  # execute build_cdc_demo to check dlopen and compile
  if (NOT OB_STATIC_LINK_LGPL_DEPS)
    add_custom_command(TARGET obcdc_tailf POST_BUILD
      COMMAND /usr/bin/sh build_cdc_demo.sh ${DEVTOOLS_DIR} ${PROJECT_BINARY_DIR} ${DEP_DIR}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/demo/
      )
  else()
    add_custom_command(TARGET obcdc_tailf POST_BUILD
      COMMAND /usr/bin/sh build_cdc_demo.sh ${DEVTOOLS_DIR} ${PROJECT_BINARY_DIR}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/demo/
      )
  endif()
endif()
