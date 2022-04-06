add_executable(obcdc_tailf
  main.cpp
  oblog_main.h oblog_main.cpp
  ob_binlog_record_printer.h ob_binlog_record_printer.cpp
  )
target_link_libraries(obcdc_tailf PRIVATE obcdc)
target_include_directories(obcdc_tailf PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
