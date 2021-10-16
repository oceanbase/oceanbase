add_executable(oblog_tailf
  main.cpp
  oblog_main.h oblog_main.cpp
  ob_binlog_record_printer.h ob_binlog_record_printer.cpp
  )
target_link_libraries(oblog_tailf PRIVATE oblog)
target_include_directories(oblog_tailf PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
