execute_process(
  COMMAND env bash gen_parser.sh
  RESULT_VARIABLE RET
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/src/observer/dbms_scheduler/parser
  OUTPUT_FILE _gen_parser.output
  ERROR_FILE _gen_parser.error)
if(RET AND NOT RET EQUAL 0)
  message(FATAL_ERROR "Generate calendar parser failed and check error in ${PROJECT_SOURCE_DIR}/src/observer/dbms_scheduler/parser/_gen_parser.error")
endif()

ob_set_subtarget(dbms_scheduler ALONE
  parser/dbms_sched_parser_base.c
  parser/dbms_sched_calendar_parser.cpp
  parser/dbms_sched_parser_calendar_lex.c
  parser/dbms_sched_parser_calendar_tab.c
)

ob_server_add_target(dbms_scheduler)
