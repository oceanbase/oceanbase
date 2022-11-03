# -*- coding: utf-8 -*-

# Copyright 2014 - 2018 Alibaba Inc. All Rights Reserved.
# Author:

# __all_privilege
def gen_all_privilege_init_data(h_f):
  h_f.write('// initial data for __all_privilege\n')
  h_f.write("""struct PrivilegeRow {
  const char *privilege_;
  const char *context_;
  const char *comment_;
};\n""")
  h_f.write("""
  const char* const ALTER_TB_MSG = "To alter the table";
  const char* const CREATE_DB_TB_MSG = "To create new databases and tables";
  const char* const CREATE_VIEW_MSG = "To create new views";
  const char* const CREATE_USER_MSG = "To create new users";
  const char* const DELETE_ROWS_MSG = "To delete existing rows";
  const char* const DROP_DB_TB_VIEWS_MSG = "To drop databases, tables, and views";
  const char* const GRANT_OPTION_MSG = "To give to other users those privileges you possess";
  const char* const INDEX_MSG = "To create or drop indexes";
  const char* const INSERT_MSG = "To insert data into tables";
  const char* const PROCESS_MSG = "To view the plain text of currently executing queries";
  const char* const SELECT_MSG = "To retrieve rows from table";
  const char* const SHOW_DB_MSG = "To see all databases with SHOW DATABASES";
  const char* const SHOW_VIEW_MSG = "To see views with SHOW CREATE VIEW";
  const char* const SUPER_MSG = "To use KILL thread, SET GLOBAL, CHANGE MASTER, etc.";
  const char* const UPDATE_MSG = "To update existing rows";
  const char* const USAGE_MSG = "No privileges - allow connect only";
""") 
  h_f.write("""static const PrivilegeRow all_privileges[] =
{
  {"Alter", "Tables",  ALTER_TB_MSG},
  {"Create", "Databases,Tables,Indexes",  CREATE_DB_TB_MSG},
  {"Create view", "Tables",  CREATE_VIEW_MSG},
  {"Create user", "Server Admin",  CREATE_USER_MSG},
  {"Delete", "Tables",  DELETE_ROWS_MSG},
  {"Drop", "Databases,Tables", DROP_DB_TB_VIEWS_MSG},
  {"Grant option",  "Databases,Tables,Functions,Procedures", GRANT_OPTION_MSG},
  {"Index", "Tables",  INDEX_MSG},
  {"Insert", "Tables",  INSERT_MSG},
  {"Process", "Server Admin", PROCESS_MSG},
  {"Select", "Tables",  SELECT_MSG},
  {"Show databases","Server Admin", SHOW_DB_MSG},
  {"Show view","Tables", SHOW_VIEW_MSG},
  {"Super","Server Admin", SUPER_MSG},
  {"Update", "Tables",  UPDATE_MSG},
  {"Usage","Server Admin",USAGE_MSG},
  {NULL, NULL, NULL},
};\n""")
