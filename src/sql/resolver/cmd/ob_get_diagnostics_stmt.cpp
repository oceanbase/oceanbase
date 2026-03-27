/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/cmd/ob_get_diagnostics_stmt.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObGetDiagnosticsStmt::get_diag_info_type_by_name(const ObString &val, DIAG_INFO_TYPE &type)
{
  int ret = OB_SUCCESS;
  if (val == "MYSQL_ERRNO") {
    type = MYSQL_ERRNO_TYPE;
  } else if (val == "MESSAGE_TEXT") {
    type = MESSAGE_TEXT_TYPE;
  } else if (val == "RETURNED_SQLSTATE") {
    type = RETURNED_SQLSTATE_TYPE;
  } else if (val == "CLASS_ORIGIN") {
    type = CLASS_ORIGIN_TYPE;
  } else if (val == "SUBCLASS_ORIGIN") {
    type = SUBCLASS_ORIGIN_TYPE;
  } else if (val == "TABLE_NAME") {
    type = TABLE_NAME_TYPE;
  } else if (val == "COLUMN_NAME") {
    type = COLUMN_NAME_TYPE;
  } else if (val == "CONSTRAINT_CATALOG") {
    type = CONSTRAINT_CATALOG_TYPE;
  } else if (val == "CONSTRAINT_SCHEMA") {
    type = CONSTRAINT_SCHEMA_TYPE;
  } else if (val == "CONSTRAINT_NAME") {
    type = CONSTRAINT_NAME_TYPE;  
  } else if (val == "CATALOG_NAME") {
    type = CATALOG_NAME_TYPE;
  } else if (val == "SCHEMA_NAME") {
    type = SCHEMA_NAME_TYPE;
  } else if (val == "CURSOR_NAME") {
    type = CURSOR_NAME_TYPE;
  } else if (val == "NUMBER") {
    type = NUMBER_TYPE;
  } else if (val == "ROW_COUNT") {
    type = ROW_COUNT_TYPE;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected diagnostic name", K(ret));
  }
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase 
