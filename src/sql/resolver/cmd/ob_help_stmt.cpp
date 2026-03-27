/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/resolver/cmd/ob_help_stmt.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
ObHelpStmt::ObHelpStmt(ObIAllocator &allocator)
  : ObDMLStmt(stmt::T_HELP, allocator),
    row_store_(),
    col_names_(allocator)
{
}

ObHelpStmt::~ObHelpStmt()
{

}
int ObHelpStmt::add_col_name(ObString col_name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(col_names_.push_back(col_name))) {
    SQL_RESV_LOG(WARN, "fail to push back column name", K(ret), K(col_name));
  }
  return ret;
}
int ObHelpStmt::get_col_name(int64_t idx, ObString &col_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx < 0 || idx >= col_names_.count())) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(
        WARN, "invalid argument", K(ret), K(idx), K(col_names_.count()));
  } else {
    col_name = col_names_.at(idx);
  }
  return ret;
}
