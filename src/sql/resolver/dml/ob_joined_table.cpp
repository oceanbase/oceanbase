/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase
{
namespace sql
{

int JoinedTable::get_input_relations(ObSelectStmt *stmt, ObIArray<ObRelation *> &inputs)
{
  int ret = OB_SUCCESS;
  UNUSED(stmt);
  if (OB_ISNULL(left_table_) || OB_ISNULL(right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left or right table is unexpected null", K(ret), K(left_table_), K(right_table_));
  } else if (OB_FAIL(inputs.push_back(left_table_))) {
    LOG_WARN("failed to push back left table", K(ret));
  } else if (OB_FAIL(inputs.push_back(right_table_))) {
    LOG_WARN("failed to push back right table", K(ret));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
