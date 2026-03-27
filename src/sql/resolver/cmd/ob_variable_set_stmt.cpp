/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/cmd/ob_variable_set_stmt.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
int ObVariableSetStmt::get_variable_node(int64_t index,
                                         ObVariableSetStmt::VariableSetNode &var_node) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index < 0 || index >= variable_nodes_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index", K(index), K(ret));
  } else if (OB_FAIL(variable_nodes_.at(index, var_node))) {
    LOG_WARN("fail to get variable_nodes", K(index), K(ret));
  } else {}
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase 
