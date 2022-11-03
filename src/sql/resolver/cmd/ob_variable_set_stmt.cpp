/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
