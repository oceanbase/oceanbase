/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "ob_lock_table_stmt.h"

namespace oceanbase
{
namespace sql
{
int ObLockTableStmt::add_mysql_lock_node(const ObMySQLLockNode &node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!node.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock node invalid", K(ret), K(node));
  } else if (OB_FAIL(mysql_lock_list_.push_back(node))) {
    LOG_WARN("add mysql lock node failed", K(ret), K(node));
  }
  return ret;
}

} // sql
} // oceanbase
