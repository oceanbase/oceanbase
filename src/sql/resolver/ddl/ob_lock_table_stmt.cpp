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
