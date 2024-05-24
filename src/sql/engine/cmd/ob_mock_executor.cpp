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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_mock_executor.h"
#include "sql/resolver/cmd/ob_mock_stmt.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
int ObMockExecutor::execute(ObExecContext &exec_ctx, ObMockStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (stmt::T_FLUSH_PRIVILEGES == stmt.get_stmt_type()) {
    LOG_USER_WARN(OB_NOT_SUPPORTED, "After executing the GRANT and REVOKE statements, "
                                    "the permissions will be automatically applied and take effect. "
                                    "There is no need to execute the FLUSH PRIVILEGES command. FLUSH PRIVILEGES");
  } else if (stmt::T_REPAIR_TABLE == stmt.get_stmt_type()) {
    LOG_USER_WARN(OB_NOT_SUPPORTED, "Repair table Statement just mocks the syntax of MySQL without supporting specific realization");
  } else if (stmt::T_CHECKSUM_TABLE == stmt.get_stmt_type()) {
    LOG_USER_WARN(OB_NOT_SUPPORTED, "Checksum table Statement just mocks the syntax of MySQL without supporting specific realization");
  }
  return ret;
}

}
}
