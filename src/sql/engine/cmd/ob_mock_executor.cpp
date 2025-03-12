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
namespace oceanbase
{
using namespace common;
namespace sql
{
int ObMockExecutor::execute(ObExecContext &exec_ctx, ObMockStmt &stmt)
{
  int ret = OB_SUCCESS;

  if (stmt::T_INSTALL_PLUGIN == stmt.get_stmt_type()
      || stmt::T_UNINSTALL_PLUGIN == stmt.get_stmt_type()
      || stmt::T_FLUSH_MOCK == stmt.get_stmt_type()
      || stmt::T_HANDLER_MOCK == stmt.get_stmt_type()
      || stmt::T_SHOW_PLUGINS == stmt.get_stmt_type()
      || stmt::T_CREATE_SERVER == stmt.get_stmt_type()
      || stmt::T_ALTER_SERVER == stmt.get_stmt_type()
      || stmt::T_DROP_SERVER == stmt.get_stmt_type()
      || stmt::T_CREATE_LOGFILE_GROUP == stmt.get_stmt_type()
      || stmt::T_ALTER_LOGFILE_GROUP == stmt.get_stmt_type()
      || stmt::T_DROP_LOGFILE_GROUP == stmt.get_stmt_type()
      || stmt::T_GRANT_PROXY == stmt.get_stmt_type()
      || stmt::T_REVOKE_PROXY == stmt.get_stmt_type()) {
    LOG_USER_WARN(OB_NOT_SUPPORTED, "This statement is");
  } else if (stmt::T_FLUSH_MOCK_LIST == stmt.get_stmt_type()) {
    const ObIArray<stmt::StmtType> &type_list = stmt.get_stmt_type_list();
    for (int64_t i = 0; OB_SUCC(ret) && i < type_list.count(); ++i) {
      if (stmt::T_FLUSH_MOCK == type_list.at(i)) {
        LOG_USER_WARN(OB_NOT_SUPPORTED, "This statement is");
      } else if (stmt::T_FLUSH_PRIVILEGES == type_list.at(i)) {
        LOG_USER_WARN(OB_NOT_SUPPORTED, "After executing the GRANT and REVOKE statements, "
                                        "the permissions will be automatically applied and take effect. "
                                        "There is no need to execute the FLUSH PRIVILEGES command. FLUSH PRIVILEGES");
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_USER_WARN(OB_NOT_SUPPORTED, "unknown stmt type");
      }
    }
  } else if (stmt::T_REPAIR_TABLE == stmt.get_stmt_type()) {
    LOG_USER_WARN(OB_NOT_SUPPORTED, "Repair table Statement just mocks the syntax of MySQL without supporting specific realization");
  } else if (stmt::T_CHECKSUM_TABLE == stmt.get_stmt_type()) {
    LOG_USER_WARN(OB_NOT_SUPPORTED, "Checksum table Statement just mocks the syntax of MySQL without supporting specific realization");
  } else if (stmt::T_CACHE_INDEX == stmt.get_stmt_type()) {
    LOG_USER_WARN(OB_NOT_SUPPORTED, "Cache index Statement just mocks the syntax of MySQL without supporting specific realization");
  } else if (stmt::T_LOAD_INDEX_INTO_CACHE == stmt.get_stmt_type()) {
    LOG_USER_WARN(OB_NOT_SUPPORTED, "Load index into cache Statement just mocks the syntax of MySQL without supporting specific realization");
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_USER_WARN(OB_NOT_SUPPORTED, "unknown stmt type");
  }
  return ret;
}

}
}
