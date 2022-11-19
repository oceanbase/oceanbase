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
#include "sql/engine/cmd/ob_lock_table_executor.h"

#include "sql/resolver/ddl/ob_lock_table_stmt.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

int ObLockTableExecutor::execute(ObExecContext &ctx,
                                 ObLockTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(session));
  } else if (is_mysql_mode()) {
    LOG_DEBUG("mysql mode do nothing");
  } else {
    if (OB_FAIL(ObSqlTransControl::lock_table(ctx,
                                              stmt.get_table_id(),
                                              stmt.get_lock_mode()))) {
      LOG_WARN("fail lock table", K(ret), K(stmt.get_lock_mode()), K(stmt.get_table_id()));
    }
    bool explicit_trans = session->has_explicit_start_trans();
    bool ac = false;
    bool is_commit = OB_SUCC(ret);
    session->get_autocommit(ac);
    if (!explicit_trans && ac) {
      if (OB_SUCCESS != (tmp_ret = ObSqlTransControl::end_trans(ctx,
                                                                !is_commit,
                                                                false,
                                                                nullptr))) {
        ret = COVER_SUCC(tmp_ret);
        LOG_WARN("end trans failed", K(tmp_ret), K(ctx), K(is_commit));
      }
    }
  }
  return ret;
}


} // sql
} // oceanbase
