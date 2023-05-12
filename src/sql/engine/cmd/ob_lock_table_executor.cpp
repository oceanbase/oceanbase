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
    const common::ObIArray<TableItem *> &table_items = stmt.get_table_items();
    if (OB_UNLIKELY(table_items.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("there's no table in the stmt", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
      TableItem *table_item = table_items.at(i);

      // handle compatibility
      // If the version of cluster is updated than 4.1, it can use
      // 'wait n' or 'no wait' grammar. Otherwise, it should follow
      // the previous logic (i.e. try lock until trx / sql timeout)
      int64_t wait_lock_seconds;
      if (GET_MIN_CLUSTER_VERSION() > CLUSTER_VERSION_4_1_0_0) {
        wait_lock_seconds = stmt.get_wait_lock_seconds();
      } else {
        wait_lock_seconds = -1;
      }

      if (OB_FAIL(ObSqlTransControl::lock_table(
              ctx, table_item->ref_id_, table_item->part_ids_,
              stmt.get_lock_mode(), wait_lock_seconds))) {
        if ((OB_TRY_LOCK_ROW_CONFLICT == ret ||
             OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret ||
             OB_ERR_SHARED_LOCK_CONFLICT == ret) &&
            wait_lock_seconds >= 0) {
          ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT;
        }
        LOG_WARN("fail lock table", K(ret), K(stmt.get_lock_mode()),
                 K(wait_lock_seconds), K(table_item->ref_id_),
                 K(table_item->part_ids_));
      }
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

} // namespace sql
} // namespace oceanbase
