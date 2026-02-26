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
#include "storage/tablelock/ob_mysql_lock_table_executor.h"
#include "share/ob_table_lock_compat_versions.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace transaction::tablelock;
namespace sql
{

int ObLockTableExecutor::execute(ObExecContext &ctx,
                                 ObLockTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (is_mysql_mode()) {
    LOG_DEBUG("mysql mode do nothing");
    ret = execute_mysql_(ctx, stmt);
  } else if (OB_FAIL(execute_oracle_(ctx, stmt))) {
    LOG_WARN("execute oracle lock table failed", K(ret));
  }
  return ret;
}

int ObLockTableExecutor::execute_oracle_(ObExecContext &ctx,
                                         ObLockTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
  if (is_mysql_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should be oracle mode", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(session));
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
      if (OB_SUCCESS != (tmp_ret = ObSqlTransControl::end_trans(ctx.get_my_session(),
                                                                ctx.get_need_disconnect_for_update(),
                                                                ctx.get_trans_state(),
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

int ObLockTableExecutor::execute_mysql_(ObExecContext &ctx,
                                        ObLockTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get min data version", KR(ret));
  } else if (!is_mysql_lock_table_data_version(data_version)) {
    // ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support for this data version", K(data_version));
  } else {
    // only execute normally after enable lock_priority configuration, otherwise
    // it will directly throw OB_SUCCESS, which is an empty implementation
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (!tenant_config.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      // if tenant config is invalid, this config will be set as false
      LOG_WARN("tenant config is invalid");
    } else if (tenant_config->enable_lock_priority) {
      switch(stmt.get_lock_stmt_type()) {
      case ObLockTableStmt::MYSQL_LOCK_TABLE_STMT: {
        ObMySQLLockTableExecutor executor;
        if (OB_FAIL(executor.execute(ctx, stmt.get_mysql_lock_list()))) {
          LOG_WARN("lock table failed", K(ret));
        }
        break;
      }
      case ObLockTableStmt::MYSQL_UNLOCK_TABLE_STMT: {
        ObMySQLUnlockTableExecutor executor;
        if (OB_FAIL(executor.execute(ctx))) {
          LOG_WARN("unlock table failed", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown lock statement type", K(ret), K(stmt.get_lock_stmt_type()));
      }
      }
    }
  }
  LOG_DEBUG("execute mysql lock table", K(ctx), K(stmt));
  return ret;
}

} // namespace sql
} // namespace oceanbase
