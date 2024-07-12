/**
 * Copyright (c) 2023 OceanBase
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

#include "sql/engine/cmd/ob_table_direct_insert_service.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_store.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/resolver/dml/ob_insert_stmt.h"

namespace oceanbase
{
using namespace observer;

namespace sql
{
bool ObTableDirectInsertService::is_direct_insert(const ObPhysicalPlan &phy_plan)
{
  return ((phy_plan.get_enable_append() ||
           phy_plan.get_is_insert_overwrite()) &&
           (0 != phy_plan.get_append_table_id()));
}

int ObTableDirectInsertService::start_direct_insert(ObExecContext &ctx,
    ObPhysicalPlan &phy_plan)
{
  int ret = OB_SUCCESS;
  if (!GCONF._ob_enable_direct_load) { // recheck
    phy_plan.set_enable_append(false);
    phy_plan.set_enable_inc_direct_load(false);
    phy_plan.set_enable_replace(false);
    phy_plan.set_append_table_id(0);
  } else {
    const bool is_inc_direct_load = phy_plan.get_enable_inc_direct_load();
    const bool is_inc_replace = phy_plan.get_enable_replace();
    const bool is_insert_overwrite = phy_plan.get_is_insert_overwrite();
    const double online_sample_precent = phy_plan.get_online_sample_percent();
    ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
    bool auto_commit = false;
    CK (OB_NOT_NULL(session));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(session->get_autocommit(auto_commit))) {
      LOG_WARN("failed to get auto commit", KR(ret));
    } else if (!is_inc_direct_load
        && (!auto_commit || session->is_in_transaction())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("using full direct-insert with a transaction is not supported", KR(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "using full direct-insert within a transaction is");
    } else {
      ObTableDirectInsertCtx &table_direct_insert_ctx = ctx.get_table_direct_insert_ctx();
      uint64_t table_id = phy_plan.get_append_table_id();
      int64_t parallel = phy_plan.get_px_dop();
      if (OB_FAIL(table_direct_insert_ctx.init(&ctx, phy_plan, table_id, parallel, is_inc_direct_load, is_inc_replace, is_insert_overwrite, online_sample_precent))) {
        LOG_WARN("failed to init table direct insert ctx",
            KR(ret), K(table_id), K(parallel), K(is_inc_direct_load), K(is_inc_replace), K(is_insert_overwrite));
      }
    }
  }
  return ret;
}

int ObTableDirectInsertService::commit_direct_insert(ObExecContext &ctx,
    ObPhysicalPlan &phy_plan)
{
  int ret = OB_SUCCESS;
  ObTableDirectInsertCtx &table_direct_insert_ctx = ctx.get_table_direct_insert_ctx();
  if (OB_FAIL(table_direct_insert_ctx.commit())) {
    LOG_WARN("failed to commit table direct insert ctx", KR(ret));
  }
  return ret;
}

int ObTableDirectInsertService::finish_direct_insert(ObExecContext &ctx,
    ObPhysicalPlan &phy_plan, const bool commit)
{
  int ret = OB_SUCCESS;
  ObTableDirectInsertCtx &table_direct_insert_ctx = ctx.get_table_direct_insert_ctx();
  if (commit && OB_FAIL(table_direct_insert_ctx.finish())) {
    LOG_WARN("failed to finish table direct insert ctx", KR(ret));
  }
  table_direct_insert_ctx.destroy();
  return ret;
}

int ObTableDirectInsertService::open_task(const uint64_t table_id,
                                          const int64_t px_task_id,
                                          const int64_t ddl_task_id,
                                          ObTableLoadTableCtx *&table_ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadUniqueKey key(table_id, ddl_task_id);
  if (OB_NOT_NULL(table_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_ctx should be null", KR(ret), KP(table_ctx));
  } else if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ret = OB_NOT_MASTER;
      LOG_WARN("not the master of store", KR(ret), K(key));
    }
  } else if (OB_UNLIKELY(nullptr == table_ctx->store_ctx_)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("not the master of store", KR(ret), K(key));
  } else {
    table::ObTableLoadTransId trans_id;
    trans_id.segment_id_ = px_task_id;
    trans_id.trans_gid_ = 1;
    ObTableLoadStore store(table_ctx);
    if (OB_FAIL(store.init())) {
      LOG_WARN("fail to init store", KR(ret));
    } else if (OB_FAIL(store.px_start_trans(trans_id))) {
      LOG_WARN("fail to start direct load trans", KR(ret), K(trans_id));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(table_ctx)) {
    ObTableLoadService::put_ctx(table_ctx);
    table_ctx = nullptr;
  }
  return ret;
}

int ObTableDirectInsertService::close_task(const uint64_t table_id,
                                           const int64_t px_task_id,
                                           const int64_t ddl_task_id,
                                           ObTableLoadTableCtx *table_ctx,
                                           const int error_code)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(table_ctx)) {
    table::ObTableLoadTransId trans_id;
    trans_id.segment_id_ = px_task_id;
    trans_id.trans_gid_ = 1;
    if (OB_SUCC(error_code)) {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.px_finish_trans(trans_id))) {
        LOG_WARN("fail to finish direct load trans", KR(ret), K(trans_id));
      }
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObTableLoadStore::px_abandon_trans(table_ctx, trans_id))) {
        LOG_WARN("fail to abandon direct load trans", KR(tmp_ret), K(trans_id));
      }
    }
    ObTableLoadService::put_ctx(table_ctx);
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase