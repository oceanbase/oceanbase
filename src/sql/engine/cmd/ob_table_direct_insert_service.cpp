// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yuya.yu <>

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
// Direct-insert is enabled only when:
// 1. _ob_enable_direct_load
// 2. insert into select clause
// 3. append hint + pdml
// 4. auto_commit, not in a transaction
int ObTableDirectInsertService::check_direct_insert(ObOptimizerContext &optimizer_ctx,
                                                    const ObDMLStmt &stmt,
                                                    bool &is_direct_insert)
{
  int ret = OB_SUCCESS;
  bool auto_commit = false;
  const ObSQLSessionInfo* session_info = optimizer_ctx.get_session_info();
  is_direct_insert = false;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", KR(ret), KP(session_info));
  } else if (OB_FAIL(session_info->get_autocommit(auto_commit))) {
    LOG_WARN("failed to get auto commit", KR(ret));
  } else if (GCONF._ob_enable_direct_load
      && stmt::T_INSERT == stmt.get_stmt_type()
      && static_cast<const ObInsertStmt &>(stmt).value_from_select()
      && optimizer_ctx.get_global_hint().has_append()
      && optimizer_ctx.use_pdml()
      && auto_commit
      && (!session_info->is_in_transaction())){
    is_direct_insert = true;
  }
  return ret;
}

bool ObTableDirectInsertService::is_direct_insert(const ObPhysicalPlan &phy_plan)
{
  return (phy_plan.get_enable_append() && (0 != phy_plan.get_append_table_id()));
}

int ObTableDirectInsertService::start_direct_insert(ObExecContext &ctx,
    ObPhysicalPlan &phy_plan)
{
  int ret = OB_SUCCESS;
  if (!GCONF._ob_enable_direct_load) { // recheck
    phy_plan.set_enable_append(false);
    phy_plan.set_append_table_id(0);
  } else {
    ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
    CK (OB_NOT_NULL(session));
    bool auto_commit = false;
    if (OB_FAIL(session->get_autocommit(auto_commit))) {
      LOG_WARN("failed to get auto commit", KR(ret));
    } else if (!auto_commit || session->is_in_transaction()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "using direct-insert within a transaction is");
    } else {
      ObTableDirectInsertCtx &table_direct_insert_ctx = ctx.get_table_direct_insert_ctx();
      uint64_t table_id = phy_plan.get_append_table_id();
      int64_t parallel = phy_plan.get_px_dop();
      if (OB_FAIL(table_direct_insert_ctx.init(&ctx, table_id, parallel))) {
        LOG_WARN("failed to init table direct insert ctx", KR(ret), K(table_id), K(parallel));
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
    ObPhysicalPlan &phy_plan)
{
  int ret = OB_SUCCESS;
  ObTableDirectInsertCtx &table_direct_insert_ctx = ctx.get_table_direct_insert_ctx();
  if (OB_FAIL(table_direct_insert_ctx.finish())) {
    LOG_WARN("failed to finish table direct insert ctx", KR(ret));
  }
  return ret;
}

int ObTableDirectInsertService::open_task(const uint64_t table_id, const int64_t task_id)
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCtx *table_ctx = nullptr;
  ObTableLoadKey key(MTL_ID(), table_id);
  if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
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
    trans_id.segment_id_ = task_id;
    trans_id.trans_gid_ = 1;
    ObTableLoadStore store(table_ctx);
    if (OB_FAIL(store.init())) {
      LOG_WARN("fail to init store", KR(ret));
    } else if (OB_FAIL(store.px_start_trans(trans_id))) {
      LOG_WARN("fail to start direct load trans", KR(ret), K(trans_id));
    }
  }
  if (OB_NOT_NULL(table_ctx)) {
    ObTableLoadService::put_ctx(table_ctx);
    table_ctx = nullptr;
  }
  return ret;
}

int ObTableDirectInsertService::close_task(const uint64_t table_id,
                                           const int64_t task_id,
                                           const int error_code)
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCtx *table_ctx = nullptr;
  ObTableLoadKey key(MTL_ID(), table_id);
  if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
    LOG_WARN("fail to get table ctx", KR(ret), K(key), K(table_id));
  } else {
    table::ObTableLoadTransId trans_id;
    trans_id.segment_id_ = task_id;
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
      if (OB_FAIL(ObTableLoadStore::px_abandon_trans(table_ctx, trans_id))) {
        LOG_WARN("fail to abandon direct load trans", KR(ret), K(trans_id));
      }
    }
  }
  if (OB_NOT_NULL(table_ctx)) {
    ObTableLoadService::put_ctx(table_ctx);
    table_ctx = nullptr;
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase