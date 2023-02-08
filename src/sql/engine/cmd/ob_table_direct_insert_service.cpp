// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yuya.yu <yuya.yu@oceanbase.com>

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/cmd/ob_table_direct_insert_service.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_store.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"

namespace oceanbase
{
using namespace observer;

namespace sql
{
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
      LOG_WARN("using direct-insert within a transaction is not supported",
          KR(ret), K(auto_commit), K(session->is_in_transaction()));
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
    LOG_WARN("fail to get table ctx", KR(ret), K(key), K(table_id));
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
    ObTableLoadStore store(table_ctx);
    if (OB_FAIL(store.init())) {
      LOG_WARN("fail to init store", KR(ret));
    } else {
      if (OB_SUCC(error_code)) {
        if (OB_FAIL(store.px_finish_trans(trans_id))) {
          LOG_WARN("fail to finish direct load trans", KR(ret), K(trans_id));
        }
      } else {
        if (OB_FAIL(store.px_abandon_trans(trans_id))) {
          LOG_WARN("fail to abandon direct load trans", KR(ret));
        }
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