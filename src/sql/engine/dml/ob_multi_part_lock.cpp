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
#include "sql/engine/dml/ob_multi_part_lock.h"
#include "sql/engine/dml/ob_multi_dml_plan_mgr.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_mini_task_executor.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {

class ObMultiPartLock::ObMultiPartLockCtx : public ObTableModifyCtx, public ObMultiDMLCtx {
  friend class ObMultiPartLock;

public:
  explicit ObMultiPartLockCtx(ObExecContext& ctx)
      : ObTableModifyCtx(ctx), ObMultiDMLCtx(ctx.get_allocator()), got_row_(false)
  {}
  ~ObMultiPartLockCtx()
  {}
  bool has_got_row() const
  {
    return got_row_;
  }
  void set_got_row()
  {
    got_row_ = true;
  }

  virtual void destroy() override
  {
    ObTableModifyCtx::destroy();
    ObMultiDMLCtx::destroy_ctx();
  }

private:
  bool got_row_;
};

ObMultiPartLock::ObMultiPartLock(ObIAllocator& allocator) : ObTableModify(allocator), ObMultiDMLInfo(allocator)
{}

ObMultiPartLock::~ObMultiPartLock()
{}

int ObMultiPartLock::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMultiPartLockCtx* lock_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObMultiPartLockCtx, ctx, get_id(), get_type(), lock_ctx))) {
    LOG_WARN("create phy operator context failed", K(ret));
  } else if (OB_ISNULL(lock_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null", K(ret));
  } else if (OB_FAIL(lock_ctx->init_multi_dml_ctx(ctx, table_dml_infos_, get_phy_plan(), subplan_root_))) {
    LOG_WARN("init multi dml ctx failed", K(ret));
  } else if (OB_FAIL(init_cur_row(*lock_ctx, need_copy_row_for_compute()))) {
    LOG_WARN("failed to init cur row", K(ret));
  }
  return ret;
}

int ObMultiPartLock::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObMultiPartLockCtx* lock_ctx = NULL;
  if (OB_ISNULL(lock_ctx = GET_PHY_OPERATOR_CTX(ObMultiPartLockCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get lock ctx", K(ret));
  } else if (OB_FAIL(child_op_->get_next_row(ctx, row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else if (OB_FAIL(shuffle_lock_row(ctx, row))) {
    LOG_WARN("failed to to shuffle lock row", K(ret));
  } else if (OB_FAIL(copy_cur_row(*lock_ctx, row))) {
    LOG_WARN("failed to copy cur row", K(ret));
  }
  if (OB_ITER_END == ret && lock_ctx->has_got_row()) {
    // at the end iteration, process lock rows request
    if (OB_FAIL(process_mini_task(ctx))) {
      if (ret != OB_TRY_LOCK_ROW_CONFLICT && ret != OB_TRANSACTION_SET_VIOLATION) {
        LOG_WARN("failed to process mini task", K(ret));
      }
    } else {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObMultiPartLock::shuffle_lock_row(ObExecContext& ctx, const ObNewRow* row) const
{
  int ret = OB_SUCCESS;
  const int64_t idx = 0;  // index of the primary key
  ObSqlCtx* sql_ctx = NULL;
  ObMultiPartLockCtx* lock_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(lock_ctx = GET_PHY_OPERATOR_CTX(ObMultiPartLockCtx, ctx, get_id())) ||
      OB_ISNULL(sql_ctx = ctx.get_sql_ctx()) || OB_ISNULL(row) || OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("params have null", K(ret), K(get_id()), K(lock_ctx), K(sql_ctx), K(row));
  }
  for (int64_t tid = 0; OB_SUCC(ret) && tid < table_dml_infos_.count(); ++tid) {
    const ObGlobalIndexDMLInfo& pk_dml_info = table_dml_infos_.at(tid).index_infos_.at(idx);
    const DMLSubPlan& lock_plan = pk_dml_info.dml_subplans_.at(LOCK_OP);
    const ObTableLocation* tbl_loc = pk_dml_info.table_locs_.at(0);
    const bool need_filter_null = table_dml_infos_.at(tid).need_check_filter_null_;

    ObNewRow cur_row = *row;
    cur_row.projector_ = lock_plan.value_projector_;
    cur_row.projector_size_ = lock_plan.value_projector_size_;

    ObGlobalIndexDMLCtx& pk_dml_ctx = lock_ctx->table_dml_ctxs_.at(tid).index_ctxs_.at(idx);

    int64_t part_idx = OB_INVALID_INDEX;
    bool is_null = false;
    if (OB_ISNULL(tbl_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("primary key dml info is invalid", K(ret), K(tbl_loc));
    } else if (need_filter_null && OB_FAIL(check_rowkey_is_null(cur_row, cur_row.get_count(), is_null))) {
      LOG_WARN("failed to check rowkey is null", K(ret));
    } else if (is_null) {
      // do nothing
    } else if (OB_FAIL(tbl_loc->calculate_partition_ids_by_row(
                   ctx, sql_ctx->schema_guard_, *row, pk_dml_ctx.partition_ids_, part_idx))) {
      LOG_WARN("failed to calcuate partitin ids by row", K(ret));
    } else if (OB_FAIL(lock_ctx->multi_dml_plan_mgr_.add_part_row(tid, idx, part_idx, LOCK_OP, cur_row))) {
      LOG_WARN("get or create values op failed", K(ret));
    } else {
      lock_ctx->set_got_row();
      plan_ctx->add_affected_rows(1LL);
    }
  }
  return ret;
}

int ObMultiPartLock::process_mini_task(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMultiPartLockCtx* lock_ctx = NULL;
  if (OB_ISNULL(lock_ctx = GET_PHY_OPERATOR_CTX(ObMultiPartLockCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock context is null", K(ret), K(lock_ctx));
  } else if (OB_FAIL(ObTableModify::extend_dml_stmt(ctx, table_dml_infos_, lock_ctx->table_dml_ctxs_))) {
    LOG_WARN("failed to extended dml context", K(ret));
  } else if (OB_FAIL(lock_ctx->multi_dml_plan_mgr_.build_multi_part_dml_task())) {
    LOG_WARN("failed to build multi partition dml task", K(ret));
  } else {
    ObIArray<ObTaskInfo*>& task_info_list = lock_ctx->multi_dml_plan_mgr_.get_mini_task_infos();
    const ObMiniJob& subplan_job = lock_ctx->multi_dml_plan_mgr_.get_subplan_job();
    bool table_need_first = lock_ctx->multi_dml_plan_mgr_.table_need_first();
    ObMiniTaskResult result;
    ObIsMultiDMLGuard guard(*ctx.get_physical_plan_ctx());
    UNUSED(guard);
    if (OB_FAIL(lock_ctx->mini_task_executor_.execute(ctx, subplan_job, task_info_list, table_need_first, result))) {
      LOG_WARN("execute multi table dml task failed", K(ret));
    }
  }
  return ret;
}

int ObMultiPartLock::inner_close(ObExecContext& ctx) const
{
  int wait_ret = wait_all_task(GET_PHY_OPERATOR_CTX(ObMultiPartLockCtx, ctx, get_id()), ctx.get_physical_plan_ctx());
  int close_ret = ObTableModify::inner_close(ctx);
  if (OB_SUCCESS != wait_ret || OB_SUCCESS != close_ret) {
    LOG_WARN("inner close failed", K(wait_ret), K(close_ret));
  }
  return (OB_SUCCESS == close_ret) ? wait_ret : close_ret;
}

}  // namespace sql
}  // namespace oceanbase
