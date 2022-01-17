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
#include "sql/engine/dml/ob_multi_part_delete.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_mini_task_executor.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {
class ObMultiPartDelete::ObMultiPartDeleteCtx : public ObTableModifyCtx, public ObMultiDMLCtx {
  friend class ObMultiPartDelete;

public:
  explicit ObMultiPartDeleteCtx(ObExecContext& ctx) : ObTableModifyCtx(ctx), ObMultiDMLCtx(ctx.get_allocator())
  {}

  ~ObMultiPartDeleteCtx()
  {}

  virtual void destroy() override
  {
    ObTableModifyCtx::destroy();
    ObMultiDMLCtx::destroy_ctx();
  }
};

ObMultiPartDelete::ObMultiPartDelete(ObIAllocator& allocator) : ObTableModify(allocator), ObMultiDMLInfo(allocator)
{}

ObMultiPartDelete::~ObMultiPartDelete()
{}

int ObMultiPartDelete::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObMultiPartDeleteCtx* delete_ctx = NULL;
  if (OB_FAIL(ObTableModify::inner_open(ctx))) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(delete_ctx = GET_PHY_OPERATOR_CTX(ObMultiPartDeleteCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get multi table update context from exec_ctx failed", K(get_id()));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(shuffle_delete_row(ctx, got_row))) {
    LOG_WARN("shuffle delete row failed", K(ret));
  } else if (!got_row) {
    // do nothing
  } else if (OB_FAIL(ObTableModify::extend_dml_stmt(ctx, table_dml_infos_, delete_ctx->table_dml_ctxs_))) {
    LOG_WARN("extend dml stmt failed", K(ret));
  } else if (OB_FAIL(delete_ctx->multi_dml_plan_mgr_.build_multi_part_dml_task())) {
    LOG_WARN("build multi partition dml task failed", K(ret));
  } else {
    ObIArray<ObTaskInfo*>& task_info_list = delete_ctx->multi_dml_plan_mgr_.get_mini_task_infos();
    const ObMiniJob& subplan_job = delete_ctx->multi_dml_plan_mgr_.get_subplan_job();
    bool table_need_first = delete_ctx->multi_dml_plan_mgr_.table_need_first();
    ObMiniTaskResult result;
    ObIsMultiDMLGuard guard(*ctx.get_physical_plan_ctx());
    if (OB_FAIL(delete_ctx->mini_task_executor_.execute(ctx, subplan_job, task_info_list, table_need_first, result))) {
      LOG_WARN("execute multi table dml task failed", K(ret));
    }
    UNUSED(result);
  }
  if (OB_SUCC(ret) && is_returning()) {
    delete_ctx->returning_row_iterator_ = delete_ctx->returning_row_store_.begin();
  }
  return ret;
}

int ObMultiPartDelete::inner_close(ObExecContext& ctx) const
{
  int wait_ret = wait_all_task(GET_PHY_OPERATOR_CTX(ObMultiPartDeleteCtx, ctx, get_id()), ctx.get_physical_plan_ctx());
  int close_ret = ObTableModify::inner_close(ctx);
  if (OB_SUCCESS != wait_ret || OB_SUCCESS != close_ret) {
    LOG_WARN("inner close failed", K(wait_ret), K(close_ret));
  }
  return (OB_SUCCESS == close_ret) ? wait_ret : close_ret;
}

int ObMultiPartDelete::shuffle_delete_row(ObExecContext& ctx, bool& got_row) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObMultiPartDeleteCtx* delete_ctx = NULL;
  const ObNewRow* old_row = NULL;
  ObTaskExecutorCtx* task_ctx = NULL;
  ObSqlCtx* sql_ctx = NULL;
  got_row = false;
  CK(OB_NOT_NULL(plan_ctx = ctx.get_physical_plan_ctx()));
  CK(OB_NOT_NULL(delete_ctx = GET_PHY_OPERATOR_CTX(ObMultiPartDeleteCtx, ctx, get_id())));
  CK(OB_NOT_NULL(task_ctx = ctx.get_task_executor_ctx()));
  CK(OB_NOT_NULL(sql_ctx = ctx.get_sql_ctx()));
  CK(OB_NOT_NULL(sql_ctx->schema_guard_));
  CK(OB_NOT_NULL(get_phy_plan()));
  while (OB_SUCC(ret) && OB_SUCC(inner_get_next_row(ctx, old_row))) {
    for (int64_t k = 0; OB_SUCC(ret) && k < table_dml_infos_.count(); ++k) {
      const ObArrayWrap<ObGlobalIndexDMLInfo>& global_index_infos = table_dml_infos_.at(k).index_infos_;
      ObArrayWrap<ObGlobalIndexDMLCtx>& global_index_ctxs = delete_ctx->table_dml_ctxs_.at(k).index_ctxs_;
      ObTableModify* sub_delete = global_index_ctxs.at(0).dml_subplans_.at(DELETE_OP).subplan_root_;
      CK(OB_NOT_NULL(sub_delete));
      CK(OB_NOT_NULL(old_row));
      CK(!global_index_infos.empty());
      CK(!global_index_ctxs.empty());
      if (is_returning()) {
        OZ(save_returning_row(
            delete_ctx->expr_ctx_, *old_row, delete_ctx->returning_row_, delete_ctx->returning_row_store_));
      }
      if (OB_SUCC(ret)) {
        bool is_null = false;
        ObNewRow primary_row = *old_row;
        const DMLSubPlanArray& primary_dml_subplans = global_index_ctxs.at(0).dml_subplans_;
        primary_row.projector_ = primary_dml_subplans.at(DELETE_OP).value_projector_;
        primary_row.projector_size_ = primary_dml_subplans.at(DELETE_OP).value_projector_size_;
        if (table_dml_infos_.at(k).need_check_filter_null_) {
          if (OB_FAIL(check_rowkey_is_null(primary_row, table_dml_infos_.at(k).rowkey_cnt_, is_null))) {
            LOG_WARN("check rowkey is null failed", K(ret), K(primary_row), K(table_dml_infos_), K(k));
          } else if (is_null) {
            continue;
          }
        } else {
#if !defined(NDEBUG)
          if (need_check_pk_is_null()) {
            if (OB_FAIL(check_rowkey_is_null(primary_row, table_dml_infos_.at(k).rowkey_cnt_, is_null))) {
              LOG_WARN("check rowkey is null failed", K(ret), K(primary_row), K(table_dml_infos_), K(k));
            } else if (is_null) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("delete row failed validity check", K(ret));
            }
          }
#endif
        }
        if (OB_SUCC(ret)) {
          bool is_distinct = false;
          if (OB_FAIL(check_rowkey_whether_distinct(ctx,
                  primary_row,
                  table_dml_infos_.at(k).rowkey_cnt_,
                  table_dml_infos_.at(k).distinct_algo_,
                  delete_ctx->table_dml_ctxs_.at(k).rowkey_dist_ctx_,
                  is_distinct))) {
            LOG_WARN("check rowkey whether distinct failed", K(ret));
          } else if (!is_distinct) {
            continue;
          }
        }
      }
      OZ(ForeignKeyHandle::do_handle_old_row(
          global_index_ctxs.at(0).dml_subplans_.at(DELETE_OP).subplan_root_, *delete_ctx, *old_row));
      for (int64_t i = 0; OB_SUCC(ret) && i < global_index_infos.count(); ++i) {
        const ObTableLocation* tbl_loc = NULL;
        ObIArray<int64_t>& part_ids = global_index_ctxs.at(i).partition_ids_;
        const DMLSubPlanArray& dml_subplans = global_index_ctxs.at(i).dml_subplans_;
        int64_t part_idx = OB_INVALID_INDEX;
        CK(1 == global_index_infos.at(i).table_locs_.count());
        CK(DML_OP_CNT == dml_subplans.count());
        CK(OB_NOT_NULL(tbl_loc = global_index_infos.at(i).table_locs_.at(0)));
        OZ(tbl_loc->calculate_partition_ids_by_row(ctx, sql_ctx->schema_guard_, *old_row, part_ids, part_idx));
        if (OB_SUCC(ret)) {
          ObNewRow cur_row = *old_row;
          cur_row.projector_ = dml_subplans.at(DELETE_OP).value_projector_;
          cur_row.projector_size_ = dml_subplans.at(DELETE_OP).value_projector_size_;
          OZ(delete_ctx->multi_dml_plan_mgr_.add_part_row(k, i, part_idx, DELETE_OP, cur_row));
          if (OB_SUCC(ret)) {
            LOG_DEBUG("shuffle delete row", K(part_ids), K(part_idx), K(dml_subplans), K(cur_row));
          }
        }
      }  // for (int64_t i = 0; OB_SUCC(ret) && i < global_index_infos.count(); ++i)
    }
    got_row = true;
    plan_ctx->add_affected_rows(1);
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("get next row failed", K(ret));
  }
  return ret;
}

int ObMultiPartDelete::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  ObMultiPartDeleteCtx* delete_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObMultiPartDeleteCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create phy operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else {
    delete_ctx = static_cast<ObMultiPartDeleteCtx*>(op_ctx);
    if (OB_FAIL(delete_ctx->init_multi_dml_ctx(ctx, table_dml_infos_, get_phy_plan(), subplan_root_))) {
      LOG_WARN("init multi dml ctx failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(delete_ctx)) {
    const ObTableDMLInfo& first_dml_info = table_dml_infos_.at(0);
    const ObGlobalIndexDMLInfo& first_index_dml_info = first_dml_info.index_infos_.at(0);
    const ObTableModify* first_sub_delete = first_index_dml_info.dml_subplans_.at(DELETE_OP).subplan_root_;
  }
  if (OB_SUCC(ret) && is_returning()) {
    OZ(delete_ctx->alloc_row_cells(projector_size_, delete_ctx->returning_row_));
  }
  return ret;
}

int ObMultiPartDelete::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  return child_op_->get_next_row(ctx, row);
}

int ObMultiPartDelete::get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObMultiPartDeleteCtx* delete_ctx = NULL;
  ObNewRow* return_row = NULL;
  if (!is_returning()) {
    ret = ObTableModify::get_next_row(ctx, row);
  } else if (OB_ISNULL(delete_ctx = GET_PHY_OPERATOR_CTX(ObMultiPartDeleteCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid params", K(ret), K(delete_ctx), K(is_returning()));
  } else if (OB_FAIL(delete_ctx->returning_row_iterator_.get_next_row(return_row, NULL))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else {
    row = return_row;
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
