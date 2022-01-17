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
#include "sql/engine/aggregate/ob_scalar_aggregate.h"
#include "common/row/ob_row.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {
class ObScalarAggregate::ObScalarAggregateCtx : public ObGroupByCtx {
public:
  explicit ObScalarAggregateCtx(ObExecContext& exec_ctx) : ObGroupByCtx(exec_ctx), started_(false)
  {}
  virtual void destroy()
  {
    ObGroupByCtx::destroy();
  }

private:
  bool started_;

  friend class ObScalarAggregate;
};

ObScalarAggregate::ObScalarAggregate(ObIAllocator& alloc) : ObGroupBy(alloc)
{}

ObScalarAggregate::~ObScalarAggregate()
{}

void ObScalarAggregate::reset()
{
  ObGroupBy::reset();
}

void ObScalarAggregate::reuse()
{
  reset();
}

int ObScalarAggregate::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(group_col_idxs_.count() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar aggregate has no group", K(ret));
  } else if (OB_FAIL(ObGroupBy::init_group_by(ctx))) {
    LOG_WARN("failed to init group by", K(ret));
  } else {
    ObScalarAggregateCtx* groupby_ctx = GET_PHY_OPERATOR_CTX(ObScalarAggregateCtx, ctx, get_id());
    if (OB_ISNULL(groupby_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("group by ctx is NULL", K(ret));
    } else {
      // treat scalar group by as a one group merge group by
      groupby_ctx->get_aggr_func().set_sort_based_gby();
    }
  }
  return ret;
}

int ObScalarAggregate::switch_iterator(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObScalarAggregateCtx* agg_ctx = NULL;
  if (OB_FAIL(ObGroupBy::switch_iterator(ctx))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("switch iterator ObGroupBy failed", K(ret));
    }
  } else if (OB_ISNULL(agg_ctx = GET_PHY_OPERATOR_CTX(ObScalarAggregateCtx, ctx, id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get scalar aggregate ctx failed");
  } else {
    agg_ctx->started_ = false;
    agg_ctx->aggr_func_.reuse();
  }
  return ret;
}

int ObScalarAggregate::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObScalarAggregateCtx* agg_ctx = NULL;
  if (OB_FAIL(ObGroupBy::rescan(ctx))) {
    LOG_WARN("rescan ObGroupBy failed", K(ret));
  } else if (OB_ISNULL(agg_ctx = GET_PHY_OPERATOR_CTX(ObScalarAggregateCtx, ctx, id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get scalar aggregate ctx failed");
  } else {
    agg_ctx->started_ = false;
    agg_ctx->aggr_func_.reuse();
  }
  return ret;
}

int ObScalarAggregate::inner_close(ObExecContext& ctx) const
{
  UNUSED(ctx);
  return OB_SUCCESS;  // ObGroupBy::close(ctx);
}

int ObScalarAggregate::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* input_row = NULL;
  ObScalarAggregateCtx* scalar_aggregate_ctx = NULL;
  // no group columns and only aggr functions
  if (OB_ISNULL(scalar_aggregate_ctx = GET_PHY_OPERATOR_CTX(ObScalarAggregateCtx, ctx, id_))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (OB_ISNULL(child_op_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("child operator is null");
  } else if (!scalar_aggregate_ctx->started_) {
    scalar_aggregate_ctx->started_ = true;
    if (OB_FAIL(child_op_->get_next_row(ctx, input_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      } else {
        // empty result, should return a row with info which indicates empty result set
        ret = scalar_aggregate_ctx->aggr_func_.get_result_for_empty_set(scalar_aggregate_ctx->get_cur_row());
      }
    } else if (OB_ISNULL(input_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input row is null");
    } else if ((OB_FAIL(scalar_aggregate_ctx->aggr_func_.prepare(*input_row)))) {
      LOG_WARN("fail to prepare the aggr func", K(ret), K(*input_row));
    } else {
      ObSQLSessionInfo* my_session = scalar_aggregate_ctx->exec_ctx_.get_my_session();
      const ObTimeZoneInfo* tz_info = (NULL != my_session) ? my_session->get_timezone_info() : NULL;
      while (OB_SUCC(ret) && OB_SUCC(child_op_->get_next_row(ctx, input_row))) {
        if (OB_FAIL(try_check_status(ctx))) {
          LOG_WARN("failed to check status", K(ret));
        } else if (OB_FAIL(scalar_aggregate_ctx->aggr_func_.process(*input_row, tz_info))) {
          LOG_WARN("fail to process the aggr func", K(ret), K(*input_row));
        } else if (mem_size_limit_ > 0 && mem_size_limit_ < scalar_aggregate_ctx->aggr_func_.get_used_mem_size()) {
          ret = OB_EXCEED_MEM_LIMIT;
          LOG_WARN("merge group by has exceeded the mem limit",
              K_(mem_size_limit),
              "mem_used",
              scalar_aggregate_ctx->aggr_func_.get_used_mem_size());
        }
      }
      if (OB_ITER_END == ret) {
        ret = scalar_aggregate_ctx->aggr_func_.get_result(scalar_aggregate_ctx->get_cur_row(), tz_info);
      }
    }
  } else {
    ret = OB_ITER_END;
  }
  if (OB_SUCC(ret)) {
    row = &scalar_aggregate_ctx->get_cur_row();
  }
  return ret;
}

int ObScalarAggregate::inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const
{
  return CREATE_PHY_OPERATOR_CTX(ObScalarAggregateCtx, ctx, id_, get_type(), op_ctx);
}

int64_t ObScalarAggregate::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(N_AGGR_COLUMN, aggr_columns_, N_MEM_LIMIT, mem_size_limit_);
  return pos;
}

int ObScalarAggregate::add_group_column_idx(int64_t column_idx, common::ObCollationType cs_type)
{
  UNUSED(column_idx);
  UNUSED(cs_type);
  return OB_NOT_IMPLEMENT;
}
}  // namespace sql
}  // namespace oceanbase
