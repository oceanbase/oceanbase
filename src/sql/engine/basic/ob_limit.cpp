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
#include "sql/engine/basic/ob_limit.h"
#include "lib/utility/utility.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_exec_context.h"
namespace oceanbase {
using namespace common;
namespace sql {
// REGISTER_PHY_OPERATOR(ObLimit, PHY_LIMIT);

class ObLimit::ObLimitCtx : public ObPhyOperatorCtx {
public:
  explicit ObLimitCtx(ObExecContext& ctx)
      : ObPhyOperatorCtx(ctx),
        limit_(-1),
        offset_(0),
        input_count_(0),
        output_count_(0),
        total_count_(0),
        is_percent_first_(false),
        limit_last_row_(NULL)
  {}
  virtual void destroy()
  {
    ObPhyOperatorCtx::destroy_base();
  }

private:
  int64_t limit_;
  int64_t offset_;
  int64_t input_count_;
  int64_t output_count_;
  int64_t total_count_;
  bool is_percent_first_;
  ObNewRow* limit_last_row_;

  friend class ObLimit;
};

ObLimit::ObLimit(ObIAllocator& alloc)
    : ObSingleChildPhyOperator(alloc),
      org_limit_(NULL),
      org_offset_(NULL),
      org_percent_(NULL),
      is_calc_found_rows_(false),
      is_top_limit_(false),
      is_fetch_with_ties_(false),
      sort_columns_(alloc)
{}

ObLimit::~ObLimit()
{
  reset();
}

void ObLimit::reset()
{
  org_limit_ = NULL;
  org_offset_ = NULL;
  org_percent_ = NULL;
  is_calc_found_rows_ = false;
  is_top_limit_ = false;
  is_fetch_with_ties_ = false;
  sort_columns_.reset();
  ObSingleChildPhyOperator::reset();
}

void ObLimit::reuse()
{
  reset();
}

int ObLimit::set_limit(ObSqlExpression* limit, ObSqlExpression* offset, ObSqlExpression* percent)
{
  int ret = OB_SUCCESS;
  if (limit) {
    org_limit_ = limit;
  }
  if (offset) {
    org_offset_ = offset;
  }
  if (percent) {
    org_percent_ = percent;
  }
  return ret;
}

int ObLimit::get_int_value(
    ObExecContext& ctx, const ObSqlExpression* in_val, int64_t& out_val, bool& is_null_value) const
{
  int ret = OB_SUCCESS;
  ObNewRow input_row;
  ObObj result;
  ObExprCtx expr_ctx;
  is_null_value = false;
  if (in_val != NULL && !in_val->is_empty()) {
    if (OB_FAIL(wrap_expr_ctx(ctx, expr_ctx))) {
      LOG_WARN("wrap expr context failed", K(ret));
    } else if (OB_FAIL(in_val->calc(expr_ctx, input_row, result))) {
      LOG_WARN("Failed to calculate expression", K(ret));
    } else if (result.is_int()) {
      if (OB_FAIL(result.get_int(out_val))) {
        LOG_WARN("get_int error", K(ret), K(result));
      }
    } else if (result.is_null()) {
      out_val = 0;
      is_null_value = true;
    } else {
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      EXPR_GET_INT64_V2(result, out_val);
      if (OB_FAIL(ret)) {
        LOG_WARN("get_int error", K(ret), K(result));
      }
    }
  }
  return ret;
}

int ObLimit::get_double_value(ObExecContext& ctx, const ObSqlExpression* double_val, double& out_val) const
{
  int ret = OB_SUCCESS;
  ObNewRow input_row;
  ObObj result;
  ObExprCtx expr_ctx;
  if (double_val != NULL && !double_val->is_empty()) {
    if (OB_FAIL(wrap_expr_ctx(ctx, expr_ctx))) {
      LOG_WARN("wrap expr context failed", K(ret));
    } else if (OB_FAIL(double_val->calc(expr_ctx, input_row, result))) {
      LOG_WARN("Failed to calculate expression", K(ret));
    } else if (result.is_double()) {
      if (OB_FAIL(result.get_double(out_val))) {
        LOG_WARN("get_double error", K(ret), K(result));
      }
    } else if (result.is_null()) {
      out_val = 0.0;
    } else {
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      EXPR_GET_DOUBLE_V2(result, out_val);
      if (OB_FAIL(ret)) {
        LOG_WARN("get_double error", K(ret), K(result));
      }
    }
  }
  return ret;
}

int ObLimit::get_limit(ObExecContext& ctx, int64_t& limit, int64_t& offset, bool& is_percent_first) const
{
  int ret = OB_SUCCESS;
  double percent = 0.0;
  bool is_null_value = false;
  limit = -1;
  offset = 0;
  if (OB_FAIL(get_int_value(ctx, org_limit_, limit, is_null_value))) {
    LOG_WARN("Get limit value failed", K(ret));
  } else if (!is_null_value && OB_FAIL(get_int_value(ctx, org_offset_, offset, is_null_value))) {
    LOG_WARN("Get offset value failed", K(ret));
  } else if (is_null_value) {
    offset = 0;
    limit = 0;
  } else {
    is_percent_first = (org_percent_ != NULL && !org_percent_->is_empty());
    // revise limit, offset because rownum < -1 is rewritten as limit -1
    // offset 2 rows fetch next -3 rows only --> is meaningless
    offset = offset < 0 ? 0 : offset;
    if (org_limit_ != NULL && !org_limit_->is_empty()) {
      limit = limit < 0 ? 0 : limit;
    }
  }
  return ret;
}

bool ObLimit::is_valid() const
{
  return (get_column_count() > 0 && child_op_ != NULL && child_op_->get_column_count() > 0);
}

int ObLimit::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObLimitCtx* limit_ctx = NULL;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("limit operator is invalid");
  } else if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else if (OB_ISNULL(limit_ctx = GET_PHY_OPERATOR_CTX(ObLimitCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (OB_FAIL(get_limit(ctx, limit_ctx->limit_, limit_ctx->offset_, limit_ctx->is_percent_first_))) {
    LOG_WARN("Failed to instantiate limit/offset", K(ret));
  } else {
  }
  return ret;
}

int ObLimit::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObLimitCtx* limit_ctx = NULL;
  if (OB_FAIL(ObSingleChildPhyOperator::rescan(ctx))) {
    LOG_WARN("rescan child physical operator failed", K(ret));
  } else if (OB_ISNULL(limit_ctx = GET_PHY_OPERATOR_CTX(ObLimitCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("limit_ctx is null");
  } else {
    limit_ctx->input_count_ = 0;
    limit_ctx->output_count_ = 0;
  }
  return ret;
}

int ObLimit::inner_close(ObExecContext& ctx) const
{
  UNUSED(ctx);
  return OB_SUCCESS;
}

int ObLimit::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObLimitCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("failed to create LimitCtx", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, need_copy_row_for_compute()))) {
    LOG_WARN("init current row failed", K(ret));
  }
  return ret;
}

int ObLimit::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObLimitCtx* limit_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  const ObNewRow* input_row = NULL;

  if (OB_FAIL(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("limit operator is invalid");
  } else if (OB_ISNULL(limit_ctx = GET_PHY_OPERATOR_CTX(ObLimitCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator ctx failed");
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else {
  }
  while (OB_SUCC(ret) && limit_ctx->input_count_ < limit_ctx->offset_) {
    if (OB_FAIL(child_op_->get_next_row(ctx, input_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("child_op failed to get next row", K(limit_ctx->input_count_), K(limit_ctx->offset_), K(ret));
      }
    } else if (limit_ctx->is_percent_first_ && OB_FAIL(convert_limit_percent(ctx, limit_ctx))) {
      LOG_WARN("failed to convert limit percent", K(ret));
    } else {
      ++limit_ctx->input_count_;
    }
  }  // end while

  /*
   * 1. is_percent_first_: for 'select * from t1 fetch next 50 percent rows only',
   *    need set lower block operators like sort or agg, and then reset is_percent_first_ to false.
   * 2. is_fetch_with_ties_: when we get enough rows as limit count, shall we keep fetching for
   *    those rows which equal to the last row of specified order (by order by clause) ?
   */
  int64_t left_count = 0;
  if (OB_SUCC(ret)) {
    if (limit_ctx->is_percent_first_ || limit_ctx->output_count_ < limit_ctx->limit_ || limit_ctx->limit_ < 0) {
      if (OB_FAIL(child_op_->get_next_row(ctx, input_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("child_op failed to get next row",
              K(ret),
              K_(limit_ctx->limit),
              K_(limit_ctx->offset),
              K_(limit_ctx->input_count),
              K_(limit_ctx->output_count));
        }
      } else if (limit_ctx->is_percent_first_ && OB_FAIL(convert_limit_percent(ctx, limit_ctx))) {
        LOG_WARN("failed to convert limit percent", K(ret));
      } else if (limit_ctx->limit_ == 0) {
        ret = OB_ITER_END;
      } else {
        ++limit_ctx->output_count_;
        row = input_row;
        if (is_fetch_with_ties_ && limit_ctx->output_count_ == limit_ctx->limit_ &&
            OB_FAIL(deep_copy_limit_last_rows(limit_ctx, *row))) {
          LOG_WARN("failed to deep copy limit last rows");
        } else if (OB_FAIL(copy_cur_row(*limit_ctx, row))) {
          LOG_WARN("copy current row failed", K(ret));
        } else { /*do nothing*/
        }
      }
    } else if (limit_ctx->limit_ > 0 && is_fetch_with_ties_) {
      bool is_equal = false;
      if (OB_FAIL(child_op_->get_next_row(ctx, input_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("child_op failed to get next row",
              K(ret),
              K_(limit_ctx->limit),
              K_(limit_ctx->offset),
              K_(limit_ctx->input_count),
              K_(limit_ctx->output_count));
        }
      } else if (OB_FAIL(is_row_order_by_item_value_equal(limit_ctx, input_row, is_equal))) {
        LOG_WARN("failed to is row order by item value equal", K(ret));
      } else if (is_equal) {
        ++limit_ctx->output_count_;
        row = input_row;
        if (OB_FAIL(copy_cur_row(*limit_ctx, row))) {
          LOG_WARN("copy current row failed", K(ret));
        } else {
          LOG_DEBUG("copy cur row", K(*row));
        }
      } else {
        ret = OB_ITER_END;
      }
    } else {
      ret = OB_ITER_END;
      if (is_calc_found_rows_) {
        const ObNewRow* tmp_row = NULL;
        while (OB_SUCC(child_op_->get_next_row(ctx, tmp_row))) {
          ++left_count;
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row from child", K(ret));
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    if (is_top_limit_) {
      limit_ctx->total_count_ = left_count + limit_ctx->output_count_ + limit_ctx->input_count_;
      ObPhysicalPlanCtx* plan_ctx = NULL;
      if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("get physical plan context failed");
      } else {
        NG_TRACE_EXT(
            found_rows, OB_ID(total_count), limit_ctx->total_count_, OB_ID(input_count), limit_ctx->input_count_);
        plan_ctx->set_found_rows(limit_ctx->total_count_);
      }
    }
  }
  return ret;
}

int ObLimit::deep_copy_limit_last_rows(ObLimitCtx* limit_ctx, const ObNewRow row) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(limit_ctx) || OB_ISNULL(limit_ctx->calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(limit_ctx));
  } else {
    const int64_t buf_len = sizeof(ObNewRow) + row.get_deep_copy_size();
    int64_t pos = sizeof(ObNewRow);
    char* buf = NULL;
    if (OB_ISNULL(buf = static_cast<char*>(limit_ctx->calc_buf_->alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc new row failed", K(ret), K(buf_len));
    } else if (OB_ISNULL(limit_ctx->limit_last_row_ = new (buf) ObNewRow())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new_row is null", K(ret), K(buf_len));
    } else if (OB_FAIL(limit_ctx->limit_last_row_->deep_copy(row, buf, buf_len, pos))) {
      LOG_WARN("deep copy row failed", K(ret), K(buf_len), K(pos));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLimit::is_row_order_by_item_value_equal(ObLimitCtx* limit_ctx, const ObNewRow* input_row, bool& is_equal) const
{
  int ret = OB_SUCCESS;
  ObNewRow* limit_last_row = NULL;
  is_equal = false;
  if (OB_ISNULL(limit_ctx) || OB_ISNULL(limit_last_row = limit_ctx->limit_last_row_) || OB_ISNULL(input_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(limit_ctx), K(input_row), K(limit_last_row));
  } else {
    is_equal = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < sort_columns_.count(); ++i) {
      if (OB_UNLIKELY(sort_columns_.at(i).index_ >= input_row->count_ ||
                      sort_columns_.at(i).index_ >= limit_last_row->count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("order by projector is invalid",
            K(ret),
            K(sort_columns_.at(i).index_),
            K(input_row->count_),
            K(limit_last_row->count_));
      } else {
        is_equal = 0 == input_row->cells_[sort_columns_.at(i).index_].compare(
                            limit_last_row->cells_[sort_columns_.at(i).index_], sort_columns_.at(i).cs_type_);
      }
    }
  }
  return ret;
}

int64_t ObLimit::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (org_limit_ && org_offset_) {
    J_KV(N_LIMIT, org_limit_, N_OFFSET, org_offset_);
  } else if (org_limit_) {
    J_KV(N_LIMIT, org_limit_);
  } else if (org_offset_) {
    J_KV(N_OFFSET, org_offset_);
  }
  return pos;
}

int ObLimit::add_filter(ObSqlExpression* expr)
{
  UNUSED(expr);
  LOG_ERROR("limit operator should have no filter expr");
  return OB_NOT_SUPPORTED;
}

int ObLimit::add_sort_columns(ObSortColumn sort_column)
{
  return sort_columns_.push_back(sort_column);
}

int ObLimit::convert_limit_percent(ObExecContext& ctx, ObLimitCtx* limit_ctx) const
{
  int ret = OB_SUCCESS;
  double percent = 0.0;
  if (OB_ISNULL(limit_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(limit_ctx));
  } else if (OB_FAIL(get_double_value(ctx, org_percent_, percent))) {
    LOG_WARN("failed to get double value", K(ret));
  } else if (percent > 0) {
    int64_t tot_count = 0;
    if (OB_UNLIKELY(limit_ctx->limit_ != -1) || OB_ISNULL(child_op_) ||
        OB_UNLIKELY(child_op_->get_type() != PHY_MATERIAL && child_op_->get_type() != PHY_SORT)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(limit_ctx->limit_), K(child_op_));
    } else if (child_op_->get_type() == PHY_MATERIAL &&
               OB_FAIL(static_cast<ObMaterial*>(child_op_)->get_material_row_count(ctx, tot_count))) {
      LOG_WARN("failed to get op row count", K(ret));
    } else if (child_op_->get_type() == PHY_SORT &&
               OB_FAIL(static_cast<ObSort*>(child_op_)->get_sort_row_count(ctx, tot_count))) {
      LOG_WARN("failed to get op row count", K(ret));
    } else if (OB_UNLIKELY(tot_count < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid child op row count", K(tot_count), K(ret));
    } else if (percent < 100) {
      int64_t percent_int64 = static_cast<int64_t>(percent);
      int64_t offset = (tot_count * percent / 100 - tot_count * percent_int64 / 100) > 0 ? 1 : 0;
      limit_ctx->limit_ = tot_count * percent_int64 / 100 + offset;
      limit_ctx->is_percent_first_ = false;
    } else {
      limit_ctx->limit_ = tot_count;
      limit_ctx->is_percent_first_ = false;
    }
  } else {
    limit_ctx->limit_ = 0;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObLimit)
{
  int ret = OB_SUCCESS;
  bool has_limit_count = (org_limit_ != NULL && !org_limit_->is_empty());
  bool has_limit_offset = (org_offset_ != NULL && !org_offset_->is_empty());
  bool has_limit_percent = (org_percent_ != NULL && !org_percent_->is_empty());

  OB_UNIS_ENCODE(is_calc_found_rows_);
  OB_UNIS_ENCODE(has_limit_count);
  OB_UNIS_ENCODE(has_limit_offset);
  OB_UNIS_ENCODE(is_top_limit_);
  if (has_limit_count) {
    OB_UNIS_ENCODE(*org_limit_);
  }
  if (has_limit_offset) {
    OB_UNIS_ENCODE(*org_offset_);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObSingleChildPhyOperator::serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize child operator failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_ENCODE(has_limit_percent);
    OB_UNIS_ENCODE(is_fetch_with_ties_);
    OB_UNIS_ENCODE(sort_columns_);
    if (has_limit_percent) {
      OB_UNIS_ENCODE(*org_percent_);
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLimit)
{
  int64_t len = 0;
  bool has_limit_count = (org_limit_ != NULL && !org_limit_->is_empty());
  bool has_limit_offset = (org_offset_ != NULL && !org_offset_->is_empty());
  bool has_limit_percent = (org_percent_ != NULL && !org_percent_->is_empty());

  OB_UNIS_ADD_LEN(is_calc_found_rows_);
  OB_UNIS_ADD_LEN(has_limit_count);
  OB_UNIS_ADD_LEN(has_limit_offset);
  OB_UNIS_ADD_LEN(is_top_limit_);
  if (has_limit_count) {
    OB_UNIS_ADD_LEN(*org_limit_);
  }
  if (has_limit_offset) {
    OB_UNIS_ADD_LEN(*org_offset_);
  }
  len += ObSingleChildPhyOperator::get_serialize_size();
  OB_UNIS_ADD_LEN(has_limit_percent);
  OB_UNIS_ADD_LEN(is_fetch_with_ties_);
  OB_UNIS_ADD_LEN(sort_columns_);
  if (has_limit_percent) {
    OB_UNIS_ADD_LEN(*org_percent_);
  }
  return len;
}

OB_DEF_DESERIALIZE(ObLimit)
{
  int ret = OB_SUCCESS;
  bool has_limit_count = false;
  bool has_limit_offset = false;
  bool has_limit_percent = false;

  OB_UNIS_DECODE(is_calc_found_rows_);
  OB_UNIS_DECODE(has_limit_count);
  OB_UNIS_DECODE(has_limit_offset);
  OB_UNIS_DECODE(is_top_limit_);
  if (OB_SUCC(ret) && has_limit_count) {
    if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, org_limit_))) {
      LOG_WARN("make sql expression failed", K(ret));
    } else if (OB_LIKELY(org_limit_ != NULL)) {
      OB_UNIS_DECODE(*org_limit_);
    }
  }
  if (OB_SUCC(ret) && has_limit_offset) {
    if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, org_offset_))) {
      LOG_WARN("make sql expression failed", K(ret));
    } else if (OB_LIKELY(org_offset_ != NULL)) {
      OB_UNIS_DECODE(*org_offset_);
    }
  }
  if (OB_SUCC(ret)) {
    ret = ObSingleChildPhyOperator::deserialize(buf, data_len, pos);
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(has_limit_percent);
    OB_UNIS_DECODE(is_fetch_with_ties_);
    OB_UNIS_DECODE(sort_columns_);
    if (OB_SUCC(ret) && has_limit_percent) {
      if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, org_percent_))) {
        LOG_WARN("make sql expression failed", K(ret));
      } else if (OB_LIKELY(org_percent_ != NULL)) {
        OB_UNIS_DECODE(*org_percent_);
      }
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
