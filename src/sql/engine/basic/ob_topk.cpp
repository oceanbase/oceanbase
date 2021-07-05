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
#include "sql/engine/basic/ob_topk.h"
#include "lib/utility/utility.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/sort/ob_sort.h"
#include "sql/engine/basic/ob_material.h"
#include "sql/engine/aggregate/ob_hash_groupby.h"
#include "sql/engine/ob_exec_context.h"
namespace oceanbase {
using namespace common;
namespace sql {

class ObTopK::ObTopKCtx : public ObPhyOperatorCtx {
public:
  explicit ObTopKCtx(ObExecContext& ctx) : ObPhyOperatorCtx(ctx), topk_final_count_(-1), output_count_(0)
  {}
  virtual void destroy()
  {
    ObPhyOperatorCtx::destroy_base();
  }

private:
  int64_t topk_final_count_;  // count of rows that need to be output upforward
  int64_t output_count_;
  friend class ObTopK;
};

ObTopK::ObTopK(ObIAllocator& alloc)
    : ObSingleChildPhyOperator(alloc), minimum_row_count_(-1), topk_precision_(-1), org_limit_(NULL), org_offset_(NULL)
{}

ObTopK::~ObTopK()
{
  reset();
}

void ObTopK::reset()
{
  org_limit_ = NULL;
  org_offset_ = NULL;
  minimum_row_count_ = -1;
  topk_precision_ = -1;
  ObSingleChildPhyOperator::reset();
}

void ObTopK::reuse()
{
  reset();
}

int ObTopK::set_topk_params(
    ObSqlExpression* limit, ObSqlExpression* offset, int64_t minimum_row_count, int64_t topk_precision)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(limit) || minimum_row_count < 0 || topk_precision < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KP(limit), K(minimum_row_count), K(topk_precision), K(ret));
  } else {
    org_limit_ = limit;
    org_offset_ = offset;
    minimum_row_count_ = minimum_row_count;
    topk_precision_ = topk_precision;
  }
  return ret;
}

int ObTopK::get_int_value(
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
    } else if (OB_LIKELY(result.is_int())) {
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

int ObTopK::get_topk_final_count(ObExecContext& ctx, int64_t& topk_final_count) const
{
  int ret = OB_SUCCESS;

  int64_t limit = -1;
  int64_t offset = 0;
  bool is_null_value = false;
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid plan ctx is NULL", K(ret));
  } else if (OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid child_op_ is NULL", K(ret));
  } else if (OB_FAIL(get_int_value(ctx, org_limit_, limit, is_null_value))) {
    LOG_WARN("Get limit value failed", K(ret));
  } else if (!is_null_value && OB_FAIL(get_int_value(ctx, org_offset_, offset, is_null_value))) {
    LOG_WARN("Get offset value failed", K(ret));
  } else {
    // revise limit, offset because rownum < -1 is rewritten as limit -1
    limit = (is_null_value || limit < 0) ? 0 : limit;
    offset = (is_null_value || offset < 0) ? 0 : offset;
    topk_final_count = std::max(minimum_row_count_, limit + offset);
    int64_t row_count = 0;
    ObPhyOperatorType op_type = child_op_->get_type();
    // TODO(): may be we should add one func to paremt class
    if (PHY_SORT == op_type) {
      ObSort* sort_op = static_cast<ObSort*>(child_op_);
      if (OB_ISNULL(sort_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("casted sort_op is NULL", K(ret));
      } else if (OB_FAIL(sort_op->get_sort_row_count(ctx, row_count))) {
        LOG_WARN("failed to get sort row count", K(ret));
      } else { /*do nothing*/
      }
    } else if (PHY_MATERIAL == op_type) {
      ObMaterial* material_op = static_cast<ObMaterial*>(child_op_);
      if (OB_ISNULL(material_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("casted material_op is NULL", K(ret));
      } else if (OB_FAIL(material_op->get_material_row_count(ctx, row_count))) {
        LOG_WARN("failed to get material row count", K(ret));
      } else { /*do nothing*/
      }
    } else if (PHY_HASH_GROUP_BY == op_type) {
      ObHashGroupBy* hash_groupby_op = static_cast<ObHashGroupBy*>(child_op_);
      if (OB_ISNULL(hash_groupby_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("casted hash_groupby_op is NULL", K(ret));
      } else if (OB_FAIL(hash_groupby_op->get_hash_groupby_row_count(ctx, row_count))) {
        LOG_WARN("failed to get material row count", K(ret));
      } else { /*do nothing*/
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid child_op_", K(op_type), K(ret));
    }

    if (OB_SUCC(ret)) {
      topk_final_count = std::max(topk_final_count, static_cast<int64_t>(row_count * topk_precision_ / 100));
      if (topk_final_count >= row_count) {
        plan_ctx->set_is_result_accurate(true);
      } else {
        plan_ctx->set_is_result_accurate(false);
      }
    }
  }
  return ret;
}

bool ObTopK::is_valid() const
{
  return (get_column_count() > 0 && (NULL != org_limit_) && (NULL != child_op_) && child_op_->get_column_count() > 0);
}

int ObTopK::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("limit operator is invalid");
  } else if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObTopK::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTopKCtx* topk_ctx = NULL;
  if (OB_FAIL(ObSingleChildPhyOperator::rescan(ctx))) {
    LOG_WARN("rescan child physical operator failed", K(ret));
  } else if (OB_ISNULL(topk_ctx = GET_PHY_OPERATOR_CTX(ObTopKCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("topk_ctx is null");
  } else {
    topk_ctx->output_count_ = 0;
  }
  return ret;
}

int ObTopK::inner_close(ObExecContext& ctx) const
{
  UNUSED(ctx);
  return OB_SUCCESS;
}

int ObTopK::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObTopKCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("failed to create TopKCtx", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, need_copy_row_for_compute()))) {
    LOG_WARN("init current row failed", K(ret));
  }
  return ret;
}

int ObTopK::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObTopKCtx* topk_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  const ObNewRow* input_row = NULL;

  if (OB_FAIL(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("limit operator is invalid");
  } else if (OB_ISNULL(topk_ctx = GET_PHY_OPERATOR_CTX(ObTopKCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator ctx failed");
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else {
    if (0 == topk_ctx->output_count_ || topk_ctx->output_count_ < topk_ctx->topk_final_count_) {
      if (OB_FAIL(child_op_->get_next_row(ctx, input_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN(
              "child_op failed to get next row", K_(topk_ctx->topk_final_count), K_(topk_ctx->output_count), K(ret));
        }
      } else {
        if (0 == topk_ctx->output_count_) {
          if (OB_FAIL(get_topk_final_count(ctx, topk_ctx->topk_final_count_))) {
            LOG_WARN("failed to get_topk_final_count", K(ret));
          } else if (OB_UNLIKELY(0 == topk_ctx->topk_final_count_)) {
            ret = OB_ITER_END;
          } else { /*do nothing*/
          }
        }
        if (OB_SUCC(ret)) {
          ++topk_ctx->output_count_;
          row = input_row;
          if (OB_FAIL(copy_cur_row(*topk_ctx, row))) {
            LOG_WARN("copy current row failed", K(ret));
          } else {
            LOG_DEBUG("copy cur row", K(*row));
          }
        }
      }
    } else {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int64_t ObTopK::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  // TODO():macro define N_TOPK_PRECISION
  if (org_limit_ && org_offset_) {
    J_KV(N_LIMIT,
        org_limit_,
        N_OFFSET,
        org_offset_,
        "minimum_row_count",
        minimum_row_count_,
        "topk_precision",
        topk_precision_);
  } else if (NULL != org_limit_) {
    J_KV(N_LIMIT, org_limit_, "minimum_row_count", minimum_row_count_, "topk_precision", topk_precision_);
  } else if (NULL != org_offset_) {
    J_KV(N_OFFSET, org_offset_, "minimum_row_count", minimum_row_count_, "topk_precision", topk_precision_);
  } else { /*do nothing*/
  }
  return pos;
}

int ObTopK::add_filter(ObSqlExpression* expr)
{
  UNUSED(expr);
  LOG_ERROR("limit operator should have no filter expr");
  return OB_NOT_SUPPORTED;
}

OB_DEF_SERIALIZE(ObTopK)
{
  int ret = OB_SUCCESS;
  bool has_limit_count = (org_limit_ != NULL && !org_limit_->is_empty());
  bool has_limit_offset = (org_offset_ != NULL && !org_offset_->is_empty());

  OB_UNIS_ENCODE(minimum_row_count_);
  OB_UNIS_ENCODE(topk_precision_);
  OB_UNIS_ENCODE(has_limit_count);
  OB_UNIS_ENCODE(has_limit_offset);
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
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTopK)
{
  int64_t len = 0;
  bool has_limit_count = (org_limit_ != NULL && !org_limit_->is_empty());
  bool has_limit_offset = (org_offset_ != NULL && !org_offset_->is_empty());

  OB_UNIS_ADD_LEN(minimum_row_count_);
  OB_UNIS_ADD_LEN(topk_precision_);
  OB_UNIS_ADD_LEN(has_limit_count);
  OB_UNIS_ADD_LEN(has_limit_offset);
  if (has_limit_count) {
    OB_UNIS_ADD_LEN(*org_limit_);
  }
  if (has_limit_offset) {
    OB_UNIS_ADD_LEN(*org_offset_);
  }
  len += ObSingleChildPhyOperator::get_serialize_size();
  return len;
}

OB_DEF_DESERIALIZE(ObTopK)
{
  int ret = OB_SUCCESS;
  bool has_limit_count = false;
  bool has_limit_offset = false;

  OB_UNIS_DECODE(minimum_row_count_);
  OB_UNIS_DECODE(topk_precision_);
  OB_UNIS_DECODE(has_limit_count);
  OB_UNIS_DECODE(has_limit_offset);

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
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
