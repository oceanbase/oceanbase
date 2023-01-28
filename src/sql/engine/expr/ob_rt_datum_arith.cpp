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

#include "ob_rt_datum_arith.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{

ObRTDatumArith::Item ObRTDatumArith::Item::operator+(const ObRTDatumArith::Item &r)
{
  return NULL == arith_ ? ObRTDatumArith::Item() : arith_->build_arith_expr(*this, r, T_OP_ADD);
}

ObRTDatumArith::Item ObRTDatumArith::Item::operator-(const ObRTDatumArith::Item &r)
{
  return NULL == arith_ ? ObRTDatumArith::Item() : arith_->build_arith_expr(*this, r, T_OP_MINUS);
}

ObRTDatumArith::Item ObRTDatumArith::Item::operator*(const ObRTDatumArith::Item &r)
{
  return NULL == arith_ ? ObRTDatumArith::Item() : arith_->build_arith_expr(*this, r, T_OP_MUL);
}

ObRTDatumArith::Item ObRTDatumArith::Item::operator/(const ObRTDatumArith::Item &r)
{
  return NULL == arith_ ? ObRTDatumArith::Item() : arith_->build_arith_expr(*this, r, T_OP_DIV);
}


ObRTDatumArith::ObRTDatumArith(ObExecContext &ctx, ObSQLSessionInfo &session)
    : alloc_("RTDatumArith", OB_MALLOC_NORMAL_BLOCK_SIZE, session.get_effective_tenant_id()),
    exec_ctx_(ctx), session_(session), factory_(alloc_), frame_info_(alloc_),
    eval_ctx_(NULL), expr_(NULL), raw_cols_(alloc_), cols_(alloc_)
{
}

ObRTDatumArith::~ObRTDatumArith()
{
  if (NULL != eval_ctx_) {
    eval_ctx_->~ObEvalCtx();
  }
}

int ObRTDatumArith::inner_setup_datum_metas(const ObDatumMeta *metas, int64_t cnt)
{
  int ret = OB_SUCCESS;
  CK(NULL != metas && cnt > 0);
  OZ(raw_cols_.init(cnt));
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt; i++) {
    ObColumnRefRawExpr *col = NULL;
    OZ(factory_.create_raw_expr(T_REF_COLUMN, col));
    CK(NULL != col);
    if (OB_SUCC(ret)) {
      ObExprResType &res_type = const_cast<ObExprResType &>(col->get_result_type());
      res_type.set_type(metas[i].type_);
      res_type.set_collation_type(metas[i].cs_type_);
      res_type.set_scale(metas[i].scale_);
      res_type.set_precision(metas[i].precision_);
      OZ(raw_cols_.push_back(col));
    }
  }
  return ret;
}

ObRTDatumArith::Item ObRTDatumArith::ref(const int64_t input_idx)
{
  int ret = OB_SUCCESS;
  Item item;
  if (input_idx >= 0 && input_idx < raw_cols_.count()) {
    item.expr_ = raw_cols_.at(input_idx);
    item.arith_ = this;
  } else {
    ret = OB_INVALID_INDEX;
    LOG_WARN("invalid ref index", K(ret), K(input_idx), K(raw_cols_.count()));
  }
  return item;
}

ObRTDatumArith::Item ObRTDatumArith::build_arith_expr(const Item &l, const Item &r, ObItemType op)
{
  int ret = OB_SUCCESS;
  Item item;
  if (NULL != l.arith_ && NULL != l.expr_
      && NULL != r.arith_ && NULL != r.expr_) {
    ObOpRawExpr *expr = NULL;
    OZ(factory_.create_raw_expr(op, expr));
    CK(NULL != expr);
    OZ(expr->add_param_expr(l.expr_));
    OZ(expr->add_param_expr(r.expr_));

    item.arith_ = this;
    item.expr_ = expr;
  }
  return item;
}

int ObRTDatumArith::generate(const ObRTDatumArith::Item item)
{
  int ret = OB_SUCCESS;
  if (NULL == item.expr_ || NULL == item.arith_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("invalid ref index or build arith raw expr failed", K(ret));
  } else {
    CK(exec_ctx_.get_physical_plan_ctx());
    OZ(item.expr_->formalize(&session_));

    ObStaticEngineExprCG expr_cg(alloc_,
                                 &session_,
                                 NULL /* schema_guard */,
                                 0 /* original_param_cnt*/,
                                 0 /* param_cnt */,
                                 exec_ctx_.get_min_cluster_version());
    ObRawExprUniqueSet raw_exprs(false);
    OZ(raw_exprs.append(item.expr_));
    OZ(raw_exprs.append(raw_cols_));
    OZ(expr_cg.generate(raw_exprs, frame_info_));

    OZ(cols_.init(raw_cols_.count()));
    FOREACH_CNT_X(e, raw_cols_, OB_SUCC(ret)) {
      CK(NULL != (*e)->rt_expr_);
      cols_.push_back((*e)->rt_expr_);
    }

    if (OB_SUCC(ret)) {
      expr_ = item.expr_->rt_expr_;
      CK(NULL != expr_);
    }

    char **frames = NULL;
    uint64_t frame_cnt = 0;

    ObPhysicalPlanCtx *phy_ctx = exec_ctx_.get_physical_plan_ctx();
    OZ(frame_info_.alloc_frame(alloc_,
                               phy_ctx->get_param_frame_ptrs(),
                               frame_cnt,
                               frames));
    CK(NULL != frames);
    if (OB_SUCC(ret)) {
      eval_ctx_ = OB_NEWx(ObEvalCtx, (&alloc_), exec_ctx_);
      OV(NULL != eval_ctx_, OB_ALLOCATE_MEMORY_FAILED);
      if (OB_SUCC(ret)) {
        // overwrite the frames of eval_ctx
        eval_ctx_->frames_ = frames;
      }
    }
  }
  return ret;
}

// make the clear evaluate flag function be a static function to avoid abusing.
static void clear_arith_eval_flag(const ObExpr &expr, ObEvalCtx &ctx)
{
  if (expr.eval_func_ != NULL) {
    expr.get_eval_info(ctx).clear_evaluated_flag();
    for (int64_t i = 0; i < expr.arg_cnt_; i++) {
      clear_arith_eval_flag(*expr.args_[i], ctx);
    }
  }
}

int ObRTDatumArith::inner_eval(ObDatum *&res, const ObDatum **args, int64_t arg_cnt)
{
  int ret = OB_SUCCESS;
  CK(NULL != eval_ctx_);
  CK(NULL != args && arg_cnt == cols_.count());

  if (OB_SUCC(ret)) {
    // clear evaluate flags first
    clear_arith_eval_flag(*expr_, *eval_ctx_);
    for (int64_t i = 0; i < arg_cnt; i++) {
      cols_.at(i)->locate_expr_datum(*eval_ctx_) = *args[i];
    }
    OZ(expr_->eval(*eval_ctx_, res));
  }

  return ret;
}

} // end namespace sql
} // end namespace oceanbase
