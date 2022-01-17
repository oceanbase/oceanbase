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

#include "sql/engine/basic/ob_expr_values_op.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/engine/expr/ob_expr_type_to_str.h"
#include "sql/engine/px/ob_dfo.h"

namespace oceanbase {
using namespace common;
namespace sql {

OB_SERIALIZE_MEMBER(ObExprValuesOpInput, partition_id_values_);

int ObExprValuesSpec::serialize(char* buf, int64_t buf_len, int64_t& pos, ObPhyOpSeriCtx& seri_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t len = get_serialize_size_(seri_ctx);
  OB_UNIS_ENCODE(UNIS_VERSION);
  OB_UNIS_ENCODE(len);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObOpSpec::serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize physical operator failed", K(ret));
    } else {
      const ObIArray<int64_t>* row_id_list = static_cast<const ObIArray<int64_t>*>(seri_ctx.row_id_list_);
      int64_t col_num = get_output_count();
      if (row_id_list != NULL) {
        int64_t value_count = col_num * row_id_list->count();
        OB_UNIS_ENCODE(value_count);
        if (OB_SUCC(ret)) {
          ARRAY_FOREACH(*row_id_list, idx)
          {
            int64_t start_idx = row_id_list->at(idx) * col_num;
            int64_t end_idx = start_idx + col_num;
            for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
              OB_UNIS_ENCODE(values_.at(i));
            }
          }
        }
      } else if (OB_NOT_NULL(seri_ctx.exec_ctx_) && !seri_ctx.exec_ctx_->get_row_id_list_array().empty()) {
        int64_t value_count = seri_ctx.exec_ctx_->get_row_id_list_total_count() * col_num;
        OB_UNIS_ENCODE(value_count);
        for (int array_idx = 0; OB_SUCC(ret) && array_idx < seri_ctx.exec_ctx_->get_row_id_list_array().count();
             ++array_idx) {
          if (OB_ISNULL(row_id_list = seri_ctx.exec_ctx_->get_row_id_list_array().at(array_idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row id list is null", K(ret));
          } else {
            ARRAY_FOREACH(*row_id_list, idx)
            {
              int64_t start_idx = row_id_list->at(idx) * col_num;
              int64_t end_idx = start_idx + col_num;
              for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
                OB_UNIS_ENCODE(values_.at(i));
              }
            }
          }
        }
      } else {
        OB_UNIS_ENCODE(values_);
      }
      OB_UNIS_ENCODE(str_values_array_);
    }
  }

  return ret;
}

int64_t ObExprValuesSpec::get_serialize_size(const ObPhyOpSeriCtx& seri_ctx) const
{
  int64_t len = get_serialize_size_(seri_ctx);
  OB_UNIS_ADD_LEN(len);
  OB_UNIS_ADD_LEN(UNIS_VERSION);
  return len;
}

OB_DEF_SERIALIZE_SIZE(ObExprValuesSpec)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObExprValuesSpec, ObOpSpec));
  OB_UNIS_ADD_LEN(values_);
  OB_UNIS_ADD_LEN(str_values_array_);
  return len;
}

OB_DEF_SERIALIZE(ObExprValuesSpec)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObExprValuesSpec, ObOpSpec));
  OB_UNIS_ENCODE(values_);
  OB_UNIS_ENCODE(str_values_array_);
  return ret;
}

OB_DEF_DESERIALIZE(ObExprValuesSpec)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObExprValuesSpec, ObOpSpec));
  OB_UNIS_DECODE(values_);
  OB_UNIS_DECODE(str_values_array_);

  return ret;
}

int64_t ObExprValuesSpec::get_serialize_size_(const ObPhyOpSeriCtx& seri_ctx) const
{
  int64_t len = 0;
  const ObIArray<int64_t>* row_id_list = static_cast<const ObIArray<int64_t>*>(seri_ctx.row_id_list_);
  len += ObOpSpec::get_serialize_size();
  int64_t col_num = get_output_count();
  if (row_id_list != NULL) {
    int64_t value_size = col_num * row_id_list->count();
    OB_UNIS_ADD_LEN(value_size);
    ARRAY_FOREACH_NORET(*row_id_list, idx)
    {
      int64_t start_idx = row_id_list->at(idx) * col_num;
      int64_t end_idx = start_idx + col_num;
      for (int64_t i = start_idx; i < end_idx; ++i) {
        OB_UNIS_ADD_LEN(values_.at(i));
      }
    }
  } else if (OB_NOT_NULL(seri_ctx.exec_ctx_) && !seri_ctx.exec_ctx_->get_row_id_list_array().empty()) {
    int64_t value_count = seri_ctx.exec_ctx_->get_row_id_list_total_count() * col_num;
    OB_UNIS_ADD_LEN(value_count);
    for (int array_idx = 0; array_idx < seri_ctx.exec_ctx_->get_row_id_list_array().count(); ++array_idx) {
      if (OB_ISNULL(row_id_list = seri_ctx.exec_ctx_->get_row_id_list_array().at(array_idx))) {
        LOG_WARN("row id list is null");
      } else {
        for (int idx = 0; idx < row_id_list->count(); ++idx) {
          int64_t start_idx = row_id_list->at(idx) * col_num;
          int64_t end_idx = start_idx + col_num;
          for (int64_t i = start_idx; i < end_idx; ++i) {
            OB_UNIS_ADD_LEN(values_.at(i));
          }
        }
      }
    }
  } else {
    OB_UNIS_ADD_LEN(values_);
  }
  OB_UNIS_ADD_LEN(str_values_array_);

  return len;
}

ObExprValuesOp::ObExprValuesOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObOperator(exec_ctx, spec, input),
      node_idx_(0),
      vector_index_(0),
      datum_caster_(),
      cm_(CM_NONE),
      value_count_(0),
      switch_value_(false)
{}

int ObExprValuesOp::inner_open()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  const bool is_explicit_cast = false;
  const int32_t result_flag = 0;
  if (OB_FAIL(datum_caster_.init(eval_ctx_.exec_ctx_))) {
    LOG_WARN("fail to init datum_caster", K(ret));
  } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(is_explicit_cast, result_flag, ctx_.get_my_session(), cm_))) {
    LOG_WARN("fail to get_default_cast_mode", K(ret));
  } else {
    switch_value_ = true;
    // see ObSQLUtils::wrap_column_convert_ctx(), add CM_WARN_ON_FAIL for INSERT IGNORE.
    ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
    cm_ = cm_ | CM_COLUMN_CONVERT;
    if (plan_ctx->is_ignore_stmt()) {
      // CM_CHARSET_CONVERT_IGNORE_ERR is will give '?' when do string_string convert.
      // eg: insert into t(gbk_col) values('𐐀');
      cm_ = cm_ | CM_WARN_ON_FAIL | CM_CHARSET_CONVERT_IGNORE_ERR;
      LOG_TRACE("is ignore, set CM_WARN_ON_FAIL and CM_CHARSET_CONVERT_IGNORE_ERR", K(cm_));
    }
  }

  return ret;
}

int ObExprValuesOp::get_value_count()
{
  int ret = OB_SUCCESS;
  if (MY_INPUT.partition_id_values_ != 0) {
    common::ObIArray<ObPxSqcMeta::PartitionIdValue>* pid_values =
        reinterpret_cast<common::ObIArray<ObPxSqcMeta::PartitionIdValue>*>(MY_INPUT.partition_id_values_);
    int64_t partition_id = ctx_.get_expr_partition_id();
    bool find = false;
    int64_t col_num = MY_SPEC.get_output_count();
    CK(partition_id != OB_INVALID_ID);
    for (int i = 0; OB_SUCC(ret) && i < pid_values->count() && !find; ++i) {
      if (partition_id == pid_values->at(i).partition_id_) {
        node_idx_ = pid_values->at(i).value_begin_idx_ * col_num;
        value_count_ = node_idx_ + pid_values->at(i).value_count_ * col_num;
        if (OB_FAIL(value_count_ > MY_SPEC.values_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value count", K(ret), K(value_count_), K(MY_SPEC.values_.count()));
        }
        find = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (!find) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected partition id", K(partition_id));
      } else {
        switch_value_ = false;
      }
    }
  } else {
    value_count_ = MY_SPEC.values_.count();
  }
  return ret;
}

int ObExprValuesOp::rescan()
{
  int ret = OB_SUCCESS;
  if (MY_INPUT.partition_id_values_ != 0 && ctx_.is_gi_restart()) {
    switch_value_ = true;
  } else {
    node_idx_ = 0;
  }
  OZ(ObOperator::rescan());
  return ret;
}

int ObExprValuesOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  node_idx_ = 0;
  if (plan_ctx->get_bind_array_idx() >= plan_ctx->get_bind_array_count() - 1) {
    ret = OB_ITER_END;
  }

  return ret;
}

int ObExprValuesOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (OB_SUCC(ret)) {
    plan_ctx->set_autoinc_id_tmp(0);
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("check physical plan status failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (switch_value_ && OB_FAIL(get_value_count())) {
      LOG_WARN("fail to get value count", K(ret));
    } else if (OB_FAIL(calc_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from row store failed", K(ret));
      }
    } else {
      LOG_DEBUG("output row", "row", ROWEXPR2STR(eval_ctx_, MY_SPEC.output_));
    }
  }

  return ret;
}

OB_INLINE int ObExprValuesOp::calc_next_row()
{
  int ret = OB_SUCCESS;
  NG_TRACE_TIMES(2, value_start_calc_row);
  ObSQLSessionInfo* session = ctx_.get_my_session();
  int64_t col_num = MY_SPEC.get_output_count();
  int64_t col_idx = 0;
  if (node_idx_ == value_count_) {
    // there is no values any more
    ret = OB_ITER_END;
  } else {
    bool is_break = false;
    ObDatum* datum = NULL;
    ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
    while (OB_SUCC(ret) && node_idx_ < value_count_ && !is_break) {
      ObExpr* src_expr = MY_SPEC.values_.at(node_idx_);
      ObExpr* dst_expr = MY_SPEC.output_.at(col_idx);
      ObDatumMeta src_meta = src_expr->datum_meta_;
      ObObjMeta src_obj_meta = src_expr->obj_meta_;
      if (T_QUESTIONMARK == src_expr->type_ &&
          (src_expr->frame_idx_ < spec_.plan_->get_expr_frame_info().const_frame_.count() +
                                      spec_.plan_->get_expr_frame_info().param_frame_.count())) {
        /*
         * the 2nd condition with frame_idx is used to support subquery in values,
         * in this case the subquery expr will be replaced to question mark, we can
         * get its meta info from expr directly, not from param_store.
         */
        int64_t param_idx = src_expr->extra_;
        ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
        if (param_idx < 0 || param_idx >= plan_ctx->get_param_store().count()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid param idx", K(ret), K(param_idx));
        } else {
          src_obj_meta = plan_ctx->get_param_store().at(param_idx).meta_;
          src_meta.type_ = src_obj_meta.get_type();
          src_meta.cs_type_ = src_obj_meta.get_collation_type();
          const ObAccuracy& src_obj_acc = plan_ctx->get_param_store().at(param_idx).get_accuracy();
          src_meta.scale_ = src_obj_acc.get_scale();
          src_meta.precision_ = src_obj_acc.get_precision();
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (src_expr == dst_expr) {
        // do nothing
      } else if (src_meta.type_ == dst_expr->datum_meta_.type_ && src_meta.cs_type_ == dst_expr->datum_meta_.cs_type_) {
        if (OB_FAIL(src_expr->eval(eval_ctx_, datum))) {
          LOG_WARN("fail to cast values", K(ret), K(*src_expr));
        } else {
          dst_expr->locate_datum_for_write(eval_ctx_) = *datum;
          dst_expr->get_eval_info(eval_ctx_).evaluated_ = true;
        }
      } else {
        ObExpr real_src_expr = *src_expr;
        real_src_expr.datum_meta_ = src_meta;
        real_src_expr.obj_meta_ = src_obj_meta;
        real_src_expr.obj_datum_map_ = ObDatum::get_obj_datum_map_type(src_meta.type_);
        if (dst_expr->obj_meta_.is_enum_or_set()) {
          if (OB_UNLIKELY(col_idx < 0 || col_idx >= MY_SPEC.str_values_array_.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("col_idx is out of size", K(ret), K(col_idx), K(MY_SPEC.str_values_array_.count()));
          } else {
            const ObIArray<ObString>& str_values = MY_SPEC.str_values_array_.at(col_idx);
            if (OB_FAIL(datum_caster_.to_type(dst_expr->datum_meta_, str_values, real_src_expr, cm_, datum))) {
              LOG_WARN("fail to do to_type", K(ret), K(*dst_expr), K(real_src_expr));
            }
          }
        } else {
          if (OB_FAIL(datum_caster_.to_type(dst_expr->datum_meta_, real_src_expr, cm_, datum))) {
            LOG_WARN("fail to do to_type", K(ret), K(*dst_expr), K(real_src_expr));
          }
        }
        if (OB_SUCC(ret)) {
          ObDatum& dst_datum = dst_expr->locate_datum_for_write(eval_ctx_);
          if (ObObjDatumMapType::OBJ_DATUM_STRING == dst_expr->obj_datum_map_ ||
              OBJ_DATUM_LOB_LOCATOR == dst_expr->obj_datum_map_) {
            ObExprStrResAlloc res_alloc(*dst_expr, eval_ctx_);
            if (OB_FAIL(dst_datum.deep_copy(*datum, res_alloc))) {
              LOG_WARN("fail to deep copy datum from cast res datum", K(ret), K(*datum));
            }
          } else {
            ObDataBuffer res_alloc(const_cast<char*>(dst_datum.ptr_), dst_expr->res_buf_len_);
            if (OB_FAIL(dst_datum.deep_copy(*datum, res_alloc))) {
              LOG_WARN("fail to deep copy datum from cast res datum", K(ret), K(*datum));
            }
          }
          dst_expr->get_eval_info(eval_ctx_).evaluated_ = true;
        }
      }
      if (OB_SUCC(ret)) {
        LOG_DEBUG("expr values row columns", K(node_idx_), K(col_idx), K(*datum));
        ++node_idx_;
        if (col_idx == col_num - 1) {
          // last cell values resolved, output row now
          is_break = true;
        } else {
          col_idx = (col_idx + 1) % col_num;
        }
      }
    }  // while end
  }
  NG_TRACE_TIMES(2, value_after_calc_row);

  return ret;
}

int ObExprValuesOp::inner_close()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  if (OB_FAIL(datum_caster_.destroy())) {
    LOG_WARN("fail to destroy datum_caster", K(ret));
  }

  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
