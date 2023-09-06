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
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/engine/expr/ob_expr_type_to_str.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/dml/ob_dml_service.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObExprValuesSpec::serialize(char *buf,
                                int64_t buf_len,
                                int64_t &pos,
                                ObPhyOpSeriCtx &seri_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t len = get_serialize_size_(seri_ctx);
  OB_UNIS_ENCODE(UNIS_VERSION);
  OB_UNIS_ENCODE(len);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObOpSpec::serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize physical operator failed", K(ret));
    } else {
      const ObIArray<int64_t> *row_id_list = static_cast<const ObIArray<int64_t> *>(
                                                             seri_ctx.row_id_list_);
      int64_t col_num = get_output_count();
      if (row_id_list != NULL) {
        int64_t value_count = col_num * row_id_list->count();
        OB_UNIS_ENCODE(value_count);
        if (OB_SUCC(ret)) {
          ARRAY_FOREACH(*row_id_list, idx) {
            int64_t start_idx = row_id_list->at(idx) * col_num;
            int64_t end_idx = start_idx + col_num;
            for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
              OB_UNIS_ENCODE(values_.at(i));
            }
          }
        }
      } else if (OB_NOT_NULL(seri_ctx.exec_ctx_) &&
            !seri_ctx.exec_ctx_->get_row_id_list_array().empty()) {
        int64_t value_count = seri_ctx.exec_ctx_->get_row_id_list_total_count() * col_num;
        OB_UNIS_ENCODE(value_count);
        for (int array_idx = 0; OB_SUCC(ret) &&
             array_idx < seri_ctx.exec_ctx_->get_row_id_list_array().count();
              ++array_idx) {
          if (OB_ISNULL(row_id_list = seri_ctx.exec_ctx_->get_row_id_list_array().at(array_idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row id list is null", K(ret));
          } else {
            ARRAY_FOREACH(*row_id_list, idx) {
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
      OB_UNIS_ENCODE(err_log_ct_def_);
      OB_UNIS_ENCODE(contain_ab_param_);
      OB_UNIS_ENCODE(ins_values_batch_opt_);
      OB_UNIS_ENCODE(column_names_);
      OB_UNIS_ENCODE(array_group_idx_);
    }
  }

  return ret;
}

int64_t ObExprValuesSpec::get_serialize_size(const ObPhyOpSeriCtx &seri_ctx) const
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
  OB_UNIS_ADD_LEN(err_log_ct_def_);
  OB_UNIS_ADD_LEN(contain_ab_param_);
  OB_UNIS_ADD_LEN(ins_values_batch_opt_);
  OB_UNIS_ADD_LEN(column_names_);
  OB_UNIS_ADD_LEN(array_group_idx_);
  return len;
}

OB_DEF_SERIALIZE(ObExprValuesSpec)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObExprValuesSpec, ObOpSpec));
  OB_UNIS_ENCODE(values_);
  OB_UNIS_ENCODE(str_values_array_);
  OB_UNIS_ENCODE(err_log_ct_def_);
  OB_UNIS_ENCODE(contain_ab_param_);
  OB_UNIS_ENCODE(ins_values_batch_opt_);
  OB_UNIS_ENCODE(column_names_);
  OB_UNIS_ENCODE(array_group_idx_);
  return ret;
}

OB_DEF_DESERIALIZE(ObExprValuesSpec)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObExprValuesSpec, ObOpSpec));
  OB_UNIS_DECODE(values_);
  OB_UNIS_DECODE(str_values_array_);
  OB_UNIS_DECODE(err_log_ct_def_);
  OB_UNIS_DECODE(contain_ab_param_);
  OB_UNIS_DECODE(ins_values_batch_opt_);
  OB_UNIS_DECODE(column_names_);
  OB_UNIS_DECODE(array_group_idx_);
  return ret;
}

int64_t ObExprValuesSpec::get_serialize_size_(const ObPhyOpSeriCtx &seri_ctx) const
{
  int64_t len = 0;
  const ObIArray<int64_t> *row_id_list = static_cast<const ObIArray<int64_t> *>(
                                                          seri_ctx.row_id_list_);
  len += ObOpSpec::get_serialize_size();
  int64_t col_num = get_output_count();
  if (row_id_list != NULL) {
    int64_t value_size = col_num * row_id_list->count();
    OB_UNIS_ADD_LEN(value_size);
    ARRAY_FOREACH_NORET(*row_id_list, idx) {
      int64_t start_idx = row_id_list->at(idx) * col_num;
      int64_t end_idx = start_idx + col_num;
      for (int64_t i = start_idx; i < end_idx; ++i) {
        OB_UNIS_ADD_LEN(values_.at(i));
      }
    }
  } else if (OB_NOT_NULL(seri_ctx.exec_ctx_) &&
        !seri_ctx.exec_ctx_->get_row_id_list_array().empty()) {
    int64_t value_count = seri_ctx.exec_ctx_->get_row_id_list_total_count() * col_num;
    OB_UNIS_ADD_LEN(value_count);
    for (int array_idx = 0;
         array_idx < seri_ctx.exec_ctx_->get_row_id_list_array().count();
         ++array_idx) {
      if (OB_ISNULL(row_id_list = seri_ctx.exec_ctx_->get_row_id_list_array().at(array_idx))) {
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "row id list is null");
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
  OB_UNIS_ADD_LEN(err_log_ct_def_);
  OB_UNIS_ADD_LEN(contain_ab_param_);
  OB_UNIS_ADD_LEN(ins_values_batch_opt_);
  OB_UNIS_ADD_LEN(column_names_);
  OB_UNIS_ADD_LEN(array_group_idx_);
  return len;
}

ObExprValuesOp::ObExprValuesOp(ObExecContext &exec_ctx,
                               const ObOpSpec &spec,
                               ObOpInput *input)
  : ObOperator(exec_ctx, spec, input),
    node_idx_(0),
    vector_index_(0),
    datum_caster_(),
    cm_(CM_NONE),
    err_log_service_(get_eval_ctx()),
    err_log_rt_def_(),
    has_sequence_(false),
    real_value_cnt_(0),
    param_idx_(0),
    param_cnt_(0)
{
}

int ObExprValuesOp::inner_open()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  const bool is_explicit_cast = false;
  const int32_t result_flag = 0;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(ctx_.get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected NULL ptr", K(ret), KP(plan_ctx), KP(ctx_.get_sql_ctx()));
  } else if (OB_FAIL(datum_caster_.init(eval_ctx_.exec_ctx_))) {
    LOG_WARN("fail to init datum_caster", K(ret));
  } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(is_explicit_cast, result_flag,
                                                       ctx_.get_my_session(), cm_))) {
    LOG_WARN("fail to get_default_cast_mode", K(ret));
  } else {
    // see ObSQLUtils::wrap_column_convert_ctx(), add CM_WARN_ON_FAIL for INSERT IGNORE.
    cm_ = cm_ | CM_COLUMN_CONVERT;
    if (plan_ctx->is_ignore_stmt() || !is_strict_mode(ctx_.get_my_session()->get_sql_mode())) {
      // CM_CHARSET_CONVERT_IGNORE_ERR is will give '?' when do string_string convert.
      // eg: insert into t(gbk_col) values('𐐀');
      cm_ = cm_ | CM_WARN_ON_FAIL | CM_CHARSET_CONVERT_IGNORE_ERR;
      LOG_TRACE("is ignore, set CM_WARN_ON_FAIL and CM_CHARSET_CONVERT_IGNORE_ERR", K(cm_));
    }
    if (0 == child_cnt_) {
    } else if (1 == child_cnt_) {
      CK (PHY_SEQUENCE == left_->get_spec().get_type());
      has_sequence_ = true;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected child cnt", K(child_cnt_), K(ret));
    }
    if (OB_SUCC(ret)) {
      if (MY_SPEC.contain_ab_param_ && !ctx_.has_dynamic_values_table()) {
        int64_t value_group = MY_SPEC.contain_ab_param_ ?
                              ctx_.get_sql_ctx()->get_batch_params_count() : 1;
        real_value_cnt_ = MY_SPEC.get_value_count() * value_group;
        param_idx_ = 0;
        param_cnt_ = plan_ctx->get_datum_param_store().count();
      } else if (MY_SPEC.contain_ab_param_ && ctx_.has_dynamic_values_table() &&
                 MY_SPEC.array_group_idx_ >= 0) {
        if (OB_UNLIKELY(MY_SPEC.array_group_idx_ >= plan_ctx->get_array_param_groups().count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected idx", K(ret), K(MY_SPEC.array_group_idx_));
        } else {
          ObArrayParamGroup &array_param_group = plan_ctx->get_array_param_groups().at(MY_SPEC.array_group_idx_);
          real_value_cnt_ = MY_SPEC.get_value_count() * array_param_group.row_count_;
          param_cnt_ = array_param_group.column_count_;
          param_idx_ = array_param_group.start_param_idx_;
        }
      } else {
        real_value_cnt_ = MY_SPEC.get_value_count();
      }
      LOG_TRACE("init expr values op", K(real_value_cnt_), K(param_cnt_), K(param_idx_));
    }
  }
  return ret;
}

int ObExprValuesOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to do inner rescan", K(ret));
  } else {
    node_idx_ = 0;
  }
  return ret;
}
//ObExprValuesOp has its own switch iterator
int ObExprValuesOp::switch_iterator()
{
  int ret = ObOperator::inner_switch_iterator();
  if (OB_SUCC(ret)) {
    ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
    node_idx_ = 0;
    if (plan_ctx->get_bind_array_idx() >= plan_ctx->get_bind_array_count() - 1) {
      ret = OB_ITER_END;
    }
  }

#ifndef NDEBUG
  OX(OB_ASSERT(false == brs_.end_));
#endif

  return ret;
}

int ObExprValuesOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx_);
  if (OB_SUCC(ret)) {
    plan_ctx->set_autoinc_id_tmp(0);
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("check physical plan status faild", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    do {
      clear_evaluated_flag();
      err_log_rt_def_.reset();
      if (OB_FAIL(calc_next_row())) {
        if(OB_ITER_END != ret) {
          LOG_WARN("get next row from row store failed", K(ret));
        }
      } else if (MY_SPEC.err_log_ct_def_.is_error_logging_ && OB_SUCCESS != err_log_rt_def_.first_err_ret_) {
        // only if error_logging is true then first_err_ret_ could be set values
        if (OB_FAIL(err_log_service_.insert_err_log_record(session,
                                                           MY_SPEC.err_log_ct_def_,
                                                           err_log_rt_def_,
                                                           ObDASOpType::DAS_OP_TABLE_INSERT))) {
          LOG_WARN("insert_err_log_record failed", K(ret), K(err_log_rt_def_.first_err_ret_));
        } else {
          err_log_rt_def_.curr_err_log_record_num_++;
        }
      } else {
        LOG_DEBUG("output row", "row", ROWEXPR2STR(eval_ctx_, MY_SPEC.output_));
      }
    } while (OB_SUCC(ret) &&
        MY_SPEC.err_log_ct_def_.is_error_logging_ &&
        OB_SUCCESS != err_log_rt_def_.first_err_ret_);
  }

  return ret;
}

void ObExprValuesOp::update_src_meta(ObDatumMeta &src_meta, const ObObjMeta &src_obj_meta, const ObAccuracy &src_obj_acc)
{
  src_meta.type_ = src_obj_meta.get_type();
  src_meta.cs_type_ = src_obj_meta.get_collation_type();
  src_meta.scale_ = src_obj_acc.get_scale();
  src_meta.precision_ = src_obj_acc.get_precision();
}

int ObExprValuesOp::get_real_batch_obj_type(ObDatumMeta &src_meta,
                                            ObObjMeta &src_obj_meta,
                                            ObExpr *src_expr,
                                            int64_t group_idx)
{
  int ret = OB_SUCCESS;
  if ((MY_SPEC.ins_values_batch_opt_ || (ctx_.has_dynamic_values_table() && MY_SPEC.array_group_idx_ >= 0)) &&
      T_QUESTIONMARK == src_expr->type_ &&
      src_expr->frame_idx_ < spec_.plan_->get_expr_frame_info().const_frame_.count() +
                             spec_.plan_->get_expr_frame_info().param_frame_.count()) {
    int64_t param_idx = src_expr->extra_;
    ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
    const ObSqlArrayObj *array_obj = NULL;
    if (param_idx < 0 || param_idx >= plan_ctx->get_param_store().count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid param idx", K(ret), K(param_idx));
    } else if (plan_ctx->get_param_store().at(param_idx).is_ext_sql_array()) {
      // 如果是is_ext_sql_array的参数
      if (OB_ISNULL(array_obj =
          reinterpret_cast<const ObSqlArrayObj*>(plan_ctx->get_param_store().at(param_idx).get_ext()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), K(param_idx), K(plan_ctx->get_param_store()));
      } else if (array_obj->count_ <= group_idx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected group_idx", K(ret), K(array_obj->count_),
                 K(group_idx), K(param_idx), K(plan_ctx->get_param_store()));
      } else {
        src_obj_meta = array_obj->data_[group_idx].meta_;
        const ObAccuracy &src_obj_acc = array_obj->data_[group_idx].get_accuracy();
        update_src_meta(src_meta, src_obj_meta, src_obj_acc);
      }
    } else {
      // 如果不是is_ext_sql_array的参数
      src_obj_meta = plan_ctx->get_param_store().at(param_idx).meta_;
      const ObAccuracy &src_obj_acc = plan_ctx->get_param_store().at(param_idx).get_accuracy();
      update_src_meta(src_meta, src_obj_meta, src_obj_acc);
    }
  }
  return ret;
}

int ObExprValuesOp::eval_values_op_dynamic_cast_to_lob(ObExpr &real_src_expr,
                                                       ObObjMeta &src_obj_meta,
                                                       ObExpr *dst_expr)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = NULL;
  // large string types to temp lob needs lots of memory,
  // for example char type from send long/piece data which is 40M, cast to longtext
  // 1. char to longtext 40M (only used to add lob header if cs type is the same)
  // 2. deep copy use another 40M
  // if cast only used to build temp lob header, memory allocation in step 1 can be avoid.
  bool string_to_lob_withsame_cs_type = false;
  if (ob_is_string_tc(src_obj_meta.get_type())
      && ob_is_text_tc(dst_expr->obj_meta_.get_type())
      && (src_obj_meta.get_charset_type() == dst_expr->obj_meta_.get_charset_type())) {
    string_to_lob_withsame_cs_type = true;
  }
  ObDatum &dst_datum = dst_expr->locate_datum_for_write(eval_ctx_);
  if (!string_to_lob_withsame_cs_type) {
    if (OB_FAIL(datum_caster_.to_type(dst_expr->datum_meta_, real_src_expr,
                                      cm_, datum))) {
      LOG_WARN("fail to dynamic cast", K(dst_expr->datum_meta_),
                                        K(real_src_expr), K(cm_), K(ret));
    } else if (lib::is_oracle_mode() && dst_expr->datum_meta_.type_ == common::ObLongTextType) {
      if (ob_is_text_tc(real_src_expr.datum_meta_.type_) && dst_expr->obj_meta_.has_lob_header()) {
        if (datum->get_string().ptr() == NULL || datum->get_string().length() == 0) {
          datum->set_null(); // compat 4.0, empty text to ObLobType, result is NULL
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObExprStrResAlloc res_alloc(*dst_expr, eval_ctx_);
      // need adjust lob header, since lob to lob may not handle headers
      if (is_lob_storage(src_obj_meta.get_type()) &&
          OB_FAIL(ob_adjust_lob_datum(*datum,
                                      src_obj_meta,
                                      dst_expr->obj_meta_,
                                      eval_ctx_.exec_ctx_.get_eval_tmp_allocator()))) {
        LOG_WARN("adjust lob datum failed",
                K(ret), K(*datum), K(src_obj_meta), K(dst_expr->obj_meta_));
      } else if (OB_FAIL(dst_datum.deep_copy(*datum, res_alloc))) {
        LOG_WARN("fail to deep copy datum from cast res datum", K(ret), K(*datum));
      }
    }
  } else {
    ObDatum *src_datum;
    if (OB_FAIL(real_src_expr.eval(eval_ctx_, src_datum))) {
      LOG_WARN("fail to eval src", K(real_src_expr), K(cm_), K(ret));
    } else if (src_datum->is_null()) {
      dst_datum.set_null();
    } else if (src_datum->get_string().empty()
                && lib::is_oracle_mode()
                && dst_expr->datum_meta_.type_ == common::ObLongTextType) {
      dst_datum.set_null();
    } else {
      ObString src_string = src_datum->get_string();
      ObTextStringDatumResult lob_result(dst_expr->obj_meta_.get_type(),
                                         dst_expr, &eval_ctx_, &dst_datum);
      if (OB_FAIL(lob_result.init(src_string.length()))) {
      } else if (OB_FAIL(lob_result.append(src_string))) {
      } else {
        lob_result.set_result();
      }
    }
  }
  return ret;
}

OB_INLINE int ObExprValuesOp::calc_next_row()
{
  int ret = OB_SUCCESS;
  NG_TRACE_TIMES(2, value_start_calc_row);
  ObSQLSessionInfo *session = ctx_.get_my_session();
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  int64_t col_num = MY_SPEC.get_output_count();
  int64_t col_idx = 0;
  if (node_idx_ == real_value_cnt_) {
    // there is no values any more
    ret = OB_ITER_END;
  } else {
    bool is_break = false;
    ObDatum *datum = NULL;
    int64_t group_idx = 0;
    if (MY_SPEC.contain_ab_param_) {
      group_idx = node_idx_ / MY_SPEC.get_value_count();
      if (OB_FAIL(plan_ctx->replace_batch_param_datum(group_idx, param_idx_, param_cnt_))) {
        LOG_WARN("replace batch param datum failed", K(ret), K(group_idx));
      }
    }
    if (OB_SUCC(ret) && has_sequence_) {
      if (OB_FAIL(left_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to calc next row", K(ret));
        }
      }
    }
    while (OB_SUCC(ret) && node_idx_ < real_value_cnt_ && !is_break) {
      int64_t real_node_idx = node_idx_ % MY_SPEC.get_value_count();
      int64_t row_num = (MY_SPEC.ins_values_batch_opt_ ? group_idx : (real_node_idx / col_num)) + 1;
      ObExpr *src_expr = MY_SPEC.values_.at(real_node_idx);
      ObExpr *dst_expr = MY_SPEC.output_.at(col_idx);
      ObDatumMeta src_meta = src_expr->datum_meta_;
      bool is_strict_json = MY_SPEC.get_is_strict_json_desc_count() == 0 ? false :
                            MY_SPEC.is_strict_json_desc_.at(node_idx_ % MY_SPEC.get_is_strict_json_desc_count());

      ObObjMeta src_obj_meta = src_expr->obj_meta_;
      if (MY_SPEC.contain_ab_param_) {
        if (OB_FAIL(get_real_batch_obj_type(src_meta, src_obj_meta, src_expr, group_idx))) {
          LOG_WARN("fail to get real batch obj type info", K(ret), K(real_node_idx), K(group_idx), KPC(src_expr));
        }
      } else {
        if (T_QUESTIONMARK == src_expr->type_
          && (src_expr->frame_idx_
              < spec_.plan_->get_expr_frame_info().const_frame_.count()
                  + spec_.plan_->get_expr_frame_info().param_frame_.count())) {
          /*
           * the 2nd condition with frame_idx is used to support subquery in values,
           * in this case the subquery expr will be replaced to question mark, we can
           * get its meta info from expr directly, not from param_store.
           */
          int64_t param_idx = src_expr->extra_;
          ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
          if (param_idx < 0 || param_idx >= plan_ctx->get_param_store().count()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid param idx", K(ret), K(param_idx));
          } else {
            src_obj_meta = plan_ctx->get_param_store().at(param_idx).meta_;
            const ObAccuracy &src_obj_acc =
              plan_ctx->get_param_store().at(param_idx).get_accuracy();
            update_src_meta(src_meta, src_obj_meta, src_obj_acc);
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (T_PSEUDO_STMT_ID == src_expr->type_) {
          src_expr->locate_datum_for_write(eval_ctx_).set_int(group_idx);
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (src_expr == dst_expr) {
        // do nothing
        // 处理select 1, 2, 3; 这种情况, values_和output中表达式指针相同,
        // 不需要进行动态cast, 后面直接计算output就可以;
        //
        // 如果这里也进行了动态cast, 动态构造的cast表达式指向的expr datum为output expr
        // 指向的expr datum, 也就是与value expr指向的相同的expr datum内存区域;
        // 构造的cast表达式进行eval计算时, 如果发现datum中ptr执行的内存不为reserve内存，
        // 则会将ptr指向reserve内存， 而在参数化情况下，value expr和output expr均为
        // T_QUESTIONMARK的表达式, 该表达式是没有reserve内存的，因此会导致ptr指向非预期
        // 内存， 可能出现结果不对
      } else if (src_meta.type_ == dst_expr->datum_meta_.type_
                 && src_meta.cs_type_ == dst_expr->datum_meta_.cs_type_
                 && src_obj_meta.has_lob_header() == dst_expr->obj_meta_.has_lob_header()) {
        // 将values中数据copy到output中
        if (OB_FAIL(src_expr->eval(eval_ctx_, datum))) {
          // catch err and print log later
        } else {
          dst_expr->locate_datum_for_write(eval_ctx_) = *datum;
          dst_expr->set_evaluated_projected(eval_ctx_);
        }
      } else if (OB_FAIL(ObCharset::check_valid_implicit_convert(src_meta.cs_type_, dst_expr->datum_meta_.cs_type_))) {
        LOG_WARN("failed to check valid implicit convert", K(ret));
      } else {
        // 需要动态cast原因:
        // 对于以下场景:
        //   create table t1(c1 int primary key);
        //   sql_1: insert into t1 values(null);
        //   sql_2: insert into t1 values('1');
        // sql_1和sql_2会命中相同的执行计划, 但sql_1中values类型是null类型,
        // sql_2中values类型为varchar类型, 最终output均为int类型, 为了在新引擎下
        // 对于不同类型输入(values值), 能够最终转化为相同数据类型输出(output),
        // 因此引入了动态cast; 根据不同values值类型, 动态确定cast 函数, 并将结果
        // 存入output中;
        // 对于不同values值类型, 如果是参数化场景, 不能直接使用plan中values expr
        // 的meta信息, 该meta信息存放的是第一次生成计划时对应sql的值的meta信息,
        // 需要根据param store中实际值确定values类型, 然后获取动态cast函数
        ObExpr real_src_expr = *src_expr;
        real_src_expr.datum_meta_ = src_meta;
        real_src_expr.obj_meta_ = src_obj_meta;
        real_src_expr.obj_datum_map_ = ObDatum::get_obj_datum_map_type(src_meta.type_);
        // for table modify in oracle mode, we ignore charset convert failed
        if (lib::is_oracle_mode()) {
          cm_ = cm_ | CM_CHARSET_CONVERT_IGNORE_ERR;
          if (is_strict_json) {
            cm_ = cm_ | CM_STRICT_JSON;
          }
        }
        if (dst_expr->obj_meta_.is_enum_or_set()) {
          if (OB_UNLIKELY(col_idx < 0 || col_idx >= MY_SPEC.str_values_array_.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("col_idx is out of size", K(ret), K(col_idx),
                     K(MY_SPEC.str_values_array_.count()));
          } else {
            const ObIArray<ObString> &str_values = MY_SPEC.str_values_array_.at(col_idx);
            if (OB_FAIL(datum_caster_.to_type(dst_expr->datum_meta_, str_values,
                                              real_src_expr, cm_, datum))) {
              LOG_WARN("fail to do to_type", K(ret), K(*dst_expr), K(real_src_expr));
              ObString column_name = MY_SPEC.column_names_.at(col_idx);
              ret = ObDMLService::log_user_error_inner(ret, row_num, column_name, ctx_);
            }
          }
        } else if (!dst_expr->obj_meta_.is_lob_storage()) {
          if (OB_FAIL(datum_caster_.to_type(dst_expr->datum_meta_, real_src_expr,
                                            cm_, datum))) {
            LOG_WARN("fail to dynamic cast", K(dst_expr->datum_meta_),
                                             K(real_src_expr), K(cm_), K(ret));
            if (dst_expr->obj_meta_.is_geometry()) {
              ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
              LOG_USER_WARN(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
            }
            ObString column_name = MY_SPEC.column_names_.at(col_idx);
            ret = ObDMLService::log_user_error_inner(ret, row_num, column_name, ctx_);
          }
        } else { // dst type is lob
          if (OB_FAIL(eval_values_op_dynamic_cast_to_lob(real_src_expr, src_obj_meta, dst_expr))) {
            LOG_WARN("fail to dynamic cast to lob types", K(dst_expr->datum_meta_),
                                                          K(real_src_expr), K(cm_), K(ret));
          } else {
            dst_expr->set_evaluated_projected(eval_ctx_);
          }
        }

        if (OB_SUCC(ret) && !dst_expr->obj_meta_.is_lob_storage()) {
          ObDatum &dst_datum = dst_expr->locate_datum_for_write(eval_ctx_);
          if (ObObjDatumMapType::OBJ_DATUM_STRING == dst_expr->obj_datum_map_) {
            ObExprStrResAlloc res_alloc(*dst_expr, eval_ctx_);
            if (OB_FAIL(dst_datum.deep_copy(*datum, res_alloc))) {
              LOG_WARN("fail to deep copy datum from cast res datum", K(ret), KP(datum));
            }
          } else {
            ObDataBuffer res_alloc(const_cast<char*>(dst_datum.ptr_),
                                   dst_expr->res_buf_len_);
            if (OB_FAIL(dst_datum.deep_copy(*datum, res_alloc))) {
              LOG_WARN("fail to deep copy datum from cast res datum", K(ret), KP(datum));
            }
          }
          dst_expr->set_evaluated_projected(eval_ctx_);
        }
      }

      if (OB_FAIL(ret)) {
        if (MY_SPEC.err_log_ct_def_.is_error_logging_ && should_catch_err(ret)) {
          if (OB_SUCCESS == err_log_rt_def_.first_err_ret_) {
            err_log_rt_def_.first_err_ret_ = ret;
          }
          dst_expr->locate_datum_for_write(eval_ctx_).set_null();
          dst_expr->set_evaluated_projected(eval_ctx_);
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to do to_type and not need to catch err", K(ret), KPC(dst_expr), KPC(src_expr));
        }
      }

      if (OB_SUCC(ret)) {
        LOG_DEBUG("expr values row columns", K(node_idx_), K(real_node_idx),
                  K(col_idx), KPC(datum), K(datum), KPC(src_expr), KPC(dst_expr));
        ++node_idx_;
        if (col_idx == col_num - 1) {
          //last cell values resolved, output row now
          is_break = true;
        } else {
          col_idx = (col_idx + 1) % col_num;
        }
      }
    } // while end

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
} // end namespace sql
} // end namespace oceanbase
