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

#define USING_LOG_PREFIX SQL_CG

#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/code_generator/ob_column_index_provider.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_extra_info_factory.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

struct ObExprCallDepth
{
  ObExprCallDepth()
      : expr_(NULL),
      need_stack_check_(false),
      max_check_depth_(0),
      max_call_depth_(0),
      checked_parent_cnt_(0)
  {
  }

  TO_STRING_KV(KP(expr_), K(need_stack_check_), K(max_check_depth_),
               K(max_call_depth_), K(checked_parent_cnt_));

  ObExpr *expr_;
  bool need_stack_check_;
  // max call depth of stack check expr or root
  int64_t max_check_depth_;
  int64_t max_call_depth_;
  int64_t checked_parent_cnt_;
};

// 1. 将所有raw exprs展开
//    cg: c1 + 1, c1 + c2  ==>  c1, 1, c1 + 1, c2, c1 + c2
// 2. 构造ObExpr, 并将ObExpr对应设置到对应ObRawExpr中
// 3. 初始化ObExpr所有成员, 并返回Frame相关信息
int ObStaticEngineExprCG::generate(const ObRawExprUniqueSet &all_raw_exprs,
                                   ObExprFrameInfo &expr_info)
{
  int ret = OB_SUCCESS;
  ObRawExprUniqueSet flattened_raw_exprs(true);
  if (all_raw_exprs.count() <= 0) {
    // do nothing
  } else if (OB_FAIL(flattened_raw_exprs.flatten_and_add_raw_exprs(all_raw_exprs))) {
    LOG_WARN("failed to flatten raw exprs", K(ret));
  } else if (OB_FAIL(divide_probably_local_exprs(
                     const_cast<ObIArray<ObRawExpr *> &>(flattened_raw_exprs.get_expr_array())))) {
    LOG_WARN("divided probably local exprs failed", K(ret));
  } else if (OB_FAIL(construct_exprs(flattened_raw_exprs.get_expr_array(),
                                     expr_info.rt_exprs_))) {
    LOG_WARN("failed to construct rt exprs", K(ret));
  } else if (OB_FAIL(cg_exprs(flattened_raw_exprs.get_expr_array(), expr_info))) {
    LOG_WARN("failed to cg exprs", K(ret));
  }
  return ret;
}

// used for temp expr generate
int ObStaticEngineExprCG::generate(ObRawExpr *expr,
                                   ObRawExprUniqueSet &flattened_raw_exprs,
                                   ObExprFrameInfo &expr_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(flattened_raw_exprs.flatten_temp_expr(expr))) {
    LOG_WARN("failed to flatten raw exprs", K(ret));
  } else if (OB_FAIL(construct_exprs(flattened_raw_exprs.get_expr_array(),
                                     expr_info.rt_exprs_))) {
    LOG_WARN("failed to construct rt exprs", K(ret));
  } else if (OB_FAIL(cg_exprs(flattened_raw_exprs.get_expr_array(), expr_info))) {
    LOG_WARN("failed to cg exprs", K(ret));
  }
  return ret;
}

int ObStaticEngineExprCG::detect_batch_size(const ObRawExprUniqueSet &exprs,
                                            int64_t &batch_size,
                                            int64_t config_maxrows,
                                            int64_t config_target_maxsize,
                                            const double scan_cardinality)
{
  int ret = OB_SUCCESS;
  int64_t MAX_ROWSIZE = 65535;
  int64_t MIN_ROWSIZE = 2;
  const common::ObIArray<ObRawExpr *> &raw_exprs = exprs.get_expr_array();
  auto size = get_expr_execute_size(raw_exprs);
  if (size == ObExprBatchSize::full) {
    if (config_maxrows) {
      batch_size = config_maxrows;
      for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
        if (is_vectorized_expr(raw_exprs.at(i))) {
          int64_t max_batch_size = compute_max_batch_size(raw_exprs.at(i));
          batch_size = std::min(batch_size, max_batch_size);
        }
      }
      if (OB_UNLIKELY(batch_size != config_maxrows)) {
        batch_size = is2n(batch_size) ? batch_size : next_pow2(batch_size) >> 1;
        LOG_TRACE("After adjust batch_size adaptively", K(config_maxrows), K(batch_size));
      }
    } else {
      uint32_t row_size = 1;
      ObSEArray<ObRawExpr *, 64> vectorized_exprs;
      if (OB_FAIL(get_vectorized_exprs(raw_exprs, vectorized_exprs))) {
        LOG_WARN("failed to flatten raw exprs", K(ret));
      } else {
        auto expr_cnt = vectorized_exprs.count();
        for (int i = 0; i < expr_cnt; i++) {
          ObRawExpr *raw_expr = vectorized_exprs.at(i);
          const ObObjMeta &result_meta = raw_expr->get_result_meta();
          row_size += reserve_data_consume(result_meta.get_type()) +
                      get_expr_datum_fixed_header_size();
        }
        batch_size = config_target_maxsize / row_size;
        LOG_TRACE("detect_batch_size", K(row_size), K(batch_size), K(expr_cnt));
        // recalculate batch_size: count 2 additional bitmaps: skip + eval_flags
        batch_size = (config_target_maxsize -
                      expr_cnt * 2 * ObBitVector::memory_size(batch_size)) /
                     row_size;
        batch_size = next_pow2(batch_size);
        // range limit check
        if (batch_size < MIN_ROWSIZE) {
          batch_size = MIN_ROWSIZE;
        } else if (batch_size > MAX_ROWSIZE) {
          batch_size = MAX_ROWSIZE;
        }
      }
    }
  } else if (size == ObExprBatchSize::small) {
    batch_size = static_cast<int64_t>(ObExprBatchSize::small); //
  } else {
    batch_size = static_cast<int64_t>(ObExprBatchSize::one);
  }

  if (is_oltp_workload(scan_cardinality)) {
    // downgrade batchsize to a smaller value to minimize rowsets cost for TP
    // workload
    batch_size = min(OLTP_WORKLOAD_CARDINALITY, batch_size);
  }
  return ret;
}

// Attention : Please think over before you have to use this function.
// This function is different from generate_rt_expr.
// It won't put raw_expr into cur_op_exprs_ because it doesn't need to be calculated.
void *ObStaticEngineExprCG::get_left_value_rt_expr(const ObRawExpr &raw_expr)
{
  return reinterpret_cast<void*>(get_rt_expr(raw_expr));
}

int ObStaticEngineExprCG::generate_rt_expr(const ObRawExpr &raw_expr,
                                           ObIArray<ObRawExpr *> &exprs,
                                           ObExpr *&rt_expr)
{
  int ret = OB_SUCCESS;
  rt_expr = get_rt_expr(raw_expr);
  if (OB_ISNULL(rt_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rt expr is null", K(ret), K(raw_expr));
  } else if (OB_FAIL(exprs.push_back(const_cast<ObRawExpr *>(&raw_expr)))) {
    LOG_WARN("fail to push rt expr", K(ret));
  }

  return ret;
}

ObExpr *ObStaticEngineExprCG::get_rt_expr(const ObRawExpr &raw_expr)
{
  return raw_expr.rt_expr_;
}

// 构造ObExpr, 并将ObExpr对应设置到对应ObRawExpr中
int ObStaticEngineExprCG::construct_exprs(const ObIArray<ObRawExpr *> &raw_exprs,
                                          ObIArray<ObExpr> &rt_exprs)
{
  int ret = OB_SUCCESS;
  int64_t rt_expr_cnt = raw_exprs.count();
  if (OB_FAIL(rt_exprs.prepare_allocate(rt_expr_cnt))) {
    LOG_WARN("fail to reserve frame infos", K(ret), K(rt_expr_cnt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rt_expr_cnt; i++) {
    raw_exprs.at(i)->set_rt_expr(&rt_exprs.at(i));
  }

  return ret;
}


int ObStaticEngineExprCG::cg_exprs(const ObIArray<ObRawExpr *> &raw_exprs,
                                  ObExprFrameInfo &expr_info)
{
  int ret = OB_SUCCESS;
  // 此处判空后, cg_exprs调用的所有私有函数不再对raw_expr和rt_expr判空
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    ObExpr *rt_expr = NULL;
    if (OB_ISNULL(raw_exprs.at(i))
        || OB_ISNULL(rt_expr = get_rt_expr(*raw_exprs.at(i)))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(raw_exprs.at(i)), K(rt_expr));
    }
  }
  if (OB_SUCC(ret)) {
    // init type_, datum_meta_, obj_meta_, obj_datum_map_, args_, arg_cnt_
    // row_dimension_, op_
    if (OB_FAIL(cg_expr_basic(raw_exprs))) {
      LOG_WARN("fail to init expr", K(ret), K(raw_exprs));
    } else if (OB_FAIL(cg_expr_basic_funcs(raw_exprs))) {
      LOG_WARN("fail to init basic funcs", K(ret));
      // init eval_func_, inner_eval_func_, expr_ctx_id_, extra_
    } else if (OB_FAIL(cg_expr_by_operator(raw_exprs, expr_info.need_ctx_cnt_))) {
      LOG_WARN("fail to init expr special", K(ret), K(raw_exprs));
    // init parent_cnt_, parents_
    // cg_expr_parents must be after cg_expr_by_operator,
    // because cg_expr_by_operator may replace rt_expr.args_
    } else if (OB_FAIL(cg_expr_parents(raw_exprs))) {
      LOG_WARN("fail to init expr parents", K(ret), K(raw_exprs));
      // init res_buf_len_, frame_idx_, datum_off_, res_buf_off_
    } else if (OB_FAIL(cg_all_frame_layout(raw_exprs, expr_info))) {
      LOG_WARN("fail to init expr data layout", K(ret), K(raw_exprs));
    } else if (OB_FAIL(alloc_so_check_exprs(raw_exprs, expr_info))) {
      LOG_WARN("alloc stack overflow check exprs failed", K(ret));
    }
  }

  return ret;
}

// init type_, datum_meta_, obj_meta_, obj_datum_map_, args_, arg_cnt_, op_
int ObStaticEngineExprCG::cg_expr_basic(const ObIArray<ObRawExpr *> &raw_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    ObRawExpr *raw_expr = raw_exprs.at(i);
    ObExpr *rt_expr = get_rt_expr(*raw_expr);
    LOG_DEBUG("cg expr basic", K(raw_expr), K(rt_expr), KPC(raw_expr));
    const ObObjMeta &result_meta = raw_expr->get_result_meta();
    // init type_
    rt_expr->type_ = raw_expr->get_expr_type();
    rt_expr->batch_result_ = batch_size_ > 0 && raw_expr->is_vectorize_result();
    rt_expr->batch_idx_mask_ = rt_expr->batch_result_ ? UINT64_MAX : 0;
    rt_expr->is_called_in_sql_ = raw_expr->is_called_in_sql();
    rt_expr->is_static_const_ = raw_expr->is_static_const_expr();
    rt_expr->is_dynamic_const_ = raw_expr->is_dynamic_const_expr();
    rt_expr->is_boolean_ = raw_expr->is_bool_expr();
    if (T_OP_ROW != raw_expr->get_expr_type()) {
      // init datum_meta_
      rt_expr->datum_meta_ = ObDatumMeta(result_meta.get_type(),
                                        result_meta.get_collation_type(),
                                        raw_expr->get_result_type().get_scale(),
                                        raw_expr->get_result_type().get_precision());
      // init obj_meta_
      rt_expr->obj_meta_ = result_meta;
      // pl extend type has its own explanation for scale
      if (ObExtendType != rt_expr->obj_meta_.get_type()
          && ObUserDefinedSQLType != rt_expr->obj_meta_.get_type()) {
        rt_expr->obj_meta_.set_scale(rt_expr->datum_meta_.scale_);
      }
      if (is_lob_storage(rt_expr->obj_meta_.get_type())) {
        if (cur_cluster_version_ >= CLUSTER_VERSION_4_1_0_0) {
          rt_expr->obj_meta_.set_has_lob_header();
        }
      }
      // For bit type, `length_semantics_` is used as width in datum mate, and `scale_` is used
      // as width in obj, so passing length meta to scale in obj_mate.
      if (ob_is_bit_tc(result_meta.get_type())) {
        rt_expr->obj_meta_.set_scale(rt_expr->datum_meta_.length_semantics_);
      }
      // init max_length_
      rt_expr->max_length_ = raw_expr->get_result_type().get_length();
      // init obj_datum_map_
      rt_expr->obj_datum_map_ = ObDatum::get_obj_datum_map_type(result_meta.get_type());
    }
    if (T_REF_COLUMN == raw_expr->get_expr_type()) {
      // do nothing.
    } else {
      // init arg_cnt_
      rt_expr->arg_cnt_ = raw_expr->get_param_count();
      // init args_;
      if (rt_expr->arg_cnt_ > 0) {
        int64_t alloc_size = rt_expr->arg_cnt_ * sizeof(ObExpr *);
        ObExpr **buf = static_cast<ObExpr **>(allocator_.alloc(alloc_size));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret));
        } else {
          memset(buf, 0, alloc_size);
          rt_expr->args_ = buf;
          for (int64_t i = 0; OB_SUCC(ret) && i < raw_expr->get_param_count();
               i++) {
            ObRawExpr *child_expr = NULL;
            if (OB_ISNULL(child_expr = raw_expr->get_param_expr(i))) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument", K(ret));
            } else if (OB_ISNULL(get_rt_expr(*child_expr))) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("expr is null", K(ret));
            } else {
              rt_expr->args_[i] = get_rt_expr(*child_expr);
            }
          }
        }
      }
    }
  } // for end

  return ret;
}

// init parent_cnt_, parents_
int ObStaticEngineExprCG::cg_expr_parents(const ObIArray<ObRawExpr *> &raw_exprs)
{
  int ret = OB_SUCCESS;
  // init expr parent cnt
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    ObExpr *rt_expr = get_rt_expr(*raw_exprs.at(i));
    if (rt_expr->arg_cnt_ > 0 && OB_ISNULL(rt_expr->args_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(rt_expr->arg_cnt_), KP(rt_expr->args_));
    }
    for (int64_t child_idx = 0;
         OB_SUCC(ret) && child_idx < rt_expr->arg_cnt_;
         child_idx++) {
      if (OB_ISNULL(rt_expr->args_[child_idx])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(rt_expr->args_[child_idx]));
      } else {
        rt_expr->args_[child_idx]->parent_cnt_ += 1;
      }
    }
  }
  // alloc expr parents memory
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    ObExpr *rt_expr = get_rt_expr(*raw_exprs.at(i));
    if (rt_expr->parent_cnt_ > 0) {
      int64_t alloc_size = 0;
      alloc_size = rt_expr->parent_cnt_ * sizeof(ObExpr *);
      if (OB_ISNULL(rt_expr->parents_ =
                    static_cast<ObExpr **>(allocator_.alloc(alloc_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(alloc_size));
      }
    }
  }
  // reset parent cnt;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    ObExpr *rt_expr = get_rt_expr(*raw_exprs.at(i));
    rt_expr->parent_cnt_ = 0;
  }
  // init parent_cnt_ and parents_
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    ObExpr *rt_expr = get_rt_expr(*raw_exprs.at(i));
    for (int64_t arg_idx = 0;
         OB_SUCC(ret) && arg_idx < rt_expr->arg_cnt_;
         arg_idx++) {
      if (OB_ISNULL(rt_expr->args_[arg_idx])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("child expr is null", K(ret), K(arg_idx));
      } else {
        uint32_t &parent_cnt = rt_expr->args_[arg_idx]->parent_cnt_;
        rt_expr->args_[arg_idx]->parents_[parent_cnt] = rt_expr;
        parent_cnt++;
      }
    }
  }

  return ret;
}

extern int eval_question_mark_func(EVAL_FUNC_ARG_DECL);
extern int eval_assign_question_mark_func(EVAL_FUNC_ARG_DECL);

// init eval_func_, inner_eval_func_, expr_ctx_id_, extra_
int ObStaticEngineExprCG::cg_expr_by_operator(const ObIArray<ObRawExpr *> &raw_exprs,
                                              int64_t &total_ctx_cnt)
{
  int ret = OB_SUCCESS;
  RowDesc row_desc;
  ObExprOperatorFetcher expr_op_fetcher;
  ObExprGeneratorImpl expr_cg_impl(0, 0, NULL, row_desc);
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    expr_op_fetcher.op_ = NULL;
    ObRawExpr *raw_expr = NULL;
    ObExpr *rt_expr = NULL;
    if (OB_ISNULL(raw_expr = raw_exprs.at(i))
        || OB_ISNULL(rt_expr = get_rt_expr(*raw_expr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("arg is null", K(raw_expr), K(rt_expr), K(ret));
    } else if (T_QUESTIONMARK == rt_expr->type_ &&
              (raw_expr->has_flag(IS_TABLE_ASSIGN) || rt_question_mark_eval_)) {
      // generate question mark expr for get param from param store directly
      // if the questionmark is from TABLE_ASSIGN, use eval_assign_question_mark_func
      ObConstRawExpr *c_expr = static_cast<ObConstRawExpr*>(raw_expr);
      int64_t param_idx = 0;
      OZ(c_expr->get_value().get_unknown(param_idx));
      if (OB_SUCC(ret)) {
        rt_expr->extra_ = param_idx;
        rt_expr->eval_func_ = raw_expr->has_flag(IS_TABLE_ASSIGN) ?
                              &eval_assign_question_mark_func:
                              &eval_question_mark_func;
      }
    } else if (!IS_EXPR_OP(rt_expr->type_) || IS_AGGR_FUN(rt_expr->type_)) {
      // do nothing
    } else if (OB_FAIL(expr_cg_impl.generate_expr_operator(*raw_expr, expr_op_fetcher))) {
      LOG_WARN("generate expr operator failed", K(ret));
    } else if (NULL == expr_op_fetcher.op_) {
      // do nothing, some raw do not generate expr operator. e.g: T_OP_ROW
    } else {
      const ObExprOperator *op = expr_op_fetcher.op_;
      if (op->need_rt_ctx()) {
        rt_expr->expr_ctx_id_ = total_ctx_cnt;
        total_ctx_cnt += 1;
      }
      if (OB_FAIL(op->cg_expr(op_cg_ctx_, *raw_expr, *rt_expr))) {
        LOG_WARN("fail to init expr inner", K(ret));
      } else if (OB_NOT_NULL(rt_expr->extra_info_)
                && !ObExprExtraInfoFactory::is_registered(rt_expr->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unregistered type, extra_info_ is not null", K(ret));
      } else if (OB_ISNULL(rt_expr->eval_func_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null evaluate function returned", K(ret));
      } else if (OB_INVALID_INDEX == ObFuncSerialization::get_serialize_index(
              reinterpret_cast<void *>(rt_expr->eval_func_))
          || OB_INVALID_INDEX == ObFuncSerialization::get_serialize_index(
              reinterpret_cast<void *>(rt_expr->eval_batch_func_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("evaluate function or evaluate batch function not serializable, "
                 "may be you should add the function into ob_expr_eval_functions",
                 K(ret), KP(rt_expr->eval_func_), KP(rt_expr->eval_batch_func_),
                 K(*raw_expr), K(*rt_expr));
      } else if (rt_expr->inner_func_cnt_ > 0) {
        if (OB_ISNULL(rt_expr->inner_functions_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL inner functions", K(ret), K(*raw_expr), K(rt_expr->inner_func_cnt_));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < rt_expr->inner_func_cnt_; j++) {
            const uint64_t idx = ObFuncSerialization::get_serialize_index(
                rt_expr->inner_functions_[j]);
            if (0 == idx || OB_INVALID_INDEX == idx) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("inner function not serializable", K(ret), K(idx),
                       KP(rt_expr->inner_functions_[j]), K(*raw_expr), K(*rt_expr));
            }
          }
        }
      }
    }

    // set default eval batch func
    if (OB_SUCC(ret) && batch_size_ > 0
        && NULL != rt_expr->eval_func_ && NULL == rt_expr->eval_batch_func_
        && rt_expr->is_batch_result()) {
      rt_expr->eval_batch_func_ = &expr_default_eval_batch_func;
    }
  }

  return ret;
}

// init res_buf_len_, frame_idx_, datum_off_, res_buf_off_
int ObStaticEngineExprCG::cg_all_frame_layout(const ObIArray<ObRawExpr *> &raw_exprs,
                                              ObExprFrameInfo &expr_info)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObRawExpr *, 64> const_exprs;
  ObSEArray<ObRawExpr *, 64> param_exprs;
  ObSEArray<ObRawExpr *, 64> dynamic_param_exprs;
  ObSEArray<ObRawExpr *, 64> no_const_param_exprs;
  int64_t frame_idx_pos = 0;
  if (OB_FAIL(classify_exprs(raw_exprs,
                             const_exprs,
                             param_exprs,
                             dynamic_param_exprs,
                             no_const_param_exprs))) {
    LOG_WARN("fail to classify exprs", K(raw_exprs), K(ret));
  } else if (OB_FAIL(cg_const_frame_layout(const_exprs,
                                           frame_idx_pos,
                                           expr_info.const_frame_))) {
    LOG_WARN("fail to init const expr datum layout", K(ret), K(const_exprs));
  } else if (OB_FAIL(cg_param_frame_layout(param_exprs,
                                           frame_idx_pos,
                                           expr_info.param_frame_))) {
    LOG_WARN("fail to init param frame layout", K(ret), K(param_exprs));
  } else if (OB_FAIL(cg_dynamic_frame_layout(dynamic_param_exprs,
                                             frame_idx_pos,
                                             expr_info.dynamic_frame_))) {
    LOG_WARN("fail to init const", K(ret), K(dynamic_param_exprs));
  } else if (OB_FAIL(cg_datum_frame_layouts(no_const_param_exprs,
                                           frame_idx_pos,
                                           expr_info.datum_frame_))) {
    LOG_WARN("fail to init const", K(ret), K(no_const_param_exprs));
  } else if (OB_FAIL(alloc_const_frame(const_exprs,
                                       expr_info.const_frame_,
                                       expr_info.const_frame_ptrs_))) {
    LOG_WARN("fail to build const frame", K(ret), K(const_exprs), K(expr_info));
  }

  return ret;
}

// 将表达式按所属frame类型分成4类
int ObStaticEngineExprCG::classify_exprs(const ObIArray<ObRawExpr *> &raw_exprs,
                                        ObIArray<ObRawExpr *> &const_exprs,
                                        ObIArray<ObRawExpr *> &param_exprs,
                                        ObIArray<ObRawExpr *> &dynamic_param_exprs,
                                        ObIArray<ObRawExpr *> &no_const_param_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    ObItemType type = raw_exprs.at(i)->get_expr_type();
    if (T_QUESTIONMARK == type && !rt_question_mark_eval_
      && !raw_exprs.at(i)->has_flag(IS_TABLE_ASSIGN)) {
      if (raw_exprs.at(i)->has_flag(IS_DYNAMIC_PARAM)) {
        if (dynamic_param_exprs.push_back(raw_exprs.at(i))) {
          LOG_WARN("fail to push expr", K(ret), K(i), K(raw_exprs));
        }
      } else {
        if (param_exprs.push_back(raw_exprs.at(i))) {
          LOG_WARN("fail to push expr", K(ret), K(i), K(raw_exprs));
        }
      }
    } else if (IS_CONST_LITERAL(type)) {
      if (OB_FAIL(const_exprs.push_back(raw_exprs.at(i)))) {
        LOG_WARN("fail to push expr", K(ret), K(i), K(raw_exprs));
      }
    } else {
      if (OB_FAIL(no_const_param_exprs.push_back(raw_exprs.at(i)))) {
        LOG_WARN("fail to push expr", K(ret), K(i), K(raw_exprs));
      }
    }
  }

  return ret;
}

// 初始化const expr在frame中布局
int ObStaticEngineExprCG::cg_const_frame_layout(const ObIArray<ObRawExpr *> &const_exprs,
                                               int64_t &frame_index_pos,
                                               ObIArray<ObFrameInfo> &frame_info_arr)
{
  const bool reserve_empty_string = true;
  const bool continuous_datum = true;
  return cg_frame_layout(const_exprs,
                         reserve_empty_string,
                         continuous_datum,
                         frame_index_pos,
                         frame_info_arr);
}

void ObStaticEngineExprCG::get_param_frame_idx(const int64_t idx,
                                               int64_t &frame_idx,
                                               int64_t &datum_idx)
{
  const int64_t cnt_per_frame = ObExprFrameInfo::EXPR_CNT_PER_FRAME;
  if (idx < original_param_cnt_) {
    frame_idx = idx / cnt_per_frame;
    datum_idx = idx % cnt_per_frame;
  } else {
    const int64_t base = original_param_cnt_ <= 0
        ? 0
        : (original_param_cnt_ + cnt_per_frame - 1) / cnt_per_frame;

    frame_idx = (idx - original_param_cnt_) / cnt_per_frame + base;
    datum_idx = (idx - original_param_cnt_) % cnt_per_frame;
  }
}

// 初始化param frame内存布局, 不需要有res_buf_, 在生成param frame时, 均使用动态分配的内存;
// 1. 根据param_expr, 找到该表达式在param store中下标
// 2. 根据param store下标初始化ObExpr中frame布局frame_idx_, datum_off_
// 3. 初始化每个frame的ObFrameInfo
int ObStaticEngineExprCG::cg_param_frame_layout(const ObIArray<ObRawExpr *> &param_exprs,
                                               int64_t &frame_index_pos,
                                               ObIArray<ObFrameInfo> &frame_info_arr)
{
  int ret = OB_SUCCESS;
  if (!param_exprs.empty() && param_cnt_ + flying_param_cnt_ == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  }
  int64_t frame_idx = 0;
  int64_t datum_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_exprs.count(); i++) {
    ObExpr *rt_expr = get_rt_expr(*param_exprs.at(i));
    if (T_QUESTIONMARK != rt_expr->type_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(rt_expr));
    } else {
      ObConstRawExpr *c_expr = static_cast<ObConstRawExpr*>(param_exprs.at(i));
      int64_t param_idx = 0;
      if (OB_FAIL(c_expr->get_value().get_unknown(param_idx))) {
        SQL_LOG(WARN, "get question mark value failed", K(ret), K(*c_expr));
      } else if (param_idx < 0 || param_idx >= param_cnt_ + flying_param_cnt_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid param idx",
                 K(ret), K(param_idx), K(param_cnt_), K(flying_param_cnt_));
      } else {
        get_param_frame_idx(param_idx, frame_idx, datum_idx);
        rt_expr->frame_idx_ = frame_index_pos + frame_idx;
        rt_expr->datum_off_ = datum_idx * DATUM_EVAL_INFO_SIZE;
        rt_expr->eval_info_off_ = rt_expr->datum_off_ + sizeof(ObDatum);
        rt_expr->res_buf_off_ = 0;
        rt_expr->res_buf_len_ = 0;
        // 对于T_QUESTIONMARK的param表达式, 使用extra_记录其实际value在
        // datam_param_store中下标,  通过该下标, 在执行期可以访问
        // datum_param_store中存放的信息,
        // 当前使用场景是在ObExprValuesOp中进行动态cast时,
        // 可以通过该下标最终获取参数化后原始参数值的类型;
        rt_expr->extra_ = param_idx;
      }
    }
  }
  if (OB_SUCC(ret) && !param_exprs.empty()) {
    // 就进行param frame内存分配及初始化;
    int64_t total = param_cnt_;
    int64_t frame_cnt = 0;
    if (total > 0) {
      get_param_frame_idx(total - 1, frame_idx, datum_idx);
      frame_cnt = frame_idx + 1;
    }
    if (OB_FAIL(frame_info_arr.prepare_allocate(frame_cnt))) {
      LOG_WARN("fail to reserve frame infos", K(frame_cnt), K(ret));
    }
    for (int64_t i = frame_cnt - 1; OB_SUCC(ret) && i >= 0; i--) {
      get_param_frame_idx(total - 1, frame_idx, datum_idx);
      CK(frame_idx == i);
      if (OB_SUCC(ret)) {
        ObFrameInfo &frame_info = frame_info_arr.at(i);
        frame_info.expr_cnt_ = datum_idx + 1;
        frame_info.frame_size_ = (datum_idx + 1) * DATUM_EVAL_INFO_SIZE;
        frame_info.frame_idx_ = frame_index_pos + i;
        total -= frame_info.expr_cnt_;
      }
    }
    CK(0 == total);
    if (OB_SUCC(ret)) {
      frame_index_pos += frame_cnt;
    }
  }

  return ret;
}

// 初始化dynamic param expr在frame中布局
int ObStaticEngineExprCG::cg_dynamic_frame_layout(const ObIArray<ObRawExpr *> &exprs,
                                                  int64_t &frame_index_pos,
                                                  ObIArray<ObFrameInfo>& frame_info_arr)
{
  const bool reserve_empty_string = false;
  //需要保证continuous_datum = true， 因为在expr_frame_info中将所有datum预先置为null
  const bool continuous_datum = true;
  return cg_frame_layout(exprs,
                         reserve_empty_string,
                         continuous_datum,
                         frame_index_pos,
                         frame_info_arr);
}

// 初始化非const和param expr在frame中布局
int ObStaticEngineExprCG::cg_datum_frame_layouts(const ObIArray<ObRawExpr *> &exprs,
                                                int64_t &frame_index_pos,
                                                ObIArray<ObFrameInfo>& frame_info_arr)
{
  int ret = OB_SUCCESS;
  const bool reserve_empty_string = false;
  const bool continuous_datum = true;
  if (batch_size_ > 0) {
    ret = cg_frame_layout_vector_version(exprs,
                                         true/*is_vectorized*/,
                                         frame_index_pos,
                                         frame_info_arr);
  } else {
    ret = cg_frame_layout(exprs, reserve_empty_string, continuous_datum, frame_index_pos,
                          frame_info_arr);
  }
  return ret;
}

int ObStaticEngineExprCG::cg_frame_layout_vector_version(const ObIArray<ObRawExpr *> &exprs,
                                                        const bool continuous_datum,
                                                        int64_t &frame_index_pos,
                                                        ObIArray<ObFrameInfo>& frame_info_arr)
{
  int ret = OB_SUCCESS;
  ObSEArray<TmpFrameInfo, 4> tmp_frame_infos;
  if (OB_FAIL(calc_exprs_res_buf_len(exprs))) {
    LOG_WARN("Failed to calc expr res buf len", K(ret));
  } else if (OB_FAIL(create_tmp_frameinfo(exprs, tmp_frame_infos, frame_index_pos))) {
    LOG_WARN("Failed to create tmp frame info", K(ret));
  }

  // caculate the datums layout in each frame
  for (int64_t idx = 0; OB_SUCC(ret) && idx < tmp_frame_infos.count(); idx++) {
    const ObFrameInfo &frame = tmp_frame_infos.at(idx).frame_info_;
    int64_t expr_start_pos = tmp_frame_infos.at(idx).expr_start_pos_;
    ObArrayHelper<ObRawExpr *> frame_exprs(
        frame.expr_cnt_,
        const_cast<ObRawExpr **>(exprs.get_data() + expr_start_pos),
        frame.expr_cnt_);
    OZ(arrange_datums_data(frame_exprs, frame, continuous_datum));
  }
  // copy tmpFrameInfo into ObFrameInfo
  if (OB_SUCC(ret)) {
    frame_index_pos += tmp_frame_infos.count();
    if (OB_FAIL(frame_info_arr.reserve(tmp_frame_infos.count()))) {
      LOG_WARN("fail to reserve frame infos", K(ret), K(tmp_frame_infos.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_frame_infos.count(); i++) {
        if (OB_FAIL((frame_info_arr.push_back(tmp_frame_infos.at(i).frame_info_)))) {
          LOG_WARN("fail to push frame info", K(ret), K(tmp_frame_infos), K(i));
        }
      } // for end
    }
  }

  return ret;
}

int ObStaticEngineExprCG::cg_frame_layout(const ObIArray<ObRawExpr *> &exprs,
                                          const bool reserve_empty_string,
                                          const bool continuous_datum,
                                          int64_t &frame_index_pos,
                                          ObIArray<ObFrameInfo>& frame_info_arr)
{
  int ret = OB_SUCCESS;
  int64_t start_pos = 0; // frame中第一个expr在rt_exprs中偏移
  int64_t frame_expr_cnt = 0;
  int64_t frame_size = 0;
  int32_t frame_idx = 0;
  //获取每个frame size, expr cnt, frame_idx, 以及涉及的expr在该类expr中的偏移
  ObSEArray<TmpFrameInfo, 4> tmp_frame_infos;
  //init res_buf_len_
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    ObExpr *rt_expr = get_rt_expr(*exprs.at(i));
    uint32_t def_res_len = ObDatum::get_reserved_size(rt_expr->obj_datum_map_);
    if (ObDynReserveBuf::supported(rt_expr->datum_meta_.type_)) {
      if (!reserve_empty_string) {
        // 有的表达式没有设置accuracy的length，这里的max_length_就是默认的-1
        // 负数的情况下，对于string的res_buf_len_依然使用def_res_len
        if (rt_expr->max_length_ > 0) {
          rt_expr->res_buf_len_ = min(def_res_len,
                                      static_cast<uint32_t>(rt_expr->max_length_));
        } else {
          rt_expr->res_buf_len_ = def_res_len;
        }
      } else {
        rt_expr->res_buf_len_ = 0;
      }
    } else {
      rt_expr->res_buf_len_ = def_res_len;
    }
  }
  for (int64_t expr_idx = 0;
       OB_SUCC(ret) && expr_idx < exprs.count();
       expr_idx++) {
    ObExpr *rt_expr = get_rt_expr(*exprs.at(expr_idx));
    const int64_t datum_size = DATUM_EVAL_INFO_SIZE + reserve_data_consume(*rt_expr);
    if (frame_size + datum_size <= MAX_FRAME_SIZE) {
      frame_size += datum_size;
      frame_expr_cnt++;
    } else {
      if (OB_FAIL(tmp_frame_infos.push_back(TmpFrameInfo(start_pos,
                                                         frame_expr_cnt,
                                                         frame_index_pos + frame_idx,
                                                         frame_size,
                                                         0, /*zero_init_pos*/
                                                         frame_size/*zero_init_size*/)))) {
        LOG_WARN("fail to push frame_size", K(ret));
      } else {
        ++frame_idx;
        frame_size = datum_size;
        start_pos = expr_idx;
        frame_expr_cnt = 1;
      }
    }
  } // for end
  //将最后一个frame加到tmp_frame_infos中
  if (OB_SUCC(ret) && 0 != frame_size) {
    if (OB_FAIL(tmp_frame_infos.push_back(TmpFrameInfo(start_pos,
                                                       frame_expr_cnt,
                                                       frame_index_pos + frame_idx,
                                                       frame_size,
                                                       0, /*zero_init_pos*/
                                                       frame_size/*zero_init_size*/)))) {
      LOG_WARN("fail to push frame_size", K(ret), K(frame_size));
    }
  }
  //初始化每个ObExpr中frame_idx, datum_off_, res_buf_off_
  for (int64_t idx = 0; OB_SUCC(ret) && idx < tmp_frame_infos.count(); idx++) {
    const ObFrameInfo &frame = tmp_frame_infos.at(idx).frame_info_;
    int64_t expr_start_pos = tmp_frame_infos.at(idx).expr_start_pos_;
    ObArrayHelper<ObRawExpr *> frame_exprs(
        frame.expr_cnt_,
        const_cast<ObRawExpr **>(exprs.get_data() + expr_start_pos),
        frame.expr_cnt_);
    OZ(arrange_datum_data(frame_exprs, frame, continuous_datum));
  }
  // init ObFrameInfo
  if (OB_SUCC(ret)) {
    frame_index_pos += tmp_frame_infos.count();
    if (OB_FAIL(frame_info_arr.reserve(tmp_frame_infos.count()))) {
      LOG_WARN("fail to reserve frame infos", K(ret), K(tmp_frame_infos.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_frame_infos.count(); i++) {
        if (OB_FAIL((frame_info_arr.push_back(tmp_frame_infos.at(i).frame_info_)))) {
          LOG_WARN("fail to push frame info", K(ret), K(tmp_frame_infos), K(i));
        }
      } // for end
    }
  }

  return ret;
}

int ObStaticEngineExprCG::arrange_datum_data(ObIArray<ObRawExpr *> &exprs,
                                             const ObFrameInfo &frame,
                                             const bool continuous_datum)
{
  int ret = OB_SUCCESS;
  if (continuous_datum) {
    int64_t data_off = frame.expr_cnt_ * DATUM_EVAL_INFO_SIZE;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr *e = get_rt_expr(*exprs.at(i));
      e->frame_idx_ = frame.frame_idx_;
      e->datum_off_ = i * DATUM_EVAL_INFO_SIZE;
      e->eval_info_off_ = e->datum_off_ + sizeof(ObDatum);
      const int64_t consume_size = reserve_data_consume(*e);
      if (consume_size > 0) {
        data_off += consume_size;
        e->res_buf_off_ = data_off - e->res_buf_len_;
        e->dyn_buf_header_offset_ = e->res_buf_off_ - sizeof(ObDynReserveBuf);
      } else {
        e->res_buf_off_ = 0;
      }
    }
    CK(data_off == frame.frame_size_);
  } else {
    // FIXME bin.lb: ALIGN_SIZE may affect the performance, set to 1 if no affect
    // make sure all ObDatum is aligned with %ALIGN_SIZE
    const static int64_t ALIGN_SIZE = 8;
    // offset for data only area
    int64_t data_off = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr *e = get_rt_expr(*exprs.at(i));
      data_off += DATUM_EVAL_INFO_SIZE;
      if (!ob_is_string_type(e->datum_meta_.type_) && 0 == (e->res_buf_len_ % ALIGN_SIZE)) {
        // data follow ObDatum
        data_off += reserve_data_consume(*e);
      }
    }
    // offset for datum + data area
    int64_t datum_off = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr *e = get_rt_expr(*exprs.at(i));
      e->frame_idx_ = frame.frame_idx_;
      e->datum_off_ = datum_off;
      e->eval_info_off_ = e->datum_off_ + sizeof(ObDatum);
      datum_off += DATUM_EVAL_INFO_SIZE;
      const int64_t consume_size = reserve_data_consume(*e);
      if (!ob_is_string_type(e->datum_meta_.type_) && 0 == (e->res_buf_len_ % ALIGN_SIZE)) {
        if (consume_size > 0) {
          datum_off += consume_size;
          e->res_buf_off_ = datum_off - e->res_buf_len_;
          e->dyn_buf_header_offset_ = e->res_buf_off_ - sizeof(ObDynReserveBuf);
        } else {
          e->res_buf_off_ = 0;
        }
      } else {
        if (consume_size > 0) {
          data_off += consume_size;
          e->res_buf_off_ = data_off - e->res_buf_len_;
          e->dyn_buf_header_offset_ = e->res_buf_off_ - sizeof(ObDynReserveBuf);
        } else {
          e->res_buf_off_ = 0;
        }
      }
    }
    CK(data_off == frame.frame_size_);
  }
  return ret;
}

int ObStaticEngineExprCG::arrange_datums_data(ObIArray<ObRawExpr *> &exprs,
                                             const ObFrameInfo &frame,
                                             const bool continuous_datum)
{
  int ret = OB_SUCCESS;
  if (continuous_datum) {
    // Layout1: Frame is separated from meta part and data part.
    // Meta part(datum header) are allocated continuously.
    // Reserved data/buf part are allocated continuously
    // Frame layouts:
    // +--------------------------------+
    // | Datums in Expr1                |
    // |--------------------------------+
    // | PVT Skip in Expr1              |
    // +--------------------------------+
    // | Datums in Expr2                |
    // +--------------------------------+
    // | PVT Skip in Expr2              |
    // +--------------------------------+
    // |      ......                    |
    // |--------------------------------+
    // | EvalInfo in Expr1              |
    // +--------------------------------+
    // | EvalInfo in Expr2              |
    // +--------------------------------+
    // |      ......                    |
    // +--------------------------------+
    // | EvalFlag in Expr1              |
    // +--------------------------------+
    // | EvalFlag in Expr2              |
    // +--------------------------------+
    // |      ......                    |
    // +--------------------------------+
    // | Dynamic buf header in expr1    |
    // +--------------------------------+
    // | Dynamic buf header in expr2    |
    // |--------------------------------+
    // |      ......                    |
    // +--------------------------------+
    // | Reserved datum data in expr1   |
    // +--------------------------------+
    // | Reserved datum data in expr2   |
    // |--------------------------------+
    // |      ......                    |
    // +--------------------------------+
    int64_t total_header_len = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr *e = get_rt_expr(*exprs.at(i));
      e->datum_off_ = total_header_len;
      total_header_len +=
          get_expr_skip_vector_size(*e) /* skip bitmap + evalflag bitmap */ +
          get_datums_header_size(*e);
    }

    uint32_t eval_info_total = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr *e = get_rt_expr(*exprs.at(i));
      e->eval_info_off_ = total_header_len + eval_info_total;
      eval_info_total += sizeof(ObEvalInfo);
    }
    total_header_len += eval_info_total;

    uint32_t eval_flags_total = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr *e = get_rt_expr(*exprs.at(i));
      e->eval_flags_off_ = total_header_len + eval_flags_total;
      eval_flags_total += get_expr_skip_vector_size(*e);
    }
    total_header_len += eval_flags_total;

    uint32_t dyn_buf_total = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr *e = get_rt_expr(*exprs.at(i));
      e->dyn_buf_header_offset_ = total_header_len + dyn_buf_total;
      dyn_buf_total += dynamic_buf_header_size(*e);
    }
    total_header_len += dyn_buf_total;

    uint32_t expr_data_offset = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr *e = get_rt_expr(*exprs.at(i));
      e->frame_idx_ = frame.frame_idx_;
      // datum meta/header part:
      // e->datum_off_ is marked in previous iteratoration, use it directly!
      e->pvt_skip_off_ = e->datum_off_ + get_datums_header_size(*e);
      // datum data part: reserved buf data + dynamic buf header
      e->res_buf_off_ =
          total_header_len + expr_data_offset;
      expr_data_offset += e->res_buf_len_ * get_expr_datums_count(*e);
      LOG_TRACE("expression details during CG", K(e->is_batch_result()), KPC(e),
                K(expr_data_offset));
    }
    CK((total_header_len + expr_data_offset) == frame.frame_size_);
  } else {
    // Layout2: Frame is seperated by exprs
    // All data(metas + reserved data/buf) within one expr are allocated continuously
    // Frame layouts:
    // +--------------------------------+
    // | Datums in Expr1                |
    // |--------------------------------+
    // | EvalInfo in Expr1              |
    // +--------------------------------+
    // | EvalFlag in Expr1              |
    // |--------------------------------+
    // | PVT Skip in Expr1              |
    // +--------------------------------+
    // | Dynamic buf header in expr1    |
    // +--------------------------------+
    // | Reserved datum data in expr1   |
    // +--------------------------------+
    // | Datums in Expr2                |
    // +--------------------------------+
    // | EvalInfo in Expr2              |
    // +--------------------------------+
    // | EvalFlag in Expr2              |
    // +--------------------------------+
    // | PVT Skip in Expr2              |
    // |--------------------------------+
    // | Dynamic buf header in expr2    |
    // +--------------------------------+
    // | Reserved datum data in expr2   |
    // |--------------------------------+
    // |      ......                    |
    // +--------------------------------+
    uint64_t expr_offset = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr *e = get_rt_expr(*exprs.at(i));
      e->frame_idx_ = frame.frame_idx_;
      // datum meta/header part:
      e->datum_off_ = expr_offset;
      e->eval_info_off_  = e->datum_off_ + get_datums_header_size(*e);
      e->eval_flags_off_ = e->eval_info_off_ + sizeof(ObEvalInfo);
      e->pvt_skip_off_   = e->eval_flags_off_ + get_expr_skip_vector_size(*e);
      // datum data part: reserved buf data + dynamic buf header
      const int64_t cur_off = e->pvt_skip_off_ + get_expr_skip_vector_size(*e);
      e->res_buf_off_ = cur_off + dynamic_buf_header_size(*e);
      expr_offset = cur_off + reserve_datums_buf_len(*e);
      LOG_TRACE("expression details during CG", K(e->is_batch_result()), KPC(e),
                K(expr_offset));
      CK(get_expr_datums_size(*e) == (expr_offset - e->datum_off_));
    }
    CK(expr_offset == frame.frame_size_);
  }
  return ret;
}

// 分配常量表达式frame内存, 并初始化
int ObStaticEngineExprCG::alloc_const_frame(const ObIArray<ObRawExpr *> &exprs,
                                            const ObIArray<ObFrameInfo> &const_frames,
                                            ObIArray<char *> &frame_ptrs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(frame_ptrs.reserve(const_frames.count()))) {
    LOG_WARN("fail to init frame ptr", K(ret), K(const_frames));
  }
  int64_t expr_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < const_frames.count(); i++) {
    char *frame_mem = static_cast<char *>(allocator_.alloc(const_frames.at(i).frame_size_));
    if (OB_ISNULL(frame_mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), K(const_frames.at(i).frame_size_));
    } else {
      memset(frame_mem, 0, const_frames.at(i).frame_size_);
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < const_frames.at(i).expr_cnt_; j++) {
      ObRawExpr *raw_expr = exprs.at(expr_idx++);
      ObExpr *rt_expr = get_rt_expr(*raw_expr);
      ObObj tmp_obj;
      if (!IS_CONST_LITERAL(raw_expr->get_expr_type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("not const expr", K(ret));
      } else if (OB_FAIL(ob_write_obj(allocator_,
                                      static_cast<ObConstRawExpr *>(raw_expr)->get_value(),
                                      tmp_obj))) {
        LOG_WARN("fail to deep copy obj", K(ret));
      } else {
        ObDatum *datum = reinterpret_cast<ObDatum *>(
            frame_mem + j * DATUM_EVAL_INFO_SIZE);
        datum->ptr_ = frame_mem + rt_expr->res_buf_off_;
        datum->from_obj(tmp_obj);
        if (0 == datum->len_) {
          datum->ptr_ = NULL;
        } else {
          if (is_lob_storage(tmp_obj.get_type())) {
            if (OB_FAIL(ob_adjust_lob_datum(tmp_obj, rt_expr->obj_meta_, allocator_, datum))) {
              LOG_WARN("fail to adjust lob datum", K(ret), K(tmp_obj), K(rt_expr->obj_meta_), K(datum));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(frame_ptrs.push_back(frame_mem))) {
      LOG_WARN("fail to push const frame", K(ret));
    }
  } // for end

  return ret;
}

int ObStaticEngineExprCG::cg_expr_basic_funcs(const ObIArray<ObRawExpr *> &raw_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    ObExpr *rt_expr = get_rt_expr(*raw_exprs.at(i));
    if (OB_ISNULL(rt_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rt expr is null", K(ret), K(*raw_exprs.at(i)));
    } else {
      rt_expr->basic_funcs_ = ObDatumFuncs::get_basic_func(rt_expr->datum_meta_.type_,
                                                        rt_expr->datum_meta_.cs_type_,
                                                        rt_expr->datum_meta_.scale_,
                                                        lib::is_oracle_mode(),
                                                        rt_expr->obj_meta_.has_lob_header());
      CK(NULL != rt_expr->basic_funcs_);
    }
  }
  return ret;
}

int ObStaticEngineExprCG::generate_calculable_exprs(
                                            const ObIArray<ObHiddenColumnItem> &calculable_exprs,
                                            ObPreCalcExprFrameInfo &pre_calc_frame)
{
  int ret = OB_SUCCESS;
  flying_param_cnt_ = calculable_exprs.count();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(inner_generate_calculable_exprs(calculable_exprs, pre_calc_frame))) {
    LOG_WARN("failed to generate calculate exprs", K(ret));
  } else if (OB_FAIL(pre_calc_frame.pre_calc_rt_exprs_.prepare_allocate(
                       calculable_exprs.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < calculable_exprs.count(); i++) {
      ObExpr *rt_expr = get_rt_expr(*calculable_exprs.at(i).expr_);
      if (OB_ISNULL(rt_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null rt expr", K(ret));
      } else {
        pre_calc_frame.pre_calc_rt_exprs_.at(i) = rt_expr;
      }
    } // for end
  }

  if (OB_SUCC(ret)) {
    // Replace flying question mark expr with the corresponding calculable expr.
    // Because the calculable exprs may reference to each other.
    FOREACH_CNT_X(e, pre_calc_frame.rt_exprs_, OB_SUCC(ret)) {
      // %extra_ is the array index of param store
      if (T_QUESTIONMARK == e->type_ && e->extra_ >= param_cnt_) {
        int64_t idx = e->extra_ - param_cnt_;
        CK(idx < flying_param_cnt_);
        if (OB_SUCC(ret)) {
          ObExpr **parents = e->parents_;
          uint32_t parent_cnt = e->parent_cnt_;
          *e = *pre_calc_frame.pre_calc_rt_exprs_.at(idx);
          e->parents_ = parents;
          e->parent_cnt_ = parent_cnt;
        }
      }
    }
  }
  flying_param_cnt_ = 0;
  return ret;
}

int ObStaticEngineExprCG::generate_calculable_expr(ObRawExpr *raw_expr,
                                                   ObPreCalcExprFrameInfo &pre_calc_frame)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null raw expr", K(ret), K(raw_expr));
  } else {
    ObSEArray<ObHiddenColumnItem, 1> calculable_exprs;
    if (OB_FAIL(calculable_exprs.prepare_allocate(1))) {
      LOG_WARN("failed to prepare allocate raw expr", K(ret));
    } else {
      calculable_exprs.at(0).hidden_idx_ = 0;
      calculable_exprs.at(0).expr_ = raw_expr;

      if (OB_FAIL(generate_calculable_exprs(calculable_exprs, pre_calc_frame))) {
        LOG_WARN("failed to generate pre calculate exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObStaticEngineExprCG::inner_generate_calculable_exprs(
                              const common::ObIArray<ObHiddenColumnItem> &calculable_exprs,
                              ObPreCalcExprFrameInfo &expr_info)
{
  int ret = OB_SUCCESS;
  if (calculable_exprs.count() <= 0) {
    // do nothing
  } else {
    ObRawExprUniqueSet raw_exprs(false);
    ARRAY_FOREACH(calculable_exprs, i) {
      const ObHiddenColumnItem &hidden_item = calculable_exprs.at(i);
      if (OB_ISNULL(hidden_item.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null raw expr", K(ret));
      } else if (OB_FAIL(raw_exprs.append(hidden_item.expr_))) {
        LOG_WARN("failed to push back element", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(generate(raw_exprs, expr_info))) {
      LOG_WARN("failed to flatten and cg exprs", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObStaticEngineExprCG::alloc_so_check_exprs(const ObIArray<ObRawExpr *> &raw_exprs,
                                               ObExprFrameInfo &expr_info)
{
  // TODO bin.lb: add vectorize support.
  int ret = OB_SUCCESS;
  if (expr_info.rt_exprs_.count() > STACK_OVERFLOW_CHECK_DEPTH) {
    auto &exprs = expr_info.rt_exprs_;
    ObArray<ObExprCallDepth> exprs_call_depth;
    ObArray<ObExprCallDepth *> unchecked_exprs;
    // 1. allocate expr call depth array && associate with ObExpr.
    OZ(exprs_call_depth.prepare_allocate(exprs.count()));
    OZ(unchecked_exprs.reserve(exprs.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr *e = &exprs.at(i);
      ObExprCallDepth *ecd = &exprs_call_depth.at(i);
      ecd->expr_ = e;
      if (0 == e->parent_cnt_ && e->arg_cnt_ > 0) {
        OZ(unchecked_exprs.push_back(ecd));
      }
    }

    // 2. check expr call depth && decide we should add stack overflow check above which expr.
    int64_t stack_check_expr_cnt = 0;
    int64_t max_call_depth = 0;
    while (OB_SUCC(ret) && !unchecked_exprs.empty()) {
      ObExprCallDepth *ecd = NULL;
      ObExpr *e = NULL;
      OZ(unchecked_exprs.pop_back(ecd));
      if (OB_SUCC(ret)) {
        e = ecd->expr_;
        for (int64_t i = 0; OB_SUCC(ret) && i < e->arg_cnt_; i++) {
          ObExpr *c_e = e->args_[i];
          const int64_t c_ecd_idx = c_e - exprs.get_data();
          ObExprCallDepth *c_ecd = &exprs_call_depth.at(c_ecd_idx);
          c_ecd->max_check_depth_ = std::max(c_ecd->max_check_depth_, ecd->max_check_depth_ + 1);
          c_ecd->max_call_depth_ = std::max(c_ecd->max_call_depth_, ecd->max_call_depth_ + 1);
          max_call_depth = std::max(c_ecd->max_call_depth_, max_call_depth);
          c_ecd->checked_parent_cnt_ += 1;
          // only the last parent reach the expr need check the expr.
          if (c_ecd->checked_parent_cnt_ == c_e->parent_cnt_ && c_e->arg_cnt_ > 0) {
            if (c_ecd->max_check_depth_ >= STACK_OVERFLOW_CHECK_DEPTH) {
              c_ecd->max_check_depth_ = 0;
              c_ecd->need_stack_check_ = true;
              stack_check_expr_cnt += 1;
            }
            OZ(unchecked_exprs.push_back(c_ecd));
          }
        }
      }
    }

    // 3. expand expr array && add stack overflow check expr.
    if (OB_SUCC(ret) && stack_check_expr_cnt > 0) {
      LOG_TRACE("stack check expr needed",
                K(exprs.count()), K(stack_check_expr_cnt), K(max_call_depth));

      FOREACH_CNT_X(ecd, exprs_call_depth, OB_SUCC(ret)) {
        if (ecd->need_stack_check_) {
          ObExpr *e = ecd->expr_;
          // stack overflow check expr can not added above T_OP_ROW
          if (T_OP_ROW == e->type_ && e->parent_cnt_ > 0) {
            e = e->parents_[0];
            if (T_OP_ROW == e->type_ && e->parent_cnt_ > 0) {
              e = e->parents_[0];
              if (T_OP_ROW == e->type_) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("T_OP_ROW can not be nested twice", K(ret));
              }
            }
          }
          if (OB_SUCC(ret) && e->parent_cnt_ > 0) {
            e->need_stack_check_ = true;
          }
        }
      } // END FOREACH_CNT_X
    }
  }
  return ret;
}

int ObStaticEngineExprCG::calc_exprs_res_buf_len(const ObIArray<ObRawExpr *> &raw_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    ObExpr *rt_expr = get_rt_expr(*raw_exprs.at(i));
    uint32_t def_res_len = ObDatum::get_reserved_size(rt_expr->obj_datum_map_);
    if (ObDynReserveBuf::supported(rt_expr->datum_meta_.type_)) {
      if (rt_expr->max_length_ > 0) {
        rt_expr->res_buf_len_ = min(def_res_len,
                                    static_cast<uint32_t>(rt_expr->max_length_));
      } else {
        // max_length may equal -1
        rt_expr->res_buf_len_ = def_res_len;
      }
    } else {
      // OBOBJ_DATUM_MAP_TYPE_TO_RES_SIZE_MAP
      rt_expr->res_buf_len_ = def_res_len;
    }
  }

  return ret;
}

// Construct tmpFrameInfo instances.
// tmpFrameInfo consist of two parts: meta part + data part
// meta part： datums / evalinfo/ evalflags/ skip map
// data part:  reserved datums buf
int ObStaticEngineExprCG::create_tmp_frameinfo(const common::ObIArray<ObRawExpr *> &raw_exprs,
                                               common::ObIArray<TmpFrameInfo> &tmp_frame_infos,
                                               int64_t &frame_index_pos)
{
  int ret = OB_SUCCESS;
  int32_t frame_idx = 0;
  int64_t frame_expr_cnt = 0;
  int64_t frame_size = 0;
  int64_t zero_init_pos = 0;
  int64_t zero_init_size = 0;
  int64_t initial_expr_id = 0; // frame中第一个expr在rt_exprs数组中id
  for (int64_t expr_idx = 0; OB_SUCC(ret) && expr_idx < raw_exprs.count(); expr_idx++) {
    ObExpr *rt_expr = get_rt_expr(*raw_exprs.at(expr_idx));
    const int64_t expr_datums_size = get_expr_datums_size(*rt_expr);
    const int64_t expr_zero_init_pos = get_datums_header_size(*rt_expr)
                                       + get_expr_skip_vector_size(*rt_expr);
    const int64_t expr_zero_init_size =  dynamic_buf_header_size(*rt_expr) +
      sizeof(ObEvalInfo) + get_expr_skip_vector_size(*rt_expr);

    if (expr_datums_size > MAX_FRAME_SIZE) {
      // FIXME: should never hit this block.
      // expr_datums_size larger than max frame size, means _rowsets_max_rows is tool large.
      // So far manually tune sys arg _rowsets_max_rows to a smaller value.
      // Long term: create a function automaticaly tune the datums size
      LOG_WARN("Frame allocation failure, please tune _rowsets_max_rows to a smaller number",
                K(expr_datums_size));
      ret = OB_ERR_UNEXPECTED;
    } else {
      if (frame_size + expr_datums_size <= MAX_FRAME_SIZE) {
        frame_size += expr_datums_size;
        zero_init_pos += expr_zero_init_pos;
        zero_init_size += expr_zero_init_size;
        frame_expr_cnt++;
      } else {
        if (frame_expr_cnt == 1) {
          LOG_WARN("Only one expr in a frame, tune _rowsets_max_rows to a small value, frame (id)",
                   K(frame_idx));
        }
        if (OB_FAIL(tmp_frame_infos.push_back(TmpFrameInfo(initial_expr_id,
                                                           frame_expr_cnt,
                                                           frame_index_pos + frame_idx,
                                                           frame_size,
                                                           zero_init_pos,
                                                           zero_init_size)))) {
          LOG_WARN("fail to push frame_size", K(ret));
        } else {
          ++frame_idx;
          zero_init_pos = expr_zero_init_pos;
          zero_init_size = expr_zero_init_size;
          frame_size = expr_datums_size;
          initial_expr_id = expr_idx;
          frame_expr_cnt = 1;
        }
      }
    }
  } // for end

  // append last tmp frame
  if (OB_SUCC(ret) && 0 != frame_size) {
    if (OB_FAIL(tmp_frame_infos.push_back(TmpFrameInfo(initial_expr_id,
                                                       frame_expr_cnt,
                                                       frame_index_pos + frame_idx,
                                                       frame_size,
                                                       zero_init_pos,
                                                       zero_init_size)))) {
      LOG_WARN("fail to push frame_size", K(ret), K(frame_size));
    }
  }
  return ret;
}

int ObStaticEngineExprCG::get_vectorized_exprs(
    const common::ObIArray<ObRawExpr *> &raw_exprs,
    common::ObIArray<ObRawExpr *> &no_const_param_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    if (is_vectorized_expr(raw_exprs.at(i))) {
      if (OB_FAIL(no_const_param_exprs.push_back(raw_exprs.at(i)))) {
        LOG_WARN("fail to push expr", K(ret), K(i), K(raw_exprs));
      }
    }
  }

  return ret;
}

ObStaticEngineExprCG::ObExprBatchSize ObStaticEngineExprCG::get_expr_execute_size(
    const common::ObIArray<ObRawExpr *> &raw_exprs)
{
  ObExprBatchSize size = ObExprBatchSize::full;
  bool has_udf_expr = false;
  bool has_usr_var_expr = false;
  bool has_wrapper_inner_expr = false;
  for (int64_t i = 0; i < raw_exprs.count(); i++) {
    ObItemType type = raw_exprs.at(i)->get_expr_type();
    if (T_OP_GET_USER_VAR == type) {
      has_usr_var_expr = true;
    } else if (T_FUN_UDF == type) {
      has_udf_expr = true;
      if (!(static_cast<ObUDFRawExpr*>(raw_exprs.at(i))->is_deterministic())) {
        size = ObExprBatchSize::one;
        break;
      }
    } else if (T_FUN_SYS_WRAPPER_INNER == type) {
      size = ObExprBatchSize::one;
      break;
    } else if (T_REF_QUERY == type) {
      if (static_cast<ObQueryRefRawExpr*>(raw_exprs.at(i))->is_cursor()) {
        size = ObExprBatchSize::one;
        break;
      }
    }
    LOG_DEBUG("check expr type", K(type), KPC(raw_exprs.at(i)));
    // There are certain cases: that a SQL could not be executed vectorizely
    // For example:
    // A UDF functions may modify session value or global var, and could not be
    // executed vectorizely
    // E.G:
    //   create function f1() returns int
    //   begin
    //    if @a=1 then set @b='abc';
    //    else set @b=1;
    //    end if;
    //    set @a=1;
    //    return 0;
    //   end|
    // create table t1 (a int)|
    // insert into t1 (a) values (1), (2)|
    // set @a=0|
    // select f1(), @b from t1
    //
    // Execute UDF f1() vectorizely would set @b='abc' within a batch and leave
    // NO chance to disaply @b=1
    if (has_udf_expr & has_usr_var_expr) {
      size = ObExprBatchSize::one;
      break;
    }
    if (is_large_data(raw_exprs.at(i)->get_data_type()) &&
        raw_exprs.at(i)->get_expr_type() != T_FUN_TOP_FRE_HIST &&
        raw_exprs.at(i)->get_expr_type() != T_FUN_HYBRID_HIST) {
      // 1. batchsize should be scale down when longtext/mediumtext/lob shows up
      // 2. keep searching
      size = ObExprBatchSize::small;
    }

  }
  LOG_TRACE("can_execute_vectorizely", K(size));
  return size;
}
int ObStaticEngineExprCG::divide_probably_local_exprs(common::ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;;
  int64_t begin = 0;
  int64_t end = exprs.count() - 1;

  while (begin < end) {
    while (begin <= end && !exprs.at(begin)->has_flag(IS_PROBABLY_LOCAL)) {
      begin++;
    }
    while (begin <= end && exprs.at(end)->has_flag(IS_PROBABLY_LOCAL)) {
      end--;
    }
    if (begin < end) {
      std::swap(exprs.at(begin), exprs.at(end));
      begin++;
      end--;
    }
  }

  return ret;
}

int ObStaticEngineExprCG::gen_expr_with_row_desc(const ObRawExpr *expr,
                                                 const RowDesc &row_desc,
                                                 ObIAllocator &allocator,
                                                 ObSQLSessionInfo *session,
                                                 ObSchemaGetterGuard *schema_guard,
                                                 ObTempExpr *&temp_expr)
{
  int ret = OB_SUCCESS;
  temp_expr = NULL;
  ObRawExprUniqueSet flattened_raw_exprs(true);
  char *buf = static_cast<char *>(allocator.alloc(sizeof(ObTempExpr)));
  if (NULL == schema_guard) {
    schema_guard = &session->get_cached_schema_guard_info().get_schema_guard();
  }
  CK(OB_NOT_NULL(buf));
  CK(OB_NOT_NULL(expr));
  if (OB_SUCC(ret)) {
    LOG_TRACE("generate temp expr", K(*expr), K(row_desc));
    temp_expr = new(buf)ObTempExpr(allocator);
    ObStaticEngineExprCG expr_cg(allocator,
                                 session,
                                 schema_guard,
                                 0,
                                 0,
                                 GET_MIN_CLUSTER_VERSION()); // ?
    expr_cg.set_rt_question_mark_eval(true);
    expr_cg.set_need_flatten_gen_col(false);
    OZ(expr_cg.generate(const_cast<ObRawExpr *>(expr), flattened_raw_exprs, *temp_expr));
  }
  // generate row_idx to column expr pair;
  if (OB_SUCC(ret)) {
    ObSEArray<RowIdxColumnPair, 6> idx_col_arr;
    for (int64_t i = 0; OB_SUCC(ret) && i < flattened_raw_exprs.get_expr_array().count(); i++) {
      ObRawExpr *raw_expr = flattened_raw_exprs.get_expr_array().at(i);
      if (T_REF_COLUMN == raw_expr->get_expr_type()) {
        int64_t idx = 0;
        OZ(row_desc.get_idx(raw_expr, idx));
        OZ(idx_col_arr.push_back(RowIdxColumnPair(idx, i)));
      }
    } // for end
    OZ(temp_expr->idx_col_arr_.assign(idx_col_arr));
    OX(temp_expr->expr_idx_ = get_rt_expr(*expr) - &(temp_expr->rt_exprs_.at(0)));
    CK(temp_expr->expr_idx_ >=0 && temp_expr->expr_idx_ <= temp_expr->rt_exprs_.count());
  }

  return ret;
}


// use the highest bit of %inner_functions_ to mark expr is added
static inline void mark_expr_is_added(ObExpr &e)
{
  uint64_t &v = *reinterpret_cast<uint64_t *>(&e.inner_functions_);
  v |= 1UL << 63;
}

static inline void clear_expr_is_added(ObExpr &e)
{
  uint64_t &v = *reinterpret_cast<uint64_t *>(&e.inner_functions_);
  v &= ~(1UL << 63);
}

static inline bool expr_is_added(ObExpr &e)
{
  uint64_t &v = *reinterpret_cast<uint64_t *>(&e.inner_functions_);
  return v >> 63;
}

int ObStaticEngineExprCG::generate_partial_expr_frame(
    const ObPhysicalPlan &plan,
    ObExprFrameInfo &partial_expr_frame_info,
    ObIArray<ObRawExpr *> &raw_exprs)
{
  int ret = OB_SUCCESS;
  const ObExprFrameInfo &global = plan.get_expr_frame_info();
  ObExprFrameInfo &partial = partial_expr_frame_info;
  CK(&global.rt_exprs_ == &partial.rt_exprs_);

  ObArray<ObExpr *> exprs;
  OZ(exprs.reserve(raw_exprs.count()));

  // <1>: get all exprs need to partial serialization

  // add %raw_exprs to %exprs
  FOREACH_CNT_X(raw, raw_exprs, OB_SUCC(ret)) {
    CK(NULL != *raw);
    if (OB_SUCC(ret)) {
      ObExpr *e = get_rt_expr(**raw);
      CK(NULL != e);
      if (OB_SUCC(ret)) {
        if (!expr_is_added(*e)) {
          OZ(exprs.push_back(e));
          mark_expr_is_added(*e);
        }
      }
    }
  }

  // flatten %exprs
  if (OB_SUCC(ret)) {
    int64_t inc = 0;
    int64_t begin = 0;
    do {
      const int64_t cnt = exprs.count();
      for (int64_t idx = begin; OB_SUCC(ret) && idx < cnt; idx++) {
        ObExpr *expr = exprs.at(idx);
        for (int64_t i = 0 ; OB_SUCC(ret) && i < expr->arg_cnt_; i++) {
          ObExpr *e = expr->args_[i];
          if (!expr_is_added(*e)) {
            OZ(exprs.push_back(e));
            mark_expr_is_added(*e);
          }
        }
      }
      begin = cnt;
      inc = exprs.count() - cnt;
    } while (OB_SUCC(ret) && inc > 0);
  }

  // always clear expr is added flag
  FOREACH_CNT(e, exprs) {
    clear_expr_is_added(**e);
  }

  // <2> setup %ser_expr_marks_ and found farthest expr for each frame
  int64_t frame_cnt = global.const_frame_ptrs_.count()
      + global.param_frame_.count()
      + global.dynamic_frame_.count()
      + global.datum_frame_.count();
  ObSEArray<ObExpr *, 8> farthest_exprs;
  const ObExpr *base = global.rt_exprs_.count() > 0 ? &global.rt_exprs_.at(0) : NULL;
  int64_t batch_size = std::max<int64_t>(plan.get_batch_size(), 1);
  auto frame_max_offset = [](const ObExpr &e, const int64_t batch_size)
  {
    int64_t size = 0;
    size = std::max<int64_t>(size, e.datum_off_ + sizeof(ObDatum));
    size = std::max<int64_t>(size, e.eval_info_off_ + sizeof(ObEvalInfo));
    size = std::max<int64_t>(size, e.res_buf_off_ + batch_size * e.res_buf_len_);
    if (e.is_batch_result()) {
      size = std::max<int64_t>(size, e.eval_flags_off_ + ObBitVector::memory_size(batch_size));
      size = std::max<int64_t>(size, e.pvt_skip_off_ + ObBitVector::memory_size(batch_size));
    }
    return size;
  };
  OZ(farthest_exprs.prepare_allocate(frame_cnt));
  if (OB_SUCC(ret)) {
    FOREACH(it, farthest_exprs) {
      *it = NULL;
    }
    int64_t max_expr_idx = 0;
    FOREACH_CNT_X(e, exprs, OB_SUCC(ret)) {
      const int64_t expr_idx = *e - base;
      max_expr_idx = std::max(max_expr_idx, expr_idx);
      // set max_expr_idx to index of parent or %parent_ array  deserialize
      // will got index out of expr array error.
      if ((*e)->parent_cnt_ > 0) {
        for (int64_t i = 0; i < (*e)->parent_cnt_; i++) {
          max_expr_idx = std::max(max_expr_idx, (*e)->parents_[i] - base);
        }
      }

      ObExpr *&p = farthest_exprs.at((*e)->frame_idx_);
      if (NULL == p || frame_max_offset(*p, batch_size) < frame_max_offset(**e, batch_size)) {
        p = *e;
      }
    }
    OZ(partial.ser_expr_marks_.init(max_expr_idx + 1));
    OZ(partial.ser_expr_marks_.prepare_allocate(max_expr_idx + 1));
    FOREACH_CNT_X(it, partial.ser_expr_marks_, OB_SUCC(ret)) {
      *it = 0;
    }
    FOREACH_CNT_X(e, exprs, OB_SUCC(ret)) {
      const int64_t expr_idx = *e - base;
      partial.ser_expr_marks_.at(expr_idx) = 1;
    }
  }

  // <3>setup frames with farthest expr

  if (OB_SUCC(ret)) {
    partial.need_ctx_cnt_ = global.need_ctx_cnt_;
    OZ(partial.const_frame_ptrs_.assign(global.const_frame_ptrs_));
    OZ(partial.const_frame_.assign(global.const_frame_));
    OZ(partial.param_frame_.assign(global.param_frame_));
    OZ(partial.dynamic_frame_.assign(global.dynamic_frame_));
    OZ(partial.datum_frame_.assign(global.datum_frame_));

    auto set_param_frame = [&](ObFrameInfo &f) {
      ObExpr *e = farthest_exprs.at(f.frame_idx_);
      int cnt = NULL == e
          ? std::min<int64_t>(1, f.expr_cnt_)
          : (e->datum_off_ / DATUM_EVAL_INFO_SIZE) + 1;
      f.expr_cnt_ = std::min<int64_t>(f.expr_cnt_, cnt);
      f.frame_size_ = std::min<int64_t>(f.frame_size_, cnt * DATUM_EVAL_INFO_SIZE);
    };
    FOREACH_CNT_X(f, partial.param_frame_, OB_SUCC(ret)) {
      set_param_frame(*f);
    }
    FOREACH_CNT_X(f, partial.dynamic_frame_, OB_SUCC(ret)) {
      set_param_frame(*f);
    }
    FOREACH_CNT_X(f, partial.datum_frame_, OB_SUCC(ret)) {
      ObExpr *e = farthest_exprs.at(f->frame_idx_);
      int64_t size = DATUM_EVAL_INFO_SIZE; // avoid zero frame size
      if (NULL != e) {
        size = std::max(size, frame_max_offset(*e, batch_size));
      }
      f->frame_size_ = std::min<int64_t>(size, f->frame_size_);
    }
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("generate partial expr frame", K(partial), K(global));
  }

  return ret;
}

bool ObStaticEngineExprCG::is_vectorized_expr(const ObRawExpr *raw_expr) const
{
  bool bret = false;
  ObItemType type = raw_expr->get_expr_type();
  if (T_QUESTIONMARK == type || IS_CONST_LITERAL(type)) {
  } else {
    bret = raw_expr->is_vectorize_result();
  }
  return bret;
}

int ObStaticEngineExprCG::compute_max_batch_size(const ObRawExpr *raw_expr)
{
  const ObObjMeta &result_meta = raw_expr->get_result_meta();
  return (MAX_FRAME_SIZE - sizeof(ObEvalInfo)) /
           (1 + sizeof(ObDatum) + reserve_data_consume(result_meta.get_type()));
}

} // end namespace sql
} // end namespace oceanbase
