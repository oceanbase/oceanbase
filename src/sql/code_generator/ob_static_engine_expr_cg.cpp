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
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase {
using namespace common;
namespace sql {

struct ObExprCallDepth {
  ObExprCallDepth()
      : expr_(NULL), need_stack_check_(false), max_check_depth_(0), max_call_depth_(0), checked_parent_cnt_(0)
  {}

  TO_STRING_KV(KP(expr_), K(need_stack_check_), K(max_check_depth_), K(max_call_depth_), K(checked_parent_cnt_));

  ObExpr* expr_;
  bool need_stack_check_;
  // max call depth of stack check expr or root
  int64_t max_check_depth_;
  int64_t max_call_depth_;
  int64_t checked_parent_cnt_;
};

// 1. expand all raw exprs:
//    cg: c1 + 1, c1 + c2  ==>  c1, 1, c1 + 1, c2, c1 + c2
// 2. construct ObExpr, add ObExpr ptr to RawExpr
// 3. init ObExpr info, return frame infos
int ObStaticEngineExprCG::generate(const ObRawExprUniqueSet& all_raw_exprs, ObExprFrameInfo& expr_info)
{
  int ret = OB_SUCCESS;
  ObRawExprUniqueSet flattened_raw_exprs(allocator_);
  if (all_raw_exprs.count() <= 0) {
    // do nothing
  } else if (OB_FAIL(flattened_raw_exprs.init())) {
    LOG_WARN("fail to create hash set", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::flatten_raw_exprs(all_raw_exprs, flattened_raw_exprs))) {
    LOG_WARN("failed to flatten raw exprs", K(ret));
  } else if (OB_FAIL(construct_exprs(flattened_raw_exprs.get_expr_array(), expr_info.rt_exprs_))) {
    LOG_WARN("failed to construct rt exprs", K(ret));
  } else if (OB_FAIL(cg_exprs(flattened_raw_exprs.get_expr_array(), expr_info))) {
    LOG_WARN("failed to cg exprs", K(ret));
  }
  return ret;
}

// Attention : Please think over before you have to use this function.
// This function is different from generate_rt_expr.
// It won't put raw_expr into cur_op_exprs_ because it doesn't need to be calculated.
void* ObStaticEngineExprCG::get_left_value_rt_expr(const ObRawExpr& raw_expr)
{
  return reinterpret_cast<void*>(get_rt_expr(raw_expr));
}

int ObStaticEngineExprCG::generate_rt_expr(const ObRawExpr& raw_expr, ObIArray<ObRawExpr*>& exprs, ObExpr*& rt_expr)
{
  int ret = OB_SUCCESS;
  rt_expr = get_rt_expr(raw_expr);
  if (OB_ISNULL(rt_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rt expr is null", K(ret), K(raw_expr));
  } else if (OB_FAIL(exprs.push_back(const_cast<ObRawExpr*>(&raw_expr)))) {
    LOG_WARN("fail to push rt expr", K(ret));
  }

  return ret;
}

ObExpr* ObStaticEngineExprCG::get_rt_expr(const ObRawExpr& raw_expr)
{
  return raw_expr.rt_expr_;
}

int ObStaticEngineExprCG::construct_exprs(const ObIArray<ObRawExpr*>& raw_exprs, ObIArray<ObExpr>& rt_exprs)
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

int ObStaticEngineExprCG::cg_exprs(const ObIArray<ObRawExpr*>& raw_exprs, ObExprFrameInfo& expr_info)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    ObExpr* rt_expr = NULL;
    if (OB_ISNULL(raw_exprs.at(i)) || OB_ISNULL(rt_expr = get_rt_expr(*raw_exprs.at(i)))) {
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
      LOG_WARN("fail to init expr parenets", K(ret), K(raw_exprs));
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
int ObStaticEngineExprCG::cg_expr_basic(const ObIArray<ObRawExpr*>& raw_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    ObRawExpr* raw_expr = raw_exprs.at(i);
    ObExpr* rt_expr = get_rt_expr(*raw_expr);
    const ObObjMeta& result_meta = raw_expr->get_result_meta();
    // init type_
    rt_expr->type_ = raw_expr->get_expr_type();
    rt_expr->is_boolean_ = raw_expr->is_bool_expr();
    if (T_OP_ROW != raw_expr->get_expr_type()) {
      // init datum_meta_
      rt_expr->datum_meta_ = ObDatumMeta(result_meta.get_type(),
          result_meta.get_collation_type(),
          raw_expr->get_result_type().get_scale(),
          raw_expr->get_result_type().get_precision());
      // init obj_meta_
      rt_expr->obj_meta_ = result_meta;
      rt_expr->obj_meta_.set_scale(rt_expr->datum_meta_.scale_);
      // init max_length_
      rt_expr->max_length_ = raw_expr->get_result_type().get_length();
      // init obj_datum_map_
      rt_expr->obj_datum_map_ = ObDatum::get_obj_datum_map_type(result_meta.get_type());
    }
    if (T_REF_COLUMN == raw_expr->get_expr_type()) {
      const ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(raw_expr);
      // generated column's arg_cnt = 1, used to store dependant expr's rt_expr
      if (col_expr->is_generated_column() || OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == col_expr->get_column_id()) {
        rt_expr->arg_cnt_ = 1;
        ObExpr** buf = static_cast<ObExpr**>(allocator_.alloc(sizeof(ObExpr*)));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret));
        } else {
          memset(buf, 0, sizeof(ObExpr*));
          rt_expr->args_ = buf;
          if (OB_ISNULL(get_rt_expr(*col_expr->get_dependant_expr()))) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("expr is null", K(ret));
          } else {
            rt_expr->args_[0] = get_rt_expr(*col_expr->get_dependant_expr());
            rt_expr->eval_func_ = ObExprUtil::eval_generated_column;
          }
        }
      }
    } else {
      // init arg_cnt_
      rt_expr->arg_cnt_ = raw_expr->get_param_count();
      // init args_;
      if (rt_expr->arg_cnt_ > 0) {
        int64_t alloc_size = rt_expr->arg_cnt_ * sizeof(ObExpr*);
        ObExpr** buf = static_cast<ObExpr**>(allocator_.alloc(alloc_size));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret));
        } else {
          memset(buf, 0, alloc_size);
          rt_expr->args_ = buf;
          for (int64_t i = 0; OB_SUCC(ret) && i < raw_expr->get_param_count(); i++) {
            ObRawExpr* child_expr = NULL;
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
  }  // for end

  return ret;
}

// init parent_cnt_, parents_
int ObStaticEngineExprCG::cg_expr_parents(const ObIArray<ObRawExpr*>& raw_exprs)
{
  int ret = OB_SUCCESS;
  // init expr parent cnt
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    ObExpr* rt_expr = get_rt_expr(*raw_exprs.at(i));
    if (rt_expr->arg_cnt_ > 0 && OB_ISNULL(rt_expr->args_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(rt_expr->arg_cnt_), KP(rt_expr->args_));
    }
    for (int64_t child_idx = 0; OB_SUCC(ret) && child_idx < rt_expr->arg_cnt_; child_idx++) {
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
    ObExpr* rt_expr = get_rt_expr(*raw_exprs.at(i));
    if (rt_expr->parent_cnt_ > 0) {
      int64_t alloc_size = 0;
      alloc_size = rt_expr->parent_cnt_ * sizeof(ObExpr*);
      if (OB_ISNULL(rt_expr->parents_ = static_cast<ObExpr**>(allocator_.alloc(alloc_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(alloc_size));
      }
    }
  }
  // reset parent cnt;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    ObExpr* rt_expr = get_rt_expr(*raw_exprs.at(i));
    rt_expr->parent_cnt_ = 0;
  }
  // init parent_cnt_ and parents_
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    ObExpr* rt_expr = get_rt_expr(*raw_exprs.at(i));
    for (int64_t arg_idx = 0; OB_SUCC(ret) && arg_idx < rt_expr->arg_cnt_; arg_idx++) {
      if (OB_ISNULL(rt_expr->args_[arg_idx])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("child expr is null", K(ret), K(arg_idx));
      } else {
        uint32_t& parent_cnt = rt_expr->args_[arg_idx]->parent_cnt_;
        rt_expr->args_[arg_idx]->parents_[parent_cnt] = rt_expr;
        parent_cnt++;
      }
    }
  }

  return ret;
}

// init eval_func_, inner_eval_func_, expr_ctx_id_, extra_
int ObStaticEngineExprCG::cg_expr_by_operator(const ObIArray<ObRawExpr*>& raw_exprs, int64_t& total_ctx_cnt)
{
  int ret = OB_SUCCESS;
  RowDesc row_desc;
  ObExprOperatorFetcher expr_op_fetcher;
  ObExprGeneratorImpl expr_cg_impl(0, 0, NULL, row_desc);
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    expr_op_fetcher.op_ = NULL;
    ObRawExpr* raw_expr = NULL;
    ObExpr* rt_expr = NULL;
    if (OB_ISNULL(raw_expr = raw_exprs.at(i)) || OB_ISNULL(rt_expr = get_rt_expr(*raw_expr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("arg is null", K(raw_expr), K(rt_expr), K(ret));
    } else if (!IS_EXPR_OP(rt_expr->type_) || IS_AGGR_FUN(rt_expr->type_)) {
      // do nothing
    } else if (OB_FAIL(expr_cg_impl.generate_expr_operator(*raw_expr, expr_op_fetcher))) {
      LOG_WARN("generate expr operator failed", K(ret));
    } else if (NULL == expr_op_fetcher.op_) {
      // do nothing, some raw do not generate expr operator. e.g: T_OP_ROW
    } else {
      const ObExprOperator* op = expr_op_fetcher.op_;
      if (op->need_rt_ctx()) {
        rt_expr->expr_ctx_id_ = total_ctx_cnt;
        total_ctx_cnt += 1;
      }
      if (OB_FAIL(op->cg_expr(get_operator_cg_ctx(), *raw_expr, *rt_expr))) {
        LOG_WARN("fail to init expr inner", K(ret));
      } else if (OB_ISNULL(rt_expr->eval_func_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null evaluate function returned", K(ret));
      } else if (OB_INVALID_INDEX ==
                 ObFuncSerialization::get_serialize_index(reinterpret_cast<void*>(rt_expr->eval_func_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("evaluate function not serializable, "
                 "may be you should add the function into ob_expr_eval_functions",
            K(ret),
            KP(rt_expr->eval_func_),
            K(*raw_expr),
            K(*rt_expr));
      } else if (rt_expr->inner_func_cnt_ > 0) {
        if (OB_ISNULL(rt_expr->inner_functions_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL inner functions", K(ret), K(*raw_expr), K(rt_expr->inner_func_cnt_));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < rt_expr->inner_func_cnt_; j++) {
            const uint64_t idx = ObFuncSerialization::get_serialize_index(rt_expr->inner_functions_[j]);
            if (0 == idx || OB_INVALID_INDEX == idx) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("inner function not serializable",
                  K(ret),
                  K(idx),
                  KP(rt_expr->inner_functions_[j]),
                  K(*raw_expr),
                  K(*rt_expr));
            }
          }
        }
      }
    }
  }

  return ret;
}

// init res_buf_len_, frame_idx_, datum_off_, res_buf_off_
int ObStaticEngineExprCG::cg_all_frame_layout(const ObIArray<ObRawExpr*>& raw_exprs, ObExprFrameInfo& expr_info)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObRawExpr*, 64> const_exprs;
  ObSEArray<ObRawExpr*, 64> param_exprs;
  ObSEArray<ObRawExpr*, 64> dynamic_param_exprs;
  ObSEArray<ObRawExpr*, 64> no_const_param_exprs;
  int64_t frame_idx_pos = 0;
  if (OB_FAIL(classify_exprs(raw_exprs, const_exprs, param_exprs, dynamic_param_exprs, no_const_param_exprs))) {
    LOG_WARN("fail to classify exprs", K(raw_exprs), K(ret));
  } else if (OB_FAIL(cg_const_frame_layout(const_exprs, frame_idx_pos, expr_info.const_frame_))) {
    LOG_WARN("fail to init const expr datum layout", K(ret), K(const_exprs));
  } else if (OB_FAIL(cg_param_frame_layout(param_exprs, frame_idx_pos, expr_info.param_frame_))) {
    LOG_WARN("fail to init param frame layout", K(ret), K(param_exprs));
  } else if (OB_FAIL(cg_dynamic_frame_layout(dynamic_param_exprs, frame_idx_pos, expr_info.dynamic_frame_))) {
    LOG_WARN("fail to init const", K(ret), K(dynamic_param_exprs));
  } else if (OB_FAIL(cg_datum_frame_layout(no_const_param_exprs, frame_idx_pos, expr_info.datum_frame_))) {
    LOG_WARN("fail to init const", K(ret), K(no_const_param_exprs));
  } else if (OB_FAIL(alloc_const_frame(const_exprs, expr_info.const_frame_, expr_info.const_frame_ptrs_))) {
    LOG_WARN("fail to build const frame", K(ret), K(const_exprs), K(expr_info));
  }

  return ret;
}

// classify expr to frames
int ObStaticEngineExprCG::classify_exprs(const ObIArray<ObRawExpr*>& raw_exprs, ObIArray<ObRawExpr*>& const_exprs,
    ObIArray<ObRawExpr*>& param_exprs, ObIArray<ObRawExpr*>& dynamic_param_exprs,
    ObIArray<ObRawExpr*>& no_const_param_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    ObItemType type = raw_exprs.at(i)->get_expr_type();
    if (T_QUESTIONMARK == type) {
      if (raw_exprs.at(i)->has_flag(IS_EXEC_PARAM)) {
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

struct TmpFrameInfo {
  TmpFrameInfo() : expr_start_pos_(0), frame_info_()
  {}
  TmpFrameInfo(uint64_t start_pos, uint64_t expr_cnt, uint32_t frame_idx, uint32_t frame_size)
      : expr_start_pos_(start_pos), frame_info_(expr_cnt, frame_idx, frame_size)
  {}
  TO_STRING_KV(K_(expr_start_pos), K_(frame_info));

public:
  uint64_t expr_start_pos_;
  ObFrameInfo frame_info_;
};

int ObStaticEngineExprCG::cg_no_reserved_buf_layout(
    const ObIArray<ObRawExpr*>& exprs, int64_t& frame_index_pos, ObIArray<ObFrameInfo>& frame_info_arr)
{
  int ret = OB_SUCCESS;
  int64_t expr_cnt_per_frame = ObExprFrameInfo::EXPR_CNT_PER_FRAME;
  int64_t expr_cnt = exprs.count();
  // init frame_idx_, datum_off_
  for (int64_t i = 0; i < expr_cnt; i++) {
    ObExpr* rt_expr = get_rt_expr(*exprs.at(i));
    rt_expr->frame_idx_ = frame_index_pos + i / expr_cnt_per_frame;
    rt_expr->datum_off_ = (i % expr_cnt_per_frame) * DATUM_EVAL_INFO_SIZE;
    rt_expr->eval_info_off_ = rt_expr->datum_off_ + sizeof(ObDatum);
    rt_expr->res_buf_off_ = 0;
    rt_expr->res_buf_len_ = 0;
  }
  // init frame info
  int64_t frame_cnt = (expr_cnt + expr_cnt_per_frame - 1) / expr_cnt_per_frame;
  if (OB_FAIL(frame_info_arr.reserve(frame_cnt))) {
    LOG_WARN("fail to reserve frame infos", K(frame_cnt), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < frame_cnt; i++) {
    int64_t frame_size = 0;
    if (i == frame_cnt - 1) {
      frame_size = (expr_cnt % expr_cnt_per_frame) * DATUM_EVAL_INFO_SIZE;
    } else {
      frame_size = expr_cnt_per_frame * DATUM_EVAL_INFO_SIZE;
    }
    if (OB_FAIL(frame_info_arr.push_back(ObFrameInfo(expr_cnt_per_frame, frame_index_pos + i, frame_size)))) {
      LOG_WARN("fail to push frame info", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    frame_index_pos += frame_cnt;
  }

  return ret;
}

int ObStaticEngineExprCG::cg_const_frame_layout(
    const ObIArray<ObRawExpr*>& const_exprs, int64_t& frame_index_pos, ObIArray<ObFrameInfo>& frame_info_arr)
{
  const bool reserve_empty_string = true;
  const bool continuous_datum = true;
  return cg_frame_layout(const_exprs, reserve_empty_string, continuous_datum, frame_index_pos, frame_info_arr);
}

int ObStaticEngineExprCG::cg_param_frame_layout(
    const ObIArray<ObRawExpr*>& param_exprs, int64_t& frame_index_pos, ObIArray<ObFrameInfo>& frame_info_arr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_store_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param_store_));
  }
  int64_t expr_cnt_per_frame = ObExprFrameInfo::EXPR_CNT_PER_FRAME;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_exprs.count(); i++) {
    ObExpr* rt_expr = get_rt_expr(*param_exprs.at(i));
    if (T_QUESTIONMARK != rt_expr->type_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(rt_expr));
    } else {
      ObConstRawExpr* c_expr = static_cast<ObConstRawExpr*>(param_exprs.at(i));
      int64_t param_idx = 0;
      if (OB_FAIL(c_expr->get_value().get_unknown(param_idx))) {
        SQL_LOG(WARN, "get question mark value failed", K(ret), K(*c_expr));
      } else if (param_idx < 0 || param_idx >= param_store_->count() + flying_param_cnt_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid param idx", K(ret), K(param_idx), K(param_store_->count()), K(flying_param_cnt_));
      } else {
        rt_expr->frame_idx_ = frame_index_pos + param_idx / expr_cnt_per_frame;
        rt_expr->datum_off_ = (param_idx % expr_cnt_per_frame) * DATUM_EVAL_INFO_SIZE;
        rt_expr->eval_info_off_ = rt_expr->datum_off_ + sizeof(ObDatum);
        rt_expr->res_buf_off_ = 0;
        rt_expr->res_buf_len_ = 0;
        rt_expr->extra_ = param_idx;
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t frame_cnt = (param_store_->count() + expr_cnt_per_frame - 1) / expr_cnt_per_frame;
    if (OB_FAIL(frame_info_arr.reserve(frame_cnt))) {
      LOG_WARN("fail to reserve frame infos", K(frame_cnt), K(ret));
    }
    int64_t frame_size = 0;
    int64_t expr_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < frame_cnt; i++) {
      if (i == frame_cnt - 1) {
        expr_cnt = 0 == (param_store_->count() % expr_cnt_per_frame) ? expr_cnt_per_frame * DATUM_EVAL_INFO_SIZE
                                                                     : param_store_->count() % expr_cnt_per_frame;
        frame_size = expr_cnt * DATUM_EVAL_INFO_SIZE;
      } else {
        frame_size = expr_cnt_per_frame * DATUM_EVAL_INFO_SIZE;
        expr_cnt = expr_cnt_per_frame;
      }
      if (OB_FAIL(frame_info_arr.push_back(ObFrameInfo(expr_cnt, frame_index_pos + i, frame_size)))) {
        LOG_WARN("fail to push frame info", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      frame_index_pos += frame_cnt;
    }
  }

  return ret;
}

int ObStaticEngineExprCG::cg_dynamic_frame_layout(
    const ObIArray<ObRawExpr*>& exprs, int64_t& frame_index_pos, ObIArray<ObFrameInfo>& frame_info_arr)
{
  const bool reserve_empty_string = true;
  const bool continuous_datum = true;
  return cg_frame_layout(exprs, reserve_empty_string, continuous_datum, frame_index_pos, frame_info_arr);
}

int ObStaticEngineExprCG::cg_datum_frame_layout(
    const ObIArray<ObRawExpr*>& exprs, int64_t& frame_index_pos, ObIArray<ObFrameInfo>& frame_info_arr)
{
  const bool reserve_empty_string = false;
  // const bool continuous_datum = false;
  const bool continuous_datum = true;
  return cg_frame_layout(exprs, reserve_empty_string, continuous_datum, frame_index_pos, frame_info_arr);
}

int ObStaticEngineExprCG::cg_frame_layout(const ObIArray<ObRawExpr*>& exprs, const bool reserve_empty_string,
    const bool continuous_datum, int64_t& frame_index_pos, ObIArray<ObFrameInfo>& frame_info_arr)
{
  int ret = OB_SUCCESS;
  int64_t start_pos = 0;
  int64_t frame_expr_cnt = 0;
  int64_t frame_size = 0;
  int32_t frame_idx = 0;
  // get frame size, expr cnt, frame_idx, and offset
  ObSEArray<TmpFrameInfo, 4> tmp_frame_infos;
  // init res_buf_len_
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    ObExpr* rt_expr = get_rt_expr(*exprs.at(i));
    uint32_t def_res_len = ObDatum::get_reserved_size(rt_expr->obj_datum_map_);
    if (ObDynReserveBuf::supported(rt_expr->datum_meta_.type_)) {
      if (!reserve_empty_string) {
        // some expr's accuracy not set, max_length_ is -1
        if (rt_expr->max_length_ > 0) {
          rt_expr->res_buf_len_ = min(def_res_len, static_cast<uint32_t>(rt_expr->max_length_));
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
  for (int64_t expr_idx = 0; OB_SUCC(ret) && expr_idx < exprs.count(); expr_idx++) {
    ObExpr* rt_expr = get_rt_expr(*exprs.at(expr_idx));
    const int64_t datum_size = DATUM_EVAL_INFO_SIZE + reserve_data_consume(*rt_expr);
    if (frame_size + datum_size <= MAX_FRAME_SIZE) {
      frame_size += datum_size;
      frame_expr_cnt++;
    } else {
      if (OB_FAIL(tmp_frame_infos.push_back(
              TmpFrameInfo(start_pos, frame_expr_cnt, frame_index_pos + frame_idx, frame_size)))) {
        LOG_WARN("fail to push frame_size", K(ret));
      } else {
        ++frame_idx;
        frame_size = datum_size;
        start_pos = expr_idx;
        frame_expr_cnt = 1;
      }
    }
  }  // for end
  if (OB_SUCC(ret) && 0 != frame_size) {
    if (OB_FAIL(tmp_frame_infos.push_back(
            TmpFrameInfo(start_pos, frame_expr_cnt, frame_index_pos + frame_idx, frame_size)))) {
      LOG_WARN("fail to push frame_size", K(ret), K(frame_size));
    }
  }
  // init ObExpr's frame_idx, datum_off_, res_buf_off_
  for (int64_t idx = 0; OB_SUCC(ret) && idx < tmp_frame_infos.count(); idx++) {
    const ObFrameInfo& frame = tmp_frame_infos.at(idx).frame_info_;
    int64_t expr_start_pos = tmp_frame_infos.at(idx).expr_start_pos_;
    ObArrayHelper<ObRawExpr*> frame_exprs(
        frame.expr_cnt_, const_cast<ObRawExpr**>(exprs.get_data() + expr_start_pos), frame.expr_cnt_);
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
      }  // for end
    }
  }

  return ret;
}

int ObStaticEngineExprCG::arrange_datum_data(
    ObIArray<ObRawExpr*>& exprs, const ObFrameInfo& frame, const bool continuous_datum)
{
  int ret = OB_SUCCESS;
  if (continuous_datum) {
    int64_t data_off = frame.expr_cnt_ * DATUM_EVAL_INFO_SIZE;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr* e = get_rt_expr(*exprs.at(i));
      e->frame_idx_ = frame.frame_idx_;
      e->datum_off_ = i * DATUM_EVAL_INFO_SIZE;
      e->eval_info_off_ = e->datum_off_ + sizeof(ObDatum);
      const int64_t consume_size = reserve_data_consume(*e);
      if (consume_size > 0) {
        data_off += consume_size;
        e->res_buf_off_ = data_off - e->res_buf_len_;
      } else {
        e->res_buf_off_ = 0;
      }
    }
    CK(data_off == frame.frame_size_);
  } else {
    // FIXME : ALIGN_SIZE may affect the performance, set to 1 if no affect
    // make sure all ObDatum is aligned with %ALIGN_SIZE
    const static int64_t ALIGN_SIZE = 8;
    // offset for data only area
    int64_t data_off = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr* e = get_rt_expr(*exprs.at(i));
      data_off += DATUM_EVAL_INFO_SIZE;
      if (!ob_is_string_type(e->datum_meta_.type_) && 0 == (e->res_buf_len_ % ALIGN_SIZE)) {
        // data follow ObDatum
        data_off += reserve_data_consume(*e);
      }
    }
    // offset for datum + data area
    int64_t datum_off = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr* e = get_rt_expr(*exprs.at(i));
      e->frame_idx_ = frame.frame_idx_;
      e->datum_off_ = datum_off;
      e->eval_info_off_ = e->datum_off_ + sizeof(ObDatum);
      datum_off += DATUM_EVAL_INFO_SIZE;
      const int64_t consume_size = reserve_data_consume(*e);
      if (!ob_is_string_type(e->datum_meta_.type_) && 0 == (e->res_buf_len_ % ALIGN_SIZE)) {
        if (consume_size > 0) {
          datum_off += consume_size;
          e->res_buf_off_ = datum_off - e->res_buf_len_;
        } else {
          e->res_buf_off_ = 0;
        }
      } else {
        if (consume_size > 0) {
          data_off += consume_size;
          e->res_buf_off_ = data_off - e->res_buf_len_;
        } else {
          e->res_buf_off_ = 0;
        }
      }
    }
    CK(data_off == frame.frame_size_);
  }
  return ret;
}

// all const expr frame memory
int ObStaticEngineExprCG::alloc_const_frame(
    const ObIArray<ObRawExpr*>& exprs, const ObIArray<ObFrameInfo>& const_frames, ObIArray<char*>& frame_ptrs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(frame_ptrs.reserve(const_frames.count()))) {
    LOG_WARN("fail to init frame ptr", K(ret), K(const_frames));
  }
  int64_t expr_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < const_frames.count(); i++) {
    char* frame_mem = static_cast<char*>(allocator_.alloc(const_frames.at(i).frame_size_));
    if (OB_ISNULL(frame_mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), K(const_frames.at(i).frame_size_));
    } else {
      memset(frame_mem, 0, const_frames.at(i).frame_size_);
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < const_frames.at(i).expr_cnt_; j++) {
      ObRawExpr* raw_expr = exprs.at(expr_idx++);
      ObExpr* rt_expr = get_rt_expr(*raw_expr);
      ObObj tmp_obj;
      if (!IS_CONST_LITERAL(raw_expr->get_expr_type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("not const expr", K(ret));
      } else if (OB_FAIL(ob_write_obj(allocator_, static_cast<ObConstRawExpr*>(raw_expr)->get_value(), tmp_obj))) {
        LOG_WARN("fail to deep copy obj", K(ret));
      } else {
        ObDatum* datum = reinterpret_cast<ObDatum*>(frame_mem + j * DATUM_EVAL_INFO_SIZE);
        datum->ptr_ = frame_mem + rt_expr->res_buf_off_;
        datum->from_obj(tmp_obj);
        if (0 == datum->len_) {
          datum->ptr_ = NULL;
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(frame_ptrs.push_back(frame_mem))) {
      LOG_WARN("fail to push const frame", K(ret));
    }
  }  // for end

  return ret;
}

int ObStaticEngineExprCG::cg_expr_basic_funcs(const ObIArray<ObRawExpr*>& raw_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_exprs.count(); i++) {
    ObExpr* rt_expr = get_rt_expr(*raw_exprs.at(i));
    if (OB_ISNULL(rt_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rt expr is null", K(ret), K(*raw_exprs.at(i)));
    } else {
      rt_expr->basic_funcs_ = ObDatumFuncs::get_basic_func(rt_expr->datum_meta_.type_, rt_expr->datum_meta_.cs_type_);
      CK(NULL != rt_expr->basic_funcs_);
    }
  }
  return ret;
}

int ObStaticEngineExprCG::generate_calculable_exprs(
    const ObIArray<ObHiddenColumnItem>& calculable_exprs, ObPreCalcExprFrameInfo& pre_calc_frame)
{
  int ret = OB_SUCCESS;
  flying_param_cnt_ = calculable_exprs.count();
  CK(NULL != param_store_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(inner_generate_calculable_exprs(calculable_exprs, pre_calc_frame))) {
    LOG_WARN("failed to generate calculate exprs", K(ret));
  } else if (OB_FAIL(pre_calc_frame.pre_calc_rt_exprs_.prepare_allocate(calculable_exprs.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else {
    // TODO deal with array binding param
    for (int i = 0; OB_SUCC(ret) && i < calculable_exprs.count(); i++) {
      ObExpr* rt_expr = get_rt_expr(*calculable_exprs.at(i).expr_);
      if (OB_ISNULL(rt_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null rt expr", K(ret));
      } else {
        pre_calc_frame.pre_calc_rt_exprs_.at(i) = rt_expr;
      }
    }  // for end
  }

  if (OB_SUCC(ret)) {
    // Replace flying question mark expr with the corresponding calculable expr.
    // Because the calculable exprs may reference to each other.
    FOREACH_CNT_X(e, pre_calc_frame.rt_exprs_, OB_SUCC(ret))
    {
      // %extra_ is the array index of param store
      if (T_QUESTIONMARK == e->type_ && e->extra_ >= param_store_->count()) {
        int64_t idx = e->extra_ - param_store_->count();
        CK(idx < flying_param_cnt_);
        if (OB_SUCC(ret)) {
          ObExpr** parents = e->parents_;
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

int ObStaticEngineExprCG::generate_calculable_expr(ObRawExpr* raw_expr, ObPreCalcExprFrameInfo& pre_calc_frame)
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
    const common::ObIArray<ObHiddenColumnItem>& calculable_exprs, ObPreCalcExprFrameInfo& expr_info)
{
  int ret = OB_SUCCESS;
  if (calculable_exprs.count() <= 0) {
    // do nothing
  } else {
    ObRawExprUniqueSet raw_exprs(allocator_);
    if (OB_FAIL(raw_exprs.init())) {
      LOG_WARN("fail to create hash set", K(ret));
    }
    ARRAY_FOREACH(calculable_exprs, i)
    {
      const ObHiddenColumnItem& hidden_item = calculable_exprs.at(i);
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

int ObStaticEngineExprCG::alloc_so_check_exprs(const ObIArray<ObRawExpr*>& raw_exprs, ObExprFrameInfo& expr_info)
{
  int ret = OB_SUCCESS;
  if (expr_info.rt_exprs_.count() > STACK_OVERFLOW_CHECK_DEPTH) {
    auto& exprs = expr_info.rt_exprs_;
    ObArray<ObExprCallDepth> exprs_call_depth;
    ObArray<ObExprCallDepth*> unchecked_exprs;
    // 1. allocate expr call depth array && associate with ObExpr.
    OZ(exprs_call_depth.prepare_allocate(exprs.count()));
    OZ(unchecked_exprs.reserve(exprs.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr* e = &exprs.at(i);
      ObExprCallDepth* ecd = &exprs_call_depth.at(i);
      ecd->expr_ = e;
      if (0 == e->parent_cnt_ && e->arg_cnt_ > 0) {
        OZ(unchecked_exprs.push_back(ecd));
      }
    }

    // 2. check expr call depth && decide we should add stack overflow check above which expr.
    int64_t stack_check_expr_cnt = 0;
    int64_t max_call_depth = 0;
    while (OB_SUCC(ret) && !unchecked_exprs.empty()) {
      ObExprCallDepth* ecd = NULL;
      ObExpr* e = NULL;
      OZ(unchecked_exprs.pop_back(ecd));
      if (OB_SUCC(ret)) {
        e = ecd->expr_;
        for (int64_t i = 0; OB_SUCC(ret) && i < e->arg_cnt_; i++) {
          ObExpr* c_e = e->args_[i];
          const int64_t c_ecd_idx = c_e - exprs.get_data();
          ObExprCallDepth* c_ecd = &exprs_call_depth.at(c_ecd_idx);
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
      LOG_TRACE("stack check expr needed", K(exprs.count()), K(stack_check_expr_cnt), K(max_call_depth));
      ObExpr* ori_base = exprs.get_data();
      OZ(exprs.reserve(exprs.count() + stack_check_expr_cnt));
      ObExpr* base = exprs.get_data();
      // relocate expr ptr
      if (OB_SUCC(ret) && ori_base != base) {
        const int64_t offset = reinterpret_cast<char*>(base) - reinterpret_cast<char*>(ori_base);
        FOREACH_CNT(e, raw_exprs)
        {
          (*e)->set_rt_expr(reinterpret_cast<ObExpr*>(reinterpret_cast<char*>(get_rt_expr(**e)) + offset));
        }
        FOREACH_CNT(e, exprs_call_depth)
        {
          *reinterpret_cast<char**>(&e->expr_) += offset;
        }
        FOREACH_CNT(e, exprs)
        {
          for (int64_t i = 0; i < e->parent_cnt_; i++) {
            *reinterpret_cast<char**>(&e->parents_[i]) += offset;
          }
          for (int64_t i = 0; i < e->arg_cnt_; i++) {
            *reinterpret_cast<char**>(&e->args_[i]) += offset;
          }
        }
      }

      FOREACH_CNT_X(ecd, exprs_call_depth, OB_SUCC(ret))
      {
        if (ecd->need_stack_check_) {
          ObExpr* e = ecd->expr_;
          // stack overflow check expr can not added above T_OP_ROW
          if (T_OP_ROW == e->type_ && e->parent_cnt_ > 0) {
            e = e->parents_[0];
            if (T_OP_ROW == e->type_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("T_OP_ROW can not be nested", K(ret));
            }
          }
          if (OB_SUCC(ret) && e->parent_cnt_ > 0) {
            OZ(add_so_check_expr_above(exprs, e));
          }
        }
      }  // END FOREACH_CNT_X
    }
  }
  return ret;
}

int ObStaticEngineExprCG::add_so_check_expr_above(ObIArray<ObExpr>& exprs, ObExpr* e)
{
  int ret = OB_SUCCESS;
  ObExpr* base = exprs.get_data();
  CK(NULL != e);
  CK(e->parent_cnt_ > 0 && e->arg_cnt_ > 0);
  OZ(exprs.push_back(*e));
  CK(base == exprs.get_data());
  if (OB_SUCC(ret)) {
    // stack overflow check expr point to the same ObDatum of child.
    ObExpr* so = &exprs.at(exprs.count() - 1);
    so->type_ = T_OP_STACK_OVERFLOW_CHECK;
    so->extra_ = 0;
    so->inner_functions_ = 0;
    so->inner_func_cnt_ = 0;
    ObExpr** parents = static_cast<ObExpr**>(allocator_.alloc(sizeof(ObExpr*) * 2));
    if (NULL == parents) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      ObExpr** args = parents + 1;

      parents[0] = so;
      args[0] = e;

      e->parents_ = parents;
      e->parent_cnt_ = 1;
      so->args_ = args;
      so->arg_cnt_ = 1;

      for (int64_t i = 0; i < so->parent_cnt_; i++) {
        ObExpr* p = so->parents_[i];
        for (int64_t j = 0; j < p->arg_cnt_; j++) {
          if (p->args_[j] == e) {
            p->args_[j] = so;
          }
        }
      }

      so->eval_func_ = ObExprUtil::eval_stack_overflow_check;
    }
  }
  return ret;
}

int ObStaticEngineExprCG::replace_var_rt_expr(ObExpr* origin_expr,
    ObExpr* var_expr, ObExpr* parent_expr, int32_t var_idx)  // child pos of parent_expr
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(origin_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(origin_expr));
  } else if (T_EXEC_VAR == var_expr->type_) {
    if (OB_ISNULL(parent_expr) || OB_UNLIKELY(parent_expr->arg_cnt_ < var_idx + 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid param", KPC(parent_expr));
    } else {
      parent_expr->args_[var_idx] = origin_expr;
    }
  } else {
    parent_expr = var_expr;
    while (OB_SUCC(ret)) {
      // May two exprs above ObVarRawExpr:
      // 1. implicit cast add in type deduce
      // 2. ENUM_TO_STR/SET_TO_STR for enum/set
	    if (T_FUN_ENUM_TO_STR == parent_expr->type_
          || T_FUN_SET_TO_STR == parent_expr->type_
          || T_FUN_ENUM_TO_INNER_TYPE == parent_expr->type_
          || T_FUN_SET_TO_INNER_TYPE == parent_expr->type_) {
        var_idx = 1;
      } else if (T_FUN_SYS_CAST == parent_expr->type_) {
        var_idx = 0;
      } else {
        break;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(parent_expr->arg_cnt_ < var_idx + 1) ||
                 OB_ISNULL(var_expr = parent_expr->args_[var_idx])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret), KPC(parent_expr));
      } else if (T_EXEC_VAR == var_expr->type_) {
        parent_expr->args_[var_idx] = origin_expr;
        break;
      } else {
        parent_expr = var_expr;
      }
    }
  }
  if (OB_SUCC(ret) && T_EXEC_VAR == var_expr->type_) {
    if (OB_UNLIKELY(
            ObNullType != var_expr->datum_meta_.type_ && ObNullType != origin_expr->datum_meta_.type_ &&
            ob_obj_type_class(var_expr->datum_meta_.type_) != ob_obj_type_class(origin_expr->datum_meta_.type_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec var meta diff from origin expr meta", K(ret), KPC(origin_expr), KPC(var_expr));
    } else if (OB_UNLIKELY(ob_is_string_or_lob_type(var_expr->datum_meta_.type_) &&
                           ob_is_string_or_lob_type(origin_expr->datum_meta_.type_) &&
                           var_expr->datum_meta_.cs_type_ != origin_expr->datum_meta_.cs_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec var collation diff from origin expr collation", K(ret), KPC(origin_expr), KPC(var_expr));
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
