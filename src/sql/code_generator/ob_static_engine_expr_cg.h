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

#ifndef OCEANBASE_SQL_CODE_GENERATOR_OB_STATIC_ENGINE_EXPR_CG_
#define OCEANBASE_SQL_CODE_GENERATOR_OB_STATIC_ENGINE_EXPR_CG_

#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_frame_info.h"

namespace oceanbase {
namespace common {
class ObIAllocator;
}

namespace sql {

class ObPhysicalPlan;
class ObDMLStmt;
class ObRawExprUniqueSet;

class ObExprCGCtx {
public:
  ObExprCGCtx() : allocator_(NULL), exec_ctx_(NULL)
  {}
  ObExprCGCtx(common::ObIAllocator* allocator, ObExecContext* exec_ctx) : allocator_(allocator), exec_ctx_(exec_ctx)
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCGCtx);

public:
  common::ObIAllocator* allocator_;
  ObExecContext* exec_ctx_;
};
class ObRawExpr;
class ObHiddenColumnItem;
class ObStaticEngineExprCG {
public:
  static const int64_t STACK_OVERFLOW_CHECK_DEPTH = 16;
  static const int64_t DATUM_EVAL_INFO_SIZE = sizeof(ObDatum) + sizeof(ObEvalInfo);
  friend class ObRawExpr;
  ObStaticEngineExprCG(common::ObIAllocator& allocator, DatumParamStore* param_store)
      : allocator_(allocator), param_store_(param_store), op_cg_ctx_(), flying_param_cnt_(0)
  {}
  virtual ~ObStaticEngineExprCG()
  {}

  int generate(const ObRawExprUniqueSet& all_raw_exprs, ObExprFrameInfo& expr_info);

  int generate_calculable_exprs(
      const common::ObIArray<ObHiddenColumnItem>& calculable_exprs, ObPreCalcExprFrameInfo& pre_calc_frame);

  int generate_calculable_expr(ObRawExpr* raw_expr, ObPreCalcExprFrameInfo& pre_calc_frame);

  static int generate_rt_expr(const ObRawExpr& src, common::ObIArray<ObRawExpr*>& exprs, ObExpr*& dst);

  static int replace_var_rt_expr(ObExpr* origin_expr, ObExpr* var_expr, ObExpr* parent_expr, int32_t var_idx);

  // Attention : Please think over before you have to use this function.
  // This function is different from generate_rt_expr.
  // It won't put raw_expr into cur_op_exprs_ because it doesn't need to be calculated.
  static void* get_left_value_rt_expr(const ObRawExpr& raw_expr);

  void init_operator_cg_ctx(ObExecContext* exec_ctx)
  {
    op_cg_ctx_.exec_ctx_ = exec_ctx;
    op_cg_ctx_.allocator_ = &allocator_;
  }

  ObExprCGCtx& get_operator_cg_ctx()
  {
    return op_cg_ctx_;
  }

private:
  static ObExpr* get_rt_expr(const ObRawExpr& raw_expr);
  int construct_exprs(const common::ObIArray<ObRawExpr*>& raw_exprs, common::ObIArray<ObExpr>& rt_exprs);

  int cg_exprs(const common::ObIArray<ObRawExpr*>& raw_exprs, ObExprFrameInfo& expr_info);

  // init type_, datum_meta_, obj_meta_, obj_datum_map_, args_, arg_cnt_
  // row_dimension_, op_
  int cg_expr_basic(const common::ObIArray<ObRawExpr*>& raw_exprs);

  // init parent_cnt_, parents_
  int cg_expr_parents(const common::ObIArray<ObRawExpr*>& raw_exprs);

  // init eval_func_, inner_eval_func_, expr_ctx_id_, extra_
  int cg_expr_by_operator(const common::ObIArray<ObRawExpr*>& raw_exprs, int64_t& total_ctx_cnt);

  // init res_buf_len_, frame_idx_, datum_off_, res_buf_off_
  // @param [in/out] raw_exprs
  // @param [out] expr_info frame info
  int cg_all_frame_layout(const common::ObIArray<ObRawExpr*>& raw_exprs, ObExprFrameInfo& expr_info);

  int cg_expr_basic_funcs(const common::ObIArray<ObRawExpr*>& raw_exprs);

  // alloc stack overflow check exprs.
  int alloc_so_check_exprs(const common::ObIArray<ObRawExpr*>& raw_exprs, ObExprFrameInfo& expr_info);
  // add stack overflow check expr above %e
  int add_so_check_expr_above(common::ObIArray<ObExpr>& exprs, ObExpr* e);

  int classify_exprs(const common::ObIArray<ObRawExpr*>& raw_exprs, common::ObIArray<ObRawExpr*>& const_exprs,
      common::ObIArray<ObRawExpr*>& param_exprs, common::ObIArray<ObRawExpr*>& dynamic_param_exprs,
      common::ObIArray<ObRawExpr*>& no_const_param_exprs) const;

  int cg_no_reserved_buf_layout(const common::ObIArray<ObRawExpr*>& exprs, int64_t& frame_index_pos,
      common::ObIArray<ObFrameInfo>& frame_info_arr);

  int cg_const_frame_layout(const common::ObIArray<ObRawExpr*>& const_exprs, int64_t& frame_index_pos,
      common::ObIArray<ObFrameInfo>& frame_info_arr);

  int cg_param_frame_layout(const common::ObIArray<ObRawExpr*>& param_exprs, int64_t& frame_index_pos,
      common::ObIArray<ObFrameInfo>& frame_info_arr);

  int cg_dynamic_frame_layout(const common::ObIArray<ObRawExpr*>& exprs, int64_t& frame_index_pos,
      common::ObIArray<ObFrameInfo>& frame_info_arr);

  int cg_datum_frame_layout(const common::ObIArray<ObRawExpr*>& exprs, int64_t& frame_index_pos,
      common::ObIArray<ObFrameInfo>& frame_info_arr);

  int cg_frame_layout(const common::ObIArray<ObRawExpr*>& exprs, const bool reserve_empty_string,
      const bool continuous_datum, int64_t& frame_index_pos, common::ObIArray<ObFrameInfo>& frame_info_arr);

  int alloc_const_frame(const common::ObIArray<ObRawExpr*>& exprs, const common::ObIArray<ObFrameInfo>& const_frames,
      common::ObIArray<char*>& frame_ptrs);

  // called after res_buf_len_ assigned to get the buffer size with dynamic reserved buffer.
  int64_t reserve_data_consume(const ObExpr& expr)
  {
    const bool need_dyn_buf = ObDynReserveBuf::supported(expr.datum_meta_.type_);
    return expr.res_buf_len_ + (need_dyn_buf && expr.res_buf_len_ > 0 ? sizeof(ObDynReserveBuf) : 0);
  }

  int arrange_datum_data(common::ObIArray<ObRawExpr*>& exprs, const ObFrameInfo& frame, const bool continuous_datum);

  int inner_generate_calculable_exprs(
      const common::ObIArray<ObHiddenColumnItem>& calculable_exprs, ObPreCalcExprFrameInfo& expr_info);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObStaticEngineExprCG);

private:
  common::ObIAllocator& allocator_;
  DatumParamStore* param_store_;
  ObExprCGCtx op_cg_ctx_;
  // Count of param store in generating, for calculable expressions CG.
  int64_t flying_param_cnt_;
};

}  // end namespace sql
}  // end namespace oceanbase
#endif /*OCEANBASE_SQL_CODE_GENERATOR_OB_STATIC_ENGINE_EXPR_CG_*/
