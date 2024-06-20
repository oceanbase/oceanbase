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

#ifndef OCEANBASE_SQL_ENGINE_OB_FRAME_INFO_
#define OCEANBASE_SQL_ENGINE_OB_FRAME_INFO_
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/engine/expr/ob_expr.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
namespace sql
{
class ObPhysicalPlanCtx;
class ObDynExtReuseAlloc;
class ObExprOperatorCtx;

struct ObFrameInfo
{
  OB_UNIS_VERSION_V(1);
public:
  ObFrameInfo() : expr_cnt_(0),
                  frame_idx_(0),
                  frame_size_(0),
                  zero_init_pos_(0),
                  zero_init_size_(0),
                  use_rich_format_(false)
  {}
  virtual ~ObFrameInfo() = default;

  ObFrameInfo(uint64_t expr_cnt, uint32_t frame_idx, uint64_t frame_size,
              uint32_t zero_init_pos, uint32_t zero_init_size, bool use_rich_format)
    : expr_cnt_(expr_cnt),
      frame_idx_(frame_idx),
      frame_size_(frame_size),
      zero_init_pos_(zero_init_pos),
      zero_init_size_(zero_init_size),
      use_rich_format_(use_rich_format)
  {}

  TO_STRING_KV(K_(expr_cnt), K_(frame_idx), K_(frame_size), K_(zero_init_pos),
               K_(zero_init_size), K_(use_rich_format));

public:
  uint64_t expr_cnt_;       // 当前frame中expr个数
  uint32_t frame_idx_;      // 当前frame在所有frame中下表
  uint64_t frame_size_;     // 当前frame大小
  uint32_t zero_init_pos_;
  uint32_t zero_init_size_;
  bool use_rich_format_;
};

struct ObExprFrameInfo
{
  OB_UNIS_VERSION_V(1);
public:
  static const int64_t EXPR_CNT_PER_FRAME =
      common::MAX_FRAME_SIZE / (sizeof(ObDatum) + sizeof(ObEvalInfo));

  ObExprFrameInfo(common::ObIAllocator &allocator) :
      _rt_exprs_(0, common::ModulePageAllocator(allocator)),
      need_ctx_cnt_(0),
      rt_exprs_(_rt_exprs_),
      const_frame_ptrs_(allocator),
      const_frame_(allocator),
      param_frame_(allocator),
      dynamic_frame_(allocator),
      datum_frame_(allocator),
      ser_expr_marks_(allocator),
      allocator_(allocator)
  {
  }

  ObExprFrameInfo(common::ObIAllocator &allocator, common::ObArray<ObExpr> &rt_exprs) :
      _rt_exprs_(0, common::ModulePageAllocator(allocator)),
      need_ctx_cnt_(0),
      rt_exprs_(rt_exprs),
      const_frame_ptrs_(allocator),
      const_frame_(allocator),
      param_frame_(allocator),
      dynamic_frame_(allocator),
      datum_frame_(allocator),
      ser_expr_marks_(allocator),
      allocator_(allocator)
  {
  }

  virtual int assign(const ObExprFrameInfo &other, common::ObIAllocator &allocator);

  // 预分配执行过程中需要的内存, 包括frame memory和expr_ctx memory
  // @param exec_ctx 执行期context，初始化其成员：frames_, frame_cnt_, expr_ctx_arr_
  int pre_alloc_exec_memory(ObExecContext &exec_ctx, ObIAllocator *allocator = NULL) const;

  // 分配frame内存, 并将所有frame指针按每个frame idx的序存放到frames数组中
  // @param [in] exec_allocator 执行期分配期
  // @param [out] frame_cnt 所有frame的个数
  // @param [out] frames 所有frame指针list
  int alloc_frame(common::ObIAllocator &exec_allocator,
                  const ObIArray<char *> &param_frame_ptrs,
                  uint64_t &frame_cnt,
                  char **&frames) const;

  int get_expr_idx_in_frame(ObExpr *expr, int64_t &expr_idx) const;

  bool is_mark_serialize() const { return &_rt_exprs_ != &rt_exprs_; }

  TO_STRING_KV(K_(need_ctx_cnt),
               "rt_expr_cnt", rt_exprs_.count(),
               K_(const_frame_ptrs),
               K_(const_frame),
               K_(param_frame),
               K_(dynamic_frame),
               K_(datum_frame),
               K_(ser_expr_marks));


private:
  common::ObArray<ObExpr> _rt_exprs_;

public:
  // 所有物理表达式中, 需要表达式级context的个数, exec_ctx分配具体的内存大小使用
  int64_t need_ctx_cnt_;
  // 表达式cg生成的所有物理表达式
  common::ObArray<ObExpr> &rt_exprs_;
  // 所有const frame的指针
  common::ObFixedArray<char *, common::ObIAllocator> const_frame_ptrs_;
  // 所有const frame的信息
  common::ObFixedArray<ObFrameInfo, common::ObIAllocator> const_frame_;
  // 所有param frame的信息
  common::ObFixedArray<ObFrameInfo, common::ObIAllocator> param_frame_;
  // 所有dynamic frame的信息
  common::ObFixedArray<ObFrameInfo, common::ObIAllocator> dynamic_frame_;
  // 所有datum frame的信息
  common::ObFixedArray<ObFrameInfo, common::ObIAllocator> datum_frame_;

  // mark need serialize exrps in mark serialization
  common::ObFixedArray<bool, common::ObIAllocator> ser_expr_marks_;
  common::ObIAllocator &allocator_;
};

// Empty struct, for unused expr serialization.
struct ObEmptyExpr
{
  OB_UNIS_VERSION(1);
public:
  static const ObEmptyExpr &instance()
  {
    static const ObEmptyExpr expr_;
    return expr_;
  }
};

struct ObPreCalcExprFrameInfo
  : public ObExprFrameInfo, public common::ObDLinkBase<ObPreCalcExprFrameInfo>
{
  ObPreCalcExprFrameInfo(common::ObIAllocator &allocator):
    ObExprFrameInfo(allocator),
    pre_calc_rt_exprs_(allocator)
  {
  }

  int assign(const ObPreCalcExprFrameInfo &other, common::ObIAllocator &allocator);
  int eval(ObExecContext &exec_ctx, common::ObIArray<ObDatumObjParam> &res_datum_params);
  int eval_expect_err(ObExecContext &exec_ctx, bool &all_eval_err);
  INHERIT_TO_STRING_KV("frame_info", ObExprFrameInfo, K_(pre_calc_rt_exprs));
private:
  int do_normal_eval(ObExecContext &exec_ctx, common::ObIArray<ObDatumObjParam> &res_datum_params);
  int do_batch_stmt_eval(ObExecContext &exec_ctx, common::ObIArray<ObDatumObjParam> &res_datum_params);
  void clear_datum_evaluted_flags(char **frames);
public:
  common::ObFixedArray<ObExpr *, common::ObIAllocator> pre_calc_rt_exprs_;
};

struct ObTempExprCtx : public ObEvalCtx
{
  ObTempExprCtx(ObExecContext &exec_ctx)
    : ObEvalCtx(exec_ctx), frame_cnt_(0), expr_op_ctx_store_(NULL), expr_op_size_(0)
  {}
  ~ObTempExprCtx();
public:
  uint64_t frame_cnt_;
  ObExprOperatorCtx **expr_op_ctx_store_;
  uint64_t expr_op_size_;
};

struct RowIdxColumnPair
{
  OB_UNIS_VERSION_V(1);
public:
  RowIdxColumnPair()
    : idx_(OB_INVALID_INDEX), expr_pos_(OB_INVALID_INDEX)
  {}
  RowIdxColumnPair(int64_t idx, int64_t expr_pos)
    : idx_(idx), expr_pos_(expr_pos)
  {}
  virtual ~RowIdxColumnPair() {}
  TO_STRING_KV(K(idx_), KP(expr_pos_));

  int64_t idx_;  // obj idx in row
  int64_t expr_pos_; // column expr pos in rt_exprs_
};

struct ObTempExpr: public ObExprFrameInfo
{
  OB_UNIS_VERSION_V(1);
public:
  ObTempExpr(ObIAllocator &allocator)
    : ObExprFrameInfo(allocator),
      expr_idx_(OB_INVALID_INDEX),
      idx_col_arr_(allocator)
  {
  }
  int eval(ObExecContext &exec_ctx, const common::ObNewRow &row, ObObj &result) const;
  int deep_copy(ObIAllocator &allocator, ObTempExpr *&dst) const;
  INHERIT_TO_STRING_KV("temp expr", ObExprFrameInfo, K_(expr_idx), K(idx_col_arr_));

private:
  int row_to_frame(const ObNewRow &row, ObTempExprCtx &temp_expr_ctx) const;

public:
  int64_t expr_idx_; // temp_expr idx in rt_exprs_
  ObFixedArray<RowIdxColumnPair, common::ObIAllocator> idx_col_arr_;
};

} // end namespace sql
} // end namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_OB_FRAME_INFO_*/
