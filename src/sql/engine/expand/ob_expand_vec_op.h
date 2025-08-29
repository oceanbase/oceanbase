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

#ifndef _OCEANBASE_SQL_ENGINE_EXPAND_H_
#define _OCEANBASE_SQL_ENGINE_EXPAND_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{

class ObVectorsResultHolder;
class ObBatchResultHolder;

class ObExpandVecSpec: public ObOpSpec
{
public:
  struct DupExprPair
  {
    OB_UNIS_VERSION_V(1);
  public:
    DupExprPair(): org_expr_(nullptr), dup_expr_(nullptr) {}
    DupExprPair(ObExpr *org_expr, ObExpr *dup_expr) : org_expr_(org_expr), dup_expr_(dup_expr)
    {}

    int assign(const DupExprPair &other)
    {
      org_expr_ = other.org_expr_;
      dup_expr_ = other.dup_expr_;
      return OB_SUCCESS;
    }
    virtual ~DupExprPair() {}
    ObExpr *org_expr_;
    ObExpr *dup_expr_;
    TO_STRING_KV(KP_(org_expr), KP_(dup_expr));
  };

public:
  OB_UNIS_VERSION_V(1);
public:
  ObExpandVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type) :
    ObOpSpec(alloc, type), expand_exprs_(alloc), gby_exprs_(alloc),
    grouping_id_expr_(nullptr), dup_expr_pairs_(alloc)
  {}
  virtual ~ObExpandVecSpec() {}
public:
  TO_STRING_KV(KP_(grouping_id_expr), K_(expand_exprs));
  // select sum(c1), count(c2) from t group by c3, c4, rollup(c1, c3, c5)
  // expand_exprs = [c1, c3, c5]
  // gby_exprs = [c3, c4]
  // dup_expr_pairs = [(c1, dup(c1))]
  ExprFixedArray expand_exprs_;
  ExprFixedArray gby_exprs_;
  ObExpr *grouping_id_expr_;
  ObFixedArray<DupExprPair, ObIAllocator> dup_expr_pairs_;
  ObFixedArray<ExprFixedArray, ObIAllocator> group_set_exprs_;
  ExprFixedArray pruned_groupby_exprs_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExpandVecSpec);
};

class ObExpandVecOp: public ObOperator
{
public:
  ObExpandVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input) :
    ObOperator(exec_ctx, spec, input), dup_status_(DupStatus::Init), expr_iter_idx_(-1),
    child_input_size_(0), child_input_skip_(nullptr),
    child_all_rows_active_(false), vec_holder_(nullptr),
    allocator_("ValueExpansion", OB_MALLOC_NORMAL_BLOCK_SIZE,
               exec_ctx.get_my_session()->get_effective_tenant_id(), ObCtxIds::WORK_AREA)
  {}
  virtual ~ObExpandVecOp() {}
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int inner_get_next_row() override
  {
    return OB_NOT_IMPLEMENT;
  }
  // virtual int inner_switch_iterator() override;
  virtual void destroy() override;
  TO_STRING_KV(K_(expr_iter_idx));
private:
  enum class DupStatus
  {
    Init,
    ORIG_ALL,
    DUP_PARTIAL,
    END,
  };
  int init();
  void reset_status()
  {
    dup_status_ = DupStatus::Init;
    expr_iter_idx_ = -1;
    child_input_size_ = 0;
    child_all_rows_active_ = false;
    child_input_skip_ = nullptr;
  }
  int get_next_batch_from_child(int64_t batch_size, const ObBatchRows *&child_brs);
  int backup_child_input(const ObBatchRows *child_brs);
  int restore_child_input();
  int setup_grouping_id();
  inline void copy_child_brs()
  {
    if (child_input_skip_ != nullptr) {
      brs_.skip_->deep_copy(*child_input_skip_, child_input_size_);
    }
    brs_.size_ = child_input_size_;
    brs_.end_ = false;
    brs_.all_rows_active_ = child_all_rows_active_;
  }

  int duplicate_rollup_exprs();

  template<VectorFormat vec>
  int duplicate_expr(ObExpr *from, ObExpr *to);

  template<typename uni_vector>
  int duplicate_expr_from_uniform(uni_vector *from, ObExpr *to);

  void copy_bitmap_based_nulls(ObIVector *from, ObIVector *to)
  {
    ObBitmapNullVectorBase *from_nulls = static_cast<ObBitmapNullVectorBase *>(from);
    ObBitmapNullVectorBase *to_nulls = static_cast<ObBitmapNullVectorBase *>(to);
    to_nulls->get_nulls()->deep_copy(*from_nulls->get_nulls(), brs_.size_);
    to_nulls->set_has_null(from_nulls->has_null());
    if (from_nulls->is_batch_ascii()) {
      to_nulls->set_is_batch_ascii();
    } else {
      to_nulls->reset_is_batch_ascii();
    }
  }

  void next_status();

  int do_dup_partial();

  bool exists_dup_expr(int cur_expr_idx)
  {
    bool ret = false;
    for (int i = 0; !ret && i < cur_expr_idx; i++) {
      ret = MY_SPEC.expand_exprs_.at(i) == MY_SPEC.expand_exprs_.at(cur_expr_idx);
    }
    return ret;
  }

  void clear_evaluated_flags();
private:
  DupStatus dup_status_;

  int64_t expr_iter_idx_;
  int64_t child_input_size_;
  ObBitVector *child_input_skip_;
  bool child_all_rows_active_;
  union
  {
    ObVectorsResultHolder *vec_holder_;
    ObBatchResultHolder *datum_holder_;
  };
  common::ObArenaAllocator allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExpandVecOp);
};
} // end sql
} // end oceanbase
#endif