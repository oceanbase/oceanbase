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

#ifndef __OB_SQL_NESTED_LOOP_JOIN_H__
#define __OB_SQL_NESTED_LOOP_JOIN_H__

#include "sql/engine/join/ob_basic_nested_loop_join.h"
#include "common/row/ob_row.h"
#include "common/row/ob_row_store.h"

namespace oceanbase {
namespace sql {
class ObExecContext;
class ObTaskInfo;
class ObNestedLoopJoin : public ObBasicNestedLoopJoin {
  OB_UNIS_VERSION_V(1);

private:
  enum ObJoinState { JS_JOIN_END = 0, JS_READ_LEFT, JS_READ_RIGHT, JS_STATE_COUNT };
  enum ObFuncType { FT_ITER_GOING = 0, FT_ITER_END, FT_TYPE_COUNT };
  struct ObBatchIndexJoinCtx {
    common::ObRowStore left_rows_;
    common::ObRowStore::Iterator left_rows_iter_;
    ObBatchIndexJoinCtx()
        : left_rows_(common::ObModIds::OB_SQL_NLJ_CACHE, common::OB_SERVER_TENANT_ID, false), left_rows_iter_()
    {}
  };
  class ObNestedLoopJoinCtx : public ObBasicNestedLoopJoinCtx {
    friend class ObNestedLoopJoin;

  public:
    ObNestedLoopJoinCtx(ObExecContext& ctx)
        : ObBasicNestedLoopJoinCtx(ctx), state_(JS_READ_LEFT), batch_join_ctx_(), is_left_end_(false)
    //  connect_by_pump_()
    {}
    virtual ~ObNestedLoopJoinCtx()
    {}
    void reset();
    virtual void destroy()
    {
      batch_join_ctx_.~ObBatchIndexJoinCtx();
      ObBasicNestedLoopJoinCtx::destroy();
    }

  private:
    ObJoinState state_;
    ObBatchIndexJoinCtx batch_join_ctx_;
    bool is_left_end_;
    //    ObConnectByPump connect_by_pump_;
  };

public:
  explicit ObNestedLoopJoin(common::ObIAllocator& alloc);
  virtual ~ObNestedLoopJoin();
  virtual void reset();
  virtual void reuse();
  virtual int rescan(ObExecContext& exec_ctx) const;
  virtual int switch_iterator(ObExecContext& ctx) const override;

  bool use_group() const
  {
    return use_group_;
  }
  void set_use_group(const bool use_group)
  {
    use_group_ = use_group;
  }
  int64_t get_mem_limit() const
  {
    return mem_limit_;
  }
  void set_mem_limit(const int64_t mem_limit)
  {
    mem_limit_ = mem_limit;
  }
  int64_t get_cache_limit() const
  {
    return cache_limit_;
  }
  void set_cache_limit(const int64_t cache_limit)
  {
    cache_limit_ = cache_limit;
  }

private:
  // state operation and transfer function type.
  typedef int (ObNestedLoopJoin::*state_operation_func_type)(ObNestedLoopJoinCtx& join_ctx) const;
  typedef int (ObNestedLoopJoin::*state_function_func_type)(
      ObNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  virtual int inner_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& exec_ctx) const;
  virtual int inner_create_operator_ctx(ObExecContext& exec_ctx, ObPhyOperatorCtx*& op_ctx) const;
  int prepare_rescan_params_for_group(ObNestedLoopJoinCtx& join_ctx) const;
  // JS_JOIN_END state operation and transfer functions.
  int join_end_operate(ObNestedLoopJoinCtx& join_ctx) const;
  int join_end_func_end(ObNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  // JS_READ_LEFT state operation and transfer functions.
  int read_left_operate(ObNestedLoopJoinCtx& join_ctx) const;
  int group_read_left_operate(ObNestedLoopJoinCtx& join_ctx) const;
  int group_rescan_right(ObNestedLoopJoinCtx& join_ctx) const;
  int read_left_func_going(ObNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int read_left_func_end(ObNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  // JS_READ_RIGHT state operation and transfer functions.
  int read_right_operate(ObNestedLoopJoinCtx& join_ctx) const;
  int read_right_func_going(ObNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int read_right_func_end(ObNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int join_row_with_semi_join(ObExecContext& ctx, const common::ObNewRow*& row) const;
  //  int join_row_with_anti_semi_join(ObExecContext &ctx, const common::ObNewRow *&row) const;
  // state operations and transfer functions array.
  state_operation_func_type state_operation_func_[JS_STATE_COUNT];
  state_function_func_type state_function_func_[JS_STATE_COUNT][FT_TYPE_COUNT];
  // nested loop join with index seek, batch
  int batch_index_join_get_next(ObExecContext& exec_ctx, const common::ObNewRow*& row) const;

private:
  int bij_fill_left_rows(ObExecContext& exec_ctx) const;
  int bij_join_rows(ObExecContext& exec_ctx, const common::ObNewRow*& row) const;

  bool is_full(common::ObRowStore& rs) const;
  // for group nested loop join.
  bool use_group_;
  int64_t mem_limit_;
  int64_t cache_limit_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObNestedLoopJoin);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_NESTED_LOOP_JOIN_H__ */
//// end of header file
