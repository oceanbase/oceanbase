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

#ifndef __OB_SQL_CONNECT_BY_WITH_PATH_H__
#define __OB_SQL_CONNECT_BY_WITH_PATH_H__

#include "sql/engine/connect_by/ob_nested_loop_connect_by.h"
#include "sql/engine/connect_by/ob_connect_by_utility_bfs.h"

namespace oceanbase {
namespace sql {
class ObExecContext;
class ObExprSysConnectByPath;
class ObExprPrior;
class ObConnectByWithIndex : public ObConnectByBase {
  OB_UNIS_VERSION_V(1);

public:
  class ObConnectByWithIndexCtx : public ObConnectByBaseCtx {
    friend ObConnectByWithIndex;

  public:
    ObConnectByWithIndexCtx(ObExecContext& ctx)
        : ObConnectByBaseCtx(ctx), output_row_(NULL), connect_by_pump_(), is_match_(false), is_cycle_(false)
    {}
    virtual ~ObConnectByWithIndexCtx()
    {}
    void reset();
    virtual void destroy()
    {
      connect_by_pump_.~ObConnectByPumpBFS();  // must be call
      ObConnectByBaseCtx::destroy();
    }
    int64_t get_current_level() const override
    {
      return connect_by_pump_.get_current_level();
    }
    virtual int get_sys_parent_path(int64_t sys_connect_by_path_id, ObString& parent_path) override
    {
      return connect_by_pump_.get_parent_path(sys_connect_by_path_id, parent_path);
    }
    virtual int set_sys_current_path(int64_t sys_connect_by_path_id, const ObString&, const ObString& res_path) override
    {
      return connect_by_pump_.set_cur_node_path(sys_connect_by_path_id, res_path);
    }

  private:
    int init(const ObConnectByWithIndex& connect_by, common::ObExprCtx* join_ctx);

  private:
    const ObNewRow* output_row_;
    ObConnectByPumpBFS connect_by_pump_;
    bool is_match_;  // whether there is a child, for calc connect_by_isleaf
    bool is_cycle_;  // whether part of a cycle, for calc connect_by_iscycle
  };

public:
  explicit ObConnectByWithIndex(common::ObIAllocator& alloc);
  virtual ~ObConnectByWithIndex();
  int rescan(ObExecContext& exec_ctx) const override;
  bool get_need_sort_siblings() const
  {
    return need_sort_siblings_;
  }

private:
  int inner_open(ObExecContext& exec_ctx) const override;
  int inner_get_next_row(ObExecContext& exec_ctx, const ObNewRow*& row) const override;
  int inner_create_operator_ctx(ObExecContext& exec_ctx, ObPhyOperatorCtx*& op_ctx) const override;

  typedef int (ObConnectByWithIndex::*state_operation_func_type)(ObConnectByWithIndexCtx& join_ctx) const;
  typedef int (ObConnectByWithIndex::*state_function_func_type)(
      ObConnectByWithIndexCtx& join_ctx, const common::ObNewRow*& row) const;

  // JS_JOIN_END state operation and transfer functions.
  int join_end_operate(ObConnectByWithIndexCtx& join_ctx) const;
  int join_end_func_end(ObConnectByWithIndexCtx& join_ctx, const common::ObNewRow*& row) const;

  // JS_READ_OUTPUT state operation and transfer functions.
  int read_output_operate(ObConnectByWithIndexCtx& join_ctx) const;
  int read_output_func_going(ObConnectByWithIndexCtx& join_ctx, const common::ObNewRow*& row) const;
  int read_output_func_end(ObConnectByWithIndexCtx& join_ctx, const common::ObNewRow*& row) const;

  // JS_READ_PUMP state operation and transfer functions.
  int read_pump_operate(ObConnectByWithIndexCtx& join_ctx) const;
  int read_pump_func_going(ObConnectByWithIndexCtx& join_ctx, const common::ObNewRow*& row) const;
  int read_pump_func_end(ObConnectByWithIndexCtx& join_ctx, const common::ObNewRow*& row) const;

  // JS_READ_LEFT state operation and transfer functions.
  int read_left_operate(ObConnectByWithIndexCtx& join_ctx) const;
  int read_left_func_going(ObConnectByWithIndexCtx& join_ctx, const common::ObNewRow*& row) const;
  int read_left_func_end(ObConnectByWithIndexCtx& join_ctx, const common::ObNewRow*& row) const;

  // JS_READ_RIGHT state operation and transfer functions.
  int read_right_operate(ObConnectByWithIndexCtx& join_ctx) const;
  int read_right_func_going(ObConnectByWithIndexCtx& join_ctx, const common::ObNewRow*& row) const;
  int read_right_func_end(ObConnectByWithIndexCtx& join_ctx, const common::ObNewRow*& row) const;

  int add_pseudo_column(
      ObConnectByWithIndexCtx& join_ctx, ObNewRow* output_row, ObConnectByPseudoColumn column_type) const;
  int calc_sort_siblings_expr(ObConnectByWithIndexCtx& join_ctx, ObNewRow* output_row) const;

private:
  // state operations and transfer functions array.
  state_operation_func_type state_operation_func_[JS_STATE_COUNT];
  state_function_func_type state_function_func_[JS_STATE_COUNT][FT_TYPE_COUNT];
  bool need_sort_siblings_;

  DISALLOW_COPY_AND_ASSIGN(ObConnectByWithIndex);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_CONNECT_BY_WITH_INDEX_H__ */
