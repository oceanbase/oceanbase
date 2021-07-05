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

#ifndef __OB_SQL_NESTED_LOOP_CONNECT_BY_H__
#define __OB_SQL_NESTED_LOOP_CONNECT_BY_H__

#include "sql/engine/join/ob_nested_loop_join.h"
#include "common/row/ob_row.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"

namespace oceanbase {
namespace sql {
class ObExecContext;
class ObExprSysConnectByPath;
class ObExprPrior;
class ObConnectByBase : public ObBasicNestedLoopJoin {
  friend ObConnectByPumpBase;
  OB_UNIS_VERSION_V(1);

protected:
  enum ObJoinState { JS_JOIN_END = 0, JS_READ_LEFT, JS_READ_RIGHT, JS_READ_OUTPUT, JS_READ_PUMP, JS_STATE_COUNT };
  enum ObFuncType { FT_ITER_GOING = 0, FT_ITER_END, FT_TYPE_COUNT };

public:
  class ObConnectByBaseCtx : public ObBasicNestedLoopJoinCtx {
    friend ObConnectByBase;
    // friend ObConnectByPumpBase;
  public:
    ObConnectByBaseCtx(ObExecContext& ctx)
        : ObBasicNestedLoopJoinCtx(ctx),
          state_(JS_READ_LEFT),
          root_row_(NULL),
          null_cell_row_(),
          mock_right_row_(),
          is_inited_(false)
    {}
    virtual ~ObConnectByBaseCtx()
    {}
    void reset();
    virtual void destroy()
    {
      ObBasicNestedLoopJoinCtx::destroy();
    }
    virtual int64_t get_current_level() const = 0;
    virtual int get_sys_parent_path(int64_t sys_connect_by_path_id, ObString& parent_path) = 0;
    virtual int set_sys_current_path(
        int64_t sys_connect_by_path_id, const ObString& cur_str, const ObString& res_path) = 0;

  protected:
    int create_null_cell_row(const ObConnectByBase& connect_by);
    int create_mock_right_row(const ObConnectByBase& connect_by);
    int init(const ObConnectByBase& connect_by, common::ObExprCtx* join_ctx);

  protected:
    ObJoinState state_;
    const common::ObNewRow* root_row_;
    common::ObNewRow null_cell_row_;   // used for root row output
    common::ObNewRow mock_right_row_;  // used for root row output
    bool is_inited_;
  };

public:
  explicit ObConnectByBase(common::ObIAllocator& alloc);
  virtual ~ObConnectByBase();
  virtual void reset();
  virtual void reuse();
  int init_exec_param_count(int64_t count)
  {
    return init_array_size<>(level_params_, count);
  }
  int add_exec_param(int64_t i)
  {
    return level_params_.push_back(i);
  };
  int add_sys_connect_by_path(ObColumnExpression* expr);
  int32_t get_sys_connect_by_path_expression_count() const
  {
    return sys_connect_exprs_.get_size();
  }

  // set level pseudo as a param store.
  int set_level_as_param(ObConnectByBaseCtx& join_ctx, int64_t level) const;
  TO_STRING_KV(N_ID, id_, N_COLUMN_COUNT, column_count_, N_PROJECTOR,
      common::ObArrayWrap<int32_t>(projector_, projector_size_), N_FILTER_EXPRS, filter_exprs_, N_CALC_EXPRS,
      calc_exprs_, N_JOIN_TYPE, ob_join_type_str(join_type_), N_JOIN_EQ_COND, rescan_params_, N_JOIN_OTHER_COND,
      other_join_conds_, N_INNER_GET, is_inner_get_, N_SELF_JOIN, is_self_join_, N_PUMP_ROW_DESC, pump_row_desc_,
      N_ROOT_ROW_DESC, root_row_desc_, N_PSEUDO_COLUMN_ROW_DESC, pseudo_column_row_desc_, N_CONNECT_BY_PRIOR_EXPRS,
      connect_by_prior_exprs_);

protected:
  int construct_mock_right_row(const ObNewRow& root_row, ObNewRow& mock_right_row) const;
  int construct_root_output_row(ObConnectByBaseCtx& join_ctx, const ObNewRow*& output_row) const;

  int calc_connect_by_root_exprs(ObConnectByBaseCtx& join_ctx, ObNewRow* root_row, const ObNewRow* output_row) const;
  int calc_sys_connect_by_path(ObConnectByBaseCtx& join_ctx, ObNewRow* output_row) const;

protected:
  // idx, used  to get param from phy plan.
  common::ObFixedArray<int64_t, common::ObIAllocator> level_params_;
  // Use to store sys connect by path sql expression.
  common::ObDList<ObSqlExpression> sys_connect_exprs_;
  DISALLOW_COPY_AND_ASSIGN(ObConnectByBase);
};

class ObConnectBy : public ObConnectByBase {
  friend ObConnectByPump;
  OB_UNIS_VERSION_V(1);

public:
  class ObConnectByCtx : public ObConnectByBaseCtx {
    friend ObConnectBy;
    friend ObExprSysConnectByPath;
    friend ObExprPrior;
    friend ObConnectByPump;

  public:
    ObConnectByCtx(ObExecContext& ctx)
        : ObConnectByBaseCtx(ctx),
          connect_by_pump_(),
          mem_context_(NULL),
          profile_(ObSqlWorkAreaType::SORT_WORK_AREA),
          sql_mem_processor_(profile_)

    {}
    virtual ~ObConnectByCtx()
    {}
    void reset();
    virtual void destroy()
    {
      connect_by_pump_.~ObConnectByPump();  // must be call
      ObConnectByBaseCtx::destroy();
    }
    int64_t get_current_level() const override
    {
      return connect_by_pump_.get_current_level();
    }
    virtual int get_sys_parent_path(int64_t sys_connect_by_path_id, ObString& parent_path) override
    {
      return connect_by_pump_.get_sys_path(sys_connect_by_path_id, parent_path);
    }
    virtual int set_sys_current_path(int64_t sys_connect_by_path_id, const ObString& cur_str, const ObString&) override
    {
      return connect_by_pump_.concat_sys_path(sys_connect_by_path_id, cur_str);
    }
    bool need_dump()
    {
      return sql_mem_processor_.get_data_size() > sql_mem_processor_.get_mem_bound();
    }

  private:
    int init(const ObConnectBy& connect_by, common::ObExprCtx* join_ctx);

  private:
    ObConnectByPump connect_by_pump_;
    lib::MemoryContext* mem_context_;
    ObSqlWorkAreaProfile profile_;
    ObSqlMemMgrProcessor sql_mem_processor_;
  };
  struct EqualConditionInfo {
    EqualConditionInfo() : cmp_type_(common::ObNullType), ctype_(common::CS_TYPE_INVALID)
    {}
    EqualConditionInfo(common::ObObjType cmp_type, common::ObCollationType ctype) : cmp_type_(cmp_type), ctype_(ctype)
    {}
    ~EqualConditionInfo()
    {
      reset();
    }

    void reset()
    {
      cmp_type_ = common::ObNullType;
      ctype_ = common::CS_TYPE_INVALID;
    }
    TO_STRING_KV(K_(cmp_type), K_(ctype));
    common::ObObjType cmp_type_;
    common::ObCollationType ctype_;  // collation type
  };

public:
  explicit ObConnectBy(common::ObIAllocator& alloc);
  virtual ~ObConnectBy();
  virtual int rescan(ObExecContext& exec_ctx) const override;
  virtual OperatorOpenOrder get_operator_open_order(ObExecContext& ctx) const override
  {
    UNUSED(ctx);
    return OPEN_CHILDREN_FIRST;
  }
  int add_hash_key_expr(ObSqlExpression* hash_key_expr)
  {
    return hash_key_exprs_.push_back(hash_key_expr);
  }
  int add_hash_probe_expr(ObSqlExpression* hash_probe_expr)
  {
    return hash_probe_exprs_.push_back(hash_probe_expr);
  }
  int add_equal_condition_info(common::ObObjType cmp_type, common::ObCollationType ctype)
  {
    return equal_cond_infos_.push_back(EqualConditionInfo(cmp_type, ctype));
  }

private:
  int inner_open(ObExecContext& exec_ctx) const override;
  int inner_get_next_row(ObExecContext& exec_ctx, const ObNewRow*& row) const override;
  int inner_create_operator_ctx(ObExecContext& exec_ctx, ObPhyOperatorCtx*& op_ctx) const override;
  int inner_close(ObExecContext& exec_ctx) const override;

  typedef int (ObConnectBy::*state_operation_func_type)(ObConnectByCtx& join_ctx) const;
  typedef int (ObConnectBy::*state_function_func_type)(ObConnectByCtx& join_ctx, const common::ObNewRow*& row) const;

  // JS_READ_LEFT state operation and transfer functions.
  int read_left_operate(ObConnectByCtx& join_ctx) const;
  int read_left_func_going(ObConnectByCtx& join_ctx, const common::ObNewRow*& row) const;
  int read_left_func_end(ObConnectByCtx& join_ctx, const common::ObNewRow*& row) const;

  // JS_READ_RIGHT state operation and transfer functions.
  int process_dump(ObConnectByCtx& join_ctx) const;
  int read_right_operate(ObConnectByCtx& join_ctx) const;
  int read_right_func_going(ObConnectByCtx& join_ctx, const common::ObNewRow*& row) const;
  int read_right_func_end(ObConnectByCtx& join_ctx, const common::ObNewRow*& row) const;

  // JS_READ_OUTPUT state operation and transfer functions.
  int read_output_operate(ObConnectByCtx& join_ctx) const;
  int calc_pseudo_flags(ObConnectByCtx& join_ctx, ObConnectByPump::PumpNode& node) const;
  int read_output_func_going(ObConnectByCtx& join_ctx, const common::ObNewRow*& row) const;
  int read_output_func_end(ObConnectByCtx& join_ctx, const common::ObNewRow*& row) const;

  // JS_READ_PUMP state operation and transfer functions.
  int read_pump_operate(ObConnectByCtx& join_ctx) const;
  int read_pump_func_going(ObConnectByCtx& join_ctx, const common::ObNewRow*& row) const;
  int read_pump_func_end(ObConnectByCtx& join_ctx, const common::ObNewRow*& row) const;

  // JS_JOIN_END state operation and transfer functions.
  int join_end_operate(ObConnectByCtx& join_ctx) const;
  int join_end_func_end(ObConnectByCtx& join_ctx, const common::ObNewRow*& row) const;

  int add_pseudo_column(ObConnectByCtx& join_ctx, ObConnectByPump::PumpNode& node) const;

private:
  // state operations and transfer functions array.
  state_operation_func_type state_operation_func_[JS_STATE_COUNT];
  state_function_func_type state_function_func_[JS_STATE_COUNT][FT_TYPE_COUNT];
  common::ObArray<ObSqlExpression*> hash_key_exprs_;
  common::ObArray<ObSqlExpression*> hash_probe_exprs_;
  common::ObArray<EqualConditionInfo> equal_cond_infos_;
  DISALLOW_COPY_AND_ASSIGN(ObConnectBy);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_NESTED_LOOP_CONNECT_BY_H__ */
