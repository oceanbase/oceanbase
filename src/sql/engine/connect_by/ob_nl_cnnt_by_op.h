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

#ifndef SRC_SQL_ENGINE_OB_NESTED_LOOP_CONNECT_BY_OP_H_
#define SRC_SQL_ENGINE_OB_NESTED_LOOP_CONNECT_BY_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/sort/ob_sort_basic_info.h"
#include "ob_cnnt_by_pump_bfs.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"

namespace oceanbase
{
namespace sql
{

class ObNLConnectBySpecBase : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObNLConnectBySpecBase(common::ObIAllocator &alloc, const ObPhyOperatorType type);
  virtual ~ObNLConnectBySpecBase() { }
  int32_t get_sys_connect_by_path_expression_count() const { return sys_connect_exprs_.count(); }

protected:
  enum ObCnntByOpState {
    CNTB_STATE_JOIN_END = 0,
    CNTB_STATE_READ_OUTPUT,
    CNTB_STATE_READ_PUMP,
    CNTB_STATE_READ_LEFT,
    CNTB_STATE_READ_RIGHT,
    CNTB_STATE_STATE_COUNT
  };
  enum ObFuncType {
    FT_ITER_GOING = 0,
    FT_ITER_END,
    FT_TYPE_COUNT
  };

public:

  common::ObCmpFuncs cmp_funcs_;
  ExprFixedArray sort_siblings_exprs_;      //用于order siblings实现
  ExprFixedArray connect_by_root_exprs_;    //用于计算connect_by_root expr
  // Use to store sys connect by path exprs.
  ExprFixedArray sys_connect_exprs_;        // connect by path
  ExprFixedArray cond_exprs_;               // 计算connect by 表达式，即 connect by prior c1 = c2
  ExprFixedArray connect_by_prior_exprs_;   //用于cycle判断
  ObExpr *level_expr_;
  ObExpr *is_leaf_expr_;
  ObExpr *is_cycle_expr_;

  ExprFixedArray cur_row_exprs_;
  // left_prior_exprs_和right_prior_exprs_中记录了左右两支的output中有对应关系的表达式，在进行左右行转换时，
  // 由一组表达式的datum组成一行，并放入另一组表达式的datum中。
  ExprFixedArray left_prior_exprs_;         // left row哪些exprs需要放入prior row
  ExprFixedArray right_prior_exprs_;        // right row哪些exprs需要放入prior row
                                            // 这里两者需要相等，同时作为cur_row_exprs也是相同的
  ObSortCollations sort_collations_;        // 排序信息
  ObSortFuncs sort_cmp_funs_;               // 排序函数
  // idx, used  to get param from phy plan.
  common::ObFixedArray<ObDynamicParamSetter, common::ObIAllocator> rescan_params_;
  bool is_nocycle_;//用于层次查询处理connect loop的问题
  bool has_prior_;
};

class ObNLConnectByOpBase : public ObOperator
{
protected:
  enum ObConnectByPseudoColumn
  {
    LEVEL = 0,
    CONNECT_BY_ISCYCLE  = 1,
    CONNECT_BY_ISLEAF = 2,
    CONNECT_BY_PSEUDO_COLUMN_CNT = 3
  };

  enum ObCnntByOpState {
    CNTB_STATE_JOIN_END = 0,
    CNTB_STATE_READ_OUTPUT,
    CNTB_STATE_READ_PUMP,
    CNTB_STATE_READ_LEFT,
    CNTB_STATE_READ_RIGHT,
    CNTB_STATE_STATE_COUNT
  };
  enum ObFuncType {
    FT_ITER_GOING = 0,
    FT_ITER_END,
    FT_TYPE_COUNT
  };
public:
  ObNLConnectByOpBase(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input),
    sys_connect_by_path_id_(INT64_MAX)
  { }
  virtual ~ObNLConnectByOpBase() { }
  virtual int64_t get_current_level() const = 0;
  virtual int get_sys_parent_path(ObString &parent_path) = 0;
  virtual int set_sys_current_path(const ObString &cur_str, const ObString &res_path) = 0;
  int construct_root_output_row();
  int calc_connect_by_root_exprs(bool is_root);
  int calc_other_conds(bool &is_match);
  int calc_sys_connect_by_path();
  // set level pseudo as a param store.
  int set_level_as_param(int64_t level);
  int64_t sys_connect_by_path_id_;
};


class ObNLConnectBySpec : public ObNLConnectBySpecBase
{
  OB_UNIS_VERSION_V(1);
public:
	ExprFixedArray hash_key_exprs_;
  ExprFixedArray hash_probe_exprs_;
  ObNLConnectBySpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObNLConnectBySpecBase(alloc, type), hash_key_exprs_(alloc), hash_probe_exprs_(alloc)
  {}
};

class ObNLConnectByOp : public ObNLConnectByOpBase
{
public:
  ObNLConnectByOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  ~ObNLConnectByOp();

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override;

  virtual OperatorOpenOrder get_operator_open_order() const override final
  { return OPEN_CHILDREN_FIRST; }

//  int prepare_rescan_params();

//  int restore_prior_expr();
  int64_t get_current_level() const override { return connect_by_pump_.get_current_level(); }
  virtual int get_sys_parent_path(ObString &parent_path) override
  { return connect_by_pump_.get_sys_path(sys_connect_by_path_id_, parent_path); }
  virtual int set_sys_current_path(const ObString &cur_str, const ObString &) override
  { return connect_by_pump_.concat_sys_path(sys_connect_by_path_id_, cur_str); }
  int calc_pseudo_flags(ObConnectByOpPump::PumpNode &node);

private:
  typedef int (ObNLConnectByOp::*state_operation_func_type)();
  typedef int (ObNLConnectByOp::*state_function_func_type)();

  //int to_expr();

  void reset();
  // CNTB_STATE_JOIN_END state operation and transfer functions.
  int join_end_operate();
  int join_end_func_end();

  // CNTB_STATE_READ_OUTPUT state operation and transfer functions.
  int read_output_operate();
  int read_output_func_going();
  int read_output_func_end();

  // CNTB_STATE_READ_PUMP state operation and transfer functions.
  int read_pump_operate();
  int read_pump_func_going();
  int read_pump_func_end();

  // CNTB_STATE_READ_LEFT state operation and transfer functions.
  int read_left_operate();
  int read_left_func_going();
  int read_left_func_end();

  // CNTB_STATE_READ_RIGHT state operation and transfer functions.
  int read_right_operate();
  int read_right_func_going();
  int read_right_func_end();

  int add_pseudo_column(ObConnectByOpPump::PumpNode &node);

  int init();
  int process_dump();
	bool need_dump()
  { return sql_mem_processor_.get_data_size() > sql_mem_processor_.get_mem_bound(); }
public:
  ObConnectByOpPump connect_by_pump_;
private:
  // state operations and transfer functions array.
  state_operation_func_type state_operation_func_[CNTB_STATE_STATE_COUNT];
  state_function_func_type state_function_func_[CNTB_STATE_STATE_COUNT][FT_TYPE_COUNT];
  ObCnntByOpState state_;
  // common::ObNewRow null_cell_row_;//used for root row output
  // common::ObNewRow mock_right_row_;//used for root row output
  bool is_inited_;
  bool output_generated_;
  lib::MemoryContext mem_context_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
};

}//sql
}//oceanbase
#endif /* SRC_SQL_ENGINE_OB_NESTED_LOOP_CONNECT_BY_OP_H_ */
