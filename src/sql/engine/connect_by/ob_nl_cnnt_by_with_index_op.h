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

#ifndef SRC_SQL_ENGINE_OB_NESTED_LOOP_CONNECT_BY_WITH_INDEX_OP_H_
#define SRC_SQL_ENGINE_OB_NESTED_LOOP_CONNECT_BY_WITH_INDEX_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/sort/ob_sort_basic_info.h"
#include "ob_cnnt_by_pump_bfs.h"
#include "ob_nl_cnnt_by_op.h"

namespace oceanbase
{
namespace sql
{

class ObNLConnectByWithIndexSpec : public ObNLConnectBySpecBase
{
  OB_UNIS_VERSION_V(1);
public:
  ObNLConnectByWithIndexSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObNLConnectBySpecBase(alloc, type)
  {}
};

class ObNLConnectByWithIndexOp : public ObNLConnectByOpBase
{
public:
  ObNLConnectByWithIndexOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  ~ObNLConnectByWithIndexOp();

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override;

  virtual OperatorOpenOrder get_operator_open_order() const override final
  { return OPEN_SELF_FIRST; }

  int prepare_rescan_params();
  int restore_prior_expr();
  int calc_connect_by_root_exprs(bool is_root);

  int64_t get_current_level() const override { return connect_by_pump_.get_current_level(); }
  virtual int get_sys_parent_path(ObString &parent_path) override
  { return connect_by_pump_.get_parent_path(sys_connect_by_path_id_, parent_path); }
  virtual int set_sys_current_path(const ObString &, const ObString &res_path) override
  { return connect_by_pump_.set_cur_node_path(sys_connect_by_path_id_, res_path); }
private:
  typedef int (ObNLConnectByWithIndexOp::*state_operation_func_type)();
  typedef int (ObNLConnectByWithIndexOp::*state_function_func_type)();

  int to_expr();

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

  int add_pseudo_column(ObExpr *pseudo_expr, ObConnectByPseudoColumn column_type);

  int init();
  int rescan_right();
public:
  ObConnectByOpBFSPump connect_by_pump_;
private:
  // state operations and transfer functions array.
  state_operation_func_type state_operation_func_[CNTB_STATE_STATE_COUNT];
  state_function_func_type state_function_func_[CNTB_STATE_STATE_COUNT][FT_TYPE_COUNT];
  ObCnntByOpState state_;
  // const common::ObNewRow *root_row_;
  const ObChunkDatumStore::StoredRow *root_row_;
  const ObChunkDatumStore::StoredRow *cur_output_row_;
  // common::ObNewRow null_cell_row_;//used for root row output
  // common::ObNewRow mock_right_row_;//used for root row output
  bool is_match_;//判断是否存在连接成功的情况，用来生产connect_by_isleaf
  bool is_cycle_;//判断是否有循环的情况，用来生产connect_by_iscycle
  bool is_inited_;
  bool need_return_;
  lib::MemoryContext mem_context_;
  ObChunkDatumStore::StoredRow *connect_by_root_row_;
};

}//sql
}//oceanbase
#endif /* SRC_SQL_ENGINE_OB_NESTED_LOOP_CONNECT_BY_WITH_INDEX_OP_H_ */
