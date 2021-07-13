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

#include "share/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#ifndef _OB_OPTIMIZER_H
#define _OB_OPTIMIZER_H 1
namespace oceanbase {
namespace sql {
class ObDMLStmt;
class ObLogPlan;
class ObOptimizerContext;
class ObRawExpr;
class ObLogicalOperator;
class ObColumnRefRawExpr;
/**
 * @enum TraverseOp
 * @brief Plan tree traversal operations
 */
enum TraverseOp {
  /**
   * The output expression allocation is a top-down and bottom-up process.
   * During the top-down process, at each operator, we put the expressions
   * needed by the operator into a request list and pass it down to its
   * children.
   *
   * Once we reach leaf operator, which is usually a table scan, we produce all
   * column items and check if we can satisfy any required expression. From
   * that point on, the process has become a bottom-up one, where each operator
   * will check if they can produce any not-yet-produced expression.
   *
   * And last, at the root operator, we check if all requested expressions has
   * been produced.
   */
  ALLOC_EXPR = 0,
  /**
   * In some cases, the operator may not need to produce any columns to the
   * parent operator, such as table scan in a cartesian join or if the join
   * condition has been placed in the filter expressions. However, in order to
   * execute the operator iterator-model, we still need to produce at least
   * one expr. Currently, we just copy all access exprs to the output. The
   * exact reason is explained inline.
   */
  ALLOC_DUMMY_OUTPUT,
  /**
   * We may have unnecessary exprs produced during the expr allocation since
   * we basically produce all column items but some are implicitly resolved
   * and are not needed during execution, such as columns involved in partition
   * expression. We perform project pruning to remove them.
   */
  PROJECT_PRUNING,
  /**
   * Exchange allocation is also known as parallel optimzation. Though there
   * are multiple possible optimization that can happen in this step, at the
   * moment we just perform simple check on table location and insert pairs of
   * exchange nodes at places where data exchange needs to happen(local
   * execution is not enough to satisfy the semantics. )
   */
  ALLOC_EXCH,
  OPERATOR_NUMBERING,            // numbering operators
  EXCHANGE_NUMBERING,            // numbering exchange out for px
  GEN_SIGNATURE,                 // generating plan signature
  GEN_LOCATION_CONSTRAINT,       // generate plan location constraint, used from plan cache
  EXPLAIN_COLLECT_WIDTH,         // explain calculate column width
  EXPLAIN_WRITE_BUFFER,          // explain write plan table
  EXPLAIN_WRITE_BUFFER_OUTPUT,   // explain write output and filters
  EXPLAIN_WRITE_BUFFER_OUTLINE,  // explain write outline
  EXPLAIN_INDEX_SELECTION_INFO,  // explain index selection info
  ADJUST_SORT_OPERATOR,          // remove useless operator, set merge sort, task order, prefix sort
  RE_CALC_OP_COST,
  /**
   * alloc granule iterator after all other operation
   * */
  ALLOC_GI,
  /**
   * Add block operator (material) to make sure DFO can be scheduled in
   * consumer/producer threads model.
   */
  PX_PIPE_BLOCKING,
  /**
   * when exchange op is on the right branch of a subplan filter,
   * it must support rescan
   */
  PX_RESCAN,
  ALLOC_MONITORING_DUMP,
  PX_ESTIMATE_SIZE,
  REORDER_PROJECT_COLUMNS,

  /**
   * Add remove_const() expr above const expr of operator output, to make sure const expr
   * not overwritten in static typing engine. (only overwrite the added remove_const() expr).
   */
  CG_PREPARE,

  // dblink
  ALLOC_LINK,
  GEN_LINK_STMT,
  TRAVERSE_OP_END
};

#define CtxcatNeedTraverseOp(op) (OPERATOR_NUMBERING <= (op) || ALLOC_DUMMY_OUTPUT == (op))

struct NumberingCtx {
  NumberingCtx() : num_(0), num_include_monitoring_dump_(0), branch_id_(0), op_id_(0), plan_depth_(0), going_up_(false)
  {}
  uint64_t num_;
  uint64_t num_include_monitoring_dump_;
  uint64_t branch_id_;
  uint64_t op_id_;
  int64_t plan_depth_;
  bool going_up_;
};

struct NumberingExchangeCtx {
private:
  struct IdStruct {
    IdStruct() : current_px_id_(common::OB_INVALID_ID), next_dfo_id_(common::OB_INVALID_ID)
    {}
    IdStruct(int64_t px_id, int64_t dfo_id) : current_px_id_(px_id), next_dfo_id_(dfo_id)
    {}
    int64_t current_px_id_;
    int64_t next_dfo_id_;
    TO_STRING_KV(K_(current_px_id), K_(next_dfo_id));
  };

public:
  NumberingExchangeCtx() : next_px_id_(common::OB_INVALID_ID), ids_()
  {}
  int64_t next_px()
  {
    next_px_id_ = (next_px_id_ <= 0) ? 1 : next_px_id_ + 1;
    return next_px_id_;
  }
  int next_dfo(int64_t& px_id, int64_t& dfo_id)
  {
    int ret = common::OB_SUCCESS;
    int64_t cnt = ids_.count();
    if (cnt <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      IdStruct& top = ids_.at(cnt - 1);
      px_id = top.current_px_id_;
      dfo_id = top.next_dfo_id_++;
    }
    return ret;
  }
  int push_px(int64_t px_id)
  {
    return ids_.push_back(IdStruct(px_id, 0));
  }
  int pop_px()
  {
    IdStruct dummy;
    return ids_.pop_back(dummy);
  }

private:
  int64_t next_px_id_;
  common::ObSEArray<IdStruct, 4> ids_;
};

enum ObOptFlagBit {
  OPT_NO_NL_JOIN_FLAG = 0,
};

struct ObExprSelPair {
  ObExprSelPair() : expr_(NULL), sel_(0)
  {}
  ObExprSelPair(const ObRawExpr* expr, double sel) : expr_(expr), sel_(sel)
  {}
  ~ObExprSelPair()
  {}
  bool operator==(const ObExprSelPair& rhs) const
  {
    return expr_ == rhs.expr_;
  }
  TO_STRING_KV(K(expr_), K(sel_));
  const ObRawExpr* expr_;
  double sel_;  // selectiviy of expr
};

class ObOptimizer {
public:
  ObOptimizer(ObOptimizerContext& ctx) : ctx_(ctx)
  {}
  virtual ~ObOptimizer()
  {}
  virtual int optimize(ObDMLStmt& stmt, ObLogPlan*& plan);
  virtual int get_optimization_cost(ObDMLStmt& stmt, double& cost);

private:
  int generate_plan_for_temp_table(ObDMLStmt& stmt);
  int init_env_info(ObDMLStmt& stmt);
  int get_stmt_max_table_dop(ObDMLStmt& stmt, int64_t& max_dop);
  int get_session_parallel_info(
      ObDMLStmt& stmt, bool use_pdml, bool& session_px_enable_parallel, uint64_t& session_force_parallel_dop);
  int check_pdml_enabled(const ObDMLStmt& stmt, const ObSQLSessionInfo& session, bool& is_use_pdml);
  int check_pdml_supported_feature(const ObDMLStmt& stmt, const ObSQLSessionInfo& session, bool& is_use_pdml);
  int check_unique_index(const common::ObIArray<ObColumnRefRawExpr*>& column_exprs, bool& has_unique_index) const;

private:
  ObOptimizerContext& ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObOptimizer);
};
}  // namespace sql
}  // namespace oceanbase
#endif
