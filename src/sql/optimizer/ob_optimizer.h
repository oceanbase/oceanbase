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
#include "sql/ob_sql_temp_table.h"
#ifndef _OB_OPTIMIZER_H
#define _OB_OPTIMIZER_H 1
namespace oceanbase
{
namespace sql
{
  class ObDMLStmt;
  class ObSelectStmt;
  class ObDelUpdStmt;
  class ObLogPlan;
  class ObOptimizerContext;
  class ObRawExpr;
  class ObLogicalOperator;
  class ObColumnRefRawExpr;
  /**
   * @enum TraverseOp
   * @brief Plan tree traversal operations
   */
  enum TraverseOp
  {
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
     * We may have unnecessary exprs produced during the expr allocation since
     * we basically produce all column items but some are implicitly resolved
     * and are not needed during execution, such as columns involved in partition
     * expression. We perform project pruning to remove them.
     */
    PROJECT_PRUNING,
    /**
     * Exchange allocation is also known as parallel optimization. Though there
     * are multiple possible optimization that can happen in this step, at the
     * moment we just perform simple check on table location and insert pairs of
     * exchange nodes at places where data exchange needs to happen(local
     * execution is not enough to satisfy the semantics. )
     */
    OPERATOR_NUMBERING,                                   // numbering operators
    EXCHANGE_NUMBERING,                                   // numbering exchange out for px
    GEN_SIGNATURE,                                  // generating plan signature
    GEN_LOCATION_CONSTRAINT,                      //generate plan location constraint, used from plan cache
    EXTRACT_PARAMS_FOR_SUBPLAN,                 // extract params for subplan
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
    PX_ESTIMATE_SIZE,
    RUNTIME_FILTER,

    ALLOC_STARTUP_EXPR,
    ADJUST_SHARED_EXPR,

    COLLECT_BATCH_EXEC_PARAM,
    ALLOC_OP,
    TRAVERSE_OP_END
  };

  struct NumberingCtx
  {
    NumberingCtx () :
      num_(0), num_include_monitoring_dump_(0), branch_id_(0), op_id_(0), plan_depth_(0), going_up_(false) {}
    uint64_t num_;
    uint64_t num_include_monitoring_dump_;
    uint64_t branch_id_;
    uint64_t op_id_;
    int64_t plan_depth_; // 算子在计划树的缩进层次，顶层算子从0算起
    bool going_up_;
  };

  struct NumberingExchangeCtx
  {
  private:
    struct IdStruct {
      IdStruct() : current_px_id_(common::OB_INVALID_ID), next_dfo_id_(common::OB_INVALID_ID) {}
      IdStruct(int64_t px_id, int64_t dfo_id) : current_px_id_(px_id), next_dfo_id_(dfo_id) {}
      int64_t current_px_id_;
      int64_t next_dfo_id_;
      TO_STRING_KV(K_(current_px_id), K_(next_dfo_id));
    };
  public:
    NumberingExchangeCtx () : next_px_id_(common::OB_INVALID_ID), ids_()
    {}
    int64_t next_px()
    {
      next_px_id_ = (next_px_id_ <= 0) ? 1 : next_px_id_ + 1;
      return next_px_id_;
    }
    // 栈顶 item 作为当前 px 的 dfo 计数器
    int next_dfo(int64_t &px_id, int64_t &dfo_id)
    {
      int ret = common::OB_SUCCESS;
      int64_t cnt = ids_.count();
      if (cnt <= 0) {
        ret = common::OB_ERR_UNEXPECTED;
      } else {
        IdStruct &top = ids_.at(cnt - 1);
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

  enum ObOptFlagBit
  {
    OPT_NO_NL_JOIN_FLAG = 0,
  };

  struct ObExprSelPair
  {
    ObExprSelPair(): expr_(NULL), sel_(0), represent_range_exprs_(false)
    {}
    ObExprSelPair(const ObRawExpr *expr, double sel, const bool represent_range_exprs = false)
        : expr_(expr), sel_(sel), represent_range_exprs_(represent_range_exprs)
    { }
    ~ObExprSelPair()
    { }
    bool operator==(const ObExprSelPair &rhs) const
    { return expr_ == rhs.expr_ && represent_range_exprs_ == rhs.represent_range_exprs_; }
    TO_STRING_KV(K(expr_), K(sel_));
    const ObRawExpr *expr_;
    double sel_;//selectivity of expr
    bool represent_range_exprs_;
  };

  class ObOptimizer
  {
  public:
    ObOptimizer(ObOptimizerContext &ctx)
      : ctx_(ctx)
    {}
    virtual ~ObOptimizer() {}
    virtual int optimize(ObDMLStmt &stmt, ObLogPlan *&plan);
    virtual int get_optimization_cost(ObDMLStmt &stmt,
                                      ObLogPlan *&plan,
                                      double &cost);
    virtual int get_cte_optimization_cost(ObDMLStmt &root_stmt,
                                          ObSelectStmt *cte_query,
                                          ObIArray<ObSelectStmt *> &stmts,
                                          double &cte_cost,
                                          ObIArray<double> &costs);
    int update_column_usage_infos();
  private:
    int generate_plan_for_temp_table(ObDMLStmt &stmt);
    int collect_temp_tables(ObIAllocator &allocator,
                            ObDMLStmt &stmt,
                            common::ObIArray<ObSqlTempTableInfo*> &temp_table_infos);
    bool exists_temp_table(const ObIArray<ObSqlTempTableInfo*> &temp_table_infos,
                           const ObSelectStmt *table_query) const;
    int init_env_info(ObDMLStmt &stmt);
    int get_session_parallel_info(int64_t &force_parallel_dop,
                                  bool &enable_auto_dop,
                                  bool &enable_manual_dop);
    int extract_opt_ctx_basic_flags(const ObDMLStmt &stmt,
                                    ObSQLSessionInfo &session);
    int init_parallel_policy(ObDMLStmt &stmt, const ObSQLSessionInfo &session);
    int set_auto_dop_params(const ObSQLSessionInfo &session);
    int check_pdml_enabled(const ObDMLStmt &stmt,
                           const ObSQLSessionInfo &session);
    int check_pdml_supported_feature(const ObDelUpdStmt &pdml_stmt,
                                     const ObSQLSessionInfo &session,
                                     bool &is_use_pdml);
    int check_is_heap_table(const ObDMLStmt &stmt);
    int check_merge_stmt_is_update_index_rowkey(const ObSQLSessionInfo &session,
                                                const ObDMLStmt &stmt,
                                                const ObIArray<uint64_t> &index_ids,
                                                bool &is_update);
    int extract_column_usage_info(const ObDMLStmt &stmt);
    int analyze_one_expr(const ObDMLStmt &stmt, const ObRawExpr *expr);
    int add_column_usage_arg(const ObDMLStmt &stmt,
                             const ObColumnRefRawExpr &column_expr,
                             int64_t flag);
    int check_whether_contain_nested_sql(const ObDMLStmt &stmt);
    int check_force_default_stat();
    int init_system_stat();
    int calc_link_stmt_count(const ObDMLStmt &stmt, int64_t &count);

  private:
    ObOptimizerContext &ctx_;
    DISALLOW_COPY_AND_ASSIGN(ObOptimizer);
  };
}
}
#endif
