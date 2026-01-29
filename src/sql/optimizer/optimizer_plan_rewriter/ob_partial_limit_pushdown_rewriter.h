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

#ifndef OCEANBASE_SQL_OB_PARTIAL_LIMIT_PUSHDOWN_REWRITER_H
#define OCEANBASE_SQL_OB_PARTIAL_LIMIT_PUSHDOWN_REWRITER_H

#include "sql/optimizer/optimizer_plan_rewriter/ob_plan_visitor.h"
#include "sql/optimizer/ob_log_join.h"
#include "lib/hash/ob_hashset.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace sql
{
class ObLogPlan;
struct LimitPushdownContext : public RewriterContext {
   LimitPushdownContext()
     : limit_count_(-1),
       limit_expr_(NULL),
       through_valuable_node_(false)
   { }

   virtual ~LimitPushdownContext()
   { }
   int64_t limit_count_;
   ObRawExpr *limit_expr_;

   bool through_valuable_node_;

   inline bool is_limit_one() {
    return limit_count_ == 1 &&
           limit_expr_ != NULL &&
           limit_expr_->is_const_expr() &&
           !limit_expr_->has_flag(CNT_STATIC_PARAM) &&
           !limit_expr_->has_flag(CNT_DYNAMIC_PARAM);
   }

   inline void becomes_valuable() {
    through_valuable_node_ = true;
   }
   static int init(common::ObIAllocator &allocator, ObRawExpr *limit_expr, int64_t limit_count, bool through_valuable_node, LimitPushdownContext *&context);
   TO_STRING_KV(
       K_(limit_count),
       K_(limit_expr),
       K_(through_valuable_node));
};

/*
 * Result of limit pushdown rewriter
 * rewrited_plan_: the rewritten plan, should always be non-null.
 */
struct LimitPushdownResult {
  LimitPushdownResult()
    : rewrited_plan_(NULL)
  { }
  virtual ~LimitPushdownResult()
  { }

  static int init(common::ObIAllocator &allocator, LimitPushdownResult *&result);
  int assign_to(common::ObIAllocator &allocator, LimitPushdownResult *&result);
  inline void set_plan(ObLogicalOperator *plan) { rewrited_plan_ = plan; }
  ObLogicalOperator *rewrited_plan_;
};
class LimitPushdownRewriter : public SimplePlanVisitor<LimitPushdownResult, LimitPushdownContext> {
  public:
    LimitPushdownRewriter(common::ObIAllocator &allocator,
                          sql::ObRawExprFactory &expr_factory,
                          ObSQLSessionInfo *session_info,
                          ObOptimizerContext* ctx);
    ~LimitPushdownRewriter();

    int is_simple_limit(ObLogLimit *log_limit, bool &is_simple);
    int default_rewrite(ObLogicalOperator *&plannode, LimitPushdownContext *context, LimitPushdownResult *&result);
    int visit_subplan_scan(ObLogSubPlanScan *subplan_scan, LimitPushdownContext *context, LimitPushdownResult *&result) override;
    int visit_exchange(ObLogExchange *exchange, LimitPushdownContext *context, LimitPushdownResult *&result) override;
    int visit_material(ObLogMaterial *material, LimitPushdownContext *context, LimitPushdownResult *&result) override;
    int visit_distinct(ObLogDistinct *distinct, LimitPushdownContext *context, LimitPushdownResult *&result) override;
    int visit_sort(ObLogSort *sort, LimitPushdownContext *context, LimitPushdownResult *&result) override;
    int visit_groupby(ObLogGroupBy *group_by, LimitPushdownContext *context, LimitPushdownResult *&result) override;
    int visit_subplan_filter(ObLogSubPlanFilter *subplan_filter, LimitPushdownContext *context, LimitPushdownResult *&result) override;
    int pushdown_limit_for_child(ObLogicalOperator *&child,
                                 LimitPushdownContext *context,
                                 LimitPushdownResult *&result,
                                 bool valuable = false);
    int contains_linked_scan(ObLogicalOperator* root, bool & contains);
    int extract_pushdown_context_from_limit(ObLogLimit *&log_limit, LimitPushdownContext *&pushdown_context, LimitPushdownContext *&child_context);
    int visit_limit(ObLogLimit *log_limit, LimitPushdownContext *context, LimitPushdownResult *&result) override;
    int visit_node(ObLogicalOperator *plannode, LimitPushdownContext *context, LimitPushdownResult *&result) override;
    int visit_set(ObLogSet *set, LimitPushdownContext *context, LimitPushdownResult *&result) override;
    int visit_join(ObLogJoin *join, LimitPushdownContext *context, LimitPushdownResult *&result) override;
    int visit_table_scan(ObLogTableScan *table_scan, LimitPushdownContext *context, LimitPushdownResult *&result) override;
    int do_pushdown_limit_for_children(ObLogicalOperator *&plannode,
                                       LimitPushdownContext *context,
                                       LimitPushdownResult *&result,
                                       bool through_valuable = false);
    int extract_pushdown_context(ObLogicalOperator *plannode,
                                  LimitPushdownContext *current_context,
                                  LimitPushdownContext *&pushdown_context,
                                  LimitPushdownContext *&not_pushdown_context);
    int valid_for_push_limit_into_group_by(ObLogGroupBy *group_by, bool &is_valid);
    int allocate_partial_limit(ObLogicalOperator *&plannode, LimitPushdownContext *context, LimitPushdownResult *&result);
  private:
    common::ObIAllocator &allocator_;
    sql::ObRawExprFactory &expr_factory_;
    ObSQLSessionInfo *session_info_;
    ObOptimizerContext* ctx_;
};
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_PARTIAL_LIMIT_PUSHDOWN_REWRITER_H
