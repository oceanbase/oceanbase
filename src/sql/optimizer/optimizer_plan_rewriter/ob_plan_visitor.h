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

#ifndef OCEANBASE_SQL_OB_PLAN_VISITOR_H
#define OCEANBASE_SQL_OB_PLAN_VISITOR_H

#include <type_traits>
#include "sql/ob_sql_define.h"
#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase
{
namespace sql
{

// Forward declarations
class ObLogPlan;
class ObLogJoin;
class ObLogSort;
class ObLogLimit;
class ObLogCount;
class ObLogGroupBy;
class ObLogTableScan;
class ObLogSubPlanScan;
class ObLogSubPlanFilter;
class ObLogExchange;
class ObLogForUpdate;
class ObLogDistinct;
class ObLogSet;
class ObLogDelUpd;
class ObLogExprValues;
class ObLogValues;
class ObLogMaterial;
class ObLogWindowFunction;
class ObLogSelectInto;
class ObLogTopk;
class ObLogMerge;
class ObLogGranuleIterator;
class ObLogJoinFilter;
class ObLogSequence;
class ObLogMonitoringDump;
class ObLogFunctionTable;
class ObLogJsonTable;
class ObLogUnpivot;
class ObLogLink;
class ObLogLinkScan;
class ObLogLinkDml;
class ObLogTempTableInsert;
class ObLogTempTableAccess;
class ObLogTempTableTransformation;
class ObLogInsertAll;
class ObLogErrLog;
class ObLogStatCollector;
class ObLogOptimizerStatsGathering;
class ObLogValuesTableAccess;
class ObLogExpand;
class ObLogUpdate;
class ObLogDelete;
class ObLogInsert;

// Base structures for rewriter context and result
struct RewriterResult {
  TO_STRING_EMPTY();
};

struct Void {
  TO_STRING_EMPTY();
};

struct RewriterContext {
  TO_STRING_EMPTY();
};

// Forward declaration
template<typename R, typename C>
class PlanVisitor;

template<typename R, typename C>
class SimplePlanRewriter;

template<typename R, typename C>
class SimplePlanVisitor;

template<typename DerivedOp, typename R, typename C>
inline int call_visit_node(PlanVisitor<R, C> &rewriter, DerivedOp *op, C* context, R*& result) {
  // Reinterpret cast bypasses type checking - safe because DerivedOp inherits from ObLogicalOperator
  ObLogicalOperator *base_op = reinterpret_cast<ObLogicalOperator *>(op);
  return rewriter.visit_node(base_op, context, result);
}

#define PLAN_VISITOR_VISIT_DECL(OP_TYPE, OP_CLASS) \
  virtual int visit_##OP_TYPE(OP_CLASS * node, C* context, R*& result) { \
    return call_visit_node(*this, node, context, result); \
  }

// Template base class for plan rewriters
template<typename R, typename C>
class PlanVisitor {
public:
  PlanVisitor() {}
  virtual ~PlanVisitor() {}

  virtual int visit_node(ObLogicalOperator * plannode, C* context, R*& result)  = 0;

  // Visitor method declarations - each defaults to calling visit_node
  PLAN_VISITOR_VISIT_DECL(join, ObLogJoin)
  PLAN_VISITOR_VISIT_DECL(sort, ObLogSort)
  PLAN_VISITOR_VISIT_DECL(limit, ObLogLimit)
  PLAN_VISITOR_VISIT_DECL(count, ObLogCount)
  PLAN_VISITOR_VISIT_DECL(groupby, ObLogGroupBy)
  PLAN_VISITOR_VISIT_DECL(table_scan, ObLogTableScan)
  PLAN_VISITOR_VISIT_DECL(subplan_scan, ObLogSubPlanScan)
  PLAN_VISITOR_VISIT_DECL(subplan_filter, ObLogSubPlanFilter)
  PLAN_VISITOR_VISIT_DECL(exchange, ObLogExchange)
  PLAN_VISITOR_VISIT_DECL(for_update, ObLogForUpdate)
  PLAN_VISITOR_VISIT_DECL(distinct, ObLogDistinct)
  PLAN_VISITOR_VISIT_DECL(set, ObLogSet)
  PLAN_VISITOR_VISIT_DECL(del_upd, ObLogDelUpd)
  PLAN_VISITOR_VISIT_DECL(expr_values, ObLogExprValues)
  PLAN_VISITOR_VISIT_DECL(values, ObLogValues)
  PLAN_VISITOR_VISIT_DECL(material, ObLogMaterial)
  PLAN_VISITOR_VISIT_DECL(window_function, ObLogWindowFunction)
  PLAN_VISITOR_VISIT_DECL(select_into, ObLogSelectInto)
  PLAN_VISITOR_VISIT_DECL(topk, ObLogTopk)
  PLAN_VISITOR_VISIT_DECL(merge, ObLogMerge)
  PLAN_VISITOR_VISIT_DECL(granule_iterator, ObLogGranuleIterator)
  PLAN_VISITOR_VISIT_DECL(join_filter, ObLogJoinFilter)
  PLAN_VISITOR_VISIT_DECL(sequence, ObLogSequence)
  PLAN_VISITOR_VISIT_DECL(monitoring_dump, ObLogMonitoringDump)
  PLAN_VISITOR_VISIT_DECL(function_table, ObLogFunctionTable)
  PLAN_VISITOR_VISIT_DECL(json_table, ObLogJsonTable)
  PLAN_VISITOR_VISIT_DECL(unpivot, ObLogUnpivot)
  PLAN_VISITOR_VISIT_DECL(link, ObLogLink)
  PLAN_VISITOR_VISIT_DECL(link_scan, ObLogLinkScan)
  PLAN_VISITOR_VISIT_DECL(link_dml, ObLogLinkDml)
  PLAN_VISITOR_VISIT_DECL(temp_table_insert, ObLogTempTableInsert)
  PLAN_VISITOR_VISIT_DECL(temp_table_access, ObLogTempTableAccess)
  PLAN_VISITOR_VISIT_DECL(temp_table_transformation, ObLogTempTableTransformation)
  PLAN_VISITOR_VISIT_DECL(insert_all, ObLogInsertAll)
  PLAN_VISITOR_VISIT_DECL(err_log, ObLogErrLog)
  PLAN_VISITOR_VISIT_DECL(stat_collector, ObLogStatCollector)
  PLAN_VISITOR_VISIT_DECL(optimizer_stats_gathering, ObLogOptimizerStatsGathering)
  PLAN_VISITOR_VISIT_DECL(values_table_access, ObLogValuesTableAccess)
  PLAN_VISITOR_VISIT_DECL(expand, ObLogExpand)
  PLAN_VISITOR_VISIT_DECL(update, ObLogUpdate)
  PLAN_VISITOR_VISIT_DECL(delete, ObLogDelete)
  PLAN_VISITOR_VISIT_DECL(insert, ObLogInsert)

  protected:
  // do notuse dipatch_visit in visitor
  // use visit instead
  int dispatch_visit(ObLogicalOperator* plannode, C* context, R*& result);

public:

  int visit(ObLogicalOperator* plannode, C* context, R*& result) {
    if (OB_ISNULL(plannode)) {
      return OB_ERR_UNEXPECTED;
    }
    return dispatch_visit(plannode, context, result);
  }
};

template<typename R, typename C>
class SimplePlanRewriter : public PlanVisitor<R, C> {
public:
  SimplePlanRewriter() {}
  virtual ~SimplePlanRewriter() {}
  int rewrite_child(ObLogicalOperator * child, C* context, R*& result) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL((this->dispatch_visit(child, context, result)))) {
    }
    return ret;
  }
  int rewrite_children(ObLogicalOperator * plannode, C* context, R*& result) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(plannode)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < plannode->get_num_of_child(); ++i) {
        ObLogicalOperator* child = plannode->get_child(i);
        if (OB_NOT_NULL(child)) {
          if (OB_FAIL((rewrite_child(child, context, result)))) {
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }
    return ret;
  }
  int rewrite_single_child(ObLogicalOperator * plannode, C* context, R*& result) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(plannode)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_UNLIKELY(plannode->get_num_of_child() != 1)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_ISNULL(plannode->get_child(0))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL((rewrite_child(plannode->get_child(0), context, result)))) {
    }
    return ret;
  }
  virtual int visit_node(ObLogicalOperator * plannode, C* context, R*& result) {
    return rewrite_children(plannode, context, result);
  }
};

template<typename R, typename C>
class SimplePlanVisitor : public PlanVisitor<R, C> {
public:
SimplePlanVisitor() {}
  virtual ~SimplePlanVisitor() {}
  int visit_child(ObLogicalOperator * child, R*& result) {
    return visit_child_with(child, NULL, result);
  }
  int visit_children(ObLogicalOperator * plannode, R*& result) {
    return visit_children_with(plannode, NULL, result);
  }
  int visit_child_with(ObLogicalOperator * child, C* context, R*& result) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL((this->dispatch_visit(child, context, result)))) {
    }
    return ret;
  }
  int visit_children_with(ObLogicalOperator * plannode, C* context, R*& result) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(plannode)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < plannode->get_num_of_child(); ++i) {
        ObLogicalOperator* child = plannode->get_child(i);
        if (OB_NOT_NULL(child)) {
          if (OB_FAIL((this->visit_child_with(child, context, result)))) {
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }
    return ret;
  }
  virtual int visit_node(ObLogicalOperator * plannode, C* context, R*& result) override {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(plannode)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = visit_children_with(plannode, context, result);
    }
    return ret;
  }
};
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_PLAN_VISITOR_H
