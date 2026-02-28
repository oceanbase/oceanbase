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

#define USING_LOG_PREFIX SQL_OPT
#include <typeinfo>
#include "sql/optimizer/optimizer_plan_rewriter/ob_plan_visitor.h"
#include "sql/optimizer/ob_log_operator_factory.h"
// Include all logical operator headers for std::is_base_of type checking
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_limit.h"
#include "sql/optimizer/ob_log_count.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_for_update.h"
#include "sql/optimizer/ob_log_distinct.h"
#include "sql/optimizer/ob_log_set.h"
#include "sql/optimizer/ob_log_expr_values.h"
#include "sql/optimizer/ob_log_values.h"
#include "sql/optimizer/ob_log_material.h"
#include "sql/optimizer/ob_log_window_function.h"
#include "sql/optimizer/ob_log_select_into.h"
#include "sql/optimizer/ob_log_topk.h"
#include "sql/optimizer/ob_log_merge.h"
#include "sql/optimizer/ob_log_granule_iterator.h"
#include "sql/optimizer/ob_log_join_filter.h"
#include "sql/optimizer/ob_log_sequence.h"
#include "sql/optimizer/ob_log_monitoring_dump.h"
#include "sql/optimizer/ob_log_function_table.h"
#include "sql/optimizer/ob_log_json_table.h"
#include "sql/optimizer/ob_log_unpivot.h"
#include "sql/optimizer/ob_log_link_scan.h"
#include "sql/optimizer/ob_log_link_dml.h"
#include "sql/optimizer/ob_log_temp_table_insert.h"
#include "sql/optimizer/ob_log_temp_table_access.h"
#include "sql/optimizer/ob_log_temp_table_transformation.h"
#include "sql/optimizer/ob_log_insert_all.h"
#include "sql/optimizer/ob_log_err_log.h"
#include "sql/optimizer/ob_log_stat_collector.h"
#include "sql/optimizer/ob_log_optimizer_stats_gathering.h"
#include "sql/optimizer/ob_log_values_table_access.h"
#include "sql/optimizer/ob_log_expand.h"
#include "sql/optimizer/ob_log_update.h"
#include "sql/optimizer/ob_log_delete.h"
#include "sql/optimizer/ob_log_insert.h"

namespace oceanbase
{
namespace sql
{
#define PLAN_REWRITER_DISPATCH_CASE(OP_TYPE_ENUM, METHOD_NAME, OP_CLASS) \
  case log_op_def::LOG_##OP_TYPE_ENUM: \
    ret = visit_##METHOD_NAME(reinterpret_cast<OP_CLASS*>(plannode), context, result); \
    break;

template<typename R, typename C>
int PlanVisitor<R, C>::dispatch_visit(ObLogicalOperator* plannode, C* context, R*& result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plannode)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    LOG_DEBUG("plan visitor:", K(typeid(*this).name()), K(plannode->get_op_id()), K(plannode->get_name()), KPC(context));
    switch (plannode->get_type()) {
      PLAN_REWRITER_DISPATCH_CASE(JOIN, join, ObLogJoin)
      PLAN_REWRITER_DISPATCH_CASE(SORT, sort, ObLogSort)
      PLAN_REWRITER_DISPATCH_CASE(LIMIT, limit, ObLogLimit)
      PLAN_REWRITER_DISPATCH_CASE(COUNT, count, ObLogCount)
      PLAN_REWRITER_DISPATCH_CASE(GROUP_BY, groupby, ObLogGroupBy)
      PLAN_REWRITER_DISPATCH_CASE(TABLE_SCAN, table_scan, ObLogTableScan)
      PLAN_REWRITER_DISPATCH_CASE(SUBPLAN_SCAN, subplan_scan, ObLogSubPlanScan)
      PLAN_REWRITER_DISPATCH_CASE(SUBPLAN_FILTER, subplan_filter, ObLogSubPlanFilter)
      PLAN_REWRITER_DISPATCH_CASE(EXCHANGE, exchange, ObLogExchange)
      PLAN_REWRITER_DISPATCH_CASE(FOR_UPD, for_update, ObLogForUpdate)
      PLAN_REWRITER_DISPATCH_CASE(DISTINCT, distinct, ObLogDistinct)
      PLAN_REWRITER_DISPATCH_CASE(SET, set, ObLogSet)
      PLAN_REWRITER_DISPATCH_CASE(EXPR_VALUES, expr_values, ObLogExprValues)
      PLAN_REWRITER_DISPATCH_CASE(VALUES, values, ObLogValues)
      PLAN_REWRITER_DISPATCH_CASE(MATERIAL, material, ObLogMaterial)
      PLAN_REWRITER_DISPATCH_CASE(WINDOW_FUNCTION, window_function, ObLogWindowFunction)
      PLAN_REWRITER_DISPATCH_CASE(SELECT_INTO, select_into, ObLogSelectInto)
      PLAN_REWRITER_DISPATCH_CASE(TOPK, topk, ObLogTopk)
      PLAN_REWRITER_DISPATCH_CASE(MERGE, merge, ObLogMerge)
      PLAN_REWRITER_DISPATCH_CASE(GRANULE_ITERATOR, granule_iterator, ObLogGranuleIterator)
      PLAN_REWRITER_DISPATCH_CASE(JOIN_FILTER, join_filter, ObLogJoinFilter)
      PLAN_REWRITER_DISPATCH_CASE(SEQUENCE, sequence, ObLogSequence)
      PLAN_REWRITER_DISPATCH_CASE(MONITORING_DUMP, monitoring_dump, ObLogMonitoringDump)
      PLAN_REWRITER_DISPATCH_CASE(FUNCTION_TABLE, function_table, ObLogFunctionTable)
      PLAN_REWRITER_DISPATCH_CASE(JSON_TABLE, json_table, ObLogJsonTable)
      PLAN_REWRITER_DISPATCH_CASE(UNPIVOT, unpivot, ObLogUnpivot)
      PLAN_REWRITER_DISPATCH_CASE(LINK_SCAN, link_scan, ObLogLinkScan)
      PLAN_REWRITER_DISPATCH_CASE(LINK_DML, link_dml, ObLogLinkDml)
      PLAN_REWRITER_DISPATCH_CASE(TEMP_TABLE_INSERT, temp_table_insert, ObLogTempTableInsert)
      PLAN_REWRITER_DISPATCH_CASE(TEMP_TABLE_ACCESS, temp_table_access, ObLogTempTableAccess)
      PLAN_REWRITER_DISPATCH_CASE(TEMP_TABLE_TRANSFORMATION, temp_table_transformation, ObLogTempTableTransformation)
      PLAN_REWRITER_DISPATCH_CASE(INSERT_ALL, insert_all, ObLogInsertAll)
      PLAN_REWRITER_DISPATCH_CASE(ERR_LOG, err_log, ObLogErrLog)
      PLAN_REWRITER_DISPATCH_CASE(STAT_COLLECTOR, stat_collector, ObLogStatCollector)
      PLAN_REWRITER_DISPATCH_CASE(OPTIMIZER_STATS_GATHERING, optimizer_stats_gathering, ObLogOptimizerStatsGathering)
      PLAN_REWRITER_DISPATCH_CASE(VALUES_TABLE_ACCESS, values_table_access, ObLogValuesTableAccess)
      PLAN_REWRITER_DISPATCH_CASE(EXPAND, expand, ObLogExpand)
      PLAN_REWRITER_DISPATCH_CASE(UPDATE, update, ObLogUpdate)
      PLAN_REWRITER_DISPATCH_CASE(DELETE, delete, ObLogDelete)
      PLAN_REWRITER_DISPATCH_CASE(INSERT, insert, ObLogInsert)

      default: {
        ret = visit_node(plannode, context, result);
        break;
      }
    }
  }
  return ret;
}

#undef PLAN_REWRITER_DISPATCH_CASE

template int PlanVisitor<RewriterResult, RewriterContext>::dispatch_visit(
    ObLogicalOperator*, RewriterContext*, RewriterResult*&);

} // namespace sql
} // namespace oceanbase
