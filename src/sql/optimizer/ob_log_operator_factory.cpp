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
#include "ob_log_operator_factory.h"
#include "ob_log_table_scan.h"
#include "ob_log_mv_table_scan.h"
#include "ob_log_join.h"
#include "ob_log_sort.h"
#include "ob_log_group_by.h"
#include "ob_log_exchange.h"
#include "ob_log_limit.h"
#include "ob_log_subplan_scan.h"
#include "ob_log_subplan_filter.h"
#include "ob_log_insert.h"
#include "ob_log_update.h"
#include "ob_log_delete.h"
#include "ob_log_set.h"
#include "ob_log_distinct.h"
#include "ob_log_expr_values.h"
#include "ob_log_function_table.h"
#include "ob_log_values.h"
#include "ob_log_material.h"
#include "ob_log_window_function.h"
#include "ob_log_select_into.h"
#include "ob_log_topk.h"
#include "ob_optimizer_context.h"
#include "ob_log_append.h"
#include "ob_log_count.h"
#include "ob_log_sequence.h"
#include "ob_log_merge.h"
#include "ob_log_granule_iterator.h"
#include "ob_log_table_lookup.h"
#include "ob_log_conflict_row_fetcher.h"
#include "ob_log_monitoring_dump.h"
#include "ob_log_unpivot.h"
#include "ob_log_link.h"
#include "ob_log_for_update.h"
#include "ob_log_temp_table_insert.h"
#include "ob_log_temp_table_access.h"
#include "ob_log_temp_table_transformation.h"
#include "ob_log_insert_all.h"
using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::sql::log_op_def;
using namespace oceanbase::common;

const char* log_op_def::get_op_name(ObLogOpType type)
{
  const char* ret_char = NULL;
  static const char* ObLogOpName[LOG_OP_END + 2] = {
#define LOG_OP_DEF(type, name) name,
#include "sql/optimizer/ob_log_operator_factory.h"
#undef LOG_OP_DEF
#define END ""
      END
#undef END
  };

  if (type >= 0 && type < LOG_OP_END + 2) {
    ret_char = ObLogOpName[type];
  } else {
    ret_char = "INVALID_OP_NAME";
  }
  return ret_char;
}

ObLogOperatorFactory::ObLogOperatorFactory(common::ObIAllocator& allocator)
    : allocator_(allocator), op_store_(allocator)
{}

ObLogicalOperator* ObLogOperatorFactory::allocate(ObLogPlan& plan, ObLogOpType type)
{
  ObLogicalOperator* ret_op = NULL;
  void* ptr = NULL;
  switch (type) {
    case LOG_GROUP_BY: {
      ptr = allocator_.alloc(sizeof(ObLogGroupBy));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogGroupBy(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_SORT: {
      ptr = allocator_.alloc(sizeof(ObLogSort));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogSort(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_JOIN: {
      ptr = allocator_.alloc(sizeof(ObLogJoin));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogJoin(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_TABLE_SCAN: {
      ptr = allocator_.alloc(sizeof(ObLogTableScan));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogTableScan(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_MV_TABLE_SCAN: {
      ptr = allocator_.alloc(sizeof(ObLogMVTableScan));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogMVTableScan(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_LIMIT: {
      ptr = allocator_.alloc(sizeof(ObLogLimit));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogLimit(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_EXCHANGE: {
      ptr = allocator_.alloc(sizeof(ObLogExchange));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogExchange(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_SUBPLAN_SCAN: {
      ptr = allocator_.alloc(sizeof(ObLogSubPlanScan));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogSubPlanScan(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_SUBPLAN_FILTER: {
      ptr = allocator_.alloc(sizeof(ObLogSubPlanFilter));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogSubPlanFilter(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_INSERT: {
      ptr = allocator_.alloc(sizeof(ObLogInsert));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogInsert(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_SET: {
      ptr = allocator_.alloc(sizeof(ObLogSet));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogSet(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_UPDATE: {
      ptr = allocator_.alloc(sizeof(ObLogUpdate));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogUpdate(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_DELETE: {
      ptr = allocator_.alloc(sizeof(ObLogDelete));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogDelete(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_DISTINCT: {
      ptr = allocator_.alloc(sizeof(ObLogDistinct));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogDistinct(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_EXPR_VALUES: {
      ptr = allocator_.alloc(sizeof(ObLogExprValues));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogExprValues(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_VALUES: {
      ptr = allocator_.alloc(sizeof(ObLogValues));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogValues(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_MATERIAL: {
      ptr = allocator_.alloc(sizeof(ObLogMaterial));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogMaterial(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_WINDOW_FUNCTION: {
      ptr = allocator_.alloc(sizeof(ObLogWindowFunction));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogWindowFunction(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_SELECT_INTO: {
      ptr = allocator_.alloc(sizeof(ObLogSelectInto));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogSelectInto(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_TOPK: {
      ptr = allocator_.alloc(sizeof(ObLogTopk));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogTopk(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_APPEND: {
      ptr = allocator_.alloc(sizeof(ObLogAppend));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogAppend(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_COUNT: {
      ptr = allocator_.alloc(sizeof(ObLogCount));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogCount(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_MERGE: {
      ptr = allocator_.alloc(sizeof(ObLogMerge));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogMerge(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_GRANULE_ITERATOR: {
      ptr = allocator_.alloc(sizeof(ObLogGranuleIterator));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogGranuleIterator(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_TABLE_LOOKUP: {
      ptr = allocator_.alloc(sizeof(ObLogTableLookup));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogTableLookup(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_CONFLICT_ROW_FETCHER: {
      ptr = allocator_.alloc(sizeof(ObLogConflictRowFetcher));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogConflictRowFetcher(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_SEQUENCE: {
      ptr = allocator_.alloc(sizeof(ObLogSequence));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogSequence(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_MONITORING_DUMP: {
      ptr = allocator_.alloc(sizeof(ObLogMonitoringDump));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogMonitoringDump(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_FUNCTION_TABLE: {
      ptr = allocator_.alloc(sizeof(ObLogFunctionTable));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogFunctionTable(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_UNPIVOT: {
      ptr = allocator_.alloc(sizeof(ObLogUnpivot));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogUnpivot(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_TEMP_TABLE_INSERT: {
      ptr = allocator_.alloc(sizeof(ObLogTempTableInsert));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogTempTableInsert(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_TEMP_TABLE_ACCESS: {
      ptr = allocator_.alloc(sizeof(ObLogTempTableAccess));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogTempTableAccess(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_TEMP_TABLE_TRANSFORMATION: {
      ptr = allocator_.alloc(sizeof(ObLogTempTableTransformation));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogTempTableTransformation(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_LINK: {
      ptr = allocator_.alloc(sizeof(ObLogLink));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogLink(plan);
      } else { /* do nothing */
      }
      break;
    }
    case LOG_FOR_UPD: {
      ptr = allocator_.alloc(sizeof(ObLogForUpdate));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogForUpdate(plan);
      }
      break;
    }
    case LOG_INSERT_ALL: {
      ptr = allocator_.alloc(sizeof(ObLogInsertAll));
      if (NULL != ptr) {
        ret_op = new (ptr) ObLogInsertAll(plan);
      }
      break;
    }
    default: {
      break;
    }
  }

  if (NULL != ret_op) {
    ret_op->set_type(type);
    int ret = OB_SUCCESS;
    if (OB_FAIL(op_store_.store_obj(ret_op))) {
      LOG_WARN("store operator failed", K(ret));
      ret_op->~ObLogicalOperator();
      ret_op = NULL;
    }
  }
  return ret_op;
}

void ObLogOperatorFactory::destory()
{
  DLIST_FOREACH_NORET(node, op_store_.get_obj_list())
  {
    if (node != NULL && node->get_obj() != NULL) {
      node->get_obj()->~ObLogicalOperator();
      node->get_obj() = NULL;
    }
  }
  op_store_.destory();
}
