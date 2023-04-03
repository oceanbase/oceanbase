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
#include "ob_log_plan_factory.h"
#include "sql/resolver/ob_stmt.h"
#include "sql/optimizer/ob_select_log_plan.h"
#include "sql/optimizer/ob_delete_log_plan.h"
#include "sql/optimizer/ob_update_log_plan.h"
#include "sql/optimizer/ob_insert_log_plan.h"
#include "sql/optimizer/ob_explain_log_plan.h"
#include "sql/optimizer/ob_help_log_plan.h"
#include "sql/optimizer/ob_merge_log_plan.h"
#include "sql/optimizer/ob_insert_all_log_plan.h"
#include "sql/optimizer/ob_optimizer_context.h"
using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObLogPlanFactory::ObLogPlanFactory(ObIAllocator &allocator)
  : allocator_(allocator),
    plan_store_(allocator)
{
}

ObLogPlanFactory::~ObLogPlanFactory()
{
  //destroy();
}
// TODO(jiuman): will rewrite it with template later to remove redundancy
ObLogPlan *ObLogPlanFactory::create(ObOptimizerContext &ctx, const ObDMLStmt &stmt)
{
  ObLogPlan *ret = NULL;
  switch (stmt.get_stmt_type()) {
  case stmt::T_SHOW_INDEXES:
  case stmt::T_SELECT: {
    void *ptr = allocator_.alloc(sizeof(ObSelectLogPlan));
    if (NULL != ptr) {
      ret = new (ptr) ObSelectLogPlan(ctx, static_cast<const ObSelectStmt*>(&stmt));
    } else {
      SQL_OPT_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "Allocate ObSelectLogPlan error");
    }
    break;
  }
  case stmt::T_DELETE: {
    void *ptr = allocator_.alloc(sizeof(ObDeleteLogPlan));
    if (NULL != ptr) {
      ret = new (ptr) ObDeleteLogPlan(ctx, static_cast<const ObDeleteStmt*>(&stmt));
    } else {
      SQL_OPT_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "Allocate ObDeleteLogPlan error");
    }
    break;
  }
  case stmt::T_UPDATE: {
    void *ptr = allocator_.alloc(sizeof(ObUpdateLogPlan));
    if (NULL != ptr) {
      ret = new (ptr) ObUpdateLogPlan(ctx, static_cast<const ObUpdateStmt*>(&stmt));
    } else {
      SQL_OPT_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "Allocate ObUpdateLogPlan error");
    }
    break;
  }
  case stmt::T_INSERT:
  case stmt::T_REPLACE: {
    void *ptr = allocator_.alloc(sizeof(ObInsertLogPlan));
    if (NULL != ptr) {
      ret = new (ptr) ObInsertLogPlan(ctx, static_cast<const ObInsertStmt*>(&stmt));
    } else {
      SQL_OPT_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "Allocate ObInsertLogPlan error");
    }
    break;
  }
  case stmt::T_EXPLAIN: {
    void *ptr = allocator_.alloc(sizeof(ObExplainLogPlan));
    if (NULL != ptr) {
      ret = new (ptr) ObExplainLogPlan(ctx, &stmt);
    } else {
      SQL_OPT_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "Allocate ObExplainLogPlan error");
    }
    break;
  }
  case stmt::T_HELP: {
    void *ptr = allocator_.alloc(sizeof(ObHelpLogPlan));
    if (NULL != ptr) {
      ret = new (ptr) ObHelpLogPlan(ctx, &stmt);
    } else {
    }
    break;
  }
  case stmt::T_MERGE: {
    void *ptr = allocator_.alloc(sizeof(ObMergeLogPlan));
    if (NULL != ptr) {
      ret = new (ptr) ObMergeLogPlan(ctx, static_cast<const ObMergeStmt*>(&stmt));
    } else {
      SQL_OPT_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "Allocate ObMergeLogPlan error");
    }
    break;
  }
  case stmt::T_INSERT_ALL: {
    void *ptr = allocator_.alloc(sizeof(ObInsertAllLogPlan));
    if (NULL != ptr) {
      ret = new (ptr) ObInsertAllLogPlan(ctx, static_cast<const ObInsertAllStmt*>(&stmt));
    } else {
      SQL_OPT_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "Allocate ObInsertAllLogPlan error");
    }
    break;
  }
  default:
    break;
  }
  if (ret != NULL) {
    int err = OB_SUCCESS;
    if (OB_SUCCESS != (err = plan_store_.store_obj(ret))) {
      LOG_WARN_RET(err, "store log plan failed", K(err));
      ret->~ObLogPlan();
      ret = NULL;
    }
  }
  return ret;
}

void ObLogPlanFactory::destroy()
{
  DLIST_FOREACH_NORET(node, plan_store_.get_obj_list()) {
    if (node != NULL && node->get_obj() != NULL) {
      node->get_obj()->destory();
    }
  }
}
