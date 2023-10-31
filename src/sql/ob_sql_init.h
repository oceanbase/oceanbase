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

#ifndef _OB_SQL_INIT_H
#define _OB_SQL_INIT_H 1

#include "lib/alloc/malloc_hook.h"
#include "engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_uuid.h"
#include "sql/engine/expr/ob_expr_res_type_map.h"
#include "sql/engine/expr/ob_expr_extra_info_factory.h"
#include "sql/plan_cache/ob_plan_cache_value.h"
#include "sql/plan_cache/ob_plan_set.h"
#include "sql/plan_cache/ob_lib_cache_register.h"
#include "sql/executor/ob_task_runner_notifier_service.h"
#include "sql/executor/ob_mini_task_executor.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/ob_end_trans_callback.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "share/object/ob_obj_cast.h"
#include "engine/ob_serializable_function.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/debug/ob_pl_debugger_manager.h"
#endif

namespace oceanbase
{
namespace sql
{
//inline void register_phy_operator_classes()
//{
//  ObRootTransmit *root_trans = new (std::nothrow) ObRootTransmit();
//  delete root_trans;
//}

inline int init_sql_factories()
{
  //**注意**,不要把这行日志删了, 该日志是为了初始化ObLog中线程局部
  //变量LogBufferMgr, 避免其在jit malloc hook中进行该线程局部变量的
  //new操作，导致malloc hook和日志模块的循环调用。
  SQL_LOG(INFO, "init sql factories");
  int ret = common::OB_SUCCESS;
  ObExprOperatorFactory::register_expr_operators();
  ObExprExtraInfoFactory::register_expr_extra_infos();
  ObLibCacheRegister::register_cache_objs();
  //register_phy_operator_classes();
  return OB_SUCCESS;
}

inline int init_sql_expr_static_var()
{
  int ret = common::OB_SUCCESS;
  lib::ObMallocAllocator *allocator = NULL;
  const lib::ObMemAttr attr(common::OB_SYS_TENANT_ID, ObModIds::OB_NUMBER);
  if (OB_FAIL(ObExprTRDateFormat::init())) {
    SQL_LOG(ERROR, "failed to init vars in oracle trunc", K(ret));
  } else if (OB_FAIL(ObExprUuid::init())) {
    SQL_LOG(ERROR, "failed to init vars in uuid", K(ret));
  } else if (OB_ISNULL(allocator = lib::ObMallocAllocator::get_instance())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(ERROR, "allocator is null", K(ret));
  } else if (OB_FAIL(common::ObNumberConstValue::init(*allocator, attr))) {
    SQL_LOG(ERROR, "failed to init ObNumberConstValue", K(ret));
  } else if (OB_FAIL(ARITH_RESULT_TYPE_ORACLE.init())) {
    SQL_LOG(ERROR, "failed to init ORACLE_ARITH_RESULT_TYPE", K(ret));
  } else if (OB_FAIL(ObCharsetUtils::init(*allocator))) {
    SQL_LOG(ERROR, "fail to init ObCharsetUtils", K(ret));
  } else if (OB_FAIL(ObCharset::init_charset())) {
    SQL_LOG(ERROR, "fail to init charset", K(ret));
  } else if (OB_FAIL(wide::ObDecimalIntConstValue::init_const_values(*allocator, attr))) {
    SQL_LOG(ERROR, "failed to init ObDecimalIntConstValue", K(ret));
  }
  return ret;
}

inline int init_sql_executor_singletons()
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(ObTaskRunnerNotifierService::build_instance())) {
    SQL_LOG(ERROR, "fail to build ObTaskRunnerNotifierService instance", K(ret));
#ifdef OB_BUILD_ORACLE_PL
  } else if (OB_FAIL(pl::ObPDBManager::build_instance())) {
    SQL_LOG(ERROR, "fail to build ObPDBManager instance", K(ret));
#endif
  } else {
    ObFuncSerialization::init();
  }
  if (OB_FAIL(ret)) {
    SQL_LOG(ERROR, "fail to init sql singletons", K(ret));
  }
  return ret;
}

inline void print_sql_stat()
{
  // do nothing
}
} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SQL_INIT_H */
