/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/ddl/ob_drop_ccl_rule_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace sql
{
ObDropCCLRuleStmt::ObDropCCLRuleStmt()
    : ObDDLStmt(stmt::T_DROP_CCL_RULE),
    drop_ccl_rule_arg_()
  {
  }

ObDropCCLRuleStmt::ObDropCCLRuleStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_DROP_CCL_RULE),
    drop_ccl_rule_arg_()
{
}

ObDropCCLRuleStmt::~ObDropCCLRuleStmt()
{
}

}//namespace sql
}//namespace oceanbase
