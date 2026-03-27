/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/ddl/ob_create_ccl_rule_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace sql
{
ObCreateCCLRuleStmt::ObCreateCCLRuleStmt()
    : ObDDLStmt(stmt::T_CREATE_CCL_RULE),
    create_ccl_rule_arg_()
  {
  }

ObCreateCCLRuleStmt::ObCreateCCLRuleStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_CREATE_CCL_RULE),
    create_ccl_rule_arg_()
{
}

ObCreateCCLRuleStmt::~ObCreateCCLRuleStmt()
{
}

}//namespace sql
}//namespace oceanbase
