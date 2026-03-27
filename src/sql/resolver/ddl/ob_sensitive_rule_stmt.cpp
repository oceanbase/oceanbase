/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/ddl/ob_sensitive_rule_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace sql
{
  ObSensitiveRuleStmt::ObSensitiveRuleStmt()
    : ObDDLStmt(stmt::T_CREATE_SENSITIVE_RULE) {}

  ObSensitiveRuleStmt::~ObSensitiveRuleStmt() {}

} // end namespace sql
} // end namespace oceanbase
