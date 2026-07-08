/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/cmd/ob_system_cmd_resolver.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_stmt_type.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{

int ObSystemCmdResolver::check_ora_priv() const
{
  int ret = OB_SUCCESS;
  if (ObSchemaChecker::is_ora_priv_check()) {
    CK(OB_NOT_NULL(schema_checker_));
    CK(OB_NOT_NULL(get_basic_stmt()));
    OZ(schema_checker_->check_ora_ddl_priv(
           session_info_->get_effective_tenant_id(),
           session_info_->get_priv_user_id(),
           ObString(""),
           get_basic_stmt()->get_stmt_type(),
           session_info_->get_enable_role_array()),
       session_info_->get_effective_tenant_id(),
       session_info_->get_user_id());
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
