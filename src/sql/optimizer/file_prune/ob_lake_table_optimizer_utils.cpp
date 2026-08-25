/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT

#include "ob_lake_table_optimizer_utils.h"

#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/dml/ob_hint.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

int ObLakeTableOptimizerUtils::get_enable_lake_table_parallel_resolving(const ObDMLStmt &stmt,
                                                                         bool &enable)
{
  int ret = OB_SUCCESS;
  bool enable_by_hint = false;
  bool is_hint_exists = false;
  enable = true;
  oceanbase::omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    LOG_WARN("tenant config is invalid", K(MTL_ID()));
  } else {
    enable = static_cast<bool>(tenant_config->enable_lake_table_parallel_resolving);
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(stmt.get_query_ctx())
      && OB_FAIL(stmt.get_query_ctx()->get_global_hint().opt_params_.get_bool_opt_param(
          ObOptParamHint::ENABLE_LAKE_TABLE_PARALLEL_RESOLVING,
          enable_by_hint,
          is_hint_exists))) {
    LOG_WARN("failed to get lake table parallel resolve hint", K(ret));
  } else if (is_hint_exists) {
    enable = enable_by_hint;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
