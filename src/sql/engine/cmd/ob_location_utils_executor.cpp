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
#define USING_LOG_PREFIX  SQL_ENG
#include "sql/engine/cmd/ob_location_utils_executor.h"
#include "sql/engine/ob_exec_context.h"
#include "share/external_table/ob_external_table_utils.h"
#include "sql/engine/expr/ob_expr_regexp_context.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

int ObLocationUtilsExecutor::execute(ObExecContext &ctx, ObLocationUtilsStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  if (NULL == (session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(ERROR, "session is NULL", K(ret), K(ctx));
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObLocationSchema *loc_schema = NULL;
    ObString location_obj = stmt.get_location_name();
    const uint64_t tenant_id = session->get_effective_tenant_id();
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pointer is null", K(ret), KP(GCTX.schema_service_));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_location_schema_by_name(tenant_id, location_obj, loc_schema))) {
      LOG_WARN("failed to get schema by location name", K(ret), K(tenant_id), K(location_obj));
    } else if (OB_ISNULL(loc_schema)) {
      ret = OB_LOCATION_OBJ_NOT_EXIST;
      LOG_WARN("location object does't exist", K(ret), K(tenant_id), K(location_obj));
    } else {
      if (OB_LOCATION_UTILS_REMOVE == stmt.get_op_type()) {
        ObSqlString full_path;
        OZ (full_path.append(loc_schema->get_location_url_str()));
        if (OB_SUCC(ret) && full_path.length() > 0
            && *(full_path.ptr() + full_path.length() - 1) != '/') {
          OZ (full_path.append("/"));
        }
        OZ (full_path.append(stmt.get_sub_path()));
        ObExprRegexpSessionVariables regexp_vars;
        OZ (ObExternalTableUtils::remove_external_file_list(tenant_id,
                                                            full_path.string(),
                                                            loc_schema->get_location_access_info_str(),
                                                            stmt.get_pattern(),
                                                            regexp_vars,
                                                            ctx.get_allocator()
                                                            ));
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid op type", K(ret), K(tenant_id), K(location_obj), K(stmt.get_op_type()));
      }
    }
  }
  return ret;
}