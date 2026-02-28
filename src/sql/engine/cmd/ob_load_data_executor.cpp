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

#include "sql/engine/cmd/ob_load_data_executor.h"

#include "sql/engine/cmd/ob_load_data_direct_impl.h"
#include "sql/optimizer/ob_direct_load_optimizer_ctx.h"

namespace oceanbase
{
namespace sql
{

int ObLoadDataExecutor::execute(ObExecContext &ctx, ObLoadDataStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTableDirectInsertCtx &table_direct_insert_ctx = ctx.get_table_direct_insert_ctx();
  ObLoadDataBase *load_impl = NULL;
  ObDirectLoadOptimizerCtx optimizer_ctx;
  stmt.set_optimizer_ctx(&optimizer_ctx);
  if (stmt.is_load_data_url()) {
    if (OB_ISNULL(load_impl = OB_NEWx(ObLoadDataURLImpl, (&ctx.get_allocator())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    }
  } else if (!stmt.get_load_arguments().is_csv_format_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid resolver results", K(ret));
  } else if (OB_FAIL(optimizer_ctx.init_direct_load_ctx(&ctx, stmt))) {
    LOG_WARN("fail to init direct load ctx", K(ret), K(stmt));
  } else {
    if (optimizer_ctx.can_use_direct_load()) {
      optimizer_ctx.set_use_direct_load();
    }
    table_direct_insert_ctx.set_is_direct(optimizer_ctx.use_direct_load());
    ObSchemaGetterGuard *schema_guard = nullptr;
    if (OB_ISNULL(schema_guard = ctx.get_sql_ctx()->schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema guard is null", KR(ret));
    } else {
      const ObLoadArgument &load_args = stmt.get_load_arguments();
      const ObSimpleTableSchemaV2 *table_schema = nullptr;
      if (OB_FAIL(schema_guard->get_simple_table_schema(load_args.tenant_id_, load_args.table_id_, table_schema))) {
        LOG_WARN("fail to get table schema", KR(ret), K(load_args.tenant_id_), K(load_args.table_id_));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", KR(ret), K(load_args.tenant_id_), K(load_args.table_id_));
      } else if (!table_direct_insert_ctx.get_is_direct() && ObTableType::TMP_TABLE == table_schema->get_table_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support simple load data to mysql_tmp_table", KR(ret), KPC(table_schema));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "please use direct load hint, normal load data to mysql temporary table");
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!table_direct_insert_ctx.get_is_direct()) {
      if (OB_ISNULL(load_impl = OB_NEWx(ObLoadDataSPImpl, (&ctx.get_allocator())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      }
    } else {
      if (OB_ISNULL(load_impl = OB_NEWx(ObLoadDataDirectImpl, (&ctx.get_allocator())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (stmt.get_table_assignment().count() > 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("direct load not support");
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(load_impl->execute(ctx, stmt))) {
      LOG_WARN("failed to execute load data stmt", K(ret));
    } else {
      LOG_TRACE("load data success");
    }
    load_impl->~ObLoadDataBase();
  }
  return ret;
}

} // sql
} // oceanbase
