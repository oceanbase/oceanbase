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
  if (!stmt.get_load_arguments().is_csv_format_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid resolver results", K(ret));
  } else if (OB_FAIL(optimizer_ctx.init_direct_load_ctx(&ctx, stmt))) {
    LOG_WARN("fail to init direct load ctx", K(ret), K(stmt));
  } else {
    if (optimizer_ctx.can_use_direct_load()) {
      optimizer_ctx.set_use_direct_load();
    }
    table_direct_insert_ctx.set_is_direct(optimizer_ctx.use_direct_load());
    if (!table_direct_insert_ctx.get_is_direct()) {
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
