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

#include "lib/oblog/ob_log_module.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "sql/engine/cmd/ob_load_data_direct_impl.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{

int ObLoadDataExecutor::check_is_direct_load(ObTableDirectInsertCtx &ctx, const ObLoadDataHint &load_hint)
{
  int ret = OB_SUCCESS;
  ctx.set_is_direct(false);
  if (GCONF._ob_enable_direct_load) {
    const bool enable_direct = load_hint.get_direct_load_hint().is_enable();
    int64_t append = 0;
    if (enable_direct) { // direct
      ctx.set_is_direct(true);
    } else if (OB_FAIL(load_hint.get_value(ObLoadDataHint::APPEND, append))) {
      LOG_WARN("fail to get APPEND", KR(ret));
    } else if (append != 0) { // append
      ctx.set_is_direct(true);
    }
  }
  LOG_INFO("check load data is direct done.", K(ctx.get_is_direct()));
  return ret;
}

int ObLoadDataExecutor::execute(ObExecContext &ctx, ObLoadDataStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTableDirectInsertCtx &table_direct_insert_ctx = ctx.get_table_direct_insert_ctx();
  ObLoadDataBase *load_impl = NULL;
  if (!stmt.get_load_arguments().is_csv_format_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid resolver results", K(ret));
  } else if (OB_FAIL(check_is_direct_load(table_direct_insert_ctx, stmt.get_hints()))) {
    LOG_WARN("fail to check is load mode", KR(ret));
  } else {
    if (!table_direct_insert_ctx.get_is_direct()) {
      if (OB_ISNULL(load_impl = OB_NEWx(ObLoadDataSPImpl, (&ctx.get_allocator())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (OB_UNLIKELY(stmt.get_load_arguments().file_iter_.count() > 1)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support multiple files", K(ret));
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
