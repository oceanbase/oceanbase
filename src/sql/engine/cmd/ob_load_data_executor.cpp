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

int ObLoadDataExecutor::check_is_direct_load(const ObLoadDataHint &load_hint, bool &check_ret)
{
  int ret = OB_SUCCESS;
  int64_t enable_direct = 0;
  int64_t append = 0;
  if (OB_FAIL(load_hint.get_value(ObLoadDataHint::ENABLE_DIRECT, enable_direct))) {
    LOG_WARN("fail to get value of ENABLE_DIRECT", K(ret));
  } else if (OB_FAIL(load_hint.get_value(ObLoadDataHint::APPEND, append))) {
    LOG_WARN("fail to get value of APPEND", K(ret));
  } else if ((enable_direct != 0 || append != 0) && GCONF._ob_enable_direct_load) {
    check_ret = true;
  } else {
    check_ret = false;
  }
  return ret;
}

int ObLoadDataExecutor::execute(ObExecContext &ctx, ObLoadDataStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObLoadDataBase *load_impl = NULL;
  bool is_direct_load = false;
  if (!stmt.get_load_arguments().is_csv_format_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid resolver results", K(ret));
  } else if (OB_FAIL(check_is_direct_load(stmt.get_hints(), is_direct_load))) {
    LOG_WARN("fail to check is load mode", KR(ret));
  } else {
    if (!is_direct_load) {
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
    }
    load_impl->~ObLoadDataBase();
  }
  return ret;
}

} // sql
} // oceanbase
