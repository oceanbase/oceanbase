/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "ob_savepoint_stmt.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

int ObSavePointStmt::set_sp_name(const char *str_value, int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str_value) || str_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid savepoint name", K(ret), KP(str_value), K(str_len));
  } else {
    sp_name_.assign_ptr(str_value, static_cast<int32_t>(str_len));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

