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

