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

#define USING_LOG_PREFIX SQL_PC
#include "ob_ps_sql_utils.h"
#include "lib/string/ob_string.h"
#include "lib/json/ob_json_print_utils.h"
#include "sql/plan_cache/ob_sql_parameterization.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

int ObPsSqlUtils::deep_copy_str(common::ObIAllocator &allocator,
                                const common::ObString &src,
                                common::ObString &dst)
{
  int ret = common::OB_SUCCESS;
  int32_t size = src.length() + 1;
  char* buf = static_cast<char *>(allocator.alloc(size));
  if (OB_ISNULL(buf)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret), K(size), K(src));
  } else {
    MEMCPY(buf, src.ptr(), src.length());
    buf[size-1] = '\0';
    dst.assign_ptr(buf, src.length());
  }
  return ret;
}

} //end of sql
} //end of oceanbase
