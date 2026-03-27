/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_PC
#include "ob_ps_sql_utils.h"
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
