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

#include "common/ob_smart_var.h"
#include "lib/rc/context.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
namespace common
{

ERRSIM_POINT_DEF(ERRSIM_SMART_VAR);

void *__attribute__((weak)) smart_alloc(const int64_t nbyte, const char *label)
{
  int ret = ERRSIM_SMART_VAR ? : OB_SUCCESS;
  ObMemAttr attr;
  attr.label_ = label;
  return OB_SUCCESS == ret ? lib::ctxalf(nbyte, attr) : nullptr;
}

void __attribute__((weak)) smart_free(void *ptr)
{
  lib::ctxfree(ptr);
}

} // namespace common
} // namespace oceanbase
