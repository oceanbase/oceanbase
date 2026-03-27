/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_smart_var.h"
#include "lib/rc/context.h"
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
