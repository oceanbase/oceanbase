/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_lib_cache_miss_diag.h"
namespace oceanbase
{
namespace sql
{

  #define REGISTER(code, level, info) \
    LibCacheMissEvent(code, level, info)

  LibCacheMissEvent ObLibCacheMissEventRegister::EVENTS[MAX_CODE + 1] = {
    REGISTER(MAX_CODE, 0, ""),
    #define LIB_CACHE_MISS_EVENT(code, level, info) REGISTER(code, level, info),
    #include "sql/plan_cache/ob_lib_cache_miss_diag.h"
    #undef LIB_CACHE_MISS_EVENT
    REGISTER(MAX_CODE, 0, "")
  };
}
}
