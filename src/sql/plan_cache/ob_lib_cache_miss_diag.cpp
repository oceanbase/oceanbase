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
