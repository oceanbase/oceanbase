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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_I_LIB_CACHE_CONTEXT_
#define OCEANBASE_SQL_PLAN_CACHE_OB_I_LIB_CACHE_CONTEXT_

#include "sql/plan_cache/ob_lib_cache_register.h"

namespace oceanbase
{
namespace sql
{
class ObILibCacheKey;

// Each object in the ObLibCacheNameSpace enumeration structure needs to inherit ObILibCacheCtx
// to expand its own context
struct ObILibCacheCtx
{
public:
  ObILibCacheCtx()
    : key_(NULL)
  {
  }
  virtual ~ObILibCacheCtx() {}
  VIRTUAL_TO_STRING_KV(KP_(key));

  ObILibCacheKey *key_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SQL_PLAN_CACHE_OB_I_LIB_CACHE_CONTEXT_
