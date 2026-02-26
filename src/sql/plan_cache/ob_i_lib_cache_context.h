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
#include<algorithm>

namespace oceanbase
{
namespace sql
{
class ObILibCacheKey;

// Each object in the ObLibCacheNameSpace enumeration structure needs to inherit ObILibCacheCtx
// to expand its own context
static const int64_t LIBCACHE_DEFAULT_LOCK_TIMEOUT = 100000; // 100ms
struct ObILibCacheCtx
{
public:
  ObILibCacheCtx()
    : key_(NULL), need_destroy_node_(false), lock_timeout_(LIBCACHE_DEFAULT_LOCK_TIMEOUT)
  {
  }
  virtual ~ObILibCacheCtx() {}
  void set_lock_timeout(int64_t timeout) { lock_timeout_ = std::min(LIBCACHE_DEFAULT_LOCK_TIMEOUT, timeout); }
  int64_t get_lock_timeout() { return lock_timeout_; }
  VIRTUAL_TO_STRING_KV(KP_(key), K_(lock_timeout));

  ObILibCacheKey *key_;
  bool need_destroy_node_; // Indicate whether the cache node corresponding to key_ in lib cache is invalid
  int64_t lock_timeout_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SQL_PLAN_CACHE_OB_I_LIB_CACHE_CONTEXT_
