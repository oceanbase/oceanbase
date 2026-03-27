/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_I_LIB_CACHE_KEY_
#define OCEANBASE_SQL_PLAN_CACHE_OB_I_LIB_CACHE_KEY_

#include "sql/plan_cache/ob_lib_cache_register.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace sql
{
// The abstract interface class of library cache key, each object in the ObLibCacheNameSpace
// enum structure needs to inherit from this interface and implement its own implementation class
struct ObILibCacheKey
{
public:
  ObILibCacheKey() : namespace_(NS_INVALID)
  {
  }
  ObILibCacheKey(ObLibCacheNameSpace ns)
    : namespace_(ns)
  {
  }
  virtual ~ObILibCacheKey() {}
  virtual int deep_copy(common::ObIAllocator &allocator, const ObILibCacheKey &other) = 0;
  virtual uint64_t hash() const = 0;
  virtual bool is_equal(const ObILibCacheKey &other) const = 0;
  virtual bool operator==(const ObILibCacheKey &other) const
  {
    bool bool_ret = false;
    if (namespace_ != other.namespace_) {
      bool_ret = false;
    } else {
      bool_ret = is_equal(other);
    }
    return bool_ret;
  }
  virtual int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  VIRTUAL_TO_STRING_KV(K_(namespace));

  ObLibCacheNameSpace namespace_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SQL_PLAN_CACHE_OB_I_LIB_CACHE_KEY_
