/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_LIB_CACHE_KEY_CREATOR_
#define OCEANBASE_SQL_PLAN_CACHE_OB_LIB_CACHE_KEY_CREATOR_

#include "sql/plan_cache/ob_lib_cache_register.h"

namespace oceanbase
{
namespace sql
{
class ObILibCacheKey;

class OBLCKeyCreator
{
public:
  static int create_cache_key(ObLibCacheNameSpace ns,
                              common::ObIAllocator &allocator,
                              ObILibCacheKey*& key);
  template<typename ClassT>
  static int create(common::ObIAllocator &allocator, ObILibCacheKey*& key)
  {
    int ret = OB_SUCCESS;
    char *ptr = NULL;
    if (NULL == (ptr = (char *)allocator.alloc(sizeof(ClassT)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "failed to allocate memory for lib cache key", K(ret));
    } else {
      key = new(ptr)ClassT();
    }
    return ret;
  }
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SQL_PLAN_CACHE_OB_LIB_CACHE_KEY_CREATOR_
