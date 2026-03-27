/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_PC
#include "sql/plan_cache/ob_lib_cache_key_creator.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace sql
{

int OBLCKeyCreator::create_cache_key(ObLibCacheNameSpace ns,
                                     common::ObIAllocator &allocator,
                                     ObILibCacheKey*& key)
{
  int ret = OB_SUCCESS;
  if (ns <= NS_INVALID || ns >= NS_MAX || OB_ISNULL(LC_CK_ALLOC[ns])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("out of the max type", K(ret), K(ns));
  } else if (OB_FAIL(LC_CK_ALLOC[ns](allocator, key))) {
    LOG_WARN("failed to create lib cache node", K(ret), K(ns));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
