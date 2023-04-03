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
