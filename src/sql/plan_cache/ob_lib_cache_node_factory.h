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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_LIB_CACHE_NODE_FACTORY_
#define OCEANBASE_SQL_PLAN_CACHE_OB_LIB_CACHE_NODE_FACTORY_

#include "sql/plan_cache/ob_i_lib_cache_node.h"
#include "sql/plan_cache/ob_plan_cache_util.h"

namespace oceanbase
{
namespace sql
{
class ObPlanCache;

class ObLCNodeFactory
{
public:
  ObLCNodeFactory() : lib_cache_(NULL) {}
  ObLCNodeFactory(ObPlanCache *lib_cache)
    : lib_cache_(lib_cache)
  {
  }
  void set_lib_cache(ObPlanCache *lib_cache) { lib_cache_ = lib_cache; }
  void destroy_cache_node(ObILibCacheNode* node);
  int create_cache_node(ObLibCacheNameSpace ns,
                        ObILibCacheNode*& node,
                        uint64_t tenant_id,
                        lib::MemoryContext &parent_context);
  template<typename ClassT>
  static int create(lib::MemoryContext &mem_ctx, ObILibCacheNode*& node, ObPlanCache *lib_cache);

private:
  ObPlanCache *lib_cache_;
};

template<typename ClassT>
int ObLCNodeFactory::create(lib::MemoryContext &mem_ctx,
                            ObILibCacheNode*& node,
                            ObPlanCache *lib_cache)
{
  int ret = OB_SUCCESS;
  char *ptr = NULL;
  if (OB_ISNULL(lib_cache)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else if (NULL == (ptr = (char *)mem_ctx->get_arena_allocator().alloc(sizeof(ClassT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to allocate memory for lib cache node", K(ret));
  } else {
    node = new(ptr)ClassT(lib_cache, mem_ctx);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_PLAN_CACHE_OB_LIB_CACHE_NODE_FACTORY_
