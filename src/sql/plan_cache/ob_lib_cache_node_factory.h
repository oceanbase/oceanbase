/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  ObLCNodeFactory() : lib_cache_(NULL), next_cache_node_id_(0) {}
  ObLCNodeFactory(ObPlanCache *lib_cache)
    : lib_cache_(lib_cache), next_cache_node_id_(0)
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
  uint64_t next_cache_node_id_;
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
