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
#include "sql/plan_cache/ob_lib_cache_register.h"
#include "sql/plan_cache/ob_lib_cache_node_factory.h"
#include "sql/plan_cache/ob_plan_cache.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace sql
{

int ObLCNodeFactory::create_cache_node(ObLibCacheNameSpace ns,
                                       ObILibCacheNode*& node,
                                       uint64_t tenant_id,
                                       MemoryContext &parent_context)
{
  int ret = OB_SUCCESS;
  lib::MemoryContext entity = NULL;
  ObMemAttr mem_attr;
  mem_attr.tenant_id_ = tenant_id;
  mem_attr.ctx_id_ = ObCtxIds::PLAN_CACHE_CTX_ID;
  if (OB_ISNULL(lib_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lib cache is null", K(ret));
  } else if (ns <= NS_INVALID || ns >= NS_MAX || OB_ISNULL(LC_CN_ALLOC[ns])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("out of the max type", K(ret), K(ns));
  } else if (FALSE_IT(mem_attr.label_ = LC_NS_TYPE_LABELS[ns])) {
  } else if (OB_FAIL(parent_context->CREATE_CONTEXT(entity,
                                                    lib::ContextParam().set_mem_attr(mem_attr)))) {
    LOG_WARN("create entity failed", K(ret), K(mem_attr));
  } else if (OB_ISNULL(entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL memory entity", K(ret));
  } else {
    WITH_CONTEXT(entity) {
      if (OB_FAIL(LC_CN_ALLOC[ns](entity, node, lib_cache_))) {
        LOG_WARN("failed to create lib cache node", K(ret), K(ns));
      }
    }
  }
  return ret;
}

void ObLCNodeFactory::destroy_cache_node(ObILibCacheNode* node)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(node) && OB_FAIL(node->before_cache_evicted())) {
    LOG_WARN("failed to process before_cache_evicted", K(ret));
  }
  // regardless of whether before_cache_evicted succeeds or fails, the cache node
  // will be evicted. so ignore the error code here
  ret = OB_SUCCESS;
  lib::MemoryContext entity = node->get_mem_context();
  WITH_CONTEXT(entity) { node->~ObILibCacheNode(); }
  node = NULL;
  DESTROY_CONTEXT(entity);
}


} // namespace sql
} // namespace oceanbase
