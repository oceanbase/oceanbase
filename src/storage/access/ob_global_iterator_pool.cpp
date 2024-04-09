/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_global_iterator_pool.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace storage
{

CachedIteratorNode::CachedIteratorNode()
  : is_occupied_(false),
    is_exception_occur_(false),
    iter_(nullptr),
    stmt_iter_pool_(nullptr),
    iter_allocator_("GlobalIter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
}

CachedIteratorNode::~CachedIteratorNode()
{
  destroy();
}

void CachedIteratorNode::reclaim(const bool force_release)
{
  if (OB_NOT_NULL(iter_)) {
    if (is_exception_occur_ ||
        force_release ||
        iter_allocator_.total() > ObGlobalIteratorPool::ITER_POOL_ITER_MEM_LIMIT) {
      LOG_TRACE("[Global Iterator Pool] should release this iter", K(is_exception_occur_),
                K(force_release), K(iter_allocator_.total()), KP(iter_));
      destroy();
    } else {
      stmt_iter_pool_->reclaim();
      iter_->reclaim();
    }
  }
  ATOMIC_STORE(&is_occupied_, false);
}

void CachedIteratorNode::destroy()
{
  if (OB_NOT_NULL(iter_)) {
    iter_->~ObQueryRowIterator();
    iter_ = nullptr;
  }
  if (OB_NOT_NULL(stmt_iter_pool_)) {
    stmt_iter_pool_->~ObStoreRowIterPool<ObStoreRowIterator>();
    iter_allocator_.free(stmt_iter_pool_);
    stmt_iter_pool_ = nullptr;
  }
  iter_allocator_.reset();
  is_exception_occur_ = false;
}

int CachedIteratorNode::alloc_stmt_iter_pool()
{
  int ret = OB_SUCCESS;
  if (nullptr == stmt_iter_pool_) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = iter_allocator_.alloc(sizeof(ObStoreRowIterPool<ObStoreRowIterator>)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[Global Iterator Pool] Failed to alloc row iter pool", K(ret));
    } else {
      stmt_iter_pool_ = new(buf) ObStoreRowIterPool<ObStoreRowIterator>(iter_allocator_);
    }
  }
  return ret;
}

ObGlobalIteratorPool::ObGlobalIteratorPool()
  : is_inited_(false),
    is_washing_(false),
    is_disabled_(false),
    get_cnt_(0),
    tenant_id_(MTL_ID()),
    bucket_cnt_(0),
    tenant_mem_user_limit_(0),
    tenant_mem_user_hold_(0),
    allocator_(ObModIds::OB_TABLE_SCAN_ITER, OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_)
{
  MEMSET(cached_node_array_, 0, sizeof(cached_node_array_));
}

ObGlobalIteratorPool::~ObGlobalIteratorPool()
{
  destroy();
}

int ObGlobalIteratorPool::mtl_init(ObGlobalIteratorPool *&pool)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[Global Iterator Pool] Invalid argument", K(ret), KP(pool));
  } else if (!pool->check_need_iterator_pool()) {
    // do not need use pool
  } else if (OB_FAIL(pool->init())) {
    LOG_WARN("[Global Iterator Pool] Failed to init ObGlobalIteratorPool", K(ret));
  } else {
    LOG_INFO("[Global Iterator Pool] mtl init ObGlobalIteratorPool", K(ret), KPC(pool));
  }
  return ret;
}

void ObGlobalIteratorPool::mtl_destroy(ObGlobalIteratorPool *&pool)
{
  if (OB_NOT_NULL(pool)) {
    pool->destroy();
    common::ob_delete(pool);
  }
}

int ObGlobalIteratorPool::init()
{
  int ret = OB_SUCCESS;
  if(IS_INIT) {
    ret = OB_INIT_TWICE;
  } else {
    bucket_cnt_ = calc_bucket_cnt();
    void *buf = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i <= ITER_POOL_MAX_CACHED_ITER_TYPE; ++i) {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(CachedIteratorNode) * bucket_cnt_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("[Global Iterator Pool] Failed to alloc memory", K(ret));
      } else if (FALSE_IT(cached_node_array_[i] = reinterpret_cast<CachedIteratorNode*>(buf))) {
      } else {
        new (buf) CachedIteratorNode[bucket_cnt_];
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    } else {
      destroy();
    }
  }
  return ret;
}

void ObGlobalIteratorPool::destroy()
{
  for (int64_t i = 0; i <= ITER_POOL_MAX_CACHED_ITER_TYPE; ++i) {
    if (OB_NOT_NULL(cached_node_array_[i])) {
      for (int64_t j = 0; j < bucket_cnt_; ++j) {
        cached_node_array_[i][j].destroy();
      }
    }
    cached_node_array_[i] = nullptr;
  }
  is_inited_ = false;
  is_washing_ = false;
  is_disabled_ = false;
  get_cnt_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  bucket_cnt_ = 0;
  tenant_mem_user_limit_ = 0;
  tenant_mem_user_hold_ = 0;
  allocator_.reset();
}

int ObGlobalIteratorPool::get(const ObQRIterType type, CachedIteratorNode *&cache_node)
{
  int ret = OB_SUCCESS;
  cache_node = nullptr;
  if (is_disabled()) {
  } else if (OB_FAIL(inner_get(type, cache_node))) {
    STORAGE_LOG(WARN, "[Global Iterator Pool] Failed to inner get", K(ret));
  } else if (nullptr != cache_node) {
    if (OB_FAIL(cache_node->alloc_stmt_iter_pool())) {
      LOG_WARN("[Global Iterator Pool] Failed to alloc stmt iter pool", K(ret));
    }
  }
  return ret;
}

void ObGlobalIteratorPool::release(CachedIteratorNode *&cache_node)
{
  if (nullptr != cache_node) {
    cache_node->reclaim(is_disabled());
    cache_node = nullptr;
  }
}

int ObGlobalIteratorPool::inner_get(const ObQRIterType type, CachedIteratorNode *&cache_node)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(tenant_id_ != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[Global Iterator Pool] Unexpected tenant id", K(ret), K(tenant_id_), K(MTL_ID()));
  } else if (OB_UNLIKELY(type > ITER_POOL_MAX_CACHED_ITER_TYPE || type <= T_INVALID_ITER_TYPE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[Global Iterator Pool] Invalid argument", K(ret), K(type));
  } else {
    int64_t tid = GETTID();
    CachedIteratorNode *cache_nodes = cached_node_array_[type];
    const int64_t bucket_idx = tid % bucket_cnt_;
    CachedIteratorNode *node_ptr = &cache_nodes[bucket_idx];
    bool is_occupied = ATOMIC_LOAD(&node_ptr->is_occupied_);
    if (is_occupied) {
    } else if (is_occupied == ATOMIC_VCAS(&node_ptr->is_occupied_, is_occupied, true)) {
      node_ptr->tid_ = tid;
      cache_node = node_ptr;
    }
  }
  if (0 == (++get_cnt_ % 100000)) {
    wash();
  }
  return ret;
}

bool ObGlobalIteratorPool::is_washing() const
{
  return ATOMIC_LOAD(&is_washing_);
}

bool ObGlobalIteratorPool::is_disabled() const
{
  return ATOMIC_LOAD(&is_disabled_);
}

void ObGlobalIteratorPool::wash()
{
  if (IS_NOT_INIT) {
  } else if (is_washing()) {
  } else if (false == ATOMIC_VCAS(&is_washing_, false, true)) {
    tenant_mem_user_limit_ = lib::get_tenant_memory_limit(tenant_id_);
    tenant_mem_user_hold_ = lib::get_tenant_memory_hold(tenant_id_);
    const bool need_wash = tenant_mem_user_hold_ * 100 > tenant_mem_user_limit_ * ITER_POOL_WASH_HIGH_THRESHOLD;
    const bool disabled = is_disabled();
    if (!need_wash) {
      if (disabled &&
          calc_bucket_cnt() >= bucket_cnt_ &&
          tenant_mem_user_hold_ * 100 < tenant_mem_user_limit_ * ITER_POOL_WASH_LOW_THRESHOLD) {
        // current holding memory is low, enable iter pool
        ATOMIC_STORE(&is_disabled_, false);
      }
    } else if (!disabled) {
      ATOMIC_STORE(&is_disabled_, true);
      for (int64_t type = 0; type <= ITER_POOL_MAX_CACHED_ITER_TYPE; ++type) {
        CachedIteratorNode *cache_nodes = cached_node_array_[type];
        for (int64_t i = 0; i < bucket_cnt_; ++i) {
          CachedIteratorNode *node_ptr = &cache_nodes[i];
          bool is_occupied = ATOMIC_LOAD(&node_ptr->is_occupied_);
          if (is_occupied) {
          } else if (is_occupied == ATOMIC_VCAS(&node_ptr->is_occupied_, is_occupied, true)) {
            node_ptr->reclaim(true);
          }
        }
      }
      tenant_mem_user_hold_ = lib::get_tenant_memory_hold(tenant_id_);
      const bool can_enable = calc_bucket_cnt() >= bucket_cnt_ && tenant_mem_user_hold_ * 100 < tenant_mem_user_limit_ * ITER_POOL_WASH_LOW_THRESHOLD;
      if (can_enable) {
        ATOMIC_STORE(&is_disabled_, false);
      } else {
         // current holding memory is still high or scaling down happened
         // 1. keep iter pool disabled, enable it later
         // 2. the following returned iters would be released directly until the iter pool is enabled
      }
    }
    ATOMIC_STORE(&is_washing_, false);
  }
}

}
}