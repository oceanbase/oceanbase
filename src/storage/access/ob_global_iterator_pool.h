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

#ifndef OCEANBASE_STORAGE_OB_GLOBAL_ITERATOR_POOL_H_
#define OCEANBASE_STORAGE_OB_GLOBAL_ITERATOR_POOL_H_
#include <stdint.h>
#include "ob_store_row_iterator.h"

namespace oceanbase
{
namespace storage
{

class CachedIteratorNode final
{
public:
  CachedIteratorNode();
  ~CachedIteratorNode();
  void reclaim(const bool force_release = false);
  void destroy();
  OB_INLINE void set_iter(ObQueryRowIterator *iter)
  {
    iter_ = iter;
  }
  OB_INLINE ObQueryRowIterator *get_iter()
  {
    return iter_;
  }
  OB_INLINE ObQRIterType get_iter_type()
  {
    return nullptr == iter_ ? T_INVALID_ITER_TYPE : iter_->get_type();
  }
  OB_INLINE ObStoreRowIterPool<ObStoreRowIterator> *get_stmt_iter_pool()
  {
    return stmt_iter_pool_;
  }
  OB_INLINE common::ObArenaAllocator *get_iter_allocator()
  {
    return &iter_allocator_;
  }
  OB_INLINE void set_exception_occur(const bool exception_occur)
  {
    is_exception_occur_ = exception_occur;
  }
  TO_STRING_KV(K_(is_occupied), K_(tid), KP_(iter));
private:
  int alloc_stmt_iter_pool();
  bool is_occupied_;
  bool is_exception_occur_;
  int64_t tid_;
  ObQueryRowIterator *iter_;
  ObStoreRowIterPool<ObStoreRowIterator> *stmt_iter_pool_;
  common::ObArenaAllocator iter_allocator_;
  friend class ObGlobalIteratorPool;
};

class ObGlobalIteratorPool final
{
public:
  ObGlobalIteratorPool();
  ~ObGlobalIteratorPool();
  static int mtl_init(ObGlobalIteratorPool *&pool);
  static void mtl_destroy(ObGlobalIteratorPool *&poll);
  int get(const ObQRIterType type, CachedIteratorNode *&cache_node);
  void release(CachedIteratorNode *&cache_node);
  void wash();
  OB_INLINE bool can_use_iter_pool(const int64_t table_cnt, const int64_t col_cnt, const ObQRIterType iter_type) const
  {
    return is_valid() &&
           table_cnt <= ITER_POOL_MAX_TABLE_CNT_LIMIT &&
           col_cnt + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt() <= ITER_POOL_MAX_COL_CNT_LIMIT &&
           iter_type <= ITER_POOL_MAX_CACHED_ITER_TYPE;
  }
  OB_INLINE bool is_valid() const
  {
    return is_inited_;
  }
  static int64_t get_max_col_count() { return ITER_POOL_MAX_COL_CNT_LIMIT; }
  static const int64_t ITER_POOL_ITER_MEM_LIMIT = 256L << 10; // 256K
  TO_STRING_KV(K_(is_inited), K_(is_washing), K_(is_disabled), K_(get_cnt), K_(tenant_id), K_(bucket_cnt),
               K_(tenant_mem_user_limit), K_(tenant_mem_user_hold), KP_(cached_node_array));
private:
  OB_INLINE bool check_need_iterator_pool()
  {
    bool need = is_not_virtual_tenant_id(tenant_id_) &&
                !is_meta_tenant(tenant_id_);
    if (need) {
      tenant_mem_user_limit_ = lib::get_tenant_memory_limit(tenant_id_);
      tenant_mem_user_hold_ = lib::get_tenant_memory_hold(tenant_id_);
      need = tenant_mem_user_limit_ > ITER_POOL_TENANT_MIN_MEM_THRESHOLD;
    }
    return need;
  }
  OB_INLINE int64_t calc_bucket_cnt() const
  {
    const int64_t mem_limit = tenant_mem_user_limit_ * ITER_POOL_MAX_MEM_PERCENT;
    int64_t bucket_cnt = mem_limit / (ITER_POOL_ITER_MEM_LIMIT * (1 + ITER_POOL_MAX_CACHED_ITER_TYPE));
    if (bucket_cnt < 2) {
      bucket_cnt = 2;
    } else if (bucket_cnt > ITER_POOL_MAX_BUCKET_CNT) {
      bucket_cnt = ITER_POOL_MAX_BUCKET_CNT;
    }
    return bucket_cnt;
  }
  int init();
  void destroy();
  int inner_get(const ObQRIterType type, CachedIteratorNode *&cache_node);
  bool is_washing() const;
  bool is_disabled() const;
  static const int64_t ITER_POOL_TENANT_MIN_MEM_THRESHOLD = 8L << 30; // 8G
  static constexpr double ITER_POOL_MAX_MEM_PERCENT = 0.002;
  static const int64_t ITER_POOL_MAX_TABLE_CNT_LIMIT = 6;
  static const int64_t ITER_POOL_MAX_COL_CNT_LIMIT = 32;
  static const int64_t ITER_POOL_MAX_BUCKET_CNT = 257;
  static const int64_t ITER_POOL_WASH_HIGH_THRESHOLD = 85;
  static const int64_t ITER_POOL_WASH_LOW_THRESHOLD = 75;
  static const int64_t ITER_POOL_MAX_CACHED_ITER_TYPE = T_SINGLE_SCAN;
  static_assert(ITER_POOL_MAX_CACHED_ITER_TYPE > T_INVALID_ITER_TYPE && ITER_POOL_MAX_CACHED_ITER_TYPE < T_MAX_ITER_TYPE,
                "[Global Iterator Pool] invalid iter type");
  bool is_inited_;
  bool is_washing_;
  bool is_disabled_;
  uint64_t get_cnt_;
  uint64_t tenant_id_;
  int64_t bucket_cnt_;
  int64_t tenant_mem_user_limit_;
  int64_t tenant_mem_user_hold_;
  CachedIteratorNode *cached_node_array_[ITER_POOL_MAX_CACHED_ITER_TYPE + 1];
  ObArenaAllocator allocator_;
};

class ObGlobalIteratorGuard final
{
public:
  ObGlobalIteratorGuard(int &ret, CachedIteratorNode *&node)
    : ret_(ret), node_(node)
  {}
  ~ObGlobalIteratorGuard()
  {
    if (nullptr != node_) {
      node_->set_exception_occur(OB_SUCCESS != ret_ && OB_ITER_END != ret_);
    }
  }
private:
  int &ret_;
  CachedIteratorNode *&node_;
};

#define ACTIVE_GLOBAL_ITERATOR_GUARD(RET, CACHED_NODE) ObGlobalIteratorGuard global_iter_guard(RET, CACHED_NODE)

}
}

#endif