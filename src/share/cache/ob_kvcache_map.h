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

#ifndef OCEANBASE_CACHE_OB_KVCACHE_MAP_H_
#define OCEANBASE_CACHE_OB_KVCACHE_MAP_H_

#include "lib/allocator/ob_malloc.h"
#include "lib/lock/ob_bucket_lock.h"
#include "share/cache/ob_kvcache_struct.h"
#include "share/cache/ob_kvcache_store.h"
#include "share/cache/ob_kvcache_hazard_version.h"

namespace oceanbase
{
namespace common
{
class ObKVCacheIterator;
class ObKVCacheMap
{
  static constexpr int64_t DEFAULT_BUCKET_SIZE = (16L << 20); // 16M
  static constexpr int64_t MIN_BUCKET_SIZE     = ( 4L << 10); //  4K
  static constexpr int64_t BUCKET_SIZE_ARRAY_LEN = 4;
  static constexpr int64_t BUCKET_SIZE_ARRAY[BUCKET_SIZE_ARRAY_LEN] = {MIN_BUCKET_SIZE, MIN_BUCKET_SIZE << 4,  MIN_BUCKET_SIZE << 8, DEFAULT_BUCKET_SIZE};
  static const int64_t HAZARD_VERSION_THREAD_WAITING_THRESHOLD = 512;
  static const int64_t DEFAULT_LFU_THRESHOLD_BASE = 2;
public:
  ObKVCacheMap();
  virtual ~ObKVCacheMap();
  int init(const int64_t bucket_num, ObKVCacheStore *store);
  void destroy();
  int erase_all();
  int erase_all(const int64_t cache_id);
  int erase_tenant(const uint64_t tenant_id, const bool force_erase = false);
  int erase_tenant_cache(const uint64_t tenant_id, const int64_t cache_id);
  int clean_garbage_node(int64_t &start_pos, const int64_t clean_num);
  int replace_fragment_node(int64_t &start_pos, int64_t &replace_node_count, const int64_t replace_num);
  int put(
    ObKVCacheInst &inst,
    const ObIKVCacheKey &key,
    const ObKVCachePair *kvpair,
    ObKVMemBlockHandle *mb_handle,
    bool overwrite = true);
  int get(
    const int64_t cache_id,
    const ObIKVCacheKey &key,
    const ObIKVCacheValue *&pvalue,
    ObKVMemBlockHandle *&out_handle);
  int erase(const int64_t cache_id, const ObIKVCacheKey &key);
  void print_hazard_version_info();
private:
  friend class ObKVCacheIterator;
  struct Node : public KVCacheHazardNode
  {
    ObKVCacheInst *inst_;
    uint64_t hash_code_;
    int32_t seq_num_;
    ObKVMemBlockHandle *mb_handle_;
    const ObIKVCacheKey *key_;
    const ObIKVCacheValue *value_;
    Node *next_;
    int64_t get_cnt_;
    Node()
      : inst_(NULL),
        hash_code_(0),
        seq_num_(0),
        mb_handle_(NULL),
        key_(NULL),
        value_(NULL),
        next_(NULL),
        get_cnt_(0)
    {}
    virtual ~Node() {};
    virtual void retire() override;  // only free memory of itself
    INHERIT_TO_STRING_KV("Node", KVCacheHazardNode, KPC_(inst), K_(hash_code), K_(seq_num), KP_(mb_handle), KP_(key),
                         KP_(value), KP_(next), K_(get_cnt));
  };
  struct Bucket
  {
    Node **nodes_;
  };
private:
  int multi_get(const int64_t cache_id, const int64_t pos, common::ObList<Node, common::ObArenaAllocator> &list);
  void internal_map_erase(Node *&prev, Node *&iter, Node *&bucket_ptr);
  void internal_map_replace(Node *&prev, Node *&iter, Node *&bucket_ptr);
  int internal_data_move(Node *&prev, Node *&iter, Node *&bucket_ptr);
  OB_INLINE bool need_modify_cache(const int64_t iter_get_cnt, const int64_t total_get_cnt, const int64_t kv_cnt) const
  {
    bool ret = false;
    int64_t threshold = total_get_cnt / (kv_cnt + 1) + DEFAULT_LFU_THRESHOLD_BASE;
    ret = iter_get_cnt > threshold;
    return ret;
  }
  Node *&get_bucket_node(const int64_t idx)
  {
    const int64_t bucket_idx = idx / bucket_size_;
    return buckets_[bucket_idx].nodes_[idx & (bucket_size_ - 1)];
  }
private:

  bool is_inited_;
  ObMalloc bucket_allocator_;
  int64_t bucket_num_;
  int64_t bucket_size_;
  Bucket *buckets_;
  ObBucketLock bucket_lock_;
  ObKVCacheStore *store_;
  GlobalHazardVersion global_hazard_version_;
};

}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_CACHE_OB_KVCACHE_MAP_H_
