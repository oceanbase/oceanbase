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

namespace oceanbase {
namespace common {
class ObKVCacheIterator;
class ObKVCacheMap {
public:
  ObKVCacheMap();
  virtual ~ObKVCacheMap();
  int init(const int64_t bucket_num, ObKVCacheStore* store);
  void destroy();
  int erase_all();
  int erase_all(const int64_t cache_id);
  int erase_tenant(const uint64_t tenant_id);
  int erase_tenant_cache(const uint64_t tenant_id, const int64_t cache_id);
  int clean_garbage_node(int64_t& start_pos, const int64_t clean_num);
  int replace_fragment_node(int64_t& start_pos, const int64_t replace_num);
  int put(ObKVCacheInst& inst, const ObIKVCacheKey& key, const ObKVCachePair* kvpair, ObKVMemBlockHandle* mb_handle,
      bool overwrite = true);
  int get(const int64_t cache_id, const ObIKVCacheKey& key, const ObIKVCacheValue*& pvalue,
      ObKVMemBlockHandle*& out_handle);
  int erase(ObKVCacheInst& inst, const ObIKVCacheKey& key);

private:
  friend class ObKVCacheIterator;
  struct Node {
    ObKVCacheInst* inst_;
    uint64_t hash_code_;
    int32_t seq_num_;
    ObKVMemBlockHandle* mb_handle_;
    const ObIKVCacheKey* key_;
    const ObIKVCacheValue* value_;
    Node* next_;
    int64_t get_cnt_;
    Node()
        : inst_(NULL), hash_code_(0), seq_num_(0), mb_handle_(NULL), key_(NULL), value_(NULL), next_(NULL), get_cnt_(0)
    {}
  };
  struct Bucket {
    static const int64_t BUCKET_SIZE = 1024L * 1024L * 16;
    Node* nodes_[BUCKET_SIZE];
  };

private:
  int multi_get(const int64_t cache_id, const int64_t pos, common::ObList<Node, common::ObArenaAllocator>& list);
  void internal_map_erase(Node*& prev, Node*& iter, const uint64_t bucket_pos);
  void internal_map_replace(Node*& prev, Node*& iter, const uint64_t bucket_pos);
  void internal_data_move(Node* iter, const enum ObKVCachePolicy policy);
  OB_INLINE bool need_modify_cache(const int64_t iter_get_cnt, const int64_t total_get_cnt, const int64_t kv_cnt) const
  {
    float avg_get_cnt = (float)(total_get_cnt) / (float)(kv_cnt);
    float re_sqrt_avg_cnt = avg_get_cnt;
    int int_avg_cnt = *reinterpret_cast<int*>(&re_sqrt_avg_cnt);  // store floating-point bits in integer
    int_avg_cnt = 0x5f3759df - (int_avg_cnt >> 1);                // initial guess for Newton's method
    re_sqrt_avg_cnt = *reinterpret_cast<float*>(&int_avg_cnt);    // convert new bits into float
    // old version confidence interval should compute from statndard error from standard derivation
    // double threshold = avg_get_cnt + 1.96 * sqrt(avg_get_cnt);
    float threshold = avg_get_cnt + 1.96f / re_sqrt_avg_cnt;
    return iter_get_cnt > threshold;
  }
  Node*& get_bucket_node(const int64_t idx);

private:
  bool is_inited_;
  ObMalloc bucket_allocator_;
  int64_t bucket_num_;
  Bucket** buckets_;
  ObBucketLock bucket_lock_;
  ObKVCacheStore* store_;
};

}  // end namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_CACHE_OB_KVCACHE_MAP_H_
