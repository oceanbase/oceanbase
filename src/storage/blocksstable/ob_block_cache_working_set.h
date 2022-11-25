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

#ifndef OCEANBASE_BLOCKSSTABLE_OB_BLOCK_CACHE_WORKING_SET_H_
#define OCEANBASE_BLOCKSSTABLE_OB_BLOCK_CACHE_WORKING_SET_H_

#include "lib/lock/ob_spin_lock.h"
#include "ob_micro_block_cache.h"

namespace oceanbase
{
namespace blocksstable
{
class ObBlockCacheWorkingSet :
    public common::ObIKVCache<ObMicroBlockCacheKey, ObMicroBlockCacheValue>,
    public ObIMicroBlockCache
{
public:
  typedef ObMicroBlockCacheKey Key;
  typedef ObMicroBlockCacheValue Value;

  ObBlockCacheWorkingSet();
  virtual ~ObBlockCacheWorkingSet();

  int init(const uint64_t tenant_id);
  inline bool inited() const { return inited_; }
  void reset();

  virtual int add_put_size(const int64_t put_size);
  virtual int get_cache(BaseBlockCache *&cache) override;
  virtual int get_allocator(common::ObIAllocator *&allocator) override;
  virtual int put(const Key &key, const Value &value, bool overwrite = true);
  virtual int put_and_fetch(const Key &key, const Value &value, const Value *&pvalue,
      common::ObKVCacheHandle &handle, bool overwrite = true);
  virtual int get(const Key &key, const Value *&pvalue, common::ObKVCacheHandle &handle);
  virtual int erase(const Key &key);
  virtual int alloc(const uint64_t tenant_id, const int64_t key_size, const int64_t value_size,
      ObKVCachePair *&kvpair, ObKVCacheHandle &handle, ObKVCacheInstHandle &inst_handle) override;
private:
  int create_working_set_if_need();
  static const int64_t USE_WORKING_SET_THRESHOLD = 1024 * 1024 * 1024 * 1024LL; // disable working set
  bool inited_;
  uint64_t tenant_id_;
  ObDataMicroBlockCache *block_cache_;
  int64_t put_size_;
  bool use_working_set_;
  common::ObCacheWorkingSet<ObMicroBlockCacheKey, ObMicroBlockCacheValue> working_set_;
  common::ObSpinLock lock_;
};

}//end namespace blocksstable
}//end namespace oceanbase

#endif //OCEANBASE_BLOCKSSTABLE_OB_BLOCK_CACHE_WORKING_SET_H_