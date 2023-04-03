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

#ifndef _OB_SHARE_SEQ_SEQUENCE_CACHE_H_
#define _OB_SHARE_SEQ_SEQUENCE_CACHE_H_

#include "lib/utility/ob_macro_utils.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/number/ob_number_v2.h"
#include "share/sequence/ob_sequence_dml_proxy.h"
#include "share/sequence/ob_sequence_option.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
}
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObSequenceSchema;
class ObMultiVersionSchemaService;
}

struct SequenceCacheNode
{
  SequenceCacheNode()
      : start_(), end_()
  {}

  void reset()
  {
  }

  TO_STRING_KV(K_(start),
               K_(end));

  int set_start(const common::number::ObNumber &start)
  {
    return start_.set(start);
  }
  int set_end(const common::number::ObNumber &end)
  {
    return end_.set(end);
  }
  const common::number::ObNumber &start() const { return start_.val(); }
  const common::number::ObNumber &end() const { return end_.val(); }
private:
  ObSequenceValue start_;
  ObSequenceValue end_;
};

// a wrapper class, adaptor for ObLinkHashMap
struct CacheItemKey
{
public:
  CacheItemKey() : tenant_id_(0), key_(0) {}
  CacheItemKey(const uint64_t tenant_id, const uint64_t key) : tenant_id_(tenant_id), key_(key) {}
  ~CacheItemKey() = default;
  bool operator==(const CacheItemKey &other) const
  {
    return tenant_id_ == other.tenant_id_ && other.key_ == key_;
  }

  int compare(const CacheItemKey &other) {
    int ret = tenant_id_ < other.tenant_id_ ? -1 : (tenant_id_ > other.tenant_id_) ? 1 : 0;
    if (0 == ret) {
      ret = key_ < other.key_ ? -1 : (key_ > other.key_) ? 1 : 0;
    }
    return ret;
  }

  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&key_, sizeof(key_), hash_val);
    hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    return hash_val;
  }
  TO_STRING_KV(K_(tenant_id), K_(key));
  uint64_t tenant_id_;
  uint64_t key_;
};

struct ObSequenceCacheItem : public common::LinkHashValue<CacheItemKey>
{
public:
  ObSequenceCacheItem()
      : prefetching_(false),
        with_prefetch_node_(false),
        base_on_last_number_(false),
        last_refresh_ts_(0),
        alloc_mutex_(common::ObLatchIds::SEQUENCE_VALUE_ALLOC_LOCK),
        last_number_()
  {}
  int combine_prefetch_node()
  {
    int ret = common::OB_SUCCESS;
    if (OB_LIKELY(with_prefetch_node_)) {
      if (curr_node_.end() != prefetch_node_.start()) {
        // 两个 node 的数据不连续，则使用 prefetch 数据，丢弃 curr_node 残余数据
        ret = curr_node_.set_start(prefetch_node_.start());
      }
      curr_node_.set_end(prefetch_node_.end());
      // 通知预取逻辑，prefetch node 可以接受新的预取数据
      with_prefetch_node_ = false;
    }
    return ret;
  }
  int set_last_number(const common::number::ObNumber &num)
  {
    return last_number_.set(num);
  }
  common::number::ObNumber &last_number() { return last_number_.val(); }
  const common::number::ObNumber &last_number() const { return last_number_.val(); }
public:
  SequenceCacheNode curr_node_;
  SequenceCacheNode prefetch_node_;
  // 标记当前 item 是否正在做 prefetching 操作，避免并发做 prefetch query
  bool prefetching_;
  // 标记 prefetch_node 是否被填充了值
  bool with_prefetch_node_;
  // 如果 next-value 的计算流程为：先读取 last_number，然后加上 increment by
  // 但对于首次取值，不加 increment by。所以使用 base_on_last_number 加以标记
  // base_on_last_number_ = false 时表示首次取值
  bool base_on_last_number_;
  // 记录上次取得的值，用于 cycle 模式下判断下次取值是否需要加上 increment_by
  int64_t last_refresh_ts_;
  lib::ObMutex alloc_mutex_;
private:
  ObSequenceValue last_number_;
public:
  TO_STRING_KV(K_(curr_node),
               K_(prefetch_node),
               K_(prefetching),
               K_(with_prefetch_node),
               K_(last_refresh_ts),
               K_(last_number),
               K_(base_on_last_number));
};

class ObSequenceCache
{
public:
  // map sequence_id => sequence cache
  typedef common::ObLinkHashMap<CacheItemKey, ObSequenceCacheItem> NodeMap;
public:
  ObSequenceCache();
  virtual ~ObSequenceCache() = default;
  static ObSequenceCache &get_instance();

  int init(share::schema::ObMultiVersionSchemaService &schema_service,
            common::ObMySQLProxy &sql_proxy);
  int nextval(const share::schema::ObSequenceSchema &schema,
              common::ObIAllocator &allocator, // 用于各种临时计算
              ObSequenceValue &nextval);
  int remove(uint64_t tenant_id, uint64_t sequence_id);
private:
  /* functions */
  int get_item(CacheItemKey &key, ObSequenceCacheItem *&item);

  int del_item(CacheItemKey &key);

  int prefetch_sequence_cache(const schema::ObSequenceSchema &schema,
                              ObSequenceCacheItem &cache);
  int find_sequence_cache(const schema::ObSequenceSchema &schema,
                          ObSequenceCacheItem &cache);
  int move_next(const schema::ObSequenceSchema &schema,
                ObSequenceCacheItem &cache,
                common::ObIAllocator &allocator,
                ObSequenceValue &nextval);
  int need_refill_cache(const schema::ObSequenceSchema &schema,
                        ObSequenceCacheItem &cache,
                        common::ObIAllocator &allocator,
                        bool &need_refill);
  int refill_sequence_cache(const schema::ObSequenceSchema &schema,
                            common::ObIAllocator &allocator,
                            ObSequenceCacheItem &cache);
  /* variables */
  ObSequenceDMLProxy dml_proxy_;
  bool inited_;
  lib::ObMutex cache_mutex_;
  NodeMap sequence_cache_;
  DISALLOW_COPY_AND_ASSIGN(ObSequenceCache);
};
}
}
#endif /* _OB_SHARE_SEQ_SEQUENCE_CACHE_H_ */
//// end of header file


