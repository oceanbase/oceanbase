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
#ifndef _OB_TABLE_THROTTLE_H
#define _OB_TABLE_THROTTLE_H

#include "ob_table_throttle.h"
#include "ob_table_hotkey_kvcache.h"
#include "ob_table_hotkey.h"

namespace oceanbase
{
namespace table
{

struct ObTableThrottleKey
{
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t partition_id_;
  ObString key_;

  ObTableThrottleKey(): tenant_id_(0), table_id_(0), key_() {};
  bool operator == (const ObTableThrottleKey &rhs) const
  {
    return (this->tenant_id_ == rhs.tenant_id_
            && this->table_id_ == rhs.table_id_
            && this->key_ == rhs.key_);
  }
  uint64_t hash() const
  {
    uint64_t hash_val = key_.hash();
    hash_val = murmurhash(&tenant_id_, sizeof(uint64_t), hash_val);
    hash_val = murmurhash(&table_id_, sizeof(uint64_t), hash_val);
    return hash_val;
  }
  int64_t size() const { return sizeof(*this) + key_.size(); }

  /**
    @param [in]   allocator allocator for malloc
    @param [out]  key copy to
  */
  int deep_copy(ObFIFOAllocator &allocator, ObTableThrottleKey *&key) const
  {
    int ret = OB_SUCCESS;
    ObTableThrottleKey *new_key;
    if (nullptr == (new_key = static_cast<ObTableThrottleKey*>(allocator.alloc(size())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to allocate task", K(ret), K(size()));
    } else {
      new_key = new(new_key) ObTableThrottleKey;

      new_key->tenant_id_ = this->tenant_id_;
      new_key->table_id_ = this->table_id_;
      new_key->partition_id_ = this->partition_id_;
      // deep copy key_
      memcpy((char*)new_key + sizeof(ObTableThrottleKey), key_.ptr(), key_.length());
      new_key->key_ = ObString(key_.size(), key_.length(), (char*)new_key + sizeof(ObTableThrottleKey));

      key = new_key;
    }
    return ret;
  }
  TO_STRING_KV(K(tenant_id_), K(table_id_), K(partition_id_), K(key_));
};

typedef hash::ObHashMap<ObTableThrottleKey*, int64_t> ThrottleKeyHashMap;

class ThrottleKeyHashMapCallBack
{
public:
  ThrottleKeyHashMapCallBack(int64_t value) : v_(value) {};
  void operator()(common::hash::HashMapPair<ObTableThrottleKey *, int64_t>& v)
  {
    v.second = v_;
  };
private:
  int64_t v_;
};

class ObTableHotKeyThrottle
{
public:
  static const int64_t THROTTLE_PERCENTAGE_UNIT = 10;
  static const int64_t THROTTLE_BUCKET_NUM = 256;
  static constexpr double THROTTLE_MAINTAIN_PERCENTAGE = 0.7;
  // only KV_HOTKEY_STAT_RATIO of input will be recorded
  // so the recorded number should be multi by 1 / KV_HOTKEY_STAT_RATIO (or decrease the threshold)
  // should be the same as ObTableHotKeyMgr::KV_HOTKEY_STAT_RATIO
  static constexpr double KV_HOTKEY_STAT_RATIO = 0.2;

  int init();
  void destroy();
  void clear();
  ThrottleKeyHashMap &get_throttle_map() { return throttle_keys_; }
  static ObTableHotKeyThrottle &get_instance();
  // create_throttle_hotkey will reuse memory of hotkey rowstr NOT DEEP COPY
  int create_throttle_hotkey(const ObTableHotKey &hotkey, ObTableThrottleKey &throttle_hotkey);
  // try_add_throttle_key will deep copy hotkey if not exist in map
  int try_add_throttle_key(ObTableThrottleKey &hotkey);
  void set_rowkey_collection(ObRowkey &rowkey);
  int check_need_reject_common(const ObTableThrottleKey &key);
  int check_need_reject_req(ObArenaAllocator &allocator,
                            uint64_t tenant_id,
                            uint64_t table_id,
                            const ObRowkey &rowkey,
                            ObTableEntityType entity_type);
  int is_same_key(const ObTableBatchOperation &batch_operation, ObTableEntityType entity_type, bool &result);
  int check_need_reject_batch_req(ObArenaAllocator &allocator,
                                  const ObTableBatchOperation &batch_operation,
                                  uint64_t tenant_id,
                                  uint64_t table_id,
                                  ObTableEntityType entity_type);
  int check_need_reject_query(ObArenaAllocator &allocator,
                              uint64_t tenant_id,
                              uint64_t table_id,
                              const ObTableQuery &query,
                              ObTableEntityType entity_type);
  int refresh(uint64_t epoch);

private:
  ObTableHotKeyThrottle();
  virtual ~ObTableHotKeyThrottle() {}

  ThrottleKeyHashMap throttle_keys_;
  ObFIFOAllocator allocator_;
  ObRandom random_;

  // status of ObTableHotKeyThrottle
  static int64_t inited_;
  static ObTableHotKeyThrottle *instance_;
};

} /* namespace table */
} /* namespace oceanbase */

#endif /* _OB_TABLE_THROTTLE_H */