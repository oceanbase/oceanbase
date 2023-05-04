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

#ifndef OCEANBASE_CACHE_OB_KVCACHE_HANDLE_REF_CHECKER_H_
#define OCEANBASE_CACHE_OB_KVCACHE_HANDLE_REF_CHECKER_H_
#include "share/cache/ob_kvcache_struct.h"
namespace oceanbase
{
namespace common
{
class ObKVCacheHandle;
class ObKVCacheHandleRefChecker final
{
public:
  struct Key
  {
    Key();
    Key(const ObKVCacheHandle &cache_handle);
    ~Key() = default;
    uint64_t hash() const;
    int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; };
    OB_INLINE bool is_valid() const { return handle_ != nullptr; }
    bool operator== (const Key &other) const;
    TO_STRING_KV(KP_(handle));
    const ObKVCacheHandle *handle_;
  };
  struct Value
  {
    Value();
    Value(const Value &other);
    ~Value() = default;
    uint64_t hash() const;
    int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; };
    bool operator== (const Value &other) const;
    Value & operator= (const Value &other);
    TO_STRING_KV(K_(tenant_id), K_(cache_id), K_(bt));
    uint64_t tenant_id_;
    int64_t cache_id_;
    char bt_[512];
  };
  static const char ALL_CACHE_NAME[MAX_CACHE_NAME_LENGTH];
public:
  ObKVCacheHandleRefChecker();
  ~ObKVCacheHandleRefChecker();
  static ObKVCacheHandleRefChecker &get_instance();
  void reset();
  void handle_ref_inc(const ObKVCacheHandle &cache_handle);
  void handle_ref_de(const ObKVCacheHandle &cache_handle);
  int set_cache_id(const int64_t cache_id);
  int get_aggregate_bt_info(hash::ObHashMap<Value, int64_t> &bt_info);
private:
  static const int64_t HANDLE_BT_MAP_BUCKET_NUM = 10000;
  int64_t cache_id_;
  hash::ObHashMap<Key, Value> handle_ref_bt_info_;
  bool is_inited_;
};
}  // common
}  // oceanbase
#endif  // OCEANBASE_CACHE_OB_KVCACHE_HANDLE_REF_CHECKER_H_