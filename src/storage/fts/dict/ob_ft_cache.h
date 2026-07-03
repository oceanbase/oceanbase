/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_CACHE_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_CACHE_H_

#include "lib/alloc/alloc_assist.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/cache/ob_kvcache_struct.h"
#include "storage/fts/dict/ob_ft_dat_dict.h"
#include "storage/fts/dict/ob_ft_dict_def.h"

#include <cstdint>

namespace oceanbase
{
namespace storage
{
class ObDictCacheKey : public common::ObIKVCacheKey
{
public:
  ObDictCacheKey() = default;
  ObDictCacheKey(const uint64_t table_id,
                 const uint64_t tenant_id,
                 int32_t range_id)
      : table_id_(table_id),
        tenant_id_(tenant_id),
        range_id_(range_id)
  {
  }
  // Copy constructor for deep_copy
  ObDictCacheKey(const ObDictCacheKey &other)
      : table_id_(other.table_id_),
        tenant_id_(other.tenant_id_),
        range_id_(other.range_id_)
  {
  }
  ~ObDictCacheKey() override {}

  bool operator==(const ObIKVCacheKey &other) const override
  {
    const ObDictCacheKey &other_key = reinterpret_cast<const ObDictCacheKey &>(other);
    return (&other == this)
           || ((other_key.tenant_id_ == tenant_id_)
               && (other_key.table_id_ == table_id_)
               && (other_key.range_id_ == range_id_));
  }

  uint64_t hash() const override
  {
    uint64_t hash_val = 0;
    hash_val = murmurhash(&table_id_, sizeof(table_id_), hash_val);
    hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    hash_val = murmurhash(&range_id_, sizeof(range_id_), hash_val);
    return hash_val;
  }

  int equal(const ObIKVCacheKey &other, bool &equal) const override
  {
    equal = *this == other;
    return OB_SUCCESS;
  }
  int hash(uint64_t &hash_value) const override
  {
    hash_value = hash();
    return OB_SUCCESS;
  }
  uint64_t get_tenant_id() const override { return tenant_id_; }
  int32_t get_range_id() const { return range_id_; }
  int64_t size() const override { return sizeof(ObDictCacheKey); }

  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override
  {
    int ret = OB_SUCCESS;
    const int64_t key_size = size();
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < key_size)) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid argument for ob dict cache", K(ret), K(buf_len), K(key_size));
    } else {
      ObDictCacheKey *new_key = new (buf) ObDictCacheKey(*this);
      key = new_key;
    }
    return ret;
  }

  TO_STRING_KV(K_(table_id), K_(tenant_id), K_(range_id));
private:
  uint64_t table_id_;
  uint64_t tenant_id_;
  int32_t range_id_;
};

class ObDictCacheValue : public common::ObIKVCacheValue
{
public:
  // range_id = 0: range_count_ stores the total number of ranges
  // range_id > 0: range_count_ is not used
  // snapshot_version: snapshot version used when building this cache entry
  // start_word and end_word are stored in dat_block_->start_word_ and dat_block_->end_word_
  ObDictCacheValue(const ObFTDAT *dat_block,
                   const int64_t snapshot_version,
                   const int32_t range_count)
      : dat_block_(dat_block),
        snapshot_version_(snapshot_version),
        range_count_(range_count) {}

  ~ObDictCacheValue() override {}

  int64_t size() const override
  {
    return sizeof(ObDictCacheValue) + (dat_block_ ? dat_block_->mem_block_size_ : 0);
  }

  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override
  {
    int ret = OB_SUCCESS;
    const int64_t value_size = size();
    if (OB_UNLIKELY(OB_ISNULL(buf)) || OB_UNLIKELY(buf_len < value_size) || OB_UNLIKELY(OB_ISNULL(dat_block_))) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid argument for ob dict cache", K(ret), K(buf_len), K(value_size), KP(dat_block_));
    } else {
      ObFTDAT *new_data = reinterpret_cast<ObFTDAT *>(buf + sizeof(ObDictCacheValue));
      MEMCPY(new_data, dat_block_, dat_block_->mem_block_size_);

      ObIKVCacheValue *new_value = new (buf) ObDictCacheValue(
          new_data, snapshot_version_, range_count_);
      value = new_value;
    }
    return ret;
  }

  const ObFTDAT *get_dat_block() const { return dat_block_; }
  int32_t get_range_count() const { return range_count_; }
  void set_range_count(int32_t range_count) { range_count_ = range_count; }
  int64_t get_snapshot_version() const { return snapshot_version_; }
  void set_snapshot_version(int64_t snapshot_version) { snapshot_version_ = snapshot_version; }

public:
  const ObFTDAT *dat_block_;
  int64_t snapshot_version_; // Snapshot version used when building this cache entry
  int32_t range_count_; // Only valid for range_id = 0, stores total number of ranges
};

class ObDictCache : public common::ObKVCache<ObDictCacheKey, ObDictCacheValue>
{
public:
  ObDictCache() {}
  virtual ~ObDictCache() {}
  int get_dict(const ObDictCacheKey &key,
               const ObDictCacheValue *&value,
               common::ObKVCacheHandle &handle);
  int put_dict(const ObDictCacheKey &key, const ObDictCacheValue &value);

  int put_and_fetch_dict(const ObDictCacheKey &key,
                         const ObDictCacheValue &value,
                         const ObDictCacheValue *&pvalue,
                         common::ObKVCacheHandle &handle,
                         bool overwrite = true);

public:
  static ObDictCache &get_instance()
  {
    static ObDictCache cache;
    return cache;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObDictCache);
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_CACHE_H_
