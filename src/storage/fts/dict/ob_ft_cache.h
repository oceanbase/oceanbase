/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
  ObDictCacheKey(const uint64_t name,
                 const uint64_t tenant,
                 const ObFTDictType dict_type,
                 int32_t range_id)
      : name_(name), tenant_(tenant), dict_type_(dict_type), range_id_(range_id)
  {
  }
  ~ObDictCacheKey() override {}

  bool operator==(const ObIKVCacheKey &other) const override
  {
    const ObDictCacheKey &other_key = reinterpret_cast<const ObDictCacheKey &>(other);
    return (&other == this)
           || ((other_key.name_ == name_) && (other_key.dict_type_ == dict_type_)
               && (other_key.tenant_ == tenant_));
  }

  uint64_t hash() const override
  {
    uint64_t hash_val = 0;
    hash_val = murmurhash(&name_, sizeof(name_), hash_val);
    hash_val = murmurhash(&tenant_, sizeof(tenant_), hash_val);
    hash_val = murmurhash(&dict_type_, sizeof(dict_type_), hash_val);
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
  uint64_t get_tenant_id() const override { return tenant_; }
  int64_t size() const override { return sizeof(ObDictCacheKey); }

  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid argument for ob dict cache", K(ret), K(buf_len), K(size()));
    } else {
      ObDictCacheKey *new_key = new (buf) ObDictCacheKey(name_, tenant_, dict_type_, range_id_);
      key = new_key;
    }
    return ret;
  }

private:
  // to change to name
  uint64_t name_; // when build dict
  uint64_t tenant_;
  ObFTDictType dict_type_;
  int32_t range_id_;
};

class ObDictCacheValue : public common::ObIKVCacheValue
{
public:
  ObDictCacheValue(ObFTDAT *dat_block) : dat_block_(dat_block) {}
  ~ObDictCacheValue() override {}
  int64_t size() const override { return sizeof(ObDictCacheValue) + dat_block_->mem_block_size_; }
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid argument for ob dict cache", K(ret), K(buf_len), K(size()));
    } else {
      ObFTDAT *new_data = reinterpret_cast<ObFTDAT *>(buf + sizeof(ObDictCacheValue));
      MEMCPY(new_data, dat_block_, dat_block_->mem_block_size_);
      ObIKVCacheValue *new_value = new (buf) ObDictCacheValue(new_data);
      value = new_value;
    }
    return ret;
  }

public:
  ObFTDAT *dat_block_;
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
                         common::ObKVCacheHandle &handle);

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
