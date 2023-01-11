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
#ifndef _OB_TABLE_HOTKEY_KVCACHE_H
#define _OB_TABLE_HOTKEY_KVCACHE_H

#define USING_LOG_PREFIX SERVER

#include "ob_table_service.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace table
{

enum HotKeyType
{
  TABLE_HOTKEY_ALL,
  TABLE_HOTKEY_INSERT,
  TABLE_HOTKEY_DELETE,
  TABLE_HOTKEY_UPDATE,
  TABLE_HOTKEY_QUERY,
  TABLE_HOTKEY_INVALID
};

struct ObTableHotKeyCacheKey : public common::ObIKVCacheKey
{
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t partition_id_;
  uint64_t epoch_;
  HotKeyType type_;
  ObString rowkey_;

  ObTableHotKeyCacheKey() : tenant_id_(0),
                            table_id_(0),
                            partition_id_(0),
                            epoch_(0),
                            type_(HotKeyType::TABLE_HOTKEY_INVALID),
                            rowkey_()
  {
  }

  ObTableHotKeyCacheKey(uint64_t tenant_id, uint64_t table_id, int64_t partition_id, uint64_t epoch,
                        HotKeyType type, ObString rowkey) : tenant_id_(tenant_id),
                                                            table_id_(table_id),
                                                            partition_id_(partition_id),
                                                            epoch_(epoch),
                                                            type_(type),
                                                            rowkey_(rowkey)
  {
  }

  uint64_t hash() const
  {
    uint64_t hash_val = rowkey_.hash();
    hash_val = murmurhash(&tenant_id_, sizeof(uint64_t), hash_val);
    hash_val = murmurhash(&table_id_, sizeof(uint64_t), hash_val);
    hash_val = murmurhash(&epoch_, sizeof(uint64_t), hash_val);
    hash_val = murmurhash(&type_, sizeof(HotKeyType), hash_val);
    return hash_val;
  }
  bool operator==(const ObIKVCacheKey &other) const
  {
    const ObTableHotKeyCacheKey &other_key = reinterpret_cast<const ObTableHotKeyCacheKey&>(other);
    return tenant_id_ == other_key.tenant_id_
        && table_id_ == other_key.table_id_
        && epoch_ == other_key.epoch_
        && type_ == other_key.type_
        && rowkey_ == other_key.rowkey_;
  }
  bool operator==(const ObTableHotKeyCacheKey &other) const
  {
    return tenant_id_ == other.tenant_id_
        && table_id_ == other.table_id_
        && epoch_ == other.epoch_
        && type_ == other.type_
        && rowkey_ == other.rowkey_;
  }
  inline uint64_t get_tenant_id() const;
  // space of rowkey_.size() is used to store the rowkey string
  int64_t size() const { return sizeof(*this) + rowkey_.size(); }
  int deep_copy(ObFIFOAllocator &allocator, ObTableHotKeyCacheKey *&key) const;
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey  *&key) const;
  TO_STRING_KV(K(tenant_id_), K(table_id_), K(partition_id_), K(epoch_), K(type_), K(rowkey_));
};

struct ObTableSlideWindowHotkey : public common::ObIKVCacheKey
{
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t partition_id_;
  HotKeyType type_;
  ObString rowkey_;

  ObTableSlideWindowHotkey() : tenant_id_(0),
                               table_id_(0),
                               partition_id_(0),
                               type_(HotKeyType::TABLE_HOTKEY_INVALID),
                               rowkey_()
  {
  }
  ObTableSlideWindowHotkey(ObTableHotKeyCacheKey &other) : tenant_id_(other.tenant_id_),
                                                           table_id_(other.table_id_),
                                                           partition_id_(other.partition_id_),
                                                           type_(other.type_),
                                                           rowkey_()
  {
    this->rowkey_ = other.rowkey_;
  }
  uint64_t hash() const
  {
    uint64_t hash_val = rowkey_.hash();
    hash_val = murmurhash(&tenant_id_, sizeof(uint64_t), hash_val);
    hash_val = murmurhash(&table_id_, sizeof(uint64_t), hash_val);
    hash_val = murmurhash(&type_, sizeof(HotKeyType), hash_val);
    return hash_val;
  }
  bool operator == (const ObIKVCacheKey &other) const
  {
    const ObTableSlideWindowHotkey &other_key = reinterpret_cast<const ObTableSlideWindowHotkey&>(other);
    return tenant_id_ == other_key.tenant_id_
        && table_id_ == other_key.table_id_
        && type_ == other_key.type_
        && rowkey_ == other_key.rowkey_;
  }
  bool operator == (const ObTableSlideWindowHotkey &other) const
  {
    return tenant_id_ == other.tenant_id_
        && table_id_ == other.table_id_
        && type_ == other.type_
        && rowkey_ == other.rowkey_;
  }
  ObTableSlideWindowHotkey& operator=(const ObTableHotKeyCacheKey &other)
  {
    this->tenant_id_ = other.tenant_id_;
    this->table_id_ = other.table_id_;
    this->partition_id_ = other.partition_id_;
    this->type_ = other.type_;
    this->rowkey_ = other.rowkey_;
    return *this;
  }
  inline uint64_t get_tenant_id() const;
  // space of rowkey_.size() is used to store the rowkey string
  virtual int64_t size() const { return sizeof(ObTableSlideWindowHotkey) + rowkey_.size(); }
  virtual int deep_copy(ObFIFOAllocator &allocator, ObTableSlideWindowHotkey *&key) const;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const;
  TO_STRING_KV(K(tenant_id_), K(table_id_), K(partition_id_), K(type_), K(rowkey_));
};

struct ObTableHotKeyCacheValue : public common::ObIKVCacheValue
{
public:
  int64_t size() const {return sizeof(ObTableHotKeyCacheValue); }
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const;
};

class ObTableHotKeyCache : public ObKVCache<ObTableHotKeyCacheKey, ObTableHotKeyCacheValue>
{
public:
  const char* OB_TABLE_HOTKEY_CACHE = "TableHKCache";
  int64_t OB_TABLE_HOTKEY_CACHE_PRIORITY = 1;

  ObTableHotKeyCache() {
    hotkey_inited_ = false;
  };

  int init(const char* cache_name, const int64_t priority = 1);
  int get(const ObTableHotKeyCacheKey &key, const ObTableHotKeyCacheValue &value, int64_t get_cnt_inc, int64_t &get_cnt);
  int get_without_put(const ObTableHotKeyCacheKey &key, const ObTableHotKeyCacheValue *&value, int64_t get_cnt_inc, int64_t &get_cnt);
  int put(const ObTableHotKeyCacheKey &key, const ObTableHotKeyCacheValue &value);
  void destroy() {
    ObKVCache::destroy();
  }
private:
  bool hotkey_inited_;
};

} /* namespace table */
} /* namespace oceanbase */

#endif /* _OB_TABLE_HOTKEY_KVCACHE_H */