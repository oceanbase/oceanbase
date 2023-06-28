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

#ifndef OCEANBASE_HASH_OB_REFERED_MAP_H_
#define OCEANBASE_HASH_OB_REFERED_MAP_H_

#include "lib/hash/ob_hashtable.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
namespace common
{
namespace hash
{

template <typename K, typename V>
class ObReferedMap
{
public:
  struct Item
  {
  public:
    struct GetKey
    {
      const K &operator ()(const Item &item) { return item.v_.get_key(); }
    };

    Item() : ref_cnt_(0), map_(NULL), v_() {}
    int64_t get_ref_cnt() const { return ref_cnt_; }
    void ref() { ref_cnt_++; }
    void unref();
    void set_map(ObReferedMap *map) { map_ = map; }
    int assign(const Item &other);

    TO_STRING_KV(K_(ref_cnt), K_(map), K_(v));

  private:
    int64_t ref_cnt_;
    ObReferedMap *map_;
  public:
    V v_;
  };
  typedef common::hash::ObHashTable<K, Item, common::hash::hash_func<K>,
          common::hash::equal_to<K>, typename Item::GetKey,
          common::hash::SimpleAllocer<typename common::hash::HashTableTypes<Item>::AllocType>,
          common::hash::NoPthreadDefendMode,
          common::hash::NormalPointer, common::ObMalloc> HashTable;

  ObReferedMap(const lib::ObLabel &label = ObModIds::OB_REFERED_MAP) : inited_(false), allocator_(),
    bucket_allocator_(), hash_table_()
  { allocator_.set_attr(ObMemAttr(common::OB_SERVER_TENANT_ID, label)); }
  virtual ~ObReferedMap() {}

  int init(const int64_t bucket_num);
  bool is_inited() const { return inited_; }
  const HashTable &get_hash_table() const { return hash_table_; }

  // return exist item or create a new one.
  int locate(const K &key, Item *&value);

  // return OB_HASH_NOT_EXIST if not found
  int get(const K&key, V &value) const;

  void reuse() { hash_table_.reuse(); }

  TO_STRING_EMPTY();
private:
  bool inited_;
  common::hash::SimpleAllocer<typename common::hash::HashTableTypes<Item>::AllocType> allocator_;
  common::ObMalloc bucket_allocator_;
  HashTable hash_table_;

  DISALLOW_COPY_AND_ASSIGN(ObReferedMap);
};

template <typename K, typename V>
int ObReferedMap<K, V>::Item::assign(const ObReferedMap<K, V>::Item &other)
{
  int ret = OB_SUCCESS;
  ref_cnt_ = other.ref_cnt_;
  map_ = other.map_;
  if (OB_FAIL(common::copy_assign(v_, other.v_))) {
    LIB_LOG(WARN, "failed to copy v_", K(ret));
  }
  return ret;
}

template <typename K, typename V>
void ObReferedMap<K, V>::Item::unref()
{
  ref_cnt_--;
  if (ref_cnt_ <= 0 && NULL != map_) {
    ObReferedMap *map = map_;
    map_ = NULL;
    // ignore erase value
    map->hash_table_.erase_refactored(v_.get_key());
  }
}

template <typename K, typename V>
int ObReferedMap<K, V>::init(const int64_t bucket_num)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LIB_LOG(WARN, "already inited", K(ret));
  } else if (bucket_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid bucket num", K(ret), K(bucket_num));
  } else {
    bucket_allocator_.set_label(ObModIds::OB_REFERED_MAP);
    if (OB_FAIL(hash_table_.create(common::hash::cal_next_prime(bucket_num),
        &allocator_, &bucket_allocator_))) {
      LIB_LOG(WARN, "create hash table failed", K(bucket_num), K(ret));
    } else {
      inited_ = true;
    }
  }
  return ret;
}

template <typename K, typename V>
int ObReferedMap<K, V>::locate(const K &key, ObReferedMap<K, V>::Item *&value)
{
  int ret = OB_SUCCESS;
  const Item *const_value = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "not init", K(ret));
  } else if (OB_SUCC(hash_table_.get_refactored(key, const_value))) {
    if (NULL == const_value) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "get from hash table failed", K(ret), K(const_value), K(key));
    }
  } else if (OB_HASH_NOT_EXIST == ret) {
    Item item;
    item.v_.set_key(key);
    if (OB_FAIL(hash_table_.set_refactored(key, item))) {
      LIB_LOG(WARN, "insert to hash table failed", K(ret), K(key));
    } else {
      ret = hash_table_.get_refactored(key, const_value);
      if (OB_FAIL(ret) || NULL == const_value) {
        ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
        LIB_LOG(WARN, "get again from hash failed",
            K(ret), K(const_value), K(key));
      } else {
        const_cast<Item *>(const_value)->set_map(this);
      }
    }
  } else {
    LIB_LOG(WARN, "get from hash table failed", K(ret), K(const_value), K(key));
  }
  if (OB_SUCC(ret)) {
    if (NULL == const_value) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "NULL value", K(ret));
    } else {
      value = const_cast<Item *>(const_value);
    }
  }
  return ret;
}

template <typename K, typename V>
int ObReferedMap<K, V>::get(const K &key, V &value) const
{
  int ret = OB_SUCCESS;
  const Item *item = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(const_cast<HashTable &>(hash_table_).get_refactored(key, item))
      || NULL == item) {
    ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
    if (OB_HASH_NOT_EXIST != ret) {
      LIB_LOG(WARN, "get from hash table failed", K(ret), K(value), K(key));
    }
  } else {
    value = item->v_;
  }
  return ret;
}

}
}
}
#endif // OCEANBASE_HASH_OB_REFERED_MAP_H_
