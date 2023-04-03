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

#ifndef OCEANBASE_LIB_HASH_OB_CONCURRENT_HASH_MAP_
#define OCEANBASE_LIB_HASH_OB_CONCURRENT_HASH_MAP_

#include <lib/allocator/ob_mod_define.h>
#include <lib/hash/ob_hash.h>
#include <lib/allocator/ob_small_allocator.h>

namespace oceanbase
{
namespace common
{

class HashAlloc : public IAlloc
{
public:
  HashAlloc(): is_inited_(false),
               label_(ObModIds::OB_CONCURRENT_HASH_MAP),
               tenant_id_(OB_SERVER_TENANT_ID),
               size_(0) {}
  virtual ~HashAlloc() {}
  int init(const lib::ObLabel &label, const uint64_t tenant_id);
  void *alloc(const int64_t sz);
  void free(void *p);
private:
  DISALLOW_COPY_AND_ASSIGN(HashAlloc);
private:
  bool is_inited_;
  lib::ObLabel label_;
  uint64_t tenant_id_;
  int64_t size_;
  ObSmallAllocator alloc_;
};

class ArrayAlloc: public IAlloc
{
public:
  ArrayAlloc(): label_(ObModIds::OB_CONCURRENT_HASH_MAP), tenant_id_(OB_SERVER_TENANT_ID) {}
  virtual ~ArrayAlloc() {}
  int init(const lib::ObLabel &label, const uint64_t tenant_id);
  void *alloc(const int64_t sz);
  void free(void *p);
private:
  DISALLOW_COPY_AND_ASSIGN(ArrayAlloc);
private:
  lib::ObLabel label_;
  uint64_t tenant_id_;
};

template<typename Key, typename Value>
class ObConcurrentHashMap
{
private:
  typedef HashRoot                  SHashRoot;
  typedef HashBase<Key, Value>      SHash;
  typedef typename SHash::Iterator  SScanIterator;
  typedef typename SHash::Handle    SScanHandle;
  typedef typename SHash::Handle    SGetHandle;
  typedef typename SHash::Handle    SPutHandle;
public:
  ObConcurrentHashMap();
  ObConcurrentHashMap(IHashAlloc &hash_alloc, IArrayAlloc &array_alloc);
  int init(const lib::ObLabel &label = ObModIds::OB_CONCURRENT_HASH_MAP,
      const uint64_t tenant_id = OB_SERVER_TENANT_ID);
  void reset();
  void destroy();
  // return OB_SUCCESS for success, OB_ENTRY_EXIST for key exist, other for errors.
  int put_refactored(const Key &key, const Value &value);
  // return OB_SUCCESS for success, OB_ENTRY_NOT_EXIST for key not exist, other for errors.
  int remove_refactored(const Key &key);
  // return OB_SUCCESS for success, OB_ENTRY_NOT_EXIST for key not exist, other for errors.
  int get_refactored(const Key &key, Value &value) const;
  // return OB_ENTRY_EXIST for key exist, OB_ENTRY_NOT_EXIST for key not exist, other for errors.
  int contains_key(const Key &key) const;
  // return OB_SUCCESS for success, other for errors.
  template <typename Function> int for_each(Function &fn);
  template <typename Function> int remove_if(Function &fn);
private:
  DISALLOW_COPY_AND_ASSIGN(ObConcurrentHashMap);
  int err_code_map(int err) const;
private:
  class ReclaimCallback : public SHash::IKVRCallback
  {
  public:
    virtual void reclaim_key_value(Key &key, Value &value) {UNUSED(key); UNUSED(value);}
  };
  template <typename Function>
  class RemoveIf
  {
  public:
    RemoveIf(ObConcurrentHashMap &hash, Function &fn) : hash_(hash), predicate_(fn) {}
    bool operator()(Key &key, Value &value);
  private:
    ObConcurrentHashMap &hash_;
    Function &predicate_;
  };
  static int always_true(Key &key, Value &value) { UNUSED(key); UNUSED(value); return true; }
private:
  HashAlloc        hash_alloc_;
  ArrayAlloc       array_alloc_;
  ReclaimCallback  relaim_callback_;
  SHash            hash_;
  SHashRoot        hash_root_;
};

template<typename Key, typename Value>
ObConcurrentHashMap<Key, Value>::ObConcurrentHashMap()
  : hash_alloc_(),
    array_alloc_(),
    relaim_callback_(),
    hash_(hash_alloc_, array_alloc_, relaim_callback_, 1<<16),
    hash_root_()
{
}

template<typename Key, typename Value>
ObConcurrentHashMap<Key, Value>::ObConcurrentHashMap(IHashAlloc &hash_alloc, IArrayAlloc &array_alloc)
  : hash_alloc_(),
    array_alloc_(),
    relaim_callback_(),
    hash_(hash_alloc, array_alloc, relaim_callback_, 1<<16),
    hash_root_()
{
}

template<typename Key, typename Value>
int ObConcurrentHashMap<Key, Value>::init(const lib::ObLabel &label, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(label), K(tenant_id));
  } else if (OB_FAIL(hash_alloc_.init(label, tenant_id))) {
    COMMON_LOG(ERROR, "hash_alloc_ init error", K(ret), K(label), K(tenant_id));
  } else if (OB_FAIL(array_alloc_.init(label, tenant_id))) {
    COMMON_LOG(ERROR, "array_alloc_ init error", K(ret), K(label), K(tenant_id));
  } else {
    // empty
  }
  return ret;
}

template<typename Key, typename Value>
void ObConcurrentHashMap<Key, Value>::reset()
{
  (void)remove_if(always_true);
}

template<typename Key, typename Value>
void ObConcurrentHashMap<Key, Value>::destroy()
{
  reset();
}

template<typename Key, typename Value>
int ObConcurrentHashMap<Key, Value>::put_refactored(const Key &key, const Value &value)
{
  int hash_ret = 0;
  SPutHandle handle(hash_, hash_root_);
  while (-EAGAIN == (hash_ret = hash_.insert(handle, key, value))) {
    // empty
  }
  return err_code_map(hash_ret);
}

template<typename Key, typename Value>
int ObConcurrentHashMap<Key, Value>::remove_refactored(const Key &key)
{
  int hash_ret = 0;
  Value v;
  SPutHandle handle(hash_, hash_root_);
  while (-EAGAIN == (hash_ret = hash_.del(handle, key, v))) {
    // empty
  }
  return err_code_map(hash_ret);
}

template<typename Key, typename Value>
int ObConcurrentHashMap<Key, Value>::get_refactored(const Key &key, Value &value) const
{
  int hash_ret = 0;
  SGetHandle handle(const_cast<SHash&>(hash_), const_cast<SHashRoot&>(hash_root_));
  while (-EAGAIN == (hash_ret = const_cast<SHash&>(hash_).get(handle, key, value))) {
    // empty
  }
  return err_code_map(hash_ret);
}

template<typename Key, typename Value>
int ObConcurrentHashMap<Key, Value>::contains_key(const Key &key) const
{
  int hash_ret = 0;
  Value v;
  SGetHandle handle(const_cast<SHash&>(hash_), const_cast<SHashRoot&>(hash_root_));
  while (-EAGAIN == (hash_ret = const_cast<SHash&>(hash_).get(handle, key, v))) {
    // empty
  }
  hash_ret = 0 == hash_ret ? -EEXIST : hash_ret;
  return err_code_map(hash_ret);
}

template <typename Key, typename Value>
template <typename Function>
int ObConcurrentHashMap<Key, Value>::for_each(Function &fn)
{
  int ret = OB_SUCCESS;
  int hash_ret = 0;
  SScanHandle handle(hash_, hash_root_);
  SScanIterator iter;
  while (-EAGAIN == (hash_ret = hash_.get_iter(handle, iter))) {
    // empty
  }
  if (0 != hash_ret) {
    // empty
  } else {
    Key k;
    Value v;
    while (OB_SUCCESS == ret && OB_SUCCESS == (hash_ret = iter.next(k, v))) {
      if (!fn(k, v)) {
        ret = OB_EAGAIN;
        COMMON_LOG(WARN, "for_each encounters error", K(k), K(v));
      }
    }
    if (OB_SUCCESS == ret && -ENOENT != hash_ret) {
      ret = err_code_map(hash_ret);
    }
  }
  return ret;
}

template <typename Key, typename Value>
template <typename Function>
int ObConcurrentHashMap<Key, Value>::remove_if(Function &fn)
{
  RemoveIf<Function> remove_if(*this, fn);
  return for_each(remove_if);
}

template<typename Key, typename Value>
int ObConcurrentHashMap<Key, Value>::err_code_map(int err) const
{
  int ret = OB_SUCCESS;
  switch (err) {
  case 0:           ret = OB_SUCCESS; break;
  case -ENOENT:     ret = OB_ENTRY_NOT_EXIST; break;
  case -EEXIST:     ret = OB_ENTRY_EXIST; break;
  case -ENOMEM:     ret = OB_ALLOCATE_MEMORY_FAILED; break;
  case -EOVERFLOW:  ret = OB_SIZE_OVERFLOW; break;
  default:          ret = OB_ERROR;
  }
  return ret;
}

// 
// semantics of remove_if(fn)
// 1. traverse map;
// 2. delete it if fn() return true, and remove_if return if deletion is success;
// 3. remove_if return true and do nothing if fn() return false.
template<typename Key, typename Value>
template <typename Function>
bool ObConcurrentHashMap<Key, Value>::RemoveIf<Function>::operator()(Key &key, Value &value)
{
  bool bool_ret = true;
  int tmp_ret = OB_SUCCESS;

  //predicate_ is fn
  if (predicate_(key, value)) {
    if (OB_SUCCESS != (tmp_ret = hash_.remove_refactored(key))) {
      //if remove failed, stop traversing.
      bool_ret = false;
      COMMON_LOG_RET(WARN, tmp_ret, "hash remove error", K(tmp_ret), K(key), K(value));
    }
  }
  return bool_ret;
}

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_HASH_OB_CONCURRENT_HASH_MAP_
