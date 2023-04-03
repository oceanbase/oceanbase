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
 *
 * ObEasyHazardMap
 * An easy-to-use HashMap that supports the get/revert/remove interface and implements the methods:
 * 1. use ObObConcurrentHashMapWithHazardValue as a HashMap that supports Hazard protection
 * 2. use ObSmallObjPool as the Value object allocator
 * 3. Require a fixed size of Value object
 */

#ifndef OCEANBASE_LIBOBCDC_EASY_HAZARD_MAP_
#define OCEANBASE_LIBOBCDC_EASY_HAZARD_MAP_

#include "lib/atomic/ob_atomic.h"                               // ATOMIC_*
#include "lib/objectpool/ob_small_obj_pool.h"                   // ObSmallObjPool
#include "lib/hash/ob_concurrent_hash_map_with_hazard_value.h"  // ObConcurrentHashMapWithHazardValue
#include "share/ob_errno.h"                                     // KR

namespace oceanbase
{
namespace libobcdc
{
template <class K, class V>
class ObEasyHazardMap
{
  typedef common::ObConcurrentHashMapWithHazardValue<K, V*> EMap;
  typedef common::ObSmallObjPool<V> EPool;

  class EAlloc : public EMap::IValueAlloc
  {
  public:
    EAlloc() : inited_(false), pool_() {}
    ~EAlloc() { destroy(); }

  public:
    V* alloc();
    void free(V* value);
    int64_t get_alloc_count() const { return pool_.get_alloc_count(); }
    int64_t get_free_count() const { return pool_.get_free_count(); }

  public:
    int init(const int64_t max_cached_count,
        const int64_t block_size,
        const char *label,
        const uint64_t tenant_id);
    void destroy();

  private:
    bool  inited_;
    EPool pool_;
  };

public:
  ObEasyHazardMap() : inited_(false), valid_count_(0), alloc_(), map_() {}
  virtual ~ObEasyHazardMap() { destroy(); }

public:
  int init(const int64_t max_cached_count,
      const int64_t block_size,
      const char *label,
      const uint64_t tenant_id = common::OB_SERVER_TENANT_ID);
  void destroy();
  int64_t get_valid_count() const { return valid_count_; }
  int64_t get_alloc_count() const { return alloc_.get_alloc_count(); }
  int64_t get_free_count() const { return alloc_.get_free_count(); }

  /// 元素的分配与释放
  V* alloc() { return alloc_.alloc(); }
  void free(V *value) { alloc_.free(value); }

  /// contains key or not
  ///
  /// @param key key to find
  ///
  /// @retval OB_ENTRY_EXIST        element exist
  /// @retval OB_ENTRY_NOT_EXIST    element not exist
  /// @retval other errcode         fail
  int contains_key(const K &key);

  /// insert element
  /// NOTICE:
  /// 1. The interface has no get semantics, no need to call revert(), if the element is to be used further, call get() once
  /// 2. Elements must be allocated and freed using the alloc()/free() functions provided by this class
  ///
  /// @param [in]   key          key
  /// @param [int]  value        Value
  ///
  /// @retval OB_SUCCESS         success
  /// @retval OB_ENTRY_EXIST     element already exist
  /// @retval other errcode      fail
  int insert(const K &key, V *value);

  /// Get Key-Value record, support creating a new Value when it does not exist
  ///
  /// @param [in] key           key
  /// @param [out] value        value of key
  /// @param [in] enable_create Whether to allow the creation of a new Value object if it does not exist
  ///
  /// @retval OB_SUCCESS          success
  /// @retval OB_ENTRY_NOT_EXIST  Does not exist, return when enable_create is false
  /// @retval other errcode       fail
  int get(const K &key, V *&value, bool enable_create = false);

  /// Return to Value object
  ///
  /// @param value Value object
  ///
  /// @retval OB_SUCCESS     success
  /// @retval other errcode  fail
  int revert(V *value);

  /// Delete Key-Value records
  ///
  /// @param key key of target operation
  ///
  /// @retval OB_SUCCESS          success
  /// @retval OB_ENTRY_NOT_EXIST  key not exist
  /// @retval other errcode       fail
  int remove(const K &key);

  void print_state(const char *mod_str) const;

  template <typename Function> int for_each(Function &fn)
  {
    int ret = common::OB_SUCCESS;
    if (! inited_) {
      ret = common::OB_NOT_INIT;
    } else {
      ret = map_.for_each(fn);
    }
    return ret;
  }

private:
  bool    inited_;
  int64_t valid_count_;
  EAlloc  alloc_;
  EMap    map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObEasyHazardMap);
};

///////////////////////////////////////////////////////////////////////////////////

template <class K, class V>
int ObEasyHazardMap<K, V>::EAlloc::init(const int64_t max_cached_count,
    const int64_t block_size,
    const char *label,
    const uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  if (inited_) {
    ret = common::OB_INIT_TWICE;
  } else if (max_cached_count <= 0 || block_size <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(pool_.init(max_cached_count, label, tenant_id, block_size))) {
    LIB_LOG(ERROR, "init value pool fail", KR(ret), K(max_cached_count), K(block_size));
  } else {
    inited_ = true;
  }
  return ret;
}

template <class K, class V>
void ObEasyHazardMap<K, V>::EAlloc::destroy()
{
  inited_ = false;
  pool_.destroy();
}

template <class K, class V>
V* ObEasyHazardMap<K, V>::EAlloc::alloc()
{
  int ret = common::OB_SUCCESS;
  V *ret_obj = NULL;

  if (! inited_) {
    ret = common::OB_NOT_INIT;
  } else if (OB_FAIL(pool_.alloc(ret_obj))) {
    LIB_LOG(ERROR, "alloc value from pool fail", KR(ret));
  } else {
    ret_obj->reset();
  }

  return ret_obj;
}

template <class K, class V>
void ObEasyHazardMap<K, V>::EAlloc::free(V *value)
{
  int ret = common::OB_SUCCESS;
  if (inited_ && NULL != value) {
    if (OB_FAIL(pool_.free(value))) {
      LIB_LOG(ERROR, "free value fail", K(value), KR(ret));
    } else {
      value = NULL;
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////

template <class K, class V>
int ObEasyHazardMap<K, V>::init(const int64_t max_cached_count,
    const int64_t block_size,
    const char *label,
    const uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  if (inited_) {
    ret = common::OB_INIT_TWICE;
  } else if (max_cached_count <= 0 || block_size <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(alloc_.init(max_cached_count, block_size, label, tenant_id))) {
    LIB_LOG(ERROR, "init allocator fail", KR(ret), K(max_cached_count), K(block_size));
  } else if (OB_FAIL(map_.init(&alloc_))) {
    LIB_LOG(ERROR, "init map fail", KR(ret));
  } else {
    valid_count_ = 0;
    inited_ = true;
  }
  return ret;
}

template <class K, class V>
void ObEasyHazardMap<K, V>::destroy()
{
  inited_ = false;
  valid_count_ = 0;

  // FIXME: 由于EMap依赖EAlloc，但是EMap没有destroy()函数，
  // 因此，此处不能调用EAlloc的destroy()函数

  // TODO: 回收每一个在Map中的元素
}

template <class K, class V>
int ObEasyHazardMap<K, V>::contains_key(const K &key)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    ret = common::OB_NOT_INIT;
  } else {
    ret = map_.contains_key(key);
  }
  return ret;
}

template <class K, class V>
int ObEasyHazardMap<K, V>::insert(const K &key, V *value)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_FAIL(map_.put_refactored(key, value))) {
    if (common::OB_ENTRY_EXIST == ret) {
      // element exist
    } else {
      LIB_LOG(WARN, "put value into easy hazard map fail", KR(ret), K(key), K(value));
    }
  } else {
    // Inserted successfully, increase the number of valid
    ATOMIC_INC(&valid_count_);
  }
  return ret;
}

template <class K, class V>
int ObEasyHazardMap<K, V>::get(const K &key, V *&value, bool enable_create)
{
  int ret = common::OB_SUCCESS;
  if (! inited_) {
    ret = common::OB_NOT_INIT;
  } else if (OB_SUCC(map_.get_refactored(key, value))) {
    // succ
  } else if (OB_LIKELY(common::OB_ENTRY_NOT_EXIST == ret) && OB_LIKELY(enable_create)) {
    // Create a new record when the record does not exist and is allowed to create a new record
    while (common::OB_ENTRY_NOT_EXIST == ret) {
      if (OB_SUCC(map_.create_refactored(key, value))) {
        // Created successfully and returned the object just created
        ATOMIC_INC(&valid_count_);
      } else if (OB_UNLIKELY(common::OB_ENTRY_EXIST == ret)) {
        // Create operational conflicts and get them through the get interface
        LIB_LOG(DEBUG, "create value conflict, get value instead", K(key));

        ret = map_.get_refactored(key, value);

        if (OB_UNLIKELY(common::OB_SUCCESS != ret)
            && OB_UNLIKELY(common::OB_ENTRY_NOT_EXIST != ret)) {
          LIB_LOG(ERROR, "get value from map fail", KR(ret));
        } else if (OB_UNLIKELY(common::OB_ENTRY_NOT_EXIST == ret)) {
          // The second get record still does not exist, which means the record has been deleted in the meantime, try again next time
          LIB_LOG(WARN, "value not exist after create-get. retry immediately.");
        }
      } else {
        LIB_LOG(ERROR, "create value from map fail", KR(ret));
      }
    }
  } else if (OB_UNLIKELY(common::OB_ENTRY_NOT_EXIST != ret)) {
    LIB_LOG(ERROR, "get value from map fail", KR(ret), K(key));
  }

  return ret;
}

template <class K, class V>
int ObEasyHazardMap<K, V>::revert(V *value)
{
  int ret = common::OB_SUCCESS;
  if (! inited_) {
    ret = common::OB_NOT_INIT;
  } else if (OB_UNLIKELY(NULL == value)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(map_.revert_value(value))) {
    LIB_LOG(ERROR, "revert value fail", KR(ret), K(value));
  } else {
    // succ
    value = NULL;
  }
  return ret;
}

template <class K, class V>
int ObEasyHazardMap<K, V>::remove(const K &key)
{
  int ret = common::OB_SUCCESS;
  if (! inited_) {
    ret = common::OB_NOT_INIT;
  } else if (OB_FAIL(map_.remove_refactored(key))) {
    if (common::OB_ENTRY_NOT_EXIST != ret) {
      LIB_LOG(ERROR, "remove value fail", KR(ret), K(key));
    }
  } else {
    // succ
    ATOMIC_DEC(&valid_count_);
  }
  return ret;
}

template <class K, class V>
void ObEasyHazardMap<K, V>::print_state(const char *mod_str) const
{
  _LIB_LOG(INFO, "%s VALID=%ld HAZARD_CACHED=%ld ALLOC=%ld FREE=%ld",
      mod_str,
      get_valid_count(),
      get_alloc_count() - get_free_count() - get_valid_count(),
      get_alloc_count(),
      get_free_count());
}
} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_EASY_HAZARD_MAP_ */
