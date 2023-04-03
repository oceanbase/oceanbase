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

#ifndef OCEANBASE_COMMON_HASH_OB_ITERATABLE_HASHMAP_
#define OCEANBASE_COMMON_HASH_OB_ITERATABLE_HASHMAP_
#include "lib/list/ob_dlist.h"
#include "lib/hash/ob_placement_hashutils.h"
#include "lib/utility/ob_print_utils.h"
#include <utility>
namespace oceanbase
{
namespace common
{
namespace hash
{
template <typename HashMap>
class ObIteratableHashMapConstIterator
{
public:
  typedef typename HashMap::const_key_pointer_t const_key_pointer_t;
  typedef typename HashMap::const_value_pointer_t const_value_pointer_t;
  typedef typename HashMap::const_key_t const_key_t;
  typedef typename HashMap::const_value_t const_value_t;
  typedef typename HashMap::const_key_ref_t const_key_ref_t;
  typedef typename HashMap::const_value_ref_t const_value_ref_t;
  typedef std::pair<const_key_ref_t, const_value_ref_t> const_iter_value_t;
  typedef ObIteratableHashMapConstIterator<HashMap> self_t;
  typedef typename HashMap::inner_key_t inner_key_t;
public:
  ObIteratableHashMapConstIterator()
      : map_(NULL), cur_(NULL)
  {
  }
  ObIteratableHashMapConstIterator(const HashMap *map, const inner_key_t *cur)
      : map_(map), cur_(cur)
  {
  }
  virtual ~ObIteratableHashMapConstIterator() {}
  ObIteratableHashMapConstIterator(const self_t &other)
      : map_(other.map_), cur_(other.cur_)
  {
  }
  ObIteratableHashMapConstIterator &operator=(const self_t &other)
  {
    if (this != &other) {
      map_ = other.map_;
      cur_ = other.cur_;
    }
    return *this;
  }
  bool operator!= (const self_t &other) const
  {
    return map_ != other.map_ || cur_ != other.cur_;
  }
  bool operator== (const self_t &other) const
  {
    return map_ == other.map_ && cur_ == other.cur_;
  }
  self_t &operator++()
  {
    cur_ = cur_->get_next();
    return *this;
  }
  const_iter_value_t operator*() const
  {
    uint64_t pos = cur_ - map_->keys_;
    return const_iter_value_t(cur_->get_data(), map_->values_[pos]);
  }
private:
  // data members
  const HashMap *map_;
  const inner_key_t *cur_;
};

template <typename K, typename V, uint64_t N = 1031, bool auto_free = false>
class ObIteratableHashMap
{
public:
  typedef K key_t;
  typedef const K const_key_t;
  typedef K *key_pointer_t;
  typedef const K *const_key_pointer_t;
  typedef K &key_ref_t;
  typedef const K &const_key_ref_t;
  typedef V value_t;
  typedef const V const_value_t;
  typedef V *value_pointer_t;
  typedef const V *const_value_pointer_t;
  typedef V &value_ref_t;
  typedef const V &const_value_ref_t;
  typedef ObIteratableHashMapConstIterator<ObIteratableHashMap> const_iterator_t;
  typedef common::ObDLinkNode<K> inner_key_t;
public:
  ObIteratableHashMap() : flags_(), keys_(), values_(), count_(0), list_() {}
  ~ObIteratableHashMap() {}
  /**
   * put a key value pair into HashMap
   * when flag = 0, do not overwrite existing <key,value> pair
   * when flag != 0 and overwrite_key = 0, overwrite existing value
   * when flag != 0 and overwrite_key != 0, overwrite existing <key,value> pair
   * @retval OB_SUCCESS success
   * @retval OB_HASH_EXIST key exist when flag = 0
   * @retval other for errors
   */
  int  set_refactored(const K &key, const V &value, int flag = 0, int overwrite_key = 0);
  /**
   * @retval OB_SUCCESS  success
   * @retval OB_HASH_NOT_EXIST key does not exist
   * @retval other for errors
   */
  int  get_refactored(const K &key, V &value) const;
  /**
   * @retval value get the corresponding value of key
   * @retval NULL key does not exist or error happen
   */
  const V *get(const K &key) const;
  V *get(const K &key);

  void reuse() {flags_.reuse(); list_.reset(); count_ = 0;}
  int64_t count() const {return count_;};

  const_iterator_t begin() const
  {
    const_iterator_t ret(this, list_.get_first());
    return ret;
  }
  const_iterator_t end() const
  {
    const_iterator_t ret(this, list_.get_header());
    return ret;
  }
  ObIteratableHashMap &operator=(const ObIteratableHashMap &other)
  {
    int ret = OB_SUCCESS;
    reuse();
    for (const_iterator_t it = other.begin();
         it != other.end();
         ++it) {
      if (OB_FAIL(set_refactored((*it).first, (*it).second))) {
        _OB_LOG(ERROR, "fail set value. ret=%d", ret);
        break;
      }
    }
    return *this;
  }
  DECLARE_TO_STRING;
private:
  // types and constants
  template <typename HashMap>
  friend class ObIteratableHashMapConstIterator;

private:
  // disallow copy
  ObIteratableHashMap(const ObIteratableHashMap &other);
  // function members
private:
  // data members
  ObBitSet<N, ModulePageAllocator, auto_free> flags_;
  inner_key_t keys_[N];
  V values_[N];
  int64_t count_;
  common::ObDList<inner_key_t> list_; // list of used elements
};

template <typename K, typename V, uint64_t N, bool auto_free>
int  ObIteratableHashMap<K, V, N, auto_free>::set_refactored(const K &key,
                                       const V &value,
                                       int flag /*= 0*/,
                                       int overwrite_key /*= 0*/)
{
  int ret = OB_SUCCESS;
  inner_key_t inner_key;
  inner_key.get_data() = key;
  uint64_t pos = 0;
  bool exist = false;
  ret = placement_hash_find_set_pos<inner_key_t, N, ModulePageAllocator, auto_free>(keys_, flags_, inner_key, flag, pos, exist);
  if (OB_SUCC(ret)) {
    if (!exist) {
      keys_[pos] = inner_key;
      values_[pos] = value;
      list_.add_last(&keys_[pos]);
      ++count_;
    } else {
      if (overwrite_key == 0) {
        keys_[pos].get_data() = key;
      }
      values_[pos] = value;
    }
  }
  return ret;
}

template <typename K, typename V, uint64_t N, bool auto_free>
int ObIteratableHashMap<K, V, N, auto_free>::get_refactored(const K &key, V &value) const
{
  int ret = OB_SUCCESS;
  inner_key_t inner_key;
  inner_key.get_data() = key;
  uint64_t pos = 0;
  ret = placement_hash_search<inner_key_t, N, ModulePageAllocator, auto_free>(keys_, flags_, inner_key, pos);
  if (OB_SUCC(ret)) {
    value = values_[pos];
  }
  return ret;
}

template <typename K, typename V, uint64_t N, bool auto_free>
const V *ObIteratableHashMap<K, V, N, auto_free>::get(const K &key) const
{
  const V *value = NULL;
  inner_key_t inner_key;
  inner_key.get_data() = key;
  uint64_t pos = 0;
  int tmp_ret = placement_hash_search<inner_key_t, N, ModulePageAllocator, auto_free>(keys_, flags_, inner_key, pos);
  if (OB_SUCCESS == tmp_ret) {
    value = &values_[pos];
  }
  return value;
}

template <typename K, typename V, uint64_t N, bool auto_free>
V *ObIteratableHashMap<K, V, N, auto_free>::get(const K &key)
{
  V *value = NULL;
  inner_key_t inner_key;
  inner_key.get_data() = key;
  uint64_t pos = 0;
  int tmp_ret = placement_hash_search<inner_key_t, N, ModulePageAllocator,  auto_free>(keys_, flags_, inner_key, pos);
  if (OB_SUCCESS == tmp_ret) {
    value = &values_[pos];
  }
  return value;
}

template <typename K, typename V, uint64_t N, bool auto_free>
int64_t ObIteratableHashMap<K, V, N, auto_free>::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_ARRAY_START();
  const_iterator_t beg = begin();
  for (const_iterator_t it = beg; it != end(); ++it) {
    if (it != beg) {
      J_COMMA();
    }
    J_OBJ_START();
    J_KV(N_KEY, (*it).first,
         N_VALUE, (*it).second);
    J_OBJ_END();
  }
  J_ARRAY_END();
  return pos;
}

} // end namespace hash
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_HASH_OB_ITERATABLE_HASHMAP_
