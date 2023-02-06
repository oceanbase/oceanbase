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

#ifndef OCEANBASE_COMMON_HASH_OB_ITERATABLE_HASHSET_
#define OCEANBASE_COMMON_HASH_OB_ITERATABLE_HASHSET_
#include "lib/hash/ob_placement_hashutils.h"
#include "lib/hash/ob_placement_hashset.h"
#include "lib/list/ob_dlist.h"
#include "lib/utility/ob_print_utils.h"
namespace oceanbase
{
namespace common
{
namespace hash
{
template <class K, uint64_t N>
class ObIteratableHashSet;

template <typename HashSet>
class ObIteratableHashSetConstIterator
{
  typedef typename HashSet::const_key_ref_t const_key_ref_t;
  typedef typename HashSet::inner_key_t inner_key_t;
  typedef ObIteratableHashSetConstIterator<HashSet> self_t;
public:
  ObIteratableHashSetConstIterator(const HashSet *set, const inner_key_t *cur)
      : set_(set), cur_(cur)
  {
  }
  virtual ~ObIteratableHashSetConstIterator() {}
  ObIteratableHashSetConstIterator(const self_t &other)
      : set_(other.set_), cur_(other.cur_)
  {
  }
  ObIteratableHashSetConstIterator &operator=(const self_t &other)
  {
    if (this != &other) {
      set_ = other.set_;
      cur_ = other.cur_;
    }
    return *this;
  }
  bool operator== (const self_t &other) const
  {
    return set_ == other.set_ && cur_ == other.cur_;
  }
  bool operator!= (const self_t &other) const
  {
    return set_ != other.set_ || cur_ != other.cur_;
  }
  self_t &operator++()
  {
    cur_ = cur_->get_next();
    return *this;
  }
  const_key_ref_t operator*() const
  {
    return cur_->get_data();
  }
private:
  // data members
  const HashSet *set_;
  const inner_key_t *cur_;
};

template <class K, uint64_t N = 1031>
class ObIteratableHashSet: protected ObPlacementHashSet<ObDLinkNode<K>, N>
{
public:
  typedef K *key_pointer_t;
  typedef const K *const_key_pointer_t;
  typedef K &key_ref_t;
  typedef const K &const_key_ref_t;
  typedef ObDLinkNode<K> inner_key_t;
  typedef ObPlacementHashSet<ObDLinkNode<K>, N> parent_t;
  typedef ObIteratableHashSet<K, N> self_t;
  typedef ObIteratableHashSetConstIterator<self_t> const_iterator_t;
public:
  ObIteratableHashSet(): parent_t(), list_() {}
  virtual ~ObIteratableHashSet() {}
  /**
   * @retval OB_SUCCESS       insert new key succ
   * @retval OB_HASH_EXIST    key exist
   * @retval other            errors
   */
  int set_refactored(const K &key)
  {
    int ret = OB_SUCCESS;
    inner_key_t ikey;
    ikey.get_data() = key;
    uint64_t pos = 0;
    bool exist = false;
    // TODO: template arg
    ret = placement_hash_find_set_pos<inner_key_t, N>(parent_t::keys_, parent_t::flags_, ikey, 0, pos, exist);
    if (OB_SUCC(ret)) {
      parent_t::keys_[pos] = ikey;
      list_.add_last(&parent_t::keys_[pos]);
      ++parent_t::count_;
    }
    return ret;
  }
  /**
   * @retval OB_HASH_EXIST     success
   * @retval OB_HASH_NOT_EXIST key does not exist
   * @retval other             errors
   */
  int exist_refactored(const K &key) const
  {
    inner_key_t ikey;
    ikey.get_data() = key;
    return parent_t::exist_refactored(ikey);
  }
  void reset() { clear(); }
  void clear()
  {
    parent_t::clear();
    list_.reset();
  }
  int64_t count() const {return parent_t::count();}
  const_iterator_t begin() const
  {
    return const_iterator_t(this, list_.get_first());
  }
  const_iterator_t end() const
  {
    return const_iterator_t(this, list_.get_header());
  }
  ObIteratableHashSet &operator=(const ObIteratableHashSet &other)
  {
    int tmp_ret = OB_SUCCESS;
    clear();
    for (const_iterator_t it = other.begin();
         it != other.end();
         ++it) {
      if (OB_SUCCESS != (tmp_ret = set_refactored(*it))) {
        _OB_LOG_RET(ERROR, tmp_ret, "fail set value. tmp_ret=%d", tmp_ret);
        break;
      }
    }
    return *this;
  }
  DECLARE_TO_STRING;
private:
  // types and constants
  template <typename HashSet>
  friend class ObIteratableHashSetConstIterator;
private:
  // disallow copy
  ObIteratableHashSet(const ObIteratableHashSet &other);
  // function members
protected:
  // data members
  ObDList<inner_key_t> list_;
};

template <class K, uint64_t N>
int64_t ObIteratableHashSet<K, N>::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_ARRAY_START();
  const_iterator_t beg = begin();
  for (const_iterator_t it = beg;
       it != end();
       ++it) {
    if (it != beg) {
      J_COMMA();
    }
    BUF_PRINTO(*it);
  }
  J_ARRAY_END();
  return pos;
}
} // end namespace hash
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_HASH_OB_ITERATABLE_HASHSET_
