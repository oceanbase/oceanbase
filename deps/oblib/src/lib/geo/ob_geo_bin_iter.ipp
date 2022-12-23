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
 * This file contains implementation support for the geo bin wkb iterator.
 */

#include "ob_geo.h"
#include "lib/container/ob_vector.h"

namespace oceanbase {
namespace common {

template<typename T, typename O>
ObWkbConstIterator<T, O>::ObWkbConstIterator()
  : idx_(-1), owner_(NULL), diff_info_(),
    diff_info_ptr_(&diff_info_), offsets_(), offsets_ptr_(&offsets_)
{}

template<typename T, typename O>
ObWkbConstIterator<T, O>::ObWkbConstIterator(index_type idx, const owner_t* owner)
  : idx_(idx), diff_info_(), diff_info_ptr_(&diff_info_),
    offsets_(), offsets_ptr_(&offsets_)
{
  owner_ = const_cast<owner_t*>(owner);
}

// TODO: do vector copy?
template<typename T, typename O>
ObWkbConstIterator<T, O>::ObWkbConstIterator(self& iter, bool do_array_assign)
  : idx_(iter.idx_),
    owner_(iter.owner_), diff_info_(iter.diff_info_),
    diff_info_ptr_(&diff_info_), offsets_(), offsets_ptr_(&offsets_)
{
  if (do_array_assign) {
      offsets_.assign(iter.offsets_);
  }
}

// TODO: do vector copy?
template<typename T, typename O>
ObWkbConstIterator<T, O>::ObWkbConstIterator(const self& iter, bool do_array_assign)
  : idx_(iter.idx_),
    owner_(iter.owner_), diff_info_(iter.diff_info_),
    diff_info_ptr_(&diff_info_), offsets_(), offsets_ptr_(&offsets_)
{
  if (do_array_assign) {
      offsets_.assign(iter.offsets_);
  }
}

// shift iter interface
template<typename T, typename O>
typename ObWkbConstIterator<T, O>::self& ObWkbConstIterator<T, O>::operator++()
{
  move(*this, 1, false);
  return *this;
}

template<typename T, typename O>
typename ObWkbConstIterator<T, O>::self ObWkbConstIterator<T, O>::operator++(int)
{
  self iter(*this, false);
  move(*this, 1, false);
  return iter;
}

template<typename T, typename O>
typename ObWkbConstIterator<T, O>::self& ObWkbConstIterator<T, O>::operator--()
{
  move(*this, 1, true);
  return *this;
}

template<typename T, typename O>
typename ObWkbConstIterator<T, O>::self ObWkbConstIterator<T, O>::operator--(int)
{
  self iter(*this, false);
  move(*this, 1, true);
  return iter;
}

template<typename T, typename O>
const typename ObWkbConstIterator<T, O>::self& ObWkbConstIterator<T, O>::operator=(const self& iter)
{
  idx_ = iter.idx_;
  owner_ = iter.owner_;
  diff_info_ = iter.diff_info_;
  // TODO: do vector copy?
  offsets_ = iter.offsets_;
  return iter;
}

template<typename T, typename O>
typename ObWkbConstIterator<T, O>::self ObWkbConstIterator<T, O>::operator+(difference_type diff) const
{
  self iter(*this);
  move(iter, diff, false);
  return iter;
}

template<typename T, typename O>
const typename ObWkbConstIterator<T, O>::self& ObWkbConstIterator<T, O>::operator+=(difference_type diff)
{
  move(*this, diff, false);
  return *this;
}

template<typename T, typename O>
typename ObWkbConstIterator<T, O>::self ObWkbConstIterator<T, O>::operator-(difference_type diff) const
{
  self iter(*this);
  move(iter, diff, true);
  return iter;
}

template<typename T, typename O>
const typename ObWkbConstIterator<T, O>::self& ObWkbConstIterator<T, O>::operator-=(difference_type diff)
{
  move(*this, diff, true);
  return *this;
}

// distance iter interface
template<typename T, typename O>
typename ObWkbConstIterator<T, O>::difference_type ObWkbConstIterator<T, O>::operator-(const self& iter) const
{
  return idx_ - iter.idx_;
}

// retrieve value interface
template<typename T, typename O>
typename ObWkbConstIterator<T, O>::reference ObWkbConstIterator<T, O>::operator*() const
{
  self* ptr = const_cast<self*>(this);
  this->update_val(*ptr);
  return *(diff_info_.last_addr_);
}

template<typename T, typename O>
typename ObWkbConstIterator<T, O>::pointer ObWkbConstIterator<T, O>::operator->() const
{
  self* ptr = const_cast<self*>(this);
  this->update_val(*ptr);
  return (diff_info_.last_addr_);
}

template<typename T, typename O>
typename ObWkbConstIterator<T, O>::reference ObWkbConstIterator<T, O>::operator[](difference_type diff) const
{
  self iter(*this, false);
  this->move(iter, diff, false);
  iter.diff_info_ptr_ = this->diff_info_ptr_;
  iter.offsets_ptr_ = this->offsets_ptr_;
  this->update_val(iter);
  return *(diff_info_.last_addr_);
}

template<typename T, typename O>
void ObWkbConstIterator<T, O>::move(self& iter, difference_type step, bool back) const
{
  if (back) {
    step = -step;
  }
  index_type new_idx = iter.idx_ + step;
  index_type idx_min = iter.owner_->iter_idx_min();
  index_type idx_max = iter.owner_->iter_idx_max();
  if (new_idx < idx_min) {
    new_idx = idx_min - 1;
  } else if (new_idx >= idx_max) {
    new_idx = idx_max;
  }
  iter.idx_ = new_idx;
}

template<typename T, typename O>
void ObWkbConstIterator<T, O>::update_val(self& iter) const
{
  pointer data = NULL;
  iter.owner_->get_sub_addr(iter.diff_info_ptr_->last_addr_,
                            iter.diff_info_ptr_->last_idx_,
                            iter.idx_, iter.offsets_ptr_, data);
  // update info
  iter.diff_info_ptr_->last_addr_ = data;
  iter.diff_info_ptr_->last_idx_ = iter.idx_;
}


// iterator
// override shift functions
template<typename T, typename O>
typename ObWkbIterator<T, O>::self& ObWkbIterator<T, O>::operator++()
{
  this->move(*this, 1, false);
  return *this;
}

template<typename T, typename O>
typename ObWkbIterator<T, O>::self ObWkbIterator<T, O>::operator++(int)
{
  self iter(*this, false);
  this->move(*this, 1, false);
  return iter;
}

template<typename T, typename O>
typename ObWkbIterator<T, O>::self& ObWkbIterator<T, O>::operator--()
{
  this->move(*this, 1, true);
  return *this;
}

template<typename T, typename O>
typename ObWkbIterator<T, O>::self ObWkbIterator<T, O>::operator--(int)
{
  self iter(*this, false);
  this->move(*this, 1, true);
  return iter;
}

template<typename T, typename O>
const typename ObWkbIterator<T, O>::self& ObWkbIterator<T, O>::operator=(const self& iter)
{
  base::operator=(iter);
  return iter;
}

template<typename T, typename O>
typename ObWkbIterator<T, O>::self ObWkbIterator<T, O>::operator+(difference_type diff) const
{
  self iter(*this);
  this->move(iter, diff, false);
  return iter;
}

template<typename T, typename O>
const typename ObWkbIterator<T, O>::self& ObWkbIterator<T, O>::operator+=(difference_type diff)
{
  this->move(*this, diff, false);
  return *this;
}

template<typename T, typename O>
typename ObWkbIterator<T, O>::self ObWkbIterator<T, O>::operator-(difference_type diff) const
{
  self iter(*this);
  this->move(iter, diff, true);
  return iter;
}

template<typename T, typename O>
const typename ObWkbIterator<T, O>::self& ObWkbIterator<T, O>::operator-=(difference_type diff)
{
  this->move(*this, diff, true);
  return *this;
}

template<typename T, typename O>
typename ObWkbIterator<T, O>::difference_type ObWkbIterator<T, O>::operator-(const self& iter) const
{
  return base::operator-(iter);
}

// override retrieve functions
template<typename T, typename O>
typename ObWkbIterator<T, O>::reference ObWkbIterator<T, O>::operator*()
{
  this->update_val(*this);
  return *(this->diff_info_.last_addr_);
}

template<typename T, typename O>
typename ObWkbIterator<T, O>::pointer ObWkbIterator<T, O>::operator->()
{
  this->update_val(*this);
  return (this->diff_info_.last_addr_);
}

template<typename T, typename O>
typename ObWkbIterator<T, O>::reference ObWkbIterator<T, O>::operator[](difference_type diff)
{
  self iter(*this, false);
  this->move(iter, diff, false);
  iter.diff_info_ptr_ = this->diff_info_ptr_;
  iter.offsets_ptr_ = this->offsets_ptr_;
  this->update_val(iter);
  return *(this->diff_info_.last_addr_);
}


template<typename T>
typename T::size_type ObWkbUtils::sum_sub_size(const T& obj) {
  typename T::size_type s = 0;
  typename T::const_iterator iter = obj.begin();
  typename T::const_iterator ei = obj.end();
  for (; iter != ei; ++iter) {
    s += obj.get_sub_size(iter.operator->());
  }
  return s;
}

template<typename T>
void ObWkbUtils::get_sub_addr_common(const T& obj,
                                    typename T::const_pointer last_addr,
                                    typename T::index_type last_idx,
                                    typename T::index_type cur_idx,
                                    ObWkbIterOffsetArray* offsets,
                                    typename T::pointer& data)
{
  // init or reverse offset, search from head
  INIT_SUCC(ret);
  bool enable_offset_info = (offsets != NULL);
  bool need_do_scan = false;
  char* scan_ptr = nullptr;
  uint32_t offset = 0;
  typename T::index_type st = 0;
  typename T::index_type et = cur_idx;
  if (last_idx == -1) {
    scan_ptr = obj.ptr();
    offset = obj.data_offset();
    et = obj.et(cur_idx);
    need_do_scan = true;
  } else if (last_idx == cur_idx) {
    scan_ptr = reinterpret_cast<char*>(const_cast<typename T::pointer>(last_addr));
  } else if (last_idx < cur_idx) {
    scan_ptr = reinterpret_cast<char*>(const_cast<typename T::pointer>(last_addr));
    st = last_idx;
    need_do_scan = true;
    // check last idx should not bigger than offsets size
    if (enable_offset_info && st > offsets->count()) {
      COMMON_LOG(WARN, "Invalid situation for last idx bigger than offsets size!",
                 K(OB_ERR_INTERVAL_INVALID), K(st), K(offsets->count()));
      enable_offset_info = false;
    }
  } else {
    if (!enable_offset_info || offsets->count() < last_idx) {
      scan_ptr = obj.ptr();
      offset = obj.data_offset();
      et = obj.et(cur_idx);
      need_do_scan = true;
    } else {
      scan_ptr = obj.ptr();
      offset = offsets->operator[](obj.et(cur_idx));
    }
  }
  if (need_do_scan) {
    // TODO: prealloc whole array?
    if (enable_offset_info && OB_FAIL(offsets->prepare_allocate(obj.iter_idx_max()))) {
      COMMON_LOG(WARN, "failed to reserve for offsets!", K(ret), K(obj.iter_idx_max()));
    }
    // assum offsets -> prepare_allocate will do memset 0
    uint64_t base_offset = (enable_offset_info) ?  offsets->operator[](st) : 0;
    for (typename T::index_type i = st; i < et; i++) {
      if (enable_offset_info) {
        offsets->operator[](i) = base_offset + offset;
      }
      typename T::pointer sub_gis = reinterpret_cast<typename T::pointer>(scan_ptr + offset);
      offset += obj.get_sub_size(sub_gis);
    }
    // log et
    if (enable_offset_info) {
      offsets->operator[](et) = base_offset + offset;
    }
  }
  data = reinterpret_cast<typename T::pointer>(scan_ptr + offset);
}

} // namespace common
} // namespace oceanbase