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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_BIN_ITER_
#define OCEANBASE_LIB_GEO_OB_GEO_BIN_ITER_

#include "ob_geo.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_vector.h"

namespace oceanbase {
namespace common {

typedef common::ObSEArray<uint64_t, 64> ObWkbIterOffsetArray;

template<typename T, typename O>
class ObWkbConstIterator
{
public:
  // type define
  typedef ObWkbConstIterator<T, O> self;
  typedef O owner_t;
  typedef ptrdiff_t index_type;
  typedef T value_type;
  typedef ptrdiff_t difference_type;
  typedef difference_type distance_type;
  typedef typename owner_t::size_type size_type;
  // for access
  typedef value_type& reference;
  typedef value_type* pointer;
  // STL tag
  typedef std::random_access_iterator_tag iterator_category;
  // contructor
  ObWkbConstIterator();
  ObWkbConstIterator(index_type idx, const owner_t* owner);
  ObWkbConstIterator(self& iter, bool do_array_assign = true);
  ObWkbConstIterator(const self& iter, bool do_array_assign = true);
  ~ObWkbConstIterator() {}
  // compare iter interface
  bool operator==(const self& iter) const { return (idx_ == iter.idx_); }
  bool operator!=(const self& iter) const { return (idx_ != iter.idx_); }
  bool operator<(const self& iter) const { return (idx_ < iter.idx_); }
  bool operator>(const self& iter) const { return (idx_ > iter.idx_); }
  bool operator<=(const self& iter) const { return (idx_ <= iter.idx_); }
  bool operator>=(const self& iter) const { return (idx_ >= iter.idx_); }
  // shift iter interface
  self& operator++();
  self operator++(int);
  self& operator--();
  self operator--(int);
  const self& operator=(const self& iter);
  self operator+(difference_type diff) const;
  const self& operator+=(difference_type diff);
  self operator-(difference_type diff) const;
  const self& operator-=(difference_type diff);
  // distance iter interface
  difference_type operator-(const self& iter) const;
  // retrieve value interface
  reference operator*() const;
  pointer operator->() const;
  reference operator[](difference_type diff) const;
protected:
  // internal functino
  void move(self& iter, difference_type step, bool back) const;
  void update_val(self& iter) const;
  // external info for wkb
  class DiffInfo {
  public:
    DiffInfo() : last_idx_(-1), last_addr_(NULL) {}
    DiffInfo(DiffInfo& info) : last_idx_(info.last_idx_), last_addr_(info.last_addr_) {}
    DiffInfo(const DiffInfo& info) : last_idx_(info.last_idx_), last_addr_(info.last_addr_) {}
    ~DiffInfo() {};
    index_type last_idx_;
    pointer last_addr_;
  };
protected:
  index_type idx_;
  index_type idx_min_;
  index_type idx_max_;
  owner_t* owner_;
  DiffInfo diff_info_;
  DiffInfo* diff_info_ptr_;
  ObWkbIterOffsetArray offsets_;
  ObWkbIterOffsetArray* offsets_ptr_;
};


template<typename T, typename O>
class ObWkbIterator : public ObWkbConstIterator<T, O>
{
public:
  // type define
  typedef ObWkbConstIterator<T, O> base;
  typedef ObWkbIterator<T, O> self;
  typedef O owner_t;
  typedef ptrdiff_t index_type;
  typedef T value_type;
  typedef ptrdiff_t difference_type;
  typedef difference_type distance_type;
  typedef typename owner_t::size_type size_type;
  // for access
  typedef value_type& reference;
  typedef value_type* pointer;
  // STL tag
  typedef std::random_access_iterator_tag iterator_category;
  // contructor'
  ObWkbIterator() : base() {}
  ObWkbIterator(index_type idx, const owner_t* owner) : base(idx, owner) {}
  ObWkbIterator(base& iter, bool do_array_assign = true) : base(iter, do_array_assign) {}
  ObWkbIterator(const base& iter, bool do_array_assign = true) : base(iter, do_array_assign) {}
  ObWkbIterator(self& iter, bool do_array_assign = true) : base(iter, do_array_assign) {}
  ObWkbIterator(const self& iter, bool do_array_assign = true) : base(iter, do_array_assign) {}
  // override shift functions
  self& operator++();
  self operator++(int);
  self& operator--();
  self operator--(int);
  const self& operator=(const self& iter);
  self operator+(difference_type diff) const;
  const self& operator+=(difference_type diff);
  self operator-(difference_type diff) const;
  const self& operator-=(difference_type diff);
  difference_type operator-(const self& iter) const;
  // override retrieve functions
  reference operator*();
  pointer operator->();
  reference operator[](difference_type diff);
};

template<typename T, typename O>
ObWkbConstIterator<T, O> operator+(
  typename ObWkbConstIterator<T, O>::difference_type diff,
  const ObWkbConstIterator<T, O>& iter)
{
  ObWkbConstIterator<T, O> iter2 = iter;
  iter2 += diff;
  return iter2;
}

template<typename T, typename O>
ObWkbIterator<T, O> operator+(
  typename ObWkbIterator<T, O>::difference_type diff,
  const ObWkbIterator<T, O>& iter)
{
  ObWkbIterator<T, O> iter2 = iter;
  iter2 += diff;
  return iter2;
}

class ObWkbUtils{
public:
    template<typename T>
    static typename T::size_type sum_sub_size(const T& obj);

    template<typename T>
    static void get_sub_addr_common(const T& obj,
                                    typename T::const_pointer last_addr,
                                    typename T::index_type last_idx,
                                    typename T::index_type cur_idx,
                                    ObWkbIterOffsetArray* offsets,
                                    typename T::pointer& data);
};


} // namespace common
} // namespace oceanbase

#include "ob_geo_bin_iter.ipp"

#endif // OCEANBASE_LIB_GEO_OB_GEO_BIN_ITER_
