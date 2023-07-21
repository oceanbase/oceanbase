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

#ifndef OCEANBASE_COMMON_OB_VECTOR_H_
#define OCEANBASE_COMMON_OB_VECTOR_H_

#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
namespace oceanbase
{
namespace common
{

template <typename T>
struct ob_vector_traits
{
public:
  typedef T &pointee_type;
  typedef T value_type;
  typedef const T &const_value_type;
  typedef value_type *iterator;
  typedef const value_type *const_iterator;
  typedef int64_t difference_type;
};

template <typename T>
struct ob_vector_traits<T *>
{
  public:
  typedef T pointee_type;
  typedef T *value_type;
  typedef const T *const_value_type;
  typedef value_type *iterator;
  typedef const value_type *const_iterator;
  typedef int64_t difference_type;
};

template <typename T>
struct ob_vector_traits<const T *>
{
  typedef T pointee_type;
  typedef const T *value_type;
  typedef const T *const const_value_type;
  typedef value_type *iterator;
  typedef const value_type *const_iterator;
  typedef int64_t difference_type;
};

template <>
struct ob_vector_traits<int64_t>
{
  typedef int64_t &pointee_type;
  typedef int64_t value_type;
  typedef const int64_t const_value_type;
  typedef value_type *iterator;
  typedef const value_type *const_iterator;
  typedef int64_t difference_type;
};

template <>
struct ob_vector_traits<double>
{
  typedef double &pointee_type;
  typedef double value_type;
  typedef const double const_value_type;
  typedef value_type *iterator;
  typedef const value_type *const_iterator;
  typedef double difference_type;
};

template <typename T, typename Allocator = PageArena<T> >
class ObVector
{
public:
  typedef typename ob_vector_traits<T>::pointee_type pointee_type;
  typedef typename ob_vector_traits<T>::value_type value_type;
  typedef typename ob_vector_traits<T>::const_value_type const_value_type;
  typedef typename ob_vector_traits<T>::iterator iterator;
  typedef typename ob_vector_traits<T>::const_iterator const_iterator;
  typedef typename ob_vector_traits<T>::difference_type difference_type;
public:
  explicit ObVector(Allocator *alloc = NULL, const lib::ObLabel &label = ObModIds::OB_MOD_DO_NOT_USE_ME);
  explicit ObVector(int64_t size, Allocator *alloc = NULL,
                    const lib::ObLabel &label = ObModIds::OB_MOD_DO_NOT_USE_ME);
  explicit ObVector(int64_t size, Allocator *alloc,
                    const lib::ObMemAttr &attr);
  virtual ~ObVector();
  int assign(const ObVector &other);
  ObVector<T, Allocator> &operator=(const ObVector<T, Allocator> &other);
  explicit ObVector(const ObVector<T, Allocator> &other);
public:
  int32_t size() const { return static_cast<int32_t>(mem_end_ - mem_begin_); }
  int64_t capacity() const { return static_cast<int64_t>(mem_end_of_storage_ - mem_begin_); }
  int64_t remain() const { return static_cast<int64_t>(mem_end_of_storage_ - mem_end_); }
public:
  iterator begin() const { return mem_begin_; }
  iterator end() const { return mem_end_; }
  iterator last() const { return mem_end_ - 1; }
  Allocator &get_allocator() const { return *pallocator_; }
  void set_allocator(Allocator &allc) { pallocator_ = &allc; }
  void set_label(const lib::ObLabel &label) { pallocator_->set_label(label); }


  inline bool at(const int64_t index, value_type &v) const
  {
    bool bret = false;
    if (index >= 0 && index < size()) {
      v = *(mem_begin_ + index);
      bret = true;
    }
    return bret;
  }

  inline value_type &at(const int64_t index) const
  {
    if (OB_UNLIKELY(index < 0 || index >= size())) {
      COMMON_LOG_RET(ERROR, OB_ARRAY_OUT_OF_RANGE, "invalid index", K(index));
    }
    return *(mem_begin_ + index);
  }

  inline value_type &operator[](const int64_t index) const
  {
    return at(index);
  }

public:
  int push_back(const_value_type value);
  int insert(iterator pos, const_value_type value);
  int replace(iterator pos, const_value_type value, value_type &replaced_value);
  iterator find(const_value_type value);
  template <typename Equal>
  iterator find_if(const_value_type value, Equal equal);
  int remove(iterator pos);
  int remove(const int64_t index);
  int remove(iterator start_pos, iterator end_pos);
  int remove_if(const_value_type value);
  template <typename ValueType, typename Predicate>
  int remove_if(const ValueType &value, Predicate predicate);
  template <typename ValueType, typename Predicate>
  int remove_if(const ValueType &value, Predicate predicate, value_type &removed_value);
  int reserve(int64_t size) { return expand(size); }
  void clear();
  inline void reset() { mem_end_ = mem_begin_; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  void destroy();
  int expand(int64_t size);
private:
  iterator  alloc_array(const int64_t size);

  static iterator fill(iterator ptr, const_value_type value);
  static iterator copy(iterator dest, const_iterator begin, const_iterator end);
  static iterator move(iterator dest, const_iterator begin, const_iterator end);
protected:
  iterator mem_begin_;
  iterator mem_end_;
  iterator mem_end_of_storage_;
  mutable Allocator default_allocator_;
  Allocator *pallocator_;
};

template <typename T, typename Allocator = PageArena<T> >
class ObSortedVector
{
public:
  typedef Allocator allocator_type;
  typedef ObVector<T, Allocator> vector_type;
  typedef typename vector_type::iterator iterator;
  typedef typename vector_type::const_iterator const_iterator;
  typedef typename vector_type::value_type value_type;
  typedef typename vector_type::const_value_type const_value_type;
  typedef typename vector_type::difference_type difference_type;

public:
  ObSortedVector() {}
  explicit ObSortedVector(int64_t size, Allocator *alloc = NULL,
                          const lib::ObLabel &label = ObModIds::OB_MOD_DO_NOT_USE_ME)
      : vector_(size, alloc, label) {}
  ObSortedVector(int64_t size, Allocator *alloc,
                 const lib::ObMemAttr &attr)
      : vector_(size, alloc, attr) {}
  virtual ~ObSortedVector() {}
  int assign(const ObSortedVector &other)
  {
    int ret = OB_SUCCESS;
    if (this != &other) {
      ret = vector_.assign(other.vector_);
      if (OB_FAIL(ret)) {
        COMMON_LOG(WARN, "assign vector failed", K(ret));
      }
    }
    return ret;
  }
  ObSortedVector<T, Allocator> &operator=(const ObSortedVector<T, Allocator> &other)
  {
    int ret = assign(other);
    if (OB_FAIL(ret)) {
      COMMON_LOG(WARN, "assign failed", K(ret));
    }
    return *this;
  }
  explicit ObSortedVector(const ObSortedVector<T, Allocator> &other)
  {
    int ret = assign(other);
    if (OB_FAIL(ret)) {
      COMMON_LOG(WARN, "assign failed", K(ret));
    }
  }

public:
  inline int32_t size() const { return vector_.size(); }
  inline int64_t count() const { return vector_.size(); }
  inline int64_t capacity() const { return vector_.capacity(); }
  inline int64_t remain() const { return vector_.remain(); }
  inline int reserve(int64_t size) { return vector_.reserve(size); }
  inline int remove(iterator pos) { return vector_.remove(pos); }
  inline void clear() { vector_.clear(); }
  inline void reset() { vector_.reset(); };
  inline int push_back(const_value_type value) { return vector_.push_back(value); }
  int64_t to_string(char *buf, const int64_t buf_len)const {return vector_.to_string(buf, buf_len);}

public:
  inline iterator begin() const { return vector_.begin(); }
  inline iterator end() const { return vector_.end(); }
  inline iterator last() const { return vector_.last(); }
  inline Allocator &get_allocator() const { return vector_.get_allocator(); }

public:
  inline bool at(const int64_t index, value_type &v) const
  {
    return vector_.at(index, v);
  }

  inline value_type &at(const int64_t index) const
  {
    return vector_.at(index);
  }

  inline value_type &operator[](const int64_t index) const
  {
    return at(static_cast<int64_t>(index));
  }

public:
  template <typename Compare>
  int insert(const_value_type value, iterator &insert_pos, Compare compare);
  /**
   * WARNING: dangerous function, if entry is existent, overwrite,
   * else insert. we don't free the memory of the entry, be careful.
   */
  template <typename Compare, typename Equal>
  int replace(const_value_type value, iterator &replace_pos,
              Compare compare, Equal equal, value_type &replaced_value);
  template <typename Compare, typename Unique>
  int insert_unique(const_value_type value,
                    iterator &insert_pos, Compare compare, Unique unique);
  template <typename Compare>
  int find(const_value_type value, iterator &pos, Compare compare) const;

  template <typename ValueType, typename Compare>
  iterator lower_bound(const ValueType &value, Compare compare) const;
  template <typename ValueType, typename Compare>
  iterator upper_bound(const ValueType &value, Compare compare) const;

  template <typename ValueType, typename Compare, typename Equal>
  int find(const ValueType &value, iterator &pos, Compare compare, Equal equal) const;
  template <typename ValueType, typename Compare, typename Equal>
  int remove_if(const ValueType &value, Compare comapre, Equal equal);
  template <typename ValueType, typename Compare, typename Equal>
  int remove_if(const ValueType &value, Compare comapre, Equal equal, value_type &removed_value);

  int remove(iterator start_pos, iterator end_pos);

  template <typename Compare> void sort(Compare compare);

private:
  ObVector<T, Allocator> vector_;

};

} // end namespace common
} // end namespace oceanbase

#include "ob_vector.ipp"


#endif //OCEANBASE_ROOTSERVER_ROOTMETA_H_
