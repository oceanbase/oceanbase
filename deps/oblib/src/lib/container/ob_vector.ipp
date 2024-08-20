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

#include <algorithm>

namespace oceanbase
{
namespace common
{

// --------------------------------------------------------
// class ObVector<T, Allocator> implements
// --------------------------------------------------------
template <typename T, typename Allocator>
ObVector<T, Allocator>::ObVector(Allocator *alloc, const lib::ObLabel &label)
    : mem_begin_(NULL), mem_end_(NULL), mem_end_of_storage_(NULL)
{
  if (NULL == alloc) {
    pallocator_ = &default_allocator_;
    pallocator_->set_label(label);
  } else {
    pallocator_ = alloc;
  }
}

template <typename T, typename Allocator>
ObVector<T, Allocator>::ObVector(int64_t size, Allocator *alloc, const lib::ObLabel &label)
    : ObVector(size, alloc, ObMemAttr(common::OB_SERVER_TENANT_ID, label)) {}

template <typename T, typename Allocator>
ObVector<T, Allocator>::ObVector(int64_t size, Allocator *alloc, const lib::ObMemAttr &attr)
    : mem_begin_(NULL), mem_end_(NULL), mem_end_of_storage_(NULL)
{
  if (NULL == alloc) {
    pallocator_ = &default_allocator_;
    pallocator_->set_attr(attr);
  } else {
    pallocator_ = alloc;
  }
  int ret = expand(size);
  if (OB_FAIL(ret)) {
    COMMON_LOG(WARN, "expand failed", K(ret), K(size));
  }
}

template <typename T, typename Allocator>
ObVector<T, Allocator>::~ObVector()
{
  destroy();
}

template <typename T, typename Allocator>
ObVector<T, Allocator>::ObVector(const ObVector<T, Allocator> &other)
    : mem_begin_(NULL), mem_end_(NULL), mem_end_of_storage_(NULL),
      pallocator_(&default_allocator_)
{
  pallocator_->set_label(ObModIds::VECTOR);
  *this = other;
}

template <typename T, typename Allocator>
int ObVector<T, Allocator>::assign(const ObVector<T, Allocator> &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    destroy();
    if (other.size() > 0) {
      if (OB_SUCCESS != (ret = expand(other.size()))) {
        COMMON_LOG(WARN, "expand failed", K(ret), "size", other.size());
      } else {
        copy(mem_begin_, other.begin(), other.end());
        mem_end_ = mem_begin_ + other.size();
      }
    }
  }
  return ret;
}

template <typename T, typename Allocator>
ObVector<T, Allocator> &ObVector<T, Allocator>::operator=(const ObVector<T, Allocator> &other)
{
  int ret = assign(other);
  if (OB_FAIL(ret)) {
    COMMON_LOG(WARN, "assign vector failed", K(ret));
  }
  return *this;
}

template <typename T, typename Allocator>
void ObVector<T, Allocator>::destroy()
{
  if (mem_begin_) {
    pallocator_->free(mem_begin_);
  }
  mem_begin_ = NULL;
  mem_end_ = NULL;
  mem_end_of_storage_ = NULL;
}

template <typename T, typename Allocator>
int ObVector<T, Allocator>::push_back(const_value_type value)
{
  return insert(end(), value);
}

template <typename T, typename Allocator>
void ObVector<T, Allocator>::clear()
{
  pallocator_->free(mem_begin_);
  if (pallocator_ == &default_allocator_) {
    pallocator_->free();
  }
  mem_end_ = mem_begin_ = mem_end_of_storage_ = NULL;
}

/**
 * expand %size bytes memory when buffer not enough.
 * copy origin memory content to new buffer
 * @return
 * <  0  allocate new memory failed
 * == 0  no need expand
 * == %size  expand succeed.
 */
template <typename T, typename Allocator>
int ObVector<T, Allocator>::expand(int64_t size)
{
  int64_t old_size = ObVector<T, Allocator>::size();
  int ret = OB_SUCCESS;
  if (old_size < size) {
    iterator new_mem = alloc_array(size);
    if (NULL == new_mem) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "alloc memory failed", K(ret), K(size));
    } else {
      if (old_size != 0) {
        copy(new_mem, mem_begin_, mem_end_);
      }

      // deallocate old memory
      destroy();

      mem_begin_ = new_mem;
      mem_end_ = mem_begin_ + old_size;
      mem_end_of_storage_ = mem_begin_ + size;
    }
  }
  return ret;
}

template <typename T, typename Allocator>
typename ObVector<T, Allocator>::iterator ObVector<T, Allocator>::alloc_array(const int64_t size)
{
  iterator ptr = reinterpret_cast<iterator>
                 (pallocator_->alloc(size * sizeof(value_type)));
  return ptr;
}

template <typename T, typename Allocator>
typename ObVector<T, Allocator>::iterator ObVector<T, Allocator>::fill(iterator ptr,
                                                                       const_value_type value)
{
  if (NULL != ptr) {
    //*ptr = value;
    MEMCPY(ptr, &value, sizeof(value_type));
  }
  return ptr;
}

/*
 * [dest, x] && [begin, end] can be overlap
 * @param [in] dest: move dest memory
 * @param [in] begin: source memory start pointer
 * @param [in] end: source memory end pointer
 * @return
 */
template <typename T, typename Allocator>
typename ObVector<T, Allocator>::iterator ObVector<T, Allocator>::move(iterator dest,
                                                                       const_iterator begin, const_iterator end)
{
  // assert(dest);
  // assert(end >= begin);
  int64_t n = static_cast<int64_t>(end - begin);
  if (n > 0) {
    MEMMOVE(dest, begin, n * sizeof(value_type));
  }
  return dest + n;
}

/*
 * [dest, x] && [begin, end] cannot be overlap
 */
template <typename T, typename Allocator>
typename ObVector<T, Allocator>::iterator ObVector<T, Allocator>::copy(iterator dest,
                                                                       const_iterator begin, const_iterator end)
{
  // assert(dest);
  // assert(end >= begin);
  int64_t n = static_cast<int64_t>(end - begin);
  if (n > 0) {
    MEMCPY(dest, begin, n * sizeof(value_type));
  }
  return dest + n;
}

template <typename T, typename Allocator>
typename ObVector<T, Allocator>::iterator ObVector<T, Allocator>::find(const_value_type value)
{
  iterator pos = mem_begin_;
  while (pos != mem_end_) {
    if (*pos == value) { break; }
    ++pos;
  }

  return pos;
}

template <typename T, typename Allocator>
template <typename Equal>
typename ObVector<T, Allocator>::iterator ObVector<T, Allocator>::find_if(const_value_type value, Equal equal)
{
  iterator pos = mem_begin_;
  while (pos != mem_end_) {
    if (equal(*pos, value)) { break; }
    ++pos;
  }

  return pos;
}


template <typename T, typename Allocator>
int ObVector<T, Allocator>::remove(iterator pos)
{
  int ret = OB_SUCCESS;
  if (pos < mem_begin_ || pos >= mem_end_) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else {
    iterator new_end_pos = move(pos, pos + 1, mem_end_);
    // assert(mem_end_ == new_end_pos + 1);
    mem_end_ = new_end_pos;
  }
  return ret;
}

template <typename T, typename Allocator>
int ObVector<T, Allocator>::remove(iterator start_pos, iterator end_pos)
{
  int ret = OB_SUCCESS;
  if (start_pos < mem_begin_ || start_pos >= mem_end_
      || end_pos < mem_begin_ || end_pos > mem_end_) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else if (end_pos - start_pos > 0) {
    iterator new_end_pos = move(start_pos, end_pos, mem_end_);
    // assert(mem_end_ == new_end_pos + (end_pos - start_pos));
    mem_end_ = new_end_pos;
  }
  return ret;
}

template <typename T, typename Allocator>
int ObVector<T, Allocator>::remove(const int64_t index)
{
  int ret = OB_SUCCESS;
  if (index >= 0 && index < size()) {
    ret = remove(mem_begin_ + index);
  } else {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

template <typename T, typename Allocator>
int ObVector<T, Allocator>::remove_if(const_value_type value)
{
  int ret = OB_SUCCESS;
  iterator pos = mem_begin_;
  while (pos != mem_end_) {
    if (*pos == value) { break; }
    ++pos;
  }

  if (pos >= mem_end_) { ret = OB_ENTRY_NOT_EXIST; }
  else { ret = remove(pos); }

  return ret;
}

template <typename T, typename Allocator>
template <typename ValueType, typename Predicate>
int ObVector<T, Allocator>::remove_if(const ValueType &value, Predicate predicate)
{
  int ret = OB_SUCCESS;
  iterator pos = mem_begin_;
  while (pos != mem_end_) {
    if (predicate(*pos, value)) { break; }
    ++pos;
  }

  if (pos >= mem_end_) { ret = OB_ENTRY_NOT_EXIST; }
  else { ret = remove(pos); }

  return ret;
}

template <typename T, typename Allocator>
template <typename ValueType, typename Predicate>
int ObVector<T, Allocator>::remove_if(const ValueType &value, Predicate predicate,
                                      value_type &removed_value)
{
  int ret = OB_SUCCESS;
  iterator pos = mem_begin_;
  while (pos != mem_end_) {
    if (predicate(*pos, value)) { break; }
    ++pos;
  }

  if (pos >= mem_end_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    removed_value = *pos;
    ret = remove(pos);
  }

  return ret;
}

template <typename T, typename Allocator>
int ObVector<T, Allocator>::insert(iterator pos, const_value_type value)
{
  int ret = OB_SUCCESS;

  if (remain() >= 1) {
    if (pos == mem_end_) {
      fill(pos, value);
      mem_end_ += 1;
    } else {
      move(pos + 1, pos, mem_end_);
      fill(pos, value);
      mem_end_ += 1;
    }
  } else {
    // need expand
    int64_t old_size = size();
    int64_t new_size = (old_size + 1) << 1;
    iterator new_mem = alloc_array(new_size);
    if (!new_mem) {
      ret = OB_ALLOCATE_MEMORY_FAILED; 
    } else {
      iterator new_end = new_mem + old_size + 1;
      if (pos == mem_end_) {
        copy(new_mem, mem_begin_, mem_end_);
        fill(new_mem + old_size, value);
      } else {
        // copy head part;
        iterator pivot = copy(new_mem, mem_begin_, pos);
        // copy tail part;
        copy(pivot + 1, pos, mem_end_);
        fill(pivot, value);
      }

      // free old memory and reset pointers..
      destroy();
      mem_begin_ = new_mem;
      mem_end_ = new_end;
      mem_end_of_storage_ = new_mem + new_size;
    }
  }
  return ret;
}

template <typename T, typename Allocator>
int ObVector<T, Allocator>::replace(iterator pos, const_value_type value,
                                    value_type &replaced_value)
{
  int ret = OB_SUCCESS;
  if (pos >= mem_begin_ && pos < mem_end_) {
    replaced_value = *pos;
    fill(pos, value);
  } else {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

template <typename T, typename Allocator>
int64_t ObVector<T, Allocator>::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_ARRAY_START();
  iterator it = begin();
  for (; it != end(); ++it) {
    BUF_PRINTO(*it);
    if (it != last()) {
      J_COMMA();
    }
  }
  J_ARRAY_END();
  return pos;
}

// ObSortedVector
template <typename T, typename Allocator>
template <typename Compare>
int ObSortedVector<T, Allocator>::insert(const_value_type value,
                                         iterator &insert_pos, Compare compare)
{
  int ret = OB_SUCCESS;
  if (NULL == value) { ret = OB_ERROR; }
  insert_pos = vector_.end();
  if (OB_SUCC(ret)) {
    iterator find_pos = std::lower_bound(vector_.begin(), vector_.end(), value, compare);
    insert_pos = find_pos;

    ret = vector_.insert(insert_pos, value);
  }
  return ret;
}

template <typename T, typename Allocator>
template <typename Compare, typename Equal>
int ObSortedVector<T, Allocator>::replace(const_value_type value,
                                          iterator &replace_pos, Compare compare, Equal equal, value_type &replaced_value)
{
  int ret = OB_SUCCESS;
  replace_pos = vector_.end();
  if (NULL != value) {
    iterator find_pos = std::lower_bound(vector_.begin(), vector_.end(), value, compare);
    if (find_pos != end() && equal(*find_pos, value)) {
      //existent, overwrite
      ret = vector_.replace(find_pos, value, replaced_value);
    } else {
      //non-existent, insert
      ret = vector_.insert(find_pos, value);
    }
    if (OB_SUCC(ret)) {
      replace_pos = find_pos;
    }
  } else {
    ret = OB_ERROR;
  }
  return ret;
}

template <typename T, typename Allocator>
template <typename Compare, typename Unique>
int ObSortedVector<T, Allocator>::insert_unique(const_value_type value,
                                                iterator &insert_pos, Compare compare, Unique unique)
{
  int ret = OB_SUCCESS;
  if (NULL == value) { ret = OB_ERROR; }
  iterator begin_iterator = begin();
  iterator end_iterator = end();
  insert_pos = end_iterator;
  if (OB_SUCC(ret)) {
    iterator find_pos = std::lower_bound(begin_iterator,
                                         end_iterator, value, compare);
    insert_pos = find_pos;
    iterator compare_pos = find_pos;
    iterator prev_pos = end_iterator;

    if (find_pos == end_iterator && size() > 0) {
      compare_pos = last();
    }
    if (find_pos > begin_iterator && find_pos < end_iterator) {
      prev_pos = find_pos - 1;
    }

    if (compare_pos >= begin_iterator && compare_pos < end_iterator) {
      if (unique(*compare_pos, value)) {
        insert_pos = compare_pos;
        ret = OB_CONFLICT_VALUE;
      }
    }

    if (OB_SUCCESS == ret && prev_pos >= begin_iterator && prev_pos < end_iterator) {
      if (unique(*prev_pos, value)) {
        insert_pos = prev_pos;
        ret = OB_CONFLICT_VALUE;
      }
    }


    if (OB_SUCC(ret)) {
      ret = vector_.insert(insert_pos, value);
    }
  }
  return ret;
}

template <typename T, typename Allocator>
template <typename Compare>
int ObSortedVector<T, Allocator>::find(const_value_type value,
                                       iterator &pos, Compare compare) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  pos = std::lower_bound(begin(), end(), value, compare);
  if (pos != end()) {
    if (!compare(*pos, value) && !compare(value, *pos)) {
      ret = OB_SUCCESS;
    } else {
      pos = end();
    }
  }
  return ret;
}

template <typename T, typename Allocator>
template <typename ValueType, typename Compare, typename Equal>
int ObSortedVector<T, Allocator>::find(const ValueType &value,
                                       iterator &pos, Compare compare, Equal equal) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  pos = std::lower_bound(begin(), end(), value, compare);
  if (pos != end()) {
    if (equal(*pos, value)) {
      ret = OB_SUCCESS;
    } else {
      pos = end();
    }
  }
  return ret;
}

template <typename T, typename Allocator>
template <typename ValueType, typename Compare>
typename ObSortedVector<T, Allocator>::iterator
ObSortedVector<T, Allocator>::lower_bound(const ValueType &value, Compare compare) const
{
  return std::lower_bound(begin(), end(), value, compare);
}

template <typename T, typename Allocator>
template <typename ValueType, typename Compare>
typename ObSortedVector<T, Allocator>::iterator
ObSortedVector<T, Allocator>::upper_bound(const ValueType &value, Compare compare) const
{
  return std::upper_bound(begin(), end(), value, compare);
}

template <typename T, typename Allocator>
template <typename ValueType, typename Compare, typename Equal>
int ObSortedVector<T, Allocator>::remove_if(const ValueType &value,
                                            Compare comapre, Equal equal)
{
  iterator pos = end();
  int ret = find(value, pos, comapre, equal);
  if (OB_SUCC(ret)) {
    ret = vector_.remove(pos);
  }
  return ret;
}

template <typename T, typename Allocator>
template <typename ValueType, typename Compare, typename Equal>
int ObSortedVector<T, Allocator>::remove_if(const ValueType &value,
                                            Compare comapre, Equal equal, value_type &removed_value)
{
  iterator pos = end();
  int ret = find(value, pos, comapre, equal);
  if (OB_SUCC(ret)) {
    removed_value = *pos;
    ret = vector_.remove(pos);
  }
  return ret;
}

template <typename T, typename Allocator>
int ObSortedVector<T, Allocator>::remove(iterator start_pos, iterator end_pos)
{
  int ret = OB_SUCCESS;
  if (end_pos - start_pos > 0) {
    ret = vector_.remove(start_pos, end_pos);
  }
  return ret;
}

template <typename T, typename Allocator>
template <typename Compare>
void ObSortedVector<T, Allocator>::sort(Compare compare)
{
  lib::ob_sort(begin(), end(), compare);
}

} // end namespace common
} // end namespace oceanbase
