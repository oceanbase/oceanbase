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

#ifndef OCEANBASE_LIB_CORE_LOCAL_OB_CORE_LOCAL_STORAGE_
#define OCEANBASE_LIB_CORE_LOCAL_OB_CORE_LOCAL_STORAGE_

#include "lib/ob_errno.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/cpu/ob_cpu_topology.h"

#define VAL_ARRAY_AT(T, i) \
    (*reinterpret_cast<T *>(reinterpret_cast<char *>(val_array_) + i * ITEM_SIZE))

namespace oceanbase
{
namespace common
{
template <class T>
class ObCoreLocalStorage
{
public:
  ObCoreLocalStorage();
  ~ObCoreLocalStorage();
  inline int init(int64_t array_len = INT64_MAX);
  inline int destroy();
  inline int get_value(T& val) const;
  inline int get_value(int64_t index, T &val) const;
  inline int set_value(const T & val);
  inline int set_value(int64_t index, const T &val);
  inline int64_t get_array_idx() const;
protected:
  inline int check_inited() const;

  DISALLOW_COPY_AND_ASSIGN(ObCoreLocalStorage);
protected:
  static const int64_t ITEM_SIZE = sizeof(T) > CACHE_ALIGN_SIZE ? sizeof(T) : CACHE_ALIGN_SIZE;
  static const int64_t STORAGE_SIZE_TIMES = 2;
  void * val_array_;
  int64_t core_num_;
  int64_t array_len_;
  bool is_inited_;
} CACHE_ALIGNED;

class ObCoreLocalPtr : public ObCoreLocalStorage<void *>
{
public:
  inline int cas_value(void * & old_val, void * & new_val);
} CACHE_ALIGNED;


template <class T>
ObCoreLocalStorage<T>::ObCoreLocalStorage() : val_array_(NULL), core_num_(0), array_len_(0), is_inited_(false) {}

template <class T>
ObCoreLocalStorage<T>::~ObCoreLocalStorage()
{
  destroy();
}

template <class T>
int ObCoreLocalStorage<T>::init(int64_t array_len/* = INT64_MAX*/)
{
  int ret = OB_SUCCESS;
  core_num_ = get_cpu_count();
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LIB_LOG(WARN, "init twice", K(ret));
  } else if (OB_UNLIKELY(array_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN , "invalid argument", K(array_len));
  } else if (INT64_MAX == array_len) {
    array_len_ = core_num_ * STORAGE_SIZE_TIMES;
  } else {
    array_len_ = array_len;
  }
  if (OB_SUCC(ret)) {
    if (NULL == (val_array_ = ob_malloc(ITEM_SIZE * array_len_,
                                        ObModIds::OB_CORE_LOCAL_STORAGE))) {
      LIB_LOG(ERROR, "ob_malloc failed", K(ITEM_SIZE * array_len_));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      for (int64_t i = 0; i < array_len_; i++) {
        new (&VAL_ARRAY_AT(T, i)) T();
      }
      is_inited_ = true;
    }
  }
  return ret;
}

template <class T>
int ObCoreLocalStorage<T>::destroy()
{
  int ret = OB_SUCCESS;
  if (NULL != val_array_) {
    for (int64_t i = 0; i < array_len_; i++) {
      VAL_ARRAY_AT(T, i).~T();
    }
    ob_free(val_array_);
    val_array_ = NULL;
  }
  core_num_ = 0;
  array_len_ = 0;
  is_inited_ = false;
  return ret;
}

template <class T>
inline int ObCoreLocalStorage<T>::get_value(T& val) const
{
  int ret = OB_SUCCESS;
  int64_t array_idx = get_array_idx();
  if (OB_FAIL(check_inited())) {
  } else if (OB_ISNULL(val_array_) || OB_UNLIKELY(array_idx < 0) || OB_UNLIKELY(array_idx >= array_len_)) {
    LIB_LOG(ERROR, "get_value failed", K_(val_array), K(array_idx), K_(array_len));
    ret = OB_ERR_UNEXPECTED;
  } else {
     val = VAL_ARRAY_AT(T, array_idx);
  }
  return ret;
}

template <class T>
inline int ObCoreLocalStorage<T>::get_value(int64_t index, T &val) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inited())) {
  } else if (NULL == val_array_ || index < 0 || index >= array_len_) {
    LIB_LOG(ERROR, "get value failed", K_(val_array), K(index), K_(array_len));
    ret = OB_ERR_UNEXPECTED;
  } else {
    val = VAL_ARRAY_AT(T, index);
  }
  return ret;
}

template <class T>
inline int ObCoreLocalStorage<T>::set_value(const T & val)
{
  int ret = OB_SUCCESS;
  int64_t array_idx = get_array_idx();
  if (OB_FAIL(check_inited())) {
  } else if (NULL == val_array_ || array_idx < 0 || array_idx >= array_len_) {
    LIB_LOG(ERROR, "set_value failed", K_(val_array), K(array_idx), K_(array_len));
    ret = OB_ERR_UNEXPECTED;
  } else {
    VAL_ARRAY_AT(T, array_idx) = val;
  }
  return ret;
}

template <class T>
inline int ObCoreLocalStorage<T>::set_value(int64_t index, const T &val)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inited())) {
  } else if (NULL == val_array_ || index < 0 || index >= array_len_) {
    LIB_LOG(ERROR, "set value failed", K_(val_array), K(index), K_(array_len));
    ret = OB_ERR_UNEXPECTED;
  } else {
    VAL_ARRAY_AT(T, index) = val;
  }
  return ret;
}

template <class T>
inline int64_t ObCoreLocalStorage<T>::get_array_idx() const
{
  int64_t array_idx = get_itid() % array_len_;
  return array_idx;
}

template <class T>
inline int ObCoreLocalStorage<T>::check_inited() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ObCoreLocalStorage has not been initialized", K(ret));
  }
  return ret;
}

inline int ObCoreLocalPtr::cas_value(void * & old_val, void * & new_val)
{
  int ret = OB_SUCCESS;
  int64_t array_idx = get_array_idx();
  if (OB_FAIL(check_inited())) {
  } else if (NULL == val_array_ || array_idx < 0 || array_idx >= array_len_) {
    LIB_LOG(ERROR, "cas_value failed", K_(val_array), K(array_idx), K_(array_len));
    ret = OB_ERR_UNEXPECTED;
  } else if (!__sync_bool_compare_and_swap(&VAL_ARRAY_AT(void *, array_idx), old_val, new_val)) {
    ret = OB_EAGAIN;
  }
  return ret;
}

}//namespace common
}//namespace oceanbase

#endif //OCEANBASE_LIB_CORE_LOCAL_OB_CORE_LOCAL_STORAGE_
