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

#ifndef OCEANBASE_HASH_OB_ARRAY_HASH_MAP_
#define OCEANBASE_HASH_OB_ARRAY_HASH_MAP_
#include "lib/allocator/ob_malloc.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{
template<typename key_t, typename val_t>
class ObArrayHashMap
{
public:
  typedef ObSpinLock Lock;
  typedef ObSpinLockGuard LockGuard;
  class Item
  {
  public:
    Item(): is_valid_(false), key_(), val_() {}
    Item(key_t &key, val_t &val): is_valid_(true), key_(key), val_(val) {}
    ~Item() {}
    void set(const key_t &key, const val_t &val)
    {
      is_valid_ = true;
      key_ = key;
      val_ = val;
    }
    bool is_empty() const { return !is_valid_; }
    bool equal(const key_t &key) const { return is_valid_ && key_ == key; }
    const key_t &get_key() const { return key_; }
    const val_t &get_val() const { return val_; }
  private:
    bool is_valid_;
    key_t key_;
    val_t val_;
  };

  ObArrayHashMap(): lock_(common::ObLatchIds::HASH_MAP_LOCK), size_(0), capacity_(0), items_(NULL) {}
  ~ObArrayHashMap() { destroy(); }

  int init(const lib::ObMemAttr attr, int64_t capacity)
  {
    int ret = OB_SUCCESS;
    if (capacity <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (is_inited()) {
      ret = OB_INIT_TWICE;
    } else if (NULL == (items_ = static_cast<Item *>(ob_malloc(capacity * sizeof(Item), attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      capacity_ = capacity;
      reset();
    }
    return ret;
  }

  int init(const lib::ObLabel label, int64_t capacity)
  {
    ObMemAttr attr;
    attr.label_ = label;
    return init(attr, capacity);
  }

  void destroy()
  {
    if (NULL != items_) {
      ob_free(items_);
      items_ = NULL;
    }
    size_ = 0;
    capacity_ = 0;
  }

  int64_t size() const { return size_; }
  static int64_t item_size() { return sizeof(Item); }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    if (!check_serialize_buf(buf, buf_len, pos)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(mem_chunk_serialize(buf, buf_len, pos, reinterpret_cast<char *>(items_),
                                           capacity_ * sizeof(Item)))) {
    }
    return ret;
  }

  int deserialize(const char *buf, const int64_t data_len, int64_t &pos)
  {
    int ret = OB_SUCCESS;
    int64_t real_size = 0;
    if (!check_serialize_buf(buf, data_len, pos)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(mem_chunk_deserialize(buf, data_len, pos, reinterpret_cast<char *>(items_),
                                             capacity_ * sizeof(Item), real_size))) {
    } else {
      capacity_ = real_size / sizeof(Item);
    }
    return ret;
  }

  int64_t get_serialize_size() const
  {
    int64_t buf_size = capacity_ * sizeof(Item);
    return buf_size + serialization::encoded_length_i64(buf_size);
  }

  void reset()
  {
    LockGuard guard(lock_);
    for (int64_t i = 0; i < capacity_; ++i) {
      new(items_ + i)Item();
    }
    size_ = 0;
  }

  void print() const
  {
    COMMON_LOG(INFO, "array_hash dump begin:", K(this));
    for (int64_t i = 0; i < capacity_; i++) {
      Item *pi = items_ + i;
      if (!pi->is_empty()) {
        COMMON_LOG(INFO, "array_hash", K(i), K(pi->get_key()), K(pi->get_val()));
      }
    }
  }

  int64_t to_string(char *buf, int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    for (int64_t i = 0; i < capacity_; i++) {
      Item *pi = items_ + i;
      if (!pi->is_empty()) {
        J_OBJ_START();
        J_KV("Key", pi->get_key(), " Value", pi->get_val());
        J_OBJ_END();
      }
    }
    J_OBJ_END();
    return pos;
  }

  int get(const key_t &key, val_t &val) const
  {
    int ret = OB_ENTRY_NOT_EXIST;
    LockGuard guard(lock_);
    if (!is_inited()) {
      ret = OB_NOT_INIT;
    } else {
      for (uint64_t start_idx = key.hash(), i = 0; OB_ENTRY_NOT_EXIST == ret &&
           i < (uint64_t)capacity_; i++) {
        Item *pi = items_ + ((start_idx + i) % capacity_);
        if (pi->is_empty()) {
          break; // slot is empty
        } else if (pi->equal(key)) {
          val = pi->get_val();
          ret = OB_SUCCESS;
        }
      }
    }
    return ret;
  }

  int insert(const key_t &key, const val_t &val)
  {
    int ret = OB_SIZE_OVERFLOW;
    LockGuard guard(lock_);
    if (!is_inited()) {
      ret = OB_NOT_INIT;
    } else {
      for (uint64_t start_idx = key.hash(), i = 0; OB_SIZE_OVERFLOW == ret &&
           i < (uint64_t)capacity_; ++i) {
        Item *pi = items_ + ((start_idx + i) % capacity_);
        if (pi->is_empty()) {
          pi->set(key, val);
          ret = OB_SUCCESS;
        } else if (pi->equal(key)) {
          ret = OB_ENTRY_EXIST;
        }
      }
    }
    if (OB_SUCC(ret)) {
      size_++;
    }
    return ret;
  }

  int update(const key_t &key, const val_t &val)
  {
    int ret = OB_SIZE_OVERFLOW;
    LockGuard guard(lock_);
    if (!is_inited()) {
      ret = OB_NOT_INIT;
    } else {
      for (uint64_t start_idx = key.hash(), i = 0; OB_SIZE_OVERFLOW == ret &&
          i < (uint64_t)capacity_; ++i) {
        Item *pi = items_ + ((start_idx + i) % capacity_);
        if (pi->is_empty()) {
          ret = OB_ENTRY_NOT_EXIST;
        } else if (pi->equal(key)) {
          pi->set(key, val);
          ret = OB_SUCCESS;
        }
      }
    }
    return ret;
  }

  template<typename func>
  int for_each(func &f)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < capacity_; i++) {
      LockGuard guard(lock_);
      Item *pi = items_ + i;
      if (!pi->is_empty()) {
        if (!f(pi->get_key(), pi->get_val())) {
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }
    return ret;
  }
  bool is_inited() const { return capacity_ > 0 && NULL != items_; }
private:

  static bool check_serialize_buf(const char *buf, int64_t buf_len, int64_t pos)
  {
    return NULL != buf && buf_len > 0 && pos >= 0 && pos <= buf_len;
  }
private:
  mutable Lock lock_;
  int64_t size_;
  int64_t capacity_;
  Item *items_;

  DISALLOW_COPY_AND_ASSIGN(ObArrayHashMap);
};
}; // end namespace common
}; // end namespace oceanbase
#endif //OCEANBASE_HASH_OB_ARRAY_HASH_MAP_
