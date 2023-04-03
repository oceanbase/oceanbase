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

#ifndef OCEANBASE_COMMON_OB_DATA_BUFFER_H_
#define OCEANBASE_COMMON_OB_DATA_BUFFER_H_

#include <stdint.h>
#include <stdlib.h>
#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace common
{
class ObDataBuffer : public ObIAllocator
{
public:
  ObDataBuffer() : data_(NULL), capacity_(0), position_(0), limit_(0)
  {
  }

  ObDataBuffer(char *data, const int64_t capacity)
      : data_(data), capacity_(capacity), position_(0), limit_(0)
  {
  }

  virtual ~ObDataBuffer() {}

  inline bool set_data(char *data, const int64_t capacity)
  {
    bool ret = false;

    if (OB_LIKELY(data != NULL && capacity > 0)) {
      data_ = data;
      capacity_ = capacity;
      position_ = 0;
      limit_ = 0;
      ret = true;
    }

    return ret;
  }

  /** WARNING: make sure the memory if freed before call this method */
  inline void reset()
  {
    data_ = NULL;
    capacity_ = 0;
    position_ = 0;
    limit_ = 0;
  }

  inline virtual void *alloc(const int64_t sz)
  {
    void *ret = NULL;
    if (OB_LIKELY(sz > 0 && capacity_ - position_ >= sz)) {
      ret = &data_[position_];
      position_ += sz;
    }
    return ret;
  }
  inline virtual void *alloc(const int64_t sz, const lib::ObMemAttr &attr)
  {
    UNUSED(attr);
    return alloc(sz);
  }

  inline virtual void free()
  {
    position_ = 0;
  }

  //[[deprecated("Arena is not allowed to call free(ptr), use free() instead")]]
  inline void free(void *ptr)
  {
    UNUSED(ptr);
  }

  inline void set_label(const lib::ObLabel &label)
  {
    UNUSED(label);
  }

  inline const char *get_data() const { return data_; }
  inline char *get_data() { return data_; }

  inline int64_t get_capacity() const { return capacity_; }
  inline int64_t get_remain() const { return capacity_ - position_; }
  inline int64_t get_remain_data_len() const {return limit_ - position_; }
  inline int64_t get_position() const { return position_; }
  inline int64_t &get_position() { return position_; }

  inline int64_t get_limit() const { return limit_; }
  inline int64_t &get_limit() { return limit_; }

  inline const char *get_cur_pos() const { return data_ + position_; }
  inline char *get_cur_pos() { return data_ + position_; }
  int64_t to_string(char *buffer, const int64_t length) const;

  OB_UNIS_VERSION(1);

protected:
  char *data_;
  // the size of buf (data_), which is allocated outside
  int64_t capacity_;
  // the offset of buf (data_), data before position_ has been consumed,
  // data after position_ hasn't been used
  int64_t position_;
  // size of occupied buffer space, including data that has been consumed and hasn't been used
  int64_t limit_;
};

} /* common */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_COMMON_OB_DATA_BUFFER_H_ */
