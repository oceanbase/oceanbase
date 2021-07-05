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

#ifndef OB_ADAPTER_ALLOCATOR_H_
#define OB_ADAPTER_ALLOCATOR_H_

namespace oceanbase {
namespace common {
class AdapterAllocator {
public:
  AdapterAllocator() : buffer_(NULL), pos_(0)
  {}
  ~AdapterAllocator()
  {}
  inline void init(char* buffer)
  {
    buffer_ = buffer;
    pos_ = 0;
  }
  inline void* alloc(int64_t size)
  {
    void* ret = NULL;
    ret = static_cast<void*>(buffer_ + pos_);
    pos_ += size;
    return ret;
  }
  inline void free(void* buf)
  {
    UNUSED(buf);
  }

private:
  char* buffer_;
  int64_t pos_;
};
}  // namespace common
}  // namespace oceanbase
#endif
