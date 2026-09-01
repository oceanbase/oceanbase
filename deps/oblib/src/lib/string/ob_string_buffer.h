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

#ifndef OCEANBASE_LIB_STRING_OB_STRING_BUFFER_
#define OCEANBASE_LIB_STRING_OB_STRING_BUFFER_

#include "lib/string/ob_string.h"
#include "common/object/ob_obj_type.h"

namespace oceanbase {
namespace common {

class ObStringBuffer
{
public:
  static const int64_t STRING_BUFFER_INIT_STRING_LEN = 512;

  ObStringBuffer(common::ObIAllocator *allocator);
  // init_cap: hint for the first allocation; avoids the default 512-byte minimum for small outputs.
  ObStringBuffer(common::ObIAllocator *allocator, uint64_t init_cap);
  // Pre-allocated backing buffer.  buf must have at least `cap` usable bytes.
  // allocator is used ONLY when the buffer overflows and needs to grow.
  // Callers using arena allocators: arena->free() is a no-op, so it is safe for reset()/extend()
  // to call allocator->free(buf) when buf was not originally allocated by that allocator.
  ObStringBuffer(char *buf, uint64_t cap, common::ObIAllocator *allocator);
  ObStringBuffer();
  ~ObStringBuffer();
  // before set new alloctor, should call reset() first
  inline void set_allocator(common::ObIAllocator *allocator)
  {
    allocator_ = allocator;
  }
  common::ObIAllocator *get_allocator() const { return allocator_; }
  void reset();
  void reuse();
  int get_result_string(ObString &buffer);

  int append(const char *str);

  // Append data later
  // @param [in] flag reserves flag bits for other subsequent expansions
  // @return Returns OB_SUCCESS on success, error code otherwise.
  // When the flag is the default value -1, the size of len_*2 + len is used each time to expand the memory.
  // If the data is large, the default value is not used, and the memory application size is len_ + len.
  int append(const char *str, const uint64_t len, int8_t flag = -1);
  int append(const ObString &str, int8_t flag = -1);
  int reserve(const uint64_t len);
  int extend(const uint64_t len);

  const char *ptr() const { return data_; }
  uint64_t length() const { return len_; }
  uint64_t capacity() const { return cap_ ; }
  inline uint64_t remain() const
  {
    return OB_LIKELY(cap_ > 0) ? (cap_ - len_) : cap_;
  }
  bool empty() const { return 0 == length(); }
  char *ptr() { return data_; }
  int set_length(const uint64_t len);
  char back() { return data_[len_ - 1]; };
  const ObString string() const;
  int deep_copy(common::ObIAllocator *allocator, ObStringBuffer &input);
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "len = %ld, cap = %ld", len_, cap_);
    return pos;
  }
private:
  common::ObIAllocator *allocator_;
  char *data_;
  uint64_t len_;
  uint64_t cap_;
  uint64_t init_cap_;

  DISALLOW_COPY_AND_ASSIGN(ObStringBuffer);
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_STRING_OB_STRING_BUFFER_