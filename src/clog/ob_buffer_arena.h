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

#ifndef OCEANBASE_CLOG_OB_BUFFER_ARENA_
#define OCEANBASE_CLOG_OB_BUFFER_ARENA_

#include "share/ob_define.h"

namespace oceanbase {
namespace clog {
// It is responsible for buffer alloc, which reserves header and aligns memory.
class ObIBufferArena {
public:
  ObIBufferArena()
  {}
  virtual ~ObIBufferArena()
  {}
  virtual char* alloc() = 0;
  virtual int64_t get_buffer_size() const = 0;
  virtual int64_t get_buffer_count() const = 0;
};

class ObBufferArena : public ObIBufferArena {
public:
  ObBufferArena();
  virtual ~ObBufferArena();
  int init(const char* label, int64_t align_size, int64_t header_size, int64_t trailer_size, int64_t buffer_size,
      int64_t buffer_count);
  int64_t get_buffer_size() const
  {
    return buffer_size_ - header_size_ - trailer_size_;
  }
  int64_t get_buffer_count() const
  {
    return buffer_count_;
  }
  char* alloc();

private:
  bool is_inited_;
  int64_t align_size_;
  int64_t header_size_;
  int64_t trailer_size_;
  int64_t buffer_size_;
  int64_t buffer_count_;
  char* alloc_start_;
  int64_t alloc_bytes_;
  DISALLOW_COPY_AND_ASSIGN(ObBufferArena);
};
};      // end namespace clog
};      // end namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_BUFFER_ARENA_
