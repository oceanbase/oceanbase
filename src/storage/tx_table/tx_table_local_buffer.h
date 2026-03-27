/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_TX_TABLE_LOCAL_BUFFER
#define OCEANBASE_STORAGE_OB_TX_TABLE_LOCAL_BUFFER

#include "lib/allocator/ob_allocator.h"

namespace oceanbase {
namespace storage {

struct ObTxLocalBuffer
{
  ObTxLocalBuffer() = delete;
  ObTxLocalBuffer(common::ObIAllocator &allocator) : allocator_(allocator) { buf_ = nullptr; buf_len_ = 0; }
  ~ObTxLocalBuffer() { reset(); }
  OB_INLINE char *get_ptr() { return buf_; }
  OB_INLINE int64_t get_length() { return buf_len_; }
  OB_INLINE void reset()
  {
    if (nullptr != buf_) {
      allocator_.free(buf_);
    }
    buf_ = nullptr;
    buf_len_ = 0;
  }
  OB_INLINE int reserve(const int64_t buf_len)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(buf_len <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "Invalid argument to reserve local buffer", K(buf_len));
    } else if (buf_len > buf_len_) {
      reset();
      if (OB_ISNULL(buf_ = reinterpret_cast<char *>(allocator_.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "Failed to alloc memory", K(ret), K(buf_len));
      } else {
        buf_len_ = buf_len;
      }
    }
    return ret;
  }

  TO_STRING_KV(K_(buf), K_(buf_len));

  common::ObIAllocator &allocator_;
  char *buf_;
  int64_t buf_len_;
};

}
}
#endif // OCEANBASE_STORAGE_OB_TX_TABLE_LOCAL_BUFFER
