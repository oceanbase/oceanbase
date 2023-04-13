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

#include "lib/allocator/ob_malloc.h"
#include <errno.h>
#include "lib/thread_local/ob_tsi_factory.h"
#include "lib/utility/utility.h"
#include <algorithm>
#ifdef __OB_MTRACE__
#include <execinfo.h>
#endif


int oceanbase::common::ObMemBuf::ensure_space(const int64_t size, const lib::ObLabel &label)
{
  int ret         = OB_SUCCESS;
  char *new_buf   = NULL;
  int64_t buf_len = size > buf_size_ ? size : buf_size_;

  if (size <= 0 || (NULL != buf_ptr_ && buf_size_ <= 0)) {
    _OB_LOG(WARN, "invalid param, size=%ld, buf_ptr_=%p, "
              "buf_size_=%ld",
              size, buf_ptr_, buf_size_);
    ret = OB_ERROR;
  } else if (NULL == buf_ptr_ || (NULL != buf_ptr_ && size > buf_size_)) {
    new_buf = static_cast<char *>(ob_malloc(buf_len, label));
    if (NULL == new_buf) {
      _OB_LOG(ERROR, "Problem allocate memory for buffer");
      ret = OB_ERROR;
    } else {
      if (NULL != buf_ptr_) {
        ob_free(buf_ptr_);
        buf_ptr_ = NULL;
      }
      buf_size_ = buf_len;
      buf_ptr_ = new_buf;
      label_ = label;
    }
  }

  return ret;
}

void *oceanbase::common::ob_malloc_align(const int64_t alignment, const int64_t nbyte,
                                         const lib::ObLabel &label)
{
  char *ptr = static_cast<char *>(oceanbase::common::ob_malloc(nbyte + alignment, label));
  char *align_ptr = NULL;
  if (NULL != ptr) {
    align_ptr = reinterpret_cast<char *>(oceanbase::common::upper_align(reinterpret_cast<int64_t>(ptr),
                                                                        alignment));
    if (align_ptr == ptr) {
      align_ptr = ptr + alignment;
    }
    int64_t padding = align_ptr - ptr;
    if (!(padding <= alignment && padding > 0)) {
      _OB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid padding(padding=%ld, alignment=%ld", padding, alignment);
    }
    uint8_t *sign_ptr = reinterpret_cast<uint8_t *>(align_ptr - 1);
    int64_t *header_ptr = reinterpret_cast<int64_t *>(align_ptr - 1 - sizeof(int64_t));
    if (padding < (int64_t)sizeof(int64_t) + 1) {
      *sign_ptr = static_cast<uint8_t>(padding) & 0x7f;
    } else {
      *sign_ptr = 0x80;
      *header_ptr = padding;
    }
  } else {
    _OB_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "ob_tc_malloc allocate memory failed, alignment[%ld], nbyte[%ld], label[%s].",
            alignment, nbyte, (const char *)label);
  }
  return align_ptr;
}

void *oceanbase::common::ob_malloc_align(const int64_t alignment, const int64_t nbyte,
                                         const ObMemAttr &attr)
{
  char *ptr = static_cast<char *>(oceanbase::common::ob_malloc(nbyte + alignment, attr));
  char *align_ptr = NULL;
  if (NULL != ptr) {
    align_ptr = reinterpret_cast<char *>(oceanbase::common::upper_align(reinterpret_cast<int64_t>(ptr),
                                                                        alignment));
    if (align_ptr == ptr) {
      align_ptr = ptr + alignment;
    }
    int64_t padding = align_ptr - ptr;
    if (!(padding <= alignment && padding > 0)) {
      _OB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid padding(padding=%ld, alignment=%ld", padding, alignment);
    }
    uint8_t *sign_ptr = reinterpret_cast<uint8_t *>(align_ptr - 1);
    int64_t *header_ptr = reinterpret_cast<int64_t *>(align_ptr - 1 - sizeof(int64_t));
    if (padding < (int64_t)sizeof(int64_t) + 1) {
      *sign_ptr = static_cast<uint8_t>(padding) & 0x7f;
    } else {
      *sign_ptr = 0x80;
      *header_ptr = padding;
    }
  } else {
    _OB_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "ob_tc_malloc allocate memory failed, alignment[%ld], nbyte[%ld], tenant_id[%lu], label[%s].",
            alignment, nbyte, attr.tenant_id_, (const char*)attr.label_);
  }
  return align_ptr;
}

void oceanbase::common::ob_free_align(void *ptr)
{
  if (NULL == ptr) {
    _OB_LOG_RET(WARN, OB_INVALID_ARGUMENT, "cannot free NULL pointer.");
  } else if (oceanbase::lib::ObMallocAllocator::is_inited_) {
    uint8_t *sign_ptr = reinterpret_cast<uint8_t *>(static_cast<char *>(ptr) - 1);
    int64_t *header_ptr = reinterpret_cast<int64_t *>(static_cast<char *>(ptr) - 1 - sizeof(int64_t));
    char *origin_ptr = NULL;
    if (NULL != sign_ptr) {
      if ((*sign_ptr & 0x80) != 0) {
        origin_ptr = reinterpret_cast<char *>(ptr) - (*header_ptr);
      } else {
        origin_ptr = reinterpret_cast<char *>(ptr) - (*sign_ptr & 0x7f);
      }
      if (NULL != origin_ptr) {
        oceanbase::common::ob_free(origin_ptr);
      }
    }
  }
}


void *ob_zalloc(const int64_t nbyte)
{
  return ::oceanbase::common::ob_malloc(nbyte, ::oceanbase::common::ObModIds::OB_ZLIB);
}

void ob_zfree(void *ptr)
{
  ::oceanbase::common::ob_free(ptr);
}
