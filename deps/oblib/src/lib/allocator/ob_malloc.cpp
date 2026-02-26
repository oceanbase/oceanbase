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

#include "ob_malloc.h"
#include "lib/utility/utility.h"
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
  ObMemAttr attr;
  attr.label_ = label;
  return ob_malloc_align(alignment, nbyte, attr);
}

void *oceanbase::common::ob_malloc_align(const int64_t align, const int64_t nbyte,
                                         const ObMemAttr &attr)
{
  return ObAllocAlign::alloc_align(nbyte, align,
      [](const int64_t size, const ObMemAttr &attr){ return ob_malloc(size, attr); }, attr);
}

void oceanbase::common::ob_free_align(void *ptr)
{
  ObAllocAlign::free_align(ptr, [](void *ptr){ ob_free(ptr); });
}


void *ob_zalloc(const int64_t nbyte)
{
  return ::oceanbase::common::ob_malloc(nbyte, ::oceanbase::common::ObModIds::OB_ZLIB);
}

void ob_zfree(void *ptr)
{
  ::oceanbase::common::ob_free(ptr);
}
