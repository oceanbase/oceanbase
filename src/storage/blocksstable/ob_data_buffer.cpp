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

#include "ob_data_buffer.h"
#include <malloc.h>
#include "lib/allocator/ob_malloc.h"
#include "share/rc/ob_tenant_base.h"
using namespace oceanbase;
using namespace common;
using namespace share;

namespace oceanbase
{
namespace blocksstable
{
ObSelfBufferWriter::ObSelfBufferWriter(
    const char *label, const int64_t size, const bool need_align, const bool use_fixed_blk)
    : ObBufferWriter(NULL, 0, 0), label_(label), is_aligned_(need_align), use_fixed_blk_(use_fixed_blk),
      macro_block_mem_ctx_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(macro_block_mem_ctx_.init())) {
    STORAGE_LOG(WARN, "fail to init macro block memory context.", K(ret));
  } else if (OB_FAIL(ensure_space(size))) {
    STORAGE_LOG(WARN, "cannot allocate memory for data buffer.", K(size), K(ret));
  }
}

ObSelfBufferWriter::~ObSelfBufferWriter()
{
  free();
  is_aligned_ = false;
  pos_ = 0;
  capacity_ = 0;
  use_fixed_blk_ = false;
  macro_block_mem_ctx_.destroy();
}

char *ObSelfBufferWriter::alloc(const int64_t size)
{

  char *data = NULL;
#ifndef OB_USE_ASAN
  if (use_fixed_blk_ && size == macro_block_mem_ctx_.get_block_size()) {
    data = (char *)macro_block_mem_ctx_.alloc();
    if (OB_ISNULL(data)) {
      STORAGE_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "fail to alloc buf from mem ctx", K(size));
    }
  }
#endif
  if (OB_ISNULL(data)) {
    if (is_aligned_) {
      data = (char *)mtl_malloc_align(BUFFER_ALIGN_SIZE, size, label_);
    } else {
      data = (char *)mtl_malloc(size, label_);
    }
  }
  return data;
}

int ObSelfBufferWriter::ensure_space(int64_t size)
{
  int ret = OB_SUCCESS;
  //size = upper_align(size, common::ObLogConstants::LOG_FILE_ALIGN_SIZE);
  if (size <= 0) {
    // do nothing.
  } else if (is_aligned_ && size % BUFFER_ALIGN_SIZE != 0) {
    STORAGE_LOG(WARN, "not aligned buffer size", K(is_aligned_), K(size));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == data_) {
    if (NULL == (data_ = alloc(size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "allocate buffer memory error.", K(ret), K(size));
    } else {
      pos_ = 0;
      capacity_ = size;
    }
  } else if (capacity_ < size) {
    // resize;
    char *new_data = NULL;
    if (NULL == (new_data = alloc(size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "allocate buffer memory error.", K(ret), K(size));
    } else {
      MEMCPY(new_data, data_, pos_);
      free();
      capacity_ = size;
      data_ = new_data;
    }
  }

  return ret;
}

void ObSelfBufferWriter::free()
{
  if (NULL != data_) {
#ifndef OB_USE_ASAN
    if (macro_block_mem_ctx_.get_allocator().contains(data_)) {
      macro_block_mem_ctx_.free(data_);
      data_ = NULL;
    }
#endif
    if(NULL != data_) {
      if (is_aligned_) {
        mtl_free_align(data_);
      } else {
        mtl_free(data_);
      }
      data_ = NULL;
    }
  }
}
}
}