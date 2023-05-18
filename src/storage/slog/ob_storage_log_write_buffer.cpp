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

#define USING_LOG_PREFIX STORAGE_REDO

#include "ob_storage_log_write_buffer.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/allocator/ob_malloc.h"
#include "storage/slog/ob_storage_log_item.h"
#include "common/log/ob_log_constants.h"

namespace oceanbase
{
using namespace common;

namespace storage
{
ObStorageLogWriteBuffer::ObStorageLogWriteBuffer()
  : is_inited_(false), buf_(nullptr),
    buf_size_(0), write_len_(0),
    log_data_len_(0), align_size_(0)
{
}

ObStorageLogWriteBuffer::~ObStorageLogWriteBuffer()
{
  destroy();
}

int ObStorageLogWriteBuffer::init(
    const int64_t align_size,
    const int64_t buf_size,
    const int64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_REDO_LOG(WARN, "Init twice", K(ret));
  } else if (OB_UNLIKELY(align_size <= 0) || OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments", K(ret), K(align_size), K(buf_size));
  } else if (OB_ISNULL(buf_ = static_cast<char *>(ob_malloc_align(align_size,
      buf_size, ObMemAttr(tenant_id, "SlogWriteBuffer"))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_REDO_LOG(WARN, "Fail to alloc write buffer",
        K(ret), KP_(buf), K(buf_size), K_(align_size));
  } else {
    buf_size_ = buf_size;
    write_len_ = 0;
    log_data_len_ = 0;
    align_size_ = align_size;
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    destroy();
  }

  return ret;
}

void ObStorageLogWriteBuffer::destroy()
{
  if (nullptr != buf_) {
    ob_free_align(buf_);
    buf_ = nullptr;
  }
  buf_size_ = 0;
  write_len_ = 0;
  log_data_len_ = 0;
  align_size_ = 0;
  is_inited_ = false;
}

int ObStorageLogWriteBuffer::copy_log_item(const ObStorageLogItem *item)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init", K(ret));
  } else if (OB_ISNULL(item)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments", K(ret), KP(item));
  } else if (OB_UNLIKELY(log_data_len_ + item->get_data_len() > buf_size_)) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_REDO_LOG(WARN, "Left space is not enough for copy", K(ret),
        "data size", item->get_data_len(), K_(write_len), K_(buf_size));
  } else {
    MEMCPY(buf_ + log_data_len_, item->get_buf(), item->get_data_len());
    write_len_ = log_data_len_ + item->get_data_len();
    log_data_len_ += item->get_log_data_len();

  }

  STORAGE_REDO_LOG(DEBUG, "Write buffer after copy",
      K_(write_len), K_(log_data_len), "log item", *item);
  return ret;
}

int ObStorageLogWriteBuffer::move_buffer(int64_t &backward_size)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init", K(ret));
  } else if (OB_UNLIKELY((0 != write_len_ % align_size_) || (write_len_ < log_data_len_))) {
    ret = OB_INVALID_DATA;
    STORAGE_REDO_LOG(WARN, "invalid len, write_len_ should always be aligned after a batch of log items is copied",
        K(ret), K_(write_len), K_(log_data_len), K_(align_size), K_(buf_size));
  } else {
    int64_t lower_align_offset = lower_align(log_data_len_, align_size_);
    if (0 == lower_align_offset) {
      // do nothing
    } else {
      int64_t move_size = write_len_ - lower_align_offset;
      if (0 == move_size) {
        // write_len_ equals to log_data_len_, do nothing
      } else {
        MEMMOVE(buf_, buf_ + lower_align_offset, move_size);
      }
      log_data_len_ -= lower_align_offset;
      write_len_ = move_size;
    }
    backward_size = lower_align_offset;
  }

  STORAGE_REDO_LOG(DEBUG, "Successfully move", K_(write_len), K_(log_data_len));
  return ret;
}

void ObStorageLogWriteBuffer::reuse()
{
  write_len_ = 0;
  log_data_len_ = 0;
}

}
}
