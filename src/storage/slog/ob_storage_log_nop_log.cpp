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
#include "ob_storage_log_nop_log.h"
#include <string.h>
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/allocator/ob_malloc.h"
#include "common/log/ob_log_constants.h"
#include "storage/slog/ob_storage_log_entry.h"
#include "storage/slog/ob_storage_log_item.h"
#include "storage/slog/ob_storage_log_batch_header.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
ObStorageLogNopLog::ObStorageLogNopLog()
  : is_inited_(false), buffer_(nullptr),
    buffer_size_(0), needed_size_(0)
{

}

ObStorageLogNopLog::~ObStorageLogNopLog()
{
  destroy();
}

int ObStorageLogNopLog::init(const int64_t tenant_id, const int64_t buffer_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(buffer_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buffer_size));
  } else {
    buffer_ = static_cast<char *>(ob_malloc_align(
        ObLogConstants::LOG_FILE_ALIGN_SIZE,
        buffer_size, ObMemAttr(tenant_id, "SlogNopLog")));
    if (OB_ISNULL(buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc nop log buffer", K(ret));
    } else {
      buffer_size_ = buffer_size;
      MEMSET(buffer_, 0x01, buffer_size_);
      is_inited_ = true;
    }
  }

  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void ObStorageLogNopLog::destroy()
{
  if (nullptr != buffer_) {
    ob_free_align(buffer_);
    buffer_ = nullptr;
  }
  buffer_size_ = 0;
  needed_size_ = 0;
  is_inited_ = false;
}

int ObStorageLogNopLog::set_needed_size(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init", K(ret));
  } else if (OB_UNLIKELY(size < 0 || size > buffer_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(size), K_(buffer_size));
  } else {
    needed_size_ = size;
  }
  return ret;
}

int ObStorageLogNopLog::serialize(char *buf, const int64_t limit, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_REDO_LOG(WARN, "Not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == buf || limit < 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(limit), K(pos));
  } else if (OB_UNLIKELY(pos + needed_size_ > limit)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    MEMCPY(buf + pos, buffer_, needed_size_);
    pos += needed_size_;
  }
  return ret;
}

int ObStorageLogNopLog::deserialize(const char *buf, const int64_t limit, int64_t &pos)
{
  UNUSED(buf);
  UNUSED(limit);
  UNUSED(pos);
  return OB_NOT_SUPPORTED;
}

int64_t ObStorageLogNopLog::to_string(char* buf, const int64_t buf_len) const
{
  UNUSED(buf);
  UNUSED(buf_len);
  return OB_NOT_SUPPORTED;
}

int64_t ObStorageLogNopLog::get_fixed_serialize_len(const int64_t used_len)
{
  int64_t ret_len = 0;
  const ObStorageLogEntry dummy_entry;
  const ObStorageLogBatchHeader dummy_header;
  int64_t occupied_size = used_len +
                          dummy_entry.get_serialize_size() +
                          dummy_header.get_serialize_size();
  ret_len = ObStorageLogItem::get_align_padding_size(occupied_size,
      ObLogConstants::LOG_FILE_ALIGN_SIZE);
  LOG_DEBUG("log data len", K(occupied_size));
  return ret_len;
}
} // namespace blocksstable
} // namespace oceanbase
