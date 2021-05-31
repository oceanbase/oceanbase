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

#include "ob_sort_temp_macro_block_reader.h"
#include "ob_store_file.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObSortTempMacroBlockReader::ObSortTempMacroBlockReader()
    : macro_handles_(), prefetch_count_(0), allocator_(ObNewModIds::OB_SORT_TEMP_MACRO_READER), is_inited_(false)
{
  read_info_.io_desc_.category_ = SYS_IO;
  read_info_.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_READ;
  read_info_.offset_ = 0;
  read_info_.size_ = OB_STORE_FILE.get_macro_block_size();
}

ObSortTempMacroBlockReader::~ObSortTempMacroBlockReader()
{
  reset();
}

int ObSortTempMacroBlockReader::open(const int64_t max_handle_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObSortTempMacroBlockReader has been inited twice", K(ret), K(is_inited_));
  } else if (max_handle_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(max_handle_count));
  } else {
    void* buf = NULL;
    ObMacroBlockHandle* macro_handle = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < max_handle_count; ++i) {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMacroBlockHandle)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "Fail to allocate memory", K(ret));
      } else {
        macro_handle = new (buf) ObMacroBlockHandle();
        if (OB_FAIL(macro_handles_.push_back(macro_handle))) {
          STORAGE_LOG(WARN, "Fail to push index handle to list", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSortTempMacroBlockReader::prefetch(const ObIArray<MacroBlockId>& macro_blocks)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObNewModIds::OB_SORT_TEMP_MACRO_READER);
  ObStoreFileCtx file_ctx(allocator);
  ObMacroBlockCtx macro_block_ctx;  // TODO(): fix it for ofs later
  file_ctx.file_system_type_ = STORE_FILE_SYSTEM_LOCAL;
  macro_block_ctx.file_ctx_ = &file_ctx;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSortTempMacroBlockReader has not been inited", K(ret));
  } else if (0 == macro_blocks.count() || macro_blocks.count() > macro_handles_.count()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(macro_blocks.count()), K(macro_handles_.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_blocks.count(); ++i) {
      macro_block_ctx.sstable_block_id_.macro_block_id_ = macro_blocks.at(i);
      read_info_.macro_block_ctx_ = &macro_block_ctx;
      if (OB_FAIL(OB_STORE_FILE.async_read_block(read_info_, *(macro_handles_.at(i))))) {
        STORAGE_LOG(WARN, "Fail to submit aio read request, ", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      prefetch_count_ = macro_blocks.count();
    }
  }
  return ret;
}

int ObSortTempMacroBlockReader::fetch_buffer(
    const int64_t io_timeout_ms, const int64_t buf_capacity, char* buf, int64_t& buf_len)
{
  int ret = OB_SUCCESS;
  buf_len = 0;
  const int64_t macro_block_size = OB_STORE_FILE.get_macro_block_size();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSortTempMacroBlockReader has not been inited", K(ret), K(is_inited_));
  } else if (buf_capacity < 0 || (buf_capacity % macro_block_size != 0) || NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(buf_capacity), KP(buf), K(macro_block_size));
  } else if (OB_FAIL(wait_io_finish(io_timeout_ms))) {
    STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(io_timeout_ms));
  } else {
    char* read_buf = buf;
    int64_t buf_pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < prefetch_count_; ++i) {
      if (buf_pos + macro_block_size > buf_capacity) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "fetch buffer not enough", K(ret), K(buf_pos), K(macro_block_size), K(buf_capacity));
      } else {
        MEMCPY(read_buf + buf_pos, macro_handles_.at(i)->get_buffer(), macro_block_size);
        buf_pos += macro_block_size;
        macro_handles_.at(i)->reset();
      }
    }

    if (OB_SUCC(ret)) {
      buf_len = buf_pos;
    }
  }
  return ret;
}

int ObSortTempMacroBlockReader::wait_io_finish(const int64_t io_timeout_ms)
{
  int ret = OB_SUCCESS;
  int64_t timeout_ms = io_timeout_ms;
  for (int64_t i = 0; OB_SUCC(ret) && i < prefetch_count_; ++i) {
    const int64_t start_timestamp = ObTimeUtility::current_time();
    if (OB_FAIL(macro_handles_.at(i)->wait(timeout_ms))) {
      STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(i), "io_handle", macro_handles_.at(i));
    } else {
      timeout_ms -= (ObTimeUtility::current_time() - start_timestamp) / 1000;
    }
  }
  return ret;
}

void ObSortTempMacroBlockReader::reset()
{
  is_inited_ = false;
  prefetch_count_ = 0;
  for (int64_t i = 0; i < macro_handles_.count(); ++i) {
    macro_handles_.at(i)->reset();
  }
  allocator_.reset();
}
