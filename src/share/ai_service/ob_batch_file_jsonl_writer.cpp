/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include "share/ai_service/ob_batch_file_jsonl_writer.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
namespace share
{

ObBatchFileJsonlWriter::ObBatchFileJsonlWriter()
    : is_inited_(false),
      fd_(-1),
      dir_id_(-1),
      tenant_id_(OB_INVALID_TENANT_ID),
      current_size_(0),
      line_count_(0),
      write_buffer_(NULL),
      buffer_size_(DEFAULT_BUFFER_SIZE),
      buffer_pos_(0)
{
}

ObBatchFileJsonlWriter::~ObBatchFileJsonlWriter()
{
  destroy();
}

int ObBatchFileJsonlWriter::init(int64_t dir_id, uint64_t tenant_id, const char *label)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already initialized", K(ret));
  } else if (OB_UNLIKELY(dir_id < 0 || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dir_id), K(tenant_id));
  } else {
    // allocate write buffer
    ObMemAttr mem_attr(tenant_id, label != NULL ? label : "BatchJsonlW");
    write_buffer_ = static_cast<char *>(ob_malloc(buffer_size_, mem_attr));
    if (OB_ISNULL(write_buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate write buffer", K(ret), K_(buffer_size));
    }
    // open tmp file
    if (OB_SUCC(ret)) {
      if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.open(
              tenant_id, fd_, dir_id, label))) {
        LOG_WARN("failed to open tmp file", K(ret), K(dir_id), K(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      dir_id_ = dir_id;
      tenant_id_ = tenant_id;
      current_size_ = 0;
      line_count_ = 0;
      buffer_pos_ = 0;
      is_inited_ = true;
      LOG_INFO("batch file jsonl writer init success", K_(fd), K_(dir_id), K_(tenant_id));
    } else {
      // cleanup on failure
      if (write_buffer_ != NULL) {
        ob_free(write_buffer_);
        write_buffer_ = NULL;
      }
    }
  }
  return ret;
}

void ObBatchFileJsonlWriter::reset()
{
  destroy();
}

void ObBatchFileJsonlWriter::destroy()
{
  // Best-effort cleanup of TmpFileManager fd if finish() was not called
  if (fd_ >= 0 && tenant_id_ != OB_INVALID_TENANT_ID) {
    int tmp_ret = FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(tenant_id_, fd_);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN_RET(tmp_ret, "best-effort cleanup of jsonl writer fd failed", K_(fd), K_(tenant_id));
    }
  }
  if (write_buffer_ != NULL) {
    ob_free(write_buffer_);
    write_buffer_ = NULL;
  }
  is_inited_ = false;
  fd_ = -1;
  dir_id_ = -1;
  tenant_id_ = OB_INVALID_TENANT_ID;
  current_size_ = 0;
  line_count_ = 0;
  buffer_pos_ = 0;
}

int ObBatchFileJsonlWriter::write_line(const common::ObString &line)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(line.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty line", K(ret));
  } else {
    // write line content
    if (OB_FAIL(append_to_buffer_(line.ptr(), line.length()))) {
      LOG_WARN("failed to append line content", K(ret), K(line.length()));
    }
    // write newline
    if (OB_SUCC(ret)) {
      const char newline = '\n';
      if (OB_FAIL(append_to_buffer_(&newline, 1))) {
        LOG_WARN("failed to append newline", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      line_count_++;
    }
  }
  return ret;
}

int ObBatchFileJsonlWriter::write_chunk(const char *data, int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(NULL == data || size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(data), K(size));
  } else if (size > 0) {
    if (OB_FAIL(append_to_buffer_(data, size))) {
      LOG_WARN("failed to append chunk", K(ret), K(size));
    } else {
      for (int64_t i = 0; i < size; i++) {
        if (data[i] == '\n') {
          line_count_++;
        }
      }
    }
  }
  return ret;
}

int ObBatchFileJsonlWriter::finish(ObBatchFileDataSegment &segment)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else {
    // flush remaining buffer
    if (buffer_pos_ > 0) {
      if (OB_FAIL(flush_buffer_())) {
        LOG_WARN("failed to flush remaining buffer", K(ret), K_(buffer_pos));
      }
    }
    // seal the fd
    if (OB_SUCC(ret)) {
      if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.seal(tenant_id_, fd_))) {
        LOG_WARN("failed to seal tmp file", K(ret), K_(fd), K_(tenant_id));
      }
    }
    // populate segment and release fd ownership (task takes over)
    if (OB_SUCC(ret)) {
      segment.fd_ = fd_;
      segment.start_offset_ = 0;
      segment.size_ = current_size_;
      segment.line_count_ = line_count_;
      // Transfer fd ownership to caller — writer no longer owns it
      fd_ = -1;
      LOG_INFO("batch file jsonl writer finish", K(segment));
    }
  }
  return ret;
}

int ObBatchFileJsonlWriter::flush_buffer_()
{
  int ret = OB_SUCCESS;
  if (buffer_pos_ > 0) {
    tmp_file::ObTmpFileIOInfo io_info;
    io_info.fd_ = fd_;
    io_info.buf_ = write_buffer_;
    io_info.size_ = buffer_pos_;
    io_info.io_desc_.set_wait_event(ObWaitEventIds::ROW_STORE_DISK_WRITE);
    io_info.io_timeout_ms_ = DEFAULT_IO_TIMEOUT_MS;
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.write(tenant_id_, io_info))) {
      LOG_WARN("failed to write to tmp file", K(ret), K_(fd), K_(buffer_pos));
    } else {
      current_size_ += buffer_pos_;
      buffer_pos_ = 0;
    }
  }
  return ret;
}

int ObBatchFileJsonlWriter::append_to_buffer_(const char *data, int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t remaining = size;
  int64_t offset = 0;
  while (OB_SUCC(ret) && remaining > 0) {
    int64_t avail = buffer_size_ - buffer_pos_;
    int64_t copy_size = MIN(remaining, avail);
    MEMCPY(write_buffer_ + buffer_pos_, data + offset, copy_size);
    buffer_pos_ += copy_size;
    offset += copy_size;
    remaining -= copy_size;
    // flush if buffer is full
    if (buffer_pos_ >= buffer_size_) {
      if (OB_FAIL(flush_buffer_())) {
        LOG_WARN("failed to flush buffer", K(ret));
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
