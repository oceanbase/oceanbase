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

#include "share/ai_service/ob_batch_file_jsonl_iterator.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
namespace share
{

ObBatchFileJsonlIterator::ObBatchFileJsonlIterator()
    : is_inited_(false),
      fd_(-1),
      start_offset_(0),
      total_size_(0),
      file_read_offset_(0),
      tenant_id_(OB_INVALID_TENANT_ID),
      read_buffer_(NULL),
      buffer_size_(DEFAULT_BUFFER_SIZE),
      buffer_data_size_(0),
      buffer_pos_(0)
{
}

ObBatchFileJsonlIterator::~ObBatchFileJsonlIterator()
{
  destroy();
}

int ObBatchFileJsonlIterator::init(int64_t fd, int64_t start_offset,
                                    int64_t size, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already initialized", K(ret));
  } else if (OB_UNLIKELY(fd < 0 || start_offset < 0 || size < 0
             || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(fd), K(start_offset), K(size), K(tenant_id));
  } else {
    ObMemAttr mem_attr(tenant_id, "BatchJsonlR");
    read_buffer_ = static_cast<char *>(ob_malloc(buffer_size_, mem_attr));
    if (OB_ISNULL(read_buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate read buffer", K(ret), K_(buffer_size));
    } else {
      fd_ = fd;
      start_offset_ = start_offset;
      total_size_ = size;
      file_read_offset_ = start_offset;
      tenant_id_ = tenant_id;
      buffer_data_size_ = 0;
      buffer_pos_ = 0;
      is_inited_ = true;
    }
  }
  return ret;
}

void ObBatchFileJsonlIterator::reset()
{
  destroy();
}

void ObBatchFileJsonlIterator::destroy()
{
  if (read_buffer_ != NULL) {
    ob_free(read_buffer_);
    read_buffer_ = NULL;
  }
  is_inited_ = false;
  fd_ = -1;
  start_offset_ = 0;
  total_size_ = 0;
  file_read_offset_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  buffer_data_size_ = 0;
  buffer_pos_ = 0;
}

int ObBatchFileJsonlIterator::get_next_line(common::ObString &line)
{
  int ret = OB_SUCCESS;
  line.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else {
    // ensure we have data in buffer
    if (buffer_pos_ >= buffer_data_size_) {
      if (OB_FAIL(refill_buffer_())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to refill buffer", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      // search for newline in current buffer
      char *line_start = read_buffer_ + buffer_pos_;
      int64_t search_len = buffer_data_size_ - buffer_pos_;
      char *newline_pos = static_cast<char *>(memchr(line_start, '\n', search_len));

      if (newline_pos != NULL) {
        // found newline within current buffer
        int64_t line_len = newline_pos - line_start;
        line.assign_ptr(line_start, static_cast<int32_t>(line_len));
        buffer_pos_ += line_len + 1;  // skip the \n
      } else {
        // newline not found: line spans buffer boundary
        // move remaining data to buffer start
        int64_t remaining = buffer_data_size_ - buffer_pos_;
        if (remaining > 0 && buffer_pos_ > 0) {
          MEMMOVE(read_buffer_, read_buffer_ + buffer_pos_, remaining);
        }
        buffer_data_size_ = remaining;
        buffer_pos_ = 0;

        // keep refilling to find the complete line
        bool found = false;
        while (OB_SUCC(ret) && !found) {
          int64_t end_offset = start_offset_ + total_size_;
          int64_t file_remaining = end_offset - file_read_offset_;
          if (file_remaining <= 0 && buffer_data_size_ > 0) {
            // no more data from file; treat remaining buffer as last line
            line.assign_ptr(read_buffer_, static_cast<int32_t>(buffer_data_size_));
            buffer_pos_ = buffer_data_size_;
            found = true;
          } else if (file_remaining <= 0) {
            ret = OB_ITER_END;
          } else {
            // read more data after existing data in buffer
            int64_t space = buffer_size_ - buffer_data_size_;
            if (space <= 0) {
              // buffer full but no newline found: line exceeds buffer size
              // return entire buffer as one line (caller handles oversized lines)
              LOG_WARN("JSONL line exceeds buffer size, returning partial",
                       K_(buffer_size), K_(buffer_data_size));
              line.assign_ptr(read_buffer_, static_cast<int32_t>(buffer_data_size_));
              buffer_pos_ = buffer_data_size_;
              found = true;
            } else {
              int64_t read_size = MIN(space, file_remaining);
              tmp_file::ObTmpFileIOInfo io_info;
              io_info.fd_ = fd_;
              io_info.buf_ = read_buffer_ + buffer_data_size_;
              io_info.size_ = read_size;
              io_info.io_desc_.set_wait_event(ObWaitEventIds::ROW_STORE_DISK_READ);
              io_info.io_timeout_ms_ = DEFAULT_IO_TIMEOUT_MS;

              tmp_file::ObTmpFileIOHandle handle;
              if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.pread(
                      tenant_id_, io_info, file_read_offset_, handle))) {
                LOG_WARN("failed to pread from tmp file", K(ret), K_(fd),
                         K_(file_read_offset), K(read_size));
              } else {
                int64_t done_size = handle.get_done_size();
                file_read_offset_ += done_size;
                buffer_data_size_ += done_size;

                // search for newline in newly read data
                char *search_start = read_buffer_ + buffer_data_size_ - done_size;
                newline_pos = static_cast<char *>(memchr(search_start, '\n', done_size));
                if (newline_pos != NULL) {
                  int64_t line_len = newline_pos - read_buffer_;
                  line.assign_ptr(read_buffer_, static_cast<int32_t>(line_len));
                  buffer_pos_ = line_len + 1;
                  found = true;
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObBatchFileJsonlIterator::read_chunk(char *buf, int64_t buf_size, int64_t &actual_size)
{
  int ret = OB_SUCCESS;
  actual_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(NULL == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_size));
  } else {
    // first, drain any data remaining in our internal buffer
    int64_t buffered = buffer_data_size_ - buffer_pos_;
    if (buffered > 0) {
      int64_t copy_size = MIN(buffered, buf_size);
      MEMCPY(buf, read_buffer_ + buffer_pos_, copy_size);
      buffer_pos_ += copy_size;
      actual_size = copy_size;
    } else {
      // internal buffer is empty; read directly from tmp file
      int64_t end_offset = start_offset_ + total_size_;
      int64_t file_remaining = end_offset - file_read_offset_;
      if (file_remaining <= 0) {
        actual_size = 0;  // EOF
      } else {
        int64_t read_size = MIN(buf_size, file_remaining);
        tmp_file::ObTmpFileIOInfo io_info;
        io_info.fd_ = fd_;
        io_info.buf_ = buf;
        io_info.size_ = read_size;
        io_info.io_desc_.set_wait_event(ObWaitEventIds::ROW_STORE_DISK_READ);
        io_info.io_timeout_ms_ = DEFAULT_IO_TIMEOUT_MS;

        tmp_file::ObTmpFileIOHandle handle;
        if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.pread(
                tenant_id_, io_info, file_read_offset_, handle))) {
          LOG_WARN("failed to pread from tmp file", K(ret), K_(fd),
                   K_(file_read_offset), K(read_size));
        } else {
          actual_size = handle.get_done_size();
          file_read_offset_ += actual_size;
        }
      }
    }
  }
  return ret;
}

int ObBatchFileJsonlIterator::refill_buffer_()
{
  int ret = OB_SUCCESS;
  int64_t end_offset = start_offset_ + total_size_;
  int64_t file_remaining = end_offset - file_read_offset_;
  if (file_remaining <= 0) {
    ret = OB_ITER_END;
  } else {
    int64_t read_size = MIN(buffer_size_, file_remaining);
    tmp_file::ObTmpFileIOInfo io_info;
    io_info.fd_ = fd_;
    io_info.buf_ = read_buffer_;
    io_info.size_ = read_size;
    io_info.io_desc_.set_wait_event(ObWaitEventIds::ROW_STORE_DISK_READ);
    io_info.io_timeout_ms_ = DEFAULT_IO_TIMEOUT_MS;

    tmp_file::ObTmpFileIOHandle handle;
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.pread(
            tenant_id_, io_info, file_read_offset_, handle))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to pread from tmp file", K(ret), K_(fd),
                 K_(file_read_offset), K(read_size));
      }
    } else {
      buffer_data_size_ = handle.get_done_size();
      buffer_pos_ = 0;
      file_read_offset_ += buffer_data_size_;
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
