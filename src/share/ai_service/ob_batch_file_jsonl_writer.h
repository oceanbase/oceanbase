/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_BATCH_FILE_JSONL_WRITER_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_BATCH_FILE_JSONL_WRITER_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/tmp_file/ob_tmp_file_io_info.h"

namespace oceanbase
{
namespace share
{

// Metadata describing a contiguous data segment in a TmpFileManager fd
struct ObBatchFileDataSegment
{
  ObBatchFileDataSegment() { reset(); }
  ~ObBatchFileDataSegment() = default;

  void reset()
  {
    fd_ = -1;
    start_offset_ = 0;
    size_ = 0;
    line_count_ = 0;
  }

  bool is_valid() const { return fd_ >= 0 && size_ >= 0; }

  TO_STRING_KV(K_(fd), K_(start_offset), K_(size), K_(line_count));

  int64_t fd_;            // TmpFileManager fd
  int64_t start_offset_;  // data start offset (bytes)
  int64_t size_;          // data size (bytes)
  int64_t line_count_;    // JSONL line count
};

/**
 * @brief Stream writer for JSONL data into TmpFileManager
 *
 * Provides two write interfaces:
 * - write_line(): for DDL Pipeline line-by-line writing (auto-appends \n)
 * - write_chunk(): for curl download callback (arbitrary byte chunks)
 *
 * Uses a fixed 64KB write buffer to keep memory usage constant.
 */
class ObBatchFileJsonlWriter
{
public:
  static const int64_t DEFAULT_BUFFER_SIZE = 64 * 1024;  // 64KB
  static const int64_t DEFAULT_IO_TIMEOUT_MS = 10000;     // 10s

  ObBatchFileJsonlWriter();
  ~ObBatchFileJsonlWriter();

  /**
   * @brief Initialize writer and open a new TmpFileManager fd
   * @param dir_id Pre-allocated directory ID from TmpFileManager
   * @param tenant_id Tenant ID for multi-tenant isolation
   * @param label Memory label for buffer allocation
   * @return OB_SUCCESS on success
   */
  int init(int64_t dir_id, uint64_t tenant_id, const char *label);

  void reset();
  void destroy();

  /**
   * @brief Write a JSONL line (auto-appends \n)
   * @param line JSONL line content (without trailing \n)
   * @return OB_SUCCESS on success
   */
  int write_line(const common::ObString &line);

  /**
   * @brief Write arbitrary byte chunk (for curl callback use)
   * @param data Pointer to data
   * @param size Data size in bytes
   * @return OB_SUCCESS on success
   */
  int write_chunk(const char *data, int64_t size);

  /**
   * @brief Finish writing: flush remaining buffer, seal fd, populate segment
   * @param segment Output segment metadata
   * @return OB_SUCCESS on success
   */
  int finish(ObBatchFileDataSegment &segment);

  int64_t get_fd() const { return fd_; }
  int64_t get_size() const { return current_size_; }
  int64_t get_line_count() const { return line_count_; }
  bool is_inited() const { return is_inited_; }

  TO_STRING_KV(K_(is_inited), K_(fd), K_(dir_id), K_(tenant_id),
               K_(current_size), K_(line_count), K_(buffer_pos));

private:
  int flush_buffer_();
  int append_to_buffer_(const char *data, int64_t size);

private:
  bool is_inited_;
  int64_t fd_;
  int64_t dir_id_;
  uint64_t tenant_id_;

  int64_t current_size_;   // total bytes written to TmpFileManager
  int64_t line_count_;     // total JSONL lines written

  char *write_buffer_;
  int64_t buffer_size_;
  int64_t buffer_pos_;

  DISALLOW_COPY_AND_ASSIGN(ObBatchFileJsonlWriter);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_BATCH_FILE_JSONL_WRITER_H_
