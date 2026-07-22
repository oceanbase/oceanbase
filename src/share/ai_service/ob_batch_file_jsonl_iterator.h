/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_BATCH_FILE_JSONL_ITERATOR_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_BATCH_FILE_JSONL_ITERATOR_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/tmp_file/ob_tmp_file_io_info.h"
#include "storage/tmp_file/ob_tmp_file_io_handle.h"

namespace oceanbase
{
namespace share
{

/**
 * @brief Stream reader for JSONL data from TmpFileManager
 *
 * Provides two read interfaces:
 * - get_next_line(): returns next \n-delimited JSONL line (for parsing)
 * - read_chunk(): returns raw bytes up to buf_size (for curl upload callback)
 *
 * Handles lines spanning buffer boundaries by preserving partial data
 * across refills. Uses a fixed 64KB read buffer.
 */
class ObBatchFileJsonlIterator
{
public:
  static const int64_t DEFAULT_BUFFER_SIZE = 64 * 1024;  // 64KB
  static const int64_t DEFAULT_IO_TIMEOUT_MS = 10000;     // 10s

  ObBatchFileJsonlIterator();
  ~ObBatchFileJsonlIterator();

  /**
   * @brief Initialize iterator for reading a data range from TmpFileManager
   * @param fd TmpFileManager file descriptor
   * @param start_offset Start offset in fd (typically 0 for per-task fd)
   * @param size Total data size to read
   * @param tenant_id Tenant ID
   * @return OB_SUCCESS on success
   */
  int init(int64_t fd, int64_t start_offset, int64_t size, uint64_t tenant_id);

  void reset();
  void destroy();

  /**
   * @brief Read next \n-delimited JSONL line
   * @param line Output line (points into internal buffer, valid until next call)
   * @return OB_SUCCESS on success, OB_ITER_END when no more lines
   */
  int get_next_line(common::ObString &line);

  /**
   * @brief Read raw bytes (for curl upload callback)
   * @param buf Output buffer
   * @param buf_size Maximum bytes to read
   * @param actual_size Actual bytes read (0 at EOF)
   * @return OB_SUCCESS on success
   */
  int read_chunk(char *buf, int64_t buf_size, int64_t &actual_size);

  int64_t get_read_offset() const { return file_read_offset_; }
  int64_t get_remaining_size() const { return total_size_ - (file_read_offset_ - start_offset_); }
  bool has_more() const { return file_read_offset_ < start_offset_ + total_size_ || buffer_data_size_ > buffer_pos_; }
  bool is_inited() const { return is_inited_; }

  TO_STRING_KV(K_(is_inited), K_(fd), K_(start_offset), K_(total_size),
               K_(file_read_offset), K_(buffer_pos), K_(buffer_data_size));

private:
  int refill_buffer_();

private:
  bool is_inited_;
  int64_t fd_;
  int64_t start_offset_;
  int64_t total_size_;
  int64_t file_read_offset_;  // next offset to read from TmpFileManager
  uint64_t tenant_id_;

  char *read_buffer_;
  int64_t buffer_size_;
  int64_t buffer_data_size_;  // valid data bytes in buffer
  int64_t buffer_pos_;        // current read position in buffer

  DISALLOW_COPY_AND_ASSIGN(ObBatchFileJsonlIterator);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_BATCH_FILE_JSONL_ITERATOR_H_
