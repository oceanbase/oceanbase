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

#ifndef OCEANBASE_CLOG_OB_DIRECT_LOG_READER_
#define OCEANBASE_CLOG_OB_DIRECT_LOG_READER_

#include <libaio.h>
#include "lib/oblog/ob_base_log_buffer.h"
#include "share/redolog/ob_log_file_store.h"
#include "ob_log_define.h"
#include "ob_log_common.h"
#include "ob_log_file_pool.h"
#include "ob_log_reader_interface.h"
#include "ob_log_cache.h"

namespace oceanbase {
namespace share {
class ObLogReadFdHandle;
}
namespace common {
class ObILogFileStore;
}
namespace clog {
class Log2File;
// Alloc a buffer which is aligned, used to read or write file directly
class ObAlignedBuffer {
public:
  ObAlignedBuffer();
  ~ObAlignedBuffer();
  // Allocate memory space according to size and align_size during initialization
  int init(const int64_t size, const int64_t align_size, const char* label);
  void destroy();
  void reuse();
  bool is_inited() const
  {
    return is_inited_;
  }
  char* get_align_buf() const
  {
    return align_buf_;
  }
  int64_t get_size() const
  {
    return size_;
  }
  TO_STRING_KV(KP(align_buf_), K(size_), K(align_size_), K(is_inited_));

private:
  // Save the aligned starting address in the allocated space
  char* align_buf_;
  // The size of the externally visible space
  int64_t size_;
  // Size of address alignment
  int64_t align_size_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAlignedBuffer);
};

class ObLogDirectReader : public ObILogDirectReader {
public:
  ObLogDirectReader();
  virtual ~ObLogDirectReader();

public:
  int init(const char* log_dir, const char* shm_buf, const bool use_cache, ObLogCache* log_cache, ObTailCursor* tail,
      ObLogWritePoolType type);
  int get_file_id_range(uint32_t& min_file_id, uint32_t& max_file_id) const;
  void destroy();

public:
  // Interface of read
  // Read data from disk directlly
  //
  // Feature:
  // 1. rbuf is aligned by CLOG_DIO_ALIGN_SIZE
  // 2. Not support read data from different logs
  // 3. The amount read from the disk at a time must
  //    be an integer multiple of CLOG_DIO_ALIGN_SIZE,
  //    therefore, if read_len is smaller than CLOG_DIO_ALIGN_SIZE,
  //    the actually read length is CLOG_DIO_ALIGN_SIZE
  // 4. If the offset of the read file is not aligned according to
  //    CLOG_DIO_ALIGN_SIZE, then the starting position of the buffer
  //    returned is not equal to rbuf
  // 5. It will be truncated if the size exceeds OB_MAX_LOG_BUFFER_SIZE
  int read_data_direct(const ObReadParam& param, ObReadBuf& rbuf, ObReadRes& res, ObReadCost& cost);

  int read_data(const ObReadParam& param, const common::ObAddr& addr, const int64_t seq, ObReadBuf& rbuf,
      ObReadRes& res, ObReadCost& cost);
  int read_log(const ObReadParam& param, const common::ObAddr& addr, const int64_t seq, ObReadBuf& rbuf,
      ObLogEntry& entry, ObReadCost& cost);
  int read_trailer(
      const ObReadParam& param, ObReadBuf& rbuf, int64_t& start_pos, file_id_t& next_file_id, ObReadCost& cost);
  int read_data_from_hot_cache(const common::ObAddr& addr, const int64_t seq, const file_id_t want_file_id,
      const offset_t want_offset, const int64_t want_size, char* user_buf);
  int get_batch_log_cursor(const common::ObPartitionKey& partition_key, const common::ObAddr& addr, const int64_t seq,
      const uint64_t log_id, const Log2File& log2file_item, ObGetCursorResult& result);
  // interface only used by clog
  int get_clog_real_length(
      const common::ObAddr& addr, const int64_t seq, const ObReadParam& param, int64_t& real_length);
  // Return the origin data, if data has been compressed, need uncompress it,
  // therefore, want_size is a size of ObLogEntry.
  int read_uncompressed_data_from_hot_cache(const common::ObAddr& addr, const int64_t seq, const file_id_t want_file_id,
      const offset_t want_offset, const int64_t want_size, char* user_buf, const int64_t buf_size,
      int64_t& origin_size);

private:
  offset_t limit_by_file_size(const int64_t offset) const;
  int limit_param_by_tail(const ObReadParam& want_param, ObReadParam& real_param);
  void align_param_for_dio_without_cache(const ObReadParam& param, ObReadParam& align_param, int64_t& backoff);
  int read_data_from_file(
      const ObReadParam& aligned_param, ObReadBuf& rbuf, ObReadRes& read_file_res, ObReadCost& read_cost);
  inline void stat_cache_hit_rate();
  int handle_no_file(const file_id_t file_id);
  int64_t calc_read_size(const int64_t rbuf_len, const offset_t line_key_offset) const;
  int read_lines_from_file(const file_id_t want_file_id, const share::ObLogReadFdHandle& fd_handle,
      const offset_t line_key_offset, ObReadBuf& read_buf, int64_t& total_read_size);
  int put_into_kvcache(const common::ObAddr& addr, const int64_t seq, const file_id_t want_file_id,
      const offset_t line_key_offset, char* buf);
  int read_disk_and_update_kvcache(const common::ObAddr& addr, const int64_t seq, const file_id_t want_file_id,
      const offset_t line_key_offset, ObReadBuf& read_buf, const char*& line_buf, ObReadCost& cost);
  int get_line_buf(common::ObKVCacheHandle& handle, const common::ObAddr& addr, const int64_t seq,
      const file_id_t want_file_id, const offset_t line_key_offset, ObReadBuf& read_buf, const char*& line_buf,
      ObReadCost& cost);
  int read_from_cold_cache(common::ObKVCacheHandle& handle, const common::ObAddr& addr, const int64_t seq,
      const file_id_t want_file_id, const offset_t want_offset, const int64_t want_size, char* user_buf,
      ObReadCost& cost);
  int read_data_direct_impl(const ObReadParam& param, ObReadBuf& rbuf, ObReadRes& res, ObReadCost& cost);
  int read_data_impl(common::ObKVCacheHandle& handle, const common::ObAddr& addr, const int64_t seq,
      const file_id_t want_file_id, const offset_t want_offset, const int64_t want_size, char* user_buf,
      ObReadCost& cost);
  int read_from_io(const file_id_t want_file_id, const share::ObLogReadFdHandle& fd_handle, char* buf,
      const int64_t count, const int64_t offset, int64_t& read_count);
  int read_disk(const share::ObLogReadFdHandle& fd_handle, char* buf, const int64_t count, const int64_t offset,
      int64_t& read_count);

  int is_valid_read_param(const ObReadParam& param, bool& is_valid) const;

  int get_log_file_type(ObLogWritePoolType& file_type) const;

private:
  static const int64_t STAT_TIME_INTERVAL = 10 * 1000 * 1000;
  static const int64_t READ_DISK_SIZE = CLOG_CACHE_SIZE;

private:
  bool is_inited_;
  bool use_cache_;
  common::ObILogFileStore* file_store_;
  ObLogCache* log_cache_;
  ObTailCursor* tail_;
  common::ObBaseLogBufferCtrl* log_ctrl_;
  common::ObBaseLogBuffer* log_buffer_;
  int64_t cache_miss_count_;
  int64_t cache_hit_count_;
  DISALLOW_COPY_AND_ASSIGN(ObLogDirectReader);
};
}  // end namespace clog
}  // end namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_DIRECT_LOG_READER_
