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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_ENTRY_ITERATOR_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_ENTRY_ITERATOR_H_

#include "lib/compress/ob_compressor_pool.h"
#include "common/ob_partition_key.h"
#include "clog/ob_log_block.h"
#include "clog/ob_log_common.h"
#include "clog/ob_log_define.h"
#include "clog/ob_log_reader_interface.h"
#include "ob_log_archive_define.h"
#include "ob_log_archive_struct.h"

namespace oceanbase {
namespace clog {
class ObLogEntry;
}
namespace archive {

enum ObArchiveItemType {
  UNKNOWN_TYPE = 0,
  ARCHIVE_BLOCK_META = 1,
  CLOG_ENTRY = 2,
  ARCHIVE_COMPRESSED_CHUNK = 3,
  ARCHIVE_ENCRYPTED_CHUNK = 4,
  ARCHIVE_COMPRESSED_ENCRYPTED_CHUNK = 5,
};

class ObIArchiveLogFileStore;
struct LogBufPackage {
  LogBufPackage();
  ~LogBufPackage();
  bool is_empty();

  char* cur_;
  char* end_;

  TO_STRING_KV(K(cur_), K(end_), K(rbuf_));

  clog::ObReadBuf rbuf_;
};

/*
 * single archive data file per iterator, start from start_offset
 */
class ObArchiveEntryIterator {
public:
  ObArchiveEntryIterator();
  ~ObArchiveEntryIterator()
  {
    reset();
  }
  void reset();
  static int parse_archive_item_type(const char* buf, const int64_t len, ObArchiveItemType& type);

public:
  int init(ObIArchiveLogFileStore* file_store, const common::ObPGKey& pg_key, const uint64_t file_id,
      const int64_t start_offset, const int64_t timeout, const bool need_limit_bandwidth,
      const uint64_t real_tenant_id = common::OB_INVALID_TENANT_ID);
  void destroy()
  {
    reset();
  }
  int next_entry(clog::ObLogEntry& entry);
  int64_t get_cur_block_start_offset() const
  {
    return cur_block_start_offset_;
  }
  const ObArchiveBlockMeta& get_last_block_meta() const
  {
    return last_block_meta_;
  }
  int64_t get_io_cost() const
  {
    return io_cost_;
  }
  int64_t get_io_count() const
  {
    return io_count_;
  }
  int64_t get_limit_bandwidth_cost() const
  {
    return limit_bandwidth_cost_;
  }
  TO_STRING_KV(K(is_inited_), K(need_limit_bandwidth_), KP(file_store_), K(pg_key_), K(real_tenant_id_), K(file_id_),
      K(cur_offset_), K(cur_block_start_offset_), K(cur_block_end_offset_), KP(buf_cur_), KP(buf_end_), KP(block_end_),
      K(rbuf_), K(dd_buf_), K(origin_buf_), K(timeout_), K(io_cost_), K(io_count_), K(limit_bandwidth_cost_),
      K(has_load_entire_file_));

private:
  int prepare_buffer_();
  void advance_(const int64_t step);
  int get_entry_type_(ObArchiveItemType& type) const;
  int get_next_entry_(clog::ObLogEntry& entry);
  void handle_serialize_ret_(int& ret_code);
  int limit_bandwidth_and_sleep_(const int64_t read_size);

  void reset_log_buf_package_();
  int consume_decompress_decrypt_buf_(clog::ObLogEntry& entry);
  int consume_origin_buf_(clog::ObLogEntry& entry);
  int get_log_entry_(char* buf, const int64_t buf_size, int64_t& pos, clog::ObLogEntry& entry);
  int try_construct_log_buf_();
  int extract_block_meta_(ObArchiveBlockMeta& meta);
  int decompress_buf_(ObArchiveBlockMeta& block_meta);
  int decompress_(const char* src_buf, const int64_t src_size, common::ObCompressorType compress_type, char* dst_buf,
      const int64_t dst_buf_size, int64_t& dst_data_size);
  void fill_log_package_(char* buf, const int64_t buf_len);
  int fill_origin_buf_(ObArchiveBlockMeta& block_meta);

private:
  static const int64_t MAGIC_NUM_LEN = 2;
  const int64_t MAX_IDLE_TIME = 10 * 1000LL * 1000LL;  // 10s
  const int64_t MAX_READ_BUF_SIZE = MAX_ARCHIVE_BLOCK_SIZE;
  bool is_inited_;
  bool need_limit_bandwidth_;
  ObIArchiveLogFileStore* file_store_;

  // pg_key value valid only in OB, faked in ob_admin for convenient usage
  common::ObPGKey pg_key_;
  // get encrypt meta from tennat parameter according to real tenant id
  uint64_t real_tenant_id_;

  // current consume archive data file
  uint64_t file_id_;
  // current offset in current archive data file, step by every archive block
  int64_t cur_offset_;
  // start offset of current archive block in current archive data file, not used
  int64_t cur_block_start_offset_;
  // end offset of current archive block in current archive data file, not used
  int64_t cur_block_end_offset_;
  // start of current consumed buffer
  char* buf_cur_;
  // end of current consumed buffer
  char* buf_end_;

  // end of current block
  char* block_end_;

  // buf to read archive data file
  clog::ObReadBuf rbuf_;
  clog::ObReadCost read_cost_;
  // decompressed or decrypted buffer to consume, dd: decryption decompress
  // both original and encrypted buffer smaller than 2M
  LogBufPackage dd_buf_;
  // original buffer to consume, not compress or encrypt
  LogBufPackage origin_buf_;

  // timeout threshold to read archive data
  int64_t timeout_;
  // time cost in iteration
  int64_t io_cost_;
  // IO count in iteration
  int64_t io_count_;
  // sleep time due to bandwidth limit in iteration
  int64_t limit_bandwidth_cost_;
  bool has_load_entire_file_;
  ObArchiveBlockMeta last_block_meta_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObArchiveEntryIterator);
};

}  // end namespace archive
}  // end namespace oceanbase

#endif  // OCEANBASE_ARCHIVE_OB_ARCHIVE_ENTRY_ITERATOR_H_
