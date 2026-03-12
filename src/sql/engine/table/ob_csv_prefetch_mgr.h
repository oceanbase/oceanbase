/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_CSV_PREFETCH_MGR_H
#define OB_CSV_PREFETCH_MGR_H

#include "lib/allocator/ob_allocator.h"
#include "sql/engine/table/ob_external_file_access.h"
#include "sql/engine/cmd/ob_load_data_parser.h"

namespace oceanbase {
namespace sql {

class ObDecompressor;

class ObCSVPrefetchMgr
{
public:
  ObCSVPrefetchMgr();
  ~ObCSVPrefetchMgr();

  int init(common::ObIAllocator &allocator,
           ObCSVGeneralFormat::ObCSVCompression compression_format,
           const int64_t buf_size = OB_MALLOC_BIG_BLOCK_SIZE,
           const int64_t prefetch_count = 5);

  int open(const ObExternalFileUrlInfo &file_info,
           const ObExternalFileCacheOptions &cache_options,
           const int64_t start_offset = 0,
           const int64_t end_offset = INT64_MAX);

  int get_buffer(char *buf, const int64_t buf_size, int64_t &read_size);

  void close();

  void reset();
  bool eof() const;
  bool is_inited() const { return is_inited_; }

private:
  static const int64_t DEFAULT_IO_TIMEOUT_MS = 5000;
  static const int64_t DEFAULT_COMPRESSED_DATA_BUFFER_SIZE = 2 * 1024 * 1024;

  enum class SlotState
  {
    EMPTY = 0,
    LOADING,
  };

  struct PrefetchSlot
  {
    char *buf_;
    int64_t data_size_;
    ObExternalFileReadHandle handle_;
    SlotState state_;

    PrefetchSlot() : buf_(nullptr), data_size_(0), state_(SlotState::EMPTY) {}
    void reuse()
    {
      data_size_ = 0;
      handle_.reset();
      state_ = SlotState::EMPTY;
    }
  };

  int submit_prefetch(int64_t slot_idx);
  int create_decompressor(ObCSVGeneralFormat::ObCSVCompression compression_format);
  int get_buffer_decompress(char *buf, const int64_t buf_size, int64_t &read_size);
  int get_buffer_raw(char *buf, const int64_t buf_size, int64_t &read_size);
  int read_compressed_data();

private:
  ObExternalFileAccess file_access_;
  PrefetchSlot *slots_;
  int64_t prefetch_buf_size_;
  int64_t prefetch_count_;
  int64_t cur_slot_idx_;
  int64_t pending_slot_count_;
  int64_t next_prefetch_offset_;
  int64_t file_size_;
  int64_t end_offset_;
  bool all_prefetched_;
  bool is_inited_;
  common::ObIAllocator *allocator_;

  char *remain_buf_;
  int64_t remain_data_size_;

  // compression support
  ObDecompressor *decompressor_;
  ObCSVGeneralFormat::ObCSVCompression compression_format_;
  char *compressed_data_;
  int64_t compressed_data_capacity_;
  int64_t compress_data_size_;
  int64_t consumed_data_size_;

  DISALLOW_COPY_AND_ASSIGN(ObCSVPrefetchMgr);
};

}
}

#endif // OB_CSV_PREFETCH_MGR_H
