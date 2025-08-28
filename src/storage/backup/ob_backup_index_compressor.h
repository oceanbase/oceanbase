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

#ifndef STORAGE_LOG_STREAM_BACKUP_INDEX_COMPRESSOR_H_
#define STORAGE_LOG_STREAM_BACKUP_INDEX_COMPRESSOR_H_

#include "lib/compress/ob_compressor.h"
#include "lib/compress/ob_compress_util.h"
#include "storage/blocksstable/ob_data_buffer.h"

namespace oceanbase
{
namespace backup
{

// This class represents a compressor for backup index blocks.
class ObBackupIndexBlockCompressor final
{
public:
  ObBackupIndexBlockCompressor();
  ~ObBackupIndexBlockCompressor();
  void reuse();
  void reset();
  int init(const int64_t block_size, const common::ObCompressorType type);
  int compress(const char *in, const int64_t in_size, const char *&out, int64_t &out_size);
  int decompress(const char *in, const int64_t in_size, const int64_t uncomp_size,
      const char *&out, int64_t &out_size);

private: 
  bool is_inited_;
  bool is_none_;
  int64_t block_size_;
  common::ObCompressor *compressor_;
  blocksstable::ObSelfBufferWriter comp_buf_;
  blocksstable::ObSelfBufferWriter decomp_buf_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupIndexBlockCompressor);
};

}
}

#endif