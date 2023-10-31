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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_BLOOM_FILTER_DATA_WRITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_BLOOM_FILTER_DATA_WRITER_H_

#include "lib/ob_define.h"
#include "lib/container/ob_array.h"
#include "ob_macro_block.h"
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/blocksstable/ob_bloom_filter_cache.h"
#include "storage/compaction/ob_compaction_memory_pool.h"

namespace oceanbase
{
namespace blocksstable
{

class ObBloomFilterMicroBlockWriter
{
public:
  ObBloomFilterMicroBlockWriter();
  virtual ~ObBloomFilterMicroBlockWriter();
  void reset();
  void reuse();
  int init(const int64_t micro_block_size);
  int write(const ObBloomFilterCacheValue &bf_cache_value, const char *&block_buf, int64_t &block_size);
private:
  int build_micro_block_header(const int64_t rowkey_column_count, const int64_t row_count);
private:
  ObBloomFilterMicroBlockHeader *bf_micro_header_;
  storage::ObCompactionBufferWriter data_buffer_;
  bool is_inited_;
};

class ObBloomFilterMacroBlockWriter
{
public:
  ObBloomFilterMacroBlockWriter();
  virtual ~ObBloomFilterMacroBlockWriter();
  void reset();
  void reuse();
  int init(const ObDataStoreDesc &desc);
  int write(const ObBloomFilterCacheValue &bf_cache_value);
  OB_INLINE ObMacroBlocksWriteCtx &get_block_write_ctx() { return block_write_ctx_; }
private:
  int init_headers(const int64_t row_count);
  int write_micro_block(const char *comp_block_buf, const int64_t comp_block_size,
                        const int64_t orig_block_size);
  int flush_macro_block();
private:
  storage::ObCompactionBufferWriter data_buffer_;
  ObBloomFilterMacroBlockHeader *bf_macro_header_;
  ObMacroBlockCommonHeader common_header_;
  ObMicroBlockCompressor compressor_;
  ObBloomFilterMicroBlockWriter bf_micro_writer_;
  ObMacroBlocksWriteCtx block_write_ctx_;
  const ObDataStoreDesc *desc_;
  bool is_inited_;
};

class ObBloomFilterDataWriter
{
public:
  ObBloomFilterDataWriter();
  virtual ~ObBloomFilterDataWriter();
  int init(const ObDataStoreDesc &desc);
  void reset();
  void reuse();
  int append(const ObDatumRowkey &rowkey, const ObStorageDatumUtils &datum_utils);
  int append(const ObBloomFilterCacheValue &bf_cache_value);
  int flush_bloom_filter();
  OB_INLINE int32_t get_row_count() const { return bf_cache_value_.get_row_count(); }
  OB_INLINE ObMacroBlocksWriteCtx &get_block_write_ctx() { return bf_macro_writer_.get_block_write_ctx(); }
  OB_INLINE const ObBloomFilterCacheValue &get_bloomfilter_cache_value() const { return bf_cache_value_; }
private:
  static const int64_t BLOOM_FILTER_MAX_ROW_COUNT = 1500000L;
  ObBloomFilterCacheValue bf_cache_value_;
  ObBloomFilterMacroBlockWriter bf_macro_writer_;
  int64_t rowkey_column_count_;
  bool is_inited_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_BLOOM_FILTER_DATA_WRITER_H_
