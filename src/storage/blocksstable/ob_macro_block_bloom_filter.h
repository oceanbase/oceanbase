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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_BLOOM_FILTER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_BLOOM_FILTER_H_

#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/blocksstable/ob_bloom_filter_cache.h"
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/compaction/ob_compaction_memory_context.h"

namespace oceanbase
{
namespace blocksstable
{

class ObDatumRow;
class ObDataStoreDesc;
class ObStorageDatumUtils;
class ObMicroIndexInfo;
class ObMicroBlock;
class ObBloomFilter;

class ObMacroBlockBloomFilter
{
public:
  static const int32_t MACRO_BLOCK_BLOOM_FILTER_V1 = 1;

public:
  ObMacroBlockBloomFilter();
  ~ObMacroBlockBloomFilter();
  int alloc_bf(const ObDataStoreDesc &data_store_desc, const int64_t bf_size = 64 * 1024 /* 64KB */);
  bool is_valid() const;
  bool should_persist() const;
  int64_t calc_max_row_count() const;
  int insert_row(const ObDatumRow &row);
  int insert_micro_block(const ObMicroBlock &micro_block);
  int insert_micro_block(const ObMicroBlockDesc &micro_block_desc, const ObMicroIndexInfo &micro_index_info);
  OB_INLINE int64_t get_row_count() const { return row_count_; }
  OB_INLINE const ObBloomFilter & get_bloom_filter() const { return bf_; }
  void reuse();
  void reset();
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size() const;
  TO_STRING_KV(K(rowkey_column_count_),
               KPC(datum_utils_),
               K(enable_macro_block_bloom_filter_),
               K(max_row_count_),
               K(version_),
               K(bf_),
               K(row_count_));

private:
  int decrypt_and_decompress_micro_data(const ObMicroBlockHeader &header,
                                        const ObMicroBlockData &micro_data,
                                        const ObMicroIndexInfo &micro_index_info,
                                        ObMicroBlockData &decompressed_data);

private:
  int64_t rowkey_column_count_;
  const blocksstable::ObStorageDatumUtils * datum_utils_;
  bool enable_macro_block_bloom_filter_;
  int64_t max_row_count_;
  int32_t version_;
  int64_t row_count_;  // row count of this macro block rather than bloom filter.
  ObBloomFilter bf_;
  ObMacroBlockReader macro_reader_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_BLOOM_FILTER_H_