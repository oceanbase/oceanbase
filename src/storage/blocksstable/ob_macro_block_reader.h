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

#ifndef OB_MACRO_BLOCK_READER_H_
#define OB_MACRO_BLOCK_READER_H_

#include "lib/hash/ob_array_index_hash_set.h"
#include "lib/compress/ob_compressor.h"
#include "storage/blocksstable/ob_column_map.h"
#include "ob_macro_block_common_header.h"
#include "ob_micro_block_index_mgr.h"
#include "ob_imicro_block_reader.h"
namespace oceanbase {
namespace common {
class ObCompressor;
}
namespace share {
namespace schema {
struct ObColDesc;
}
}  // namespace share
namespace blocksstable {
class ObFullMacroBlockMeta;
class ObMacroBlockReader {
public:
  ObMacroBlockReader();
  virtual ~ObMacroBlockReader();
  int decompress_data(const ObFullMacroBlockMeta& meta, const char* buf, const int64_t size, const char*& uncomp_buf,
      int64_t& uncomp_size, bool& is_compressed, const bool need_deep_copy = false);
  int decompress_data(const char* compressor_name, const char* buf, const int64_t size, const char*& uncomp_buf,
      int64_t& uncomp_size, bool& is_compressed);
  int decompress_data_with_prealloc_buf(const char* compressor_name, const char* buf, const int64_t size,
      char* uncomp_buf, const int64_t uncomp_buf_size);

private:
  int alloc_buf(const int64_t buf_size);
  common::ObCompressor* compressor_;
  char* uncomp_buf_;
  int64_t uncomp_buf_size_;
  common::ObArenaAllocator allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMacroBlockReader);
};

class ObSSTableDataBlockReader {
public:
  ObSSTableDataBlockReader();
  virtual ~ObSSTableDataBlockReader();

  int init(const char* data, const int64_t size);
  void reset();
  int dump();

private:
  int dump_macro_block_header();
  int dump_sstable_data_block();
  int dump_lob_data_block();
  int dump_bloom_filter_data_block();
  int dump_sstable_micro_header(const char* micro_block_buf, const int64_t micro_idx);
  int dump_sstable_micro_data(const ObMicroBlockData& micro_data);
  int decompressed_micro_block(const char* micro_block_buf, const int64_t micro_block_size,
      const int16_t micro_header_magic, ObMicroBlockData& micro_data);

private:
  // raw data
  const char* data_;
  int64_t size_;
  ObMacroBlockCommonHeader common_header_;
  // parsed objects
  const ObSSTableMacroBlockHeader* block_header_;
  const uint16_t* column_ids_;
  const common::ObObjMeta* column_types_;
  const common::ObOrderType* column_orders_;
  const int64_t* column_checksum_;
  common::ObSEArray<share::schema::ObColDesc, common::OB_DEFAULT_SE_ARRAY_COUNT> columns_;
  // facility objects
  ObMacroBlockReader macro_reader_;
  ObColumnMap col_map_;
  common::ObArenaAllocator allocator_;
  bool is_trans_sstable_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTableDataBlockReader);
};
} /* namespace blocksstable */
} /* namespace oceanbase */

#endif /* OB_MACRO_BLOCK_READER_H_ */
