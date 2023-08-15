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
#include "share/schema/ob_table_param.h"
#include "storage/slog_ckpt/ob_linked_macro_block_struct.h"
#include "ob_macro_block_common_header.h"
#include "ob_imicro_block_reader.h"
#include "ob_micro_block_encryption.h"
namespace oceanbase
{
namespace common
{
class ObCompressor;
}
namespace share
{
namespace schema
{
struct ObColDesc;
}
}
namespace blocksstable
{
struct ObMicroBlockData;
struct ObMicroBlockDesc;
class ObDataMacroBlockMeta;
class ObMacroBlockRowBareIterator;
class ObMacroBlockReader
{
public:
  ObMacroBlockReader();
  virtual ~ObMacroBlockReader();
  int decompress_data(
      const common::ObCompressorType compressor_type,
      const char *buf,
      const int64_t size,
      const char *&uncomp_buf,
      int64_t &uncomp_size,
      bool &is_compressed);
  int decompress_data_buf(
      const common::ObCompressorType compressor_type,
      const char *header_buf,
      const int64_t header_size,
      const char *comp_buf,
      const int64_t comp_size,
      const char *&uncomp_buf,
      int64_t &uncomp_size,
      ObIAllocator *ext_allocator = nullptr);
  int decompress_data_with_prealloc_buf(
      const common::ObCompressorType compressor_type,
      const char *buf,
      const int64_t size,
      char *uncomp_buf,
      const int64_t uncomp_buf_size);
  int decompress_data_with_prealloc_buf(
      const char *compressor_name,
      const char *buf,
      const int64_t size,
      char *uncomp_buf,
      const int64_t uncomp_buf_size);
  int decrypt_and_decompress_data(
      const ObSSTableMacroBlockHeader &block_header,
      const char *buf,
      const int64_t size,
      const char *&uncomp_buf,
      int64_t &uncomp_size,
      bool &is_compressed);
  int decrypt_and_decompress_data(
      const ObMicroBlockDesMeta &deserialize_meta,
      const char *input,
      const int64_t size,
      const char *&uncomp_buf,
      int64_t &uncomp_size,
      bool &is_compressed,
      const bool need_deep_copy = false,
      ObIAllocator *ext_allocator = nullptr);
#ifdef OB_BUILD_TDE_SECURITY
  int decrypt_buf(
      const ObMicroBlockDesMeta &deserialize_meta,
      const char *buf,
      const int64_t size,
      const char *&decrypt_buf,
      int64_t &decrypt_size);
#endif
private:
  int alloc_buf(const int64_t req_size, char *&buf, int64_t &buf_size);
  int alloc_buf(ObIAllocator &allocator, const int64_t buf_size, char *&buf);
#ifdef OB_BUILD_TDE_SECURITY
  int init_encrypter_if_needed();
#endif

private:
  common::ObCompressor *compressor_;
  char *uncomp_buf_;
  int64_t uncomp_buf_size_;
  char *decrypt_buf_;
  int64_t decrypt_buf_size_;
  common::ObArenaAllocator allocator_;
  ObMicroBlockEncryption *encryption_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMacroBlockReader);
};

class ObSSTableDataBlockReader
{
public:
  ObSSTableDataBlockReader();
  virtual ~ObSSTableDataBlockReader();

  int init(const char *data, const int64_t size, const bool hex_print = false);
  void reset();
  int dump(const uint64_t tablet_id, const int64_t scn);
private:
  int dump_sstable_macro_block(const bool is_index_block);
  int dump_bloom_filter_data_block();
  int dump_sstable_micro_block(
      const int64_t micro_idx,
      const bool is_index_block,
      ObMacroBlockRowBareIterator &macro_bare_iter);
  int dump_macro_block_meta_block(ObMacroBlockRowBareIterator &macro_bare_iter);
  int dump_sstable_micro_header(
      const ObMicroBlockData &micro_data,
      const int64_t micro_idx,
      const bool is_index_block);
  int dump_sstable_micro_data(
      const bool is_index_block,
      ObMacroBlockRowBareIterator &macro_bare_iter);
  int dump_sstable_micro_data(const ObMicroBlockData &micro_data, const bool is_index_block);
  int dump_column_info(const int64_t col_cnt, const int64_t type_array_col_cnt);
  bool check_need_print(const uint64_t tablet_id, const int64_t scn);
private:
  // raw data
  const char *data_;
  int64_t size_;
  ObMacroBlockCommonHeader common_header_;
  ObSSTableMacroBlockHeader macro_header_;
  ObLinkedMacroBlockHeader linked_header_;
  // parsed objects
  const ObBloomFilterMacroBlockHeader *bloomfilter_header_;
  const common::ObObjMeta *column_types_;
  const common::ObOrderType *column_orders_;
  const int64_t *column_checksum_;
  common::ObSEArray<share::schema::ObColDesc, common::OB_DEFAULT_SE_ARRAY_COUNT> columns_;
  // facility objects
  ObMacroBlockReader macro_reader_;
  common::ObArenaAllocator allocator_;
  blocksstable::ObDatumRow row_;
  char *hex_print_buf_;
  bool is_trans_sstable_;
  bool is_inited_;
  int64_t column_type_array_cnt_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTableDataBlockReader);
};
} /* namespace blocksstable */
} /* namespace oceanbase */

#endif /* OB_MACRO_BLOCK_READER_H_ */
