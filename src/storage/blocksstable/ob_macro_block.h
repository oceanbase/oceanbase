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

#ifndef STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_H_
#define STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_H_

#include "lib/compress/ob_compress_util.h"
#include "index_block/ob_index_block_util.h"
#include "ob_block_sstable_struct.h"
#include "ob_data_buffer.h"
#include "ob_imicro_block_writer.h"
#include "ob_macro_block_common_header.h"
#include "ob_sstable_meta.h"
#include "share/ob_encryption_util.h"
#include "storage/blocksstable/ob_macro_block_meta.h"
#include "storage/compaction/ob_compaction_util.h"
#include "storage/compaction/ob_compaction_memory_pool.h"

namespace oceanbase {
namespace storage {
struct ObSSTableMergeInfo;
}
namespace blocksstable {
class ObSSTableIndexBuilder;
struct ObMicroBlockDesc;
struct ObMacroBlocksWriteCtx;
class ObMacroBlockHandle;
struct ObDataStoreDesc;

class ObMicroBlockCompressor
{
public:
  ObMicroBlockCompressor();
  virtual ~ObMicroBlockCompressor();
  void reset();
  int init(const int64_t micro_block_size, const ObCompressorType type);
  int compress(const char *in, const int64_t in_size, const char *&out, int64_t &out_size);
  int decompress(const char *in, const int64_t in_size, const int64_t uncomp_size,
      const char *&out, int64_t &out_size);
private:
  bool is_none_;
  int64_t micro_block_size_;
  common::ObCompressor *compressor_;
  storage::ObCompactionBufferWriter comp_buf_;
  storage::ObCompactionBufferWriter decomp_buf_;
};


class ObMacroBlock
{
public:
  ObMacroBlock();
  virtual ~ObMacroBlock();
  int init(const ObDataStoreDesc &spec, const int64_t &cur_macro_seq);
  int write_micro_block(const ObMicroBlockDesc &micro_block_desc, int64_t &data_offset);
  int write_index_micro_block(
      const ObMicroBlockDesc &micro_block_desc,
      const bool is_leaf_index_block,
      int64_t &data_offset);
  int get_macro_block_meta(ObDataMacroBlockMeta &macro_meta);
  int flush(ObMacroBlockHandle &macro_handle, ObMacroBlocksWriteCtx &block_write_ctx);
  void reset();
  void reuse();
  OB_INLINE bool is_dirty() const { return is_dirty_; }
  int64_t get_data_size() const;
  int64_t get_remain_size() const;
  int64_t get_current_macro_seq() const {return cur_macro_seq_; }
  OB_INLINE char *get_data_buf() { return data_.data(); }
  OB_INLINE int32_t get_row_count() const { return macro_header_.fixed_header_.row_count_; }
  OB_INLINE int32_t get_micro_block_count() const { return macro_header_.fixed_header_.micro_block_count_; }
  OB_INLINE ObRowStoreType get_row_store_type() const
  {
    return static_cast<ObRowStoreType>(macro_header_.fixed_header_.row_store_type_);
  }
  void update_max_merged_trans_version(const int64_t max_merged_trans_version)
  {
    if (max_merged_trans_version > max_merged_trans_version_) {
      max_merged_trans_version_ = max_merged_trans_version;
    }
  }
  void set_contain_uncommitted_row()
  {
    contain_uncommitted_row_ = true;
  }
  static int64_t calc_basic_micro_block_data_offset(
    const int64_t column_cnt,
    const int64_t rowkey_col_cnt,
    const uint16_t fixed_header_version);
private:
  int inner_init();
  int reserve_header(const ObDataStoreDesc &spec, const int64_t &cur_macro_seq);
  int check_micro_block(const ObMicroBlockDesc &micro_block_desc) const;
  int write_macro_header();
  int add_column_checksum(
      const int64_t *to_add_checksum,
      const int64_t column_cnt,
      int64_t *column_checksum);
private:
  OB_INLINE const char *get_micro_block_data_ptr() const { return data_.data() + data_base_offset_; }
  OB_INLINE int64_t get_micro_block_data_size() const { return data_.length() - data_base_offset_; }
private:
  const ObDataStoreDesc *spec_;
  storage::ObCompactionBufferWriter data_; //micro header + data blocks;
  ObSSTableMacroBlockHeader macro_header_; //macro header store in head of data_;
  int64_t data_base_offset_;
  ObDatumRowkey last_rowkey_;
  compaction::ObLocalArena rowkey_allocator_;
  bool is_dirty_;
  ObMacroBlockCommonHeader common_header_;
  int64_t max_merged_trans_version_;
  bool contain_uncommitted_row_;
  int64_t original_size_;
  int64_t data_size_;
  int64_t data_zsize_;
  int64_t cur_macro_seq_;
  bool is_inited_;
};

}
}

#endif /* OB_MACRO_BLOCK_H_ */
