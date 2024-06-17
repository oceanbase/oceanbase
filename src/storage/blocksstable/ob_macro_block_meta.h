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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_META_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_META_H_

#include "lib/compress/ob_compress_util.h"
#include "storage/blocksstable/ob_datum_rowkey.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "share/schema/ob_table_param.h"
#include "share/ob_encryption_util.h"
#include "common/ob_store_format.h"
#include "storage/blocksstable/ob_logic_macro_id.h"


namespace oceanbase
{
namespace blocksstable
{
enum ObMacroBlockMetaType
{
  DATA_BLOCK_META = 0,
  MAX = 1,
};

class ObDataBlockMetaVal final
{
public:
  static const int32_t DATA_BLOCK_META_VAL_VERSION = 1;
  static const int32_t DATA_BLOCK_META_VAL_VERSION_V2 = 2;
public:
  ObDataBlockMetaVal();
  explicit ObDataBlockMetaVal(ObIAllocator &allocator);
  ~ObDataBlockMetaVal();
  void reset();
  bool is_valid() const;
  int assign(const ObDataBlockMetaVal &val);
  int build_value(ObStorageDatum &datum, ObIAllocator &allocator) const;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size() const;
  int64_t get_max_serialize_size() const;
  TO_STRING_KV(K_(version), K_(length), K_(data_checksum), K_(rowkey_count),
        K_(column_count), K_(micro_block_count), K_(occupy_size), K_(data_size),
        K_(data_zsize), K_(original_size), K_(progressive_merge_round), K_(block_offset), K_(block_size), K_(row_count),
        K_(row_count_delta), K_(max_merged_trans_version), K_(is_encrypted),
        K_(is_deleted), K_(contain_uncommitted_row), K_(compressor_type),
        K_(master_key_id), K_(encrypt_id), K_(encrypt_key), K_(row_store_type),
        K_(schema_version), K_(snapshot_version), K_(is_last_row_last_flag),
        K_(logic_id), K_(macro_id), K_(column_checksums), K_(has_string_out_row), K_(all_lob_in_row),
          K_(agg_row_len), KP_(agg_row_buf), K_(ddl_end_row_offset));
public:
  int32_t version_;
  int32_t length_;
  int64_t data_checksum_;
  int64_t rowkey_count_;
  int64_t column_count_;
  int64_t micro_block_count_;
  int64_t occupy_size_;   // size of whole macro block (including headers)
  int64_t data_size_; // sum of size of micro blocks (after encoding)
  int64_t data_zsize_;    // sum of size of compressed/encrypted micro blocks
  int64_t original_size_; // sum of size of original micro blocks
  int64_t progressive_merge_round_;
  int64_t block_offset_;  // offset of n-1 level index micro blocks
  int64_t block_size_;    // size of n-1 level index micro blocks
  int64_t row_count_;
  int64_t row_count_delta_;
  int64_t max_merged_trans_version_;
  bool is_encrypted_;
  bool is_deleted_;
  bool contain_uncommitted_row_;
  bool is_last_row_last_flag_;
  ObCompressorType compressor_type_;
  int64_t master_key_id_;
  int64_t encrypt_id_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  ObRowStoreType row_store_type_;
  uint64_t schema_version_;
  int64_t snapshot_version_;
  ObLogicMacroBlockId logic_id_;
  MacroBlockId macro_id_;
  common::ObSEArray<int64_t, 4> column_checksums_;
  bool has_string_out_row_;
  bool all_lob_in_row_;
  int64_t agg_row_len_; // size of agg_row_buf_
  const char *agg_row_buf_; // data buffer for pre aggregated row
  // used for ddl sstable migration & backup rebuild sstable
  // eg: if only one macro block with 100 rows, ddl_end_row_offset_ is 99.
  int64_t ddl_end_row_offset_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDataBlockMetaVal);
};

class ObDataMacroBlockMeta final
{
public:
  ObDataMacroBlockMeta();
  explicit ObDataMacroBlockMeta(ObIAllocator &allocator);
  ~ObDataMacroBlockMeta();
  int assign(const ObDataMacroBlockMeta &meta);
  int deep_copy(ObDataMacroBlockMeta *&dst, ObIAllocator &allocator) const;
  int build_row(ObDatumRow &row, ObIAllocator &allocator) const;
  int build_estimate_row(ObDatumRow &row, ObIAllocator &allocator) const;
  int parse_row(ObDatumRow &row);
  OB_INLINE const ObDataBlockMetaVal &get_meta_val() const { return val_; }
  OB_INLINE const MacroBlockId &get_macro_id() const
  {
    return val_.macro_id_;
  }
  OB_INLINE const ObLogicMacroBlockId &get_logic_id() const
  {
    return val_.logic_id_;
  }
  OB_INLINE int get_rowkey(ObDatumRowkey &rowkey) const
  {
    return rowkey.assign(end_key_.datums_, end_key_.datum_cnt_);
  }
  OB_INLINE bool is_last_row_last_flag() const
  {
    return val_.is_last_row_last_flag_;
  }
  OB_INLINE bool is_valid() const
  {
    return val_.is_valid() && end_key_.is_valid();
  }
  OB_INLINE void reset()
  {
    val_.reset();
    end_key_.reset();
    nested_offset_ = 0;
    nested_size_ = 0;
  }
  TO_STRING_KV(K_(val), K_(end_key));
public:
  ObDataBlockMetaVal val_;
  ObDatumRowkey end_key_; // rowkey is primary key
  int64_t nested_offset_;
  int64_t nested_size_;
  DISALLOW_COPY_AND_ASSIGN(ObDataMacroBlockMeta);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_META_H_ */
