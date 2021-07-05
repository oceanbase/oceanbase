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

#ifndef OB_MACRO_BLOCK_H_
#define OB_MACRO_BLOCK_H_

#include "lib/compress/ob_compressor.h"
#include "storage/ob_multi_version_col_desc_generate.h"
#include "ob_micro_block_index_writer.h"
#include "ob_block_sstable_struct.h"
#include "storage/ob_tenant_file_struct.h"
#include "ob_block_mark_deletion_maker.h"
#include "ob_row_reader.h"
#include "ob_macro_block_common_header.h"
#include "storage/ob_pg_mgr.h"

namespace oceanbase {
namespace storage {
struct ObSSTableMergeInfo;
}
namespace blocksstable {
struct ObDataStoreDesc {
  static const int64_t DEFAULT_RESERVE_PERCENT = 90;
  static const int64_t MIN_RESERVED_SIZE = 1024;          // 1KB;
  static const int64_t MIN_SSTABLE_SNAPSHOT_VERSION = 1;  // ref to ORIGIN_FOZEN_VERSION
  static const char* DEFAULT_MINOR_COMPRESS_NAME;
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t data_version_;
  int64_t macro_block_size_;
  int64_t macro_store_size_;  // macro_block_size_ * reserved_percent
  int64_t micro_block_size_;
  int64_t micro_block_size_limit_;
  int64_t row_column_count_;
  int64_t rowkey_column_count_;
  int64_t column_index_scale_;  // TODO:step or scale
  ObRowStoreType row_store_type_;
  int64_t schema_version_;
  int64_t schema_rowkey_col_cnt_;
  int64_t pct_free_;
  ObBlockMarkDeletionMaker* mark_deletion_maker_;
  storage::ObSSTableMergeInfo* merge_info_;
  bool has_lob_column_;
  bool is_major_;

  char compressor_name_[common::OB_MAX_HEADER_COMPRESSOR_NAME_LENGTH];
  uint64_t column_ids_[common::OB_MAX_COLUMN_NUMBER];
  common::ObObjMeta column_types_[common::OB_MAX_COLUMN_NUMBER];
  common::ObOrderType column_orders_[common::OB_MAX_COLUMN_NUMBER];
  bool need_calc_column_checksum_;
  bool store_micro_block_column_checksum_;
  int64_t snapshot_version_;
  bool need_calc_physical_checksum_;
  bool need_index_tree_;
  int64_t progressive_merge_round_;
  bool need_prebuild_bloomfilter_;
  int64_t bloomfilter_size_;
  int64_t bloomfilter_rowkey_prefix_;
  storage::ObSSTableRowkeyHelper* rowkey_helper_;
  common::ObPartitionKey pg_key_;
  ObStorageFileHandle file_handle_;
  bool need_check_order_;
  // indicate the min_cluster_version which trigger the major freeze
  // major_working_cluster_version_ == 0 means upgrade from old cluster
  // which still use freezeinfo without cluster version
  int64_t major_working_cluster_version_;
  ObDataStoreDesc()
  {
    reset();
  }
  int init(const share::schema::ObTableSchema& table_schema, const int64_t data_version,
      const storage::ObMultiVersionRowInfo* multi_version_row_info, const int64_t partition_id,
      const storage::ObMergeType merge_type, const bool need_calc_column_checksum,
      const bool store_micro_block_column_checksum, const common::ObPGKey& pg_key,
      const ObStorageFileHandle& file_handle, const int64_t snapshot_version = MIN_SSTABLE_SNAPSHOT_VERSION,
      const bool need_check_order = true, const bool need_index_tree = false);
  bool is_valid() const;
  void reset();
  int assign(const ObDataStoreDesc& desc);
  bool is_multi_version_minor_sstable() const
  {
    return rowkey_column_count_ != schema_rowkey_col_cnt_;
  }
  inline bool enable_sparse_format() const
  {
    return SPARSE_ROW_STORE == row_store_type_;
  }
  TO_STRING_KV(K_(table_id), K_(data_version), K_(micro_block_size), K_(row_column_count), K_(rowkey_column_count),
      K_(column_index_scale), K_(row_store_type), K_(compressor_name), K_(schema_version), K_(schema_rowkey_col_cnt),
      K_(partition_id), K_(pct_free), K_(has_lob_column), K_(is_major), K_(need_calc_column_checksum),
      K_(store_micro_block_column_checksum), K_(snapshot_version), K_(need_calc_physical_checksum), K_(need_index_tree),
      K_(need_prebuild_bloomfilter), K_(bloomfilter_rowkey_prefix), KP_(rowkey_helper), "column_types",
      common::ObArrayWrap<common::ObObjMeta>(column_types_, row_column_count_), K_(pg_key), K_(file_handle),
      K_(need_check_order), K_(need_index_tree), K_(major_working_cluster_version));

private:
  int cal_row_store_type(const share::schema::ObTableSchema& table_schema, const storage::ObMergeType merge_type);
  int get_major_working_cluster_version();

private:
  DISALLOW_COPY_AND_ASSIGN(ObDataStoreDesc);
};

class ObMicroBlockCompressor {
public:
  ObMicroBlockCompressor();
  virtual ~ObMicroBlockCompressor();
  void reset();
  int init(const int64_t micro_block_size, const char* comp_name);
  int compress(const char* in, const int64_t in_size, const char*& out, int64_t& out_size);
  int decompress(const char* in, const int64_t in_size, const int64_t uncomp_size, const char*& out, int64_t& out_size);

private:
  bool is_none_;
  int64_t micro_block_size_;
  common::ObCompressor* compressor_;
  ObSelfBufferWriter comp_buf_;
  ObSelfBufferWriter decomp_buf_;
};

struct ObMicroBlockDesc {
  common::ObString last_rowkey_;
  const char* buf_;  // after compressed
  int64_t buf_size_;
  int64_t data_size_;
  int64_t row_count_;
  int64_t column_count_;
  int32_t row_count_delta_;
  bool can_mark_deletion_;
  int64_t* column_checksums_;
  int64_t max_merged_trans_version_;
  bool contain_uncommitted_row_;

  ObMicroBlockDesc()
  {
    reset();
  }
  bool is_valid() const;
  void reset();

  // last_rowkey is byte stream, don't print it
  TO_STRING_KV(K_(last_rowkey), KP_(buf), K_(buf_size), K_(data_size), K_(row_count), K_(column_count),
      K_(row_count_delta), K_(can_mark_deletion), KP_(column_checksums), K_(max_merged_trans_version),
      K_(contain_uncommitted_row));
};

class ObMacroBlock {
public:
  ObMacroBlock();
  virtual ~ObMacroBlock();
  int init(ObDataStoreDesc& spec);
  int write_micro_block(const ObMicroBlockDesc& micro_block_desc, int64_t& data_offset);
  int flush(const int64_t cur_macro_seq, ObMacroBlockHandle& macro_handle, ObMacroBlocksWriteCtx& block_write_ctx);
  int merge(const ObMacroBlock& macro_block);
  bool can_merge(const ObMacroBlock& macro_block);
  void reset();
  OB_INLINE bool is_dirty() const
  {
    return is_dirty_;
  }
  OB_INLINE int64_t get_data_size() const
  {
    return data_.length() + index_.get_block_size();
  }
  OB_INLINE int32_t get_row_count() const
  {
    int32_t int_ret = 0;
    if (NULL != header_) {
      int_ret = header_->row_count_;
    }
    return int_ret;
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
  OB_INLINE int get_last_rowkey(common::ObString& rowkey)
  {
    return index_.get_last_rowkey(rowkey);
  }
  OB_INLINE int32_t get_index_size()
  {
    return header_->micro_block_index_size_;
  }
  OB_INLINE int32_t get_index_offset()
  {
    return header_->micro_block_index_offset_;
  }

private:
  int write_micro_record_header(const ObMicroBlockDesc& micro_block_desc);
  int reserve_header(const ObDataStoreDesc& spec);
  int build_header(const int64_t cur_macro_seq);
  int build_macro_meta(ObFullMacroBlockMeta& full_meta);
  int build_index();
  int merge_data_checksum(const ObMacroBlock& macro_block);
  int add_column_checksum(const int64_t* to_add_checksum, const int64_t column_cnt, int64_t* column_checksum);
  int init_row_reader(const ObRowStoreType row_store_type);

private:
  OB_INLINE int64_t get_remain_size() const
  {
    return data_.remain() - index_.get_block_size();
  }
  OB_INLINE const char* get_micro_block_data_ptr() const
  {
    return data_.data() + data_base_offset_;
  }
  OB_INLINE int64_t get_micro_block_data_size() const
  {
    return data_.length() - data_base_offset_;
  }
  OB_INLINE int64_t get_raw_data_size() const
  {
    return get_micro_block_data_size() + index_.get_block_size() - ObMicroBlockIndexWriter::INDEX_ENTRY_SIZE;
  }

private:
  ObDataStoreDesc* spec_;
  ObIRowReader* row_reader_;
  ObFlatRowReader flat_row_reader_;
  ObSparseRowReader sparse_row_reader_;
  ObSelfBufferWriter data_;  // micro header + data blocks;
  ObMicroBlockIndexWriter index_;
  ObSSTableMacroBlockHeader* header_;  // macro header store in head of data_;
  uint16_t* column_ids_;
  common::ObObjMeta* column_types_;
  common::ObOrderType* column_orders_;
  int64_t* column_checksum_;
  int64_t data_base_offset_;
  common::ObNewRow row_;
  common::ObObj rowkey_objs_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
  bool is_dirty_;
  bool is_multi_version_;
  bool macro_block_deletion_flag_;  // the flag is marked if all the micro blocks of the macro block are marked deleted
  int32_t delta_;                   // row count delta of this macro block, only used for multi-version minor sstable
  bool need_calc_column_checksum_;
  ObMacroBlockCommonHeader common_header_;
  int64_t max_merged_trans_version_;
  bool contain_uncommitted_row_;
  storage::ObIPartitionGroupGuard pg_guard_;
  blocksstable::ObStorageFile* pg_file_;
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif /* OB_MACRO_BLOCK_H_ */
