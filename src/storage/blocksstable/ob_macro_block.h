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

#include "lib/compress/ob_compress_util.h"
#include "ob_block_sstable_struct.h"
#include "ob_data_buffer.h"
#include "ob_imicro_block_writer.h"
#include "ob_macro_block_common_header.h"
#include "ob_sstable_meta.h"
#include "share/ob_encryption_util.h"
#include "storage/blocksstable/ob_macro_block_meta.h"
#include "storage/compaction/ob_compaction_util.h"
#include "share/scn.h"

namespace oceanbase {
namespace storage {
struct ObSSTableMergeInfo;
}
namespace blocksstable {
class ObSSTableIndexBuilder;
struct ObMicroBlockDesc;
struct ObMacroBlocksWriteCtx;
class ObBlockMarkDeletionMaker;
class ObMacroBlockHandle;

struct ObDataStoreDesc
{
  static const int64_t DEFAULT_RESERVE_PERCENT = 90;
  static const int64_t MIN_MICRO_BLOCK_SIZE = 4 * 1024; //4KB
  static const int64_t MIN_RESERVED_SIZE = 1024; //1KB;
  static const int64_t MIN_SSTABLE_SNAPSHOT_VERSION = 1; // ref to ORIGIN_FOZEN_VERSION
  static const int64_t MIN_SSTABLE_END_LOG_TS = 1; // ref to ORIGIN_FOZEN_VERSION
  static const ObCompressorType DEFAULT_MINOR_COMPRESSOR_TYPE = ObCompressorType::LZ4_COMPRESSOR;
  // emergency magic table id is 10000
  static const uint64_t EMERGENCY_TENANT_ID_MAGIC = 0;
  static const uint64_t EMERGENCY_LS_ID_MAGIC = 0;
  static const ObTabletID EMERGENCY_TABLET_ID_MAGIC;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  int64_t macro_block_size_;
  int64_t macro_store_size_; //macro_block_size_ * reserved_percent
  int64_t micro_block_size_;
  int64_t micro_block_size_limit_;
  int64_t row_column_count_;
  int64_t rowkey_column_count_; // mv rowkey cnt
  ObRowStoreType row_store_type_;
  bool need_build_hash_index_for_micro_block_;
  int64_t schema_version_;
  int64_t schema_rowkey_col_cnt_;
  int64_t full_stored_col_cnt_; // table stored column count including hidden columns
  ObMicroBlockEncoderOpt encoder_opt_;
  ObSSTableMergeInfo *merge_info_;
  storage::ObMergeType merge_type_;

  ObSSTableIndexBuilder *sstable_index_builder_;
  ObCompressorType compressor_type_;
  int64_t snapshot_version_;
  share::SCN end_scn_;
  int64_t progressive_merge_round_;
  int64_t encrypt_id_;
  bool need_prebuild_bloomfilter_;
  int64_t bloomfilter_rowkey_prefix_; // to be remove
  int64_t master_key_id_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  // indicate the min_cluster_version which trigger the major freeze
  // major_working_cluster_version_ == 0 means upgrade from old cluster
  // which still use freezeinfo without cluster version
  int64_t major_working_cluster_version_;
  bool is_ddl_;
  bool need_pre_warm_;
  bool is_force_flat_store_type_;
  bool default_col_checksum_array_valid_;
  common::ObArenaAllocator allocator_;
  common::ObFixedArray<int64_t, common::ObIAllocator> col_default_checksum_array_;
  blocksstable::ObStorageDatumUtils datum_utils_;
  ObDataStoreDesc();
  ~ObDataStoreDesc();
  //ATTENSION!!! Only decreasing count of parameters is acceptable
  int init(
      const share::schema::ObMergeSchema &schema,
      const share::ObLSID &ls_id,
      const common::ObTabletID tablet_id,
      const storage::ObMergeType merge_type,
      const int64_t snapshot_version = MIN_SSTABLE_SNAPSHOT_VERSION,
      const int64_t cluster_version = 0);
  int init_as_index(
      const share::schema::ObMergeSchema &schema,
      const share::ObLSID &ls_id,
      const common::ObTabletID tablet_id,
      const storage::ObMergeType merge_type,
      const int64_t snapshot_version = MIN_SSTABLE_SNAPSHOT_VERSION,
      const int64_t cluster_version = 0);
  bool is_valid() const;
  void reset();
  int assign(const ObDataStoreDesc &desc);
  bool encoding_enabled() const { return ObStoreFormat::is_row_store_type_with_encoding(row_store_type_); }
  void force_flat_store_type()
  {
    row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
    is_force_flat_store_type_ = true;
  }
  bool is_store_type_valid() const;
  OB_INLINE bool is_major_merge() const { return storage::is_major_merge_type(merge_type_); }
  OB_INLINE bool is_meta_major_merge() const { return storage::is_meta_major_merge(merge_type_); }
  OB_INLINE bool is_use_pct_free() const { return macro_block_size_ != macro_store_size_; }
  int64_t get_logical_version() const
  {
    return (is_major_merge() || is_meta_major_merge()) ? snapshot_version_ : end_scn_.get_val_for_tx();
  }
  const common::ObIArray<share::schema::ObColDesc> &get_rowkey_col_descs() const
  {
    return col_desc_array_;
  }
  const common::ObIArray<share::schema::ObColDesc> &get_full_stored_col_descs() const
  {
    OB_ASSERT_MSG(is_major_merge(), "ObDataStoreDesc dose not promise a full stored col descs");
    return col_desc_array_;
  }
  bool use_old_version_macro_header() const
  {
    return is_major_merge() && major_working_cluster_version_ < DATA_VERSION_4_2_0_0;
  }
  int64_t get_fixed_header_version() const
  {
    return use_old_version_macro_header() ? ObSSTableMacroBlockHeader::SSTABLE_MACRO_BLOCK_HEADER_VERSION_V1 : ObSSTableMacroBlockHeader::SSTABLE_MACRO_BLOCK_HEADER_VERSION_V2;
  }
  int64_t get_fixed_header_col_type_cnt() const
  {
    return use_old_version_macro_header() ? row_column_count_ : rowkey_column_count_;
  }
  TO_STRING_KV(
      K_(ls_id),
      K_(tablet_id),
      K_(micro_block_size),
      K_(micro_block_size_limit),
      K_(row_column_count),
      K_(rowkey_column_count),
      K_(schema_rowkey_col_cnt),
      K_(full_stored_col_cnt),
      K_(row_store_type),
      K_(encoder_opt),
      K_(compressor_type),
      K_(schema_version),
      KP_(merge_info),
      K_(merge_type),
      K_(snapshot_version),
      K_(end_scn),
      K_(need_prebuild_bloomfilter),
      K_(bloomfilter_rowkey_prefix),
      K_(encrypt_id),
      K_(master_key_id),
      KPHEX_(encrypt_key, sizeof(encrypt_key_)),
      K_(major_working_cluster_version),
      KP_(sstable_index_builder),
      K_(is_ddl),
      K_(col_desc_array),
      K_(default_col_checksum_array_valid),
      K_(col_default_checksum_array));

private:
  int inner_init(
      const share::schema::ObMergeSchema &schema,
      const share::ObLSID &ls_id,
      const common::ObTabletID tablet_id,
      const storage::ObMergeType merge_type,
      const int64_t snapshot_version,
      const int64_t cluster_version);
  int cal_row_store_type(
      const share::schema::ObMergeSchema &schema,
      const storage::ObMergeType merge_type);
  int get_emergency_row_store_type();
  void fresh_col_meta();
  int init_col_default_checksum_array(
    const share::schema::ObMergeSchema &merge_schema,
    const bool init_by_schema);
private:
  common::ObFixedArray<share::schema::ObColDesc, common::ObIAllocator> col_desc_array_;
  DISALLOW_COPY_AND_ASSIGN(ObDataStoreDesc);
};

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
  ObSelfBufferWriter comp_buf_;
  ObSelfBufferWriter decomp_buf_;
};


class ObMacroBlock
{
public:
  ObMacroBlock();
  virtual ~ObMacroBlock();
  int init(ObDataStoreDesc &spec, const int64_t &cur_macro_seq);
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
  ObDataStoreDesc *spec_;
  ObSelfBufferWriter data_; //micro header + data blocks;
  ObSSTableMacroBlockHeader macro_header_; //macro header store in head of data_;
  int64_t data_base_offset_;
  ObDatumRowkey last_rowkey_;
  ObArenaAllocator rowkey_allocator_;
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
