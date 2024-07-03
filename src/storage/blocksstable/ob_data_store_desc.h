//Copyright (c) 2023 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_BLOCK_SSTABLE_DATA_STORE_DESC_H_
#define OB_STORAGE_BLOCK_SSTABLE_DATA_STORE_DESC_H_
#include "storage/compaction/ob_compaction_util.h"
#include "storage/blocksstable/index_block/ob_index_block_util.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/ob_sstable_macro_block_header.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "common/ob_tablet_id.h"
#include "common/ob_store_format.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
namespace oceanbase
{
namespace storage {
struct ObSSTableMergeInfo;
struct ObStorageColumnGroupSchema;
struct ObSSTableMergeInfo;
}
namespace share
{
namespace schema
{
class ObMergeSchema;
}
}

/*
  ObStaticDataStoreDesc : record static info
  ObColDataStoreDesc : record column related info
  ObDataStoreDesc : ObStaticDataStoreDesc & ObColDataStoreDesc ptr
  ObWholeDataStoreDesc : ObStaticDataStoreDesc & ObColDataStoreDesc object

  for compaction, ObDataStoreDesc will record a common ObStaticDataStoreDesc ptr and a special ObColDataStoreDesc object for each table/cg
  for other situation, use ObWholeDataStoreDesc instead of ObDataStoreDesc
*/
namespace blocksstable {
class ObSSTableIndexBuilder;
struct ObSSTableBasicMeta;
// same for all cg/all parallel merge task
struct ObStaticDataStoreDesc
{
public:
  ObStaticDataStoreDesc(const bool is_ddl = false);
  ~ObStaticDataStoreDesc() { reset(); }
  int init(
    const share::schema::ObMergeSchema &merge_schema,
    const share::ObLSID &ls_id,
    const common::ObTabletID tablet_id,
    const compaction::ObMergeType merge_type,
    const int64_t snapshot_version,
    const share::SCN &end_scn,
    const int64_t cluster_version);
  bool is_valid() const;
  void reset();
  int assign(const ObStaticDataStoreDesc &desc);
  TO_STRING_KV(
      K_(ls_id),
      K_(tablet_id),
      "merge_type", merge_type_to_str(merge_type_),
      K_(snapshot_version),
      K_(end_scn),
      K_(is_ddl),
      K_(compressor_type),
      K_(macro_block_size),
      K_(macro_store_size),
      K_(micro_block_size_limit),
      K_(schema_version),
      K_(encrypt_id),
      K_(master_key_id),
      KPHEX_(encrypt_key, sizeof(encrypt_key_)),
      K_(major_working_cluster_version));
private:
  OB_INLINE int init_encryption_info(const share::schema::ObMergeSchema &merge_schema);
  OB_INLINE void init_block_size(const share::schema::ObMergeSchema &merge_schema);
  static const int64_t DEFAULT_RESERVE_PERCENT = 90;
  static const int64_t MIN_RESERVED_SIZE = 1024; //1KB;
  static const ObCompressorType DEFAULT_MINOR_COMPRESSOR_TYPE = ObCompressorType::LZ4_COMPRESSOR;
public:
  bool is_ddl_;
  compaction::ObMergeType merge_type_;
  ObCompressorType compressor_type_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  int64_t macro_block_size_;
  int64_t macro_store_size_; //macro_block_size_ * reserved_percent
  int64_t micro_block_size_limit_;
  int64_t schema_version_;
  int64_t snapshot_version_;
  share::SCN end_scn_;
  int64_t progressive_merge_round_;
  // indicate the min_cluster_version which trigger the major freeze
  // major_working_cluster_version_ == 0 means upgrade from old cluster
  // which still use freezeinfo without cluster version
  int64_t major_working_cluster_version_;
  int64_t encrypt_id_;
  int64_t master_key_id_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
};

// ObColDataStoreDesc is same for every parallel task
// but different for each column group
struct ObColDataStoreDesc
{
  ObColDataStoreDesc();
  ~ObColDataStoreDesc() { reset(); }
  void reset();
  bool is_valid() const;
  int assign(const ObColDataStoreDesc &desc);
  int init(
    const bool is_major,
    const share::schema::ObMergeSchema &merge_schema,
    const uint16_t table_cg_idx,
    const int64_t major_working_cluster_version);
  int init(const bool is_major,
          const share::schema::ObMergeSchema &merge_schema,
          const storage::ObStorageColumnGroupSchema &cg_schema,
          const uint16_t table_cg_idx,
          const int64_t major_working_cluster_version);
  // be carefule to cal mock function
  int mock_valid_col_default_checksum_array(int64_t column_cnt);
  OB_INLINE int add_col_desc(const ObObjMeta meta, int64_t col_idx);
  OB_INLINE int add_binary_col_desc(int64_t col_idx);
  TO_STRING_KV(K_(is_row_store), K_(table_cg_idx), K_(row_column_count),
               K_(rowkey_column_count), K_(schema_rowkey_col_cnt),
               K_(full_stored_col_cnt), K_(col_desc_array),
               K_(default_col_checksum_array_valid),
               K_(col_default_checksum_array), K_(agg_meta_array));

private:
  // simplified do not generate skip index, do not init agg_meta_array
  int generate_skip_index_meta(
      const share::schema::ObMergeSchema &schema,
      const storage::ObStorageColumnGroupSchema *cg_schema,
      const int64_t major_working_cluster_version);
  void fresh_col_meta(const share::schema::ObMergeSchema &merge_schema);
  int gene_col_default_checksum_array(
      const share::schema::ObMergeSchema &merge_schema);
  int init_col_default_checksum_array(
      const share::schema::ObMergeSchema &merge_schema);
  int init_col_default_checksum_array(
      const int64_t column_cnt);
  int generate_single_cg_skip_index_meta(
    const ObSkipIndexColumnAttr &skip_idx_attr_by_user,
    const storage::ObStorageColumnGroupSchema &cg_schema,
    const int64_t major_working_cluster_version);
  int add_col_desc_from_cg_schema(
    const share::schema::ObMergeSchema &merge_schema,
    const storage::ObStorageColumnGroupSchema &cg_schema);
  static int get_compat_mode_from_schema(
    const share::schema::ObMergeSchema &merge_schema,
    bool &is_oracle_mode);
public:
  bool is_row_store_;
  bool default_col_checksum_array_valid_;
  uint16_t table_cg_idx_;
  int64_t row_column_count_;
  int64_t rowkey_column_count_; // mv rowkey cnt
  int64_t schema_rowkey_col_cnt_;
  int64_t full_stored_col_cnt_; // table stored column count including hidden columns
  compaction::ObLocalArena allocator_;
  common::ObFixedArray<int64_t, common::ObIAllocator> col_default_checksum_array_;
  common::ObFixedArray<ObSkipIndexColMeta, common::ObIAllocator> agg_meta_array_;
  blocksstable::ObStorageDatumUtils datum_utils_; // TODO(chaser.ch) rm this
  common::ObFixedArray<share::schema::ObColDesc, common::ObIAllocator> col_desc_array_;
};

struct ObDataStoreDesc
{
public:
  ObDataStoreDesc();
  ~ObDataStoreDesc();
  // CAREFUL! input ObStaticDataStoreDesc/ObColDataStoreDesc must be destroyed after inited ObDataStoreDesc
  int init(ObStaticDataStoreDesc &static_desc,
           ObColDataStoreDesc &col_desc,
           const share::schema::ObMergeSchema &schema,
           const ObRowStoreType row_store_type);
  bool is_valid() const;
  void reset();
  // CAREFUL! static_desc_/col_desc_ are pointer
  int shallow_copy(const ObDataStoreDesc &desc);
  bool encoding_enabled() const { return ObStoreFormat::is_row_store_type_with_encoding(row_store_type_); }
  void force_flat_store_type()
  {
    row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
    is_force_flat_store_type_ = true;
  }
  bool is_store_type_valid() const;
  OB_INLINE bool is_major_merge_type() const { return compaction::is_major_merge_type(get_merge_type()); }
  OB_INLINE bool is_major_or_meta_merge_type() const { return compaction::is_major_or_meta_merge_type(get_merge_type()); }
  OB_INLINE bool is_use_pct_free() const { return get_macro_block_size() != get_macro_store_size(); }
  int64_t get_logical_version() const
  {
    return is_major_or_meta_merge_type() ? get_snapshot_version() : get_end_scn().get_val_for_tx();
  }
  bool is_cg() const { return !get_is_row_store(); }
  const common::ObIArray<share::schema::ObColDesc> &get_rowkey_col_descs() const
  {
    return col_desc_->col_desc_array_;
  }
  const common::ObIArray<share::schema::ObColDesc> &get_full_stored_col_descs() const
  {
    OB_ASSERT_MSG(contain_full_col_descs(), "ObDataStoreDesc dose not promise a full stored col descs");
    return col_desc_->col_desc_array_;
  }
  bool contain_full_col_descs() const
  {
    return get_row_column_count() == get_col_desc_array().count();
  }
  bool use_old_version_macro_header() const
  {
    return is_major_merge_type() && get_major_working_cluster_version() < DATA_VERSION_4_2_0_0;
  }
  int64_t get_fixed_header_version() const
  {
    return use_old_version_macro_header() ? ObSSTableMacroBlockHeader::SSTABLE_MACRO_BLOCK_HEADER_VERSION_V1 : ObSSTableMacroBlockHeader::SSTABLE_MACRO_BLOCK_HEADER_VERSION_V2;
  }
  int64_t get_fixed_header_col_type_cnt() const
  {
    return use_old_version_macro_header() ? col_desc_->row_column_count_ : col_desc_->rowkey_column_count_;
  }
  int update_basic_info_from_macro_meta(const ObSSTableBasicMeta &meta);
  /* GET FUNC */
  #define STORE_DESC_DEFINE_POINT_FUNC(var_type, desc, var_name) \
    OB_INLINE var_type get_##var_name() const { return desc-> var_name##_; }
  #define STATIC_DESC_FUNC(var_type, var_name) \
    STORE_DESC_DEFINE_POINT_FUNC(var_type, static_desc_, var_name)
  #define COL_DESC_FUNC(var_type, var_name) \
    STORE_DESC_DEFINE_POINT_FUNC(var_type, col_desc_, var_name)
  STATIC_DESC_FUNC(int64_t, macro_block_size);
  STATIC_DESC_FUNC(int64_t, macro_store_size);
  STATIC_DESC_FUNC(int64_t, micro_block_size_limit);
  STATIC_DESC_FUNC(compaction::ObMergeType, merge_type);
  STATIC_DESC_FUNC(const share::ObLSID&, ls_id);
  STATIC_DESC_FUNC(const ObTabletID&, tablet_id);
  STATIC_DESC_FUNC(int64_t, progressive_merge_round);
  STATIC_DESC_FUNC(int64_t, schema_version);
  STATIC_DESC_FUNC(int64_t, snapshot_version);
  STATIC_DESC_FUNC(int64_t, encrypt_id);
  STATIC_DESC_FUNC(int64_t, master_key_id);
  STATIC_DESC_FUNC(share::SCN, end_scn);
  STATIC_DESC_FUNC(bool, is_ddl);
  STATIC_DESC_FUNC(ObCompressorType, compressor_type);
  STATIC_DESC_FUNC(int64_t, major_working_cluster_version);
  STATIC_DESC_FUNC(const char *, encrypt_key);
  COL_DESC_FUNC(bool, is_row_store);
  COL_DESC_FUNC(uint16_t, table_cg_idx);
  COL_DESC_FUNC(int64_t, row_column_count);
  COL_DESC_FUNC(int64_t, rowkey_column_count);
  COL_DESC_FUNC(int64_t, schema_rowkey_col_cnt);
  COL_DESC_FUNC(int64_t, full_stored_col_cnt);
  COL_DESC_FUNC(bool, default_col_checksum_array_valid);
  COL_DESC_FUNC(const ObIArray<int64_t> &, col_default_checksum_array);
  COL_DESC_FUNC(const ObIArray<ObSkipIndexColMeta> &, agg_meta_array);
  COL_DESC_FUNC(const ObIArray<share::schema::ObColDesc> &, col_desc_array);
  COL_DESC_FUNC(const blocksstable::ObStorageDatumUtils &, datum_utils);
  #undef COL_DESC_FUNC
  #undef STATIC_DESC_FUNC
  #undef STORE_DESC_DEFINE_POINT_FUNC
  OB_INLINE int64_t get_encrypt_key_size() const { return sizeof(static_desc_->encrypt_key_); }
  OB_INLINE int64_t get_micro_block_size() const { return micro_block_size_; }
  OB_INLINE common::ObRowStoreType get_row_store_type() const { return row_store_type_; }
  static const int64_t MIN_MICRO_BLOCK_SIZE = 4 * 1024; //4KB
  // emergency magic table id is 10000
  static const uint64_t EMERGENCY_TENANT_ID_MAGIC = 0;
  static const uint64_t EMERGENCY_LS_ID_MAGIC = 0;
  static const ObTabletID EMERGENCY_TABLET_ID_MAGIC;

  TO_STRING_KV(
      KPC_(static_desc),
      "row_store_type", ObStoreFormat::get_row_store_name(row_store_type_),
      KPC_(col_desc),
      K_(encoder_opt),
      KP_(merge_info),
      KP_(sstable_index_builder),
      K_(micro_block_size));

private:
  int inner_init(
      const share::schema::ObMergeSchema &schema,
      const ObRowStoreType row_store_type);
  int cal_row_store_type(
      const ObRowStoreType row_store_type,
      const compaction::ObMergeType merge_type);
  int get_emergency_row_store_type();
public:
  ObStaticDataStoreDesc *static_desc_;
  ObColDataStoreDesc *col_desc_;
  int64_t micro_block_size_;
  ObRowStoreType row_store_type_;
  ObMicroBlockEncoderOpt encoder_opt_; // binding to row_store_type_
  storage::ObSSTableMergeInfo *merge_info_;
  ObSSTableIndexBuilder *sstable_index_builder_;
  bool is_force_flat_store_type_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDataStoreDesc);
};

struct ObWholeDataStoreDesc
{
  ObWholeDataStoreDesc(bool is_ddl = false)
    : static_desc_(is_ddl),
      col_desc_(),
      desc_()
  {}
  ~ObWholeDataStoreDesc() { reset(); }
  void reset()
  {
    desc_.reset();
    static_desc_.reset();
    col_desc_.reset();
  }
  int init(
    const ObStaticDataStoreDesc &static_desc,
    const share::schema::ObMergeSchema &merge_schema,
    const storage::ObStorageColumnGroupSchema *cg_schema = nullptr,
    const uint16_t table_cg_idx = 0);
  int init(
    const share::schema::ObMergeSchema &merge_schema,
    const share::ObLSID &ls_id,
    const common::ObTabletID tablet_id,
    const compaction::ObMergeType merge_type,
    const int64_t snapshot_version,
    const int64_t cluster_version,
    const share::SCN &end_scn = share::SCN::invalid_scn(),
    const storage::ObStorageColumnGroupSchema *cg_schema = nullptr,
    const uint16_t table_cg_idx = 0);
  int gen_index_store_desc(const ObDataStoreDesc &data_desc);
  int assign(const ObDataStoreDesc &desc);
  ObStaticDataStoreDesc &get_static_desc() { return static_desc_; }
  ObColDataStoreDesc &get_col_desc() {return col_desc_; }
  ObDataStoreDesc &get_desc() { return desc_; }
  const ObDataStoreDesc &get_desc() const { return desc_; }
  bool is_valid() const
  {
    return desc_.is_valid()
      && (&static_desc_ == desc_.static_desc_)
      && (&col_desc_ == desc_.col_desc_);
  }
  TO_STRING_KV(K_(desc));
private:
  int inner_init(
    const share::schema::ObMergeSchema &merge_schema,
    const storage::ObStorageColumnGroupSchema *cg_schema,
    const uint16_t table_cg_idx);
  ObStaticDataStoreDesc static_desc_;
  ObColDataStoreDesc col_desc_;
  ObDataStoreDesc desc_;
};


} // namespace blocksstable
} // namespace oceanbase

#endif // OB_STORAGE_BLOCK_SSTABLE_DATA_STORE_DESC_H_
