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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SSTABLE_META_H
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SSTABLE_META_H

#include "lib/container/ob_iarray.h"
#include "share/schema/ob_table_schema.h"
#include "storage/ob_storage_schema.h"
#include "storage/ob_i_table.h"
#include "storage/blocksstable/index_block/ob_sstable_meta_info.h"
#include "share/scn.h"
#include "storage/tablet/ob_table_store_util.h"

namespace oceanbase
{
namespace storage
{
class ObStoreRow;
struct ObTabletCreateSSTableParam;
}
namespace blocksstable
{
class ObTxContext final
{
public:
  struct ObTxDesc final
  {
    int64_t tx_id_;
    int64_t row_count_;
    int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
    int deserialize(const char *buf, const int64_t buf_len, int64_t &pos);
    int64_t get_serialize_size() const;
    TO_STRING_KV(K(tx_id_), K(row_count_));
  };

  ObTxContext() : len_(0), count_(0), tx_descs_(nullptr) {};

  int init(const common::ObIArray<ObTxDesc> &tx_descs, common::ObArenaAllocator &allocator);
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int64_t get_serialize_size() const;
  int deserialize(common::ObArenaAllocator &allocator, const char *buf, const int64_t buf_len, int64_t &pos);
  int deep_copy(
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    ObTxContext &dest) const;
  int64_t get_variable_size() const;
  OB_INLINE int64_t get_count() const { return count_; }
  int64_t get_tx_id(const int64_t idx) const;
  void reset()
  {
    len_ = 0;
    count_ = 0;
    tx_descs_ = nullptr;
  }
  TO_STRING_KV(K_(count), K(ObArrayWrap<ObTxDesc>(tx_descs_, count_)));
private:
  int push_back(const ObTxDesc &desc);

private:
  static const int64_t MAX_TX_IDS_COUNT = 16;
  int32_t len_; // for compat
  int64_t count_; // actual item count
  ObTxDesc *tx_descs_;
  DISALLOW_COPY_AND_ASSIGN(ObTxContext);
};

//For compatibility, the variables in this struct MUST NOT be deleted or moved.
//You should ONLY add variables at the end.
//Note that if you use complex structure as variables, the complex structure should also keep compatibility.
struct ObSSTableBasicMeta final
{
public:
  static const int32_t SSTABLE_BASIC_META_VERSION = 1;
  static const int64_t SSTABLE_FORMAT_VERSION_1 = 1;

  ObSSTableBasicMeta();
  ~ObSSTableBasicMeta() = default;
  bool operator==(const ObSSTableBasicMeta &other) const;
  bool operator!=(const ObSSTableBasicMeta &other) const;
  bool check_basic_meta_equality(const ObSSTableBasicMeta &other) const; // only for small sstable defragmentation
  bool is_valid() const;
  void reset();

  OB_INLINE int64_t get_total_macro_block_count() const
  {
    return data_macro_block_count_ + index_macro_block_count_;
  }
  OB_INLINE int64_t get_data_macro_block_count() const { return data_macro_block_count_; }
  OB_INLINE int64_t get_data_micro_block_count() const { return data_micro_block_count_; }
  OB_INLINE int64_t get_index_macro_block_count() const { return index_macro_block_count_; }
  OB_INLINE int64_t get_total_use_old_macro_block_count() const
  {
    return use_old_macro_block_count_;
  }
  OB_INLINE int64_t get_upper_trans_version() const { return ATOMIC_LOAD(&upper_trans_version_); }
  OB_INLINE int64_t get_max_merged_trans_version() const { return max_merged_trans_version_; }
  OB_INLINE share::SCN get_ddl_scn() const { return ddl_scn_; }
  OB_INLINE int64_t get_create_snapshot_version() const { return create_snapshot_version_; }
  OB_INLINE share::SCN get_filled_tx_scn() const { return filled_tx_scn_; }
  OB_INLINE int16_t get_data_index_tree_height() const { return data_index_tree_height_; }
  OB_INLINE int64_t get_recycle_version() const { return recycle_version_; }
  OB_INLINE int16_t get_sstable_seq() const { return sstable_logic_seq_; }
  OB_INLINE common::ObCompressorType get_compressor_type() const { return compressor_type_; }
  OB_INLINE common::ObRowStoreType get_latest_row_store_type() const { return latest_row_store_type_; }
  int decode_for_compat(const char *buf, const int64_t data_len, int64_t &pos);

  void set_upper_trans_version(const int64_t upper_trans_version);
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size() const;
private:
  OB_INLINE bool is_latest_row_store_type_valid() const
  {
    // Before version 4.0, latest_row_store_type was not serialized in sstable meta, but it is
    // required and added in version 4.1. For compatibility, when deserialize from older version
    // data, latest_row_store_type_ is filled with DUMMY_ROW_STORE
    return latest_row_store_type_ < ObRowStoreType::MAX_ROW_STORE
        || ObRowStoreType::DUMMY_ROW_STORE == latest_row_store_type_;
  }
public:
  TO_STRING_KV(K_(version), K_(length), K(row_count_), K(occupy_size_), K(original_size_),
      K(data_checksum_), K(index_type_), K(rowkey_column_count_), K(column_cnt_),
      K(data_macro_block_count_), K(data_micro_block_count_), K(use_old_macro_block_count_),
      K(index_macro_block_count_), K(sstable_format_version_), K(schema_version_),
      K(create_snapshot_version_), K(progressive_merge_round_),
      K(progressive_merge_step_), K(data_index_tree_height_), K(table_mode_),
      K(upper_trans_version_), K(max_merged_trans_version_), K_(recycle_version),
      K(ddl_scn_), K(filled_tx_scn_),
      K(contain_uncommitted_row_), K(status_), K_(root_row_store_type), K_(compressor_type),
      K_(encrypt_id), K_(master_key_id), K_(sstable_logic_seq), KPHEX_(encrypt_key, sizeof(encrypt_key_)),
      K_(latest_row_store_type));

public:
  int32_t version_;
  int32_t length_;
  int64_t row_count_;
  int64_t occupy_size_;
  int64_t original_size_;
  int64_t data_checksum_;
  int64_t index_type_;
  int64_t rowkey_column_count_; //rowkey for sstable, including multi-version columns if needed
  int64_t column_cnt_;
  int64_t data_macro_block_count_;
  int64_t data_micro_block_count_;
  int64_t use_old_macro_block_count_;
  int64_t index_macro_block_count_;
  int64_t sstable_format_version_;
  int64_t schema_version_;
  int64_t create_snapshot_version_;
  int64_t progressive_merge_round_;
  int64_t progressive_merge_step_;
  int64_t upper_trans_version_;
  // major/meta major: snapshot version; others: max commit version
  int64_t max_merged_trans_version_;
  // recycle_version only available for minor sstable, recored recycled multi version start
  int64_t recycle_version_;
  share::SCN ddl_scn_; // only used in DDL SSTable, all MB in DDL SSTable should have the same scn(start_scn)
  share::SCN filled_tx_scn_; // only for rebuild
  int16_t data_index_tree_height_;
  share::schema::ObTableMode table_mode_;
  uint8_t status_;
  bool contain_uncommitted_row_;
  common::ObRowStoreType root_row_store_type_;
  common::ObCompressorType compressor_type_;
  int64_t encrypt_id_;
  int64_t master_key_id_;
  int16_t sstable_logic_seq_;
  common::ObRowStoreType latest_row_store_type_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  //Add new variable need consider ObSSTableMetaChecker
};

class ObSSTableMeta final
{
public:
  ObSSTableMeta();
  ~ObSSTableMeta();
  int init(const storage::ObTabletCreateSSTableParam &param, common::ObArenaAllocator &allocator);
  int fill_cg_sstables(common::ObArenaAllocator &allocator, const common::ObIArray<ObITable *> &cg_tables);
  void reset();
  OB_INLINE bool is_valid() const { return is_inited_; }
  OB_INLINE bool contain_uncommitted_row() const { return basic_meta_.contain_uncommitted_row_; }
  OB_INLINE ObSSTableArray &get_cg_sstables() { return cg_sstables_; }
  OB_INLINE const ObSSTableArray &get_cg_sstables() const { return cg_sstables_; }
  OB_INLINE bool is_empty() const {
    return 0 == basic_meta_.data_macro_block_count_;
  }
  OB_INLINE ObSSTableBasicMeta &get_basic_meta() { return basic_meta_; }
  OB_INLINE const ObSSTableBasicMeta &get_basic_meta() const { return basic_meta_; }
  OB_INLINE int64_t get_col_checksum_cnt() const { return column_checksum_count_; }
  OB_INLINE int64_t *get_col_checksum() const { return column_checksums_; }
  OB_INLINE int64_t get_tx_id_count() const { return tx_ctx_.get_count(); }
  OB_INLINE int64_t get_tx_ids(const int64_t idx) const { return tx_ctx_.get_tx_id(idx); }
  OB_INLINE int64_t get_data_checksum() const { return basic_meta_.data_checksum_; }
  OB_INLINE int64_t get_rowkey_column_count() const { return basic_meta_.rowkey_column_count_; }
  OB_INLINE int64_t get_column_count() const { return basic_meta_.column_cnt_; }
  OB_INLINE int64_t get_schema_rowkey_column_count() const
  {
    return basic_meta_.rowkey_column_count_ - ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  }
  OB_INLINE int64_t get_schema_column_count() const
  {
    return basic_meta_.column_cnt_ - ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  }

  OB_INLINE int16_t get_index_tree_height(const bool is_ddl_merge_sstable) const { return is_ddl_merge_sstable ? 2 : basic_meta_.data_index_tree_height_; }
  OB_INLINE ObSSTableStatus get_status() const
  {
    return static_cast<ObSSTableStatus>(basic_meta_.status_);
  }
  OB_INLINE int64_t get_occupy_size() const { return basic_meta_.occupy_size_; }
  OB_INLINE int64_t get_row_count() const { return basic_meta_.row_count_; }
  OB_INLINE int64_t get_end_row_id(const bool is_ddl_merge_empty_sstable) const { return is_ddl_merge_empty_sstable ? INT64_MAX : basic_meta_.row_count_ - 1; }
  OB_INLINE int64_t get_data_micro_block_count() const
  {
    return basic_meta_.get_data_micro_block_count();
  }
  // FIXME: do we really need all these get_xxx_macro_block_count() interfaces?
  OB_INLINE int64_t get_data_macro_block_count() const
  {
    return basic_meta_.get_data_macro_block_count();
  }
  OB_INLINE int64_t get_index_macro_block_count() const
  {
    return basic_meta_.get_index_macro_block_count();
  }
  OB_INLINE int64_t get_linked_macro_block_count() const
  {
    return macro_info_.get_linked_block_count();
  }
  OB_INLINE int64_t get_total_use_old_macro_block_count() const
  {
    return basic_meta_.get_total_use_old_macro_block_count();
  }
  OB_INLINE int64_t get_total_macro_block_count() const
  {
    return basic_meta_.get_total_macro_block_count();
  }
  OB_INLINE int64_t get_upper_trans_version() const
  {
    return basic_meta_.get_upper_trans_version();
  }
  OB_INLINE int64_t get_max_merged_trans_version() const
  {
    return basic_meta_.get_max_merged_trans_version();
  }
  OB_INLINE int64_t get_create_snapshot_version() const
  {
    return basic_meta_.get_create_snapshot_version();
  }
  OB_INLINE share::SCN get_ddl_scn() const { return basic_meta_.get_ddl_scn(); }
  OB_INLINE share::SCN get_filled_tx_scn() const { return basic_meta_.get_filled_tx_scn(); }
  OB_INLINE int16_t get_data_index_tree_height() const { return basic_meta_.get_data_index_tree_height(); }
  OB_INLINE int64_t get_recycle_version() const { return basic_meta_.get_recycle_version(); }
  OB_INLINE int16_t get_sstable_seq() const { return basic_meta_.get_sstable_seq(); }
  OB_INLINE int64_t get_schema_version() const { return basic_meta_.schema_version_; }
  OB_INLINE int64_t get_progressive_merge_round() const { return basic_meta_.progressive_merge_round_; }
  OB_INLINE int64_t get_progressive_merge_step() const { return basic_meta_.progressive_merge_step_; }
  OB_INLINE const ObRootBlockInfo &get_root_info() const { return data_root_info_; }
  OB_INLINE const ObSSTableMacroInfo &get_macro_info() const { return macro_info_; }
  int load_root_block_data(common::ObArenaAllocator &allocator); //TODO:@jinzhu remove me after using kv cache.
  inline int transform_root_block_extra_buf(common::ObArenaAllocator &allocator)
  {
    return data_root_info_.transform_root_block_extra_buf(allocator);
  }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;
  int64_t get_variable_size() const;
  inline int64_t get_deep_copy_size() const
  {
    return sizeof(ObSSTableMeta) + get_variable_size();
  }
  int deep_copy(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      ObSSTableMeta *&dest) const;
  TO_STRING_KV(K_(basic_meta), KP_(column_checksums), K_(column_checksum_count), K_(data_root_info), K_(macro_info), K_(cg_sstables), K_(tx_ctx), K_(is_inited));
private:
  bool check_meta() const;
  int init_base_meta(const ObTabletCreateSSTableParam &param, common::ObArenaAllocator &allocator);
  int init_data_index_tree_info(
      const storage::ObTabletCreateSSTableParam &param,
      common::ObArenaAllocator &allocator);
  int prepare_column_checksum(
      const common::ObIArray<int64_t> &column_checksums,
      common::ObArenaAllocator &allocator);
  int prepare_tx_context(
    const ObTxContext::ObTxDesc &tx_desc,
    common::ObArenaAllocator &allocator);
  int serialize_(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size_() const;
private:
  friend class ObSSTable;
  static const int64_t SSTABLE_META_VERSION = 1;
private:
  ObSSTableBasicMeta basic_meta_;
  ObRootBlockInfo data_root_info_;
  ObSSTableMacroInfo macro_info_;
  ObSSTableArray cg_sstables_;
  int64_t *column_checksums_;
  int64_t column_checksum_count_;
  ObTxContext tx_ctx_;
  // The following fields don't to persist
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMeta);
};

class ObMigrationSSTableParam final
{
public:
  ObMigrationSSTableParam();
  ~ObMigrationSSTableParam();
  bool is_valid() const;
  void reset();
  int assign(const ObMigrationSSTableParam &param);
  TO_STRING_KV(K_(basic_meta), K(column_checksums_.count()), K(column_default_checksums_.count()), K_(column_checksums),
               K_(column_default_checksums), K_(table_key), K_(column_group_cnt), K_(co_base_type));
private:
  static const int64_t MIGRATION_SSTABLE_PARAM_VERSION = 1;
  typedef common::ObSEArray<int64_t, common::OB_ROW_DEFAULT_COLUMNS_COUNT> ColChecksumArray;
public:
  common::ObArenaAllocator allocator_;
  ObSSTableBasicMeta basic_meta_;
  ColChecksumArray column_checksums_;
  storage::ObITable::TableKey table_key_;
  ColChecksumArray column_default_checksums_;
  bool is_small_sstable_;
  // The following two members are used only for co sstable
  bool is_empty_cg_sstables_;
  int32_t column_group_cnt_;
  int32_t full_column_cnt_;
  int32_t co_base_type_;
  OB_UNIS_VERSION(MIGRATION_SSTABLE_PARAM_VERSION);
private:
  DISALLOW_COPY_AND_ASSIGN(ObMigrationSSTableParam);
};

class ObSSTableMetaChecker
{
public:
  // only for small sstable defragmentation
  static int check_sstable_meta_strict_equality(
      const ObSSTableMeta &old_sstable_meta,
      const ObSSTableMeta &new_sstable_meta);
  static int check_sstable_meta(
      const ObSSTableMeta &old_sstable_meta,
      const ObSSTableMeta &new_sstable_meta);
  static int check_sstable_meta(
      const ObMigrationSSTableParam &migration_param,
      const ObSSTableMeta &new_sstable_meta);
private:
  static int check_sstable_basic_meta_(
      const ObSSTableBasicMeta &old_sstable_basic_meta,
      const ObSSTableBasicMeta &new_sstable_basic_meta);
  static int check_sstable_column_checksum_(
      const int64_t *old_column_checksum,
      const int64_t old_column_count,
      const int64_t *new_column_checksum,
      const int64_t new_column_count);
  static int check_sstable_column_checksum_(
      const common::ObIArray<int64_t> &old_column_checksum,
      const int64_t *new_column_checksum,
      const int64_t new_column_count);
};


} // namespace blocksstable
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SSTABLE_META_H */
