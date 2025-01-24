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

#define USING_LOG_PREFIX STORAGE


#include "ob_tablet_create_sstable_param.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"
#include "storage/compaction/ob_basic_tablet_merge_ctx.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "share/compaction/ob_shared_storage_compaction_util.h"
#endif

namespace oceanbase
{
using namespace share;
namespace storage
{
// if you add membership for this param, plz check all paths that use it, including
// but not limited to unittest, merge, ddl, shared_macro_block
ObTabletCreateSSTableParam::ObTabletCreateSSTableParam()
  : table_key_(),
    sstable_logic_seq_(-1),
    schema_version_(-1),
    create_snapshot_version_(-1),
    progressive_merge_round_(-1),
    progressive_merge_step_(-1),
    is_ready_for_read_(true),
    table_mode_(),
    index_type_(share::schema::ObIndexType::INDEX_TYPE_MAX),
    root_block_addr_(),
    root_block_data_(),
    root_row_store_type_(common::ObRowStoreType::MAX_ROW_STORE),
    latest_row_store_type_(common::ObRowStoreType::MAX_ROW_STORE),
    data_index_tree_height_(-1),
    data_block_macro_meta_addr_(),
    data_block_macro_meta_(),
    index_blocks_cnt_(-1),
    data_blocks_cnt_(-1),
    micro_block_cnt_(-1),
    use_old_macro_block_count_(-1),
    row_count_(-1),
    column_group_cnt_(0),
    co_base_type_(ObCOSSTableBaseType::INVALID_TYPE),
    rowkey_column_cnt_(-1),
    column_cnt_(-1),
    full_column_cnt_(-1),
    data_checksum_(0),
    occupy_size_(-1),
    original_size_(-1),
    max_merged_trans_version_(0),
    ddl_scn_(),
    filled_tx_scn_(),
    tx_data_recycle_scn_(),
    is_co_table_without_cgs_(false),
    contain_uncommitted_row_(false),
    is_meta_root_(false),
    compressor_type_(ObCompressorType::INVALID_COMPRESSOR),
    encrypt_id_(0),
    master_key_id_(0),
    recycle_version_(-1),
    nested_offset_(-1),
    nested_size_(-1),
    root_macro_seq_(-1),
    data_block_ids_(),
    other_block_ids_(),
    table_backup_flag_(),
    table_shared_flag_(),
    uncommitted_tx_id_(0),
    co_base_snapshot_version_(-1)
{
  MEMSET(encrypt_key_, 0, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
}

bool ObTabletCreateSSTableParam::is_valid() const
{
  bool ret = true;
  const bool is_shared_storage = GCTX.is_shared_storage_mode();
  if (OB_UNLIKELY(!table_key_.is_valid())) {
    ret = false;
    LOG_WARN("invalid table key", K(table_key_));
  } else if (OB_UNLIKELY(!table_mode_.is_valid())) {
    ret = false;
    LOG_WARN("invalid table mode", K(table_mode_));
  } else if (OB_UNLIKELY(!table_backup_flag_.is_valid() || !table_shared_flag_.is_valid())) {
    ret = false;
    LOG_WARN("invalid table backup flag or invalid table shared flag", K_(table_backup_flag), K_(table_shared_flag));
  } else if (!(schema_version_ >= 0
               && sstable_logic_seq_ >= 0
               && create_snapshot_version_ >= 0
               && progressive_merge_round_ >= 0
               && progressive_merge_step_ >= 0
               && index_type_ < share::schema::ObIndexType::INDEX_TYPE_MAX
               && root_row_store_type_ < ObRowStoreType::MAX_ROW_STORE
               && (latest_row_store_type_ < ObRowStoreType::MAX_ROW_STORE
                  || ObRowStoreType::DUMMY_ROW_STORE == latest_row_store_type_)
               && data_index_tree_height_ >= 0
               && index_blocks_cnt_ >= 0
               && data_blocks_cnt_ >= 0
               && micro_block_cnt_ >= 0
               && use_old_macro_block_count_ >= 0
               && row_count_ >= 0
               && column_group_cnt_ > 0
               && rowkey_column_cnt_ >= 0
               && column_cnt_ >= 0
               && occupy_size_ >= 0
               && ddl_scn_.is_valid()
               && filled_tx_scn_.is_valid()
               && tx_data_recycle_scn_.is_valid()
               && original_size_ >= 0
               && recycle_version_ >= 0
               && root_macro_seq_ >= 0
               && co_base_snapshot_version_ >= 0
               && nested_offset_ >= 0
               && nested_size_ >= 0
               && co_base_snapshot_version_ >= 0)) {
    ret = false;
    LOG_WARN("invalid basic params", KPC(this)); // LOG_KVS arg number overflow
  } else if (ObITable::is_ddl_sstable(table_key_.table_type_)) {
    // ddl sstable can have invalid meta addr, so skip following ifs
    if (!ddl_scn_.is_valid_and_not_min()) {
      ret = false;
      LOG_WARN("ddl log ts is invalid", K(ddl_scn_), K(table_key_));
    } else if (is_shared_storage && table_key_.is_ddl_dump_sstable() && !table_shared_flag_.is_shared_macro_blocks()) {
      ret = false;
      LOG_ERROR("invalid ddl dump sstable table flag", K(is_shared_storage), K(table_key_), K(table_shared_flag_));
    }
  } else if (!is_block_meta_valid(root_block_addr_, root_block_data_)) {
    ret = false;
    LOG_WARN("invalid root meta", K(root_block_addr_), K(root_block_data_));
  } else if (!is_block_meta_valid(data_block_macro_meta_addr_, data_block_macro_meta_)) {
    ret = false;
    LOG_WARN("invalid data meta", K(data_block_macro_meta_addr_), K(data_block_macro_meta_));
  } else if (table_shared_flag_.is_shared_sstable() && (data_blocks_cnt_ + index_blocks_cnt_) > 0 && 0 == root_macro_seq_) {
    ret = false;
    LOG_ERROR("invalid root macro seq", K(data_blocks_cnt_), K(data_blocks_cnt_), K(root_macro_seq_));
  } else if (!table_key_.get_tablet_id().is_ls_inner_tablet() && table_key_.is_minor_sstable() && filled_tx_scn_ < table_key_.get_end_scn()) {
    ret = false;
    LOG_WARN("filled tx scn is invalid", K(filled_tx_scn_), K(table_key_));
  }
  return ret;
}

bool ObTabletCreateSSTableParam::is_block_meta_valid(const storage::ObMetaDiskAddr &addr,
                                                     const blocksstable::ObMicroBlockData &data) const
{
  return addr.is_valid() && (!addr.is_memory() || (data.is_valid() && data.size_ == addr.size()));
}

// careful! init_for_ha func not call inner_init_with_merge_res
int ObTabletCreateSSTableParam::inner_init_with_merge_res(const blocksstable::ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  blocksstable::ObSSTableMergeRes::fill_addr_and_data(res.root_desc_,
      root_block_addr_, root_block_data_);
  blocksstable::ObSSTableMergeRes::fill_addr_and_data(res.data_root_desc_,
      data_block_macro_meta_addr_, data_block_macro_meta_);
  root_row_store_type_ = res.root_row_store_type_;
  data_index_tree_height_ = res.root_desc_.height_;
  index_blocks_cnt_ = res.index_blocks_cnt_;
  data_blocks_cnt_ = res.data_blocks_cnt_;
  micro_block_cnt_ = res.micro_block_cnt_;
  use_old_macro_block_count_ = res.use_old_macro_block_count_;
  row_count_ = res.row_count_;
  data_checksum_ = res.data_checksum_;
  occupy_size_ = res.occupy_size_;
  original_size_ = res.original_size_;
  contain_uncommitted_row_ = res.contain_uncommitted_row_;
  compressor_type_ = res.compressor_type_;
  encrypt_id_ = res.encrypt_id_;
  master_key_id_ = res.master_key_id_;
  is_meta_root_ = res.data_root_desc_.is_meta_root_;
  root_macro_seq_ = res.root_macro_seq_;
  STATIC_ASSERT(ARRAYSIZEOF(encrypt_key_) == share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH,
  "ObTabletCreateSSTableParam encrypt_key_ array size mismatch OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH");
  STATIC_ASSERT(ARRAYSIZEOF(res.encrypt_key_) == share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH,
  "ObSSTableMergeRes encrypt_key_ array size mismatch OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH");
  MEMCPY(encrypt_key_, res.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  table_backup_flag_ = res.table_backup_flag_;

  if (OB_FAIL(data_block_ids_.assign(res.data_block_ids_))) {
    LOG_WARN("fail to fill data block ids", K(ret), K(res.data_block_ids_));
  } else if (OB_FAIL(other_block_ids_.assign(res.other_block_ids_))) {
    LOG_WARN("fail to fill other block ids", K(ret), K(res.other_block_ids_));
  }
  return ret;
}

int ObTabletCreateSSTableParam::init_for_empty_major_sstable(const ObTabletID &tablet_id,
                                                             const ObStorageSchema &storage_schema,
                                                             const int64_t snapshot_version,
                                                             const int64_t column_group_idx,
                                                             const bool has_all_column_group)
{
  int ret = OB_SUCCESS;
  set_init_value_for_column_store_();
  if (OB_UNLIKELY(!storage_schema.is_valid() || !tablet_id.is_valid()
      || OB_INVALID_VERSION == snapshot_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(storage_schema), K(snapshot_version));
  } else if (OB_FAIL(storage_schema.get_encryption_id(encrypt_id_))) {
    LOG_WARN("fail to get_encryption_id", K(ret), K(storage_schema));
  } else {
    master_key_id_ = storage_schema.get_master_key_id();
    MEMCPY(encrypt_key_, storage_schema.get_encrypt_key_str(), storage_schema.get_encrypt_key_len());

    const int64_t multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    table_key_.table_type_ = 1 < storage_schema.get_column_group_count()
                                 ? ObITable::TableType::COLUMN_ORIENTED_SSTABLE
                                 : ObITable::TableType::MAJOR_SSTABLE;

    table_key_.tablet_id_ = tablet_id;
    table_key_.version_range_.snapshot_version_ = snapshot_version;
    max_merged_trans_version_ = snapshot_version;

    schema_version_ = storage_schema.get_schema_version();
    create_snapshot_version_ = 0;
    progressive_merge_round_ = storage_schema.get_progressive_merge_round();
    progressive_merge_step_ = 0;

    table_mode_ = storage_schema.get_table_mode_struct();
    index_type_ = storage_schema.get_index_type();
    rowkey_column_cnt_ = storage_schema.get_rowkey_column_num()
            + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    root_block_addr_.set_none_addr();
    data_block_macro_meta_addr_.set_none_addr();
    root_row_store_type_ = (ObRowStoreType::ENCODING_ROW_STORE == storage_schema.get_row_store_type()
        ? ObRowStoreType::SELECTIVE_ENCODING_ROW_STORE : storage_schema.get_row_store_type());
    latest_row_store_type_ = storage_schema.get_row_store_type();
    data_index_tree_height_ = 0;
    index_blocks_cnt_ = 0;
    data_blocks_cnt_ = 0;
    micro_block_cnt_ = 0;
    use_old_macro_block_count_ = 0;
    data_checksum_ = 0;
    occupy_size_ = 0;
    ddl_scn_.set_min();
    filled_tx_scn_.set_min();
    tx_data_recycle_scn_.set_min();
    original_size_ = 0;
    compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    table_backup_flag_.reset();
    table_shared_flag_.reset();
    sstable_logic_seq_ = 0;
    row_count_ = 0;
    recycle_version_ = 0;
    root_macro_seq_ = 0;
    nested_size_ = 0;
    nested_offset_ = 0;
    column_group_cnt_ = 1;
    co_base_type_ = ObCOSSTableBaseType::INVALID_TYPE;
    full_column_cnt_ = 0;
    is_co_table_without_cgs_ = false;
    co_base_snapshot_version_ = 0;
    if (OB_FAIL(storage_schema.get_store_column_count(column_cnt_, true/*is_full*/))) {
      LOG_WARN("fail to get stored col cnt of table schema", K(ret), K(storage_schema));
    } else if (FALSE_IT(column_cnt_ += multi_version_col_cnt)) {
    } else if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(column_cnt_,
        column_checksums_))) {
      LOG_WARN("fail to fill column checksum for empty major", K(ret), K(column_cnt_));
    }
  }

  if (OB_SUCC(ret) && column_group_idx >= 0) {
    table_key_.column_group_idx_ = column_group_idx;
    is_co_table_without_cgs_ = true;

    if (OB_FAIL(storage_schema.get_stored_column_count_in_sstable(full_column_cnt_))) {
      LOG_WARN("failed to get_stored_column_count_in_sstable", K(ret));
    } else {
      const ObStorageColumnGroupSchema &cg_schema = storage_schema.get_column_groups().at(column_group_idx);

      if (cg_schema.is_all_column_group()) {
        table_key_.table_type_ = ObITable::TableType::COLUMN_ORIENTED_SSTABLE;
        co_base_type_ = ObCOSSTableBaseType::ALL_CG_TYPE;
      } else if (cg_schema.is_rowkey_column_group()) {
        table_key_.table_type_ = has_all_column_group
                                        ? ObITable::TableType::ROWKEY_COLUMN_GROUP_SSTABLE
                                        : ObITable::TableType::COLUMN_ORIENTED_SSTABLE;

        co_base_type_ = has_all_column_group
                              ? ObCOSSTableBaseType::ALL_CG_TYPE
                              : ObCOSSTableBaseType::ROWKEY_CG_TYPE;

        rowkey_column_cnt_ = cg_schema.column_cnt_;
        column_cnt_ = cg_schema.column_cnt_;
      } else {
        table_key_.table_type_ = ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE;
        rowkey_column_cnt_ = 0;
        column_cnt_ = cg_schema.column_cnt_;
      }

      if (ObITable::TableType::COLUMN_ORIENTED_SSTABLE == table_key_.table_type_) {
        column_group_cnt_ = storage_schema.get_column_group_count();
      }
    }
  }
  return ret;
}

int ObTabletCreateSSTableParam::init_for_split_empty_minor_sstable(const ObTabletID &tablet_id,
                                                                   const share::SCN &start_scn,
                                                                   const share::SCN &end_scn,
                                                                   const blocksstable::ObSSTableBasicMeta &basic_meta)
{
  int ret = OB_SUCCESS;
  set_init_value_for_column_store_();
  table_key_.table_type_ = ObITable::TableType::MINOR_SSTABLE;
  table_key_.tablet_id_ = tablet_id;
  table_key_.scn_range_.start_scn_ = start_scn;
  table_key_.scn_range_.end_scn_ = end_scn;
  max_merged_trans_version_ = 0;

  schema_version_ = basic_meta.schema_version_;
  progressive_merge_round_ = basic_meta.progressive_merge_round_;
  progressive_merge_step_ = basic_meta.progressive_merge_step_;
  sstable_logic_seq_ = basic_meta.sstable_logic_seq_;
  filled_tx_scn_ = basic_meta.filled_tx_scn_;
  table_mode_ = basic_meta.table_mode_;
  index_type_ = static_cast<share::schema::ObIndexType> (basic_meta.index_type_);
  rowkey_column_cnt_ = basic_meta.rowkey_column_count_;
  latest_row_store_type_ = basic_meta.latest_row_store_type_;
  recycle_version_ = basic_meta.recycle_version_;
  schema_version_ = basic_meta.schema_version_;
  create_snapshot_version_ = basic_meta.create_snapshot_version_;
  ddl_scn_ = basic_meta.ddl_scn_;
  progressive_merge_round_ = basic_meta.progressive_merge_round_;
  progressive_merge_step_ = basic_meta.progressive_merge_step_;
  column_cnt_ = basic_meta.column_cnt_;
  master_key_id_ = basic_meta.master_key_id_;
  co_base_snapshot_version_ = basic_meta.co_base_snapshot_version_;
  MEMCPY(encrypt_key_, basic_meta.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);

  root_block_addr_.set_none_addr();
  data_block_macro_meta_addr_.set_none_addr();
  root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  data_index_tree_height_ = 0;
  index_blocks_cnt_ = 0;
  data_blocks_cnt_ = 0;
  micro_block_cnt_ = 0;
  use_old_macro_block_count_ = 0;
  row_count_ = 0;
  data_checksum_ = 0;
  occupy_size_ = 0;
  ddl_scn_.set_min();
  filled_tx_scn_ = end_scn;
  tx_data_recycle_scn_.set_min();
  original_size_ = 0;
  compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  nested_offset_ = 0;
  nested_size_ = 0;
  root_macro_seq_ = 0;
  full_column_cnt_ = 0;
  column_group_cnt_ = 1;
  return ret;
}

int ObTabletCreateSSTableParam::init_for_transfer_empty_minor_sstable(const common::ObTabletID &tablet_id,
                                                                      const share::SCN &start_scn,
                                                                      const share::SCN &end_scn,
                                                                      const ObStorageSchema &table_schema)
{
  int ret = OB_SUCCESS;
  set_init_value_for_column_store_();
  master_key_id_ = table_schema.get_master_key_id();
  MEMCPY(encrypt_key_, table_schema.get_encrypt_key_str(), table_schema.get_encrypt_key_len());
  const int64_t multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  table_key_.table_type_ = ObITable::TableType::MINOR_SSTABLE;
  table_key_.tablet_id_ = tablet_id;
  table_key_.scn_range_.start_scn_ = start_scn;
  table_key_.scn_range_.end_scn_ = end_scn;
  max_merged_trans_version_ = 0;

  schema_version_ = table_schema.get_schema_version();
  create_snapshot_version_ = 0;
  progressive_merge_round_ = table_schema.get_progressive_merge_round();
  progressive_merge_step_ = 0;

  table_mode_ = table_schema.get_table_mode_struct();
  index_type_ = table_schema.get_index_type();
  rowkey_column_cnt_ = table_schema.get_rowkey_column_num()
          + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  root_block_addr_.set_none_addr();
  data_block_macro_meta_addr_.set_none_addr();
  root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  data_index_tree_height_ = 0;
  index_blocks_cnt_ = 0;
  data_blocks_cnt_ = 0;
  micro_block_cnt_ = 0;
  use_old_macro_block_count_ = 0;
  column_cnt_ = table_schema.get_column_count() + multi_version_col_cnt;
  data_checksum_ = 0;
  occupy_size_ = 0;
  ddl_scn_.set_min();
  filled_tx_scn_ = end_scn;
  tx_data_recycle_scn_.set_min();
  original_size_ = 0;
  compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  table_backup_flag_.reset();
  table_shared_flag_.reset();
  sstable_logic_seq_ = 0;
  row_count_ = 0;
  recycle_version_ = 0;
  root_macro_seq_ = 0;
  nested_offset_ = 0;
  nested_size_ = 0;
  column_group_cnt_ = 1;
  co_base_type_ = ObCOSSTableBaseType::INVALID_TYPE;
  full_column_cnt_ = 0;
  is_co_table_without_cgs_ = false;
  co_base_snapshot_version_ = 0;

  if (OB_FAIL(table_schema.get_encryption_id(encrypt_id_))) {
    LOG_WARN("fail to get encryption id", K(ret), K(table_schema));
  }

  if (OB_SUCC(ret) && !is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), K(*this));
  }
  return ret;
}

int ObTabletCreateSSTableParam::init_for_small_sstable(const blocksstable::ObSSTableMergeRes &res,
                                                       const ObITable::TableKey &table_key,
                                                       const blocksstable::ObSSTableMeta &sstable_meta,
                                                       const blocksstable::ObBlockInfo &block_info)
{
  int ret = OB_SUCCESS;
  set_init_value_for_column_store_();
  const blocksstable::ObSSTableBasicMeta &basic_meta = sstable_meta.get_basic_meta();
  filled_tx_scn_ = basic_meta.filled_tx_scn_;
  tx_data_recycle_scn_ = basic_meta.tx_data_recycle_scn_;
  ddl_scn_ = basic_meta.ddl_scn_;
  table_key_ = table_key;
  sstable_logic_seq_ = sstable_meta.get_sstable_seq();
  table_mode_ = basic_meta.table_mode_;
  index_type_ = static_cast<share::schema::ObIndexType>(basic_meta.index_type_);
  schema_version_ = basic_meta.schema_version_;
  create_snapshot_version_ = basic_meta.create_snapshot_version_;
  progressive_merge_round_ = basic_meta.progressive_merge_round_;
  progressive_merge_step_ = basic_meta.progressive_merge_step_;
  rowkey_column_cnt_ = basic_meta.rowkey_column_count_;
  recycle_version_ = basic_meta.recycle_version_;
  latest_row_store_type_ = basic_meta.latest_row_store_type_;
  co_base_snapshot_version_ = basic_meta.co_base_snapshot_version_;
  is_ready_for_read_ = true;
  column_cnt_ = res.data_column_cnt_;
  max_merged_trans_version_ = res.max_merged_trans_version_;
  nested_offset_ = block_info.nested_offset_;
  nested_size_ = block_info.nested_size_;
  table_shared_flag_.reset();

  if (table_key_.is_column_store_sstable()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("small sstable do not support co sstable", K(ret), K(table_key));
  } else if (OB_FAIL(inner_init_with_merge_res(res))) {
    LOG_WARN("fail to inner init with merge res", K(ret), K(res));
  } else if (table_key_.is_major_sstable()) {
    if (OB_FAIL(column_checksums_.assign(res.data_column_checksums_))) {
      LOG_WARN("fail to fill column checksum", K(ret), K(res.data_column_checksums_));
    }
  }
  if (OB_SUCC(ret)) {
    if (!is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("init for small sstable get invalid argument", K(ret), K(res), K(table_key), KPC(this),
          K(sstable_meta), K(block_info));
    }
  }
  return ret;
}

int ObTabletCreateSSTableParam::init_for_merge(const compaction::ObBasicTabletMergeCtx &ctx,
                                               const blocksstable::ObSSTableMergeRes &res,
                                               const ObStorageColumnGroupSchema *cg_schema,
                                               const int64_t column_group_idx)
{
  int ret = OB_SUCCESS;
  const compaction::ObStaticMergeParam &static_param = ctx.static_param_;
  set_init_value_for_column_store_();

  if (OB_FAIL(static_param.schema_->get_stored_column_count_in_sstable(full_column_cnt_))) {
    LOG_WARN("fail to get_stored_column_count_in_sstable", K(ret), KPC(cg_schema), K(res));
  } else {
    ObITable::TableKey table_key;
    bool is_main_table = (nullptr == cg_schema) ? false : (cg_schema->is_all_column_group() || cg_schema->is_rowkey_column_group());
    table_key.table_type_ = ctx.get_merged_table_type(cg_schema, is_main_table);
    table_key.tablet_id_ = ctx.get_tablet_id();
    table_key.column_group_idx_ = (nullptr == cg_schema) ? 0 : column_group_idx;
    if (is_major_or_meta_merge_type(static_param.get_merge_type())) {
      table_key.version_range_.snapshot_version_ = static_param.version_range_.snapshot_version_;
    } else {
      table_key.scn_range_ = static_param.scn_range_;
    }
    if (is_minor_merge_type(static_param.get_merge_type()) && res.contain_uncommitted_row_) {
      uncommitted_tx_id_ = static_param.tx_id_;
    } else {
      uncommitted_tx_id_ = 0;
    }
    table_key_ = table_key;

    if (ObITable::TableType::COLUMN_ORIENTED_SSTABLE == table_key.table_type_ ||
        ObITable::TableType::COLUMN_ORIENTED_META_SSTABLE == table_key.table_type_) {
      co_base_type_ = cg_schema->is_all_column_group()
                          ? ObCOSSTableBaseType::ALL_CG_TYPE
                          : ObCOSSTableBaseType::ROWKEY_CG_TYPE;
    }

    sstable_logic_seq_ = static_param.sstable_logic_seq_;
    filled_tx_scn_ = ctx.get_merge_scn();

    table_mode_ = ctx.get_schema()->get_table_mode_struct();
    index_type_ = ctx.get_schema()->get_index_type();
    if (nullptr != cg_schema && !cg_schema->is_rowkey_column_group() && !cg_schema->is_all_column_group()) {
      rowkey_column_cnt_ = 0;
    } else {
      column_group_cnt_ = static_param.schema_->get_column_group_count();
      rowkey_column_cnt_ = static_param.schema_->get_rowkey_column_num()
            + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    }
    latest_row_store_type_ = ctx.get_schema()->get_row_store_type();
    if (is_minor_merge_type(static_param.get_merge_type())) {
      recycle_version_ = static_param.version_range_.base_version_;
    } else {
      recycle_version_ = 0;
    }
    schema_version_ = ctx.get_schema()->get_schema_version();
    create_snapshot_version_ = static_param.create_snapshot_version_;
    progressive_merge_round_ = ctx.get_progressive_merge_round();
    progressive_merge_step_ = ctx.get_result_progressive_merge_step(column_group_idx);
    is_co_table_without_cgs_ = is_main_table ? (0 == res.data_blocks_cnt_ || static_param.is_build_row_store()) : false;
    column_cnt_ = res.data_column_cnt_;
    if ((0 == res.row_count_ && 0 == res.max_merged_trans_version_)
        || (nullptr != cg_schema && !cg_schema->has_multi_version_column())) {
      // empty mini table merged forcely
      max_merged_trans_version_ = static_param.version_range_.snapshot_version_;
    } else {
      max_merged_trans_version_ = res.max_merged_trans_version_;
    }
    nested_size_ = res.nested_size_;
    nested_offset_ = res.nested_offset_;
    ddl_scn_.set_min();
    table_shared_flag_.reset();
    co_base_snapshot_version_ = ctx.static_param_.co_base_snapshot_version_;
    tx_data_recycle_scn_.set_min();

    if (OB_FAIL(inner_init_with_merge_res(res))) {
      LOG_WARN("fail to init with merge res", K(ret), K(res.data_block_ids_));
    } else if (is_major_or_meta_merge_type(static_param.get_merge_type())) {
      if (OB_FAIL(column_checksums_.assign(res.data_column_checksums_))) {
        LOG_WARN("fail to fill column checksum", K(ret), K(res.data_column_checksums_));
      } else if (GCTX.is_shared_storage_mode() && is_major_merge_type(static_param.get_merge_type())) {
        table_shared_flag_.set_shared_sstable();
      }
    }

    if (OB_SUCC(ret)) {
      if (!is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("init for merge sstable get invalid argument", K(ret), K(table_key), KPC(this),
            K(res), K(ctx));
      }
    }
  }
  return ret;
}

int ObTabletCreateSSTableParam::init_for_ddl(blocksstable::ObSSTableIndexBuilder *sstable_index_builder,
                                             const ObTabletDDLParam &ddl_param,
                                             const blocksstable::ObSSTable *first_ddl_sstable,
                                             const ObStorageSchema &storage_schema,
                                             const int64_t macro_block_column_count,
                                             const int64_t create_schema_version_on_tablet,
                                             const ObIArray<blocksstable::MacroBlockId> &macro_id_array)
{
  int ret = OB_SUCCESS;
  SMART_VAR(blocksstable::ObSSTableMergeRes, res) {
    int64_t column_count = 0;
    int64_t full_column_cnt = 0; // only used for co sstable
    share::schema::ObTableMode table_mode = storage_schema.get_table_mode_struct();
    share::schema::ObIndexType index_type = storage_schema.get_index_type();
    int64_t rowkey_column_cnt = storage_schema.get_rowkey_column_num() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    common::ObRowStoreType row_store_type = storage_schema.get_row_store_type();
    set_init_value_for_column_store_();

    if (nullptr != first_ddl_sstable) {
      blocksstable::ObSSTableMetaHandle meta_handle;
      if (OB_FAIL(first_ddl_sstable->get_meta(meta_handle))) {
        LOG_WARN("get sstable meta handle fail", K(ret), KPC(first_ddl_sstable));
      } else {
        column_count = meta_handle.get_sstable_meta().get_column_count();
        table_mode = meta_handle.get_sstable_meta().get_basic_meta().table_mode_;
        index_type = static_cast<share::schema::ObIndexType>(meta_handle.get_sstable_meta().get_basic_meta().index_type_);
        rowkey_column_cnt = meta_handle.get_sstable_meta().get_basic_meta().rowkey_column_count_;
        row_store_type = meta_handle.get_sstable_meta().get_basic_meta().latest_row_store_type_;
        if (first_ddl_sstable->is_co_sstable()) {
          const ObCOSSTableV2 *first_co_sstable = static_cast<const ObCOSSTableV2 *>(first_ddl_sstable);
          if (OB_ISNULL((first_co_sstable))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("first co sstable is null", K(ret), KP(first_co_sstable), KPC(first_ddl_sstable));
          } else {
            full_column_cnt = first_co_sstable->get_cs_meta().full_column_cnt_;
          }
        }
      }
    } else if (ddl_param.table_key_.is_column_store_sstable()) {
      if (ddl_param.table_key_.is_normal_cg_sstable()) {
        rowkey_column_cnt = 0;
        column_count = 1;
      } else { // co sstable with all cg or rowkey cg
        const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema.get_column_groups();
        const int64_t cg_idx = ddl_param.table_key_.get_column_group_id();
        if (cg_idx < 0 || cg_idx >= cg_schemas.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected column group index", K(ret), K(cg_idx));
        } else if (OB_FAIL(storage_schema.get_stored_column_count_in_sstable(full_column_cnt))) { // set full_column_cnt in first ddl sstable
          LOG_WARN("fail to get stored column count in sstable", K(ret));
        } else if (cg_schemas.at(cg_idx).is_rowkey_column_group()) {
          column_count = rowkey_column_cnt;
        } else {
          column_count = full_column_cnt;
          if (macro_block_column_count > 0 && macro_block_column_count < column_count) {
            LOG_INFO("use macro block column count", K(ddl_param), K(macro_block_column_count), K(column_count));
            column_count = macro_block_column_count;
            full_column_cnt = macro_block_column_count;
          }
        }
      }
    } else { // row store sstable
      if (OB_FAIL(storage_schema.get_stored_column_count_in_sstable(column_count))) {
        LOG_WARN("fail to get stored column count in sstable", K(ret));
      } else if (macro_block_column_count > 0 && macro_block_column_count < column_count) {
        LOG_INFO("use macro block column count", K(ddl_param), K(macro_block_column_count), K(column_count));
        column_count = macro_block_column_count;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sstable_index_builder->close(res))) {
      LOG_WARN("close sstable index builder close failed", K(ret));
    } else if (ddl_param.table_key_.is_normal_cg_sstable() // index builder of cg sstable cannot get trans_version from row, manually set it
        && FALSE_IT(res.max_merged_trans_version_ = ddl_param.snapshot_version_)) {
    } else if (OB_UNLIKELY((ddl_param.table_key_.is_major_sstable() ||
                            ddl_param.table_key_.is_ddl_sstable()) &&
                            res.row_count_ > 0 &&
                            res.max_merged_trans_version_ != ddl_param.snapshot_version_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("max_merged_trans_version_ in res is different from ddl snapshot version", K(ret),
                K(res), K(ddl_param));
    } else {
      table_key_ = ddl_param.table_key_;
      table_mode_ = table_mode;
      index_type_ = index_type;
      rowkey_column_cnt_ = rowkey_column_cnt;
      schema_version_ = create_schema_version_on_tablet;
      latest_row_store_type_ = row_store_type;
      create_snapshot_version_ = ddl_param.snapshot_version_;
      ddl_scn_ = ddl_param.start_scn_;
      column_cnt_ = column_count;
      full_column_cnt_ = full_column_cnt;
      max_merged_trans_version_ = ddl_param.snapshot_version_;
      nested_size_ = res.nested_size_;
      nested_offset_ = res.nested_offset_;
      table_shared_flag_.reset();
      filled_tx_scn_ = table_key_.get_end_scn();
      sstable_logic_seq_ = 0;
      progressive_merge_round_ = 0;
      progressive_merge_step_ = 0;
      tx_data_recycle_scn_.set_min();
      recycle_version_ = 0;

      if (OB_FAIL(inner_init_with_merge_res(res))) {
        LOG_WARN("fail to inner init with merge res", K(ret), K(res));
      } else if (ddl_param.table_key_.is_co_sstable()) {
        column_group_cnt_ = storage_schema.get_column_group_count();
        // only set true when build empty major sstable. ddl co sstable must set false and fill cg sstables
        is_co_table_without_cgs_ = ddl_param.table_key_.is_major_sstable() && 0 == data_blocks_cnt_;
        co_base_snapshot_version_ = 0;
        const int64_t base_cg_idx = ddl_param.table_key_.get_column_group_id();
        if (base_cg_idx < 0 || base_cg_idx >= storage_schema.get_column_group_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column group index", K(ret), K(ddl_param.table_key_));
        } else {
          const ObStorageColumnGroupSchema &base_cg_schema = storage_schema.get_column_groups().at(base_cg_idx);
          if (base_cg_schema.is_all_column_group()) {
            co_base_type_ = ObCOSSTableBaseType::ALL_CG_TYPE;
          } else if (base_cg_schema.is_rowkey_column_group()) {
            co_base_type_ = ObCOSSTableBaseType::ROWKEY_CG_TYPE;
          } else {
            ret = OB_ERR_SYS;
            LOG_WARN("unknown type of base cg schema", K(ret), K(base_cg_idx));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (GCTX.is_shared_storage_mode()) {
          // in shared storage mode:
          // if the ddl kv is from inc direct load, then the other_block_ids_ contains the index block of sstables and macro_id_array is empty
          // else the ddl kv is from the ddl or full direct load, then the other_block_ids_ is empty and macro_id_array contains the shared blocks.
          if (other_block_ids_.count() > 0 && macro_id_array.count() > 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("other block id not empty", K(ret), K(other_block_ids_), K(macro_id_array));
          } else if (other_block_ids_.empty() && OB_FAIL(other_block_ids_.assign(macro_id_array))) {
            LOG_WARN("assign macro block id array to other block id array failed", K(ret), K(macro_id_array.count()));
          } else if (macro_id_array.count() > 0) {
            table_shared_flag_.set_share_macro_blocks();
          }
        } else {
          if (macro_id_array.count() > 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("other_block_ids should be empty in share nothing mode", K(ret), K(macro_id_array.count()));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!is_incremental_direct_load(ddl_param.direct_load_type_)) {
        if (OB_FAIL(column_checksums_.assign(res.data_column_checksums_))) {
          LOG_WARN("fail to fill column checksum for empty major", K(ret), K(res.data_column_checksums_));
        } else if (OB_UNLIKELY(column_checksums_.count() != column_count)) {
          // we have corrected the col_default_checksum_array_ in prepare_index_data_desc
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected column checksums", K(ret), K(column_count), KPC(this));
        }
      }

      if (OB_SUCC(ret)) {
        if (!is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("init for ddl sstable get invalid argument", K(ret), K(ddl_param), KPC(this),
              K(res));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!ddl_param.table_key_.is_co_sstable()) {
      uncommitted_tx_id_ = ddl_param.trans_id_.get_id();
    }
  }
  return ret;
}


int ObTabletCreateSSTableParam::init_for_ddl_mem(const ObITable::TableKey &table_key,
                                                 const share::SCN &ddl_start_scn,
                                                 const ObStorageSchema &storage_schema,
                                                 ObBlockMetaTree &block_meta_tree)
{
  int ret = OB_SUCCESS;
  set_init_value_for_column_store_();
  int64_t column_count = 0;
  const ObDataStoreDesc &data_desc = block_meta_tree.get_data_desc();
  const int64_t root_block_size = sizeof(ObBlockMetaTree);

  if (OB_FAIL(storage_schema.get_stored_column_count_in_sstable(column_count))) {
    LOG_WARN("fail to get stored column count in sstable", K(ret));
  } else {
    table_key_ = table_key;
    if (table_key.is_column_store_sstable()) {
      if (table_key.is_normal_cg_sstable()) {
        table_key_.table_type_ = ObITable::TableType::DDL_MEM_CG_SSTABLE;
        rowkey_column_cnt_ = 0;
        column_cnt_ = 1;
      } else { // co sstable with all cg or rowkey cg
        table_key_.table_type_ = ObITable::TableType::DDL_MEM_CO_SSTABLE;
        rowkey_column_cnt_ = storage_schema.get_rowkey_column_num() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();

        // calculate column count
        const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema.get_column_groups();
        const int64_t cg_idx = table_key_.get_column_group_id();
        if (cg_idx < 0 || cg_idx >= cg_schemas.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected column group index", K(ret), K(cg_idx));
        } else if (cg_schemas.at(cg_idx).is_rowkey_column_group()) {
          column_count = storage_schema.get_rowkey_column_num() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
        } else {
          if (OB_FAIL(storage_schema.get_stored_column_count_in_sstable(column_count))) {
            LOG_WARN("fail to get stored column count in sstable", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          column_cnt_ = column_count;
        }
      }
    } else {
      if (table_key.table_type_ == ObITable::TableType::MINI_SSTABLE) {
        table_key_.table_type_ = ObITable::TableType::DDL_MEM_MINI_SSTABLE;
      } else {
        table_key_.table_type_ = ObITable::TableType::DDL_MEM_SSTABLE;
      }
      rowkey_column_cnt_ = storage_schema.get_rowkey_column_num() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
      column_cnt_ = column_count;
    }
    is_ready_for_read_ = true;
    table_mode_ = storage_schema.get_table_mode_struct();
    index_type_ = storage_schema.get_index_type();
    schema_version_ = storage_schema.get_schema_version();
    latest_row_store_type_ = storage_schema.get_row_store_type();
    create_snapshot_version_ = table_key.get_snapshot_version();
    max_merged_trans_version_ = table_key.get_snapshot_version();
    ddl_scn_ = ddl_start_scn;
    root_row_store_type_ = data_desc.get_row_store_type(); // for root block, not used for ddl memtable
    data_index_tree_height_ = 2; // fixed tree height, because there is only one root block
    contain_uncommitted_row_ = table_key.is_minor_sstable();
    compressor_type_ = data_desc.get_compressor_type();
    encrypt_id_ = data_desc.get_encrypt_id();
    master_key_id_ = data_desc.get_master_key_id();
    MEMCPY(encrypt_key_, data_desc.get_encrypt_key(), share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    use_old_macro_block_count_ = 0; // all new, no reuse
    index_blocks_cnt_ = 0; // index macro block count, the index is in memory, so be 0.
    other_block_ids_.reset(); // other blocks contains only index macro blocks now, so empty.
    filled_tx_scn_ = table_key.is_major_sstable() ? SCN::min_scn() : table_key.get_end_scn();
    tx_data_recycle_scn_.set_min();
    table_backup_flag_.reset();
    table_shared_flag_.reset();
    sstable_logic_seq_ = 0;
    row_count_ = 0;
    recycle_version_ = 0;
    root_macro_seq_ = 0;
    nested_size_ = 0;
    nested_offset_ = 0;
    data_blocks_cnt_ = 0;
    micro_block_cnt_ = 0;
    occupy_size_ = 0;
    original_size_ = 0;
    progressive_merge_round_ = 0;
    progressive_merge_step_ = 0;
    column_group_cnt_ = 1;
    co_base_type_ = ObCOSSTableBaseType::INVALID_TYPE;
    full_column_cnt_ = 0;
    is_co_table_without_cgs_ = false;
    co_base_snapshot_version_ = 0;

    if (OB_SUCC(ret)) {
      // set root block for data tree
      if (OB_FAIL(root_block_addr_.set_mem_addr(0/*offset*/, root_block_size/*size*/))) {
        LOG_WARN("set root block address for data tree failed", K(ret));
      } else {
        root_block_data_.type_ = ObMicroBlockData::DDL_BLOCK_TREE;
        root_block_data_.buf_ = reinterpret_cast<char *>(&block_meta_tree);
        root_block_data_.size_ = root_block_size;
      }
    }

    if (OB_SUCC(ret)) {
      // set root block for secondary meta tree
      if (OB_FAIL(data_block_macro_meta_addr_.set_mem_addr(0/*offset*/, root_block_size/*size*/))) {
        LOG_WARN("set root block address for secondary meta tree failed", K(ret));
      } else {
        data_block_macro_meta_.type_ = ObMicroBlockData::DDL_BLOCK_TREE;
        data_block_macro_meta_.buf_ = reinterpret_cast<char *>(&block_meta_tree);
        data_block_macro_meta_.size_ = root_block_size;
      }
    }
  }
  return ret;
}

// todo @qilu: after steady, merge init_for_ss_ddl() and init_for_ddl()
// For now, try not to break the logic of share_nothing
int ObTabletCreateSSTableParam::init_for_ss_ddl(blocksstable::ObSSTableMergeRes &res,
                                                const ObITable::TableKey &table_key,
                                                const ObStorageSchema &storage_schema,
                                                const int64_t create_schema_version_on_tablet)
{
  int ret = OB_SUCCESS;
  int64_t snapshot_version = table_key.get_snapshot_version();
  int64_t column_count = 0;
  int64_t full_column_cnt = 0; // only used for co sstable
  share::schema::ObTableMode table_mode = storage_schema.get_table_mode_struct();
  share::schema::ObIndexType index_type = storage_schema.get_index_type();
  int64_t rowkey_column_cnt = storage_schema.get_rowkey_column_num() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  common::ObRowStoreType row_store_type = storage_schema.get_row_store_type();
  set_init_value_for_column_store_();

  if (table_key.is_column_store_sstable()) {
    if (table_key.is_normal_cg_sstable()) {
      rowkey_column_cnt = 0;
      column_count = 1;
    } else { // co sstable with all cg or rowkey cg
      const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema.get_column_groups();
      const int64_t cg_idx = table_key.get_column_group_id();
      if (cg_idx < 0 || cg_idx >= cg_schemas.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column group index", K(ret), K(cg_idx));
      } else if (OB_FAIL(storage_schema.get_stored_column_count_in_sstable(full_column_cnt))) { // set full_column_cnt in first ddl sstable
        LOG_WARN("fail to get stored column count in sstable", K(ret));
      } else if (cg_schemas.at(cg_idx).is_rowkey_column_group()) {
        column_count = rowkey_column_cnt;
      } else {
        column_count = full_column_cnt;
      }
    }
  } else { // row store sstable
    if (OB_FAIL(storage_schema.get_stored_column_count_in_sstable(column_count))) {
      LOG_WARN("fail to get stored column count in sstable", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (table_key.is_normal_cg_sstable() // index builder of cg sstable cannot get trans_version from row, manually set it
      && FALSE_IT(res.max_merged_trans_version_ = snapshot_version)) {
  } else if (OB_UNLIKELY((table_key.is_major_sstable() ||
                          table_key.is_ddl_sstable()) &&
                          res.row_count_ > 0 &&
                          res.max_merged_trans_version_ != snapshot_version)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("max_merged_trans_version_ in res is different from ddl snapshot version", K(ret),
              K(res));
  } else {
    table_key_ = table_key;
    table_mode_ = table_mode;
    index_type_ = index_type;
    rowkey_column_cnt_ = rowkey_column_cnt;
    schema_version_ = create_schema_version_on_tablet;
    latest_row_store_type_ = row_store_type;
    create_snapshot_version_ = snapshot_version;
    column_cnt_ = column_count;
    full_column_cnt_ = full_column_cnt;
    max_merged_trans_version_ = snapshot_version;
    nested_size_ = res.nested_size_;
    nested_offset_ = res.nested_offset_;
    table_shared_flag_.set_shared_sstable();
    filled_tx_scn_ = table_key_.get_end_scn();
    ddl_scn_.set_min();
    sstable_logic_seq_ = 0;
    progressive_merge_round_ = 0;
    progressive_merge_step_ = 0;
    tx_data_recycle_scn_.set_min();
    recycle_version_ = 0;

    if (OB_FAIL(inner_init_with_merge_res(res))) {
      LOG_WARN("fail to inner init with merge res", K(ret), K(res));
    } else if (table_key.is_co_sstable()) {
      column_group_cnt_ = storage_schema.get_column_group_count();
      co_base_snapshot_version_ = 0;
      // only set true when build empty major sstable. ddl co sstable must set false and fill cg sstables
      //is_empty_co_table_ = table_key.is_major_sstable() && 0 == data_blocks_cnt_;
      const int64_t base_cg_idx = table_key.get_column_group_id();
      if (base_cg_idx < 0 || base_cg_idx >= storage_schema.get_column_group_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column group index", K(ret), K(table_key));
      } else {
        const ObStorageColumnGroupSchema &base_cg_schema = storage_schema.get_column_groups().at(base_cg_idx);
        if (base_cg_schema.is_all_column_group()) {
          co_base_type_ = ObCOSSTableBaseType::ALL_CG_TYPE;
        } else if (base_cg_schema.is_rowkey_column_group()) {
          co_base_type_ = ObCOSSTableBaseType::ROWKEY_CG_TYPE;
        } else {
          ret = OB_ERR_SYS;
          LOG_WARN("unknown type of base cg schema", K(ret), K(base_cg_idx));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(column_checksums_.assign(res.data_column_checksums_))) {
      LOG_WARN("fail to fill column checksum for empty major", K(ret), K(res.data_column_checksums_));
    } else if (OB_UNLIKELY(column_checksums_.count() != column_count)) {
      // we have corrected the col_default_checksum_array_ in prepare_index_data_desc
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column checksums", K(ret), K(column_count), KPC(this));
    }
  }
  LOG_INFO("[SHARED STORAGE]init ddl param", K(ret), K(table_key), K(*this), K(column_count));
  return ret;
}


int ObTabletCreateSSTableParam::init_for_split(const ObTabletID &dst_tablet_id,
                                               const ObITable::TableKey &src_table_key,
                                               const blocksstable::ObSSTableBasicMeta &basic_meta,
                                               const int64_t schema_version,
                                               const blocksstable::ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  set_init_value_for_column_store_();
  table_key_ = src_table_key;
  table_key_.tablet_id_ = dst_tablet_id;
  sstable_logic_seq_ = basic_meta.sstable_logic_seq_;
  filled_tx_scn_ = basic_meta.filled_tx_scn_;
  table_mode_ = basic_meta.table_mode_;
  index_type_ = static_cast<share::schema::ObIndexType> (basic_meta.index_type_);
  rowkey_column_cnt_ = basic_meta.rowkey_column_count_;
  latest_row_store_type_ = basic_meta.latest_row_store_type_;
  recycle_version_ = basic_meta.recycle_version_;
  schema_version_ = schema_version; // use new schema version.
  create_snapshot_version_ = basic_meta.create_snapshot_version_;
  ddl_scn_ = basic_meta.ddl_scn_;
  progressive_merge_round_ = basic_meta.progressive_merge_round_;
  progressive_merge_step_ = basic_meta.progressive_merge_step_;
  co_base_snapshot_version_ = basic_meta.co_base_snapshot_version_;

  ddl_scn_.set_min();
  nested_size_ = res.nested_size_;
  nested_offset_ = res.nested_offset_;
  max_merged_trans_version_ = res.max_merged_trans_version_;
  column_cnt_ = res.data_column_cnt_;
  full_column_cnt_ = 0;
  tx_data_recycle_scn_.set_min();
  column_group_cnt_ = 1;

  if (OB_FAIL(inner_init_with_merge_res(res))) {
    LOG_WARN("fail to inner init with merge res", K(ret), K(res));
  }
  if (OB_SUCC(ret) && table_key_.is_major_sstable()) {
    if (OB_FAIL(column_checksums_.assign(res.data_column_checksums_))) {
      LOG_WARN("fill column checksum failed", K(ret), K(res));
    }
  }
  return ret;
}

int ObTabletCreateSSTableParam::init_for_lob_split(const ObTabletID &new_tablet_id,
                                                   const ObITable::TableKey &table_key,
                                                   const blocksstable::ObSSTableBasicMeta &basic_meta,
                                                   const compaction::ObMergeType &merge_type,
                                                   const int64_t schema_version,
                                                   const int64_t dst_major_snapshot_version,
                                                   const int64_t uncommitted_tx_id,
                                                   const int64_t sstable_logic_seq,
                                                   const blocksstable::ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  set_init_value_for_column_store_();
  table_key_ = table_key;
  table_key_.tablet_id_ = new_tablet_id;
  if (is_major_merge(merge_type)) {
    table_key_.version_range_.snapshot_version_ = dst_major_snapshot_version;
  }
  uncommitted_tx_id_ = uncommitted_tx_id;
  schema_version_ = schema_version;

  // init from basic_meta
  table_mode_ = basic_meta.table_mode_;
  index_type_ = static_cast<share::schema::ObIndexType>(basic_meta.index_type_);
  rowkey_column_cnt_ = basic_meta.rowkey_column_count_;

  create_snapshot_version_ = is_major_merge(merge_type) ? dst_major_snapshot_version : basic_meta.create_snapshot_version_;

  sstable_logic_seq_ = sstable_logic_seq;
  filled_tx_scn_ = basic_meta.filled_tx_scn_;
  latest_row_store_type_ = basic_meta.latest_row_store_type_;
  recycle_version_ = basic_meta.recycle_version_;
  ddl_scn_ = basic_meta.ddl_scn_;
  progressive_merge_round_ = basic_meta.progressive_merge_round_;
  progressive_merge_step_ = basic_meta.progressive_merge_step_;
  co_base_snapshot_version_ = basic_meta.co_base_snapshot_version_;

  ddl_scn_.set_min();

  // init from merge_res
  column_cnt_ = res.data_column_cnt_;
  max_merged_trans_version_ = res.max_merged_trans_version_;
  nested_size_ = res.nested_size_;
  nested_offset_ = res.nested_offset_;
  full_column_cnt_ = 0;
  tx_data_recycle_scn_.set_min();
  column_group_cnt_ = 1;

  if (OB_FAIL(inner_init_with_merge_res(res))) {
    LOG_WARN("fail to inner init with merge res", K(ret), K(res));
  }
  if (OB_SUCC(ret) && table_key.is_major_sstable()) {
    if (OB_FAIL(column_checksums_.assign(res.data_column_checksums_))) {
      LOG_WARN("fill column checksum failed", K(ret), K(res));
    }
  }
  return ret;
}

int ObTabletCreateSSTableParam::init_for_ha(
    const blocksstable::ObMigrationSSTableParam &sstable_param,
    const blocksstable::ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  set_init_value_for_column_store_();

  table_key_ = sstable_param.table_key_;
  sstable_logic_seq_ = sstable_param.basic_meta_.sstable_logic_seq_;
  schema_version_ = sstable_param.basic_meta_.schema_version_;
  table_mode_ = sstable_param.basic_meta_.table_mode_;
  index_type_ = static_cast<share::schema::ObIndexType>(sstable_param.basic_meta_.index_type_);
  create_snapshot_version_ = sstable_param.basic_meta_.create_snapshot_version_;
  progressive_merge_round_ = sstable_param.basic_meta_.progressive_merge_round_;
  progressive_merge_step_ = sstable_param.basic_meta_.progressive_merge_step_;
  latest_row_store_type_ = sstable_param.basic_meta_.latest_row_store_type_;
  column_cnt_ = res.data_column_cnt_;
  nested_size_ = res.nested_size_;
  nested_offset_ = res.nested_offset_;
  max_merged_trans_version_ = res.max_merged_trans_version_;
  rowkey_column_cnt_ = sstable_param.basic_meta_.rowkey_column_count_;
  ddl_scn_ = sstable_param.basic_meta_.ddl_scn_;
  table_shared_flag_ = sstable_param.basic_meta_.table_shared_flag_;
  filled_tx_scn_ = sstable_param.basic_meta_.filled_tx_scn_;
  tx_data_recycle_scn_ = sstable_param.basic_meta_.tx_data_recycle_scn_;
  co_base_snapshot_version_ = sstable_param.basic_meta_.co_base_snapshot_version_;
  if (table_key_.is_co_sstable()) {
    column_group_cnt_ = sstable_param.column_group_cnt_;
    full_column_cnt_ = sstable_param.full_column_cnt_;
    co_base_type_ = sstable_param.co_base_type_;
    is_co_table_without_cgs_ = sstable_param.is_empty_cg_sstables_;
  }
  recycle_version_ = sstable_param.basic_meta_.recycle_version_;
  if (OB_FAIL(inner_init_with_merge_res(res))) {
    LOG_WARN("fail to inner init with merge res", K(ret), K(res));
  } else if (OB_FAIL(column_checksums_.assign(sstable_param.column_checksums_))) {
    LOG_WARN("fail to fill column checksum", K(ret), K(sstable_param));
  } else if (OB_FAIL(blocksstable::ObSSTableMetaCompactUtil::fix_filled_tx_scn_value_for_compact(table_key_, filled_tx_scn_))) {
    LOG_WARN("failed to fix filled tx scn value for compact", K(ret), K(table_key_), K(sstable_param));
  } else {
    root_macro_seq_ = MAX(root_macro_seq_, sstable_param.basic_meta_.root_macro_seq_);
#ifdef OB_BUILD_SHARED_STORAGE
    root_macro_seq_ += oceanbase::compaction::MACRO_STEP_SIZE;
#endif
    if (!is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("init for ha sstable get invalid argument", K(ret), K(sstable_param), KPC(this),
          K(res));
    }
  }
  return ret;
}

int ObTabletCreateSSTableParam::init_for_ha(const blocksstable::ObMigrationSSTableParam &sstable_param)
{
  int ret = OB_SUCCESS;
  set_init_value_for_column_store_();

  table_key_ = sstable_param.table_key_;
  sstable_logic_seq_ = sstable_param.basic_meta_.sstable_logic_seq_;
  schema_version_ = sstable_param.basic_meta_.schema_version_;
  create_snapshot_version_ = sstable_param.basic_meta_.create_snapshot_version_;
  table_mode_ = sstable_param.basic_meta_.table_mode_;
  index_type_ = static_cast<share::schema::ObIndexType>(sstable_param.basic_meta_.index_type_);
  progressive_merge_round_ = sstable_param.basic_meta_.progressive_merge_round_;
  progressive_merge_step_ = sstable_param.basic_meta_.progressive_merge_step_;
  is_ready_for_read_ = true;
  root_row_store_type_ = sstable_param.basic_meta_.root_row_store_type_;
  latest_row_store_type_ = sstable_param.basic_meta_.latest_row_store_type_;
  index_blocks_cnt_ = sstable_param.basic_meta_.index_macro_block_count_;
  data_blocks_cnt_ = sstable_param.basic_meta_.data_macro_block_count_;
  micro_block_cnt_ = sstable_param.basic_meta_.data_micro_block_count_;
  use_old_macro_block_count_ = sstable_param.basic_meta_.use_old_macro_block_count_;
  row_count_ = sstable_param.basic_meta_.row_count_;
  column_cnt_ = sstable_param.basic_meta_.column_cnt_;
  data_checksum_ = sstable_param.basic_meta_.data_checksum_;
  occupy_size_ = sstable_param.basic_meta_.occupy_size_;
  original_size_ = sstable_param.basic_meta_.original_size_;
  max_merged_trans_version_ = sstable_param.basic_meta_.max_merged_trans_version_;
  ddl_scn_ = sstable_param.basic_meta_.ddl_scn_;
  filled_tx_scn_ = sstable_param.basic_meta_.filled_tx_scn_;
  tx_data_recycle_scn_ = sstable_param.basic_meta_.tx_data_recycle_scn_;
  contain_uncommitted_row_ = sstable_param.basic_meta_.contain_uncommitted_row_;
  compressor_type_ = sstable_param.basic_meta_.compressor_type_;
  encrypt_id_ = sstable_param.basic_meta_.encrypt_id_;
  master_key_id_ = sstable_param.basic_meta_.master_key_id_;
  rowkey_column_cnt_ = sstable_param.basic_meta_.rowkey_column_count_;
  root_macro_seq_ = sstable_param.basic_meta_.root_macro_seq_;
  MEMCPY(encrypt_key_, sstable_param.basic_meta_.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  table_backup_flag_ = sstable_param.basic_meta_.table_backup_flag_;
  table_shared_flag_ = sstable_param.basic_meta_.table_shared_flag_;
  is_meta_root_ = sstable_param.is_meta_root_;
  co_base_snapshot_version_ = sstable_param.basic_meta_.co_base_snapshot_version_;
  root_block_addr_.set_none_addr();
  data_block_macro_meta_addr_.set_none_addr();
  if (table_key_.is_co_sstable()) {
    column_group_cnt_ = sstable_param.column_group_cnt_;
    is_co_table_without_cgs_ = sstable_param.is_empty_cg_sstables_;
    full_column_cnt_ = sstable_param.full_column_cnt_;
    co_base_type_ = sstable_param.co_base_type_;
  }
  data_index_tree_height_ = sstable_param.basic_meta_.data_index_tree_height_;
  recycle_version_ = sstable_param.basic_meta_.recycle_version_;
  nested_offset_ = 0;
  nested_size_ = 0;
  if (OB_FAIL(column_checksums_.assign(sstable_param.column_checksums_))) {
    LOG_WARN("fail to assign column checksums", K(ret), K(sstable_param));
  } else if (OB_FAIL(blocksstable::ObSSTableMetaCompactUtil::fix_filled_tx_scn_value_for_compact(table_key_, filled_tx_scn_))) {
    LOG_WARN("failed to fix filled tx scn value for compact", K(ret), K(table_key_), K(sstable_param));
  } else if (sstable_param.is_shared_sstable() && OB_FAIL(inner_init_with_shared_sstable(sstable_param))) {
    LOG_WARN("failed to inner init with shared sstable", K(ret), K(sstable_param));
  } else if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init for ha sstable get invalid argument", K(ret), K(sstable_param), KPC(this));
  }
  return ret;
}

int ObTabletCreateSSTableParam::init_for_remote(const blocksstable::ObMigrationSSTableParam &sstable_param)
{
  int ret = OB_SUCCESS;
  set_init_value_for_column_store_();

  table_key_ = sstable_param.table_key_;
  sstable_logic_seq_ = sstable_param.basic_meta_.sstable_logic_seq_;
  schema_version_ = sstable_param.basic_meta_.schema_version_;
  table_mode_ = sstable_param.basic_meta_.table_mode_;
  index_type_ = static_cast<share::schema::ObIndexType>(sstable_param.basic_meta_.index_type_);
  create_snapshot_version_ = sstable_param.basic_meta_.create_snapshot_version_;
  progressive_merge_round_ = sstable_param.basic_meta_.progressive_merge_round_;
  progressive_merge_step_ = sstable_param.basic_meta_.progressive_merge_step_;
  latest_row_store_type_ = sstable_param.basic_meta_.latest_row_store_type_;

  root_block_addr_ = sstable_param.root_block_addr_;
  root_block_data_.buf_ = sstable_param.root_block_buf_;
  root_block_data_.size_ = sstable_param.root_block_addr_.size();
  data_block_macro_meta_addr_ = sstable_param.data_block_macro_meta_addr_;
  data_block_macro_meta_.buf_ = sstable_param.data_block_macro_meta_buf_;
  data_block_macro_meta_.size_ = sstable_param.data_block_macro_meta_addr_.size();

  is_meta_root_ = sstable_param.is_meta_root_;
  root_row_store_type_ = sstable_param.basic_meta_.root_row_store_type_;
  data_index_tree_height_ = sstable_param.basic_meta_.data_index_tree_height_;
  index_blocks_cnt_ = sstable_param.basic_meta_.index_macro_block_count_;
  data_blocks_cnt_ = sstable_param.basic_meta_.data_macro_block_count_;
  micro_block_cnt_ = sstable_param.basic_meta_.data_micro_block_count_;
  use_old_macro_block_count_ = sstable_param.basic_meta_.use_old_macro_block_count_;
  row_count_ = sstable_param.basic_meta_.row_count_;
  column_cnt_ = sstable_param.basic_meta_.column_cnt_;
  data_checksum_ = sstable_param.basic_meta_.data_checksum_;
  occupy_size_ = sstable_param.basic_meta_.occupy_size_;
  original_size_ = sstable_param.basic_meta_.original_size_;
  max_merged_trans_version_ = sstable_param.basic_meta_.max_merged_trans_version_;
  contain_uncommitted_row_ = sstable_param.basic_meta_.contain_uncommitted_row_;
  compressor_type_ = sstable_param.basic_meta_.compressor_type_;
  encrypt_id_ = sstable_param.basic_meta_.encrypt_id_;
  master_key_id_ = sstable_param.basic_meta_.master_key_id_;
  root_macro_seq_ = sstable_param.basic_meta_.root_macro_seq_;
  rowkey_column_cnt_ = sstable_param.basic_meta_.rowkey_column_count_;
  ddl_scn_ = sstable_param.basic_meta_.ddl_scn_;
  table_backup_flag_ = sstable_param.basic_meta_.table_backup_flag_;
  table_backup_flag_.set_has_backup();
  table_backup_flag_.set_no_local();
  table_shared_flag_ = sstable_param.basic_meta_.table_shared_flag_;
  filled_tx_scn_ = sstable_param.basic_meta_.filled_tx_scn_;
  tx_data_recycle_scn_ = sstable_param.basic_meta_.tx_data_recycle_scn_;
  co_base_snapshot_version_ = sstable_param.basic_meta_.co_base_snapshot_version_;
  if (table_key_.is_co_sstable()) {
    column_group_cnt_ = sstable_param.column_group_cnt_;
    full_column_cnt_ = sstable_param.full_column_cnt_;
    co_base_type_ = sstable_param.co_base_type_;
    is_co_table_without_cgs_ = sstable_param.is_empty_cg_sstables_;
  }
  recycle_version_ = sstable_param.basic_meta_.recycle_version_;
  nested_offset_ = 0;
  nested_size_ = 0;
  MEMCPY(encrypt_key_, sstable_param.basic_meta_.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  if (OB_FAIL(column_checksums_.assign(sstable_param.column_checksums_))) {
    LOG_WARN("fail to fill column checksum", K(ret), K(sstable_param));
  } else if (OB_FAIL(blocksstable::ObSSTableMetaCompactUtil::fix_filled_tx_scn_value_for_compact(table_key_, filled_tx_scn_))) {
    LOG_WARN("failed to fix filled tx scn value for compact", K(ret), K(table_key_), K(sstable_param));
  } else if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init for remote sstable get invalid argument", K(ret), K(sstable_param), KPC(this));
  }
  return ret;
}

int ObTabletCreateSSTableParam::init_for_mds(
    const compaction::ObBasicTabletMergeCtx &ctx,
    const blocksstable::ObSSTableMergeRes &res,
    const ObStorageSchema &mds_schema)
{
  // TODO: @baichangmin.bcm check ctx valid for mds
  // reference to merge info
  int ret = OB_SUCCESS;
  set_init_value_for_column_store_();
  const compaction::ObStaticMergeParam &static_param = ctx.static_param_;

  ObITable::TableKey table_key;
  table_key.table_type_ = ctx.get_merged_table_type(nullptr, false);
  table_key.tablet_id_ = ctx.get_tablet_id();
  table_key.column_group_idx_ = 0;
  table_key.scn_range_ = static_param.scn_range_;

  table_key_ = table_key;
  sstable_logic_seq_ = static_param.sstable_logic_seq_;
  filled_tx_scn_ = ctx.get_merge_scn();


  table_mode_ = mds_schema.get_table_mode_struct();
  index_type_ = mds_schema.get_index_type();
  column_group_cnt_ = 1; // for row store;
  rowkey_column_cnt_ = mds_schema.get_rowkey_column_num()
      + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  latest_row_store_type_ = mds_schema.get_row_store_type();
  recycle_version_ = 0;
  schema_version_ = mds_schema.get_schema_version();
  create_snapshot_version_ = static_param.create_snapshot_version_;
  progressive_merge_round_ = 0;
  progressive_merge_step_ = 0;
  full_column_cnt_ = 0;

  column_cnt_ = res.data_column_cnt_;
  co_base_snapshot_version_ = 0;
  if (0 == res.row_count_ && 0 == res.max_merged_trans_version_) {
    // empty mini table merged forcely
    max_merged_trans_version_ = static_param.version_range_.snapshot_version_;
  } else {
    max_merged_trans_version_ = res.max_merged_trans_version_;
  }
  nested_size_ = res.nested_size_;
  nested_offset_ = res.nested_offset_;
  ddl_scn_.set_min();
  table_shared_flag_.reset();
  tx_data_recycle_scn_.set_min();

  if (OB_FAIL(inner_init_with_merge_res(res))) {
    LOG_WARN("fail to init with merge res", K(ret), K(res.data_block_ids_));
  } else if (is_major_or_meta_merge_type(static_param.get_merge_type())) {
    if (OB_FAIL(column_checksums_.assign(res.data_column_checksums_))) {
      LOG_WARN("fail to fill column checksum", K(ret), K(res.data_column_checksums_));
    }
  }

  if (OB_SUCC(ret)) {
    if (!is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("init for mds sstable get invalid argument", K(ret), K(res), K(ctx), KPC(this));
    }
  }
  return ret;
}

int ObTabletCreateSSTableParam::inner_init_with_shared_sstable(const blocksstable::ObMigrationSSTableParam &sstable_param)
{
  int ret = OB_SUCCESS;
  if (sstable_param.root_block_addr_.is_valid()) {
    root_block_addr_ = sstable_param.root_block_addr_;
    root_block_data_.buf_ = sstable_param.root_block_buf_;
    root_block_data_.size_ = sstable_param.root_block_addr_.size();
  }

  if (sstable_param.data_block_macro_meta_addr_.is_valid()) {
    data_block_macro_meta_addr_ = sstable_param.data_block_macro_meta_addr_;
    data_block_macro_meta_.buf_ = sstable_param.data_block_macro_meta_buf_;
    data_block_macro_meta_.size_ = sstable_param.data_block_macro_meta_addr_.size();
  }

  root_row_store_type_ = sstable_param.basic_meta_.root_row_store_type_;
  data_index_tree_height_ = sstable_param.basic_meta_.data_index_tree_height_;
  index_blocks_cnt_ = sstable_param.basic_meta_.index_macro_block_count_;
  data_blocks_cnt_ = sstable_param.basic_meta_.data_macro_block_count_;
  micro_block_cnt_ = sstable_param.basic_meta_.data_micro_block_count_;
  use_old_macro_block_count_ = sstable_param.basic_meta_.use_old_macro_block_count_;
  row_count_ = sstable_param.basic_meta_.row_count_;
  data_checksum_ = sstable_param.basic_meta_.data_checksum_;
  occupy_size_ = sstable_param.basic_meta_.occupy_size_;
  original_size_ = sstable_param.basic_meta_.original_size_;
  contain_uncommitted_row_ = sstable_param.basic_meta_.contain_uncommitted_row_;
  compressor_type_ = sstable_param.basic_meta_.compressor_type_;
  encrypt_id_ = sstable_param.basic_meta_.encrypt_id_;
  master_key_id_ = sstable_param.basic_meta_.master_key_id_;
  is_meta_root_ = sstable_param.is_meta_root_;
  root_macro_seq_ = sstable_param.basic_meta_.root_macro_seq_;
  STATIC_ASSERT(ARRAYSIZEOF(encrypt_key_) == share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH,
  "ObTabletCreateSSTableParam encrypt_key_ array size mismatch OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH");
  STATIC_ASSERT(ARRAYSIZEOF(sstable_param.basic_meta_.encrypt_key_) == share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH,
  "ObSSTableMergeRes encrypt_key_ array size mismatch OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH");
  MEMCPY(encrypt_key_, sstable_param.basic_meta_.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  table_backup_flag_ = sstable_param.basic_meta_.table_backup_flag_;
  co_base_snapshot_version_ = sstable_param.basic_meta_.co_base_snapshot_version_;
  return ret;
}

void ObTabletCreateSSTableParam::set_init_value_for_column_store_()
{
  column_group_cnt_ = 1;
  co_base_type_ = ObCOSSTableBaseType::INVALID_TYPE;
  full_column_cnt_ = 0;
  is_co_table_without_cgs_ = false;
  co_base_snapshot_version_ = 0;
}

} // namespace storage
} // namespace oceanbase
