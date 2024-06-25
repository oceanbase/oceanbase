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

#include "storage/tablet/ob_tablet_create_sstable_param.h"

#include "lib/oblog/ob_log_module.h"
#include "share/schema/ob_table_schema.h"
#include "storage/ob_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"
#include "storage/compaction/ob_basic_tablet_merge_ctx.h"
#include "storage/ddl/ob_direct_load_struct.h"

namespace oceanbase
{
using namespace share;
namespace storage
{
// if you add membership for this param, plz check all paths that use it, including
// but not limited to unittest, merge, ddl, shared_macro_block
ObTabletCreateSSTableParam::ObTabletCreateSSTableParam()
  : table_key_(),
    sstable_logic_seq_(0),
    schema_version_(-1),
    create_snapshot_version_(0),
    progressive_merge_round_(0),
    progressive_merge_step_(0),
    is_ready_for_read_(true),
    table_mode_(),
    index_type_(share::schema::ObIndexType::INDEX_TYPE_MAX),
    root_block_addr_(),
    root_block_data_(),
    root_row_store_type_(common::ObRowStoreType::MAX_ROW_STORE),
    latest_row_store_type_(common::ObRowStoreType::MAX_ROW_STORE),
    data_index_tree_height_(0),
    data_block_macro_meta_addr_(),
    data_block_macro_meta_(),
    index_blocks_cnt_(0),
    data_blocks_cnt_(0),
    micro_block_cnt_(0),
    use_old_macro_block_count_(0),
    row_count_(0),
    column_group_cnt_(1),
    co_base_type_(ObCOSSTableBaseType::INVALID_TYPE),
    rowkey_column_cnt_(0),
    column_cnt_(0),
    full_column_cnt_(0),
    data_checksum_(0),
    occupy_size_(0),
    original_size_(0),
    max_merged_trans_version_(0),
    ddl_scn_(SCN::min_scn()),
    filled_tx_scn_(SCN::min_scn()),
    is_co_table_without_cgs_(false),
    contain_uncommitted_row_(false),
    is_meta_root_(false),
    compressor_type_(ObCompressorType::INVALID_COMPRESSOR),
    encrypt_id_(0),
    master_key_id_(0),
    recycle_version_(0),
    nested_offset_(0),
    nested_size_(0),
    data_block_ids_(),
    other_block_ids_(),
    uncommitted_tx_id_(0)
{
  MEMSET(encrypt_key_, 0, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
}

bool ObTabletCreateSSTableParam::is_valid() const
{
  bool ret = true;
  if (OB_UNLIKELY(!table_key_.is_valid())) {
    ret = false;
    LOG_WARN("invalid table key", K(table_key_));
  } else if (OB_UNLIKELY(!table_mode_.is_valid())) {
    ret = false;
    LOG_WARN("invalid table mode", K(table_mode_));
  } else if (!(schema_version_ >= 0
               && sstable_logic_seq_ >= 0
               && create_snapshot_version_ >= 0
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
               && original_size_ >= 0
               && recycle_version_ >= 0)) {
    ret = false;
    LOG_WARN("invalid basic params", K(schema_version_), K_(sstable_logic_seq), K(create_snapshot_version_), K(index_type_),
             K(root_row_store_type_), K_(latest_row_store_type), K(data_index_tree_height_), K(index_blocks_cnt_),
             K(data_blocks_cnt_), K(micro_block_cnt_), K(use_old_macro_block_count_),
             K(row_count_), K(column_group_cnt_), K(rowkey_column_cnt_), K(column_cnt_), K(occupy_size_),
             K(original_size_), K(ddl_scn_), K(filled_tx_scn_), K_(recycle_version));
  } else if (ObITable::is_ddl_sstable(table_key_.table_type_)) {
    // ddl sstable can have invalid meta addr, so skip following ifs
    if (!ddl_scn_.is_valid_and_not_min()) {
      ret = false;
      LOG_WARN("ddl log ts is invalid", K(ddl_scn_), K(table_key_));
    }
  } else if (!is_block_meta_valid(root_block_addr_, root_block_data_)) {
    ret = false;
    LOG_WARN("invalid root meta", K(root_block_addr_), K(root_block_data_));
  } else if (!is_block_meta_valid(data_block_macro_meta_addr_, data_block_macro_meta_)) {
    ret = false;
    LOG_WARN("invalid data meta", K(data_block_macro_meta_addr_), K(data_block_macro_meta_));
  }
  return ret;
}

bool ObTabletCreateSSTableParam::is_block_meta_valid(const storage::ObMetaDiskAddr &addr,
                                                     const blocksstable::ObMicroBlockData &data) const
{
  return addr.is_valid() && (!addr.is_memory() || (data.is_valid() && data.size_ == addr.size()));
}

int ObTabletCreateSSTableParam::inner_init_with_merge_res(const blocksstable::ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  ObSSTableMergeRes::fill_addr_and_data(res.root_desc_,
      root_block_addr_, root_block_data_);
  ObSSTableMergeRes::fill_addr_and_data(res.data_root_desc_,
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
  STATIC_ASSERT(ARRAYSIZEOF(encrypt_key_) == share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH,
  "ObTabletCreateSSTableParam encrypt_key_ array size mismatch OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH");
  STATIC_ASSERT(ARRAYSIZEOF(res.encrypt_key_) == share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH,
  "ObSSTableMergeRes encrypt_key_ array size mismatch OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH");
  MEMCPY(encrypt_key_, res.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);

  if (OB_FAIL(data_block_ids_.assign(res.data_block_ids_))) {
    LOG_WARN("fail to fill data block ids", K(ret), K(res.data_block_ids_));
  } else if (OB_FAIL(other_block_ids_.assign(res.other_block_ids_))) {
    LOG_WARN("fail to fill other block ids", K(ret), K(res.other_block_ids_));
  }
  return ret;
}

int ObTabletCreateSSTableParam::init_for_small_sstable(const blocksstable::ObSSTableMergeRes &res,
                                                       const ObITable::TableKey &table_key,
                                                       const blocksstable::ObSSTableMeta &sstable_meta,
                                                       const blocksstable::ObBlockInfo &block_info)
{
  int ret = OB_SUCCESS;
  const blocksstable::ObSSTableBasicMeta &basic_meta = sstable_meta.get_basic_meta();
  filled_tx_scn_ = basic_meta.filled_tx_scn_;
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
  is_ready_for_read_ = true;
  column_cnt_ = res.data_column_cnt_;
  max_merged_trans_version_ = res.max_merged_trans_version_;
  nested_offset_ = block_info.nested_offset_;
  nested_size_ = block_info.nested_size_;
  if (OB_FAIL(inner_init_with_merge_res(res))) {
    LOG_WARN("fail to inner init with merge res", K(ret), K(res));
  } else if (table_key_.is_major_sstable()) {
    if (OB_FAIL(column_checksums_.assign(res.data_column_checksums_))) {
      LOG_WARN("fail to fill column checksum", K(ret), K(res.data_column_checksums_));
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
    schema_version_ = static_param.schema_version_;
    create_snapshot_version_ = static_param.create_snapshot_version_;
    progressive_merge_round_ = static_param.progressive_merge_round_;
    progressive_merge_step_ = std::min(
            static_param.progressive_merge_num_, static_param.progressive_merge_step_ + 1);
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

    if (OB_FAIL(inner_init_with_merge_res(res))) {
      LOG_WARN("fail to init with merge res", K(ret), K(res.data_block_ids_));
    } else if (is_major_or_meta_merge_type(static_param.get_merge_type())) {
      if (OB_FAIL(column_checksums_.assign(res.data_column_checksums_))) {
        LOG_WARN("fail to fill column checksum", K(ret), K(res.data_column_checksums_));
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
                                             const int64_t create_schema_version_on_tablet)
{
  int ret = OB_SUCCESS;
  SMART_VAR(blocksstable::ObSSTableMergeRes, res) {
    int64_t column_count = 0;
    int64_t full_column_cnt = 0; // only used for co sstable
    share::schema::ObTableMode table_mode = storage_schema.get_table_mode_struct();
    share::schema::ObIndexType index_type = storage_schema.get_index_type();
    int64_t rowkey_column_cnt = storage_schema.get_rowkey_column_num() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    common::ObRowStoreType row_store_type = storage_schema.get_row_store_type();
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

      if (OB_FAIL(inner_init_with_merge_res(res))) {
        LOG_WARN("fail to inner init with merge res", K(ret), K(res));
      } else if (ddl_param.table_key_.is_co_sstable()) {
        column_group_cnt_ = storage_schema.get_column_group_count();
        // only set true when build empty major sstable. ddl co sstable must set false and fill cg sstables
        is_co_table_without_cgs_ = ddl_param.table_key_.is_major_sstable() && 0 == data_blocks_cnt_;
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
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(column_checksums_.assign(res.data_column_checksums_))) {
        LOG_WARN("fail to fill column checksum for empty major", K(ret), K(res.data_column_checksums_));
      } else if (OB_UNLIKELY(column_checksums_.count() != column_count)) {
        // we have corrected the col_default_checksum_array_ in prepare_index_data_desc
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column checksums", K(ret), K(column_count), KPC(this));
      }
    }
  }
  return ret;
}

int ObTabletCreateSSTableParam::init_for_ha(
    const blocksstable::ObMigrationSSTableParam &sstable_param,
    const blocksstable::ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;

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
  if (OB_FAIL(inner_init_with_merge_res(res))) {
    LOG_WARN("fail to inner init with merge res", K(ret), K(res));
  } else if (OB_FAIL(column_checksums_.assign(sstable_param.column_checksums_))) {
    LOG_WARN("fail to fill column checksum", K(ret), K(sstable_param));
  }

  return ret;
}

int ObTabletCreateSSTableParam::init_for_ha(const blocksstable::ObMigrationSSTableParam &sstable_param)
{
  int ret = OB_SUCCESS;
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
  contain_uncommitted_row_ = sstable_param.basic_meta_.contain_uncommitted_row_;
  compressor_type_ = sstable_param.basic_meta_.compressor_type_;
  encrypt_id_ = sstable_param.basic_meta_.encrypt_id_;
  master_key_id_ = sstable_param.basic_meta_.master_key_id_;
  root_block_addr_.set_none_addr();
  data_block_macro_meta_addr_.set_none_addr();
  rowkey_column_cnt_ = sstable_param.basic_meta_.rowkey_column_count_;
  MEMCPY(encrypt_key_, sstable_param.basic_meta_.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  if (OB_FAIL(column_checksums_.assign(sstable_param.column_checksums_))) {
    LOG_WARN("fail to assign column checksums", K(ret), K(sstable_param));
  }

  return ret;
}

int ObTabletCreateSSTableParam::init_for_mds(
    const compaction::ObBasicTabletMergeCtx &ctx,
    const blocksstable::ObSSTableMergeRes &res,
    const ObStorageSchema &mds_schema)
{
  // TODO: @luhaopeng.lhp check ctx valid for mds
  // reference to merge info
  int ret = OB_SUCCESS;
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
  progressive_merge_round_ = static_param.progressive_merge_round_;
  progressive_merge_step_ = std::min(
          static_param.progressive_merge_num_, static_param.progressive_merge_step_ + 1);

  column_cnt_ = res.data_column_cnt_;
  if (0 == res.row_count_ && 0 == res.max_merged_trans_version_) {
    // empty mini table merged forcely
    max_merged_trans_version_ = static_param.version_range_.snapshot_version_;
  } else {
    max_merged_trans_version_ = res.max_merged_trans_version_;
  }
  nested_size_ = res.nested_size_;
  nested_offset_ = res.nested_offset_;
  ddl_scn_.set_min();

  if (OB_FAIL(inner_init_with_merge_res(res))) {
    LOG_WARN("fail to init with merge res", K(ret), K(res.data_block_ids_));
  } else if (is_major_or_meta_merge_type(static_param.get_merge_type())) {
    if (OB_FAIL(column_checksums_.assign(res.data_column_checksums_))) {
      LOG_WARN("fail to fill column checksum", K(ret), K(res.data_column_checksums_));
    }
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase
