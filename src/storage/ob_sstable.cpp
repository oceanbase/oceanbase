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
#include "ob_sstable.h"
#include <math.h>
#include "lib/objectpool/ob_resource_pool.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/container/ob_bit_set.h"
#include "lib/container/ob_fixed_array_iterator.h"
#include "share/stat/ob_table_stat.h"
#include "sql/ob_sql_define.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "blocksstable/ob_macro_block_writer.h"
#include "blocksstable/ob_block_cache_working_set.h"
#include "ob_partition_scheduler.h"
#include "ob_warm_up_request.h"
#include "ob_multiple_merge.h"
#include "storage/ob_sstable_row_exister.h"
#include "storage/ob_sstable_row_multi_exister.h"
#include "storage/ob_sstable_row_getter.h"
#include "storage/ob_sstable_row_multi_getter.h"
#include "storage/ob_sstable_row_scanner.h"
#include "storage/ob_sstable_row_multi_scanner.h"
#include "storage/ob_sstable_multi_version_row_iterator.h"
#include "storage/ob_sstable_row_whole_scanner.h"
#include "storage/ob_sstable_estimator.h"
#include "storage/ob_table_mgr.h"
#include "storage/ob_partition_split_task.h"
#include "storage/ob_partition_meta_redo_module.h"
#include "storage/ob_tenant_config_mgr.h"
#include "storage/ob_sstable_row_lock_checker.h"
#include "storage/ob_macro_meta_replay_map.h"
#include "storage/ob_file_system_util.h"
#include "storage/compaction/ob_partition_merge_util.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;
using namespace share;
using namespace share::schema;
using namespace sql;
namespace storage {

ObSSTable::ObSSTableGroupMacroBlocks::ObSSTableGroupMacroBlocks(
    const MacroBlockArray& data_macro_blocks, const MacroBlockArray& lob_macro_blocks)
    : data_macro_blocks_(data_macro_blocks), lob_macro_blocks_(lob_macro_blocks)
{}

ObSSTable::ObSSTableGroupMacroBlocks::~ObSSTableGroupMacroBlocks()
{}

const MacroBlockId& ObSSTable::ObSSTableGroupMacroBlocks::at(const int64_t idx) const
{
  if (OB_UNLIKELY(0 > idx || count() <= idx)) {
    STORAGE_LOG(ERROR, "SSTable get macro blocks out of range", K(idx));
  }
  if (idx < data_macro_block_count()) {
    return data_macro_blocks_.at(idx);
  } else {
    return lob_macro_blocks_.at(idx - data_macro_block_count());
  }
}

int ObSSTable::ObSSTableGroupMacroBlocks::at(const int64_t idx, MacroBlockId& obj) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > idx || count() <= idx)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    STORAGE_LOG(ERROR, "SSTable get macro blocks out of range", K(idx));
  } else if (idx < data_macro_block_count()) {
    ret = data_macro_blocks_.at(idx, obj);
  } else {
    ret = lob_macro_blocks_.at(idx - data_macro_block_count(), obj);
  }
  return ret;
}

int64_t ObSSTable::ObSSTableGroupMacroBlocks::to_string(char* buf, int64_t buf_len) const
{
  int64_t pos = 0;

  J_NAME("data_blocks");
  J_COLON();
  pos += data_macro_blocks_.to_string(buf + pos, buf_len - pos);
  J_NAME("lob_blocks");
  J_COLON();
  pos += lob_macro_blocks_.to_string(buf + pos, buf_len - pos);

  return pos;
}

ObSSTable::ObSSTable()
    : allocator_(ObModIds::OB_SSTABLE, DEFAULT_ALLOCATOR_BLOCK_SIZE),
      map_allocator_(),
      meta_(allocator_),
      rowkey_helper_(),
      sstable_merge_info_(),
      status_(SSTABLE_NOT_INIT),
      total_macro_blocks_(meta_.macro_block_array_, meta_.lob_macro_block_array_),
      exist_invalid_collation_free_meta_(false),
      file_handle_(),
      pt_replay_module_(nullptr),
      dump_memtable_timestamp_(0)
{}

ObSSTable::~ObSSTable()
{
  destroy();
}

int ObSSTable::init(const ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;

  if (SSTABLE_NOT_INIT != status_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable is not in close status.", K(ret), K_(status));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", K(table_key), K(ret));
  } else if (OB_FAIL(ObITable::init(table_key))) {
    TRANS_LOG(WARN, "failed to set_table_key", K(ret), K(table_key));
  } else if (OB_FAIL(map_allocator_.init(DEFAULT_ALLOCATOR_BLOCK_SIZE, lib::ObLabel("SstableMetaMap")))) {
    LOG_WARN("fail to init map allocator", K(ret));
  } else {
    meta_.reset();
    meta_.total_sstable_count_ = 0;
    status_ = SSTABLE_INIT;
    allocator_.set_attr(ObMemAttr(
        extract_tenant_id(table_key.table_id_), lib::ObLabel("SstableMeta"), ObCtxIds::STORAGE_SHORT_TERM_META_CTX_ID));
  }
  return ret;
}

int ObSSTable::replay_set_table_key(const ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;

  if (key_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "cannot set table key twice.", K(ret), K_(key));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", K(table_key), K(ret));
  } else if (OB_FAIL(ObITable::init(table_key))) {
    TRANS_LOG(WARN, "failed to set_table_key", K(ret), K(table_key));
  }
  return ret;
}

int ObSSTable::open(const ObCreateSSTableParamWithTable& param)
{
  int ret = OB_SUCCESS;
  ObCreateSSTableParamWithPartition local_param;
  ObCreatePartitionMeta schema;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid sstable input param.", K(ret), K(param));
  } else if (SSTABLE_INIT != status_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not initialized.", K(ret), K(param), K_(status));
  } else if (OB_FAIL(local_param.extract_from(param, schema))) {
    STORAGE_LOG(WARN, "failed to extract partition param", K(param), K(ret));
  } else {
    ret = open(local_param);
  }
  return ret;
}

int ObSSTable::open(const ObCreateSSTableParamWithPartition& param)
{
  int ret = OB_SUCCESS;
  ObSSTableColumnMeta col_meta;
  const ObCreatePartitionMeta* schema = param.schema_;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid sstable input param.", K(ret), K(param));
  } else if (SSTABLE_INIT != status_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not initialized.", K(ret), K_(status));
  } else if (key_.table_id_ != schema->get_table_id() || key_.version_ != param.table_key_.version_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR,
        "table_id or version not match",
        K(ret),
        K(key_),
        "schema_table_id",
        schema->get_table_id(),
        "data_version",
        param.table_key_.version_);
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.file_ctx_))) {
    LOG_WARN("failed to init file ctx", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.lob_file_ctx_))) {
    LOG_WARN("Failed to init lob file ctx", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.bloom_filter_file_ctx_))) {
    LOG_WARN("Failed to init bloomfilter file ctx", K(ret));
  } else {
    meta_.pg_key_ = param.pg_key_;
    const ObIArray<ObColDesc>& columns = schema->get_store_column_ids();
    meta_.index_id_ = schema->get_table_id();
    meta_.row_count_ = 0;
    meta_.occupy_size_ = 0;
    meta_.data_checksum_ = 0;
    meta_.row_checksum_ = 0;
    meta_.data_version_ = param.table_key_.version_;
    meta_.logical_data_version_ = param.logical_data_version_;
    meta_.rowkey_column_count_ = schema->get_rowkey_column_num();
    meta_.table_type_ = schema->get_table_type();
    meta_.index_type_ = schema->get_index_type();
    meta_.available_version_ = 0;
    meta_.macro_block_count_ = 0;
    meta_.use_old_macro_block_count_ = 0;
    meta_.macro_block_second_index_ = 0;
    meta_.lob_macro_block_count_ = 0;
    meta_.lob_use_old_macro_block_count_ = 0;
    meta_.column_cnt_ = columns.count();
    // meta since 2.1
    meta_.sstable_format_version_ = ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_6;
    set_multi_version_rowkey_type(ObMultiVersionRowkeyHelpper::MVRC_VERSION_AFTER_3_0);
    meta_.bloom_filter_block_id_.reset();
    meta_.bloom_filter_block_id_in_files_ = 0;
    // use frozen schema version instead of schema.get_schema_version(),
    // it's better more schema cache
    meta_.create_index_base_version_ = param.create_index_base_version_;
    meta_.schema_version_ = param.schema_version_;
    meta_.progressive_merge_start_version_ = param.progressive_merge_start_version_;
    meta_.progressive_merge_end_version_ = param.progressive_merge_end_version_;
    meta_.create_snapshot_version_ = param.create_snapshot_version_;
    meta_.checksum_method_ = param.checksum_method_;

    meta_.column_metas_.set_capacity(static_cast<uint32_t>(columns.count()));
    meta_.new_column_metas_.set_capacity(static_cast<uint32_t>(columns.count()));
    meta_.progressive_merge_round_ = param.progressive_merge_round_;
    meta_.progressive_merge_step_ = param.progressive_merge_step_;
    meta_.table_mode_ = schema->get_table_mode();
    meta_.has_compact_row_ = param.has_compact_row_;
    meta_.max_merged_trans_version_ = 0;
    meta_.contain_uncommitted_row_ = false;
    meta_.upper_trans_version_ = param.table_key_.trans_version_range_.snapshot_version_;

    dump_memtable_timestamp_ = param.dump_memtable_timestamp_;

    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      const uint64_t col_id = columns.at(i).col_id_;
      const ObColumnSchemaV2* col_schema = schema->get_column_schema(col_id);

      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "col_schema must not null", K(ret), K(col_id), K(*schema));
      } else if (!col_schema->is_valid()) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "invalid col schema", K(ret), K(col_schema));
      } else if (!col_schema->is_column_stored_in_sstable() && !schema->is_storage_index_table()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "virtual generated column should be filtered already", K(ret), K(col_id));
      } else {
        if (ob_is_large_text(col_schema->get_data_type())) {
          col_meta.column_default_checksum_ = 0;
        } else {
          col_meta.column_default_checksum_ = CCM_TYPE_AND_VALUE == meta_.checksum_method_
                                                  ? col_schema->get_orig_default_value().checksum(0)
                                                  : col_schema->get_orig_default_value().checksum_v2(0);
        }
        col_meta.column_id_ = col_id;
        col_meta.column_checksum_ = 0;
        if (OB_FAIL(meta_.column_metas_.push_back(col_meta))) {
          STORAGE_LOG(WARN, "Fail to push column meta", K(ret));
        } else {
          if (!ob_is_large_text(col_schema->get_data_type())) {
            col_meta.column_default_checksum_ = col_schema->get_orig_default_value().checksum_v2(0);
          }
          if (OB_FAIL(meta_.new_column_metas_.push_back(col_meta))) {
            STORAGE_LOG(WARN, "fail to push back new column meta");
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (!meta_.is_valid()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("sstable meta is not valid", K(ret), K(meta_), K(*schema));
      } else {
        status_ = SSTABLE_WRITE_BUILDING;
      }
    }
  }
  return ret;
}

int ObSSTable::open(const blocksstable::ObSSTableBaseMeta& meta)
{
  int ret = OB_SUCCESS;
  ObSSTableColumnMeta col_meta;
  if (SSTABLE_INIT != status_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not initialized.", K(ret), K_(status));
  } else if (!meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(meta));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.file_ctx_))) {
    LOG_WARN("Failed to init file ctx", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.lob_file_ctx_))) {
    LOG_WARN("Failed to init lob file ctx", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.bloom_filter_file_ctx_))) {
    LOG_WARN("Failed to init bloomfilter file ctx", K(ret));
  } else {
    meta_.pg_key_ = meta.pg_key_;
    meta_.index_id_ = meta.index_id_;
    meta_.row_count_ = 0;
    meta_.occupy_size_ = 0;
    meta_.data_checksum_ = 0;
    meta_.row_checksum_ = 0;
    meta_.data_version_ = meta.data_version_;
    meta_.rowkey_column_count_ = meta.rowkey_column_count_;
    meta_.table_type_ = meta.table_type_;
    meta_.index_type_ = meta.index_type_;
    meta_.available_version_ = meta.available_version_;
    meta_.macro_block_count_ = 0;
    meta_.use_old_macro_block_count_ = 0;
    meta_.lob_macro_block_count_ = 0;
    meta_.bloom_filter_block_id_.reset();
    meta_.bloom_filter_block_id_in_files_ = 0;
    meta_.column_cnt_ = meta.column_cnt_;
    meta_.sstable_format_version_ = meta.sstable_format_version_;
    meta_.multi_version_rowkey_type_ = meta.multi_version_rowkey_type_;
    if (is_major_sstable()) {
      if (meta_.sstable_format_version_ <= ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_4) {
        meta_.logical_data_version_ = meta_.data_version_;
      } else {
        meta_.logical_data_version_ = meta.logical_data_version_;
      }
    } else {
      if (meta_.sstable_format_version_ < ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_6) {
        meta_.logical_data_version_ = get_snapshot_version();
      } else {
        meta_.logical_data_version_ = meta.logical_data_version_;
      }
    }
    if (meta_.sstable_format_version_ < ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_6) {
      set_multi_version_rowkey_type(ObMultiVersionRowkeyHelpper::MVRC_OLD_VERSION);
      meta_.sstable_format_version_ = ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_6;
    } else if (ObMultiVersionRowkeyHelpper::MVRC_VERSION_MAX == meta_.multi_version_rowkey_type_) {
      set_multi_version_rowkey_type(ObMultiVersionRowkeyHelpper::MVRC_VERSION_AFTER_3_0);
    }
    meta_.max_logic_block_index_ = meta.max_logic_block_index_;
    meta_.build_on_snapshot_ = meta.build_on_snapshot_;
    meta_.create_index_base_version_ = meta.create_index_base_version_;
    meta_.schema_version_ = meta.schema_version_;
    meta_.progressive_merge_start_version_ = meta.progressive_merge_start_version_;
    meta_.progressive_merge_end_version_ = meta.progressive_merge_end_version_;
    meta_.create_snapshot_version_ = meta.create_snapshot_version_;
    meta_.checksum_method_ = meta.checksum_method_;
    meta_.total_sstable_count_ = meta.total_sstable_count_;
    meta_.column_metas_.set_capacity(static_cast<uint32_t>(meta_.column_cnt_));
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_.column_cnt_; ++i) {
      col_meta.column_id_ = meta.column_metas_.at(i).column_id_;
      col_meta.column_default_checksum_ = meta.column_metas_.at(i).column_default_checksum_;
      // for compatibility, if checksum method is CCM_VALUE_ONLY, the column checksum will be calculated when add macro
      // meta
      col_meta.column_checksum_ =
          blocksstable::CCM_TYPE_AND_VALUE == meta_.checksum_method_ ? meta.column_metas_.at(i).column_checksum_ : 0;
      if (OB_FAIL(meta_.column_metas_.push_back(col_meta))) {
        STORAGE_LOG(WARN, "Fail to push column meta, ", K(ret));
      }
    }

    meta_.new_column_metas_.set_capacity(static_cast<uint32_t>(meta.new_column_metas_.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < meta.new_column_metas_.count(); ++i) {
      col_meta.column_id_ = meta.new_column_metas_.at(i).column_id_;
      col_meta.column_default_checksum_ = meta.new_column_metas_.at(i).column_default_checksum_;
      col_meta.column_checksum_ = blocksstable::CCM_TYPE_AND_VALUE == meta_.checksum_method_
                                      ? meta.new_column_metas_.at(i).column_checksum_
                                      : 0;
      if (OB_FAIL(meta_.new_column_metas_.push_back(col_meta))) {
        STORAGE_LOG(WARN, "fail to push back col meta", K(ret));
      }
    }

    meta_.progressive_merge_round_ = meta.progressive_merge_round_;
    meta_.progressive_merge_step_ = meta.progressive_merge_step_;
    meta_.table_mode_ = meta.table_mode_;
    meta_.contain_uncommitted_row_ = meta.contain_uncommitted_row_;
    meta_.max_merged_trans_version_ = meta.max_merged_trans_version_;
    meta_.upper_trans_version_ = meta.upper_trans_version_;
    if (0 == meta.upper_trans_version_) {
      meta_.max_merged_trans_version_ = get_snapshot_version();
      meta_.upper_trans_version_ = get_snapshot_version();
    }
    meta_.has_compact_row_ = meta.has_compact_row_;

    if (OB_SUCC(ret)) {
      status_ = SSTABLE_WRITE_BUILDING;
    }
  }
  return ret;
}

int ObSSTable::convert_array_to_meta_map(const common::ObIArray<blocksstable::MacroBlockId>& block_ids,
    const common::ObIArray<blocksstable::ObFullMacroBlockMeta>& block_metas)
{
  int ret = OB_SUCCESS;
  if (block_ids.count() != block_metas.count()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "invalid arguments", K(ret), K(block_ids), K(block_metas), KP(this), KP(&block_ids), KP(&block_metas));
  } else if (block_ids.count() > 0) {
    ObMacroBlockMetaV2* new_meta = nullptr;
    ObMacroBlockSchemaInfo* new_schema = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < block_ids.count(); ++i) {
      const ObMacroBlockMetaV2* meta = block_metas.at(i).meta_;
      if (OB_ISNULL(meta)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "error sys, block meta must not be null", K(ret));
      } else if (OB_FAIL(meta->deep_copy(new_meta, allocator_))) {
        STORAGE_LOG(WARN, "fail to deep copy macro block meta", K(ret));
      } else if (OB_FAIL(block_meta_map_.set(block_ids.at(i), new_meta))) {
        STORAGE_LOG(WARN, "fail to set macro block meta to map", K(ret), K(block_ids.at(i)));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < block_ids.count(); ++i) {
      const ObMacroBlockSchemaInfo* schema = block_metas.at(i).schema_;
      const ObMacroBlockSchemaInfo* org_schema = nullptr;
      bool need_set = false;
      if (OB_ISNULL(schema)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "error sys, schema must not be null", K(ret));
      } else if (OB_FAIL(schema_map_.get(schema->schema_version_, org_schema))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          need_set = true;
        } else {
          STORAGE_LOG(WARN, "fail to get schema map", K(ret));
        }
      }
      if (OB_SUCC(ret) && need_set) {
        if (OB_FAIL(schema->deep_copy(new_schema, allocator_))) {
          STORAGE_LOG(WARN, "fail to deep copy macro block meta", K(ret));
        } else if (OB_FAIL(schema_map_.set(new_schema->schema_version_, new_schema))) {
          STORAGE_LOG(WARN, "fail to set macro block meta to map", K(ret), K(*new_schema));
        }
      }
    }
  }
  return ret;
}

int ObSSTable::convert_array_to_id_map(const common::ObIArray<blocksstable::MacroBlockId>& block_ids,
    const common::ObIArray<blocksstable::ObFullMacroBlockMeta>& block_metas)
{
  int ret = OB_SUCCESS;
  ObLogicMacroBlockId logic_block_id;

  for (int64_t i = 0; OB_SUCC(ret) && i < block_metas.count(); ++i) {
    const ObMacroBlockMetaV2* meta = block_metas.at(i).meta_;
    if (OB_ISNULL(meta)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "error sys, block meta must not be null", K(ret));
    } else {
      int64_t data_version = 0;
      if (meta->data_version_ > 0) {
        data_version = meta->data_version_;
      } else if (meta->snapshot_version_ > 0 && 0 == meta->data_version_) {
        data_version = meta->snapshot_version_;
      } else {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "Invalid snapshot version and data version of lob macro meta", K(*meta), K(ret));
      }
      if (OB_SUCC(ret)) {
        logic_block_id.data_version_ = data_version;
        logic_block_id.data_seq_ = meta->data_seq_;
        if (OB_FAIL(logic_block_id_map_.set(logic_block_id, block_ids.at(i)))) {
          STORAGE_LOG(WARN, "fail to set macro block to logic block id map", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSSTable::build_block_meta_map()
{
  int ret = OB_SUCCESS;
  const int64_t total_macro_count =
      macro_meta_array_.count() + lob_macro_meta_array_.count() + bloomfilter_macro_meta_array_.count();
  if (total_macro_count > 0) {
    ObArray<MacroBlockId> bloomfilter_block_ids;
    if (OB_FAIL(block_meta_map_.create(total_macro_count, &map_allocator_))) {
      STORAGE_LOG(WARN, "fail to create block meta map", K(ret));
    } else if (!schema_map_.is_inited()) {
      if (OB_FAIL(schema_map_.create(DDL_VERSION_COUNT, &map_allocator_))) {
        STORAGE_LOG(WARN, "fail to create block meta map", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(convert_array_to_meta_map(meta_.macro_block_array_, macro_meta_array_))) {
      STORAGE_LOG(WARN, "fail to convert array to map", K(ret));
    } else if (OB_FAIL(convert_array_to_meta_map(meta_.lob_macro_block_array_, lob_macro_meta_array_))) {
      STORAGE_LOG(WARN, "fail to convert array to map", K(ret));
    } else if (meta_.bloom_filter_block_id_.is_valid()) {
      if (OB_FAIL(bloomfilter_block_ids.push_back(meta_.bloom_filter_block_id_))) {
        STORAGE_LOG(WARN, "fail to push back bloom filter block id", K(ret));
      } else if (OB_FAIL(convert_array_to_meta_map(bloomfilter_block_ids, bloomfilter_macro_meta_array_))) {
        STORAGE_LOG(WARN, "fail to convert array to map", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      block_meta_map_.destroy();
    }
  }
  return ret;
}

int ObSSTable::check_logical_data_version(const ObIArray<blocksstable::ObFullMacroBlockMeta>& block_metas)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < block_metas.count(); ++i) {
    const ObMacroBlockMetaV2* meta = block_metas.at(i).meta_;
    if (OB_ISNULL(meta)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "error sys, block meta must not be null", K(ret));
    } else if (OB_UNLIKELY(!meta->is_valid())) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "Invalid macro block meta", K(*meta), K(ret));
    } else {
      const int64_t data_version = is_major_sstable() ? meta->data_version_ : meta->snapshot_version_;
      if (data_version > meta_.logical_data_version_) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "Invalid data_version of macro block", K(ret), KPC(meta), K_(meta));
      }
    }
  }

  return ret;
}

int ObSSTable::build_logic_block_id_map()
{
  int ret = OB_SUCCESS;
  const int64_t total_macro_count = macro_meta_array_.count() + lob_macro_meta_array_.count();
  if (total_macro_count > 0) {
    if (OB_FAIL(logic_block_id_map_.create(total_macro_count, &map_allocator_))) {
      STORAGE_LOG(WARN, "fail to create logic block id map", K(ret));
    } else if (OB_FAIL(convert_array_to_id_map(meta_.macro_block_array_, macro_meta_array_))) {
      STORAGE_LOG(WARN, "fail to convert array to id map", K(ret));
    } else if (OB_FAIL(convert_array_to_id_map(meta_.lob_macro_block_array_, lob_macro_meta_array_))) {
      STORAGE_LOG(WARN, "fail to convert lob macro meta array", K(ret));
    }
    if (OB_FAIL(ret)) {
      logic_block_id_map_.destroy();
    }
  }
  return ret;
}

int ObSSTable::get_meta(const blocksstable::MacroBlockId& block_id, blocksstable::ObFullMacroBlockMeta& full_meta) const
{
  int ret = OB_SUCCESS;
  full_meta.reset();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSSTable has not been inited", K(ret));
  } else if (OB_FAIL(block_meta_map_.get(block_id, full_meta.meta_))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    STORAGE_LOG(WARN, "fail to get macro block meta", K(ret), K(block_id));
  } else if (OB_FAIL(schema_map_.get(full_meta.meta_->schema_version_, full_meta.schema_))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    STORAGE_LOG(WARN, "fail to get macro block schema info", K(ret), K(full_meta));
  }
  return ret;
}

int ObSSTable::close()
{
  int ret = OB_SUCCESS;
  if (SSTABLE_WRITE_BUILDING == status_) {
    sstable_merge_info_.occupy_size_ = meta_.occupy_size_;
  } else if (SSTABLE_INIT != status_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not initialized.", K(ret), K_(status));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_block_meta_map())) {
      STORAGE_LOG(WARN, "fail to build block meta map", K(ret));
    } else if (OB_FAIL(build_logic_block_id_map())) {
      STORAGE_LOG(WARN, "failed to build block id set", K(ret));
    } else if (OB_FAIL(check_logical_data_version(macro_meta_array_))) {
      STORAGE_LOG(WARN, "fail to check logical data version for sstable for data block", K(ret), K_(meta));
    } else if (OB_FAIL(check_logical_data_version(lob_macro_meta_array_))) {
      STORAGE_LOG(WARN, "fail to check logical data version for sstable for lob block", K(ret), K_(meta));
    } else if (blocksstable::CCM_TYPE_AND_VALUE == meta_.checksum_method_ &&
               meta_.data_version_ == meta_.progressive_merge_end_version_ - 1 && meta_.new_column_metas_.count() > 0) {
      meta_.checksum_method_ = blocksstable::CCM_VALUE_ONLY;
      for (int64_t i = 0; i < meta_.column_cnt_; ++i) {
        meta_.column_metas_.at(i).column_id_ = meta_.new_column_metas_.at(i).column_id_;
        meta_.column_metas_.at(i).column_default_checksum_ = meta_.new_column_metas_.at(i).column_default_checksum_;
        meta_.column_metas_.at(i).column_checksum_ = meta_.new_column_metas_.at(i).column_checksum_;
        STORAGE_LOG(INFO, "copy column checksum", K(meta_.column_metas_.at(i)));
      }
    }
    if (OB_SUCC(ret)) {
      status_ = SSTABLE_READY_FOR_READ;
    }
    if (OB_FAIL(ret)) {

    } else if (OB_FAIL(clean_lob_column_checksum())) {
      STORAGE_LOG(WARN, "Failed to clean lob column checksums", K(ret));
    } else {
      meta_.max_merged_trans_version_ = std::max(meta_.max_merged_trans_version_, get_snapshot_version());
      if (meta_.contain_uncommitted_row_) {
        meta_.upper_trans_version_ = INT64_MAX;
      } else {
        meta_.upper_trans_version_ = meta_.max_merged_trans_version_;
      }
      status_ = SSTABLE_READY_FOR_READ;
      if (OB_FAIL(rowkey_helper_.init(
              meta_.macro_block_array_, meta_, !exist_invalid_collation_free_meta_, this, allocator_))) {
        if (OB_TENANT_NOT_EXIST == ret) {
          // may be in reboot, rowkey helper is not nesserary
          rowkey_helper_.reset();
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "fail to load macro endkeys and cmp funcs", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    macro_meta_array_.reset();
    lob_macro_meta_array_.reset();
    bloomfilter_macro_meta_array_.reset();
  }
  return ret;
}

void ObSSTable::destroy()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "destroy sstable.", K_(meta));
  rowkey_helper_.reset();
  storage::ObIPartitionGroupGuard pg_guard;
  ObStorageFile* file = NULL;

  (void)logic_block_id_map_.destroy();

  for (int64_t i = 0; i < meta_.macro_block_array_.count(); ++i) {
    if (OB_ISNULL(file = file_handle_.get_storage_file())) {
      STORAGE_LOG(WARN, "fail to get pg file", K(ret), K(meta_.pg_key_), K(file_handle_));
    } else if (OB_FAIL(file->dec_ref(meta_.macro_block_array_.at(i)))) {
      STORAGE_LOG(ERROR, "fail to dec ref for pg_file", K(ret), K(i), K(meta_.macro_block_array_.at(i)));
    }
  }

  for (int64_t i = 0; i < meta_.lob_macro_block_array_.count(); ++i) {
    if (OB_ISNULL(file = file_handle_.get_storage_file())) {
      STORAGE_LOG(WARN, "fail to get pg file", K(ret), K(meta_.pg_key_), K(file_handle_));
    } else if (OB_FAIL(file->dec_ref(meta_.lob_macro_block_array_.at(i)))) {
      STORAGE_LOG(ERROR, "fail to dec ref for pg_file", K(ret), K(i), K(meta_.lob_macro_block_array_.at(i)));
    }
  }

  if (meta_.bloom_filter_block_id_.is_valid()) {
    if (OB_ISNULL(file = file_handle_.get_storage_file())) {
      STORAGE_LOG(WARN, "fail to get pg file", K(ret), K(meta_.pg_key_), K(file_handle_));
    } else if (OB_FAIL(file->dec_ref(meta_.bloom_filter_block_id_))) {
      STORAGE_LOG(ERROR, "fail to dec ref for pg_file", K(ret), K(meta_.bloom_filter_block_id_));
    }
  }

  // TODO(): release ofs files
  meta_.reset();
  status_ = SSTABLE_NOT_INIT;
  macro_meta_array_.reset();
  lob_macro_meta_array_.reset();
  bloomfilter_macro_meta_array_.reset();
  file_handle_.reset();
  pt_replay_module_ = nullptr;
  block_meta_map_.destroy();
  schema_map_.destroy();
  allocator_.reset();
  map_allocator_.reset();
  dump_memtable_timestamp_ = 0;
}

void ObSSTable::clear()
{
  meta_.reset();  // avoid release ofs file and macro block ref
  destroy();
}

int ObSSTable::append_macro_blocks(blocksstable::ObMacroBlocksWriteCtx& macro_block_write_ctx)
{
  int ret = OB_SUCCESS;

  if (SSTABLE_WRITE_BUILDING != status_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "cannot append macro block in this status", K(ret), K_(status));
  } else if (OB_FAIL(append_macro_block_write_ctx(
                 macro_block_write_ctx, meta_.macro_block_array_, meta_.macro_block_idx_array_, meta_.file_ctx_))) {
    LOG_WARN("failed to append_macro_block_write_ctx", K(ret));
  }
  return ret;
}

int ObSSTable::append_lob_macro_blocks(blocksstable::ObMacroBlocksWriteCtx& macro_block_write_ctx)
{
  int ret = OB_SUCCESS;

  if (SSTABLE_WRITE_BUILDING != status_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "cannot append macro block in this status", K(ret), K_(status));
  } else if (OB_FAIL(append_macro_block_write_ctx(macro_block_write_ctx,
                 meta_.lob_macro_block_array_,
                 meta_.lob_macro_block_idx_array_,
                 meta_.lob_file_ctx_))) {
    LOG_WARN("failed to append_macro_block_write_ctx", K(ret));
  }
  return ret;
}

int ObSSTable::append_bf_macro_blocks(ObMacroBlocksWriteCtx& macro_block_write_ctx)
{
  int ret = OB_SUCCESS;

  if (SSTABLE_WRITE_BUILDING != status_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "cannot append macro block in this status", K(ret), K_(status));
  } else if (meta_.bloom_filter_file_ctx_.file_system_type_ != macro_block_write_ctx.file_ctx_.file_system_type_ ||
             meta_.bloom_filter_file_ctx_.file_type_ != macro_block_write_ctx.file_ctx_.file_type_ ||
             meta_.bloom_filter_file_ctx_.block_count_per_file_ !=
                 macro_block_write_ctx.file_ctx_.block_count_per_file_ ||
             !macro_block_write_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid args", K(ret), K(macro_block_write_ctx), K(meta_.bloom_filter_file_ctx_));
  } else if (macro_block_write_ctx.is_empty()) {
    // pass
  } else if (macro_block_write_ctx.get_macro_block_count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected macro block write ctx count", K(macro_block_write_ctx), K(ret));
  } else if (OB_FAIL(add_macro_block_meta(
                 macro_block_write_ctx.macro_block_list_.at(0), macro_block_write_ctx.macro_block_meta_list_.at(0)))) {
    LOG_WARN("Failed to add macro block meta", K(macro_block_write_ctx.macro_block_list_.at(0)), K(ret));
  } else {
    meta_.bloom_filter_block_id_ = macro_block_write_ctx.macro_block_list_.at(0);
    meta_.bloom_filter_block_id_in_files_ = 0;
    if (macro_block_write_ctx.file_ctx_.need_file_id_list()) {
      if (macro_block_write_ctx.file_ctx_.file_list_.count() <= 0 ||
          macro_block_write_ctx.file_ctx_.file_list_.count() > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected macro block file ctx count", K(macro_block_write_ctx.file_ctx_), K(ret));
      } else if (OB_FAIL(meta_.bloom_filter_file_ctx_.file_list_.push_back(
                     macro_block_write_ctx.file_ctx_.file_list_.at(0)))) {
        LOG_ERROR("failed to add file list, fatal error", K(ret));
        ob_abort();
      }
    }
    if (OB_SUCC(ret)) {
      ObStorageFile* file = NULL;
      if (OB_ISNULL(file = file_handle_.get_storage_file())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "fail to get pg file", K(ret), K(meta_.pg_key_), K(file_handle_));
      } else if (OB_FAIL(file->inc_ref(meta_.bloom_filter_block_id_))) {
        STORAGE_LOG(ERROR, "fail to inc ref", K(ret));
      }
      if (OB_FAIL(ret)) {
        meta_.bloom_filter_block_id_.reset();
      }
    }
  }

  return ret;
}

int ObSSTable::get_combine_macro_block_ctx(const int64_t idx, blocksstable::ObMacroBlockCtx& ctx)
{
  int ret = OB_SUCCESS;

  if (SSTABLE_READY_FOR_READ != status_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("sstable not ready for read", K(ret), K(*this));
  } else if (idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", K(ret), K(idx));
  } else if (idx < meta_.macro_block_array_.count()) {
    if (OB_FAIL(get_macro_block_ctx(meta_.macro_block_array_.at(idx), meta_.file_ctx_, ctx))) {
      LOG_WARN("failed to get macro block ctx", K(ret), K(idx));
    }
  } else {
    if (OB_FAIL(get_lob_macro_block_ctx(idx - meta_.macro_block_array_.count(), ctx))) {
      LOG_WARN("failed to get lob macro block ctx", K(ret), K(idx));
    }
  }
  return ret;
}

int ObSSTable::get_macro_block_ctx(const int64_t idx, blocksstable::ObMacroBlockCtx& ctx)
{
  int ret = OB_SUCCESS;

  if (SSTABLE_READY_FOR_READ != status_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("sstable not ready for read", K(ret), K(*this));
  } else if (OB_FAIL(get_macro_block_ctx(meta_.macro_block_array_.at(idx), meta_.file_ctx_, ctx))) {
    LOG_WARN("failed to get macro block ctx", K(ret), K(idx));
  }
  return ret;
}

int ObSSTable::get_lob_macro_block_ctx(const int64_t idx, blocksstable::ObMacroBlockCtx& ctx)
{
  int ret = OB_SUCCESS;

  if (SSTABLE_READY_FOR_READ != status_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("sstable not ready for read", K(ret), K(*this));
  } else if (OB_FAIL(get_macro_block_ctx(meta_.lob_macro_block_array_.at(idx), meta_.lob_file_ctx_, ctx))) {
    LOG_WARN("failed to get lob macro block ctx", K(ret), K(idx));
  }
  return ret;
}

int ObSSTable::get_bf_macro_block_ctx(ObMacroBlockCtx& ctx)
{
  int ret = OB_SUCCESS;

  if (SSTABLE_READY_FOR_READ != status_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("sstable not ready for read", K(ret), K(*this));
  } else if (!meta_.bloom_filter_block_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid bloomfilter macro id to get", K(meta_.bloom_filter_block_id_), K(ret));
  } else {
    ctx.reset();
    ctx.file_ctx_ = &meta_.bloom_filter_file_ctx_;
    ctx.sstable_block_id_.macro_block_id_ = meta_.bloom_filter_block_id_;
    ctx.sstable_block_id_.macro_block_id_in_files_ = 0;
    ctx.sstable_ = this;
  }
  return ret;
}

int ObSSTable::get_tenant_file_key(ObTenantFileKey& file_key)
{
  int ret = OB_SUCCESS;
  ObStorageFile* file = NULL;
  if (OB_ISNULL(file = file_handle_.get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get pg file", K(ret), K(meta_.pg_key_), K(file_handle_));
  } else {
    file_key.file_id_ = file->get_file_id();
    file_key.tenant_id_ = key_.get_tenant_id();
  }
  return ret;
}

int ObSSTable::get_macro_block_ctx(const MacroBlockId& block_id, blocksstable::ObMacroBlockCtx& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_macro_block_ctx(block_id, meta_.file_ctx_, ctx))) {
    STORAGE_LOG(WARN, "fail to get macro block ctx", K(ret));
  }
  return ret;
}

int ObSSTable::get_macro_block_ctx(
    const MacroBlockId& block_id, const blocksstable::ObStoreFileCtx& file_ctx, blocksstable::ObMacroBlockCtx& ctx)
{
  int ret = OB_SUCCESS;
  ctx.reset();

  if (SSTABLE_READY_FOR_READ != status_) {
    ret = OB_ERR_SYS;
    LOG_WARN("sstable not ready for read", K(ret), K(*this));
  } else {
    ctx.file_ctx_ = &file_ctx;
    ctx.sstable_ = this;
    ctx.sstable_block_id_.macro_block_id_ = block_id;
  }
  return ret;
}

// only used during observer reboot
int ObSSTable::set_sstable(ObSSTable& src_sstable)
{
  int ret = OB_SUCCESS;
  ObMacroMetaReplayMap* replay_map = nullptr;

  if (SSTABLE_NOT_INIT != status_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable is not in close status.", K(ret), K_(status));
  } else if (OB_FAIL(init(src_sstable.get_key()))) {
    LOG_WARN("failed to init sstable", K(ret), K(src_sstable));
  } else if (OB_FAIL(open(src_sstable.get_meta()))) {
    LOG_WARN("failed to open sstable", K(ret), K(src_sstable));
  } else if (OB_FAIL(meta_.macro_block_array_.assign(src_sstable.meta_.macro_block_array_))) {
    LOG_WARN("failed to copy macro block array", K(ret));
  } else if (OB_FAIL(meta_.lob_macro_block_array_.assign(src_sstable.meta_.lob_macro_block_array_))) {
    LOG_WARN("failed to copy lob macro block array", K(ret));
  } else if (OB_FAIL(meta_.macro_block_idx_array_.assign(src_sstable.meta_.macro_block_idx_array_))) {
    LOG_WARN("failed to copy macro block idx array", K(ret));
  } else if (OB_FAIL(meta_.lob_macro_block_idx_array_.assign(src_sstable.meta_.lob_macro_block_idx_array_))) {
    LOG_WARN("failed to copy lob macro block idx array", K(ret));
  } else if (!schema_map_.is_inited() && OB_FAIL(schema_map_.create(DDL_VERSION_COUNT, &allocator_))) {
    STORAGE_LOG(WARN, "fail to create block meta map", K(ret));
  } else if (OB_FAIL(replay_add_macro_block_meta(meta_.macro_block_array_, src_sstable))) {
    LOG_WARN("failed to add macro block meta", K(ret));
  } else if (OB_FAIL(replay_add_macro_block_meta(meta_.lob_macro_block_array_, src_sstable))) {
    LOG_WARN("failed to add lob macro block meta", K(ret));
  } else if (OB_FAIL(file_handle_.assign(src_sstable.file_handle_))) {
    LOG_WARN("failed to assign file handle", K(ret));
  } else {
    meta_.pg_key_ = src_sstable.meta_.pg_key_;
    meta_.file_ctx_.reset();
    meta_.lob_file_ctx_.reset();
    meta_.bloom_filter_file_ctx_.reset();
    if (OB_FAIL(meta_.file_ctx_.assign(src_sstable.meta_.file_ctx_))) {
      LOG_WARN("failed to copy file ctx", K(ret));
    } else if (OB_FAIL(meta_.lob_file_ctx_.assign(src_sstable.meta_.lob_file_ctx_))) {
      LOG_WARN("Failed to copy lob file ctx", K(ret));
    } else if (OB_FAIL(meta_.bloom_filter_file_ctx_.assign(src_sstable.meta_.bloom_filter_file_ctx_))) {
      LOG_WARN("Failed to copy bloomfilter file ctx", K(ret));
    } else {
      sstable_merge_info_ = src_sstable.get_sstable_merge_info();
      meta_.use_old_macro_block_count_ = src_sstable.meta_.use_old_macro_block_count_;
      meta_.lob_use_old_macro_block_count_ = src_sstable.get_meta().lob_use_old_macro_block_count_;
      meta_.build_on_snapshot_ = src_sstable.meta_.build_on_snapshot_;
      meta_.create_index_base_version_ = src_sstable.meta_.create_index_base_version_;
      meta_.max_merged_trans_version_ = src_sstable.meta_.max_merged_trans_version_;
      meta_.contain_uncommitted_row_ = src_sstable.meta_.contain_uncommitted_row_;
      for (int64_t i = 0; i < meta_.column_cnt_; i++) {
        meta_.column_metas_.at(i).column_checksum_ = src_sstable.get_meta().column_metas_.at(i).column_checksum_;
      }
      for (int64_t i = 0; i < src_sstable.get_meta().new_column_metas_.count(); i++) {
        meta_.new_column_metas_.at(i).column_checksum_ =
            src_sstable.get_meta().new_column_metas_.at(i).column_checksum_;
      }
      meta_.bloom_filter_block_id_ = src_sstable.meta_.bloom_filter_block_id_;
      meta_.bloom_filter_block_id_in_files_ = src_sstable.meta_.bloom_filter_block_id_in_files_;
      if (meta_.bloom_filter_block_id_.is_valid() &&
          OB_FAIL(replay_add_macro_block_meta(meta_.bloom_filter_block_id_, src_sstable))) {
        LOG_WARN("failed to add bloomfilter macro block meta", K(meta_.bloom_filter_block_id_), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(close())) {
      LOG_WARN("failed to close sstable", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("succeed to set sstable, clear src sstable", K(ret), K(src_sstable), K(*this));
    src_sstable.clear();
    int tmp_ret = OB_SUCCESS;
    ObStorageFile* file = NULL;
    if (OB_ISNULL(file = file_handle_.get_storage_file())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get pg file", K(ret), K(meta_.pg_key_), K(file_handle_));
    } else if (OB_SUCCESS != (tmp_ret = file->get_replay_map().remove(get_key(), meta_.macro_block_array_))) {
      LOG_WARN("fail to remove replay map macro metas", K(ret));
    }
    if (OB_SUCCESS != (tmp_ret = file->get_replay_map().remove(get_key(), meta_.lob_macro_block_array_))) {
      LOG_WARN("fail to remove replay map macro metas", K(ret));
    }
    if (meta_.bloom_filter_block_id_.is_valid() &&
        OB_SUCCESS != (tmp_ret = file->get_replay_map().remove(get_key(), meta_.bloom_filter_block_id_))) {
      LOG_WARN("fail to remove replay map macro metas", K(ret));
    }
  } else {
    LOG_WARN("failed to set sstable, clear dest sstable", K(ret), K(src_sstable), K(meta_.macro_block_array_));
    clear();
  }
  return ret;
}

int ObSSTable::set_column_checksum(const int64_t column_cnt, const int64_t* column_checksum)
{
  int ret = OB_SUCCESS;
  if (meta_.column_cnt_ != column_cnt || NULL == column_checksum) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(meta_.column_cnt_), K(column_cnt), KP(column_checksum));
  } else {
    for (int64_t i = 0; i < meta_.column_cnt_; ++i) {
      meta_.column_metas_.at(i).column_checksum_ = column_checksum[i];
    }
    STORAGE_LOG(INFO, "set column checksum", K(meta_.column_metas_));
  }
  return ret;
}

int ObSSTable::set_column_checksum(const hash::ObHashMap<int64_t, int64_t>& column_checksum_map)
{
  int ret = OB_SUCCESS;
  if (0 == column_checksum_map.size()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(column_checksum_map.size()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_.column_cnt_; ++i) {
      const int64_t column_id = meta_.column_metas_.at(i).column_id_;
      int64_t column_checksum = 0;
      if (column_id >= OB_MIN_SHADOW_COLUMN_ID) {
        meta_.column_metas_.at(i).column_checksum_ = 0;
        continue;
      } else if (OB_FAIL(column_checksum_map.get_refactored(column_id, column_checksum))) {
        STORAGE_LOG(WARN, "fail to get from map", K(ret), K(column_id));
      } else {
        meta_.column_metas_.at(i).column_checksum_ = column_checksum;
      }
    }
  }
  return ret;
}

int ObSSTable::add_sstable_merge_info(const ObSSTableMergeInfo& sstable_merge_info)
{
  int ret = OB_SUCCESS;
  if (SSTABLE_WRITE_BUILDING != status_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable is in invalid status, cannot add sstable info", K(ret), K_(status));
  } else if (OB_FAIL(sstable_merge_info_.add(sstable_merge_info))) {
    STORAGE_LOG(WARN, "Fail to add sstable merge info, ", K(ret));
  }
  return ret;
}

int ObSSTable::bf_may_contain_rowkey(const ObStoreRowkey& rowkey, bool& contain)
{
  int ret = OB_SUCCESS;
  ObStorageFile* file = NULL;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSTable has not been inited", K(ret));
  } else if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to check bloomfilter", K(ret));
  } else if (!has_bloom_filter_macro_block()) {
    // pass sstable without bf macro
  } else if (OB_ISNULL(file = file_handle_.get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get pg file", K(ret), K(meta_.pg_key_), K(file_handle_));
  } else if (OB_SUCC(ObStorageCacheSuite::get_instance().get_bf_cache().may_contain(
                 meta_.index_id_, meta_.bloom_filter_block_id_, file->get_file_id(), rowkey, contain))) {
    STORAGE_LOG(DEBUG, "Succ to check sstable bloomfilter", K(meta_.index_id_), K(meta_.bloom_filter_block_id_));
  } else if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
    STORAGE_LOG(WARN, "Failed to get bloomfilter cache value from exist bf cache", K(ret));
  } else {
    // schedule build task
    if (OB_FAIL(ObPartitionScheduler::get_instance().schedule_load_bloomfilter(
            meta_.index_id_, meta_.bloom_filter_block_id_, get_key()))) {
      STORAGE_LOG(WARN, "Failed to schedule bloomfilter build task", K(ret));
    } else {
      STORAGE_LOG(
          DEBUG, "Succ to schedule bloomfilter build task", K(meta_.index_id_), K(meta_.bloom_filter_block_id_));
    }
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

bool ObSSTable::is_valid() const
{
  return key_.is_valid() && meta_.is_valid() && SSTABLE_READY_FOR_READ == status_;
}

int ObSSTable::get(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
    const common::ObExtStoreRowkey& rowkey, ObStoreRowIterator*& row_iter)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSStore has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(param.table_id_ != key_.table_id_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table id not match", K(ret), K(key_.table_id_), K(param.table_id_), K(*this), K(param));
  } else if (OB_UNLIKELY(!param.is_valid() || (!context.is_valid()) || (!rowkey.get_store_rowkey().is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid get arguments.", K(ret), K(param), K(context), K(rowkey));
  } else {
    void* buf = NULL;
    ObISSTableRowIterator* row_getter = NULL;
    if (is_multi_version_minor_sstable() && (context.is_multi_version_read(get_upper_trans_version()) ||
                                                contain_uncommitted_row() || !meta_.has_compact_row_)) {
      if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableMultiVersionRowGetter)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        row_getter = new (buf) ObSSTableMultiVersionRowGetter();
      }
    } else {
      if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableRowGetter)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        row_getter = new (buf) ObSSTableRowGetter();
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(row_getter->init(param, context, this, &rowkey))) {
        STORAGE_LOG(WARN, "Fail to open getter, ", K(ret), K(param), K(context), K(rowkey));
      }
    }

    if (OB_FAIL(ret)) {
      if (NULL != row_getter) {
        row_getter->~ObISSTableRowIterator();
      }
    } else {
      row_iter = row_getter;
    }
  }
  return ret;
}

int ObSSTable::multi_get(const ObTableIterParam& param, ObTableAccessContext& context,
    const common::ObIArray<common::ObExtStoreRowkey>& rowkeys, ObStoreRowIterator*& row_iter)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSStore has not been inited, ", K(ret), K(meta_), K(status_));
  } else if (OB_UNLIKELY(param.table_id_ != key_.table_id_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table id not match", K(ret), K(key_.table_id_), K(param.table_id_), K(*this), K(param));
  } else if (OB_UNLIKELY(!param.is_valid() || (!context.is_valid()) || (rowkeys.count() <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(param), K(context), K(rowkeys.count()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); ++i) {
      if (!rowkeys.at(i).get_store_rowkey().is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "rowkey is invalid", K(ret), K(i), K(rowkeys));
      }
    }
    if (OB_SUCC(ret)) {
      void* buf = NULL;
      ObISSTableRowIterator* row_getter = NULL;
      if (is_multi_version_minor_sstable() && (context.is_multi_version_read(get_upper_trans_version()) ||
                                                  contain_uncommitted_row() || !meta_.has_compact_row_)) {
        if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableMultiVersionRowMultiGetter)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
        } else {
          row_getter = new (buf) ObSSTableMultiVersionRowMultiGetter();
        }
      } else {
        if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableRowMultiGetter)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
        } else {
          row_getter = new (buf) ObSSTableRowMultiGetter();
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(row_getter->init(param, context, this, &rowkeys))) {
          STORAGE_LOG(WARN, "Fail to open row multi getter, ", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
        if (NULL != row_getter) {
          row_getter->~ObISSTableRowIterator();
        }
      } else {
        row_iter = row_getter;
      }
    }
  }
  return ret;
}

int ObSSTable::get_last_rowkey(ObStoreRowkey& rowkey, ObArenaAllocator& allocator)
{
  int ret = OB_SUCCESS;

  ObFullMacroBlockMeta full_meta;
  ObStoreRowkey temp_key;
  const ObIArray<MacroBlockId>& macro_blocks = meta_.macro_block_array_;

  if (macro_blocks.count() <= 0) {
    // no need to set rowkey
  } else if (OB_FAIL(get_meta(macro_blocks.at(macro_blocks.count() - 1), full_meta))) {
    STORAGE_LOG(WARN, "Fail to get macro block meta, ", K(ret));
  } else if (!full_meta.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "macro block meta is NULL.", K(ret), K(full_meta), K(macro_blocks));
  } else {
    temp_key.assign(full_meta.meta_->endkey_, full_meta.meta_->rowkey_column_number_);
    if (OB_FAIL(temp_key.deep_copy(rowkey, allocator))) {
      STORAGE_LOG(WARN, "failed to deep copy rowkey", K(ret));
    }
  }

  return ret;
}

int ObSSTable::get_all_macro_block_endkey(ObIArray<common::ObRowkey>& rowkey_array, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSStore has not been inited", K(ret));
  } else {
    ObFullMacroBlockMeta full_meta;
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_.macro_block_array_.count(); ++i) {
      if (OB_FAIL(get_meta(meta_.macro_block_array_.at(i), full_meta))) {
        STORAGE_LOG(WARN, "fail to get meta", K(ret), K(meta_.macro_block_array_.at(i)));
      } else if (!full_meta.is_valid()) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "error sys, macro meta must not be null", K(ret));
      } else {
        ObRowkey tmp_rowkey1;
        ObRowkey tmp_rowkey2;
        tmp_rowkey1.assign(full_meta.meta_->endkey_, full_meta.schema_->schema_rowkey_col_cnt_);

        if (OB_FAIL(tmp_rowkey1.deep_copy(tmp_rowkey2, allocator))) {
          STORAGE_LOG(WARN, "failed to deep copy rowkey", K(ret));
        } else if (OB_FAIL(rowkey_array.push_back(tmp_rowkey2))) {
          STORAGE_LOG(WARN, "rowkey_array push_back failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSSTable::scan(const ObTableIterParam& param, ObTableAccessContext& context,
    const common::ObExtStoreRange& key_range, ObStoreRowIterator*& row_iter)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable is not in not ready for access", K(ret), K_(meta), K_(status));
  } else if (OB_UNLIKELY(param.table_id_ != key_.table_id_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table id not match", K(ret), K(key_.table_id_), K(param.table_id_), K(*this), K(param));
  } else if (OB_UNLIKELY(!param.is_valid() || (!context.is_valid()) || (!key_range.get_range().is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(param), K(context), K(key_range), K(ret));
  } else {
    void* buf = NULL;
    ObISSTableRowIterator* row_scanner = NULL;
    if (context.query_flag_.is_whole_macro_scan()) {
      if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableRowWholeScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        row_scanner = new (buf) ObSSTableRowWholeScanner();
      }
    } else if (is_multi_version_minor_sstable()) {
      if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableMultiVersionRowScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        row_scanner = new (buf) ObSSTableMultiVersionRowScanner();
      }
    } else {
      if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableRowScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        row_scanner = new (buf) ObSSTableRowScanner();
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(row_scanner->init(param, context, this, &key_range))) {
        STORAGE_LOG(WARN, "Fail to open row scanner, ", K(ret), K(param), K(context), K(key_range));
      }
    }
    if (OB_FAIL(ret)) {
      if (NULL != row_scanner) {
        row_scanner->~ObISSTableRowIterator();
      }
    } else {
      row_iter = row_scanner;
    }
  }

  return ret;
}

int ObSSTable::dump2text(const char* dir_name, const share::schema::ObTableSchema& schema, const char* fname)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  char real_fname[OB_MAX_FILE_NAME_LENGTH];
  FILE* fd = NULL;
  ObArenaAllocator allocator(ObModIds::OB_CS_MERGER);
  ObArenaAllocator stmt_allocator(ObModIds::OB_CS_MERGER);
  ObArray<ObColDesc, ObIAllocator&> column_ids(2048, allocator);
  const storage::ObStoreRow* curr_row;
  ObExtStoreRange range;
  range.get_range().set_whole_range();
  ObTableIterParam param;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  const uint64_t tenant_id = extract_tenant_id(schema.get_table_id());
  ObBlockCacheWorkingSet block_cache_ws;
  ObStoreRowIterator* scanner = NULL;
  memtable::ObIMemtableCtxFactory* memctx_factory = ObPartitionService::get_instance().get_mem_ctx_factory();

  STORAGE_LOG(INFO, "dump2text", K_(meta));
  if (OB_ISNULL(fname) || OB_ISNULL(memctx_factory)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument for dump2text", K(ret), KP(fname), KP(memctx_factory));
  } else if (OB_FAIL(block_cache_ws.init(tenant_id))) {
    STORAGE_LOG(WARN, "block_cache_ws init failed", K(ret), K(tenant_id));
  } else if (OB_ISNULL(store_ctx.mem_ctx_ = memctx_factory->alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memtable ctx", K(ret));
  } else if (OB_FAIL(store_ctx.init_trans_ctx_mgr(meta_.pg_key_))) {
    LOG_WARN("failed to init trans ctx mgr", K(ret), K(tenant_id), K_(meta));
  } else {
    ObQueryFlag query_flag(ObQueryFlag::Forward,
        true, /*is daily merge scan*/
        true, /*is read multiple macro block*/
        true, /*sys task scan, read one macro block in single io*/
        false /*is full row scan?*/,
        false,
        false);
    param.table_id_ = schema.get_table_id();
    param.rowkey_cnt_ = schema.get_rowkey_column_num();
    param.schema_version_ = schema.get_schema_version();
    context.query_flag_ = query_flag;
    context.store_ctx_ = &store_ctx;
    context.allocator_ = &allocator;
    context.block_cache_ws_ = &block_cache_ws;
    context.stmt_allocator_ = &stmt_allocator;
    context.trans_version_range_ = key_.trans_version_range_;
    context.is_inited_ = true;  // just used for dump

    pret =
        snprintf(real_fname, sizeof(real_fname), "%s.%ld", fname, ::oceanbase::common::ObTimeUtility::current_time());
    if (pret < 0 || pret >= OB_MAX_FILE_NAME_LENGTH) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "fname too long", K(fname));
    } else if (NULL == (fd = fopen(real_fname, "w"))) {
      ret = OB_IO_ERROR;
      STORAGE_LOG(WARN, "open file fail:", K(fname));
    } else if (OB_FAIL(schema.get_column_ids(column_ids, true))) {
      STORAGE_LOG(WARN, "Fail to get column ids. ", K(ret));
    } else if (FALSE_IT(param.out_cols_ = &column_ids)) {
    } else if (OB_FAIL(scan(param, context, range, scanner))) {
      STORAGE_LOG(WARN, "Fail to scan range, ", K(ret));
    } else {
      fprintf(fd, "sstable: sstable_meta=%s\n", to_cstring(meta_));
      for (int64_t row_idx = 0; OB_SUCC(ret) && OB_SUCCESS == (ret = scanner->get_next_row(curr_row)); row_idx++) {
        if (REACH_TIME_INTERVAL(1000 * 1000) && 0 != access(real_fname, W_OK)) {
          STORAGE_LOG(ERROR, "file not exist when dump", K(real_fname));
          ret = OB_IO_ERROR;
        } else if (NULL == curr_row) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "iter NULL value");
        } else {
          fprintf(fd, "row_idx=%ld %s\n", row_idx, to_cstring(*curr_row));
        }
        if (OB_SUCC(ret) && 0 == row_idx % compaction::ObTableDumper::ROW_COUNT_CHECK_INTERVAL) {
          fflush(fd);
          if (OB_FAIL(compaction::ObTableDumper::check_disk_free_space(dir_name))) {
            if (OB_CS_OUTOF_DISK_SPACE == ret) {
              STORAGE_LOG(WARN, "disk space is not enough", K(ret));
              break;
            } else {
              STORAGE_LOG(WARN, "failed to judge disk space", K(ret), K(dir_name));
            }
          }
        }
      }  // end for
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (NULL != scanner) {
    scanner->~ObStoreRowIterator();
  }
  if (nullptr != memctx_factory && nullptr != store_ctx.mem_ctx_) {
    memctx_factory->free(store_ctx.mem_ctx_);
    store_ctx.mem_ctx_ = nullptr;
  }
  if (NULL != fd) {
    fprintf(fd, "end of sstable\n");
    fclose(fd);
    fd = NULL;
    STORAGE_LOG(INFO, "succ to dump");
  }
  return ret;
}

int ObSSTable::multi_scan(const ObTableIterParam& param, ObTableAccessContext& context,
    const common::ObIArray<common::ObExtStoreRange>& ranges, ObStoreRowIterator*& row_iter)
{
  int ret = OB_SUCCESS;
  row_iter = NULL;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSStore has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(param.table_id_ != key_.table_id_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table id not match", K(ret), K(key_.table_id_), K(param.table_id_), K(*this), K(param));
  } else if (OB_UNLIKELY(!param.is_valid() || (!context.is_valid()) || (0 >= ranges.count()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(param), K(context), K(ranges.count()), K(ret));
  } else {
    void* buf = NULL;
    ObISSTableRowIterator* row_scanner = NULL;
    if (is_multi_version_minor_sstable()) {
      if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableMultiVersionRowMultiScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        row_scanner = new (buf) ObSSTableMultiVersionRowMultiScanner();
      }
    } else {
      if (NULL == (buf = context.allocator_->alloc(sizeof(ObSSTableRowMultiScanner)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        row_scanner = new (buf) ObSSTableRowMultiScanner();
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(row_scanner->init(param, context, this, &ranges))) {
        STORAGE_LOG(WARN, "Fail to open row scanner, ", K(ret), K(param), K(context), K(ranges));
      }
    }
    if (OB_FAIL(ret)) {
      if (NULL != row_scanner) {
        row_scanner->~ObISSTableRowIterator();
      }
    } else {
      row_iter = row_scanner;
    }
  }

  return ret;
}

int ObSSTable::estimate_get_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
    const common::ObIArray<common::ObExtStoreRowkey>& rowkeys, ObPartitionEst& part_est)
{
  int ret = OB_SUCCESS;
  UNUSED(query_flag);
  ObStoreRowMultiGetEstimator getter;
  ObSSTableEstimateContext context;
  ObBlockCacheWorkingSet block_cache_ws;
  const uint64_t tenant_id = extract_tenant_id(table_id);

  part_est.reset();
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSStore has not been inited, ", K(ret));
  } else if (OB_INVALID_ID == table_id || rowkeys.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid get arguments.", K(ret), K(table_id), K(rowkeys.count()));
  } else if (table_id != key_.table_id_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table id not match", K(ret), K(key_.table_id_), K(table_id), K(*this));
  } else if (OB_FAIL(block_cache_ws.init(tenant_id))) {
    STORAGE_LOG(WARN, "block_cache_ws init failed", K(ret), K(tenant_id));
  } else {
    context.sstable_ = this;
    context.cache_context_.set(block_cache_ws);
    context.rowkeys_ = &rowkeys;
    if (OB_FAIL(getter.set_context(context))) {
      STORAGE_LOG(WARN, "failed to set context", K(ret));
    } else if (OB_FAIL(getter.estimate_row_count(part_est))) {
      STORAGE_LOG(WARN, "Fail to estimate cost of get.", K(ret));
    } else {
      STORAGE_LOG(DEBUG, "estimate_get_cost", K(ret), K(table_id), K(rowkeys), K(part_est), K_(meta));
    }
  }
  return ret;
}

int ObSSTable::estimate_scan_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
    const common::ObExtStoreRange& key_range, ObPartitionEst& part_est)
{
  int ret = OB_SUCCESS;
  UNUSED(query_flag);
  ObSSTableScanEstimator scan_estimator;
  ObSSTableEstimateContext context;
  ObBlockCacheWorkingSet block_cache_ws;
  const uint64_t tenant_id = extract_tenant_id(table_id);

  part_est.reset();
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSStore has not been inited, ", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid get arguments.", K(ret), K(table_id), K(key_range));
  } else if (table_id != key_.table_id_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table id not match", K(ret), K(key_.table_id_), K(table_id), K(*this));
  } else if (key_range.get_range().empty()) {
    // empty range
  } else if (OB_FAIL(block_cache_ws.init(tenant_id))) {
    STORAGE_LOG(WARN, "block_cache_ws init failed", K(ret), K(tenant_id));
  } else {
    context.sstable_ = this;
    context.cache_context_.set(block_cache_ws);
    context.range_ = &key_range;
    if (OB_FAIL(scan_estimator.set_context(context))) {
      STORAGE_LOG(WARN, "fail to set context", K(ret), K(key_range));
    } else if (OB_FAIL(scan_estimator.estimate_row_count(part_est))) {
      STORAGE_LOG(WARN, "Fail to estimate cost of scan.", K(ret), K(table_id));
    } else {
      STORAGE_LOG(DEBUG,
          "estimate_scan_cost",
          K(ret),
          K(table_id),
          K(key_range),
          K(part_est),
          K_(meta),
          K(sizeof(scan_estimator)));
    }
  }
  return ret;
}

int ObSSTable::estimate_multi_scan_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
    const common::ObIArray<common::ObExtStoreRange>& ranges, ObPartitionEst& part_est)
{
  UNUSED(query_flag);
  int ret = OB_SUCCESS;
  ObStoreRowMultiScanEstimator scanner;
  ObSSTableEstimateContext context;
  ObBlockCacheWorkingSet block_cache_ws;
  const uint64_t tenant_id = extract_tenant_id(table_id);

  part_est.reset();

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSStore has not been inited, ", K(ret));
  } else if (OB_INVALID_ID == table_id || ranges.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid get arguments.", K(ret), K(table_id), K(ranges.count()));
  } else if (table_id != key_.table_id_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table id not match", K(ret), K(key_.table_id_), K(table_id), K(*this));
  } else if (OB_FAIL(block_cache_ws.init(tenant_id))) {
    STORAGE_LOG(WARN, "block_cache_ws init failed", K(ret), K(tenant_id));
  } else {
    context.sstable_ = this;
    context.cache_context_.set(block_cache_ws);
    context.ranges_ = &ranges;
    if (OB_FAIL(scanner.set_context(context))) {
      STORAGE_LOG(WARN, "fail to set context", K(ret));
    } else if (OB_FAIL(scanner.estimate_row_count(part_est))) {
      STORAGE_LOG(WARN, "Fail to estimate cost of scan.", K(ret), K(table_id));
    } else {
      STORAGE_LOG(DEBUG, "estimate_multi_scan_cost", K(ret), K(table_id), K(ranges), K(part_est), K_(meta));
    }
  }
  return ret;
}

int ObSSTable::set(const ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_size,
    const common::ObIArray<share::schema::ObColDesc>& columns, ObStoreRowIterator& row_iter)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(ctx);
  UNUSED(table_id);
  UNUSED(rowkey_size);
  UNUSED(columns);
  UNUSED(row_iter);
  STORAGE_LOG(ERROR, "sstable not support set operation", K(ret), K(table_id));
  return ret;
}

int ObSSTable::scan_macro_block_totally(ObMacroBlockIterator& macro_block_iter)
{
  int ret = OB_SUCCESS;
  // TODO  spilit lob macroblock iter
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "sstable is not in open status, ", K(ret), K_(status));
  } else if (OB_FAIL(macro_block_iter.open(*this, false, true))) {
    STORAGE_LOG(WARN, "setup macro block iterator failed", K(ret));
  }
  return ret;
}

int ObSSTable::scan_macro_block(ObMacroBlockIterator& macro_block_iter, const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "sstable is not in open status, ", K(ret), K_(status));
  } else if (OB_FAIL(macro_block_iter.open(*this, is_reverse_scan, false))) {
    STORAGE_LOG(WARN, "setup macro block iterator failed", K(ret));
  }
  return ret;
}

int ObSSTable::scan_macro_block(
    const ObExtStoreRange& range, ObMacroBlockIterator& macro_block_iter, const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "sstable is not in open status, ", K(ret), K_(status));
  } else if (OB_FAIL(macro_block_iter.open(*this, range, is_reverse_scan))) {
    STORAGE_LOG(WARN, "Fail to open macro block iter, ", K(ret), K(*this));
  } else {
    STORAGE_LOG(DEBUG, "Succ to scan range, ", K(range.get_range()));
  }
  return ret;
}

int ObSSTable::get_range(
    const int64_t idx, const int64_t concurrent_cnt, common::ObIAllocator& allocator, common::ObExtStoreRange& range)
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta start;
  ObFullMacroBlockMeta last;
  const ObIArray<MacroBlockId>& macro_blocks = meta_.macro_block_array_;
  ObStoreRowkey rowkey;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "sstable is not in open status, ", K(ret), K_(status));
  } else if (idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid idx, ", K(ret), K(idx));
  } else {
    int64_t macro_cnts = (macro_blocks.count() + concurrent_cnt - 1) / concurrent_cnt;
    int64_t begin = idx * macro_cnts;
    int64_t end = (idx + 1) * macro_cnts - 1;
    if (0 == begin) {
      range.get_range().set_start_key(ObStoreRowkey::MIN_STORE_ROWKEY);
      range.get_range().set_left_open();
    } else if (OB_FAIL(get_meta(macro_blocks.at(begin - 1), start))) {
      STORAGE_LOG(WARN, "Fail to get macro block meta, ", K(ret));
    } else if (!start.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro block meta is NULL.", K(ret), K(begin), K(macro_blocks));
    } else {
      rowkey.assign(start.meta_->endkey_, start.meta_->rowkey_column_number_);
      if (OB_FAIL(rowkey.deep_copy(range.get_range().get_start_key(), allocator))) {
        STORAGE_LOG(WARN, "fail to deep copy start key", K(ret), K(start.meta_->endkey_));
      } else {
        range.get_range().set_left_open();
      }
    }

    if (OB_SUCC(ret)) {
      if (end >= macro_blocks.count() - 1) {
        range.get_range().set_end_key(ObStoreRowkey::MAX_STORE_ROWKEY);
        range.get_range().set_right_open();
      } else if (OB_FAIL(get_meta(macro_blocks.at(end), last))) {
        STORAGE_LOG(WARN, "Fail to get macro block meta, ", K(ret));
      } else if (!last.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "macro block meta is NULL.", K(ret), K(end), K(macro_blocks));
      } else {
        rowkey.assign(last.meta_->endkey_, last.meta_->rowkey_column_number_);
        if (OB_FAIL(rowkey.deep_copy(range.get_range().get_end_key(), allocator))) {
          STORAGE_LOG(WARN, "fail to deep copy start key", K(ret), K(last.meta_->endkey_));
        } else {
          range.get_range().set_right_closed();
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(range.to_collation_free_range_on_demand_and_cutoff_range(allocator))) {
        STORAGE_LOG(WARN, "Failed to transform range to collation free and range cutoff", K(range), K(ret));
      } else {
        STORAGE_LOG(INFO,
            "succ to get range, ",
            K(ret),
            K(meta_.index_id_),
            K(idx),
            K(concurrent_cnt),
            K(range.get_range()),
            K(macro_cnts),
            "macro_block_count",
            macro_blocks.count());
      }
    }
  }

  return ret;
}

DEFINE_SERIALIZE(ObSSTable)
{
  int ret = OB_SUCCESS;

  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "argument is invalid", K(ret), KP(buf), K(buf_len));
  } else if (ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_6 != meta_.sstable_format_version_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "cannot serialize old format sstable", K(ret));
  } else if (OB_FAIL(meta_.serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "sstable meta fail to serialize.", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(ObITable::serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "failed to serialize table key", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(serialize_schema_map(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "fail to serialize schema map", K(ret));
  } else {
    FLOG_INFO("succeed to serialize sstable", K(*this), "table_key", get_key(), K(meta_.macro_block_array_));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSSTable)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "argument is invalid", K(ret), K(buf), K(data_len));
  } else if (SSTABLE_NOT_INIT != status_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "sstable in this status cannot deserialize.", K(ret), K_(status));
  } else if (OB_FAIL(meta_.deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "sstable meta fail to deserialize.", K(ret), K(data_len), K(pos));
  } else if (!meta_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable meta is not valid", K_(meta));
  } else if (meta_.sstable_format_version_ >= ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_2 &&
             OB_FAIL(ObITable::deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "failed to deserialize ob i table", K(ret), K(data_len), K(pos));
  } else if (OB_FAIL(deserialize_schema_map(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize schema map", K(ret));
  } else {
    ObStorageFile* file = NULL;
    if (!file_handle_.is_valid() && OB_FAIL(get_file_handle_from_replay_module(meta_.pg_key_))) {
      if (OB_PARTITION_NOT_EXIST == ret) {
        STORAGE_LOG(WARN, "pg not exist", K(ret), K(meta_.pg_key_));
      } else {
        STORAGE_LOG(ERROR, "fail to get pg file from replay module", K(ret), K(meta_.pg_key_));
      }
    } else if (OB_ISNULL(file = file_handle_.get_storage_file())) {
      ret = OB_ERR_UNEXPECTED;
      meta_.macro_block_array_.reset();
      meta_.lob_macro_block_array_.reset();
      meta_.bloom_filter_block_id_.reset();
      STORAGE_LOG(ERROR, "fail to get pg file", K(ret), K(meta_.pg_key_), K(file_handle_));
    } else if (OB_FAIL(add_macro_ref())) {
      STORAGE_LOG(WARN, "fail to add macro ref", K(ret));
    } else {
      status_ =
          SSTABLE_READY_FOR_READ;  // TODO(): status need to serialized if need support continue migrate after reboot
    }
  }

  if (OB_SUCC(ret) && meta_.sstable_format_version_ < ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_5) {
    if (meta_.sstable_format_version_ <= ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_2) {
      if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.file_ctx_))) {
        LOG_WARN("failed to init file ctx", K(ret));
      } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.lob_file_ctx_))) {
        LOG_WARN("failed to init lob file ctx", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.bloom_filter_file_ctx_))) {
        LOG_WARN("Failed to init bloomfilter file ctx", K(ret));
      } else {
        meta_.bloom_filter_block_id_.reset();
        meta_.bloom_filter_block_id_in_files_ = 0;
      }
    }
    if (OB_SUCC(ret)) {
      meta_.logical_data_version_ = meta_.data_version_;
    }
  }
  if (OB_SUCC(ret) && meta_.sstable_format_version_ < ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_6) {
    meta_.logical_data_version_ = is_major_sstable() ? meta_.logical_data_version_ : get_snapshot_version();
    meta_.sstable_format_version_ = ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_6;
    set_multi_version_rowkey_type(ObMultiVersionRowkeyHelpper::MVRC_OLD_VERSION);
  }
  if (OB_SUCC(ret) && meta_.upper_trans_version_ == 0) {
    // old sstable from 2.x
    meta_.upper_trans_version_ = get_snapshot_version();
    meta_.max_merged_trans_version_ = get_snapshot_version();
    meta_.contain_uncommitted_row_ = false;
  }
  LOG_TRACE("finish to deserialize sstable", K(ret), K(*this));
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSSTable)
{
  return ObITable::get_serialize_size() + meta_.get_serialize_size() + get_schema_map_serialize_size();
}

int ObSSTable::append_macro_block_write_ctx(blocksstable::ObMacroBlocksWriteCtx& write_ctx,
    common::ObIArray<blocksstable::MacroBlockId>& macro_block_ids, common::ObIArray<int64_t>& block_id_in_files,
    blocksstable::ObStoreFileCtx& file_ctx)
{
  int ret = OB_SUCCESS;
  int64_t new_count = macro_block_ids.count() + write_ctx.macro_block_list_.count();
  int64_t new_file_count = file_ctx.file_list_.count() + write_ctx.file_ctx_.file_list_.count();
  int64_t cur_file_id_files = file_ctx.block_count_per_file_ * file_ctx.file_list_.count();

  if ((file_ctx.need_file_id_list() && macro_block_ids.count() != block_id_in_files.count()) ||
      file_ctx.file_system_type_ != write_ctx.file_ctx_.file_system_type_ ||
      file_ctx.file_type_ != write_ctx.file_ctx_.file_type_ ||
      file_ctx.block_count_per_file_ != write_ctx.file_ctx_.block_count_per_file_ || !write_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args",
        K(ret),
        "block_ids_count",
        macro_block_ids.count(),
        "block_in_files",
        block_id_in_files.count(),
        K(write_ctx),
        K(file_ctx));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < write_ctx.macro_block_list_.count(); ++i) {
    const MacroBlockId& macro_block_id = write_ctx.macro_block_list_.at(i);
    const ObFullMacroBlockMeta& meta = write_ctx.macro_block_meta_list_.at(i);
    if (OB_FAIL(add_macro_block_meta(macro_block_id, meta))) {
      LOG_WARN("failed to add macro block meta", K(ret), K(macro_block_id));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(macro_block_ids.reserve(new_count))) {
      LOG_WARN("Failed to reserve macro block ids", K(ret), K(new_count));
    } else if (write_ctx.file_ctx_.need_file_id_list()) {
      if (OB_FAIL(block_id_in_files.reserve(new_count))) {
        LOG_WARN("failed to reserve block id in files", K(ret), K(new_count));
      } else if (OB_FAIL(file_ctx.file_list_.reserve(new_file_count))) {
        LOG_WARN("failed to reserve new file count", K(ret), K(new_file_count));
      }
    }
  }

  // has reserved, should not fail
  for (int64_t i = 0; OB_SUCC(ret) && i < write_ctx.macro_block_list_.count(); ++i) {
    const MacroBlockId& macro_block_id = write_ctx.macro_block_list_.at(i);
    if (OB_FAIL(macro_block_ids.push_back(macro_block_id))) {
      LOG_ERROR("failed to add macro block id, fatal error", K(ret), K(macro_block_id));
      ob_abort();
    } else if (write_ctx.file_ctx_.need_file_id_list() && OB_FAIL(block_id_in_files.push_back(cur_file_id_files + i))) {
      LOG_ERROR("Failed to add block id in files, fatal error", K(ret));
      ob_abort();
    } else {
      ObStorageFile* file = NULL;
      if (OB_ISNULL(file = file_handle_.get_storage_file())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to get pg file", K(ret), K(meta_.pg_key_), K(file_handle_));
      } else if (OB_FAIL(file->inc_ref(macro_block_id))) {
        STORAGE_LOG(ERROR, "fail to inc ref of pg file", K(ret), K(i), K(macro_block_id));
      }
      if (OB_FAIL(ret)) {
        macro_block_ids.pop_back();
      }
    }
  }

  // has reserved, should not fail
  if (OB_SUCC(ret) && write_ctx.file_ctx_.need_file_id_list()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < write_ctx.file_ctx_.file_list_.count(); ++i) {
      if (OB_FAIL(file_ctx.file_list_.push_back(write_ctx.file_ctx_.file_list_.at(i)))) {
        LOG_ERROR("failed to add file list, fatal error", K(ret));
        ob_abort();
      }
    }
  }

  return ret;
}

int ObSSTable::add_macro_block_meta(
    const ObIArray<MacroBlockId>& macro_block_ids, const ObIArray<ObFullMacroBlockMeta>& macro_block_metas)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_ids.count(); ++i) {
    if (OB_FAIL(add_macro_block_meta(macro_block_ids.at(i), macro_block_metas.at(i)))) {
      LOG_WARN("failed to add macro block meta", K(ret), K(i));
    }
  }
  return ret;
}

int ObSSTable::add_macro_block_meta(const MacroBlockId& macro_block_id, const ObFullMacroBlockMeta& full_meta)
{
  int ret = OB_SUCCESS;
  const ObMacroBlockMetaV2* macro_block_meta = full_meta.meta_;

  if (!full_meta.is_valid() || !macro_block_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(full_meta), K(macro_block_id));
  } else if (!macro_block_meta->is_valid()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "meta is not valid", K(ret), K(*macro_block_meta));
  } else if (macro_block_meta->is_lob_data_block()) {
    meta_.lob_macro_block_count_++;
    if (macro_block_meta->data_version_ < meta_.data_version_) {
      meta_.lob_use_old_macro_block_count_++;
    }
    if (OB_FAIL(lob_macro_meta_array_.push_back(full_meta))) {
      STORAGE_LOG(WARN, "fail to push back macro block meta", K(ret));
    }
    STORAGE_LOG(DEBUG, "[LOB] sstable add lob macro block", K(*macro_block_meta), K(meta_), K(ret));
  } else if (macro_block_meta->is_sstable_data_block()) {
    meta_.macro_block_count_++;
    if (macro_block_meta->data_version_ < meta_.data_version_) {
      meta_.use_old_macro_block_count_++;
    }
    meta_.row_count_ += macro_block_meta->row_count_;
    if (macro_block_meta->max_merged_trans_version_ > meta_.max_merged_trans_version_) {
      meta_.max_merged_trans_version_ = macro_block_meta->max_merged_trans_version_;
    }
    if (macro_block_meta->contain_uncommitted_row_) {
      meta_.contain_uncommitted_row_ = true;
    }
    if (OB_FAIL(macro_meta_array_.push_back(full_meta))) {
      STORAGE_LOG(WARN, "fail to push back macro block meta", K(ret));
    }
  } else if (macro_block_meta->is_bloom_filter_data_block()) {
    if (OB_FAIL(bloomfilter_macro_meta_array_.push_back(full_meta))) {
      STORAGE_LOG(WARN, "fail to push back bloom filter macro meta", K(ret));
    }
  }
  // bloomfilter macro block is generated only in minor compaction,
  // so it do not need to be migrated, thus, the checksum and occupy_size of this macro block is not calculated
  if (OB_SUCC(ret) && !macro_block_meta->is_bloom_filter_data_block()) {
    meta_.occupy_size_ += macro_block_meta->occupy_size_;
    meta_.data_checksum_ =
        ob_crc64_sse42(meta_.data_checksum_, &macro_block_meta->data_checksum_, sizeof(meta_.data_checksum_));
    if (OB_FAIL(accumulate_macro_column_checksum(full_meta))) {
      STORAGE_LOG(WARN, "fail to accumulate macro column checksum", K(ret));
    }
  }

  return ret;
}

int ObSSTable::accumulate_macro_column_checksum(const blocksstable::ObFullMacroBlockMeta& full_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!full_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(full_meta));
  } else if (full_meta.meta_->is_lob_data_block()) {
    // lob column checksum temporarily disabled
  } else if (full_meta.meta_->is_sstable_data_block()) {
    if (blocksstable::CCM_TYPE_AND_VALUE == meta_.checksum_method_) {
      if (OB_FAIL(accumulate_macro_column_checksum_impl(full_meta, meta_.new_column_metas_))) {
        STORAGE_LOG(WARN, "fail to accumulate macro block column checksum", K(ret), K(full_meta));
      }
    } else if (blocksstable::CCM_VALUE_ONLY == meta_.checksum_method_) {
      if (OB_FAIL(accumulate_macro_column_checksum_impl(full_meta, meta_.column_metas_))) {
        STORAGE_LOG(WARN, "fail to accumulate macro block column checksum", K(ret), K(full_meta));
      }
    }
  }
  return ret;
}

int ObSSTable::accumulate_macro_column_checksum_impl(
    const blocksstable::ObFullMacroBlockMeta& full_meta, common::ObIArray<ObSSTableColumnMeta>& column_metas)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!full_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(full_meta));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_metas.count(); ++i) {
      int64_t j = 0;
      for (; OB_SUCC(ret) && j < full_meta.meta_->column_number_; ++j) {
        if (column_metas.at(i).column_id_ == full_meta.schema_->column_id_array_[j]) {
          break;
        }
      }
      if (j < full_meta.meta_->column_number_) {
        column_metas.at(i).column_checksum_ += full_meta.meta_->column_checksum_[j];
      } else {
        // column not found in macro meta, it it a new added column, add default column checksum
        column_metas.at(i).column_checksum_ +=
            full_meta.meta_->row_count_ * column_metas.at(i).column_default_checksum_;
      }
    }
  }
  return ret;
}

int ObSSTable::find_macros(const ExtStoreRowkeyArray& rowkeys, ObIArray<blocksstable::ObMacroBlockCtx>& macro_blocks)
{
  int ret = OB_SUCCESS;
  ObMacroBlockIterator macro_iter;
  ObMacroBlockCtx macro_block_ctx;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not in not ready for access.", K(ret), K_(status));
  } else if (rowkeys.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(rowkeys));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); ++i) {
      if (OB_FAIL(macro_iter.open(*this, rowkeys.at(i)))) {
        STORAGE_LOG(WARN, "Fail to open macro block iter, ", K(ret), K(i), K(rowkeys.at(i)));
      } else if (OB_FAIL(macro_iter.get_next_macro_block(macro_block_ctx))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          macro_block_ctx.reset();
        } else {
          STORAGE_LOG(WARN, "Fail to get next macro block, ", K(ret), K(i), K(rowkeys.at(i)));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(macro_blocks.push_back(macro_block_ctx))) {
          STORAGE_LOG(WARN, "Fail to push macro block ctx to array, ", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSSTable::find_macros(const ObExtStoreRange& range, common::ObIArray<blocksstable::ObMacroBlockCtx>& macro_blocks)
{
  int ret = OB_SUCCESS;
  blocksstable::ObMacroBlockCtx macro_block_ctx;
  ObMacroBlockIterator macro_iter;

  macro_blocks.reset();
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not in not ready for access.", K(ret), K_(status));
  } else if (!range.get_range().is_valid()) {
    // empty range
  } else if (OB_FAIL(macro_iter.open(*this, range))) {
    STORAGE_LOG(WARN, "Fail to open macro iter, ", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(macro_iter.get_next_macro_block(macro_block_ctx))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "Fail to get next macro block, ", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(macro_blocks.push_back(macro_block_ctx))) {
        STORAGE_LOG(WARN, "Fail to push block ctx to array, ", K(ret));
      }
    }
  }

  return ret;
}

int ObSSTable::locate_lob_macro(
    const ObExtStoreRowkey& ext_rowkey, const bool upper_bound, MacroBlockArray::iterator& iter)
{
  int ret = OB_SUCCESS;
  ObMacroBlockRowComparor comparer;
  MacroBlockArray::iterator begin = meta_.lob_macro_block_array_.begin();
  MacroBlockArray::iterator end = meta_.lob_macro_block_array_.end();
  MacroBlockArray::iterator meta_iter;
  bool use_collation_free = false;
  const ObStoreRowkey* cmp_rowkey = NULL;
  comparer.set_sstable(*this);

  if (OB_FAIL(ext_rowkey.check_use_collation_free(exist_invalid_collation_free_meta_, use_collation_free))) {
    STORAGE_LOG(WARN, "Fail to check use collation free", K(ret), K(ext_rowkey));
  } else {
    comparer.set_use_collation_free(use_collation_free);
    if (use_collation_free) {
      cmp_rowkey = &(ext_rowkey.get_collation_free_store_rowkey());
    } else {
      cmp_rowkey = &(ext_rowkey.get_store_rowkey());
    }
    if (upper_bound) {
      meta_iter = std::upper_bound(begin, end, *cmp_rowkey, comparer);
    } else {
      meta_iter = std::lower_bound(begin, end, *cmp_rowkey, comparer);
    }
    if (OB_FAIL(comparer.get_ret())) {
      STORAGE_LOG(ERROR, "Fail to find lob start macro", K(ret));
    } else if (meta_iter == end) {
      iter = meta_.lob_macro_block_array_.end();
    } else {
      iter = meta_.lob_macro_block_array_.begin() + (meta_iter - begin);
    }
  }

  return ret;
}

int ObSSTable::find_lob_macros(const ObExtStoreRange& range, ObIArray<ObMacroBlockInfoPair>& lob_macro_blocks)
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not in not ready for acccess", K(ret), K_(status));
  } else if (!range.get_range().is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(range));
  } else if (!has_lob_macro_blocks()) {
    // no lob macros
  } else {
    MacroBlockArray::iterator iter_start, iter_end;
    if (OB_FAIL(locate_lob_macro(
            range.get_ext_start_key(), !range.get_range().get_border_flag().inclusive_start(), iter_start))) {
      STORAGE_LOG(WARN, "Failed to locate lob start macro block", K(range.get_ext_start_key()), K(ret));
    } else if (iter_start == meta_.lob_macro_block_array_.end()) {
      // no block found
    } else if (OB_FAIL(locate_lob_macro(range.get_ext_end_key(), true, iter_end))) {
      STORAGE_LOG(WARN, "Failed to locate lob end macro block", K(range.get_ext_end_key()), K(ret));
    } else {
      for (; OB_SUCC(ret) && iter_start < iter_end; iter_start++) {
        ObFullMacroBlockMeta meta;
        if (OB_FAIL(get_meta(*iter_start, meta))) {
          STORAGE_LOG(WARN, "fail to get meta", K(ret));
        } else if (OB_FAIL(lob_macro_blocks.push_back(ObMacroBlockInfoPair(*iter_start, meta)))) {
          STORAGE_LOG(WARN, "Failed to push lob macro block to array", K(*iter_start), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObSSTable::find_macros(const ObIArray<common::ObExtStoreRange>& ranges,
    ObIArray<blocksstable::ObMacroBlockCtx>& macro_blocks, ObIArray<int64_t>& end_block_idx_of_ranges)
{
  int ret = OB_SUCCESS;
  blocksstable::ObMacroBlockCtx block_ctx;
  ObMacroBlockIterator macro_iter;

  macro_blocks.reset();
  end_block_idx_of_ranges.reset();
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not ready for access", K(ret), K_(status));
  } else if (ranges.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(ranges.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    if (!ranges.at(i).get_range().is_valid()) {
      STORAGE_LOG(WARN, "invalid range", "range", ranges.at(i));
    } else {
      if (OB_FAIL(macro_iter.open(*this, ranges.at(i)))) {
        STORAGE_LOG(WARN, "Fail to open macro block iter, ", K(ret), K(i), K(ranges.at(i)));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(macro_iter.get_next_macro_block(block_ctx))) {
            if (OB_ITER_END != ret) {
              STORAGE_LOG(WARN, "Fail to get next macro block, ", K(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(macro_blocks.push_back(block_ctx))) {
            STORAGE_LOG(WARN, "Fail to push block id to array, ", K(ret), K(block_ctx));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(end_block_idx_of_ranges.push_back(macro_blocks.count() - 1))) {
        STORAGE_LOG(WARN, "fail to push end block idx of ranges", K(ret));
      }
    }
  }

  return ret;
}

int ObSSTable::sort_ranges(const ObIArray<ObStoreRange>& ranges, ObIArray<ObStoreRange>& ordered_ranges)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 32> pos;
  ordered_ranges.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    int64_t j = 0;
    const ObStoreRange& range = ranges.at(i);
    for (j = 0; OB_SUCC(ret) && j < pos.count(); j++) {
      if ((range.get_start_key().simple_equal(ranges.at(pos.at(j)).get_start_key()) &&
              range.get_border_flag().inclusive_start() && !ranges.at(pos.at(j)).get_border_flag().inclusive_start()) ||
          (0 > range.get_start_key().compare(ranges.at(pos.at(j)).get_start_key()))) {
        break;
      }
    }

    if (j == pos.count()) {
      ret = pos.push_back(i);
    } else {

      int64_t prev = pos.at(j);
      pos.at(j) = i;
      for (int64_t k = j + 1; k < pos.count(); ++k) {
        // swap prev and k-th value
        prev = prev ^ pos.at(k);
        pos.at(k) = prev ^ pos.at(k);
        prev = prev ^ pos.at(k);
      }
      ret = pos.push_back(prev);
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < pos.count(); ++i) {
    ret = ordered_ranges.push_back(ranges.at(pos.at(i)));
  }

  return ret;
}

// Note: assuming inputs are ordered
int ObSSTable::remove_duplicate_ordered_block_id(
    const ObIArray<blocksstable::ObMacroBlockCtx>& origin_blocks, ObIArray<blocksstable::ObMacroBlockCtx>& blocks)
{
  int ret = OB_SUCCESS;
  blocks.reset();
  if (origin_blocks.count() > 0) {
    ret = blocks.push_back(origin_blocks.at(0));
    for (int64_t i = 1; OB_SUCC(ret) && i < origin_blocks.count(); ++i) {
      if (origin_blocks.at(i).get_macro_block_id() != origin_blocks.at(i - 1).get_macro_block_id()) {
        ret = blocks.push_back(origin_blocks.at(i));
      }
    }
  }
  return ret;
}

/*
  SPLIT the input ranges into group based on their corresponding MACROS

  allocator         IN			allocator used to deep copy ranges
  ranges            IN   		all input ranges
  type						  IN      decide the performance of this function
  macros_count		  OUT			the macros count belong to these ranges
  total_task_count  IN     	total_task_count
  splitted_ranges   OUT   	all output ranges
  split_index       OUT   	position of the last range of the group, one
                            for each group
 */
int ObSSTable::query_range_to_macros(ObIAllocator& allocator, const ObIArray<ObStoreRange>& ranges, const int64_t type,
    uint64_t* macros_count, const int64_t* total_task_count, ObIArray<ObStoreRange>* splitted_ranges,
    ObIArray<int64_t>* split_index)
{
  int ret = OB_SUCCESS;
  int64_t expected_task_count = 0;
  uint64_t tmp_count = 0;
  if ((sql::OB_GET_MACROS_COUNT_BY_QUERY_RANGE == type && macros_count == nullptr) ||
      (sql::OB_GET_BLOCK_RANGE == type &&
          (total_task_count == nullptr || splitted_ranges == nullptr || split_index == nullptr))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid params", K(type));
  } else if ((sql::OB_GET_BLOCK_RANGE == type) && FALSE_IT(expected_task_count = *total_task_count)) {

  } else if (ranges.count() >= 1) {
    ObSEArray<ObStoreRange, 32> ordered_ranges;
    if (OB_FAIL(sort_ranges(ranges, ordered_ranges))) {
      STORAGE_LOG(WARN, "failed to sort input ranges", K(ret));
    } else {

      ObSEArray<ObExtStoreRange, 32> ext_ranges;
      ObSEArray<blocksstable::ObMacroBlockCtx, 32> macro_blocks;

      // generate all ext_ranges
      for (int64_t i = 0; OB_SUCC(ret) && i < ordered_ranges.count(); ++i) {
        ObExtStoreRange ext_range(ordered_ranges.at(i));
        if (OB_FAIL(ext_range.to_collation_free_range_on_demand_and_cutoff_range(allocator))) {
          STORAGE_LOG(WARN, "failed to set start/end key", K(ret));
        } else if (OB_FAIL(ext_ranges.push_back(ext_range))) {
          STORAGE_LOG(WARN, "failed add ext_range", K(ret), K(ext_range));
        } else {
          STORAGE_LOG(DEBUG, "ext range added", K(ext_range), K(ext_ranges.at(ext_ranges.count() - 1)));
        }
      }

      if (OB_SUCC(ret)) {
        ObSEArray<int64_t, 32> end_block_idx_of_ranges;
        ObSEArray<blocksstable::ObMacroBlockCtx, 32> blocks;
        // find all macros
        if (OB_FAIL(find_macros(ext_ranges, macro_blocks, end_block_idx_of_ranges))) {
          STORAGE_LOG(WARN, "failed to find macros", K(ret), K(ext_ranges));
        } else if (OB_FAIL(remove_duplicate_ordered_block_id(macro_blocks, blocks))) {
          STORAGE_LOG(WARN, "failed to remove duplicate macro blocks", K(ret), K(macro_blocks));
        } else if (sql::OB_GET_MACROS_COUNT_BY_QUERY_RANGE == type) {
          tmp_count = blocks.count();
        } else if (sql::OB_GET_BLOCK_RANGE == type && expected_task_count <= 1) {
          if (1 == expected_task_count) {
            FOREACH_CNT_X(it, ranges, OB_SUCC(ret))
            {
              if (OB_FAIL(splitted_ranges->push_back(*it))) {
                LOG_WARN("push back ranges failed", K(ret));
              }
            }
            if (OB_SUCC(ret) && OB_FAIL(split_index->push_back(splitted_ranges->count() - 1))) {
              LOG_WARN("push back offsets failed", K(ret));
            }
          }
        } else if (sql::OB_GET_BLOCK_RANGE == type) {
          // number of macros for each splitted range
          ObSEArray<int64_t, 32> num_macros;

          if (blocks.count() < expected_task_count) {
            for (int64_t i = 0; OB_SUCC(ret) && i < blocks.count(); ++i) {
              ret = num_macros.push_back(1);
            }
          } else {
            // set the base number
            for (int64_t i = 0; OB_SUCC(ret) && i < expected_task_count; ++i) {
              if (OB_FAIL(num_macros.push_back((int)(blocks.count() / expected_task_count)))) {}
            }
            // add the remainder
            for (int64_t i = 0; OB_SUCC(ret) && i < blocks.count() % expected_task_count; ++i) {
              num_macros.at(i) += 1;
            }
          }

          STORAGE_LOG(DEBUG,
              "found macros",
              K(ret),
              K(ordered_ranges),
              "# of macros",
              blocks.count(),
              K(expected_task_count),
              K(num_macros));

          // split the range
          if (OB_SUCC(ret) && blocks.count() > 1) {

            // the last macro block for current group
            ObFullMacroBlockMeta end_block_meta;
            int64_t group_id = 0;
            int64_t curr_block_index = num_macros.at(group_id) - 1;

            // For each range, decide which group it belongs - split it
            // into multiple ranges if necessary.
            for (int64_t idx = 0; OB_SUCC(ret) && idx < ordered_ranges.count();) {

              if (curr_block_index >= blocks.count() || curr_block_index < 0) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(WARN, "invalid argument", K(curr_block_index), K(blocks));
              } else if (OB_FAIL(get_meta(blocks.at(curr_block_index).get_macro_block_id(), end_block_meta))) {
                STORAGE_LOG(WARN, "Fail to get macro block meta, ", K(ret));
              } else if (!end_block_meta.is_valid()) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(WARN, "The macro block meta is NULL, ", K(ret), K(end_block_meta));
              } else {
                ObStoreRowkey block_endkey(end_block_meta.meta_->endkey_, end_block_meta.meta_->rowkey_column_number_);
                ObStoreRange& cur_range = ordered_ranges.at(idx);

                if (cur_range.get_start_key().compare(block_endkey) > 0 &&
                    !(0 < idx && (end_block_idx_of_ranges.at(idx) == end_block_idx_of_ranges.at(idx - 1)))) {

                  // current range belongs to the next group
                  if (group_id >= num_macros.count() - 1) {
                    ret = OB_ERR_UNEXPECTED;
                    STORAGE_LOG(WARN,
                        "invalid group",
                        K(curr_block_index),
                        K(group_id),
                        K(num_macros),
                        K(cur_range),
                        K(block_endkey));
                  } else if (splitted_ranges->count() < 1) {
                    ret = OB_ERR_UNEXPECTED;
                    STORAGE_LOG(WARN, "invalid argument", K(splitted_ranges->count()));
                  } else if (OB_FAIL(split_index->push_back(splitted_ranges->count() - 1))) {
                    STORAGE_LOG(WARN, "failed to add split index", K(splitted_ranges->count()));
                  } else {
                    // move to the next group's end block
                    curr_block_index += num_macros.at(++group_id);
                  }
                } else if (  // first range and it has no blocks
                    -1 == end_block_idx_of_ranges.at(idx)
                    // non-first range and has no blocks
                    || (0 < idx && (end_block_idx_of_ranges.at(idx) == end_block_idx_of_ranges.at(idx - 1)))
                    // regular range within the boundary
                    || !(cur_range.get_end_key().compare(block_endkey) > 0)
                    // last block and range end key is larger - i.e. MAX
                    || (group_id == num_macros.count() - 1 && curr_block_index == blocks.count() - 1)) {

                  // current range is WITHIN the boundary of current macro
                  // block or has no blocks

                  ObStoreRange* copied_range = NULL;
                  if (OB_FAIL(deep_copy_range(allocator, cur_range, copied_range))) {
                    STORAGE_LOG(WARN, "failed to deep copy range", K(ret));
                  } else if (OB_ISNULL(copied_range)) {
                    ret = OB_ERR_UNEXPECTED;
                    STORAGE_LOG(WARN, "invalid copied range");
                  } else if (OB_FAIL(splitted_ranges->push_back(*copied_range))) {
                    STORAGE_LOG(WARN, "failed to add copied range");
                  } else {
                    // move to the next range
                    ++idx;
                    continue;
                  }
                } else if (group_id == num_macros.count() - 1) {
                  ret = OB_ERR_UNEXPECTED;
                  STORAGE_LOG(WARN,
                      "invalid group id",
                      K(end_block_idx_of_ranges),
                      K(cur_range.get_end_key()),
                      K(block_endkey),
                      K(num_macros),
                      K(group_id),
                      K(idx),
                      K(ordered_ranges));
                } else {

                  // current range is BEYOND the boundary of the current block
                  // split the current range into two parts, one within the
                  // boundary of the block and one beyond

                  // all macro block are left-open and right-close and the
                  // current range's start key is guaranteed to be larger than
                  // the start key of the current block's start. Therefore, we
                  // split the current ranges into two parts.
                  // The first part is from range's start key to block's
                  // end_key(inclusive) and the second is from block's
                  // end_key(exclusive) to range's end key.
                  ObStoreRange first_half;
                  first_half.set_table_id(cur_range.get_table_id());
                  first_half.set_border_flag(cur_range.get_border_flag());
                  first_half.set_right_closed();
                  first_half.set_start_key(cur_range.get_start_key());
                  first_half.set_end_key(block_endkey);

                  ObStoreRange second_half;
                  second_half.set_table_id(cur_range.get_table_id());
                  second_half.set_border_flag(cur_range.get_border_flag());
                  second_half.set_left_open();
                  second_half.set_start_key(block_endkey);
                  second_half.set_end_key(cur_range.get_end_key());

                  // move to the next group's end block
                  curr_block_index += num_macros.at(++group_id);

                  ObStoreRange* copied_first_half = NULL;
                  ObStoreRange* copied_second_half = NULL;
                  if (OB_FAIL(deep_copy_range(allocator, first_half, copied_first_half))) {
                    STORAGE_LOG(WARN, "failed to deep copy range", K(ret));
                  } else if (OB_FAIL(deep_copy_range(allocator, second_half, copied_second_half))) {
                    STORAGE_LOG(WARN, "failed to deep copy range", K(ret));
                  } else if (OB_ISNULL(copied_first_half) || OB_ISNULL(copied_second_half)) {
                    ret = OB_ERR_UNEXPECTED;
                    STORAGE_LOG(WARN, "invalid copied range", K(copied_first_half), K(copied_second_half));
                  } else if (OB_FAIL(splitted_ranges->push_back(*copied_first_half))) {
                    STORAGE_LOG(WARN, "failed to add splitted range", K(ret));
                  } else if (OB_FAIL(split_index->push_back(splitted_ranges->count() - 1))) {
                    STORAGE_LOG(WARN, "failed to add split index", K(splitted_ranges->count()));
                  } else {
                    // replace the current range with the second half
                    cur_range = *copied_second_half;
                    // note that we DID NOT increment idx
                    STORAGE_LOG(DEBUG,
                        "split macros",
                        K(splitted_ranges),
                        K(*copied_first_half),
                        K(*copied_second_half),
                        K(idx),
                        K(curr_block_index));
                  }
                }
              }
            }

            // last group
            if (OB_SUCC(ret)) {
              if (splitted_ranges->count() == 0) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(WARN, "failed to split ranges", K(ret));
              } else if (OB_FAIL(split_index->push_back(splitted_ranges->count() - 1))) {
                STORAGE_LOG(WARN, "failed to end splitting", K(ret));
              } else {
                STORAGE_LOG(DEBUG, "finish splitting ranges", K(splitted_ranges), K(split_index));
              }
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "undefined type", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && sql::OB_GET_MACROS_COUNT_BY_QUERY_RANGE == type) {
    *macros_count = tmp_count;
  }
  return ret;
}

int ObSSTable::prefix_exist(ObRowsInfo& rows_info, bool& may_exist)
{
  int ret = OB_SUCCESS;
  ObSSTableRowExister exister;
  if (!rows_info.is_valid() || rows_info.table_id_ != meta_.index_id_) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(rows_info), K(meta_.index_id_));
  } else if (rows_info.table_id_ != key_.table_id_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table id not match", K(ret), K(key_.table_id_), K(rows_info.table_id_), K(*this));
  } else if (meta_.macro_block_array_.count() == 0) {  // skip
  } else {
    const ObExtStoreRowkey& rowkey = rows_info.get_prefix_rowkey();
    if (OB_FAIL(exister.init(
            rows_info.exist_helper_.table_iter_param_, rows_info.exist_helper_.table_access_context_, this, &rowkey))) {
      STORAGE_LOG(WARN, "Failed to init sstable row exister", K(ret), K(rows_info));
    } else {
      const ObStoreRow* store_row = NULL;
      may_exist = false;
      if (OB_FAIL(exister.get_next_row(store_row))) {
        STORAGE_LOG(WARN, "Failed to get exist row", K(rowkey), K(ret));
      } else if (OB_ISNULL(store_row)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null store row", K(ret));
      } else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == store_row->flag_) {
      } else if (ObActionFlag::OP_DEL_ROW == store_row->flag_ || ObActionFlag::OP_ROW_EXIST == store_row->flag_) {
        may_exist = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected row flag", K(ret), K(store_row->flag_));
      }
      if (lib::is_diagnose_info_enabled()) {
        exister.report_stat();
      }
    }
  }

  return ret;
}

int ObSSTable::exist(ObRowsInfo& rows_info, bool& is_exist, bool& all_rows_found)
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta full_meta;
  is_exist = false;

  if (!rows_info.is_valid() || rows_info.table_id_ != meta_.index_id_) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(rows_info), K(meta_.index_id_));
  } else if (rows_info.table_id_ != key_.table_id_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table id not match", K(ret), K(key_.table_id_), K(rows_info.table_id_), K(*this));
  } else if (meta_.macro_block_array_.count() == 0) {  // skip
  } else if (rows_info.all_rows_found()) {
    all_rows_found = true;
  } else if (OB_FAIL(get_meta(meta_.macro_block_array_.at(meta_.macro_block_array_.count() - 1), full_meta))) {
    STORAGE_LOG(WARN, "fail to get meta", K(ret));
  } else if (!full_meta.is_valid()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "the macro block meta is not valid any more", K(ret));
  } else {
    const ObMacroBlockMetaV2* macro_meta = full_meta.meta_;
    ObStoreRowkey max_rowkey(macro_meta->endkey_, macro_meta->rowkey_column_number_);
    if (rows_info.get_min_rowkey().compare(max_rowkey) > 0) {  // skip
    } else if (OB_FAIL(rows_info.refine_ext_rowkeys(this))) {
      STORAGE_LOG(WARN, "Failed to refine ext rowkeys", K(rows_info), K(ret));
    } else if (rows_info.ext_rowkeys_.count() == 0) {  // skip
      STORAGE_LOG(INFO, "Skip unexpected empty ext_rowkeys", K(rows_info), K(ret));
      all_rows_found = true;
    } else {
      ObStoreRowIterator* iter = NULL;
      const ObStoreRow* store_row = NULL;
      is_exist = false;
      all_rows_found = false;

      if (OB_FAIL(build_multi_exist_iterator(rows_info, iter))) {
        LOG_WARN("failed to build exist iterator", K(ret));
      }

      for (int64_t i = 0; OB_SUCC(ret) && !is_exist && i < rows_info.ext_rowkeys_.count(); i++) {
        if (OB_FAIL(iter->get_next_row(store_row))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            STORAGE_LOG(WARN, "Failed to get next row", K(rows_info), K(rows_info.ext_rowkeys_.at(i)), K(ret));
          }
        } else if (OB_ISNULL(store_row)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected null store row", K(ret));
        } else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == store_row->flag_) {
        } else if (ObActionFlag::OP_DEL_ROW == store_row->flag_) {
          if (OB_FAIL(rows_info.clear_found_rowkey(i))) {
            STORAGE_LOG(WARN, "Failed to clear rowkey in rowsinfo", K(rows_info), K(i), K(ret));
          }
        } else if (ObActionFlag::OP_ROW_EXIST == store_row->flag_) {
          is_exist = true;
          all_rows_found = true;
          rows_info.get_duplicate_rowkey() = rows_info.ext_rowkeys_.at(i).get_store_rowkey();
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected row flag", K(ret), K(store_row->flag_));
        }

        if (OB_SUCC(ret) && !is_exist) {
          all_rows_found = rows_info.all_rows_found();
        }
      }
      if (OB_NOT_NULL(iter)) {
        if (lib::is_diagnose_info_enabled()) {
          iter->report_stat();
        }
        iter->~ObStoreRowIterator();
      }
    }
  }

  return ret;
}

int ObSSTable::exist(const ObStoreCtx& ctx, const uint64_t table_id, const common::ObStoreRowkey& rowkey,
    const common::ObIArray<share::schema::ObColDesc>& columns, bool& is_exist, bool& has_found)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_STORE_ROW_EXISTER);
  ObExtStoreRowkey ext_rowkey(rowkey);

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not ready for access", K(ret), K_(status));
  } else if (table_id != key_.table_id_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table id not match", K(ret), K(key_.table_id_), K(table_id), K(*this));
  } else if (!rowkey.is_valid() || columns.count() <= 0 || table_id != meta_.index_id_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "invalid arguments", K(ret), "column count", columns.count(), K(rowkey), K(table_id), K(meta_.index_id_));
  } else if (OB_FAIL(ext_rowkey.to_collation_free_on_demand_and_cutoff_range(allocator))) {
    STORAGE_LOG(WARN, "Failed to collation free on demand", K(ext_rowkey), K(ret));
  } else {
    ObTableIterParam iter_param;
    ObTableAccessContext access_context;
    ObBlockCacheWorkingSet block_cache_ws;
    common::ObVersionRange trans_version_range;
    common::ObQueryFlag query_flag;

    trans_version_range.base_version_ = 0;
    trans_version_range.multi_version_start_ = 0;
    trans_version_range.snapshot_version_ = EXIST_READ_SNAPSHOT_VERSION;
    query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;
    query_flag.read_latest_ = ObQueryFlag::OBSF_MASK_READ_LATEST;

    if (OB_FAIL(block_cache_ws.init(extract_tenant_id(meta_.index_id_)))) {
      STORAGE_LOG(WARN, "block_cache_ws init failed", K(ret), K(meta_.index_id_));
    } else if (OB_FAIL(
                   access_context.init(query_flag, ctx, allocator, allocator, block_cache_ws, trans_version_range))) {
      LOG_WARN("failed to init access context", K(ret), "pkey", key_.pkey_);
    } else {
      iter_param.table_id_ = meta_.index_id_;
      iter_param.schema_version_ = 0;
      iter_param.rowkey_cnt_ = rowkey.get_obj_cnt();
      iter_param.out_cols_ = &columns;
    }

    const ObStoreRow* store_row = NULL;
    is_exist = false;
    has_found = false;
    ObStoreRowIterator* iter = NULL;

    if (OB_SUCC(ret)) {
      if (OB_FAIL(build_exist_iterator(iter_param, access_context, ext_rowkey, iter))) {
        LOG_WARN("failed to build exist iterator", K(ret));
      } else if (OB_FAIL(iter->get_next_row(store_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else if (OB_ISNULL(store_row)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null store row", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == store_row->flag_) {
      } else if (ObActionFlag::OP_DEL_ROW == store_row->flag_) {
        has_found = true;
      } else if (ObActionFlag::OP_ROW_EXIST == store_row->flag_) {
        is_exist = true;
        has_found = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected row flag", K(ret), K(store_row->flag_));
      }
    } else if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }

    if (OB_NOT_NULL(iter)) {
      if (lib::is_diagnose_info_enabled()) {
        iter->report_stat();
      }
      iter->~ObStoreRowIterator();
    }
  }

  return ret;
}

int ObSSTable::clean_lob_column_checksum()
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta full_meta;
  ObBitSet<OB_ROW_MAX_COLUMNS_COUNT> lob_col_bitset;

  for (int64_t i = 0; OB_SUCC(ret) && i < meta_.macro_block_count_; ++i) {
    if (OB_FAIL(get_meta(meta_.macro_block_array_.at(i), full_meta))) {
      STORAGE_LOG(WARN, "fail to get meta", K(ret), K(meta_.macro_block_array_.at(i)));
    } else if (!full_meta.is_valid()) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "error sys, meta must not be null", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < full_meta.meta_->column_number_; j++) {
        if (!ob_is_large_text(full_meta.schema_->column_type_array_[j].get_type())) {
          // tinytext/tinyblob skip
        } else if (!lob_col_bitset.has_member(full_meta.schema_->column_id_array_[j]) &&
                   OB_FAIL(lob_col_bitset.add_member(full_meta.schema_->column_id_array_[j]))) {
          STORAGE_LOG(WARN,
              "Fail to lob column id to bitset",
              "column_id",
              full_meta.schema_->column_id_array_[j],
              K(j),
              K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && lob_col_bitset.num_members() > 0) {
    STORAGE_LOG(INFO, "Clean lob column checksum in sstable", K(ret));
    for (int64_t i = 0; i < meta_.column_cnt_; i++) {
      if (lob_col_bitset.has_member(meta_.column_metas_.at(i).column_id_)) {
        meta_.column_metas_.at(i).column_checksum_ = 0;
        meta_.column_metas_.at(i).column_default_checksum_ = 0;
      }
    }
  }

  return ret;
}

int ObSSTable::get_concurrent_cnt(int64_t tablet_size, int64_t& concurrent_cnt)
{
  int ret = OB_SUCCESS;
  const ObIArray<MacroBlockId>& macro_blocks = meta_.macro_block_array_;

  if (tablet_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "tablet size is invalid", K(tablet_size), K(ret));
  } else if (0 == tablet_size) {
    concurrent_cnt = 1;
  } else {
    const int64_t macro_block_size = OB_FILE_SYSTEM.get_macro_block_size();
    if (((macro_blocks.count() * macro_block_size + tablet_size - 1) / tablet_size) <= MAX_MERGE_THREAD) {
      concurrent_cnt = (macro_blocks.count() * macro_block_size + tablet_size - 1) / tablet_size;
      if (0 == concurrent_cnt) {
        concurrent_cnt = 1;
      }
    } else {
      int64_t macro_cnts = (macro_blocks.count() + MAX_MERGE_THREAD - 1) / MAX_MERGE_THREAD;
      concurrent_cnt = (macro_blocks.count() + macro_cnts - 1) / macro_cnts;
    }
  }
  return ret;
}

int ObSSTable::check_collation_free_valid()
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta full_meta;
  bool is_collation_free_valid = false;

  exist_invalid_collation_free_meta_ = false;
  for (int64_t i = 0; OB_SUCC(ret) && !exist_invalid_collation_free_meta_ && i < meta_.macro_block_array_.count();
       i++) {
    if (OB_FAIL(get_meta(meta_.macro_block_array_.at(i), full_meta))) {
      STORAGE_LOG(WARN, "fail to get meta", K(ret), K(meta_.macro_block_array_.at(i)));
    } else if (!full_meta.is_valid()) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "Unexpected null macro meta", K(ret), K(i));
    } else if (OB_FAIL(full_meta.meta_->check_collation_free_valid(is_collation_free_valid))) {
      STORAGE_LOG(WARN, "fail to check collation free is valid", K(ret), K(full_meta));
    } else if (!is_collation_free_valid) {
      exist_invalid_collation_free_meta_ = true;
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && !exist_invalid_collation_free_meta_ && i < meta_.lob_macro_block_array_.count();
       i++) {
    if (OB_FAIL(get_meta(meta_.lob_macro_block_array_.at(i), full_meta))) {
      STORAGE_LOG(WARN, "fail to get meta", K(ret), K(meta_.lob_macro_block_array_.at(i)));
    } else if (!full_meta.is_valid()) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "Unexpected null macro meta", K(ret), K(i));
    } else if (OB_FAIL(full_meta.meta_->check_collation_free_valid(is_collation_free_valid))) {
      STORAGE_LOG(WARN, "fail to check collation free is valid", K(ret), K(full_meta));
    } else if (!is_collation_free_valid) {
      exist_invalid_collation_free_meta_ = true;
    }
  }

  return ret;
}

int ObSSTable::get_table_stat(common::ObTableStat& stat)
{
  int ret = OB_SUCCESS;
  if (SSTABLE_READY_FOR_READ != status_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not initialized.", K(ret), K_(status));
  } else {
    stat.set_data_version(meta_.data_version_);
    stat.set_row_count(meta_.row_count_);
    stat.set_data_size(meta_.occupy_size_);
    if (0 == meta_.row_count_) {
      stat.set_average_row_size(0);
    } else {
      stat.set_average_row_size(meta_.occupy_size_ / meta_.row_count_);
    }
    stat.set_macro_blocks_num(meta_.macro_block_array_.count());
  }
  return ret;
}

int ObSSTable::get_frozen_schema_version(int64_t& schema_version) const
{
  int ret = OB_SUCCESS;
  if (SSTABLE_READY_FOR_READ != status_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "sstable is not initialized.", K(ret), K_(status));
  } else {
    schema_version = meta_.schema_version_;
  }
  return ret;
}

int ObSSTable::fill_old_meta_info(
    const int64_t schema_version, const int64_t step_merge_start_version, const int64_t step_merge_end_version)
{
  int ret = OB_SUCCESS;

  if (meta_.sstable_format_version_ != ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_1) {
    ret = OB_ERR_SYS;
    LOG_ERROR("only old version sstable can fill old meta info", K(ret), K(meta_));
  } else if (schema_version < 0 || step_merge_end_version < 0 || step_merge_start_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(schema_version), K(step_merge_end_version), K(step_merge_start_version));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.file_ctx_))) {
    LOG_WARN("failed to init file ctx", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.lob_file_ctx_))) {
    LOG_WARN("failed to init lob file ctx", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, meta_.bloom_filter_file_ctx_))) {
    LOG_WARN("Failed to init bloomfilter file ctx", K(ret));
  } else {
    meta_.sstable_format_version_ = ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_6;
    set_multi_version_rowkey_type(ObMultiVersionRowkeyHelpper::MVRC_OLD_VERSION);
    meta_.max_logic_block_index_ = 0;
    meta_.build_on_snapshot_ = 0;
    meta_.create_index_base_version_ = 0;
    meta_.schema_version_ = schema_version;
    meta_.progressive_merge_start_version_ = step_merge_start_version;
    meta_.progressive_merge_end_version_ = step_merge_end_version;
    meta_.create_snapshot_version_ = 0;
    meta_.bloom_filter_block_id_.reset();
    meta_.bloom_filter_block_id_in_files_ = 0;
  }
  return ret;
}

int ObSSTable::get_macro_max_row_count(int64_t& count)
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta full_meta;
  count = 0;
  if (meta_.row_count_ > 0 && meta_.macro_block_array_.count() > 1) {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_.macro_block_array_.count(); ++i) {
      if (OB_FAIL(get_meta(meta_.macro_block_array_.at(i), full_meta))) {
        STORAGE_LOG(WARN, "fail to get meta", K(ret), K(meta_.macro_block_array_.at(i)));
      } else if (!full_meta.is_valid()) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "error sys, macro meta must not be null", K(ret));
      } else if (count < full_meta.meta_->row_count_) {
        count = full_meta.meta_->row_count_;
      }
    }
  }
  return ret;
}

int ObSSTable::build_exist_iterator(const ObTableIterParam& iter_param, ObTableAccessContext& access_context,
    const ObExtStoreRowkey& ext_rowkey, ObStoreRowIterator*& iter)
{
  int ret = OB_SUCCESS;
  if (contain_uncommitted_row()) {
    if (OB_FAIL(get(iter_param, access_context, ext_rowkey, iter))) {
      LOG_WARN("failed to get row", K(ret), K(ext_rowkey));
    }
  } else {
    void* buf = NULL;
    ObSSTableRowExister* exister = NULL;
    if (NULL == (buf = access_context.allocator_->alloc(sizeof(ObSSTableRowExister)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
    } else {
      exister = new (buf) ObSSTableRowExister();
      if (OB_FAIL(exister->init(iter_param, access_context, this, &ext_rowkey))) {
        STORAGE_LOG(
            WARN, "Failed to init sstable row exister", K(ret), K(iter_param), K(access_context), K(ext_rowkey));
      } else {
        iter = exister;
      }
    }
  }

  return ret;
}

int ObSSTable::build_multi_exist_iterator(ObRowsInfo& rows_info, ObStoreRowIterator*& iter)
{
  int ret = OB_SUCCESS;
  if (contain_uncommitted_row()) {
    if (OB_FAIL(multi_get(rows_info.exist_helper_.table_iter_param_,
            rows_info.exist_helper_.table_access_context_,
            rows_info.ext_rowkeys_,
            iter))) {
      LOG_WARN("failed to get row", K(ret), K(rows_info.ext_rowkeys_));
    }
  } else {
    void* buf = NULL;
    ObSSTableRowMultiExister* multi_exister = NULL;
    if (NULL ==
        (buf = rows_info.exist_helper_.table_access_context_.allocator_->alloc(sizeof(ObSSTableRowMultiExister)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
    } else {
      multi_exister = new (buf) ObSSTableRowMultiExister();
      if (OB_FAIL(multi_exister->init(rows_info.exist_helper_.table_iter_param_,
              rows_info.exist_helper_.table_access_context_,
              this,
              &rows_info.ext_rowkeys_))) {
        STORAGE_LOG(WARN, "Failed to init sstable row exister", K(ret), K(rows_info));
      } else {
        iter = multi_exister;
      }
    }
  }

  return ret;
}

int ObSSTable::check_row_locked(const ObStoreCtx& ctx, const common::ObStoreRowkey& rowkey,
    const ObIArray<ObColDesc>& columns, ObStoreRowLockState& lock_state)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObISSTableRowIterator* row_iterator = NULL;
  const ObStoreRow* store_row = NULL;
  const int64_t read_snapshot = ctx.mem_ctx_->get_read_snapshot();
  LOG_DEBUG("check_row_locked", K(rowkey), K(has_uncommitted_trans()));
  transaction::ObTransService* trans_service = NULL;
  ObArenaAllocator allocator(ObModIds::OB_STORE_ROW_LOCK_CHECKER);
  lock_state.trans_version_ = 0;
  lock_state.is_locked_ = false;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObSSStore has not been inited, ", K(ret));
  } else if (get_upper_trans_version() <= read_snapshot) {
    // there is no lock at this sstable
    lock_state.trans_version_ = get_upper_trans_version();
  } else if (NULL == (buf = allocator.alloc(sizeof(ObSSTableRowLockChecker)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
  } else if (OB_ISNULL(trans_service = ObPartitionService::get_instance().get_trans_service())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else {
    row_iterator = new (buf) ObSSTableRowLockChecker();
    ObTableIterParam iter_param;
    ObTableAccessContext access_context;
    common::ObVersionRange trans_version_range;
    common::ObQueryFlag query_flag;

    trans_version_range.base_version_ = 0;
    trans_version_range.multi_version_start_ = 0;
    trans_version_range.snapshot_version_ = read_snapshot;
    query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;

    ObExtStoreRowkey ext_rowkey(rowkey);
    if (OB_FAIL(ext_rowkey.to_collation_free_on_demand_and_cutoff_range(allocator))) {
      TRANS_LOG(WARN, "failed to collation free on demand and cutoff", K(ret), K(ext_rowkey));
    } else if (OB_FAIL(access_context.init(query_flag, ctx, allocator, trans_version_range))) {
      LOG_WARN("failed to init access context", K(ret), "pkey", key_.pkey_);
    } else {
      iter_param.table_id_ = meta_.index_id_;
      iter_param.schema_version_ = 0;
      iter_param.rowkey_cnt_ = rowkey.get_obj_cnt();
      iter_param.out_cols_ = &columns;

      if (OB_FAIL(row_iterator->init(iter_param, access_context, this, &ext_rowkey))) {
        STORAGE_LOG(WARN, "failed to open getter, ", K(ret), K(iter_param), K(access_context), K(ext_rowkey));
      } else if (OB_SUCC(row_iterator->get_next_row(store_row))) {
        lock_state.trans_version_ = store_row->snapshot_version_;
        lock_state.is_locked_ = ((ObSSTableRowLockChecker*)row_iterator)->is_curr_row_locked();
        lock_state.lock_trans_id_ = ((ObSSTableRowLockChecker*)row_iterator)->get_lock_trans_id();
      }
    }
    row_iterator->~ObISSTableRowIterator();
  }

  return ret;
}

int ObSSTable::replay_add_macro_block_meta(const blocksstable::MacroBlockId& block_id, ObSSTable& other)
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaV2* meta = nullptr;
  const ObMacroBlockSchemaInfo* schema = nullptr;
  ObMacroBlockKey macro_key(get_key(), block_id);
  if (OB_UNLIKELY(!block_id.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid arguments", K(ret), K(block_id));
  } else if (OB_UNLIKELY(!other.file_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file handle is invalid", K(ret), K(other.file_handle_));
  } else if (OB_FAIL(other.file_handle_.get_storage_file()->get_replay_map().get(macro_key, meta))) {
    LOG_WARN("fail to get macro meta",
        K(ret),
        K(macro_key),
        K(block_id),
        K(macro_key.hash()),
        "tenant_id",
        other.file_handle_.get_storage_file()->get_tenant_id(),
        "file_id",
        other.file_handle_.get_storage_file()->get_file_id());
  } else if (OB_FAIL(other.schema_map_.get(meta->schema_version_, schema))) {
    LOG_WARN("fail to get schema map", K(ret), K(meta->schema_version_));
  } else {
    ObFullMacroBlockMeta full_meta;
    full_meta.meta_ = meta;
    full_meta.schema_ = schema;
    if (OB_FAIL(add_macro_block_meta(block_id, full_meta))) {
      LOG_WARN("fail to add macro meta", K(ret));
    }
  }
  return ret;
}

int ObSSTable::replay_add_macro_block_meta(
    const common::ObIArray<blocksstable::MacroBlockId>& macro_block_ids, ObSSTable& other)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_ids.count(); ++i) {
    if (OB_FAIL(replay_add_macro_block_meta(macro_block_ids.at(i), other))) {
      LOG_WARN("fail to replay add macro meta", K(ret));
    }
  }
  return ret;
}

int ObSSTable::get_file_handle_from_replay_module(const common::ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard pg_guard;
  ObIPartitionGroup* pg = nullptr;
  if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pg_key));
  } else if (OB_UNLIKELY(file_handle_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("file handle is not invalid", K(ret), K(file_handle_));
  } else if (nullptr == pt_replay_module_) {
    // do nothing
  } else if (OB_FAIL(pt_replay_module_->get_partition(pg_key, pg_guard))) {
    LOG_WARN("fail to get pg_guard", K(ret), K(pg_key));
  } else if (OB_ISNULL(pg = pg_guard.get_partition_group())) {
    ret = OB_ERR_SYS;
    LOG_WARN("fail to get pg", K(ret), K(pg_key));
  } else if (OB_FAIL(file_handle_.assign(pg->get_storage_file_handle()))) {
    LOG_WARN("fail to assign file handle", K(ret), K(pg->get_storage_file_handle()));
  }

  if (OB_FAIL(ret)) {
    pg_guard.reset();
  }
  return ret;
}

int ObSSTable::get_all_macro_info(ObIArray<ObMacroBlockInfoPair>& macro_infos)
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta full_meta;
  macro_infos.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < meta_.macro_block_array_.count(); ++i) {
    if (OB_FAIL(get_meta(meta_.macro_block_array_.at(i), full_meta))) {
      STORAGE_LOG(WARN, "fail to get meta", K(ret), K(meta_.macro_block_array_.at(i)));
    } else if (!full_meta.is_valid()) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "error sys, macro meta must not be null", K(ret));
    } else if (OB_FAIL(macro_infos.push_back(ObMacroBlockInfoPair(meta_.macro_block_array_.at(i), full_meta)))) {
      STORAGE_LOG(WARN, "fail to push back macro info", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < meta_.lob_macro_block_array_.count(); ++i) {
    if (OB_FAIL(get_meta(meta_.lob_macro_block_array_.at(i), full_meta))) {
      STORAGE_LOG(WARN, "fail to get meta", K(ret), K(meta_.lob_macro_block_array_.at(i)));
    } else if (!full_meta.is_valid()) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "error sys, macro meta must not be null", K(ret));
    } else if (OB_FAIL(macro_infos.push_back(ObMacroBlockInfoPair(meta_.lob_macro_block_array_.at(i), full_meta)))) {
      STORAGE_LOG(WARN, "fail to push back macro info", K(ret));
    }
  }

  if (OB_SUCC(ret) && meta_.bloom_filter_block_id_.is_valid()) {
    if (OB_FAIL(get_meta(meta_.bloom_filter_block_id_, full_meta))) {
      STORAGE_LOG(WARN, "fail to get meta", K(ret), K(meta_.bloom_filter_block_id_));
    } else if (!full_meta.is_valid()) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "error sys, meta must not be null", K(ret), K(meta_.bloom_filter_block_id_));
    } else if (OB_FAIL(macro_infos.push_back(ObMacroBlockInfoPair(meta_.bloom_filter_block_id_, full_meta)))) {
      STORAGE_LOG(WARN, "fail to push back macro info pair", K(ret));
    }
  }
  return ret;
}

int ObSSTable::convert_from_old_sstable(ObOldSSTable& src_sstable)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_SSTABLE);
  if (SSTABLE_NOT_INIT != status_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, sstable is not in init status", K(ret), K(status_));
  } else if (OB_UNLIKELY(!src_sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(src_sstable));
  } else if (OB_FAIL(init(src_sstable.get_key()))) {
    STORAGE_LOG(WARN, "fail to init sstable", K(ret), K(src_sstable));
  } else if (OB_FAIL(open(src_sstable.get_meta()))) {
    STORAGE_LOG(WARN, "fail to open sstable", K(ret), K(src_sstable));
  } else if (OB_FAIL(meta_.macro_block_array_.assign(src_sstable.get_meta().macro_block_array_))) {
    STORAGE_LOG(WARN, "fail to assign macro block array", K(ret));
  } else if (OB_FAIL(meta_.lob_macro_block_array_.assign(src_sstable.get_meta().lob_macro_block_array_))) {
    STORAGE_LOG(WARN, "fail to assign lob macro block array", K(ret));
  } else if (OB_FAIL(meta_.macro_block_idx_array_.assign(src_sstable.get_meta().macro_block_idx_array_))) {
    STORAGE_LOG(WARN, "fail to assign macro block idx array", K(ret));
  } else if (OB_FAIL(meta_.lob_macro_block_idx_array_.assign(src_sstable.get_meta().lob_macro_block_idx_array_))) {
    STORAGE_LOG(WARN, "fail to assign lob macro block idx array", K(ret));
  } else if (OB_FAIL(convert_add_macro_block_meta(meta_.macro_block_array_, allocator))) {
    STORAGE_LOG(WARN, "fail to convert add macro block meta", K(ret));
  } else if (OB_FAIL(convert_add_macro_block_meta(meta_.lob_macro_block_array_, allocator))) {
    STORAGE_LOG(WARN, "fail to convert add macro block meta", K(ret));
  } else {
    meta_.pg_key_ = src_sstable.get_meta().pg_key_;
    meta_.file_ctx_.reset();
    meta_.lob_file_ctx_.reset();
    meta_.bloom_filter_file_ctx_.reset();
    if (OB_FAIL(meta_.file_ctx_.assign(src_sstable.get_meta().file_ctx_))) {
      STORAGE_LOG(WARN, "fail to assign meta file ctx", K(ret));
    } else if (OB_FAIL(meta_.lob_file_ctx_.assign(src_sstable.get_meta().lob_file_ctx_))) {
      STORAGE_LOG(WARN, "fail to assign lob file ctx", K(ret));
    } else if (OB_FAIL(meta_.bloom_filter_file_ctx_.assign(src_sstable.get_meta().bloom_filter_file_ctx_))) {
      STORAGE_LOG(WARN, "fail to assign bloom filter file ctx", K(ret));
    } else {
      sstable_merge_info_ = src_sstable.get_sstable_merge_info();
      meta_.use_old_macro_block_count_ = src_sstable.get_meta().use_old_macro_block_count_;
      meta_.lob_use_old_macro_block_count_ = src_sstable.get_meta().lob_use_old_macro_block_count_;
      meta_.build_on_snapshot_ = src_sstable.get_meta().build_on_snapshot_;
      meta_.create_index_base_version_ = src_sstable.get_meta().create_index_base_version_;
      for (int64_t i = 0; i < meta_.column_cnt_; i++) {
        meta_.column_metas_.at(i).column_checksum_ = src_sstable.get_meta().column_metas_.at(i).column_checksum_;
      }
      for (int64_t i = 0; i < src_sstable.get_meta().new_column_metas_.count(); i++) {
        meta_.new_column_metas_.at(i).column_checksum_ =
            src_sstable.get_meta().new_column_metas_.at(i).column_checksum_;
      }
      meta_.bloom_filter_block_id_ = src_sstable.get_meta().bloom_filter_block_id_;
      meta_.bloom_filter_block_id_in_files_ = src_sstable.get_meta().bloom_filter_block_id_in_files_;
      if (meta_.bloom_filter_block_id_.is_valid() &&
          OB_FAIL(convert_add_macro_block_meta(meta_.bloom_filter_block_id_, allocator))) {
        LOG_WARN("failed to add bloomfilter macro block meta", K(meta_.bloom_filter_block_id_), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(close())) {
      LOG_WARN("failed to close sstable", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_macro_ref())) {
      LOG_WARN("fail to add macro ref", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to set sstable, clear dest sstable", K(ret), K(src_sstable), K(meta_.macro_block_array_));
    clear();
  }

  return ret;
}

int ObSSTable::add_macro_ref()
{
  int ret = OB_SUCCESS;
  ObStorageFile* file = NULL;
  if (!file_handle_.is_valid() && OB_FAIL(get_file_handle_from_replay_module(meta_.pg_key_))) {
    STORAGE_LOG(ERROR, "fail to get pg file from replay module", K(ret), K(meta_.pg_key_));
  } else if (OB_ISNULL(file = file_handle_.get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get file", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < meta_.macro_block_array_.count(); ++i) {
    if (OB_FAIL(file->inc_ref(meta_.macro_block_array_.at(i)))) {
      STORAGE_LOG(ERROR, "fail to inc ref of pg file", K(ret), K(*file), K(i), K(meta_.macro_block_array_.at(i)));
      for (int64_t j = i - 1; j >= 0; j--) {
        file->dec_ref(meta_.macro_block_array_.at(j));
      }
      meta_.macro_block_array_.reset();
      meta_.lob_macro_block_array_.reset();
      meta_.bloom_filter_block_id_.reset();
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < meta_.lob_macro_block_array_.count(); ++i) {
    if (OB_FAIL(file->inc_ref(meta_.lob_macro_block_array_.at(i)))) {
      STORAGE_LOG(ERROR, "fail to inc ref of pg file", K(ret), K(*file), K(i), K(meta_.lob_macro_block_array_.at(i)));
      for (int64_t j = i - 1; j >= 0; j--) {
        file->dec_ref(meta_.lob_macro_block_array_.at(j));
      }
      meta_.lob_macro_block_array_.reset();
      meta_.bloom_filter_block_id_.reset();
    }
  }

  if (OB_SUCC(ret) && meta_.bloom_filter_block_id_.is_valid()) {
    if (OB_FAIL(file->inc_ref(meta_.bloom_filter_block_id_))) {
      meta_.bloom_filter_block_id_.reset();
      STORAGE_LOG(ERROR, "fail to inc ref of pg file", K(ret), K(*file), K(meta_.bloom_filter_block_id_));
    }
  }

  return ret;
}

int ObSSTable::convert_add_macro_block_meta(const blocksstable::MacroBlockId& block_id, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  const ObMacroBlockMeta* meta = nullptr;
  ObMacroBlockMetaHandle meta_handle;
  ObMacroBlockMetaV2 new_meta;
  ObMacroBlockSchemaInfo new_schema;
  ObFullMacroBlockMeta full_meta;
  if (OB_UNLIKELY(!block_id.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid arguments", K(ret), K(block_id));
  } else if (OB_FAIL(ObMacroBlockMetaMgr::get_instance().get_old_meta(block_id, meta_handle))) {
    LOG_WARN("fail to get macro meta", K(ret), K(block_id));
  } else if (OB_ISNULL(meta = meta_handle.get_meta())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, meta must not be null", K(ret));
  } else if (OB_FAIL(full_meta.convert_from_old_macro_meta(*meta, allocator))) {
    LOG_WARN("fail to convert from old macro meta", K(ret));
  } else if (OB_FAIL(add_macro_block_meta(block_id, full_meta))) {
    LOG_WARN("fail to add macro meta", K(ret));
  }
  return ret;
}

int ObSSTable::convert_add_macro_block_meta(
    const common::ObIArray<blocksstable::MacroBlockId>& macro_block_ids, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_ids.count(); ++i) {
    if (OB_FAIL(convert_add_macro_block_meta(macro_block_ids.at(i), allocator))) {
      LOG_WARN("fail to replay add macro meta", K(ret));
    }
  }
  return ret;
}

int ObSSTable::serialize_schema_map(char* buf, int64_t data_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_i64(buf, data_len, pos, schema_map_.size()))) {
    STORAGE_LOG(WARN, "fail to serialize map count", K(ret));
  }
  hash::ObCuckooHashMap<int64_t, const ObMacroBlockSchemaInfo*>& map =
      const_cast<hash::ObCuckooHashMap<int64_t, const ObMacroBlockSchemaInfo*>&>(schema_map_);
  for (hash::ObCuckooHashMap<int64_t, const ObMacroBlockSchemaInfo*>::iterator iter = map.begin();
       OB_SUCC(ret) && iter != map.end();
       ++iter) {
    if (OB_FAIL(iter->second->serialize(buf, data_len, pos))) {
      STORAGE_LOG(WARN, "fail to serialize macro schema info", K(ret));
    }
  }
  return ret;
}

int ObSSTable::deserialize_schema_map(const char* buf, int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t schema_map_cnt = 0;
  if (OB_FAIL(schema_map_.create(DDL_VERSION_COUNT, &allocator_))) {
    STORAGE_LOG(WARN, "fail to create block meta map", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &schema_map_cnt))) {
    STORAGE_LOG(WARN, "fail to serialize map count", K(ret));
  } else if (schema_map_cnt > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < schema_map_cnt; ++i) {
      ObMacroBlockSchemaInfo schema;
      ObMacroBlockSchemaInfo* new_schema = nullptr;
      if (OB_FAIL(schema.deserialize(buf, data_len, pos))) {
        STORAGE_LOG(WARN, "fail to deseriiaze macro block schema info", K(ret));
      } else if (OB_FAIL(schema.deep_copy(new_schema, allocator_))) {
        STORAGE_LOG(WARN, "fail to deep copy schema info", K(ret));
      } else if (OB_FAIL(schema_map_.set(schema.schema_version_, new_schema))) {
        STORAGE_LOG(WARN, "fail to set schema map", K(ret));
      }
    }
  }
  return ret;
}

int64_t ObSSTable::get_schema_map_serialize_size() const
{
  int64_t serialize_size = 0;
  const int64_t schema_map_cnt = schema_map_.size();
  hash::ObCuckooHashMap<int64_t, const ObMacroBlockSchemaInfo*>& map =
      const_cast<hash::ObCuckooHashMap<int64_t, const ObMacroBlockSchemaInfo*>&>(schema_map_);
  serialize_size += serialization::encoded_length_i64(schema_map_cnt);
  for (hash::ObCuckooHashMap<int64_t, const ObMacroBlockSchemaInfo*>::iterator iter = map.begin(); iter != map.end();
       ++iter) {
    serialize_size += iter->second->get_serialize_size();
  }
  return serialize_size;
}

int ObSSTable::set_upper_trans_version(const int64_t upper_trans_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(meta_.set_upper_trans_version(std::max(upper_trans_version, get_snapshot_version())))) {
    LOG_WARN("failed to set upper trans version", K(ret), K(upper_trans_version), K(key_));
  } else {
    LOG_INFO("succeed to set upper trans version", K(key_), K(upper_trans_version), K(meta_.upper_trans_version_));
  }
  return ret;
}

}  // end namespace storage
}  // end namespace oceanbase
