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
#include "ob_partition_base_data_ob_reader.h"
#include "ob_partition_group.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace blocksstable;

namespace storage {

int ObIPartitionMacroBlockReader::deserialize_macro_meta(
    char* buf, int64_t data_len, int64_t& pos, ObIAllocator& allocator, ObFullMacroBlockMeta& full_meta)
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaV2 tmp_meta;
  ObMacroBlockSchemaInfo tmp_schema_info;
  ObMacroBlockMetaV2* new_meta = nullptr;
  ObMacroBlockSchemaInfo* new_schema_info = nullptr;
  common::ObObj* endkey = NULL;
  ObFullMacroBlockMetaEntry meta_entry(tmp_meta, tmp_schema_info);
  full_meta.reset();
  if (OB_UNLIKELY(nullptr == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), KP(data_len));
  } else if (NULL ==
             (endkey = reinterpret_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * common::OB_MAX_COLUMN_NUMBER)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc endkey", K(ret));
  } else {
    endkey = new (endkey) ObObj[common::OB_MAX_COLUMN_NUMBER];
    tmp_meta.endkey_ = endkey;
    if (OB_FAIL(meta_entry.deserialize(buf, data_len, pos))) {
      LOG_WARN("fail to deserialize meta entry", K(ret));
    } else if (OB_FAIL(tmp_meta.deep_copy(new_meta, allocator))) {
      LOG_WARN("failed to deep copy tmp_meta", K(ret));
    } else if (NULL == new_meta) {
      ret = OB_ERR_SYS;
      LOG_ERROR("meta must not null", K(ret));
    } else if (OB_FAIL(tmp_schema_info.deep_copy(new_schema_info, allocator))) {
      LOG_WARN("fail to deep copy schema info", K(ret));
    } else {
      full_meta.meta_ = new_meta;
      full_meta.schema_ = new_schema_info;
    }
  }
  return ret;
}

void ObMigrateArgMacroBlockInfo::reset()
{
  fetch_arg_.reset();
  macro_block_id_.reset();
}

ObMigrateSSTableInfo::ObMigrateSSTableInfo()
    : is_inited_(false), pending_idx_(0), table_key_(), sstable_meta_(), macro_block_array_()
{}

int ObMigrateSSTableInfo::assign(const ObMigrateSSTableInfo& sstable_info)
{
  int ret = OB_SUCCESS;
  if (!sstable_info.is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "src sstable info do not init, can not assign", K(ret), K(sstable_info));
  } else {
    is_inited_ = sstable_info.is_inited_;
    pending_idx_ = sstable_info.pending_idx_;
    table_key_ = sstable_info.table_key_;
    if (OB_FAIL(sstable_meta_.assign(sstable_info.sstable_meta_))) {
      STORAGE_LOG(WARN, "fail to assign sstable meta", K(ret), K(sstable_info.sstable_meta_));
    } else if (OB_FAIL(macro_block_array_.assign(sstable_info.macro_block_array_))) {
      STORAGE_LOG(WARN, "fail to assign macro block array", K(ret), K(sstable_info.macro_block_array_.count()));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObLogicTableMeta, schema_version_);

ObSameVersionIncTable::ObSameVersionIncTable() : src_table_keys_(), pkey_(), version_(), table_id_(0)
{}

bool ObSameVersionIncTable::is_valid() const
{
  return src_table_keys_.count() > 0 && pkey_.is_valid() && version_.is_valid() && table_id_ > 0 &&
         table_id_ != OB_INVALID_ID;
}

void ObSameVersionIncTable::reset()
{
  src_table_keys_.reset();
  pkey_.reset();
  version_.reset();
  table_id_ = 0;
}

int ObSameVersionIncTable::assign(const ObSameVersionIncTable& same_version_inc_table)
{
  int ret = OB_SUCCESS;
  if (!same_version_inc_table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("same version inc table is invalid", K(ret), K(same_version_inc_table));
  } else if (OB_FAIL(src_table_keys_.assign(same_version_inc_table.src_table_keys_))) {
    LOG_WARN("failed to assign src table keys", K(ret), K(same_version_inc_table));
  } else {
    pkey_ = same_version_inc_table.pkey_;
    version_ = same_version_inc_table.version_;
    table_id_ = same_version_inc_table.table_id_;
  }
  return ret;
}

int64_t ObSameVersionIncTable::get_max_snapshot_version()
{
  int64_t max_snapshot_version = 0;
  if (src_table_keys_.count() > 0) {
    max_snapshot_version = src_table_keys_.at(src_table_keys_.count() - 1).trans_version_range_.snapshot_version_;
  }
  return max_snapshot_version;
}

int64_t ObSameVersionIncTable::get_min_base_version()
{
  int64_t min_base_version = 0;
  if (src_table_keys_.count() > 0) {
    min_base_version = src_table_keys_.at(0).trans_version_range_.snapshot_version_;
  }
  return min_base_version;
}

ObMigrateTableInfo::SSTableInfo::SSTableInfo() : src_table_key_(), dest_base_version_(-1), dest_log_ts_range_()
{}

bool ObMigrateTableInfo::SSTableInfo::is_valid() const
{
  bool bret = src_table_key_.is_valid() && dest_base_version_ >= 0 && dest_log_ts_range_.is_valid();
  if (OB_UNLIKELY(!bret)) {
  } else if (src_table_key_.is_major_sstable()) {
    if (src_table_key_.log_ts_range_.end_log_ts_ == ObLogTsRange::MIN_TS) {
      bret = dest_log_ts_range_.end_log_ts_ >= ObLogTsRange::MIN_TS;
    } else {
      bret = dest_log_ts_range_ == src_table_key_.log_ts_range_;
    }
  } else {
    // TODO support reuse local minor
  }
  return bret;
}

void ObMigrateTableInfo::SSTableInfo::reset()
{
  src_table_key_.reset();
  dest_base_version_ = -1;
  dest_log_ts_range_.reset();
}

ObMigrateTableInfo::ObMigrateTableInfo()
    : table_id_(OB_INVALID_ID), major_sstables_(), minor_sstables_(), multi_version_start_(0), ready_for_read_(false)
{}

ObMigrateTableInfo::~ObMigrateTableInfo()
{}

int ObMigrateTableInfo::assign(const ObMigrateTableInfo& info)
{
  int ret = OB_SUCCESS;

  if (OB_INVALID_ID != table_id_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot assign twice", K(ret), K(*this));
  } else if (OB_FAIL(major_sstables_.assign(info.major_sstables_))) {
    LOG_WARN("failed to assign major sstables", K(ret));
  } else if (OB_FAIL(minor_sstables_.assign(info.minor_sstables_))) {
    LOG_WARN("failed to assign minor sstables", K(ret));
  } else {
    table_id_ = info.table_id_;
    multi_version_start_ = info.multi_version_start_;
    ready_for_read_ = info.ready_for_read_;
  }
  return ret;
}

void ObMigrateTableInfo::reuse()
{
  table_id_ = OB_INVALID_ID;
  major_sstables_.reuse();
  minor_sstables_.reuse();
  multi_version_start_ = 0;
  ready_for_read_ = false;
}

ObMigratePartitionInfo::ObMigratePartitionInfo() : meta_(), table_infos_(), table_id_list_(), src_(), is_restore_(false)
{}

ObMigratePartitionInfo::~ObMigratePartitionInfo()
{}

int ObMigratePartitionInfo::assign(const ObMigratePartitionInfo& info)
{
  int ret = OB_SUCCESS;

  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot assign twice", K(ret));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(info));
  } else if (OB_FAIL(meta_.deep_copy(info.meta_))) {
    LOG_WARN("failed to deep copy meta", K(ret));
  } else if (OB_FAIL(table_infos_.assign(info.table_infos_))) {
    LOG_WARN("failed to assign table info", K(ret));
  } else if (OB_FAIL(table_id_list_.assign(info.table_id_list_))) {
    LOG_WARN("failed to assign table id list", K(ret));
  } else {
    src_ = info.src_;
    is_restore_ = info.is_restore_;
  }
  return ret;
}

// TODO() delete it later
bool ObMigratePartitionInfo::is_valid() const
{
  bool valid = true;
  // allow table_infos and table_id_list be empty for empty pg migrate

  if (!meta_.is_valid()) {
    valid = false;
  }
  return valid;
}

void ObMigratePartitionInfo::reset()
{
  meta_.reset();
  table_infos_.reset();
  table_id_list_.reset();
  src_.reset();
  is_restore_ = false;
}

}  // namespace storage
}  // namespace oceanbase
