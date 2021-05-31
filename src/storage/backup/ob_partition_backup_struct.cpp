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
#include "lib/utility/ob_tracepoint.h"
#include "lib/hash/ob_hashset.h"
#include "share/ob_force_print_log.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/ob_table_mgr.h"
#include "share/ob_debug_sync_point.h"
#include "storage/ob_storage_struct.h"
#include "share/backup/ob_backup_path.h"
#include "ob_partition_backup_struct.h"

namespace oceanbase {
using namespace common;
using namespace common::hash;
using namespace share;
using namespace blocksstable;
using namespace oceanbase::share::schema;

namespace storage {

/**********************ObBackupSSTableMetaInfo***********************/
OB_SERIALIZE_MEMBER(ObBackupSSTableMetaInfo, sstable_meta_, table_key_);

// TODO() need consider allocator memory
ObBackupSSTableMetaInfo::ObBackupSSTableMetaInfo()
    : allocator_(ObModIds::BACKUP), sstable_meta_(allocator_), table_key_()
{}

void ObBackupSSTableMetaInfo::reset()
{
  sstable_meta_.reset();
  allocator_.reset();
  table_key_.reset();
}

bool ObBackupSSTableMetaInfo::is_valid() const
{
  return sstable_meta_.is_valid() && table_key_.is_valid() && sstable_meta_.index_id_ == table_key_.table_id_;
}

int ObBackupSSTableMetaInfo::assign(const ObBackupSSTableMetaInfo& sstable_meta_info)
{
  int ret = OB_SUCCESS;
  reset();
  if (!sstable_meta_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "sstable meta info is invalid", K(ret), K(sstable_meta_info));
  } else if (OB_FAIL(sstable_meta_.assign(sstable_meta_info.sstable_meta_))) {
    STORAGE_LOG(WARN, "failed to assign base sstable meta", K(ret));
  } else {
    table_key_ = sstable_meta_info.table_key_;
  }
  return ret;
}

int ObBackupSSTableMetaInfo::set_sstable_meta_info(
    const ObSSTableBaseMeta& sstable_meta, const ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;
  reset();
  if (!sstable_meta.is_valid() || !table_key.is_valid() || sstable_meta.index_id_ != table_key.table_id_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "set sstable meta info get invalid argument", K(ret), K(sstable_meta), K(table_key));
  } else if (OB_FAIL(sstable_meta_.assign(sstable_meta))) {
    STORAGE_LOG(WARN, "failed to assign sstable meta", K(ret), K(sstable_meta));
  } else {
    table_key_ = table_key;
  }
  return ret;
}

/**********************ObBackupPartitionStoreMetaInfo***********************/
OB_SERIALIZE_MEMBER(ObBackupPartitionStoreMetaInfo, partition_store_meta_, sstable_meta_info_array_);

ObBackupPartitionStoreMetaInfo::ObBackupPartitionStoreMetaInfo() : partition_store_meta_(), sstable_meta_info_array_()
{}

void ObBackupPartitionStoreMetaInfo::reset()
{
  partition_store_meta_.reset();
  sstable_meta_info_array_.reset();
}

bool ObBackupPartitionStoreMetaInfo::is_valid() const
{
  bool b_ret = true;
  b_ret = partition_store_meta_.is_valid();
  if (b_ret) {
    for (int64_t i = 0; i < sstable_meta_info_array_.count() && b_ret; ++i) {
      const ObBackupSSTableMetaInfo& sstable_meta_info = sstable_meta_info_array_.at(i);
      b_ret = sstable_meta_info.is_valid();
    }
  }

  return b_ret;
}

int ObBackupPartitionStoreMetaInfo::assign(const ObBackupPartitionStoreMetaInfo& partition_store_meta_info)
{
  int ret = OB_SUCCESS;
  reset();
  if (!partition_store_meta_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "partition store meta info is invalid", K(ret), K(partition_store_meta_info));
  } else if (OB_FAIL(partition_store_meta_.assign(partition_store_meta_info.partition_store_meta_))) {
    STORAGE_LOG(WARN, "failed to assign partition store meta", K(ret), K(partition_store_meta_info));
  } else if (OB_FAIL(sstable_meta_info_array_.assign(partition_store_meta_info.sstable_meta_info_array_))) {
    STORAGE_LOG(WARN, "failed to assign partition store meta info", K(ret), K(partition_store_meta_info));
  }
  return ret;
}

/**********************ObBackupPGMetaInfo***********************/
OB_SERIALIZE_MEMBER(ObBackupPGMetaInfo, pg_meta_, partition_store_meta_info_array_);

ObBackupPGMetaInfo::ObBackupPGMetaInfo() : pg_meta_(), partition_store_meta_info_array_()
{}

void ObBackupPGMetaInfo::reset()
{
  pg_meta_.reset();
  partition_store_meta_info_array_.reset();
}

bool ObBackupPGMetaInfo::is_valid() const
{
  bool b_ret = true;
  b_ret = pg_meta_.is_valid();
  if (b_ret) {
    for (int64_t i = 0; i < partition_store_meta_info_array_.count() && b_ret; ++i) {
      const ObBackupPartitionStoreMetaInfo& partition_store_meta_info = partition_store_meta_info_array_.at(i);
      b_ret = partition_store_meta_info.is_valid();
    }
  }
  return b_ret;
}

/**********************ObBackupTableMacroIndex***********************/
OB_SERIALIZE_MEMBER(ObBackupTableMacroIndex, sstable_macro_index_, data_version_, data_seq_, backup_set_id_,
    sub_task_id_, offset_, data_length_);

ObBackupTableMacroIndex::ObBackupTableMacroIndex()
    : sstable_macro_index_(0),
      data_version_(0),
      data_seq_(0),
      backup_set_id_(0),
      sub_task_id_(0),
      offset_(0),
      data_length_(0),
      table_key_ptr_(NULL)
{}

void ObBackupTableMacroIndex::reset()
{
  sstable_macro_index_ = 0;
  data_version_ = 0;
  data_seq_ = 0;
  backup_set_id_ = 0;
  sub_task_id_ = 0;
  offset_ = 0;
  data_length_ = 0;
  table_key_ptr_ = NULL;
}

bool ObBackupTableMacroIndex::is_valid() const
{
  return sstable_macro_index_ >= 0 && data_version_ >= 0 && data_seq_ >= 0 && backup_set_id_ > 0 && sub_task_id_ >= 0 &&
         offset_ >= 0 && data_length_ > 0 && NULL != table_key_ptr_ && table_key_ptr_->is_valid();
}

/**********************ObBackupTableKeyIndex***********************/
OB_SERIALIZE_MEMBER(ObBackupTableKeyInfo, table_key_, total_macro_block_count_);

ObBackupTableKeyInfo::ObBackupTableKeyInfo() : table_key_(), total_macro_block_count_(0)
{}

void ObBackupTableKeyInfo::reset()
{
  table_key_.reset();
  total_macro_block_count_ = 0;
}

bool ObBackupTableKeyInfo::is_valid() const
{
  return table_key_.is_valid() && total_macro_block_count_ >= 0;
}

}  // namespace storage
}  // namespace oceanbase
