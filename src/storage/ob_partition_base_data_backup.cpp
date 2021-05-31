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

#include "ob_partition_base_data_backup.h"
#include "observer/ob_server.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/restore/ob_storage.h"
#include "common/ob_smart_var.h"
#include "share/ob_tenant_mgr.h"
#include "storage/ob_partition_migrator.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_partition_migrator_table_key_mgr.h"
#include "storage/ob_sstable.h"
#include "storage/ob_pg_storage.h"
#include "storage/ob_file_system_util.h"

namespace oceanbase {
using namespace common;
using namespace common::hash;
using namespace share;
using namespace blocksstable;
using namespace lib;

namespace storage {

/******************ObBackupSSTableInfo*********************/

void ObBackupSSTableInfo::reset()
{
  sstable_meta_.reset();
  part_list_.reset();
}

bool ObBackupSSTableInfo::is_valid() const
{
  return sstable_meta_.is_valid() && sstable_meta_.get_total_macro_block_count() == part_list_.count();
}

int ObBackupSSTableInfo::assign(const ObBackupSSTableInfo& result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sstable_meta_.assign(result.sstable_meta_))) {
    STORAGE_LOG(WARN, "fail to copy sstable meta", K(ret), K(result));
  } else if (OB_FAIL(part_list_.assign(result.part_list_))) {
    STORAGE_LOG(WARN, "fail to assign part list", K(ret), K(result));
  }
  return ret;
}

/**********************ObBackupMacroBlockArg***********************/
ObBackupMacroBlockArg::ObBackupMacroBlockArg() : fetch_arg_(), table_key_ptr_(NULL), need_copy_(true)
{}

void ObBackupMacroBlockArg::reset()
{
  fetch_arg_.reset();
  table_key_ptr_ = NULL;
  need_copy_ = true;
}

bool ObBackupMacroBlockArg::is_valid() const
{
  return NULL != table_key_ptr_ && table_key_ptr_->is_valid();
}

/******************ObPartitionMetaBackupReader*********************/
ObPartitionMetaBackupReader::ObPartitionMetaBackupReader()
    : is_inited_(false),
      data_size_(0),
      sstable_info_array_(),
      table_key_array_(),
      arg_(NULL),
      meta_allocator_(ObModIds::BACKUP),
      table_count_(0),
      pkey_()
{}

int ObPartitionMetaBackupReader::init(const share::ObPhysicalBackupArg& arg, const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "already inited", K(ret));
  } else if (!arg.is_valid() || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "argument is invalid", K(ret), K(arg), K(pkey));
  } else if (OB_FAIL(read_partition_meta_info(pkey, arg.backup_snapshot_version_))) {
    STORAGE_LOG(WARN, "failed to read partition meta info", K(ret), K(pkey), K(arg));
  } else {
    pkey_ = pkey;
    arg_ = &arg;
    is_inited_ = true;
  }
  return ret;
}

int ObPartitionMetaBackupReader::read_all_sstable_metas()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sstable_info_array_.reserve(table_key_array_.count()))) {
    STORAGE_LOG(WARN, "failed to reserve sstable info", K(ret), K(table_key_array_.count()));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < table_key_array_.count(); ++i) {
      ObITable::TableKey& table_key = table_key_array_.at(i);
      ObBackupSSTableInfo sstable_info;
      if (OB_FAIL(read_sstable_meta(table_key, sstable_info))) {
        STORAGE_LOG(WARN, "failed to read sstable meta", K(ret), K(table_key));
      } else if (OB_FAIL(sstable_info_array_.push_back(sstable_info))) {
        STORAGE_LOG(WARN, "failed to push back to sstable_info_array_", K(ret), K(table_key));
      } else {
        data_size_ += sstable_info.part_list_.get_serialize_size() + sstable_info.sstable_meta_.get_serialize_size();
      }
    }
  }
  return ret;
}

int ObPartitionMetaBackupReader::read_sstable_meta(ObITable::TableKey& table_key, ObBackupSSTableInfo& sstable_info)
{
  int ret = OB_SUCCESS;
  ObTableHandle tmp_handle;
  ObSSTable* sstable = NULL;
  blocksstable::ObSSTablePair pair;
  blocksstable::ObMacroBlockMetaHandle meta_handle;
  ObFullMacroBlockMeta full_meta;

  if (OB_FAIL(ObPartitionService::get_instance().acquire_sstable(table_key, tmp_handle))) {
    STORAGE_LOG(WARN, "failed to get table", K(table_key), K(ret));
  } else if (OB_FAIL(tmp_handle.get_sstable(sstable))) {
    STORAGE_LOG(WARN, "failed to get table", K(table_key), K(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable should not be null here", K(ret));
  } else {
    const ObSSTable::ObSSTableGroupMacroBlocks& macro_list = sstable->get_total_macro_blocks();

    if (OB_FAIL(sstable_info.sstable_meta_.assign(sstable->get_meta()))) {
      STORAGE_LOG(WARN, "fail to assign sstable meta", K(ret));
    } else if (OB_FAIL(sstable_info.part_list_.reserve(macro_list.count()))) {
      STORAGE_LOG(WARN, "failed to reserve list", K(ret), K(macro_list.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_list.count(); ++i) {
      if (OB_FAIL(sstable->get_meta(macro_list.at(i), full_meta))) {
        STORAGE_LOG(ERROR, "Fail to get macro meta, ", K(ret), "macro_block_id", macro_list.at(i));
      } else if (!full_meta.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "meta must not null", K(ret), "macro_block_id", macro_list.at(i));
      } else {
        pair.data_seq_ = full_meta.meta_->data_seq_;
        pair.data_version_ = full_meta.meta_->data_version_;
        if (OB_FAIL(sstable_info.part_list_.push_back(pair))) {
          STORAGE_LOG(WARN, "failed to add pair list", K(ret));
        }
      }
    }
    if (OB_UNLIKELY(!sstable_info.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "sstable info is not valid", K(ret), K(sstable_info));
      sstable_info.reset();
    }
  }
  return ret;
}

int ObPartitionMetaBackupReader::read_sstable_meta(const uint64_t backup_index_id, ObSSTableBaseMeta& meta)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_INVALID_ID == backup_index_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "backup index id is invalid", K(ret), K(backup_index_id));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < sstable_info_array_.count(); ++i) {
      ObBackupSSTableInfo& sstable_info = sstable_info_array_.at(i);
      if (OB_LIKELY(!sstable_info.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "sstable info is invalid", K(ret));
      } else if (backup_index_id == sstable_info.sstable_meta_.index_id_) {
        if (OB_FAIL(meta.assign(sstable_info.sstable_meta_))) {
          STORAGE_LOG(WARN, "fail to assign sstable meta", K(ret));
        } else {
          found = true;
        }
      }
    }

    if (OB_SUCC(ret) && !found) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(
          WARN, "can not find backup index id in sstable meta", K(backup_index_id), K(sstable_info_array_.count()));
    }
  }

  return ret;
}

int ObPartitionMetaBackupReader::read_sstable_pair_list(
    const uint64_t backup_index_id, ObIArray<ObSSTablePair>& part_list)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_INVALID_ID == backup_index_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "index id is invalid", K(ret), K(backup_index_id));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < sstable_info_array_.count(); ++i) {
      ObBackupSSTableInfo& sstable_info = sstable_info_array_.at(i);
      if (OB_LIKELY(!sstable_info.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "sstable info is invalid", K(ret));
      } else if (backup_index_id == sstable_info.sstable_meta_.index_id_) {
        if (OB_FAIL(part_list.assign(sstable_info.part_list_))) {
          STORAGE_LOG(WARN, "fail to assign sstable meta", K(ret));
        } else {
          found = true;
        }
      }
    }

    if (OB_SUCC(ret) && !found) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "can not find backup index id", K(backup_index_id), K(sstable_info_array_.count()));
    }
  }

  return ret;
}

int ObPartitionMetaBackupReader::read_table_ids(ObIArray<uint64_t>& table_id_array)
{
  int ret = OB_SUCCESS;
  ObHashSet<uint64_t> table_ids_set;
  const bool overwrite_key = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "reader is not inited", K(ret));
  } else if (table_count_ <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "sstable meta array count is empty", K(ret), K(table_count_));
  } else if (OB_FAIL(table_ids_set.create(sstable_info_array_.count()))) {
    STORAGE_LOG(WARN, "failed to create table id set", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_info_array_.count(); ++i) {
      ObBackupSSTableInfo& sstable_info = sstable_info_array_.at(i);
      if (OB_FAIL(table_ids_set.set_refactored_1(
              static_cast<uint64_t>(sstable_info.sstable_meta_.index_id_), overwrite_key))) {
        STORAGE_LOG(WARN, "failed to set table id into set", K(ret), K(sstable_info));
      }
    }
    for (hash::ObHashSet<uint64_t>::const_iterator it = table_ids_set.begin();
         OB_SUCC(ret) && it != table_ids_set.end();
         it++) {
      if (OB_FAIL(table_id_array.push_back(it->first))) {
        STORAGE_LOG(WARN, "failed to push table id into array", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionMetaBackupReader::read_table_keys_by_table_id(
    const uint64_t table_id, ObIArray<ObITable::TableKey>& table_keys_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "reader is not inited", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "table id is invalid", K(ret), K(table_id));
  } else if (table_key_array_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "table_key_array count is unexpected", K(ret), K(table_key_array_.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_key_array_.count(); ++i) {
      ObITable::TableKey table_key = table_key_array_.at(i);
      if (table_id == table_key.table_id_) {
        if (OB_FAIL(table_keys_array.push_back(table_key))) {
          STORAGE_LOG(WARN, "fail to put table key into array", K(ret), K(table_key));
        }
      }
    }
  }
  return ret;
}

int ObPartitionMetaBackupReader::read_backup_metas(ObPGPartitionStoreMeta*& partition_store_meta,
    common::ObIArray<ObBackupSSTableInfo>*& sstable_info_array, common::ObArray<ObITable::TableKey>*& table_key_array)
{
  int ret = OB_SUCCESS;
  partition_store_meta = NULL;
  sstable_info_array = NULL;
  table_key_array = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "reader is not inited", K(ret));
  } else {
    partition_store_meta = &meta_info_.meta_;
    sstable_info_array = &sstable_info_array_;
    table_key_array = &table_key_array_;
  }
  return ret;
}

int ObPartitionMetaBackupReader::fetch_table_ids(hash::ObHashSet<uint64_t>& tables_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "reader is not inited", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < meta_info_.table_id_list_.count(); ++i) {
      const uint64_t table_id = meta_info_.table_id_list_.at(i);
      if (OB_FAIL(tables_ids.set_refactored(table_id))) {
        STORAGE_LOG(WARN, "append to hashset fail", K(ret), K(table_id));
      }
    }
  }
  return ret;
}

int ObPartitionMetaBackupReader::read_partition_meta_info(
    const ObPartitionKey& pkey, const int64_t backup_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObPGStorage* pg_storage = NULL;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition_group = NULL;
  ObPGPartitionStoreMeta partition_store_meta;
  ObTablesHandle handle;

  if (!pkey.is_valid() || backup_snapshot_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "get partition meta info with invalid argument",
        K(ret),
        K(pkey),
        KP(pg_storage),
        K(backup_snapshot_version));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_ISNULL(partition_group = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition group should not be NULL", K(ret), KP(partition_group));
  } else if (OB_ISNULL(pg_storage = &(partition_group->get_pg_storage()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failt to get pg partition storage", K(ret), KP(pg_storage));
  } else if (OB_FAIL(pg_storage->get_backup_partition_meta_data(
                 pkey, backup_snapshot_version, partition_store_meta, handle))) {
    STORAGE_LOG(WARN, "failed to get backup partition meta data", K(ret), K(backup_snapshot_version), K(pkey));
  } else if (OB_FAIL(prepare_partition_store_meta_info(partition_store_meta, handle))) {
    STORAGE_LOG(WARN, "failed to prepare partition store meta info", K(ret), K(pkey));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < handle.get_count(); ++i) {
      ObITable* table = handle.get_table(i);
      ObSSTable* sstable = NULL;
      ObBackupSSTableInfo sstable_info;
      if (OB_ISNULL(table) || table->is_memtable()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "backup table is unexpected", K(ret), KP(table));
      } else if (FALSE_IT(sstable = reinterpret_cast<ObSSTable*>(table))) {
      } else if (OB_FAIL(table_key_array_.push_back(table->get_key()))) {
        STORAGE_LOG(WARN, "failed to push table key into array", K(ret), K(*table));
      } else if (OB_FAIL(build_backup_sstable_info(sstable, sstable_info))) {
        STORAGE_LOG(WARN, "failed to build backup sstable info", K(ret), K(*table));
      } else if (OB_FAIL(sstable_info_array_.push_back(sstable_info))) {
        STORAGE_LOG(WARN, "failed to push backup sstable info into array ", K(ret), K(sstable_info));
      }
    }
  }
  return ret;
}

int ObPartitionMetaBackupReader::prepare_partition_store_meta_info(
    const ObPGPartitionStoreMeta& partition_store_meta, const ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObHashSet<uint64_t> table_ids_set;
  if (!partition_store_meta.is_valid() || handle.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "prepare partition store meta info get invalid argument", K(ret), K(partition_store_meta), K(handle));
  } else if (OB_FAIL(meta_info_.meta_.assign(partition_store_meta))) {
    STORAGE_LOG(WARN, "failed to assign partition store meta", K(ret), K(partition_store_meta));
  } else if (OB_FAIL(table_ids_set.create(handle.get_count()))) {
    STORAGE_LOG(WARN, "failed to create table ids set", K(ret), K(partition_store_meta), K(handle));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < handle.get_count(); ++i) {
      const ObITable* table = handle.get_table(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "table should not be NULL", K(ret), KP(table), K(handle));
      } else if (OB_FAIL(table_ids_set.set_refactored(table->get_key().table_id_))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "failed to set table id into set", K(ret), K(*table));
        }
      }
    }

    if (OB_SUCC(ret)) {
      for (hash::ObHashSet<uint64_t>::const_iterator it = table_ids_set.begin();
           OB_SUCC(ret) && it != table_ids_set.end();
           it++) {
        if (OB_FAIL(meta_info_.table_id_list_.push_back(it->first))) {
          STORAGE_LOG(WARN, "failed to push table id into array", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPartitionMetaBackupReader::build_backup_sstable_info(const ObSSTable* sstable, ObBackupSSTableInfo& sstable_info)
{
  int ret = OB_SUCCESS;
  sstable_info.reset();
  blocksstable::ObSSTablePair pair;
  blocksstable::ObMacroBlockMetaHandle meta_handle;
  ObFullMacroBlockMeta full_meta;

  if (OB_ISNULL(sstable)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "read sstable meta get invalid argument", K(ret), KP(sstable));
  } else if (OB_FAIL(sstable_info.sstable_meta_.assign(sstable->get_meta()))) {
    STORAGE_LOG(WARN, "failed to assign sstable meta", K(ret), K(*sstable));
  } else {
    const ObSSTable::ObSSTableGroupMacroBlocks& macro_list = sstable->get_total_macro_blocks();

    if (OB_FAIL(sstable_info.sstable_meta_.assign(sstable->get_meta()))) {
      STORAGE_LOG(WARN, "fail to assign sstable meta", K(ret));
    } else if (OB_FAIL(sstable_info.part_list_.reserve(macro_list.count()))) {
      STORAGE_LOG(WARN, "failed to reserve list", K(ret), K(macro_list.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_list.count(); ++i) {
      if (OB_FAIL(sstable->get_meta(macro_list.at(i), full_meta))) {
        STORAGE_LOG(ERROR, "Fail to get macro meta, ", K(ret), "macro_block_id", macro_list.at(i));
      } else if (!full_meta.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "meta must not null", K(ret), "macro_block_id", macro_list.at(i));
      } else {
        pair.data_seq_ = full_meta.meta_->data_seq_;
        pair.data_version_ = full_meta.meta_->data_version_;
        if (OB_FAIL(sstable_info.part_list_.push_back(pair))) {
          STORAGE_LOG(WARN, "failed to add pair list", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!sstable_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "sstable info is not valid", K(ret), K(sstable_info));
      } else {
        ++table_count_;
      }
    }

    if (OB_SUCC(ret)) {
      data_size_ += meta_info_.get_serialize_size();
    }
  }
  return ret;
}

int ObPartitionMetaBackupReader::read_table_info(const ObPartitionKey& pkey, const uint64_t table_id,
    const int64_t multi_version_start, const int64_t version, ObPGStorage* pg_storage,
    ObFetchTableInfoResult& table_info)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;

  if (!pkey.is_valid() || OB_ISNULL(pg_storage) || OB_INVALID_ID == table_id || multi_version_start < 0 ||
      version < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "get partition meta info get invalid argument",
        K(ret),
        K(pkey),
        KP(pg_storage),
        K(table_id),
        K(multi_version_start),
        K(version));
  } else if (OB_FAIL(pg_storage->get_partition_tables(pkey, table_id, handle, table_info.is_ready_for_read_))) {
    STORAGE_LOG(WARN, "fail to get partition tables", K(ret), K(pkey), K(table_id));
    // TODO use replay_log_ts to convert src table keys
  } else if (OB_FAIL(ObTableKeyMgrUtil::convert_src_table_keys(0,  // log_id
                 table_id,
                 true,
                 handle,
                 table_info.table_keys_))) {
    STORAGE_LOG(WARN, "failed to convert src table keys", K(ret), K(version), K(table_id), K(pkey), K(handle));
  } else {
    table_info.multi_version_start_ = multi_version_start;
    STORAGE_LOG(INFO, "succeed to fetch table info", K(pkey), K(table_info), K(handle));
  }
  return ret;
}

/*************************ObMacroBlockBackupSyncReader***************************/
ObMacroBlockBackupSyncReader::ObMacroBlockBackupSyncReader()
    : is_inited_(false),
      args_(NULL),
      allocator_(ObModIds::BACKUP),
      data_size_(0),
      result_code_(OB_SUCCESS),
      is_data_ready_(false),
      macro_arg_(),
      backup_index_tid_(0),
      meta_handle_(),
      full_meta_(),
      macro_handle_(),
      data_(),
      pkey_(),
      store_handle_(),
      sstable_(NULL)
{}

ObMacroBlockBackupSyncReader::~ObMacroBlockBackupSyncReader()
{
  reset();
}

void ObMacroBlockBackupSyncReader::reset()
{

  is_inited_ = false;
  args_ = NULL;
  allocator_.reset();
  data_size_ = 0;
  result_code_ = OB_SUCCESS;
  is_data_ready_ = false;
  macro_arg_.reset();
  backup_index_tid_ = 0;
  meta_handle_.reset();
  full_meta_.reset();
  macro_handle_.reset();
  data_.assign(NULL, 0, 0);
  pkey_.reset();
  store_handle_.reset();
  sstable_ = NULL;
}

int ObMacroBlockBackupSyncReader::init(const share::ObPhysicalBackupArg& args, const ObITable::TableKey& table_key,
    const obrpc::ObFetchMacroBlockArg& macro_arg)
{
  int ret = OB_SUCCESS;
  ObITable* table = NULL;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "has inited", K(ret));
  } else if (!args.is_valid() || macro_arg.data_seq_ < 0 || macro_arg.data_version_ < 0 || !table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "argument is invalid", K(ret), K(macro_arg), K(table_key), K(args));
  } else if (OB_FAIL(ObPartitionService::get_instance().acquire_sstable(table_key, store_handle_))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_MAJOR_SSTABLE_NOT_EXIST;
      STORAGE_LOG(WARN, "sstable may has been merged", K(ret));
    } else {
      STORAGE_LOG(WARN, "failed to get table", K(table_key), K(ret));
    }
  } else if (OB_ISNULL(table = store_handle_.get_table())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable should not be null here", K(ret));
  } else if (!table->is_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "table should be sstable", K(table), K(ret));
  } else {
    sstable_ = static_cast<ObSSTable*>(table);
    args_ = &args;
    allocator_.reset();
    is_data_ready_ = false;
    data_size_ = 0;
    macro_arg_ = macro_arg;
    backup_index_tid_ = table_key.table_id_;
    full_meta_.reset();
    data_.assign(NULL, 0, 0);
    result_code_ = OB_SUCCESS;
    pkey_ = table_key.pkey_;
    is_inited_ = true;
  }
  return ret;
}

int ObMacroBlockBackupSyncReader::process()
{
  int ret = OB_SUCCESS;
  ObStoragePath file_path;
  blocksstable::ObMacroBlockMeta tmp_meta;
  ObStorageUtil util(false /*need retry*/);
  data_size_ = 0;
  allocator_.reuse();
  blocksstable::ObMacroBlockReadInfo read_info;
  blocksstable::ObMacroBlockCtx macro_block_ctx;
  blocksstable::ObStorageFileHandle file_handle;
  blocksstable::ObStorageFile* file = NULL;

  const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K(*this));
  } else if (is_data_ready_) {
    STORAGE_LOG(INFO, "macro data is ready, no need fetch", K(*this));
  } else if (OB_FAIL(get_macro_read_info(macro_arg_, macro_block_ctx, read_info))) {
    STORAGE_LOG(WARN, "failed to get macro block meta", K(ret), K(macro_arg_));
  } else if (!full_meta_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, " macro meta must not null", K(full_meta_));
  } else if (OB_FAIL(file_handle.assign(macro_block_ctx.sstable_->get_storage_file_handle()))) {
    STORAGE_LOG(WARN, "fail to get file handle", K(ret), K(macro_block_ctx.sstable_));
  } else if (OB_ISNULL(file = file_handle.get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg file should not be null here", K(ret), K(pkey_));
  } else if (FALSE_IT(macro_handle_.set_file(file))) {
  } else if (OB_FAIL(file->read_block(read_info, macro_handle_))) {
    STORAGE_LOG(WARN, "Fail to read macro block", K(ret), K(pkey_));
  } else if (!macro_handle_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "read handle is not valid, cannot wait", K(ret), K(macro_arg_));
  } else if (OB_FAIL(macro_handle_.wait(io_timeout_ms))) {
    STORAGE_LOG(WARN, "failed to wait read handle", K(ret));
  } else if (macro_handle_.get_data_size() != full_meta_.meta_->occupy_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("read data size not match", K(ret), K(macro_handle_), K(full_meta_));
  } else {
    data_.assign(macro_handle_.get_buffer(), macro_handle_.get_data_size(), full_meta_.meta_->occupy_size_);
    STORAGE_LOG(INFO, "fetch macro block data", K(macro_arg_), K(full_meta_));
  }

  if (OB_SUCC(ret) && !is_data_ready_) {
    data_size_ += full_meta_.meta_->occupy_size_;
    is_data_ready_ = true;
  } else {
    result_code_ = ret;
  }
  return ret;
}

int ObMacroBlockBackupSyncReader::get_macro_block_meta(
    blocksstable::ObFullMacroBlockMeta& meta, blocksstable::ObBufferReader& data)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K(*this));
  } else if (OB_FAIL(process())) {
    STORAGE_LOG(WARN, "failed to fetch macro data", K(ret), K(*this));
  } else if (OB_FAIL(result_code_)) {
    STORAGE_LOG(WARN, "error happened during fetch macro block", K(ret), K(*this));
  } else if (!is_data_ready_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "copy macro block is success, but data is not ready", K(ret), K(*this));
  } else {
    meta = full_meta_;
    data = data_;
  }
  return ret;
}

int ObMacroBlockBackupSyncReader::get_macro_read_info(const obrpc::ObFetchMacroBlockArg& arg,
    blocksstable::ObMacroBlockCtx& macro_block_ctx, blocksstable::ObMacroBlockReadInfo& read_info)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (arg.macro_block_index_ < 0 || arg.macro_block_index_ >= sstable_->get_total_macro_blocks().count()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN,
        "macro block index is out of range",
        K(ret),
        K(arg),
        "sstable macro_block_count",
        sstable_->get_total_macro_blocks().count());
  } else if (OB_FAIL(sstable_->get_combine_macro_block_ctx(arg.macro_block_index_, macro_block_ctx))) {
    STORAGE_LOG(WARN, "Failed to get combined_macro_block_ctx", K(ret), K(arg));
  } else if (OB_FAIL(sstable_->get_meta(macro_block_ctx.get_macro_block_id(), full_meta_))) {
    STORAGE_LOG(WARN, "Fail to get macro block meta, ", K(ret));
  } else if (OB_LIKELY(full_meta_.meta_->data_seq_ != arg.data_seq_ ||
                       full_meta_.meta_->data_version_ != arg.data_version_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "macro_meta version not match arg", K(ret), K(full_meta_), K(arg));
  } else {
    read_info.macro_block_ctx_ = &macro_block_ctx;
    read_info.offset_ = 0;
    read_info.size_ = full_meta_.meta_->occupy_size_;
    read_info.io_desc_.category_ = SYS_IO;
    read_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_MIGRATE_READ;
  }
  return ret;
}

/**************************ObPartitionBaseDataMetaBackupReader**************************/
ObPartitionBaseDataMetaBackupReader::ObPartitionBaseDataMetaBackupReader()
    : is_inited_(false),
      pkey_(),
      backup_arg_(NULL),
      reader_(),
      allocator_(ObModIds::OB_PARTITION_MIGRATOR),
      last_read_size_(0),
      partition_store_meta_(),
      snapshot_version_(0),
      schema_version_(0),
      data_version_(0)
{}

ObPartitionBaseDataMetaBackupReader::~ObPartitionBaseDataMetaBackupReader()
{}

int ObPartitionBaseDataMetaBackupReader::init(
    const common::ObPartitionKey& pkey, const ObDataStorageInfo& data_info, const ObPhysicalBackupArg& backup_arg)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || !backup_arg.is_valid() || !data_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(pkey), K(backup_arg), K(data_info));
  } else if (OB_FAIL(reader_.init(backup_arg, pkey))) {
    STORAGE_LOG(WARN, "failed to init meta reader", K(ret), K(pkey));
  } else if (OB_FAIL(prepare(pkey, data_info))) {
    STORAGE_LOG(WARN, "fail to prepare ", K(ret), K(pkey));
  } else {
    is_inited_ = true;
    pkey_ = pkey;
    backup_arg_ = &backup_arg;
    last_read_size_ = 0;
    data_version_ = backup_arg.backup_data_version_;
    schema_version_ = backup_arg.backup_schema_version_;
    snapshot_version_ = data_version_;
    STORAGE_LOG(INFO, "succeed to init backup reader", K(pkey), K(backup_arg));
  }

  return ret;
}

int ObPartitionBaseDataMetaBackupReader::fetch_partition_meta(ObPGPartitionStoreMeta& partition_store_meta)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(partition_store_meta.deep_copy(partition_store_meta_))) {
    STORAGE_LOG(WARN, "fail to copy partition store meta", K(ret), K(partition_store_meta_));
  } else {
    STORAGE_LOG(INFO, "succeed to fetch partition meta", K(partition_store_meta));
  }
  return ret;
}

int ObPartitionBaseDataMetaBackupReader::fetch_sstable_meta(
    const uint64_t index_id, blocksstable::ObSSTableBaseMeta& sstable_meta)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == index_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "index id is invalid", K(ret));
  } else if (OB_FAIL(reader_.read_sstable_meta(index_id, sstable_meta))) {
    STORAGE_LOG(WARN, "failed to get sstable meta", K(ret), K(index_id));
  }
  return ret;
}

int ObPartitionBaseDataMetaBackupReader::fetch_sstable_pair_list(
    const uint64_t index_id, common::ObIArray<blocksstable::ObSSTablePair>& pair_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (index_id == 0 || common::OB_INVALID_ID == index_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "index id is invalid", K(ret), K(index_id));
  } else if (OB_FAIL(reader_.read_sstable_pair_list(index_id, pair_list))) {
    STORAGE_LOG(WARN, "fail to read sstable pair list", K(ret), K(index_id));
  }
  return ret;
}

int ObPartitionBaseDataMetaBackupReader::prepare(const common::ObPartitionKey& pkey, const ObDataStorageInfo& data_info)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition_group = NULL;

  UNUSED(data_info);
  if (OB_UNLIKELY(!reader_.is_inited())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionMetaBackupReader do not init", K(ret));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pkey, guard)) ||
             OB_ISNULL(partition_group = guard.get_partition_group())) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_FAIL(partition_group->get_pg_storage().get_pg_partition_store_meta(pkey, partition_store_meta_))) {
    STORAGE_LOG(WARN, "fail to get partition store meta", K(ret));
  }
  return ret;
}

int ObPartitionBaseDataMetaBackupReader::fetch_all_table_ids(common::ObIArray<uint64_t>& table_id_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || !reader_.is_inited())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "reader do not init", K(ret));
  } else if (OB_FAIL(reader_.read_table_ids(table_id_array))) {
    STORAGE_LOG(WARN, "fail to read table ids", K(ret));
  }
  return ret;
}

int ObPartitionBaseDataMetaBackupReader::fetch_table_keys(
    const uint64_t index_id, obrpc::ObFetchTableInfoResult& table_res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(reader_.read_table_keys_by_table_id(index_id, table_res.table_keys_))) {
    STORAGE_LOG(WARN, "fail to get table keys", K(ret));
  }

  if (OB_SUCC(ret)) {
    table_res.is_ready_for_read_ = true;
    table_res.multi_version_start_ = ObTimeUtility::current_time();
  }

  return ret;
}

/**************************ObPhysicalBaseMetaBackupReader**************************/

ObPhysicalBaseMetaBackupReader::ObPhysicalBaseMetaBackupReader()
    : is_inited_(false),
      restore_info_(NULL),
      reader_(NULL),
      allocator_(common::ObModIds::BACKUP),
      pkey_(),
      table_id_(common::OB_INVALID_ID)
{}

int ObPhysicalBaseMetaBackupReader::init(ObRestoreInfo& restore_info, const ObPartitionKey& pkey,
    const uint64_t table_id, ObPartitionGroupMetaBackupReader& reader)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "can not init twice", K(ret));
  } else if (0 == table_id || OB_INVALID_ID == table_id || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "table is is invalid", K(ret), K(table_id), K(pkey));
  } else {
    is_inited_ = true;
    restore_info_ = &restore_info;
    table_id_ = table_id;
    pkey_ = pkey;
    reader_ = &reader;
  }
  return ret;
}

int ObPhysicalBaseMetaBackupReader::fetch_sstable_meta(ObSSTableBaseMeta& sstable_meta)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "physical base meta restore reader not init", K(ret));
  } else if (OB_FAIL(reader_->fetch_sstable_meta(table_id_, pkey_, sstable_meta))) {
    STORAGE_LOG(WARN, "fail to get sstable meta", K(ret), K(table_id_));
  } else if (OB_UNLIKELY(!sstable_meta.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable meta is invalid", K(ret), K(sstable_meta));
  }
  return ret;
}

int ObPhysicalBaseMetaBackupReader::fetch_macro_block_list(ObIArray<ObSSTablePair>& macro_block_list)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "physical base meta restore reader not init", K(ret));
  } else if (OB_FAIL(reader_->fetch_sstable_pair_list(table_id_, pkey_, macro_block_list))) {
    STORAGE_LOG(WARN, "fail to fetch sstable pair list", K(ret), K(table_id_));
  }
  return ret;
}

/**************************ObPartitionMacroBlockBackupReader**************************/

ObPartitionMacroBlockBackupReader::ObPartitionMacroBlockBackupReader()
    : is_inited_(false), macro_list_(), macro_idx_(0), allocator_(ObModIds::BACKUP), readers_(), read_size_(0)
{}

ObPartitionMacroBlockBackupReader::~ObPartitionMacroBlockBackupReader()
{
  for (int64_t i = 0; i < readers_.count(); ++i) {
    if (NULL != readers_.at(i)) {
      readers_.at(i)->~ObMacroBlockBackupSyncReader();
      allocator_.free(readers_.at(i));
      readers_.at(i) = NULL;
    }
  }
  is_inited_ = false;
}

int ObPartitionMacroBlockBackupReader::init(
    const ObPhysicalBackupArg& backup_arg, const ObIArray<ObBackupMacroBlockArg>& list)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_UNLIKELY(!backup_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(backup_arg));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
      ObMacroBlockBackupSyncReader* reader = NULL;
      const ObBackupMacroBlockArg& macro_arg = list.at(i);
      void* buf = NULL;
      if (!macro_arg.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "backup macro block arg is invalid", K(ret), K(macro_arg));
      } else if (!macro_arg.need_copy_) {
        STORAGE_LOG(DEBUG, "backup macro block do not need copy", K(macro_arg), K(i));
      } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMacroBlockBackupSyncReader)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc buf", K(ret));
      } else if (OB_ISNULL(reader = new (buf) ObMacroBlockBackupSyncReader())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc reader", K(ret));
      } else if (OB_FAIL(readers_.push_back(reader))) {
        STORAGE_LOG(WARN, "failed to push back reader", K(ret));
        reader->~ObMacroBlockBackupSyncReader();
      } else if (OB_FAIL(
                     schedule_macro_block_task(backup_arg, macro_arg.fetch_arg_, *macro_arg.table_key_ptr_, *reader))) {
        STORAGE_LOG(WARN, "failed to schedule macro block dag", K(ret), K(i));
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
      macro_idx_ = 0;
      read_size_ = 0;
    }
  }
  return ret;
}

int ObPartitionMacroBlockBackupReader::schedule_macro_block_task(const ObPhysicalBackupArg& backup_arg,
    const obrpc::ObFetchMacroBlockArg& arg, const ObITable::TableKey& table_key, ObMacroBlockBackupSyncReader& reader)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((is_inited_))) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_FAIL(macro_list_.push_back(arg))) {
    STORAGE_LOG(WARN, "failed to copy macro list", K(ret));
  } else if (OB_FAIL(reader.init(backup_arg, table_key, arg))) {
    STORAGE_LOG(WARN, "failed to copy macro list", K(ret));
  }

  return ret;
}

int ObPartitionMacroBlockBackupReader::get_next_macro_block(blocksstable::ObFullMacroBlockMeta& meta,
    blocksstable::ObBufferReader& data, blocksstable::MacroBlockId& src_macro_id)
{
  int ret = OB_SUCCESS;
  src_macro_id.reset();

  if (macro_idx_ > 0 && macro_idx_ < readers_.count()) {
    const int64_t last_macro_idx = macro_idx_ - 1;
    if (NULL != readers_.at(last_macro_idx)) {
      readers_.at(last_macro_idx)->reset();
    }
  }

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (macro_idx_ >= readers_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(readers_.at(macro_idx_)->get_macro_block_meta(meta, data))) {
    STORAGE_LOG(WARN, "failed to read macro block meta", K(ret), K(macro_idx_), K(readers_.count()));
  } else {
    read_size_ += readers_.at(macro_idx_)->get_data_size();
    ++macro_idx_;
  }
  return ret;
}

/**************************ObPartitionGroupMetaBackupReader**************************/

ObPartitionGroupMetaBackupReader::ObPartitionGroupMetaBackupReader()
    : is_inited_(false), pg_key_(), backup_arg_(NULL), last_read_size_(0), allocator_()
{}

ObPartitionGroupMetaBackupReader::~ObPartitionGroupMetaBackupReader()
{
  for (auto it = partition_reader_map_.begin(); it != partition_reader_map_.end(); ++it) {
    ObPartitionBaseDataMetaBackupReader* reader = it->second;
    if (nullptr != reader) {
      reader->~ObPartitionBaseDataMetaBackupReader();
    }
  }
}

int ObPartitionGroupMetaBackupReader::init(const ObPartitionGroupMeta& pg_meta, const ObPhysicalBackupArg& backup_arg)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_UNLIKELY(!pg_meta.is_valid() || !backup_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(pg_meta), K(backup_arg));
  } else if (OB_FAIL(prepare(pg_meta, backup_arg))) {
    STORAGE_LOG(WARN, "failed to prepare", K(ret), K(pg_meta));
  } else {
    is_inited_ = true;
    pg_key_ = pg_meta.pg_key_;
    backup_arg_ = &backup_arg;
    last_read_size_ = 0;
  }

  return ret;
}

int ObPartitionGroupMetaBackupReader::prepare(
    const ObPartitionGroupMeta& pg_meta, const ObPhysicalBackupArg& backup_arg)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObPGPartitionGuard pg_partition_guard;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObPartitionGroupMetaBackupReader is already init", K(ret));
  } else {
    const ObPartitionArray& partitions = pg_meta.partitions_;
    const ObDataStorageInfo& data_info = pg_meta.storage_info_.get_data_info();
    if (0 == partitions.count()) {
      STORAGE_LOG(INFO, "empty pg, no partitions");
    } else if (OB_FAIL(partition_reader_map_.create(partitions.count(), ObModIds::BACKUP))) {
      STORAGE_LOG(WARN, "failed to create partition reader map", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
        const ObPartitionKey& pkey = partitions.at(i);
        ObPartitionBaseDataMetaBackupReader* reader = NULL;
        void* buf = NULL;
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObPartitionBaseDataMetaBackupReader)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc buf", K(ret));
        } else if (OB_ISNULL(reader = new (buf) ObPartitionBaseDataMetaBackupReader())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc reader", K(ret));
        } else if (OB_FAIL(reader->init(pkey, data_info, backup_arg))) {
          STORAGE_LOG(WARN, "failed to init pg partition base data backup reader", K(ret), K(pkey));
        } else if (OB_FAIL(partition_reader_map_.set_refactored(pkey, reader))) {
          STORAGE_LOG(WARN, "failed to set reader into map", K(ret), K(pkey));
        } else {
          reader = NULL;
        }

        if (NULL != reader) {
          reader->~ObPartitionBaseDataMetaBackupReader();
        }
      }
    }
  }
  return ret;
}

int ObPartitionGroupMetaBackupReader::get_partition_readers(
    const ObPartitionArray& partitions, ObIArray<ObPartitionBaseDataMetaBackupReader*>& partition_reader_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition group meta backup reader do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
      const ObPartitionKey& pkey = partitions.at(i);
      ObPartitionBaseDataMetaBackupReader* reader = NULL;
      if (OB_FAIL(partition_reader_map_.get_refactored(pkey, reader))) {
        STORAGE_LOG(WARN, "failed to get partition base data meta backup reader from map", K(ret), K(pkey));
      } else if (OB_ISNULL(reader)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition base data meta backup reader should not be NULL", K(ret), K(pkey), KP(reader));
      } else if (OB_FAIL(partition_reader_array.push_back(reader))) {
        STORAGE_LOG(WARN, "failed to push reader into partition reader array", K(ret), K(pkey));
      }
    }
  }
  return ret;
}

int ObPartitionGroupMetaBackupReader::fetch_sstable_meta(
    const uint64_t index_id, const ObPartitionKey& pkey, blocksstable::ObSSTableBaseMeta& sstable_meta)
{
  int ret = OB_SUCCESS;
  ObPartitionBaseDataMetaBackupReader* reader = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_INVALID_ID == index_id || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "index id is invalid", K(ret), K(pkey), K(index_id));
  } else if (OB_FAIL(partition_reader_map_.get_refactored(pkey, reader))) {
    STORAGE_LOG(WARN, "failed to get partition base meta backup reader", K(ret), K(pkey));
  } else if (OB_ISNULL(reader)) {
    STORAGE_LOG(WARN, "partition base meta backup reader should not be NULL", K(ret), K(pkey), KP(reader));
  } else if (OB_FAIL(reader->fetch_sstable_meta(index_id, sstable_meta))) {
    STORAGE_LOG(WARN, "failed to get sstable meta", K(ret), K(index_id));
  }
  return ret;
}

int ObPartitionGroupMetaBackupReader::fetch_sstable_pair_list(
    const uint64_t index_id, const ObPartitionKey& pkey, common::ObIArray<blocksstable::ObSSTablePair>& pair_list)
{
  int ret = OB_SUCCESS;
  ObPartitionBaseDataMetaBackupReader* reader = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (index_id == 0 || common::OB_INVALID_ID == index_id || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "index id is invalid", K(ret), K(index_id), K(pkey));
  } else if (OB_FAIL(partition_reader_map_.get_refactored(pkey, reader))) {
    STORAGE_LOG(WARN, "failed to partition base meta backup reader", K(ret), K(pkey));
  } else if (OB_ISNULL(reader)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition base meta backup reader should not be NULL", K(ret), K(pkey), KP(reader));
  } else if (OB_FAIL(reader->fetch_sstable_pair_list(index_id, pair_list))) {
    STORAGE_LOG(WARN, "failed to fetch sstable pair list", K(ret), K(index_id), K(pkey));
  }
  return ret;
}

/**************************ObPGPartitionBaseDataMetaRestorReader**************************/
ObPGPartitionBaseDataMetaBackupReader::ObPGPartitionBaseDataMetaBackupReader()
    : is_inited_(false), reader_index_(0), partition_reader_array_()
{}

ObPGPartitionBaseDataMetaBackupReader::~ObPGPartitionBaseDataMetaBackupReader()
{}

int ObPGPartitionBaseDataMetaBackupReader::init(
    const ObPartitionArray& partitions, ObPartitionGroupMetaBackupReader* reader)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "can not init twice", K(ret));
  } else if (OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(reader));
  } else if (OB_FAIL(reader->get_partition_readers(partitions, partition_reader_array_))) {
    STORAGE_LOG(WARN, "failed to get partition readers", K(ret));
  } else {
    reader_index_ = 0;
    is_inited_ = true;
    STORAGE_LOG(INFO, "succeed to init fetch_pg partition_meta_info", K(partition_reader_array_.count()));
  }

  return ret;
}

int ObPGPartitionBaseDataMetaBackupReader::fetch_pg_partition_meta_info(
    obrpc::ObPGPartitionMetaInfo& partition_meta_info)
{
  int ret = OB_SUCCESS;
  partition_meta_info.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg base data meta ob reader do not init", K(ret));
  } else if (reader_index_ == partition_reader_array_.count()) {
    ret = OB_ITER_END;
  } else {
    ObPartitionBaseDataMetaBackupReader* reader = partition_reader_array_.at(reader_index_);
    if (OB_ISNULL(reader)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(
          WARN, "partition base data meta backup reader should not be NULL", K(ret), KP(reader), K(reader_index_));
    } else if (OB_FAIL(reader->fetch_partition_meta(partition_meta_info.meta_))) {
      STORAGE_LOG(WARN, "failed to fetch partition meta", K(ret), K(reader_index_));
    } else if (OB_FAIL(reader->fetch_all_table_ids(partition_meta_info.table_id_list_))) {
      STORAGE_LOG(WARN, "failed to fetch all table ids", K(ret), K(reader_index_), K(partition_meta_info.meta_));
    } else {
      obrpc::ObFetchTableInfoResult table_info_result;
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_meta_info.table_id_list_.count(); ++i) {
        table_info_result.reset();
        const uint64_t index_id = partition_meta_info.table_id_list_.at(i);
        if (OB_FAIL(reader->fetch_table_keys(index_id, table_info_result))) {
          STORAGE_LOG(WARN, "failed to fetch table keys", K(ret), K(index_id));
        } else if (OB_FAIL(partition_meta_info.table_info_.push_back(table_info_result))) {
          STORAGE_LOG(WARN, "failed to push table info result into array", K(ret), K(index_id));
        }
      }

      if (OB_SUCC(ret)) {
        reader_index_++;
      }
    }
  }
  return ret;
}

/**********************ObBackupFileAppender***********************/
ObBackupFileAppender::ObBackupFileAppender()
    : is_opened_(false),
      file_offset_(0),
      max_buf_size_(0),
      file_type_(ObBackupFileType::BACKUP_TYPE_MAX),
      tmp_buffer_(0, ObModIds::BACKUP, false),
      data_buffer_(0, ObModIds::BACKUP, false),
      storage_appender_(StorageOpenMode::EXCLUSIVE_CREATE),
      common_header_(NULL),
      backup_arg_(NULL),
      bandwidth_throttle_(NULL)
{}

ObBackupFileAppender::~ObBackupFileAppender()
{
  if (is_opened_) {
    int tmp_ret = OB_SUCCESS;
    LOG_ERROR("backup file appender is not closed", K(backup_arg_));
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(WARN, "reset appender fail", K(tmp_ret));
    }
  }
}

int ObBackupFileAppender::open(common::ObInOutBandwidthThrottle& bandwidth_throttle,
    const share::ObPhysicalBackupArg& backup_arg, const common::ObString& path, const ObBackupFileType type)
{
  int ret = OB_SUCCESS;
  bool file_exist = true;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_OPEN_TWICE;
    STORAGE_LOG(WARN, "appender already opened", K(ret));
  } else if (OB_UNLIKELY(type >= ObBackupFileType::BACKUP_TYPE_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arg or type", K(backup_arg), K(type));
  } else {
    file_type_ = type;
    backup_arg_ = &backup_arg;
    bandwidth_throttle_ = &bandwidth_throttle;

    switch (type) {
      case BACKUP_META:
      case BACKUP_MACRO_DATA:
        max_buf_size_ = MAX_DATA_BUF_LENGTH;
        break;
      case BACKUP_META_INDEX:
      case BACKUP_MACRO_DATA_INDEX:
      case BACKUP_SSTABLE_MACRO_INDEX:
        max_buf_size_ = MAX_INDEX_BUF_LENGTH;
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid backup file type", K(ret), K(type));
        break;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tmp_buffer_.ensure_space(max_buf_size_))) {
      STORAGE_LOG(WARN, "tmp buffer not enough", K(max_buf_size_));
    } else if (OB_FAIL(data_buffer_.ensure_space(max_buf_size_))) {
      STORAGE_LOG(WARN, "data_buffer not enough", K(max_buf_size_));
    } else if (OB_FAIL(is_exist(path, file_exist))) {
      STORAGE_LOG(WARN, "fail to check path if exist", K(ret), K(path), K(type));
    } else if (OB_UNLIKELY(file_exist)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "backup target file exist", K(path), K(type), K(file_exist));
    } else if (OB_FAIL(open(path))) {
      STORAGE_LOG(WARN, "open appender failed", K(path));
    } else {
      is_opened_ = true;
    }
  }
  return ret;
}

bool ObBackupFileAppender::is_valid() const
{
  return is_opened_ && NULL != backup_arg_ && storage_appender_.is_opened();
}

int ObBackupFileAppender::append_meta_index(const common::ObIArray<ObBackupMetaIndex>& meta_index_array)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  bool is_uploaded = true;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup file appender not init", K(ret), K(is_opened_));
  } else if (OB_UNLIKELY(BACKUP_META_INDEX != file_type_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "file type not match", K(ret), K(file_type_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_index_array.count(); ++i) {
      const ObBackupMetaIndex& meta_index = meta_index_array.at(i);
      if (OB_FAIL(write(meta_index, write_size, is_uploaded))) {
        STORAGE_LOG(WARN, "write meta index fail", K(ret), K(meta_index));
      }
    }
  }
  return ret;
}

int ObBackupFileAppender::append_macro_index(
    const common::ObIArray<ObBackupTableMacroIndex>& macro_index_array, ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  bool is_uploaded = false;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup file appender not init", K(ret), K(is_opened_));
  } else if (OB_UNLIKELY(BACKUP_MACRO_DATA_INDEX != file_type_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "file type not match", K(ret), K(file_type_));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < macro_index_array.count(); ++i) {
      const ObBackupTableMacroIndex& index = macro_index_array.at(i);
      if (*index.table_key_ptr_ == table_key) {
        // do nothing
      } else {
        ObTableHandle tmp_handle;
        ObSSTable* sstable = NULL;
        ObBackupTableKeyInfo table_key_info;
        const ObITable::TableKey& tmp_table_key = *index.table_key_ptr_;
        if (OB_FAIL(ObPartitionService::get_instance().acquire_sstable(tmp_table_key, tmp_handle))) {
          STORAGE_LOG(WARN, "failed to get table", K(tmp_table_key), K(ret));
        } else if (OB_FAIL(tmp_handle.get_sstable(sstable))) {
          STORAGE_LOG(WARN, "failed to get table", K(tmp_table_key), K(ret));
        } else if (OB_ISNULL(sstable)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "sstable should not be null", K(ret));
        } else if (FALSE_IT(table_key_info.table_key_ = tmp_table_key)) {
        } else if (FALSE_IT(
                       table_key_info.total_macro_block_count_ = sstable->get_meta().get_total_macro_block_count())) {
        } else if (OB_FAIL(write(table_key_info, write_size, is_uploaded))) {
          STORAGE_LOG(WARN, "failed to write backup table key info", K(ret), K(table_key_info));
        } else {
          table_key = tmp_table_key;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(write(index, write_size, is_uploaded))) {
        STORAGE_LOG(WARN, "write meta index fail", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupFileAppender::append_partition_group_meta(
    const ObPartitionGroupMeta& pg_meta, ObBackupMetaIndex& meta_index, bool& is_uploaded)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  is_uploaded = false;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup file appender not init", K(ret), K(is_opened_));
  } else if (OB_UNLIKELY(!pg_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "pg_meta is not valid", K(ret), K(pg_meta));
  } else if (OB_UNLIKELY(BACKUP_META != file_type_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "file type not match", K(ret), K(file_type_));
  } else {
    tmp_buffer_.reuse();
    ObBackupMetaHeader* meta_header = NULL;
    char* header_buf = tmp_buffer_.data();
    int64_t header_len = sizeof(ObBackupMetaHeader);
    int64_t data_len = pg_meta.get_serialize_size();
    if (OB_FAIL(tmp_buffer_.advance_zero(header_len))) {
      STORAGE_LOG(WARN, "advance failed", K(ret), K(header_len));
    } else if (OB_FAIL(tmp_buffer_.write_serialize(pg_meta))) {
      STORAGE_LOG(WARN, "write data failed", K(ret), K(header_len));
    } else if (OB_UNLIKELY(tmp_buffer_.length() != header_len + data_len)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "buffer length mot match", K(ret), K(header_len), K(data_len), K(tmp_buffer_.length()));
    } else {
      meta_header = reinterpret_cast<ObBackupMetaHeader*>(header_buf);
      meta_header->reset();
      meta_header->meta_type_ = PARTITION_GROUP_META;
      meta_header->data_length_ = data_len;
      if (OB_FAIL(meta_header->set_checksum(header_buf + header_len, data_len))) {
        STORAGE_LOG(WARN, "set checksum failed", K(ret));
      } else if (OB_FAIL(write(tmp_buffer_, write_size, is_uploaded))) {
        STORAGE_LOG(WARN, "write buffer failed", K(ret));
      } else if (data_buffer_.pos() < tmp_buffer_.length()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN,
            "buffer length mot match",
            K(ret),
            K(header_len),
            K(data_len),
            K(write_size),
            K(data_buffer_.pos()),
            K(tmp_buffer_.length()));
      } else {
        meta_index.data_length_ = write_size;
        meta_index.meta_type_ = PARTITION_GROUP_META;
        meta_index.offset_ = file_offset_ + data_buffer_.pos() - meta_index.data_length_;
        tmp_buffer_.reuse();
      }
    }
  }
  return ret;
}

int ObBackupFileAppender::append_partition_meta(
    const storage::ObPGPartitionStoreMeta& partition_meta, ObBackupMetaIndex& meta_index, bool& is_uploaded)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  is_uploaded = false;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup file appender not init", K(ret), K(is_opened_));
  } else if (OB_UNLIKELY(!partition_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "partition_meta is not valid", K(ret), K(partition_meta));
  } else if (OB_UNLIKELY(BACKUP_META != file_type_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "file type not match", K(ret), K(file_type_));
  } else {
    tmp_buffer_.reuse();
    ObBackupMetaHeader* meta_header = NULL;
    char* header_buf = tmp_buffer_.data();
    int64_t header_len = sizeof(ObBackupMetaHeader);
    int64_t data_len = partition_meta.get_serialize_size();
    if (OB_FAIL(tmp_buffer_.advance_zero(header_len))) {
      STORAGE_LOG(WARN, "advance failed", K(ret), K(header_len));
    } else if (OB_FAIL(tmp_buffer_.write_serialize(partition_meta))) {
      STORAGE_LOG(WARN, "write data failed", K(ret), K(header_len));
    } else if (OB_UNLIKELY(tmp_buffer_.length() != header_len + data_len)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "buffer length mot match", K(ret), K(header_len), K(data_len), K(tmp_buffer_.length()));
    } else {
      meta_header = reinterpret_cast<ObBackupMetaHeader*>(header_buf);
      meta_header->reset();
      meta_header->meta_type_ = PARTITION_META;
      meta_header->data_length_ = data_len;
      if (OB_FAIL(meta_header->set_checksum(header_buf + header_len, data_len))) {
        STORAGE_LOG(WARN, "set checksum failed", K(ret));
      } else if (OB_FAIL(write(tmp_buffer_, write_size, is_uploaded))) {
        STORAGE_LOG(WARN, "write buffer failed", K(ret));
      } else if (data_buffer_.pos() < tmp_buffer_.length()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN,
            "buffer length mot match",
            K(ret),
            K(header_len),
            K(data_len),
            K(write_size),
            K(data_buffer_.pos()),
            K(tmp_buffer_.length()));
      } else {
        meta_index.data_length_ = write_size;
        meta_index.meta_type_ = PARTITION_META;
        meta_index.offset_ = file_offset_ + data_buffer_.pos() - meta_index.data_length_;
        tmp_buffer_.reuse();
      }
    }
  }
  return ret;
}

int ObBackupFileAppender::append_sstable_metas(
    const common::ObIArray<ObBackupSSTableInfo>& sstable_info_array, ObBackupMetaIndex& meta_index, bool& is_uploaded)
{
  int ret = OB_SUCCESS;
  is_uploaded = false;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup file appender not init", K(ret), K(is_opened_));
  } else if (OB_UNLIKELY(BACKUP_META != file_type_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "file type not match", K(ret), K(file_type_));
  } else if (OB_UNLIKELY(0 == sstable_info_array.count())) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "sstable_info_array is empty", K(ret), K(sstable_info_array.count()));
  } else {
    tmp_buffer_.reuse();
    ObBackupMetaHeader* meta_header = NULL;
    char* header_buf = tmp_buffer_.data();
    int64_t header_len = sizeof(ObBackupMetaHeader);
    int64_t sstable_count = sstable_info_array.count();
    int64_t data_len = 0;
    int64_t write_size = 0;

    if (OB_FAIL(tmp_buffer_.advance_zero(header_len))) {
      STORAGE_LOG(WARN, "advance failed", K(ret), K(header_len));
    } else if (OB_FAIL(tmp_buffer_.write(sstable_count))) {
      STORAGE_LOG(WARN, "write sstable count fail", K(ret), K(sstable_count));
    } else {
      data_len += (tmp_buffer_.length() - header_len);
      for (int64_t idx = 0; OB_SUCC(ret) && idx < sstable_info_array.count(); ++idx) {
        const blocksstable::ObSSTableBaseMeta& sstable_meta = sstable_info_array.at(idx).sstable_meta_;
        int64_t start_pos = tmp_buffer_.pos();
        int64_t meta_len = sstable_meta.get_serialize_size();
        if (OB_UNLIKELY(!sstable_meta.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid argument", K(idx), K(sstable_meta));
        } else if (sstable_meta.sstable_format_version_ < ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_5) {
          ret = OB_NOT_SUPPORTED;
          STORAGE_LOG(WARN, "not support backup sstable version", K(ret), K(sstable_meta));
        } else if (OB_FAIL(tmp_buffer_.write_serialize(sstable_meta))) {
          STORAGE_LOG(WARN, "write sstable meta fail", K(ret), K(sstable_meta));
        } else if (OB_UNLIKELY(tmp_buffer_.pos() - start_pos != meta_len)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "the serialize size is not match", K(start_pos), K(tmp_buffer_.pos()), K(meta_len), K(ret));
        } else {
          data_len += meta_len;
        }
      }
      if (OB_SUCC(ret)) {
        meta_header = reinterpret_cast<ObBackupMetaHeader*>(header_buf);
        meta_header->reset();
        meta_header->meta_type_ = SSTABLE_METAS;
        meta_header->data_length_ = data_len;
        if (OB_FAIL(meta_header->set_checksum(header_buf + header_len, data_len))) {
          STORAGE_LOG(WARN, "set checksum failed", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(tmp_buffer_.length() != header_len + data_len)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "buffer length mot match", K(ret), K(header_len), K(data_len), K(tmp_buffer_.length()));
    } else if (OB_FAIL(write(tmp_buffer_, write_size, is_uploaded))) {
      STORAGE_LOG(WARN, "write buffer failed", K(ret));
    } else if (data_buffer_.pos() < tmp_buffer_.length()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(
          WARN, "buffer length mot match", K(ret), K(write_size), K(data_buffer_.pos()), K(tmp_buffer_.length()));
    } else {
      meta_index.data_length_ = write_size;
      meta_index.meta_type_ = SSTABLE_METAS;
      meta_index.offset_ = file_offset_ + data_buffer_.pos() - meta_index.data_length_;
      tmp_buffer_.reuse();
    }
  }
  return ret;
}

int ObBackupFileAppender::append_table_keys(
    const common::ObIArray<storage::ObITable::TableKey>& table_keys, ObBackupMetaIndex& meta_index, bool& is_uploaded)
{
  int ret = OB_SUCCESS;
  is_uploaded = false;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup file appender not init", K(ret), K(is_opened_));
  } else if (OB_UNLIKELY(BACKUP_META != file_type_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "file type not match", K(ret), K(file_type_));
  } else if (OB_UNLIKELY(0 == table_keys.count())) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "table_keys array is empty", K(ret), K(table_keys.count()));
  } else {
    tmp_buffer_.reuse();
    ObBackupMetaHeader* meta_header = NULL;
    char* header_buf = tmp_buffer_.data();
    int64_t header_len = sizeof(ObBackupMetaHeader);
    int64_t table_count = table_keys.count();
    int64_t data_len = 0;
    int64_t write_size = 0;
    if (OB_FAIL(tmp_buffer_.advance_zero(header_len))) {
      STORAGE_LOG(WARN, "advance failed", K(ret), K(header_len));
    } else if (OB_FAIL(tmp_buffer_.write(table_count))) {
      STORAGE_LOG(WARN, "write table key count fail", K(ret), K(table_count));
    } else {
      data_len += (tmp_buffer_.length() - header_len);
      for (int64_t idx = 0; OB_SUCC(ret) && idx < table_keys.count(); ++idx) {
        const ObITable::TableKey& table_key = table_keys.at(idx);
        int64_t start_pos = tmp_buffer_.pos();
        int64_t meta_len = table_key.get_serialize_size();
        if (OB_UNLIKELY(!table_key.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid argument", K(idx), K(table_key));
        } else if (OB_FAIL(tmp_buffer_.write_serialize(table_key))) {
          STORAGE_LOG(WARN, "write table key fail", K(idx), K(table_key));
        } else if (OB_UNLIKELY(tmp_buffer_.pos() - start_pos != meta_len)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "the serialize size is not match", K(start_pos), K(tmp_buffer_.pos()), K(meta_len), K(ret));
        } else {
          data_len += meta_len;
        }
      }
      if (OB_SUCC(ret)) {
        meta_header = reinterpret_cast<ObBackupMetaHeader*>(header_buf);
        meta_header->reset();
        meta_header->meta_type_ = TABLE_KEYS;
        meta_header->data_length_ = data_len;
        if (OB_FAIL(meta_header->set_checksum(header_buf + header_len, data_len))) {
          STORAGE_LOG(WARN, "set checksum failed", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(tmp_buffer_.length() != header_len + data_len)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "buffer length mot match", K(ret), K(header_len), K(data_len), K(tmp_buffer_.length()));
    } else if (OB_FAIL(write(tmp_buffer_, write_size, is_uploaded))) {
      STORAGE_LOG(WARN, "write buffer failed", K(ret));
    } else if (OB_UNLIKELY(data_buffer_.pos() < tmp_buffer_.length())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(
          WARN, "buffer length mot match", K(ret), K(write_size), K(data_buffer_.pos()), K(tmp_buffer_.length()));
    } else {
      meta_index.data_length_ = write_size;
      meta_index.meta_type_ = TABLE_KEYS;
      meta_index.offset_ = file_offset_ + data_buffer_.pos() - meta_index.data_length_;
      tmp_buffer_.reuse();
    }
  }
  return ret;
}

int ObBackupFileAppender::append_macroblock_data(blocksstable::ObFullMacroBlockMeta& macro_meta,
    blocksstable::ObBufferReader& macro_data, ObBackupTableMacroIndex& macro_index)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  bool is_uploaded = true;
  ObArenaAllocator allocator;
  blocksstable::ObMacroBlockMetaV2* meta = nullptr;
  tmp_buffer_.reuse();

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup file appender not init", K(ret), K(is_opened_));
  } else if (OB_UNLIKELY(!macro_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "macro_meta is not valid", K(ret), K(macro_meta));
  } else if (OB_UNLIKELY(BACKUP_MACRO_DATA != file_type_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "file type not match", K(ret), K(file_type_));
  } else if (nullptr != common_header_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common_header_ must null for append macro block data", K(ret), K(*common_header_));
  } else {
    // TODO():bug need fix:macro_meta serialize_size not equal real_size
    ObFullMacroBlockMetaEntry marco_block_meta_entry(
        *const_cast<ObMacroBlockMetaV2*>(macro_meta.meta_), *const_cast<ObMacroBlockSchemaInfo*>(macro_meta.schema_));
    ObBackupMacroData backup_macro_data(tmp_buffer_, macro_data);
    macro_index.offset_ = file_offset_;

    if (OB_FAIL(tmp_buffer_.write_serialize(marco_block_meta_entry))) {
      LOG_WARN("failed to write macro meta", K(ret), K(meta));
    } else if (OB_FAIL(write(backup_macro_data, write_size, is_uploaded))) {
      LOG_WARN("failed to write backup macro data", K(ret), K(backup_macro_data));
    } else if (OB_FAIL(sync_upload())) {
      LOG_WARN("failed to sync_upload", K(ret));
    } else if (OB_UNLIKELY(0 != data_buffer_.length())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "buffer length mot match", K(ret), K(data_buffer_.pos()));
    } else if (OB_UNLIKELY(macro_index.data_version_ != macro_meta.meta_->data_version_ ||
                           macro_index.data_seq_ != macro_meta.meta_->data_seq_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro meta version not match", K(ret), K(macro_index), K(macro_meta));
    } else {
      macro_index.data_length_ = file_offset_ - macro_index.offset_;  // include common header
      if (macro_index.data_length_ < backup_macro_data.get_serialize_size() + sizeof(ObBackupCommonHeader)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR,
            "data length not match",
            K(ret),
            K(macro_index),
            K(file_offset_),
            K(backup_macro_data),
            K(write_size),
            "backup_macro_data need size",
            backup_macro_data.get_serialize_size(),
            "header size",
            sizeof(ObBackupCommonHeader));
      }
    }
  }

  if (nullptr != meta) {
    meta->~ObMacroBlockMetaV2();
    meta = nullptr;
  }
  return ret;
}

int ObBackupFileAppender::append_backup_pg_meta_info(
    const ObBackupPGMetaInfo& pg_meta_info, ObBackupMetaIndex& meta_index, bool& is_uploaded)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  is_uploaded = false;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup file appender not init", K(ret), K(is_opened_));
  } else if (OB_UNLIKELY(!pg_meta_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "pg_meta is not valid", K(ret), K(pg_meta_info));
  } else if (OB_UNLIKELY(BACKUP_META != file_type_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "file type not match", K(ret), K(file_type_));
  } else {
    tmp_buffer_.reuse();
    ObBackupMetaHeader* meta_header = NULL;
    char* header_buf = tmp_buffer_.data();
    int64_t header_len = sizeof(ObBackupMetaHeader);
    int64_t data_len = pg_meta_info.get_serialize_size();
    if (data_len + header_len >= max_buf_size_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN,
          "max buf size is smaller than backup meta data size",
          K(ret),
          K(data_len),
          K(header_len),
          K(max_buf_size_));
    } else if (OB_FAIL(tmp_buffer_.advance_zero(header_len))) {
      STORAGE_LOG(WARN, "advance failed", K(ret), K(header_len));
    } else if (OB_FAIL(tmp_buffer_.write_serialize(pg_meta_info))) {
      STORAGE_LOG(WARN, "write data failed", K(ret), K(header_len));
    } else if (OB_UNLIKELY(tmp_buffer_.length() != header_len + data_len)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "buffer length mot match", K(ret), K(header_len), K(data_len), K(tmp_buffer_.length()));
    } else {
      meta_header = reinterpret_cast<ObBackupMetaHeader*>(header_buf);
      meta_header->reset();
      meta_header->meta_type_ = PARTITION_GROUP_META_INFO;
      meta_header->data_length_ = data_len;
      if (OB_FAIL(meta_header->set_checksum(header_buf + header_len, data_len))) {
        STORAGE_LOG(WARN, "set checksum failed", K(ret));
      } else if (OB_FAIL(write(tmp_buffer_, write_size, is_uploaded))) {
        STORAGE_LOG(WARN, "write buffer failed", K(ret));
      } else if (data_buffer_.pos() < tmp_buffer_.length()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN,
            "buffer length mot match",
            K(ret),
            K(header_len),
            K(data_len),
            K(write_size),
            K(data_buffer_.pos()),
            K(tmp_buffer_.length()));
      } else {
        meta_index.data_length_ = write_size;
        meta_index.meta_type_ = PARTITION_GROUP_META_INFO;
        meta_index.offset_ = file_offset_ + data_buffer_.pos() - meta_index.data_length_;
        tmp_buffer_.reuse();
      }
    }
  }
  return ret;
}

int ObBackupFileAppender::get_data_version(const ObBackupFileType data_type, uint16& data_version)
{
  int ret = OB_SUCCESS;
  switch (data_type) {
    case ObBackupFileType::BACKUP_META:
      data_version = ObBackupMetaHeader::META_HEADER_VERSION;
      break;
    case ObBackupFileType::BACKUP_MACRO_DATA:
      data_version = ObBackupCommonHeader::MACRO_DATA_HEADER_VERSION;
      break;
    case ObBackupFileType::BACKUP_META_INDEX:
      data_version = ObBackupMetaIndex::META_INDEX_VERSION;
      break;
    case ObBackupFileType::BACKUP_MACRO_DATA_INDEX:
      data_version = ObBackupMacroIndex::MACRO_INDEX_VERSION;
      break;
    case ObBackupFileType::BACKUP_SSTABLE_MACRO_INDEX:
      data_version = ObBackupSStableMacroIndex::SSTABLE_MACRO_INDEX_VERSION;
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid data type", K(ret), K(data_type));
      break;
  }
  return ret;
}

int ObBackupFileAppender::is_exist(const common::ObString& uri, bool& exist)
{
  int ret = OB_SUCCESS;
  ObStorageUtil storage_util(false /*need retry*/);
  exist = false;
  if (OB_ISNULL(backup_arg_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "backup arg is null");
  } else if (OB_FAIL(storage_util.is_exist(uri, backup_arg_->storage_info_, exist))) {
    STORAGE_LOG(WARN, "storage_util is_exist fail", K(ret), K(uri), K(backup_arg_->storage_info_), K(exist));
  }

  return ret;
}

int ObBackupFileAppender::open(const common::ObString path)
{
  int ret = OB_SUCCESS;

  ObStorageUtil util(false /*need retry*/);
  ObStorageAppender::AppenderParam param;
  param.strategy_ = ObAppendStrategy::OB_APPEND_USE_SLICE_PUT;
  param.version_param_.open_object_version_ = false;
  if (OB_ISNULL(backup_arg_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "backup arg is null");
  } else if (OB_FAIL(util.mk_parent_dir(path, backup_arg_->storage_info_))) {
    STORAGE_LOG(WARN, "failed to mkdir", K(ret), K(path));
  } else if (OB_FAIL(storage_appender_.open(path, backup_arg_->storage_info_, param))) {
    STORAGE_LOG(WARN, "storage_writer open fail", K(ret), K(path));
  }
  return ret;
}

int ObBackupFileAppender::write_tail()
{
  int ret = OB_SUCCESS;
  const int64_t header_len = sizeof(ObBackupCommonHeader);
  if (!is_opened_ || NULL != common_header_ || data_buffer_.length() > 0 || !storage_appender_.is_opened()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "storage_appender is not finish",
        K(is_opened_),
        KP(common_header_),
        K(data_buffer_.length()),
        K(storage_appender_));
  } else if (OB_FAIL(data_buffer_.advance_zero(header_len))) {
    STORAGE_LOG(WARN, "advance failed", K(ret), K(header_len));
  } else {
    char* header_buf = data_buffer_.data();
    common_header_ = reinterpret_cast<ObBackupCommonHeader*>(header_buf);
    common_header_->reset();
    common_header_->compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    common_header_->data_type_ = ObBackupFileType::BACKUP_FILE_END_MARK;
    if (OB_FAIL(sync_upload())) {
      STORAGE_LOG(WARN, "sync upload file tail fail", K(ret));
    }
  }
  return ret;
}

int ObBackupFileAppender::close()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_BACKUP_FILE_APPENDER_CLOSE) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "fake EN_BACKUP_FILE_APPENDER_CLOSE", K(ret));
    }
  }
#endif

  if (OB_FAIL(ret)) {
  } else if (!is_opened_ || 0 == data_buffer_.length() || !storage_appender_.is_opened()) {
    STORAGE_LOG(INFO, "no need to upload remain buffer", K(is_opened_), K(data_buffer_.length()), K(storage_appender_));
  } else if (OB_FAIL(sync_upload())) {
    STORAGE_LOG(WARN, "sync upload fail", K(ret));
  } else if (OB_UNLIKELY(data_buffer_.length() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "upload remain buffer fail", K(ret));
  }

  if (storage_appender_.is_opened()) {
    if (OB_SUCC(ret)) {
      if (OB_FAIL(write_tail())) {
        STORAGE_LOG(WARN, "writer tail mark fail", K(ret), K(storage_appender_));
      }
    }
    if (OB_SUCCESS != (tmp_ret = storage_appender_.close())) {
      if (OB_SUCCESS == ret) {
        ret = tmp_ret;
      }
      STORAGE_LOG(WARN, "close appender fail", K(ret), K(tmp_ret), K(storage_appender_));
    }
  }
  file_offset_ = 0;
  is_opened_ = false;
  STORAGE_LOG(INFO, "finish close bakcup file appender", K(ret), K(tmp_ret));
  return ret;
}

int ObBackupFileAppender::sync_upload()
{
  // TODO:(): compress and encrypt
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBackupFileAppender is not init", K(ret), K(!is_opened_));
  } else if (OB_UNLIKELY(0 == data_buffer_.length())) {
    STORAGE_LOG(INFO, "no need to upload", K(ret), K(data_buffer_.length()));
  } else if (OB_ISNULL(common_header_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "no need to upload", K(ret), K(data_buffer_.length()));
  } else {
    int64_t align_size = common::upper_align(data_buffer_.length(), DIO_READ_ALIGN_SIZE);
    int64_t advance_size = align_size - data_buffer_.length();
    common_header_->data_zlength_ = common_header_->data_length_;
    common_header_->align_length_ = advance_size;
    if (OB_FAIL(common_header_->set_checksum(data_buffer_.data() + common_header_->header_length_,
            data_buffer_.length() - common_header_->header_length_))) {
      STORAGE_LOG(WARN, "common header set checksum fail", K(ret), K(*common_header_));
    } else if (advance_size > 0 && OB_FAIL(data_buffer_.advance_zero(advance_size))) {
      STORAGE_LOG(WARN, "advance align size fail", K(ret), K(align_size), K(advance_size));
    }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = E(EventTable::EN_BACKUP_META_INDEX_BUFFER_NOT_COMPLETED) OB_SUCCESS;
      if (OB_FAIL(ret) && ObBackupFileType::BACKUP_META_INDEX == file_type_) {
        if (0 == data_buffer_.length() / 2) {
          ret = OB_SUCCESS;
        } else if (OB_FAIL(storage_appender_.write(data_buffer_.data(), data_buffer_.length() / 2))) {
          STORAGE_LOG(WARN, "storage_writer writer fail", K(ret), K(storage_appender_));
        } else {
          ret = OB_IO_ERROR;
        }
      }
    }
#endif

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = E(EventTable::EN_BACKUP_MACRO_INDEX_BUFFER_NOT_COMPLETED) OB_SUCCESS;
      if (OB_FAIL(ret) && ObBackupFileType::BACKUP_MACRO_DATA_INDEX == file_type_) {
        if (0 == data_buffer_.length() / 2) {
          ret = OB_SUCCESS;
        } else if (OB_FAIL(storage_appender_.write(data_buffer_.data(), data_buffer_.length() / 2))) {
          STORAGE_LOG(WARN, "storage_writer writer fail", K(ret), K(storage_appender_));
        } else {
          ret = OB_IO_ERROR;
        }
      }
    }
#endif

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(storage_appender_.write(data_buffer_.data(), data_buffer_.length()))) {
      STORAGE_LOG(WARN, "storage_writer writer fail", K(ret), K(storage_appender_));
    } else if (OB_UNLIKELY(file_offset_ + data_buffer_.length() != storage_appender_.get_length())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN,
          "file length not match",
          K(ret),
          K(file_offset_),
          K(data_buffer_.length()),
          K(storage_appender_.get_length()));
    } else {
      if (OB_NOT_NULL(bandwidth_throttle_)) {
        const int64_t active_time = ObTimeUtility::current_time();
        if (OB_SUCCESS !=
            (tmp_ret = bandwidth_throttle_->limit_out_and_sleep(data_buffer_.length(), active_time, MAX_IDLE_TIME))) {
          STORAGE_LOG(WARN, "failed to limit_out_and_sleep", K(ret));
        }
      }

      file_offset_ += data_buffer_.length();
      data_buffer_.reuse();
      common_header_ = NULL;
    }
  }
  return ret;
}

/**********************ObBackupMetaWriter***********************/
ObBackupMetaWriter::ObBackupMetaWriter()
    : is_inited_(false), task_id_(0), task_list_(NULL), cp_fty_(NULL), meta_appender_(), index_appender_()
{}

ObBackupMetaWriter::~ObBackupMetaWriter()
{}

int ObBackupMetaWriter::open(
    common::ObInOutBandwidthThrottle& bandwidth_throttle, ObIArray<ObPartMigrationTask>& task_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(0 == task_list.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(task_list.count()));
  } else if (OB_FAIL(check_task(task_list))) {
    STORAGE_LOG(WARN, "check task list fail", K(ret));
  } else {
    const share::ObPhysicalBackupArg& arg = task_list.at(0).arg_.backup_arg_;
    cp_fty_ = ObPartitionMigrator::get_instance().get_cp_fty();
    task_id_ = arg.task_id_;
    if (OB_FAIL(prepare_appender(bandwidth_throttle, arg))) {
      STORAGE_LOG(WARN, "prepare meta appender fail", K(ret), K(arg));
    }
  }

  if (OB_SUCC(ret)) {
    task_list_ = &task_list;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupMetaWriter::close()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_SUCCESS != (tmp_ret = meta_appender_.close())) {
    LOG_WARN("failed to close meta appender", K(ret), K(tmp_ret));
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }

  if (OB_SUCC(ret) && !meta_index_array_.empty()) {
    if (OB_FAIL(index_appender_.append_meta_index(meta_index_array_))) {
      LOG_WARN("failed to append meta index", K(ret));
    } else {
      meta_index_array_.reset();
    }
  }

  if (OB_SUCCESS != (tmp_ret = index_appender_.close())) {
    LOG_WARN("failed to close meta appender", K(ret), K(tmp_ret));
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  return ret;
}

int ObBackupMetaWriter::process()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret), K(is_inited_));
  } else {
    const share::ObPhysicalBackupArg& first_arg = task_list_->at(0).arg_.backup_arg_;
    if (OB_UNLIKELY(!first_arg.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "backup arg is invalid", K(ret), K(first_arg));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < task_list_->count(); ++i) {
      ObPartMigrationTask& task = task_list_->at(i);
      const share::ObPhysicalBackupArg& backup_arg = task.arg_.backup_arg_;
      const common::ObPGKey& pg_key = task.arg_.key_;
      ObIPartitionGroupGuard guard;
      ObIPartitionGroup* partition_group = NULL;
      ObPGStorage* pg_storage = NULL;
      ObBackupPGMetaInfo backup_pg_meta_info;
      ObPartitionGroupMeta& pg_meta = backup_pg_meta_info.pg_meta_;
      if (OB_FAIL(ObPartitionService::get_instance().get_partition(pg_key, guard))) {
        STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pg_key));
      } else if (OB_ISNULL(partition_group = guard.get_partition_group())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition group should not be NULL", K(ret), KP(partition_group));
      } else if (OB_ISNULL(pg_storage = &(partition_group->get_pg_storage()))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "failt to get pg partition storage", K(ret), KP(pg_storage));
      } else if (OB_FAIL(pg_storage->get_backup_pg_meta_data(first_arg.backup_snapshot_version_, pg_meta))) {
        // TODO() if multi version start > backup_snapshot_version, no need retry
        STORAGE_LOG(WARN, "failed to get backup pg meta data", K(ret), K(first_arg), K(pg_key));
      } else {
        const ObPartitionArray& partitions = pg_meta.partitions_;
        const int64_t start_meta_data_offset = meta_appender_.get_upload_size();
        const int64_t start_index_data_offset = index_appender_.get_upload_size();
        // backup partition_meta/sstable meta/table keys
        for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
          const ObPartitionKey& pkey = partitions.at(i);
          ObPartitionMetaBackupReader meta_reader;
          ObPGPartitionStoreMeta* partition_store_meta = NULL;
          common::ObIArray<ObBackupSSTableInfo>* sstable_info_array = NULL;
          common::ObArray<ObITable::TableKey>* table_key_array = NULL;
          ObBackupPartitionStoreMetaInfo partition_store_meta_info;

          if (OB_FAIL(meta_reader.init(backup_arg, pkey))) {
            STORAGE_LOG(WARN, "init PartitionMetaBackupReader fail", K(ret), K(pkey));
          } else if (OB_FAIL(
                         meta_reader.read_backup_metas(partition_store_meta, sstable_info_array, table_key_array))) {
            STORAGE_LOG(WARN, "fail to read partition meta info", K(ret), K(pkey));
          } else if (OB_ISNULL(partition_store_meta) || OB_ISNULL(sstable_info_array) || OB_ISNULL(table_key_array)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN,
                "fail to read partition meta info",
                K(ret),
                KP(partition_store_meta),
                KP(sstable_info_array),
                KP(table_key_array));
          } else if (OB_UNLIKELY(!partition_store_meta->is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "partition store meta is invalid.", K(ret), K(*partition_store_meta));
          } else if (OB_FAIL(create_partition_store_meta_info(
                         *partition_store_meta, *sstable_info_array, *table_key_array, partition_store_meta_info))) {
            STORAGE_LOG(WARN, "failed ot create partition store meta info", K(ret), K(*partition_store_meta));
          } else if (OB_FAIL(
                         backup_pg_meta_info.partition_store_meta_info_array_.push_back(partition_store_meta_info))) {
            STORAGE_LOG(WARN,
                "failed to push backup partition store meta info into array",
                K(ret),
                K(partition_store_meta_info));
          } else {
            task.ctx_.data_statics_.input_bytes_ += meta_reader.get_data_size();
          }
        }

        // backup pg meta
        if (OB_SUCC(ret)) {
          if (OB_FAIL(write_backup_pg_meta_info(backup_pg_meta_info))) {
            STORAGE_LOG(WARN, "failed to write backup pg meta info", K(ret), K(backup_pg_meta_info));
          } else {
            task.ctx_.data_statics_.input_bytes_ += pg_meta.get_serialize_size();
            task.ctx_.data_statics_.output_bytes_ += (meta_appender_.get_upload_size() - start_meta_data_offset);
            task.ctx_.data_statics_.output_bytes_ += (index_appender_.get_upload_size() - start_index_data_offset);
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupMetaWriter::prepare_appender(
    common::ObInOutBandwidthThrottle& bandwidth_throttle, const share::ObPhysicalBackupArg& arg)
{
  int ret = OB_SUCCESS;
  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid backup arg", K(ret), K(arg));
  } else if (OB_FAIL(arg.get_backup_base_data_info(path_info))) {
    STORAGE_LOG(WARN, "failed to get base data info", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_meta_file_path(path_info, arg.task_id_, path))) {
    STORAGE_LOG(WARN, "failed to get meta file path", K(ret));
  } else if (OB_FAIL(meta_appender_.open(bandwidth_throttle, arg, path.get_obstr(), ObBackupFileType::BACKUP_META))) {
    STORAGE_LOG(WARN, "failed to init meta appender", K(ret));
  } else if (FALSE_IT(path.reset())) {
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_meta_index_path(path_info, arg.task_id_, path))) {
    STORAGE_LOG(WARN, "failed to get meta index file path", K(ret));
  } else if (OB_FAIL(index_appender_.open(
                 bandwidth_throttle, arg, path.get_obstr(), ObBackupFileType::BACKUP_META_INDEX))) {
    STORAGE_LOG(WARN, "failed to init meta index appender", K(ret));
  }
  return ret;
}

int ObBackupMetaWriter::check_task(const ObIArray<ObPartMigrationTask>& task_list) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == task_list.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(task_list.count()));
  } else {
    const uint64_t tenant_id = task_list.at(0).arg_.backup_arg_.tenant_id_;
    for (int i = 0; OB_SUCC(ret) && i < task_list.count(); ++i) {
      if (tenant_id != task_list.at(i).arg_.backup_arg_.tenant_id_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN,
            "task list not same tenant",
            K(ret),
            K(tenant_id),
            K(i),
            "cur_tenant_id",
            task_list.at(i).arg_.backup_arg_.tenant_id_);
      }
    }
  }
  return ret;
}

int ObBackupMetaWriter::get_min_snapshot_version(
    const common::ObIArray<ObITable::TableKey>& table_key_array, int64_t& snapshot_version)
{
  int ret = OB_SUCCESS;
  snapshot_version = INT64_MAX;
  if (table_key_array.count() == 0) {
    STORAGE_LOG(INFO, "table_key_array is empty", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < table_key_array.count(); ++i) {
      const ObITable::TableKey& table_key = table_key_array.at(i);
      snapshot_version = MIN(snapshot_version, table_key.trans_version_range_.snapshot_version_);
    }
  }
  return ret;
}

int ObBackupMetaWriter::append_meta_index(const bool need_upload, ObBackupMetaIndex& meta_index)
{
  int ret = OB_SUCCESS;

  if (need_upload) {
    if (OB_FAIL(index_appender_.append_meta_index(meta_index_array_))) {
      STORAGE_LOG(WARN, "fail to write normal meta index", K(ret), K(meta_index));
    } else {
      meta_index_array_.reuse();
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(meta_index_array_.push_back(meta_index))) {
      LOG_WARN("failed to add meta index", K(ret));
    }
  }

  LOG_DEBUG("append meta index", K(need_upload), "meta_index_count", meta_index_array_.count());
  return ret;
}

void ObBackupMetaWriter::set_meta_index(const ObPartitionKey& pkey, ObBackupMetaIndex& meta_index)
{
  meta_index.reset();
  meta_index.table_id_ = pkey.get_table_id();
  meta_index.partition_id_ = pkey.get_partition_id();
  meta_index.task_id_ = task_id_;
}

int ObBackupMetaWriter::write_pg_meta(const ObPartitionKey& pgkey, const ObPartitionGroupMeta& pg_meta)
{
  int ret = OB_SUCCESS;
  ObBackupMetaIndex meta_index;
  bool is_uploaded = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "meta writer not init", K(ret), K(is_inited_));
  } else if (FALSE_IT(set_meta_index(pgkey, meta_index))) {
  } else if (OB_FAIL(meta_appender_.append_partition_group_meta(pg_meta, meta_index, is_uploaded))) {
    STORAGE_LOG(WARN, "fail to write pg meta", K(ret), K(pgkey));
  } else if (OB_FAIL(append_meta_index(is_uploaded, meta_index))) {
    STORAGE_LOG(WARN, "fail to write pg meta index", K(ret), K(pgkey), K(meta_index));
  } else {
    STORAGE_LOG(INFO, "backup pg meta succ", K(pgkey), K(meta_index));
  }
  return ret;
}

int ObBackupMetaWriter::write_partition_meta(const ObPartitionKey& pkey, const ObPGPartitionStoreMeta& partition_meta)
{
  int ret = OB_SUCCESS;
  ObBackupMetaIndex meta_index;
  bool is_uploaded = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "meta writer not init", K(ret), K(is_inited_));
  } else if (FALSE_IT(set_meta_index(pkey, meta_index))) {
  } else if (OB_FAIL(meta_appender_.append_partition_meta(partition_meta, meta_index, is_uploaded))) {
    STORAGE_LOG(WARN, "fail to write partition meta", K(ret), K(pkey));
  } else if (OB_FAIL(append_meta_index(is_uploaded, meta_index))) {
    STORAGE_LOG(WARN, "fail to write meta index", K(ret), K(pkey), K(meta_index));
  } else {
    STORAGE_LOG(INFO, "backup partition meta succ", K(pkey), K(meta_index));
  }
  return ret;
}

int ObBackupMetaWriter::write_sstable_metas(
    const ObPartitionKey& pkey, const common::ObIArray<ObBackupSSTableInfo>& sstable_info_array)
{
  int ret = OB_SUCCESS;
  ObBackupMetaIndex meta_index;
  bool is_uploaded = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "meta writer not init", K(ret), K(is_inited_));
  } else if (FALSE_IT(set_meta_index(pkey, meta_index))) {
  } else if (OB_FAIL(meta_appender_.append_sstable_metas(sstable_info_array, meta_index, is_uploaded))) {
    STORAGE_LOG(WARN, "fail to write sstable meta", K(ret), K(pkey));
  } else if (OB_FAIL(append_meta_index(is_uploaded, meta_index))) {
    STORAGE_LOG(WARN, "fail to write meta index", K(ret), K(pkey), K(meta_index));
  } else {
    STORAGE_LOG(INFO, "backup sstable meta succ", K(pkey), K(meta_index));
  }
  return ret;
}

int ObBackupMetaWriter::write_table_keys(
    const ObPartitionKey& pkey, const common::ObArray<ObITable::TableKey>& table_keys)
{
  int ret = OB_SUCCESS;
  ObBackupMetaIndex meta_index;
  bool is_uploaded = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "meta writer not init", K(ret), K(is_inited_));
  } else if (FALSE_IT(set_meta_index(pkey, meta_index))) {
  } else if (OB_FAIL(meta_appender_.append_table_keys(table_keys, meta_index, is_uploaded))) {
    STORAGE_LOG(WARN, "fail to write table keys", K(ret), K(pkey));
  } else if (OB_FAIL(append_meta_index(is_uploaded, meta_index))) {
    STORAGE_LOG(WARN, "fail to write meta index", K(ret), K(pkey), K(meta_index));
  } else {
    STORAGE_LOG(INFO, "backup table keys succ", K(pkey), K(meta_index));
  }
  return ret;
}

int ObBackupMetaWriter::create_partition_store_meta_info(const ObPGPartitionStoreMeta& partition_store_meta,
    const common::ObIArray<ObBackupSSTableInfo>& sstable_info_array,
    const common::ObIArray<ObITable::TableKey>& table_keys, ObBackupPartitionStoreMetaInfo& partition_store_meta_info)
{
  int ret = OB_SUCCESS;
  partition_store_meta_info.reset();
  ObArray<ObBackupSSTableMetaInfo> sstable_meta_info;
  ObIArray<ObBackupSSTableMetaInfo>& sstable_meta_info_array = partition_store_meta_info.sstable_meta_info_array_;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "meta writer not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(partition_store_meta_info.partition_store_meta_.assign(partition_store_meta))) {
    STORAGE_LOG(WARN, "failed to assign partition store meta", K(ret), K(partition_store_meta));
  } else if (table_keys.count() != sstable_info_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "table keys not equal sstable metas count", K(ret), K(table_keys), K(sstable_info_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_keys.count(); ++i) {
      const ObITable::TableKey& table_key = table_keys.at(i);
      const ObSSTableBaseMeta& sstable_meta = sstable_info_array.at(i).sstable_meta_;
      ObBackupSSTableMetaInfo sstable_meta_info;
      if (table_key.table_id_ != sstable_meta.index_id_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "table key table id not equal to sstable base meta", K(ret), K(table_key), K(sstable_meta));
      } else if (OB_FAIL(sstable_meta_info.set_sstable_meta_info(sstable_meta, table_key))) {
        STORAGE_LOG(WARN, "failed to set sstable meta info", K(ret), K(sstable_meta), K(table_key));
      } else if (OB_FAIL(sstable_meta_info_array.push_back(sstable_meta_info))) {
        STORAGE_LOG(WARN, "failed to push sstable meta info into array", K(ret), K(sstable_meta_info));
      }
    }
  }
  return ret;
}

int ObBackupMetaWriter::write_backup_pg_meta_info(const ObBackupPGMetaInfo& pg_meta_info)
{
  int ret = OB_SUCCESS;
  ObBackupMetaIndex meta_index;
  bool is_uploaded = false;
  const ObPGKey& pg_key = pg_meta_info.pg_meta_.pg_key_;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "meta writer not init", K(ret), K(is_inited_));
  } else if (!pg_meta_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "write backup pg meta info get invalid argument", K(ret), K(pg_meta_info));
  } else if (FALSE_IT(set_meta_index(pg_key, meta_index))) {
  } else if (OB_FAIL(meta_appender_.append_backup_pg_meta_info(pg_meta_info, meta_index, is_uploaded))) {
    STORAGE_LOG(WARN, "fail to write pg meta", K(ret), K(pg_meta_info));
  } else if (OB_FAIL(append_meta_index(is_uploaded, meta_index))) {
    STORAGE_LOG(WARN, "fail to write pg meta index", K(ret), K(pg_meta_info), K(meta_index));
  } else {
    STORAGE_LOG(INFO, "backup pg meta succ", K(pg_key), K(meta_index));
  }
  return ret;
}

/**********************ObBackupMacroBlockInfo***********************/
ObBackupMacroBlockInfo::ObBackupMacroBlockInfo()
    : table_key_(), start_index_(0), cur_block_count_(0), total_block_count_(0)
{}

bool ObBackupMacroBlockInfo::is_valid() const
{
  return table_key_.is_valid() && start_index_ >= 0 && cur_block_count_ >= 0 &&
         start_index_ + cur_block_count_ <= total_block_count_;
}

/**********************ObBackupPhysicalPGCtx***********************/
ObBackupPhysicalPGCtx::SubTask::SubTask() : block_info_(), block_count_(0)
{}

ObBackupPhysicalPGCtx::SubTask::~SubTask()
{
  reset();
}

void ObBackupPhysicalPGCtx::SubTask::reset()
{
  block_info_.reset();
  block_count_ = 0;
}

ObBackupPhysicalPGCtx::MacroIndexMergePoint::MacroIndexMergePoint()
    : table_id_(OB_INVALID_ID), sstable_idx_(0), macro_index_array_(NULL)
{}

void ObBackupPhysicalPGCtx::MacroIndexMergePoint::reset()
{
  table_id_ = OB_INVALID_ID;
  sstable_idx_ = 0;
  macro_index_array_ = NULL;
}

bool ObBackupPhysicalPGCtx::MacroIndexMergePoint::is_valid() const
{
  bool bool_ret = true;
  if (OB_INVALID_ID == table_id_ || NULL == macro_index_array_) {
    bool_ret = false;
  } else if (sstable_idx_ < 0 || sstable_idx_ >= macro_index_array_->count()) {
    bool_ret = false;
  }
  return bool_ret;
}

ObBackupPhysicalPGCtx::MacroIndexRetryPoint::MacroIndexRetryPoint() : table_key_(), last_idx_(-1)
{}

void ObBackupPhysicalPGCtx::MacroIndexRetryPoint::reset()
{
  table_key_.reset();
  last_idx_ = -1;
}

bool ObBackupPhysicalPGCtx::MacroIndexRetryPoint::is_valid() const
{
  return table_key_.is_valid() && last_idx_ >= 0;
}

ObBackupPhysicalPGCtx::ObBackupPhysicalPGCtx()
    : bandwidth_throttle_(NULL),
      table_keys_(),
      tasks_(),
      macro_index_appender_(),
      macro_block_count_(0),
      block_count_per_task_(MAX_MACRO_BLOCK_COUNT_PER_TASK),
      base_task_id_(0),
      retry_cnt_(0),
      cond_(),
      task_turn_(0),
      lock_(),
      result_(OB_SUCCESS),
      pg_key_(),
      backup_arg_(NULL),
      find_breakpoint_(false),
      retry_points_(),
      is_opened_(false),
      backup_data_type_(),
      is_inited_(false)
{}

ObBackupPhysicalPGCtx::~ObBackupPhysicalPGCtx()
{}

int ObBackupPhysicalPGCtx::init(common::ObInOutBandwidthThrottle& bandwidth_throttle,
    const share::ObPhysicalBackupArg& arg, const common::ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "backup physical context init twice", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid() || !pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(arg), K(pg_key));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::RESTORE_READER_COND_WAIT))) {
    STORAGE_LOG(WARN, "fail to init thread cond", K(ret));
  } else {
    bandwidth_throttle_ = &bandwidth_throttle;
    backup_arg_ = &arg;
    pg_key_ = pg_key;
    block_count_per_task_ = MAX_MACRO_BLOCK_COUNT_PER_TASK;

#ifdef ERRSIM
    block_count_per_task_ = GCONF._max_block_per_backup_task;
    if (0 == block_count_per_task_) {
      block_count_per_task_ = MAX_MACRO_BLOCK_COUNT_PER_TASK;
    }
    STORAGE_LOG(INFO, "set small block_count_per_task_ succ", K(ret), K(block_count_per_task_));
#endif
    is_inited_ = true;
  }
  return ret;
}

int ObBackupPhysicalPGCtx::open()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "physcial backup ctx do not init", K(ret));
  } else if (is_opened_) {
    ret = OB_OPEN_TWICE;
    STORAGE_LOG(WARN, "physical backup ctx open twice", K(ret));
  } else if (OB_FAIL(init_already_backup_data())) {
    STORAGE_LOG(WARN, "init already backup data fail", K(ret));
  } else if (OB_FAIL(init_macro_index_appender())) {
    STORAGE_LOG(WARN, "init macro index appender fail", K(ret));
  } else {
    is_opened_ = true;
  }
  return ret;
}

int ObBackupPhysicalPGCtx::open(common::ObInOutBandwidthThrottle& bandwidth_throttle,
    const share::ObPhysicalBackupArg& arg, const common::ObPGKey& pg_key, const ObBackupDataType& backup_data_type)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "physical backup ctx do not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid() || !pg_key.is_valid()) || !backup_data_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(arg), K(pg_key), K(backup_data_type));
  } else {
    bandwidth_throttle_ = &bandwidth_throttle;
    backup_arg_ = &arg;
    pg_key_ = pg_key;
    block_count_per_task_ = MAX_MACRO_BLOCK_COUNT_PER_TASK;
    backup_data_type_ = backup_data_type;

#ifdef ERRSIM
    block_count_per_task_ = GCONF._max_block_per_backup_task;
    if (0 == block_count_per_task_) {
      block_count_per_task_ = MAX_MACRO_BLOCK_COUNT_PER_TASK;
    }
    STORAGE_LOG(INFO, "set small block_count_per_task_ succ", K(ret), K(block_count_per_task_));
#endif

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_already_backup_data())) {
      STORAGE_LOG(WARN, "init already backup data fail", K(ret));
    } else if (OB_FAIL(init_macro_index_appender())) {
      STORAGE_LOG(WARN, "init macro index appender fail", K(ret));
    } else {
      is_opened_ = true;
    }
  }
  return ret;
}

void ObBackupPhysicalPGCtx::reset()
{
  int tmp_ret = OB_SUCCESS;
  bandwidth_throttle_ = NULL;
  table_keys_.reset();
  tasks_.reset();
  macro_block_count_ = 0;
  block_count_per_task_ = 0;
  base_task_id_ = 0;
  retry_cnt_ = 0;

  task_turn_ = 0;
  index_merge_point_.reset();
  pg_key_.reset();
  backup_arg_ = NULL;
  find_breakpoint_ = false;
  retry_points_.reset();
  if (is_opened_) {
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(ERROR, "failed to close physical backup ctx", K(tmp_ret));
    }
  }
  cond_.destroy();
  result_ = OB_SUCCESS;
  is_opened_ = false;
  is_inited_ = false;
}

int ObBackupPhysicalPGCtx::close()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "physical backup ctx is not init", K(ret));
  } else if (!is_opened_) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not opened", K(ret));
  } else if (OB_FAIL(macro_index_appender_.close())) {
    LOG_WARN("failed to close macro index appender", K(ret));
  }

  if (OB_FAIL(ret) && OB_SUCCESS == result_) {
    result_ = ret;
  }
  is_opened_ = false;
  return ret;
}

bool ObBackupPhysicalPGCtx::is_valid() const
{
  bool valid = true;
  if (0 == tasks_.count() || 0 == table_keys_.count() || !macro_index_appender_.is_valid() || 0 == macro_block_count_) {
    valid = false;
  } else if (!is_opened_ || !is_inited_) {
    valid = false;
  } else {
    for (int64_t i = 0; valid && i < tasks_.count(); ++i) {
      if (tasks_.at(i).block_count_ <= 0 || 0 == tasks_.at(i).block_info_.count()) {
        valid = false;
        STORAGE_LOG(WARN, "sub task not valid", K(i), K(tasks_.count()), K(tasks_.at(i)));
      }
    }
  }
  return valid;
}

int64_t ObBackupPhysicalPGCtx::get_task_count() const
{
  return tasks_.count();
}

void ObBackupPhysicalPGCtx::set_result(const int32_t ret)
{
  if (OB_SUCCESS != ret) {
    common::SpinWLockGuard guard(lock_);
    result_ = OB_SUCCESS == result_ ? ret : result_;
  }
}

int ObBackupPhysicalPGCtx::get_result() const
{
  int ret = OB_SUCCESS;
  {
    common::SpinRLockGuard guard(lock_);
    ret = result_;
  }
  return ret;
}

int ObBackupPhysicalPGCtx::add_backup_macro_block_info(const ObBackupMacroBlockInfo& block_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "physical backup ctx do not init", K(ret));
  } else if (!is_opened_) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not opened yet", K(ret));
  } else if (OB_UNLIKELY(!block_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ObBackupMacroBlockInfo is not valid,", K(block_info));
  } else {
    ObBackupMacroBlockInfo remain_info = block_info;
    if (retry_cnt_ > 0) {
      if (OB_FAIL(reuse_already_backup_data(remain_info))) {
        STORAGE_LOG(WARN, "reuse backup data failed,", K(ret), K(remain_info));
      }
    }
    while (OB_SUCC(ret) && remain_info.cur_block_count_ > 0) {
      SubTask* last_task = NULL;
      if (OB_UNLIKELY(!remain_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "remain blockinfo is not valid,", K(remain_info));
      } else if (OB_FAIL(fetch_available_sub_task(last_task))) {
        STORAGE_LOG(WARN, "fetch last available sub task failed,", K(ret), K(remain_info));
      } else if (OB_ISNULL(last_task)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "last task is null", K(ret), K(remain_info));
      } else {
        int64_t last_remain_cnt = block_count_per_task_ - last_task->block_count_;
        ObBackupMacroBlockInfo add_info = remain_info;
        if (add_info.cur_block_count_ > last_remain_cnt) {
          add_info.cur_block_count_ = last_remain_cnt;
          remain_info.start_index_ += last_remain_cnt;
          remain_info.cur_block_count_ -= last_remain_cnt;
        } else {
          remain_info.cur_block_count_ = 0;
        }

        if (OB_UNLIKELY(!add_info.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "block info is not valid,", K(add_info));
        } else if (OB_FAIL(last_task->block_info_.push_back(add_info))) {
          STORAGE_LOG(WARN, "push back block_info failed", K(ret), K(add_info));
        } else {
          last_task->block_count_ += add_info.cur_block_count_;
          macro_block_count_ += add_info.cur_block_count_;
        }
      }
    }
    FLOG_INFO("[PG_BACKUP] add backup macro block info", K(block_info), K(remain_info));
  }
  return ret;
}

int ObBackupPhysicalPGCtx::wait_for_turn(const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "phiscal backup ctx do not init", K(ret));
  } else if (!is_opened_) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not opened yet", K(ret));
  }

  while (OB_SUCC(ret) && task_idx != ATOMIC_LOAD(&task_turn_)) {
    if (OB_FAIL(get_result())) {
      STORAGE_LOG(WARN, "backup pg task already failed", K(ret), K(task_idx));
      break;
    }
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(cond_.wait_us(DEFAULT_WAIT_TIME))) {
      if (OB_TIMEOUT == ret) {
        ret = OB_SUCCESS;
        STORAGE_LOG(WARN, "waiting for turn to build macro index is too slow", K(ret), K(task_idx), K(task_turn_));
      }
    }
  }
  return ret;
}

int ObBackupPhysicalPGCtx::finish_task(const int64_t task_idx)
{
  int ret = OB_SUCCESS;

  if (!is_opened_) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not opened yet", K(ret));
  } else if (OB_UNLIKELY(task_idx != ATOMIC_LOAD(&task_turn_))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "backup data, build index order unexpected", K(ret), K(task_idx), K(task_turn_));
  } else {
    ObThreadCondGuard guard(cond_);
    ATOMIC_INC(&task_turn_);
    if (OB_FAIL(cond_.broadcast())) {
      STORAGE_LOG(ERROR, "Fail to broadcast other condition, ", K(ret));
    }
  }
  return ret;
}

int ObBackupPhysicalPGCtx::fetch_prev_macro_index(const ObPhyRestoreMacroIndexStoreV2& macro_index_store,
    const ObBackupMacroBlockArg& macro_arg, ObBackupTableMacroIndex& macro_index)
{
  int ret = OB_SUCCESS;

  if (!is_opened_) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not opened yet", K(ret));
  } else if (OB_UNLIKELY(!macro_index_store.is_inited() || !macro_arg.is_valid() ||
                         !macro_arg.table_key_ptr_->is_major_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(macro_index_store), K(macro_arg));
  } else {
    const uint64_t table_id = macro_arg.table_key_ptr_->table_id_;
    if (table_id != index_merge_point_.table_id_) {
      index_merge_point_.table_id_ = table_id;
      index_merge_point_.sstable_idx_ = 0;

      if (OB_FAIL(macro_index_store.get_major_macro_index_array(table_id, index_merge_point_.macro_index_array_))) {
        STORAGE_LOG(WARN, "get macro index array fail", K(ret), K(table_id));
      } else if (OB_ISNULL(index_merge_point_.macro_index_array_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "macro_index_array_ should not be null here", K(ret), K(table_id), K(index_merge_point_));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!index_merge_point_.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "index_merge_point_ is not vaild", K(ret), K(table_id), K(index_merge_point_));
      } else {
        int64_t& idx = index_merge_point_.sstable_idx_;
        const common::ObArray<ObBackupTableMacroIndex>* index_array = index_merge_point_.macro_index_array_;
        while (OB_SUCC(ret)) {
          if (idx >= index_array->count()) {
            ret = OB_ERR_SYS;
            STORAGE_LOG(WARN, "get prev macro block index fail", K(ret), K(macro_arg), K(idx));
            break;
          }
          const ObBackupTableMacroIndex& tmp_index = index_array->at(idx);
          if (macro_arg.fetch_arg_.data_version_ == tmp_index.data_version_) {
            if (macro_arg.fetch_arg_.data_seq_ == tmp_index.data_seq_) {  // reuse
              macro_index = tmp_index;
              STORAGE_LOG(DEBUG, "backup macro block reuse", K(macro_index));
              ++idx;
              break;
            } else if (macro_arg.fetch_arg_.data_seq_ < tmp_index.data_seq_) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "", K(ret), K(macro_arg), K(tmp_index));
              break;
            }
          }
          ++idx;
        }
      }
    }
  }
  return ret;
}

int ObBackupPhysicalPGCtx::fetch_available_sub_task(SubTask*& sub_task)
{
  int ret = OB_SUCCESS;
  sub_task = NULL;
  const int64_t last_idx = tasks_.count() - 1;
  const int64_t max_block_cnt = block_count_per_task_;
  bool need_new_task = false;
  if (last_idx < 0) {
    need_new_task = true;
  } else {
    int64_t last_block_cnt = tasks_.at(last_idx).block_count_;
    if (max_block_cnt > last_block_cnt) {
      // need_new_task = false
    } else if (max_block_cnt == last_block_cnt) {
      need_new_task = true;
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "last task has too much macro block,", K(ret), K(last_block_cnt), K(max_block_cnt));
    }
  }
  if (OB_SUCC(ret) && need_new_task) {
    SubTask tmp_task;
    if (OB_FAIL(tasks_.push_back(tmp_task))) {
      STORAGE_LOG(WARN, "push back sub task failed", K(ret), K(tasks_.count()));
    }
  }
  if (OB_SUCC(ret)) {
    sub_task = &(tasks_.at(tasks_.count() - 1));
  }
  return ret;
}

int ObBackupPhysicalPGCtx::init_already_backup_data()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(backup_arg_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup arg should not be null here", K(ret), KP(backup_arg_));
  } else {
    ObBackupBaseDataPathInfo path_info;
    ObBackupPath path;
    int64_t max_index_file_id = -1;
    int64_t max_macro_file_id = -1;
    if (OB_FAIL(backup_arg_->get_backup_base_data_info(path_info))) {
      STORAGE_LOG(WARN, "failed to get base data info", K(ret));
    } else if (OB_FAIL(get_tenant_pg_data_path(path_info, path))) {
      STORAGE_LOG(WARN, "failed to get tenant pg data path", K(ret), K(path_info), K(pg_key_));
    } else if (OB_FAIL(ObRestoreFileUtil::fetch_max_backup_file_id(path.get_obstr(),
                   path_info.dest_.get_storage_info(),
                   path_info.inc_backup_set_id_,
                   max_index_file_id,
                   max_macro_file_id))) {
      STORAGE_LOG(WARN, "failed to get max macro index file id", K(ret), K(path));
    } else if (OB_UNLIKELY(max_index_file_id < -1 || max_macro_file_id < -1)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "failed to get max macro index file id", K(ret));
    } else {
      retry_cnt_ = max_index_file_id + 1;
      base_task_id_ = max_macro_file_id + 1;
      for (int i = 0; OB_SUCC(ret) && i <= max_index_file_id; ++i) {
        path.reset();
        if (OB_FAIL(get_macro_block_index_path(path_info, i, path))) {
          STORAGE_LOG(WARN, "failed to get macro block index path", K(ret), K(path_info), K(i), K(pg_key_));
        } else if (OB_FAIL(fetch_retry_points(path.get_obstr(), path_info.dest_.get_storage_info()))) {
          STORAGE_LOG(WARN, "failed to fetch retry points", K(ret), K(i), K(path_info), K(pg_key_));
        }
      }
    }
  }
  return ret;
}

int ObBackupPhysicalPGCtx::fetch_retry_points(const ObString& path, const ObString& storage_info)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::BACKUP);
  char* read_buf = NULL;
  int64_t read_size = 0;
  int64_t sstable_macro_block_index = -1;

  if (OB_FAIL(ObRestoreFileUtil::read_one_file(path, storage_info, allocator, read_buf, read_size))) {
    STORAGE_LOG(WARN, "fail to read index file", K(ret), K(path));
  } else if (read_size <= 0) {
    STORAGE_LOG(INFO, "may be empty block index file", K(ret), K(path));
  } else {
    ObBufferReader buffer_reader(read_buf, read_size, 0);
    const ObBackupCommonHeader* common_header = NULL;
    ObBackupTableKeyInfo table_key_info;
    ObBackupTableMacroIndex macro_index;
    MacroIndexRetryPoint point;
    if (retry_points_.count() > 0) {
      if (OB_FAIL(retry_points_.pop_back(point))) {
        STORAGE_LOG(WARN, "fetch last point fail", K(ret), K(retry_points_));
      }
    }
    while (OB_SUCC(ret) && buffer_reader.remain() > 0) {
      common_header = NULL;
      int64_t end_pos = 0;
      if (OB_FAIL(buffer_reader.get(common_header))) {
        STORAGE_LOG(WARN, "read macro index common header fail", K(ret));
      } else if (OB_ISNULL(common_header)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "macro index common header is null", K(ret));
      } else if (OB_FAIL(common_header->check_valid())) {
        STORAGE_LOG(WARN, "common_header is not vaild", K(ret));
      } else if (common_header->data_length_ > buffer_reader.remain()) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "buffer_reader not enough", K(ret));
      } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_length_))) {
        STORAGE_LOG(WARN, "common header data checksum fail", K(ret));
      } else if (FALSE_IT(end_pos = buffer_reader.pos() + common_header->data_length_)) {
      } else {
        while (OB_SUCC(ret) && buffer_reader.pos() < end_pos) {
          if (-1 == sstable_macro_block_index) {
            if (OB_FAIL(buffer_reader.read_serialize(table_key_info))) {
              STORAGE_LOG(WARN, "failed to read backup table key meta", K(ret));
            } else if (0 == table_key_info.total_macro_block_count_) {
              sstable_macro_block_index = -1;
              table_key_info.reset();
            } else {
              sstable_macro_block_index = 0;
            }
          } else {
            if (OB_FAIL(buffer_reader.read_serialize(macro_index))) {
              STORAGE_LOG(WARN, "read macro index fail", K(ret));
            } else {
              sstable_macro_block_index = macro_index.sstable_macro_index_;
              if (point.table_key_ != table_key_info.table_key_) {
                if (point.is_valid()) {
                  if (OB_FAIL(retry_points_.push_back(point))) {
                    STORAGE_LOG(WARN, "push back retry point fail", K(ret), K(point));
                  } else {
                    point.reset();
                  }
                }
                if (OB_FAIL(ret)) {
                } else if (OB_UNLIKELY(0 != macro_index.sstable_macro_index_)) {
                  ret = OB_ERR_UNEXPECTED;
                  STORAGE_LOG(WARN, "macro_index is not continue", K(ret), K(point), K(macro_index));
                } else {
                  point.table_key_ = table_key_info.table_key_;
                  point.last_idx_ = macro_index.sstable_macro_index_;
                }
              } else {
                if (point.last_idx_ + 1 != macro_index.sstable_macro_index_) {
                  ret = OB_ERR_UNEXPECTED;
                  STORAGE_LOG(WARN, "macro_index is not continue", K(ret), K(point), K(macro_index));
                } else {
                  point.last_idx_ = macro_index.sstable_macro_index_;
                }
              }
            }

            if (OB_SUCC(ret)) {
              if (table_key_info.total_macro_block_count_ - 1 == sstable_macro_block_index) {
                sstable_macro_block_index = -1;
                table_key_info.reset();
              }
            }
          }
        }

        if (OB_SUCC(ret) && common_header->align_length_ > 0) {
          if (OB_FAIL(buffer_reader.advance(common_header->align_length_))) {
            STORAGE_LOG(WARN, "buffer_reader buf not enough", K(ret), K(*common_header));
          }
        }
      }
    }
    if (OB_SUCC(ret) && point.is_valid()) {
      if (OB_FAIL(retry_points_.push_back(point))) {
        STORAGE_LOG(WARN, "push back retry point fail", K(ret), K(point));
      }
    }
    STORAGE_LOG(INFO, "[PG_BACKUP]fetch retry point succ", K(path), K(base_task_id_), K(retry_points_));
  }
  return ret;
}

int ObBackupPhysicalPGCtx::init_macro_index_appender()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(backup_arg_) || OB_ISNULL(bandwidth_throttle_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "arg should not be null here", K(ret), KP(backup_arg_), KP(bandwidth_throttle_));
  } else {
    const ObBackupFileType type = ObBackupFileType::BACKUP_MACRO_DATA_INDEX;
    ObBackupBaseDataPathInfo path_info;
    ObBackupPath path;
    if (OB_FAIL(backup_arg_->get_backup_base_data_info(path_info))) {
      STORAGE_LOG(WARN, "failed to get base data info", K(ret));
    } else if (OB_FAIL(get_macro_block_index_path(path_info, retry_cnt_, path))) {
      STORAGE_LOG(WARN, "failed to get macro block index path", K(ret), K(path_info), K(pg_key_));
    } else if (OB_FAIL(macro_index_appender_.open(*bandwidth_throttle_, *backup_arg_, path.get_obstr(), type))) {
      STORAGE_LOG(WARN, "failed to init macro index appender", K(ret), K(path_info), K(pg_key_));
    }
  }
  return ret;
}

int ObBackupPhysicalPGCtx::reuse_already_backup_data(ObBackupMacroBlockInfo& block_info)
{
  int ret = OB_SUCCESS;
  if (0 == retry_points_.count()) {  // may be first backup
  } else if (OB_UNLIKELY(!block_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ObBackupMacroBlockInfo", K(ret), K(block_info));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < retry_points_.count(); ++i) {
      const MacroIndexRetryPoint& point = retry_points_.at(i);
      if (block_info.table_key_ == point.table_key_) {
        if (block_info.total_block_count_ <= point.last_idx_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "sstable idx is unecpected", K(ret), K(block_info), K(point));
        } else if (block_info.total_block_count_ == point.last_idx_ + 1) {
          // full sstable is already backup
          block_info.cur_block_count_ = 0;
          block_info.start_index_ = block_info.total_block_count_;
          FLOG_INFO("[PG_BACKUP] reuse all macro block", K(point), K(block_info));
        } else if (find_breakpoint_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN,
              "macro index file has more than two breakpoint",
              K(ret),
              K(find_breakpoint_),
              K(block_info),
              K(point));
        } else {
          block_info.start_index_ = point.last_idx_ + 1;
          block_info.cur_block_count_ = block_info.total_block_count_ - block_info.start_index_;
          find_breakpoint_ = true;
          FLOG_INFO("[PG_BACKUP] reuse already_backup_data succ", K(find_breakpoint_), K(block_info), K(point));
        }
      }
    }
  }
  return ret;
}

int ObBackupPhysicalPGCtx::check_table_exist(
    const ObITable::TableKey& table_key, const ObPhyRestoreMacroIndexStoreV2& macro_index_store, bool& is_exist)
{
  int ret = OB_SUCCESS;
  const ObArray<ObBackupTableMacroIndex>* macro_index_array = NULL;
  is_exist = false;
  if (OB_UNLIKELY(!macro_index_store.is_inited() || !table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(macro_index_store), K(table_key));
  } else if (OB_FAIL(macro_index_store.get_macro_index_array(table_key, macro_index_array))) {
    if (OB_HASH_NOT_EXIST == ret) {
      is_exist = false;
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(macro_index_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro index array should not be NULL", K(ret), K(table_key));
  } else {
    is_exist = true;
  }
  return ret;
}

int ObBackupPhysicalPGCtx::get_tenant_pg_data_path(const ObBackupBaseDataPathInfo& path_info, ObBackupPath& path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_ISNULL(backup_arg_) || !backup_data_type_.is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup physical PG ctx do not init", K(ret), KP(backup_arg_), K(backup_data_type_));
  } else if (backup_data_type_.is_major_backup()) {
    if (OB_FAIL(ObBackupPathUtil::get_tenant_pg_major_data_path(
            path_info, pg_key_.get_table_id(), pg_key_.get_partition_id(), path))) {
      STORAGE_LOG(WARN, "failed to get tenant pg data path", K(ret), K(path_info), K(pg_key_));
    }
  } else {
    if (OB_FAIL(ObBackupPathUtil::get_tenant_pg_minor_data_path(
            path_info, pg_key_.get_table_id(), pg_key_.get_partition_id(), backup_arg_->task_id_, path))) {
      STORAGE_LOG(WARN, "failed to get tenant pg minor data path", K(ret), K(path_info), K(pg_key_));
    }
  }
  return ret;
}

int ObBackupPhysicalPGCtx::get_macro_block_index_path(
    const ObBackupBaseDataPathInfo& path_info, const int64_t retry_cnt, ObBackupPath& path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_ISNULL(backup_arg_) || !backup_data_type_.is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup physical PG ctx do not init", K(ret), KP(backup_arg_), K(backup_data_type_));
  } else if (backup_data_type_.is_major_backup()) {
    if (OB_FAIL(ObBackupPathUtil::get_major_macro_block_index_path(
            path_info, pg_key_.get_table_id(), pg_key_.get_partition_id(), retry_cnt, path))) {
      STORAGE_LOG(WARN, "failed to get tenant pg data path", K(ret), K(path_info), K(pg_key_));
    }
  } else {
    if (OB_FAIL(ObBackupPathUtil::get_minor_macro_block_index_path(
            path_info, pg_key_.get_table_id(), pg_key_.get_partition_id(), backup_arg_->task_id_, retry_cnt, path))) {
      STORAGE_LOG(WARN, "failed to get tenant pg minor data path", K(ret), K(path_info), K(pg_key_));
    }
  }
  return ret;
}

/**********************ObBackupCopyPhysicalTask***********************/
ObBackupCopyPhysicalTask::ObBackupCopyPhysicalTask()
    : ObITask(TASK_TYPE_MIGRATE_COPY_PHYSICAL),
      is_inited_(false),
      ctx_(NULL),
      task_idx_(-1),
      base_task_id_(0),
      backup_pg_ctx_(NULL),
      sub_task_(NULL),
      cp_fty_(NULL),
      checker_(),
      already_backup_table_key_(),
      output_macro_data_bytes_(0),
      input_macro_data_bytes_(0)
{}

ObBackupCopyPhysicalTask::~ObBackupCopyPhysicalTask()
{}

int ObBackupCopyPhysicalTask::init(const int64_t task_idx, ObMigrateCtx& ctx)
{
  int ret = OB_SUCCESS;
  ObBackupPhysicalPGCtx& physical_backup_ctx = ctx.physical_backup_ctx_;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(task_idx < 0 || !physical_backup_ctx.is_valid() ||
                         task_idx >= physical_backup_ctx.get_task_count() || !ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid args",
        K(ret),
        K(task_idx),
        K(physical_backup_ctx.is_valid()),
        K(ctx.is_valid()),
        K(physical_backup_ctx),
        K(ctx));
  } else {
    is_inited_ = true;
    ctx_ = &ctx;
    task_idx_ = task_idx;
    base_task_id_ = physical_backup_ctx.base_task_id_;
    backup_pg_ctx_ = &physical_backup_ctx;
    sub_task_ = &(physical_backup_ctx.tasks_.at(task_idx_));
    cp_fty_ = ObPartitionMigrator::get_instance().get_cp_fty();
  }
  return ret;
}

int ObBackupCopyPhysicalTask::init(
    const int64_t task_idx, const ObITable::TableKey& already_backup_table_key, ObMigrateCtx& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret), K(is_inited_));
  } else if (!already_backup_table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "init backup copy physical task get invalid argument", K(ret), K(already_backup_table_key));
  } else if (OB_FAIL(init(task_idx, ctx))) {
    STORAGE_LOG(WARN, "failed to init backup copy physical task", K(ret));
  } else {
    already_backup_table_key_ = already_backup_table_key;
  }
  return ret;
}

int ObBackupCopyPhysicalTask::get_macro_block_backup_reader(const ObIArray<ObBackupMacroBlockArg>& list,
    const ObPhysicalBackupArg& backup_arg, ObPartitionMacroBlockBackupReader*& reader)
{
  int ret = OB_SUCCESS;
  ObPartitionMacroBlockBackupReader* tmp_reader = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(tmp_reader = cp_fty_->get_macro_block_backup_reader())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to get macro block ob reader", K(ret));
  } else if (OB_FAIL(tmp_reader->init(backup_arg, list))) {
    STORAGE_LOG(WARN, "failed to init backup reader", K(ret), K(list.count()));
  } else {
    reader = tmp_reader;
    tmp_reader = NULL;
  }

  if (NULL != cp_fty_) {
    if (OB_FAIL(ret)) {
      if (NULL != reader) {
        cp_fty_->free(reader);
        reader = NULL;
      }
    }
    if (NULL != tmp_reader) {
      cp_fty_->free(tmp_reader);
      tmp_reader = NULL;
    }
  }
  return ret;
}

int ObBackupCopyPhysicalTask::generate_next_task(ObITask*& next_task)
{
  int ret = OB_SUCCESS;
  const int64_t next_task_idx = task_idx_ + 1;
  ObBackupCopyPhysicalTask* tmp_next_task = NULL;
  const ObITable::TableKey& already_backup_table_key =
      sub_task_->block_info_.at(sub_task_->block_info_.count() - 1).table_key_;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(backup_pg_ctx_->get_result())) {
    STORAGE_LOG(WARN, "backup task has already failed", K(ret), K(task_idx_));
  } else if (task_idx_ + 1 == backup_pg_ctx_->get_task_count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(dag_->alloc_task(tmp_next_task))) {
    STORAGE_LOG(WARN, "failed to alloc task", K(ret));
  } else if (OB_FAIL(tmp_next_task->init(next_task_idx, already_backup_table_key, *ctx_))) {
    STORAGE_LOG(WARN, "failed o init next task", K(ret), K(already_backup_table_key_));
  } else {
    next_task = tmp_next_task;
  }
  return ret;
}

int ObBackupCopyPhysicalTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  common::ObArray<ObBackupMacroBlockArg> list;
  ObBackupMacroBlockArg macro_arg;
  int64_t copy_count = 0;
  int64_t reuse_count = 0;
  DEBUG_SYNC(BEFORE_MIGRATE_COPY_BASE_DATA);

  if (NULL != ctx_) {
    STORAGE_LOG(INFO, "start ObBackupCopyPhysicalTask process", K(ctx_->pg_meta_.pg_key_), K(task_idx_));
  }

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(0 == sub_task_->block_count_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "current task is empty task", K(ret), K(task_idx_), K(*sub_task_));
  } else {
    const share::ObPhysicalBackupArg& backup_arg = ctx_->replica_op_arg_.backup_arg_;
    for (int64_t i = 0; OB_SUCC(ret) && i < sub_task_->block_info_.count(); ++i) {
      const ObBackupMacroBlockInfo& block_info = sub_task_->block_info_.at(i);
      if (OB_UNLIKELY(!block_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "current task is empty task", K(ret), K(i), K(block_info));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < block_info.cur_block_count_; ++j) {
        macro_arg.reset();
        const int64_t macro_index = block_info.start_index_ + j;

        if (OB_FAIL(fetch_backup_macro_block_arg(backup_arg, block_info.table_key_, macro_index, macro_arg))) {
          STORAGE_LOG(WARN, "fetch backup macro block arg fail", K(ret), K(block_info.table_key_), K(macro_index));
        } else if (OB_FAIL(list.push_back(macro_arg))) {
          STORAGE_LOG(WARN, "failed to add list", K(ret));
        } else {
          if (macro_arg.need_copy_) {
            ++copy_count;
          } else {
            ++reuse_count;
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (list.count() != sub_task_->block_count_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ObMigrateArgMacroBlockInfo list", K(ret), K(task_idx_), K(*sub_task_), K(list.count()));
  } else if (OB_FAIL(fetch_physical_block_with_retry(list, copy_count, reuse_count))) {
    STORAGE_LOG(WARN, "failed to fetch major block", K(ret), K(list.count()));
  } else if (OB_SUCCESS != (tmp_ret = calc_migrate_data_statics(copy_count, reuse_count))) {
    STORAGE_LOG(WARN, "failed to calc migrate data statics", K(tmp_ret));
  }

  if (OB_FAIL(ret) && NULL != ctx_) {
    if (ctx_->is_need_retry(ret)) {
      ctx_->need_rebuild_ = true;
      STORAGE_LOG(INFO, "backup task need retry", K(ret), K(*ctx_));
    }
    ctx_->set_result_code(ret);
    backup_pg_ctx_->set_result(ret);
  }

  if (NULL != ctx_) {
    STORAGE_LOG(INFO, "finish ObBackupCopyPhysicalTask process", K(ret), K(task_idx_), "pkey", ctx_->pg_meta_.pg_key_);
  }
  return ret;
}

int ObBackupCopyPhysicalTask::fetch_backup_macro_block_arg(const share::ObPhysicalBackupArg& backup_arg,
    const ObITable::TableKey& table_key, const int64_t macro_idx, ObBackupMacroBlockArg& macro_arg)
{
  int ret = OB_SUCCESS;
  ObTableHandle tmp_handle;
  ObSSTable* sstable = NULL;
  blocksstable::ObMacroBlockMetaHandle meta_handle;
  ObFullMacroBlockMeta full_meta;

  if (!table_key.is_valid() || macro_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "table_key is null or macro idx invalid", K(table_key), K(macro_idx), K(ret));
  } else if (OB_FAIL(ObPartitionService::get_instance().acquire_sstable(table_key, tmp_handle))) {
    STORAGE_LOG(WARN, "failed to get table handle", K(table_key), K(ret));
  } else if (OB_FAIL(tmp_handle.get_sstable(sstable))) {
    STORAGE_LOG(WARN, "failed to get table", K(table_key), K(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable should not be null here", K(ret));
  } else {
    const ObSSTable::ObSSTableGroupMacroBlocks& macro_list = sstable->get_total_macro_blocks();
    if (OB_UNLIKELY(macro_idx >= macro_list.count())) {
      ret = OB_ARRAY_OUT_OF_RANGE;
      STORAGE_LOG(WARN, "macro_idx is out of range in macro list", K(macro_list.count()), K(macro_idx), K(ret));
    } else if (OB_FAIL(sstable->get_meta(macro_list.at(macro_idx), full_meta))) {
      STORAGE_LOG(WARN, "Fail to get macro meta, ", K(ret), "macro_block_id", macro_list.at(macro_idx));
    } else if (!full_meta.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "meta is null", K(ret), "macro_block_id", macro_list.at(macro_idx));
    } else {
      macro_arg.table_key_ptr_ = &table_key;
      macro_arg.fetch_arg_.macro_block_index_ = macro_idx;
      macro_arg.fetch_arg_.data_version_ = full_meta.meta_->data_version_;
      macro_arg.fetch_arg_.data_seq_ = full_meta.meta_->data_seq_;
      if (!table_key.is_major_sstable()) {
        macro_arg.need_copy_ = true;
      } else {
        switch (backup_arg.backup_type_) {
          case ObBackupType::FULL_BACKUP:
            macro_arg.need_copy_ = true;
            break;
          case ObBackupType::INCREMENTAL_BACKUP: {
            bool is_exist = false;
            ObPhyRestoreMacroIndexStoreV2* macro_index = NULL;
            if (FALSE_IT(macro_index = reinterpret_cast<ObPhyRestoreMacroIndexStoreV2*>(ctx_->macro_indexs_))) {
            } else if (OB_ISNULL(macro_index)) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "phaysical restore macro index should not be NULL", K(ret), KP(macro_index));
            } else if (OB_FAIL(backup_pg_ctx_->check_table_exist(table_key, *macro_index, is_exist))) {
              STORAGE_LOG(WARN, "failed to check table exist", K(ret), K(macro_arg));
            } else if (!is_exist) {
              macro_arg.need_copy_ = true;
            } else {
              macro_arg.need_copy_ = full_meta.meta_->data_version_ > backup_arg.prev_data_version_;
            }
            break;
          }
          default:
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "unknown backup type", K(ret), K(backup_arg));
        }
      }
    }
  }
  return ret;
}

int ObBackupCopyPhysicalTask::fetch_physical_block_with_retry(
    const ObIArray<ObBackupMacroBlockArg>& list, const int64_t copy_count, const int64_t reuse_count)
{
  int ret = OB_SUCCESS;
  int64_t retry_times = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup copy physical task do not init", K(ret));
  } else if (0 == list.count()) {
    STORAGE_LOG(INFO, "no macro block need fetch", K(list.count()));
  } else if (OB_FAIL(backup_physical_block(list, copy_count, reuse_count))) {
    STORAGE_LOG(WARN, "failed to backup major block", K(ret), K(retry_times));
  }

  return ret;
}

int ObBackupCopyPhysicalTask::backup_physical_block(
    const ObIArray<ObBackupMacroBlockArg>& list, const int64_t copy_count, const int64_t reuse_count)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup copy physical task do not init", K(ret));
  } else if (0 == list.count()) {
    STORAGE_LOG(INFO, "no macro block need fetch", K(list.count()));
  } else if (OB_UNLIKELY(reuse_count + copy_count != list.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "macro block count not match", K(list.count()), K(reuse_count), K(copy_count));
  } else {
    ObBackupFileAppender macro_file;
    ObPartitionMacroBlockBackupReader* reader = NULL;
    const share::ObPhysicalBackupArg& backup_arg = ctx_->replica_op_arg_.backup_arg_;
    common::ObPGKey& pg_key = ctx_->pg_meta_.pg_key_;
    const ObBackupMacroBlockArg& first_macro_block_arg = list.at(0);

    // prepare appender and macro reader
    if (0 == copy_count) {
      STORAGE_LOG(INFO, "no macro block need copy", K(list.count()), K(reuse_count));
    } else if (!first_macro_block_arg.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro block arg is invalid", K(ret), K(first_macro_block_arg));
    } else if (OB_FAIL(get_datafile_appender(
                   first_macro_block_arg.table_key_ptr_->table_type_, backup_arg, pg_key, macro_file))) {
      STORAGE_LOG(WARN, "fail to data file appender", K(ret), K(pg_key));
    } else if (OB_FAIL(get_macro_block_backup_reader(list, backup_arg, reader))) {
      STORAGE_LOG(WARN, "fail to get macro block reader", K(ret));
    } else if (OB_ISNULL(reader)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro block reader should not be NULL", K(ret), KP(reader));
    } else if (copy_count != reader->get_block_count()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN,
          "macro block reader count not match",
          K(ret),
          K(copy_count),
          "readers count",
          reader->get_block_count());
    }

    // upload
    int write_idx = 0;
    ObArray<ObBackupTableMacroIndex> macro_indexs;
    ObBackupTableMacroIndex tmp_index;

    while (OB_SUCC(ret)) {
      if (ObPartGroupMigrator::get_instance().is_stop()) {
        ret = OB_SERVER_IS_STOPPING;
        STORAGE_LOG(WARN, "server is stop, interrupts copy data.", K(ret));
        break;
      } else if (ctx_->partition_guard_.get_partition_group()->is_removed()) {
        ret = OB_PG_IS_REMOVED;
        STORAGE_LOG(WARN, "partition has been removed, can not backup", K(ret), K(ctx_->replica_op_arg_.key_));
      } else if (OB_FAIL(backup_pg_ctx_->get_result())) {
        STORAGE_LOG(WARN, "backup pg task is already failed", K(ret), K(write_idx));
        break;
      } else if (write_idx >= sub_task_->block_count_) {
        // finish
        break;
      } else if (OB_UNLIKELY(write_idx >= list.count())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "idx is too large", K(ret), K(write_idx), K(list.count()));
        break;
      } else {
        dag_yield();
        const ObBackupMacroBlockArg& macro_arg = list.at(write_idx);
        if (!macro_arg.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "backup macro block arg is invalid", K(ret), K(macro_arg));
        } else {
          tmp_index.reset();
          tmp_index.table_key_ptr_ = macro_arg.table_key_ptr_;
          tmp_index.sstable_macro_index_ = macro_arg.fetch_arg_.macro_block_index_;
          tmp_index.data_version_ = macro_arg.fetch_arg_.data_version_;
          tmp_index.data_seq_ = macro_arg.fetch_arg_.data_seq_;

          if (macro_arg.need_copy_) {
            tmp_index.backup_set_id_ = backup_arg.backup_set_id_;
            tmp_index.sub_task_id_ = base_task_id_ + task_idx_;
            if (OB_ISNULL(reader)) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "macro block reader should not be null here", K(macro_arg));
            } else if (OB_FAIL(backup_block_data(*reader, macro_file, tmp_index))) {
              if (OB_ITER_END != ret) {
                STORAGE_LOG(WARN, "backup macro block data fail", K(ret), K(write_idx));
              } else {
                STORAGE_LOG(INFO, "get next macro block end");
                ret = OB_SUCCESS;
              }
              break;
            } else {
              input_macro_data_bytes_ += OB_DEFAULT_MACRO_BLOCK_SIZE;
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(macro_indexs.push_back(tmp_index))) {
            STORAGE_LOG(WARN, "failed to push index into array", K(ret), K(tmp_index));
          } else {
            ++write_idx;
          }
        }
      }
    }
#ifdef ERRSIM
    if (OB_SUCC(ret) && task_idx_ >= 2) {
      ret = E(EventTable::EN_BACKUP_MACRO_BLOCK_SUBTASK_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(INFO,
            "EN_BACKUP_MACRO_BLOCK_SUBTASK_FAILED",
            K(ret),
            K(task_idx_),
            K(macro_indexs),
            K(*backup_pg_ctx_),
            K(ctx_->replica_op_arg_.backup_arg_));
      }
    }
#endif

    if (OB_SUCC(ret)) {
      if (OB_LIKELY(write_idx != macro_indexs.count())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "index count and data count not match", K(ret), K(write_idx), K(macro_indexs.count()));
      } else if (OB_FAIL(backup_block_index(reuse_count, list, macro_indexs))) {
        STORAGE_LOG(WARN, "backup block index fail", K(ret), K(task_idx_));
      }
    }

    if (OB_SUCCESS != (tmp_ret = macro_file.close())) {
      LOG_WARN("failed to close macro file", K(tmp_ret), K(ret));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    } else {
      output_macro_data_bytes_ += macro_file.get_upload_size();
    }
    if (NULL != reader) {
      cp_fty_->free(reader);
    }
  }
  return ret;
}

int ObBackupCopyPhysicalTask::get_datafile_appender(const ObITable::TableType& table_type,
    const share::ObPhysicalBackupArg& arg, const common::ObPGKey& pg_key, ObBackupFileAppender& macro_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup copy physical task do not init", K(ret));
  } else if (OB_UNLIKELY(macro_file.is_valid())) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "macro_file is already inited", K(ret), K(macro_file));
  } else if (OB_UNLIKELY(!arg.is_valid() || !pg_key.is_valid() || ObITable::TableType::MAX_TABLE_TYPE == table_type)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid backup arg or pg_key", K(ret), K(arg), K(pg_key), K(table_type));
  } else {
    const ObBackupFileType type = ObBackupFileType::BACKUP_MACRO_DATA;
    ObBackupBaseDataPathInfo path_info;
    ObBackupPath path;
    const int64_t sub_task_id = base_task_id_ + task_idx_;
    common::ObInOutBandwidthThrottle* throttle = backup_pg_ctx_->bandwidth_throttle_;
    if (OB_ISNULL(throttle)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "BandwidthThrottle should not be null here", K(ret));
    } else if (OB_FAIL(arg.get_backup_base_data_info(path_info))) {
      STORAGE_LOG(WARN, "get backup base data info fail", K(ret), K(arg), K(pg_key));
    } else if (ObITable::is_major_sstable(table_type)) {
      if (OB_FAIL(ObBackupPathUtil::get_major_macro_block_file_path(path_info,
              pg_key.get_table_id(),
              pg_key.get_partition_id(),
              path_info.inc_backup_set_id_,
              sub_task_id,
              path))) {
        STORAGE_LOG(WARN, "failed to get macro file path", K(ret));
      }
    } else if (ObITable::is_minor_sstable(table_type)) {
      if (OB_FAIL(ObBackupPathUtil::get_minor_macro_block_file_path(path_info,
              pg_key.get_table_id(),
              pg_key.get_partition_id(),
              path_info.inc_backup_set_id_,
              arg.task_id_,
              sub_task_id,
              path))) {
        STORAGE_LOG(WARN, "failed to get macro file path", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "table type is unexpected", K(ret), K(table_type));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(macro_file.open(*throttle, arg, path.get_obstr(), type))) {
      STORAGE_LOG(WARN, "failed to init macro file", K(ret));
    }
  }
  return ret;
}

int ObBackupCopyPhysicalTask::backup_block_data(
    ObPartitionMacroBlockBackupReader& reader, ObBackupFileAppender& macro_file, ObBackupTableMacroIndex& block_index)
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId src_macro_id;

  blocksstable::ObFullMacroBlockMeta meta;
  blocksstable::ObBufferReader data(NULL, 0, 0);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup copy physical task do not init", K(ret));
  } else if (OB_UNLIKELY(!reader.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid backup reader", K(ret), K(macro_file));
  } else if (OB_UNLIKELY(!macro_file.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro file appender", K(ret), K(macro_file));
  } else if (OB_FAIL(reader.get_next_macro_block(meta, data, src_macro_id))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "failed to get next macro block", K(ret));
    }
  } else if (!meta.is_valid() || 0 == data.length()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "meta and data must not null", K(ret), K(meta), K(data.length()));
  } else if (OB_FAIL(checker_.check(data.data(), data.length(), meta, ObMacroBlockCheckLevel::CHECK_LEVEL_AUTO))) {
    LOG_WARN("failed to check macro block", K(ret), K(data), K(meta));
  } else if (OB_FAIL(macro_file.append_macroblock_data(meta, data, block_index))) {
    STORAGE_LOG(WARN, "append macro data fail", K(ret), K(meta), K(data.length()));
  }
  return ret;
}

int ObBackupCopyPhysicalTask::backup_block_index(const int64_t reuse_count, const ObIArray<ObBackupMacroBlockArg>& list,
    ObIArray<ObBackupTableMacroIndex>& macro_indexs)
{
  int ret = OB_SUCCESS;
  int64_t real_reuse_count = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup copy physical task do not init", K(ret));
  } else if (OB_UNLIKELY(macro_indexs.count() != sub_task_->block_count_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "macro index count mismatch", K(ret), K(macro_indexs.count()), K(sub_task_->block_count_));
  } else if (OB_FAIL(backup_pg_ctx_->wait_for_turn(task_idx_))) {
    STORAGE_LOG(WARN, "wait to write data index fail", K(ret), K(task_idx_));
  } else if (OB_FAIL(reuse_block_index(list, macro_indexs, real_reuse_count))) {
    STORAGE_LOG(WARN, "wait to write data index fail", K(ret), K(task_idx_));
  } else if (OB_UNLIKELY(reuse_count != real_reuse_count)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "reuse count not match", K(ret), K(task_idx_), K(reuse_count), K(real_reuse_count));
  } else if (OB_FAIL(
                 backup_pg_ctx_->macro_index_appender_.append_macro_index(macro_indexs, already_backup_table_key_))) {
    STORAGE_LOG(WARN, "write macro index fail", K(ret), K(task_idx_));
  } else if (OB_FAIL(backup_pg_ctx_->macro_index_appender_.sync_upload())) {
    STORAGE_LOG(WARN, "finish backup sub task fail", K(ret), K(task_idx_));
  } else if (OB_FAIL(backup_pg_ctx_->finish_task(task_idx_))) {
    STORAGE_LOG(WARN, "finish backup sub task fail", K(ret), K(task_idx_));
  } else {
  }
  return ret;
}

int ObBackupCopyPhysicalTask::reuse_block_index(
    const ObIArray<ObBackupMacroBlockArg>& list, ObIArray<ObBackupTableMacroIndex>& macro_indexs, int64_t& reuse_count)
{
  int ret = OB_SUCCESS;
  reuse_count = 0;
  ObPhyRestoreMacroIndexStoreV2* macro_index = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup copy physical task do not init", K(ret));
  } else if (OB_UNLIKELY(list.count() != macro_indexs.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "arg list and index list count not match", K(ret), K(list.count()), K(macro_indexs.count()));
  } else if (FALSE_IT(macro_index = reinterpret_cast<ObPhyRestoreMacroIndexStoreV2*>(ctx_->macro_indexs_))) {
  } else {
    ObBackupTableMacroIndex prev_index;
    for (int i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
      const ObBackupMacroBlockArg& macro_arg = list.at(i);
      ObBackupTableMacroIndex& cur_index = macro_indexs.at(i);
      if (macro_arg.need_copy_) {
        if (OB_FAIL(!cur_index.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "macro index is invalid", K(ret), K(i), K(cur_index));
        }
      } else {
        if (OB_ISNULL(macro_index)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "phaysical restore macro index should not be NULL", K(ret), KP(macro_index));
        } else if (OB_UNLIKELY(cur_index.data_length_ > 0)) {
          ret = OB_INIT_TWICE;
          STORAGE_LOG(WARN, "macro index is already init before reuse.", K(ret), K(i), K(cur_index));
        } else if (!cur_index.table_key_ptr_->is_major_sstable()) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "sstable is not major sstable, can not reuse block index", K(ret), K(cur_index));
        } else if (OB_FAIL(backup_pg_ctx_->fetch_prev_macro_index(*macro_index, macro_arg, prev_index))) {
          STORAGE_LOG(WARN, "fetch prev macro index fail", K(ret), K(macro_arg));
        } else if (prev_index.table_key_ptr_->table_id_ != cur_index.table_key_ptr_->table_id_ ||
                   prev_index.data_version_ != cur_index.data_version_ || prev_index.data_seq_ != cur_index.data_seq_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "cur macro index and prev macro index not match", K(ret), K(cur_index), K(prev_index));
        } else {
          cur_index.backup_set_id_ = prev_index.backup_set_id_;
          cur_index.sub_task_id_ = prev_index.sub_task_id_;
          cur_index.offset_ = prev_index.offset_;
          cur_index.data_length_ = prev_index.data_length_;
          ++reuse_count;
        }
      }
    }
  }
  return ret;
}

int ObBackupCopyPhysicalTask::calc_migrate_data_statics(const int64_t copy_count, const int64_t reuse_count)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup copy physic task do not init", K(ret));
  } else {
    ctx_->action_ = ObMigrateCtx::COPY_MAJOR;
    ATOMIC_AAF(&ctx_->data_statics_.ready_macro_block_, sub_task_->block_count_);
    ATOMIC_AAF(&ctx_->data_statics_.major_count_, copy_count);
    ATOMIC_AAF(&ctx_->data_statics_.reuse_count_, reuse_count);
    ATOMIC_AAF(&ctx_->data_statics_.output_bytes_, output_macro_data_bytes_);
    ATOMIC_AAF(&ctx_->data_statics_.input_bytes_, input_macro_data_bytes_);
    if (OB_SUCCESS != (tmp_ret = ctx_->update_partition_migration_status())) {
      STORAGE_LOG(WARN, "failed to update_partition_migration_status", K(tmp_ret));
    }
  }
  return ret;
}

/**********************ObBackupCopyPhysicalTask***********************/
ObBackupFinishTask::ObBackupFinishTask() : ObITask(TASK_TYPE_MIGRATE_FINISH), is_inited_(false), ctx_(NULL)
{}

ObBackupFinishTask::~ObBackupFinishTask()
{}

int ObBackupFinishTask::init(ObMigrateCtx& ctx)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else {
    is_inited_ = true;
    ctx_ = &ctx;
  }
  return ret;
}

// TODO():pg backup finish, update progress status
int ObBackupFinishTask::process()
{
  int ret = OB_SUCCESS;
  if (NULL != ctx_) {
    STORAGE_LOG(INFO, "start ObBackupFinishTask process", "pkey", ctx_->replica_op_arg_.key_);
  }
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(ctx_->physical_backup_ctx_.close())) {
    STORAGE_LOG(WARN, "failed to close physical backup ctx", K(ret), "pkey", ctx_->replica_op_arg_.key_);
  } else {
    const int64_t end_macro_index_pos = ctx_->physical_backup_ctx_.macro_index_appender_.get_upload_size();
    ctx_->data_statics_.output_bytes_ += end_macro_index_pos;
    ctx_->data_statics_.finish_partition_count_ = ctx_->data_statics_.partition_count_;
    STORAGE_LOG(INFO, "backup task finished", K(ctx_->pg_meta_));
  }

  if (OB_FAIL(ret) && nullptr != ctx_) {
    ctx_->set_result_code(ret);
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBackupMacroData, meta_, data_);
ObBackupMacroData::ObBackupMacroData(blocksstable::ObBufferHolder& meta, blocksstable::ObBufferReader& data)
    : meta_(meta), data_(data)
{}

}  // namespace storage
}  // namespace oceanbase
