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
#include "observer/ob_server.h"
#include "lib/utility/ob_tracepoint.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_partition_migrator.h"
#include "ob_partition_base_data_physical_restore_v2.h"
#include "storage/ob_pg_storage.h"

using namespace oceanbase;
using namespace storage;
using namespace blocksstable;

/************************ObPartitionBaseDataMetaRestoreReaderV2************************/
ObPartitionBaseDataMetaRestoreReaderV2::ObPartitionBaseDataMetaRestoreReaderV2()
    : is_inited_(false),
      pkey_(),
      restore_info_(NULL),
      macro_indexs_(NULL),
      partition_store_meta_info_(NULL),
      last_read_size_(0),
      partition_store_meta_(),
      data_version_(0),
      schema_version_(OB_INVALID_VERSION)
{}

ObPartitionBaseDataMetaRestoreReaderV2::~ObPartitionBaseDataMetaRestoreReaderV2()
{
  reset();
}

void ObPartitionBaseDataMetaRestoreReaderV2::reset()
{
  is_inited_ = false;
  pkey_.reset();
  restore_info_ = NULL;
  macro_indexs_ = NULL;
  partition_store_meta_info_ = NULL;
  last_read_size_ = 0;
  partition_store_meta_.reset();
  data_version_ = 0;
  schema_version_ = OB_INVALID_VERSION;
}

int ObPartitionBaseDataMetaRestoreReaderV2::init(const common::ObPartitionKey& pkey,
    const ObPhysicalRestoreArg& restore_info, const ObPhyRestoreMacroIndexStoreV2& macro_indexs,
    const ObBackupPartitionStoreMetaInfo& partition_store_meta_info, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObPartitionKey src_pkey;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (!pkey.is_valid() || !restore_info.is_valid() || schema_version < OB_INVALID_VERSION) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(pkey), K(restore_info), K(schema_version));
  } else if (OB_FAIL(restore_info.change_dst_pkey_to_src_pkey(pkey, src_pkey))) {
    STORAGE_LOG(WARN, "failed to change dst pkey to src pkey", K(ret), K(pkey), K(restore_info));
  } else if (OB_FAIL(prepare(pkey))) {
    STORAGE_LOG(WARN, "fail to prepare partition meta", K(ret), K(pkey));
  } else {
    pkey_ = pkey;
    restore_info_ = &restore_info;
    macro_indexs_ = &macro_indexs;
    partition_store_meta_info_ = &partition_store_meta_info;
    last_read_size_ = 0;
    data_version_ = restore_info.restore_data_version_;
    schema_version_ = schema_version;
    is_inited_ = true;
  }

  return ret;
}

int ObPartitionBaseDataMetaRestoreReaderV2::fetch_partition_meta(ObPGPartitionStoreMeta& partition_store_meta)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t backup_table_id = 0;
  const ObPGPartitionStoreMeta* backup_partition_store_meta = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (FALSE_IT(backup_partition_store_meta = &(partition_store_meta_info_->partition_store_meta_))) {
  } else if (OB_ISNULL(backup_partition_store_meta)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "backup partition store meta should not be NULL",
        K(ret),
        KP(backup_partition_store_meta),
        KP(partition_store_meta_info_));
  } else if (OB_FAIL(restore_info_->trans_to_backup_schema_id(pkey_.get_table_id(), backup_table_id))) {
    STORAGE_LOG(WARN, "failed to trans to backup table id", K(ret), K(pkey_), K(backup_table_id));
  } else if (OB_UNLIKELY(backup_partition_store_meta->pkey_.table_id_ != backup_table_id)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR,
        "backup table id not match",
        K(ret),
        "backup table id",
        backup_partition_store_meta->pkey_.table_id_,
        "backup_table_id",
        backup_table_id,
        K(*backup_partition_store_meta),
        K(*restore_info_));
  } else if (OB_FAIL(partition_store_meta.deep_copy(partition_store_meta_))) {
    STORAGE_LOG(WARN, "fail to copy partition store meta", K(ret), K(partition_store_meta_));
  } else {
    STORAGE_LOG(INFO, "succeed to fetch partition meta", K(partition_store_meta));
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReaderV2::trans_table_key(
    const ObITable::TableKey& table_key, ObITable::TableKey& backup_table_key)
{
  int ret = OB_SUCCESS;
  uint64_t backup_index_id = 0;
  uint64_t backup_table_id = 0;
  backup_table_key = table_key;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(restore_info_->trans_to_backup_schema_id(table_key.table_id_, backup_index_id))) {
    STORAGE_LOG(WARN, "failed to trans_from_backup_index_id", K(ret), K(table_key), K(*restore_info_));
  } else if (OB_FAIL(restore_info_->trans_to_backup_schema_id(table_key.pkey_.table_id_, backup_table_id))) {
    STORAGE_LOG(WARN, "failed to trans backup table id", K(ret), K(table_key), K(*restore_info_));
  } else if (OB_FAIL(backup_table_key.pkey_.init(backup_table_id, table_key.pkey_.get_partition_id(), 0))) {
    STORAGE_LOG(WARN, "failed to init backup partition key", K(ret), K(table_key));
  } else {
    backup_table_key.table_id_ = backup_index_id;
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReaderV2::get_backup_sstable_meta_info(
    const ObITable::TableKey& backup_table_key, const ObBackupSSTableMetaInfo*& backup_sstable_meta_info)
{
  int ret = OB_SUCCESS;
  backup_sstable_meta_info = NULL;
  bool found = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    for (int64_t i = 0; i < partition_store_meta_info_->sstable_meta_info_array_.count() && !found; ++i) {
      const ObBackupSSTableMetaInfo& tmp_sstable_meta_info = partition_store_meta_info_->sstable_meta_info_array_.at(i);
      if (backup_table_key == tmp_sstable_meta_info.table_key_) {
        backup_sstable_meta_info = &(partition_store_meta_info_->sstable_meta_info_array_.at(i));
        found = true;
      }
    }
    if (!found) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "backup sstable meta info not exist", K(backup_table_key));
    } else if (OB_ISNULL(backup_sstable_meta_info)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "sstbale meta info should not be NULL", K(backup_table_key), KP(backup_sstable_meta_info));
    }
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReaderV2::get_backup_table_keys(
    const uint64_t backup_index_id, ObIArray<ObITable::TableKey>& table_keys)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_store_meta_info_->sstable_meta_info_array_.count(); ++i) {
      const ObITable::TableKey& table_key = partition_store_meta_info_->sstable_meta_info_array_.at(i).table_key_;
      if (table_key.table_id_ == backup_index_id) {
        if (table_key.is_complement_minor_sstable() && !table_key.log_ts_range_.is_empty()) {
          // do nothing
        } else if (OB_FAIL(table_keys.push_back(table_key))) {
          STORAGE_LOG(WARN, "failed to push table key into array", K(ret), K(table_key));
        }
      }
    }
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReaderV2::fetch_sstable_meta(
    const ObITable::TableKey& table_key, blocksstable::ObSSTableBaseMeta& sstable_meta)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObITable::TableKey backup_table_key;
  const ObBackupSSTableMetaInfo* backup_sstable_meta_info = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "index id is invalid", K(ret), K(table_key));
  } else if (OB_FAIL(trans_table_key(table_key, backup_table_key))) {
    STORAGE_LOG(WARN, "failed to trans table key to backup table key", K(ret), K(table_key));
  } else if (!backup_table_key.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup table key is invalid", K(ret), K(table_key), K(backup_table_key));
  } else if (OB_FAIL(get_backup_sstable_meta_info(backup_table_key, backup_sstable_meta_info))) {
    STORAGE_LOG(WARN, "failed to get backup sstable meta info", K(ret), K(backup_table_key), K(table_key));
  } else if (OB_FAIL(sstable_meta.assign(backup_sstable_meta_info->sstable_meta_))) {
    STORAGE_LOG(WARN, "failed to assign sstable meta", K(ret), K(*backup_sstable_meta_info));
  } else {
    // physical restore, use backup meta info
    sstable_meta.index_id_ = table_key.table_id_;
    sstable_meta.data_version_ = data_version_;
    sstable_meta.available_version_ = data_version_;
    if (sstable_meta.sstable_format_version_ >= ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_5) {
      sstable_meta.logical_data_version_ = sstable_meta.logical_data_version_ < sstable_meta.data_version_
                                               ? sstable_meta.data_version_
                                               : sstable_meta.logical_data_version_;
    } else {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "not support restore sstable version", K(ret), K(sstable_meta));
      sstable_meta.reset();
    }
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReaderV2::fetch_sstable_pair_list(
    const ObITable::TableKey& table_key, common::ObIArray<blocksstable::ObSSTablePair>& pair_list)
{
  int ret = OB_SUCCESS;
  ObITable::TableKey backup_table_key;
  const common::ObArray<ObBackupTableMacroIndex>* index_list = NULL;
  const ObBackupSSTableMetaInfo* backup_sstable_meta_info = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "index id is invalid", K(ret), K(table_key));
  } else if (OB_FAIL(trans_table_key(table_key, backup_table_key))) {
    STORAGE_LOG(WARN, "failed to trans table key to backup table key", K(ret), K(table_key));
  } else if (!backup_table_key.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup table key is invalid", K(ret), K(table_key), K(backup_table_key));
  } else if (OB_FAIL(get_backup_sstable_meta_info(backup_table_key, backup_sstable_meta_info))) {
    STORAGE_LOG(WARN, "failed to get backup sstable meta info", K(ret), K(backup_table_key), K(table_key));
  } else if (OB_ISNULL(backup_sstable_meta_info)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "backup sstable meta info should not be NULL",
        K(ret),
        KP(backup_sstable_meta_info),
        K(table_key),
        K(backup_table_key));
  } else if (0 == backup_sstable_meta_info->sstable_meta_.macro_block_count_) {
    // do nothing
  } else if (OB_FAIL(macro_indexs_->get_macro_index_array(backup_table_key, index_list))) {
    STORAGE_LOG(WARN,
        "failed to get macro index array",
        K(ret),
        K(table_key),
        K(backup_table_key),
        K(*backup_sstable_meta_info));
  } else if (OB_ISNULL(index_list)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "index list should not be NULL", K(ret), K(table_key), K(backup_table_key), KP(index_list));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_list->count(); ++i) {
      const ObBackupTableMacroIndex& backup_macro_index = index_list->at(i);
      if (!backup_macro_index.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(
            WARN, "backup macro index is invalid", K(ret), K(table_key), K(backup_table_key), K(backup_macro_index));
      } else {
        ObSSTablePair pair;
        pair.data_seq_ = backup_macro_index.data_seq_;
        pair.data_version_ = backup_macro_index.data_version_;
        if (OB_FAIL(pair_list.push_back(pair))) {
          STORAGE_LOG(WARN, "failed to push pair into array", K(ret), K(pair));
        }
      }
    }
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReaderV2::prepare(const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObPGPartitionGuard pg_partition_guard;
  ObPartitionStorage* partition_storage = NULL;

  if (OB_FAIL(ObPartitionService::get_instance().get_partition(pkey, guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition_group is null", K(ret), K(pkey));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_partition(pkey, pg_partition_guard))) {
    STORAGE_LOG(WARN, "fail to get pg partition", K(ret), K(pkey));
  } else if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg_partition is null ", K(ret), K(pkey));
  } else if (OB_ISNULL(partition_storage = reinterpret_cast<ObPartitionStorage*>(
                           pg_partition_guard.get_pg_partition()->get_storage()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error, the partition storage is NULL", K(ret), K(pkey));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_storage().get_pg_partition_store_meta(
                 pkey, partition_store_meta_))) {
    STORAGE_LOG(WARN, "fail to get partition store meta", K(ret));
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReaderV2::fetch_all_table_ids(common::ObIArray<uint64_t>& table_id_array)
{
  int ret = OB_SUCCESS;
  uint64_t index_id = 0;
  ObHashSet<uint64_t> backup_table_id_set;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(backup_table_id_set.create(partition_store_meta_info_->sstable_meta_info_array_.count()))) {
    STORAGE_LOG(WARN, "failed to create backup table id set", K(ret), K(*partition_store_meta_info_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_store_meta_info_->sstable_meta_info_array_.count(); ++i) {
      const ObITable::TableKey& backup_table_key =
          partition_store_meta_info_->sstable_meta_info_array_.at(i).table_key_;
      if (OB_FAIL(backup_table_id_set.set_refactored(backup_table_key.table_id_))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "failed to set table id into set", K(ret), K(backup_table_key));
        }
      }
    }

    if (OB_SUCC(ret)) {
      for (ObHashSet<uint64_t>::iterator iter = backup_table_id_set.begin(); iter != backup_table_id_set.end();
           ++iter) {
        const uint64_t backup_table_id = iter->first;
        if (OB_FAIL(restore_info_->trans_from_backup_schema_id(backup_table_id, index_id))) {
          STORAGE_LOG(WARN, "failed to trans_from_backup_index_id", K(ret), K(backup_table_id_set), K(*restore_info_));
        } else if (OB_FAIL(table_id_array.push_back(index_id))) {
          STORAGE_LOG(WARN, "fail to push index id into array", K(ret), K(index_id));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(do_filter_tables(table_id_array))) {
          STORAGE_LOG(WARN, "failed to filter tables", K(ret), K(table_id_array));
        }
      }
    }
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReaderV2::fetch_table_keys(
    const uint64_t index_id, obrpc::ObFetchTableInfoResult& table_res)
{
  int ret = OB_SUCCESS;
  int64_t min_multi_version_start = INT64_MAX;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    uint64_t backup_index_id = 0;
    if (OB_FAIL(restore_info_->trans_to_backup_schema_id(index_id, backup_index_id))) {
      STORAGE_LOG(WARN, "failed to trans_from_backup_index_id", K(ret), K(index_id), K(*restore_info_));
    } else if (OB_FAIL(get_backup_table_keys(backup_index_id, table_res.table_keys_))) {
      STORAGE_LOG(WARN, "failed to get backup table keys", K(ret), K(backup_index_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_res.table_keys_.count(); ++i) {
        ObITable::TableKey& table_key = table_res.table_keys_.at(i);
        table_key.pkey_ = pkey_;
        table_key.table_id_ = index_id;
        if (table_key.is_minor_sstable()) {
          min_multi_version_start =
              std::min(min_multi_version_start, table_key.trans_version_range_.multi_version_start_);
        } else if (table_key.is_major_sstable()) {
          // do nothing
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "table type is unexpected", K(ret), K(table_key));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    // TODO() now res multi version start not use any more, delete it later
    table_res.is_ready_for_read_ = true;
    table_res.multi_version_start_ =
        min_multi_version_start == INT64_MAX ? ObTimeUtility::current_time() : min_multi_version_start;
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReaderV2::do_filter_tables(ObIArray<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObMultiVersionSchemaService& schema_service = ObMultiVersionSchemaService::get_instance();
  ObArray<uint64_t> restore_tables;
  bool filtered = false;
  const int64_t tenant_id = pkey_.get_tenant_id();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(ObBackupUtils::retry_get_tenant_schema_guard(
                 tenant_id, schema_service, schema_version_, schema_guard))) {
    STORAGE_LOG(WARN, "failed to get tenant schema guard", K(ret), K(pkey_), K(schema_version_));
  } else {
    bool is_exist = false;
    for (int i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
      uint64_t table_id = table_ids.at(i);
      is_exist = false;
      const ObTableSchema* table_schema = NULL;
      bool need_skip = false;
      if (is_trans_table_id(table_id)) {
        is_exist = true;
      } else if (OB_FAIL(schema_guard.check_table_exist(table_id, is_exist))) {
        STORAGE_LOG(WARN, "schema guard check table fail", K(ret), K(table_id));
      } else if (!is_exist) {
        // do nothing
      } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
        STORAGE_LOG(WARN, "failed to get table schema", K(ret), K(table_id));
      } else if (OB_FAIL(ObBackupRestoreTableSchemaChecker::check_backup_restore_need_skip_table(
                     table_schema, need_skip))) {
        LOG_WARN("failed to check backup restore need skip table", K(ret), K(table_id));
      } else if (!need_skip) {
        // do nothing
      } else {
        is_exist = false;
      }

      if (OB_FAIL(ret)) {
      } else if (is_exist) {
        if (OB_FAIL(restore_tables.push_back(table_id))) {
          STORAGE_LOG(WARN, "push back table id failed", K(ret), K(table_id));
        }
      } else {
        filtered = true;
        FLOG_INFO("restore table is not exist, no need restore", K(table_id));
      }
    }
  }

  if (OB_SUCC(ret) && filtered) {
    table_ids.reuse();
    if (OB_FAIL(table_ids.assign(restore_tables))) {
      STORAGE_LOG(WARN, "assign table id failed", K(ret), K(restore_tables));
    }
  }
  return ret;
}

/********************ObPhysicalBaseMetaRestoreReaderV2*********************/
ObPhysicalBaseMetaRestoreReaderV2::ObPhysicalBaseMetaRestoreReaderV2()
    : is_inited_(false),
      restore_info_(NULL),
      reader_(NULL),
      allocator_(common::ObModIds::RESTORE),
      bandwidth_throttle_(NULL),
      table_key_()
{}

int ObPhysicalBaseMetaRestoreReaderV2::init(common::ObInOutBandwidthThrottle& bandwidth_throttle,
    const ObPhysicalRestoreArg& restore_info, const ObITable::TableKey& table_key,
    ObIPartitionGroupMetaRestoreReader& reader)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "can not init twice", K(ret));
  } else if (!restore_info.is_valid() || !table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "table is is invalid", K(ret), K(restore_info), K(table_key));
  } else {
    is_inited_ = true;
    restore_info_ = &restore_info;
    bandwidth_throttle_ = &bandwidth_throttle;
    table_key_ = table_key;
    reader_ = &reader;
  }
  return ret;
}

int ObPhysicalBaseMetaRestoreReaderV2::fetch_sstable_meta(ObSSTableBaseMeta& sstable_meta)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "physical base meta restore reader not init", K(ret));
  } else if (OB_FAIL(reader_->fetch_sstable_meta(table_key_, sstable_meta))) {
    STORAGE_LOG(WARN, "fail to get sstable meta", K(ret), K(table_key_));
  } else if (OB_UNLIKELY(!sstable_meta.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable meta is invalid", K(ret), K(table_key_), K(sstable_meta));
  }
  return ret;
}

int ObPhysicalBaseMetaRestoreReaderV2::fetch_macro_block_list(ObIArray<ObSSTablePair>& macro_block_list)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "physical base meta restore reader not init", K(ret));
  } else if (OB_FAIL(reader_->fetch_sstable_pair_list(table_key_, macro_block_list))) {
    STORAGE_LOG(WARN, "fail to fetch sstable pair list", K(ret), K(table_key_));
  }
  return ret;
}

/******ObPartitionMacroBlockRestoreReaderV2************************/

ObPartitionMacroBlockRestoreReaderV2::ObPartitionMacroBlockRestoreReaderV2()
    : is_inited_(false),
      macro_list_(),
      macro_idx_(0),
      read_size_(0),
      table_id_(OB_INVALID_ID),
      backup_path_info_(),
      macro_indexs_(nullptr),
      bandwidth_throttle_(nullptr),
      backup_pgkey_(),
      backup_table_key_(),
      allocator_()
{}

ObPartitionMacroBlockRestoreReaderV2::~ObPartitionMacroBlockRestoreReaderV2()
{}

int ObPartitionMacroBlockRestoreReaderV2::init(common::ObInOutBandwidthThrottle& bandwidth_throttle,
    common::ObIArray<ObMigrateArgMacroBlockInfo>& list, const ObPhysicalRestoreArg& restore_info,
    const ObPhyRestoreMacroIndexStoreV2& macro_indexs, const ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;
  uint64_t backup_table_id = 0;
  uint64_t backup_index_id = 0;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_UNLIKELY(!restore_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(restore_info));
  } else if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "table key is invalid", K(ret), K(table_key));
  } else if (OB_FAIL(restore_info.get_backup_base_data_info(backup_path_info_))) {
    LOG_WARN("failed to get backup_base_data_info", K(ret));
  } else if (OB_FAIL(restore_info.get_backup_pgkey(backup_pgkey_))) {
    LOG_WARN("failed to get backup pgkey", K(ret));
  } else if (OB_FAIL(restore_info.trans_to_backup_schema_id(table_key.pkey_.get_table_id(), backup_table_id))) {
    STORAGE_LOG(WARN, "failed to trans to backup schema id", K(ret), K(table_key));
  } else if (OB_FAIL(restore_info.trans_to_backup_schema_id(table_key.table_id_, backup_index_id))) {
    STORAGE_LOG(WARN, "failed to get backup block info", K(ret), K(table_key));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
      if (OB_FAIL(macro_list_.push_back(list.at(i).fetch_arg_))) {
        LOG_WARN("failed to add macro list", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    macro_idx_ = 0;
    read_size_ = 0;
    table_id_ = table_key.table_id_;
    macro_indexs_ = &macro_indexs;
    bandwidth_throttle_ = &bandwidth_throttle;
    backup_table_key_ = table_key;
    backup_table_key_.table_id_ = backup_index_id;

    if (OB_FAIL(backup_table_key_.pkey_.init(backup_table_id, table_key.pkey_.get_partition_id(), 0))) {
      STORAGE_LOG(WARN, "failed to init backup partition key", K(ret), K(table_key));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObPartitionMacroBlockRestoreReaderV2::get_next_macro_block(blocksstable::ObFullMacroBlockMeta& meta,
    blocksstable::ObBufferReader& data, blocksstable::MacroBlockId& src_macro_id)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObBackupTableMacroIndex macro_index;
  ObMacroBlockSchemaInfo* new_schema = nullptr;
  ObMacroBlockMetaV2* new_meta = nullptr;

  allocator_.reuse();
  src_macro_id.reset();  // src_macro_id only used for fast migrator on ofs

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (macro_idx_ >= macro_list_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(macro_indexs_->get_macro_index(
                 backup_table_key_, macro_list_.at(macro_idx_).macro_block_index_, macro_index))) {
    STORAGE_LOG(WARN, "fail to get table keys index", K(ret));
  } else if (OB_FAIL(get_macro_block_path(macro_index, path))) {
    STORAGE_LOG(WARN, "failed to get macro block path", K(ret), K(macro_index));
  } else if (OB_FAIL(ObRestoreFileUtil::read_macroblock_data(path.get_obstr(),
                 backup_path_info_.dest_.get_storage_info(),
                 macro_index,
                 allocator_,
                 new_schema,
                 new_meta,
                 data))) {
    STORAGE_LOG(WARN, "fail to get meta file path", K(ret));
  } else if (OB_FAIL(trans_macro_block(table_id_, *new_meta, data))) {
    STORAGE_LOG(WARN, "failed to trans_macro_block", K(ret));
  } else {
    meta.schema_ = new_schema;
    meta.meta_ = new_meta;
    read_size_ += macro_index.data_length_;
    ++macro_idx_;
  }

  return ret;
}

int ObPartitionMacroBlockRestoreReaderV2::trans_macro_block(
    const uint64_t table_id, blocksstable::ObMacroBlockMetaV2& meta, blocksstable::ObBufferReader& data)
{
  int ret = OB_SUCCESS;
  ObMacroBlockCommonHeader common_header;
  int64_t pos = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(!meta.is_valid())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid macro meta", K(ret), K(meta));
  } else if (NULL == data.data() || data.length() < 0 || data.length() > data.capacity()) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid data", K(ret), K(data));
  } else if (data.length() < common_header.get_serialize_size()) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN,
        "data buf not enough for ObMacroBlockCommonHeader",
        K(ret),
        K(data),
        "need_size",
        common_header.get_serialize_size());
  } else if (OB_FAIL(common_header.deserialize(data.data(), data.length(), pos))) {
    STORAGE_LOG(ERROR, "deserialize common header fail", K(ret), K(data), K(pos), K(common_header));
  } else if (data.length() + pos < sizeof(ObSSTableMacroBlockHeader)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN,
        "data buf not enough for ObSSTableMacroBlockHeader",
        K(ret),
        K(data),
        "need_size",
        sizeof(ObSSTableMacroBlockHeader));
  } else {
    ObSSTableMacroBlockHeader* sstable_macro_block_header =
        reinterpret_cast<ObSSTableMacroBlockHeader*>(data.data() + pos);
    meta.table_id_ = table_id;
    sstable_macro_block_header->table_id_ = meta.table_id_;
    sstable_macro_block_header->data_version_ = meta.data_version_;
    sstable_macro_block_header->data_seq_ = meta.data_seq_;
  }

  return ret;
}

int ObPartitionMacroBlockRestoreReaderV2::get_macro_block_path(
    const ObBackupTableMacroIndex& macro_index, ObBackupPath& path)
{
  int ret = OB_SUCCESS;
  path.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (!macro_index.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get macro block path get invalid argument", K(ret), K(macro_index));
  } else if (backup_table_key_.is_major_sstable()) {
    if (OB_FAIL(ObBackupPathUtil::get_major_macro_block_file_path(backup_path_info_,
            backup_pgkey_.get_table_id(),
            backup_pgkey_.get_partition_id(),
            macro_index.backup_set_id_,
            macro_index.sub_task_id_,
            path))) {
      STORAGE_LOG(WARN, "fail to get meta file path", K(ret));
    }
  } else if (backup_table_key_.is_minor_sstable()) {
    if (OB_FAIL(ObBackupPathUtil::get_minor_macro_block_file_path(backup_path_info_,
            backup_pgkey_.get_table_id(),
            backup_pgkey_.get_partition_id(),
            macro_index.backup_set_id_,
            macro_indexs_->get_backup_task_id(),
            macro_index.sub_task_id_,
            path))) {
      STORAGE_LOG(WARN, "fail to get meta file path", K(ret));
    }
  }
  return ret;
}

/******************ObPartitionGroupMetaRestoreReaderV2******************/
ObPartitionGroupMetaRestoreReaderV2::ObPartitionGroupMetaRestoreReaderV2()
    : is_inited_(false),
      restore_info_(NULL),
      meta_indexs_(NULL),
      reader_(),
      pg_meta_(),
      bandwidth_throttle_(NULL),
      last_read_size_(0),
      allocator_(),
      backup_pg_meta_info_(),
      schema_version_(0)
{}

ObPartitionGroupMetaRestoreReaderV2::~ObPartitionGroupMetaRestoreReaderV2()
{
  reset();
}

void ObPartitionGroupMetaRestoreReaderV2::reset()
{
  is_inited_ = false;
  restore_info_ = NULL;
  meta_indexs_ = NULL;
  reader_.reset();
  pg_meta_.reset();
  bandwidth_throttle_ = NULL;
  last_read_size_ = 0;
  for (MetaReaderMap::iterator iter = partition_reader_map_.begin(); iter != partition_reader_map_.end(); ++iter) {
    ObPartitionBaseDataMetaRestoreReaderV2* meta_reader = iter->second;
    if (NULL != meta_reader) {
      meta_reader->~ObPartitionBaseDataMetaRestoreReaderV2();
    }
  }
  partition_reader_map_.clear();
  allocator_.reset();
  backup_pg_meta_info_.reset();
  schema_version_ = 0;
}

int ObPartitionGroupMetaRestoreReaderV2::init(common::ObInOutBandwidthThrottle& bandwidth_throttle,
    const ObPhysicalRestoreArg& restore_info, const ObPhyRestoreMetaIndexStore& meta_indexs,
    const ObPhyRestoreMacroIndexStoreV2& macro_indexs)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t tenant_id = restore_info.pg_key_.get_tenant_id();
  ObIPartitionGroup* partition_group = NULL;
  ObIPartitionGroupGuard pg_guard;
  int64_t restore_schema_version = OB_INVALID_VERSION;
  int64_t current_schema_version = OB_INVALID_VERSION;
  const ObPartitionKey& pg_key = restore_info.pg_key_;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (!restore_info.is_valid() || !meta_indexs.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(restore_info), K(meta_indexs));
  } else if (OB_FAIL(reader_.init(restore_info, meta_indexs))) {
    STORAGE_LOG(WARN, "failed to init meta reader", K(ret));
  } else if (OB_FAIL(reader_.read_backup_pg_meta_info(backup_pg_meta_info_))) {
    STORAGE_LOG(WARN, "failed to read backup pg meta info", K(ret));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pg_key, pg_guard))) {
    STORAGE_LOG(WARN, "fail to get pg guard", K(ret), K(pg_key));
  } else if (OB_ISNULL(partition_group = pg_guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition group should not be null", K(ret), K(pg_key));
  } else if (OB_FAIL(partition_group->get_pg_storage().get_pg_meta(pg_meta_))) {
    STORAGE_LOG(WARN, "fail to get partition group meta", K(ret), K(pg_key));
  } else if (is_sys_table(restore_info.pg_key_.get_table_id())) {
    schema_version_ = OB_INVALID_VERSION;
    restore_schema_version = OB_INVALID_VERSION;
    current_schema_version = OB_INVALID_VERSION;
  } else {
    if (OB_FAIL(
            ObBackupInfoMgr::get_instance().get_base_data_restore_schema_version(tenant_id, restore_schema_version))) {
      STORAGE_LOG(WARN, "failed to  get base data restore schema version", K(ret), K(tenant_id), K(restore_info));
    } else {
      current_schema_version = std::min(pg_meta_.storage_info_.get_data_info().get_schema_version(),
          backup_pg_meta_info_.pg_meta_.storage_info_.get_data_info().get_schema_version());
    }
    if (OB_FAIL(ret)) {
    } else {
      schema_version_ = current_schema_version;
      LOG_INFO("get restore schema version",
          K(restore_schema_version),
          K(current_schema_version),
          K(schema_version_),
          K(pg_meta_),
          K(backup_pg_meta_info_.pg_meta_));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(prepare(restore_info, macro_indexs))) {
    STORAGE_LOG(WARN, "failed to prepare", K(ret), K(restore_info));
  } else if (OB_SUCCESS != (tmp_ret = ObRestoreFileUtil::limit_bandwidth_and_sleep(
                                bandwidth_throttle, reader_.get_data_size(), last_read_size_))) {
    STORAGE_LOG(WARN, "failed to limit_bandwidth_and_sleep", K(tmp_ret));
  } else {
    meta_indexs_ = &meta_indexs;
    restore_info_ = &restore_info;
    bandwidth_throttle_ = &bandwidth_throttle;
    last_read_size_ = 0;
    schema_version_ = current_schema_version;
    is_inited_ = true;
    STORAGE_LOG(INFO, "succeed to init partition group restore reader", K(restore_info_));
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV2::fetch_partition_group_meta(ObPartitionGroupMeta& pg_meta)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(!pg_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "cannot restore a invalid partition", K(ret), K(pg_meta_));
  } else if (OB_FAIL(pg_meta.deep_copy(pg_meta_))) {
    STORAGE_LOG(WARN, "fail to copy partition store meta", K(ret), K(pg_meta_));
  }

  return ret;
}

int ObPartitionGroupMetaRestoreReaderV2::trans_backup_pgmeta(
    const ObPhysicalRestoreArg& restore_info, ObPartitionGroupMeta& backup_pg_meta)
{
  int ret = OB_SUCCESS;
  ObPartitionArray current_partitions;

  if (OB_UNLIKELY(!pg_meta_.is_valid() || !backup_pg_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pg_meta_), K(backup_pg_meta));
  } else if (OB_FAIL(trans_from_backup_partitions(restore_info, backup_pg_meta.partitions_, current_partitions))) {
    STORAGE_LOG(WARN, "failed to trans from backup partitions", K(ret), K(backup_pg_meta));
  } else if (OB_FAIL(do_filter_pg_partitions(pg_meta_.pg_key_, current_partitions))) {
    STORAGE_LOG(WARN, "failed do filter pg partitions", K(ret), K(backup_pg_meta), K(pg_meta_));
  } else if (OB_FAIL(trans_to_backup_partitions(restore_info, current_partitions, backup_pg_meta.partitions_))) {
    STORAGE_LOG(WARN, "failed to trans from backup partitions", K(ret), K(backup_pg_meta));
  } else {
    const ObSavedStorageInfoV2& cur_info = pg_meta_.storage_info_;
    const int64_t cur_schema_version = cur_info.get_data_info().get_schema_version();
    ObSavedStorageInfoV2& backup_info = backup_pg_meta.storage_info_;
    backup_info.get_data_info().set_schema_version(cur_schema_version);
    STORAGE_LOG(INFO, "succeed trans backup pg meta", K(backup_pg_meta));
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV2::read_partition_meta(
    const ObPartitionKey& pkey, const ObPhysicalRestoreArg& restore_info, ObPGPartitionStoreMeta& partition_store_meta)
{
  int ret = OB_SUCCESS;
  bool found = false;
  partition_store_meta.reset();

  if (!backup_pg_meta_info_.is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition group meta restore reader v2 do not init", K(ret), K(pkey));
  } else if (OB_UNLIKELY(!pkey.is_valid() || !restore_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(restore_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_pg_meta_info_.partition_store_meta_info_array_.count() && !found;
         ++i) {
      const ObPGPartitionStoreMeta& tmp_partition_store_meta =
          backup_pg_meta_info_.partition_store_meta_info_array_.at(i).partition_store_meta_;
      if (tmp_partition_store_meta.pkey_ == pkey) {
        if (OB_FAIL(partition_store_meta.assign(tmp_partition_store_meta))) {
          STORAGE_LOG(WARN, "failed to assign partition store meta", K(ret), K(tmp_partition_store_meta));
        } else {
          found = true;
        }
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "backup partition do not exist", K(ret), K(pkey));
    }
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV2::create_pg_partition_if_need(
    const ObPhysicalRestoreArg& restore_info, const ObPartitionGroupMeta& backup_pg_meta)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard pg_guard;
  ObIPartitionGroup* pg = NULL;
  const ObPartitionKey& pg_key = restore_info.pg_key_;

  if (OB_UNLIKELY(!restore_info.is_valid() || !backup_pg_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(restore_info), K(backup_pg_meta));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pg_key, pg_guard))) {
    STORAGE_LOG(WARN, "fail to get restore pg", K(ret), K(pg_key));
  } else if (OB_ISNULL(pg = pg_guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition group should not be null", K(ret), K(pg_key));
  } else {
    const ObSavedStorageInfoV2& storage_info = backup_pg_meta.storage_info_;
    ObPGPartitionStoreMeta partition_meta;
    bool succ_create_partition = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_pg_meta.partitions_.count(); ++i) {
      const ObPartitionKey& src_pkey = backup_pg_meta.partitions_.at(i);
      ObPartitionKey dst_pkey;
      ObIPartitionGroupGuard guard;
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(restore_info.change_src_pkey_to_dst_pkey(src_pkey, dst_pkey))) {
        STORAGE_LOG(WARN, "get dst pkey fail", K(ret), K(restore_info), K(src_pkey));
      } else if (OB_SUCCESS == (tmp_ret = ObPartitionService::get_instance().get_partition(dst_pkey, guard))) {
        STORAGE_LOG(INFO, "partition exist, not need to create", K(ret), K(dst_pkey));
      } else if (OB_PARTITION_NOT_EXIST != tmp_ret) {
        ret = tmp_ret;
        STORAGE_LOG(WARN, "restore get partition fail", K(ret), K(dst_pkey));
      } else if (OB_FAIL(read_partition_meta(src_pkey, restore_info, partition_meta))) {
        STORAGE_LOG(WARN, "read restore partition meta fail", K(ret), K(src_pkey));
      } else {
        obrpc::ObCreatePartitionArg arg;
        arg.schema_version_ = storage_info.get_data_info().get_schema_version();
        arg.lease_start_ = partition_meta.create_timestamp_;
        arg.restore_ = REPLICA_RESTORE_DATA;
        const bool is_replay = false;
        const uint64_t unused = 0;
        const bool in_slog_trans = false;
        const uint64_t data_table_id = replace_tenant_id(restore_info.get_tenant_id(), partition_meta.data_table_id_);
        const int64_t multi_version_start = restore_info.restore_info_.restore_snapshot_version_;
        ObTablesHandle sstables_handle;
        if (OB_FAIL(pg->create_pg_partition(dst_pkey,
                multi_version_start,
                data_table_id,
                arg,
                in_slog_trans,
                is_replay,
                unused,
                sstables_handle))) {
          STORAGE_LOG(WARN, "fail to create pg partition", K(ret), K(partition_meta));
        } else {
          succ_create_partition = true;
          sstables_handle.reset();
          STORAGE_LOG(INFO, "succeed to create pg partiton during physical restore", K(dst_pkey), K(partition_meta));
        }
      }
    }

    if (OB_SUCC(ret) && succ_create_partition) {
      pg_meta_.reset();
      if (OB_FAIL(pg->get_pg_storage().get_pg_meta(pg_meta_))) {
        STORAGE_LOG(WARN, "fail to get partition group meta", K(ret), K(pg_key));
      } else if (pg_meta_.partitions_.count() != backup_pg_meta.partitions_.count()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "restore pg partition count is not match", K(ret), K(pg_meta_), K(backup_pg_meta));
      }
    }
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV2::prepare_pg_meta(const ObPhysicalRestoreArg& restore_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!backup_pg_meta_info_.is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup pg meta info do not init", K(ret));
  } else if (OB_UNLIKELY(!pg_meta_.is_valid() || !backup_pg_meta_info_.is_valid() ||
                         REPLICA_RESTORE_DATA != pg_meta_.is_restore_ ||
                         REPLICA_NOT_RESTORE != backup_pg_meta_info_.pg_meta_.is_restore_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "cannot restore a invalid partition", K(ret), K(pg_meta_), K(backup_pg_meta_info_));
  } else if (OB_FAIL(trans_backup_pgmeta(restore_info, backup_pg_meta_info_.pg_meta_))) {
    STORAGE_LOG(WARN, "trans backup pg meta fail", K(ret), K(pg_meta_));
  } else if (OB_FAIL(check_backup_partitions_in_pg(restore_info, backup_pg_meta_info_.pg_meta_))) {
    STORAGE_LOG(WARN, "failed to check back partitions in schema", K(ret), K(backup_pg_meta_info_));
  } else if (OB_FAIL(create_pg_partition_if_need(restore_info, backup_pg_meta_info_.pg_meta_))) {
    STORAGE_LOG(WARN, "fail to create pg partition", K(ret), K(backup_pg_meta_info_));
  } else if (OB_FAIL(pg_meta_.storage_info_.deep_copy(backup_pg_meta_info_.pg_meta_.storage_info_))) {
    STORAGE_LOG(WARN, "restore storage info fail", K(ret), K(pg_meta_), K(backup_pg_meta_info_));
  }

  if (OB_FAIL(ret)) {
    pg_meta_.reset();
  } else {
    STORAGE_LOG(INFO, "fetch restore pg meta succ", K(pg_meta_));
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV2::prepare(
    const ObPhysicalRestoreArg& restore_info, const ObPhyRestoreMacroIndexStoreV2& macro_indexs)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!reader_.is_inited())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "reader do not init", K(ret));
  } else if (OB_FAIL(prepare_pg_meta(restore_info))) {
    STORAGE_LOG(WARN, "prepare pg meta fail", K(ret));
  } else {
    const ObPartitionArray& partitions = pg_meta_.partitions_;
    if (0 == partitions.count()) {
      STORAGE_LOG(INFO, "empty pg, no partitions");
    } else if (OB_FAIL(partition_reader_map_.create(partitions.count(), ObModIds::RESTORE))) {
      STORAGE_LOG(WARN, "failed to create partition reader map", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
        const ObPartitionKey& pkey = partitions.at(i);
        ObPartitionBaseDataMetaRestoreReaderV2* reader = NULL;
        void* buf = NULL;
        const ObBackupPartitionStoreMetaInfo* backup_partition_meta_info = NULL;
        if (OB_FAIL(get_backup_partition_meta_info(pkey, restore_info, backup_partition_meta_info))) {
          STORAGE_LOG(WARN, "failed to get backup partition meta info", K(ret), K(pkey));
        } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObPartitionBaseDataMetaRestoreReaderV2)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc buf", K(ret));
        } else if (OB_ISNULL(reader = new (buf) ObPartitionBaseDataMetaRestoreReaderV2())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc reader", K(ret));
        } else if (OB_FAIL(
                       reader->init(pkey, restore_info, macro_indexs, *backup_partition_meta_info, schema_version_))) {
          STORAGE_LOG(WARN, "failed to init ObPartitionGroupMetaRestoreReaderV2", K(ret), K(pkey));
        } else if (OB_FAIL(partition_reader_map_.set_refactored(pkey, reader))) {
          STORAGE_LOG(WARN, "failed to set reader into map", K(ret), K(pkey));
        } else {
          reader = NULL;
        }

        if (NULL != reader) {
          reader->~ObPartitionBaseDataMetaRestoreReaderV2();
        }
      }
    }
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV2::get_partition_readers(
    const ObPartitionArray& partitions, ObIArray<ObPartitionBaseDataMetaRestoreReaderV2*>& partition_reader_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition group meta restore reader do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
      const ObPartitionKey& pkey = partitions.at(i);
      ObPartitionBaseDataMetaRestoreReaderV2* reader = NULL;
      if (OB_FAIL(partition_reader_map_.get_refactored(pkey, reader))) {
        STORAGE_LOG(WARN, "failed to get partition base data meta restore reader", K(ret), K(pkey));
      } else if (OB_ISNULL(reader)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition base data meta restore reader is NULL", K(ret), K(pkey), KP(reader));
      } else if (OB_FAIL(partition_reader_array.push_back(reader))) {
        STORAGE_LOG(WARN, "failed to push reader into partition reader array", K(ret), K(pkey));
      }
    }
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV2::fetch_sstable_meta(
    const ObITable::TableKey& table_key, blocksstable::ObSSTableBaseMeta& sstable_meta)
{
  int ret = OB_SUCCESS;
  ObPartitionBaseDataMetaRestoreReaderV2* reader = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "fetch sstable meta get invalid argument", K(ret), K(table_key));
  } else if (OB_FAIL(partition_reader_map_.get_refactored(table_key.pkey_, reader))) {
    STORAGE_LOG(WARN, "failed to get partition base meta retore reader", K(ret), K(table_key));
  } else if (OB_ISNULL(reader)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition base meta restore reader is NULL", K(ret), K(table_key), KP(reader));
  } else if (OB_FAIL(reader->fetch_sstable_meta(table_key, sstable_meta))) {
    STORAGE_LOG(WARN, "failed to get sstable meta", K(ret), K(table_key));
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV2::fetch_sstable_pair_list(
    const ObITable::TableKey& table_key, common::ObIArray<blocksstable::ObSSTablePair>& pair_list)
{
  int ret = OB_SUCCESS;
  ObArray<blocksstable::ObSSTablePair> backup_pair_list;
  ObPartitionBaseDataMetaRestoreReaderV2* reader = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "index id is invalid", K(ret), K(table_key));
  } else if (OB_FAIL(partition_reader_map_.get_refactored(table_key.get_partition_key(), reader))) {
    STORAGE_LOG(WARN, "failed to partition base meta restore reader", K(ret), K(table_key));
  } else if (OB_ISNULL(reader)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition base meta restore reader is NULL", K(ret), K(table_key), KP(reader));
  } else if (OB_FAIL(reader->fetch_sstable_pair_list(table_key, pair_list))) {
    STORAGE_LOG(WARN, "failed to fetch sstable pair list", K(ret), K(table_key));
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV2::check_backup_partitions_in_pg(
    const ObPhysicalRestoreArg& restore_info, ObPartitionGroupMeta& backup_pg_meta)
{
  int ret = OB_SUCCESS;
  ObHashSet<uint64_t> restore_table_ids_set;
  ObSchemaGetterGuard schema_guard;
  ObMultiVersionSchemaService& schema_service = ObMultiVersionSchemaService::get_instance();
  const uint64_t tenant_id = restore_info.pg_key_.get_tenant_id();
  const uint64_t tablegroup_id = restore_info.pg_key_.get_tablegroup_id();
  ObArray<uint64_t> table_ids;
  int64_t max_bucket = 0;

  if (OB_UNLIKELY(!restore_info.is_valid() || !backup_pg_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(restore_info), K(backup_pg_meta));
  } else if (is_sys_table(restore_info.pg_key_.get_table_id())) {
    // do nothing
  } else if (OB_FAIL(ObBackupUtils::retry_get_tenant_schema_guard(
                 tenant_id, schema_service, schema_version_, schema_guard))) {
    STORAGE_LOG(WARN, "failed to get tenant schema guard", K(ret), K(restore_info), K(backup_pg_meta));
  } else if (!restore_info.pg_key_.is_pg()) {
    if (OB_FAIL(table_ids.push_back(restore_info.pg_key_.get_table_id()))) {
      STORAGE_LOG(WARN, "failed to push table id into array", K(ret), K(restore_info));
    }
  } else if (OB_FAIL(schema_guard.get_table_ids_in_tablegroup(tenant_id, tablegroup_id, table_ids))) {
    STORAGE_LOG(WARN, "failed to get table ids in tablegroup", K(ret), K(restore_info), K(backup_pg_meta));
  }

  if (OB_FAIL(ret)) {
  } else if (backup_pg_meta.partitions_.empty() && table_ids.empty()) {
    // do nothing
  } else if (FALSE_IT(max_bucket = std::max(backup_pg_meta.partitions_.count(), table_ids.count()))) {
  } else if (OB_FAIL(restore_table_ids_set.create(max_bucket))) {
    STORAGE_LOG(WARN, "failed to create restore table ids set", K(ret), K(max_bucket));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_pg_meta.partitions_.count(); ++i) {
      const uint64_t backup_table_id = backup_pg_meta.partitions_.at(i).get_table_id();
      uint64_t table_id = 0;
      if (OB_FAIL(restore_info.trans_from_backup_schema_id(backup_table_id, table_id))) {
        STORAGE_LOG(WARN, "get dst table id fail", K(ret), K(restore_info), K(backup_table_id));
      } else if (OB_FAIL(restore_table_ids_set.set_refactored(table_id))) {
        STORAGE_LOG(WARN, "failed to set table id into hash set", K(ret), K(table_id));
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
        const uint64_t table_id = table_ids.at(i);
        const ObTableSchema* table_schema = NULL;
        int hash_ret = OB_SUCCESS;
        if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
          STORAGE_LOG(WARN, "failed to get table schema", K(ret), K(table_id));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "table schema should not be NULL", K(ret), K(table_id), KP(table_schema));
        }

        if (OB_SUCC(ret)) {
          hash_ret = restore_table_ids_set.exist_refactored(table_id);
          if (OB_HASH_EXIST == hash_ret) {
            if (table_schema->is_dropped_schema()) {
              // skip ret, just print log
              STORAGE_LOG(WARN, "restore table should not be dropped schema", K(ret), K(table_id), K(backup_pg_meta));
            }
          } else if (OB_HASH_NOT_EXIST == hash_ret) {
            bool need_skip = false;
            if (OB_FAIL(
                    ObBackupRestoreTableSchemaChecker::check_backup_restore_need_skip_table(table_schema, need_skip))) {
              STORAGE_LOG(WARN, "failed to check backup restore need skip table", K(ret));
            } else if (!need_skip) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(ERROR, "restore table not in pg, may lost data!", K(ret), K(table_id), K(backup_pg_meta));
            }
          } else {
            ret = OB_SUCCESS == hash_ret ? OB_ERR_UNEXPECTED : hash_ret;
            STORAGE_LOG(WARN, "failed to get table id in hash set", K(ret), K(table_id), K(backup_pg_meta));
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV2::get_restore_tenant_id(uint64_t& tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_ID;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition group meta restore reader is not init", K(ret));
  } else {
    tenant_id = pg_meta_.pg_key_.get_tenant_id();
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV2::get_backup_partition_meta_info(const ObPartitionKey& pkey,
    const ObPhysicalRestoreArg& restore_info, const ObBackupPartitionStoreMetaInfo*& backup_partition_meta_info)
{
  int ret = OB_SUCCESS;
  backup_partition_meta_info = NULL;
  ObPartitionKey backup_pkey;
  uint64_t backup_table_id = 0;
  bool found = false;

  if (!backup_pg_meta_info_.is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "backup pg meta info is invalid", K(ret), K(pkey));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get backup partition meta info get invalid argument", K(ret), K(pkey));
  } else if (OB_FAIL(restore_info.trans_to_backup_schema_id(pkey.table_id_, backup_table_id))) {
    STORAGE_LOG(WARN, "failed to trans backup schema id", K(ret), K(pkey));
  } else {
    backup_pkey = pkey;
    backup_pkey.table_id_ = backup_table_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_pg_meta_info_.partition_store_meta_info_array_.count() && !found;
         ++i) {
      const ObPartitionKey& tmp_key =
          backup_pg_meta_info_.partition_store_meta_info_array_.at(i).partition_store_meta_.pkey_;
      if (backup_pkey == tmp_key) {
        backup_partition_meta_info = &(backup_pg_meta_info_.partition_store_meta_info_array_.at(i));
        found = true;
      }
    }

    if (OB_SUCC(ret) && !found) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "can not find partition", K(ret), K(pkey), K(backup_pg_meta_info_));
    }
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV2::get_restore_schema_version(int64_t& schema_version)
{
  int ret = OB_SUCCESS;
  schema_version = OB_INVALID_VERSION;
  const int64_t tenant_id = pg_meta_.pg_key_.get_tenant_id();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition group meta restore reader is not init", K(ret));
  } else {
    schema_version = schema_version_;
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV2::trans_from_backup_partitions(const ObPhysicalRestoreArg& restore_info,
    const common::ObPartitionArray& backup_partitions, common::ObPartitionArray& current_partitions)
{
  int ret = OB_SUCCESS;
  current_partitions.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < backup_partitions.count(); ++i) {
    const ObPartitionKey& backup_pkey = backup_partitions.at(i);
    ObPartitionKey current_pkey = backup_pkey;
    if (OB_FAIL(restore_info.trans_from_backup_schema_id(backup_pkey.table_id_, current_pkey.table_id_))) {
      STORAGE_LOG(WARN, "failed to trans from backup schema id", K(ret), K(backup_pkey));
    } else if (OB_FAIL(current_partitions.push_back(current_pkey))) {
      STORAGE_LOG(WARN, "failed to push pkey into array", K(ret), K(current_pkey));
    }
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV2::trans_to_backup_partitions(const ObPhysicalRestoreArg& restore_info,
    const common::ObPartitionArray& current_partitions, common::ObPartitionArray& backup_partitions)
{
  int ret = OB_SUCCESS;
  backup_partitions.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < current_partitions.count(); ++i) {
    const ObPartitionKey& current_pkey = current_partitions.at(i);
    ObPartitionKey backup_pkey = current_pkey;
    if (OB_FAIL(restore_info.trans_to_backup_schema_id(current_pkey.table_id_, backup_pkey.table_id_))) {
      STORAGE_LOG(WARN, "failed to trans to backup schema id", K(ret), K(current_pkey));
    } else if (OB_FAIL(backup_partitions.push_back(backup_pkey))) {
      STORAGE_LOG(WARN, "failed to push pkey into array", K(ret), K(backup_pkey));
    }
  }
  return ret;
}

int ObPartitionGroupMetaRestoreReaderV2::do_filter_pg_partitions(const ObPGKey& pg_key, ObPartitionArray& partitions)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObMultiVersionSchemaService& schema_service = ObMultiVersionSchemaService::get_instance();
  const bool check_dropped_partition = false;

  if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "do filter pg partitions get invalid argument", K(ret), K(pg_key));
  } else if (OB_FAIL(ObBackupUtils::retry_get_tenant_schema_guard(
                 pg_key.get_tenant_id(), schema_service, schema_version_, schema_guard))) {
    STORAGE_LOG(WARN, "failed to get tenant schema guard", K(ret), K(pg_key), K(schema_version_));
  } else {
    bool is_exist = false;
    bool filtered = false;
    ObPartitionArray exist_partitions;
    for (int i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
      const ObPartitionKey& pkey = partitions.at(i);
      if (pkey.is_trans_table()) {
        is_exist = true;
      } else if (OB_FAIL(schema_guard.check_partition_exist(
                     pkey.get_table_id(), pkey.get_partition_id(), check_dropped_partition, is_exist))) {
        STORAGE_LOG(WARN, "schema guard check partition fail", K(ret), K(pkey));
      }

      if (OB_FAIL(ret)) {
      } else if (is_exist) {
        if (OB_FAIL(exist_partitions.push_back(pkey))) {
          STORAGE_LOG(WARN, "push back partition failed", K(ret), K(pkey));
        }
      } else {
        filtered = true;
        STORAGE_LOG(INFO, "restore partition is filtered", K(pkey));
      }
    }

    if (OB_SUCC(ret) && filtered) {
      partitions.reuse();
      if (OB_FAIL(partitions.assign(exist_partitions))) {
        STORAGE_LOG(WARN, "assign pg partitions failed", K(ret), K(exist_partitions));
      }
    }
  }
  return ret;
}

/**********************ObPGPartitionBaseDataMetaRestoreReaderV2**********************/
ObPGPartitionBaseDataMetaRestoreReaderV2::ObPGPartitionBaseDataMetaRestoreReaderV2()
    : is_inited_(false), reader_index_(0), partition_reader_array_(), schema_version_(0)
{}

ObPGPartitionBaseDataMetaRestoreReaderV2::~ObPGPartitionBaseDataMetaRestoreReaderV2()
{
  reset();
}

void ObPGPartitionBaseDataMetaRestoreReaderV2::reset()
{
  is_inited_ = false;
  reader_index_ = 0;
  partition_reader_array_.reset();
  schema_version_ = 0;
}

int ObPGPartitionBaseDataMetaRestoreReaderV2::init(
    const ObPartitionArray& partitions, ObPartitionGroupMetaRestoreReaderV2* reader)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 0;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "can not init twice", K(ret));
  } else if (OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(reader));
  } else if (OB_FAIL(reader->get_partition_readers(partitions, partition_reader_array_))) {
    STORAGE_LOG(WARN, "failed to get partition readers", K(ret));
  } else if (OB_FAIL(reader->get_restore_tenant_id(tenant_id))) {
    STORAGE_LOG(WARN, "failed to get restore tenant id", K(ret));
  } else if (OB_FAIL(reader->get_restore_schema_version(schema_version_))) {
    STORAGE_LOG(WARN, "failed to get restore schema version", K(ret));
  } else {
    reader_index_ = 0;
    is_inited_ = true;
    STORAGE_LOG(INFO, "succeed to init fetch_pg partition_meta_info", K(partition_reader_array_.count()));
  }

  return ret;
}

int ObPGPartitionBaseDataMetaRestoreReaderV2::fetch_pg_partition_meta_info(
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
    ObPartitionBaseDataMetaRestoreReaderV2* reader = partition_reader_array_.at(reader_index_);
    if (OB_ISNULL(reader)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "partition base data meta restore reader is NULL", K(ret), KP(reader), K(reader_index_));
    } else if (OB_FAIL(reader->fetch_partition_meta(partition_meta_info.meta_))) {
      STORAGE_LOG(WARN, "failed to fetch partition meta", K(ret), K(reader_index_));
    } else if (OB_FAIL(reader->fetch_all_table_ids(partition_meta_info.table_id_list_))) {
      STORAGE_LOG(WARN, "failed to fetch all table ids", K(ret), K(reader_index_));
    } else if (OB_FAIL(check_sstable_table_ids_in_table(
                   partition_meta_info.meta_.pkey_, partition_meta_info.table_id_list_))) {
      STORAGE_LOG(WARN, "failed to check table ids in schema", K(ret), K(partition_meta_info));
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

int ObPGPartitionBaseDataMetaRestoreReaderV2::check_sstable_table_ids_in_table(
    const ObPartitionKey& pkey, const common::ObIArray<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;
  ObHashSet<uint64_t> table_ids_set;
  const uint64_t data_table_id = pkey.get_table_id();
  const uint64_t tenant_id = pkey.get_tenant_id();
  ObArray<ObIndexTableStat> index_stats;
  ObSchemaGetterGuard schema_guard;
  ObMultiVersionSchemaService& schema_service = ObMultiVersionSchemaService::get_instance();
  const int64_t MAX_BUCKET_SIZE = 1024;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg base data meta ob reader do not init", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "check table ids in schema get invalid argument", K(ret), K(pkey), K(table_ids));
  } else if (pkey.is_trans_table() || is_sys_table(data_table_id)) {
    // do nothing
  } else if (OB_FAIL(table_ids_set.create(MAX_BUCKET_SIZE))) {
    STORAGE_LOG(WARN, "failed to create table ids set", K(ret));
  } else if (OB_FAIL(ObBackupUtils::retry_get_tenant_schema_guard(
                 tenant_id, schema_service, schema_version_, schema_guard))) {
    STORAGE_LOG(WARN, "failed to get tenant schema guard", K(ret), K(tenant_id), K(schema_version_));
  } else if (OB_FAIL(schema_guard.get_index_status(data_table_id, false /*with global index*/, index_stats))) {
    STORAGE_LOG(WARN, "failed to get index status", K(ret), K(data_table_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
      const uint64_t table_id = table_ids.at(i);
      if (OB_FAIL(table_ids_set.set_refactored(table_id))) {
        STORAGE_LOG(WARN, "can not set table id into set", K(ret), K(table_id));
      }
    }

    // data table or global index
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_sstable_ids_contain_schema_table_id(table_ids_set, data_table_id, schema_guard))) {
        STORAGE_LOG(WARN, "failed to check sstable ids contain schema table id", K(ret), K(data_table_id));
      }
    }

    // local index table
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < index_stats.count(); ++i) {
        const ObIndexTableStat& index_stat = index_stats.at(i);
        const uint64_t index_table_id = index_stat.index_id_;
        if (OB_FAIL(check_sstable_ids_contain_schema_table_id(table_ids_set, index_table_id, schema_guard))) {
          STORAGE_LOG(WARN, "failed to check sstable ids contain schema table id", K(ret), K(index_table_id));
        }
      }
    }
  }
  return ret;
}

int ObPGPartitionBaseDataMetaRestoreReaderV2::check_sstable_ids_contain_schema_table_id(
    const hash::ObHashSet<uint64_t>& table_ids_set, const uint64_t schema_table_id,
    schema::ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "pg base data meta ob reader do not init", K(ret));
  } else {
    bool need_skip = false;
    int hash_ret = table_ids_set.exist_refactored(schema_table_id);
    if (OB_HASH_EXIST == hash_ret) {
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      const ObTableSchema* table_schema = NULL;
      if (OB_FAIL(schema_guard.get_table_schema(schema_table_id, table_schema))) {
        STORAGE_LOG(WARN, "failed to get table schema", K(ret), K(schema_table_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "table schema should not be NULL", K(ret), KP(table_schema));
      } else if (OB_FAIL(ObBackupRestoreTableSchemaChecker::check_backup_restore_need_skip_table(
                     table_schema, need_skip))) {
        STORAGE_LOG(WARN, "failed to check backup restore need skip table", K(ret));
      } else if (!need_skip) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR,
            "restore index table in schema, but not in partition, may los data!",
            K(ret),
            K(schema_table_id),
            K(*table_schema));
      }
    } else {
      ret = OB_SUCCESS == hash_ret ? OB_ERR_UNEXPECTED : hash_ret;
      STORAGE_LOG(WARN, "can not check table id in hash set", K(ret), K(table_ids_set));
    }
  }
  return ret;
}

/***********************ObPhyRestoreMacroIndexStoreV1***************************/
ObPhyRestoreMacroIndexStoreV2::ObPhyRestoreMacroIndexStoreV2()
    : is_inited_(false), allocator_(ObModIds::RESTORE), index_map_(), backup_task_id_(-1), table_keys_ptr_()
{}

ObPhyRestoreMacroIndexStoreV2::~ObPhyRestoreMacroIndexStoreV2()
{
  reset();
}

void ObPhyRestoreMacroIndexStoreV2::reset()
{
  index_map_.clear();
  allocator_.reset();
  is_inited_ = false;
  backup_task_id_ = -1;
  table_keys_ptr_.reset();
}

bool ObPhyRestoreMacroIndexStoreV2::is_inited() const
{
  return is_inited_;
}

int ObPhyRestoreMacroIndexStoreV2::init(
    const int64_t backup_task_id, const share::ObPhysicalRestoreArg& arg, const ObReplicaRestoreStatus& restore_status)
{
  int ret = OB_SUCCESS;
  ObBackupBaseDataPathInfo path_info;
  ObStorageUtil util(true /*need retry*/);
  ObBackupPath path;
  common::ObPartitionKey pkey;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "physical restore macro index store init twice", K(ret));
  } else if (backup_task_id < 0 || OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "init physical restore macro index get invalid argument", K(ret), K(backup_task_id), K(arg));
  } else if (OB_FAIL(arg.get_backup_base_data_info(path_info))) {
    STORAGE_LOG(WARN, "get backup base data info fail", K(ret));
  } else if (OB_FAIL(arg.get_backup_pgkey(pkey))) {
    STORAGE_LOG(WARN, "failed to get backup pgkey", K(ret));
  } else if (OB_FAIL(index_map_.create(BUCKET_SIZE, ObModIds::RESTORE))) {
    STORAGE_LOG(WARN, "failed to create partition reader map", K(ret));
  } else if (OB_FAIL(init_major_macro_index(pkey, path_info, restore_status))) {
    STORAGE_LOG(WARN, "failed to init major macro index", K(ret), K(pkey), K(path_info));
  } else if (OB_FAIL(init_minor_macro_index(backup_task_id, pkey, path_info, restore_status))) {
    STORAGE_LOG(WARN, "failed to init minor macro index", K(ret), K(backup_task_id), K(pkey));
  } else {
    backup_task_id_ = backup_task_id;
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    index_map_.clear();
  }
  return ret;
}

int ObPhyRestoreMacroIndexStoreV2::init_major_macro_index(const common::ObPartitionKey& backup_pg_key,
    const ObBackupBaseDataPathInfo& path_info, const ObReplicaRestoreStatus& restore_status)
{
  int ret = OB_SUCCESS;
  bool is_exist = true;
  bool last_file_complete = false;
  ObBackupPath path;
  ObStorageUtil util(true /*need retry*/);

  if (!backup_pg_key.is_valid() || !path_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "init major macro index get invalid argument", K(ret), K(backup_pg_key), K(path_info));
  } else {
    int64_t retry_cnt_index = 0;
    while (OB_SUCC(ret) && is_exist) {
      path.reset();
      if (OB_FAIL(ObBackupPathUtil::get_major_macro_block_index_path(
              path_info, backup_pg_key.get_table_id(), backup_pg_key.get_partition_id(), retry_cnt_index, path))) {
        STORAGE_LOG(WARN, "failed to get major macro block index path", K(ret), K(path_info), K(backup_pg_key));
      } else if (OB_FAIL(util.is_exist(path.get_obstr(), path_info.dest_.get_storage_info(), is_exist))) {
        STORAGE_LOG(WARN, "fail to check index file exist or not", K(ret));
      } else if (!is_exist) {
        break;
      } else if (OB_FAIL(init_one_file(path.get_obstr(), path_info.dest_.get_storage_info()))) {
        STORAGE_LOG(WARN, "fail to init index file", K(ret), K(path));
      } else {
        ++retry_cnt_index;
        last_file_complete = true;
      }
    }
  }

  if (OB_SUCC(ret) && !last_file_complete && REPLICA_RESTORE_DATA == restore_status) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(
        WARN, "last macro index file is not complete", K(ret), K(backup_pg_key), K(path_info), K(restore_status));
  }
  return ret;
}

int ObPhyRestoreMacroIndexStoreV2::init_minor_macro_index(const int64_t backup_task_id,
    const common::ObPartitionKey& backup_pg_key, const ObBackupBaseDataPathInfo& path_info,
    const ObReplicaRestoreStatus& restore_status)
{
  int ret = OB_SUCCESS;
  bool is_exist = true;
  bool last_file_complete = false;
  ObBackupPath path;
  ObStorageUtil util(true /*need retry*/);

  if (backup_task_id < 0 || !backup_pg_key.is_valid() || !path_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "init minor macro index get invalid argument", K(ret), K(backup_pg_key), K(path_info), K(backup_task_id));
  } else {
    int64_t retry_cnt_index = 0;
    while (OB_SUCC(ret) && is_exist) {
      path.reset();
      if (OB_FAIL(ObBackupPathUtil::get_minor_macro_block_index_path(path_info,
              backup_pg_key.get_table_id(),
              backup_pg_key.get_partition_id(),
              backup_task_id,
              retry_cnt_index,
              path))) {
        STORAGE_LOG(WARN, "failed to get major macro block index path", K(ret), K(path_info), K(backup_pg_key));
      } else if (OB_FAIL(util.is_exist(path.get_obstr(), path_info.dest_.get_storage_info(), is_exist))) {
        STORAGE_LOG(WARN, "fail to check index file exist or not", K(ret));
      } else if (!is_exist) {
        break;
      } else if (OB_FAIL(init_one_file(path.get_obstr(), path_info.dest_.get_storage_info()))) {
        STORAGE_LOG(WARN, "fail to init index file", K(ret), K(path));
      } else {
        ++retry_cnt_index;
        last_file_complete = true;
      }
    }
  }

  if (OB_SUCC(ret) && !last_file_complete && REPLICA_RESTORE_DATA == restore_status) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(
        WARN, "last macro index file is not complete", K(ret), K(backup_pg_key), K(path_info), K(restore_status));
  }
  return ret;
}

int ObPhyRestoreMacroIndexStoreV2::init(const int64_t backup_task_id, const common::ObPartitionKey& pkey,
    const share::ObPhysicalBackupArg& arg, const ObBackupDataType& backup_data_type)
{
  int ret = OB_SUCCESS;
  ObBackupBaseDataPathInfo path_info;
  ObStorageUtil util(true /*need retry*/);
  ObBackupPath path;
  const ObReplicaRestoreStatus restore_status = ObReplicaRestoreStatus::REPLICA_NOT_RESTORE;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "physical restore macro index store init twice", K(ret));
  } else if (backup_task_id < 0 || !pkey.is_valid() || OB_UNLIKELY(!arg.is_valid()) || !backup_data_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "init physical restore macro index get invalid argument",
        K(ret),
        K(backup_task_id),
        K(pkey),
        K(arg),
        K(backup_data_type));
  } else if (OB_FAIL(arg.get_backup_base_data_info(path_info))) {
    STORAGE_LOG(WARN, "get backup base data info fail", K(ret));
  } else if (OB_FAIL(index_map_.create(BUCKET_SIZE, ObModIds::RESTORE))) {
    STORAGE_LOG(WARN, "failed to create partition reader map", K(ret));
  } else if (backup_data_type.is_major_backup()) {
    if (OB_FAIL(init_major_macro_index(pkey, path_info, restore_status))) {
      STORAGE_LOG(WARN, "failed to init major macro index", K(ret), K(pkey), K(path_info));
    }
  } else {
    if (OB_FAIL(init_minor_macro_index(backup_task_id, pkey, path_info, restore_status))) {
      STORAGE_LOG(WARN, "failed to init minor macro index", K(ret), K(backup_task_id), K(pkey));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else {
    index_map_.clear();
  }
  return ret;
}

int ObPhyRestoreMacroIndexStoreV2::init_one_file(const ObString& path, const ObString& storage_info)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::RESTORE);
  char* read_buf = NULL;
  int64_t read_size = 0;
  if (OB_FAIL(ObRestoreFileUtil::read_one_file(path, storage_info, allocator, read_buf, read_size))) {
    STORAGE_LOG(WARN, "fail to read index file", K(ret), K(path));
  } else if (read_size <= 0) {
    STORAGE_LOG(INFO, "may be empty block index file", K(ret), K(path));
  } else {
    ObBufferReader buffer_reader(read_buf, read_size, 0);
    const ObBackupCommonHeader* common_header = NULL;
    ObBackupTableMacroIndex macro_index;
    ObArray<ObBackupTableMacroIndex> index_list;
    ObITable::TableKey cur_table_key;
    ObBackupTableKeyInfo table_key_info;
    int64_t sstable_macro_block_index = -1;
    const ObITable::TableKey* table_key_ptr = NULL;
    while (OB_SUCC(ret) && buffer_reader.remain() > 0) {
      common_header = NULL;
      if (OB_FAIL(buffer_reader.get(common_header))) {
        STORAGE_LOG(WARN, "read macro index common header fail", K(ret));
      } else if (OB_ISNULL(common_header)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "macro index common header is null", K(ret));
      } else if (OB_FAIL(common_header->check_valid())) {
        STORAGE_LOG(WARN, "common_header is not vaild", K(ret));
      } else if (BACKUP_FILE_END_MARK == common_header->data_type_) {
        STORAGE_LOG(INFO, "file reach the end mark, ", K(path));
        break;
      } else if (common_header->data_length_ > buffer_reader.remain()) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "buffer_reader not enough", K(ret));
      } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_length_))) {
        STORAGE_LOG(WARN, "common header data checksum fail", K(ret), K(*common_header));
      } else {
        int64_t end_pos = buffer_reader.pos() + common_header->data_length_;
        while (OB_SUCC(ret) && buffer_reader.pos() < end_pos) {
          if (-1 == sstable_macro_block_index) {
            int64_t start_pos = buffer_reader.pos();
            if (OB_FAIL(buffer_reader.read_serialize(table_key_info))) {
              STORAGE_LOG(
                  WARN, "failed to read backup table key meta", K(ret), K(start_pos), K(end_pos), K(*common_header));
            } else if (0 == table_key_info.total_macro_block_count_) {
              sstable_macro_block_index = -1;
              table_key_info.reset();
            } else {
              sstable_macro_block_index = 0;
            }
          } else {
            if (OB_FAIL(buffer_reader.read_serialize(macro_index))) {
              STORAGE_LOG(WARN, "read macro index fail", K(ret));
            } else if (OB_FAIL(get_table_key_ptr(table_key_info.table_key_, table_key_ptr))) {
              STORAGE_LOG(WARN, "failed to get table key ptr", K(ret), K(table_key_info));
            } else if (FALSE_IT(macro_index.table_key_ptr_ = table_key_ptr)) {
            } else if (!macro_index.is_valid()) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "macro index is invalid", K(ret), K(macro_index));
            } else {
              sstable_macro_block_index = macro_index.sstable_macro_index_;
              if (0 == index_list.count()) {
                if (OB_FAIL(index_list.push_back(macro_index))) {
                  STORAGE_LOG(WARN, "fail to put macro index", K(ret), K(macro_index));
                }
                cur_table_key = *macro_index.table_key_ptr_;
              } else if (cur_table_key == *macro_index.table_key_ptr_) {
                if (OB_FAIL(index_list.push_back(macro_index))) {
                  STORAGE_LOG(WARN, "fail to put macro index", K(ret), K(macro_index));
                }
              } else {
                if (OB_FAIL(add_sstable_index(cur_table_key, index_list))) {
                  STORAGE_LOG(WARN, "fail to put index array to map", K(ret), K(cur_table_key));
                } else {
                  index_list.reuse();
                  cur_table_key = *macro_index.table_key_ptr_;
                  if (OB_FAIL(index_list.push_back(macro_index))) {
                    STORAGE_LOG(WARN, "fail to put macro index", K(ret), K(macro_index));
                  }
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

        if (OB_SUCC(ret) && index_list.count() > 0) {
          if (OB_FAIL(add_sstable_index(cur_table_key, index_list))) {
            STORAGE_LOG(WARN, "fail to put index array to map", K(ret), K(cur_table_key));
          } else {
            index_list.reset();
          }
        }

        if (OB_SUCC(ret) && common_header->align_length_ > 0) {
          if (OB_FAIL(buffer_reader.advance(common_header->align_length_))) {
            STORAGE_LOG(WARN, "buffer_reader buf not enough", K(ret), K(*common_header));
          }
        }
      }
    }
  }
  return ret;
}

int ObPhyRestoreMacroIndexStoreV2::add_sstable_index(
    const ObITable::TableKey& table_key, const common::ObIArray<ObBackupTableMacroIndex>& index_list)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObArray<ObBackupTableMacroIndex>* new_block_list = NULL;
  int64_t block_count = index_list.count();

  if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "add sstable index get invalid argument", K(ret), K(table_key));
  } else if (index_list.empty()) {
    // do nothing
  } else if (OB_FAIL(index_map_.get_refactored(table_key, new_block_list))) {
    if (OB_HASH_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "get old block list fail", K(ret), K(table_key));
    } else {
      ret = OB_SUCCESS;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObArray<ObBackupTableMacroIndex>)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc buf", K(ret));
      } else if (OB_ISNULL(new_block_list = new (buf) ObArray<ObBackupTableMacroIndex>())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to new block list", K(ret));
      } else if (OB_FAIL(index_map_.set_refactored(table_key, new_block_list, 1 /*rewrite*/))) {
        STORAGE_LOG(WARN, "failed to set macro index map", K(ret));
        new_block_list->~ObArray();
      }
    }
  } else if (OB_ISNULL(new_block_list)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "exist block list should not be null here", K(ret), K(table_key));
  }

  for (int i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const ObBackupTableMacroIndex& index = index_list.at(i);  // check
    if (index.sstable_macro_index_ < new_block_list->count()) {
      // skip
    } else if (OB_UNLIKELY(index.sstable_macro_index_ != new_block_list->count())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN,
          "sstable macro index is not continued",
          K(ret),
          K(table_key),
          K(index),
          "new_block_list_count",
          new_block_list->count(),
          K(*new_block_list));
    } else if (OB_FAIL(new_block_list->push_back(index))) {
      STORAGE_LOG(WARN, "failed to push back block index", K(ret), K(i));
    }
  }
  return ret;
}

int ObPhyRestoreMacroIndexStoreV2::get_macro_index_array(
    const ObITable::TableKey& table_key, const common::ObArray<ObBackupTableMacroIndex>*& index_list) const
{
  int ret = OB_SUCCESS;
  index_list = NULL;
  common::ObArray<ObBackupTableMacroIndex>* tmp_list = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "macro index store do not init", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get macro index array get invalid argument", K(ret), K(table_key));
  } else if (OB_FAIL(index_map_.get_refactored(table_key, tmp_list))) {
    STORAGE_LOG(WARN, "fail to get macro index", K(ret), K(table_key));
  } else {
    index_list = tmp_list;
  }
  return ret;
}

int ObPhyRestoreMacroIndexStoreV2::get_macro_index(
    const ObITable::TableKey& table_key, const int64_t sstable_macro_idx, ObBackupTableMacroIndex& macro_index) const
{
  int ret = OB_SUCCESS;
  macro_index.reset();
  common::ObArray<ObBackupTableMacroIndex>* index_list = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "macro index store do not init", K(ret));
  } else if (!table_key.is_valid() || sstable_macro_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get macro index get invalid argument", K(ret), K(table_key), K(sstable_macro_idx));
  } else if (OB_FAIL(index_map_.get_refactored(table_key, index_list))) {
    STORAGE_LOG(WARN, "fail to get macro index", K(ret), K(table_key));
  } else if (OB_ISNULL(index_list)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get sstable index array fail", K(ret), K(table_key));
  } else if (sstable_macro_idx >= index_list->count()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    STORAGE_LOG(
        WARN, "sstable idx not match with sstable index array", K(ret), K(sstable_macro_idx), K(index_list->count()));
  } else {
    macro_index = index_list->at(sstable_macro_idx);
    if (sstable_macro_idx != macro_index.sstable_macro_index_ || table_key != *macro_index.table_key_ptr_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro block index not match", K(ret), K(macro_index), K(table_key), K(sstable_macro_idx));
    }
    if (OB_FAIL(ret)) {
      macro_index.reset();
    }
  }
  return ret;
}

int ObPhyRestoreMacroIndexStoreV2::get_sstable_pair_list(
    const ObITable::TableKey& table_key, common::ObIArray<blocksstable::ObSSTablePair>& pair_list) const
{
  int ret = OB_SUCCESS;
  common::ObArray<ObBackupTableMacroIndex>* index_list = NULL;
  pair_list.reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "macro index store do not init", K(ret));
  } else if (OB_FAIL(index_map_.get_refactored(table_key, index_list))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      STORAGE_LOG(INFO, "macro index not exist, may be empty sstable", K(ret), K(table_key));
    } else {
      STORAGE_LOG(WARN, "fail to get macro index", K(ret), K(table_key));
    }
  } else if (OB_ISNULL(index_list)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get sstable index array fail", K(ret), K(table_key));
  } else {
    blocksstable::ObSSTablePair pair;
    for (int i = 0; OB_SUCC(ret) && i < index_list->count(); ++i) {
      pair.data_version_ = index_list->at(i).data_version_;
      pair.data_seq_ = index_list->at(i).data_seq_;
      if (OB_FAIL(pair_list.push_back(pair))) {
        STORAGE_LOG(WARN, "push sstable pair to pair list fail", K(ret), K(table_key), K(pair));
      }
    }
    if (OB_FAIL(ret)) {
      pair_list.reset();
    }
  }
  return ret;
}

int ObPhyRestoreMacroIndexStoreV2::get_major_macro_index_array(
    const uint64_t table_id, const common::ObArray<ObBackupTableMacroIndex>*& index_list) const
{
  int ret = OB_SUCCESS;
  index_list = NULL;
  common::ObArray<ObBackupTableMacroIndex>* tmp_list = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "macro index store do not init", K(ret));
  } else if (table_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get macro index array get invalid argument", K(ret), K(table_id));
  } else {
    for (MacroIndexMap::const_iterator iter = index_map_.begin(); iter != index_map_.end(); ++iter) {
      const ObITable::TableKey& table_key = iter->first;
      if (!table_key.is_major_sstable()) {
        // do nothing
      } else if (table_id == table_key.table_id_) {
        index_list = iter->second;
        break;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(index_list)) {
        ret = OB_HASH_NOT_EXIST;
        LOG_WARN("table do not has major sstable", K(ret), K(table_id));
      }
    }
  }
  return ret;
}

int ObPhyRestoreMacroIndexStoreV2::get_table_key_ptr(
    const ObITable::TableKey& table_key, const ObITable::TableKey*& table_key_ptr)
{
  int ret = OB_SUCCESS;
  table_key_ptr = NULL;
  if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get table key ptr get invalid argument", K(ret), K(table_key));
  } else {
    bool found = false;
    for (int64_t i = 0; i < table_keys_ptr_.count() && !found; ++i) {
      const ObITable::TableKey& tmp_table_key = *table_keys_ptr_.at(i);
      if (tmp_table_key == table_key) {
        table_key_ptr = table_keys_ptr_.at(i);
        found = true;
      }
    }

    if (!found) {
      void* buf = allocator_.alloc(sizeof(ObITable::TableKey));
      if (NULL == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
      } else {
        ObITable::TableKey* tmp_table_key_ptr = new (buf) ObITable::TableKey();
        *tmp_table_key_ptr = table_key;
        if (OB_FAIL(table_keys_ptr_.push_back(tmp_table_key_ptr))) {
          STORAGE_LOG(WARN, "failed to push table key into array", K(ret), K(*tmp_table_key_ptr));
        } else {
          table_key_ptr = tmp_table_key_ptr;
        }
      }
    }
  }
  return ret;
}
