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

#include "ob_partition_base_data_restore_reader.h"
#include "observer/ob_server.h"
#include "lib/utility/ob_tracepoint.h"
#include "ob_partition_migrator.h"
#include "ob_partition_service.h"
#include "lib/utility/ob_tracepoint.h"
#include "storage/ob_pg_storage.h"

using namespace oceanbase;
using namespace storage;
using namespace blocksstable;

int limit_bandwidth_and_sleep(ObInOutBandwidthThrottle& throttle, const int64_t cur_data_size, int64_t& last_read_size)
{
  int ret = OB_SUCCESS;
  const int64_t last_active_time = ObTimeUtility::current_time();
  const int64_t max_idle_time = 10 * 1000LL * 1000LL;  // 10s
  const int64_t read_size = cur_data_size - last_read_size;
  last_read_size = cur_data_size;

  if (OB_FAIL(throttle.limit_in_and_sleep(read_size, last_active_time, max_idle_time))) {
    STORAGE_LOG(WARN, "failed to limit_in_and_sleep", K(ret));
  }
  return ret;
}

// int ObMigrateInfoRestoreFetcher::init(const ObSavedStorageInfo &old_saved_info)
// {
//   int ret = OB_SUCCESS;

//   if (saved_info_.is_valid()) {
//     ret = OB_INIT_TWICE;
//     STORAGE_LOG(WARN, "cannot init twice", K(ret));
//   } else if (!old_saved_info.is_valid()) {
//     ret = OB_INVALID_ARGUMENT;
//     STORAGE_LOG(WARN, "invalid args", K(ret), K(old_saved_info));
//   } else if (OB_FAIL(saved_info_.deep_copy(old_saved_info))) {
//     STORAGE_LOG(WARN, "failed to copy saved info", K(ret));
//   }
//   return ret;
// }

// int ObMigrateInfoRestoreFetcher::fetch_migrate_info_result(
//     obrpc::ObMigrateInfoFetchResult &info)
// {
//   int ret = OB_SUCCESS;
//   ObStoreInfo tmp_info;
//   common::ObAddr fake_server = OBSERVER.get_self();
//   const int64_t fake_sstable_count = 0;
//   info.reset();

//   if (!saved_info_.is_valid()) {
//     ret = OB_NOT_INIT;
//     STORAGE_LOG(WARN, "saved info not inited", K(ret), K(saved_info_));
//   } else if (OB_FAIL(tmp_info.assign(saved_info_, fake_sstable_count))) {
//     STORAGE_LOG(WARN, "failed to assign store info", K(ret));
//   } else if (OB_FAIL(info.add_store_info(tmp_info, fake_server))) {
//     STORAGE_LOG(WARN, "failed to add store info", K(ret));
//   } else {
//     STORAGE_LOG(INFO, "succeed to fetch_migrate_info_result", K(info));
//   }
//   return ret;
// }

////////////////////////////////ObPartitionBaseDataMetaRestoreReader///////////////////////////////////

ObPartitionBaseDataMetaRestoreReader::ObPartitionBaseDataMetaRestoreReader()
    : is_inited_(false),
      pkey_(),
      restore_info_(NULL),
      reader_(),
      allocator_(ObModIds::OB_PARTITION_MIGRATOR),
      bandwidth_throttle_(NULL),
      last_read_size_(0),
      partition_store_meta_(),
      snapshot_version_(0),
      schema_version_(0),
      data_version_(0)
{}

ObPartitionBaseDataMetaRestoreReader::~ObPartitionBaseDataMetaRestoreReader()
{}

int ObPartitionBaseDataMetaRestoreReader::init(common::ObInOutBandwidthThrottle& bandwidth_throttle,
    const common::ObPartitionKey& pkey, const ObDataStorageInfo& data_info, ObRestoreInfo& restore_info)
{
  int ret = OB_SUCCESS;
  ObPartitionKey src_pkey;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (!pkey.is_valid() || !restore_info.is_inited() || !data_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(pkey), K(restore_info.get_restore_args()), K(data_info));
  } else if (OB_FAIL(ObPartitionKeyChangeUtil::change_dst_pkey_to_src_pkey(pkey, restore_info, src_pkey))) {
    STORAGE_LOG(WARN, "failed to change dst pkey to src pkey", K(ret), K(pkey), K(restore_info));
  } else if (OB_FAIL(reader_.init(restore_info.get_restore_args(), src_pkey))) {
    STORAGE_LOG(WARN, "failed to init meta oss reader", K(ret), K(src_pkey));
  } else if (OB_FAIL(prepare(pkey, data_info))) {
    STORAGE_LOG(WARN, "fail to prepare ", K(ret), K(pkey));
  } else {
    is_inited_ = true;
    pkey_ = pkey;
    restore_info_ = &restore_info;
    bandwidth_throttle_ = &bandwidth_throttle;
    last_read_size_ = 0;
    STORAGE_LOG(INFO, "succeed to init restore reader", K(pkey), K(restore_info_));
  }

  return ret;
}

int ObPartitionBaseDataMetaRestoreReader::fetch_partition_meta(ObPGPartitionStoreMeta& partition_store_meta)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPGPartitionStoreMeta oss_par_meta;
  uint64_t backup_table_id = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(reader_.read_partition_meta(oss_par_meta))) {  // Get partition_store_meta in oss
    STORAGE_LOG(WARN, "failed to read partition meta", K(ret));
  } else if (OB_FAIL(
                 restore_info_->get_restore_args().trans_to_backup_schema_id(pkey_.get_table_id(), backup_table_id))) {
    STORAGE_LOG(WARN, "failed to trans to backup table id", K(ret), K(pkey_), K(backup_table_id));
  } else if (oss_par_meta.pkey_.table_id_ != backup_table_id) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR,
        "backup table id not match",
        K(ret),
        K(oss_par_meta.pkey_.table_id_),
        "backup_table_id",
        backup_table_id,
        K(oss_par_meta),
        K(restore_info_->get_restore_args()));
  } else if (OB_FAIL(partition_store_meta.deep_copy(partition_store_meta_))) {
    STORAGE_LOG(WARN, "fail to copy partition store meta", K(ret), K(partition_store_meta_));
  } else if (OB_SUCCESS !=
             (tmp_ret = limit_bandwidth_and_sleep(*bandwidth_throttle_, reader_.get_data_size(), last_read_size_))) {
    STORAGE_LOG(WARN, "failed to limit_bandwidth_and_sleep", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to fetch partition meta", K(partition_store_meta));
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReader::fetch_sstable_meta(
    const uint64_t index_id, blocksstable::ObSSTableBaseMeta& sstable_meta)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t backup_index_id = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_INVALID_ID == index_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "index id is invalid", K(ret));
  } else if (OB_FAIL(restore_info_->get_restore_args().trans_to_backup_schema_id(index_id, backup_index_id))) {
    STORAGE_LOG(
        WARN, "failed to trans_from_backup_index_id", K(ret), K(index_id), K(restore_info_->get_restore_args()));
  } else if (OB_FAIL(reader_.read_sstable_meta(backup_index_id, sstable_meta))) {
    STORAGE_LOG(WARN, "failed to get sstable meta", K(ret), K(index_id), K(backup_index_id));
  } else {
    sstable_meta.index_id_ = index_id;
    sstable_meta.data_version_ = data_version_;
    sstable_meta.available_version_ = sstable_meta.data_version_;
    // 2.0
    sstable_meta.schema_version_ = schema_version_;
    sstable_meta.progressive_merge_start_version_ = 0;
    sstable_meta.progressive_merge_end_version_ = 0;
    sstable_meta.create_snapshot_version_ = snapshot_version_;
    // 2.2.3
    sstable_meta.logical_data_version_ = sstable_meta.logical_data_version_ < sstable_meta.data_version_
                                             ? sstable_meta.data_version_
                                             : sstable_meta.logical_data_version_;
    if (OB_SUCCESS !=
        (tmp_ret = limit_bandwidth_and_sleep(*bandwidth_throttle_, reader_.get_data_size(), last_read_size_))) {
      STORAGE_LOG(WARN, "failed to limit_bandwidth_and_sleep", K(ret));
    }
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReader::fetch_sstable_pair_list(
    const uint64_t index_id, common::ObIArray<blocksstable::ObSSTablePair>& pair_list)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t backup_index_id = 0;
  ObArray<blocksstable::ObSSTablePair> backup_pair_list;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (index_id == 0 || common::OB_INVALID_ID == index_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "index id is invalid", K(ret), K(index_id));
  } else if (OB_FAIL(restore_info_->get_restore_args().trans_to_backup_schema_id(index_id, backup_index_id))) {
    STORAGE_LOG(WARN, "fail to get backup index id", K(ret), K(index_id), K(backup_index_id));
  } else if (OB_FAIL(reader_.read_sstable_pair_list(backup_index_id, backup_pair_list))) {
    STORAGE_LOG(WARN, "fail to read sstable pair list", K(ret), K(backup_index_id));
  } else if (OB_FAIL(restore_info_->add_sstable_info(backup_index_id, backup_pair_list))) {
    STORAGE_LOG(WARN, "fail to add sstable info into restore info ", K(ret), K(backup_index_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_pair_list.count(); ++i) {
      blocksstable::ObSSTablePair pair = backup_pair_list.at(i);
      if (pair.data_seq_ < 0) {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(ERROR, "cannot restore too old format macro block", K(ret), K(pair));
      } else {
        pair.data_seq_ = i;
        pair.data_version_ = data_version_;
        if (OB_FAIL(pair_list.push_back(pair))) {
          STORAGE_LOG(WARN, "fail to push sstable pair into pair list", K(ret));
        }
      }
    }
    if (OB_SUCCESS !=
        (tmp_ret = limit_bandwidth_and_sleep(*bandwidth_throttle_, reader_.get_data_size(), last_read_size_))) {
      STORAGE_LOG(WARN, "failed to limit_bandwidth_and_sleep", K(ret));
    }
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReader::prepare(
    const common::ObPartitionKey& pkey, const ObDataStorageInfo& data_info)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObPGPartitionGuard pg_partition_guard;
  ObPartitionStorage* partition_storage = NULL;
  int64_t snapshot_version = INT64_MAX;

  if (!reader_.is_inited()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "oss reader do not init", K(ret));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pkey, guard)) ||
             OB_ISNULL(guard.get_partition_group())) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_partition(pkey, pg_partition_guard)) ||
             OB_ISNULL(pg_partition_guard.get_pg_partition())) {
    STORAGE_LOG(WARN, "fail to get pg partition", K(ret), K(pkey));
  } else if (OB_ISNULL(partition_storage = reinterpret_cast<ObPartitionStorage*>(
                           pg_partition_guard.get_pg_partition()->get_storage()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error, the partition storage is NULL", K(ret), K(pkey));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_storage().get_pg_partition_store_meta(
                 pkey, partition_store_meta_))) {
    STORAGE_LOG(WARN, "fail to get partition store meta", K(ret));
  } else if (OB_FAIL(get_smallest_base_version(&partition_storage->get_partition_store(), snapshot_version))) {
    STORAGE_LOG(WARN, "fail to get smallest base version", K(ret));
  } else if (OB_FAIL(get_freeze_info(snapshot_version, pkey, data_info))) {
    STORAGE_LOG(WARN, "fail to get freeze info", K(ret), K(snapshot_version), K(pkey));
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReader::get_smallest_base_version(
    ObPartitionStore* partition_store, int64_t& base_version)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids;
  ObTablesHandle tables_handle;
  bool is_ready_for_read = false;
  ObITable* table = NULL;
  int64_t tmp_base_version = INT64_MAX;
  bool is_all_complete = true;

  if (OB_ISNULL(partition_store)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "partition storage should not be NULL", K(ret), KP(partition_store));
  } else if (OB_FAIL(fetch_all_table_ids(table_ids))) {
    STORAGE_LOG(WARN, "fail to fetch_all_table_ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
      tables_handle.reset();
      if (OB_FAIL(partition_store->get_effective_tables(table_ids.at(i), tables_handle, is_ready_for_read))) {
        STORAGE_LOG(WARN, "fail to get effectve tables", K(ret), K(i), K(table_ids.at(i)));
      } else if (tables_handle.empty()) {
        STORAGE_LOG(INFO, "table id not exists", K(i), "tableid", table_ids.at(i));
        is_all_complete = false;
        continue;
      } else if (OB_ISNULL(table = tables_handle.get_table(0))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to get sstable", K(ret), K(tables_handle.get_count()), KP(table));
      } else {
        if (!table->is_major_sstable()) {
          is_all_complete = false;
        } else if (tables_handle.get_count() < 2) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(ERROR, "table handle has major sstable, but has no memtable", K(ret), K(tables_handle));
        } else {
          // Splitting is currently not supported, so just skip the first baseline.
          table = tables_handle.get_table(1);
        }

        if (tmp_base_version > table->get_key().trans_version_range_.base_version_) {
          tmp_base_version = table->get_key().trans_version_range_.base_version_;
          STORAGE_LOG(INFO, "update base version", K(tmp_base_version), K(i), K(tables_handle));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_all_complete) {
        // Explain that all sstables have restored baseline storage, there is no need to perform the current partition
        // task execution
        ret = OB_RESTORE_PARTITION_IS_COMPELETE;
        STORAGE_LOG(
            INFO, "restore partition has compelete sstable", K(ret), "pkey", table->get_key().pkey_, K(table_ids));
      } else if (INT64_MAX == tmp_base_version) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "cannot fins base version", K(ret), K(table_ids));
      } else {
        base_version = tmp_base_version;
      }
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_ADD_RESTORE_TASK_ERROR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "fake EN_ADD_RESTORE_TASK_ERROR", K(ret));
    }
  }
#endif
  return ret;
}

int ObPartitionBaseDataMetaRestoreReader::get_freeze_info(
    const int64_t snapshot_version, const ObPartitionKey& pkey, const ObDataStorageInfo& data_info)
{
  int ret = OB_SUCCESS;
  if (snapshot_version < 0 || INT64_MAX == snapshot_version || !pkey.is_valid() || !data_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "snapshot version is invalid", K(ret), K(snapshot_version), K(pkey), K(data_info));
  } else {
    const uint64_t table_id = pkey.get_table_id();
    ObFrozenStatus frozen_status;
    ObFreezeInfoSnapshotMgr& snapshot_mgr = ObFreezeInfoMgrWrapper::get_instance();
    ObFreezeInfoSnapshotMgr::FreezeInfoLite freeze_info_lite;
    if (OB_FAIL(snapshot_mgr.get_freeze_info_by_snapshot_version(snapshot_version, freeze_info_lite))) {
      STORAGE_LOG(WARN, "failed to get neighbour freeze info", K(ret), K(freeze_info_lite));
    } else {
      data_version_ = freeze_info_lite.freeze_version;
      schema_version_ = data_info.get_schema_version();
      snapshot_version_ = snapshot_version;
    }
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReader::fetch_all_table_ids(common::ObIArray<uint64_t>& table_id_array)
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> backup_table_id_array;
  uint64_t index_id = 0;
  if (!reader_.is_inited()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "oss reader do not init", K(ret));
  } else if (OB_ISNULL(restore_info_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "restore_info_ must not null", K(ret), KP(restore_info_));
  } else if (OB_FAIL(reader_.read_table_ids(backup_table_id_array))) {
    STORAGE_LOG(WARN, "fail to read table ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_table_id_array.count(); ++i) {
      if (OB_FAIL(
              restore_info_->get_restore_args().trans_from_backup_schema_id(backup_table_id_array.at(i), index_id))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          STORAGE_LOG(INFO, "index not backup, skip restore", K(i), "index_id", backup_table_id_array.at(i));
        } else {
          STORAGE_LOG(WARN,
              "failed to trans_from_backup_index_id",
              K(ret),
              K(backup_table_id_array.at(i)),
              K(restore_info_->get_restore_args()));
        }
      } else if (OB_FAIL(table_id_array.push_back(index_id))) {
        STORAGE_LOG(WARN, "fail to push index id into array", K(ret), K(index_id));
      }
    }
  }
  return ret;
}

int ObPartitionBaseDataMetaRestoreReader::fetch_table_keys(
    const uint64_t index_id, obrpc::ObFetchTableInfoResult& table_res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    uint64_t backup_index_id = 0;
    if (OB_FAIL(restore_info_->get_restore_args().trans_to_backup_schema_id(index_id, backup_index_id))) {
      STORAGE_LOG(
          WARN, "failed to trans_from_backup_index_id", K(ret), K(index_id), K(restore_info_->get_restore_args()));
    } else if (OB_FAIL(reader_.read_table_keys_by_table_id(backup_index_id, table_res.table_keys_))) {
      STORAGE_LOG(WARN, "fail to get table keys", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_res.table_keys_.count(); ++i) {
        ObITable::TableKey& table_key = table_res.table_keys_.at(i);
        if (!table_key.is_major_sstable() || 0 != table_key.trans_version_range_.base_version_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "table key has wrong table type", K(ret), K(table_key));
        } else {
          table_key.pkey_ = pkey_;
          table_key.table_id_ = index_id;
          table_key.version_ = data_version_;
          table_key.trans_version_range_.multi_version_start_ = snapshot_version_;
          table_key.trans_version_range_.snapshot_version_ = snapshot_version_;
          // no need change
          // table_key.table_type_
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    table_res.is_ready_for_read_ = true;
    table_res.multi_version_start_ = ObTimeUtility::current_time();
  }

  return ret;
}

////////////////////////////////ObPartitionKeyChangeUtil///////////////////////////////////

int ObPartitionKeyChangeUtil::change_dst_pkey_to_src_pkey(
    const ObPartitionKey& dst_pkey, const ObRestoreInfo& restore_info, ObPartitionKey& src_pkey)
{
  int ret = OB_SUCCESS;
  uint64_t src_table_id = 0;

  if (!dst_pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "dst pkey is invalid", K(ret), K(dst_pkey));
  } else if (OB_FAIL(
                 restore_info.get_restore_args().trans_to_backup_schema_id(dst_pkey.get_table_id(), src_table_id))) {
    STORAGE_LOG(WARN, "failed to change dst table id to src table id", K(dst_pkey), K(restore_info));
  } else {
    src_pkey = dst_pkey;
    src_pkey.table_id_ = src_table_id;
  }
  return ret;
}
