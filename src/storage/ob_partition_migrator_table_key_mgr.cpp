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
#include "storage/ob_partition_migrator_table_key_mgr.h"
#include "lib/container/ob_array_iterator.h"
#include "share/ob_force_print_log.h"
#include "storage/ob_sstable.h"

namespace oceanbase {
namespace storage {

int ObTableKeyMgrUtil::convert_minor_table_key(
    const ObITable::TableKey& minor_table_key, ObITable::TableKey& tmp_table_key)
{
  int ret = OB_SUCCESS;
  ObFreezeInfoSnapshotMgr::NeighbourFreezeInfoLite freeze_info;
  tmp_table_key.reset();

  if (!minor_table_key.is_minor_sstable()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "invalid table type", K(tmp_table_key), K(ret));
  } else if (OB_FAIL(ObFreezeInfoMgrWrapper::get_instance().get_neighbour_major_freeze(
                 minor_table_key.trans_version_range_.base_version_, freeze_info))) {
    STORAGE_LOG(
        WARN, "failed to get freeze version", "snapshot", minor_table_key.trans_version_range_.base_version_, K(ret));
  } else {
    // value copy
    tmp_table_key = minor_table_key;
    // Set here to the current major version for easy processing, and it will be cleaned up when it is finally taken out
    if (freeze_info.next.freeze_version == ObVersion::MIN_VERSION) {
      // If it is MIN VERSION, it means that this is the last VERSION, and you need to take the last VERSION + 1
      tmp_table_key.version_ = freeze_info.prev.freeze_version + 1;
    } else {
      tmp_table_key.version_ = freeze_info.next.freeze_version;
    }
    STORAGE_LOG(INFO, "covert minor table key", K(minor_table_key), K(freeze_info), K(tmp_table_key));
  }
  return ret;
}

int ObTableKeyMgrUtil::classify_mgirate_tables(const uint64_t table_id,
    const common::ObIArray<ObITable::TableKey>& all_tables, common::ObIArray<ObITable::TableKey>& major_sstables,
    common::ObIArray<ObITable::TableKey>& inc_sstables)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < all_tables.count(); ++i) {
    const ObITable::TableKey& table_key = all_tables.at(i);
    if (table_id != table_key.table_id_) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "table id not match", K(ret), K(table_id), K(table_key));
    } else if (!table_key.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "table is invalid", K(ret), K(table_key));
    } else if (ObITable::is_major_sstable(table_key.table_type_)) {
      if (OB_FAIL(major_sstables.push_back(table_key))) {
        STORAGE_LOG(WARN, "failed to push major table into array", K(ret), K(table_key));
      }
    } else if (ObITable::is_minor_sstable(table_key.table_type_)) {
      if (OB_FAIL(inc_sstables.push_back(table_key))) {
        STORAGE_LOG(WARN, "failed to push minor table into array", K(ret), K(table_key));
      }
    } else {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "invalid table type in migrate", K(ret), K(table_key));
    }
  }

  return ret;
}

// This function depends on tableshandle orderly
// Ordered: [(major_sstables), (minor_sstables)]
int ObTableKeyMgrUtil::convert_src_table_keys(const int64_t log_ts, const int64_t table_id,
    const bool is_only_major_sstable, const ObTablesHandle& handle, ObIArray<storage::ObITable::TableKey>& table_keys)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(log_ts == ObLogTsRange::MAX_TS || table_id == OB_INVALID_ID)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to convert src table keys", K(ret), K(log_ts), K(table_id));
  } else {
    ObITable* table = NULL;
    for (int i = 0; OB_SUCC(ret) && i < handle.get_count(); ++i) {
      bool need_add = true;
      ObITable::TableKey table_key;
      if (OB_ISNULL(table = handle.get_tables().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to get tables ", K(ret));
      } else if (table->is_memtable()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected memtable in migrate source tables", K(ret), K(i), KPC(table), K(handle));
      } else if (table->get_key().table_id_ != table_id) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "table id not match", K(ret), K(i), K(table_id), K(handle));
      } else if (is_only_major_sstable && !table->is_major_sstable()) {
        // skip minor sstable
      } else if (table->is_trans_sstable() && table->get_end_log_ts() != log_ts) {
        ret = OB_EAGAIN;
        FLOG_WARN("end_log_ts of trans sstable is not equal to replay_log_ts, try again",
            K(ret),
            "trans sstable key",
            table->get_key(),
            "replay log ts",
            log_ts);
      } else if (FALSE_IT(table_key = table->get_key())) {
      } else if (OB_UNLIKELY(!table_key.is_valid(true /*skip version range*/))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected invalid tablekey", K(ret), K(table_key));
      } else if (OB_FAIL(table_keys.push_back(table_key))) {
        STORAGE_LOG(WARN, "Failed to push back table key", K(ret), K(table_key));
      }
    }  // end of loop
  }    // end of else

  return ret;
}

ObSrcTableKeyManager::ObSrcTableKeyManager()
    : is_inited_(false),
      snapshot_version_(0),
      max_memtable_base_version_(-1),  // TODO  to be delete
      table_id_(0),
      pkey_(),
      major_table_keys_(),
      inc_table_keys_(),
      is_only_major_sstable_(false)
{}

ObSrcTableKeyManager::~ObSrcTableKeyManager()
{
  major_table_keys_.destroy();
  inc_table_keys_.destroy();
}

int ObSrcTableKeyManager::init(const int64_t snapshot_version, const int64_t table_id, const ObPartitionKey& pkey,
    const bool is_only_major_sstable)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "src table mananger init twice", K(ret));
  } else if (snapshot_version <= 0 || table_id <= 0 || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "snapshot version is invalid", K(ret), K(snapshot_version), K(table_id), K(pkey));
  } else {
    snapshot_version_ = snapshot_version;
    table_id_ = table_id;
    pkey_ = pkey;
    is_only_major_sstable_ = is_only_major_sstable;
    is_inited_ = true;
  }
  return ret;
}

int ObSrcTableKeyManager::add_table_key(const ObITable::TableKey& table_key)
{
  bool need_add = true;
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "src table key manager do not init", K(ret));
  } else if (!table_key.is_valid(true)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "table key is invalid", K(ret), K(table_key), K(pkey_));
  } else {
    if (ObITable::is_major_sstable(table_key.table_type_)) {
      if (OB_FAIL(add_major_table(table_key))) {
        STORAGE_LOG(WARN, "failed to add major table", K(ret), K(table_key));
      }
    } else if (is_only_major_sstable_) {
      STORAGE_LOG(INFO, "skip not major sstable", K(table_key));
    } else if (ObITable::is_minor_sstable(table_key.table_type_)) {
      if (OB_FAIL(add_minor_table(table_key))) {
        STORAGE_LOG(WARN, "failed to add minor table", K(ret), K(table_key));
      }
    } else if (ObITable::is_memtable(table_key.table_type_)) {
      need_add = false;
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "table table is unexpected", K(ret), K(table_key));
    }
  }

  return ret;
}

int ObSrcTableKeyManager::add_major_table(const ObITable::TableKey& major_table_key)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "src table key mananger do not init", K(ret));
  } else if (!major_table_key.is_valid() || !ObITable::is_major_sstable(major_table_key.table_type_) ||
             major_table_key.table_id_ != table_id_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "add major table get invalid argument", K(ret), K(major_table_key), K(table_id_));
  } else if (OB_FAIL(major_table_keys_.push_back(major_table_key))) {
    STORAGE_LOG(WARN, "failed to add major table key into array", K(ret), K(major_table_key));
  }

  return ret;
}

int ObSrcTableKeyManager::add_minor_table(const ObITable::TableKey& minor_table_key)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "src table key do not init", K(ret));
  } else if (!minor_table_key.is_valid() || !ObITable::is_minor_sstable(minor_table_key.table_type_) ||
             minor_table_key.table_id_ != table_id_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "minor table key is invalid", K(ret), K(minor_table_key), K(table_id_));
  } else if (OB_FAIL(inc_table_keys_.push_back(minor_table_key))) {
    STORAGE_LOG(WARN, "fail add table key", K(minor_table_key), K(ret));
  }
  return ret;
}

int ObSrcTableKeyManager::add_mem_table(const ObITable::TableKey& mem_table_key)
{
  int ret = OB_SUCCESS;
  ObArray<ObITable::TableKey> tmp_key_list;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "src table key manager do not init", K(ret));
  } else if (!mem_table_key.is_valid(true) || !ObITable::is_memtable(mem_table_key.table_type_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "add memtable get invalid argument", K(ret), K(mem_table_key));
  } else if (OB_FAIL(conver_mem_table_key(mem_table_key, tmp_key_list))) {
    STORAGE_LOG(WARN, "failed to conver mem table key", K(ret), K(snapshot_version_), K(mem_table_key));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_key_list.count(); ++i) {
      ObITable::TableKey& tmp_table_key = tmp_key_list.at(i);
      if (OB_FAIL(check_and_cut_memtable_range(tmp_table_key))) {
        STORAGE_LOG(WARN, "failed to check and cut range", K(ret), K(tmp_table_key));
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_key_list.count(); ++i) {
        if (OB_FAIL(inc_table_keys_.push_back(tmp_key_list.at(i)))) {
          STORAGE_LOG(WARN, "failed to push mem table key into inc table keys", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSrcTableKeyManager::conver_mem_table_key(
    const storage::ObITable::TableKey& mem_table_key, common::ObIArray<storage::ObITable::TableKey>& tmp_key_list)
{
  int ret = OB_SUCCESS;
  ObFreezeInfoSnapshotMgr::NeighbourFreezeInfoLite freeze_info;
  tmp_key_list.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "src table key manager do not init", K(ret));
  } else if (mem_table_key.trans_version_range_.multi_version_start_ !=
                 mem_table_key.trans_version_range_.base_version_ ||
             !mem_table_key.is_memtable()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid mem table key", K(mem_table_key), K(ret));
    // Try to obtain the frozen information of the snapshot point, if not, you need to try again on the destination end
  } else if (mem_table_key.trans_version_range_.base_version_ >= snapshot_version_ ||
             max_memtable_base_version_ >= snapshot_version_) {
    // do nothing, skip this memtable
  } else {
    ObITable::TableKey tmp_table_key;
    tmp_table_key.reset();
    tmp_table_key.pkey_ = pkey_;
    tmp_table_key.table_id_ = table_id_;
    tmp_table_key.version_ = ObVersion::MIN_VERSION;
    tmp_table_key.trans_version_range_.base_version_ =
        std::max(mem_table_key.trans_version_range_.base_version_, max_memtable_base_version_);
    tmp_table_key.trans_version_range_.multi_version_start_ = tmp_table_key.trans_version_range_.base_version_;
    tmp_table_key.trans_version_range_.snapshot_version_ =
        std::min(mem_table_key.trans_version_range_.snapshot_version_, snapshot_version_);
    tmp_table_key.table_type_ = ObITable::MEMTABLE;

    if (!tmp_table_key.is_valid()) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "invalid tmp key", K(tmp_table_key), K(ret));
    } else if (OB_FAIL(tmp_key_list.push_back(tmp_table_key))) {
      STORAGE_LOG(WARN, "failed to push back", K(ret));
    } else {
      STORAGE_LOG(INFO, "succ to add new tmp mem_table", K(tmp_table_key));
    }
  }
  return ret;
}

int ObSrcTableKeyManager::check_and_cut_memtable_range(ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "src table key manager do not init", K(ret));
  } else if (!table_key.is_valid() || !table_key.is_memtable()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "check table key get invalid argument", K(ret), K(table_key));
  } else {
    // If the snapshot version number exceeds the snapshot, it needs to be cropped
    if (table_key.trans_version_range_.snapshot_version_ > snapshot_version_) {
      if (table_key.trans_version_range_.multi_version_start_ > snapshot_version_) {
        ret = OB_SNAPSHOT_DISCARDED;
        STORAGE_LOG(WARN, "snapshot discarded", K(table_key), K(snapshot_version_), K(ret));
      } else {
        STORAGE_LOG(INFO, "cut table key", K(table_key), K_(snapshot_version));
        table_key.trans_version_range_.snapshot_version_ = snapshot_version_;
      }
    }
  }
  return ret;
}

int ObSrcTableKeyManager::get_table_keys(ObIArray<ObITable::TableKey>& table_keys)
{
  int ret = OB_SUCCESS;
  table_keys.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "src table key manager do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < major_table_keys_.count(); ++i) {
      if (OB_FAIL(table_keys.push_back(major_table_keys_.at(i)))) {
        STORAGE_LOG(WARN, "failed to push major table key into array", K(ret), K(major_table_keys_.at(i)));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < inc_table_keys_.count(); ++i) {
      if (OB_FAIL(table_keys.push_back(inc_table_keys_.at(i)))) {
        STORAGE_LOG(WARN, "failed to push inc table key into array", K(ret), K(major_table_keys_.at(i)));
      }
    }
  }
  return ret;
}

int ObDestTableKeyManager::check_table_continues(const ObIArray<ObITable::TableKey>& tables, bool& is_continues)
{
  int ret = OB_SUCCESS;
  is_continues = true;
  ObITable::TableKey pre_table_key;
  for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
    const ObITable::TableKey& curr_table_key = tables.at(i);
    if (!curr_table_key.is_valid()) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "table key is invalid", K(ret), K(curr_table_key));
    } else if (0 == i) {
      // do noting
    } else {
      if (pre_table_key.trans_version_range_.snapshot_version_ > curr_table_key.trans_version_range_.base_version_) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN,
            "pre table key trans version range has overlap",
            K(ret),
            "pre table key",
            pre_table_key,
            "current table key",
            curr_table_key);
      } else if (pre_table_key.trans_version_range_.snapshot_version_ <
                 curr_table_key.trans_version_range_.base_version_) {
        is_continues = false;
        STORAGE_LOG(INFO, "table keys is not continues", K(tables));
        break;
      } else {
        if (pre_table_key.log_ts_range_.end_log_ts_ > curr_table_key.log_ts_range_.start_log_ts_) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(WARN,
              "pre table key log_ts range has overlap",
              K(ret),
              "pre table key",
              pre_table_key,
              "current table key",
              curr_table_key);
        } else if (pre_table_key.log_ts_range_.end_log_ts_ < curr_table_key.log_ts_range_.start_log_ts_) {
          is_continues = false;
          LOG_INFO("table keys are not continuous", K(pre_table_key), K(curr_table_key));
        }
      }
    }
    pre_table_key = curr_table_key;
  }
  return ret;
}

int ObDestTableKeyManager::convert_needed_inc_table_key(const common::ObIArray<ObITable::TableKey>& local_inc_tables,
    const common::ObIArray<ObITable::TableKey>& remote_inc_tables,
    const common::ObIArray<ObITable::TableKey>& remote_gc_inc_tables, const bool is_copy_cover_minor,
    common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables)
{
  int ret = OB_SUCCESS;
  bool is_continues = true;
  if (OB_FAIL(check_table_continues(remote_inc_tables, is_continues))) {
    STORAGE_LOG(WARN, "failed to check remote table continues", K(ret), K(remote_inc_tables));
  } else if (!is_continues) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "remote table key is not continues", K(ret), K(remote_inc_tables));
  } else if (OB_FAIL(check_table_continues(local_inc_tables, is_continues))) {
    STORAGE_LOG(WARN, "failed to check local table continues", K(ret), K(local_inc_tables));
  } else if (!is_continues) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "local table key is not continues", K(ret), K(local_inc_tables));
  } else if (OB_FAIL(get_all_needed_table(
                 local_inc_tables, remote_inc_tables, remote_gc_inc_tables, is_copy_cover_minor, copy_sstables))) {
    STORAGE_LOG(WARN,
        "failed to get all needed table",
        K(ret),
        K(is_copy_cover_minor),
        K(local_inc_tables),
        K(remote_inc_tables));
  }
  return ret;
}

int ObDestTableKeyManager::get_all_needed_table_with_cover_range(
    const common::ObIArray<ObITable::TableKey>& local_inc_tables,
    const common::ObIArray<ObITable::TableKey>& remote_inc_tables,
    common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables)
{
  int ret = OB_SUCCESS;
  bool need_add = true;
  int64_t last_continue_snapshot_version = 0;
  copy_sstables.reset();

  if (local_inc_tables.count() > 0) {
    last_continue_snapshot_version =
        local_inc_tables.at(local_inc_tables.count() - 1).trans_version_range_.snapshot_version_;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < remote_inc_tables.count(); ++i) {
    const ObITable::TableKey& remote_inc_table = remote_inc_tables.at(i);
    need_add = true;
    ObMigrateTableInfo::SSTableInfo sstable_info;
    sstable_info.src_table_key_ = remote_inc_table;
    sstable_info.src_table_key_.version_ = 0;
    sstable_info.dest_base_version_ = remote_inc_table.trans_version_range_.base_version_;

    if (last_continue_snapshot_version >= remote_inc_table.trans_version_range_.snapshot_version_) {
      need_add = false;
      LOG_INFO("remote table is less than last_continue_snapshot_version, no need add",
          K(last_continue_snapshot_version),
          K(remote_inc_table));
    }
    for (int64_t j = 0; need_add && OB_SUCC(ret) && j < local_inc_tables.count(); ++j) {
      const ObITable::TableKey& local_inc_table = local_inc_tables.at(j);
      if (remote_inc_table.trans_version_range_.snapshot_version_ <=
          local_inc_table.trans_version_range_.base_version_) {
        // new table is no left
      } else if (remote_inc_table.trans_version_range_.snapshot_version_ <
                 local_inc_table.trans_version_range_.snapshot_version_) {
        need_add = false;
        if (remote_inc_table.trans_version_range_.base_version_ >= local_inc_table.trans_version_range_.base_version_) {
          // new table is contained by old table
          STORAGE_LOG(
              INFO, "remote table is contained by local table, not need add", K(local_inc_table), K(remote_inc_table));
        } else {
          STORAGE_LOG(INFO,
              "right part of remote table is cross with local table, cannot add",
              K(local_inc_table),
              K(remote_inc_table));
        }
      } else {  // new_table->get_snapshot_version() >= table->get_snapshot_version()
        if (remote_inc_table.trans_version_range_.base_version_ >=
            local_inc_table.trans_version_range_.snapshot_version_) {
          // new table is on right
        } else if (remote_inc_table.trans_version_range_.base_version_ >
                   local_inc_table.trans_version_range_.base_version_) {
          if (last_continue_snapshot_version >= remote_inc_table.trans_version_range_.snapshot_version_) {
            need_add = false;
          } else {
            sstable_info.dest_base_version_ = local_inc_table.trans_version_range_.snapshot_version_;
            STORAGE_LOG(INFO,
                "left part of remote table is cross with old table, cut range",
                K(local_inc_table),
                K(remote_inc_table),
                K(sstable_info));
          }
        } else if (remote_inc_table.trans_version_range_.base_version_ ==
                   local_inc_table.trans_version_range_.base_version_) {
          if (remote_inc_table.trans_version_range_.snapshot_version_ ==
              local_inc_table.trans_version_range_.snapshot_version_) {
            need_add = false;
          } else {  // remote_inc_table.trans_version_range_.snapshot_version_ >
                    // local_inc_table.trans_version_range_.snapshot_version_
            // local is covered by remote
          }
        } else {  // remote_inc_table.trans_version_range_.b ase_version_ <
                  // local_inc_table.trans_version_range_.base_version_
          // local is covered by remote
        }
      }
    }

    if (OB_SUCC(ret) && need_add) {
      if (OB_FAIL(copy_sstables.push_back(sstable_info))) {
        STORAGE_LOG(WARN, "failed to push sstable info into array", K(ret), K(sstable_info));
      }
    }
  }

  LOG_INFO("finish get_all_needed_table_with_cover_range", K(local_inc_tables), K(remote_inc_tables), K(copy_sstables));
  return ret;
}

int ObDestTableKeyManager::convert_sstable_info_to_table_key(
    const ObMigrateTableInfo::SSTableInfo& sstable_info, ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;
  if (!sstable_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "convert sstable into to table key get invalid argument", K(ret), K(sstable_info));
  } else {
    table_key = sstable_info.src_table_key_;
    table_key.trans_version_range_.base_version_ = sstable_info.dest_base_version_;
    table_key.log_ts_range_ = sstable_info.dest_log_ts_range_;
    if (table_key.is_minor_sstable()) {
      table_key.version_ = 0;
    }
  }
  return ret;
}

int ObDestTableKeyManager::get_all_needed_table_without_cover_range(
    const common::ObIArray<ObITable::TableKey>& local_inc_tables,
    const common::ObIArray<ObITable::TableKey>& remote_inc_tables,
    const common::ObIArray<ObITable::TableKey>& remote_gc_inc_tables,
    common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables, bool& is_succ)
{
  int ret = OB_SUCCESS;
  int64_t first_base_version = 0;
  int64_t last_continue_snapshot_version = 0;
  common::ObArray<ObITable::TableKey> remote_all_tables;
  copy_sstables.reset();
  is_succ = true;

  for (int64_t i = 0; OB_SUCC(ret) && i < remote_inc_tables.count(); ++i) {
    const ObITable::TableKey& remote_inc_table = remote_inc_tables.at(i);
    if (OB_FAIL(remote_all_tables.push_back(remote_inc_table))) {
      LOG_WARN("failed to add remote inc table", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (local_inc_tables.empty()) {  // use remote
      first_base_version = remote_inc_tables.at(0).trans_version_range_.base_version_;
      last_continue_snapshot_version = first_base_version;
    } else {
      first_base_version = local_inc_tables.at(0).trans_version_range_.base_version_;
      last_continue_snapshot_version =
          local_inc_tables.at(local_inc_tables.count() - 1).trans_version_range_.snapshot_version_;
    }

    if (!local_inc_tables.empty()) {
      ObTableKeyVersionRangeCompare cmp;

      for (int64_t i = 0; OB_SUCC(ret) && i < remote_gc_inc_tables.count(); ++i) {
        if (OB_FAIL(remote_all_tables.push_back(remote_gc_inc_tables.at(i)))) {
          LOG_WARN("faield to add table key", K(ret));
        }
      }
      std::sort(remote_all_tables.begin(), remote_all_tables.end(), cmp);
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < remote_all_tables.count(); ++i) {
      const ObITable::TableKey& remote_table = remote_all_tables.at(i);
      const ObVersionRange& remote_version_range = remote_table.trans_version_range_;
      if (last_continue_snapshot_version >= remote_version_range.snapshot_version_) {
        // remote is covered, no need copy
      } else if (last_continue_snapshot_version < remote_version_range.base_version_) {
        // not continue, no need copy
      } else {
        ObMigrateTableInfo::SSTableInfo sstable_info;
        sstable_info.src_table_key_ = remote_table;
        sstable_info.src_table_key_.version_ = 0;
        sstable_info.dest_base_version_ = remote_version_range.base_version_;

        if (last_continue_snapshot_version > remote_version_range.base_version_) {
          sstable_info.dest_base_version_ = last_continue_snapshot_version;  // need cut
        }

        if (OB_FAIL(copy_sstables.push_back(sstable_info))) {
          STORAGE_LOG(WARN, "failed to push sstable into array", K(ret), K(sstable_info));
        }
        last_continue_snapshot_version = remote_version_range.snapshot_version_;
      }
    }

    LOG_INFO("get_all_needed_table_without_cover_range",
        K(is_succ),
        K(remote_all_tables),
        K(local_inc_tables),
        K(copy_sstables));
  }
  return ret;
}

int ObDestTableKeyManager::get_all_needed_table(const common::ObIArray<ObITable::TableKey>& local_inc_tables,
    const common::ObIArray<ObITable::TableKey>& remote_inc_tables,
    const common::ObIArray<ObITable::TableKey>& remote_gc_inc_tables, const bool is_copy_cover_minor,
    common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables)
{
  int ret = OB_SUCCESS;
  bool is_succ = false;

  if (remote_inc_tables.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid remote_inc_tables", K(ret), K(remote_inc_tables));
  }

  if (OB_SUCC(ret) && !is_copy_cover_minor && !remote_gc_inc_tables.empty()) {
    if (OB_FAIL(get_all_needed_table_without_cover_range(
            local_inc_tables, remote_inc_tables, remote_gc_inc_tables, copy_sstables, is_succ))) {
      STORAGE_LOG(WARN, "failed to get all needed table without cover range", K(ret));
    }
  }

  if (OB_SUCC(ret) && !is_succ) {
    if (OB_FAIL(get_all_needed_table_with_cover_range(local_inc_tables, remote_inc_tables, copy_sstables))) {
      STORAGE_LOG(WARN, "failed to get all needed table with cover range", K(ret));
    }
  }

  LOG_INFO("finish get_all_needed_table",
      "local_count",
      local_inc_tables.count(),
      "remote_count",
      remote_inc_tables.count(),
      "remote_gc_count",
      remote_gc_inc_tables.count(),
      K(is_copy_cover_minor),
      "copy_count",
      copy_sstables.count());
  return ret;
}

bool ObTableKeyVersionRangeCompare::operator()(const ObITable::TableKey& left, const ObITable::TableKey& right)
{
  bool is_less = false;
  if (left.trans_version_range_.snapshot_version_ < right.trans_version_range_.snapshot_version_) {
    is_less = true;
  } else if (left.trans_version_range_.snapshot_version_ > right.trans_version_range_.snapshot_version_) {
    is_less = false;
  } else if (left.trans_version_range_.base_version_ > right.trans_version_range_.base_version_) {
    is_less = true;
  } else if (left.trans_version_range_.base_version_ < right.trans_version_range_.base_version_) {
    is_less = false;
  } else if (left.is_mini_minor_sstable() != right.is_mini_minor_sstable()) {
    if (left.is_mini_minor_sstable()) {
      is_less = true;
    } else {  // right.is_mini_minor_sstable()
      is_less = false;
    }
  } else {  // If base_version\snapshot_versiond\is_mini_minor_sstable is the same, it is considered equal
    is_less = false;
  }
  return is_less;
}

// TODO  to be rewrited
int ObDestTableKeyManager::get_all_needed_table_without_cover_range_v2(
    const common::ObIArray<ObITable::TableKey>& local_inc_tables,
    const common::ObIArray<ObITable::TableKey>& remote_inc_tables,
    const common::ObIArray<ObITable::TableKey>& remote_gc_inc_tables,
    common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables, bool& is_succ)
{
  int ret = OB_SUCCESS;
  int64_t last_continue_end_log_ts = 0;
  int64_t last_continue_snapshot_version = 0;
  common::ObArray<ObITable::TableKey> remote_all_tables;
  copy_sstables.reset();
  is_succ = true;

  for (int64_t i = 0; OB_SUCC(ret) && i < remote_inc_tables.count(); ++i) {
    const ObITable::TableKey& remote_inc_table = remote_inc_tables.at(i);
    if (OB_FAIL(remote_all_tables.push_back(remote_inc_table))) {
      LOG_WARN("failed to add remote inc table", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (local_inc_tables.empty()) {  // use remote
      last_continue_end_log_ts = remote_inc_tables.at(0).log_ts_range_.start_log_ts_;
      last_continue_snapshot_version = remote_inc_tables.at(0).trans_version_range_.base_version_;
    } else {
      last_continue_end_log_ts = local_inc_tables.at(local_inc_tables.count() - 1).log_ts_range_.end_log_ts_;
      last_continue_snapshot_version =
          local_inc_tables.at(local_inc_tables.count() - 1).trans_version_range_.snapshot_version_;
    }

    if (!local_inc_tables.empty()) {
      ObTableKeyCompare cmp;

      for (int64_t i = 0; OB_SUCC(ret) && i < remote_gc_inc_tables.count(); ++i) {
        if (OB_FAIL(remote_all_tables.push_back(remote_gc_inc_tables.at(i)))) {
          LOG_WARN("faield to add table key", K(ret));
        }
      }
      std::sort(remote_all_tables.begin(), remote_all_tables.end(), cmp);
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < remote_all_tables.count(); ++i) {
      const ObITable::TableKey& remote_table = remote_all_tables.at(i);
      const ObVersionRange& remote_version_range = remote_table.trans_version_range_;
      const ObLogTsRange& remote_log_ts_range = remote_table.log_ts_range_;
      if (last_continue_end_log_ts >= remote_log_ts_range.end_log_ts_) {
        // remote is covered, no need copy
      } else {
        ObMigrateTableInfo::SSTableInfo sstable_info;
        sstable_info.src_table_key_ = remote_table;
        sstable_info.src_table_key_.version_ = 0;
        sstable_info.dest_base_version_ = remote_version_range.base_version_;
        sstable_info.dest_log_ts_range_ = remote_log_ts_range;

        // log_ts range cross
        if (last_continue_end_log_ts >= remote_log_ts_range.start_log_ts_) {
          sstable_info.dest_log_ts_range_.start_log_ts_ = last_continue_end_log_ts;
        }

        if (OB_FAIL(copy_sstables.push_back(sstable_info))) {
          STORAGE_LOG(WARN, "failed to push sstable into array", K(ret), K(sstable_info));
        }
        last_continue_end_log_ts = remote_log_ts_range.end_log_ts_;
      }
    }

    LOG_INFO("get_all_needed_table_without_cover_range",
        K(is_succ),
        K(remote_all_tables),
        K(local_inc_tables),
        K(copy_sstables));
  }
  return ret;
}

int ObDestTableKeyManager::get_all_needed_table_with_cover_range_v2(const int64_t need_reserve_major_snapshot,
    const common::ObIArray<ObITable::TableKey>& local_inc_tables,
    const common::ObIArray<ObITable::TableKey>& remote_inc_tables,
    common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables)
{
  int ret = OB_SUCCESS;
  bool need_add = true;
  int64_t last_continue_end_log_ts = 0;
  int64_t remote_start_copy_pos = -1;
  bool can_use_remote_to_cover_local = false;
  copy_sstables.reset();

  if (local_inc_tables.count() > 0) {
    last_continue_end_log_ts = local_inc_tables.at(local_inc_tables.count() - 1).log_ts_range_.end_log_ts_;

    for (int64_t i = 0; OB_SUCC(ret) && i < remote_inc_tables.count(); ++i) {
      const ObITable::TableKey& remote_table = remote_inc_tables.at(i);
      if (last_continue_end_log_ts < remote_table.log_ts_range_.end_log_ts_) {
        remote_start_copy_pos = i;
        break;
      }
    }
  } else {
    remote_start_copy_pos = 0;
  }

  if (OB_SUCC(ret) && remote_start_copy_pos >= 0) {
    if (OB_FAIL(check_can_use_remote_to_cover_local(local_inc_tables,
            remote_inc_tables,
            remote_start_copy_pos,
            need_reserve_major_snapshot,
            can_use_remote_to_cover_local))) {
      LOG_WARN("failed to check_can_use_remote_to_cover_local",
          K(ret),
          K(local_inc_tables),
          K(remote_inc_tables),
          K(remote_start_copy_pos),
          K(need_reserve_major_snapshot));
    }
    // cal log_ts range and version range
    int64_t last_snapshot_version = 0;
    for (int64_t i = remote_start_copy_pos; OB_SUCC(ret) && i < remote_inc_tables.count(); ++i) {
      const ObITable::TableKey& remote_table = remote_inc_tables.at(i);
      const ObVersionRange& remote_version_range = remote_table.trans_version_range_;
      const ObLogTsRange& remote_log_ts_range = remote_table.log_ts_range_;
      bool found = false;

      ObMigrateTableInfo::SSTableInfo sstable_info;
      sstable_info.src_table_key_ = remote_table;
      sstable_info.src_table_key_.version_ = 0;
      sstable_info.dest_base_version_ = std::max(last_snapshot_version, remote_version_range.base_version_);
      sstable_info.dest_log_ts_range_ = remote_log_ts_range;
      if (i == remote_start_copy_pos) {
        if (can_use_remote_to_cover_local) {
          for (int64_t j = local_inc_tables.count() - 1; OB_SUCC(ret) && j >= 0; --j) {
            const ObITable::TableKey& local_inc_table = local_inc_tables.at(j);
            const ObVersionRange& local_version_range = local_inc_table.trans_version_range_;
            const ObLogTsRange& local_log_ts_range = local_inc_table.log_ts_range_;
            if (local_log_ts_range.start_log_ts_ < remote_log_ts_range.start_log_ts_ &&
                local_log_ts_range.end_log_ts_ >= remote_log_ts_range.start_log_ts_) {
              // cross log_ts range, reset start_log_ts and base_version
              sstable_info.dest_log_ts_range_ = local_log_ts_range;
              sstable_info.dest_base_version_ = local_version_range.snapshot_version_;
              found = true;
            } else if (local_log_ts_range.start_log_ts_ == remote_log_ts_range.start_log_ts_) {
              if (local_log_ts_range.end_log_ts_ > remote_log_ts_range.start_log_ts_) {
                // Non-empty local table will be overwritten if the log_ts range can be overwritten, and base_version
                // needs to be reset So that the version range can also be covered, because the replacement condition of
                // the minor sstable is log_ts range and version range covers
                sstable_info.dest_base_version_ = local_version_range.base_version_;
              } else {  // local_log_ts_range.end_log_ts_ == remote_log_ts_range.start_log_ts_
                // For the convenience of calculation, the remote table does not cover the empty log_ts range table
                sstable_info.dest_base_version_ = local_version_range.snapshot_version_;
              }
              found = true;
            }
            if (found) {
              const ObVersionRange& last_local_version_range =
                  local_inc_tables.at(local_inc_tables.count() - 1).trans_version_range_;
              break;
            }
          }
        } else if (local_inc_tables.count() > 0) {
          // local and remote have intersection
          const ObLogTsRange& last_local_log_ts_range = local_inc_tables.at(local_inc_tables.count() - 1).log_ts_range_;
          const ObVersionRange& last_local_version_range =
              local_inc_tables.at(local_inc_tables.count() - 1).trans_version_range_;
          if (last_local_log_ts_range.end_log_ts_ >= remote_log_ts_range.start_log_ts_) {
            sstable_info.dest_log_ts_range_ = last_local_log_ts_range;
            sstable_info.dest_base_version_ = last_local_version_range.snapshot_version_;
            FLOG_INFO("remote is not allowed to cover local, reset start_log_ts and version range",
                K(last_local_version_range),
                K(remote_version_range),
                K(last_local_log_ts_range),
                K(remote_log_ts_range),
                K(sstable_info));
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(copy_sstables.push_back(sstable_info))) {
        LOG_WARN("failed to copy sstables", K(ret), K(sstable_info));
      }
    }
  }

  FLOG_INFO("finish get_all_needed_table_with_cover_range",
      K(need_reserve_major_snapshot),
      K(local_inc_tables),
      K(remote_inc_tables),
      K(copy_sstables));
  return ret;
}

int ObDestTableKeyManager::convert_needed_inc_table_key_v2(const int64_t need_reserve_major_snapshot,
    const common::ObIArray<ObITable::TableKey>& local_inc_tables,
    const common::ObIArray<ObITable::TableKey>& remote_inc_tables,
    const common::ObIArray<ObITable::TableKey>& remote_gc_inc_tables, const bool is_copy_cover_minor,
    common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables)
{
  int ret = OB_SUCCESS;
  bool is_continues = true;
  if (OB_FAIL(check_table_continues(remote_inc_tables, is_continues))) {
    STORAGE_LOG(WARN, "failed to check remote table continues", K(ret), K(remote_inc_tables));
  } else if (!is_continues) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "remote table key is not continues", K(ret), K(remote_inc_tables));
  } else if (OB_FAIL(check_table_continues(local_inc_tables, is_continues))) {
    STORAGE_LOG(WARN, "failed to check local table continues", K(ret), K(local_inc_tables));
  } else if (!is_continues) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "local table key is not continues", K(ret), K(local_inc_tables));
  } else if (OB_FAIL(get_all_needed_table_v2(need_reserve_major_snapshot,
                 local_inc_tables,
                 remote_inc_tables,
                 remote_gc_inc_tables,
                 is_copy_cover_minor,
                 copy_sstables))) {
    STORAGE_LOG(WARN,
        "failed to get all needed table",
        K(ret),
        K(is_copy_cover_minor),
        K(local_inc_tables),
        K(remote_inc_tables));
  }
  return ret;
}

int ObDestTableKeyManager::get_all_needed_table_v2(const int64_t need_reserve_major_snapshot,
    const common::ObIArray<ObITable::TableKey>& local_inc_tables,
    const common::ObIArray<ObITable::TableKey>& remote_inc_tables,
    const common::ObIArray<ObITable::TableKey>& remote_gc_inc_tables, const bool is_copy_cover_minor,
    common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables)
{
  int ret = OB_SUCCESS;
  bool is_succ = false;

  if (remote_inc_tables.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid remote_inc_tables", K(ret), K(remote_inc_tables));
  }

  if (OB_SUCC(ret) && !is_copy_cover_minor && !remote_gc_inc_tables.empty()) {
    if (OB_FAIL(get_all_needed_table_without_cover_range_v2(
            local_inc_tables, remote_inc_tables, remote_gc_inc_tables, copy_sstables, is_succ))) {
      STORAGE_LOG(WARN, "failed to get all needed table without cover range", K(ret));
    }
  }

  if (OB_SUCC(ret) && !is_succ) {
    if (OB_FAIL(get_all_needed_table_with_cover_range_v2(
            need_reserve_major_snapshot, local_inc_tables, remote_inc_tables, copy_sstables))) {
      STORAGE_LOG(WARN, "failed to get all needed table with cover range", K(ret));
    }
  }

  LOG_INFO("finish get_all_needed_table",
      "local_count",
      local_inc_tables.count(),
      "remote_count",
      remote_inc_tables.count(),
      "remote_gc_count",
      remote_gc_inc_tables.count(),
      K(is_copy_cover_minor),
      "copy_count",
      copy_sstables.count(),
      K(need_reserve_major_snapshot));
  return ret;
}

int ObDestTableKeyManager::check_can_use_remote_to_cover_local(
    const common::ObIArray<ObITable::TableKey>& local_inc_tables,
    const common::ObIArray<ObITable::TableKey>& remote_inc_tables, const int64_t remote_start_copy_pos,
    const int64_t need_reserve_major_snapshot, bool& can_use_remote_to_cover_local)
{
  int ret = OB_SUCCESS;
  int64_t remote_multi_version_start = 0;
  can_use_remote_to_cover_local = need_reserve_major_snapshot == 0;
  // check if remote table contain need_reserved_major_snapshot
  const int64_t remote_start_copy_log_ts = remote_inc_tables.at(remote_start_copy_pos).log_ts_range_.start_log_ts_;
  for (int64_t i = 0; OB_SUCC(ret) && !can_use_remote_to_cover_local && i < local_inc_tables.count(); ++i) {
    const ObITable::TableKey local_table = local_inc_tables.at(i);
    if (local_table.log_ts_range_.start_log_ts_ < remote_start_copy_log_ts ||
        (local_table.log_ts_range_.start_log_ts_ == remote_start_copy_log_ts &&
            local_table.log_ts_range_.end_log_ts_ == remote_start_copy_log_ts)) {
      if (local_table.trans_version_range_.contain(need_reserve_major_snapshot)) {
        FLOG_INFO("local tables already have need_reserve_major_snapshot, can use remote to cover",
            K(need_reserve_major_snapshot),
            K(remote_start_copy_log_ts),
            K(local_table));
        can_use_remote_to_cover_local = true;
      }
    } else {
      break;
    }
  }
  if (!can_use_remote_to_cover_local) {
    for (int64_t i = remote_start_copy_pos;
         OB_SUCC(ret) && !can_use_remote_to_cover_local && i < remote_inc_tables.count();
         ++i) {
      const ObITable::TableKey& remote_table = remote_inc_tables.at(i);
      if (remote_table.trans_version_range_.contain(need_reserve_major_snapshot)) {
        FLOG_INFO("remote tables do have need_reserve_major_snapshot, can cover local table",
            K(remote_multi_version_start),
            K(need_reserve_major_snapshot),
            K(remote_inc_tables),
            K(remote_start_copy_pos));
        can_use_remote_to_cover_local = true;
      }
    }
    if (!can_use_remote_to_cover_local) {
      FLOG_INFO("remote tables do not have need_reserve_major_snapshot, can not cover local table",
          K(remote_multi_version_start),
          K(need_reserve_major_snapshot),
          K(remote_inc_tables),
          K(remote_start_copy_pos));
    }
  }
  return ret;
}

bool ObTableKeyCompare::operator()(const ObITable::TableKey& left, const ObITable::TableKey& right)
{
  bool is_less = false;
  if (left.log_ts_range_.end_log_ts_ < right.log_ts_range_.end_log_ts_) {
    is_less = true;
  } else if (left.log_ts_range_.end_log_ts_ > right.log_ts_range_.end_log_ts_) {
    is_less = false;
  } else if (left.log_ts_range_.start_log_ts_ > right.log_ts_range_.start_log_ts_) {
    is_less = true;
  } else if (left.log_ts_range_.start_log_ts_ < right.log_ts_range_.start_log_ts_) {
    is_less = false;
  } else if (left.trans_version_range_.snapshot_version_ < right.trans_version_range_.snapshot_version_) {
    is_less = true;
  } else if (left.trans_version_range_.snapshot_version_ > right.trans_version_range_.snapshot_version_) {
    is_less = false;
  } else if (left.trans_version_range_.base_version_ > right.trans_version_range_.base_version_) {
    is_less = true;
  } else if (left.trans_version_range_.base_version_ < right.trans_version_range_.base_version_) {
    is_less = false;
  } else if (left.is_mini_minor_sstable() != right.is_mini_minor_sstable()) {
    if (left.is_mini_minor_sstable()) {
      is_less = true;
    } else {  // right.is_mini_minor_sstable()
      is_less = false;
    }
  } else {  // If base_version\snapshot_versiond\is_mini_minor_sstable is the same, it is considered equal
    is_less = false;
  }
  return is_less;
}

}  // namespace storage
}  // namespace oceanbase
