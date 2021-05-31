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

#define USING_LOG_PREFIX RS
#include "ob_backup_data_mgr.h"

namespace oceanbase {

using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace storage;

namespace rootserver {

ObBackupDataMgr::ObBackupDataMgr()
    : is_inited_(false),
      meta_index_(),
      cluster_backup_dest_(),
      full_backup_set_id_(0),
      inc_backup_set_id_(0),
      tenant_id_(OB_INVALID_ID)
{}

ObBackupDataMgr::~ObBackupDataMgr()
{}

int ObBackupDataMgr::init(const share::ObClusterBackupDest& cluster_backup_dest, const uint64_t tenant_id,
    const int64_t full_backup_set_id, const int64_t inc_backup_set_id)
{
  int ret = OB_SUCCESS;
  ObBackupBaseDataPathInfo path_info;
  ObStorageUtil util(false /*need retry*/);
  ObBackupPath path;
  ObArray<common::ObString> file_names;
  ObArenaAllocator allocator;
  int64_t meta_file_count = 0;
  const bool need_check_completed = false;
  const int64_t fake_compatible = 0;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup data mgr init twice", K(ret));
  } else if (!cluster_backup_dest.is_valid() || OB_INVALID_ID == tenant_id || full_backup_set_id <= 0 ||
             inc_backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup data mgr init get invalid argument",
        K(ret),
        K(cluster_backup_dest),
        K(tenant_id),
        K(full_backup_set_id),
        K(inc_backup_set_id));
  } else {
    path_info.dest_ = cluster_backup_dest;
    path_info.tenant_id_ = tenant_id;
    path_info.full_backup_set_id_ = full_backup_set_id;
    path_info.inc_backup_set_id_ = inc_backup_set_id;

    if (OB_FAIL(ObBackupPathUtil::get_tenant_data_inc_backup_set_path(path_info, path))) {
      STORAGE_LOG(WARN, "failed to get inc backup path", K(ret));
    } else if (OB_FAIL(util.list_files(path.get_obstr(), path_info.dest_.get_storage_info(), allocator, file_names))) {
      STORAGE_LOG(WARN, "list files fail", K(ret), K(path));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < file_names.count(); ++i) {
        ObString& file_name = file_names.at(i);
        if (!(file_name.prefix_match(OB_STRING_BACKUP_META_INDEX))) {
          // filter meta data file
          STORAGE_LOG(INFO, "skip not meta data file", K(file_name));
        } else {
          ++meta_file_count;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (0 == meta_file_count) {
      // do nothing
    } else if (OB_FAIL(meta_index_.init(path_info, fake_compatible, need_check_completed))) {
      LOG_WARN("failed to init meta index", K(ret), K(path_info));
    }

    if (OB_FAIL(ret)) {
    } else {
      cluster_backup_dest_ = cluster_backup_dest;
      full_backup_set_id_ = full_backup_set_id;
      inc_backup_set_id_ = inc_backup_set_id;
      tenant_id_ = tenant_id;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBackupDataMgr::get_base_data_table_id_list(common::ObIArray<int64_t>& table_id_array)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<int64_t> table_id_set;
  const int64_t MAX_BUCKET_NUM = 1024;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data mgr do not init", K(ret));
  } else if (OB_FAIL(table_id_set.create(MAX_BUCKET_NUM))) {
    LOG_WARN("failed to create table id set", K(ret));
  } else {
    const int overwrite_key = 1;
    storage::ObPhyRestoreMetaIndexStore::MetaIndexMap::const_iterator iter;
    const storage::ObPhyRestoreMetaIndexStore::MetaIndexMap& meta_index_map = meta_index_.get_meta_index_map();
    for (iter = meta_index_map.begin(); OB_SUCC(ret) && iter != meta_index_map.end(); ++iter) {
      const ObMetaIndexKey& meta_index_key = iter->first;
      const int64_t table_id = meta_index_key.table_id_;
      if (ObBackupMetaType::PARTITION_GROUP_META != meta_index_key.meta_type_) {
        // do nothing
      } else if (OB_FAIL(table_id_set.set_refactored_1(table_id, overwrite_key))) {
        LOG_WARN("failed to set table id into set", K(ret), K(table_id));
      }
    }

    hash::ObHashSet<int64_t>::iterator table_id_iter;
    for (table_id_iter = table_id_set.begin(); OB_SUCC(ret) && table_id_iter != table_id_set.end(); ++table_id_iter) {
      const int64_t table_id = table_id_iter->first;
      if (OB_FAIL(table_id_array.push_back(table_id))) {
        LOG_WARN("failed to push table id into array", K(ret), K(table_id));
      }
    }
  }
  return ret;
}

int ObBackupDataMgr::get_table_pg_meta_index(
    const int64_t table_id, common::ObIArray<ObBackupMetaIndex>& meta_index_array)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data mgr do not init", K(ret));
  } else if (table_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get table pg meta index get invalid argument", K(ret), K(table_id));
  } else {
    storage::ObPhyRestoreMetaIndexStore::MetaIndexMap::const_iterator iter;
    const storage::ObPhyRestoreMetaIndexStore::MetaIndexMap& meta_index_map = meta_index_.get_meta_index_map();
    for (iter = meta_index_map.begin(); OB_SUCC(ret) && iter != meta_index_map.end(); ++iter) {
      const ObMetaIndexKey& meta_index_key = iter->first;
      const ObBackupMetaIndex& meta_index = iter->second;
      if (ObBackupMetaType::PARTITION_GROUP_META != meta_index_key.meta_type_) {
        // do nothing
      } else if (meta_index_key.table_id_ != table_id) {
        // do nothing
      } else if (OB_FAIL(meta_index_array.push_back(meta_index))) {
        LOG_WARN("failed to push meta index into array", K(ret), K(meta_index));
      }
    }
  }
  return ret;
}

int ObBackupDataMgr::get_pg_meta_index(const ObPartitionKey& pkey, ObBackupMetaIndex& meta_index)
{
  int ret = OB_SUCCESS;
  meta_index.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data mgr do not init", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg meta index get invalid argument", K(ret), K(pkey));
  } else {
    storage::ObPhyRestoreMetaIndexStore::MetaIndexMap::const_iterator iter;
    const storage::ObPhyRestoreMetaIndexStore::MetaIndexMap& meta_index_map = meta_index_.get_meta_index_map();
    ObMetaIndexKey tmp_key;
    tmp_key.meta_type_ = ObBackupMetaType::PARTITION_GROUP_META;
    tmp_key.table_id_ = pkey.get_table_id();
    tmp_key.partition_id_ = pkey.get_partition_id();
    if (OB_FAIL(meta_index_map.get_refactored(tmp_key, meta_index))) {
      LOG_WARN("failed to get meta index", K(ret), K(tmp_key), K(pkey));
    }
  }
  return ret;
}

int ObBackupDataMgr::get_pg_meta(const ObPartitionKey& pkey, ObPartitionGroupMeta& pg_meta)
{
  int ret = OB_SUCCESS;
  ObBackupMetaIndex meta_index;
  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data mgr do not init", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg meta get invalid argument", K(ret), K(pkey));
  } else if (OB_FAIL(get_pg_meta_index(pkey, meta_index))) {
    LOG_WARN("failed to get pg meta index", K(ret), K(pkey));
  } else {
    path_info.dest_ = cluster_backup_dest_;
    path_info.tenant_id_ = tenant_id_;
    path_info.full_backup_set_id_ = full_backup_set_id_;
    path_info.inc_backup_set_id_ = inc_backup_set_id_;
    if (OB_FAIL(ObBackupPathUtil::get_tenant_data_meta_file_path(path_info, meta_index.task_id_, path))) {
      LOG_WARN("fail to get meta file path", K(ret));
    } else if (OB_FAIL(ObRestoreFileUtil::read_partition_group_meta(
                   path.get_ptr(), path_info.dest_.get_storage_info(), meta_index, pg_meta))) {
      LOG_WARN("fail to get partition meta", K(ret), K(path), K(meta_index));
    }
  }
  return ret;
}

ObBackupListDataMgr::ObBackupListDataMgr()
    : is_inited_(false), cluster_backup_dest_(), log_archive_round_(0), tenant_id_(OB_INVALID_ID)
{}

ObBackupListDataMgr::~ObBackupListDataMgr()
{}

int ObBackupListDataMgr::init(
    const share::ObClusterBackupDest& cluster_backup_dest, const int64_t log_archive_round, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup list data mgr init twice", K(ret));
  } else if (!cluster_backup_dest.is_valid() || log_archive_round <= 0 || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init backup list data mgr get invalid argument", K(ret), K(cluster_backup_dest), K(log_archive_round));
  } else {
    cluster_backup_dest_ = cluster_backup_dest;
    log_archive_round_ = log_archive_round;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupListDataMgr::get_clog_pkey_list(common::ObIArray<ObPartitionKey>& pkey_list)
{
  int ret = OB_SUCCESS;
  pkey_list.reset();
  share::ObBackupPath path;
  ObStorageUtil util(false /*need_retry*/);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_clog_archive_key_prefix(
                 cluster_backup_dest_, tenant_id_, log_archive_round_, path))) {
    LOG_WARN("failed to get tenant clog data path", K(ret), K(cluster_backup_dest_));
  } else if (OB_FAIL(util.get_pkeys_from_dir(path.get_ptr(), cluster_backup_dest_.get_storage_info(), pkey_list))) {
    LOG_WARN("failed to get pkeys from dir", K(ret), K(path));
  }

  LOG_INFO("get clog pkey list count", K(pkey_list.count()));
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
