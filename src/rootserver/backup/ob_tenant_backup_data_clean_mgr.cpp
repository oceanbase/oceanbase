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
#include "ob_tenant_backup_data_clean_mgr.h"
#include "archive/ob_archive_log_file_store.h"
#include "archive/ob_archive_clear.h"
#include "rootserver/ob_backup_data_clean.h"

namespace oceanbase {

using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace storage;

namespace rootserver {

ObBackupSetId::ObBackupSetId() : backup_set_id_(0), clean_mode_(MAX)
{}

void ObBackupSetId::reset()
{
  backup_set_id_ = 0;
  clean_mode_ = MAX;
}

bool ObBackupSetId::is_valid() const
{
  return backup_set_id_ > 0 && clean_mode_ >= CLEAN && clean_mode_ < MAX;
}

ObLogArchiveRound::ObLogArchiveRound()
    : log_archive_round_(0), log_archive_status_(ObLogArchiveStatus::INVALID), start_ts_(0), checkpoint_ts_(0)
{}

void ObLogArchiveRound::reset()
{
  log_archive_round_ = 0;
  log_archive_status_ = ObLogArchiveStatus::INVALID;
  start_ts_ = 0;
  checkpoint_ts_ = 0;
}

bool ObLogArchiveRound::is_valid() const
{
  return log_archive_round_ > 0 && log_archive_status_ != ObLogArchiveStatus::INVALID && start_ts_ > 0;
}

bool ObBackupDataCleanElement::is_valid() const
{
  return OB_INVALID_CLUSTER_ID != cluster_id_ && incarnation_ > 0 && backup_dest_.is_valid();
}

void ObBackupDataCleanElement::reset()
{
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  incarnation_ = 0;
  backup_dest_.reset();
  backup_set_id_array_.reset();
}

bool ObBackupDataCleanElement::is_same_element(
    const int64_t cluster_id, const int64_t incarnation, const ObBackupDest& backup_dest) const
{
  return cluster_id_ == cluster_id && incarnation_ == incarnation && backup_dest_ == backup_dest;
}

int ObBackupDataCleanElement::set_backup_set_id(const ObBackupSetId& backup_set_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not set backup id", K(ret), K(*this));
  } else if (!backup_set_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup set id get invalid argument", K(ret), K(backup_set_id));
  } else if (OB_FAIL(backup_set_id_array_.push_back(backup_set_id))) {
    LOG_WARN("failed to set backup set id", K(ret), K(backup_set_id));
  }
  return ret;
}

int ObBackupDataCleanElement::set_log_archive_round(const ObLogArchiveRound& log_archive_round)
{
  int ret = OB_SUCCESS;
  if (!log_archive_round.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set log archive round get invalid argument", K(ret), K(log_archive_round));
  } else if (OB_FAIL(log_archive_round_array_.push_back(log_archive_round))) {
    LOG_WARN("failed to set log archive round", K(ret), K(log_archive_round));
  }
  return ret;
}

bool ObSimpleBackupDataCleanTenant::is_valid() const
{
  return OB_INVALID_ID != tenant_id_;
}

void ObSimpleBackupDataCleanTenant::reset()
{
  tenant_id_ = OB_INVALID_ID;
  is_deleted_ = false;
}

uint64_t ObSimpleBackupDataCleanTenant::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  return hash_val;
}

bool ObBackupDataCleanTenant::is_valid() const
{
  return simple_clean_tenant_.is_valid();
}
void ObBackupDataCleanTenant::reset()
{
  simple_clean_tenant_.reset();
  backup_element_array_.reset();
  clog_data_clean_point_.reset();
  clog_gc_snapshot_ = 0;
}

int ObBackupDataCleanTenant::set_backup_clean_backup_set_id(const int64_t cluster_id, const int64_t incarnation,
    const share::ObBackupDest& backup_dest, const ObBackupSetId backup_set_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup data clean tenant is invalid", K(ret), K(*this));
  } else if (cluster_id < 0 || incarnation < 0 || !backup_dest.is_valid() || !backup_set_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup clean element get invalid argument",
        K(ret),
        K(cluster_id),
        K(incarnation),
        K(backup_dest),
        K(backup_set_id));
  } else {
    bool found_same_elemnt = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_element_array_.count() && !found_same_elemnt; ++i) {
      ObBackupDataCleanElement& backup_element = backup_element_array_.at(i);
      if (backup_element.is_same_element(cluster_id, incarnation, backup_dest)) {
        if (OB_FAIL(backup_element.backup_set_id_array_.push_back(backup_set_id))) {
          LOG_WARN("failed to push backup set id into array", K(ret), K(backup_set_id));
        } else {
          found_same_elemnt = true;
        }
      }
    }

    if (OB_SUCC(ret) && !found_same_elemnt) {
      ObBackupDataCleanElement backup_element;
      backup_element.backup_dest_ = backup_dest;
      backup_element.cluster_id_ = cluster_id;
      backup_element.incarnation_ = incarnation;
      if (OB_FAIL(backup_element.backup_set_id_array_.push_back(backup_set_id))) {
        LOG_WARN("failed to push backup set id into array", K(ret), K(backup_set_id));
      } else if (OB_FAIL(backup_element_array_.push_back(backup_element))) {
        LOG_WARN("failed to push backup element", K(ret), K(backup_element));
      }
    }
  }
  return ret;
}

int ObBackupDataCleanTenant::set_backup_clean_archive_round(const int64_t cluster_id, const int64_t incarnation,
    const share::ObBackupDest& backup_dest, const ObLogArchiveRound& archive_round)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup data clean tenant is invalid", K(ret), K(*this));
  } else if (cluster_id < 0 || incarnation < 0 || !backup_dest.is_valid() || !archive_round.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup clean element get invalid argument",
        K(ret),
        K(cluster_id),
        K(incarnation),
        K(backup_dest),
        K(archive_round));
  } else {
    bool found_same_elemnt = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_element_array_.count() && !found_same_elemnt; ++i) {
      ObBackupDataCleanElement& backup_element = backup_element_array_.at(i);
      if (backup_element.is_same_element(cluster_id, incarnation, backup_dest)) {
        if (OB_FAIL(backup_element.log_archive_round_array_.push_back(archive_round))) {
          LOG_WARN("failed to push backup set id into array", K(ret), K(archive_round));
        } else {
          found_same_elemnt = true;
        }
      }
    }

    if (OB_SUCC(ret) && !found_same_elemnt) {
      ObBackupDataCleanElement backup_element;
      backup_element.backup_dest_ = backup_dest;
      backup_element.cluster_id_ = cluster_id;
      backup_element.incarnation_ = incarnation;
      if (OB_FAIL(backup_element.log_archive_round_array_.push_back(archive_round))) {
        LOG_WARN("failed to push backup set id into array", K(ret), K(archive_round));
      } else if (OB_FAIL(backup_element_array_.push_back(backup_element))) {
        LOG_WARN("failed to push backup element", K(ret), K(backup_element));
      }
    }
  }
  return ret;
}

bool ObBackupDataCleanTenant::has_clean_backup_set(const int64_t backup_set_id) const
{
  bool b_ret = false;
  for (int64_t i = 0; i < backup_element_array_.count(); ++i) {
    const ObBackupDataCleanElement& backup_element = backup_element_array_.at(i);
    for (int64_t j = 0; j < backup_element.backup_set_id_array_.count(); ++j) {
      const int64_t tmp_backup_set_id = backup_element.backup_set_id_array_.at(j).backup_set_id_;
      if (tmp_backup_set_id == backup_set_id) {
        b_ret = true;
        break;
      }
    }
  }
  return b_ret;
}

ObBackupDataCleanStatics::ObBackupDataCleanStatics()
    : touched_base_data_files_(0),
      deleted_base_data_files_(0),
      touched_clog_files_(0),
      deleted_clog_files_(0),
      touched_base_data_files_ts_(0),
      deleted_base_data_files_ts_(0),
      touched_clog_files_ts_(0),
      deleted_clog_files_ts_(0)
{}

void ObBackupDataCleanStatics::reset()
{
  touched_base_data_files_ = 0;
  deleted_base_data_files_ = 0;
  touched_clog_files_ = 0;
  deleted_clog_files_ = 0;
  touched_base_data_files_ts_ = 0;
  deleted_base_data_files_ts_ = 0;
  touched_clog_files_ts_ = 0;
  deleted_clog_files_ts_ = 0;
}

ObBackupDataCleanStatics& ObBackupDataCleanStatics::operator+=(const ObBackupDataCleanStatics& clean_statics)
{
  touched_base_data_files_ += clean_statics.touched_base_data_files_;
  deleted_base_data_files_ += clean_statics.deleted_base_data_files_;
  touched_clog_files_ += clean_statics.touched_clog_files_;
  deleted_clog_files_ += clean_statics.deleted_clog_files_;
  touched_base_data_files_ts_ += clean_statics.touched_base_data_files_ts_;
  deleted_base_data_files_ts_ += clean_statics.deleted_base_data_files_ts_;
  touched_clog_files_ts_ += clean_statics.touched_clog_files_ts_;
  deleted_clog_files_ts_ += clean_statics.deleted_clog_files_ts_;
  return *this;
}

int ObBackupDataCleanUtil::get_backup_path_info(const ObBackupDest& backup_dest, const int64_t incarnation,
    const uint64_t tenant_id, const int64_t full_backup_set_id, const int64_t inc_backup_set_id,
    ObBackupBaseDataPathInfo& path_info)
{
  int ret = OB_SUCCESS;
  if (!backup_dest.is_valid() || incarnation <= 0 || OB_INVALID_ID == tenant_id || full_backup_set_id <= 0 ||
      inc_backup_set_id <= 0 || full_backup_set_id > inc_backup_set_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup path info get invalid argument",
        K(ret),
        K(backup_dest),
        K(incarnation),
        K(tenant_id),
        K(full_backup_set_id),
        K(inc_backup_set_id));
  } else if (OB_FAIL(path_info.dest_.set(backup_dest, incarnation))) {
    LOG_WARN("failed to set path info", K(ret), K(backup_dest));
  } else {
    path_info.full_backup_set_id_ = full_backup_set_id;
    path_info.inc_backup_set_id_ = inc_backup_set_id;
    path_info.tenant_id_ = tenant_id;
  }
  return ret;
}

int ObBackupDataCleanUtil::touch_backup_dir_files(const ObBackupPath& path, const char* storage_info,
    const common::ObStorageType& device_type, ObBackupDataCleanStatics& clean_statics,
    share::ObIBackupLeaseService& lease_service)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::BACKUP);
  ObArray<common::ObString> index_file_names;
  ObStorageUtil util(false /*need retry*/);
  bool can_touch = true;
  const int64_t start_ts = ObTimeUtil::current_time();

  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("touch backup file get invalid argument", K(ret), K(path), KP(storage_info));
  } else if (OB_FAIL(check_can_touch(device_type, can_touch))) {
    LOG_WARN("failed to check can touch file", K(ret), K(device_type), K(path));
  } else if (!can_touch) {
    // do nothing
  } else if (OB_FAIL(util.list_files(path.get_ptr(), storage_info, allocator, index_file_names))) {
    LOG_WARN("failed to list files", K(ret), K(path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_file_names.count(); ++i) {
      const ObString& file_name = index_file_names.at(i);
      ObBackupPath tmp_path = path;
      if (OB_FAIL(lease_service.check_lease())) {
        LOG_WARN("failed to check lease", K(ret));
      } else if (OB_FAIL(tmp_path.join(file_name))) {
        LOG_WARN("failed to join file name", K(ret), K(file_name));
      } else if (OB_FAIL(util.update_file_modify_time(tmp_path.get_ptr(), storage_info))) {
        LOG_WARN("failed to update file modify time", K(ret), K(tmp_path), K(storage_info));
      }
    }
    clean_statics.touched_base_data_files_ += index_file_names.count();
    clean_statics.touched_base_data_files_ts_ += ObTimeUtil::current_time() - start_ts;
  }
  return ret;
}

int ObBackupDataCleanUtil::delete_backup_dir_files(const ObBackupPath& path, const char* storage_info,
    const common::ObStorageType& device_type, ObBackupDataCleanStatics& clean_statics,
    share::ObIBackupLeaseService& lease_service)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::BACKUP);
  ObArray<common::ObString> index_file_names;
  ObStorageUtil util(false /*need retry*/);
  bool can_delete = true;
  const int64_t start_ts = ObTimeUtil::current_time();
  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("touch backup file get invalid argument", K(ret), K(path), KP(storage_info));
  } else if (OB_FAIL(check_can_delete(device_type, can_delete))) {
    LOG_WARN("failed to check can delete", K(ret), K(device_type), K(path));
  } else if (!can_delete) {
    // do nothing
  } else if (OB_FAIL(util.list_files(path.get_ptr(), storage_info, allocator, index_file_names))) {
    LOG_WARN("failed to list files", K(ret), K(path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_file_names.count(); ++i) {
      const ObString& file_name = index_file_names.at(i);
      ObBackupPath tmp_path = path;
      if (OB_FAIL(lease_service.check_lease())) {
        LOG_WARN("failed to check lease", K(ret));
      } else if (OB_FAIL(tmp_path.join(file_name))) {
        LOG_WARN("failed to join file name", K(ret), K(file_name));
      } else if (OB_FAIL(util.del_file(tmp_path.get_ptr(), storage_info))) {
        LOG_WARN("failed to update file modify time", K(ret), K(tmp_path), K(storage_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(delete_backup_dir(path, storage_info, device_type))) {
        LOG_WARN("failed to delete backup dir", K(ret), K(path), K(storage_info));
      }
    }
    clean_statics.deleted_base_data_files_ += index_file_names.count();
    clean_statics.deleted_base_data_files_ts_ += ObTimeUtil::current_time() - start_ts;
  }
  return ret;
}

int ObBackupDataCleanUtil::touch_clog_dir_files(const ObBackupPath& path, const char* storage_info,
    const uint64_t file_id, const common::ObStorageType& device_type, ObBackupDataCleanStatics& clean_statics,
    share::ObIBackupLeaseService& lease_service)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::BACKUP);
  ObArray<common::ObString> index_file_names;
  ObStorageUtil util(false /*need retry*/);
  bool can_touch = true;
  const int64_t start_ts = ObTimeUtil::current_time();
  int64_t touched_file_num = 0;
  int64_t tmp_file_id = 0;

  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("touch backup file get invalid argument", K(ret), K(path), KP(storage_info), K(file_id));
  } else if (OB_FAIL(check_can_touch(device_type, can_touch))) {
    LOG_WARN("failed to check can touch", K(ret), K(device_type), K(path));
  } else if (!can_touch) {
    // do nothing
  } else if (OB_FAIL(util.list_files(path.get_ptr(), storage_info, allocator, index_file_names))) {
    LOG_WARN("failed to list files", K(ret), K(path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_file_names.count(); ++i) {
      const ObString& file_name = index_file_names.at(i);
      tmp_file_id = 0;
      if (OB_FAIL(lease_service.check_lease())) {
        LOG_WARN("failed to check lease", K(ret));
      } else if (OB_FAIL(get_file_id(file_name, tmp_file_id))) {
        LOG_WARN("failed to get file id", K(ret), K(file_name));
      } else if (tmp_file_id <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get file id", K(ret), K(file_name), K(tmp_file_id));
      } else if (tmp_file_id <= file_id) {
        // do nothing
      } else {
        ObBackupPath tmp_path = path;
        if (OB_FAIL(tmp_path.join(file_name))) {
          LOG_WARN("failed to join file name", K(ret), K(file_name));
        } else if (OB_FAIL(util.update_file_modify_time(tmp_path.get_ptr(), storage_info))) {
          LOG_WARN("failed to update file modify time", K(ret), K(tmp_path), K(storage_info));
        } else {
          ++touched_file_num;
        }
      }
    }
    clean_statics.touched_clog_files_ += touched_file_num;
    clean_statics.touched_clog_files_ts_ += ObTimeUtil::current_time() - start_ts;
  }
  return ret;
}

int ObBackupDataCleanUtil::delete_clog_dir_files(const ObBackupPath& path, const char* storage_info,
    const uint64_t file_id, const common::ObStorageType& device_type, ObBackupDataCleanStatics& clean_statics,
    share::ObIBackupLeaseService& lease_service)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::BACKUP);
  ObArray<common::ObString> index_file_names;
  ObStorageUtil util(false /*need retry*/);
  bool can_delete = true;
  const int64_t start_ts = ObTimeUtil::current_time();
  int64_t deleted_file_num = 0;
  int64_t tmp_file_id = 0;

  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("touch backup file get invalid argument", K(ret), K(path), KP(storage_info), K(file_id));
  } else if (OB_FAIL(check_can_delete(device_type, can_delete))) {
    LOG_WARN("failed to check can delete", K(ret), K(device_type));
  } else if (!can_delete) {
    // do nothing
  } else if (OB_FAIL(util.list_files(path.get_ptr(), storage_info, allocator, index_file_names))) {
    LOG_WARN("failed to list files", K(ret), K(path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_file_names.count(); ++i) {
      const ObString& file_name = index_file_names.at(i);
      tmp_file_id = 0;
      if (OB_FAIL(lease_service.check_lease())) {
        LOG_WARN("failed to check lease", K(ret));
      } else if (OB_FAIL(get_file_id(file_name, tmp_file_id))) {
        LOG_WARN("failed to get file id", K(ret), K(file_name));
      } else if (tmp_file_id <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get file id", K(ret), K(file_name), K(tmp_file_id));
      } else if (tmp_file_id > file_id) {
        LOG_INFO("no need delete clog file, skip it", K(tmp_file_id), K(file_id));
      } else {
        ObBackupPath tmp_path = path;
        if (OB_FAIL(tmp_path.join(file_name))) {
          LOG_WARN("failed to join file name", K(ret), K(file_name));
        } else if (OB_FAIL(util.del_file(tmp_path.get_ptr(), storage_info))) {
          LOG_WARN("failed to update file modify time", K(ret), K(tmp_path), K(storage_info));
        } else {
          ++deleted_file_num;
        }
      }
    }

    clean_statics.deleted_clog_files_ += deleted_file_num;
    clean_statics.deleted_clog_files_ts_ += ObTimeUtil::current_time() - start_ts;
  }
  return ret;
}

int ObBackupDataCleanUtil::delete_backup_dir(
    const ObBackupPath& path, const char* storage_info, const common::ObStorageType& device_type)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::BACKUP);
  ObStorageUtil util(false /*need retry*/);
  bool can_delete = true;
  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("touch backup file get invalid argument", K(ret), K(path), KP(storage_info));
  } else if (OB_FAIL(check_can_delete(device_type, can_delete))) {
    LOG_WARN("failed to check can delete", K(ret), K(device_type));
  } else if (!can_delete) {
    // do nothing
  } else if (OB_FAIL(util.del_dir(path.get_ptr(), storage_info))) {
    LOG_WARN("failed to del dir time", K(ret), K(path), K(storage_info));
  } else {
    LOG_INFO("delete backup dir", K(path));
  }
  return ret;
}

int ObBackupDataCleanUtil::check_can_delete(const common::ObStorageType& device_type, bool& can_delete)
{
  int ret = OB_SUCCESS;
  can_delete = true;
  if (ObStorageType::OB_STORAGE_OSS == device_type) {
    can_delete = false;
#ifdef ERRSIM
    can_delete = ObServerConfig::get_instance().mock_oss_delete;
#endif
  } else if (ObStorageType::OB_STORAGE_FILE == device_type) {
    can_delete = true;
  } else if (ObStorageType::OB_STORAGE_COS == device_type) {
    can_delete = true;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("new device type not support delete", K(ret), K(device_type));
  }
  return ret;
}

int ObBackupDataCleanUtil::check_can_touch(const common::ObStorageType& device_type, bool& can_touch)
{
  int ret = OB_SUCCESS;
  can_touch = true;
  if (ObStorageType::OB_STORAGE_OSS == device_type) {
    can_touch = true;
  } else if (ObStorageType::OB_STORAGE_FILE == device_type) {
    can_touch = false;

#ifdef ERRSIM
    can_touch = ObServerConfig::get_instance().mock_nfs_device_touch;
#endif
  } else if (ObStorageType::OB_STORAGE_COS == device_type) {
    can_touch = false;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("new device type not support touch", K(ret), K(device_type));
  }
  return ret;
}

int ObBackupDataCleanUtil::delete_backup_file(
    const ObBackupPath& path, const char* storage_info, const common::ObStorageType& device_type)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false /*need_retry*/);
  bool can_delete = true;
  bool is_exist = true;
  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("touch backup file get invalid argument", K(ret), K(path), KP(storage_info));
  } else if (OB_FAIL(check_can_delete(device_type, can_delete))) {
    LOG_WARN("failed to check can delete", K(ret), K(device_type), K(path));
  } else if (!can_delete) {
    // do nothing
  } else if (OB_FAIL(util.is_exist(path.get_ptr(), storage_info, is_exist))) {
    LOG_WARN("failed to check path is exist", K(ret), K(path), K(storage_info), K(is_exist));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(util.del_file(path.get_ptr(), storage_info))) {
    LOG_WARN("failed to update file modify time", K(ret), K(path), K(storage_info));
  }
  return ret;
}

int ObBackupDataCleanUtil::touch_backup_file(
    const ObBackupPath& path, const char* storage_info, const common::ObStorageType& device_type)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false /*need_retry*/);
  bool can_delete = true;
  bool is_exist = true;
  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("touch backup file get invalid argument", K(ret), K(path), KP(storage_info));
  } else if (OB_FAIL(check_can_delete(device_type, can_delete))) {
    LOG_WARN("failed to check can delete", K(ret), K(device_type), K(path));
  } else if (!can_delete) {
    // do nothing
  } else if (OB_FAIL(util.is_exist(path.get_ptr(), storage_info, is_exist))) {
    LOG_WARN("failed to check path is exist", K(ret), K(path), K(storage_info), K(is_exist));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(util.update_file_modify_time(path.get_ptr(), storage_info))) {
    LOG_WARN("failed to update file modify time", K(ret), K(path), K(storage_info));
  }
  return ret;
}

int ObBackupDataCleanUtil::get_file_id(const ObString& file_name, int64_t& file_id)
{
  int ret = OB_SUCCESS;
  file_id = 0 == file_name.length() ? -1 : 0;
  // check num
  for (int i = 0; OB_SUCC(ret) && i < file_name.length(); ++i) {
    if (file_name[i] < '0' || file_name[i] > '9') {
      file_id = -1;
      break;
    }
    file_id = file_id * 10 + file_name[i] - '0';
  }
  return ret;
}

int ObBackupDataCleanUtil::delete_tmp_files(const ObBackupPath& path, const char* storage_info)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false /*need_retry*/);
  bool is_exist = true;
  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete tmp files get invalid argument", K(ret), K(path), KP(storage_info));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(util.delete_tmp_files(path.get_ptr(), storage_info))) {
    LOG_WARN("failed to delete tmp files", K(ret), K(path), K(storage_info));
  }
  return ret;
}

ObTenantBackupDataCleanMgr::ObTenantBackupDataCleanMgr() : is_inited_(false), clean_tenant_(), data_clean_(NULL)
{}

ObTenantBackupDataCleanMgr::~ObTenantBackupDataCleanMgr()
{}

int ObTenantBackupDataCleanMgr::init(const ObBackupDataCleanTenant& clean_tenant, ObBackupDataClean* data_clean)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant backup data clean mgr init twice", K(ret));
  } else if (!clean_tenant.is_valid() || OB_ISNULL(data_clean)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant backup data clean mgr get invalid argument", K(ret), K(clean_tenant), KP(data_clean));
  } else {
    clean_tenant_ = clean_tenant;
    data_clean_ = data_clean;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantBackupDataCleanMgr::do_clean()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();
  ObTenantBackupBaseDataCleanTask base_data_clean_task;
  ObTenantBackupClogDataCleanTask clog_data_clean_task;
  ObBackupDataCleanStatics clean_statics;
  ObBackupDataCleanStatics base_data_clean_statics;
  ObBackupDataCleanStatics clog_clean_statics;

  LOG_INFO("tenant backup data clean mgr do clean", K(clean_tenant_.simple_clean_tenant_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup data clean mgr do not init", K(ret));
  } else if (OB_FAIL(data_clean_->check_can_do_task())) {
    LOG_WARN("failed to check can do task", K(ret));
  } else if (OB_FAIL(clog_data_clean_task.init(clean_tenant_, data_clean_))) {
    LOG_WARN("failed to init clog data clean task", K(ret), K(clean_tenant_));
  } else if (OB_FAIL(clog_data_clean_task.do_clean())) {
    LOG_WARN("failed to do clean clog data", K(ret));
  } else if (OB_FAIL(clog_data_clean_task.get_clean_statics(clog_clean_statics))) {
    LOG_WARN("failed to get clog data clean statics", K(ret));
  } else if (OB_FAIL(base_data_clean_task.init(clean_tenant_, data_clean_))) {
    LOG_WARN("failed to init base data clean task", K(ret), K(clean_tenant_));
  } else if (OB_FAIL(base_data_clean_task.do_clean())) {
    LOG_WARN("failed to do base data clean", K(ret));
  } else if (OB_FAIL(base_data_clean_task.get_clean_statics(base_data_clean_statics))) {
    LOG_WARN("failed to get base data clean statics", K(ret));
  } else {
    clean_statics += clog_clean_statics;
    clean_statics += base_data_clean_statics;
  }

  LOG_INFO("finish tenant backup data clean mgr do clean",
      "cost_ts",
      ObTimeUtil::current_time() - start_ts,
      K(ret),
      K(clean_tenant_.simple_clean_tenant_),
      K(clean_statics));

  return ret;
}

ObTenantBackupBaseDataCleanTask::ObTenantBackupBaseDataCleanTask()
    : is_inited_(false), clean_tenant_(), clean_statics_(), data_clean_(NULL)
{}

ObTenantBackupBaseDataCleanTask::~ObTenantBackupBaseDataCleanTask()
{}

int ObTenantBackupBaseDataCleanTask::init(const ObBackupDataCleanTenant& clean_tenant, ObBackupDataClean* data_clean)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant backup base data clean task init twice", K(ret));
  } else if (!clean_tenant.is_valid() || OB_ISNULL(data_clean)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant backup base data clean task get invalid argument", K(ret), K(clean_tenant), KP(data_clean));
  } else {
    clean_tenant_ = clean_tenant;
    data_clean_ = data_clean;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::do_clean()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();
  LOG_INFO("start tenant backup base data clean task", K(clean_tenant_.simple_clean_tenant_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_tenant_.backup_element_array_.count(); ++i) {
      const ObSimpleBackupDataCleanTenant& simple_clean_tenant = clean_tenant_.simple_clean_tenant_;
      const ObBackupDataCleanElement& backup_clean_element = clean_tenant_.backup_element_array_.at(i);
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(do_inner_clean(simple_clean_tenant, backup_clean_element))) {
        LOG_WARN("failed to do inner clean", K(ret), K(simple_clean_tenant), K(backup_clean_element));
      }
    }
  }

  LOG_INFO("finish tenant backup base data clean task",
      "cost",
      ObTimeUtil::current_time() - start_ts,
      K(clean_tenant_.simple_clean_tenant_));

  return ret;
}

int ObTenantBackupBaseDataCleanTask::do_inner_clean(
    const ObSimpleBackupDataCleanTenant& simple_clean_tenant, const ObBackupDataCleanElement& clean_element)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (!simple_clean_tenant.is_valid() || !clean_element.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner do clean get invalid argument", K(ret), K(simple_clean_tenant), K(clean_element));
  } else {
    // should be low backup set to high backup set
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_element.backup_set_id_array_.count(); ++i) {
      const ObBackupSetId& backup_set_id = clean_element.backup_set_id_array_.at(i);
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(clean_backup_data(simple_clean_tenant, clean_element, backup_set_id))) {
        LOG_WARN("failed to clean backup data", K(ret), K(simple_clean_tenant), K(clean_element), K(backup_set_id));
      } else if (OB_FAIL(try_clean_backup_set_dir(simple_clean_tenant.tenant_id_, clean_element, backup_set_id))) {
        LOG_WARN("failed to try clean backup set dir", K(ret), K(backup_set_id), K(simple_clean_tenant));
      }
    }
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::clean_backup_data(const ObSimpleBackupDataCleanTenant& simple_clean_tenant,
    const ObBackupDataCleanElement& clean_element, const ObBackupSetId& backup_set_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObExternBackupSetInfo> extern_backup_set_infos;
  storage::ObPhyRestoreMetaIndexStore meta_index;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (OB_FAIL(get_tenant_backup_set_infos(
                 simple_clean_tenant, clean_element, backup_set_id, extern_backup_set_infos))) {
    LOG_WARN("failed to get tenant max succeed backup set info",
        K(ret),
        K(backup_set_id),
        K(simple_clean_tenant),
        K(clean_element));
  } else {
    LOG_INFO("get extern backup set infos", K(extern_backup_set_infos), K(backup_set_id));

    for (int64_t i = 0; OB_SUCC(ret) && i < extern_backup_set_infos.count(); ++i) {
      const ObExternBackupSetInfo& backup_set_info = extern_backup_set_infos.at(i);
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(clean_backp_set(simple_clean_tenant, clean_element, backup_set_id, backup_set_info))) {
        LOG_WARN("failed to clean backup set", K(ret), K(simple_clean_tenant), K(clean_element), K(backup_set_info));
      }
    }
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::clean_backp_set(const ObSimpleBackupDataCleanTenant& simple_clean_tenant,
    const ObBackupDataCleanElement& clean_element, const ObBackupSetId& backup_set_id,
    const ObExternBackupSetInfo& extern_backup_set_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupMetaIndex> meta_index_array;
  const int64_t full_backup_set_id = backup_set_id.backup_set_id_;
  const int64_t inc_backup_set_id = extern_backup_set_info.backup_set_id_;
  const uint64_t tenant_id = simple_clean_tenant.tenant_id_;
  ObBackupDataMgr backup_data_mgr;
  ObClusterBackupDest cluster_backup_dest;
  ObArray<int64_t> table_id_array;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (!simple_clean_tenant.is_valid() || !clean_element.is_valid() || !backup_set_id.is_valid() ||
             !extern_backup_set_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("clean backup dest get invalid argument",
        K(ret),
        K(simple_clean_tenant),
        K(clean_element),
        K(backup_set_id),
        K(extern_backup_set_info));
  } else if (OB_FAIL(cluster_backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set cluster backup destt", K(ret), K(clean_element));
  } else if (OB_FAIL(backup_data_mgr.init(cluster_backup_dest, tenant_id, full_backup_set_id, inc_backup_set_id))) {
    LOG_WARN(
        "failed to init backup data mgr", K(ret), K(cluster_backup_dest), K(full_backup_set_id), K(inc_backup_set_id));
  } else if (OB_FAIL(backup_data_mgr.get_base_data_table_id_list(table_id_array))) {
    LOG_WARN("failed ot get base data table id list", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_id_array.count(); ++i) {
      meta_index_array.reset();
      const int64_t table_id = table_id_array.at(i);
      ObTableBaseDataCleanMgr table_data_clean_mgr;
      ObBackupDataCleanStatics clean_statics;
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(backup_data_mgr.get_table_pg_meta_index(table_id, meta_index_array))) {
        LOG_WARN("failed to get table meta index", K(ret), K(table_id));
      } else if (OB_FAIL(table_data_clean_mgr.init(
                     table_id, clean_element, backup_set_id, inc_backup_set_id, meta_index_array, *data_clean_))) {
        LOG_WARN("failed to init table data clean mgr", K(ret));
      } else if (OB_FAIL(table_data_clean_mgr.do_clean())) {
        LOG_WARN("failed to do table data clean", K(ret));
      } else if (OB_FAIL(table_data_clean_mgr.get_clean_statics(clean_statics))) {
        LOG_WARN("failed to get clean statics", K(ret));
      } else {
        clean_statics_ += clean_statics;
        LOG_INFO("clean table base data statics",
            K(table_id),
            K(cluster_backup_dest),
            K(full_backup_set_id),
            K(clean_statics));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(clean_backup_set_meta(simple_clean_tenant, clean_element, backup_set_id, extern_backup_set_info))) {
        LOG_WARN("failed to clean backup set meta",
            K(ret),
            K(simple_clean_tenant),
            K(clean_element),
            K(backup_set_id),
            K(extern_backup_set_info));
      }
    }
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::get_tenant_backup_set_infos(
    const ObSimpleBackupDataCleanTenant& simple_clean_tenant, const ObBackupDataCleanElement& clean_element,
    const ObBackupSetId& backup_set_id, common::ObIArray<ObExternBackupSetInfo>& extern_backup_set_infos)
{
  int ret = OB_SUCCESS;
  ObExternBackupSetInfoMgr extern_backup_set_info_mgr;
  ObClusterBackupDest backup_dest;
  const uint64_t tenant_id = simple_clean_tenant.tenant_id_;
  const int64_t full_backup_set_id = backup_set_id.backup_set_id_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (OB_FAIL(backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(clean_element));
  } else if (OB_FAIL(extern_backup_set_info_mgr.init(
                 tenant_id, full_backup_set_id, backup_dest, *data_clean_->get_backup_lease_service()))) {
    LOG_WARN("failed to init extern backup set info mgr", K(ret), K(tenant_id), K(full_backup_set_id));
  } else if (OB_FAIL(extern_backup_set_info_mgr.get_extern_backup_set_infos(extern_backup_set_infos))) {
    LOG_WARN("failed to get extern backup set infos", K(ret), K(tenant_id), K(full_backup_set_id), K(backup_dest));
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::clean_backup_set_meta(const ObSimpleBackupDataCleanTenant& simple_clean_tenant,
    const ObBackupDataCleanElement& clean_element, const ObBackupSetId& backup_set_id,
    const ObExternBackupSetInfo& extern_backup_set_info)
{
  int ret = OB_SUCCESS;
  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;
  const int64_t full_backup_set_id = backup_set_id.backup_set_id_;
  const int64_t inc_backup_set_id = extern_backup_set_info.backup_set_id_;
  const uint64_t tenant_id = simple_clean_tenant.tenant_id_;
  const ObBackupDest& backup_dest = clean_element.backup_dest_;
  const int64_t incarnation = clean_element.incarnation_;
  const char* storage_info = clean_element.backup_dest_.storage_info_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::get_backup_path_info(
                 backup_dest, incarnation, tenant_id, full_backup_set_id, inc_backup_set_id, path_info))) {
    LOG_WARN("failed to get backup path info", K(ret), K(clean_element), K(simple_clean_tenant));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_inc_backup_set_path(path_info, path))) {
    STORAGE_LOG(WARN, "failed to get inc backup path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_tmp_files(path, storage_info))) {
    STORAGE_LOG(WARN, "failed to delete tmp files", K(ret), K(path));
  } else if (ObBackupDataCleanMode::TOUCH == backup_set_id.clean_mode_) {
    if (OB_FAIL(touch_backup_set_meta(clean_element, path))) {
      LOG_WARN("failed to touch backup set meta", K(ret), K(simple_clean_tenant), K(backup_set_id));
    }
  } else if (ObBackupDataCleanMode::CLEAN == backup_set_id.clean_mode_) {
    if (OB_FAIL(delete_backup_set_meta(clean_element, path))) {
      LOG_WARN("failed to delete backup set meta", K(ret), K(simple_clean_tenant), K(backup_set_id));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("clean mode is invalid", K(ret), K(backup_set_id));
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::touch_backup_set_meta(
    const ObBackupDataCleanElement& clean_element, const ObBackupPath& path)
{
  int ret = OB_SUCCESS;
  const char* storage_info = clean_element.backup_dest_.storage_info_;
  const ObStorageType device_type = clean_element.backup_dest_.device_type_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::touch_backup_dir_files(
                 path, storage_info, device_type, clean_statics_, *data_clean_->get_backup_lease_service()))) {
    LOG_WARN("failed to touch backup file", K(ret), K(path));
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::delete_backup_set_meta(
    const ObBackupDataCleanElement& clean_element, const ObBackupPath& path)
{
  int ret = OB_SUCCESS;
  const char* storage_info = clean_element.backup_dest_.storage_info_;
  const ObStorageType device_type = clean_element.backup_dest_.device_type_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir_files(
                 path, storage_info, device_type, clean_statics_, *data_clean_->get_backup_lease_service()))) {
    LOG_WARN("failed to touch backup file", K(ret), K(path));
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::get_table_id_list(
    const storage::ObPhyRestoreMetaIndexStore::MetaIndexMap& meta_index_map, hash::ObHashSet<int64_t>& table_id_set)
{
  int ret = OB_SUCCESS;
  const int overwrite_key = 1;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else {
    storage::ObPhyRestoreMetaIndexStore::MetaIndexMap::const_iterator iter;
    for (iter = meta_index_map.begin(); OB_SUCC(ret) && iter != meta_index_map.end(); ++iter) {
      const ObMetaIndexKey& meta_index_key = iter->first;
      const int64_t table_id = meta_index_key.table_id_;
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (ObBackupMetaType::PARTITION_GROUP_META != meta_index_key.meta_type_) {
        // do nothing
      } else if (OB_FAIL(table_id_set.set_refactored_1(table_id, overwrite_key))) {
        LOG_WARN("failed to set table id into set", K(ret), K(table_id));
      }
    }
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::try_clean_backup_set_dir(
    const uint64_t tenant_id, const ObBackupDataCleanElement& clean_element, const ObBackupSetId& backup_set_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (!backup_set_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("try clean backup set dir get invalid argument", K(ret), K(backup_set_id));
  } else if (OB_FAIL(try_clean_backup_set_info(tenant_id, clean_element, backup_set_id))) {
    LOG_WARN("failed to try clean backup set info", K(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(try_clean_backup_set_data_dir(tenant_id, clean_element, backup_set_id))) {
    LOG_WARN("failed to try clean backup set data dir", K(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(try_clean_full_backup_set_dir(tenant_id, clean_element, backup_set_id))) {
    LOG_WARN("failed to try clean full backup set dir", K(ret), K(tenant_id), K(backup_set_id));
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::try_clean_backup_set_data_dir(
    const uint64_t tenant_id, const ObBackupDataCleanElement& clean_element, const ObBackupSetId& backup_set_id)
{
  int ret = OB_SUCCESS;
  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;
  const int64_t full_backup_set_id = backup_set_id.backup_set_id_;
  const int64_t inc_backup_set_id = backup_set_id.backup_set_id_;
  const ObBackupDest& backup_dest = clean_element.backup_dest_;
  const int64_t incarnation = clean_element.incarnation_;
  const char* storage_info = clean_element.backup_dest_.storage_info_;
  const ObStorageType& device_type = clean_element.backup_dest_.device_type_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (ObBackupDataCleanMode::TOUCH == backup_set_id.clean_mode_) {
    // do nothing
  } else if (OB_FAIL(ObBackupDataCleanUtil::get_backup_path_info(
                 backup_dest, incarnation, tenant_id, full_backup_set_id, inc_backup_set_id, path_info))) {
    LOG_WARN("failed to get backup path info", K(ret), K(clean_element), K(tenant_id));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_full_backup_set_path(path_info, path))) {
    LOG_WARN("failed to get tenant data full backup set path", K(ret), K(path_info));
  } else if (OB_FAIL(path.join(OB_STRING_BACKUP_DATA))) {
    LOG_WARN("failed to join backup path", K(ret), K(path), K(path_info));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(path), K(storage_info));
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::try_clean_backup_set_info(
    const uint64_t tenant_id, const ObBackupDataCleanElement& clean_element, const ObBackupSetId& backup_set_id)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest backup_dest;
  ObExternBackupSetInfoMgr extern_backup_set_info_mgr;
  const int64_t full_backup_set_id = backup_set_id.backup_set_id_;
  ObBackupPath prefix_path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (OB_FAIL(backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(clean_element));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_backup_data_path(backup_dest, tenant_id, prefix_path))) {
    LOG_WARN("failed to get tenant backup data path", K(ret), K(backup_dest), K(tenant_id));
  } else if (OB_FAIL(prefix_path.join_full_backup_set(full_backup_set_id))) {
    LOG_WARN("failed to join full backup set id", K(ret), K(full_backup_set_id));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_tmp_files(prefix_path, backup_dest.get_storage_info()))) {
    LOG_WARN("failed to delete tmp files", K(ret), K(prefix_path), K(backup_dest));
  } else if (OB_FAIL(extern_backup_set_info_mgr.init(
                 tenant_id, full_backup_set_id, backup_dest, *data_clean_->get_backup_lease_service()))) {
    LOG_WARN("failed to init extern backup set info mgr", K(ret), K(backup_dest), K(full_backup_set_id));
  } else if (ObBackupDataCleanMode::TOUCH == backup_set_id.clean_mode_) {
    if (OB_FAIL(extern_backup_set_info_mgr.touch_extern_backup_set_info())) {
      LOG_WARN("failed to touch backup file", K(ret), K(backup_dest));
    }
  } else if (ObBackupDataCleanMode::CLEAN == backup_set_id.clean_mode_) {
    if (OB_FAIL(extern_backup_set_info_mgr.delete_extern_backup_set_info())) {
      LOG_WARN("failed to delete backup file", K(ret), K(backup_dest));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("try clean backup set info's clean mode is not supported", K(ret), K(backup_set_id));
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::try_clean_full_backup_set_dir(
    const uint64_t tenant_id, const ObBackupDataCleanElement& clean_element, const ObBackupSetId& backup_set_id)
{
  int ret = OB_SUCCESS;
  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;
  const int64_t full_backup_set_id = backup_set_id.backup_set_id_;
  const int64_t inc_backup_set_id = backup_set_id.backup_set_id_;
  const ObBackupDest& backup_dest = clean_element.backup_dest_;
  const int64_t incarnation = clean_element.incarnation_;
  const char* storage_info = clean_element.backup_dest_.storage_info_;
  const ObStorageType& device_type = clean_element.backup_dest_.device_type_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (ObBackupDataCleanMode::TOUCH == backup_set_id.clean_mode_) {
    // do nothing
  } else if (OB_FAIL(ObBackupDataCleanUtil::get_backup_path_info(
                 backup_dest, incarnation, tenant_id, full_backup_set_id, inc_backup_set_id, path_info))) {
    LOG_WARN("failed to get backup path info", K(ret), K(clean_element), K(tenant_id));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_full_backup_set_path(path_info, path))) {
    LOG_WARN("failed to get tenant data full backup set path", K(ret), K(path_info));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(path), K(storage_info));
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::get_clean_statics(ObBackupDataCleanStatics& clean_statics)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else {
    clean_statics = clean_statics_;
  }
  return ret;
}

ObTenantBackupClogDataCleanTask::ObTenantBackupClogDataCleanTask()
    : is_inited_(false), clean_tenant_(), pkey_set_(), clean_statics_(), data_clean_(NULL)
{}

ObTenantBackupClogDataCleanTask::~ObTenantBackupClogDataCleanTask()
{}

int ObTenantBackupClogDataCleanTask::init(const ObBackupDataCleanTenant& clean_tenant, ObBackupDataClean* data_clean)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant backup clog data clean task init twice", K(ret));
  } else if (!clean_tenant.is_valid() || OB_ISNULL(data_clean)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant backup clog data clean task get invalid argument", K(ret), K(clean_tenant), KP(data_clean));
  } else if (OB_FAIL(pkey_set_.create(MAX_BUCKET_NUM))) {
    LOG_WARN("failed to create pkey set", K(ret));
  } else {
    clean_tenant_ = clean_tenant;
    data_clean_ = data_clean;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::do_clean()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();
  LOG_INFO("tenant backup clog data clean task do clean", K(clean_tenant_.simple_clean_tenant_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup clog data clean task do not init", K(ret));
  } else {
    const int64_t clog_gc_snapshot = clean_tenant_.clog_gc_snapshot_;
    const ObSimpleBackupDataCleanTenant& simple_clean_tenant = clean_tenant_.simple_clean_tenant_;
    const ObTenantBackupTaskInfo& clog_data_clean_point = clean_tenant_.clog_data_clean_point_;
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_tenant_.backup_element_array_.count(); ++i) {
      const ObBackupDataCleanElement& backup_clean_element = clean_tenant_.backup_element_array_.at(i);
      pkey_set_.reuse();

      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(do_inner_clean(simple_clean_tenant, backup_clean_element, clog_data_clean_point))) {
        LOG_WARN("failed to do inner clean", K(ret), K(simple_clean_tenant), K(backup_clean_element));
      } else if (OB_FAIL(check_and_delete_clog_data(simple_clean_tenant, backup_clean_element, clog_gc_snapshot))) {
        LOG_WARN("failed to check and delete clog data", K(ret), K(simple_clean_tenant), K(backup_clean_element));
      }
    }
  }

  LOG_INFO("finish tenant backup clog data clean task do clean",
      "cost",
      ObTimeUtil::current_time() - start_ts,
      K(ret),
      K(clean_tenant_.simple_clean_tenant_));

  return ret;
}

int ObTenantBackupClogDataCleanTask::do_inner_clean(const ObSimpleBackupDataCleanTenant& simple_clean_tenant,
    const ObBackupDataCleanElement& clean_element, const ObTenantBackupTaskInfo& clog_data_clean_point)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = simple_clean_tenant.tenant_id_;
  const int64_t full_backup_set_id = clog_data_clean_point.backup_set_id_;
  const int64_t inc_backup_set_id = clog_data_clean_point.backup_set_id_;
  ObBackupDataMgr backup_data_mgr;
  ObClusterBackupDest cluster_backup_dest;
  ObArray<int64_t> table_id_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup clog data clean task do not init", K(ret));
  } else if (!simple_clean_tenant.is_valid() || !clean_element.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner do clean get invalid argument", K(ret), K(simple_clean_tenant), K(clean_element));
  } else if (!clog_data_clean_point.is_valid()) {
    // do nothing
  } else if (OB_FAIL(cluster_backup_dest.set(clog_data_clean_point.backup_dest_, clog_data_clean_point.incarnation_))) {
    LOG_WARN("failed to set cluster backup destt", K(ret), K(clean_element));
  } else if (OB_FAIL(backup_data_mgr.init(cluster_backup_dest, tenant_id, full_backup_set_id, inc_backup_set_id))) {
    LOG_WARN(
        "failed to init backup data mgr", K(ret), K(cluster_backup_dest), K(full_backup_set_id), K(inc_backup_set_id));
  } else if (OB_FAIL(backup_data_mgr.get_base_data_table_id_list(table_id_array))) {
    LOG_WARN("failed ot get base data table id list", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_element.log_archive_round_array_.count(); ++i) {
      const ObLogArchiveRound& log_archive_round = clean_element.log_archive_round_array_.at(i);
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (ObLogArchiveStatus::STOP == log_archive_round.log_archive_status_) {
        if (log_archive_round.start_ts_ > clog_data_clean_point.snapshot_version_ ||
            log_archive_round.checkpoint_ts_ > clog_data_clean_point.snapshot_version_) {
          // clean set
          if (OB_FAIL(clean_clog_data(simple_clean_tenant,
                  clean_element,
                  clog_data_clean_point,
                  log_archive_round,
                  table_id_array,
                  backup_data_mgr))) {
            LOG_WARN("failed to clean clog data", K(ret), K(simple_clean_tenant), K(clog_data_clean_point));
          }
        } else {
          // log_archive_status.start_ts <= snapshot_version &&
          // log_archive_status.check_point_ts <= snapshot_version
          // clean clog archive round directly
          if (OB_FAIL(clean_interrputed_clog_data(simple_clean_tenant, clean_element, log_archive_round))) {
            LOG_WARN("failed to clean interrputed clog data", K(ret), K(simple_clean_tenant));
          }
        }
      } else {
        // clean set
        if (OB_FAIL(clean_clog_data(simple_clean_tenant,
                clean_element,
                clog_data_clean_point,
                log_archive_round,
                table_id_array,
                backup_data_mgr))) {
          LOG_WARN("failed to clean clog data", K(ret), K(simple_clean_tenant), K(clog_data_clean_point));
        }
      }
    }
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::clean_clog_data(const ObSimpleBackupDataCleanTenant& simple_clean_tenant,
    const ObBackupDataCleanElement& clean_element, const ObTenantBackupTaskInfo& clog_data_clean_point,
    const ObLogArchiveRound& log_archive_round, const common::ObIArray<int64_t>& table_id_array,
    ObBackupDataMgr& backup_data_mgr)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupMetaIndex> meta_index_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup clog data clean task do not init", K(ret));
  } else if (!simple_clean_tenant.is_valid() || !clean_element.is_valid() || !log_archive_round.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "clean clog data get invalid argument", K(ret), K(simple_clean_tenant), K(clean_element), K(log_archive_round));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_id_array.count(); ++i) {
      meta_index_array.reset();
      const int64_t table_id = table_id_array.at(i);
      ObTableClogDataCleanMgr table_clog_data_clean_mgr;
      ObBackupDataCleanStatics clean_statics;
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(backup_data_mgr.get_table_pg_meta_index(table_id, meta_index_array))) {
        LOG_WARN("failed to get table meta index", K(ret), K(table_id));
      } else if (OB_FAIL(table_clog_data_clean_mgr.init(table_id,
                     clean_element,
                     log_archive_round,
                     clog_data_clean_point,
                     meta_index_array,
                     *data_clean_))) {
        LOG_WARN("failed to init table data clean mgr", K(ret));
      } else if (OB_FAIL(table_clog_data_clean_mgr.do_clean())) {
        LOG_WARN("failed to do table data clean", K(ret));
      } else if (OB_FAIL(set_partition_into_set(meta_index_array))) {
        LOG_WARN("failed to set partition into set", K(ret));
      } else if (OB_FAIL(table_clog_data_clean_mgr.get_clean_statics(clean_statics))) {
        LOG_WARN("failed to get table clgo data clean statics", K(ret));
      } else {
        clean_statics_ += clean_statics;
        LOG_INFO("table clog data clean statis", K(table_id), K(log_archive_round), K(clean_statics));
      }
    }
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::set_partition_into_set(const ObIArray<ObBackupMetaIndex>& meta_index_array)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    LOG_WARN("tenant backup clog data clean mgr do not init", K(ret));
  } else {
    const int flag = 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_index_array.count(); ++i) {
      const ObBackupMetaIndex& meta_index = meta_index_array.at(i);
      const int64_t table_id = meta_index.table_id_;
      const int64_t partition_id = meta_index.partition_id_;
      ObPartitionKey pkey(table_id, partition_id, 0);
      if (OB_FAIL(pkey_set_.set_refactored_1(pkey, flag))) {
        LOG_WARN("failed to set pkey into set", K(ret), K(pkey));
      }
    }
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::check_and_delete_clog_data(
    const ObSimpleBackupDataCleanTenant& simple_clean_tenant, const ObBackupDataCleanElement& backup_clean_element,
    const int64_t clog_gc_snapshot)
{
  int ret = OB_SUCCESS;
  const ObBackupDest& backup_dest = backup_clean_element.backup_dest_;
  const int64_t incarnation = backup_clean_element.incarnation_;
  ObClusterBackupDest cluster_backup_dest;
  ObBackupListDataMgr backup_list_data_mgr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup data clean mgr do not init", K(ret));
  } else if (OB_FAIL(cluster_backup_dest.set(backup_dest, incarnation))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(backup_dest));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_clean_element.log_archive_round_array_.count(); ++i) {
      const ObLogArchiveRound& log_archive_round = backup_clean_element.log_archive_round_array_.at(i);
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(check_and_delete_clog_data_with_round(
                     simple_clean_tenant, cluster_backup_dest, log_archive_round, clog_gc_snapshot))) {
        LOG_WARN("failed to check and delete clog data with round", K(ret), K(log_archive_round));
      }
    }
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::check_and_delete_clog_data_with_round(
    const ObSimpleBackupDataCleanTenant& simple_clean_tenant, const ObClusterBackupDest& cluster_backup_dest,
    const ObLogArchiveRound& log_archive_round, const int64_t max_clean_clog_snapshot)
{
  int ret = OB_SUCCESS;
  ObArray<ObPartitionKey> pkey_list;
  const int64_t archive_round = log_archive_round.log_archive_round_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed ot check and delete clog data with round", K(ret), K(simple_clean_tenant));
  } else if (OB_FAIL(get_clog_pkey_list_not_in_base_data(cluster_backup_dest,
                 log_archive_round.log_archive_round_,
                 simple_clean_tenant.tenant_id_,
                 pkey_list))) {
    LOG_WARN("failed to get clog pkey list not in base data", K(ret), K(cluster_backup_dest), K(log_archive_round));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pkey_list.count(); ++i) {
      const ObPartitionKey& pkey = pkey_list.at(i);
      ObPartitionClogDataCleanMgr clog_data_clean_mgr;
      archive::ObArchiveClear archive_clear;
      uint64_t data_file_id = 0;
      uint64_t index_file_id = 0;
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(archive_clear.get_clean_max_clog_file_id_by_log_ts(
                     cluster_backup_dest, archive_round, pkey, max_clean_clog_snapshot, index_file_id, data_file_id))) {
        LOG_WARN("failed to get clean max clog file id by log ts",
            K(ret),
            K(pkey),
            K(log_archive_round),
            K(max_clean_clog_snapshot));
      } else if (OB_FAIL(clog_data_clean_mgr.init(
                     cluster_backup_dest, log_archive_round, pkey, data_file_id, index_file_id, *data_clean_))) {
        LOG_WARN("failed to init clog data clean mgr", K(ret), K(cluster_backup_dest), K(log_archive_round), K(pkey));
      } else if (OB_FAIL(clog_data_clean_mgr.touch_clog_backup_data())) {
        LOG_WARN("failed to touch clog backup data", K(ret), K(pkey), K(cluster_backup_dest));
      } else if (OB_FAIL(clog_data_clean_mgr.clean_clog_backup_data())) {
        LOG_WARN("failed to clean clog backup data", K(ret), K(pkey), K(cluster_backup_dest));
      } else {
        LOG_INFO("check and delete clog data", K(pkey), K(log_archive_round));
      }
    }
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::get_clog_pkey_list_not_in_base_data(const ObClusterBackupDest& cluster_backup_dest,
    const int64_t log_archive_round, const uint64_t tenant_id, common::ObIArray<ObPartitionKey>& pkey_list)
{
  int ret = OB_SUCCESS;
  ObBackupListDataMgr backup_list_data_mgr;
  ObArray<ObPartitionKey> clog_pkey_list;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup data clean mgr do not init", K(ret));
  } else if (OB_FAIL(backup_list_data_mgr.init(cluster_backup_dest, log_archive_round, tenant_id))) {
    LOG_WARN("failed to init backup list data mgr", K(ret), K(cluster_backup_dest), K(log_archive_round));
  } else if (OB_FAIL(backup_list_data_mgr.get_clog_pkey_list(clog_pkey_list))) {
    LOG_WARN("failed to get clog pkey list", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clog_pkey_list.count(); ++i) {
      const ObPartitionKey& pkey = clog_pkey_list.at(i);
      int hash_ret = pkey_set_.exist_refactored(pkey);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        if (OB_FAIL(pkey_list.push_back(pkey))) {
          LOG_WARN("failed to push pkey into list", K(ret), K(pkey));
        }
      } else if (OB_HASH_EXIST == hash_ret) {
        // do nothing
      } else {
        ret = OB_SUCCESS == hash_ret ? OB_ERR_UNEXPECTED : hash_ret;
        LOG_WARN("failed to check exist from set", K(ret), K(pkey));
      }
    }
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::clean_interrputed_clog_data(
    const ObSimpleBackupDataCleanTenant& simple_clean_tenant, const ObBackupDataCleanElement& clean_element,
    const ObLogArchiveRound& log_archive_round)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = simple_clean_tenant.tenant_id_;
  const int64_t round = log_archive_round.log_archive_round_;
  const char* storage_info = clean_element.backup_dest_.storage_info_;
  const ObStorageType& storage_type = clean_element.backup_dest_.device_type_;
  ObClusterBackupDest cluster_backup_dest;
  ObBackupListDataMgr backup_list_data_mgr;
  ObArray<ObPartitionKey> clog_pkey_list;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup data clean mgr do not init", K(ret));
  } else if (ObLogArchiveStatus::STOP != log_archive_round.log_archive_status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("clean interrputed clog data get invalid argument", K(ret), K(log_archive_round));
  } else if (OB_FAIL(cluster_backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(clean_element));
  } else if (OB_FAIL(backup_list_data_mgr.init(cluster_backup_dest, round, tenant_id))) {
    LOG_WARN("failed to init backup list data mgr", K(ret), K(cluster_backup_dest), K(log_archive_round));
  } else if (OB_FAIL(backup_list_data_mgr.get_clog_pkey_list(clog_pkey_list))) {
    LOG_WARN("failed to get clog pkey list", K(ret), K(cluster_backup_dest));
  } else {
    LOG_INFO("start clean interruped clog data", K(clean_element), K(log_archive_round));
    const uint64_t data_file_id = UINT64_MAX;
    const uint64_t index_file_id = UINT64_MAX;

    for (int64_t i = 0; OB_SUCC(ret) && i < clog_pkey_list.count(); ++i) {
      const ObPartitionKey& pkey = clog_pkey_list.at(i);
      ObPartitionClogDataCleanMgr partition_clog_data_clean_mgr;
      ObBackupDataCleanStatics clean_statics;
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(partition_clog_data_clean_mgr.init(
                     cluster_backup_dest, log_archive_round, pkey, data_file_id, index_file_id, *data_clean_))) {
        LOG_WARN("failed to init partition clog data clean mgr", K(ret), K(pkey), K(cluster_backup_dest));
      } else if (OB_FAIL(partition_clog_data_clean_mgr.clean_clog_backup_data())) {
        LOG_WARN("failed to clean clog backup data", K(ret), K(pkey));
      } else if (OB_FAIL(try_clean_table_clog_data_dir(
                     cluster_backup_dest, tenant_id, round, pkey.get_table_id(), storage_info, storage_type))) {
        LOG_WARN("failed to try clean clog data dir", K(ret), K(pkey));
      } else if (OB_FAIL(partition_clog_data_clean_mgr.get_clean_statics(clean_statics))) {
        LOG_WARN("failed to get partition clog data clean statics", K(ret));
      } else {
        clean_statics_ += clean_statics;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(try_clean_clog_data_dir(cluster_backup_dest, tenant_id, round, storage_info, storage_type))) {
        LOG_WARN("failed to try clean clog data dir", K(ret), K(cluster_backup_dest));
      }
    }
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::try_clean_table_clog_data_dir(const ObClusterBackupDest& cluster_backup_dest,
    const uint64_t tenant_id, const int64_t log_archive_round, const int64_t table_id, const char* storage_info,
    const common::ObStorageType& device_type)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;

  if (!cluster_backup_dest.is_valid() || log_archive_round <= 0 || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("try clean clog data dir get invlaid argument", K(ret), K(cluster_backup_dest), KP(storage_info));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_data_path(
                 cluster_backup_dest, tenant_id, log_archive_round, path))) {
    LOG_WARN("fail to get meta file path", K(ret));
  } else if (OB_FAIL(path.join(table_id))) {
    LOG_WARN("failed to join path", K(ret), K(table_id));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(storage_info), K(device_type));
  } else if (FALSE_IT(path.reset())) {
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_index_path(
                 cluster_backup_dest, tenant_id, log_archive_round, path))) {
    LOG_WARN("fail to get meta file path", K(ret));
  } else if (OB_FAIL(path.join(table_id))) {
    LOG_WARN("failed to join path", K(ret), K(table_id));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(storage_info), K(device_type));
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::try_clean_clog_data_dir(const ObClusterBackupDest& cluster_backup_dest,
    const uint64_t tenant_id, const int64_t log_archive_round, const char* storage_info,
    const common::ObStorageType& device_type)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;

  if (!cluster_backup_dest.is_valid() || log_archive_round <= 0 || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("try clean clog data dir get invlaid argument", K(ret), K(cluster_backup_dest), KP(storage_info));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_data_path(
                 cluster_backup_dest, tenant_id, log_archive_round, path))) {
    LOG_WARN("fail to get meta file path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(storage_info), K(device_type));
  } else if (FALSE_IT(path.reset())) {
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_index_path(
                 cluster_backup_dest, tenant_id, log_archive_round, path))) {
    LOG_WARN("fail to get meta file path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(storage_info), K(device_type));
  } else if (FALSE_IT(path.reset())) {
  } else if (OB_FAIL(ObBackupPathUtil::get_cluster_clog_prefix_path(
                 cluster_backup_dest, tenant_id, log_archive_round, path))) {
    LOG_WARN("failed to get cluster clog prefix path", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(storage_info), K(device_type));
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::get_clean_statics(ObBackupDataCleanStatics& clean_statics)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup data clean mgr do not init", K(ret));
  } else {
    clean_statics = clean_statics_;
  }
  return ret;
}

ObTableBaseDataCleanMgr::ObTableBaseDataCleanMgr()
    : is_inited_(false),
      table_id_(0),
      clean_element_(),
      backup_set_id_(),
      inc_backup_set_id_(0),
      meta_index_array_(),
      path_info_(),
      clean_statics_(),
      data_clean_(NULL)
{}

ObTableBaseDataCleanMgr::~ObTableBaseDataCleanMgr()
{}

int ObTableBaseDataCleanMgr::init(const int64_t table_id, const ObBackupDataCleanElement& clean_element,
    const ObBackupSetId& backup_set_id, const int64_t inc_backup_set_id,
    const common::ObIArray<ObBackupMetaIndex>& meta_index_array, ObBackupDataClean& data_clean)
{
  int ret = OB_SUCCESS;
  const int64_t full_backup_set_id = backup_set_id.backup_set_id_;
  const uint64_t tenant_id = extract_tenant_id(table_id);

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("table data clean mgr init twice", K(ret));
  } else if (!clean_element.is_valid() || !backup_set_id.is_valid() || table_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "init partition data clean mgr get invalid argument", K(ret), K(clean_element), K(backup_set_id), K(table_id));
  } else if (OB_FAIL(ObBackupDataCleanUtil::get_backup_path_info(clean_element.backup_dest_,
                 clean_element.incarnation_,
                 tenant_id,
                 full_backup_set_id,
                 inc_backup_set_id,
                 path_info_))) {
    LOG_WARN("failed to get backup path info", K(ret), K(clean_element));
  } else if (OB_FAIL(meta_index_array_.assign(meta_index_array))) {
    LOG_WARN("failed to assign meta index array", K(ret));
  } else {
    table_id_ = table_id;
    clean_element_ = clean_element;
    backup_set_id_ = backup_set_id;
    inc_backup_set_id_ = inc_backup_set_id;
    data_clean_ = &data_clean;
    is_inited_ = true;
  }
  return ret;
}

int ObTableBaseDataCleanMgr::do_clean()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();
  LOG_INFO(
      "start table base data clean mgr", K(table_id_), K(clean_element_), K(backup_set_id_), K(inc_backup_set_id_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant data clean mgr do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_index_array_.count(); ++i) {
      const ObBackupMetaIndex& meta_index = meta_index_array_.at(i);
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(clean_partition_backup_data(meta_index))) {
        LOG_WARN("failed to clean partition backup data", K(ret), K(meta_index));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(try_clean_backup_table_dir())) {
        LOG_WARN("failed to clean backup table dir", K(ret));
      }
    }
  }
  LOG_INFO("finish table base data clean mgr",
      "cost_ts",
      ObTimeUtil::current_time() - start_ts,
      K(table_id_),
      K(clean_element_),
      K(backup_set_id_),
      K(inc_backup_set_id_));

  return ret;
}

int ObTableBaseDataCleanMgr::clean_partition_backup_data(const ObBackupMetaIndex& meta_index)
{
  int ret = OB_SUCCESS;
  ObPartitionDataCleanMgr partition_data_clean_mgr;
  ObBackupDataCleanStatics clean_statics;
  const ObPartitionKey pkey(meta_index.table_id_, meta_index.partition_id_, 0);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table data clean mgr do not init", K(ret));
  } else if (OB_FAIL(partition_data_clean_mgr.init(
                 pkey, clean_element_, backup_set_id_, inc_backup_set_id_, *data_clean_))) {
    LOG_WARN(
        "failed to init partition data clean mgr", K(ret), K(clean_element_), K(backup_set_id_), K(inc_backup_set_id_));
  } else if (OB_FAIL(partition_data_clean_mgr.do_clean())) {
    LOG_WARN("failed to do clean partition backup data",
        K(ret),
        K(clean_element_),
        K(backup_set_id_),
        K(inc_backup_set_id_));
  } else if (OB_FAIL(partition_data_clean_mgr.get_clean_statics(clean_statics))) {
    LOG_WARN("failed to get partition data clean mgr", K(ret));
  } else {
    clean_statics_ += clean_statics;
    LOG_INFO("table clean partition backup data statics",
        K(clean_element_),
        K(backup_set_id_),
        K(meta_index),
        K(clean_statics));
  }
  return ret;
}

int ObTableBaseDataCleanMgr::try_clean_backup_table_dir()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  const char* storage_info = clean_element_.backup_dest_.storage_info_;
  const ObStorageType& device_type = clean_element_.backup_dest_.device_type_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table data clean mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_table_data_path(path_info_, table_id_, path))) {
    LOG_WARN("fail to get meta file path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(storage_info), K(device_type));
  }
  return ret;
}

int ObTableBaseDataCleanMgr::get_clean_statics(ObBackupDataCleanStatics& clean_statics)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table data clean mgr do not init", K(ret));
  } else {
    clean_statics = clean_statics_;
  }
  return ret;
}

ObTableClogDataCleanMgr::ObTableClogDataCleanMgr()
    : is_inited_(false),
      table_id_(0),
      clean_element_(),
      log_archive_round_(),
      clog_data_clean_point_(),
      meta_index_array_(),
      clean_statics_(),
      data_clean_(NULL)
{}

ObTableClogDataCleanMgr::~ObTableClogDataCleanMgr()
{}

int ObTableClogDataCleanMgr::init(const int64_t table_id, const ObBackupDataCleanElement& clean_element,
    const ObLogArchiveRound& log_archive_round, const ObTenantBackupTaskInfo& clog_data_clean_point,
    const common::ObIArray<ObBackupMetaIndex>& meta_index_array, ObBackupDataClean& data_clean)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("table clog data clean mgr init twice", K(ret));
  } else if (!clean_element.is_valid() || table_id <= 0 || !log_archive_round.is_valid() ||
             !clog_data_clean_point.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init partition data clean mgr get invalid argument",
        K(ret),
        K(clean_element),
        K(table_id),
        K(log_archive_round),
        K(clog_data_clean_point));
  } else if (OB_FAIL(meta_index_array_.assign(meta_index_array))) {
    LOG_WARN("failed to assign meta index array", K(ret));
  } else {
    table_id_ = table_id;
    clean_element_ = clean_element;
    log_archive_round_ = log_archive_round;
    clog_data_clean_point_ = clog_data_clean_point;
    data_clean_ = &data_clean;
    is_inited_ = true;
  }
  return ret;
}

int ObTableClogDataCleanMgr::do_clean()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();
  LOG_INFO("start table clog data clean mgr do clean",
      K(table_id_),
      K(clean_element_),
      K(log_archive_round_),
      K(clog_data_clean_point_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table clog data clean mgr do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_index_array_.count(); ++i) {
      const ObBackupMetaIndex& meta_index = meta_index_array_.at(i);
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(clean_partition_clog_backup_data(meta_index))) {
        LOG_WARN("failed to clean partition backup data", K(ret), K(meta_index));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(try_clean_backup_table_clog_dir())) {
        LOG_WARN("failed to clean backup table dir", K(ret));
      }
    }
  }

  LOG_INFO("finish table clog data clean mgr do clean",
      "cost_ts",
      ObTimeUtil::current_time() - start_ts,
      K(ret),
      K(table_id_),
      K(clean_element_),
      K(log_archive_round_),
      K(clog_data_clean_point_));

  return ret;
}

int ObTableClogDataCleanMgr::clean_partition_clog_backup_data(const ObBackupMetaIndex& meta_index)
{
  int ret = OB_SUCCESS;
  ObPartitionGroupMeta pg_meta;
  ObBackupDataCleanStatics clean_statics;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table clog data clean mgr do not init", K(ret));
  } else if (OB_FAIL(meta_index.check_valid())) {
    LOG_WARN("failed to check meta index is valid", K(ret), K(meta_index));
  } else if (OB_FAIL(get_partition_meta(meta_index, pg_meta))) {
    LOG_WARN("failed to get partition meta", K(ret), K(meta_index));
  } else {
    ObClusterBackupDest cluster_backup_dest;
    ObBackupDest& backup_dest = clean_element_.backup_dest_;
    const int64_t incarnation = clean_element_.incarnation_;
    const uint64_t last_replay_log_id = pg_meta.storage_info_.get_clog_info().get_last_replay_log_id();
    const ObPartitionKey pkey(meta_index.table_id_, meta_index.partition_id_, 0);
    uint64_t index_file_id = 0;
    uint64_t data_file_id = 0;
    archive::ObArchiveClear archive_clear;
    ObPartitionClogDataCleanMgr clog_data_clean_mgr;
    if (OB_FAIL(cluster_backup_dest.set(backup_dest, incarnation))) {
      LOG_WARN("failed to set cluster backup dest", K(ret), K(backup_dest));
    } else if (OB_FAIL(archive_clear.get_clean_max_clog_file_id_by_log_id(cluster_backup_dest,
                   log_archive_round_.log_archive_round_,
                   pg_meta.pg_key_,
                   last_replay_log_id,
                   index_file_id,
                   data_file_id))) {
      LOG_WARN("failed ot get clean max clog file id by log id", K(ret), K(last_replay_log_id));
    } else {
      LOG_INFO("get clean max clog file id by log id",
          K(pg_meta.pg_key_),
          K(last_replay_log_id),
          K(index_file_id),
          K(data_file_id),
          K(log_archive_round_),
          K(cluster_backup_dest));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(clog_data_clean_mgr.init(
                   cluster_backup_dest, log_archive_round_, pkey, data_file_id, index_file_id, *data_clean_))) {
      LOG_WARN("failed to init clog data clean mgr", K(ret), K(cluster_backup_dest));
    } else if (OB_FAIL(clog_data_clean_mgr.touch_clog_backup_data())) {
      LOG_WARN("failed to touch clog backup data", K(ret), K(cluster_backup_dest), K(data_file_id), K(index_file_id));
    } else if (OB_FAIL(clog_data_clean_mgr.clean_clog_backup_data())) {
      LOG_WARN("failed to touch clog backup data", K(ret), K(cluster_backup_dest), K(data_file_id), K(index_file_id));
    } else if (OB_FAIL(clog_data_clean_mgr.get_clean_statics(clean_statics))) {
      LOG_WARN("failed to get clean statics", K(ret));
    } else {
      clean_statics_ += clean_statics;
      LOG_INFO("clean clog data statics",
          K(log_archive_round_),
          K(cluster_backup_dest),
          K(pg_meta.pg_key_),
          K(last_replay_log_id),
          K(index_file_id),
          K(data_file_id),
          K(clean_statics));
    }
  }
  return ret;
}

int ObTableClogDataCleanMgr::try_clean_backup_table_clog_dir()
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest cluster_backup_dest;
  ObBackupDest& backup_dest = clean_element_.backup_dest_;
  const int64_t incarnation = clean_element_.incarnation_;
  const uint64_t tenant_id = extract_tenant_id(table_id_);
  const int64_t round = log_archive_round_.log_archive_round_;

  const char* storage_info = clean_element_.backup_dest_.storage_info_;
  const ObStorageType& device_type = clean_element_.backup_dest_.device_type_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table data clean mgr do not init", K(ret));
  } else if (OB_FAIL(cluster_backup_dest.set(backup_dest, incarnation))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(backup_dest), K(incarnation));
  } else if (OB_FAIL(ObTenantBackupClogDataCleanTask::try_clean_table_clog_data_dir(
                 cluster_backup_dest, tenant_id, round, table_id_, storage_info, device_type))) {
    LOG_WARN("failed to do try clean clog data dir", K(ret), K(cluster_backup_dest));
  }
  return ret;
}

int ObTableClogDataCleanMgr::get_partition_meta(const ObBackupMetaIndex& meta_index, ObPartitionGroupMeta& pg_meta)
{
  int ret = OB_SUCCESS;
  pg_meta.reset();
  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table data clean mgr do not init", K(ret));
  } else if (OB_FAIL(meta_index.check_valid())) {
    LOG_WARN("failed to check meta index is valid", K(ret), K(meta_index));
  } else if (OB_FAIL(path_info.dest_.set(clog_data_clean_point_.backup_dest_, clog_data_clean_point_.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(clog_data_clean_point_));
  } else {
    path_info.full_backup_set_id_ = clog_data_clean_point_.backup_set_id_;
    path_info.inc_backup_set_id_ = clog_data_clean_point_.backup_set_id_;
    path_info.tenant_id_ = extract_tenant_id(meta_index.table_id_);
    if (OB_FAIL(ObBackupPathUtil::get_tenant_data_meta_file_path(path_info, meta_index.task_id_, path))) {
      LOG_WARN("fail to get meta file path", K(ret));
    } else if (OB_FAIL(ObRestoreFileUtil::read_partition_group_meta(
                   path.get_obstr(), path_info.dest_.get_storage_info(), meta_index, pg_meta))) {
      LOG_WARN("fail to get partition meta", K(ret), K(path), K(pg_meta));
    }
  }
  return ret;
}

int ObTableClogDataCleanMgr::get_clean_statics(ObBackupDataCleanStatics& clean_statics)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table data clean mgr do not init", K(ret));
  } else {
    clean_statics = clean_statics_;
  }
  return ret;
}

ObPartitionClogDataCleanMgr::ObPartitionClogDataCleanMgr()
    : is_inited_(false),
      cluster_backup_dest_(),
      log_archive_round_(),
      pkey_(),
      data_file_id_(0),
      index_file_id_(0),
      need_clean_dir_(false),
      clean_statics_(),
      data_clean_(NULL)
{}

ObPartitionClogDataCleanMgr::~ObPartitionClogDataCleanMgr()
{}

int ObPartitionClogDataCleanMgr::init(const ObClusterBackupDest& cluster_backup_dest,
    const ObLogArchiveRound& log_archive_round, const ObPartitionKey& pkey, const uint64_t data_file_id,
    const uint64_t index_file_id, ObBackupDataClean& data_clean)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("partition clog data clean mgr do not init", K(ret));
  } else if (!cluster_backup_dest.is_valid() || !log_archive_round.is_valid() || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init partition clog data clean mgr get invalid argument",
        K(ret),
        K(cluster_backup_dest),
        K(log_archive_round),
        K(pkey));
  } else if (OB_FAIL(set_need_delete_clog_dir(cluster_backup_dest, log_archive_round))) {
    LOG_WARN("failed to check can delete clog dir", K(ret), K(log_archive_round));
  } else {
    cluster_backup_dest_ = cluster_backup_dest;
    log_archive_round_ = log_archive_round;
    pkey_ = pkey;
    data_file_id_ = data_file_id;
    index_file_id_ = index_file_id;
    data_clean_ = &data_clean;
    is_inited_ = true;
  }
  return ret;
}

int ObPartitionClogDataCleanMgr::touch_clog_backup_data()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition clog data clean mgr do not init", K(ret));
  } else if (OB_FAIL(touch_clog_data_())) {
    LOG_WARN("failed to touch clog data", K(ret));
  } else if (OB_FAIL(touch_clog_meta_())) {
    LOG_WARN("failed to touch clog meta", K(ret));
  }
  return ret;
}

int ObPartitionClogDataCleanMgr::clean_clog_backup_data()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition clog data clean mgr do not init", K(ret));
  } else if (OB_FAIL(clean_clog_data_())) {
    LOG_WARN("failed to touch clog data", K(ret));
  } else if (OB_FAIL(clean_clog_meta_())) {
    LOG_WARN("failed to touch clog meta", K(ret));
  }
  return ret;
}

int ObPartitionClogDataCleanMgr::touch_clog_data_()
{
  int ret = OB_SUCCESS;
  const char* storage_info = cluster_backup_dest_.get_storage_info();
  const uint64_t tenant_id = pkey_.get_tenant_id();
  const ObStorageType& device = cluster_backup_dest_.dest_.device_type_;
  ObBackupPath path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_table_clog_data_path(cluster_backup_dest_,
                 tenant_id,
                 log_archive_round_.log_archive_round_,
                 pkey_.get_table_id(),
                 pkey_.get_partition_id(),
                 path))) {
    LOG_WARN("failed to get tenant clog data path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::touch_clog_dir_files(path,
                 storage_info,
                 data_file_id_,
                 device,
                 clean_statics_,
                 *data_clean_->get_backup_lease_service()))) {
    LOG_WARN("failed to touch clog file", K(ret), K(path), K(data_file_id_));
  }
  return ret;
}

int ObPartitionClogDataCleanMgr::touch_clog_meta_()
{
  int ret = OB_SUCCESS;
  const char* storage_info = cluster_backup_dest_.get_storage_info();
  const uint64_t tenant_id = pkey_.get_tenant_id();
  const ObStorageType& device = cluster_backup_dest_.dest_.device_type_;
  ObBackupPath path;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_table_clog_index_path(cluster_backup_dest_,
                 tenant_id,
                 log_archive_round_.log_archive_round_,
                 pkey_.get_table_id(),
                 pkey_.get_partition_id(),
                 path))) {
    LOG_WARN("failed to get tenant clog data path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::touch_clog_dir_files(path,
                 storage_info,
                 index_file_id_,
                 device,
                 clean_statics_,
                 *data_clean_->get_backup_lease_service()))) {
    LOG_WARN("failed to touch clog file", K(ret), K(path), K(index_file_id_));
  }
  return ret;
}

int ObPartitionClogDataCleanMgr::clean_clog_data_()
{
  int ret = OB_SUCCESS;
  const char* storage_info = cluster_backup_dest_.get_storage_info();
  const uint64_t tenant_id = pkey_.get_tenant_id();
  const ObStorageType& device = cluster_backup_dest_.dest_.device_type_;
  ObBackupPath path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_table_clog_data_path(cluster_backup_dest_,
                 tenant_id,
                 log_archive_round_.log_archive_round_,
                 pkey_.get_table_id(),
                 pkey_.get_partition_id(),
                 path))) {
    LOG_WARN("failed to get tenant clog data path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_clog_dir_files(path,
                 storage_info,
                 data_file_id_,
                 device,
                 clean_statics_,
                 *data_clean_->get_backup_lease_service()))) {
    LOG_WARN("failed to delete clog file", K(ret), K(path), K(data_file_id_));
  } else if (need_clean_dir_) {
    if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device))) {
      LOG_WARN("failed to delete backup dir", K(ret), K(path));
    }
  }
  return ret;
}

int ObPartitionClogDataCleanMgr::clean_clog_meta_()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  const char* storage_info = cluster_backup_dest_.get_storage_info();
  const uint64_t tenant_id = pkey_.get_tenant_id();
  const ObStorageType& device = cluster_backup_dest_.dest_.device_type_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_table_clog_index_path(cluster_backup_dest_,
                 tenant_id,
                 log_archive_round_.log_archive_round_,
                 pkey_.get_table_id(),
                 pkey_.get_partition_id(),
                 path))) {
    LOG_WARN("failed to get tenant clog data path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_clog_dir_files(path,
                 storage_info,
                 index_file_id_,
                 device,
                 clean_statics_,
                 *data_clean_->get_backup_lease_service()))) {
    LOG_WARN("failed to delete clog file", K(ret), K(path), K(index_file_id_));
  } else if (need_clean_dir_) {
    if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device))) {
      LOG_WARN("failed to delete backup dir", K(ret), K(path));
    }
  }
  return ret;
}

// TODO need use schema to check partition exist
int ObPartitionClogDataCleanMgr::set_need_delete_clog_dir(
    const ObClusterBackupDest& cluster_backup_dest, const ObLogArchiveRound& log_arcvhie_round)
{
  int ret = OB_SUCCESS;
  need_clean_dir_ = false;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("partition clog data clean mgr init twice", K(ret));
  } else if (ObStorageType::OB_STORAGE_OSS == cluster_backup_dest.dest_.device_type_) {
    need_clean_dir_ = false;
  } else if (ObLogArchiveStatus::STOP == log_arcvhie_round.log_archive_status_) {
    need_clean_dir_ = true;
  } else if (ObStorageType::OB_STORAGE_COS == cluster_backup_dest.dest_.device_type_) {
    need_clean_dir_ = true;
  }
  return ret;
}

int ObPartitionClogDataCleanMgr::get_clean_statics(ObBackupDataCleanStatics& clean_statics)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else {
    clean_statics = clean_statics_;
  }
  return ret;
}

ObPartitionDataCleanMgr::ObPartitionDataCleanMgr()
    : is_inited_(false),
      pkey_(),
      backup_set_id_(),
      inc_backup_set_id_(0),
      path_info_(),
      clean_statics_(),
      data_clean_(NULL)
{}

ObPartitionDataCleanMgr::~ObPartitionDataCleanMgr()
{}

int ObPartitionDataCleanMgr::init(const ObPartitionKey& pkey, const ObBackupDataCleanElement& clean_element,
    const ObBackupSetId& backup_set_id, const int64_t inc_backup_set_id, ObBackupDataClean& data_clean)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = pkey.get_tenant_id();
  const int64_t full_backup_set_id = backup_set_id.backup_set_id_;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("partition data clean mgr init twice", K(ret));
  } else if (!pkey.is_valid() || !clean_element.is_valid() || !backup_set_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init partition data clean mgr get invalid argument", K(ret), K(clean_element), K(backup_set_id));
  } else if (OB_FAIL(ObBackupDataCleanUtil::get_backup_path_info(clean_element.backup_dest_,
                 clean_element.incarnation_,
                 tenant_id,
                 full_backup_set_id,
                 inc_backup_set_id,
                 path_info_))) {
    LOG_WARN("failed to get backup path info", K(ret), K(clean_element));
  } else {
    pkey_ = pkey;
    backup_set_id_ = backup_set_id;
    inc_backup_set_id_ = inc_backup_set_id;
    data_clean_ = &data_clean;
    is_inited_ = true;
  }
  return ret;
}

int ObPartitionDataCleanMgr::do_clean()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else {
    if (ObBackupDataCleanMode::TOUCH == backup_set_id_.clean_mode_) {
      if (OB_FAIL(touch_backup_data())) {
        LOG_WARN("failed to touch backup data", K(ret));
      }
    } else if (ObBackupDataCleanMode::CLEAN == backup_set_id_.clean_mode_) {
      if (OB_FAIL(clean_backup_data())) {
        LOG_WARN("failed to clean backup data", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup set id mod is invalid", K(ret), K(backup_set_id_));
    }
  }

  return ret;
}

int ObPartitionDataCleanMgr::touch_backup_data()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  const ObStorageType device_type = path_info_.dest_.dest_.device_type_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_pg_data_path(
                 path_info_, pkey_.get_table_id(), pkey_.get_partition_id(), path))) {
    STORAGE_LOG(WARN, "failed to get inc backup path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::touch_backup_dir_files(path,
                 path_info_.dest_.get_storage_info(),
                 device_type,
                 clean_statics_,
                 *data_clean_->get_backup_lease_service()))) {
    LOG_WARN("failed to touch backup file", K(ret), K(path));
  }
  return ret;
}

int ObPartitionDataCleanMgr::clean_backup_data()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  const ObStorageType device_type = path_info_.dest_.dest_.device_type_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_pg_data_path(
                 path_info_, pkey_.get_table_id(), pkey_.get_partition_id(), path))) {
    STORAGE_LOG(WARN, "failed to get inc backup path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir_files(path,
                 path_info_.dest_.get_storage_info(),
                 device_type,
                 clean_statics_,
                 *data_clean_->get_backup_lease_service()))) {
    LOG_WARN("failed to touch backup file", K(ret), K(path));
  }
  return ret;
}

int ObPartitionDataCleanMgr::get_clean_statics(ObBackupDataCleanStatics& clean_statics)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else {
    clean_statics_ = clean_statics;
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
