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
#include "share/backup/ob_log_archive_backup_info_mgr.h"

namespace oceanbase {

using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace storage;

namespace rootserver {

ObBackupSetId::ObBackupSetId() : backup_set_id_(0), clean_mode_(MAX), copy_id_(0)
{}

void ObBackupSetId::reset()
{
  backup_set_id_ = 0;
  clean_mode_ = MAX;
  copy_id_ = 0;
}

bool ObBackupSetId::is_valid() const
{
  return backup_set_id_ > 0 && clean_mode_ >= CLEAN && clean_mode_ < MAX && copy_id_ >= 0;
}

ObSimplePieceInfo::ObSimplePieceInfo()
    : round_id_(0),
      backup_piece_id_(0),
      create_date_(0),
      start_ts_(0),
      checkpoint_ts_(0),
      max_ts_(0),
      status_(ObBackupPieceStatus::BACKUP_PIECE_MAX),
      file_status_(ObBackupFileStatus::BACKUP_FILE_MAX),
      copies_num_(0)
{}

void ObSimplePieceInfo::reset()
{
  round_id_ = 0;
  backup_piece_id_ = 0;
  create_date_ = 0;
  start_ts_ = 0;
  checkpoint_ts_ = 0;
  max_ts_ = 0;
  status_ = ObBackupPieceStatus::BACKUP_PIECE_MAX;
  file_status_ = ObBackupFileStatus::BACKUP_FILE_MAX;
  copies_num_ = 0;
}

bool ObSimplePieceInfo::is_valid() const
{
  return round_id_ > 0 && backup_piece_id_ >= 0 && create_date_ >= 0 && status_ >= 0 &&
         status_ < ObBackupPieceStatus::BACKUP_PIECE_MAX && file_status_ >= 0 &&
         file_status_ < ObBackupFileStatus::BACKUP_FILE_MAX && start_ts_ >= 0 && checkpoint_ts_ >= 0 && max_ts_ >= 0 &&
         copies_num_ >= 0;
}

ObLogArchiveRound::ObLogArchiveRound()
    : log_archive_round_(0),
      log_archive_status_(ObLogArchiveStatus::INVALID),
      start_ts_(0),
      checkpoint_ts_(0),
      start_piece_id_(0),
      copy_id_(0),
      piece_infos_(),
      copies_num_(0)
{}

void ObLogArchiveRound::reset()
{
  log_archive_round_ = 0;
  log_archive_status_ = ObLogArchiveStatus::INVALID;
  start_ts_ = 0;
  checkpoint_ts_ = 0;
  start_piece_id_ = 0;
  copy_id_ = 0;
  piece_infos_.reset();
  copies_num_ = 0;
}

bool ObLogArchiveRound::is_valid() const
{
  return log_archive_round_ > 0 && log_archive_status_ != ObLogArchiveStatus::INVALID && start_ts_ > 0 &&
         copy_id_ >= 0 && piece_infos_.count() >= 0 && copies_num_ >= 0;
}

int ObLogArchiveRound::add_simpe_piece_info(const ObSimplePieceInfo &piece_info)
{
  int ret = OB_SUCCESS;
  if (!piece_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add simpe piece info get invalid argument", K(ret), K(piece_info));
  } else if (OB_FAIL(piece_infos_.push_back(piece_info))) {
    LOG_WARN("failed to push piece info into array", K(ret), K(piece_info));
  }
  return ret;
}

int ObLogArchiveRound::assign(const ObLogArchiveRound &log_archive_round)
{
  int ret = OB_SUCCESS;
  if (!log_archive_round.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign get invalid argument", K(ret), K(log_archive_round));
  } else {
    log_archive_round_ = log_archive_round.log_archive_round_;
    log_archive_status_ = log_archive_round.log_archive_status_;
    start_ts_ = log_archive_round.start_ts_;
    checkpoint_ts_ = log_archive_round.checkpoint_ts_;
    start_piece_id_ = log_archive_round.start_piece_id_;
    copy_id_ = log_archive_round.copy_id_;
    copies_num_ = log_archive_round.copies_num_;
    if (OB_FAIL(piece_infos_.assign(log_archive_round.piece_infos_))) {
      LOG_WARN("failed to assign piece infos", K(ret), K(log_archive_round));
    }
  }
  return ret;
}

bool ObBackupDataCleanElement::is_valid() const
{
  return OB_INVALID_CLUSTER_ID != cluster_id_ && incarnation_ > 0 && backup_dest_.is_valid() &&
         backup_dest_option_.is_valid();
}

void ObBackupDataCleanElement::reset()
{
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  incarnation_ = 0;
  backup_dest_.reset();
  backup_set_id_array_.reset();
  backup_dest_option_.reset();
}

bool ObBackupDataCleanElement::is_same_element(
    const int64_t cluster_id, const int64_t incarnation, const ObBackupDest &backup_dest) const
{
  return cluster_id_ == cluster_id && incarnation_ == incarnation && backup_dest_ == backup_dest;
}

int ObBackupDataCleanElement::set_backup_set_id(const ObBackupSetId &backup_set_id)
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

int ObBackupDataCleanElement::set_log_archive_round(const ObLogArchiveRound &log_archive_round)
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
    const share::ObBackupDest &backup_dest, const ObBackupSetId backup_set_id,
    const ObBackupDestOpt &backup_dest_option)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup data clean tenant is invalid", K(ret), K(*this));
  } else if (cluster_id < 0 || incarnation < 0 || !backup_dest.is_valid() || !backup_set_id.is_valid() ||
             !backup_dest_option.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup clean element get invalid argument",
        K(ret),
        K(cluster_id),
        K(incarnation),
        K(backup_dest),
        K(backup_set_id),
        K(backup_dest_option));
  } else {
    bool found_same_elemnt = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_element_array_.count() && !found_same_elemnt; ++i) {
      ObBackupDataCleanElement &backup_element = backup_element_array_.at(i);
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
      backup_element.backup_dest_option_ = backup_dest_option;
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
    const share::ObBackupDest &backup_dest, const ObLogArchiveRound &archive_round,
    const ObBackupDestOpt &backup_dest_option)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup data clean tenant is invalid", K(ret), K(*this));
  } else if (cluster_id < 0 || incarnation < 0 || !backup_dest.is_valid() || !archive_round.is_valid() ||
             !backup_dest_option.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup clean element get invalid argument",
        K(ret),
        K(cluster_id),
        K(incarnation),
        K(backup_dest),
        K(archive_round),
        K(backup_dest_option));
  } else {
    bool found_same_elemnt = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_element_array_.count() && !found_same_elemnt; ++i) {
      ObBackupDataCleanElement &backup_element = backup_element_array_.at(i);
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
      backup_element.backup_dest_option_ = backup_dest_option;
      if (OB_FAIL(backup_element.log_archive_round_array_.push_back(archive_round))) {
        LOG_WARN("failed to push backup set id into array", K(ret), K(archive_round));
      } else if (OB_FAIL(backup_element_array_.push_back(backup_element))) {
        LOG_WARN("failed to push backup element", K(ret), K(backup_element));
      }
    }
  }
  return ret;
}

bool ObBackupDataCleanTenant::has_clean_backup_set(const int64_t backup_set_id, const int64_t copy_id) const
{
  bool b_ret = false;
  for (int64_t i = 0; i < backup_element_array_.count(); ++i) {
    const ObBackupDataCleanElement &backup_element = backup_element_array_.at(i);
    for (int64_t j = 0; j < backup_element.backup_set_id_array_.count(); ++j) {
      const int64_t tmp_backup_set_id = backup_element.backup_set_id_array_.at(j).backup_set_id_;
      const int64_t tmp_copy_id = backup_element.backup_set_id_array_.at(j).copy_id_;
      if (tmp_backup_set_id == backup_set_id && tmp_copy_id == copy_id) {
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

ObBackupDataCleanStatics &ObBackupDataCleanStatics::operator+=(const ObBackupDataCleanStatics &clean_statics)
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

ObSimplePieceKey::ObSimplePieceKey() : incarnation_(0), round_id_(0), backup_piece_id_(0), copy_id_(0)
{}

void ObSimplePieceKey::reset()
{
  incarnation_ = 0;
  round_id_ = 0;
  backup_piece_id_ = 0;
  copy_id_ = 0;
}

bool ObSimplePieceKey::is_valid() const
{
  return incarnation_ > 0 && round_id_ > 0 && backup_piece_id_ >= 0 && copy_id_ >= 0;
}

uint64_t ObSimplePieceKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&incarnation_, sizeof(incarnation_), hash_val);
  hash_val = murmurhash(&round_id_, sizeof(round_id_), hash_val);
  hash_val = murmurhash(&backup_piece_id_, sizeof(backup_piece_id_), hash_val);
  hash_val = murmurhash(&copy_id_, sizeof(copy_id_), hash_val);
  return hash_val;
}

bool ObSimplePieceKey::operator==(const ObSimplePieceKey &other) const
{
  return (incarnation_ == other.incarnation_ && round_id_ == other.round_id_ &&
          backup_piece_id_ == other.backup_piece_id_ && copy_id_ == other.copy_id_);
}

ObSimpleArchiveRound::ObSimpleArchiveRound() : incarnation_(0), round_id_(0), copy_id_(0)
{}

void ObSimpleArchiveRound::reset()
{
  incarnation_ = 0;
  round_id_ = 0;
  copy_id_ = 0;
}

bool ObSimpleArchiveRound::is_valid() const
{
  return incarnation_ > 0 && round_id_ > 0 && copy_id_ >= 0;
}

uint64_t ObSimpleArchiveRound::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&incarnation_, sizeof(incarnation_), hash_val);
  hash_val = murmurhash(&round_id_, sizeof(round_id_), hash_val);
  hash_val = murmurhash(&copy_id_, sizeof(copy_id_), hash_val);
  return hash_val;
}

bool ObSimpleArchiveRound::operator==(const ObSimpleArchiveRound &other) const
{
  return (incarnation_ == other.incarnation_ && round_id_ == other.round_id_ && copy_id_ == other.copy_id_);
}

int ObBackupDataCleanUtil::get_backup_path_info(const ObBackupDest &backup_dest, const int64_t incarnation,
    const uint64_t tenant_id, const int64_t full_backup_set_id, const int64_t inc_backup_set_id,
    const int64_t backup_date, const int64_t compatible, ObBackupBaseDataPathInfo &path_info)
{
  int ret = OB_SUCCESS;
  if (!backup_dest.is_valid() || incarnation <= 0 || OB_INVALID_ID == tenant_id || full_backup_set_id <= 0 ||
      inc_backup_set_id <= 0 || full_backup_set_id > inc_backup_set_id || backup_date < 0 || compatible < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup path info get invalid argument",
        K(ret),
        K(backup_dest),
        K(incarnation),
        K(tenant_id),
        K(full_backup_set_id),
        K(inc_backup_set_id),
        K(backup_date),
        K(compatible));
  } else if (OB_FAIL(path_info.dest_.set(backup_dest, incarnation))) {
    LOG_WARN("failed to set path info", K(ret), K(backup_dest));
  } else {
    path_info.full_backup_set_id_ = full_backup_set_id;
    path_info.inc_backup_set_id_ = inc_backup_set_id;
    path_info.tenant_id_ = tenant_id;
    path_info.backup_date_ = backup_date;
    path_info.compatible_ = compatible;
  }
  return ret;
}

int ObBackupDataCleanUtil::touch_backup_dir_files(const ObBackupPath &path, const char *storage_info,
    const common::ObStorageType &device_type, ObBackupDataCleanStatics &clean_statics,
    share::ObIBackupLeaseService &lease_service)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::BACKUP);
  ObArray<common::ObString> index_file_names;
  ObStorageUtil util(false /*need retry*/);
  const int64_t start_ts = ObTimeUtil::current_time();
  UNUSED(device_type);

  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("touch backup file get invalid argument", K(ret), K(path), KP(storage_info));
  } else if (OB_FAIL(util.list_files(path.get_ptr(), storage_info, allocator, index_file_names))) {
    LOG_WARN("failed to list files", K(ret), K(path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_file_names.count(); ++i) {
      const ObString &file_name = index_file_names.at(i);
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

int ObBackupDataCleanUtil::delete_backup_dir_files(const ObBackupPath &path, const char *storage_info,
    const common::ObStorageType &device_type, ObBackupDataCleanStatics &clean_statics,
    share::ObIBackupLeaseService &lease_service)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::BACKUP);
  ObArray<common::ObString> index_file_names;
  ObStorageUtil util(false /*need retry*/);
  const int64_t start_ts = ObTimeUtil::current_time();
  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("touch backup file get invalid argument", K(ret), K(path), KP(storage_info));
  } else if (OB_FAIL(util.list_files(path.get_ptr(), storage_info, allocator, index_file_names))) {
    LOG_WARN("failed to list files", K(ret), K(path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_file_names.count(); ++i) {
      const ObString &file_name = index_file_names.at(i);
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

int ObBackupDataCleanUtil::touch_clog_dir_files(const ObBackupPath &path, const char *storage_info,
    const uint64_t file_id, const common::ObStorageType &device_type, ObBackupDataCleanStatics &clean_statics,
    share::ObIBackupLeaseService &lease_service, bool &has_remaining_files)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::BACKUP);
  ObArray<common::ObString> index_file_names;
  ObStorageUtil util(false /*need retry*/);
  const int64_t start_ts = ObTimeUtil::current_time();
  int64_t touched_file_num = 0;
  int64_t tmp_file_id = 0;
  has_remaining_files = true;
  UNUSED(device_type);

  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("touch backup file get invalid argument", K(ret), K(path), KP(storage_info), K(file_id));
  } else if (OB_FAIL(util.list_files(path.get_ptr(), storage_info, allocator, index_file_names))) {
    LOG_WARN("failed to list files", K(ret), K(path));
  } else if (index_file_names.empty()) {
    has_remaining_files = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_file_names.count(); ++i) {
      const ObString &file_name = index_file_names.at(i);
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

int ObBackupDataCleanUtil::delete_clog_dir_files(const ObBackupPath &path, const char *storage_info,
    const uint64_t file_id, const common::ObStorageType &device_type, ObBackupDataCleanStatics &clean_statics,
    share::ObIBackupLeaseService &lease_service, bool &has_remaning_files)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::BACKUP);
  ObArray<common::ObString> data_file_names;
  ObStorageUtil util(false /*need retry*/);
  const int64_t start_ts = ObTimeUtil::current_time();
  int64_t deleted_file_num = 0;
  int64_t tmp_file_id = 0;
  has_remaning_files = true;
  UNUSED(device_type);

  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("touch backup file get invalid argument", K(ret), K(path), KP(storage_info), K(file_id));
  } else if (OB_FAIL(util.list_files(path.get_ptr(), storage_info, allocator, data_file_names))) {
    LOG_WARN("failed to list files", K(ret), K(path));
  } else if (data_file_names.empty()) {
    has_remaning_files = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < data_file_names.count(); ++i) {
      const ObString &file_name = data_file_names.at(i);
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

    if (OB_SUCC(ret)) {
      has_remaning_files = deleted_file_num != data_file_names.count();
    }

    clean_statics.deleted_clog_files_ += deleted_file_num;
    clean_statics.deleted_clog_files_ts_ += ObTimeUtil::current_time() - start_ts;
  }
  return ret;
}

int ObBackupDataCleanUtil::delete_backup_dir(
    const ObBackupPath &path, const char *storage_info, const common::ObStorageType &device_type)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::BACKUP);
  ObStorageUtil util(false /*need retry*/);
  UNUSED(device_type);

  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("touch backup file get invalid argument", K(ret), K(path), KP(storage_info));
  } else if (OB_FAIL(util.del_dir(path.get_ptr(), storage_info))) {
    LOG_WARN("failed to del dir time", K(ret), K(path), K(storage_info));
  } else {
    LOG_INFO("delete backup dir", K(path));
  }
  return ret;
}

int ObBackupDataCleanUtil::delete_backup_file(
    const ObBackupPath &path, const char *storage_info, const common::ObStorageType &device_type)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false /*need_retry*/);
  bool is_exist = true;
  UNUSED(device_type);
  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("touch backup file get invalid argument", K(ret), K(path), KP(storage_info));
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
    const ObBackupPath &path, const char *storage_info, const common::ObStorageType &device_type)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(false /*need_retry*/);
  bool is_exist = true;
  UNUSED(device_type);
  if (path.is_empty() || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("touch backup file get invalid argument", K(ret), K(path), KP(storage_info));
  } else if (OB_FAIL(util.is_exist(path.get_ptr(), storage_info, is_exist))) {
    LOG_WARN("failed to check path is exist", K(ret), K(path), K(storage_info), K(is_exist));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(util.update_file_modify_time(path.get_ptr(), storage_info))) {
    LOG_WARN("failed to update file modify time", K(ret), K(path), K(storage_info));
  }
  return ret;
}

int ObBackupDataCleanUtil::get_file_id(const ObString &file_name, int64_t &file_id)
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

int ObBackupDataCleanUtil::delete_tmp_files(const ObBackupPath &path, const char *storage_info)
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

void ObBackupDataCleanUtil::check_need_retry(
    const int64_t result, int64_t &retry_count, int64_t &io_limit_retry_count, bool &need_retry)
{
  need_retry = true;
  if (OB_SUCCESS == result) {
    need_retry = false;
  } else if (OB_IO_LIMIT == result) {
    ++io_limit_retry_count;
    need_retry = true;
  } else if (retry_count < OB_MAX_RETRY_TIMES) {
    ++retry_count;
    need_retry = true;
  } else {
    need_retry = false;
  }
}

ObTenantBackupDataCleanMgr::ObTenantBackupDataCleanMgr()
    : is_inited_(false), clean_info_(), clean_tenant_(), data_clean_(NULL), sql_proxy_(NULL)
{}

ObTenantBackupDataCleanMgr::~ObTenantBackupDataCleanMgr()
{}

int ObTenantBackupDataCleanMgr::init(const ObBackupCleanInfo &clean_info, const ObBackupDataCleanTenant &clean_tenant,
    ObBackupDataClean *data_clean, common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant backup data clean mgr init twice", K(ret));
  } else if (!clean_info.is_valid() || ObBackupCleanInfoStatus::DOING != clean_info.status_ ||
             !clean_tenant.is_valid() || OB_ISNULL(data_clean) || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant backup data clean mgr get invalid argument", K(ret), K(clean_tenant), KP(data_clean));
  } else {
    clean_info_ = clean_info;
    clean_tenant_ = clean_tenant;
    data_clean_ = data_clean;
    sql_proxy_ = sql_proxy;
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
  } else if (OB_FAIL(clog_data_clean_task.init(clean_info_, clean_tenant_, data_clean_, sql_proxy_))) {
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

int ObTenantBackupBaseDataCleanTask::init(const ObBackupDataCleanTenant &clean_tenant, ObBackupDataClean *data_clean)
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
      const ObSimpleBackupDataCleanTenant &simple_clean_tenant = clean_tenant_.simple_clean_tenant_;
      const ObBackupDataCleanElement &backup_clean_element = clean_tenant_.backup_element_array_.at(i);
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
    const ObSimpleBackupDataCleanTenant &simple_clean_tenant, const ObBackupDataCleanElement &clean_element)
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
      const ObBackupSetId &backup_set_id = clean_element.backup_set_id_array_.at(i);
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(clean_backup_data(simple_clean_tenant, clean_element, backup_set_id))) {
        LOG_WARN("failed to clean backup data", K(ret), K(simple_clean_tenant), K(clean_element), K(backup_set_id));
      }
    }
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::clean_backup_data(const ObSimpleBackupDataCleanTenant &simple_clean_tenant,
    const ObBackupDataCleanElement &clean_element, const ObBackupSetId &backup_set_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObExternBackupInfo> extern_backup_infos;
  storage::ObPhyRestoreMetaIndexStore meta_index;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (OB_FAIL(get_tenant_backup_infos(simple_clean_tenant, clean_element, backup_set_id, extern_backup_infos))) {
    LOG_WARN("failed to get tenant backup infos", K(ret), K(backup_set_id), K(simple_clean_tenant), K(clean_element));
  } else {
    LOG_INFO("get extern backup infos", K(extern_backup_infos), K(backup_set_id));

    for (int64_t i = 0; OB_SUCC(ret) && i < extern_backup_infos.count(); ++i) {
      const ObExternBackupInfo &backup_info = extern_backup_infos.at(i);
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(clean_backp_set(simple_clean_tenant, clean_element, backup_set_id, backup_info))) {
        LOG_WARN("failed to clean backup set", K(ret), K(simple_clean_tenant), K(clean_element), K(backup_info));
      } else if (OB_FAIL(try_clean_backup_set_dir(
                     simple_clean_tenant.tenant_id_, clean_element, backup_set_id, backup_info))) {
        LOG_WARN("failed to try clean backup set dir", K(ret), K(backup_set_id), K(simple_clean_tenant));
      }
    }
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::clean_backp_set(const ObSimpleBackupDataCleanTenant &simple_clean_tenant,
    const ObBackupDataCleanElement &clean_element, const ObBackupSetId &backup_set_id,
    const ObExternBackupInfo &extern_backup_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupMetaIndex> meta_index_array;
  const uint64_t tenant_id = simple_clean_tenant.tenant_id_;
  ObBackupDataMgr backup_data_mgr;
  ObClusterBackupDest cluster_backup_dest;
  ObArray<int64_t> table_id_array;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (!simple_clean_tenant.is_valid() || !clean_element.is_valid() || !backup_set_id.is_valid() ||
             !extern_backup_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("clean backup dest get invalid argument",
        K(ret),
        K(simple_clean_tenant),
        K(clean_element),
        K(backup_set_id),
        K(extern_backup_info));
  } else if (OB_FAIL(cluster_backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set cluster backup destt", K(ret), K(clean_element));
  } else if (OB_FAIL(backup_data_mgr.init(cluster_backup_dest, tenant_id, extern_backup_info))) {
    LOG_WARN("failed to init backup data mgr", K(ret), K(cluster_backup_dest), K(extern_backup_info));
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
                     table_id, clean_element, backup_set_id, extern_backup_info, meta_index_array, *data_clean_))) {
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
            K(extern_backup_info),
            K(clean_statics));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(clean_backup_set_meta(simple_clean_tenant, clean_element, backup_set_id, extern_backup_info))) {
        LOG_WARN("failed to clean backup set meta",
            K(ret),
            K(simple_clean_tenant),
            K(clean_element),
            K(backup_set_id),
            K(extern_backup_info));
      }
    }
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::get_tenant_backup_infos(const ObSimpleBackupDataCleanTenant &simple_clean_tenant,
    const ObBackupDataCleanElement &clean_element, const ObBackupSetId &backup_set_id,
    common::ObIArray<ObExternBackupInfo> &extern_backup_infos)
{
  int ret = OB_SUCCESS;
  ObExternBackupInfoMgr extern_backup_info_mgr;
  ObClusterBackupDest backup_dest;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (OB_FAIL(backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(clean_element));
  } else if (OB_FAIL(extern_backup_info_mgr.init(
                 simple_clean_tenant.tenant_id_, backup_dest, *data_clean_->get_backup_lease_service()))) {
    LOG_WARN("failed to init extern backup info mgr", K(ret), K(simple_clean_tenant), K(backup_dest));
  } else if (OB_FAIL(
                 extern_backup_info_mgr.get_extern_backup_infos(backup_set_id.backup_set_id_, extern_backup_infos))) {
    LOG_WARN("failed to get extern full backup infos", K(ret), K(simple_clean_tenant), K(backup_set_id));
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::clean_backup_set_meta(const ObSimpleBackupDataCleanTenant &simple_clean_tenant,
    const ObBackupDataCleanElement &clean_element, const ObBackupSetId &backup_set_id,
    const ObExternBackupInfo &extern_backup_info)
{
  int ret = OB_SUCCESS;
  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;
  const int64_t full_backup_set_id = extern_backup_info.full_backup_set_id_;
  const int64_t inc_backup_set_id = extern_backup_info.inc_backup_set_id_;
  const uint64_t tenant_id = simple_clean_tenant.tenant_id_;
  const ObBackupDest &backup_dest = clean_element.backup_dest_;
  const int64_t incarnation = clean_element.incarnation_;
  const char *storage_info = clean_element.backup_dest_.storage_info_;
  const int64_t backup_date = extern_backup_info.date_;
  const int64_t compatible = extern_backup_info.compatible_;
  ObClusterBackupDest cluster_backup_dest;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (OB_FAIL(cluster_backup_dest.set(backup_dest, incarnation))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(backup_dest));
  } else if (OB_FAIL(path_info.set(
                 cluster_backup_dest, tenant_id, full_backup_set_id, inc_backup_set_id, backup_date, compatible))) {
    LOG_WARN("failed to set backup path info", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_inc_backup_set_path(path_info, path))) {
    STORAGE_LOG(WARN, "failed to get inc backup path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_tmp_files(path, storage_info))) {
    STORAGE_LOG(WARN, "failed to delete tmp files", K(ret), K(path));
  } else if (ObBackupDataCleanMode::TOUCH == backup_set_id.clean_mode_) {
    if (clean_element.backup_dest_option_.auto_delete_obsolete_backup_) {
      // do nothing
    } else if (OB_FAIL(touch_backup_set_meta(clean_element, path))) {
      LOG_WARN("failed to touch backup set meta", K(ret), K(simple_clean_tenant), K(backup_set_id));
    }
  } else if (ObBackupDataCleanMode::CLEAN == backup_set_id.clean_mode_) {
    if (clean_element.backup_dest_option_.auto_touch_reserved_backup_) {
      // do nothing
    } else if (OB_FAIL(delete_backup_set_meta(clean_element, path))) {
      LOG_WARN("failed to delete backup set meta", K(ret), K(simple_clean_tenant), K(backup_set_id));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("clean mode is invalid", K(ret), K(backup_set_id));
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::touch_backup_set_meta(
    const ObBackupDataCleanElement &clean_element, const ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  const char *storage_info = clean_element.backup_dest_.storage_info_;
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
    const ObBackupDataCleanElement &clean_element, const ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  const char *storage_info = clean_element.backup_dest_.storage_info_;
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
    const storage::ObPhyRestoreMetaIndexStore::MetaIndexMap &meta_index_map, hash::ObHashSet<int64_t> &table_id_set)
{
  int ret = OB_SUCCESS;
  const int overwrite_key = 1;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else {
    storage::ObPhyRestoreMetaIndexStore::MetaIndexMap::const_iterator iter;
    for (iter = meta_index_map.begin(); OB_SUCC(ret) && iter != meta_index_map.end(); ++iter) {
      const ObMetaIndexKey &meta_index_key = iter->first;
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

int ObTenantBackupBaseDataCleanTask::try_clean_backup_set_dir(const uint64_t tenant_id,
    const ObBackupDataCleanElement &clean_element, const ObBackupSetId &backup_set_id,
    const ObExternBackupInfo &extern_backup_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (!backup_set_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("try clean backup set dir get invalid argument", K(ret), K(backup_set_id));
  } else if (OB_FAIL(try_clean_backup_set_info(tenant_id, clean_element, backup_set_id, extern_backup_info))) {
    LOG_WARN("failed to try clean backup set info", K(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(try_clean_backup_set_data_dir(tenant_id, clean_element, backup_set_id, extern_backup_info))) {
    LOG_WARN("failed to try clean backup set data dir", K(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(try_clean_full_backup_set_dir(tenant_id, clean_element, backup_set_id, extern_backup_info))) {
    LOG_WARN("failed to try clean full backup set dir", K(ret), K(tenant_id), K(backup_set_id));
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::try_clean_backup_set_data_dir(const uint64_t tenant_id,
    const ObBackupDataCleanElement &clean_element, const ObBackupSetId &backup_set_id,
    const ObExternBackupInfo &extern_backup_info)
{
  int ret = OB_SUCCESS;
  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;
  const int64_t full_backup_set_id = extern_backup_info.full_backup_set_id_;
  const int64_t inc_backup_set_id = extern_backup_info.inc_backup_set_id_;
  const int64_t backup_date = extern_backup_info.date_;
  const int64_t compatible = extern_backup_info.compatible_;
  const ObBackupDest &backup_dest = clean_element.backup_dest_;
  const int64_t incarnation = clean_element.incarnation_;
  const char *storage_info = clean_element.backup_dest_.storage_info_;
  const ObStorageType &device_type = clean_element.backup_dest_.device_type_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (ObBackupDataCleanMode::TOUCH == backup_set_id.clean_mode_) {
    // do nothing
  } else if (OB_FAIL(ObBackupDataCleanUtil::get_backup_path_info(backup_dest,
                 incarnation,
                 tenant_id,
                 full_backup_set_id,
                 inc_backup_set_id,
                 backup_date,
                 compatible,
                 path_info))) {
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

int ObTenantBackupBaseDataCleanTask::try_clean_backup_set_info(const uint64_t tenant_id,
    const ObBackupDataCleanElement &clean_element, const ObBackupSetId &backup_set_id,
    const ObExternBackupInfo &extern_backup_info)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest backup_dest;
  ObExternBackupSetInfoMgr extern_backup_set_info_mgr;
  ObExternSingleBackupSetInfoMgr extern_single_backup_set_info_mgr;
  const int64_t full_backup_set_id = extern_backup_info.full_backup_set_id_;
  const int64_t inc_backup_set_id = extern_backup_info.inc_backup_set_id_;
  const int64_t backup_date = extern_backup_info.date_;
  const int64_t compatible = extern_backup_info.compatible_;
  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (OB_FAIL(backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(clean_element));
  } else if (OB_FAIL(path_info.set(
                 backup_dest, tenant_id, full_backup_set_id, inc_backup_set_id, backup_date, compatible))) {
    LOG_WARN("failed to set backup path info", K(ret), K(backup_dest), K(tenant_id), K(extern_backup_info));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_full_backup_set_path(path_info, path))) {
    LOG_WARN("failed to get tenant date full backup set path", K(ret), K(path_info));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_tmp_files(path, backup_dest.get_storage_info()))) {
    LOG_WARN("failed to delete tmp files", K(ret), K(path), K(backup_dest));
  } else if (OB_FAIL(extern_backup_set_info_mgr.init(tenant_id,
                 full_backup_set_id,
                 inc_backup_set_id,
                 backup_dest,
                 backup_date,
                 compatible,
                 *data_clean_->get_backup_lease_service()))) {
    LOG_WARN("failed to init extern backup set info mgr", K(ret), K(backup_dest), K(full_backup_set_id));
  } else if (compatible > ObBackupCompatibleVersion::OB_BACKUP_COMPATIBLE_VERSION_V1 &&
             OB_FAIL(extern_single_backup_set_info_mgr.init(tenant_id,
                 full_backup_set_id,
                 inc_backup_set_id,
                 backup_date,
                 backup_dest,
                 *data_clean_->get_backup_lease_service()))) {
    LOG_WARN("failed to init extern single backup set info mgr", K(ret), K(tenant_id), K(extern_backup_info));
  } else if (ObBackupDataCleanMode::TOUCH == backup_set_id.clean_mode_) {
    if (clean_element.backup_dest_option_.auto_delete_obsolete_backup_) {
      // do nothing
    } else if (OB_FAIL(extern_backup_set_info_mgr.touch_extern_backup_set_info())) {
      LOG_WARN("failed to touch backup file", K(ret), K(backup_dest));
    } else if (compatible > ObBackupCompatibleVersion::OB_BACKUP_COMPATIBLE_VERSION_V1 &&
               OB_FAIL(extern_single_backup_set_info_mgr.touch_extern_backup_set_file_info())) {
      LOG_WARN("failed to touch extern backup set file info", K(ret), K(backup_dest));
    }
  } else if (ObBackupDataCleanMode::CLEAN == backup_set_id.clean_mode_) {
    if (clean_element.backup_dest_option_.auto_touch_reserved_backup_) {
      // do nothing
    } else if (OB_FAIL(extern_backup_set_info_mgr.delete_extern_backup_set_info())) {
      LOG_WARN("failed to delete backup file", K(ret), K(backup_dest));
    } else if (compatible > ObBackupCompatibleVersion::OB_BACKUP_COMPATIBLE_VERSION_V1 &&
               OB_FAIL(extern_single_backup_set_info_mgr.delete_extern_backup_set_file_info())) {
      LOG_WARN("failed to delete extern backup set file info", K(ret), K(backup_dest));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("try clean backup set info's clean mode is not supported", K(ret), K(backup_set_id));
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::try_clean_full_backup_set_dir(const uint64_t tenant_id,
    const ObBackupDataCleanElement &clean_element, const ObBackupSetId &backup_set_id,
    const ObExternBackupInfo &extern_backup_info)
{
  int ret = OB_SUCCESS;
  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;
  const int64_t full_backup_set_id = extern_backup_info.full_backup_set_id_;
  const int64_t inc_backup_set_id = extern_backup_info.inc_backup_set_id_;
  const int64_t backup_date = extern_backup_info.date_;
  const int64_t compatible = extern_backup_info.compatible_;
  const ObBackupDest &backup_dest = clean_element.backup_dest_;
  const int64_t incarnation = clean_element.incarnation_;
  const char *storage_info = clean_element.backup_dest_.storage_info_;
  const ObStorageType &device_type = clean_element.backup_dest_.device_type_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup base data clean task do not init", K(ret));
  } else if (ObBackupDataCleanMode::TOUCH == backup_set_id.clean_mode_) {
    // do nothing
  } else if (OB_FAIL(ObBackupDataCleanUtil::get_backup_path_info(backup_dest,
                 incarnation,
                 tenant_id,
                 full_backup_set_id,
                 inc_backup_set_id,
                 backup_date,
                 compatible,
                 path_info))) {
    LOG_WARN("failed to get backup path info", K(ret), K(clean_element), K(tenant_id));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_full_backup_set_path(path_info, path))) {
    LOG_WARN("failed to get tenant data full backup set path", K(ret), K(path_info));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(path), K(storage_info));
  }
  return ret;
}

int ObTenantBackupBaseDataCleanTask::get_clean_statics(ObBackupDataCleanStatics &clean_statics)
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
    : is_inited_(false), clean_tenant_(), pkey_set_(), clean_statics_(), data_clean_(NULL), sql_proxy_(NULL)
{}

ObTenantBackupClogDataCleanTask::~ObTenantBackupClogDataCleanTask()
{}

int ObTenantBackupClogDataCleanTask::init(const ObBackupCleanInfo &clean_info,
    const ObBackupDataCleanTenant &clean_tenant, ObBackupDataClean *data_clean, common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant backup clog data clean task init twice", K(ret));
  } else if (!clean_info.is_valid() || !clean_tenant.is_valid() || OB_ISNULL(data_clean) || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant backup clog data clean task get invalid argument",
        K(ret),
        K(clean_info),
        K(clean_tenant),
        KP(data_clean),
        KP(sql_proxy));
  } else if (OB_FAIL(pkey_set_.create(MAX_BUCKET_NUM))) {
    LOG_WARN("failed to create pkey set", K(ret));
  } else {
    clean_info_ = clean_info;
    clean_tenant_ = clean_tenant;
    data_clean_ = data_clean;
    sql_proxy_ = sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::do_clean()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();
  int64_t start_replay_log_ts = 0;
  LOG_INFO("tenant backup clog data clean task do clean", K(clean_tenant_.simple_clean_tenant_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup clog data clean task do not init", K(ret));
  } else if (clean_tenant_.clog_data_clean_point_.is_valid()) {
    start_replay_log_ts = clean_tenant_.clog_data_clean_point_.start_replay_log_ts_;
  } else {
    start_replay_log_ts = clean_tenant_.clog_gc_snapshot_;
  }

  if (OB_FAIL(ret)) {
  } else {
    const ObSimpleBackupDataCleanTenant &simple_clean_tenant = clean_tenant_.simple_clean_tenant_;
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_tenant_.backup_element_array_.count(); ++i) {
      const ObBackupDataCleanElement &backup_clean_element = clean_tenant_.backup_element_array_.at(i);
      pkey_set_.reuse();

      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(do_inner_clean(simple_clean_tenant, backup_clean_element, start_replay_log_ts))) {
        LOG_WARN("failed to do inner clean",
            K(ret),
            K(simple_clean_tenant),
            K(backup_clean_element),
            K(start_replay_log_ts));
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

int ObTenantBackupClogDataCleanTask::do_inner_clean(const ObSimpleBackupDataCleanTenant &simple_clean_tenant,
    const ObBackupDataCleanElement &clean_element, const int64_t start_replay_log_ts)
{
  int ret = OB_SUCCESS;
  ObBackupListDataMgr list_data_mgr;
  ObClusterBackupDest cluster_backup_dest;
  ObArray<ObPartitionKey> pg_keys;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup clog data clean task do not init", K(ret));
  } else if (!simple_clean_tenant.is_valid() || !clean_element.is_valid() || start_replay_log_ts < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner do clean get invalid argument", K(ret), K(simple_clean_tenant), K(clean_element));
  } else if (OB_FAIL(cluster_backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set cluster backup destt", K(ret), K(clean_element));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_element.log_archive_round_array_.count(); ++i) {
      const ObLogArchiveRound &log_archive_round = clean_element.log_archive_round_array_.at(i);
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (clean_info_.is_delete_backup_set() && 0 != log_archive_round.start_piece_id_) {
        // do nothing
      } else if (OB_FAIL(generate_backup_piece_tasks(
                     simple_clean_tenant, clean_element, log_archive_round, start_replay_log_ts))) {
        LOG_WARN("failed to generate backup piece tasks", K(ret), K(simple_clean_tenant), K(log_archive_round));
      }
    }
  }
  return ret;
}

/*delete it later*/
int ObTenantBackupClogDataCleanTask::do_inner_clean(const ObSimpleBackupDataCleanTenant &simple_clean_tenant,
    const ObBackupDataCleanElement &clean_element, const ObTenantBackupTaskInfo &clog_data_clean_point)
{
  int ret = OB_SUCCESS;
  // const uint64_t tenant_id = simple_clean_tenant.tenant_id_;
  // const int64_t full_backup_set_id = clog_data_clean_point.backup_set_id_;
  // const int64_t inc_backup_set_id = clog_data_clean_point.backup_set_id_;
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
    //} else if (OB_FAIL(backup_data_mgr.init(cluster_backup_dest,
    //    tenant_id, full_backup_set_id, inc_backup_set_id))) {
    //  LOG_WARN("failed to init backup data mgr", K(ret), K(cluster_backup_dest),
    //      K(full_backup_set_id), K(inc_backup_set_id));
  } else if (OB_FAIL(backup_data_mgr.get_base_data_table_id_list(table_id_array))) {
    LOG_WARN("failed ot get base data table id list", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clean_element.log_archive_round_array_.count(); ++i) {
      const ObLogArchiveRound &log_archive_round = clean_element.log_archive_round_array_.at(i);
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

/*delete it later*/
int ObTenantBackupClogDataCleanTask::clean_clog_data(const ObSimpleBackupDataCleanTenant &simple_clean_tenant,
    const ObBackupDataCleanElement &clean_element, const ObTenantBackupTaskInfo &clog_data_clean_point,
    const ObLogArchiveRound &log_archive_round, const common::ObIArray<int64_t> &table_id_array,
    ObBackupDataMgr &backup_data_mgr)
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

/*delete it later*/
int ObTenantBackupClogDataCleanTask::set_partition_into_set(const ObIArray<ObBackupMetaIndex> &meta_index_array)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    LOG_WARN("tenant backup clog data clean mgr do not init", K(ret));
  } else {
    const int flag = 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_index_array.count(); ++i) {
      const ObBackupMetaIndex &meta_index = meta_index_array.at(i);
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

/*delete it later*/
int ObTenantBackupClogDataCleanTask::check_and_delete_clog_data(
    const ObSimpleBackupDataCleanTenant &simple_clean_tenant, const ObBackupDataCleanElement &backup_clean_element,
    const int64_t clog_gc_snapshot)
{
  int ret = OB_SUCCESS;
  const ObBackupDest &backup_dest = backup_clean_element.backup_dest_;
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
      const ObLogArchiveRound &log_archive_round = backup_clean_element.log_archive_round_array_.at(i);
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

/*delete it later*/
int ObTenantBackupClogDataCleanTask::check_and_delete_clog_data_with_round(
    const ObSimpleBackupDataCleanTenant &simple_clean_tenant, const ObClusterBackupDest &cluster_backup_dest,
    const ObLogArchiveRound &log_archive_round, const int64_t max_clean_clog_snapshot)
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
    // TODO(muwei)
    const int64_t fake_piece_id = 0;
    const int64_t fake_piece_create_date = 0;
    ObSimplePieceInfo fake_backup_piece_info;
    for (int64_t i = 0; OB_SUCC(ret) && i < pkey_list.count(); ++i) {
      const ObPartitionKey &pkey = pkey_list.at(i);
      ObPartitionClogDataCleanMgr clog_data_clean_mgr;
      archive::ObArchiveClear archive_clear;
      uint64_t data_file_id = 0;
      uint64_t index_file_id = 0;
      const int64_t unused_retention_timestamp = INT64_MAX;
      const bool is_backup_backup = false;
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(archive_clear.get_clean_max_clog_file_id_by_log_ts(cluster_backup_dest,
                     archive_round,
                     fake_piece_id,
                     fake_piece_create_date,
                     pkey,
                     max_clean_clog_snapshot,
                     unused_retention_timestamp,
                     index_file_id,
                     data_file_id))) {
        LOG_WARN("failed to get clean max clog file id by log ts",
            K(ret),
            K(pkey),
            K(log_archive_round),
            K(max_clean_clog_snapshot));
      } else if (OB_FAIL(clog_data_clean_mgr.init(cluster_backup_dest,
                     fake_backup_piece_info,
                     pkey,
                     data_file_id,
                     index_file_id,
                     is_backup_backup,
                     *data_clean_))) {
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

/*delete it later*/
int ObTenantBackupClogDataCleanTask::get_clog_pkey_list_not_in_base_data(const ObClusterBackupDest &cluster_backup_dest,
    const int64_t log_archive_round, const uint64_t tenant_id, common::ObIArray<ObPartitionKey> &pkey_list)
{
  int ret = OB_SUCCESS;
  ObBackupListDataMgr backup_list_data_mgr;
  ObArray<ObPartitionKey> clog_pkey_list;
  UNUSED(cluster_backup_dest);
  UNUSED(log_archive_round);
  UNUSED(tenant_id);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup data clean mgr do not init", K(ret));
    //} else if (OB_FAIL(backup_list_data_mgr.init(
    //    cluster_backup_dest, log_archive_round, tenant_id))) {
    //  LOG_WARN("failed to init backup list data mgr", K(ret),
    //      K(cluster_backup_dest), K(log_archive_round));
  } else if (OB_FAIL(backup_list_data_mgr.get_clog_pkey_list(clog_pkey_list))) {
    LOG_WARN("failed to get clog pkey list", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < clog_pkey_list.count(); ++i) {
      const ObPartitionKey &pkey = clog_pkey_list.at(i);
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

/*delete it later*/
int ObTenantBackupClogDataCleanTask::clean_interrputed_clog_data(
    const ObSimpleBackupDataCleanTenant &simple_clean_tenant, const ObBackupDataCleanElement &clean_element,
    const ObLogArchiveRound &log_archive_round)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = simple_clean_tenant.tenant_id_;
  const int64_t round = log_archive_round.log_archive_round_;
  const char *storage_info = clean_element.backup_dest_.storage_info_;
  const ObStorageType &storage_type = clean_element.backup_dest_.device_type_;
  ObClusterBackupDest cluster_backup_dest;
  ObBackupListDataMgr backup_list_data_mgr;
  ObArray<ObPartitionKey> clog_pkey_list;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup data clean mgr do not init", K(ret));
  } else if (ObLogArchiveStatus::STOP != log_archive_round.log_archive_status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("clean interrputed clog data get invalid argument", K(ret), K(log_archive_round));
  } else if (data_clean_->is_update_reserved_backup_timestamp()) {
    LOG_INFO("update reserved backup timestamp, skip clean interrputed clog data", K(log_archive_round));
  } else if (OB_FAIL(cluster_backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(clean_element));
    // } else if (OB_FAIL(backup_list_data_mgr.init(
    //     cluster_backup_dest, round, tenant_id))) {
    //   LOG_WARN("failed to init backup list data mgr", K(ret),
    //       K(cluster_backup_dest), K(log_archive_round));
  } else if (OB_FAIL(backup_list_data_mgr.get_clog_pkey_list(clog_pkey_list))) {
    LOG_WARN("failed to get clog pkey list", K(ret), K(cluster_backup_dest));
  } else {
    LOG_INFO("start clean interruped clog data", K(clean_element), K(log_archive_round));
    const uint64_t data_file_id = UINT64_MAX;
    const uint64_t index_file_id = UINT64_MAX;
    ObSimplePieceInfo fake_backup_piece_info;
    const bool is_backup_backup = false;

    for (int64_t i = 0; OB_SUCC(ret) && i < clog_pkey_list.count(); ++i) {
      const ObPartitionKey &pkey = clog_pkey_list.at(i);
      ObPartitionClogDataCleanMgr partition_clog_data_clean_mgr;
      ObBackupDataCleanStatics clean_statics;
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(partition_clog_data_clean_mgr.init(cluster_backup_dest,
                     fake_backup_piece_info,
                     pkey,
                     data_file_id,
                     index_file_id,
                     is_backup_backup,
                     *data_clean_))) {
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

int ObTenantBackupClogDataCleanTask::try_clean_table_clog_data_dir(const ObClusterBackupDest &cluster_backup_dest,
    const uint64_t tenant_id, const int64_t log_archive_round, const int64_t table_id, const char *storage_info,
    const common::ObStorageType &device_type)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  const int64_t fake_piece_id = 0;
  const int64_t fake_piece_create_date = 0;

  if (!cluster_backup_dest.is_valid() || log_archive_round <= 0 || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("try clean clog data dir get invlaid argument", K(ret), K(cluster_backup_dest), KP(storage_info));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_data_path(
                 cluster_backup_dest, tenant_id, log_archive_round, fake_piece_id, fake_piece_create_date, path))) {
    LOG_WARN("fail to get meta file path", K(ret));
  } else if (OB_FAIL(path.join(table_id))) {
    LOG_WARN("failed to join path", K(ret), K(table_id));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(storage_info), K(device_type));
  } else if (FALSE_IT(path.reset())) {
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_index_path(
                 cluster_backup_dest, tenant_id, log_archive_round, fake_piece_id, fake_piece_create_date, path))) {
    LOG_WARN("fail to get meta file path", K(ret));
  } else if (OB_FAIL(path.join(table_id))) {
    LOG_WARN("failed to join path", K(ret), K(table_id));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(storage_info), K(device_type));
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::try_clean_clog_data_dir(const ObClusterBackupDest &cluster_backup_dest,
    const uint64_t tenant_id, const int64_t log_archive_round, const char *storage_info,
    const common::ObStorageType &device_type)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  // TODO(muwei)
  const int64_t fake_piece_id = 0;
  const int64_t fake_piece_create_date = 0;

  if (!cluster_backup_dest.is_valid() || log_archive_round <= 0 || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("try clean clog data dir get invlaid argument", K(ret), K(cluster_backup_dest), KP(storage_info));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_data_path(
                 cluster_backup_dest, tenant_id, log_archive_round, fake_piece_id, fake_piece_create_date, path))) {
    LOG_WARN("fail to get meta file path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(storage_info), K(device_type));
  } else if (FALSE_IT(path.reset())) {
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_index_path(
                 cluster_backup_dest, tenant_id, log_archive_round, fake_piece_id, fake_piece_create_date, path))) {
    LOG_WARN("fail to get meta file path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(storage_info), K(device_type));
  } else if (FALSE_IT(path.reset())) {
  } else if (OB_FAIL(ObBackupPathUtil::get_cluster_clog_prefix_path(
                 cluster_backup_dest, tenant_id, log_archive_round, fake_piece_id, fake_piece_create_date, path))) {
    LOG_WARN("failed to get cluster clog prefix path", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(storage_info), K(device_type));
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::get_clean_statics(ObBackupDataCleanStatics &clean_statics)
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

int ObTenantBackupClogDataCleanTask::generate_backup_piece_tasks(
    const ObSimpleBackupDataCleanTenant &simple_clean_tenant, const ObBackupDataCleanElement &clean_element,
    const ObLogArchiveRound &log_archive_round, const int64_t start_replay_log_ts)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup data clean mgr do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < log_archive_round.piece_infos_.count(); ++i) {
      const ObSimplePieceInfo &backup_piece_info = log_archive_round.piece_infos_.at(i);
      if (OB_FAIL(generate_backup_piece_pg_tasks(
              simple_clean_tenant, clean_element, log_archive_round, backup_piece_info, start_replay_log_ts))) {
        LOG_WARN("failed to generate backup piece pg tasks",
            K(ret),
            K(backup_piece_info),
            K(start_replay_log_ts),
            K(log_archive_round));
      }
    }
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::generate_backup_piece_pg_tasks(
    const ObSimpleBackupDataCleanTenant &simple_clean_tenant, const ObBackupDataCleanElement &clean_element,
    const ObLogArchiveRound &log_archive_round, const ObSimplePieceInfo &backup_piece_info,
    const int64_t start_replay_log_ts)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest cluster_backup_dest;
  ObBackupListDataMgr list_data_mgr;
  ObArray<ObPartitionKey> pg_keys;
  ObBackupDeleteClogMode delete_clog_mode;
  const int64_t tenant_id = simple_clean_tenant.tenant_id_;
  const int64_t backup_copies = clean_element.backup_dest_option_.backup_copies_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup data clean mgr do not init", K(ret));
  } else if (OB_FAIL(cluster_backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(clean_element));
  } else if (0 == log_archive_round.start_piece_id_) {
    if (ObLogArchiveStatus::STOP == log_archive_round.log_archive_status_) {
      if (log_archive_round.copies_num_ < backup_copies || 0 == start_replay_log_ts) {
        delete_clog_mode.mode_ = ObBackupDeleteClogMode::NONE;
      } else if (start_replay_log_ts < log_archive_round.checkpoint_ts_) {
        delete_clog_mode.mode_ = ObBackupDeleteClogMode::DELETE_ARCHIVE_LOG;
      } else {
        delete_clog_mode.mode_ = ObBackupDeleteClogMode::DELETE_BACKUP_PIECE;
      }
    } else if ((ObLogArchiveStatus::DOING == log_archive_round.log_archive_status_ ||
                   ObLogArchiveStatus::INTERRUPTED == log_archive_round.log_archive_status_) &&
               start_replay_log_ts < log_archive_round.checkpoint_ts_ && start_replay_log_ts > 0 &&
               log_archive_round.copies_num_ >= backup_copies) {
      delete_clog_mode.mode_ = ObBackupDeleteClogMode::DELETE_ARCHIVE_LOG;
    } else {
      delete_clog_mode.mode_ = ObBackupDeleteClogMode::NONE;
    }
  } else if (ObBackupPieceStatus::BACKUP_PIECE_FROZEN == backup_piece_info.status_) {
    if (ObBackupFileStatus::BACKUP_FILE_DELETED == backup_piece_info.file_status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup file status is unexpected", K(ret), K(backup_piece_info));
    } else if (ObBackupFileStatus::BACKUP_FILE_COPYING == backup_piece_info.file_status_) {
      delete_clog_mode.mode_ = ObBackupDeleteClogMode::NONE;
    } else if (backup_piece_info.max_ts_ > start_replay_log_ts || backup_piece_info.copies_num_ < backup_copies) {
      delete_clog_mode.mode_ = ObBackupDeleteClogMode::NONE;
    } else {
      delete_clog_mode.mode_ = ObBackupDeleteClogMode::DELETE_BACKUP_PIECE;
    }
  } else {
    delete_clog_mode.mode_ = ObBackupDeleteClogMode::NONE;
  }

  if (OB_SUCC(ret)) {
    if ((ObBackupDeleteClogMode::DELETE_BACKUP_PIECE == delete_clog_mode.mode_ &&
            clean_element.backup_dest_option_.auto_touch_reserved_backup_) ||
        (ObBackupDeleteClogMode::NONE == delete_clog_mode.mode_ &&
            clean_element.backup_dest_option_.auto_delete_obsolete_backup_)) {
      // do nothing
    } else if (OB_FAIL(list_data_mgr.init(cluster_backup_dest,
                   log_archive_round.log_archive_round_,
                   simple_clean_tenant.tenant_id_,
                   backup_piece_info.backup_piece_id_,
                   backup_piece_info.create_date_))) {
    } else if (OB_FAIL(list_data_mgr.get_clog_pkey_list(pg_keys))) {
      LOG_WARN("failed to get clog pkey list", K(ret), K(log_archive_round), K(clean_element));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < pg_keys.count(); ++i) {
        const ObPartitionKey &pg_key = pg_keys.at(i);
        if (OB_FAIL(generate_backup_piece_pg_delete_task(simple_clean_tenant,
                clean_element,
                log_archive_round,
                backup_piece_info,
                start_replay_log_ts,
                pg_key,
                delete_clog_mode))) {
          LOG_WARN("failed to generate backup piece pg delete task",
              K(ret),
              K(log_archive_round),
              K(backup_piece_info),
              K(pg_key));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(handle_backup_clog_piece_infos(
                tenant_id, cluster_backup_dest, backup_piece_info, delete_clog_mode, pg_keys))) {
          LOG_WARN("failed to handle backup clog piece infos", K(ret), K(clean_element), K(backup_piece_info));
        }
      }
    }
  }
  return ret;
}

// is clean is false need touch clog data, so data file id and index file is both 0
int ObTenantBackupClogDataCleanTask::generate_backup_piece_pg_delete_task(
    const ObSimpleBackupDataCleanTenant &simple_clean_tenant, const ObBackupDataCleanElement &clean_element,
    const ObLogArchiveRound &log_archive_round, const ObSimplePieceInfo &backup_piece_info,
    const int64_t start_replay_log_ts, const ObPartitionKey &pg_key, const ObBackupDeleteClogMode &delete_clog_mode)
{
  int ret = OB_SUCCESS;
  ObPartitionClogDataCleanMgr partition_clog_data_clean_mgr;
  uint64_t data_file_id = 0;
  uint64_t index_file_id = 0;
  ObClusterBackupDest cluster_backup_dest;
  ObBackupDataCleanStatics clean_statics;
  const bool is_backup_backup = clean_info_.copy_id_ > 0 || clean_info_.is_delete_obsolete_backup_backup();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup data clean mgr do not init", K(ret));
  } else if (OB_FAIL(data_clean_->check_can_do_task())) {
    LOG_WARN("failed to check can do task", K(ret));
  } else if (OB_FAIL(cluster_backup_dest.set(clean_element.backup_dest_, clean_element.incarnation_))) {
    LOG_WARN("failed to set cluster backup dest", K(ret), K(clean_element));
  } else if (OB_FAIL(get_clog_file_id(cluster_backup_dest,
                 log_archive_round,
                 backup_piece_info,
                 delete_clog_mode,
                 start_replay_log_ts,
                 pg_key,
                 data_file_id,
                 index_file_id))) {
    LOG_WARN(
        "failed to get clog file id", K(ret), K(cluster_backup_dest), K(backup_piece_info), K(start_replay_log_ts));
  } else if (OB_FAIL(partition_clog_data_clean_mgr.init(cluster_backup_dest,
                 backup_piece_info,
                 pg_key,
                 data_file_id,
                 index_file_id,
                 is_backup_backup,
                 *data_clean_))) {
    LOG_WARN("failed to init clog data clean mgr", K(ret), K(cluster_backup_dest), K(log_archive_round), K(pg_key));
  } else if (clean_element.backup_dest_option_.auto_delete_obsolete_backup_) {
    if (OB_FAIL(partition_clog_data_clean_mgr.clean_clog_backup_data())) {
      LOG_WARN("failed to clean clog backup data", K(ret), K(pg_key), K(cluster_backup_dest));
    }
  } else {
    if (OB_FAIL(partition_clog_data_clean_mgr.touch_clog_backup_data())) {
      LOG_WARN("failed to clean clog backup data", K(ret), K(pg_key), K(cluster_backup_dest));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    clean_statics_ += clean_statics;
    LOG_INFO("check and delete clog data",
        K(pg_key),
        K(log_archive_round),
        K(backup_piece_info),
        K(simple_clean_tenant),
        K(start_replay_log_ts),
        K(delete_clog_mode));
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::get_clog_file_id(const ObClusterBackupDest &backup_dest,
    const ObLogArchiveRound &log_archive_round, const ObSimplePieceInfo &backup_piece_info,
    const ObBackupDeleteClogMode &delete_clog_mode, const int64_t start_replay_log_ts, const ObPartitionKey &pg_key,
    uint64_t &data_file_id, uint64_t &index_file_id)
{
  int ret = OB_SUCCESS;
  archive::ObArchiveClear archive_clear;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup clog data clean task do not init", K(ret));
  } else if (!backup_dest.is_valid() || !log_archive_round.is_valid() || !backup_piece_info.is_valid() ||
             !delete_clog_mode.is_valid() || !pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get clog file id get invalid argument",
        K(ret),
        K(backup_dest),
        K(log_archive_round),
        K(backup_piece_info),
        K(delete_clog_mode),
        K(pg_key));
  } else if (OB_FAIL(data_clean_->check_can_do_task())) {
    LOG_WARN("failed to check can do task", K(ret));
  } else if (ObBackupDeleteClogMode::NONE == delete_clog_mode.mode_) {
    data_file_id = 0;
    index_file_id = 0;
  } else if (ObBackupDeleteClogMode::DELETE_ARCHIVE_LOG == delete_clog_mode.mode_) {
    // TODO(muwei.ym) retention_timestamp
    const int64_t retention_timestamp = start_replay_log_ts;
    if (OB_FAIL(archive_clear.get_clean_max_clog_file_id_by_log_ts(backup_dest,
            log_archive_round.log_archive_round_,
            backup_piece_info.backup_piece_id_,
            backup_piece_info.create_date_,
            pg_key,
            start_replay_log_ts,
            retention_timestamp,
            index_file_id,
            data_file_id))) {
      LOG_WARN("failed to get clean max clog file id by log ts",
          K(ret),
          K(pg_key),
          K(log_archive_round),
          K(start_replay_log_ts));
    }
  } else if (ObBackupDeleteClogMode::DELETE_BACKUP_PIECE == delete_clog_mode.mode_) {
    data_file_id = UINT64_MAX;
    index_file_id = UINT64_MAX;
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succeed get clog file id",
        K(pg_key),
        K(data_file_id),
        K(index_file_id),
        K(start_replay_log_ts),
        K(delete_clog_mode));
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::handle_archive_key(const uint64_t tenant_id,
    const ObClusterBackupDest &cluster_backup_dest, const ObSimplePieceInfo &backup_piece_info,
    const ObBackupDeleteClogMode &delete_clog_mode, const common::ObIArray<ObPGKey> &pg_keys)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup clog data clean task do not init", K(ret));
  } else if (!cluster_backup_dest.is_valid() || !backup_piece_info.is_valid() || !delete_clog_mode.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("handle archive key get invalid argument",
        K(ret),
        K(cluster_backup_dest),
        K(backup_piece_info),
        K(delete_clog_mode));
  } else if (ObBackupDeleteClogMode::DELETE_BACKUP_PIECE == delete_clog_mode.mode_) {
    // delete archive key
    if (OB_FAIL(delete_archive_key(tenant_id, cluster_backup_dest, backup_piece_info, pg_keys))) {
      LOG_WARN("failed to delete archive key", K(ret), K(cluster_backup_dest));
    }
  } else {
    // touch archive key
    if (OB_FAIL(update_archive_key_timestamp(tenant_id, cluster_backup_dest, backup_piece_info, pg_keys))) {
      LOG_WARN("failed to update archive key timestamp", K(ret), K(cluster_backup_dest));
    }
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::delete_archive_key(const uint64_t tenant_id,
    const ObClusterBackupDest &cluster_backup_dest, const ObSimplePieceInfo &backup_piece_info,
    const common::ObIArray<ObPGKey> &pg_keys)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  const common::ObStorageType device_type = cluster_backup_dest.dest_.device_type_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup clog data clean task do not init", K(ret));
  } else if (!cluster_backup_dest.is_valid() || !backup_piece_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete archive key get invalid argument", K(ret), K(cluster_backup_dest), K(backup_piece_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_keys.count(); ++i) {
      const ObPGKey &pg_key = pg_keys.at(i);
      path.reset();
      if (OB_FAIL(ObBackupPathUtil::get_clog_archive_key_path(cluster_backup_dest,
              tenant_id,
              backup_piece_info.round_id_,
              backup_piece_info.backup_piece_id_,
              backup_piece_info.create_date_,
              pg_key,
              path))) {
        LOG_WARN("failed to get clog archive key path", K(ret), K(cluster_backup_dest), K(pg_key));
      } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_file(
                     path, cluster_backup_dest.get_storage_info(), device_type))) {
        LOG_WARN("failed to delete backup file", K(ret), K(path));
      }
    }

    if (OB_SUCC(ret)) {
      path.reset();
      if (OB_FAIL(ObBackupPathUtil::get_clog_archive_key_prefix(cluster_backup_dest,
              tenant_id,
              backup_piece_info.round_id_,
              backup_piece_info.backup_piece_id_,
              backup_piece_info.create_date_,
              path))) {
        LOG_WARN("failed to get clog archive key path", K(ret), K(cluster_backup_dest));
      } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(
                     path, cluster_backup_dest.get_storage_info(), device_type))) {
        LOG_WARN("failed to delete backup dir", K(ret), K(path));
      }
    }
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::update_archive_key_timestamp(const uint64_t tenant_id,
    const ObClusterBackupDest &cluster_backup_dest, const ObSimplePieceInfo &backup_piece_info,
    const common::ObIArray<ObPGKey> &pg_keys)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  const common::ObStorageType device_type = cluster_backup_dest.dest_.device_type_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup clog data clean task do not init", K(ret));
  } else if (!cluster_backup_dest.is_valid() || !backup_piece_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete archive key get invalid argument", K(ret), K(cluster_backup_dest), K(backup_piece_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_keys.count(); ++i) {
      const ObPGKey &pg_key = pg_keys.at(i);
      path.reset();
      if (OB_FAIL(ObBackupPathUtil::get_clog_archive_key_path(cluster_backup_dest,
              tenant_id,
              backup_piece_info.round_id_,
              backup_piece_info.backup_piece_id_,
              backup_piece_info.create_date_,
              pg_key,
              path))) {
        LOG_WARN("failed to get clog archive key path", K(ret), K(cluster_backup_dest), K(pg_key));
      } else if (OB_FAIL(ObBackupDataCleanUtil::touch_backup_file(
                     path, cluster_backup_dest.get_storage_info(), device_type))) {
        LOG_WARN("failed to delete backup file", K(ret), K(path));
      }
    }
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::handle_single_piece_info(const uint64_t tenant_id,
    const ObClusterBackupDest &cluster_backup_dest, const ObSimplePieceInfo &backup_piece_info,
    const ObBackupDeleteClogMode &delete_clog_mode)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  const ObStorageType type = cluster_backup_dest.dest_.device_type_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup clog data clean task do not init", K(ret));
  } else if (!cluster_backup_dest.is_valid() || !backup_piece_info.is_valid() || !delete_clog_mode.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("handle archive key get invalid argument",
        K(ret),
        K(cluster_backup_dest),
        K(backup_piece_info),
        K(delete_clog_mode));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_backup_single_piece_info_path(cluster_backup_dest,
                 tenant_id,
                 backup_piece_info.round_id_,
                 backup_piece_info.backup_piece_id_,
                 backup_piece_info.create_date_,
                 path))) {
    LOG_WARN("failed to get tenant clog backup single piece info path", K(ret), K(backup_piece_info));
  } else if (ObBackupDeleteClogMode::DELETE_BACKUP_PIECE == delete_clog_mode.mode_) {
    if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_file(path, cluster_backup_dest.get_storage_info(), type))) {
      LOG_WARN("failed to delete backup file", K(ret), K(path), K(cluster_backup_dest));
    }
  } else {
    // touch archive key
    if (OB_FAIL(ObBackupDataCleanUtil::touch_backup_file(path, cluster_backup_dest.get_storage_info(), type))) {
      LOG_WARN("failed to touch backup file", K(ret), K(path), K(cluster_backup_dest));
    }
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::handle_data_and_index_dir(const uint64_t tenant_id,
    const ObClusterBackupDest &cluster_backup_dest, const ObSimplePieceInfo &backup_piece_info,
    const ObBackupDeleteClogMode &delete_clog_mode, const common::ObIArray<ObPGKey> &pg_keys)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  hash::ObHashSet<uint64_t> table_id_set;
  const int64_t MAX_BUCKET_NUM = 1024;
  const ObStorageType &type = cluster_backup_dest.dest_.device_type_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup clog data clean task do not init", K(ret));
  } else if (!cluster_backup_dest.is_valid() || !backup_piece_info.is_valid() || !delete_clog_mode.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("handle data and inde dir get invalid argument",
        K(ret),
        K(cluster_backup_dest),
        K(backup_piece_info),
        K(delete_clog_mode));
  } else if (ObBackupDeleteClogMode::DELETE_BACKUP_PIECE != delete_clog_mode.mode_) {
    // do nothing
  } else if (OB_FAIL(table_id_set.create(MAX_BUCKET_NUM))) {
    LOG_WARN("failed to create table id set", K(ret), K(backup_piece_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_keys.count(); ++i) {
      const ObPGKey &pg_key = pg_keys.at(i);
      if (OB_FAIL(table_id_set.set_refactored(pg_key.get_table_id()))) {
        LOG_WARN("failed to set table id into set", K(ret), K(pg_key));
      }
    }

    hash::ObHashSet<uint64_t>::const_iterator iter;
    for (iter = table_id_set.begin(); OB_SUCC(ret) && iter != table_id_set.end(); ++iter) {
      const uint64_t table_id = iter->first;
      path.reset();
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
      } else if (OB_FAIL(ObBackupPathUtil::get_table_clog_data_dir_path(cluster_backup_dest,
                     tenant_id,
                     backup_piece_info.round_id_,
                     backup_piece_info.backup_piece_id_,
                     backup_piece_info.create_date_,
                     table_id,
                     path))) {
        LOG_WARN("failed to get table clog data dir path", K(ret), K(cluster_backup_dest));
      } else if (OB_FAIL(
                     ObBackupDataCleanUtil::delete_backup_dir(path, cluster_backup_dest.get_storage_info(), type))) {
        LOG_WARN("failed to delete backup dir", K(ret), K(path));
      } else if (FALSE_IT(path.reset())) {
      } else if (OB_FAIL(ObBackupPathUtil::get_table_clog_index_dir_path(cluster_backup_dest,
                     tenant_id,
                     backup_piece_info.round_id_,
                     backup_piece_info.backup_piece_id_,
                     backup_piece_info.create_date_,
                     table_id,
                     path))) {
        LOG_WARN("failed to get table clog data dir path", K(ret), K(cluster_backup_dest));
      } else if (OB_FAIL(
                     ObBackupDataCleanUtil::delete_backup_dir(path, cluster_backup_dest.get_storage_info(), type))) {
        LOG_WARN("failed to delete backup dir", K(ret), K(path));
      }
    }

    if (OB_SUCC(ret)) {
      path.reset();
      if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_backup_piece_data_dir_path(cluster_backup_dest,
              tenant_id,
              backup_piece_info.round_id_,
              backup_piece_info.backup_piece_id_,
              backup_piece_info.create_date_,
              path))) {
        LOG_WARN("failed to get table clog data dir path", K(ret), K(cluster_backup_dest));
      } else if (OB_FAIL(
                     ObBackupDataCleanUtil::delete_backup_dir(path, cluster_backup_dest.get_storage_info(), type))) {
        LOG_WARN("failed to delete backup dir", K(ret), K(path));
      } else if (FALSE_IT(path.reset())) {
      } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_backup_piece_index_dir_path(cluster_backup_dest,
                     tenant_id,
                     backup_piece_info.round_id_,
                     backup_piece_info.backup_piece_id_,
                     backup_piece_info.create_date_,
                     path))) {
        LOG_WARN("failed to get table clog data dir path", K(ret), K(cluster_backup_dest));
      } else if (OB_FAIL(
                     ObBackupDataCleanUtil::delete_backup_dir(path, cluster_backup_dest.get_storage_info(), type))) {
        LOG_WARN("failed to delete backup dir", K(ret), K(path));
      }
    }
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::handle_backup_piece_dir(const uint64_t tenant_id,
    const ObClusterBackupDest &cluster_backup_dest, const ObSimplePieceInfo &backup_piece_info,
    const ObBackupDeleteClogMode &delete_clog_mode)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  const ObStorageType &type = cluster_backup_dest.dest_.device_type_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup clog data clean task do not init", K(ret));
  } else if (!cluster_backup_dest.is_valid() || !backup_piece_info.is_valid() || !delete_clog_mode.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("handle backup piece dir get invalid argument",
        K(ret),
        K(cluster_backup_dest),
        K(backup_piece_info),
        K(delete_clog_mode));
  } else if (ObBackupDeleteClogMode::DELETE_BACKUP_PIECE != delete_clog_mode.mode_) {
    // do nothing
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_clog_backup_piece_dir_path(cluster_backup_dest,
                 tenant_id,
                 backup_piece_info.round_id_,
                 backup_piece_info.backup_piece_id_,
                 backup_piece_info.create_date_,
                 path))) {
    LOG_WARN("failed to get tenant clog backup piece dir path", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, cluster_backup_dest.get_storage_info(), type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(path));
  }
  return ret;
}

int ObTenantBackupClogDataCleanTask::handle_backup_clog_piece_infos(const uint64_t tenant_id,
    const ObClusterBackupDest &cluster_backup_dest, const ObSimplePieceInfo &backup_piece_info,
    const ObBackupDeleteClogMode &delete_clog_mode, const common::ObIArray<ObPGKey> &pg_keys)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup clog data clean task do not init", K(ret));
  } else if (!backup_piece_info.is_valid() || !delete_clog_mode.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "handle archive key get invalid argument", K(ret), K(tenant_id), K(backup_piece_info), K(delete_clog_mode));
  } else if (OB_FAIL(handle_data_and_index_dir(
                 tenant_id, cluster_backup_dest, backup_piece_info, delete_clog_mode, pg_keys))) {
    LOG_WARN("failed to handle data and index dir", K(ret), K(cluster_backup_dest), K(backup_piece_info));
  } else if (OB_FAIL(
                 handle_archive_key(tenant_id, cluster_backup_dest, backup_piece_info, delete_clog_mode, pg_keys))) {
    LOG_WARN("failed to handle archive key", K(ret), K(cluster_backup_dest), K(backup_piece_info));
  } else if (OB_FAIL(handle_single_piece_info(tenant_id, cluster_backup_dest, backup_piece_info, delete_clog_mode))) {
    LOG_WARN("failed to handle single piece info", K(ret), K(cluster_backup_dest));
  } else if (OB_FAIL(handle_backup_piece_dir(tenant_id, cluster_backup_dest, backup_piece_info, delete_clog_mode))) {
    LOG_WARN("failed to handle backup piece dir", K(ret), K(cluster_backup_dest));
  }
  return ret;
}

ObTableBaseDataCleanMgr::ObTableBaseDataCleanMgr()
    : is_inited_(false),
      table_id_(0),
      clean_element_(),
      backup_set_id_(),
      extern_backup_info_(),
      meta_index_array_(),
      path_info_(),
      clean_statics_(),
      data_clean_(NULL)
{}

ObTableBaseDataCleanMgr::~ObTableBaseDataCleanMgr()
{}

int ObTableBaseDataCleanMgr::init(const int64_t table_id, const ObBackupDataCleanElement &clean_element,
    const ObBackupSetId &backup_set_id, const ObExternBackupInfo &extern_backup_info,
    const common::ObIArray<ObBackupMetaIndex> &meta_index_array, ObBackupDataClean &data_clean)
{
  int ret = OB_SUCCESS;
  const int64_t full_backup_set_id = extern_backup_info.full_backup_set_id_;
  const int64_t inc_backup_set_id = extern_backup_info.inc_backup_set_id_;
  const int64_t backup_date = extern_backup_info.date_;
  const int64_t compatible = extern_backup_info.compatible_;
  const uint64_t tenant_id = extract_tenant_id(table_id);

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("table data clean mgr init twice", K(ret));
  } else if (!clean_element.is_valid() || !backup_set_id.is_valid() || table_id <= 0 ||
             !extern_backup_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init partition data clean mgr get invalid argument",
        K(ret),
        K(clean_element),
        K(backup_set_id),
        K(table_id),
        K(extern_backup_info));
  } else if (OB_FAIL(ObBackupDataCleanUtil::get_backup_path_info(clean_element.backup_dest_,
                 clean_element.incarnation_,
                 tenant_id,
                 full_backup_set_id,
                 inc_backup_set_id,
                 backup_date,
                 compatible,
                 path_info_))) {
    LOG_WARN("failed to get backup path info", K(ret), K(clean_element));
  } else if (OB_FAIL(meta_index_array_.assign(meta_index_array))) {
    LOG_WARN("failed to assign meta index array", K(ret));
  } else {
    table_id_ = table_id;
    clean_element_ = clean_element;
    backup_set_id_ = backup_set_id;
    extern_backup_info_ = extern_backup_info;
    data_clean_ = &data_clean;
    is_inited_ = true;
  }
  return ret;
}

int ObTableBaseDataCleanMgr::do_clean()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();
  const int64_t inc_backup_set_id = extern_backup_info_.inc_backup_set_id_;
  LOG_INFO("start table base data clean mgr", K(table_id_), K(clean_element_), K(backup_set_id_), K(inc_backup_set_id));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant data clean mgr do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_index_array_.count(); ++i) {
      const ObBackupMetaIndex &meta_index = meta_index_array_.at(i);
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
      K(inc_backup_set_id));

  return ret;
}

int ObTableBaseDataCleanMgr::clean_partition_backup_data(const ObBackupMetaIndex &meta_index)
{
  int ret = OB_SUCCESS;
  ObPartitionDataCleanMgr partition_data_clean_mgr;
  ObBackupDataCleanStatics clean_statics;
  const ObPartitionKey pkey(meta_index.table_id_, meta_index.partition_id_, 0);
  const int64_t start_ts = ObTimeUtil::current_time();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table data clean mgr do not init", K(ret));
  } else if (OB_FAIL(partition_data_clean_mgr.init(
                 pkey, clean_element_, backup_set_id_, extern_backup_info_, *data_clean_))) {
    LOG_WARN("failed to init partition data clean mgr",
        K(ret),
        K(clean_element_),
        K(backup_set_id_),
        K(extern_backup_info_));
  } else if (OB_FAIL(partition_data_clean_mgr.do_clean())) {
    LOG_WARN("failed to do clean partition backup data",
        K(ret),
        K(clean_element_),
        K(backup_set_id_),
        K(extern_backup_info_));
  } else if (OB_FAIL(partition_data_clean_mgr.get_clean_statics(clean_statics))) {
    LOG_WARN("failed to get partition data clean mgr", K(ret));
  } else {
    clean_statics_ += clean_statics;
    const int64_t cost_ts = ObTimeUtil::current_time() - start_ts;
    LOG_INFO("table clean partition backup data statics",
        K(clean_element_),
        K(backup_set_id_),
        K(meta_index),
        K(clean_statics),
        K(cost_ts));
  }
  return ret;
}

int ObTableBaseDataCleanMgr::try_clean_backup_table_dir()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  const char *storage_info = clean_element_.backup_dest_.storage_info_;
  const ObStorageType &device_type = clean_element_.backup_dest_.device_type_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table data clean mgr do not init", K(ret));
  } else if (clean_element_.backup_dest_option_.auto_touch_reserved_backup_) {
    // do nothing
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_table_data_path(path_info_, table_id_, path))) {
    LOG_WARN("fail to get meta file path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device_type))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(storage_info), K(device_type));
  }
  return ret;
}

int ObTableBaseDataCleanMgr::get_clean_statics(ObBackupDataCleanStatics &clean_statics)
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

int ObTableClogDataCleanMgr::init(const int64_t table_id, const ObBackupDataCleanElement &clean_element,
    const ObLogArchiveRound &log_archive_round, const ObTenantBackupTaskInfo &clog_data_clean_point,
    const common::ObIArray<ObBackupMetaIndex> &meta_index_array, ObBackupDataClean &data_clean)
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
      const ObBackupMetaIndex &meta_index = meta_index_array_.at(i);
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

int ObTableClogDataCleanMgr::clean_partition_clog_backup_data(const ObBackupMetaIndex &meta_index)
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
    ObBackupDest &backup_dest = clean_element_.backup_dest_;
    const int64_t incarnation = clean_element_.incarnation_;
    const uint64_t last_replay_log_id = pg_meta.storage_info_.get_clog_info().get_last_replay_log_id();
    const ObPartitionKey pkey(meta_index.table_id_, meta_index.partition_id_, 0);
    uint64_t index_file_id = 0;
    uint64_t data_file_id = 0;
    archive::ObArchiveClear archive_clear;
    ObPartitionClogDataCleanMgr clog_data_clean_mgr;
    ObSimplePieceInfo fake_backup_piece_info;
    // TODO(muwei)
    const int64_t fake_piece_id = 0;
    const int64_t fake_piece_create_date = 0;
    const int64_t unused_retention_timestamp = INT64_MAX;
    const bool is_backup_backup = false;
    if (OB_FAIL(cluster_backup_dest.set(backup_dest, incarnation))) {
      LOG_WARN("failed to set cluster backup dest", K(ret), K(backup_dest));
    } else if (OB_FAIL(archive_clear.get_clean_max_clog_file_id_by_log_id(cluster_backup_dest,
                   log_archive_round_.log_archive_round_,
                   fake_piece_id,
                   fake_piece_create_date,
                   pg_meta.pg_key_,
                   last_replay_log_id,
                   unused_retention_timestamp,
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
    } else if (OB_FAIL(clog_data_clean_mgr.init(cluster_backup_dest,
                   fake_backup_piece_info,
                   pkey,
                   data_file_id,
                   index_file_id,
                   is_backup_backup,
                   *data_clean_))) {
      LOG_WARN("failed to init clog data clean mgr", K(ret), K(cluster_backup_dest));
    } else if (data_clean_->is_update_reserved_backup_timestamp()) {
      if (OB_FAIL(clog_data_clean_mgr.touch_clog_backup_data())) {
        LOG_WARN("failed to touch clog backup data", K(ret), K(cluster_backup_dest), K(data_file_id), K(index_file_id));
      }
    } else {
      if (OB_FAIL(clog_data_clean_mgr.clean_clog_backup_data())) {
        LOG_WARN("failed to touch clog backup data", K(ret), K(cluster_backup_dest), K(data_file_id), K(index_file_id));
      }
    }

    if (OB_FAIL(ret)) {
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
  ObBackupDest &backup_dest = clean_element_.backup_dest_;
  const int64_t incarnation = clean_element_.incarnation_;
  const uint64_t tenant_id = extract_tenant_id(table_id_);
  const int64_t round = log_archive_round_.log_archive_round_;

  const char *storage_info = clean_element_.backup_dest_.storage_info_;
  const ObStorageType &device_type = clean_element_.backup_dest_.device_type_;

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

int ObTableClogDataCleanMgr::get_partition_meta(const ObBackupMetaIndex &meta_index, ObPartitionGroupMeta &pg_meta)
{
  int ret = OB_SUCCESS;
  pg_meta.reset();
  ObBackupBaseDataPathInfo path_info;
  ObBackupPath path;
  ObClusterBackupDest cluster_backup_dest;
  uint64_t tenant_id = 0;
  // TODO(muwei.ym) increment backup delete, backup set id

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table data clean mgr do not init", K(ret));
  } else if (OB_FAIL(meta_index.check_valid())) {
    LOG_WARN("failed to check meta index is valid", K(ret), K(meta_index));
  } else if (FALSE_IT(tenant_id = extract_tenant_id(meta_index.table_id_))) {
  } else if (OB_FAIL(
                 cluster_backup_dest.set(clog_data_clean_point_.backup_dest_, clog_data_clean_point_.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(clog_data_clean_point_));
  } else if (OB_FAIL(path_info.set(cluster_backup_dest,
                 tenant_id,
                 clog_data_clean_point_.backup_set_id_,
                 clog_data_clean_point_.backup_set_id_,
                 clog_data_clean_point_.date_,
                 clog_data_clean_point_.compatible_))) {
    LOG_WARN("failed to set path info", K(ret), K(clog_data_clean_point_));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_meta_file_path(path_info, meta_index.task_id_, path))) {
    LOG_WARN("fail to get meta file path", K(ret));
  } else if (OB_FAIL(ObRestoreFileUtil::read_partition_group_meta(
                 path.get_obstr(), path_info.dest_.get_storage_info(), meta_index, pg_meta))) {
    LOG_WARN("fail to get partition meta", K(ret), K(path), K(pg_meta));
  }
  return ret;
}

int ObTableClogDataCleanMgr::get_clean_statics(ObBackupDataCleanStatics &clean_statics)
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
      backup_piece_info_(),
      pkey_(),
      data_file_id_(0),
      index_file_id_(0),
      need_clean_dir_(false),
      clean_statics_(),
      data_clean_(NULL),
      is_backup_backup_(false)
{}

ObPartitionClogDataCleanMgr::~ObPartitionClogDataCleanMgr()
{}

int ObPartitionClogDataCleanMgr::init(const ObClusterBackupDest &cluster_backup_dest,
    const ObSimplePieceInfo &backup_piece_info, const ObPartitionKey &pkey, const uint64_t data_file_id,
    const uint64_t index_file_id, const bool is_backup_backup, ObBackupDataClean &data_clean)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("partition clog data clean mgr do not init", K(ret));
  } else if (!cluster_backup_dest.is_valid() || !backup_piece_info.is_valid() || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init partition clog data clean mgr get invalid argument",
        K(ret),
        K(cluster_backup_dest),
        K(backup_piece_info),
        K(pkey));
  } else if (OB_FAIL(set_need_delete_clog_dir(cluster_backup_dest, backup_piece_info))) {
    LOG_WARN("failed to check can delete clog dir", K(ret), K(backup_piece_info));
  } else {
    cluster_backup_dest_ = cluster_backup_dest;
    backup_piece_info_ = backup_piece_info;
    pkey_ = pkey;
    data_file_id_ = data_file_id;
    index_file_id_ = index_file_id;
    is_backup_backup_ = is_backup_backup;
    data_clean_ = &data_clean;
    is_inited_ = true;
  }
  return ret;
}

int ObPartitionClogDataCleanMgr::touch_clog_backup_data()
{
  int ret = OB_SUCCESS;
  bool need_retry = true;
  int64_t retry_count = 0;
  bool has_remaining_data_files = true;
  bool has_remaining_meta_files = true;
  bool has_remaining_files = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition clog data clean mgr do not init", K(ret));
  } else {
    while (need_retry) {
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
        break;
      } else if (OB_FAIL(touch_clog_data_(has_remaining_data_files))) {
        LOG_WARN("failed to touch clog data", K(ret));
      } else if (OB_FAIL(touch_clog_meta_(has_remaining_meta_files))) {
        LOG_WARN("failed to touch clog meta", K(ret));
      } else if (FALSE_IT(has_remaining_files = has_remaining_data_files || has_remaining_meta_files)) {
      } else if (OB_FAIL(touch_archive_key_(has_remaining_files))) {
        LOG_WARN("failed to touch archive key", K(ret), K(pkey_));
      } else {
        break;
      }

      if (OB_IO_LIMIT != ret) {
        need_retry = false;
      } else {
        ++retry_count;
        usleep(10 * 1000);                            // 10ms
        if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {  // 60s
          LOG_INFO("backup io limit, need retry", "retry_count", retry_count, K(ret));
        }
      }
    }
  }

  return ret;
}

int ObPartitionClogDataCleanMgr::clean_clog_backup_data()
{
  int ret = OB_SUCCESS;
  bool need_retry = true;
  int64_t retry_count = 0;
  int64_t io_limit_retry_count = 0;
  bool has_remaining_data_files = true;
  bool has_remaining_meta_files = true;
  bool has_remaining_files = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition clog data clean mgr do not init", K(ret));
  } else {
    while (need_retry) {
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
        break;
      } else if (OB_FAIL(clean_clog_data_(has_remaining_data_files))) {
        LOG_WARN("failed to touch clog data", K(ret));
      } else if (OB_FAIL(clean_clog_meta_(has_remaining_meta_files))) {
        LOG_WARN("failed to touch clog meta", K(ret));
      } else if (FALSE_IT(has_remaining_files = has_remaining_data_files || has_remaining_meta_files)) {
      } else if (OB_FAIL(clean_archive_key_(has_remaining_files))) {
        LOG_WARN("failed to clean archive key", K(ret), K(pkey_));
      } else {
        break;
      }

      ObBackupDataCleanUtil::check_need_retry(ret, retry_count, io_limit_retry_count, need_retry);
      if (need_retry) {
        usleep(10 * 1000);                            // 10ms
        if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {  // 60s
          LOG_INFO("backup io limit, need retry", K(retry_count), K(io_limit_retry_count), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObPartitionClogDataCleanMgr::touch_clog_data_(bool &has_remaing_files)
{
  int ret = OB_SUCCESS;
  const char *storage_info = cluster_backup_dest_.get_storage_info();
  const uint64_t tenant_id = pkey_.get_tenant_id();
  const ObStorageType &device = cluster_backup_dest_.dest_.device_type_;
  ObBackupPath path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_table_clog_data_path(cluster_backup_dest_,
                 tenant_id,
                 backup_piece_info_.round_id_,
                 backup_piece_info_.backup_piece_id_,
                 backup_piece_info_.create_date_,
                 pkey_.get_table_id(),
                 pkey_.get_partition_id(),
                 path))) {
    LOG_WARN("failed to get tenant clog data path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::touch_clog_dir_files(path,
                 storage_info,
                 data_file_id_,
                 device,
                 clean_statics_,
                 *data_clean_->get_backup_lease_service(),
                 has_remaing_files))) {
    LOG_WARN("failed to touch clog file", K(ret), K(path), K(data_file_id_));
  }
  return ret;
}

int ObPartitionClogDataCleanMgr::touch_clog_meta_(bool &has_remaing_files)
{
  int ret = OB_SUCCESS;
  const char *storage_info = cluster_backup_dest_.get_storage_info();
  const uint64_t tenant_id = pkey_.get_tenant_id();
  const ObStorageType &device = cluster_backup_dest_.dest_.device_type_;
  ObBackupPath path;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_table_clog_index_path(cluster_backup_dest_,
                 tenant_id,
                 backup_piece_info_.round_id_,
                 backup_piece_info_.backup_piece_id_,
                 backup_piece_info_.create_date_,
                 pkey_.get_table_id(),
                 pkey_.get_partition_id(),
                 path))) {
    LOG_WARN("failed to get tenant clog data path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::touch_clog_dir_files(path,
                 storage_info,
                 index_file_id_,
                 device,
                 clean_statics_,
                 *data_clean_->get_backup_lease_service(),
                 has_remaing_files))) {
    LOG_WARN("failed to touch clog file", K(ret), K(path), K(index_file_id_));
  }
  return ret;
}

int ObPartitionClogDataCleanMgr::clean_clog_data_(bool &has_remaing_files)
{
  int ret = OB_SUCCESS;
  const char *storage_info = cluster_backup_dest_.get_storage_info();
  const uint64_t tenant_id = pkey_.get_tenant_id();
  const ObStorageType &device = cluster_backup_dest_.dest_.device_type_;
  ObBackupPath path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_table_clog_data_path(cluster_backup_dest_,
                 tenant_id,
                 backup_piece_info_.round_id_,
                 backup_piece_info_.backup_piece_id_,
                 backup_piece_info_.create_date_,
                 pkey_.get_table_id(),
                 pkey_.get_partition_id(),
                 path))) {
    LOG_WARN("failed to get tenant clog data path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_clog_dir_files(path,
                 storage_info,
                 data_file_id_,
                 device,
                 clean_statics_,
                 *data_clean_->get_backup_lease_service(),
                 has_remaing_files))) {
    LOG_WARN("failed to delete clog file", K(ret), K(path), K(data_file_id_));
  } else if (need_clean_dir_ && !has_remaing_files) {
    if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device))) {
      LOG_WARN("failed to delete backup dir", K(ret), K(path));
    }
  }
  return ret;
}

int ObPartitionClogDataCleanMgr::clean_clog_meta_(bool &has_remaing_files)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  const char *storage_info = cluster_backup_dest_.get_storage_info();
  const uint64_t tenant_id = pkey_.get_tenant_id();
  const ObStorageType &device = cluster_backup_dest_.dest_.device_type_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_table_clog_index_path(cluster_backup_dest_,
                 tenant_id,
                 backup_piece_info_.round_id_,
                 backup_piece_info_.backup_piece_id_,
                 backup_piece_info_.create_date_,
                 pkey_.get_table_id(),
                 pkey_.get_partition_id(),
                 path))) {
    LOG_WARN("failed to get tenant clog data path", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_clog_dir_files(path,
                 storage_info,
                 index_file_id_,
                 device,
                 clean_statics_,
                 *data_clean_->get_backup_lease_service(),
                 has_remaing_files))) {
    LOG_WARN("failed to delete clog file", K(ret), K(path), K(index_file_id_));
  } else if (need_clean_dir_ && !has_remaing_files) {
    if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(path, storage_info, device))) {
      LOG_WARN("failed to delete backup dir", K(ret), K(path));
    }
  }
  return ret;
}

// TODO need use schema to check partition exist
int ObPartitionClogDataCleanMgr::set_need_delete_clog_dir(
    const ObClusterBackupDest &cluster_backup_dest, const ObSimplePieceInfo &backup_piece_info)
{
  int ret = OB_SUCCESS;
  need_clean_dir_ = false;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("partition clog data clean mgr init twice", K(ret));
  } else if (ObStorageType::OB_STORAGE_OSS == cluster_backup_dest.dest_.device_type_ ||
             ObStorageType::OB_STORAGE_COS == cluster_backup_dest.dest_.device_type_) {
    need_clean_dir_ = false;
  } else if (ObBackupPieceStatus::BACKUP_PIECE_FROZEN == backup_piece_info.status_) {
    need_clean_dir_ = true;
  }
  return ret;
}

int ObPartitionClogDataCleanMgr::get_clean_statics(ObBackupDataCleanStatics &clean_statics)
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

int ObPartitionClogDataCleanMgr::clean_archive_key_(const bool has_remaining_files)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  const ObStorageType device_type = cluster_backup_dest_.dest_.device_type_;
  bool can_delete_file = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition clog data clean mgr do not init", K(ret));
  } else if (has_remaining_files) {
    // do nothing
  } else if (OB_FAIL(check_can_delete_file(can_delete_file))) {
    LOG_WARN("failed to check can delete file", K(ret));
  } else if (!can_delete_file) {
    // do nothing
  } else if (OB_FAIL(ObBackupPathUtil::get_clog_archive_key_path(cluster_backup_dest_,
                 pkey_.get_tenant_id(),
                 backup_piece_info_.round_id_,
                 backup_piece_info_.backup_piece_id_,
                 backup_piece_info_.create_date_,
                 pkey_,
                 path))) {
    LOG_WARN("failed to get clog archive key path", K(ret), K(cluster_backup_dest_), K(pkey_));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_file(
                 path, cluster_backup_dest_.get_storage_info(), device_type))) {
    LOG_WARN("failed to delete backup file", K(ret), K(path));
  }
  return ret;
}

int ObPartitionClogDataCleanMgr::touch_archive_key_(const bool has_remaining_files)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  const ObStorageType device_type = cluster_backup_dest_.dest_.device_type_;
  bool can_delete_file = false;
  bool need_touch = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition clog data clean mgr do not init", K(ret));
  } else if (!has_remaining_files) {
    if (OB_FAIL(check_can_delete_file(can_delete_file))) {
      LOG_WARN("failed to check can delete file", K(ret));
    } else {
      need_touch = !can_delete_file;
    }
  } else {
    need_touch = true;
  }
  if (OB_FAIL(ret)) {
  } else if (!need_touch) {
  } else if (OB_FAIL(ObBackupPathUtil::get_clog_archive_key_path(cluster_backup_dest_,
                 pkey_.get_tenant_id(),
                 backup_piece_info_.round_id_,
                 backup_piece_info_.backup_piece_id_,
                 backup_piece_info_.create_date_,
                 pkey_,
                 path))) {
    LOG_WARN("failed to get clog archive key path", K(ret), K(cluster_backup_dest_), K(pkey_));
  } else if (OB_FAIL(ObBackupDataCleanUtil::touch_backup_file(
                 path, cluster_backup_dest_.get_storage_info(), device_type))) {
    LOG_WARN("failed to delete backup file", K(ret), K(path));
  }
  return ret;
}

int ObPartitionClogDataCleanMgr::check_can_delete_file(bool &can_delete_file)
{
  int ret = OB_SUCCESS;
  can_delete_file = false;
  ObClusterBackupDest backup_dest;
  ObClusterBackupDest backup_backup_dest;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition clog data clean mgr do not init", K(ret));
  } else if (!is_backup_backup_) {
    if (!data_clean_->get_backup_dest().is_valid()) {
      can_delete_file = true;
    } else if (OB_FAIL(backup_dest.set(data_clean_->get_backup_dest(), cluster_backup_dest_.incarnation_))) {
      LOG_WARN("failed to set backup dest", K(ret), K(cluster_backup_dest_));
    } else if (!backup_dest.is_same(cluster_backup_dest_)) {
      can_delete_file = true;
    }
  } else {
    if (!data_clean_->get_backup_backup_dest().is_valid()) {
      can_delete_file = true;
    } else if (OB_FAIL(
                   backup_backup_dest.set(data_clean_->get_backup_backup_dest(), cluster_backup_dest_.incarnation_))) {
      LOG_WARN("failed to set backup dest", K(ret), K(cluster_backup_dest_));
    } else if (!backup_backup_dest.is_same(cluster_backup_dest_)) {
      can_delete_file = true;
    }
  }

  if (OB_SUCC(ret) && !can_delete_file) {
    const bool check_dropped_partition = true;
    bool is_dropped = false;
    bool is_exist = false;
    ObMultiVersionSchemaService *schema_service = NULL;
    ObSchemaGetterGuard guard;
    if (NULL == (schema_service = GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get schema service", K(ret), KP(schema_service));
    } else if (OB_FAIL(schema_service->check_if_tenant_has_been_dropped(pkey_.get_tenant_id(), is_dropped))) {
      LOG_WARN("failed to check tenant has been dropped", K(ret), K(pkey_));
    } else if (is_dropped) {
      can_delete_file = true;
    } else if (is_sys_table(pkey_.get_table_id())) {
      can_delete_file = false;
    } else if (OB_FAIL(schema_service->get_tenant_schema_guard(pkey_.get_tenant_id(), guard))) {
      LOG_WARN("failed to get tenant schema guard", K(ret), K(pkey_));
    } else if (OB_FAIL(guard.check_partition_exist(
                   pkey_.get_table_id(), pkey_.get_partition_id(), check_dropped_partition, is_exist))) {
      LOG_WARN("failed to check partition exist", K(ret), K(pkey_));
    } else {
      can_delete_file = !is_exist;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!can_delete_file) {
    // do nothing
  } else if (!is_backup_backup_) {
    // do nothing
  } else {
    ObBackupPath path;
    ObStorageUtil util(false /*need retry*/);
    bool is_exist = false;
    if (!data_clean_->get_backup_backup_dest().is_valid()) {
      can_delete_file = true;
    } else if (OB_FAIL(backup_dest.set(data_clean_->get_backup_dest(), cluster_backup_dest_.incarnation_))) {
      LOG_WARN("failed to set backup dest", K(ret), K(cluster_backup_dest_));
    } else if (OB_FAIL(ObBackupPathUtil::get_clog_archive_key_path(backup_dest,
                   pkey_.get_tenant_id(),
                   backup_piece_info_.round_id_,
                   backup_piece_info_.backup_piece_id_,
                   backup_piece_info_.create_date_,
                   pkey_,
                   path))) {
      LOG_WARN("failed to get clog archive key path", K(ret), K(cluster_backup_dest_), K(pkey_));
    } else if (OB_FAIL(util.is_exist(path.get_ptr(), backup_dest.get_storage_info(), is_exist))) {
      LOG_WARN("failed to check file exist", K(ret), K(path));
    } else {
      can_delete_file = !is_exist;
    }
  }
  return ret;
}

// TODO(muwei.ym) fix path info
ObPartitionDataCleanMgr::ObPartitionDataCleanMgr()
    : is_inited_(false),
      pkey_(),
      backup_set_id_(),
      path_info_(),
      clean_statics_(),
      data_clean_(NULL),
      backup_dest_option_()
{}

ObPartitionDataCleanMgr::~ObPartitionDataCleanMgr()
{}

int ObPartitionDataCleanMgr::init(const ObPartitionKey &pkey, const ObBackupDataCleanElement &clean_element,
    const ObBackupSetId &backup_set_id, const ObExternBackupInfo &extern_backup_info, ObBackupDataClean &data_clean)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = pkey.get_tenant_id();
  const int64_t full_backup_set_id = extern_backup_info.full_backup_set_id_;
  const int64_t inc_backup_set_id = extern_backup_info.inc_backup_set_id_;
  const int64_t backup_date = extern_backup_info.date_;
  const int64_t compatible = extern_backup_info.compatible_;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("partition data clean mgr init twice", K(ret));
  } else if (!pkey.is_valid() || !clean_element.is_valid() || !backup_set_id.is_valid() ||
             !extern_backup_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init partition data clean mgr get invalid argument",
        K(ret),
        K(clean_element),
        K(backup_set_id),
        K(extern_backup_info));
  } else if (OB_FAIL(ObBackupDataCleanUtil::get_backup_path_info(clean_element.backup_dest_,
                 clean_element.incarnation_,
                 tenant_id,
                 full_backup_set_id,
                 inc_backup_set_id,
                 backup_date,
                 compatible,
                 path_info_))) {
    LOG_WARN("failed to get backup path info", K(ret), K(clean_element));
  } else {
    pkey_ = pkey;
    backup_set_id_ = backup_set_id;
    data_clean_ = &data_clean;
    backup_dest_option_ = clean_element.backup_dest_option_;
    is_inited_ = true;
  }
  return ret;
}

int ObPartitionDataCleanMgr::do_clean()
{
  int ret = OB_SUCCESS;
  bool need_retry = true;
  int64_t retry_count = 0;
  int64_t io_limit_retry_count = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else {
    while (need_retry) {
      if (OB_FAIL(data_clean_->check_can_do_task())) {
        LOG_WARN("failed to check can do task", K(ret));
        break;
      } else if (ObBackupDataCleanMode::TOUCH == backup_set_id_.clean_mode_) {
        if (backup_dest_option_.auto_delete_obsolete_backup_) {
          // do nothing
        } else if (OB_FAIL(touch_backup_data())) {
          LOG_WARN("failed to touch backup data", K(ret));
        }
      } else if (ObBackupDataCleanMode::CLEAN == backup_set_id_.clean_mode_) {
        if (data_clean_->is_update_reserved_backup_timestamp()) {
          // do nothing
        } else if (OB_FAIL(clean_backup_data())) {
          LOG_WARN("failed to clean backup data", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup set id mod is invalid", K(ret), K(backup_set_id_));
      }

      if (OB_SUCC(ret)) {
        break;
      }
      ObBackupDataCleanUtil::check_need_retry(ret, retry_count, io_limit_retry_count, need_retry);
      if (need_retry) {
        usleep(10 * 1000);                            // 10ms
        if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {  // 60s
          LOG_INFO("backup io limit, need retry", K(retry_count), K(io_limit_retry_count), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObPartitionDataCleanMgr::touch_backup_data()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (OB_FAIL(touch_backup_major_data())) {
    LOG_WARN("failed to touch backup major data", K(ret));
  } else if (OB_FAIL(touch_backup_minor_data())) {
    LOG_WARN("failed to touch backup minor data", K(ret));
  }
  return ret;
}

int ObPartitionDataCleanMgr::touch_backup_major_data()
{
  int ret = OB_SUCCESS;
  ObBackupPath major_path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_pg_major_data_path(
                 path_info_, pkey_.get_table_id(), pkey_.get_partition_id(), major_path))) {
    STORAGE_LOG(WARN, "failed to get inc backup path", K(ret));
  } else if (OB_FAIL(touch_backup_data_(major_path))) {
    LOG_WARN("failed to touch backup data", K(ret), K(major_path));
  }
  return ret;
}

int ObPartitionDataCleanMgr::touch_backup_minor_data()
{
  int ret = OB_SUCCESS;
  ObBackupPath minor_path;
  ObBackupPath minor_task_path;
  common::ObArenaAllocator allocator(ObModIds::BACKUP);
  ObArray<common::ObString> task_id_names;
  ObStorageUtil util(false /*need retry*/);
  int64_t task_id = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_pg_minor_dir_path(
                 path_info_, pkey_.get_table_id(), pkey_.get_partition_id(), minor_path))) {
    STORAGE_LOG(WARN, "failed to get inc backup path", K(ret));
  } else if (OB_FAIL(util.list_directories(
                 minor_path.get_ptr(), path_info_.dest_.get_storage_info(), allocator, task_id_names))) {
    LOG_WARN("failed to list files", K(ret), K(minor_path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_id_names.count(); ++i) {
      const ObString &task = task_id_names.at(i);
      task_id = 0;
      minor_task_path.reset();
      if (OB_FAIL(ObBackupDataCleanUtil::get_file_id(task, task_id))) {
        LOG_WARN("failed to get file id", K(ret), K(task));
      } else if (OB_FAIL(ObBackupPathUtil::get_tenant_pg_minor_data_path(
                     path_info_, pkey_.get_table_id(), pkey_.get_partition_id(), task_id, minor_task_path))) {
        STORAGE_LOG(WARN, "failed to get minor backup path", K(ret));
      } else if (OB_FAIL(touch_backup_data_(minor_task_path))) {
        LOG_WARN("failed to clean backup data", K(ret), K(minor_task_path));
      }
    }
  }
  return ret;
}

int ObPartitionDataCleanMgr::clean_backup_data()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_pg_data_path(
                 path_info_, pkey_.get_table_id(), pkey_.get_partition_id(), path))) {
    LOG_WARN("failed to get tenant pg data path", K(ret), K(path_info_), K(pkey_));
  } else if (OB_FAIL(clean_backup_major_data())) {
    LOG_WARN("failed to clean backup major data", K(ret));
  } else if (OB_FAIL(clean_backup_minor_data())) {
    LOG_WARN("failed to clean backup minor data", K(ret));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(
                 path, path_info_.dest_.get_storage_info(), path_info_.dest_.dest_.device_type_))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(path));
  }
  return ret;
}

int ObPartitionDataCleanMgr::clean_backup_major_data()
{
  int ret = OB_SUCCESS;
  ObBackupPath major_path;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_pg_major_data_path(
                 path_info_, pkey_.get_table_id(), pkey_.get_partition_id(), major_path))) {
    STORAGE_LOG(WARN, "failed to get inc backup path", K(ret));
  } else if (OB_FAIL(clean_backup_data_(major_path))) {
    LOG_WARN("failed to clean backup data", K(ret), K(major_path));
  }
  return ret;
}

int ObPartitionDataCleanMgr::clean_backup_minor_data()
{
  int ret = OB_SUCCESS;
  ObBackupPath minor_path;
  ObBackupPath minor_task_path;
  common::ObArenaAllocator allocator(ObModIds::BACKUP);
  ObArray<common::ObString> task_id_names;
  ObStorageUtil util(false /*need retry*/);
  int64_t task_id = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_pg_minor_dir_path(
                 path_info_, pkey_.get_table_id(), pkey_.get_partition_id(), minor_path))) {
    STORAGE_LOG(WARN, "failed to get inc backup path", K(ret));
  } else if (OB_FAIL(util.list_directories(
                 minor_path.get_ptr(), path_info_.dest_.get_storage_info(), allocator, task_id_names))) {
    LOG_WARN("failed to list files", K(ret), K(minor_path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_id_names.count(); ++i) {
      const ObString &task = task_id_names.at(i);
      task_id = 0;
      minor_task_path.reset();
      if (OB_FAIL(ObBackupDataCleanUtil::get_file_id(task, task_id))) {
        LOG_WARN("failed to get file id", K(ret), K(task));
      } else if (OB_FAIL(ObBackupPathUtil::get_tenant_pg_minor_data_path(
                     path_info_, pkey_.get_table_id(), pkey_.get_partition_id(), task_id, minor_task_path))) {
        STORAGE_LOG(WARN, "failed to get minor backup path", K(ret));
      } else if (OB_FAIL(clean_backup_data_(minor_task_path))) {
        LOG_WARN("failed to clean backup data", K(ret), K(minor_task_path));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir(
              minor_path, path_info_.dest_.get_storage_info(), path_info_.dest_.dest_.device_type_))) {
        LOG_WARN("failed to delete backup dir", K(ret), K(minor_path));
      }
    }
  }
  return ret;
}

int ObPartitionDataCleanMgr::clean_backup_data_(const ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  const ObStorageType device_type = path_info_.dest_.dest_.device_type_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (path.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("clean backup data get invalid argument", K(ret), K(path));
  } else if (OB_FAIL(ObBackupDataCleanUtil::delete_backup_dir_files(path,
                 path_info_.dest_.get_storage_info(),
                 device_type,
                 clean_statics_,
                 *data_clean_->get_backup_lease_service()))) {
    LOG_WARN("failed to touch backup file", K(ret), K(path));
  }
  return ret;
}

int ObPartitionDataCleanMgr::touch_backup_data_(const ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  const ObStorageType device_type = path_info_.dest_.dest_.device_type_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition data clean mgr do not init", K(ret));
  } else if (path.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("clean backup data get invalid argument", K(ret), K(path));
  } else if (OB_FAIL(ObBackupDataCleanUtil::touch_backup_dir_files(path,
                 path_info_.dest_.get_storage_info(),
                 device_type,
                 clean_statics_,
                 *data_clean_->get_backup_lease_service()))) {
    LOG_WARN("failed to touch backup file", K(ret), K(path));
  }
  return ret;
}

int ObPartitionDataCleanMgr::get_clean_statics(ObBackupDataCleanStatics &clean_statics)
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

}  // namespace rootserver
}  // namespace oceanbase
