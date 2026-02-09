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
#include "ob_backup_validate_scheduler.h"
#include "ob_backup_validate_task_mgr.h"
#include "ob_backup_task_scheduler.h"
#include "share/backup/ob_backup_validate_table_operator.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "share/backup/ob_backup_helper.h"
#include "share/backup/ob_backup_store.h"
#include "share/backup/ob_archive_store.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/backup/ob_archive_path.h"
#include "storage/backup/ob_backup_data_store.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace rootserver
{

ObBackupValidateInfoCollector::ObBackupValidateInfoCollector()
  : is_inited_(false),
    sql_proxy_(nullptr),
    job_attr_(nullptr),
    tenant_id_(OB_INVALID_TENANT_ID),
    backup_dest_id_(OB_INVALID_DEST_ID),
    archive_dest_id_(OB_INVALID_DEST_ID)
{
}

void ObBackupValidateInfoCollector::reset()
{
  is_inited_ = false;
  sql_proxy_ = nullptr;
  job_attr_ = nullptr;
  tenant_id_ = OB_INVALID_TENANT_ID;
  backup_dest_id_ = OB_INVALID_DEST_ID;
  archive_dest_id_ = OB_INVALID_DEST_ID;
}

int ObBackupValidateInfoCollector::init(
    common::ObMySQLProxy &sql_proxy,
    share::ObBackupValidateJobAttr &job_attr,
    const int64_t backup_dest_id,
    const int64_t archive_dest_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[BACKUP_VALIDATE]init twice", KR(ret));
  } else if (!job_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", KR(ret), K(job_attr));
  } else {
    sql_proxy_ = &sql_proxy;
    job_attr_ = &job_attr;
    tenant_id_ = job_attr.tenant_id_;
    backup_dest_id_ = backup_dest_id;
    archive_dest_id_ = archive_dest_id;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupValidateInfoCollector::collect_backup_set_info(common::ObArray<share::ObBackupSetFileDesc> &set_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_VALIDATE]not init", KR(ret));
  } else if (job_attr_->validate_path_.is_empty()) {
    if (OB_FAIL(get_backup_set_info_from_inner_table(set_list))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get backup set info from inner table", KR(ret));
    }
  } else {
    if (OB_FAIL(collect_backup_set_info_from_path(set_list))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to collect backup set info from path", KR(ret));
    }
  }
  return ret;
}

int ObBackupValidateInfoCollector::collect_archive_piece_info(
    common::ObArray<share::ObPieceKey> &piece_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_VALIDATE]not init", KR(ret));
  } else if (job_attr_->validate_path_.is_empty()) {
    if (OB_FAIL(get_archive_piece_info_from_inner_table(piece_list))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get archive piece info from inner table", KR(ret));
    }
  } else {
    if (OB_FAIL(collect_archive_piece_info_from_path(piece_list))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to collect archive piece info from path", KR(ret));
    }
  }
  return ret;
}

int ObBackupValidateInfoCollector::need_skip_backup_set_(
    const share::ObBackupSetFileDesc &backup_set_info,
    bool &is_skip) const
{
  int ret = OB_SUCCESS;
  is_skip = false;
  if (OB_ISNULL(job_attr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]job attr is null", KR(ret), KPC(job_attr_));
  } else if (!backup_set_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]backup set info is invalid", KR(ret), K(backup_set_info));
  } else {
    const bool is_basic_validate = job_attr_->level_.is_basic();
    if (ObBackupSetFileDesc::BackupSetStatus::SUCCESS != backup_set_info.status_
        || ObBackupFileStatus::BACKUP_FILE_AVAILABLE != backup_set_info.file_status_
        || !backup_set_info.is_backup_set_support_quick_restore()) {
      is_skip = true;
      LOG_INFO("[BACKUP_VALIDATE]backup set status or version is not supported for validation, skip", K(backup_set_info));
    } else if (is_basic_validate && backup_set_info.tenant_compatible_ < DATA_VERSION_4_5_1_0) {
      //file list exists since 4.5.1
      is_skip = true;
      LOG_INFO("[BACKUP_VALIDATE]backup set tenant compatible is less than 4.5.1, skip", K(backup_set_info));
    }
  }

  return ret;
}

int ObBackupValidateInfoCollector::get_backup_set_info_from_inner_table(
    common::ObArray<share::ObBackupSetFileDesc> &set_list)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObBackupSetFileDesc> backup_set_infos;

  if (OB_FAIL(ObBackupSetFileOperator::get_backup_set_files_specified_dest(
                                                *sql_proxy_, tenant_id_, backup_dest_id_, backup_set_infos))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup sets", KR(ret), K_(backup_dest_id), K_(tenant_id));
  } else if (OB_FAIL(filter_and_sort_backup_sets(backup_set_infos, set_list))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to filter and sort backup sets", KR(ret));
  }
  return ret;
}

int ObBackupValidateInfoCollector::get_archive_piece_info_from_inner_table(
    common::ObArray<share::ObPieceKey> &piece_list)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObTenantArchivePieceAttr> archive_piece_infos;
  share::ObArchivePersistHelper archive_persist_helper;

  if (OB_FAIL(archive_persist_helper.init(tenant_id_))) {
    LOG_WARN("[BACKUP_VALIDATE]fail to init archive persist helper", KR(ret));
  } else if (OB_FAIL(archive_persist_helper.get_pieces(*sql_proxy_, archive_dest_id_, archive_piece_infos))) {
    LOG_WARN("[BACKUP_VALIDATE]fail to get archive pieces", KR(ret));
  } else {
    common::ObArray<share::ObPieceKey> all_piece_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < archive_piece_infos.count(); ++i) {
      const share::ObTenantArchivePieceAttr &archive_piece_info = archive_piece_infos.at(i);
      if (!archive_piece_info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[BACKUP_VALIDATE]archive piece info is invalid", KR(ret), K(archive_piece_info));
      } else if (!archive_piece_info.status_.is_valid()
                 || ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE != archive_piece_info.file_status_) {
        LOG_INFO("[BACKUP_VALIDATE]archive piece do not need to be validated, skip", K(archive_piece_info));
      } else {
        share::ObPieceKey piece_key;
        piece_key.dest_id_ = archive_piece_info.key_.dest_id_;
        piece_key.round_id_ = archive_piece_info.key_.round_id_;
        piece_key.piece_id_ = archive_piece_info.key_.piece_id_;

        if (OB_FAIL(all_piece_list.push_back(piece_key))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to push back piece key", KR(ret));
        }
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(filter_and_sort_archive_pieces(all_piece_list, piece_list))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to filter and sort archive pieces", KR(ret));
    }
  }
  return ret;
}

int ObBackupValidateInfoCollector::collect_backup_set_info_from_path(
    common::ObArray<share::ObBackupSetFileDesc> &set_list)
{
  int ret = OB_SUCCESS;
  ObBackupDataStore store;
  share::ObBackupDest validate_dest;
  ObArray<share::ObBackupSetFileDesc> tmp_set_list;
  common::ObArray<share::ObBackupSetDesc> set_desc_list;
  const share::ObBackupValidatePathType &path_type = job_attr_->path_type_;
  const share::ObBackupPathString &validate_path = job_attr_->validate_path_;

  if (validate_path.is_empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]backup validate path is empty", KR(ret), K(validate_path));
  } else if (OB_FAIL(share::ObBackupStorageInfoOperator::get_backup_dest_by_dest_id(
      *sql_proxy_, tenant_id_, backup_dest_id_, validate_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup dest", KR(ret), K(backup_dest_id_));
  } else if (OB_FAIL(store.init(validate_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to init backup data store", KR(ret), K(validate_dest));
  } else if (path_type.is_dest_level_path()) {
    if (OB_FAIL(store.get_backup_set_list(set_desc_list))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get backup set list", KR(ret));
    } else if (set_desc_list.empty()) {
      ret = OB_INVALID_BACKUP_DEST;
      LOG_WARN("[BACKUP_VALIDATE]invalid backup dest", KR(ret), K(validate_path));
      LOG_USER_ERROR(OB_INVALID_BACKUP_DEST, validate_path.ptr());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < set_desc_list.count(); ++i) {
        const share::ObBackupSetDesc &set_desc = set_desc_list.at(i);
        storage::ObExternBackupSetInfoDesc backup_set_info;
        if (OB_FAIL(get_backup_set_info_by_desc_(set_desc, backup_set_info))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to get backup set info by desc", KR(ret), K(set_desc));
        } else if (OB_FAIL(tmp_set_list.push_back(backup_set_info.backup_set_file_))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to push backup set file", KR(ret), K(backup_set_info.backup_set_file_));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(filter_and_sort_backup_sets(tmp_set_list, set_list))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to filter and sort backup sets", KR(ret));
      }
    }
  } else {
    storage::ObExternBackupSetInfoDesc backup_set_info;
    bool is_skip = false;
    if (OB_FAIL(store.read_backup_set_info(backup_set_info))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to read backup set info", KR(ret));
    } else if (OB_FAIL(need_skip_backup_set_(backup_set_info.backup_set_file_, is_skip))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to check if backup set is supported for validation",
                  KR(ret), K(backup_set_info.backup_set_file_));
    } else if (!is_skip) {
      if (OB_FAIL(set_list.push_back(backup_set_info.backup_set_file_))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to push backup set info", KR(ret), K(backup_set_info.backup_set_file_));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("[BACKUP_VALIDATE]backup set is not supported for validation", KR(ret), K(backup_set_info));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObBackupValidateJobOperator::update_comment(*sql_proxy_, job_attr_->tenant_id_,
        job_attr_->job_id_, "backup set version or status is not supported for validation"))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to update comment", KR(ret), KPC(job_attr_));
      }
    }
  }
  return ret;
}

int ObBackupValidateInfoCollector::collect_archive_piece_info_from_path(
    common::ObArray<share::ObPieceKey> &piece_list)
{
  int ret = OB_SUCCESS;
  share::ObArchiveStore archive_piece_store;
  share::ObBackupDest validate_piece_dest;
  const share::ObBackupPathString &validate_path = job_attr_->validate_path_;
  const share::ObBackupValidatePathType &path_type = job_attr_->path_type_;
  ObBackupPath piece_path;
  if (validate_path.is_empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]backup validate path is empty", KR(ret), K(validate_path));
  } else if (OB_FAIL(share::ObBackupStorageInfoOperator::get_backup_dest_by_dest_id(
      *sql_proxy_, tenant_id_, archive_dest_id_, validate_piece_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup dest", KR(ret), K(archive_dest_id_));
  } else if (OB_FAIL(archive_piece_store.init(validate_piece_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to init archive piece store", KR(ret), K(validate_piece_dest));
  } else if (path_type.is_dest_level_path()) {
    common::ObArray<share::ObPieceKey> all_piece_list;
    if (OB_FAIL(archive_piece_store.get_all_piece_keys(all_piece_list))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get all piece keys", KR(ret));
    } else if (all_piece_list.empty()) {
      ret = OB_INVALID_BACKUP_DEST;
      LOG_WARN("[BACKUP_VALIDATE]invalid archive dest", KR(ret), K(validate_path));
      LOG_USER_ERROR(OB_INVALID_BACKUP_DEST, validate_path.ptr());
    } else if (OB_FAIL(filter_and_sort_archive_pieces(all_piece_list, piece_list))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to filter and sort archive pieces", KR(ret), K(all_piece_list));
    }
  } else {
    share::ObTenantArchivePieceInfosDesc extend_desc;
    share::ObPieceKey piece_key;
    bool supported_basic_validate = false;
    bool is_basic_validate = job_attr_->level_.is_basic();
    if (OB_FAIL(archive_piece_store.read_tenant_archive_piece_infos(extend_desc))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to read tenant archive piece store", KR(ret));
    } else if (is_basic_validate) {
      if (OB_FAIL(piece_path.init(validate_piece_dest.get_root_path()))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to init piece path", KR(ret), K(validate_piece_dest));
      } else if (OB_FAIL(check_piece_support_basic_validate_(archive_piece_store, piece_path,
                                                                supported_basic_validate))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to check if piece is supported for basic validate", KR(ret), K(extend_desc));
      } else if (!supported_basic_validate) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("[BACKUP_VALIDATE]piece is not supported for basic validate", KR(ret), K(extend_desc));
      }
    }
    if (OB_SUCC(ret)) {
      piece_key.piece_id_ = extend_desc.piece_id_;
      piece_key.round_id_ = extend_desc.round_id_;
      piece_key.dest_id_ = extend_desc.dest_id_;
      if (OB_FAIL(piece_list.push_back(piece_key))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to push back piece key", KR(ret), K(piece_key));
      }
    }
  }
  return ret;
}

int ObBackupValidateInfoCollector::filter_and_sort_backup_sets(
    const common::ObArray<share::ObBackupSetFileDesc> &all_set_list,
    common::ObArray<share::ObBackupSetFileDesc> &set_list)
{
  int ret = OB_SUCCESS;
  set_list.reset();
  const common::ObIArray<uint64_t> &filter_ids = job_attr_->backup_set_ids_;
  common::ObArray<share::ObBackupSetFileDesc> tmp_set_list;
  const bool is_basic_validate = job_attr_->level_.is_basic();
  bool is_skip = false;
  bool has_skip_set = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < all_set_list.count(); ++i) {
    const share::ObBackupSetFileDesc &backup_set_info = all_set_list.at(i);
    if (OB_FAIL(need_skip_backup_set_(backup_set_info, is_skip))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to check if backup set is supported for validation",
                  KR(ret), K(backup_set_info));
    } else if (!is_skip) {
      if (OB_FAIL(tmp_set_list.push_back(backup_set_info))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to push back backup set info", KR(ret), K(backup_set_info));
      }
    } else {
      has_skip_set = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (filter_ids.empty() && OB_FAIL(set_list.assign(tmp_set_list))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to assign backup set list", KR(ret));
  } else if (!filter_ids.empty()) {
    common::hash::ObHashSet<int64_t> backup_set_id_set;
    if (OB_FAIL(backup_set_id_set.create(filter_ids.count(), ObMemAttr(MTL_ID(), "BackupValidate")))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to create backup set id set", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < filter_ids.count(); ++i) {
        if (OB_FAIL(backup_set_id_set.set_refactored(filter_ids.at(i)))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to insert backup set id to set", KR(ret), K(i));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_set_list.count(); ++i) {
        const share::ObBackupSetFileDesc &backup_set_info = tmp_set_list.at(i);
        is_skip = false;
        int hash_ret = backup_set_id_set.exist_refactored(backup_set_info.backup_set_id_);
        if (OB_HASH_EXIST == hash_ret) {
          if (OB_FAIL(backup_set_id_set.erase_refactored(backup_set_info.backup_set_id_))) {
            LOG_WARN("[BACKUP_VALIDATE]failed to erase backup set id from set", KR(ret),
                        K(backup_set_info.backup_set_id_));
          } else if (OB_FAIL(set_list.push_back(backup_set_info))) {
            LOG_WARN("[BACKUP_VALIDATE]failed to push back backup set info", KR(ret), K(backup_set_info));
          }
        } else if (OB_HASH_NOT_EXIST == hash_ret) {
          // skip
        } else {
          ret = hash_ret;
          LOG_WARN("[BACKUP_VALIDATE]failed to check backup set id in set", KR(ret), K(backup_set_info.backup_set_id_));
        }
      }
      if (OB_SUCC(ret) && !backup_set_id_set.empty()) {
        ret = OB_ENTRY_NOT_EXIST;
        int tmp_ret = OB_SUCCESS;
        LOG_WARN("[BACKUP_VALIDATE]some specified backup set ids not found", KR(ret), K(backup_set_id_set), K(filter_ids));
        if (OB_TMP_FAIL(ObBackupValidateJobOperator::update_comment(*sql_proxy_, job_attr_->tenant_id_,
                                                        job_attr_->job_id_, "some specified backup set not found"))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to update comment", KR(ret), KR(tmp_ret), KPC(job_attr_));
        }
      }
    }
  }
  if (OB_SUCC(ret) && set_list.empty()) {
    if (OB_FAIL(ObBackupValidateJobOperator::update_comment(*sql_proxy_, job_attr_->tenant_id_,
        job_attr_->job_id_, "all backup set are not support for backup validate or not find any backup set"))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to update comment", KR(ret), KPC(job_attr_));
    }
  } else if (OB_SUCC(ret) && has_skip_set) {
    if (OB_FAIL(ObBackupValidateJobOperator::update_comment(*sql_proxy_, job_attr_->tenant_id_,
      job_attr_->job_id_, "some backup sets are not supported for validation"))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to update comment", KR(ret), KPC(job_attr_));
    }
  }
  if (OB_SUCC(ret)) {
    CompareBackupSetInfo backup_set_info_cmp;
    lib::ob_sort(set_list.begin(), set_list.end(), backup_set_info_cmp);
  }
  return ret;
}

int ObBackupValidateInfoCollector::filter_and_sort_archive_pieces(
    const common::ObArray<share::ObPieceKey> &all_piece_list,
    common::ObArray<share::ObPieceKey> &piece_list)
{
  int ret = OB_SUCCESS;
  piece_list.reset();
  const common::ObIArray<uint64_t> &filter_ids = job_attr_->logarchive_piece_ids_;
  bool is_basic_validate = job_attr_->level_.is_basic();
  bool supported_basic_validate = false;
  common::ObArray<share::ObPieceKey> tmp_piece_list;
  share::ObArchiveStore archive_piece_store;
  ObBackupDest dest;
  ObBackupPath piece_path;
  if (OB_FAIL(share::ObBackupStorageInfoOperator::get_backup_dest_by_dest_id(
      *sql_proxy_, tenant_id_, archive_dest_id_, dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup dest", KR(ret), K(archive_dest_id_));
  } else if (OB_FAIL(archive_piece_store.init(dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to init archive piece store", KR(ret), K(dest));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_piece_list.count(); ++i) {
    const share::ObPieceKey &piece_key = all_piece_list.at(i);
    if (is_basic_validate) {
      piece_path.reset();
      if (OB_FAIL(ObArchivePathUtil::get_piece_dir_path(dest, piece_key.dest_id_, piece_key.round_id_,
                                                            piece_key.piece_id_, piece_path))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to get piece dir path", KR(ret), K(piece_key));
      } else if (OB_FAIL(check_piece_support_basic_validate_(archive_piece_store, piece_path, supported_basic_validate))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to check if piece is supported for basic validate", KR(ret), K(piece_key));
      } else if (supported_basic_validate && OB_FAIL(tmp_piece_list.push_back(piece_key))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to push back piece key", KR(ret), K(piece_key));
      }
    } else if (OB_FAIL(tmp_piece_list.push_back(piece_key))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to push back piece key", KR(ret), K(piece_key));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (filter_ids.empty()) {
    if (OB_FAIL(piece_list.assign(tmp_piece_list))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to assign piece list", KR(ret));
    }
  } else {
    common::hash::ObHashSet<int64_t> piece_id_set;
    if (OB_FAIL(piece_id_set.create(filter_ids.count(), ObMemAttr(MTL_ID(), "BackupValidate")))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to create piece id set", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < filter_ids.count(); ++i) {
        if (OB_FAIL(piece_id_set.set_refactored(filter_ids.at(i)))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to insert piece id to set", KR(ret), K(i));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_piece_list.count(); ++i) {
        const share::ObPieceKey &piece_key = tmp_piece_list.at(i);
        int hash_ret = piece_id_set.exist_refactored(piece_key.piece_id_);
        if (OB_HASH_EXIST == hash_ret) {
          if (OB_FAIL(piece_id_set.erase_refactored(piece_key.piece_id_))) {
            LOG_WARN("[BACKUP_VALIDATE]failed to erase piece id from set", KR(ret), K(piece_key.piece_id_));
          } else if (OB_FAIL(piece_list.push_back(piece_key))) {
            LOG_WARN("[BACKUP_VALIDATE]failed to push back piece key", KR(ret));
          }
        } else if (OB_HASH_NOT_EXIST == hash_ret) {
          // skip
        } else {
          ret = hash_ret;
          LOG_WARN("[BACKUP_VALIDATE]failed to check piece id in set", KR(ret), K(piece_key.piece_id_));
        }
      }
      if (OB_SUCC(ret) && !piece_id_set.empty()) {
        ret = OB_ENTRY_NOT_EXIST;
        int tmp_ret = OB_SUCCESS;
        LOG_WARN("[BACKUP_VALIDATE]some specified piece ids not found", KR(ret), K(piece_id_set), K(filter_ids));
        if (OB_TMP_FAIL(ObBackupValidateJobOperator::update_comment(*sql_proxy_, job_attr_->tenant_id_,
            job_attr_->job_id_, "some specified piece ids not found"))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to update comment", KR(ret), KR(tmp_ret), KPC(job_attr_));
        }
        LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "some specified piece ids not found");
      }
    }
  }
  if (OB_SUCC(ret) && piece_list.empty()) {
    if (OB_FAIL(ObBackupValidateJobOperator::update_comment(*sql_proxy_, job_attr_->tenant_id_,
        job_attr_->job_id_, "all archive pieces are not support for backup validate or not find any piece"))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to update comment", KR(ret), KPC(job_attr_));
    }
  }
  if (OB_SUCC(ret)) {
    CompareArchivePieceKey archive_piece_key_cmp;
    lib::ob_sort(piece_list.begin(), piece_list.end(), archive_piece_key_cmp);
  }
  return ret;
}

int ObBackupValidateInfoCollector::get_backup_set_info_by_desc_(
    const share::ObBackupSetDesc &set_desc,
    storage::ObExternBackupSetInfoDesc &backup_set_info)
{
  int ret = OB_SUCCESS;
  ObBackupDataStore store;
  share::ObBackupDest validate_dest;

  if (OB_FAIL(share::ObBackupStorageInfoOperator::get_backup_dest_by_dest_id(
      *sql_proxy_, tenant_id_, backup_dest_id_, validate_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup dest", KR(ret), K(backup_dest_id_));
  } else if (OB_FAIL(store.init(validate_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to init backup data store", KR(ret), K(validate_dest));
  } else if (OB_FAIL(store.read_backup_set_info_by_desc(set_desc, backup_set_info))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to read backup set info", KR(ret), K(set_desc));
  }
  return ret;
}

int ObBackupValidateInfoCollector::check_piece_support_basic_validate_(
    const share::ObArchiveStore &archive_store,
    const share::ObBackupPath &piece_path,
    bool &supported_basic_validate) const
{
  int ret = OB_SUCCESS;
  if (!job_attr_->level_.is_basic()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]not basic validate job", KR(ret), KPC(job_attr_));
  } else {
    supported_basic_validate = false;
    ObBackupFileSuffix suffix(ObBackupFileSuffix::ARCHIVE);
    if (OB_FAIL(archive_store.is_file_list_file_exist(piece_path, suffix, supported_basic_validate))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to check if file list file exist", KR(ret), K(piece_path), K(suffix));
    } else if (!supported_basic_validate) {
      LOG_INFO("[BACKUP_VALIDATE]piece is not supported for basic validate", K(piece_path));
    }
  }
  return ret;
}

int ObBackupValidateInfoCollector::collect_complement_log_piece_info(
    const common::ObArray<share::ObBackupSetFileDesc> &set_list,
    common::hash::ObHashMap<int64_t, common::ObArray<share::ObPieceKey>> &complement_piece_map)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObPieceKey> piece_keys;

  for (int64_t i = 0; OB_SUCC(ret) && i < set_list.count(); ++i) {
    const share::ObBackupSetFileDesc &backup_set_info = set_list.at(i);
    if (backup_set_info.plus_archivelog_) {
      piece_keys.reset();
      if (OB_FAIL(collect_complement_log_piece_keys_(backup_set_info, piece_keys))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to collect complement log piece keys", KR(ret), K(backup_set_info));
      } else if (!piece_keys.empty()) {
        if (OB_FAIL(complement_piece_map.set_refactored(backup_set_info.backup_set_id_, piece_keys))) {
          LOG_WARN("[BACKUP_VALIDATE]failed to set complement piece map", KR(ret),
                      K(backup_set_info.backup_set_id_), K(piece_keys));
        } else {
          LOG_INFO("[BACKUP_VALIDATE]successfully collected complement log piece info",
                      K(backup_set_info.backup_set_id_), K(piece_keys.count()));
        }
      }
    }
  }
  return ret;
}

int ObBackupValidateInfoCollector::collect_complement_log_piece_keys_(
    const share::ObBackupSetFileDesc &backup_set_info,
    common::ObArray<share::ObPieceKey> &piece_keys)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest complement_dest;
  CompareArchivePieceKey archive_piece_key_cmp;

  if (!backup_set_info.plus_archivelog_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]backup set info is not plus archive log", KR(ret), K(backup_set_info));
  } else if (OB_FAIL(get_complement_log_backup_dest_(backup_set_info, complement_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get complement log backup dest", KR(ret), K(backup_set_info));
  } else if (backup_set_info.tenant_compatible_ >= DATA_VERSION_4_5_1_0) {
    if (OB_FAIL(collect_piece_keys_from_file_list_(complement_dest, piece_keys))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to collect complement log piece keys from file list", KR(ret), K(complement_dest));
    }
  } else {
    share::ObArchiveStore archive_store;
    if (OB_FAIL(archive_store.init(complement_dest))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to init archive store", KR(ret), K(complement_dest));
    } else if (OB_FAIL(archive_store.get_all_piece_keys(piece_keys))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get all piece keys from complement log", KR(ret), K(complement_dest));
    } else if (OB_FALSE_IT(lib::ob_sort(piece_keys.begin(), piece_keys.end(), archive_piece_key_cmp))) {
    } else {
      LOG_INFO("[BACKUP_VALIDATE]successfully got complement log piece keys",
                  K(backup_set_info.backup_set_id_), K(piece_keys.count()));
    }
  }

  return ret;
}

int ObBackupValidateInfoCollector::collect_piece_keys_from_file_list_(
    const share::ObBackupDest &complement_dest,
    common::ObArray<share::ObPieceKey> &piece_keys)
{
  int ret = OB_SUCCESS;
  if (!complement_dest.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]complement dest is not valid", KR(ret), K(complement_dest));
  } else {
    share::ObBackupPath pieces_path;
    const share::ObBackupStorageInfo *storage_info = complement_dest.get_storage_info();
    const share::ObBackupFileSuffix &suffix = ObBackupFileSuffix::ARCHIVE;
    backup::ObBackupFileListInfo file_list_info;
    common::ObStorageIdMod storage_id_mod;
    storage_id_mod.storage_id_ = backup_dest_id_;
    storage_id_mod.storage_used_mod_ = common::ObStorageUsedMod::STORAGE_USED_BACKUP;
    bool is_piece_start = false;
    share::ObPieceKey key;
    if (OB_FAIL(ObArchivePathUtil::get_pieces_dir_path(complement_dest, pieces_path))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to get complement log path", KR(ret), K(complement_dest));
    } else if (OB_FAIL(backup::ObBackupFileListReaderUtil::read_file_list_from_path(storage_info, pieces_path,
                                                                              suffix, storage_id_mod, file_list_info))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to read file list from path", KR(ret), K(pieces_path),
                  KPC(storage_info), K(storage_id_mod), K(suffix));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < file_list_info.count(); ++i) {
      const backup::ObBackupFileInfo &file_info = file_list_info.file_list_.at(i);
      is_piece_start = false;
      key.reset();
      if (OB_FAIL(ObArchiveStoreUtil::is_piece_start_file_name(file_info.path_.str(), is_piece_start))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to check is piece start file name", K(ret), K(file_info));
      } else if (!is_piece_start) {
      } else if (OB_FAIL(ObArchiveStoreUtil::parse_piece_file(file_info.path_.str(),
                                                                      key.dest_id_, key.round_id_, key.piece_id_))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to parse piece file", K(ret), K(file_info));
      } else if (OB_FAIL(piece_keys.push_back(key))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to push key to array", K(ret), K(key));
      }
    }
  }
  return ret;
}

int ObBackupValidateInfoCollector::get_complement_log_backup_dest_(
    const share::ObBackupSetFileDesc &backup_set_info,
    share::ObBackupDest &complement_dest)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_dest;

  if (OB_FAIL(share::ObBackupStorageInfoOperator::get_backup_dest_by_dest_id(*sql_proxy_, tenant_id_,
                                                                              backup_dest_id_, backup_dest))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to get backup dest by dest id", KR(ret),
              K_(tenant_id), K_(backup_dest_id));
  } else {
    share::ObBackupSetDesc backup_set_desc;
    backup_set_desc.backup_set_id_ = backup_set_info.backup_set_id_;
    backup_set_desc.backup_type_ = backup_set_info.backup_type_;
    // backup_dest_dir->backup_complement_log_dest
    if (job_attr_->validate_path_.is_empty() || job_attr_->path_type_.is_dest_level_path()) {
      if (OB_FAIL(share::ObBackupPathUtil::construct_backup_complement_log_dest(backup_dest,
                                                                                backup_set_desc, complement_dest))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to construct backup complement log dest",
                    KR(ret), K(backup_dest), K(backup_set_desc));
      }
    } else {
      // backup_set_dir->backup_complement_log_dest
      if (OB_FAIL(share::ObBackupPathUtil::construct_backup_complement_log_dest(backup_dest, complement_dest))) {
        LOG_WARN("[BACKUP_VALIDATE]failed to construct backup complement log dest", KR(ret), K(backup_dest));
      }
    }
  }

  return ret;
}

} // namespace rootserver
} // namespace oceanbase