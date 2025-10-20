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
#include "ob_backup_clean_selector.h"
#include "share/backup/ob_backup_helper.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_store.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/backup/ob_archive_store.h"
#include "share/backup/ob_archive_path.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_clean_util.h"
#include "share/backup/ob_backup_clean_operator.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/backup/ob_backup_utils.h"
namespace oceanbase
{
using namespace share;
namespace rootserver
{

//********************  ObConnectivityChecker **********************
ObConnectivityChecker::ObConnectivityChecker()
    : is_inited_(false), sql_proxy_(nullptr), rpc_proxy_(nullptr), tenant_id_(OB_INVALID_TENANT_ID)
{
}

int ObConnectivityChecker::init(common::ObMySQLProxy &sql_proxy,
                              obrpc::ObSrvRpcProxy &rpc_proxy,
                              const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObConnectivityChecker init twice", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    sql_proxy_ = &sql_proxy;
    rpc_proxy_ = &rpc_proxy;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_BACKUP_CLEAN_CONNECTIVITY_CHECK_FAIL);
int ObConnectivityChecker::check_dest_connectivity(const ObBackupPathString &backup_dest_str,
                                                   const ObBackupDestType::TYPE dest_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObConnectivityChecker not inited", K(ret));
  } else if (!ObBackupDestType::is_clean_valid(dest_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid clean dest type", K(ret), K(dest_type));
  } else if (backup_dest_str.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_INFO("backup dest string is empty, nothing to check", K(backup_dest_str));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ or rpc_proxy_ is null", K(ret));
  } else {
    ObBackupDestMgr dest_mgr;
    ObBackupPathString complete_backup_dest_str;
    ObBackupDest backup_dest;
    if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy_, tenant_id_, backup_dest_str, backup_dest))) {
      LOG_WARN("failed to get backup dest", K(ret), K(backup_dest_str));
    } else if (OB_FAIL(backup_dest.get_backup_dest_str(complete_backup_dest_str.ptr(), complete_backup_dest_str.capacity()))) {
      LOG_WARN("fail to get backup path str", K(ret), K(backup_dest));
    } else if (OB_FAIL(dest_mgr.init(tenant_id_, dest_type, complete_backup_dest_str, *sql_proxy_))) {
      LOG_WARN("failed to init dest manager", K(ret), K(tenant_id_), K(complete_backup_dest_str));
#ifdef ERRSIM
    } else if (OB_UNLIKELY(ERRSIM_BACKUP_CLEAN_CONNECTIVITY_CHECK_FAIL)) {
      ret = ERRSIM_BACKUP_CLEAN_CONNECTIVITY_CHECK_FAIL;
      LOG_WARN("errsim: connectivity check forced fail", K(ret), K(tenant_id_), K(complete_backup_dest_str), K(dest_type));
#endif
    } else if (OB_FAIL(dest_mgr.check_dest_validity(*rpc_proxy_, true/*need_format_file*/))) {
      LOG_WARN("failed to check backup dest validity", K(ret), K(tenant_id_), K(complete_backup_dest_str));
    }
    LOG_INFO("check_dest_connectivity ret", K(ret), K(tenant_id_), K(backup_dest_str), K(backup_dest), K(complete_backup_dest_str));
  }
  return ret;
}

//********************  ObBackupDeleteSelector **********************
ObBackupDeleteSelector::ObBackupDeleteSelector()
  : is_inited_(false),
    sql_proxy_(nullptr),
    schema_service_(nullptr),
    job_attr_(nullptr),
    rpc_proxy_(nullptr),
    delete_mgr_(nullptr),
    data_provider_(nullptr),
    connectivity_checker_(nullptr),
    archive_helper_(nullptr)
{}

ObBackupDeleteSelector::~ObBackupDeleteSelector()
{
  if (OB_NOT_NULL(data_provider_)) {
    OB_DELETE(IObBackupDataProvider, "BackupProvider", data_provider_);
    data_provider_ = nullptr;
  }
  if (OB_NOT_NULL(connectivity_checker_)) {
    OB_DELETE(IObConnectivityChecker, "ConnChecker", connectivity_checker_);
    connectivity_checker_ = nullptr;
  }
  if (OB_NOT_NULL(archive_helper_)) {
    OB_DELETE(ObArchivePersistHelper, "ArchiveHelper", archive_helper_);
    archive_helper_ = nullptr;
  }
}

int ObBackupDeleteSelector::init(common::ObMySQLProxy &sql_proxy,
                                 schema::ObMultiVersionSchemaService &schema_service,
                                 ObBackupCleanJobAttr &job_attr,
                                 obrpc::ObSrvRpcProxy &rpc_proxy,
                                 ObUserTenantBackupDeleteMgr &delete_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupDeleteSelector init twice", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    schema_service_ = &schema_service;
    job_attr_ = &job_attr;
    rpc_proxy_ = &rpc_proxy;
    delete_mgr_ = &delete_mgr;
    ObBackupDataProvider *data_provider_impl = OB_NEW(ObBackupDataProvider, "BackupProvider");
    ObConnectivityChecker *connectivity_checker_impl = OB_NEW(ObConnectivityChecker, "ConnChecker");
    ObArchivePersistHelper *archive_helper_impl = OB_NEW(ObArchivePersistHelper, "ArchiveHelper");

    if (OB_ISNULL(data_provider_impl) || OB_ISNULL(connectivity_checker_impl) || OB_ISNULL(archive_helper_impl)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for provider or connectivity checker objects", K(ret), K(data_provider_impl), K(connectivity_checker_impl));
    } else if (OB_FAIL(connectivity_checker_impl->init(sql_proxy, rpc_proxy, job_attr.tenant_id_))) {
      LOG_WARN("failed to init connectivity checker", K(ret));
    } else if (OB_FAIL(data_provider_impl->init(sql_proxy))) {
      LOG_WARN("failed to init data provider", K(ret));
    } else if (OB_FAIL(archive_helper_impl->init(job_attr.tenant_id_))) {
      LOG_WARN("failed to init archive helper", K(ret));
    } else {
      data_provider_ = data_provider_impl;
      data_provider_impl = nullptr;
      connectivity_checker_ = connectivity_checker_impl;
      connectivity_checker_impl = nullptr;
      archive_helper_ = archive_helper_impl;
      archive_helper_impl = nullptr;
      is_inited_ = true;
    }
    if (OB_NOT_NULL(data_provider_impl)) {
      OB_DELETE(ObBackupDataProvider, "BackupProvider", data_provider_impl);
    }
    if (OB_NOT_NULL(connectivity_checker_impl)) {
      OB_DELETE(ObConnectivityChecker, "ConnChecker", connectivity_checker_impl);
    }
    if (OB_NOT_NULL(archive_helper_impl)) {
      OB_DELETE(ObArchivePersistHelper, "ArchiveHelper", archive_helper_impl);
    }
  }
  return ret;
}


// Get the list of backup sets that are eligible for deletion based on user requests and policies.
int ObBackupDeleteSelector::get_delete_backup_set_infos(
    ObIArray<ObBackupSetFileDesc> &set_list)
{
  int ret = OB_SUCCESS;
  BackupCleanFailureInfo failure_info;
  common::hash::ObHashSet<int64_t> all_user_requested_ids_set;
  BackupGroupedSet candidate_data;
  set_list.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (OB_FAIL(init_user_requested_ids_set_(all_user_requested_ids_set))) {
    LOG_WARN("failed to init user requested ids set", K(ret));
  } else if (OB_FAIL(get_candidate_backups_(candidate_data, failure_info))) {
    LOG_WARN("failed to filter and group candidate backups", K(ret));
  } else if (candidate_data.candidates_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no valid candidate backup sets to delete, skip further checks", K(ret));
  } else if (OB_FAIL(connectivity_checker_->check_dest_connectivity(candidate_data.path_,
                        ObBackupDestType::TYPE::DEST_TYPE_BACKUP_DATA))) {
    LOG_WARN("failed to check dest connectivity", K(ret));
    failure_info.failure_id = candidate_data.candidates_.at(0).backup_set_id_;
    failure_info.failure_reason = "dest connectivity check failed";
  } else if (OB_FAIL(apply_current_path_retention_policy_(candidate_data, failure_info))) {
    LOG_WARN("failed to apply current path retention policy", K(ret));
  } else if (OB_FAIL(perform_dependency_check_(all_user_requested_ids_set, candidate_data,
                                   set_list, failure_info))) {
    LOG_WARN("failed to perform dependency check and final filtering", K(ret));
  } else {
    LOG_INFO("Final list of deletable backup sets generated", K(ret), K(set_list));
  }

  // Add failure reason if needed
  if (OB_FAIL(ret) && failure_info.failure_reason.length() > 0) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = add_failure_reason_(failure_info))) {
      LOG_WARN("failed to add failure reason", K(tmp_ret), K(failure_info));
    }
  }
  return ret;
}

int ObBackupDeleteSelector::init_user_requested_ids_set_(
    common::hash::ObHashSet<int64_t> &all_user_requested_ids_set) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (OB_FAIL(all_user_requested_ids_set.create(job_attr_->backup_set_ids_.count()))) {
    LOG_WARN("failed to create all_user_requested_ids_set", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < job_attr_->backup_set_ids_.count(); ++i) {
      if (OB_FAIL(all_user_requested_ids_set.set_refactored(job_attr_->backup_set_ids_.at(i)))) {
        LOG_WARN("failed to add member to all_user_requested_ids_set", K(ret), "id", job_attr_->backup_set_ids_.at(i));
      }
    }
  }
  return ret;
}

int ObBackupDeleteSelector::get_candidate_backups_(
    BackupGroupedSet &candidate_data,
    BackupCleanFailureInfo &failure_info)
{
  int ret = OB_SUCCESS;
  int64_t dest_id = INVALID_CLEAN_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (OB_ISNULL(data_provider_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data_provider is null", K(ret));
  } else {
    ObBackupSetFileDesc current_backup_set_info;
    for (int64_t i = 0; OB_SUCC(ret) && i < job_attr_->backup_set_ids_.count(); i++) {
      current_backup_set_info.reset();
      int64_t current_id = job_attr_->backup_set_ids_.at(i);
      if (OB_FAIL(data_provider_->get_one_backup_set_file(current_id, job_attr_->tenant_id_, current_backup_set_info))) {
        ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
        LOG_WARN("failed to get backup set file for request ID, stopping processing", K(ret), K(current_id));
        failure_info.failure_id = current_id;
        failure_info.failure_reason = "get set failed";
      } else if (!current_backup_set_info.is_valid()) {
        ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
        LOG_WARN("backup set info is invalid, cannot delete, stopping job", K(ret), K(current_backup_set_info));
        failure_info.failure_id = current_id;
        failure_info.failure_reason = "invalid";
      } else if (ObBackupSetFileDesc::BackupSetStatus::DOING == current_backup_set_info.status_) {
        ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
        LOG_WARN("backup set is DOING, cannot delete, stopping job", K(ret), K(current_backup_set_info));
        failure_info.failure_id = current_id;
        failure_info.failure_reason = "status=DOING";
      } else if (ObBackupFileStatus::BACKUP_FILE_DELETED == current_backup_set_info.file_status_) {
        ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
        LOG_WARN("backup set has already been marked as DELETED, cannot delete again", K(ret), K(current_backup_set_info));
        failure_info.failure_id = current_id;
        failure_info.failure_reason = "already deleted";
      }
      // if backup set status is FAILED or file status is DELETING, allow delete

      if (OB_SUCC(ret)) {
        if (INVALID_CLEAN_ID == dest_id) {
          dest_id = current_backup_set_info.dest_id_;
          candidate_data.path_ = current_backup_set_info.backup_path_; // the path is from OB_ALL_BACKUP_SET_FILES_TNAME
        } else if (dest_id != current_backup_set_info.dest_id_) {
          ret = OB_NOT_SUPPORTED;
          failure_info.failure_id = current_backup_set_info.backup_set_id_;
          failure_info.failure_reason = "delete from multiple dest is not supported";
          LOG_WARN("does not support deleting backup sets from multiple destinations in one command", K(ret),
              K(dest_id), "new_dest_id", current_backup_set_info.dest_id_);
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(candidate_data.candidates_.push_back(current_backup_set_info))) {
          LOG_WARN("failed to push backup set desc to candidate list", K(ret), K(current_backup_set_info));
        }
      }
    }
  }

  // Sort candidate backups after grouping is complete
  if (OB_FAIL(ret)) {
  } else if (!candidate_data.candidates_.empty()) {
    CompareBackupSetInfo backup_set_info_cmp;
    lib::ob_sort(candidate_data.candidates_.begin(), candidate_data.candidates_.end(), backup_set_info_cmp);
  }
  LOG_INFO("delete candidate backup sets", K(ret), K(candidate_data));
  return ret;
}

int ObBackupDeleteSelector::add_failure_reason_(const BackupCleanFailureInfo &failure_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (failure_info.failure_reason.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, failure_reason is empty", K(ret), K(failure_info));
  } else if (INVALID_CLEAN_ID == failure_info.failure_id) {
    if (OB_FAIL(databuff_printf(job_attr_->failure_reason_.ptr(), job_attr_->failure_reason_.capacity(),
        "%s", failure_info.failure_reason.ptr()))) {
      LOG_WARN("failed to format failure reason", K(ret), K(failure_info));
    }
  } else if (INVALID_CLEAN_ID == failure_info.related_id) {
    if (OB_FAIL(databuff_printf(job_attr_->failure_reason_.ptr(), job_attr_->failure_reason_.capacity(),
        "id=%ld(%s)", failure_info.failure_id, failure_info.failure_reason.ptr()))) {
      LOG_WARN("failed to format failure reason", K(ret), K(failure_info));
    }
  } else {
    if (OB_FAIL(databuff_printf(job_attr_->failure_reason_.ptr(), job_attr_->failure_reason_.capacity(),
        "id=%ld(%s id=%ld)", failure_info.failure_id, failure_info.failure_reason.ptr(), failure_info.related_id))) {
      LOG_WARN("failed to format failure reason", K(ret), K(failure_info));
    }
  }
  LOG_INFO("backup clean failure reason", K(ret), K(job_attr_->failure_reason_));
  return ret;
}

// Apply retention policy for current active backup path:
// 1. if delete policy(recovery_window) is set, then not allow to delete any backup set on the current active backup path
// 2. if delete policy(recovery_window) is not set, then allow to delete any backup set on the current active backup path,
//      except the backup set is newer than the latest full backup on the current path
int ObBackupDeleteSelector::apply_current_path_retention_policy_(
    const ObBackupDeleteSelector::BackupGroupedSet &candidate_data,
    BackupCleanFailureInfo &failure_info)
{
  int ret = OB_SUCCESS;
  ObBackupPathString current_backup_dest_str;
  ObBackupPathString current_backup_path_str;
  ObBackupSetFileDesc clean_point;
  ObBackupDest backup_dest;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (candidate_data.candidates_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no valid candidate backup sets to delete", K(ret));
  } else if (OB_FAIL(data_provider_->get_backup_dest(job_attr_->tenant_id_, current_backup_dest_str))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get backup dest", K(ret));
    }
  } else if (current_backup_dest_str.is_empty()) {
    LOG_INFO("backup destination string is empty, just pass");
  } else if (OB_FAIL(backup_dest.set(current_backup_dest_str))) {
    LOG_WARN("fail to set backup dest", K(ret), K(current_backup_dest_str));
  } else if (OB_FAIL(backup_dest.get_backup_path_str(current_backup_path_str.ptr(), current_backup_path_str.capacity()))) {
    LOG_WARN("fail to get backup path str", K(ret), K(backup_dest));
  } else {
    // Apply retention policy to current path
    const ObArray<ObBackupSetFileDesc> &candidate_descs_in_group = candidate_data.candidates_;
    const ObBackupPathString &candidate_path = candidate_data.path_;
    LOG_INFO("candidate_path", K(candidate_path), K(current_backup_path_str));
    if (candidate_path == current_backup_path_str) {
      bool policy_exist = false;
      if (OB_FAIL(data_provider_->is_delete_policy_exist(job_attr_->tenant_id_, policy_exist))) {
        LOG_WARN("failed to check policy exist", K(ret));
      } else if (policy_exist) {
        ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
        LOG_WARN("cannot delete set in current path when delete policy(recovery_window) is set", K(ret), K(clean_point));
        failure_info.failure_id = INVALID_CLEAN_ID;
        failure_info.failure_reason = "cannot delete backup set in current path when delete policy is set";
      } else {
        // get the latest full backup set for the current path
        if (OB_FAIL(data_provider_->get_latest_valid_full_backup_set(
                      job_attr_->tenant_id_, *archive_helper_, current_backup_path_str, clean_point))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
            LOG_WARN("No full backup exists in this dest. No sets will be deleted", K(ret), K(candidate_path));
            failure_info.failure_id = INVALID_CLEAN_ID;
            failure_info.failure_reason = "no full backup exists";
          } else {
            LOG_WARN("failed to get latest full backup set", K(ret));
          }
        } else {
          const ObBackupSetFileDesc &largest_desc = candidate_descs_in_group.at(candidate_descs_in_group.count() - 1);
          if (largest_desc.backup_set_id_ >= clean_point.backup_set_id_) {
            ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
            LOG_WARN("Candidate backup set is newer than or equal to the latest full backup on the current path",
              K(ret), "candidate_set_id", largest_desc.backup_set_id_, "latest_full_set_id", clean_point.backup_set_id_,
              K(candidate_path));
            failure_info.failure_id = largest_desc.backup_set_id_;
            failure_info.failure_reason = "newer than latest full backupset";
            failure_info.related_id = clean_point.backup_set_id_;
          }
        }
      }
    }
  }
  return ret;
}

// Perform dependency check
int ObBackupDeleteSelector::perform_dependency_check_(
    const common::hash::ObHashSet<int64_t> &requested_deletion_ids,
    const ObBackupDeleteSelector::BackupGroupedSet &candidate_data,
    ObIArray<ObBackupSetFileDesc> &set_list,
    BackupCleanFailureInfo &failure_info)
{
  int ret = OB_SUCCESS;
  CompareBackupSetInfo backup_set_info_cmp;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else {
    ObArray<ObBackupSetFileDesc> sets_in_same_dest;
    const ObArray<ObBackupSetFileDesc> &candidate_descs_in_group = candidate_data.candidates_;
    if (!candidate_descs_in_group.empty()) {
      const int64_t current_dest_id = candidate_descs_in_group.at(0).dest_id_;
      if (OB_FAIL(data_provider_->get_backup_set_files_specified_dest(
                      job_attr_->tenant_id_, current_dest_id, sets_in_same_dest))) {
        //TODO(yuhan): this can be optimized because when change dest, we only can lanch full backup
        LOG_WARN("failed to get all backup sets for group dependency check", K(ret), K(current_dest_id));
      } else {
        lib::ob_sort(sets_in_same_dest.begin(), sets_in_same_dest.end(), backup_set_info_cmp);
        // Iterate backwards from the largest backup_set_id to the smallest
        for (int64_t i = candidate_descs_in_group.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
          const ObBackupSetFileDesc &candidate_desc = candidate_descs_in_group.at(i);
          bool is_depended_on = false;
          int64_t dependent_id = INVALID_CLEAN_ID;
          if (OB_FAIL(is_backup_set_depended_on_(candidate_desc.backup_set_id_, current_dest_id,
                          sets_in_same_dest, requested_deletion_ids, is_depended_on, dependent_id))) {
            LOG_WARN("failed to check dependency for backup set", K(ret), K(candidate_desc));
          } else if (is_depended_on) {
            ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
            LOG_WARN("Backup set cannot be deleted due to dependency, stopping job", K(ret),
                "candidate_backup_set_id", candidate_desc.backup_set_id_, K(current_dest_id), K(dependent_id));
            failure_info.failure_id = candidate_desc.backup_set_id_;
            failure_info.failure_reason = "dependent by backup set";
            failure_info.related_id = dependent_id;
          } else if (OB_FAIL(set_list.push_back(candidate_desc))) {
            LOG_WARN("failed to push back set desc", K(ret), K(candidate_desc));
          }
        }
      }
    }
  }

  return ret;
}

// Determines if a candidate backup set is dependent by a subsequent, non-deleted backup set.
int ObBackupDeleteSelector::is_backup_set_depended_on_(
    const int64_t candidate_backup_set_id,
    const int64_t current_dest_id,
    const ObArray<ObBackupSetFileDesc> &sets_in_same_dest,
    const hash::ObHashSet<int64_t> &requested_deletion_ids,
    bool &is_depended_on,
    int64_t &dependent_id)
{
  int ret = OB_SUCCESS;
  int64_t current_id_to_check = candidate_backup_set_id;
  is_depended_on = false;
  dependent_id = -1;

  // Find the immediate subsequent backup set that might depend on the candidate.
  ObBackupSetFileDesc dependent_backup_desc;
  bool found_dependent = false;
  CompareBackupSetInfo backup_set_info_cmp;
  ObBackupSetFileDesc target_desc;
  target_desc.backup_set_id_ = current_id_to_check;
  ObArray<ObBackupSetFileDesc>::const_iterator iter =
      std::upper_bound(sets_in_same_dest.begin(), sets_in_same_dest.end(), target_desc, backup_set_info_cmp);
  if (iter != sets_in_same_dest.end()) {
    const ObBackupSetFileDesc &desc = *iter;
    // Verify that the found backup set is valid and explicitly depends on the candidate.
    if ((ObBackupFileStatus::BACKUP_FILE_AVAILABLE == desc.file_status_
            || ObBackupFileStatus::BACKUP_FILE_COPYING == desc.file_status_)
         && desc.dest_id_ == current_dest_id
         && (desc.prev_inc_backup_set_id_ == current_id_to_check
            || desc.prev_full_backup_set_id_ == current_id_to_check)) {
      dependent_backup_desc = desc;
      found_dependent = true;
    }
  }

  // Determine the dependency status based on the dependent backup.
  if (!found_dependent) {
    // The candidate has no subsequent dependent backup, so it is not dependent.
    is_depended_on = false;
  } else {
    int hash_result = requested_deletion_ids.exist_refactored(dependent_backup_desc.backup_set_id_);
    if (OB_HASH_NOT_EXIST == hash_result) {
      is_depended_on = true;
      dependent_id = dependent_backup_desc.backup_set_id_;
    } else if (OB_HASH_EXIST == hash_result) {
      is_depended_on = false;
    } else {
      ret = hash_result;
      LOG_ERROR("requested_deletion_ids exist_refactored failed", K(ret), "key(id)", dependent_backup_desc.backup_set_id_);
    }
  }
  return ret;
}

int ObBackupDeleteSelector::get_delete_backup_piece_infos(
    ObIArray<ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS;
  BackupCleanFailureInfo failure_info;
  BackupGroupedPiece candidate_data;
  ObArray<ObTenantArchivePieceAttr> final_deletable_pieces;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (OB_FAIL(get_candidate_pieces_(candidate_data, failure_info))) {
    LOG_WARN("failed to group candidate pieces", K(ret));
  } else if (candidate_data.candidates_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_INFO("no valid candidate backup pieces to delete, skip further checks");
  } else if (OB_FAIL(connectivity_checker_->check_dest_connectivity(candidate_data.path_,
                        ObBackupDestType::TYPE::DEST_TYPE_ARCHIVE_LOG))) {
    LOG_WARN("failed to check piece path valid", K(ret));
    failure_info.failure_id = candidate_data.candidates_.at(0).key_.piece_id_;
    failure_info.failure_reason = "piece path valid check failed";
  } else if (OB_FAIL(apply_current_path_piece_retention_policy_(candidate_data, failure_info))) {
    LOG_WARN("failed to apply current path piece retention policy", K(ret));
  } else if (OB_FAIL(perform_piece_sequential_check_(candidate_data, piece_list, failure_info))) {
    LOG_WARN("failed to perform piece sequential check and final filtering", K(ret));
  }

  // Add failure reason if needed
  if (OB_FAIL(ret) && failure_info.failure_reason.length() > 0) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = add_failure_reason_(failure_info))) {
      LOG_WARN("failed to add failure reason", K(tmp_ret), K(failure_info));
    }
  }

  LOG_INFO("Final list of deletable pieces generated", K(ret), K(piece_list));
  return ret;
}

// Initial filtering of user-requested archivelog pieces and grouping by dest_id
int ObBackupDeleteSelector::get_candidate_pieces_(
    BackupGroupedPiece &candidate_data,
    BackupCleanFailureInfo &failure_info)
{
  int ret = OB_SUCCESS;
  ObTenantArchivePieceAttr current_piece_info;
  int64_t dest_id = INVALID_CLEAN_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (OB_ISNULL(data_provider_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data_provider_ is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < job_attr_->backup_piece_ids_.count(); ++i) {
      current_piece_info.reset();
      int64_t current_id = job_attr_->backup_piece_ids_.at(i);
      int64_t piece_count = job_attr_->backup_piece_ids_.count();
      if (OB_FAIL(archive_helper_->get_piece(*sql_proxy_, current_id, false, current_piece_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
          LOG_WARN("archivelog piece with ID does not exist, stopping job", K(ret), K(current_id));
          failure_info.failure_id = current_id;
          failure_info.failure_reason = "not exist";
        } else {
          LOG_WARN("failed to get archivelog piece file for ID, stopping processing", K(ret), K(current_id));
        }
      } else if (!current_piece_info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("archivelog piece info is invalid", K(ret), K(current_piece_info));
        failure_info.failure_id = current_id;
        failure_info.failure_reason = "invalid";
      } else if (ObBackupFileStatus::BACKUP_FILE_DELETED == current_piece_info.file_status_) {
        ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
        LOG_WARN("archivelog piece has already been marked as DELETED, cannot delete again, stopping job",
                  K(ret), K(current_piece_info));
        failure_info.failure_id = current_id;
        failure_info.failure_reason = "already deleted";
      } else if (current_piece_info.status_.is_active()) {
        ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
        LOG_WARN("archivelog piece is active, cannot delete, stopping job", K(ret), K(current_piece_info));
        failure_info.failure_id = current_id;
        failure_info.failure_reason = "is active piece";
      } else {
        if (INVALID_CLEAN_ID == dest_id) {
          dest_id = current_piece_info.key_.dest_id_;
          candidate_data.path_ = current_piece_info.path_;
        } else if (dest_id != current_piece_info.key_.dest_id_) {
          ret = OB_NOT_SUPPORTED;
          failure_info.failure_id = current_piece_info.key_.piece_id_;
          failure_info.failure_reason = "delete from multiple dest is not supported";
          LOG_WARN("does not support deleting backup pieces from multiple destinations in one command", K(ret),
                  K(dest_id), "new_dest_id", current_piece_info.key_.dest_id_);
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(candidate_data.candidates_.push_back(current_piece_info))) {
          LOG_WARN("failed to push archivelog piece desc to candidate list", K(ret), K(current_piece_info));
        }
      }
    }
  }
  return ret;
}

//********************  ObBackupDataProvider **********************
ObBackupDataProvider::ObBackupDataProvider() : sql_proxy_(nullptr), is_inited_(false) {}

int ObBackupDataProvider::init(common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupDataProvider init twice", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}
int ObBackupDataProvider::get_one_backup_set_file(
    const int64_t backup_set_id,
    const uint64_t tenant_id,
    ObBackupSetFileDesc &backup_set_desc)
{
  int ret = OB_SUCCESS;
  const int64_t incarnation = OB_START_INCARNATION;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataProvider not inited", K(ret));
  } else if (OB_FAIL(ObBackupSetFileOperator::get_one_backup_set_file(
      *sql_proxy_, false, backup_set_id, incarnation, tenant_id, backup_set_desc))) {
    LOG_WARN("failed to get one backup set file", K(ret), K(backup_set_id), K(tenant_id));
  }
  return ret;
}

int ObBackupDataProvider::get_backup_set_files_specified_dest(
    const uint64_t tenant_id,
    const int64_t dest_id,
    common::ObIArray<ObBackupSetFileDesc> &backup_set_infos)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataProvider not inited", K(ret));
  } else if (OB_FAIL(ObBackupSetFileOperator::get_backup_set_files_specified_dest(
      *sql_proxy_, tenant_id, dest_id, backup_set_infos))) {
    LOG_WARN("failed to get backup set files specified dest", K(ret), K(tenant_id), K(dest_id));
  }
  return ret;
}

int ObBackupDataProvider::get_oldest_full_backup_set(
    const uint64_t tenant_id,
    const ObBackupPathString &backup_path,
    ObBackupSetFileDesc &oldest_backup_desc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataProvider not inited", K(ret));
  } else if (OB_FAIL(ObBackupSetFileOperator::get_oldest_full_backup_set(
      *sql_proxy_, tenant_id, backup_path.ptr(), oldest_backup_desc))) {
    LOG_WARN("failed to get oldest full backup set", K(ret), K(tenant_id), K(backup_path));
  }
  return ret;
}

int ObBackupDataProvider::get_latest_valid_full_backup_set(
    const uint64_t tenant_id,
    ObArchivePersistHelper &archive_helper,
    const ObBackupPathString &backup_path,
    ObBackupSetFileDesc &latest_backup_desc)
{
  int ret = OB_SUCCESS;
  int64_t backup_set_id_limit = INT64_MAX;
  bool can_used_to_restore = false;
  common::ObArray<std::pair<int64_t, int64_t>> dest_array;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataProvider not inited", K(ret));
  } else if (OB_FAIL(archive_helper.get_valid_dest_pairs(*sql_proxy_, dest_array))) {
    LOG_WARN("failed to get valid dest pairs", K(ret));
  } else if (0 == dest_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, no valid dest found", K(ret));
  } else if (1 != dest_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, more than one valid dest found", K(ret), K(dest_array));
  } else {
    while (OB_SUCC(ret) && !can_used_to_restore) {
      if (OB_FAIL(ObBackupSetFileOperator::get_latest_full_backup_set_with_limit(
          *sql_proxy_, tenant_id, backup_path.ptr(), backup_set_id_limit, latest_backup_desc))) {
        LOG_WARN("failed to get latest full backup set with limit",
                    K(ret), K(tenant_id), K(backup_path), K(backup_set_id_limit));
      } else {
        if (latest_backup_desc.plus_archivelog_) {
          can_used_to_restore = true;
          LOG_INFO("get latest valid full backup set with plus archivelog",
                    K(ret), K(tenant_id), K(backup_path), K(latest_backup_desc));
        } else {
          if (OB_FAIL(archive_helper.check_piece_continuity_between_two_scn(
                        *sql_proxy_, dest_array.at(0).second, latest_backup_desc.start_replay_scn_,
                        latest_backup_desc.min_restore_scn_, can_used_to_restore))) {
            LOG_WARN("failed to check piece continuity between two scn",
                      K(ret), K(tenant_id), K(backup_path), K(latest_backup_desc));
          } else if (!can_used_to_restore) {
            backup_set_id_limit = latest_backup_desc.backup_set_id_;
            LOG_INFO("get latest valid full backup set", K(ret), K(tenant_id),
                      K(backup_path), K(latest_backup_desc));
          }
        }
      }
    }
  }

  return ret;
}

int ObBackupDataProvider::get_backup_dest(
    const uint64_t tenant_id,
    ObBackupPathString &path)
{
  int ret = OB_SUCCESS;
  ObBackupHelper backup_helper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataProvider not inited", K(ret));
  } else if (OB_FAIL(backup_helper.init(tenant_id, *sql_proxy_))) {
    LOG_WARN("failed to init backup helper", K(ret), K(tenant_id));
  } else if (OB_FAIL(backup_helper.get_backup_dest(path))) {
    LOG_WARN("failed to get backup dest", K(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupDataProvider::get_dest_id(
    const uint64_t tenant_id,
    const ObBackupDest &backup_dest,
    int64_t &dest_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataProvider not inited", K(ret));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(*sql_proxy_, tenant_id, backup_dest, dest_id))) {
    LOG_WARN("failed to get dest_id", K(ret), K(tenant_id), K(backup_dest));
  }
  return ret;
}

int ObBackupDataProvider::get_dest_type(
    const uint64_t tenant_id,
    const ObBackupDest &backup_dest,
    ObBackupDestType::TYPE &dest_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataProvider not inited", K(ret));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_type(*sql_proxy_, tenant_id, backup_dest, dest_type))) {
    LOG_WARN("failed to get dest_type", K(ret), K(tenant_id), K(backup_dest));
  }
  return ret;
}

int ObBackupDataProvider::get_candidate_obsolete_backup_sets(
    const uint64_t tenant_id,
    const int64_t expired_time,
    const char *backup_path_str,
    common::ObIArray<share::ObBackupSetFileDesc> &backup_set_infos)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataProvider not inited", K(ret));
  } else if (OB_FAIL(ObBackupSetFileOperator::get_candidate_obsolete_backup_sets(
        *sql_proxy_, tenant_id, expired_time, backup_path_str, backup_set_infos))) {
    LOG_WARN("failed to get candidate obsolete backup sets", K(ret), K(tenant_id), K(expired_time));
  }
  return ret;
}

int ObBackupDataProvider::is_delete_policy_exist(const uint64_t tenant_id, bool &exist)
{
  int ret = OB_SUCCESS;
  ObDeletePolicyAttr delete_policy;
  exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataProvider not inited", K(ret));
  } else if (OB_FAIL(ObDeletePolicyOperator::get_delete_policy(*sql_proxy_, tenant_id, delete_policy))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get delete policy", K(ret), K(tenant_id));
    } else {
      ret = OB_SUCCESS;
      LOG_WARN("no delete policy", K(ret), K(tenant_id));
    }
  } else {
    exist = true;
  }
  return ret;
}

int ObBackupDataProvider::load_piece_info_desc(
    const uint64_t tenant_id,
    const ObTenantArchivePieceAttr &piece_attr,
    ObPieceInfoDesc &piece_info_desc)
{
  int ret = OB_SUCCESS;
  ObArchiveStore archive_store;
  bool piece_info_exist = false;
  ObBackupDest backup_dest;
  ObBackupPathString complete_backup_dest_str;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataProvider not inited", K(ret));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy_, tenant_id, piece_attr.path_, backup_dest))) {
    LOG_WARN("Failed to get backup dest with storage info", K(ret), K(tenant_id), K(piece_attr.path_));
  } else if (OB_FAIL(backup_dest.get_backup_dest_str(complete_backup_dest_str.ptr(), complete_backup_dest_str.capacity()))) {
    LOG_WARN("fail to get backup path str", K(ret), K(backup_dest));
  } else if (OB_FAIL(archive_store.init(backup_dest))) {
    LOG_WARN("Failed to init archive store", K(ret), K(complete_backup_dest_str));
  } else if (OB_FAIL(archive_store.is_piece_info_file_exist(piece_attr.key_.dest_id_,
              piece_attr.key_.round_id_, piece_attr.key_.piece_id_, piece_info_exist))) {
    LOG_WARN("Failed to check piece info file exist", K(ret), K(piece_attr.key_));
  } else if (!piece_info_exist) {
    ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
    LOG_WARN("Piece info file not exist, piece may be active", K(piece_attr.key_));
  } else if (OB_FAIL(archive_store.read_piece_info(piece_attr.key_.dest_id_,
              piece_attr.key_.round_id_, piece_attr.key_.piece_id_, piece_info_desc))) {
    LOG_WARN("Failed to read piece info", K(ret), K(piece_attr.key_));
  }
  return ret;
}


// Apply retention policy for current active backup path: keep pieces that are needed for the latest full backup
// 1. if now db only do archive, then not allow to delete any piece on the current active backup path
// 2. if now db do archive and backup, then allow to delete piece on the current active backup path, but
//    need to ensure the piece protected by backupset is not deleted
int ObBackupDeleteSelector::apply_current_path_piece_retention_policy_(
    const BackupGroupedPiece &candidate_data,
    BackupCleanFailureInfo &failure_info)
{
  int ret = OB_SUCCESS;
  ObBackupSetFileDesc clog_data_clean_point;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else {
    // only apply retention policy for the current writing backup path
    common::ObArray<std::pair<int64_t, int64_t>> dest_array;
    if (OB_FAIL(archive_helper_->get_valid_dest_pairs(*sql_proxy_, dest_array))) {
      LOG_WARN("failed to get valid dest pairs", K(ret));
    } else if (0 == dest_array.count()) {
      // do nothing
    } else if (1 != dest_array.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, more than one valid dest found", K(ret), K(dest_array));
    } else {
      const ObArray<ObTenantArchivePieceAttr> &candidate_pieces_in_group = candidate_data.candidates_;
      const ObBackupPathString &requested_path = candidate_data.path_;
      const int64_t requested_dest_id = candidate_data.candidates_.at(0).key_.dest_id_;
      // if current group is on the current writing archive path, then apply retention policy
      if (requested_dest_id == dest_array.at(0).second) {
        bool policy_exist = false;
        if (OB_FAIL(data_provider_->is_delete_policy_exist(job_attr_->tenant_id_, policy_exist))) {
          LOG_WARN("failed to check policy exist", K(ret));
        } else {
          LOG_INFO("policy_exist", K(policy_exist));
          if (policy_exist) {
            ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
            LOG_WARN("cannot delete backup piece in current path when delete policy(recovery_window) is set",
                          K(ret), K(requested_path));
            failure_info.failure_id = INVALID_CLEAN_ID;
            failure_info.failure_reason = "cannot delete backup piece in current path when delete policy is set";
          } else {
            // get the oldest full backup set on the current writing backup path
            if (OB_FAIL(get_oldest_full_backup_set_(clog_data_clean_point, failure_info))) {
              LOG_WARN("failed to get oldest full backup set for retention", K(ret));
            } else {
              // check the piece is needed for the oldest full backup
              for (int64_t i = 0; OB_SUCC(ret) && i < candidate_pieces_in_group.count(); ++i) {
                const ObTenantArchivePieceAttr &piece = candidate_pieces_in_group.at(i);
                if (piece.end_scn_ >= clog_data_clean_point.start_replay_scn_) {
                  ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
                  LOG_WARN("Piece is needed for the oldest full backup",
                            K(piece), K(clog_data_clean_point), K(requested_path));
                  failure_info.failure_id = piece.key_.piece_id_;
                  failure_info.failure_reason = "needed for oldest full backup";
                  failure_info.related_id = clog_data_clean_point.backup_set_id_;
                }
              }
            }
          }
        }
      }
    }
  }
  LOG_INFO("backup clean piece retention current path policy", K(ret), K(candidate_data));
  return ret;
}

// Get the oldest full backup set on the current writing backup path
int ObBackupDeleteSelector::get_oldest_full_backup_set_(
    ObBackupSetFileDesc &oldest_full_backup_desc,
    BackupCleanFailureInfo &failure_info)
{
  int ret = OB_SUCCESS;
  ObBackupPathString current_backup_dest_str;
  ObBackupPathString current_backup_path_str;
  ObBackupDest backup_dest;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (OB_FAIL(data_provider_->get_backup_dest(job_attr_->tenant_id_, current_backup_dest_str))) {
    ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
    LOG_WARN("failed to get backup set dest", K(ret), K(current_backup_path_str));
    failure_info.failure_id = -1;
    failure_info.failure_reason = "no backup set dest";
  } else if (OB_FAIL(backup_dest.set(current_backup_dest_str))) {
    LOG_WARN("fail to set backup dest", K(ret), K(current_backup_dest_str));
  } else if (OB_FAIL(backup_dest.get_backup_path_str(
                          current_backup_path_str.ptr(), current_backup_path_str.capacity()))) {
    LOG_WARN("fail to get backup path str", K(ret), K(backup_dest));
  } else if (current_backup_path_str.is_empty()) {
    ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
    failure_info.failure_id = -1;
    failure_info.failure_reason = "no backup set dest";
  } else if (OB_FAIL(data_provider_->get_oldest_full_backup_set(
                      job_attr_->tenant_id_, current_backup_path_str, oldest_full_backup_desc))) {
    LOG_WARN("failed to get oldest full backup set", K(ret), K(current_backup_path_str));
    failure_info.failure_id = -1;
    failure_info.failure_reason = "no full backup exists";
  }
  return ret;
}

// Piece deletion must be sequential. A piece can only be deleted if the previous piece is deleted
int ObBackupDeleteSelector::perform_piece_sequential_check_(
    BackupGroupedPiece &candidate_data,
    ObIArray<ObTenantArchivePieceAttr> &piece_list,
    BackupCleanFailureInfo &failure_info)
{
  int ret = OB_SUCCESS;
  CompareBackupPieceInfo backup_piece_info_cmp;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else {
    ObArray<ObTenantArchivePieceAttr> pieces_in_same_dest;
    ObArray<ObTenantArchivePieceAttr> &candidate_pieces_in_group = candidate_data.candidates_;
    if (candidate_pieces_in_group.empty()) {
      // do nothing
    } else {
      const int64_t current_dest_id = candidate_pieces_in_group.at(0).key_.dest_id_;
      if (OB_FAIL(archive_helper_->get_pieces(*sql_proxy_, current_dest_id, pieces_in_same_dest))) {
        LOG_WARN("failed to get all pieces in dest", K(ret), K(current_dest_id));
      } else {
        ob_sort(pieces_in_same_dest.begin(), pieces_in_same_dest.end(), backup_piece_info_cmp);
        ob_sort(candidate_pieces_in_group.begin(), candidate_pieces_in_group.end(), backup_piece_info_cmp);
        int64_t idx_of_same_dest_piece = 0;
        if (OB_FAIL(check_previous_pieces_are_deleted_(pieces_in_same_dest,
              candidate_pieces_in_group.at(0).key_.piece_id_, idx_of_same_dest_piece, failure_info))) {
          LOG_WARN("failed to check previous pieces are deleted", K(ret));
        } else if (OB_FAIL(check_candidate_pieces_are_continuous_(pieces_in_same_dest, candidate_pieces_in_group,
                                                     idx_of_same_dest_piece, failure_info))) {
          LOG_WARN("failed to check candidate pieces are contiguous", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < candidate_pieces_in_group.count(); ++i) {
            if (OB_FAIL(piece_list.push_back(candidate_pieces_in_group.at(i)))) {
              LOG_WARN("failed to push back candidate pieces to final list", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

// Check if previous pieces (smaller than first candidate) are already deleted
int ObBackupDeleteSelector::check_previous_pieces_are_deleted_(
    const ObIArray<ObTenantArchivePieceAttr> &pieces_in_same_dest,
    const int64_t &first_candidate_piece_id, int64_t &idx_of_same_dest_piece,
    BackupCleanFailureInfo &failure_info)
{
  int ret = OB_SUCCESS;
  int64_t count_of_pieces_in_same_dest = pieces_in_same_dest.count();
  // Iterate through pieces smaller than the first candidate
  while (OB_SUCC(ret)
         && idx_of_same_dest_piece < count_of_pieces_in_same_dest
         && pieces_in_same_dest.at(idx_of_same_dest_piece).key_.piece_id_ < first_candidate_piece_id) {
    const ObTenantArchivePieceAttr &current_piece = pieces_in_same_dest.at(idx_of_same_dest_piece);
    if (ObBackupFileStatus::BACKUP_FILE_DELETED != current_piece.file_status_) {
      // Found a non-deleted piece that is smaller than the first candidate
      ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
      LOG_WARN("Sequential deletion violated. A smaller, non-deleted piece exists.", K(ret),
               "smaller_piece_id", current_piece.key_.piece_id_,
               "first_requested_piece_id", first_candidate_piece_id);
      failure_info.failure_id = first_candidate_piece_id;
      failure_info.failure_reason = "smaller piece exists";
      failure_info.related_id = current_piece.key_.piece_id_;
      break;
    }
    idx_of_same_dest_piece++;
  }
  // after this loop, all_idx points to the first piece that is >= first_candidate
  return ret;
}

int ObBackupDeleteSelector::check_candidate_pieces_are_continuous_(
    const ObIArray<ObTenantArchivePieceAttr> &pieces_in_same_dest,
    const ObIArray<ObTenantArchivePieceAttr> &candidate_pieces,
    int64_t &idx_of_same_dest_piece, BackupCleanFailureInfo &failure_info)
{
  int ret = OB_SUCCESS;
  int64_t idx_of_candidate_piece = 0;
  // Ensure continuous matching
  while (OB_SUCC(ret)
         && idx_of_same_dest_piece < pieces_in_same_dest.count()
         && idx_of_candidate_piece < candidate_pieces.count()) {
    const ObTenantArchivePieceAttr &db_piece = pieces_in_same_dest.at(idx_of_same_dest_piece);
    const ObTenantArchivePieceAttr &cand_piece = candidate_pieces.at(idx_of_candidate_piece);
    if (db_piece.key_.piece_id_ == cand_piece.key_.piece_id_) {
      // Matched. Move both pointers forward.
      if (ObBackupFileStatus::BACKUP_FILE_DELETED == db_piece.file_status_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Candidate piece is already marked as deleted, should have been filtered", K(ret), K(db_piece));
        break;
      }
      ++idx_of_same_dest_piece;
      ++idx_of_candidate_piece;
    } else {
      // Gap detected. The piece in DB is not in the candidate list.
      ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
      LOG_WARN("Gap in deletion request. A piece exists but was not requested for deletion.", K(ret),
               "existing_piece_id", db_piece.key_.piece_id_,
               "expected_candidate_piece_id", cand_piece.key_.piece_id_);
      failure_info.failure_id = cand_piece.key_.piece_id_;
      failure_info.failure_reason = "smaller piece exists";
      failure_info.related_id = db_piece.key_.piece_id_;
    }
  }
  // Final check: ensure all candidates were found and matched.
  if (OB_SUCC(ret) && idx_of_candidate_piece != candidate_pieces.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("A requested candidate piece was not found in the DB list during contiguous check.", K(ret),
             "candidates_processed", idx_of_candidate_piece,
             "total_candidates", candidate_pieces.count(),
             "unmatched_candidate_id", candidate_pieces.at(idx_of_candidate_piece).key_.piece_id_);
    failure_info.failure_id = candidate_pieces.at(idx_of_candidate_piece).key_.piece_id_;
    failure_info.failure_reason = "candidate not found";
  }

  return ret;
}

int ObBackupDeleteSelector::get_delete_backup_all_infos(
    ObIArray<ObBackupSetFileDesc> &set_list,
    ObIArray<ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS;
  BackupCleanFailureInfo failure_info;
  ObBackupDestType::TYPE dest_type = ObBackupDestType::TYPE::DEST_TYPE_MAX;
  ObBackupDest backup_dest;
  ObBackupPathString backup_dest_str;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (OB_FAIL(backup_dest.set(job_attr_->backup_path_))) {
    LOG_WARN("failed to set backup dest", K(ret), "dest_path", job_attr_->backup_path_);
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(
                        *sql_proxy_, job_attr_->tenant_id_, backup_dest, job_attr_->dest_id_))) {
    LOG_WARN("failed to get dest_id", K(ret), "tenant_id", job_attr_->tenant_id_, "dest_id", job_attr_->dest_id_);
  } else if (ObBackupDestType::TYPE::DEST_TYPE_BACKUP_DATA == job_attr_->backup_path_type_) {
    if (OB_FAIL(check_current_backup_dest_(failure_info))) {
      LOG_WARN("failed to check current backup dest", K(ret));
    } else if (OB_FAIL(backup_dest.get_backup_path_str(backup_dest_str.ptr(), backup_dest_str.capacity()))) {
      LOG_WARN("failed to get backup dest str", K(ret));
    } else if (OB_FAIL(connectivity_checker_->check_dest_connectivity(
                                   backup_dest_str, ObBackupDestType::TYPE::DEST_TYPE_BACKUP_DATA))) {
      LOG_WARN("failed to check connectivity", K(ret));
    }
  } else if (ObBackupDestType::TYPE::DEST_TYPE_ARCHIVE_LOG == job_attr_->backup_path_type_) {
    if (OB_FAIL(check_current_archive_dest_(failure_info))) {
      LOG_WARN("failed to check current archive dest", K(ret));
    } else if (OB_FAIL(backup_dest.get_backup_path_str(backup_dest_str.ptr(), backup_dest_str.capacity()))) {
      LOG_WARN("failed to get backup dest str", K(ret));
    } else if (OB_FAIL(connectivity_checker_->check_dest_connectivity(
                                   backup_dest_str, ObBackupDestType::TYPE::DEST_TYPE_ARCHIVE_LOG))) {
      LOG_WARN("failed to check connectivity", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unsupported dest_type for delete all", K(ret), K(dest_type), K(job_attr_->dest_id_));
  }

  // get all sets/pieces in the dest, and filter out deleted ones
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_all_pieces_or_sets_in_dest_(set_list, piece_list))) {
    LOG_WARN("failed to get delete backup all infos from inner table", K(ret));
  }

  if (OB_FAIL(ret) && failure_info.failure_reason.length() > 0) {
    int tmp_ret = add_failure_reason_(failure_info);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to add failure reason", K(tmp_ret), K(failure_info));
    }
  }
  return ret;
}

int ObBackupDeleteSelector::check_current_backup_dest_(BackupCleanFailureInfo &failure_info)
{
  int ret = OB_SUCCESS;
  ObBackupPathString current_backup_dest_str;
  int64_t current_dest_id = INVALID_CLEAN_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (OB_FAIL(data_provider_->get_backup_dest(job_attr_->tenant_id_, current_backup_dest_str))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get backup dest", K(ret));
    }
  } else if (!current_backup_dest_str.is_empty()) {
    ObBackupDest current_backup_dest;
    if (OB_FAIL(current_backup_dest.set(current_backup_dest_str))) {
      LOG_WARN("fail to set backup dest", K(ret), K(current_backup_dest_str));
    } else if (OB_FAIL(data_provider_->get_dest_id(
        job_attr_->tenant_id_, current_backup_dest, current_dest_id))) {
      LOG_WARN("fail to get dest_id for current backup dest", K(ret), K(current_backup_dest_str));
    } else if (current_dest_id == job_attr_->dest_id_) {
      ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
      LOG_WARN("current writing dest does not support deletion",
                K(ret), "current_dest_id", current_dest_id, "dest_id", job_attr_->dest_id_);
      failure_info.failure_id = job_attr_->dest_id_;
      failure_info.failure_reason = "cannot delete current writing backup dest";
    }
  }
  return ret;
}

int ObBackupDeleteSelector::check_current_archive_dest_(BackupCleanFailureInfo &failure_info)
{
  int ret = OB_SUCCESS;
  common::ObArray<std::pair<int64_t, int64_t>> dest_array; // <dest_id, dest_type>
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (OB_FAIL(archive_helper_->get_valid_dest_pairs(*sql_proxy_, dest_array))) {
    LOG_WARN("failed to get valid dest pairs", K(ret));
  } else if (0 == dest_array.count()) {
    ret = OB_SUCCESS; // no valid archive dest, can delete
  } else if (1 != dest_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, dest_array count is not exactly 1", K(ret), K(dest_array));
  } else {
    std::pair<int64_t, int64_t> archive_dest = dest_array.at(0);
    if (archive_dest.second == job_attr_->dest_id_) {
      ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
      LOG_WARN("current writing dest does not support deletion",
                K(ret), "archive_dest_id", archive_dest.second, "dest_id", job_attr_->dest_id_);
      failure_info.failure_id = job_attr_->dest_id_;
      failure_info.failure_reason = "cannot delete current writing archive dest";
    }
  }
  return ret;
}

int ObBackupDeleteSelector::get_all_pieces_or_sets_in_dest_(
    ObIArray<ObBackupSetFileDesc> &set_list,
    ObIArray<ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (OB_ISNULL(job_attr_) || OB_ISNULL(data_provider_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job_attr_ or data_provider_ or sql_proxy_ is null",
              K(ret), KP(job_attr_), KP(data_provider_), KP(sql_proxy_));
  } else {
    const uint64_t tenant_id = job_attr_->tenant_id_;
    const int64_t dest_id = job_attr_->dest_id_;
    if (ObBackupDestType::TYPE::DEST_TYPE_BACKUP_DATA == job_attr_->backup_path_type_) {
      ObArray<ObBackupSetFileDesc> sets_in_same_dest;
      if (OB_FAIL(data_provider_->get_backup_set_files_specified_dest(tenant_id, dest_id, sets_in_same_dest))) {
        LOG_WARN("failed to get all backup set files in specified dest", K(ret), K(tenant_id), K(dest_id));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < sets_in_same_dest.count(); ++i) {
          const ObBackupSetFileDesc &set_desc = sets_in_same_dest.at(i);
          if (ObBackupSetFileDesc::BackupSetStatus::DOING == set_desc.status_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, backup set is doing", K(ret), K(set_desc));
            break;
          } else if (ObBackupFileStatus::BACKUP_FILE_DELETED != set_desc.file_status_) {
            if (OB_FAIL(set_list.push_back(set_desc))) {
              LOG_WARN("failed to push back backup set desc", K(ret), K(set_desc));
            }
          }
        }
      }
    } else if (ObBackupDestType::TYPE::DEST_TYPE_ARCHIVE_LOG == job_attr_->backup_path_type_) {
      ObArray<ObTenantArchivePieceAttr> pieces_in_same_dest;
      if (OB_FAIL(archive_helper_->get_pieces(*sql_proxy_, dest_id, pieces_in_same_dest))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;  // do nothing, empty dest is allowed
        } else {
          LOG_WARN("failed to get all archive pieces in dest", K(ret), K(tenant_id), K(dest_id));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < pieces_in_same_dest.count(); ++i) {
          const ObTenantArchivePieceAttr &piece_attr = pieces_in_same_dest.at(i);
          if (piece_attr.status_.is_active()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, piece is active", K(ret), K(piece_attr));
            break;
          } else if (ObBackupFileStatus::BACKUP_FILE_DELETED != piece_attr.file_status_) {
            if (OB_FAIL(piece_list.push_back(piece_attr))) {
              LOG_WARN("failed to push back archive piece attr", K(ret), K(piece_attr));
            }
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unsupported dest_type for delete all", K(ret), K(dest_id));
    }
  }
  return ret;
}

int ObBackupDeleteSelector::get_delete_obsolete_infos(ObIArray<ObBackupSetFileDesc> &set_list,
                         ObIArray<ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS;
  ObBackupSetFileDesc clog_data_clean_point;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (ObBackupDestType::DEST_TYPE_BACKUP_DATA == job_attr_->backup_path_type_) { // auto delete policy: default
    if (OB_FAIL(get_delete_obsolete_backup_set_infos_(clog_data_clean_point, set_list))) {
      LOG_WARN("failed to get delete obsolete backup set infos", K(ret));
    } else if (OB_FAIL(get_delete_obsolete_backup_piece_infos_(clog_data_clean_point, piece_list))) {
      LOG_WARN("failed to get delete obsolete backup piece infos", K(ret));
    }
  } else if (ObBackupDestType::DEST_TYPE_ARCHIVE_LOG == job_attr_->backup_path_type_) {// auto delete policy: log_only
    if (OB_FAIL(get_delete_obsolete_backup_piece_infos_log_only_(job_attr_->expired_time_, piece_list))) {
      LOG_WARN("failed to get delete obsolete backup piece infos", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unsupported backup path type", K(ret), K(job_attr_->backup_path_type_));
  }
  return ret;
}

#ifdef ERRSIM
int ObBackupDeleteSelector::get_errsim_expired_parameter_(int64_t &expired_param_from_errsim)
{
  int ret = OB_SUCCESS;
  int64_t errsim_expired_param = GCONF.errsim_backup_clean_override_expired_time;
  if (errsim_expired_param > 0) {
    expired_param_from_errsim = errsim_expired_param;
    LOG_INFO("errsim override expired_param", K(expired_param_from_errsim), K(errsim_expired_param));
  }
  return ret;
}
#endif

int ObBackupDeleteSelector::get_obsolete_backup_set_infos_helper_(ObBackupSetFileDesc &first_full_backup_set,
      const char *backup_path_str, ObIArray<ObBackupSetFileDesc> &set_list)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupSetFileDesc> backup_set_infos;
  CompareBackupSetInfo backup_set_info_cmp;
  ObArray<ObBackupSetFileDesc> final_deletable_set_list;
  common::ObArray<std::pair<int64_t, int64_t>> dest_array;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
#ifdef ERRSIM
  } else if (OB_FAIL(get_errsim_expired_parameter_(job_attr_->expired_time_))) {
    LOG_WARN("errsim failed to override expired time", K(ret));
#endif
  // get the archive dest, for check piece continuity with backup set below
  } else if (OB_FAIL(archive_helper_->get_valid_dest_pairs(*sql_proxy_, dest_array))) {
    LOG_WARN("failed to get valid dest pairs", K(ret));
  } else if (0 == dest_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, no valid dest found", K(ret));
  } else if (1 != dest_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, more than one valid dest found", K(ret), K(dest_array));
  } else if (OB_FAIL(data_provider_->get_candidate_obsolete_backup_sets(job_attr_->tenant_id_,
                                  job_attr_->expired_time_, backup_path_str, backup_set_infos))) {
    LOG_WARN("failed to get candidate obsolete backup sets", K(ret));
  } else if (FALSE_IT(lib::ob_sort(backup_set_infos.begin(), backup_set_infos.end(), backup_set_info_cmp))) {
  } else {
    for (int64_t i = backup_set_infos.count() - 1 ; OB_SUCC(ret) && i >= 0; i--) {
      bool need_deleted = false;
      bool can_used_to_restore = false;
      const ObBackupSetFileDesc &backup_set_info = backup_set_infos.at(i);
      if (!backup_set_info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup set info is invalid", K(ret), K(backup_set_info));
      } else if (ObBackupSetFileDesc::BackupSetStatus::DOING == backup_set_info.status_) {
        need_deleted = false;
      } else if (ObBackupSetFileDesc::BackupSetStatus::FAILED == backup_set_info.status_
          || ObBackupFileStatus::BACKUP_FILE_DELETING == backup_set_info.file_status_) {
        need_deleted = true;
        LOG_INFO("[BACKUP_CLEAN] allow delete", K(backup_set_info));
      } else if (backup_set_info.backup_type_.is_full_backup() && !first_full_backup_set.is_valid()) {
        if (backup_set_info.plus_archivelog_) {
          can_used_to_restore = true;
          LOG_INFO("get latest valid full backup set with plus archivelog", K(ret), K(backup_set_info));
        } else if (OB_FAIL(archive_helper_->check_piece_continuity_between_two_scn(*sql_proxy_, dest_array.at(0).second,
                              backup_set_info.start_replay_scn_, backup_set_info.min_restore_scn_, can_used_to_restore))) {
          LOG_WARN("failed to check piece continuity between two scn", K(ret), K(backup_set_info));
        }
        if (OB_FAIL(ret)) {
        } else if (!can_used_to_restore) {
          LOG_INFO("backup set can not be used to restore", K(backup_set_info));
        } else if (OB_FAIL(first_full_backup_set.assign(backup_set_info))) {
          LOG_WARN("failed to assign first full backup set", K(ret));
        } else {
          // Here we get a full backup set, which is the latest one that has expired and can be used for restore.
          // We will use its start_replay_scn as the limit to delete sets and pieces.
          need_deleted = false;
        }
      } else if (!backup_set_info.backup_type_.is_full_backup()
          && backup_set_info.backup_set_id_ > first_full_backup_set.backup_set_id_) {
        need_deleted = false;
      } else {
        need_deleted = true;
      }

      if (OB_FAIL(ret)) {
      } else if (need_deleted && OB_FAIL(final_deletable_set_list.push_back(backup_set_info))) {
        LOG_WARN("failed to push back set list", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      lib::ob_sort(final_deletable_set_list.begin(), final_deletable_set_list.end(), backup_set_info_cmp);
      if (OB_FAIL(set_list.assign(final_deletable_set_list))) {
        LOG_WARN("failed to assign set list", K(ret));
      }
    }
    LOG_INFO("[BACKUP_CLEAN]finish get delete obsolete backup set infos ", K(ret), K(first_full_backup_set), K(set_list));
  }
  return ret;
}

int ObBackupDeleteSelector::get_delete_obsolete_backup_set_infos_(ObBackupSetFileDesc &clog_data_clean_point,
                                ObIArray<ObBackupSetFileDesc> &set_list)
{
  int ret = OB_SUCCESS;
  ObBackupPathString backup_dest_str;
  ObBackupPathString backup_path_str;
  ObBackupHelper backup_helper;
  ObBackupDest backup_dest;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (OB_FAIL(data_provider_->get_backup_dest(job_attr_->tenant_id_, backup_dest_str))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get backup dest", K(ret));
    }
  } else if (backup_dest_str.is_empty()) {
    // do nothing
  } else if (OB_FAIL(backup_dest.set(backup_dest_str))) {
    LOG_WARN("fail to set backup dest", K(ret), K(backup_dest_str));
  } else if (OB_FAIL(backup_dest.get_backup_path_str(backup_path_str.ptr(), backup_path_str.capacity()))) {
    LOG_WARN("fail to get backup path str", K(ret), K(backup_dest));
  } else if (OB_FAIL(get_obsolete_backup_set_infos_helper_(clog_data_clean_point,
                        backup_path_str.ptr(), set_list))) {
    LOG_WARN("failed to get delete obsolete backup set infos", K(ret));
  }
  return ret;
}

int ObBackupDeleteSelector::get_all_dest_backup_piece_infos_(
    const SCN &clog_data_clean_point, ObIArray<ObTenantArchivePieceAttr> &backup_piece_infos, const bool is_log_only)
{
  int ret = OB_SUCCESS;
  ObArchivePersistHelper archive_table_op;
  common::ObArray<std::pair<int64_t, int64_t>> dest_array;
  if (!clog_data_clean_point.is_valid()) {
    LOG_INFO("[BACKUP_CLEAN]point is invalid", K(clog_data_clean_point));
  } else if (OB_FAIL(archive_table_op.init(job_attr_->tenant_id_))) {
    LOG_WARN("failed to init archive helper", K(ret));
  } else if (OB_FAIL(archive_table_op.get_valid_dest_pairs(*sql_proxy_, dest_array))) {
    LOG_WARN("failed to init archive helper", K(ret));
  } else if (0 == dest_array.count()) {
    // do nothing
  } else {
    ObBackupDest backup_dest;
    ObBackupPathString backup_dest_str;
    ObBackupPathString backup_path_str;
    bool need_lock = false;
    std::pair<int64_t, int64_t> archive_dest;
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_array.count(); i++) {
      archive_dest = dest_array.at(i);
      backup_dest.reset();
      backup_dest_str.reset();
      backup_path_str.reset();
      if (OB_FAIL(archive_table_op.get_archive_dest(*sql_proxy_, need_lock, archive_dest.first, backup_dest_str))) {
        LOG_WARN("failed to get archive path", K(ret));
      } else if (OB_FAIL(backup_dest.set(backup_dest_str))) {
        LOG_WARN("fail to set backup dest", K(ret), K(backup_dest_str));
      } else if (OB_FAIL(backup_dest.get_backup_path_str(backup_path_str.ptr(), backup_path_str.capacity()))) {
        LOG_WARN("fail to get backup path str", K(ret), K(backup_dest));
      } else if (OB_FAIL(archive_table_op.get_candidate_obsolete_backup_pieces(
                *sql_proxy_, clog_data_clean_point, backup_path_str.ptr(), backup_piece_infos))) {
        LOG_WARN("failed to get candidate obsolete backup sets", K(ret));
      } else if (is_log_only && backup_piece_infos.count() > 0) {
        // get the about to expire piece,
        ObTenantArchivePieceAttr about_to_expire_piece;
        int64_t about_to_expire_piece_id = backup_piece_infos.at(backup_piece_infos.count() - 1).key_.piece_id_ + 1;
        if (OB_FAIL(archive_table_op.get_piece(*sql_proxy_, about_to_expire_piece_id,
          true, about_to_expire_piece))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            LOG_INFO("about to expire piece not exist, skip", K(about_to_expire_piece_id));
          } else {
            LOG_WARN("failed to get about to expire piece", K(ret), K(about_to_expire_piece_id));
          }
        } else if (OB_FAIL(backup_piece_infos.push_back(about_to_expire_piece))) {
          LOG_WARN("failed to push back about to expire piece", K(ret));
        }
      }
    }
  }
  LOG_INFO("[BACKUP_CLEAN] finish get all dest backup piece infos", K(ret), K(backup_piece_infos));
  return ret;
}

int ObBackupDeleteSelector::get_delete_obsolete_backup_piece_infos_(const ObBackupSetFileDesc &clog_data_clean_point,
                                            ObIArray<ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS;
  CompareBackupPieceInfo backup_piece_info_cmp;
  ObArray<ObTenantArchivePieceAttr> backup_piece_infos;
  if (OB_FAIL(get_all_dest_backup_piece_infos_(
      clog_data_clean_point.start_replay_scn_, backup_piece_infos, false))) {
    LOG_WARN("failed to get all dest backup piece infos", K(ret), K(clog_data_clean_point));
  } else if (FALSE_IT(lib::ob_sort(backup_piece_infos.begin(), backup_piece_infos.end(), backup_piece_info_cmp))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_piece_infos.count(); i++) {
      const ObTenantArchivePieceAttr &backup_piece_info = backup_piece_infos.at(i);
      if (!backup_piece_info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup piece info is invalid", K(ret), K(backup_piece_info));
      } else if (!can_backup_pieces_be_deleted_(backup_piece_info.status_)) {
        ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
        LOG_WARN("piece can not be deleted", K(ret), K(backup_piece_info));
      } else if (OB_FAIL(piece_list.push_back(backup_piece_info))) {
        LOG_WARN("failed to push back piece list", K(ret), K(backup_piece_info));
      }
    }
  }
  LOG_INFO("[BACKUP_CLEAN] finish get delete obsolete backup piece infos", K(ret), K(piece_list));
  return ret;
}

bool ObBackupDeleteSelector::can_backup_pieces_be_deleted_(const ObArchivePieceStatus &status)
{
  return ObArchivePieceStatus::Status::INACTIVE == status.status_
      || ObArchivePieceStatus::Status::FROZEN == status.status_;
}

// This function checks if older pieces are depended by the first not expired piece and get the min depended piece idx
int ObBackupDeleteSelector::get_min_depended_piece_idx_(
  const ObIArray<ObTenantArchivePieceAttr> &candidate_piece_infos,
  int64_t &min_depended_pieces_idx)
{
  int ret = OB_SUCCESS;
  if (candidate_piece_infos.count() == 0) {
    LOG_INFO("No candidate pieces to check dependency, skip");
  } else {
    const int64_t total_piece_count = candidate_piece_infos.count();
    min_depended_pieces_idx = candidate_piece_infos.count() - 1;
    ObPieceInfoDesc last_piece_info_desc;
    // load the first not expired piece info(which is the last piece in candidate_piece_infos)
    if (OB_FAIL(data_provider_->load_piece_info_desc(job_attr_->tenant_id_,
                          candidate_piece_infos.at(min_depended_pieces_idx), last_piece_info_desc))) {
      LOG_WARN("Failed to get last piece info desc", K(ret));
    } else {
      // traverse the first not expired piece's all ls
      for (int64_t ls_idx = 0; OB_SUCC(ret) && ls_idx < last_piece_info_desc.filelist_.count(); ++ls_idx) {
        ObSingleLSInfoDesc &ls_info = last_piece_info_desc.filelist_.at(ls_idx);
        int64_t file_id = ls_info.filelist_.at(0).file_id_;
        // for each ls, find the min depended piece idx
        if (OB_FAIL(get_min_depended_piece_idx_ls_(candidate_piece_infos, ls_info, file_id, min_depended_pieces_idx))) {
          LOG_WARN("Failed to get min depended piece idx for ls", K(ret), K(ls_idx));
        } else if (min_depended_pieces_idx == 0) {
          break;
        }
      }
    }
  }
  LOG_INFO("Finished checking piece dependency", K(ret), K(min_depended_pieces_idx), K(candidate_piece_infos.count()));
  return ret;
}

int ObBackupDeleteSelector::get_min_depended_piece_idx_ls_(
  const ObIArray<ObTenantArchivePieceAttr> &candidate_piece_infos,
  const ObSingleLSInfoDesc &ls_info,
  const int64_t file_id,
  int64_t &min_depended_pieces_idx)
{
  int ret = OB_SUCCESS;
  ObPieceInfoDesc temp_piece_info_desc;
  // Look back until get different file_id ( a new file_id means a new block in clog)
  int64_t temp_piece_idx = min_depended_pieces_idx - 1;
  if (temp_piece_idx >= candidate_piece_infos.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid piece index", K(ret), K(temp_piece_idx));
  }
  while (OB_SUCC(ret) && temp_piece_idx >= 0) {
    temp_piece_info_desc.reset();
    if (OB_FAIL(data_provider_->load_piece_info_desc(job_attr_->tenant_id_,
                               candidate_piece_infos.at(temp_piece_idx), temp_piece_info_desc))) {
      LOG_WARN("Failed to get temp piece info desc", K(ret), K(temp_piece_idx));
    } else {
      bool has_same_ls_id = false;
      bool find_new_file_id = false;
      // traverse to get the target ls_id
      for (int64_t ls_idx_of_temp_piece = 0;
          OB_SUCC(ret) && !has_same_ls_id && ls_idx_of_temp_piece < temp_piece_info_desc.filelist_.count();
          ++ls_idx_of_temp_piece) {
        ObSingleLSInfoDesc &temp_ls_info = temp_piece_info_desc.filelist_.at(ls_idx_of_temp_piece);
        if (temp_ls_info.ls_id_ == ls_info.ls_id_) {
          // check the dependency of the file in this ls
          has_same_ls_id = true;
          if (temp_ls_info.filelist_.at(temp_ls_info.filelist_.count() - 1).file_id_ == file_id) {
            LOG_INFO("find dependency by 64M block question", "piece_id", temp_piece_info_desc.piece_id_);
            min_depended_pieces_idx = MIN(min_depended_pieces_idx, temp_piece_idx);
            if (temp_ls_info.filelist_.at(0).file_id_ == file_id) { // need to look back continue
              temp_piece_idx--;
            } else {
              find_new_file_id = true;
            }
          } else {
            find_new_file_id = true;
          }
          break;
        }
      }
      if (!has_same_ls_id || find_new_file_id) {
        // the new generated ls OR the same ls_id but mutiple files which break the dependency
        break;
      }
    }
  }
  return ret;
}



// This function checks whether expired pieces in log_only mode are still depended on by non-expired
// pieces (due to the issue that clog replay relies on the starting point of a 64M block).
int ObBackupDeleteSelector::check_piece_dependency_(
  ObIArray<ObTenantArchivePieceAttr> &candidate_piece_infos)
{
  int ret = OB_SUCCESS;
  if (0 == candidate_piece_infos.count()) {
    LOG_INFO("No candidate pieces to check dependency, skip");
  } else {
    LOG_INFO("Processing candidate pieces", K(candidate_piece_infos.count()));

    // find the min index of piece that is depended on by non-expired pieces
    // get_min_depended_piece_idx_ will load piece info on demand as needed
    int64_t min_depended_pieces_idx = 0;
    if (OB_FAIL(get_min_depended_piece_idx_(candidate_piece_infos, min_depended_pieces_idx))) {
      LOG_WARN("failed to get min depended pieces idx", K(ret));
    } else {
      // only delete the piece before the min_depended_pieces_idx
      for (int64_t i = candidate_piece_infos.count() - 1; OB_SUCC(ret) && i >= min_depended_pieces_idx && i >= 0; --i) {
        if (OB_FAIL(candidate_piece_infos.remove(i))) {
          LOG_WARN("failed to remove piece", K(ret));
        }
      }
    }
  }
  LOG_INFO("Finished checking piece dependency", K(ret), K(candidate_piece_infos));
  return ret;
}

int ObBackupDeleteSelector::get_delete_obsolete_backup_piece_infos_log_only_(int64_t expired_time,
                                            ObIArray<ObTenantArchivePieceAttr> &piece_list) {
  int ret = OB_SUCCESS;
  CompareBackupPieceInfo backup_piece_info_cmp;
  ObArray<ObTenantArchivePieceAttr> backup_piece_infos;
  SCN clog_data_clean_point;
  // check current writing dest backup set dest is not valid
  ObBackupPathString backup_dest_str;
  ObBackupHelper backup_helper;
  int64_t expired_scn_from_errsim = 0;
  bool is_backup_dest_valid = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (OB_FAIL(backup_helper.init(job_attr_->tenant_id_, *sql_proxy_))) {
    LOG_WARN("fail to init backup help", K(ret));
  } else if (OB_FAIL(backup::ObBackupUtils::check_tenant_backup_dest_exists(
                        job_attr_->tenant_id_, is_backup_dest_valid, *sql_proxy_))) {
    LOG_WARN("fail to check backup dest valid", K(ret), K(job_attr_->tenant_id_));
  } else if (is_backup_dest_valid) {
    ret = OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED;
    LOG_WARN("current writing dest backup set dest is valid, it's unexpected, can not use auto delete log_only policy",
              K(ret), K(backup_dest_str));
  } else if (OB_FAIL(clog_data_clean_point.convert_from_ts(expired_time))) {
    LOG_WARN("failed to convert from ts", K(ret), K(expired_time));
#ifdef ERRSIM
  } else if (OB_FAIL(get_errsim_expired_parameter_(expired_scn_from_errsim))) {
    LOG_WARN("errsim failed to override expired time for errsim", K(ret));
  } else if (OB_FAIL(clog_data_clean_point.convert_from_ts(
                 static_cast<uint64_t>(expired_scn_from_errsim / 1000)))) {
    LOG_WARN("errsim failed to convert from ts", K(ret), K(expired_scn_from_errsim));
#endif
  } else if (OB_FAIL(get_all_dest_backup_piece_infos_(clog_data_clean_point, backup_piece_infos, true))) {
    LOG_WARN("failed to get all dest backup piece infos", K(ret), K(clog_data_clean_point));
  } else if (FALSE_IT(lib::ob_sort(backup_piece_infos.begin(), backup_piece_infos.end(), backup_piece_info_cmp))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_piece_infos.count(); i++) {
      const ObTenantArchivePieceAttr &backup_piece_info = backup_piece_infos.at(i);
      if (!backup_piece_info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup piece info is invalid", K(ret), K(backup_piece_info));
      } else if (can_backup_pieces_be_deleted_(backup_piece_info.status_)) {
        if (OB_FAIL(piece_list.push_back(backup_piece_info))) {
          LOG_WARN("failed to push back piece list", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_piece_dependency_(piece_list))) {
      LOG_WARN("failed to filter backup piece infos due to clog replay require full block", K(ret));
    }
  }
  return ret;
}

} //namespace rootserver
} //namespace oceanbase