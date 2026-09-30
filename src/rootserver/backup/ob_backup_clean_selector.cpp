/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
#include "storage/backup/ob_backup_data_store.h"
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
    } else if (OB_FAIL(dest_mgr.check_dest_validity(*rpc_proxy_, true/*need_format_file*/, false/*need_check_permission*/))) {
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
    OB_DELETE(IObBackupDataProvider, "BackupClean", data_provider_);
    data_provider_ = nullptr;
  }
  if (OB_NOT_NULL(connectivity_checker_)) {
    OB_DELETE(IObConnectivityChecker, "BackupClean", connectivity_checker_);
    connectivity_checker_ = nullptr;
  }
  if (OB_NOT_NULL(archive_helper_)) {
    OB_DELETE(ObArchivePersistHelper, "BackupClean", archive_helper_);
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
    ObBackupDataProvider *data_provider_impl = OB_NEW(ObBackupDataProvider, "BackupClean");
    ObConnectivityChecker *connectivity_checker_impl = OB_NEW(ObConnectivityChecker, "BackupClean");
    ObArchivePersistHelper *archive_helper_impl = OB_NEW(ObArchivePersistHelper, "BackupClean");

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
      OB_DELETE(ObBackupDataProvider, "BackupClean", data_provider_impl);
    }
    if (OB_NOT_NULL(connectivity_checker_impl)) {
      OB_DELETE(ObConnectivityChecker, "BackupClean", connectivity_checker_impl);
    }
    if (OB_NOT_NULL(archive_helper_impl)) {
      OB_DELETE(ObArchivePersistHelper, "BackupClean", archive_helper_impl);
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
        // The retention policy only exists to protect usable restore baselines on the
        // current active path. Only SUCCESS backup sets can ever act as a restore baseline;
        // FAILED backup sets (including those produced by a canceled backup, which are
        // persisted as FAILED in __all_backup_set_files) are never restorable and therefore
        // are always safe to delete regardless of whether a valid full baseline exists.
        //
        // So we only apply the baseline protection against the newest SUCCESS candidate.
        // If every candidate on the current path is FAILED, there is nothing to protect and
        // the deletion is allowed to proceed.
        const ObBackupSetFileDesc *newest_success_candidate = nullptr;
        for (int64_t i = candidate_descs_in_group.count() - 1; OB_ISNULL(newest_success_candidate) && i >= 0; --i) {
          const ObBackupSetFileDesc &desc = candidate_descs_in_group.at(i);
          if (ObBackupSetFileDesc::BackupSetStatus::SUCCESS == desc.status_) {
            // candidates are sorted ascending by backup_set_id, so the first SUCCESS
            // one found while iterating backwards is the newest SUCCESS candidate.
            newest_success_candidate = &desc;
          }
        }
        if (OB_ISNULL(newest_success_candidate)) {
          LOG_INFO("all candidate backup sets on current path are not SUCCESS, "
                   "no restore baseline to protect, allow delete",
                   K(candidate_path), K(candidate_descs_in_group));
        } else if (OB_FAIL(data_provider_->get_latest_valid_full_backup_set(
                      job_attr_->tenant_id_, *archive_helper_, current_backup_path_str, clean_point))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            // There are SUCCESS candidate(s) but no valid full baseline can be located.
            // Deleting a SUCCESS set here could remove the only restore baseline, so keep
            // rejecting to stay safe.
            ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
            LOG_WARN("No full backup exists in this dest. No sets will be deleted", K(ret), K(candidate_path));
            failure_info.failure_id = INVALID_CLEAN_ID;
            failure_info.failure_reason = "no full backup exists";
          } else {
            LOG_WARN("failed to get latest full backup set", K(ret));
          }
        } else {
          if (newest_success_candidate->backup_set_id_ >= clean_point.backup_set_id_) {
            ret = OB_BACKUP_DELETE_BACKUP_SET_NOT_ALLOWED;
            LOG_WARN("Candidate backup set is newer than or equal to the latest full backup on the current path",
              K(ret), "candidate_set_id", newest_success_candidate->backup_set_id_,
              "latest_full_set_id", clean_point.backup_set_id_, K(candidate_path));
            failure_info.failure_id = newest_success_candidate->backup_set_id_;
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

int ObBackupDataProvider::get_backup_set_ls_restore_start_lsn(
    const uint64_t tenant_id,
    const ObBackupSetFileDesc &backup_set_desc,
    ObIArray<ObLSRestoreStartLSN> &ls_start_lsn_array)
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  ObBackupSetDesc set_desc;
  storage::ObBackupDataStore store;
  storage::ObBackupLSMetaInfosDesc ls_meta_infos;
  ls_start_lsn_array.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataProvider not inited", K(ret));
  } else if (!backup_set_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid backup set desc", K(ret), K(backup_set_desc));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy_, tenant_id,
      backup_set_desc.backup_path_, backup_dest))) {
    LOG_WARN("failed to get backup dest with storage info", K(ret), K(tenant_id), K(backup_set_desc));
  } else if (OB_FALSE_IT(set_desc.backup_set_id_ = backup_set_desc.backup_set_id_)) {
  } else if (OB_FALSE_IT(set_desc.backup_type_ = backup_set_desc.backup_type_)) {
  } else if (OB_FAIL(store.init(backup_dest, set_desc))) {
    LOG_WARN("failed to init backup data store", K(ret), K(backup_set_desc));
  } else if (OB_FAIL(store.read_ls_meta_infos(ls_meta_infos))) {
    LOG_WARN("failed to read ls meta infos", K(ret), K(backup_set_desc));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_meta_infos.ls_meta_packages_.count(); ++i) {
      const storage::ObLSMetaPackage &ls_meta_package = ls_meta_infos.ls_meta_packages_.at(i);
      ObLSRestoreStartLSN ls_start_lsn;
      ls_start_lsn.ls_id_ = ls_meta_package.ls_meta_.ls_id_;
      ls_start_lsn.start_lsn_ = ls_meta_package.palf_meta_.curr_lsn_;
      if (!ls_start_lsn.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ls restore start lsn", K(ret), K(ls_start_lsn), K(ls_meta_package));
      } else if (OB_FAIL(ls_start_lsn_array.push_back(ls_start_lsn))) {
        LOG_WARN("failed to push back ls restore start lsn", K(ret), K(ls_start_lsn));
      }
    }
    LOG_INFO("[BACKUP_CLEAN]get backup set ls restore start lsn", K(ret), K(backup_set_desc),
        K(ls_start_lsn_array));
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
                if (piece.end_scn_ > clog_data_clean_point.start_replay_scn_) {
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
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy_,
                                job_attr_->tenant_id_, job_attr_->backup_path_, backup_dest))) {
    LOG_WARN("failed to get backup dest", K(ret), "path", job_attr_->backup_path_);
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(
                        *sql_proxy_, job_attr_->tenant_id_, backup_dest, job_attr_->dest_id_))) {
    LOG_WARN("failed to get dest_id", K(ret), "tenant_id", job_attr_->tenant_id_,
             "dest_id", job_attr_->dest_id_, K(backup_dest));
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
      } else if (!is_log_only) {
        // delete obsolete: only pick the pieces whose log is really not needed by the retained
        // backup set chain any more, an un-deletable piece is skipped instead of failing the job.
        if (OB_FAIL(get_one_dest_deletable_backup_piece_infos_(clog_data_clean_point, backup_path_str.ptr(),
            archive_dest.second/*dest_id*/, archive_table_op, backup_piece_infos))) {
          LOG_WARN("failed to get deletable backup piece infos of one dest", K(ret),
              K(clog_data_clean_point), K(backup_path_str));
        }
      } else if (OB_FAIL(archive_table_op.get_candidate_obsolete_backup_pieces(
                *sql_proxy_, clog_data_clean_point, backup_path_str.ptr(), backup_piece_infos))) {
        LOG_WARN("failed to get candidate obsolete backup sets", K(ret));
      } else if (backup_piece_infos.count() > 0) {
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

// Pick the deletable pieces of one archive dest. Pieces are checked in ascending order of piece id,
// and once a piece can not be deleted, all the pieces after it are kept too, so that the remaining
// pieces are always a continuous suffix which covers the log from start_replay_scn onwards.
// Note that an un-deletable piece only means "nothing more can be reclaimed for this dest in this
// round", it MUST NOT fail the whole backup clean job, otherwise the obsolete backup sets which have
// already been figured out can not be deleted either, and the job would keep failing forever.
//
// The candidate pieces are selected by PATH while the pieces used to judge whether start_replay_scn
// is still covered are selected by DEST_ID(see check_scn_covered_by_other_piece_), so the two must be
// verified to belong to the same dest before they are compared: the caller reads the path and the
// dest_id of one dest_no with two separate unlocked reads of __all_log_archive_dest_parameter, and an
// "alter system set log_archive_dest_n" in between(allowed while archive is stopped) can repoint the
// dest_no from dest A to dest B, leaving the caller with the path of B and the dest_id of A. A
// candidate piece of another dest is therefore treated as un-deletable, which keeps it and all the
// pieces after it; the next round of clean reads a consistent path/dest_id pair and moves on.
int ObBackupDeleteSelector::get_one_dest_deletable_backup_piece_infos_(
    const SCN &start_replay_scn,
    const char *backup_path_str,
    const int64_t dest_id,
    const ObArchivePersistHelper &archive_table_op,
    ObIArray<ObTenantArchivePieceAttr> &backup_piece_infos)
{
  int ret = OB_SUCCESS;
  CompareBackupPieceInfo backup_piece_info_cmp;
  ObArray<ObTenantArchivePieceAttr> candidate_piece_infos;
  ObArray<ObTenantArchivePieceAttr> all_piece_infos;
  if (OB_ISNULL(backup_path_str) || dest_id <= 0 || !start_replay_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(backup_path_str), K(dest_id), K(start_replay_scn));
  } else if (OB_FAIL(archive_table_op.get_candidate_obsolete_backup_pieces(*sql_proxy_, start_replay_scn,
      backup_path_str, candidate_piece_infos, true/*use_checkpoint_scn*/))) {
    LOG_WARN("failed to get candidate obsolete backup pieces", K(ret), K(start_replay_scn), K(dest_id));
  } else if (candidate_piece_infos.empty()) {
    // do nothing
  } else if (OB_FAIL(archive_table_op.get_pieces(*sql_proxy_, dest_id, all_piece_infos))) {
    // Get all the pieces of the dest once here and pass it down to check_piece_can_be_deleted_, so
    // that check_scn_covered_by_other_piece_ does not query and sort the pieces again for every
    // candidate piece.
    LOG_WARN("failed to get pieces of dest", K(ret), K(dest_id));
  } else if (FALSE_IT(lib::ob_sort(candidate_piece_infos.begin(), candidate_piece_infos.end(), backup_piece_info_cmp))) {
  } else if (FALSE_IT(lib::ob_sort(all_piece_infos.begin(), all_piece_infos.end(), backup_piece_info_cmp))) {
  } else {
    bool can_be_deleted = true;
    for (int64_t i = 0; OB_SUCC(ret) && can_be_deleted && i < candidate_piece_infos.count(); i++) {
      const ObTenantArchivePieceAttr &backup_piece_info = candidate_piece_infos.at(i);
      if (OB_UNLIKELY(backup_piece_info.key_.dest_id_ != dest_id)) {
        // The piece is not archived at `dest_id`, so `all_piece_infos` says nothing about it, see the
        // comment above. Keep it and the pieces after it.
        can_be_deleted = false;
        LOG_WARN("[BACKUP_CLEAN]dest id of the candidate piece does not match the dest id of the path,"
            " the archive dest may have just been changed, keep the piece", K(dest_id),
            K(backup_piece_info));
      } else if (OB_FAIL(check_piece_can_be_deleted_(backup_piece_info, start_replay_scn, all_piece_infos,
          can_be_deleted))) {
        LOG_WARN("failed to check piece can be deleted", K(ret), K(backup_piece_info));
      } else if (!can_be_deleted) {
        LOG_INFO("[BACKUP_CLEAN]backup piece can not be deleted, skip it and the pieces after it",
            K(backup_piece_info), K(start_replay_scn));
      } else if (OB_FAIL(backup_piece_infos.push_back(backup_piece_info))) {
        LOG_WARN("failed to push back piece", K(ret), K(backup_piece_info));
      }
    }
  }
  return ret;
}

// A piece can be deleted only if none of the log it really contains is needed by the retained backup
// set chain, i.e. all the log in the piece is before start_replay_scn.
//
// Pay attention that piece.end_scn_ is only a nominal boundary calculated by
// "round.start_scn + N * piece_switch_interval"(see ObTenantArchiveMgr::decide_piece_end_scn), it is
// determined when the piece is created and never shrinks, even if archive is stopped in the middle
// of the piece. So for a FROZEN piece whose archive was stopped(e.g. the user runs
// "alter system noarchivelog" after every data backup), end_scn_ may be far greater than the scn of
// the last log it really contains. Judging by end_scn_ would treat such piece as "still needed" by
// mistake. Here we use checkpoint_scn_/max_scn_(the real upper bound of the log in a frozen piece)
// instead, and additionally require that start_replay_scn is still nominally covered by another kept
// AVAILABLE piece(see check_scn_covered_by_other_piece_), so that the retained backup set is still
// restorable.
int ObBackupDeleteSelector::check_piece_can_be_deleted_(
    const ObTenantArchivePieceAttr &backup_piece_info,
    const SCN &start_replay_scn,
    const ObIArray<ObTenantArchivePieceAttr> &sorted_all_piece_infos,
    bool &can_be_deleted)
{
  int ret = OB_SUCCESS;
  can_be_deleted = false;
  if (!backup_piece_info.is_valid() || !start_replay_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(backup_piece_info), K(start_replay_scn));
  } else if (!can_backup_pieces_be_deleted_(backup_piece_info.status_)) {
    // The piece may still be written, do not delete it.
    can_be_deleted = false;
    LOG_INFO("[BACKUP_CLEAN]piece is not frozen or inactive, can not be deleted", K(backup_piece_info));
  } else if (backup_piece_info.end_scn_ <= start_replay_scn) {
    // The whole nominal range of the piece is before start_replay_scn.
    can_be_deleted = true;
  } else if (backup_piece_info.max_scn_ > start_replay_scn
      || backup_piece_info.checkpoint_scn_ > start_replay_scn) {
    // The piece really contains the log which is needed by clog_data_clean_point.
    can_be_deleted = false;
    LOG_INFO("[BACKUP_CLEAN]piece contains the log needed by clog_data_clean_point, can not be deleted",
        K(backup_piece_info), K(start_replay_scn));
  } else {
    // The nominal range of the piece covers start_replay_scn, but the piece does not contain any log
    // after start_replay_scn. It can be deleted only if start_replay_scn is still nominally covered
    // by another kept AVAILABLE piece, otherwise the restore path can not find the first piece.
    bool is_scn_covered_by_other_kept_piece = false;
    if (OB_FAIL(check_scn_covered_by_other_piece_(backup_piece_info, start_replay_scn,
        sorted_all_piece_infos, is_scn_covered_by_other_kept_piece))) {
      LOG_WARN("failed to check scn covered by other piece", K(ret), K(backup_piece_info),
          K(start_replay_scn));
    } else {
      // The piece has no log after start_replay_scn, so it can be deleted as long as
      // start_replay_scn is still covered by another piece which will be kept.
      can_be_deleted = is_scn_covered_by_other_kept_piece;
      LOG_INFO("[BACKUP_CLEAN]the nominal range of piece covers start_replay_scn but the piece does "
          "not contain any log after it", K(is_scn_covered_by_other_kept_piece), K(backup_piece_info),
          K(start_replay_scn));
    }
  }
  return ret;
}

// Return whether `scn`(start_replay_scn) is covered by another piece which is guaranteed to be kept
// and visible to restore.
//
// Pay attention that it is NOT enough that the log BYTES at `scn` physically exist in some other
// piece. The restore path additionally requires a piece whose NOMINAL range contains `scn`:
//   - ObArchiveStore::get_piece_paths_in_range accepts a piece list only if the FIRST piece
//     satisfies "start_scn_ <= scn < end_scn_", otherwise it fails with "No enough log for restore".
//     Its cross-boundary tolerance("prev.end_scn_ == cur.start_scn_ && prev.checkpoint_scn_ < scn")
//     only applies to the END boundary of the restore range, where both prev and cur are kept; there
//     is no such tolerance at the START boundary;
//   - ObArchivePersistHelper::check_piece_continuity_between_two_scn judges a backup set restorable
//     only if a not-deleted floor piece with "start_scn <= start_replay_scn" exists.
// So deleting the only piece whose nominal range covers `scn` would make the retained backup set
// un-restorable("no enough log" at restore job creation), even when all the log bytes it needs still
// physically exist, e.g. in the first cross-boundary log group of the next piece.
//
// Therefore a piece `other` covers `scn` only if ALL the conditions below hold:
//   0. other.key_.dest_id_ == piece_to_delete.key_.dest_id_. Restore only ever uses the pieces of ONE
//      dest: ObArchiveStore::get_piece_paths_in_range takes the dest_id of its first piece and skips
//      every piece with a different dest_id. So a piece of another dest can never be the first piece
//      of a restore from the path of `piece_to_delete`, no matter how its scn range looks. The caller
//      passes in the pieces of one dest only, this is the invariant it has to keep.
//   1. other.file_status_ is AVAILABLE. The restore path skips every piece whose file_status is not
//      AVAILABLE(see ObArchiveStore::get_piece_paths_in_range), so e.g. a DELETING piece is invisible
//      to restore and must not be relied on.
//   2. other.start_scn_ <= scn < other.end_scn_. The nominal range of `other` really contains scn,
//      see above. This can hold for a piece other than the one being judged when the scn ranges of
//      two rounds overlap, e.g. round 1 was stopped in the middle of its last piece(so its nominal
//      end_scn_ exceeds the real archived progress) and round 2 started before that nominal end.
//   3. other.checkpoint_scn_ > scn, strictly greater. `other` has really archived past scn. Note
//      that a piece with checkpoint_scn_ == scn is itself a deletion candidate of this very job
//      (get_candidate_obsolete_backup_pieces selects "checkpoint_scn <= start_replay_scn"), so it
//      may be deleted in the same round and must not be treated as a piece which will be kept. With
//      checkpoint_scn_ > scn, `other` can never enter the candidate set, hence really kept.
int ObBackupDeleteSelector::check_scn_covered_by_other_piece_(
    const ObTenantArchivePieceAttr &piece_to_delete,
    const SCN &scn,
    const ObIArray<ObTenantArchivePieceAttr> &sorted_all_piece_infos,
    bool &is_scn_covered_by_other_kept_piece)
{
  int ret = OB_SUCCESS;
  is_scn_covered_by_other_kept_piece = false;
  if (!scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(scn));
  } else {
    for (int64_t i = 0; !is_scn_covered_by_other_kept_piece && i < sorted_all_piece_infos.count(); i++) {
      const ObTenantArchivePieceAttr &other_piece_info = sorted_all_piece_infos.at(i);
      if (other_piece_info.key_ == piece_to_delete.key_) {
        // skip itself, a piece can not cover for its own deletion
      } else if (other_piece_info.key_.dest_id_ != piece_to_delete.key_.dest_id_) {
        // `other` is archived at another dest, it is invisible to a restore from the path of
        // `piece_to_delete`, so it can not cover scn.
      } else if (ObBackupFileStatus::BACKUP_FILE_AVAILABLE != other_piece_info.file_status_) {
        // `other` is invisible to restore, it can not cover scn.
      } else if (other_piece_info.end_scn_ <= scn) {
        // The nominal range of `other` ends at or before scn, so it does not contain scn(as
        // <start_scn, checkpoint_scn, end_scn>, with scn = 150): e.g. other = <50, 90, 100>, an
        // older piece entirely before scn, it obviously can not cover scn.
      } else if (other_piece_info.start_scn_ > scn) {
        // The nominal range of `other` starts after scn, so it does not contain scn(as
        // <start_scn, checkpoint_scn, end_scn>, with scn = 150): e.g. piece_to_delete = <100, 140,
        // 200> and its successor other = <200, 300, 400>: the log group crossing the piece
        // boundary(e.g. entries with scn [141, 210] make one group whose scn is the max entry scn
        // 210) physically lives in `other`'s directory, so the log BYTES at scn 150 do exist in
        // `other`. But restore planning still requires a piece with start_scn_ <= 150 as its first
        // piece(see the comment above), which only piece_to_delete can provide, so `other` must
        // not be counted as a covering piece.
      } else if (other_piece_info.checkpoint_scn_ <= scn) {
        // `other` nominally covers scn but has not really archived past it, it must not be treated
        // as a piece which will be kept. Two cases(with scn = 150):
        //   1. other.checkpoint_scn_ < scn, e.g. other = <130, 140, 430>: the log at scn 150 is not
        //      archived into `other` at all(its real progress stopped at 140), and `other` is itself
        //      a deletion candidate of this job(checkpoint_scn <= start_replay_scn);
        //   2. other.checkpoint_scn_ == scn, e.g. other = <130, 150, 430>: `other` does contain the
        //      log at 150, but it is still a deletion candidate of this very job, it may be judged
        //      deletable and reclaimed in the same round(e.g. covered by yet another piece), so
        //      relying on it could end up with every piece covering scn deleted. Only a piece with
        //      checkpoint_scn_ strictly greater than scn can never enter the candidate set.
      } else {
        // Reaching here means both `piece_to_delete` and `other` nominally contain scn. Pieces of
        // one round never overlap, so `other` must come from a different round: the round of
        // `piece_to_delete` was stopped in the middle of it(its nominal end_scn_ stays ahead of the
        // real archived progress, see the comment of check_piece_can_be_deleted_), and the next
        // round started before that nominal end. For example(as <start_scn, checkpoint_scn, end_scn>):
        //   piece_to_delete: <100, 120, 200>, the last piece of round 1, which was stopped at 120
        //   other          : <130, 300, 430>, the first piece of round 2, which started at 130
        // With start_replay_scn = 150, `other` nominally covers 150(130 <= 150 < 430) and has really
        // archived past it(300 > 150), so restore can take `other` as its first piece even after
        // piece_to_delete is reclaimed.
        is_scn_covered_by_other_kept_piece = true;
        LOG_INFO("[BACKUP_CLEAN]the log at scn is covered by another kept piece", K(scn),
            K(piece_to_delete), K(other_piece_info));
      }
    }
  }
  return ret;
}

// Remove from `piece_list` the pieces whose archive log is still needed by the restore of the retained
// backup set(`clog_data_clean_point`), which the SCN based checks above can NOT see.
//
// The reason is that the restore does NOT start to fetch the archive log at the lsn of
// start_replay_scn, but at the start of the 64M palf block that lsn falls in:
//   - what the backup writes into the backup set is palf_meta_.curr_lsn_, which
//     ObLogHandler::get_palf_base_info has rounded DOWN to a block boundary
//     ("lsn_2_block(base_lsn, PALF_BLOCK_SIZE) * PALF_BLOCK_SIZE"), because palf can only be advanced
//     to a block boundary;
//   - the restore advances palf to exactly that lsn(ObLSService::restore_update_ls_ ->
//     advance_base_info) and then asks the archive for the log from palf's end_lsn
//     (ObLogRestoreArchiveDriver::get_palf_base_lsn_scn_ / submit_fetch_log_task_).
//
// Meanwhile the archive splits a block across two pieces whenever the piece switches in the middle of
// it: the file id is a pure function of the lsn("lsn / 64M + 1", archive::cal_archive_file_id), so the
// new piece starts a NEW file with the SAME file id at offset 0 and its file header start_lsn is a
// mid-block lsn(ObArchiveSender::decide_archive_file_ / fill_file_header_if_needed_), the first half of
// the block is NOT re-archived into the new piece.
//
// So if the piece holding the first half of that block is reclaimed, the restore asks for an lsn which
// is smaller than the min lsn of every remaining piece and fails fatally with OB_ARCHIVE_LOG_RECYCLED
// (ObLogArchivePieceContext::get_) or OB_ERR_UNEXPECTED in backward_piece_. It is a silent failure:
// every SCN level check(check_piece_continuity_between_two_scn, ObArchiveStore::get_piece_paths_in_range,
// check_piece_can_be_deleted_ ...) still passes, the restore job is created and only fails later, when
// it starts to restore the log.
//
// Note that the smaller the write rate of a log stream is, the more likely it is to be hit: a 64M block
// of an idle log stream can span many pieces, so the block the retained backup set sits in may have
// started several pieces before the piece holding start_replay_scn.
//
// The check here is exact, not heuristic: the lsn the restore will ask for is recorded in the backup
// set(ObLSRestoreStartLSN), and the archived lsn range of a log stream in a piece is recorded in the
// piece info(ObSingleLSInfoDesc::max_lsn_, exclusive upper bound, the same value the restore compares
// with in ObLogArchivePieceContext::check_if_switch_piece_). Hence a piece must be kept iff
// "piece.max_lsn(ls) > restore_start_lsn(ls)" for any log stream of the backup set.
//
// Only the "from which lsn is the archive log of every log stream still needed" part is specific to the
// retained backup set, the piece walk itself is shared with the log_only policy, see
// find_min_needed_piece_idx_.
int ObBackupDeleteSelector::filter_pieces_needed_by_restore_(
    const ObBackupSetFileDesc &clog_data_clean_point,
    ObIArray<ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS;
  ObArray<ObLSRestoreStartLSN> ls_need_lsn_array;
  // The index of the oldest piece which has to be kept, -1 means none of them has to.
  int64_t first_kept_idx = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (piece_list.count() <= 0) {
    // do nothing
  } else if (!clog_data_clean_point.is_valid()) {
    // No backup set is retained, so no piece is deletable at all, get_all_dest_backup_piece_infos_
    // has returned nothing in this case. Defend against it anyway.
    piece_list.reset();
    LOG_INFO("[BACKUP_CLEAN]clog data clean point is invalid, keep all the pieces");
  } else if (clog_data_clean_point.plus_archivelog_) {
    // The log needed by the restore of this backup set has been copied into the backup set itself, so
    // the restore does not read the archive piece before its min_restore_scn at all.
    LOG_INFO("[BACKUP_CLEAN]backup set is plus archivelog, skip the restore start lsn check",
        K(clog_data_clean_point));
  } else if (OB_FAIL(data_provider_->get_backup_set_ls_restore_start_lsn(job_attr_->tenant_id_,
      clog_data_clean_point, ls_need_lsn_array))) {
    // Conservative: the lsn the restore needs is unknown, so keep every piece in this round. The
    // obsolete backup sets figured out in the same round are still deleted, and the next round retries.
    LOG_WARN("[BACKUP_CLEAN]failed to get restore start lsn of the retained backup set, keep all the"
        " pieces in this round", K(ret), K(clog_data_clean_point));
    ret = OB_SUCCESS;
    piece_list.reset();
  } else if (ls_need_lsn_array.empty()) {
    LOG_INFO("[BACKUP_CLEAN]the retained backup set has no log stream, skip the restore start lsn"
        " check", K(clog_data_clean_point));
  } else if (OB_FAIL(find_min_needed_piece_idx_(piece_list, ls_need_lsn_array, first_kept_idx))) {
    LOG_WARN("failed to find min needed piece idx", K(ret), K(clog_data_clean_point));
  } else if (first_kept_idx < 0) {
    // None of the candidates holds the log the restore starts from, all of them are reclaimable.
  } else {
    // Keep the oldest piece which has to be kept AND every piece after it, so that the pieces left in
    // the dest are always a continuous suffix: a piece in the middle must not be reclaimed even if it
    // holds no needed log itself, otherwise the remaining pieces would have a hole in them.
    // `piece_list` is sorted by piece id, so the pieces to keep are exactly the suffix starting at
    // first_kept_idx: just pop them, which needs neither an extra array nor any allocation.
    while (piece_list.count() > first_kept_idx) {
      piece_list.pop_back();
    }
    LOG_INFO("[BACKUP_CLEAN]keep the pieces which hold the log the restore of the retained backup set"
        " starts from", K(first_kept_idx), K(clog_data_clean_point), K(ls_need_lsn_array));
  }
  return ret;
}

// Return in `min_needed_idx` the index of the OLDEST piece of `sorted_pieces` which still holds the
// archive log that a restore starting from `ls_need_lsn_array` needs, -1 if none of them does. The
// caller keeps that piece and every piece after it, and reclaims the pieces before it.
//
// `sorted_pieces` MUST be the pieces of ONE archive dest, sorted in ascending order of piece id: the
// whole walk relies on the archived lsn range of a log stream growing monotonically with the piece id,
// which only holds inside one dest. The invariant is verified here, and a violation is handled
// conservatively(keep every piece) instead of comparing the lsn of one dest with the pieces of another.
//
// `ls_need_lsn_array` is the "from which lsn is the archive log of this log stream still needed" of every
// log stream which puts a constraint on the pieces. Where it comes from is up to the caller and is the
// only difference between the two delete obsolete policies:
//   - default : the palf base lsn recorded in the retained backup set, see
//               filter_pieces_needed_by_restore_ / ObLSRestoreStartLSN;
//   - log_only: there is no backup set to anchor on, so it is derived from the newest candidate piece
//               itself, see get_anchor_piece_ls_need_lsn_.
//
// The walk goes in DESCENDING order of piece id: once every log stream has been "cleared", i.e. some
// piece proves that no older piece can hold the log this log stream still needs, the walk stops right
// there. In the healthy case this costs only ONE piece info read, and it never reads more piece infos
// than the pieces it is about to reclaim plus a few.
//
// This function never fails because of a piece whose piece info can not be read: such a piece is
// conservatively treated as needed. An un-deletable piece only means "nothing older can be reclaimed for
// this dest in this round", it MUST NOT fail the whole backup clean job.
int ObBackupDeleteSelector::find_min_needed_piece_idx_(
    const ObIArray<ObTenantArchivePieceAttr> &sorted_pieces,
    const ObIArray<ObLSRestoreStartLSN> &ls_need_lsn_array,
    int64_t &min_needed_idx)
{
  int ret = OB_SUCCESS;
  // Cleared for good: no piece older than the one which cleared it can hold the needed log of the log
  // stream, no matter which round that piece belongs to.
  ObArray<bool> is_ls_cleared_array;
  // Cleared only inside the archive round currently being walked, see check_piece_log_needed_: a log
  // stream which is absent from a frozen piece had not started archiving in that round yet, so it has
  // nothing in the older pieces OF THAT ROUND, but it may well have log in the pieces of an older round.
  ObArray<bool> is_ls_absent_in_round_array;
  // The round id of the piece checked right before the current one(the newer neighbour), used to detect
  // that the walk has just stepped into an older archive round.
  int64_t last_round_id = -1;
  min_needed_idx = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (sorted_pieces.empty() || ls_need_lsn_array.empty()) {
    // No piece to check, or no log stream puts any constraint on them.
    LOG_INFO("[BACKUP_CLEAN]no piece to check or no need lsn, skip", K(sorted_pieces.count()),
        K(ls_need_lsn_array.count()));
  } else if (!check_pieces_belong_to_one_dest_(sorted_pieces)) {
    // A delete obsolete job refuses to run at all when more than one archive dest is valid(see
    // get_obsolete_backup_set_infos_helper_), and get_one_dest_deletable_backup_piece_infos_ drops every
    // candidate whose dest_id does not match the dest it works on. If the invariant does not hold
    // anyway(e.g. an "alter system set log_archive_dest_n" repointed a dest_no in the middle of this job,
    // which is exactly what the dest_id check in get_one_dest_deletable_backup_piece_infos_ guards
    // against), keep every piece in this round.
    min_needed_idx = 0;
    LOG_WARN("[BACKUP_CLEAN]the pieces do not belong to one archive dest, the archive dest may have"
        " just been changed, keep all the pieces in this round", K(sorted_pieces));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_need_lsn_array.count(); ++i) {
      if (OB_FAIL(is_ls_cleared_array.push_back(false))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(is_ls_absent_in_round_array.push_back(false))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    for (int64_t i = sorted_pieces.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      const ObTenantArchivePieceAttr &backup_piece_info = sorted_pieces.at(i);
      const int64_t round_id = backup_piece_info.key_.round_id_;
      int tmp_ret = OB_SUCCESS;
      bool is_log_needed = false;
      bool are_all_ls_cleared_in_round = false;
      bool are_all_ls_cleared_in_every_round = false;
      if (round_id != last_round_id) {
        // The walk has just stepped into an older archive round. "The log stream is absent from the
        // piece, so it had not started archiving yet" is only conclusive inside one round(the pieces of
        // a log stream are continuous inside a round, but a new round restarts the archive of every log
        // stream), so drop those conclusions and let the round which has just been entered prove them
        // again.
        for (int64_t j = 0; j < is_ls_absent_in_round_array.count(); ++j) {
          is_ls_absent_in_round_array.at(j) = false;
        }
        last_round_id = round_id;
      }
      if (OB_TMP_FAIL(check_piece_log_needed_(backup_piece_info, ls_need_lsn_array,
          is_ls_cleared_array, is_ls_absent_in_round_array, is_log_needed,
          are_all_ls_cleared_in_round, are_all_ls_cleared_in_every_round))) {
        // Conservative: whether the piece is needed is unknown(e.g. the piece info file can not be
        // read, which is the case for a piece that is not frozen yet), keep it. Never fail the whole
        // clean job because of it.
        min_needed_idx = i;
        LOG_WARN("[BACKUP_CLEAN]failed to check whether the log of the piece is still needed,"
            " keep the piece", K(tmp_ret), K(backup_piece_info));
      } else {
        if (is_log_needed) {
          min_needed_idx = i;
          // Keep the message and the first key/value of this log as they are, tools/obtest matches them.
          LOG_INFO("find dependency by 64M block question", "piece_id",
              backup_piece_info.key_.piece_id_, K(backup_piece_info.key_), K(ls_need_lsn_array));
        }
        if (are_all_ls_cleared_in_every_round) {
          // None of the pieces before this one can hold the log any log stream still needs, no matter
          // whether this piece itself has to be kept.
          LOG_INFO("[BACKUP_CLEAN]no older piece can hold the log which is still needed, stop looking"
              " backwards", K(i), K(is_log_needed), K(backup_piece_info.key_));
          break;
        } else if (are_all_ls_cleared_in_round) {
          // Some log stream has only been cleared by being absent from this piece, which says nothing
          // about the pieces of an OLDER round. Skip the rest of this round without reading it and go on
          // with the newest piece of the previous round, where the absent log streams are looked at
          // again. This costs one piece info read per archive round instead of one per piece.
          int64_t prev_round_idx = i - 1;
          while (prev_round_idx >= 0 && sorted_pieces.at(prev_round_idx).key_.round_id_ == round_id) {
            --prev_round_idx;
          }
          LOG_INFO("[BACKUP_CLEAN]no older piece of this archive round can hold the log which is still"
              " needed, skip the rest of the round", K(i), K(prev_round_idx), K(is_log_needed),
              K(backup_piece_info.key_));
          if (prev_round_idx < 0) {
            break;
          } else {
            // The loop decrements i, so the next piece to check is `prev_round_idx`.
            i = prev_round_idx + 1;
          }
        }
      }
    }
  }
  return ret;
}

bool ObBackupDeleteSelector::check_pieces_belong_to_one_dest_(
    const ObIArray<ObTenantArchivePieceAttr> &piece_list)
{
  bool is_one_dest = true;
  for (int64_t i = 1; is_one_dest && i < piece_list.count(); ++i) {
    if (piece_list.at(i).key_.dest_id_ != piece_list.at(0).key_.dest_id_) {
      is_one_dest = false;
    }
  }
  return is_one_dest;
}

// Whether `backup_piece_info` still holds the archive log which a restore starting from
// `ls_need_lsn_array` needs, i.e. "piece.max_lsn(ls) > need_lsn(ls)" for any of the log streams. This is
// the per-piece worker of find_min_needed_piece_idx_, see the comment there.
//
// `is_ls_cleared_array` and `is_ls_absent_in_round_array` are in/out arrays parallel to
// `ls_need_lsn_array`. A log stream is "cleared" by a piece when that piece proves that NO piece before
// it holds the log this log stream still needs, so it does not have to be looked at again. There are
// three ways for a piece to prove that, the first two are conclusive for every older piece and are
// recorded in `is_ls_cleared_array`, the third one only for the older pieces of the SAME archive round
// and is recorded in `is_ls_absent_in_round_array`(the caller resets it when it steps into an older
// round, see find_min_needed_piece_idx_):
//   1. the log stream does have archived data in the piece and all of it is already before its need
//      lsn. The archived lsn range of a log stream grows monotonically with the piece id, so the range
//      of the same log stream in every older piece is entirely below this piece's min lsn;
//   2. the archived range of the log stream in the piece starts at the very beginning of palf(min lsn is
//      0), i.e. the piece holds the first log this log stream ever archived. This is the usual case for
//      a log stream which was created while the archive was already running, and it holds no matter
//      whether the log of this piece is still needed;
//   3. the log stream is absent from the piece. The piece info file of a FROZEN piece lists every log
//      stream which had started archiving at or before that piece: a piece is only frozen after every
//      archiving log stream has archived into a newer piece(see ObDestRoundCheckpointer::count_,
//      max_active_piece_id_ is the MIN of the max piece id of the log streams), and the pieces of one
//      log stream inside one round are continuous(enforced by ObLSDestRoundSummary::add_one_piece). So
//      an absent log stream had not started archiving in this round yet and it has nothing in the older
//      pieces of this round. The other reason for being absent - the log stream was gc'd in an earlier
//      piece - is impossible here: `ls_need_lsn_array` always comes from a point in time NEWER than
//      every piece of the walk(the retained backup set, or the newest candidate piece), and a log
//      stream which exists at that time can not have been gc'd before, ls ids are never reused.
// `are_all_ls_cleared_in_round` is true when every log stream is cleared by any of the three reasons, so
// no older piece OF THIS ROUND holds needed log; `are_all_ls_cleared_in_every_round` is true when every
// log stream is cleared by reason 1 or 2 only, so no older piece at all holds needed log. Both are
// independent of whether the log of this piece itself is still needed.
int ObBackupDeleteSelector::check_piece_log_needed_(
    const ObTenantArchivePieceAttr &backup_piece_info,
    const ObIArray<ObLSRestoreStartLSN> &ls_need_lsn_array,
    ObIArray<bool> &is_ls_cleared_array,
    ObIArray<bool> &is_ls_absent_in_round_array,
    bool &is_log_needed,
    bool &are_all_ls_cleared_in_round,
    bool &are_all_ls_cleared_in_every_round)
{
  int ret = OB_SUCCESS;
  ObPieceInfoDesc piece_info_desc;
  is_log_needed = false;
  are_all_ls_cleared_in_round = false;
  are_all_ls_cleared_in_every_round = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (!backup_piece_info.is_valid() || ls_need_lsn_array.empty()
      || ls_need_lsn_array.count() != is_ls_cleared_array.count()
      || ls_need_lsn_array.count() != is_ls_absent_in_round_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(backup_piece_info), K(ls_need_lsn_array),
        K(is_ls_cleared_array.count()), K(is_ls_absent_in_round_array.count()));
  } else if (OB_FAIL(data_provider_->load_piece_info_desc(job_attr_->tenant_id_, backup_piece_info,
      piece_info_desc))) {
    LOG_WARN("failed to load piece info desc", K(ret), K(backup_piece_info));
  } else {
    are_all_ls_cleared_in_round = true;
    are_all_ls_cleared_in_every_round = true;
    for (int64_t i = 0; i < ls_need_lsn_array.count(); ++i) {
      const ObLSRestoreStartLSN &ls_need_lsn = ls_need_lsn_array.at(i);
      bool is_ls_found = false;
      if (is_ls_cleared_array.at(i)) {
        continue;
      } else if (is_ls_absent_in_round_array.at(i)) {
        are_all_ls_cleared_in_every_round = false;
        continue;
      }
      for (int64_t j = 0; !is_ls_found && j < piece_info_desc.filelist_.count(); ++j) {
        const ObSingleLSInfoDesc &single_ls_info = piece_info_desc.filelist_.at(j);
        if (single_ls_info.ls_id_ != ls_need_lsn.ls_id_) {
          continue;
        }
        is_ls_found = true;
        // max_lsn_ is the exclusive upper bound of the log archived in this piece, the same semantic
        // as InnerPieceContext::max_lsn_in_piece_, which the restore compares with by
        // "max_lsn_in_piece_ > lsn" to decide whether the piece covers the lsn it wants.
        const palf::LSN max_lsn_in_piece(single_ls_info.max_lsn_);
        if (max_lsn_in_piece > ls_need_lsn.start_lsn_) {
          is_log_needed = true;
          LOG_INFO("[BACKUP_CLEAN]the piece holds the log which is still needed",
              K(backup_piece_info.key_), K(ls_need_lsn), K(single_ls_info.min_lsn_),
              K(single_ls_info.max_lsn_));
          if (palf::PALF_INITIAL_LSN_VAL == single_ls_info.min_lsn_) {
            // Reason 2: the piece holds the first log this log stream ever archived, so no older piece
            // holds any log of it, let alone the log which is still needed.
            is_ls_cleared_array.at(i) = true;
          }
        } else {
          // Reason 1.
          is_ls_cleared_array.at(i) = true;
        }
      }
      if (!is_ls_found && backup_piece_info.status_.is_frozen()) {
        // Reason 3. Only a FROZEN piece lists every log stream which had started archiving at or before
        // it, so the absence of a log stream is only conclusive for a frozen piece. In fact only a
        // frozen piece has a piece info file at all(see record_piece_info), the status is checked here
        // just to make the requirement explicit.
        is_ls_absent_in_round_array.at(i) = true;
        LOG_INFO("[BACKUP_CLEAN]the log stream had not started archiving in this round yet, no older"
            " piece of this round holds its log", K(backup_piece_info.key_), K(ls_need_lsn));
      }
      if (!is_ls_cleared_array.at(i)) {
        are_all_ls_cleared_in_every_round = false;
        if (!is_ls_absent_in_round_array.at(i)) {
          are_all_ls_cleared_in_round = false;
        }
      }
    }
  }
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
      } else if (OB_UNLIKELY(!can_backup_pieces_be_deleted_(backup_piece_info.status_))) {
        // defense, get_one_dest_deletable_backup_piece_infos_ has filtered out these pieces already.
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("piece can not be deleted", K(ret), K(backup_piece_info));
      } else if (OB_FAIL(piece_list.push_back(backup_piece_info))) {
        LOG_WARN("failed to push back piece list", K(ret), K(backup_piece_info));
      }
    }
    // The checks above only compare SCNs, they can not see that the restore of the retained backup set
    // starts to fetch the archive log at the start of a 64M palf block, which may live in an older
    // piece. Filter such pieces out by LSN here.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(filter_pieces_needed_by_restore_(clog_data_clean_point, piece_list))) {
      LOG_WARN("failed to filter the pieces needed by the restore of the retained backup set", K(ret),
          K(clog_data_clean_point));
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
//
// The log_only policy has no backup set to anchor on: any point covered by the pieces which are kept may
// be recovered to, so what has to stay usable is the OLDEST KEPT piece itself. Hence the anchor is the
// newest candidate piece(the first not expired one, appended by get_all_dest_backup_piece_infos_ as the
// about_to_expire_piece), and the log which is still needed is the log from the start of the 64M palf
// block that piece begins in - a restore/standby always starts to fetch the archive log at a block
// boundary, see get_anchor_piece_ls_need_lsn_. Everything after that is the same walk as the default
// policy does, see find_min_needed_piece_idx_.
int ObBackupDeleteSelector::get_min_depended_piece_idx_(
  const ObIArray<ObTenantArchivePieceAttr> &candidate_piece_infos,
  int64_t &min_depended_pieces_idx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObLSRestoreStartLSN> ls_need_lsn_array;
  const int64_t anchor_idx = candidate_piece_infos.count() - 1;
  if (anchor_idx < 0) {
    LOG_INFO("No candidate pieces to check dependency, skip");
  } else if (OB_TMP_FAIL(get_anchor_piece_ls_need_lsn_(candidate_piece_infos.at(anchor_idx),
      ls_need_lsn_array))) {
    // Conservative: the lsn from which the log is still needed is unknown(e.g. the piece info file of
    // the anchor piece can not be read), so keep every candidate piece in this round. Do NOT fail the
    // whole clean job because of it: OB_BACKUP_DELETE_BACKUP_PIECE_NOT_ALLOWED is not retryable(see
    // ObBackupUtils::is_need_retry_error), the job would go to FAILED and nothing would ever be
    // reclaimed.
    min_depended_pieces_idx = 0;
    LOG_WARN("[BACKUP_CLEAN]failed to get the need lsn of the anchor piece, keep all the candidate"
        " pieces in this round", K(tmp_ret), K(anchor_idx), K(candidate_piece_infos.at(anchor_idx)));
  } else if (OB_FAIL(find_min_needed_piece_idx_(candidate_piece_infos, ls_need_lsn_array,
      min_depended_pieces_idx))) {
    LOG_WARN("Failed to find min needed piece idx", K(ret), K(ls_need_lsn_array));
  } else if (min_depended_pieces_idx < 0) {
    // No log stream of the anchor piece depends on an older piece. The anchor piece itself is kept
    // anyway: it is the piece which is NOT expired yet, deleting it would break the recovery window.
    min_depended_pieces_idx = anchor_idx;
  }
  LOG_INFO("Finished checking piece dependency", K(ret), K(min_depended_pieces_idx), K(candidate_piece_infos.count()));
  return ret;
}

// Return the lsn from which the archive log of every log stream of `anchor_piece` is still needed, which
// is the start of the palf block `anchor_piece` begins in: whoever reads the archive advances palf to a
// block boundary first and then asks the archive for the log from exactly that lsn(see
// ObLSRestoreStartLSN), so the first half of that block, which lives in the piece the block was split
// by, must not be reclaimed.
//
// Pay attention that EVERY log stream listed in the piece info puts such a constraint, including a log
// stream which archived nothing into `anchor_piece`(max_lsn_ == min_lsn_, in which case its filelist_ is
// empty too, see record_piece_info). Being idle is not "no information": min_lsn_ is the lsn the log
// stream stands at when the anchor piece BEGINS, no matter whether the piece holds any new log of it
// (ObLSArchiveTask::ArchiveDest::compensate_piece keeps piece_min_lsn_ at the archived lsn when a piece
// switch finds nothing new to archive), which is exactly the same meaning as for a log stream which did
// archive into the piece. So whoever starts to read the anchor still has to position palf at the start of
// the block that lsn falls in, and the first half of that block lives in an older piece - reclaiming that
// piece would break the read, silently, all the way until the log is really fetched. This is the more
// likely case of the two: the more idle a log stream is, the more pieces a single 64M block of it spans.
//
// Note that a log stream which has been gc'd does not pin an older piece forever: only the LAST piece of
// a deleted log stream lists it(see ObDestRoundCheckpointer::generate_one_piece_ and
// ObLSDestRoundSummary::check_is_last_piece_for_deleted_ls), so as soon as the anchor moves past that
// piece the constraint disappears by itself.
int ObBackupDeleteSelector::get_anchor_piece_ls_need_lsn_(
    const ObTenantArchivePieceAttr &anchor_piece,
    ObIArray<ObLSRestoreStartLSN> &ls_need_lsn_array)
{
  int ret = OB_SUCCESS;
  ObPieceInfoDesc anchor_piece_info;
  ls_need_lsn_array.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDeleteSelector not inited", K(ret));
  } else if (OB_FAIL(data_provider_->load_piece_info_desc(job_attr_->tenant_id_, anchor_piece,
      anchor_piece_info))) {
    LOG_WARN("Failed to load the piece info of the anchor piece", K(ret), K(anchor_piece));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < anchor_piece_info.filelist_.count(); ++i) {
      const ObSingleLSInfoDesc &single_ls_info = anchor_piece_info.filelist_.at(i);
      const palf::LSN min_lsn_in_piece(single_ls_info.min_lsn_);
      ObLSRestoreStartLSN ls_need_lsn;
      if (OB_UNLIKELY(!single_ls_info.ls_id_.is_valid() || !min_lsn_in_piece.is_valid()
          || single_ls_info.max_lsn_ < single_ls_info.min_lsn_)) {
        // The piece info is corrupted. Fail here on purpose: the caller keeps every candidate piece of
        // this round then, which is the only safe thing to do when the lsn the log is needed from can
        // not be told. Note that the check has to be done on min_lsn_ ITSELF, checking the rounded down
        // lsn below would not do: rounding LOG_INVALID_LSN_VAL down to a block boundary turns it into a
        // value which passes is_valid().
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid single ls info of the anchor piece", K(ret), K(anchor_piece.key_),
            K(single_ls_info));
      } else {
        ls_need_lsn.ls_id_ = single_ls_info.ls_id_;
        // Round the min lsn of the piece DOWN to the start of the palf block it falls in, the same way
        // ObLogHandler::get_palf_base_info does it for the backup set.
        ls_need_lsn.start_lsn_ = palf::LSN(palf::lsn_2_block(min_lsn_in_piece, palf::PALF_BLOCK_SIZE)
            * palf::PALF_BLOCK_SIZE);
        if (OB_UNLIKELY(!ls_need_lsn.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid ls need lsn", K(ret), K(ls_need_lsn), K(single_ls_info));
        } else if (OB_FAIL(ls_need_lsn_array.push_back(ls_need_lsn))) {
          LOG_WARN("failed to push back ls need lsn", K(ret), K(ls_need_lsn));
        }
      }
    }
    LOG_INFO("[BACKUP_CLEAN]get the need lsn of the anchor piece", K(ret), K(anchor_piece.key_),
        K(ls_need_lsn_array));
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
  } else if (expired_scn_from_errsim > 0 && OB_FAIL(clog_data_clean_point.convert_for_gts(expired_scn_from_errsim))) {
    LOG_WARN("errsim failed to convert for gts", K(ret), K(expired_scn_from_errsim));
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