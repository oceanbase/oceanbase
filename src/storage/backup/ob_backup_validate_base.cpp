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

#include "ob_backup_validate_base.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_validate_struct.h"
#include "share/backup/ob_backup_validate_table_operator.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/location_cache/ob_location_service.h"
#include "storage/high_availability/ob_storage_restore_struct.h"
#include "storage/backup/ob_backup_sstable_sec_meta_iterator.h"
#include "storage/backup/ob_backup_data_struct.h"


namespace oceanbase
{
namespace storage
{
ObBackupValidateDagNetInitParam::ObBackupValidateDagNetInitParam()
  : trace_id_(), job_id_(0), tenant_id_(0), incarnation_(0), task_id_(0),
    ls_id_(0), task_type_(), validate_id_(0), dest_id_(0), round_id_(0),
    validate_level_(), validate_path_()
{
}

void ObBackupValidateDagNetInitParam::reset()
{
  trace_id_.reset();
  job_id_ = 0;
  tenant_id_ = 0;
  incarnation_ = 0;
  task_id_ = 0;
  ls_id_.reset();
  task_type_.reset();
  validate_id_ = 0;
  dest_id_ = 0;
  round_id_ = 0;
  validate_level_.reset();
  validate_path_.reset();
}

bool ObBackupValidateDagNetInitParam::is_valid() const
{
  return trace_id_.is_valid() && job_id_ > 0 && tenant_id_ > 0
          && task_id_ > 0 && ls_id_.is_valid()
          && task_type_.is_valid() && validate_id_ > 0 && dest_id_ >= share::OB_START_DEST_ID
          && validate_level_.is_valid() && !validate_path_.is_empty();
}

int ObBackupValidateDagNetInitParam::set(const obrpc::ObBackupValidateLSArg &args)
{
  int ret = OB_SUCCESS;
  if (!args.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(args));
  } else if (OB_FAIL(validate_path_.assign(args.validate_path_))) {
    LOG_WARN("failed to assign validate path", KR(ret), K(args));
  } else {
    trace_id_ = args.trace_id_;
    job_id_ = args.job_id_;
    tenant_id_ = args.tenant_id_;
    incarnation_ = args.incarnation_;
    task_id_ = args.task_id_;
    ls_id_ = args.ls_id_;
    task_type_ = args.task_type_;
    validate_id_ = args.validate_id_;
    dest_id_ = args.dest_id_;
    round_id_ = args.round_id_;
    validate_level_ = args.validate_level_;
  }
  return ret;
}

bool ObBackupValidateDagNetInitParam::operator == (const ObBackupValidateDagNetInitParam &other) const
{
  return trace_id_ == other.trace_id_ && job_id_ == other.job_id_ && tenant_id_ == other.tenant_id_
          && incarnation_ == other.incarnation_ && task_id_ == other.task_id_
          && ls_id_ == other.ls_id_ && task_type_.type_ == other.task_type_.type_
          && validate_id_ == other.validate_id_ && dest_id_ == other.dest_id_
          && round_id_ == other.round_id_ && validate_level_.level_ == other.validate_level_.level_
          && validate_path_ == other.validate_path_;
}

bool ObBackupValidateDagNetInitParam::operator != (const ObBackupValidateDagNetInitParam &other) const
{
  return !(*this == other);
}

uint64_t ObBackupValidateDagNetInitParam::hash() const
{
  uint64_t hash_value = 0;
  hash_value = validate_path_.hash();
  hash_value = common::murmurhash(&trace_id_, sizeof(trace_id_), hash_value);
  hash_value = common::murmurhash(&job_id_, sizeof(job_id_), hash_value);
  hash_value = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_value);
  hash_value = common::murmurhash(&incarnation_, sizeof(incarnation_), hash_value);
  hash_value = common::murmurhash(&task_id_, sizeof(task_id_), hash_value);
  hash_value = common::murmurhash(&ls_id_, sizeof(ls_id_), hash_value);
  hash_value = common::murmurhash(&task_type_, sizeof(task_type_), hash_value);
  hash_value = common::murmurhash(&validate_id_, sizeof(validate_id_), hash_value);
  hash_value = common::murmurhash(&dest_id_, sizeof(dest_id_), hash_value);
  hash_value = common::murmurhash(&round_id_, sizeof(round_id_), hash_value);
  hash_value = common::murmurhash(&validate_level_, sizeof(validate_level_), hash_value);
  return hash_value;
}

int ObBackupValidateDagNetInitParam::assign(const ObBackupValidateDagNetInitParam &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(other));
  } else if (OB_FAIL(validate_path_.assign(other.validate_path_))) {
    LOG_WARN("failed to assign validate path", KR(ret), K(other));
  } else {
    trace_id_ = other.trace_id_;
    job_id_ = other.job_id_;
    tenant_id_ = other.tenant_id_;
    incarnation_ = other.incarnation_;
    task_id_ = other.task_id_;
    ls_id_ = other.ls_id_;
    task_type_ = other.task_type_;
    validate_id_ = other.validate_id_;
    dest_id_ = other.dest_id_;
    round_id_ = other.round_id_;
    validate_level_ = other.validate_level_;
  }
  return ret;
}

/*
-------------------------ObBackupFileGroup-------------------------------
*/
ObBackupFileGroup::ObBackupFileGroup()
  : group_id_(-1), accumulated_file_count_(0), file_list_()
{
}

void ObBackupFileGroup::reset()
{
  group_id_ = -1;
  accumulated_file_count_ = 0;
  file_list_.reset();
}

bool ObBackupFileGroup::is_valid() const
{
  return group_id_ >= 0 && file_list_.count() > 0 && file_list_.count() <= OB_MAX_FILES_PER_VALIDATE_GROUP
            && accumulated_file_count_ > 0;
}

int ObBackupFileGroup::assign(const ObBackupFileGroup &other)
{
  int ret = OB_SUCCESS;
  if (is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file group already valid", KR(ret));
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file group", KR(ret), K(other));
  } else if (OB_FAIL(file_list_.assign(other.file_list_))) {
    LOG_WARN("failed to assign file list", KR(ret));
  } else {
    group_id_ = other.group_id_;
    accumulated_file_count_ = other.accumulated_file_count_;
  }
  return ret;
}

/*
-------------------------ObBackupArchivePieceLSNRange-------------------------------
*/
ObBackupArchivePieceLSNRange::ObBackupArchivePieceLSNRange()
  : group_id_(-1), start_lsn_(), end_lsn_()
{
}

void ObBackupArchivePieceLSNRange::reset()
{
  group_id_ = -1;
  start_lsn_.reset();
  end_lsn_.reset();
}

bool ObBackupArchivePieceLSNRange::is_valid() const
{
  return group_id_ >= 0 && start_lsn_.is_valid() && end_lsn_.is_valid();
}

int ObBackupArchivePieceLSNRange::assign(const ObBackupArchivePieceLSNRange &other)
{
  int ret = OB_SUCCESS;
  if (is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("archive piece LSN range already valid", KR(ret));
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid archive piece LSN range", KR(ret), K(other));
  } else {
    group_id_ = other.group_id_;
    start_lsn_ = other.start_lsn_;
    end_lsn_ = other.end_lsn_;
  }
  return ret;
}

/*
-------------------------ObBackupValidateTaskContext-------------------------------
*/
ObBackupValidateTaskContext::ObBackupValidateTaskContext()
  : tablet_array_(), dir_queue_(), archive_piece_lsn_ranges_(), ls_info_(), running_groups_(), result_(OB_SUCCESS),
    ctx_lock_(common::ObLatchIds::BACKUP_LOCK), next_task_id_(0), total_task_count_(0),
    total_read_bytes_(0), delta_read_bytes_(0), last_reported_checkpoint_(0), inited_(false)
{
}

void ObBackupValidateTaskContext::reset()
{
  tablet_array_.reset();
  dir_queue_.reset();
  archive_piece_lsn_ranges_.reset();
  running_groups_.clear();
  result_ = OB_SUCCESS;
  next_task_id_ = 0;
  total_task_count_ = 0;
  total_read_bytes_ = 0;
  delta_read_bytes_ = 0;
  last_reported_checkpoint_ = 0;
  running_groups_.destroy();
  inited_ = false;
}

int ObBackupValidateTaskContext::init()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(running_groups_.create(OB_MAX_VALIDATE_RUNNING_GROUP_COUNT))) {
    LOG_WARN("failed to create running groups", KR(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

bool ObBackupValidateTaskContext::is_valid() const
{
  return inited_ && (!dir_queue_.empty() || !tablet_array_.empty() || !archive_piece_lsn_ranges_.empty()) ;
}

int ObBackupValidateTaskContext::set_dir_queue(const common::ObIArray<ObBackupPathString> &dir_queue)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(ctx_lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(dir_queue_.assign(dir_queue))) {
    LOG_WARN("failed to assign dir queue", KR(ret), K(dir_queue));
  } else {
    total_task_count_ = dir_queue_.count();
  }

  return ret;
}

int ObBackupValidateTaskContext::get_dir_path(const int64_t dir_queue_idx, ObBackupPathString &dir_path)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(ctx_lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (dir_queue_idx < 0 || dir_queue_idx >= dir_queue_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dir queue idx", KR(ret), K(dir_queue_idx));
  } else {
    dir_path = dir_queue_.at(dir_queue_idx);
  }
  return ret;
}

int ObBackupValidateTaskContext::set_tablet_array(const common::ObIArray<common::ObTabletID> &tablet_array)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(ctx_lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(tablet_array_.assign(tablet_array))) {
    LOG_WARN("failed to assign tablet array", KR(ret), K(tablet_array));
  } else {
    total_task_count_ = tablet_array_.count();
  }
  return ret;
}

int ObBackupValidateTaskContext::get_tablet_id(const int64_t task_id, common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(ctx_lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (tablet_array_.empty()) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("tablet group is empty", KR(ret));
  } else if (task_id < 0 || task_id >= tablet_array_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task id", KR(ret), K(task_id));
  } else {
    tablet_id = tablet_array_.at(task_id);
  }
  return ret;
}

int ObBackupValidateTaskContext::add_archive_piece_lsn_range(const ObBackupArchivePieceLSNRange &lsn_range)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(ctx_lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!lsn_range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid archive piece LSN range", KR(ret), K(lsn_range));
  } else if (OB_FAIL(archive_piece_lsn_ranges_.push_back(lsn_range))) {
    LOG_WARN("failed to push back archive piece LSN range", KR(ret), K(lsn_range));
  } else {
    total_task_count_++;
  }
  return ret;
}

int ObBackupValidateTaskContext::get_archive_piece_lsn_range(
    const int64_t group_id,
    ObBackupArchivePieceLSNRange* &lsn_range,
    bool &is_last_task)
{
  int ret = OB_SUCCESS;
  is_last_task = false;
  common::SpinWLockGuard guard(ctx_lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (archive_piece_lsn_ranges_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("archive piece LSN range is empty", KR(ret));
  } else if (group_id < 0 || group_id >= archive_piece_lsn_ranges_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid range id", KR(ret), K(group_id));
  } else {
    lsn_range = &(archive_piece_lsn_ranges_.at(group_id));
    if (group_id == archive_piece_lsn_ranges_.count() - 1) {
      is_last_task = true;
    }
  }
  return ret;
}

int ObBackupValidateTaskContext::set_validate_result(
  const int result,
  const char *error_msg,
  const ObBackupValidateDagNetInitParam &param,
  const common::ObAddr &src_server,
  const ObTaskId &dag_id,
  backup::ObBackupReportCtx &report_ctx)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(ctx_lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(error_msg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("error message is null", KR(ret), KP(error_msg));
  } else if (!param.is_valid() || !src_server.is_valid() || !dag_id.is_valid() || !report_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(param), K(src_server), K(dag_id), K(report_ctx));
  } else if (OB_SUCCESS != result && OB_SUCCESS == result_) {
    result_ = result;
    LOG_INFO("set validate result", KR(ret), K(result), K(error_msg));
    if ('\0' != error_msg[0] && OB_FAIL(share::ObBackupValidateTaskOperator::add_comment(
          *report_ctx.sql_proxy_, param.tenant_id_, param.task_id_, error_msg))) {
      LOG_WARN("failed to add validate task comment", KR(ret), K(param), KP(error_msg));
    }
    obrpc::ObBackupTaskRes res;
    res.job_id_ = param.job_id_;
    res.task_id_ = param.task_id_;
    res.tenant_id_ = param.tenant_id_;
    res.ls_id_ = param.ls_id_;
    res.result_ = result;
    res.src_server_ = src_server;
    res.trace_id_ = param.trace_id_;
    res.dag_id_ = dag_id;
    if (OB_FAIL(ret)) {
    } else if (!res.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(res));
    } else if (OB_FAIL(ObBackupValidateObUtils::report_validate_over(res, report_ctx))) {
      LOG_WARN("failed to report validate over", KR(ret), K(res), K(report_ctx));
    }
  }
  return ret;
}

int ObBackupValidateTaskContext::get_result() const
{
  common::SpinRLockGuard guard(ctx_lock_);
  return result_;
}

int ObBackupValidateTaskContext::set_next_task_id_and_read_bytes(const int64_t next_task_id, const int64_t read_bytes)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(ctx_lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (next_task_id < 0 || next_task_id > total_task_count_ || read_bytes < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid next task id and read bytes", KR(ret), K(next_task_id), K(read_bytes));
  } else {
    next_task_id_ = next_task_id;
    last_reported_checkpoint_ = next_task_id;
    total_read_bytes_ = read_bytes;
    delta_read_bytes_ = 0;
  }
  return ret;
}

int ObBackupValidateTaskContext::get_next_task_id(int64_t &task_id)
{
  common::SpinWLockGuard guard(ctx_lock_);
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    task_id = next_task_id_ >= total_task_count_ ? -1 : next_task_id_;
    if (task_id != -1) {
      next_task_id_++;
      if (OB_FAIL(add_running_group_(task_id))) {
        LOG_WARN("failed to add running group", KR(ret), K(task_id));
      }
    }
  }
  return ret;
}

int ObBackupValidateTaskContext::add_running_group_(const int64_t running_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (running_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid running id", KR(ret), K(running_id));
  } else if (OB_FAIL(running_groups_.set_refactored(running_id))) {
    LOG_WARN("failed to add running group", KR(ret), K(running_id));
  }
  return ret;
}

int ObBackupValidateTaskContext::set_ls_info(const share::ObSingleLSInfoDesc &ls_info)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(ctx_lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!ls_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls info", KR(ret), K(ls_info));
  } else if (ls_info_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls info already set", KR(ret), K(ls_info));
  } else {
    ls_info_ = ls_info;
  }
  return ret;
}

const share::ObSingleLSInfoDesc* ObBackupValidateTaskContext::get_ls_info() const
{
  common::SpinRLockGuard guard(ctx_lock_);
  return &ls_info_;
}

// for check validated checkpoint
// get the max validated task id
int ObBackupValidateTaskContext::remove_running_task_id(
    const int64_t running_id,
    const int64_t read_bytes,
    bool &need_update_stat,
    int64_t &validate_checkpoint_group,
    int64_t &delta_read_bytes,
    int64_t &total_read_bytes)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(ctx_lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (running_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid running id", KR(ret), K(running_id));
  } else {
    const int64_t VALIDATE_CHECKPOINT_INTERVAL = 128;
    need_update_stat = false;
    validate_checkpoint_group = -1;
    delta_read_bytes = 0;
    total_read_bytes = 0;
    total_read_bytes_ += read_bytes;
    delta_read_bytes_ += read_bytes;
    if (OB_FAIL(running_groups_.erase_refactored(running_id))) {
      LOG_WARN("failed to remove running group", KR(ret), K(running_id));
    } else if (running_groups_.empty()) {
      need_update_stat = true;
      validate_checkpoint_group = next_task_id_ >= total_task_count_ ? total_task_count_ : next_task_id_;
    } else {
      int64_t min_running_id = INT64_MAX;
      hash::ObHashSet<int64_t>::iterator iter = running_groups_.begin();
      while (iter != running_groups_.end()) {
        if (iter->first < min_running_id) {
          min_running_id = iter->first;
        }
        iter++;
      }
      if (min_running_id > running_id) {
        need_update_stat = true;
        validate_checkpoint_group = min_running_id;
      }
    }
    if (OB_SUCC(ret) && need_update_stat) {
      if (validate_checkpoint_group == total_task_count_
              || validate_checkpoint_group - last_reported_checkpoint_ >= VALIDATE_CHECKPOINT_INTERVAL) {
        total_read_bytes = total_read_bytes_;
        delta_read_bytes = delta_read_bytes_;
        delta_read_bytes_ = 0;
        last_reported_checkpoint_ = validate_checkpoint_group;
      } else {
        need_update_stat = false;
      }
    }
  }
  return ret;
}

int ObBackupValidateObUtils::report_validate_over(
    const obrpc::ObBackupTaskRes &res,
    backup::ObBackupReportCtx &report_ctx)
{
  int ret = OB_SUCCESS;
  if (!res.is_valid() || !report_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(res), K(report_ctx));
  } else {
    common::ObAddr leader_addr;
    const int64_t cluster_id = GCONF.cluster_id;
    const uint64_t meta_tenant_id = gen_meta_tenant_id(res.tenant_id_);
    if (OB_FAIL(report_ctx.location_service_->get_leader_with_retry_until_timeout(cluster_id, meta_tenant_id,
                                                                        ObLSID(ObLSID::SYS_LS_ID), leader_addr))) {
      LOG_WARN("failed to get leader", KR(ret), K(res), K(report_ctx));
    } else if (OB_FAIL(report_ctx.rpc_proxy_->to(leader_addr).report_backup_validate_over(res))) {
      LOG_WARN("failed to report backup validate over", KR(ret), K(res), K(leader_addr), K(res));
    } else {
      SERVER_EVENT_ADD("backup_data", "report_result", "job_id", res.job_id_, "task_id", res.task_id_,
                          "tenant_id", res.tenant_id_, "ls_id", res.ls_id_.id(), "result", res.result_);
      LOG_INFO("finish task post rpc result", K(res));
    }
  }

  return ret;
}

int ObBackupValidateObUtils::init_meta_index_store(
    const ObExternBackupSetInfoDesc &backup_set_info, const ObBackupValidateDagNetInitParam &param,
    const bool is_sec_meta, ObBackupDataStore &store, backup::ObBackupMetaIndexStoreWrapper &meta_index_store)
{
  int ret = OB_SUCCESS;
  if (!backup_set_info.is_valid() || !param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(backup_set_info), K(param));
  } else if (!backup_set_info.backup_set_file_.is_backup_set_not_support_quick_restore() && is_sec_meta) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup set file not support quick restore", KR(ret), K(backup_set_info), K(is_sec_meta));
  } else {
    ObBackupPath backup_path;
    const ObBackupDest &backup_set_dest = store.get_backup_set_dest();
    const backup::ObBackupRestoreMode mode = backup::ObBackupRestoreMode::BACKUP_MODE;
    const backup::ObBackupIndexLevel index_level = backup::ObBackupIndexLevel::BACKUP_INDEX_LEVEL_LOG_STREAM;
    backup::ObBackupIndexStoreParam index_store_param;
    int64_t retry_id = 0;
    int64_t dest_id = 0;
    const ObLSID &ls_id = param.ls_id_;
    share::ObBackupDataType data_type;
    data_type.set_sys_data_backup();
    index_store_param.index_level_ = index_level;
    index_store_param.tenant_id_ = param.tenant_id_;
    index_store_param.backup_set_id_ = backup_set_info.backup_set_file_.backup_set_id_;
    index_store_param.ls_id_ = param.ls_id_;
    index_store_param.is_tenant_level_ = false;
    index_store_param.turn_id_ = param.ls_id_.is_sys_ls() ? 1 : backup_set_info.backup_set_file_.meta_turn_id_;
    index_store_param.dest_id_ = param.dest_id_;
    if (OB_FAIL(ObBackupPathUtil::get_ls_backup_dir_path(backup_set_dest, ls_id, backup_path))) {
      LOG_WARN("failed to get ls backup path", KR(ret), K(backup_set_dest), K(ls_id));
    } else if (OB_FAIL(store.get_max_sys_ls_retry_id(backup_path, ls_id, index_store_param.turn_id_, retry_id))) {
      LOG_WARN("failed to get max sys ls retry id", KR(ret), K(backup_path), K(ls_id), K(index_store_param));
    } else {
      index_store_param.retry_id_ = retry_id;
    }
    if (FAILEDx(meta_index_store.init(mode, index_store_param, backup_set_dest, backup_set_info.backup_set_file_,
                                          is_sec_meta, true/*init_sys_tablet_index_store*/,
                                          OB_BACKUP_INDEX_CACHE))) {
      LOG_WARN("failed to init meta index store", KR(ret), K(index_store_param),
                  K(backup_set_dest), K(backup_set_info.backup_set_file_));
    }
  }
  return ret;
}

int ObBackupValidateObUtils::get_rowkey_read_info(
    const ObITable::TableKey &table_key,
    const storage::ObMigrationTabletParam &tablet_param,
    common::ObArenaAllocator &allocator,
    const storage::ObITableReadInfo *&rowkey_read_info)
{
  int ret = OB_SUCCESS;
  rowkey_read_info = nullptr;
  if (!tablet_param.is_valid() || !table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet param", KR(ret), K(tablet_param), K(table_key));
  } else if (table_key.is_normal_cg_sstable()) {
    ObTenantCGReadInfoMgr *cg_read_info_mgr = MTL(ObTenantCGReadInfoMgr *);
    if (OB_ISNULL(cg_read_info_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cg read info mgr should not be null", K(ret), KP(cg_read_info_mgr));
    } else if (OB_FAIL(cg_read_info_mgr->get_index_read_info(rowkey_read_info))) {
      LOG_WARN("failed to get index read info from ObTenantCGReadInfoMgr", KR(ret));
    }
  } else if (table_key.is_mds_sstable()) {
    rowkey_read_info = storage::ObMdsSchemaHelper::get_instance().get_rowkey_read_info();
  } else {
    bool is_cs_replica_compat = false;
    const ObStorageSchema &storage_schema = tablet_param.storage_schema_;
    storage::ObRowkeyReadInfo *tmp_rowkey_read_info = nullptr;
    if (OB_FAIL(ObTablet::build_read_info_by_storage_schema(allocator, storage_schema,
        is_cs_replica_compat, tmp_rowkey_read_info))) {
      LOG_WARN("failed to build read info by storage schema", KR(ret), K(tablet_param));
    } else {
      rowkey_read_info = tmp_rowkey_read_info;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(rowkey_read_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index read info is null", K(ret), K(table_key), K(tablet_param), KP(rowkey_read_info));
  }
  return ret;
}

int ObBackupValidateObUtils::get_sec_meta_iterator(
    const backup::ObBackupSSTableMeta &sstable_meta,
    const storage::ObITableReadInfo &read_info,
    const ObExternBackupSetInfoDesc &backup_set_info,
    backup::ObBackupMetaIndexStoreWrapper &meta_index_store,
    const share::ObBackupDest &backup_dest,
    const common::ObStorageIdMod &mod,
    backup::ObBackupSSTableSecMetaIterator &sec_meta_iterator)
{
  int ret = OB_SUCCESS;
  if (!sstable_meta.is_valid() || !backup_set_info.is_valid() || !read_info.is_valid() || !backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sstable_meta), K(backup_set_info), K(read_info), K(backup_dest));
  } else {
    share::ObBackupSetDesc backup_set_desc;
    backup::ObRestoreMetaIndexStore *meta_index_store_ptr = nullptr;
    const storage::ObITable::TableKey &table_key = sstable_meta.sstable_meta_.table_key_;
    share::ObBackupDataType backup_data_type;

    backup_set_desc.backup_set_id_ = backup_set_info.backup_set_file_.backup_set_id_;
    backup_set_desc.backup_type_ = backup_set_info.backup_set_file_.backup_type_;
    backup_set_desc.min_restore_scn_ = backup_set_info.backup_set_file_.min_restore_scn_;
    backup_set_desc.total_bytes_ = backup_set_info.backup_set_file_.stats_.output_bytes_;

    if (OB_FAIL(oceanbase::storage::ObRestoreUtils::get_backup_data_type(table_key, backup_data_type))) {
      LOG_WARN("failed to get backup data type", K(ret), K(table_key));
    } else if (OB_FAIL(meta_index_store.get_backup_meta_index_store(backup_data_type, meta_index_store_ptr))) {
      LOG_WARN("failed to get backup meta index store", KR(ret));
    } else if (OB_ISNULL(meta_index_store_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("restore meta index store is null", KR(ret));
    } else if (OB_FAIL(sec_meta_iterator.init(sstable_meta.tablet_id_, read_info, table_key,
                                               backup_dest, backup_set_desc, mod, *meta_index_store_ptr))) {
      LOG_WARN("failed to init sstable sec meta iterator", KR(ret), K(sstable_meta), K(read_info),
                  K(backup_dest), K(backup_set_desc), K(mod), K(meta_index_store_ptr));
    }
  }
  return ret;
}

}//storage
}//oceanbase
