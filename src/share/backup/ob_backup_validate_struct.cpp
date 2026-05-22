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

#define USING_LOG_PREFIX SHARE
#include "share/backup/ob_backup_validate_struct.h"
#include "share/backup/ob_backup_util.h"
#include "share/ob_define.h"
#include "lib/oblog/ob_log.h"
using namespace oceanbase;
using namespace share;

/*
*----------------------ObBackupValidateStatus-------------------------
*/
static const char *new_backup_validate_status_str[] = {
  "INIT",
  "DOING",
  "COMPLETED",
  "FAILED",
  "CANCELING",
  "CANCELED",
  "BASIC",
  "PHYSICAL"
};

bool ObBackupValidateStatus::is_valid() const
{
  return status_ >= INIT && status_ < MAX_STATUS;
}

ObBackupValidateStatus &ObBackupValidateStatus::operator=(const Status &status)
{
  status_ = status;
  return *this;
}

const char *ObBackupValidateStatus::get_str() const
{
  const char *str = "UNKNOWN";
  STATIC_ASSERT(MAX_STATUS == ARRAYSIZEOF(new_backup_validate_status_str), "status count mismatch");
  if (status_ < INIT || status_ >= MAX_STATUS) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid backup validate job status", K(status_));
  } else {
    str = new_backup_validate_status_str[status_];
  }
  return str;
}

int ObBackupValidateStatus::set_staust(const char *str)
{
  int ret = OB_SUCCESS;
  ObString s(str);
  bool str_valid = false;
  const int64_t count = ARRAYSIZEOF(new_backup_validate_status_str);
  if (s.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]status can't empty", K(ret));
  } else {
    for (int64_t i = 0; i < count; ++i) {
      if (0 == s.case_compare(new_backup_validate_status_str[i])) {
        status_ = static_cast<Status>(i);
        str_valid = true;
        break;
      }
    }
  }
  if (OB_SUCC(ret) && false == str_valid) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]str is valid", K(ret), K(str));
  }
  return ret;
}

/*
*----------------------ObBackupValidateType-------------------------
*/
OB_SERIALIZE_MEMBER(ObBackupValidateType, type_);

static const char *new_backup_validate_type_str[] = {
  "DATABASE",
  "BACKUPSET",
  "ARCHIVELOG_PIECE"
};

int ObBackupValidateType::set(const char *type_str) {
  int ret = OB_SUCCESS;
  ObString s(type_str);
  bool str_valid = false;
  const int64_t count = ARRAYSIZEOF(new_backup_validate_type_str);
  if (s.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]type can't empty", K(ret));
  } else {
    for (int64_t i = 0; i < count; ++i) {
      if (0 == s.case_compare(new_backup_validate_type_str[i])) {
        type_ = static_cast<ValidateType>(i);
        str_valid = true;
        break;
      }
    }
  }
  if (OB_SUCC(ret) && false == str_valid) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]str is valid", K(ret), K(type_str));
  }
  return ret;
}

bool ObBackupValidateType::need_validate_backup_set() const
{
  return ValidateType::BACKUPSET == type_ || ValidateType::DATABASE == type_;
}

bool ObBackupValidateType::need_validate_archive_piece() const
{
  return ValidateType::ARCHIVELOG_PIECE == type_ || ValidateType::DATABASE == type_;
}

const char *ObBackupValidateType::get_str()  const
{
  const char *str = "UNKNOWN";
  STATIC_ASSERT(MAX_VALIDATION_TYPE == ARRAYSIZEOF(new_backup_validate_type_str), "type count mismatch");
  if (type_ < ValidateType::DATABASE || type_ >= MAX_VALIDATION_TYPE) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "[BACKUP_VALIDATE]invalid backup validate job type", K(type_));
  } else {
    str = new_backup_validate_type_str[type_];
  }
  return str;
}

int ObBackupValidateType::set(const uint64_t type_value)
{
  int ret = OB_SUCCESS;
  if (type_value >= ValidateType::MAX_VALIDATION_TYPE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid type value", K(ret), K(type_value));
  } else {
    type_ = static_cast<ValidateType>(type_value);
  }
  return ret;
}

/*
*----------------------ObBackupValidateLevel-------------------------
*/
OB_SERIALIZE_MEMBER(ObBackupValidateLevel, level_);

static const char *backup_validate_level_strs[] = {
  "basic",
  "physical"
};

int ObBackupValidateLevel::set(const char *level_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(level_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]set validate level get invalid argument", K(ret), KP(level_str));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "set validate level");
  } else {
    ValidateLevel tmp_level = ValidateLevel::MAX_LEVEL;
    for (int64_t i = 0; OB_SUCC(ret) && i < ValidateLevel::MAX_LEVEL; i++) {
      if (0 == STRCASECMP(backup_validate_level_strs[i], level_str)) {
        tmp_level = static_cast<ValidateLevel>(i);
        break;
      }
    }
    if (MAX_LEVEL == tmp_level) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[BACKUP_VALIDATE]invalid validate level", K(ret), K(level_str));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "set validate level");
    } else {
      level_ = tmp_level;
    }
  }
  return ret;
}

const char *ObBackupValidateLevel::get_str() const
{
  const char *str = "UNKNOWN";
  STATIC_ASSERT(MAX_LEVEL == ARRAYSIZEOF(backup_validate_level_strs), "level count mismatch");
  if (level_ < ValidateLevel::BASIC || level_ >= MAX_LEVEL) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid backup validate job level", K(level_));
  } else {
    str = backup_validate_level_strs[level_];
  }
  return str;
}

/*
*----------------------ObBackupValidatePathType-------------------------
*/
OB_SERIALIZE_MEMBER(ObBackupValidatePathType, type_);

static const char *new_backup_validate_path_type_str[] = {
    "BACKUP_DEST",
    "BACKUP_SET_DEST",
    "ARCHIVELOG_DEST",
    "ARCHIVELOG_PIECE_DEST"
  };

int ObBackupValidatePathType::set(const char *type_str)
{
  int ret = OB_SUCCESS;
  ObString s(type_str);
  bool str_valid = false;
  const int64_t count = ARRAYSIZEOF(new_backup_validate_path_type_str);
  if (s.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]type can't empty", K(ret));
  } else {
    for (int64_t i = 0; i < count; ++i) {
      if (0 == s.case_compare(new_backup_validate_path_type_str[i])) {
        type_ = static_cast<ValidatePathType>(i);
        str_valid = true;
        break;
      }
    }
  }
  if (OB_SUCC(ret) && false == str_valid) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]str is valid", K(ret), K(type_str));
  }
  return ret;
}

bool ObBackupValidatePathType::is_backup_validate_type() const
{
  return ValidatePathType::BACKUP_DEST == type_ || ValidatePathType::BACKUP_SET_DEST == type_;
}

bool ObBackupValidatePathType::is_archivelog_validate_type() const
{
  return ValidatePathType::ARCHIVELOG_DEST == type_ || ValidatePathType::ARCHIVELOG_PIECE_DEST == type_;
}

bool ObBackupValidatePathType::is_dest_level_path() const
{
  return ValidatePathType::BACKUP_DEST == type_ || ValidatePathType::ARCHIVELOG_DEST == type_;
}

const char *ObBackupValidatePathType::get_str() const
{
  const char *str = "UNKNOWN";
  STATIC_ASSERT(MAX_PATH_TYPE == ARRAYSIZEOF(new_backup_validate_path_type_str), "path type count mismatch");
  if (type_ < ValidatePathType::BACKUP_DEST || type_ >= MAX_PATH_TYPE) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid backup validate job path type", K(type_));
  } else {
    str = new_backup_validate_path_type_str[type_];
  }
  return str;
}

/*
*----------------------ObBackupValidateJobAttr-------------------------
*/
ObBackupValidateJobAttr::ObBackupValidateJobAttr()
  : job_id_(0),
    tenant_id_(0),
    incarnation_id_(0),
    initiator_tenant_id_(OB_INVALID_TENANT_ID),
    initiator_job_id_(0),
    executor_tenant_ids_(),
    type_(),
    validate_path_(),
    path_type_(),
    level_(),
    backup_set_ids_(),
    logarchive_piece_ids_(),
    start_ts_(0),
    end_ts_(0),
    status_(),
    result_(0),
    can_retry_(true),
    retry_count_(0),
    task_count_(0),
    success_task_count_(0),
    comment_(),
    description_()
{
}

int ObBackupValidateJobAttr::assign(const ObBackupValidateJobAttr &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", K(ret), K(other));
  } else if (OB_FAIL(executor_tenant_ids_.assign(other.executor_tenant_ids_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to assign executor tenant ids", K(ret), K(other.executor_tenant_ids_));
  } else if (OB_FAIL(validate_path_.assign(other.validate_path_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to assign validate path", K(ret), K(other.validate_path_));
  } else if (OB_FAIL(backup_set_ids_.assign(other.backup_set_ids_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to assign backup set ids", K(ret), K(other.backup_set_ids_));
  } else if (OB_FAIL(logarchive_piece_ids_.assign(other.logarchive_piece_ids_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to assign logarchive piece ids", K(ret), K(other.logarchive_piece_ids_));
  } else if (OB_FAIL(description_.assign(other.description_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to assign description", K(ret), K(other.description_));
  } else if (OB_FAIL(comment_.assign(other.comment_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to assign comment", K(ret), K(other.comment_));
  } else {
    job_id_ = other.job_id_;
    tenant_id_ = other.tenant_id_;
    incarnation_id_ = other.incarnation_id_;
    initiator_tenant_id_ = other.initiator_tenant_id_;
    initiator_job_id_ = other.initiator_job_id_;
    type_ = other.type_;
    path_type_ = other.path_type_;
    level_ = other.level_;
    start_ts_ = other.start_ts_;
    end_ts_ = other.end_ts_;
    status_ = other.status_;
    result_ = other.result_;
    can_retry_ = other.can_retry_;
    retry_count_ = other.retry_count_;
    task_count_ = other.task_count_;
    success_task_count_ = other.success_task_count_;
  }
  return ret;
}

int ObBackupValidateJobAttr::get_executor_tenant_id_str(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  char tmp_path[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = { 0 };
  int64_t cur_pos = 0;
  for (int i = 0; OB_SUCC(ret) && i < executor_tenant_ids_.count(); ++i) {
    const uint64_t tenant_id = executor_tenant_ids_.at(i);
    if (0 == i) {
      if (OB_FAIL(databuff_printf(tmp_path, sizeof(tmp_path), cur_pos, "%lu", tenant_id))) {
        LOG_WARN("[BACKUP_VALIDATE]fail to databuff printf tenant id", K(ret));
      }
    } else {
      if (OB_FAIL(databuff_printf(tmp_path, sizeof(tmp_path), cur_pos, ",%lu", tenant_id))) {
        LOG_WARN("[BACKUP_VALIDATE]fail to databuff printf tenant id", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dml.add_column(OB_STR_EXECUTOR_TENANT_ID, tmp_path))) {
    LOG_WARN("[BACKUP_VALIDATE]fail to add column", K(ret));
  }
  return ret;
}

int ObBackupValidateJobAttr::get_validate_ids_str(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  char tmp_path[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = { 0 };
  int64_t cur_pos = 0;
  const ObSArray<uint64_t> *validate_ids = nullptr;
  if (!backup_set_ids_.empty()) {
    validate_ids = &backup_set_ids_;
  } else if (!logarchive_piece_ids_.empty()) {
    validate_ids = &logarchive_piece_ids_;
  }
  if (OB_ISNULL(validate_ids)) {
    // don't need to add validate ids column
  } else {
    for (int i = 0; OB_SUCC(ret) && i < validate_ids->count(); ++i) {
      const uint64_t validate_id = validate_ids->at(i);
      if (0 == i) {
        if (OB_FAIL(databuff_printf(tmp_path, sizeof(tmp_path), cur_pos, "%lu", validate_id))) {
          LOG_WARN("[BACKUP_VALIDATE]fail to databuff printf validate id", K(ret), K(validate_id), K(cur_pos));
        }
      } else {
        if (OB_FAIL(databuff_printf(tmp_path, sizeof(tmp_path), cur_pos, ",%lu", validate_id))) {
          LOG_WARN("[BACKUP_VALIDATE]fail to databuff printf validate id", K(ret), K(validate_id), K(cur_pos));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dml.add_column(OB_STR_ID, tmp_path))) {
    LOG_WARN("[BACKUP_VALIDATE]fail to add column", K(ret));
  }
  return ret;
}

bool ObBackupValidateJobAttr::is_tmplate_valid() const
{
  return tenant_id_ != OB_INVALID_TENANT_ID && initiator_tenant_id_ != OB_INVALID_TENANT_ID;
}

bool ObBackupValidateJobAttr::is_valid() const
{
  return tenant_id_ != OB_INVALID_TENANT_ID
  && initiator_tenant_id_ != OB_INVALID_TENANT_ID
  && type_.is_valid()
  && level_.is_valid()
  && incarnation_id_ > 0
  && start_ts_ >= 0
  && status_.is_valid();
}

int ObBackupValidateJobAttr::set_path_type(const ObBackupValidatePathType &path_type)
{
  int ret = OB_SUCCESS;
  bool path_and_validate_type_consistent =true;
  bool is_backup_dest_or_backup_set_dest = false;
  if (!is_valid() || !path_type.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]backup validate job attr is not valid", K(ret), K(path_type), K(*this));
  } else if (path_type.is_backup_validate_type() && !type_.is_backupset()) {
    path_and_validate_type_consistent = false;
  } else if (path_type.is_archivelog_validate_type() && !type_.is_archivelog()) {
    path_and_validate_type_consistent = false;
  } else if (!path_type.is_dest_level_path() && (!backup_set_ids_.empty() || !logarchive_piece_ids_.empty())) {
    path_and_validate_type_consistent = false;
  }

  if (OB_FAIL(ret)) {
  } else if (!path_and_validate_type_consistent) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]The specified path type and validate type are inconsistent.",
                KR(ret), K(type_), K(path_type));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "validate: the specified path type and validate type are inconsistent.");
  } else {
    path_type_.type_ = path_type.type_;
  }

  return ret;
}

bool ObBackupValidateJobAttr::need_set_backup_dest() const
{
  return validate_path_.is_empty() && type_.need_validate_backup_set();
}

bool ObBackupValidateJobAttr::need_set_archive_dest() const
{
  return validate_path_.is_empty() && type_.need_validate_archive_piece();
}

int ObBackupValidateJobAttr::set_executor_tenant_id(const char *executor_tenant_id_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(executor_tenant_id_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]executor_tenant_id_str is null", K(ret), K(executor_tenant_id_str));
  } else if (OB_FAIL(ObBackupUtil::parse_str_to_array(executor_tenant_id_str, executor_tenant_ids_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to parse str to array", K(ret), K(executor_tenant_id_str));
  }
  return ret;
}

int ObBackupValidateJobAttr::set_validate_ids(const char *set_validate_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(set_validate_ids)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]set_validate_ids is null", K(ret), K(set_validate_ids));
  } else if (type_.is_backupset()) {
    if (OB_FAIL(ObBackupUtil::parse_str_to_array(set_validate_ids, backup_set_ids_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to parse str to array", K(ret), K(set_validate_ids));
    }
  } else if (type_.is_archivelog()) {
    if (OB_FAIL(ObBackupUtil::parse_str_to_array(set_validate_ids, logarchive_piece_ids_))) {
      LOG_WARN("[BACKUP_VALIDATE]failed to parse str to array", K(ret), K(set_validate_ids));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", K(ret), K(set_validate_ids));
  }

  return ret;
}

/*
*----------------------ObBackupValidateTaskAttr-------------------------
*/
ObBackupValidateTaskAttr::ObBackupValidateTaskAttr()
  : task_id_(0),
    tenant_id_(0),
    incarnation_id_(0),
    job_id_(0),
    type_(),
    validate_path_(),
    path_type_(),
    plus_archivelog_(false),
    initiator_task_id_(0),
    validate_id_(0),
    validate_level_(),
    round_id_(0),
    start_ts_(0),
    end_ts_(0),
    status_(),
    result_(0),
    can_retry_(true),
    retry_count_(0),
    total_ls_count_(0),
    finish_ls_count_(0),
    total_bytes_(0),
    validate_bytes_(0),
    comment_()
{
}

void ObBackupValidateTaskAttr::reset()
{
  task_id_ = 0;
  tenant_id_ = 0;
  incarnation_id_ = 0;
  job_id_ = 0;
  type_.reset();
  validate_path_.reset();
  path_type_.reset();
  plus_archivelog_ = false;
  initiator_task_id_ = 0;
  validate_id_ = 0;
  validate_level_.reset();
  round_id_ = 0;
  start_ts_ = 0;
  end_ts_ = 0;
  status_.reset();
  result_ = 0;
  can_retry_ = true;
  retry_count_ = 0;
  total_ls_count_ = 0;
  finish_ls_count_ = 0;
  total_bytes_ = 0;
  validate_bytes_ = 0;
  comment_.reset();
}

bool ObBackupValidateTaskAttr::is_valid() const
{
  return task_id_ > 0
  && tenant_id_ != OB_INVALID_TENANT_ID
  && job_id_ > 0
  && type_.is_valid()
  && validate_level_.is_valid()
  && status_.is_valid()
  && !validate_path_.is_empty()
  && validate_id_ > 0;
}

int ObBackupValidateTaskAttr::assign(const ObBackupValidateTaskAttr &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", K(ret), K(other));
  } else if (OB_FAIL(validate_path_.assign(other.validate_path_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to assign validate path", K(ret), K(other.validate_path_));
  } else if (OB_FAIL(comment_.assign(other.comment_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to assign comment", K(ret), K(other.comment_));
  } else {
    task_id_ = other.task_id_;
    tenant_id_ = other.tenant_id_;
    incarnation_id_ = other.incarnation_id_;
    job_id_ = other.job_id_;
    type_ = other.type_;
    path_type_ = other.path_type_.type_;
    plus_archivelog_ = other.plus_archivelog_;
    initiator_task_id_ = other.initiator_task_id_;
    validate_id_ = other.validate_id_;
    validate_level_ = other.validate_level_.level_;
    round_id_ = other.round_id_;
    start_ts_ = other.start_ts_;
    end_ts_ = other.end_ts_;
    status_ = other.status_;
    result_ = other.result_;
    can_retry_ = other.can_retry_;
    retry_count_ = other.retry_count_;
    total_ls_count_ = other.total_ls_count_;
    finish_ls_count_ = other.finish_ls_count_;
    total_bytes_ = other.total_bytes_;
    validate_bytes_ = other.validate_bytes_;
    dest_id_ = other.dest_id_;
  }
  return ret;
}

const char *ObBackupValidateTaskAttr::get_plus_archivelog_str() const
{
  const char *str = nullptr;
  if (plus_archivelog_) {
    str = "ON";
  } else {
    str = "OFF";
  }
  return str;
}

/*
*----------------------ObBackupValidateStats-------------------------
*/
void ObBackupValidateStats::reset()
{
  validated_bytes_ = 0;
  total_object_count_ = 0;
  finish_object_count_ = 0;
}

/*
*----------------------ObBackupValidateLSTaskAttr-------------------------
*/
ObBackupValidateLSTaskAttr::ObBackupValidateLSTaskAttr()
  : task_id_(0),
    tenant_id_(OB_INVALID_TENANT_ID),
    ls_id_(0),
    job_id_(0),
    validate_id_(0),
    round_id_(0),
    task_type_(),
    status_(),
    start_ts_(0),
    end_ts_(0),
    dst_(),
    result_(OB_SUCCESS),
    retry_id_(0),
    stats_(),
    validate_path_(),
    dest_id_(OB_INVALID_DEST_ID),
    validate_level_(),
    total_object_count_(0),
    finish_object_count_(0),
    validated_bytes_(0),
    comment_()
{
}

void ObBackupValidateLSTaskAttr::reset()
{
  task_id_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  job_id_ = 0;
  validate_id_ = 0;
  round_id_ = 0;
  task_type_.reset();
  status_.status_ = ObBackupTaskStatus::Status::INIT;
  start_ts_ = 0;
  end_ts_ = 0;
  dst_.reset();
  result_ = OB_SUCCESS;
  retry_id_ = 0;
  stats_.reset();
  validate_path_.reset();
  dest_id_ = OB_INVALID_DEST_ID;
  validate_level_.reset();
  total_object_count_ = 0;
  finish_object_count_ = 0;
  validated_bytes_ = 0;
  comment_.reset();
}

int ObBackupValidateLSTaskAttr::assign(const ObBackupValidateLSTaskAttr &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]invalid argument", K(ret), K(other));
  } else if (OB_FAIL(validate_path_.assign(other.validate_path_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to assign validate path", K(ret), K(other.validate_path_));
  } else if (OB_FAIL(comment_.assign(other.comment_))) {
    LOG_WARN("[BACKUP_VALIDATE]failed to assign comment", K(ret), K(other.comment_));
  } else {
    task_id_ = other.task_id_;
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    job_id_ = other.job_id_;
    validate_id_ = other.validate_id_;
    round_id_ = other.round_id_;
    task_type_ = other.task_type_.type_;
    status_.status_ = other.status_.status_;
    start_ts_ = other.start_ts_;
    end_ts_ = other.end_ts_;
    dst_ = other.dst_;
    task_trace_id_ = other.task_trace_id_;
    result_ = other.result_;
    retry_id_ = other.retry_id_;
    stats_ = other.stats_;
    dest_id_ = other.dest_id_;
    validate_level_ = other.validate_level_;
    total_object_count_ = other.total_object_count_;
    finish_object_count_ = other.finish_object_count_;
    validated_bytes_ = other.validated_bytes_;
  }
  return ret;
}

bool ObBackupValidateLSTaskAttr::is_valid() const
{
  bool is_valid = true;
  if (tenant_id_ == OB_INVALID_TENANT_ID || task_id_ <= 0 || !ls_id_.is_valid()) {
    is_valid = false;
  } else if (!status_.is_valid()) {
    is_valid = false;
  } else {
    is_valid = task_type_.is_valid()
        && job_id_ >= 0
        && validate_level_.is_valid()
        && !validate_path_.is_empty()
        && ((is_validate_backup_set_task() && validate_id_ > 0)
          || (is_validate_archivelog_piece_task() && validate_id_ > 0));
  }
  return is_valid;
}

int ObBackupValidateLSTaskAttr::get_validate_id(int64_t &id) const
{
  int ret = OB_SUCCESS;
  id = 0;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]backup validate ls attr is invalid", K(*this));
  } else if (is_validate_backup_set_task() || is_validate_archivelog_piece_task()) {
    id = validate_id_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]can not get validate parameter", K(ret), K(*this));
  }
  return ret;
}

int ObBackupValidateLSTaskAttr::set_validate_id(const int64_t id)
{
  int ret = OB_SUCCESS;
  if (id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_VALIDATE]set validate parameter get invalid argument", K(ret), K(id));
  } else if (is_validate_backup_set_task() || is_validate_archivelog_piece_task()) {
    validate_id_ = id;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_VALIDATE]backup validate ls attr is invalid, can not set parameter", K(ret), K(*this), K(id));
  }
  return ret;
}
