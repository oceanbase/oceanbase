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

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#define USING_LOG_PREFIX SHARE
#include "share/backup/ob_backup_clean_struct.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/hash_func/murmur_hash.h"
using namespace oceanbase;
using namespace share;

static const char *new_backup_clean_status_strs[] = {
  "INIT",
  "DOING",
  "COMPLETED",
  "FAILED",
  "CANCELING",
  "CANCELED",
};

bool ObBackupCleanStatus::is_valid() const
{
  return status_ >= INIT && status_ < MAX_STATUS;
}

const char* ObBackupCleanStatus::get_str() const
{
  const char *str = "UNKNOWN";

  STATIC_ASSERT(MAX_STATUS == ARRAYSIZEOF(new_backup_clean_status_strs), "status count mismatch");
  if (status_ < INIT || status_ >= MAX_STATUS) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid backup clean job status", K(status_));
  } else {
    str = new_backup_clean_status_strs[status_];
  }
  return str;
}

int ObBackupCleanStatus::set_status(const char *str)
{
  int ret = OB_SUCCESS;
  ObString s(str);
  bool str_valid = false;
  const int64_t count = ARRAYSIZEOF(new_backup_clean_status_strs);
  if (s.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("status can't empty", K(ret));
  } else {
    for (int64_t i = 0; i < count; ++i) {
      if (0 == s.case_compare(new_backup_clean_status_strs[i])) {
        status_ = static_cast<Status>(i);
        str_valid = true;
        break;
      }
    }
  }
  if (OB_SUCC(ret) && false == str_valid) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("str is valid", K(ret), K(str));
  }

  return ret;
}

static const char *new_backup_clean_type_str[] = {
    "EMPTY",
    "DELETE BACKUP SET",
    "DELETE BACKUP PIECE",
    "DELETE BACKUP ALL",
    "DELETE OBSOLETE BACKUP",
    "DELETE OBSOLETE BACKUP BACKUP",
    "CANCEL DELETE"
};

const char *ObNewBackupCleanType::get_str(const TYPE &type)
{
  const char *str = nullptr;

  if (type < EMPTY_TYPE || type >= MAX) {
    str = "UNKOWN";
  } else {
    str = new_backup_clean_type_str[type];
  }
  return str;
}

ObNewBackupCleanType::TYPE ObNewBackupCleanType::get_type(const char *type_str)
{
  ObNewBackupCleanType::TYPE type = ObNewBackupCleanType::MAX;

  const int64_t count = ARRAYSIZEOF(new_backup_clean_type_str);
  STATIC_ASSERT(static_cast<int64_t>(ObNewBackupCleanType::MAX) == count, "status count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(type_str, new_backup_clean_type_str[i])) {
      type = static_cast<ObNewBackupCleanType::TYPE>(i);
      break;
    }
  }
  return type;
}

static const char *backup_clean_task_type_str[] = {
    "BACKUP SET",
    "BACKUP PIECE",
};

const char *ObBackupCleanTaskType::get_str(const TYPE &type)
{
  const char *str = nullptr;

  if (type < 0 || type >= MAX) {
    str = "UNKOWN";
  } else {
    str = backup_clean_task_type_str[type];
  }
  return str;
}

ObBackupCleanTaskType::TYPE ObBackupCleanTaskType::get_type(const char *type_str)
{
  ObBackupCleanTaskType::TYPE type = ObBackupCleanTaskType::MAX;
  const int64_t count = ARRAYSIZEOF(backup_clean_task_type_str);
  STATIC_ASSERT(static_cast<int64_t>(ObBackupCleanTaskType::MAX) == count, "type count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(type_str, backup_clean_task_type_str[i])) {
      type = static_cast<ObBackupCleanTaskType::TYPE>(i);
      break;
    }
  }
  return type;
}

ObBackupCleanJobAttr::ObBackupCleanJobAttr()
  : job_id_(0), 
    tenant_id_(OB_INVALID_TENANT_ID),
    incarnation_id_(0),
    initiator_tenant_id_(OB_INVALID_TENANT_ID),
    initiator_job_id_(0),
    executor_tenant_id_(),
    clean_type_(ObNewBackupCleanType::MAX),
    expired_time_(0),
    backup_set_id_(0),
    backup_piece_id_(0),
    dest_id_(0),
    job_level_(),
    backup_path_(),
    start_ts_(0),
    end_ts_(0),   
    status_(),
    description_(),
    result_(OB_SUCCESS),
    retry_count_(0),
    can_retry_(true),
    task_count_(0),
    success_task_count_(0)
{
}

void ObBackupCleanJobAttr::reset()
{
  job_id_ = 0; 
  tenant_id_ = OB_INVALID_TENANT_ID;
  incarnation_id_ = 0;
  initiator_tenant_id_ = OB_INVALID_TENANT_ID;
  initiator_job_id_ = 0;
  executor_tenant_id_.reset(); 
  clean_type_ = ObNewBackupCleanType::MAX;
  expired_time_ = 0;
  backup_set_id_ = 0;
  backup_piece_id_ = 0;
  dest_id_ = 0;
  backup_path_.reset();
  start_ts_ = 0;
  end_ts_ = 0;
  description_.reset();
  result_ = OB_SUCCESS;
  retry_count_ = 0;
  can_retry_ = true;
  task_count_ = 0;
  success_task_count_ = 0;
}

int ObBackupCleanJobAttr::assign(const ObBackupCleanJobAttr &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else if (OB_FAIL(backup_path_.assign(other.backup_path_))) {
    LOG_WARN("failed to assign backup dest", K(ret), K(other.backup_path_));
  } else if (OB_FAIL(description_.assign(other.description_))) {
    LOG_WARN("failed to assign description", K(ret), K(other.description_));
  } else if (OB_FAIL(executor_tenant_id_.assign(other.executor_tenant_id_))) {
    LOG_WARN("failed to assign executor tenant id", K(ret), K(other.executor_tenant_id_)); 
  } else {
    job_id_ = other.job_id_;
    tenant_id_ = other.tenant_id_;
    incarnation_id_ = other.incarnation_id_;
    initiator_tenant_id_ = other.initiator_tenant_id_;
    initiator_job_id_ = other.initiator_job_id_;
    clean_type_ = other.clean_type_;
    expired_time_ = other.expired_time_;
    backup_set_id_ = other.backup_set_id_;
    backup_piece_id_ = other.backup_piece_id_;
    dest_id_ = other.dest_id_;
    job_level_.level_ = other.job_level_.level_;
    start_ts_ = other.start_ts_;
    end_ts_ = other.end_ts_;
    status_.status_ = other.status_.status_;
    result_ = other.result_;
    retry_count_ = other.retry_count_;
    can_retry_ = other.can_retry_;
    task_count_ = other.task_count_;
    success_task_count_ = other.success_task_count_;
  }
  return ret;
}

bool ObBackupCleanJobAttr::is_tmplate_valid() const
{
  return tenant_id_ != OB_INVALID_TENANT_ID && initiator_tenant_id_ != OB_INVALID_TENANT_ID;
}

bool ObBackupCleanJobAttr::is_valid() const
{
  bool is_valid = true;
  if (tenant_id_ == OB_INVALID_TENANT_ID || job_id_ < 0 
      || incarnation_id_ <= 0 || initiator_tenant_id_ == OB_INVALID_TENANT_ID || start_ts_ <= 0) {
    is_valid = false;
  } else if (!status_.is_valid()) {
    is_valid = false;
  } else {
    is_valid = ObNewBackupCleanType::is_valid(clean_type_)
        && ((is_delete_obsolete_backup() && expired_time_ > 0)
            || (is_delete_backup_set() && backup_set_id_ > 0)
            || (is_delete_backup_piece() && backup_piece_id_ > 0));
  }
  return is_valid;
}

int ObBackupCleanJobAttr::get_clean_parameter(int64_t &parameter) const
{
  int ret = OB_SUCCESS;
  parameter = 0;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup clean is invalid", K(*this));
  } else if (is_delete_obsolete_backup()) {
    parameter = expired_time_;
  } else if (is_delete_backup_set()) {
    parameter = backup_set_id_;
  } else if (is_delete_backup_piece()) {
    parameter = backup_piece_id_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not get clean paramater", K(ret), K(*this));
  }
  return ret;
}

int ObBackupCleanJobAttr::set_clean_parameter(const int64_t parameter)
{
  int ret = OB_SUCCESS;
  if (parameter < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set clean parameter get invalid argument", K(ret), K(parameter));
  } else if (is_delete_obsolete_backup()) {
    expired_time_ = parameter;
  } else if (is_delete_backup_set()) {
    backup_set_id_ = parameter;
  } else if (is_delete_backup_piece()) {
    backup_piece_id_ = parameter;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup clean info is invalid, can not set parameter", K(ret), K(*this), K(parameter));
  }

  return ret;
}

int ObBackupCleanJobAttr::set_dest_id(const int64_t dest_id)
{
  int ret = OB_SUCCESS;
  if (dest_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set copy id get invalid argument", KR(ret), K(dest_id));
  } else {
    dest_id_ = dest_id;
  }
  return ret;
}

int ObBackupCleanJobAttr::check_backup_clean_job_match(
    const ObBackupCleanJobAttr &job_attr) const
{
  int ret = OB_SUCCESS;
  if (!job_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("backup info struct is invalid", K(ret), K(job_attr));
  } else if (tenant_id_ != job_attr.tenant_id_
      || job_id_ != job_attr.job_id_
      || incarnation_id_ != job_attr.incarnation_id_
      || clean_type_ != job_attr.clean_type_
      || status_.status_ != job_attr.status_.status_) {
    ret = OB_BACKUP_CLEAN_INFO_NOT_MATCH;
    LOG_WARN("backup clean info is not match", K(ret), K(*this), K(job_attr));
  }
  return ret;
}


int ObBackupCleanJobAttr::get_executor_tenant_id_str(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  char tmp_path[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = { 0 };
  MEMSET(tmp_path, '\0', sizeof(tmp_path));
  int64_t cur_pos = 0;
  for (int i = 0; OB_SUCC(ret) && i < executor_tenant_id_.count(); ++i) {
    const uint64_t tenant_id = executor_tenant_id_.at(i);
    if (0 == i) {
      if (OB_FAIL(databuff_printf(tmp_path, sizeof(tmp_path), cur_pos, "%lu", tenant_id))) {
        LOG_WARN("fail to databuff printf tenant id", K(ret));
      } 
    } else {
      if (OB_FAIL(databuff_printf(tmp_path, sizeof(tmp_path), cur_pos, ",%lu", tenant_id))) {
        LOG_WARN("fail to databuff printf tenant id", K(ret));
      } 
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dml.add_column(OB_STR_EXECUTOR_TENANT_ID, tmp_path))) {
    LOG_WARN("fail to add column", K(ret));
  }
  return ret;
}


int ObBackupCleanJobAttr::set_executor_tenant_id(const ObString &str)
{
  int ret = OB_SUCCESS;
  char tmp_str[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = { 0 };
  if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invlaid argument", K(ret));
  } else if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%s", str.ptr()))) {
    LOG_WARN("failed to set dir name", K(ret), K(str));
  } else {
    char *token = nullptr;
    char *saveptr = nullptr;
    char *p_end = nullptr;
    token = ::STRTOK_R(tmp_str, ",", &saveptr);
    while (OB_SUCC(ret) && nullptr != token) {
      uint64_t tenant_id;
      if (OB_FAIL(ob_strtoull(token, p_end, tenant_id))) {
        LOG_WARN("failed to get value from string", K(ret), K(*token));
      } else if ('\0' == *p_end) {
        if (OB_FAIL(executor_tenant_id_.push_back(tenant_id))) {
          LOG_WARN("fail to push back tenant id", K(ret), K(tenant_id));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("set tenant id error", K(*token));
      }
      token = STRTOK_R(nullptr, ",", &saveptr);
      p_end = nullptr;
    }
  }
  return ret;
}


ObBackupCleanTaskAttr::ObBackupCleanTaskAttr()
  : task_id_(0), 
    tenant_id_(OB_INVALID_TENANT_ID),
    incarnation_id_(0),
    task_type_(ObBackupCleanTaskType::MAX),
    job_id_(0),
    backup_set_id_(0),
    backup_piece_id_(0),
    round_id_(0),
    dest_id_(0),
    start_ts_(0),
    end_ts_(0),   
    status_(),
    backup_path_(),
    result_(OB_SUCCESS),
    total_ls_count_(0),
    finish_ls_count_(0),
    stats_()
{
}

void ObBackupCleanTaskAttr::reset()
{
  task_id_ = 0; 
  tenant_id_ = OB_INVALID_TENANT_ID;
  incarnation_id_ = 0;
  job_id_ = 0;
  task_type_ = ObBackupCleanTaskType::MAX;
  backup_set_id_ = 0;
  backup_piece_id_ = 0;
  round_id_ = 0;
  dest_id_ = 0;
  start_ts_ = 0;
  end_ts_ = 0;
  backup_path_.reset();
  result_ = OB_SUCCESS;
  total_ls_count_ = 0,
  finish_ls_count_ = 0,
  stats_.reset();
}

bool ObBackupCleanTaskAttr::is_valid() const
{
  bool is_valid = true;
  if (tenant_id_ == OB_INVALID_TENANT_ID || task_id_ <= 0 || incarnation_id_ <= 0) {
    is_valid = false;
  } else if (!status_.is_valid()) {
    is_valid = false;
  } else if (0 == backup_path_.size()) {
    is_valid = false;
  } else {
    is_valid = ObBackupCleanTaskType::is_valid(task_type_)
        && job_id_ >= 0
        && ((is_delete_backup_set_task() && backup_set_id_ > 0)
          || (is_delete_backup_piece_task() && backup_piece_id_ > 0));
  }
  return is_valid; 
}

int ObBackupCleanTaskAttr::assign(const ObBackupCleanTaskAttr &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else if (OB_FAIL(backup_path_.assign(other.backup_path_.ptr()))) {
    LOG_WARN("failed to assign passwd", K(ret));
  } else {
    task_id_ = other.task_id_;
    tenant_id_ = other.tenant_id_;
    incarnation_id_ = other.incarnation_id_;
    task_type_ = other.task_type_;
    job_id_ = other.job_id_;
    backup_set_id_ = other.backup_set_id_;
    backup_piece_id_ = other.backup_piece_id_;
    round_id_ = other.round_id_;
    dest_id_ = other.dest_id_;
    start_ts_ = other.start_ts_;
    end_ts_ = other.end_ts_;
    status_.status_ = other.status_.status_;
    result_ = other.result_;
    total_ls_count_ = other.total_ls_count_;
    finish_ls_count_ = other.finish_ls_count_;
    stats_ = other.stats_;
  }
  return ret;
}

int ObBackupCleanTaskAttr::get_backup_clean_id(int64_t &id) const 
{
  int ret = OB_SUCCESS;
  id = 0;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup clean info is invalid", K(*this));
  } else if (is_delete_backup_set_task()) {
    id = backup_set_id_;
  } else if (is_delete_backup_piece_task()) {
    id = backup_piece_id_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not get clean paramater", K(ret), K(*this));
  }
  return ret; 
}

int ObBackupCleanTaskAttr::set_backup_clean_id(const int64_t id)
{
  int ret = OB_SUCCESS;
  if (id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set clean parameter get invalid argument", K(ret), K(id));
  } else if (is_delete_backup_set_task()) {
    backup_set_id_ = id;
  } else if (is_delete_backup_piece_task()) {
    backup_piece_id_ = id;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup clean info is invalid, can not set parameter", K(ret), K(*this), K(id));
  }
  return ret;
}

ObBackupCleanLSTaskAttr::ObBackupCleanLSTaskAttr()
  : task_id_(0), 
    tenant_id_(OB_INVALID_TENANT_ID),
    ls_id_(0),
    job_id_(0),
    backup_set_id_(0),
    backup_piece_id_(0),
    round_id_(0),
    task_type_(ObBackupCleanTaskType::MAX),
    status_(),
    start_ts_(0),
    end_ts_(0),
    dst_(), 
    result_(OB_SUCCESS),
    retry_id_(0),
    stats_()
{
}

void ObBackupCleanLSTaskAttr::reset()
{
  task_id_ = 0; 
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  job_id_ = 0;
  backup_set_id_ = 0;
  backup_piece_id_ = 0;
  round_id_ = 0;
  task_type_ = ObBackupCleanTaskType::MAX;
  status_.status_  = ObBackupTaskStatus::MAX_STATUS;
  start_ts_ = 0;
  end_ts_ = 0;
  dst_.reset();
  result_ = OB_SUCCESS;
  retry_id_ = 0;
  stats_.reset();
}

int ObBackupCleanLSTaskAttr::assign(const ObBackupCleanLSTaskAttr &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else {
    task_id_ = other.task_id_;
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    job_id_ = other.job_id_;
    backup_set_id_ = other.backup_set_id_;
    backup_piece_id_ = other.backup_piece_id_;
    task_type_ = other.task_type_;
    status_.status_ = other.status_.status_;
    start_ts_ = other.start_ts_;
    end_ts_ = other.end_ts_;
    round_id_ = other.round_id_;
    dst_ = other.dst_;
    task_trace_id_ = other.task_trace_id_;
    result_ = other.result_;
    retry_id_ = other.retry_id_;
    stats_ = other.stats_;
  }
  return ret;
}

bool ObBackupCleanLSTaskAttr::is_valid() const
{
  bool is_valid = true;
  if (tenant_id_ == OB_INVALID_TENANT_ID || task_id_ <= 0 || !ls_id_.is_valid()) {
    is_valid = false;
  } else if (!status_.is_valid()) {
    is_valid = false;
  } else {
    is_valid = ObBackupCleanTaskType::is_valid(task_type_)
        && job_id_ >= 0
        && ((is_delete_backup_set_task() && backup_set_id_ > 0)
          || (is_delete_backup_piece_task() && backup_piece_id_ > 0));
  }
  return is_valid; 
}

void ObBackupCleanStats::reset()
{
  total_bytes_ = 0;
  delete_bytes_ = 0;
  total_files_count_ = 0;
  delete_files_count_ = 0; 
}

ObDeletePolicyAttr::ObDeletePolicyAttr()
  : tenant_id_(OB_INVALID_TENANT_ID),
    policy_name_(),
    recovery_window_(),
    redundancy_(0),
    backup_copies_(0)
{
}

void ObDeletePolicyAttr::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  policy_name_[0] = '\0';
  recovery_window_[0] = '\0';
  redundancy_ = 0;
  backup_copies_ = 0;
}

int ObDeletePolicyAttr::assign(const ObDeletePolicyAttr &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(policy_name_, sizeof(policy_name_), "%s", other.policy_name_))) {
    LOG_WARN("failed to set policy name", K(ret), K(other.policy_name_));
  } else if (OB_FAIL(databuff_printf(recovery_window_, sizeof(recovery_window_), "%s", other.recovery_window_))) {
    LOG_WARN("failed to set recovery window", K(ret), K(other.recovery_window_));  
  } else {
    tenant_id_ = other.tenant_id_;
    redundancy_ = other.redundancy_;
    backup_copies_ = other.backup_copies_;
  }
  return ret;
}

bool ObDeletePolicyAttr::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_ && 0 != strlen(policy_name_);
}
