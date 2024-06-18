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
#include "share/restore/ob_tenant_clone_table_operator.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

static const char* cancel_clone_job_reason_strs[] = {
  "by user command",
  "by standby tenant transfer event",
  "by standby tenant upgrade event",
  "by standby tenant alter LS event"
};

const char* ObCancelCloneJobReason::get_reason_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(cancel_clone_job_reason_strs) == (int64_t)MAX,
                "cancel_clone_job_reason_strs string array size mismatch enum CancelCloneJobReason count");
  const char *str = NULL;
  if (reason_ > INVALID && reason_ < MAX) {
    str = cancel_clone_job_reason_strs[static_cast<int64_t>(reason_)];
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid CancelCloneJobReason", K_(reason));
  }
  return str;
}

int ObCancelCloneJobReason::init_by_conflict_case(
    const rootserver::ObConflictCaseWithClone &case_to_check)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!case_to_check.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(case_to_check));
  } else if (OB_UNLIKELY(!case_to_check.is_standby_related())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("can not init cancel clone job reason by these cases", KR(ret), K(case_to_check));
  } else {
    switch (case_to_check.get_case_name()) {
      case rootserver::ObConflictCaseWithClone::STANDBY_UPGRADE : {
        reason_ = CancelCloneJobReason::CANCEL_BY_STANDBY_UPGRADE;
        break;
      }
      case rootserver::ObConflictCaseWithClone::ConflictCaseWithClone::STANDBY_TRANSFER : {
        reason_ = CancelCloneJobReason::CANCEL_BY_STANDBY_TRANSFER;
        break;
      }
      case rootserver::ObConflictCaseWithClone::ConflictCaseWithClone::STANDBY_MODIFY_LS : {
        reason_ = CancelCloneJobReason::CANCEL_BY_STANDBY_ALTER_LS;
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid conflict case", K(ret), K(case_to_check));
    }
  }
  return ret;
}

ObTenantCloneStatus &ObTenantCloneStatus::operator=(const ObTenantCloneStatus &status)
{
  status_ = status.status_;
  return *this;
}

ObTenantCloneStatus &ObTenantCloneStatus::operator=(const Status &status)
{
  status_ = status;
  return *this;
}

const ObTenantCloneStatus::TenantCloneStatusStrPair ObTenantCloneStatus::TENANT_CLONE_STATUS_ARRAY[] = {
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_SYS_LOCK,
                           "CLONE_SYS_LOCK"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_SYS_CREATE_INNER_RESOURCE_POOL,
                           "CLONE_SYS_CREATE_INNER_RESOURCE_POOL"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_SYS_CREATE_SNAPSHOT,
                           "CLONE_SYS_CREATE_SNAPSHOT"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_SYS_WAIT_CREATE_SNAPSHOT,
                           "CLONE_SYS_WAIT_CREATE_SNAPSHOT"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_SYS_CREATE_TENANT,
                           "CLONE_SYS_CREATE_TENANT"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_SYS_WAIT_TENANT_RESTORE_FINISH,
                           "CLONE_SYS_WAIT_TENANT_RESTORE_FINISH"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_SYS_RELEASE_RESOURCE,
                           "CLONE_SYS_RELEASE_RESOURCE"),

  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_USER_PREPARE, "CLONE_USER_PREPARE"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_USER_CREATE_INIT_LS, "CLONE_USER_CREATE_INIT_LS"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_USER_WAIT_LS, "CLONE_USER_WAIT_LS"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_USER_POST_CHECK, "CLONE_USER_POST_CHECK"),

  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_USER_SUCCESS, "CLONE_USER_SUCCESS"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_USER_FAIL, "CLONE_USER_FAIL"),

  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_SYS_SUCCESS, "CLONE_SYS_SUCCESS"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_SYS_LOCK_FAIL, "CLONE_SYS_LOCK_FAIL"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_SYS_CREATE_INNER_RESOURCE_POOL_FAIL,
                           "CLONE_SYS_CREATE_INNER_RESOURCE_POOL_FAIL"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_SYS_CREATE_SNAPSHOT_FAIL,
                           "CLONE_SYS_CREATE_SNAPSHOT_FAIL"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_SYS_WAIT_CREATE_SNAPSHOT_FAIL,
                           "CLONE_SYS_WAIT_CREATE_SNAPSHOT_FAIL"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_SYS_CREATE_TENANT_FAIL,
                           "CLONE_SYS_CREATE_TENANT_FAIL"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_SYS_WAIT_TENANT_RESTORE_FINISH_FAIL,
                           "CLONE_SYS_WAIT_TENANT_RESTORE_FINISH_FAIL"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_SYS_RELEASE_RESOURCE_FAIL,
                           "CLONE_SYS_RELEASE_RESOURCE_FAIL"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_SYS_CANCELING,
                           "CLONE_SYS_CANCELING"),
  TenantCloneStatusStrPair(ObTenantCloneStatus::Status::CLONE_SYS_CANCELED,
                           "CLONE_SYS_CANCELED"),
};

const char *ObTenantCloneStatus::get_clone_status_str(const Status &status)
{
  const char* str = "CLONE_MAX_STATUS";
  bool find = false;

  for (int64_t i = 0; !find && i < ARRAYSIZEOF(TENANT_CLONE_STATUS_ARRAY); i++) {
    if (status == TENANT_CLONE_STATUS_ARRAY[i].status_) {
      find = true;
      if (OB_NOT_NULL(TENANT_CLONE_STATUS_ARRAY[i].str_)) {
        str = TENANT_CLONE_STATUS_ARRAY[i].str_;
      } else {
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected null");
      }
    }
  }

  if (!find) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid clone status", K(status));
  }
  return str;
}

ObTenantCloneStatus ObTenantCloneStatus::get_clone_status(const ObString &status_str)
{
  ObTenantCloneStatus::Status ret_status = ObTenantCloneStatus::Status::CLONE_MAX_STATUS;
  bool find = false;

  if (!status_str.empty()) {
    for (int64_t i = 0; !find && i < ARRAYSIZEOF(TENANT_CLONE_STATUS_ARRAY); i++) {
      if (0 == status_str.case_compare(TENANT_CLONE_STATUS_ARRAY[i].str_)) {
        ret_status = TENANT_CLONE_STATUS_ARRAY[i].status_;
        find = true;
      }
    }
  }

  if (!find) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid clone status str", K(status_str));
  }
  return ObTenantCloneStatus(ret_status);
}

bool ObTenantCloneStatus::is_user_status() const
{
  bool b_ret = false;

  if (ObTenantCloneStatus::Status::CLONE_USER_PREPARE == status_ ||
      ObTenantCloneStatus::Status::CLONE_USER_CREATE_INIT_LS == status_ ||
      ObTenantCloneStatus::Status::CLONE_USER_WAIT_LS == status_ ||
      ObTenantCloneStatus::Status::CLONE_USER_POST_CHECK == status_ ||
      ObTenantCloneStatus::Status::CLONE_USER_SUCCESS == status_ ||
      ObTenantCloneStatus::Status::CLONE_USER_FAIL == status_) {
    b_ret = true;
  }

  return b_ret;
}

bool ObTenantCloneStatus::is_user_success_status() const
{
  return ObTenantCloneStatus::Status::CLONE_USER_SUCCESS == status_;
}

bool ObTenantCloneStatus::is_sys_status() const
{
  return !is_user_status();
}

bool ObTenantCloneStatus::is_sys_success_status() const
{
  return ObTenantCloneStatus::Status::CLONE_SYS_SUCCESS == status_;
}

bool ObTenantCloneStatus::is_sys_processing_status() const
{
  bool b_ret = false;

  if (ObTenantCloneStatus::Status::CLONE_SYS_LOCK == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_CREATE_INNER_RESOURCE_POOL == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_CREATE_SNAPSHOT == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_WAIT_CREATE_SNAPSHOT == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_CREATE_TENANT == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_WAIT_TENANT_RESTORE_FINISH == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_RELEASE_RESOURCE == status_) {
    b_ret = true;
  }

  return b_ret;
}

bool ObTenantCloneStatus::is_sys_canceling_status() const
{
  return ObTenantCloneStatus::Status::CLONE_SYS_CANCELING == status_;
}

bool ObTenantCloneStatus::is_sys_failed_status() const
{
  bool b_ret = false;

  if (ObTenantCloneStatus::Status::CLONE_SYS_LOCK_FAIL == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_CREATE_INNER_RESOURCE_POOL_FAIL == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_CREATE_SNAPSHOT_FAIL == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_WAIT_CREATE_SNAPSHOT_FAIL == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_CREATE_TENANT_FAIL == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_WAIT_TENANT_RESTORE_FINISH_FAIL == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_RELEASE_RESOURCE_FAIL == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_CANCELING == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_CANCELED == status_) {
    b_ret = true;
  }

  return b_ret;
}

bool ObTenantCloneStatus::is_sys_valid_snapshot_status_for_fork() const
{
  bool b_ret = false;

  if (ObTenantCloneStatus::Status::CLONE_SYS_WAIT_CREATE_SNAPSHOT == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_WAIT_CREATE_SNAPSHOT_FAIL == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_CREATE_TENANT == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_CREATE_TENANT_FAIL == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_WAIT_TENANT_RESTORE_FINISH == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_WAIT_TENANT_RESTORE_FINISH_FAIL == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_RELEASE_RESOURCE == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_RELEASE_RESOURCE_FAIL == status_ ||
      ObTenantCloneStatus::Status::CLONE_SYS_SUCCESS == status_) {
    b_ret = true;
  }

  return b_ret;
}

bool ObTenantCloneStatus::is_sys_release_resource_status() const
{
  bool b_ret = false;

  if (is_sys_failed_status() ||
      ObTenantCloneStatus::Status::CLONE_SYS_RELEASE_RESOURCE == status_) {
    b_ret = true;
  }

  return b_ret;
}

bool ObTenantCloneStatus::is_sys_release_clone_resource_status() const
{
  bool b_ret = false;

  if (ObTenantCloneStatus::Status::CLONE_SYS_CREATE_INNER_RESOURCE_POOL_FAIL <= status_ &&
      ObTenantCloneStatus::Status::CLONE_SYS_RELEASE_RESOURCE_FAIL > status_) {
    // CLONE_SYS_RELEASE_RESOURCE means the clone_tenant has been created and restored successful.
    // thus, if the clone_job is in or is failed in this status, we just need to release the according snapshot.
    b_ret = true;
  } else if (ObTenantCloneStatus::Status::CLONE_SYS_CANCELING == status_) {
    // job has been canceled by user
    b_ret = true;
  }

  return b_ret;
}

ObCloneJob::ObCloneJob() :
  trace_id_(),
  tenant_id_(OB_INVALID_TENANT_ID),
  job_id_(OB_INVALID_ID),
  source_tenant_id_(OB_INVALID_TENANT_ID),
  source_tenant_name_(),
  clone_tenant_id_(OB_INVALID_TENANT_ID),
  clone_tenant_name_(),
  tenant_snapshot_id_(),
  tenant_snapshot_name_(),
  resource_pool_id_(OB_INVALID_ID),
  resource_pool_name_(),
  unit_config_name_(),
  restore_scn_(),
  status_(ObTenantCloneStatus::Status::CLONE_MAX_STATUS),
  job_type_(ObTenantCloneJobType::CLONE_JOB_MAX_TYPE),
  ret_code_(OB_SUCCESS),
  allocator_("CloneJob"),
  data_version_(0),
  min_cluster_version_(0)
{}

int ObCloneJob::init(const ObCloneJobInitArg &init_arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!init_arg.trace_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid trace_id", KR(ret), K(init_arg.trace_id_));
  } else if (OB_UNLIKELY(!is_sys_tenant(init_arg.tenant_id_) && !is_user_tenant(init_arg.tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(init_arg.tenant_id_));
  } else if (OB_UNLIKELY(0 > init_arg.job_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job_id", KR(ret), K(init_arg.job_id_));
  } else if (OB_UNLIKELY(!is_user_tenant(init_arg.source_tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid source_tenant_id", KR(ret), K(init_arg.source_tenant_id_));
  } else if (OB_UNLIKELY(init_arg.source_tenant_name_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid source_tenant_name", KR(ret), K(init_arg.source_tenant_name_));
  } else if (OB_UNLIKELY(init_arg.clone_tenant_name_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid clone_tenant_name", KR(ret), K(init_arg.clone_tenant_name_));
  } else if (OB_UNLIKELY(init_arg.resource_pool_name_.empty() || init_arg.unit_config_name_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resource_pool_name or unit_config_name", KR(ret),
                                  K(init_arg.resource_pool_name_), K(init_arg.unit_config_name_));
  } else if (OB_UNLIKELY(!init_arg.status_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status", KR(ret), K(init_arg.status_));
  } else if (ObTenantCloneJobType::CLONE_JOB_MAX_TYPE == init_arg.job_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job_type", KR(ret), K(init_arg.job_type_));
  } else if (ObTenantCloneJobType::RESTORE == init_arg.job_type_) {
    if (OB_UNLIKELY(init_arg.tenant_snapshot_name_.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tenant_snapshot_name", KR(ret), K(init_arg.tenant_snapshot_name_), K(init_arg.job_type_));
    } else if (OB_UNLIKELY(!init_arg.tenant_snapshot_id_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tenant_snapshot_id", KR(ret), K(init_arg.tenant_snapshot_id_), K(init_arg.job_type_));
    } else if (OB_UNLIKELY(!init_arg.restore_scn_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid restore_scn", KR(ret), K(init_arg.restore_scn_), K(init_arg.job_type_));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(deep_copy_ob_string(allocator_, init_arg.source_tenant_name_, source_tenant_name_))) {
    LOG_WARN("source_tenant_name deep copy failed", KR(ret), K(init_arg.source_tenant_name_));
  } else if (OB_FAIL(deep_copy_ob_string(allocator_, init_arg.clone_tenant_name_, clone_tenant_name_))) {
    LOG_WARN("clone_tenant_name deep copy failed", KR(ret), K(init_arg.clone_tenant_name_));
  } else if (OB_FAIL(deep_copy_ob_string(allocator_, init_arg.tenant_snapshot_name_, tenant_snapshot_name_))) {
    LOG_WARN("tenant_snapshot_name deep copy failed", KR(ret), K(init_arg.tenant_snapshot_name_));
  } else if (OB_FAIL(deep_copy_ob_string(allocator_, init_arg.resource_pool_name_, resource_pool_name_))) {
    LOG_WARN("resource_pool_name deep copy failed", KR(ret), K(init_arg.resource_pool_name_));
  } else if (OB_FAIL(deep_copy_ob_string(allocator_, init_arg.unit_config_name_, unit_config_name_))) {
    LOG_WARN("unit_config_name deep copy failed", KR(ret), K(init_arg.unit_config_name_));
  } else {
    trace_id_ = init_arg.trace_id_;
    tenant_id_ = init_arg.tenant_id_;
    job_id_ = init_arg.job_id_;
    source_tenant_id_ = init_arg.source_tenant_id_;
    clone_tenant_id_ = init_arg.clone_tenant_id_;
    tenant_snapshot_id_ = init_arg.tenant_snapshot_id_;
    resource_pool_id_ = init_arg.resource_pool_id_;
    restore_scn_ = init_arg.restore_scn_;
    status_ = init_arg.status_;
    job_type_ = init_arg.job_type_;
    ret_code_ = init_arg.ret_code_;
    data_version_ = init_arg.data_version_;
    min_cluster_version_ = init_arg.min_cluster_version_;
  }
  return ret;
}

int ObCloneJob::assign(const ObCloneJob &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_ob_string(allocator_, other.source_tenant_name_, source_tenant_name_))) {
    LOG_WARN("source_tenant_name deep copy failed", KR(ret), K(other));
  } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.clone_tenant_name_, clone_tenant_name_))) {
    LOG_WARN("clone_tenant_name deep copy failed", KR(ret), K(other));
  } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.tenant_snapshot_name_, tenant_snapshot_name_))) {
    LOG_WARN("tenant_snapshot_name deep copy failed", KR(ret), K(other));
  } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.resource_pool_name_, resource_pool_name_))) {
    LOG_WARN("resource_pool_name deep copy failed", KR(ret), K(other));
  } else if (OB_FAIL(deep_copy_ob_string(allocator_, other.unit_config_name_, unit_config_name_))) {
    LOG_WARN("unit_config_name deep copy failed", KR(ret), K(other));
  } else {
    trace_id_ = other.trace_id_;
    tenant_id_ = other.tenant_id_;
    job_id_ = other.job_id_;
    source_tenant_id_ = other.source_tenant_id_;
    clone_tenant_id_ = other.clone_tenant_id_;
    tenant_snapshot_id_ = other.tenant_snapshot_id_;
    resource_pool_id_ = other.resource_pool_id_;
    restore_scn_ = other.restore_scn_;
    status_ = other.status_;
    job_type_ = other.job_type_;
    ret_code_ = other.ret_code_;
    data_version_ = other.data_version_;
    min_cluster_version_ = other.min_cluster_version_;
  }
  return ret;
}

void ObCloneJob::reset()
{
  trace_id_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  job_id_ = OB_INVALID_ID;
  source_tenant_id_ = OB_INVALID_TENANT_ID;
  source_tenant_name_.reset();
  clone_tenant_name_.reset();
  clone_tenant_id_ = OB_INVALID_TENANT_ID;
  tenant_snapshot_id_.reset();
  tenant_snapshot_name_.reset();
  resource_pool_id_ = OB_INVALID_ID;
  resource_pool_name_.reset();
  unit_config_name_.reset();
  restore_scn_.reset();
  status_.reset();
  job_type_ = ObTenantCloneJobType::CLONE_JOB_MAX_TYPE;
  ret_code_ = OB_SUCCESS;
  allocator_.reset();
  data_version_ = 0;
  min_cluster_version_ = 0;
}

bool ObCloneJob::is_valid() const
 {
  bool bret = trace_id_.is_valid()
              && (is_sys_tenant(tenant_id_) || is_user_tenant(tenant_id_))
              && job_id_ > 0
              && is_user_tenant(source_tenant_id_)
              && !source_tenant_name_.empty()
              && !clone_tenant_name_.empty()
              && !resource_pool_name_.empty()
              && !unit_config_name_.empty()
              && status_.is_valid()
              && ObTenantCloneJobType::CLONE_JOB_MAX_TYPE != job_type_;
  if (bret && ObTenantCloneJobType::RESTORE == job_type_) {
    bret = tenant_snapshot_id_.is_valid()
           && !tenant_snapshot_name_.empty()
           && restore_scn_.is_valid();
  }
  return bret;
}

bool ObCloneJob::is_valid_status_allows_user_tenant_to_do_ls_recovery() const
{
  return ObTenantCloneStatus::Status::CLONE_USER_WAIT_LS == status_
         || ObTenantCloneStatus::Status::CLONE_USER_POST_CHECK == status_
         || ObTenantCloneStatus::Status::CLONE_USER_SUCCESS == status_;
}

ObTenantCloneTableOperator::ObTenantCloneTableOperator() :
  is_inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  proxy_(NULL)
{}

const TenantCloneJobTypeStrPair ObTenantCloneTableOperator::TENANT_CLONE_JOB_TYPE_ARRAY[] = {
  TenantCloneJobTypeStrPair(ObTenantCloneJobType::RESTORE, "RESTORE"),
  TenantCloneJobTypeStrPair(ObTenantCloneJobType::FORK, "FORK"),
};

ObTenantCloneJobType ObTenantCloneTableOperator::get_job_type(const ObString &str)
{
  ObTenantCloneJobType ret_type = ObTenantCloneJobType::CLONE_JOB_MAX_TYPE;
  bool find = false;

  if (!str.empty()) {
    for (int64_t i = 0; !find && i < ARRAYSIZEOF(TENANT_CLONE_JOB_TYPE_ARRAY); i++) {
      if (0 == str.case_compare(TENANT_CLONE_JOB_TYPE_ARRAY[i].str_)) {
        ret_type = TENANT_CLONE_JOB_TYPE_ARRAY[i].type_;
        find = true;
      }
    }
  }

  if (!find) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid clone job type str", K(str));
  }
  return ret_type;
}

const char* ObTenantCloneTableOperator::get_job_type_str(ObTenantCloneJobType job_type)
{
  const char* str = "CLONE_JOB_MAX_TYPE";
  bool find = false;

  for (int64_t i = 0; !find && i < ARRAYSIZEOF(TENANT_CLONE_JOB_TYPE_ARRAY); i++) {
    if (job_type == TENANT_CLONE_JOB_TYPE_ARRAY[i].type_) {
      find = true;
      if (OB_NOT_NULL(TENANT_CLONE_JOB_TYPE_ARRAY[i].str_)) {
        str = TENANT_CLONE_JOB_TYPE_ARRAY[i].str_;
      } else {
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected null");
      }
    }
  }

  if (!find) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid clone job type", K(job_type));
  }

  return str;
}

int ObTenantCloneTableOperator::init(const uint64_t tenant_id, ObISQLClient *proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("clone table operator init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_sys_tenant(tenant_id) && !is_user_tenant(tenant_id)
                         || OB_ISNULL(proxy))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(proxy));
  } else {
    tenant_id_ = tenant_id;
    proxy_ = proxy;
    is_inited_ = true;
  }
  return ret;
}

//get clone job according to source_tenant_id
//a source tenant only has one clone_job at the same time
int ObTenantCloneTableOperator::get_clone_job_by_source_tenant_id(
    const uint64_t source_tenant_id,
    ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  job.reset();
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clone table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!is_user_tenant(source_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(source_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND source_tenant_id = %lu",
                                    OB_ALL_CLONE_JOB_TNAME, tenant_id_, source_tenant_id))) {
    LOG_WARN("assign sql failed", KR(ret), K(tenant_id_), K(source_tenant_id));
  } else if (OB_FAIL(read_only_exist_one_job_(sql, job))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to read job", KR(ret), K(sql));
    } else {
      LOG_INFO("clone job not exist", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObTenantCloneTableOperator::get_clone_job_by_job_id(const int64_t job_id,
                                                        ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clone table operator not init", KR(ret));
  } else if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %ld AND job_id = %ld",
                                    OB_ALL_CLONE_JOB_TNAME, tenant_id_, job_id))) {
    LOG_WARN("assign sql failed", KR(ret));
  } else if (OB_FAIL(read_only_exist_one_job_(sql, job))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to read job", KR(ret), K(sql));
    } else {
      LOG_INFO("clone job not exist", KR(ret), K(sql));
    }
  }

  return ret;
}

//get clone job according to clone_tenant_name
int ObTenantCloneTableOperator::get_clone_job_by_clone_tenant_name(
    const ObString &clone_tenant_name,
    const bool need_lock,
    ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  job.reset();
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clone table operator not init", KR(ret));
  } else if (OB_UNLIKELY(clone_tenant_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(clone_tenant_name));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu AND clone_tenant_name = '%.*s'",
                     OB_ALL_CLONE_JOB_TNAME, tenant_id_, clone_tenant_name.length(), clone_tenant_name.ptr()))) {
    LOG_WARN("assign sql failed", KR(ret), K(tenant_id_), K(clone_tenant_name));
  } else if (need_lock && OB_FAIL(sql.append_fmt(" FOR UPDATE "))) {  /*lock row*/
    LOG_WARN("assign sql failed", KR(ret));
  } else if (OB_FAIL(read_only_exist_one_job_(sql, job))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to read job", KR(ret), K(sql));
    } else {
      LOG_INFO("clone job not exist", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObTenantCloneTableOperator::get_all_clone_jobs(ObArray<ObCloneJob> &jobs)
{
  int ret = OB_SUCCESS;
  jobs.reset();
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clone table operator not init", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu",
                                    OB_ALL_CLONE_JOB_TNAME, tenant_id_))) {
    LOG_WARN("assign sql failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(read_jobs_(sql, jobs))) {
    LOG_WARN("fail to read jobs", KR(ret), K(sql));
  }
  return ret;
}

int ObTenantCloneTableOperator::insert_clone_job(const ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  const int64_t start_time = 0; // in add_time_column(), "0" means now(6)

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clone table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job));
  } else if (OB_FAIL(build_insert_dml_(job, dml))) {
    LOG_WARN("fail to build insert dml", KR(ret), K(job));
  } else if (OB_FAIL(dml.add_time_column("clone_start_time", start_time))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_CLONE_JOB_TNAME, sql))) {
    LOG_WARN("splice insert sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(gen_meta_tenant_id(tenant_id_)), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }

  return ret;
}

int ObTenantCloneTableOperator::update_job_status(
    const int64_t job_id,
    const ObTenantCloneStatus &old_status,
    const ObTenantCloneStatus &new_status)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clone table operator not init", KR(ret));
  } else if (OB_UNLIKELY(job_id < 0
                         || !old_status.is_valid()
                         || !new_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job_id", KR(ret), K(tenant_id_), K(job_id), K(old_status), K(new_status));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id_))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("status", ObTenantCloneStatus::get_clone_status_str(new_status)))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.get_extra_condition().assign_fmt("%s = '%s'",
                                    "status", ObTenantCloneStatus::get_clone_status_str(old_status)))) {
    LOG_WARN("add extra_condition failed", KR(ret), K(old_status));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_CLONE_JOB_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(gen_meta_tenant_id(tenant_id_)), K(sql));
  } else if (is_zero_row(affected_rows)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("the job with old_status doesn't exist", KR(ret), K(affected_rows),
                                                      K(job_id), K(old_status));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObTenantCloneTableOperator::update_job_failed_info(const int64_t job_id,
                                                       const int ret_code,
                                                       const ObString& err_msg)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  const int64_t finished_time = 0; // in add_time_column(), "0" means now(6)

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clone table operator not init", KR(ret));
  } else if (OB_UNLIKELY(job_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job_id", KR(ret), K(tenant_id_), K(job_id));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id_))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("ret_code", ret_code))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (!err_msg.empty() && OB_FAIL(dml.add_column("error_msg", ObHexEscapeSqlStr(err_msg)))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_time_column("clone_finished_time", finished_time))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_CLONE_JOB_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(gen_meta_tenant_id(tenant_id_)), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObTenantCloneTableOperator::update_job_clone_tenant_id(
    const int64_t job_id,
    const uint64_t clone_tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clone table operator not init", KR(ret));
  } else if (OB_UNLIKELY(job_id < 0
                         || OB_INVALID_TENANT_ID == clone_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job_id", KR(ret), K(tenant_id_), K(job_id), K(clone_tenant_id));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id_))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("clone_tenant_id", clone_tenant_id))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.get_extra_condition().assign_fmt("%s = %lu",
                                                          "clone_tenant_id",
                                                          OB_INVALID_TENANT_ID))) {
    LOG_WARN("add extra_condition failed", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_CLONE_JOB_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(gen_meta_tenant_id(tenant_id_)), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }

  return ret;
}

int ObTenantCloneTableOperator::update_job_resource_pool_id(const int64_t job_id,
                                                            const uint64_t resource_pool_id)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clone table operator not init", KR(ret));
  } else if (job_id < 0 || OB_INVALID_ID == resource_pool_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id_), K(job_id), K(resource_pool_id));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id_))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("resource_pool_id", resource_pool_id))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.get_extra_condition().assign_fmt("%s = %ld",
                                                          "resource_pool_id",
                                                          OB_INVALID_ID))) {
    LOG_WARN("add extra_condition failed", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_CLONE_JOB_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret), K(sql));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(sql), K(gen_meta_tenant_id(tenant_id_)));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObTenantCloneTableOperator::update_job_snapshot_info(const int64_t job_id,
                                                         const ObTenantSnapshotID snapshot_id,
                                                         const ObString &snapshot_name)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clone table operator not init", KR(ret));
  } else if (job_id < 0 || !snapshot_id.is_valid() || snapshot_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id_), K(job_id), K(snapshot_id), K(snapshot_name));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id_))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("tenant_snapshot_id", snapshot_id.id()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("tenant_snapshot_name", snapshot_name))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.get_extra_condition().assign_fmt("%s = %ld AND %s = %s",
                                                          "tenant_snapshot_id",
                                                          ObTenantSnapshotID::OB_INVALID_SNAPSHOT_ID,
                                                          "tenant_snapshot_name",
                                                          "\'\'"))) {
    LOG_WARN("add extra_condition failed", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_CLONE_JOB_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret), K(sql));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(sql), K(gen_meta_tenant_id(tenant_id_)));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows), K(sql));
  }
  return ret;
}

int ObTenantCloneTableOperator::update_job_snapshot_scn(const int64_t job_id,
                                                        const SCN &restore_scn)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clone table operator not init", K(ret));
  } else if (job_id < 0 || !restore_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id_), K(job_id), K(restore_scn));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id_))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("restore_scn", restore_scn.get_val_for_inner_table_field()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.get_extra_condition().assign_fmt("%s = %lu",
                                                          "restore_scn",
                                                          SCN::invalid_scn().get_val_for_inner_table_field()))) {
    LOG_WARN("add extra_condition failed", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_CLONE_JOB_TNAME, sql))) {
    LOG_WARN("splice update sql failed", KR(ret), K(sql));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(sql), K(gen_meta_tenant_id(tenant_id_)));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObTenantCloneTableOperator::remove_clone_job(const ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clone table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", job.get_tenant_id()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job.get_job_id()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.splice_delete_sql(OB_ALL_CLONE_JOB_TNAME, sql))) {
    LOG_WARN("splice delete sql failed", KR(ret));
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(tenant_id_), sql.ptr(), affected_rows))) {
    LOG_WARN("exec sql failed", KR(ret), K(gen_meta_tenant_id(tenant_id_)), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }

  return ret;
}

int ObTenantCloneTableOperator::insert_clone_job_history(const ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString select_sql;
  ObSqlString insert_sql;
  int64_t start_time = 0;
  int64_t finished_time = 0;
  int ret_code = OB_SUCCESS;
  ObString err_msg;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clone table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job));
  } else if (OB_FAIL(select_sql.assign_fmt("SELECT clone_start_time, clone_finished_time, ret_code, error_msg "
                                    "FROM %s WHERE tenant_id = %lu AND job_id = %ld",
                                    OB_ALL_CLONE_JOB_TNAME,
                                    job.get_tenant_id(), job.get_job_id()))) {
    LOG_WARN("assign select_sql failed", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy_->read(res, gen_meta_tenant_id(job.get_tenant_id()), select_sql.ptr()))) {
        LOG_WARN("failed to execute select_sql", KR(ret), K(select_sql), K(job));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get select_sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("next failed", KR(ret));
      } else {
        EXTRACT_TIMESTAMP_FIELD_MYSQL(*result, "clone_start_time", start_time);
        EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(*result, "clone_finished_time", finished_time);
        EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "ret_code", ret_code, int);
        EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "error_msg", err_msg,
                                                       true /*skip_null_error*/,
                                                       false /*skip_column_error*/,
                                                       "" /*default_value*/);
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(build_insert_dml_(job, dml))) {
        LOG_WARN("fail to build insert dml", KR(ret), K(job));
      } else if (OB_FAIL(dml.add_time_column("clone_start_time", start_time))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.add_time_column("clone_finished_time", finished_time))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (!err_msg.empty() && OB_FAIL(dml.add_column("error_msg", ObHexEscapeSqlStr(err_msg)))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.add_column("ret_code", ret_code))) {
        LOG_WARN("add column failed", KR(ret));
      } else if (OB_FAIL(dml.splice_insert_sql(OB_ALL_CLONE_JOB_HISTORY_TNAME, insert_sql))) {
        LOG_WARN("splice insert_sql failed", KR(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(proxy_->write(gen_meta_tenant_id(tenant_id_), insert_sql.ptr(), affected_rows))) {
    LOG_WARN("exec insert_sql failed", KR(ret), K(gen_meta_tenant_id(tenant_id_)), K(insert_sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }

  return ret;
}

int ObTenantCloneTableOperator::get_user_clone_job_history(ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  job.reset();
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clone table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu",
                                    OB_ALL_CLONE_JOB_HISTORY_TNAME, tenant_id_))) {
    LOG_WARN("assign sql failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(read_only_exist_one_job_(sql, job))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to read job", KR(ret), K(sql));
    } else {
      LOG_INFO("clone job not exist", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObTenantCloneTableOperator::get_sys_clone_job_history(
    const int64_t job_id,
    ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  job.reset();
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clone table operator not init", KR(ret));
  } else if (OB_UNLIKELY(job_id < 0
                         || !is_sys_tenant(tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id), K(tenant_id_));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld",
                                    OB_ALL_CLONE_JOB_HISTORY_TNAME, job_id))) {
    LOG_WARN("assign sql failed", KR(ret), K(job_id));
  } else if (OB_FAIL(read_only_exist_one_job_(sql, job))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to read job", KR(ret), K(sql));
    } else {
      LOG_INFO("clone job not exist", KR(ret), K(sql));
    }
  }
  return ret;
}

int ObTenantCloneTableOperator::get_job_failed_message(
    const int64_t job_id,
    ObIAllocator &allocator,
    ObString &err_msg)
{
  int ret = OB_SUCCESS;
  err_msg.reset();
  const int32_t table_num = 2;
  ObString tables[2] = {OB_ALL_CLONE_JOB_TNAME, OB_ALL_CLONE_JOB_HISTORY_TNAME};
  ObSqlString sql;
  bool find = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant clone table operator not init", KR(ret));
  } else if (OB_UNLIKELY(job_id < 0
                         || !is_sys_tenant(tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id), K(tenant_id_));
  } else {
    for (int32_t i = 0; !find && OB_SUCC(ret) && i < table_num; i++) {
      sql.reset();
      if (OB_FAIL(sql.assign_fmt("SELECT error_msg FROM %.*s WHERE job_id = %ld",
                                      tables[i].length(), tables[i].ptr(), job_id))) {
        LOG_WARN("assign sql failed", KR(ret), K(job_id));
      } else {
        SMART_VAR(ObMySQLProxy::MySQLResult, res) {
          ObMySQLResult *result = NULL;
          if (OB_FAIL(proxy_->read(res, gen_meta_tenant_id(tenant_id_), sql.ptr()))) {
            LOG_WARN("failed to execute sql", KR(ret), K(gen_meta_tenant_id(tenant_id_)), K(sql));
          } else if (NULL == (result = res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get sql result", KR(ret));
          } else if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to get next result", KR(ret));
            } else {
              ret = OB_SUCCESS;
            }
          } else {
            find = true;
            ObString tmp_msg;
            EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "error_msg", tmp_msg,
                                                           true /*skip_null_error*/,
                                                           false /*skip_column_error*/,
                                                           "" /*default_value*/);
            if (FAILEDx(deep_copy_ob_string(allocator, tmp_msg, err_msg))) {
              LOG_WARN("error_msg deep copy failed", KR(ret), K(tmp_msg));
            }
          }
        } // end SMART_VAR
      }
    } // end for

    if (OB_SUCC(ret) && !find) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("the job doesn't exist", KR(ret), K(job_id));
    }
  }
  return ret;
}

//job num <= 1 in a certain tenant
int ObTenantCloneTableOperator::read_only_exist_one_job_(
    const ObSqlString &sql,
    ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  job.reset();
  ObArray<ObCloneJob> jobs;
  if (OB_UNLIKELY(sql.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sql));
  } else if (OB_FAIL(read_jobs_(sql, jobs))) {
    LOG_WARN("fail to read jobs", KR(ret), K(sql));
  } else if (jobs.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_INFO("clone job not exist", KR(ret));
  } else if (jobs.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected clone job count", KR(ret), K(jobs));
  } else if (OB_FAIL(job.assign(jobs.at(0)))) {
    LOG_WARN("assign failed", KR(ret), K(jobs));
  } else if (OB_UNLIKELY(!job.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid job", KR(ret), K(job));
  }
  return ret;
}

int ObTenantCloneTableOperator::read_jobs_(
    const ObSqlString &sql,
    ObArray<ObCloneJob> &jobs)
{
  int ret = OB_SUCCESS;
  jobs.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(sql.empty() || OB_ISNULL(proxy_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sql), KP(proxy_));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy_->read(res, gen_meta_tenant_id(tenant_id_), sql.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K(gen_meta_tenant_id(tenant_id_)), K(sql));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          ObCloneJob job;
          if (OB_FAIL(fill_job_from_result_(result, job))) {
            LOG_WARN("fill job from result failed", KR(ret));
          } else if (OB_FAIL(jobs.push_back(job))) {
            LOG_WARN("fail to push back job", KR(ret), K(job));
          }
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next result", KR(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObTenantCloneTableOperator::build_insert_dml_(
    const ObCloneJob &job,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  char trace_id_buf[OB_MAX_TRACE_ID_BUFFER_SIZE] = {'\0'};
  bool is_compatible_with_clone_standby_tenant = false;

  if (OB_UNLIKELY(!job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", job.get_tenant_id()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_pk_column("job_id", job.get_job_id()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (FALSE_IT(job.get_trace_id().to_string(trace_id_buf, sizeof(trace_id_buf)))) {
  } else if (OB_FAIL(dml.add_column("trace_id", trace_id_buf))) {
    LOG_WARN("add column failed", KR(ret), K(trace_id_buf));
  } else if (OB_FAIL(dml.add_column("source_tenant_id", job.get_source_tenant_id()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("source_tenant_name", job.get_source_tenant_name()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("clone_tenant_name", job.get_clone_tenant_name()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("clone_tenant_id", job.get_clone_tenant_id()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("tenant_snapshot_id", job.get_tenant_snapshot_id().id()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("tenant_snapshot_name", job.get_tenant_snapshot_name().empty() ?
                                                            "" :
                                                            job.get_tenant_snapshot_name()))) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(dml.add_column("resource_pool_id", job.get_resource_pool_id()))) {
    LOG_WARN("add column failed", K(ret));
  } else if (OB_FAIL(dml.add_column("resource_pool_name", job.get_resource_pool_name()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("unit_config_name", job.get_unit_config_name()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_uint64_column("restore_scn", job.get_restore_scn().get_val_for_inner_table_field()))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("status", ObTenantCloneStatus::get_clone_status_str(job.get_status())))) {
    LOG_WARN("add column failed", KR(ret));
  } else if (OB_FAIL(dml.add_column("job_type", get_job_type_str(job.get_job_type())))) {
    LOG_WARN("add column failed", K(ret));
  } else if (0 != job.get_data_version()
             && OB_FAIL(dml.add_column("data_version", job.get_data_version()))) {
    // data_version not 0 means (sys/meta/user)tenant's data_version must promoted to 4.3.2
    LOG_WARN("add data_version column failed", KR(ret), K(job));
  } else if (0 != job.get_min_cluster_version()
             && OB_FAIL(dml.add_column("min_cluster_version", job.get_min_cluster_version()))) {
    // min_cluster_version not 0 means (sys/meta/user)tenant's data_version must promoted to 4.3.2
    LOG_WARN("add min_cluster_version failed", KR(ret), K(job));
  }
  return ret;
}

int ObTenantCloneTableOperator::fill_job_from_result_(const ObMySQLResult *result,
                                                      ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  int64_t real_length = 0;
  common::ObCurTraceId::TraceId trace_id;
  char trace_id_buf[OB_MAX_TRACE_ID_BUFFER_SIZE] = {'\0'};
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  int64_t job_id = OB_INVALID_ID;
  uint64_t source_tenant_id = OB_INVALID_TENANT_ID;
  ObString clone_tenant_name;
  ObString source_tenant_name;
  uint64_t clone_tenant_id = OB_INVALID_TENANT_ID;
  ObTenantSnapshotID tenant_snapshot_id;
  ObString tenant_snapshot_name;
  uint64_t resource_pool_id = OB_INVALID_ID;
  ObString resource_pool_name;
  ObString unit_config_name;
  uint64_t restore_scn_val = OB_INVALID_SCN_VAL;
  SCN restore_scn;
  ObString status_str;
  ObString job_type_str;
  int ret_code = OB_SUCCESS;
  uint64_t data_version = 0;
  uint64_t min_cluster_version = 0;
  EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(*result, "job_id", job_id, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(*result, "trace_id", trace_id_buf, sizeof(trace_id_buf), real_length);
  EXTRACT_INT_FIELD_MYSQL(*result, "source_tenant_id", source_tenant_id, uint64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(*result, "source_tenant_name", source_tenant_name);
  EXTRACT_INT_FIELD_MYSQL(*result, "clone_tenant_id", clone_tenant_id, uint64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(*result, "clone_tenant_name", clone_tenant_name);
  EXTRACT_INT_FIELD_MYSQL(*result, "tenant_snapshot_id", tenant_snapshot_id, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(*result, "tenant_snapshot_name", tenant_snapshot_name);
  EXTRACT_INT_FIELD_MYSQL(*result, "resource_pool_id", resource_pool_id, uint64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(*result, "resource_pool_name", resource_pool_name);
  EXTRACT_VARCHAR_FIELD_MYSQL(*result, "unit_config_name", unit_config_name);
  EXTRACT_UINT_FIELD_MYSQL(*result, "restore_scn", restore_scn_val, uint64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(*result, "status", status_str);
  EXTRACT_VARCHAR_FIELD_MYSQL(*result, "job_type", job_type_str);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "ret_code", ret_code, int);
  EXTRACT_UINT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "data_version", data_version, uint64_t,
                                             true/*skip_null_error*/, true/*skip_column_error*/, 0/*default_value*/);
  EXTRACT_UINT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "min_cluster_version", min_cluster_version, uint64_t,
                                             true/*skip_null_error*/, true/*skip_column_error*/, 0/*default_value*/);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(trace_id.parse_from_buf(trace_id_buf))) {
      LOG_WARN("fail to parse trace id from buf", KR(ret), K(trace_id_buf));
    } else if (OB_INVALID_SCN_VAL != restore_scn_val
        && OB_FAIL(restore_scn.convert_for_inner_table_field(restore_scn_val))) {
      LOG_WARN("fail to convert_for_inner_table_field", KR(ret), K(restore_scn_val));
    } else {
      const ObCloneJob::ObCloneJobInitArg init_arg = {
        .trace_id_                   = trace_id,
        .tenant_id_                  = tenant_id,
        .job_id_                     = job_id,
        .source_tenant_id_           = source_tenant_id,
        .source_tenant_name_         = source_tenant_name,
        .clone_tenant_id_            = clone_tenant_id,
        .clone_tenant_name_          = clone_tenant_name,
        .tenant_snapshot_id_         = tenant_snapshot_id,
        .tenant_snapshot_name_       = tenant_snapshot_name,
        .resource_pool_id_           = resource_pool_id,
        .resource_pool_name_         = resource_pool_name,
        .unit_config_name_           = unit_config_name,
        .restore_scn_                = restore_scn,
        .status_                     = ObTenantCloneStatus::get_clone_status(status_str),
        .job_type_                   = get_job_type(job_type_str),
        .ret_code_                   = ret_code,
        .data_version_               = data_version,
        .min_cluster_version_         = min_cluster_version,
      };
      if (OB_FAIL(job.init(init_arg))) {
        LOG_WARN("fail to init clone job", KR(ret), K(init_arg));
      }
    }
  }
  return ret;
}
