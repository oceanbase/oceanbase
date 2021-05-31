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

#define USING_LOG_PREFIX RS_RESTORE

#include "ob_restore_util.h"
#include "lib/lock/ob_mutex.h"
#include "share/restore/ob_restore_uri_parser.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/backup/ob_backup_struct.h"
#include "share/ob_rpc_struct.h"
#include "rootserver/restore/ob_restore_table_operator.h"
#include "rootserver/ob_rs_event_history_table_operator.h"

using namespace oceanbase::common;
using namespace oceanbase;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;

lib::ObMutex ObRestoreUtil::check_job_sync_lock_;
///////////////////////////////////////////////////

void ObRecoveryHelper::ObMemberListPkeyInfo::reset()
{
  member_list_.reset();
  pkey_info_.reset();
}

///////////////////////////////////////////////////
void ObRecoveryHelper::ObMemberListPkeyList::reset()
{
  pkey_array_.reset();
  ml_pk_array_.reset();
  epoch_ = 0;
}

int ObRecoveryHelper::ObMemberListPkeyList::add_partition(const ObPartitionInfo& partition)
{
  int ret = OB_SUCCESS;
  int64_t member_list_index = OB_INVALID_INDEX;
  if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(partition));
  } else if (partition.replica_count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(partition));
  } else if (OB_FAIL(pkey_array_.push_back(partition.get_replicas_v2().at(0).partition_key()))) {
    LOG_WARN("fail to add partition key", KR(ret), K(partition));
  } else if (OB_FAIL(find_member_list(partition, member_list_index))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(add_member_list(partition))) {
        LOG_WARN("fail to add member list", KR(ret), K(partition));
      } else {
        member_list_index = ml_pk_array_.count() - 1;
      }
    } else {
      LOG_WARN("fail to find member list", KR(ret));
    }
  }
  PkeyInfo pkey_info;
  pkey_info.pkey_index_ = pkey_array_.count() - 1;
  if (OB_FAIL(ret) || OB_INVALID_INDEX == member_list_index || member_list_index >= ml_pk_array_.count()) {
    ret = OB_FAIL(ret) ? ret : OB_ERR_UNEXPECTED;
    LOG_WARN("fail to add partition", KR(ret), K(member_list_index));
  } else if (OB_FAIL(ml_pk_array_.at(member_list_index).pkey_info_.push_back(pkey_info))) {
    LOG_WARN("fail to push back", KR(ret), K(pkey_info), K(partition));
  } else {
    LOG_DEBUG("add partition", K(partition), K(member_list_index), K(ml_pk_array_), K(pkey_array_));
  }
  return ret;
}

int ObRecoveryHelper::ObMemberListPkeyList::find_member_list(
    const ObPartitionInfo& partition, int64_t& member_list_index)
{
  int ret = OB_SUCCESS;
  ObMemberList server_list;
  member_list_index = -1;
  for (int64_t i = 0; i < partition.replica_count() && OB_SUCC(ret); i++) {
    const ObPartitionReplica& replica = partition.get_replicas_v2().at(i);
    ObMember member(replica.server_, 0);
    if (OB_FAIL(server_list.add_member(member))) {
      LOG_WARN("fail to add member", KR(ret), K(replica));
    }
  }
  if (OB_FAIL(ret)) {
    // nothing todo
  } else {
    for (int64_t i = 0; i < ml_pk_array_.count() && OB_SUCC(ret); i++) {
      if (server_list.member_addr_equal(ml_pk_array_.at(i).member_list_)) {
        member_list_index = i;
        break;
      }
    }
  }
  if (OB_SUCC(ret) && -1 == member_list_index) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to find member list", KR(ret), K(partition));
  }
  return ret;
}

int ObRecoveryHelper::ObMemberListPkeyList::add_member_list(const ObPartitionInfo& partition)
{
  int ret = OB_SUCCESS;
  ObMemberListPkeyInfo ml_pk;
  for (int64_t i = 0; i < partition.replica_count() && OB_SUCC(ret); i++) {
    const ObPartitionReplica& replica = partition.get_replicas_v2().at(i);
    ObMember member(replica.server_, 0);
    if (OB_FAIL(ml_pk.member_list_.add_member(member))) {
      LOG_WARN("fail to push back", KR(ret), K(replica));
    }
  }
  if (OB_FAIL(ret)) {
    // nothing todo
  } else {
    if (OB_FAIL(ml_pk_array_.push_back(ml_pk))) {
      LOG_WARN("fail to push back", KR(ret), K(ml_pk));
    } else {
      LOG_DEBUG("add member list", K(ml_pk));
    }
  }
  return ret;
}

int ObRecoveryHelper::ObMemberListPkeyList::add_partition_valid(const obrpc::ObBatchCheckRes& result)
{
  int ret = OB_SUCCESS;
  if (!result.is_valid() || result.index_.epoch_ != epoch_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(result), K_(epoch));
  } else {
    int64_t ml_pk_index = result.index_.ml_pk_index_;
    int64_t pkey_info_start = result.index_.pkey_info_start_index_;
    if (0 > ml_pk_index || ml_pk_array_.count() <= ml_pk_index || 0 > pkey_info_start ||
        ml_pk_array_.at(ml_pk_index).pkey_info_.count() < pkey_info_start + result.results_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid result",
          K(ml_pk_index),
          K(pkey_info_start),
          K(ml_pk_array_.count()),
          "pkey_info_array.count()",
          ml_pk_array_.at(ml_pk_index).pkey_info_.count());
    } else {
      for (int64_t i = 0; i < result.results_.count() && OB_SUCC(ret); i++) {
        if (!result.results_.at(i)) {
          // nothing todo
        } else {
          ml_pk_array_.at(ml_pk_index).pkey_info_.at(pkey_info_start + i).part_valid_ = true;
        }
      }
    }
  }
  return ret;
}

bool ObRecoveryHelper::ObMemberListPkeyList::all_partitions_valid() const
{
  bool bret = true;
  for (int64_t i = 0; i < ml_pk_array_.count(); i++) {
    for (int64_t j = 0; j < ml_pk_array_.at(i).pkey_info_.count(); j++) {
      if (!ml_pk_array_.at(i).pkey_info_.at(j).part_valid_) {
        bret = false;
        break;
      }
    }
  }
  return bret;
}

int ObRecoveryHelper::ObMemberListPkeyList::compaction()
{
  int ret = OB_SUCCESS;
  epoch_++;
  ObArray<ObMemberListPkeyInfo> new_ml_pkey_array;
  ObMemberListPkeyInfo tmp_pkey_info;
  for (int64_t i = 0; i < ml_pk_array_.count() && OB_SUCC(ret); i++) {
    const ObMemberListPkeyInfo& ml_pk = ml_pk_array_.at(i);
    tmp_pkey_info.reset();
    for (int64_t j = 0; j < ml_pk.pkey_info_.count() && OB_SUCC(ret); j++) {
      if (ml_pk.pkey_info_.at(j).part_valid_) {
        // nothing todo
      } else if (OB_FAIL(tmp_pkey_info.pkey_info_.push_back(ml_pk.pkey_info_.at(j)))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (tmp_pkey_info.pkey_info_.count() <= 0) {
    } else {
      tmp_pkey_info.member_list_ = ml_pk.member_list_;
      if (OB_FAIL(new_ml_pkey_array.push_back(tmp_pkey_info))) {
        LOG_WARN("fail to push back", KR(ret), K(tmp_pkey_info));
      }
    }
  }  // end for
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ml_pk_array_.assign(new_ml_pkey_array))) {
    LOG_WARN("fail to assign", KR(ret));
  } else {
    LOG_INFO("compat partiton info success", K(new_ml_pkey_array));
  }
  return ret;
}
///////////////////////////////////////////////////
void ObRecoveryHelper::ObLeaderPkeyLists::reset()
{
  user_leader_pkeys_.reset();
  inner_leader_pkeys_.reset();
}

int ObRecoveryHelper::ObLeaderPkeyLists::add_partition(const share::ObPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;
  const ObPartitionReplica* replica = NULL;
  ObArray<ObLeaderPkeyList>* leader_pkeys = NULL;
  if (!partition_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(partition_info.find_leader_v2(replica))) {
    LOG_WARN("fail to find leader", KR(ret), K(partition_info));
  } else if (OB_ISNULL(replica)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid leader replica", KR(ret), K(replica), K(partition_info));
  } else if (is_inner_table(partition_info.get_table_id())) {
    leader_pkeys = &inner_leader_pkeys_;
  } else {
    leader_pkeys = &user_leader_pkeys_;
  }
  bool find = false;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(leader_pkeys)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader pkeys is null", K(ret), KP(leader_pkeys), K(partition_info));
  } else {
    for (int64_t i = 0; i < leader_pkeys->count() && OB_SUCC(ret); i++) {
      if (leader_pkeys->at(i).leader_ == replica->server_) {
        if (OB_FAIL(leader_pkeys->at(i).pkey_list_.push_back(replica->partition_key()))) {
          LOG_WARN("fail to push back", KR(ret), K(*replica));
        }
        find = true;
        break;
      }
    }
  }
  if (OB_FAIL(ret) || find) {
  } else {
    ObLeaderPkeyList pkey_list;
    pkey_list.leader_ = replica->server_;
    if (OB_FAIL(pkey_list.pkey_list_.push_back(replica->partition_key()))) {
      LOG_WARN("fail to push back", KR(ret));
    } else if (OB_FAIL(leader_pkeys->push_back(pkey_list))) {
      LOG_WARN("fail to push back", KR(ret), K(pkey_list));
    }
  }
  return ret;
}
///////////////////////////////////////////////////

ObRestoreUtil::ObRestoreUtil() : inited_(false), job_id_(-1), restore_ctx_(NULL), restore_args_()
{}

ObRestoreUtil::~ObRestoreUtil()
{
  inited_ = false;
}

int ObRestoreUtil::init(observer::ObRestoreCtx& restore_ctx, int64_t job_id)
{
  int ret = OB_SUCCESS;
  if (!restore_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid restore ctx", K(ret));
  } else if (0 > job_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job id", K(job_id), K(ret));
  } else {
    restore_ctx_ = &restore_ctx;
    job_id_ = job_id;
    inited_ = true;
  }
  return ret;
}

// Logical restore tenant cmd will trigger asynchronous logical restore tenant job.
int ObRestoreUtil::execute(const obrpc::ObRestoreTenantArg& arg)
{
  int ret = OB_SUCCESS;
  ROOTSERVICE_EVENT_ADD("balancer", "start_restore_tenant", "tenant", arg.tenant_name_.str());
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObRestoreURIParser::parse(arg.oss_uri_, restore_args_))) {
    LOG_WARN("fail parse oss_uri", K(arg), K(ret));
  } else if (OB_FAIL(ObRestoreURIParserHelper::set_data_version(restore_args_))) {
    LOG_WARN("fail set data version", K_(restore_args), K(ret));
  } else if (OB_FAIL(record_job(arg))) {
    LOG_WARN("fail generate task", K(arg), K(ret));
  }
  if (OB_FAIL(ret)) {
    ROOTSERVICE_EVENT_ADD("balancer", "fail_restore_tenant", "tenant", arg.tenant_name_.str(), "result", ret);
  }
  return ret;
}

// logical restore
int ObRestoreUtil::record_job(const obrpc::ObRestoreTenantArg& arg)
{
  LOG_INFO("begin generate task");
  int ret = OB_SUCCESS;
  RestoreJob job;
  ObSchemaGetterGuard schema_guard;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(restore_ctx_->sql_client_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service pointer", K(ret));
  } else {

    job.job_id_ = job_id_;
    job.level_ = 0;
    job.status_ = RESTORE_INIT;
    job.backup_uri_ = arg.oss_uri_;
    job.tenant_name_ = arg.tenant_name_.str();

    LOG_INFO("got restore job", K(job));

    if (OB_SUCC(ret)) {
      // prevent simultaneous restoring tenant
      ObRestoreTableOperator restore_op;
      common::ObSEArray<RestoreJob, 10> jobs;
      int64_t job_cnt = 0;
      lib::ObMutexGuard guard(check_job_sync_lock_);
      if (OB_FAIL(restore_op.init(restore_ctx_->sql_client_))) {
        LOG_WARN("fail init restore op", K(ret));
      } else if (OB_FAIL(restore_op.get_jobs(jobs))) {
        LOG_WARN("fail get jobs", K(ret));
      } else {
        bool has_same_job = false;
        FOREACH_CNT_X(job, jobs, !has_same_job)
        {
          if (job->tenant_name_ == arg.tenant_name_.str()) {
            has_same_job = true;
          }
        }
        if (has_same_job) {
          ret = OB_RESTORE_IN_PROGRESS;
          LOG_WARN("Another restore in progress", K(job_cnt), K(ret));
        } else if (OB_FAIL(restore_op.insert_job(job))) {
          LOG_WARN("fail insert job and partitions", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRestoreUtil::check_has_job(common::ObMySQLProxy* sql_client, const obrpc::ObRestoreTenantArg& arg, bool& has_job)
{
  int ret = OB_SUCCESS;
  has_job = false;
  if (OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql client is null", K(ret));
  } else {
    // prevent simultaneous restoring same tenant
    lib::ObMutexGuard guard(check_job_sync_lock_);
    ret = check_has_job_without_lock(*sql_client, arg.tenant_name_.str(), has_job);
  }
  return ret;
}

int ObRestoreUtil::check_has_job(common::ObMySQLProxy* sql_client, bool& has_job)
{
  int ret = OB_SUCCESS;
  // prevent simultaneous restoring same tenant
  common::ObSEArray<RestoreJob, 10> jobs;
  has_job = false;
  ObRestoreTableOperator restore_op;
  lib::ObMutexGuard guard(check_job_sync_lock_);
  if (OB_FAIL(restore_op.init(sql_client))) {
    LOG_WARN("fail init restore op", K(ret));
  } else if (OB_FAIL(restore_op.get_jobs(jobs))) {
    LOG_WARN("fail get jobs", K(ret));
  } else if (jobs.count() > 0) {
    has_job = true;
  }
  return ret;
}

int ObRestoreUtil::check_has_job_without_lock(
    common::ObISQLClient& sql_client, const ObString& tenant_name, bool& has_job)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<RestoreJob, 10> jobs;
  has_job = false;
  ObRestoreTableOperator restore_op;
  if (OB_FAIL(restore_op.init(&sql_client))) {
    LOG_WARN("fail init restore op", K(ret));
  } else if (OB_FAIL(restore_op.get_jobs(jobs))) {
    LOG_WARN("fail get jobs", K(ret));
  } else {
    FOREACH_CNT_X(job, jobs, !has_job)
    {
      if (job->tenant_name_ == tenant_name) {
        has_job = true;
      }
    }
  }
  return ret;
}

/*-------------- physical restore --------------------------*/
int ObRestoreUtil::fill_physical_restore_job(
    const int64_t job_id, const obrpc::ObPhysicalRestoreTenantArg& arg, ObPhysicalRestoreJob& job)
{
  int ret = OB_SUCCESS;
  if (job_id < 0 || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(job_id), K(arg));
  } else {
    job.reset();
    job.job_id_ = job_id;
    job.status_ = PhysicalRestoreStatus::PHYSICAL_RESTORE_CREATE_TENANT;
    job.restore_timestamp_ = arg.restore_timestamp_;
    job.incarnation_ = OB_START_INCARNATION;  // TODO:() should get from restore option
    STRNCPY(job.tenant_name_, arg.tenant_name_.ptr(), common::OB_MAX_TENANT_NAME_LENGTH_STORE);
    job.tenant_name_[common::OB_MAX_TENANT_NAME_LENGTH_STORE - 1] = '\0';
    STRNCPY(job.backup_tenant_name_, arg.backup_tenant_name_.ptr(), common::OB_MAX_TENANT_NAME_LENGTH_STORE);
    job.backup_tenant_name_[common::OB_MAX_TENANT_NAME_LENGTH_STORE - 1] = '\0';
    // check uri
    if (OB_SUCC(ret)) {
      ObBackupDest backup_dest;
      if (OB_FAIL(backup_dest.set(arg.uri_.ptr()))) {
        LOG_WARN("uri is invalid", K(ret), K(arg), K(job_id));
      } else if (!backup_dest.is_valid()) {
        ret = OB_URI_ERROR;
        LOG_WARN("uri is invalid", K(ret), K(arg), K(job_id));
      } else {
        STRNCPY(job.backup_dest_, arg.uri_.ptr(), share::OB_MAX_BACKUP_DEST_LENGTH);
        job.backup_dest_[share::OB_MAX_BACKUP_DEST_LENGTH - 1] = '\0';
      }
    }
    // check restore option
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObPhysicalRestoreOptionParser::parse(arg.restore_option_, job))) {
        LOG_WARN("fail to parse restore_option", K(ret), K(arg), K(job_id));
      } else {
        STRNCPY(job.restore_option_, arg.restore_option_.ptr(), common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH);
        job.restore_option_[common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH - 1] = '\0';
      }
    }

    if (FAILEDx(databuff_printf(job.passwd_array_,
            sizeof(job.passwd_array_),
            "%.*s",
            arg.passwd_array_.length(),
            arg.passwd_array_.ptr()))) {
      LOG_WARN("failed to copy passwd array", K(ret), K(arg));
    }
  }

  LOG_INFO("finish fill_physical_restore_job", K(job_id), K(arg), K(job));
  return ret;
}

int ObRestoreUtil::record_physical_restore_job(common::ObISQLClient& sql_client, const ObPhysicalRestoreJob& job)
{
  int ret = OB_SUCCESS;
  if (!job.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(job));
  } else {
    bool has_job = false;
    ObPhysicalRestoreTableOperator restore_op;
    if (OB_FAIL(check_has_physical_restore_job(sql_client, job.tenant_name_, has_job))) {
      LOG_WARN("fail to check if job exist", K(ret), K(job));
    } else if (has_job) {
      ret = OB_RESTORE_IN_PROGRESS;
      LOG_WARN("restore tenant job already exist", K(ret), K(job));
    } else if (OB_FAIL(restore_op.init(&sql_client))) {
      LOG_WARN("fail init restore op", K(ret));
    } else if (OB_FAIL(restore_op.insert_job(job))) {
      LOG_WARN("fail insert job and partitions", K(ret), K(job));
    }
  }
  return ret;
}

int ObRestoreUtil::check_has_physical_restore_job(
    common::ObISQLClient& sql_client, const ObString& tenant_name, bool& has_job)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObPhysicalRestoreJob, 10> jobs;
  has_job = false;
  ObPhysicalRestoreTableOperator restore_op;
  if (OB_FAIL(restore_op.init(&sql_client))) {
    LOG_WARN("fail init restore op", K(ret));
  } else if (OB_FAIL(restore_op.get_jobs(jobs))) {
    LOG_WARN("fail get jobs", K(ret));
  } else {
    int64_t len = common::OB_MAX_TENANT_NAME_LENGTH_STORE;
    FOREACH_CNT_X(job, jobs, !has_job)
    {
      if (0 == STRNCMP(job->tenant_name_, tenant_name.ptr(), len)) {
        has_job = true;
      }
    }
  }
  return ret;
}
