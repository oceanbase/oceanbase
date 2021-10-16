// Copyright 2020 Alibaba Inc. All Rights Reserved
// Author:
//     yanfeng <yangyi.yyy@alibaba-inc.com>
// Normalizer:
//     yanfeng <yangyi.yyy@alibaba-inc.com

#define USING_LOG_PREFIX RS
#include "rootserver/backup/ob_backup_backupset_scheduler.h"
#include "share/backup/ob_tenant_backup_task_updater.h"
#include "share/backup/ob_backup_backupset_operator.h"
#include "share/backup/ob_backup_manager.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace rootserver {

ObBackupBackupsetScheduler::ObBackupBackupsetScheduler()
    : is_inited_(false),
      tenant_id_(OB_INVALID_ID),
      backup_set_id_(-1),
      max_backup_times_(-1),
      backup_dest_(),
      sql_proxy_(NULL),
      backup_backupset_(NULL)
{}

int ObBackupBackupsetScheduler::init(const uint64_t tenant_id, const int64_t backup_set_id,
    const int64_t max_backup_times, const common::ObString& backup_dest_str, ObMySQLProxy& sql_proxy,
    ObBackupBackupset& backup_backupset)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup backupset scheduler init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup backupset scheduler get invalid argument", KR(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(backup_dest_.set(backup_dest_str.ptr()))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(backup_dest_str));
  } else {
    tenant_id_ = tenant_id;
    backup_set_id_ = backup_set_id;
    max_backup_times_ = max_backup_times;
    sql_proxy_ = &sql_proxy;
    backup_backupset_ = &backup_backupset;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupBackupsetScheduler::start_schedule_backup_backupset()
{
  int ret = OB_SUCCESS;
  ObBackupInfoManager manager;
  int64_t job_id = 0;
  bool has_doing_job = false;
  bool is_valid = true;
  bool is_greater = true;
  bool is_same_dest = true;
  bool is_backup_dest_valid = true;
  int64_t real_backup_set_id = backup_set_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset scheduler do not init", K(ret));
  } else if (OB_FAIL(check_backup_dest_is_same(is_same_dest))) {
    LOG_WARN("failed to check backup dest is same", KR(ret));
  } else if (is_same_dest) {
    ret = OB_BACKUP_BACKUP_CAN_NOT_START;
    LOG_WARN("same backup dest", KR(ret), K(is_same_dest));
  } else if (OB_FAIL(check_has_doing_job(has_doing_job))) {
    LOG_WARN("failed to check has doing job", KR(ret));
  } else if (has_doing_job) {
    ret = OB_EAGAIN;
    LOG_WARN("has doing backup backupset job now, try again later", KR(ret));
  } else if (OB_FAIL(check_backup_set_id_valid(tenant_id_, backup_set_id_, is_valid))) {
    LOG_WARN("failed to check backup set id is valid", KR(ret), K(tenant_id_), K(backup_set_id_));
  } else if (!is_valid) {
    ret = OB_BACKUP_BACKUP_CAN_NOT_START;
    LOG_WARN("backup set id is not valid", KR(ret), K(tenant_id_), K(backup_set_id_));
  } else if (OB_FAIL(get_largest_backup_set_id_if_all(real_backup_set_id))) {
    LOG_WARN("failed to get largest backup set id if all", KR(ret));
  } else if (OB_FAIL(check_backup_backup_dest_is_valid(real_backup_set_id, is_backup_dest_valid))) {
    LOG_WARN("failed to check backup backup dest changed", KR(real_backup_set_id));
  } else if (!is_backup_dest_valid) {
    ret = OB_BACKUP_BACKUP_CAN_NOT_START;
    LOG_WARN("backup dest is not valid", KR(ret), K(backup_dest_));
  } else if (OB_FAIL(check_is_greater_then_existing_backup_set_id(real_backup_set_id, is_greater))) {
    LOG_WARN("failed to check is greater then existing backup set id", KR(ret), K(real_backup_set_id));
  } else if (!is_greater) {
    ret = OB_BACKUP_BACKUP_CAN_NOT_START;
    LOG_ERROR("can not start backup backupset because this backup set id is smaller then existing backup id",
        KR(ret),
        K(is_greater));
  } else if (OB_FAIL(manager.init(OB_SYS_TENANT_ID, *sql_proxy_))) {
    LOG_WARN("failed to init backup info manager", KR(ret));
  } else if (OB_FAIL(manager.get_job_id(job_id))) {
    LOG_WARN("failed to get job id", KR(ret));
  } else if (OB_FAIL(insert_backup_backupset_job(job_id, real_backup_set_id))) {
    LOG_WARN("failed to insert backup backupset job info", KR(ret), K(job_id), K(real_backup_set_id));
  } else {
    backup_backupset_->wakeup();
  }
  return ret;
}

int ObBackupBackupsetScheduler::check_backup_dest_is_same(bool& same)
{
  int ret = OB_SUCCESS;
  same = false;
  char backup_dest[OB_MAX_BACKUP_DEST_LENGTH] = "";
  char backup_backup_dest[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset scheduler do not init", K(ret));
  } else if (OB_FAIL(GCONF.backup_dest.copy(backup_dest, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to copy backup dest", KR(ret));
  } else if (OB_FAIL(GCONF.backup_backup_dest.copy(backup_backup_dest, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to copy backup dest", KR(ret));
  } else {
    same = 0 == STRNCMP(backup_dest, backup_backup_dest, OB_MAX_BACKUP_DEST_LENGTH);
    if (same) {
      LOG_WARN("backup dest and backup backup dest is same", K(backup_dest), K(backup_backup_dest));
    }
  }
  return ret;
}

int ObBackupBackupsetScheduler::check_has_doing_job(bool& has)
{
  int ret = OB_SUCCESS;
  has = false;
  ObArray<ObBackupBackupsetJobInfo> job_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset scheduler do not init", K(ret));
  } else if (OB_FAIL(ObBackupBackupsetOperator::get_all_task_items(job_infos, *sql_proxy_))) {
    LOG_WARN("failed to get all task items", KR(ret));
  } else if (!job_infos.empty()) {
    has = true;
  }
  return ret;
}

int ObBackupBackupsetScheduler::check_backup_set_id_valid(
    const uint64_t tenant_id, const int64_t backup_set_id, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObTenantBackupTaskInfo task_info;
  ObBackupTaskHistoryUpdater updater;
  const bool for_update = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset scheduler do not init", K(ret));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup task history updater", K(ret));
  } else if (OB_ALL_BACKUP_SET_ID == backup_set_id) {
    ObArray<ObTenantBackupTaskInfo> task_infos;
    if (OB_FAIL(updater.get_tenant_full_backup_tasks(OB_SYS_TENANT_ID, task_infos))) {
      LOG_WARN("failed to get all tenant backup tasks", KR(ret));
    } else if (task_infos.empty()) {
      is_valid = false;
    }
  } else if (OB_FAIL(updater.get_original_tenant_backup_task(tenant_id, backup_set_id, for_update, task_info))) {
    if (OB_INVALID_BACKUP_SET_ID == ret) {
      is_valid = false;
      ret = OB_SUCCESS;
    }
  } else if (0 != task_info.result_) {
    is_valid = false;
    LOG_WARN("the corresponding backup set id is not success, can not start backup backupset", KR(ret), K(task_info));
  }
  return ret;
}

int ObBackupBackupsetScheduler::get_largest_backup_set_id_if_all(int64_t& backup_set_id)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskInfo task_info;
  ObBackupTaskHistoryUpdater updater;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset scheduler do not init", K(ret));
  } else if (OB_ALL_BACKUP_SET_ID != backup_set_id_) {
    // do nothing if not all
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup task history updater", KR(ret));
  } else if (OB_FAIL(updater.get_tenant_max_succeed_backup_task(tenant_id_, task_info))) {
    LOG_WARN("failed to get tenant max succeed backup task", KR(ret), K(tenant_id_));
  } else {
    backup_set_id = task_info.backup_set_id_;
  }
  return ret;
}

int ObBackupBackupsetScheduler::check_backup_backup_dest_is_valid(const int64_t backup_set_id, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObArray<ObTenantBackupBackupsetTaskInfo> tasks;
  const bool is_tenant_level = OB_SYS_TENANT_ID != tenant_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset scheduler do not init", K(ret));
  } else if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::get_same_backup_set_id_tasks(
                 is_tenant_level, tenant_id_, backup_set_id, tasks, *sql_proxy_))) {
    LOG_WARN("failed to get all tasks same backup set id", K(backup_set_id));
  } else if (tasks.empty()) {
    is_valid = true;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      const ObTenantBackupBackupsetTaskInfo& task = tasks.at(i);
      if (OB_SUCCESS != task.result_) {
        continue;
      } else if (task.dst_backup_dest_.is_root_path_equal(backup_dest_)) {
        is_valid = false;
        break;
      }
    }
  }
  return ret;
}

int ObBackupBackupsetScheduler::build_backup_backupset_job_info(
    const int64_t job_id, const int64_t real_backup_set_id, share::ObBackupBackupsetJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset scheduler do not init", K(ret));
  } else {
    job_info.job_id_ = job_id;
    job_info.tenant_id_ = tenant_id_;
    job_info.incarnation_ = OB_START_INCARNATION;
    job_info.backup_set_id_ = real_backup_set_id;
    job_info.type_.type_ = OB_ALL_BACKUP_SET_ID == backup_set_id_ ? ObBackupBackupsetType::ALL_BACKUP_SET
                                                                  : ObBackupBackupsetType::SINGLE_BACKUP_SET;
    job_info.job_status_ = ObBackupBackupsetJobInfo::SCHEDULE;
    job_info.max_backup_times_ = max_backup_times_;
    job_info.backup_dest_ = backup_dest_;
  }
  return ret;
}

int ObBackupBackupsetScheduler::insert_backup_backupset_job(const int64_t job_id, const int64_t real_backup_set_id)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObBackupBackupsetJobInfo job_info;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset scheduler do not init", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans", KR(ret));
  } else {
    if (OB_FAIL(build_backup_backupset_job_info(job_id, real_backup_set_id, job_info))) {
      LOG_WARN("failed to build backup backupset job info", KR(ret), K(job_id), K(real_backup_set_id));
    } else if (OB_FAIL(ObBackupBackupsetOperator::insert_job_item(job_info, trans))) {
      LOG_WARN("failed to insert backup backupset job info", KR(ret), K(job_info));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        LOG_WARN("failed to commit", KR(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        LOG_WARN("failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupBackupsetScheduler::check_is_greater_then_existing_backup_set_id(const int64_t backup_set_id, bool& greater)
{
  int ret = OB_SUCCESS;
  greater = true;
  ObArray<ObBackupBackupsetJobInfo> job_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset scheduler do not init", K(ret));
  } else if (OB_FAIL(ObBackupBackupsetHistoryOperator::get_all_task_items(job_infos, *sql_proxy_))) {
    LOG_WARN("failed to get all task items", KR(ret), K(backup_set_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < job_infos.count(); ++i) {
      const ObBackupBackupsetJobInfo& job_info = job_infos.at(i);
      if (OB_SUCCESS != job_info.result_) {
        continue;
      } else if (job_info.backup_set_id_ > backup_set_id && job_info.backup_dest_.is_root_path_equal(backup_dest_)) {
        greater = false;
        break;
      }
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
