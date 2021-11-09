// Copyright (c) 2021 OceanBase Inc. All Rights Reserved.
//
// Author:
//    yangyi.yyy <yangyi.yyy@antgroup.com>

#define USING_LOG_PREFIX RS

#include "rootserver/backup/ob_cancel_backup_backup_scheduler.h"
#include "rootserver/backup/ob_backup_backupset.h"
#include "rootserver/backup/ob_backup_archive_log_scheduler.h"
#include "share/backup/ob_backup_backupset_operator.h"
#include "share/backup/ob_backup_backuppiece_operator.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace rootserver {

ObCancelBackupBackupScheduler::ObCancelBackupBackupScheduler()
    : is_inited_(false),
      tenant_id_(OB_INVALID_ID),
      sql_proxy_(NULL),
      cancel_type_(CANCEL_MAX),
      backup_backupset_(NULL),
      backup_backuppiece_(NULL)
{}

ObCancelBackupBackupScheduler::~ObCancelBackupBackupScheduler()
{}

int ObCancelBackupBackupScheduler::init(const uint64_t tenant_id, common::ObMySQLProxy& sql_proxy,
    ObCancelBackupBackupType cancel_type, rootserver::ObBackupBackupset* backup_backupset,
    rootserver::ObBackupArchiveLogScheduler* backup_backuppiece)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cancel backup backup scheduler init twice", K(ret), K(is_inited_));
  } else if (OB_INVALID_ID == tenant_id || OB_ISNULL(backup_backupset) || OB_ISNULL(backup_backuppiece)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cancel backup backup scheduler get invalid argument",
        K(ret),
        K(tenant_id),
        KP(backup_backupset),
        KP(backup_backuppiece));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = &sql_proxy;
    cancel_type_ = cancel_type;
    backup_backupset_ = backup_backupset;
    backup_backuppiece_ = backup_backuppiece;
    is_inited_ = true;
  }
  return ret;
}

int ObCancelBackupBackupScheduler::start_schedule_cancel_backup_backup()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("cancel backup backup scheduler do not init", K(ret));
  } else {
    switch (cancel_type_) {
      case CANCEL_BACKUP_BACKUPSET: {
        if (OB_FAIL(schedule_cancel_backup_backupset())) {
          LOG_WARN("failed to scheduler cancel backup backupset", K(ret));
        }
        break;
      }
      case CANCEL_BACKUP_BACKUPPIECE: {
        if (OB_FAIL(schedule_cancel_backup_backuppiece())) {
          LOG_WARN("failed to scheduler cancel backup backuppiece", K(ret));
        }
        break;
      }
      case CANCEL_MAX: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("cancel type is invalid", K(ret), K(cancel_type_));
        break;
      }
    }
  }
  return ret;
}

int ObCancelBackupBackupScheduler::schedule_cancel_backup_backupset()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObArray<ObBackupBackupsetJobInfo> job_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("cancel backup scheduler do not init", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans", KR(ret), K(tenant_id_));
  } else {
    if (OB_FAIL(ObBackupBackupsetOperator::get_all_task_items(job_infos, trans))) {
      LOG_WARN("failed to get all task items", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < job_infos.count(); ++i) {
        ObBackupBackupsetJobInfo new_job_info = job_infos.at(i);
        new_job_info.job_status_ = ObBackupBackupsetJobInfo::CANCEL;
        if (OB_FAIL(ObBackupBackupsetOperator::report_job_item(new_job_info, trans))) {
          LOG_WARN("failed to update job info to cancel", KR(ret), K(new_job_info));
        }
      }
      if (OB_SUCC(ret)) {
        backup_backupset_->wakeup();
      }
    }
    tmp_ret = trans.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to end trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObCancelBackupBackupScheduler::schedule_cancel_backup_backuppiece()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObArray<ObBackupBackupPieceJobInfo> job_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("cancel backup scheduler do not init", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans", KR(ret), K(tenant_id_));
  } else {
    if (OB_FAIL(ObBackupBackupPieceJobOperator::get_all_job_items(trans, job_infos))) {
      LOG_WARN("failed to get all task items", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < job_infos.count(); ++i) {
        ObBackupBackupPieceJobInfo new_job_info = job_infos.at(0);
        new_job_info.status_ = ObBackupBackupPieceJobInfo::CANCEL;
        if (OB_FAIL(ObBackupBackupPieceJobOperator::report_job_item(new_job_info, trans))) {
          LOG_WARN("failed to update job info to cancel", KR(ret), K(new_job_info));
        }
      }
      if (OB_SUCC(ret)) {
        backup_backuppiece_->wakeup();
      }
    }
    tmp_ret = trans.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to end trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase