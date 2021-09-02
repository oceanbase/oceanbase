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

#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/config/ob_common_config.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "observer/ob_sql_client_decorator.h"
#include "lib/string/ob_sql_string.h"
#include "rootserver/ob_backup_data_clean_scheduler.h"
#include "rootserver/ob_root_backup.h"
#include "share/backup/ob_tenant_backup_clean_info_updater.h"
#include "share/backup/ob_log_archive_backup_info_mgr.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;
using namespace obrpc;
namespace rootserver {

ObBackupDataCleanScheduler::ObBackupDataCleanScheduler()
    : is_inited_(false),
      arg_(),
      schema_service_(NULL),
      sql_proxy_(NULL),
      data_clean_(NULL),
      is_cluster_clean_(false),
      max_job_id_(0)
{}

ObBackupDataCleanScheduler::~ObBackupDataCleanScheduler()
{}

int ObBackupDataCleanScheduler::init(const ObBackupManageArg& arg, ObMultiVersionSchemaService& schema_service,
    ObMySQLProxy& sql_proxy, ObBackupDataClean* data_clean)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup data clean scheduler init twice", K(ret));
  } else if ((ObBackupManageArg::DELETE_OBSOLETE_BACKUP != arg.type_ && ObBackupManageArg::DELETE_BACKUP != arg.type_ &&
                 ObBackupManageArg::DELETE_OBSOLETE_BACKUP_BACKUP != arg.type_ &&
                 ObBackupManageArg::DELETE_BACKUPPIECE != arg.type_ &&
                 ObBackupManageArg::DELETE_BACKUPROUND != arg.type_) ||
             OB_ISNULL(data_clean)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup manager arg is invalid", K(ret), K(arg), KP(data_clean));
  } else {
    arg_ = arg;
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    data_clean_ = data_clean;
    is_cluster_clean_ = arg_.tenant_id_ == OB_SYS_TENANT_ID;
    is_inited_ = true;
    LOG_INFO("backup data scheduler init", K(arg));
  }
  return ret;
}

int ObBackupDataCleanScheduler::start_schedule_backup_data_clean()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (OB_FAIL(get_tenant_ids(tenant_ids))) {
    LOG_WARN("failed to get tenant ids", K(ret));
  } else if (OB_FAIL(prepare_backup_clean_infos(tenant_ids))) {
    LOG_WARN("failed to prepare backup clean infos", K(ret), K(tenant_ids));
  } else if (OB_FAIL(schedule_backup_data_clean(tenant_ids))) {
    LOG_WARN("failed to schedule backup data clean", K(ret), K(tenant_ids));
  }
  return ret;
}

int ObBackupDataCleanScheduler::get_tenant_ids(ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  ObArray<uint64_t> tmp_tenant_ids;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else {
    if (is_cluster_clean_) {
      // get latest schema guard
      if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
        LOG_WARN("failed to get tenant schema guard", K(ret));
      } else if (OB_FAIL(guard.get_tenant_ids(tmp_tenant_ids))) {
        LOG_WARN("failed to get tenant ids", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp_tenant_ids.count(); ++i) {
          const uint64_t tenant_id = tmp_tenant_ids.at(i);
          bool is_restore = false;
          if (OB_FAIL(guard.check_tenant_is_restore(tenant_id, is_restore))) {
            LOG_WARN("failed to check tenant is restore", K(ret), K(tenant_id));
          } else if (is_restore) {
            // do nothing
          } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
            LOG_WARN("failed to push tenant id into array", K(ret), K(tenant_id));
          }
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("backup do not support tenant backup, will support it later", K(ret));
    }
  }
  return ret;
}

// Use the latest schema_version to get the list of tenants, insert the task generation list
// If the tenant is deleted during the backup process, this record in __all_tenant_clean_info will also be cleaned up
// To clean up the backup data of the tenants that have been deleted, it needs to be combined with the list of
// tenant_ids stored externally
int ObBackupDataCleanScheduler::prepare_backup_clean_infos(const ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  int64_t max_job_id = 0;
  const bool for_update = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      ObMySQLTransaction trans;
      ObBackupCleanInfo clean_info;
      clean_info.tenant_id_ = tenant_id;
      if (OB_FAIL(trans.start(sql_proxy_))) {
        LOG_WARN("fail to start trans", K(ret));
      } else {
        if (OB_FAIL(get_backup_clean_info(tenant_id, for_update, trans, clean_info))) {
          if (OB_BACKUP_CLEAN_INFO_NOT_EXIST == ret) {
            clean_info.status_ = ObBackupCleanInfoStatus::STOP;
            clean_info.type_ = ObBackupCleanType::EMPTY_TYPE;
            clean_info.copy_id_ = arg_.copy_id_;
            // overwrite ret
            if (OB_FAIL(insert_backup_clean_info(tenant_id, clean_info, trans))) {
              LOG_WARN("failed to insert backup clean info", K(ret), K(clean_info));
            }
          } else {
            LOG_WARN("failed to get backup clean info", K(ret), K(tenant_id));
          }
        }

        if (OB_SUCC(ret)) {
          if (clean_info.job_id_ > max_job_id) {
            max_job_id = clean_info.job_id_;
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(trans.end(true /*commit*/))) {
          OB_LOG(WARN, "failed to commit", K(ret));
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
          OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      max_job_id_ = max_job_id + 1;
    }
  }
  return ret;
}

int ObBackupDataCleanScheduler::get_backup_clean_info(
    const uint64_t tenant_id, const bool for_update, ObISQLClient& sql_proxy, ObBackupCleanInfo& clean_info)
{
  int ret = OB_SUCCESS;
  ObTenantBackupCleanInfoUpdater updater;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup clean info get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.init(sql_proxy))) {
    LOG_WARN("failed to init tenant backup clean info updater", K(ret));
  } else if (OB_FAIL(updater.get_backup_clean_info(tenant_id, for_update, clean_info))) {
    if (OB_BACKUP_CLEAN_INFO_NOT_EXIST != ret) {
      LOG_WARN("failed to get backup clean info", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObBackupDataCleanScheduler::insert_backup_clean_info(
    const uint64_t tenant_id, const ObBackupCleanInfo& clean_info, ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObTenantBackupCleanInfoUpdater updater;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup clean info get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.init(sql_proxy))) {
    LOG_WARN("failed to init tenant backup clean info updater", K(ret));
  } else if (OB_FAIL(updater.insert_backup_clean_info(tenant_id, clean_info))) {
    LOG_WARN("failed to get backup clean info", K(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupDataCleanScheduler::update_backup_clean_info(
    const ObBackupCleanInfo& src_clean_info, const ObBackupCleanInfo& dest_clean_info, ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObTenantBackupCleanInfoUpdater updater;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (!src_clean_info.is_valid() || !dest_clean_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update backup clean info get invalid argument", K(ret), K(src_clean_info), K(dest_clean_info));
  } else if (OB_FAIL(updater.init(sql_proxy))) {
    LOG_WARN("failed to init tenant backup clean info updater", K(ret));
  } else if (OB_FAIL(updater.update_backup_clean_info(OB_SYS_TENANT_ID, src_clean_info, dest_clean_info))) {
    LOG_WARN("failed to update backup clean info", K(ret), K(src_clean_info), K(dest_clean_info));
  }
  return ret;
}

int ObBackupDataCleanScheduler::schedule_backup_data_clean(const ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (tenant_ids.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup infos should not be empty", K(ret), K(tenant_ids));
  } else {
    if (is_cluster_clean_) {
      if (OB_FAIL(schedule_sys_tenant_backup_data_clean())) {
        LOG_WARN("failed to schedule sys tenant backup", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("now not support tenant clean", K(ret));
    }

    // TODO() use backup lock
    data_clean_->update_prepare_flag(false /*is_prepare_flag*/);
    data_clean_->wakeup();
  }
  return ret;
}

int ObBackupDataCleanScheduler::set_backup_clean_info(const uint64_t tenant_id, ObBackupCleanInfo& clean_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (tenant_id != clean_info.tenant_id_ || ObBackupCleanInfoStatus::STOP == clean_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set backup clean info get invalid argument", K(ret), K(tenant_id), K(clean_info));
  } else {
    if (ObBackupManageArg::DELETE_OBSOLETE_BACKUP == arg_.type_) {
      clean_info.type_ = ObBackupCleanType::DELETE_OBSOLETE_BACKUP;
    } else if (ObBackupManageArg::DELETE_BACKUP == arg_.type_) {
      clean_info.type_ = ObBackupCleanType::DELETE_BACKUP_SET;
    } else if (ObBackupManageArg::DELETE_OBSOLETE_BACKUP_BACKUP == arg_.type_) {
      clean_info.type_ = ObBackupCleanType::DELETE_OBSOLETE_BACKUP_BACKUP;
    } else if (ObBackupManageArg::DELETE_BACKUPPIECE == arg_.type_) {
      clean_info.type_ = ObBackupCleanType::DELETE_BACKUP_PIECE;
    } else if (ObBackupManageArg::DELETE_BACKUPROUND == arg_.type_) {
      clean_info.type_ = ObBackupCleanType::DELETE_BACKUP_ROUND;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup clean type invalid", K(ret), K(arg_), K(tenant_id));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(clean_info.set_clean_parameter(arg_.value_))) {
        LOG_WARN("failed to set clean parameter", K(ret), K(arg_), K(clean_info));
      } else if (OB_FAIL(clean_info.set_copy_id(arg_.copy_id_))) {
        LOG_WARN("failed to set copy id", K(ret), K(arg_), K(clean_info));
      }
    }
  }
  return ret;
}

int ObBackupDataCleanScheduler::schedule_sys_tenant_backup_data_clean()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBackupCleanInfo clean_info;
  ObBackupCleanInfo dest_clean_info;
  ObMySQLTransaction trans;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  clean_info.tenant_id_ = tenant_id;
  const int64_t ERROR_MSG_LENGTH = 1024;
  char error_msg[ERROR_MSG_LENGTH] = "";
  int64_t pos = 0;
  const ObBackupFileStatus::STATUS file_status = ObBackupFileStatus::BACKUP_FILE_DELETING;
  const bool for_update = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    if (OB_FAIL(get_backup_clean_info(tenant_id, for_update, trans, clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(tenant_id));
    } else if (ObBackupCleanInfoStatus::STOP != clean_info.status_) {
      ret = OB_BACKUP_DELETE_DATA_IN_PROGRESS;
      LOG_WARN("sys tenant clean info status is not stop, can do scheduler", K(ret), K(clean_info));
    } else if (obrpc::ObBackupManageArg::DELETE_BACKUP == arg_.type_) {
      // here has error witch do not check copy id
      ObBackupTaskHistoryUpdater updater;
      ObTenantBackupTaskInfo tenant_backup_task;
      const bool for_update = false;
      ObArray<ObTenantBackupTaskInfo> tenant_backup_tasks;
      if (OB_FAIL(updater.init(trans))) {
        LOG_WARN("failed to init backup task history updater", K(ret), K(clean_info), K(arg_));
      } else if (OB_FAIL(updater.get_tenant_backup_task(
                     tenant_id, arg_.value_, arg_.copy_id_, for_update, tenant_backup_tasks))) {
        LOG_WARN("failed to get tenant backup task", K(ret), K(tenant_id), K(arg_));
      } else if (tenant_backup_tasks.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("tenant backup tasks should not be empty", K(ret), K(arg_));
        if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg,
                               ERROR_MSG_LENGTH,
                               pos,
                               "delete backupset is not exist. backup set id: %ld, copy id: %ld .",
                               arg_.value_,
                               arg_.copy_id_))) {
          LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
        } else {
          LOG_USER_ERROR(OB_NOT_SUPPORTED, error_msg);
        }
      } else if (FALSE_IT(tenant_backup_task = tenant_backup_tasks.at(tenant_backup_tasks.count() - 1))) {
      } else if (!tenant_backup_task.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant backup task should not be invalid", K(ret), K(tenant_backup_task));
      }
    } else if (obrpc::ObBackupManageArg::DELETE_BACKUPPIECE == arg_.type_) {
      ObLogArchiveBackupInfoMgr archive_info_mgr;
      const bool for_update = false;
      ObBackupPieceInfo piece_info;
      if (OB_FAIL(archive_info_mgr.get_backup_piece(
              trans, for_update, tenant_id, arg_.value_, arg_.copy_id_, piece_info))) {
        LOG_WARN("failed to get backup piece", K(ret), K(arg_));
        if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg,
                               ERROR_MSG_LENGTH,
                               pos,
                               "delete backup piece is not exist. backup piece id: %ld, copy id: %ld .",
                               arg_.value_,
                               arg_.copy_id_))) {
          LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
        } else {
          LOG_USER_ERROR(OB_NOT_SUPPORTED, error_msg);
        }
      } else if (!ObBackupUtils::can_backup_pieces_be_deleted(piece_info.status_)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("can not delete backup piece", K(ret), K(piece_info));
        if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg,
                               ERROR_MSG_LENGTH,
                               pos,
                               "backup piece can not be deleted, because status is not frozen or inactive. backup "
                               "piece id: %ld, copy id: %ld, "
                               "status : %s .",
                               arg_.value_,
                               arg_.copy_id_,
                               piece_info.get_status_str()))) {
          LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
        } else {
          LOG_USER_ERROR(OB_NOT_SUPPORTED, error_msg);
        }
      } else if (ObBackupFileStatus::BACKUP_FILE_DELETED == piece_info.file_status_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("backup piece has already deleted", K(ret), K(piece_info));
        if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg,
                               ERROR_MSG_LENGTH,
                               pos,
                               "backup piece is already deleted. backup piece id: %ld, copy id: %ld, "
                               "status : %s .",
                               arg_.value_,
                               arg_.copy_id_,
                               piece_info.get_status_str()))) {
          LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
        } else {
          LOG_USER_ERROR(OB_NOT_SUPPORTED, error_msg);
        }
      } else if (OB_FAIL(ObBackupFileStatus::check_can_change_status(piece_info.file_status_, file_status))) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("failed to check can change file status", K(ret), K(piece_info), K(file_status));
        if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg,
                               ERROR_MSG_LENGTH,
                               pos,
                               "backup piece can not be deleted, because file status is unexpected. backup piece id: "
                               "%ld, copy id: %ld, "
                               "status : %s, file_status : %s",
                               arg_.value_,
                               arg_.copy_id_,
                               piece_info.get_status_str(),
                               ObBackupFileStatus::get_str(piece_info.file_status_)))) {
          LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
        } else {
          LOG_USER_ERROR(OB_NOT_SUPPORTED, error_msg);
        }
      }
    } else if (obrpc::ObBackupManageArg::DELETE_BACKUPROUND == arg_.type_) {
      ObLogArchiveBackupInfoMgr archive_info_mgr;
      const bool for_update = false;
      ObLogArchiveBackupInfo archive_backup_info;
      if (OB_FAIL(archive_info_mgr.get_log_archive_history_info(
              trans, tenant_id, arg_.value_, arg_.copy_id_, for_update, archive_backup_info))) {
        LOG_WARN("failed to get backup piece", K(ret), K(arg_));
        if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg,
                               ERROR_MSG_LENGTH,
                               pos,
                               "backup round is not exist. backup round id: %ld, copy id: %ld .",
                               arg_.value_,
                               arg_.copy_id_))) {
          LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
        } else {
          LOG_USER_ERROR(OB_NOT_SUPPORTED, error_msg);
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      dest_clean_info = clean_info;
      dest_clean_info.status_ = ObBackupCleanInfoStatus::PREPARE;
      // TODO() incarnation
      dest_clean_info.incarnation_ = OB_START_INCARNATION;
      dest_clean_info.job_id_ = max_job_id_;
      dest_clean_info.start_time_ = ObTimeUtil::current_time();
      if (OB_FAIL(set_backup_clean_info(tenant_id, dest_clean_info))) {
        LOG_WARN("failed to set backup clean info", K(ret), K(dest_clean_info));
      } else if (OB_FAIL(update_backup_clean_info(clean_info, dest_clean_info, trans))) {
        LOG_WARN("failed to update backup clean info", K(ret), K(clean_info), K(dest_clean_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupDataCleanScheduler::schedule_tenants_backup_data_clean(const common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  ObBackupCleanInfo sys_clean_info;
  ObMySQLTransaction trans;
  const bool for_update = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (tenant_ids.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schedule get tenants backup get invalid argument", K(ret), K(tenant_ids));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    if (OB_FAIL(get_backup_clean_info(OB_SYS_TENANT_ID, for_update, trans, sys_clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(sys_clean_info));
    } else if ((is_cluster_clean_ && ObBackupCleanInfoStatus::PREPARE != sys_clean_info.status_) ||
               (!is_cluster_clean_ && ObBackupCleanInfoStatus::STOP != sys_clean_info.status_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys backup info status is unexpected", K(ret), K(sys_clean_info));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
        const uint64_t tenant_id = tenant_ids.at(i);
        if (OB_SYS_TENANT_ID == tenant_id) {
          // do nothing
        } else if (OB_FAIL(schedule_tenant_backup_data_clean(tenant_id, trans))) {
          LOG_WARN("failed to schedule tenant backup", K(ret), K(tenant_id));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupDataCleanScheduler::schedule_tenant_backup_data_clean(
    const uint64_t tenant_id, ObISQLClient& sys_tenant_trans)
{
  int ret = OB_SUCCESS;
  ObBackupCleanInfo clean_info;
  ObBackupCleanInfo dest_clean_info;
  ObMySQLTransaction trans;
  const bool for_update = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id should be sys tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    if (OB_FAIL(get_backup_clean_info(tenant_id, for_update, trans, clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(tenant_id));
    } else if (ObBackupCleanInfoStatus::STOP != clean_info.status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("clean info status is unexpected", K(ret), K(clean_info));
    } else {
      dest_clean_info = clean_info;
      dest_clean_info.status_ = ObBackupCleanInfoStatus::DOING;
      dest_clean_info.incarnation_ = OB_START_INCARNATION;
      dest_clean_info.job_id_ = max_job_id_;
      if (OB_FAIL(rootserver::ObBackupUtil::check_sys_clean_info_trans_alive(sys_tenant_trans))) {
        LOG_WARN("failed to check sys tenant trans alive", K(ret), K(clean_info));
      } else if (OB_FAIL(set_backup_clean_info(tenant_id, dest_clean_info))) {
        LOG_WARN("failed to set backup clean info", K(ret), K(tenant_id), K(dest_clean_info));
      } else if (OB_FAIL(update_backup_clean_info(clean_info, dest_clean_info, trans))) {
        LOG_WARN("failed to update backup clean info", K(ret), K(clean_info), K(dest_clean_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupDataCleanScheduler::start_backup_clean()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (!is_cluster_clean_) {
    // do nothing
  } else {
    ObBackupCleanInfo clean_info;
    ObBackupCleanInfo dest_clean_info;
    ObMySQLTransaction trans;
    const uint64_t tenant_id = OB_SYS_TENANT_ID;
    const bool for_update = true;
    if (OB_FAIL(trans.start(sql_proxy_))) {
      LOG_WARN("fail to start trans", K(ret));
    } else {
      if (OB_FAIL(get_backup_clean_info(tenant_id, for_update, trans, clean_info))) {
        LOG_WARN("failed to get backup clean info", K(ret), K(clean_info));
      } else if (ObBackupCleanInfoStatus::PREPARE != clean_info.status_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant backup clean info status is unexpected", K(ret), K(clean_info));
      } else {
        dest_clean_info = clean_info;
        dest_clean_info.status_ = ObBackupCleanInfoStatus::PREPARE;
        if (OB_FAIL(update_backup_clean_info(clean_info, dest_clean_info, trans))) {
          LOG_WARN("failed to update backup clean info", K(ret), K(clean_info), K(dest_clean_info));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(trans.end(true /*commit*/))) {
          OB_LOG(WARN, "failed to commit", K(ret));
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
          OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObBackupDataCleanScheduler::rollback_backup_clean_infos(const common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else {
    for (int64_t i = tenant_ids.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (OB_FAIL(rollback_backup_clean_info(tenant_id))) {
        LOG_WARN("failed to rollback backup info", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObBackupDataCleanScheduler::rollback_backup_clean_info(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObBackupCleanInfo clean_info;
  ObBackupCleanInfo dest_clean_info;
  ObMySQLTransaction trans;
  const bool for_update = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data clean scheduler do not init", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    if (OB_FAIL(get_backup_clean_info(tenant_id, for_update, trans, clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(tenant_id));
    } else if (ObBackupCleanInfoStatus::STOP == clean_info.status_) {
      // do nothing
    } else {
      dest_clean_info.tenant_id_ = tenant_id;
      dest_clean_info.status_ = ObBackupCleanInfoStatus::STOP;
      if (OB_FAIL(update_backup_clean_info(clean_info, dest_clean_info, trans))) {
        LOG_WARN("failed to update backup clean info", K(ret), K(clean_info), K(dest_clean_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        OB_LOG(WARN, "failed to commit", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        OB_LOG(WARN, "failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
