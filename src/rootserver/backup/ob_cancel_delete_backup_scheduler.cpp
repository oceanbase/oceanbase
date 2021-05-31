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

#define USING_LOG_PREFIX SERVER

#include "ob_cancel_delete_backup_scheduler.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/backup/ob_backup_struct.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/backup/ob_tenant_backup_clean_info_updater.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace share;
namespace rootserver {

ObCancelDeleteBackupScheduler::ObCancelDeleteBackupScheduler()
    : is_inited_(false), tenant_id_(OB_INVALID_ID), proxy_(NULL), backup_data_clean_(NULL)
{}

ObCancelDeleteBackupScheduler::~ObCancelDeleteBackupScheduler()
{}

int ObCancelDeleteBackupScheduler::init(
    const uint64_t tenant_id, common::ObMySQLProxy& proxy, rootserver::ObBackupDataClean* backup_data_clean)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup cancel scheduler init twice", K(ret), K(is_inited_));
  } else if (OB_INVALID_ID == tenant_id || OB_ISNULL(backup_data_clean)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup cancel scheduler get invalid argument", K(ret), K(tenant_id), KP(backup_data_clean));
  } else {
    tenant_id_ = tenant_id;
    proxy_ = &proxy;
    backup_data_clean_ = backup_data_clean;
    is_inited_ = true;
  }
  return ret;
}

int ObCancelDeleteBackupScheduler::start_schedule_cacel_delete_backup()
{
  // TODO(yanfeng) cancel backup backup delete
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("cancel delete backup scheduler do not init", K(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id_) {
    // TODO() supported it later
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("now not supported tenant cancel delete backup", K(ret), K(tenant_id_));
  } else if (OB_FAIL(inner_schedule_cancel_delete_backup())) {
    LOG_WARN("failed to schedule cancel delete backup", K(ret));
  }
  return ret;
}

int ObCancelDeleteBackupScheduler::inner_schedule_cancel_delete_backup()
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  share::ObTenantBackupCleanInfoUpdater updater;
  ObBackupCleanInfo tenant_backup_clean_info;
  tenant_backup_clean_info.tenant_id_ = tenant_id_;
  ObBackupCleanInfo dest_tenant_backup_clean_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cancel delete backup scheduler do not init", K(ret));
  } else if (OB_FAIL(trans.start(proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else {
    if (OB_FAIL(updater.init(trans))) {
      LOG_WARN("failed to init tenant backup clean info updater", K(ret));
    } else if (OB_FAIL(updater.get_backup_clean_info(tenant_id_, tenant_backup_clean_info))) {
      LOG_WARN("failed to get backup clean info", K(ret), K(tenant_id_), K(tenant_backup_clean_info));
      if (OB_BACKUP_CLEAN_INFO_NOT_EXIST == ret) {
        ret = OB_CANCEL_DELETE_BACKUP_NOT_ALLOWED;
      }
    } else if (ObBackupCleanInfoStatus::STOP == tenant_backup_clean_info.status_) {
      ret = OB_CANCEL_DELETE_BACKUP_NOT_ALLOWED;
      LOG_WARN("backup clean not start, can not stopped it", K(ret), K(tenant_backup_clean_info));
    } else {
      const int32_t result = tenant_backup_clean_info.result_;
      dest_tenant_backup_clean_info = tenant_backup_clean_info;
      dest_tenant_backup_clean_info.status_ = ObBackupCleanInfoStatus::CANCEL;
      dest_tenant_backup_clean_info.result_ = OB_SUCCESS == result ? OB_CANCELED : result;
      if (OB_FAIL(
              updater.update_backup_clean_info(tenant_id_, tenant_backup_clean_info, dest_tenant_backup_clean_info))) {
        LOG_WARN("failed to update backup clean info",
            K(ret),
            K(tenant_id_),
            K(tenant_backup_clean_info),
            K(dest_tenant_backup_clean_info));
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

    if (OB_SUCC(ret)) {
      backup_data_clean_->wakeup();
    }
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
