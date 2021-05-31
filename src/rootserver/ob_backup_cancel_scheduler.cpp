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

#include "ob_backup_cancel_scheduler.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "observer/ob_sql_client_decorator.h"
#include "lib/string/ob_sql_string.h"
#include "ob_root_backup.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace share;
namespace rootserver {

ObBackupCancelScheduler::ObBackupCancelScheduler()
    : is_inited_(false), tenant_id_(OB_INVALID_ID), proxy_(NULL), root_backup_(NULL)
{}

ObBackupCancelScheduler::~ObBackupCancelScheduler()
{}

int ObBackupCancelScheduler::init(
    const uint64_t tenant_id, common::ObMySQLProxy& proxy, rootserver::ObRootBackup* root_backup)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup cancel scheduler init twice", K(ret), K(is_inited_));
  } else if (OB_INVALID_ID == tenant_id || OB_ISNULL(root_backup)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup cancel scheduler get invalid argument", K(ret), K(tenant_id), KP(root_backup));
  } else {
    tenant_id_ = tenant_id;
    proxy_ = &proxy;
    root_backup_ = root_backup;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupCancelScheduler::start_schedule_backup_cancel()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObBackupInfoManager info_manager;
  ObBaseBackupInfoStruct backup_info;
  ObBaseBackupInfoStruct dest_backup_info;
  share::ObBackupItemTransUpdater updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler do not init", K(ret));
  } else if (OB_FAIL(tenant_ids.push_back(tenant_id_))) {
    LOG_WARN("failed to push tenant id into array", K(ret), K(tenant_id_));
  } else if (OB_FAIL(info_manager.init(tenant_ids, *proxy_))) {
    LOG_WARN("failed to init info manager", K(ret), K(tenant_ids));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("failed to start trans", K(ret), K(tenant_id_));
  } else {
    if (OB_FAIL(info_manager.get_backup_info(tenant_id_, updater, backup_info))) {
      LOG_WARN("failed to get backup info", K(ret), K(tenant_id_));
    } else if (backup_info.backup_status_.is_stop_status()) {
      ret = OB_CANNOT_CANCEL_STOPPED_BACKUP;
      LOG_WARN("backup is in stoped status ,can not cancel", K(ret), K(backup_info));
    } else if (backup_info.backup_status_.is_prepare_status() || backup_info.backup_status_.is_cleanup_status()) {
      ret = OB_CANCEL_BACKUP_NOT_ALLOWED;
      LOG_WARN("can not cancel backup task", K(ret), K(backup_info));
    } else {
      dest_backup_info = backup_info;
      dest_backup_info.backup_status_.set_backup_status_cancel();
      if (OB_FAIL(info_manager.update_backup_info(tenant_id_, dest_backup_info, updater))) {
        LOG_WARN("failed to update backup info", K(ret), K(tenant_id_), K(dest_backup_info));
      } else {
        root_backup_->wakeup();
      }
    }
    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
