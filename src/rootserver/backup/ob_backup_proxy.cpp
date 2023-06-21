/**
 * Copyright (c) 2022 OceanBase
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

#include "ob_backup_proxy.h"
#include "ob_backup_service.h"
#include "ob_archive_scheduler_service.h"
#include "share/ob_rpc_struct.h"

using namespace oceanbase;
using namespace rootserver;
using namespace obrpc;

int ObBackupServiceProxy::handle_backup_database(const obrpc::ObBackupDatabaseArg &arg)
{
  int ret = OB_SUCCESS;
  ObBackupDataService *backup_service = nullptr;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  if (OB_FAIL(guard.switch_to(OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to switch to sys tenant", K(ret));
  } else if (OB_ISNULL(backup_service = MTL(ObBackupDataService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup service must not be nullptr", K(ret));
  } else if (OB_FAIL(backup_service->handle_backup_database(arg))) {
    LOG_WARN("failed handle backup database", K(ret));
  }
  return ret;
}

int ObBackupServiceProxy::handle_backup_database_cancel(const obrpc::ObBackupManageArg &arg)
{
  int ret = OB_SUCCESS;
  ObBackupDataService *backup_service = nullptr;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  if (ObBackupManageArg::CANCEL_BACKUP != arg.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("handle backup database cancel get invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(guard.switch_to(OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to switch to sys tenant", K(ret));
  } else if (OB_ISNULL(backup_service = MTL(ObBackupDataService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup service must not be nullptr", K(ret));
  } else if (OB_FAIL(backup_service->handle_backup_database_cancel(arg.tenant_id_, arg.managed_tenant_ids_))) {
    LOG_WARN("failed handle backup database", K(ret));
  }
  return ret;
}

int ObBackupServiceProxy::handle_backup_delete(const obrpc::ObBackupCleanArg &arg)
{
  int ret = OB_SUCCESS;
  ObBackupCleanService *backup_service = nullptr;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  if (OB_FAIL(guard.switch_to(OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to switch to sys tenant", K(ret));
  } else if (OB_ISNULL(backup_service = MTL(ObBackupCleanService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup service must not be nullptr", K(ret));
  } else if (OB_FAIL(backup_service->handle_backup_delete(arg))) {
    LOG_WARN("failed handle backup database", K(ret));
  }
  return ret;
}

int ObBackupServiceProxy::handle_delete_policy(const obrpc::ObDeletePolicyArg &arg)
{
  int ret = OB_SUCCESS;
  ObBackupCleanService *backup_service = nullptr;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  if (OB_FAIL(guard.switch_to(OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to switch to sys tenant", K(ret));
  } else if (OB_ISNULL(backup_service = MTL(ObBackupCleanService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup service must not be nullptr", K(ret));
  } else if (OB_FAIL(backup_service->handle_delete_policy(arg))) {
    LOG_WARN("failed handle backup database", K(ret));
  }
  return ret;
}

int ObBackupServiceProxy::handle_archive_log(const obrpc::ObArchiveLogArg &arg)
{
  int ret = OB_SUCCESS;
  ObArchiveSchedulerService *archive_service = nullptr;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  if (OB_FAIL(guard.switch_to(OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to switch to sys tenant", K(ret));
  } else if (OB_ISNULL(archive_service = MTL(ObArchiveSchedulerService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup service must not be nullptr", K(ret));
  } else if (arg.enable_ && OB_FAIL(archive_service->open_archive_mode(arg.tenant_id_, arg.archive_tenant_ids_))) {
    LOG_WARN("failed to start archive", K(ret), K(arg));
  } else if (!arg.enable_ && OB_FAIL(archive_service->close_archive_mode(arg.tenant_id_, arg.archive_tenant_ids_))) {
    LOG_WARN("failed to stop archive", K(ret), K(arg));
  }
  return ret;
}
