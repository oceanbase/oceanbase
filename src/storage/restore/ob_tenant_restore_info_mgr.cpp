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

#define USING_LOG_PREFIX STORAGE
#include "storage/restore/ob_tenant_restore_info_mgr.h"
#include "share/restore/ob_physical_restore_table_operator.h"
#include "share/backup/ob_backup_connectivity.h"
#include "rootserver/ob_tenant_info_loader.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

ObTenantBackupDestInfoMgr::ObTenantBackupDestInfoMgr()
  : is_inited_(false),
    mutex_(common::ObLatchIds::OB_TENANT_RESTORE_INFO_MGR_LOCK),
    tenant_id_(OB_INVALID_TENANT_ID),
    backup_set_list_(),
    dest_id_(OB_INVALID_DEST_ID),
    type_(InfoType::NONE)
{
}

ObTenantBackupDestInfoMgr::~ObTenantBackupDestInfoMgr()
{
  destroy();
}

int ObTenantBackupDestInfoMgr::mtl_init(ObTenantBackupDestInfoMgr *&restore_info_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(restore_info_mgr->init(MTL_ID()))) {
    LOG_WARN("failed to init tenant dest info mgr", K(ret), K(MTL_ID()));
  } else {
    LOG_INFO("success to init ObTenantBackupDestInfoMgr", K(MTL_ID()));
  }
  return ret;
}

int ObTenantBackupDestInfoMgr::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantBackupDestInfoMgr init twice", K(ret));
  }  else {
    backup_set_list_.set_tenant_id(tenant_id);
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantBackupDestInfoMgr::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant dest info mgr not inited", K(ret), K_(tenant_id));
  } else {
    LOG_INFO("tenant dest info mgr start", K_(tenant_id));
  }
  return ret;
}

void ObTenantBackupDestInfoMgr::stop()
{
  LOG_INFO("tenant dest info mgr stop", K_(tenant_id));
}

void ObTenantBackupDestInfoMgr::wait()
{
  LOG_INFO("tenant dest info mgr wait", K_(tenant_id));
}

void ObTenantBackupDestInfoMgr::destroy()
{
  LOG_INFO("tenant dest info mgr destroy", K_(tenant_id));
}

int ObTenantBackupDestInfoMgr::set_backup_set_info(
    const common::ObIArray<share::ObBackupSetBriefInfo> &backup_set_list,
    const int64_t dest_id,
    const InfoType type)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (InfoType::VALIDATE == type_) {
    ret = OB_EAGAIN;
    LOG_WARN("type not match, cannot update", K(ret), K(type_), K(type));
  } else {
    backup_set_list_.reset();
    if (OB_FAIL(backup_set_list_.assign(backup_set_list))) {
      LOG_WARN("failed to assign backup set list", K(ret));
    } else {
      dest_id_ = dest_id;
      type_ = type;
    }
  }
  return ret;
}

void ObTenantBackupDestInfoMgr::reset()
{
  lib::ObMutexGuard guard(mutex_);
  backup_set_list_.reset();
  dest_id_ = OB_INVALID_DEST_ID;
  type_ = InfoType::NONE;
}

int ObTenantBackupDestInfoMgr::get_backup_dest(const int64_t backup_set_id, share::ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  const bool enable_error_test = GCONF.enable_quick_restore_remove_backup_dest_test;
  if (enable_error_test) {
    ret = OB_EAGAIN;
    LOG_WARN("enable error test, fake backup dest is removed", K(ret));
  }
#endif
  lib::ObMutexGuard guard(mutex_);
  int64_t idx = -1;
  if (OB_FAIL(ret)) {
  } else if (InfoType::NONE == type_) {
    ret = OB_EAGAIN;
    LOG_WARN("dest info has not been set", K(ret), K(backup_set_id));
  } else if (InfoType::RESTORE == type_ && OB_FAIL(check_restore_data_mode_())) {
    LOG_WARN("failed to check restore data mode", K(ret));
  } else if (OB_FAIL(get_backup_set_brief_info_(backup_set_id, idx))) {
    LOG_WARN("failed to get backup set brief info", K(ret), K(backup_set_id));
  } else {
    const ObBackupPathString &path = backup_set_list_.at(idx).backup_set_path_;
    if (OB_FAIL(backup_dest.set(path))) {
      LOG_WARN("failed to set backup dest", K(ret));
    } else {
      LOG_INFO("get backup dest", K(backup_set_id), K(backup_dest));
    }
  }
  return ret;
}

int ObTenantBackupDestInfoMgr::get_backup_type(const int64_t backup_set_id, ObBackupType &backup_type)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  int64_t idx = -1;
  if (InfoType::NONE == type_) {
    ret = OB_EAGAIN;
    LOG_WARN("dest info has not been set", K(ret), K(backup_set_id));
  } else if (OB_FAIL(get_backup_set_brief_info_(backup_set_id, idx))) {
    LOG_WARN("failed to get restore backup set brief info", K(ret), K(backup_set_id));
  } else {
    backup_type = backup_set_list_.at(idx).backup_set_desc_.backup_type_;
    LOG_INFO("get backup type", K(backup_set_id), K(backup_type));
  }
  return ret;
}

int ObTenantBackupDestInfoMgr::get_dest_id(int64_t &dest_id)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (InfoType::NONE == type_) {
    ret = OB_EAGAIN;
    LOG_WARN("dest info has not been refreshed", K(ret));
  } else {
    dest_id = dest_id_;
    LOG_INFO("get dest id", K(dest_id));
  }
  return ret;
}

int ObTenantBackupDestInfoMgr::get_backup_set_brief_info_(
    const int64_t backup_set_id, int64_t &idx)
{
  int ret = OB_SUCCESS;
  idx = -1;
  ARRAY_FOREACH_X(backup_set_list_, i, cnt, OB_SUCC(ret)) {
    const share::ObBackupSetBriefInfo &brief_info = backup_set_list_.at(i);
    if (backup_set_id == brief_info.backup_set_desc_.backup_set_id_) {
      idx = i;
      break;
    }
  }
  if (-1 == idx) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("entry not exist", K(ret));
  }
  return ret;
}

int ObTenantBackupDestInfoMgr::check_restore_data_mode_()
{
  int ret = OB_SUCCESS;
  rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader *);
  share::ObRestoreDataMode restore_data_mode;
  if (OB_ISNULL(tenant_info_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant info loader is null", K(ret), K_(tenant_id));
  } else if (OB_FAIL(tenant_info_loader->get_restore_data_mode(restore_data_mode))) {
    LOG_WARN("fail to get restore data mode", K(ret), K_(tenant_id));
  } else if (!restore_data_mode.is_remote_mode()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    LOG_WARN("restore data mode is not REMOTE, tenant should not have any backup IO", K(ret), K_(tenant_id));
  }
  return ret;
}

}
}