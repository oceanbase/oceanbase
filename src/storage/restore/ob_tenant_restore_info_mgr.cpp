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

ObTenantRestoreInfoMgr::ObTenantRestoreInfoMgr()
  : is_inited_(false),
    refresh_info_task_(*this),
    is_refreshed_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    restore_job_id_(),
    backup_set_list_(),
    dest_id_(0)
{
}

ObTenantRestoreInfoMgr::~ObTenantRestoreInfoMgr()
{
  destroy();
}

int ObTenantRestoreInfoMgr::mtl_init(ObTenantRestoreInfoMgr *&restore_info_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(restore_info_mgr->init(MTL_ID()))) {
    LOG_WARN("failed to init tenant restore info mgr", K(ret), K(MTL_ID()));
  } else {
    LOG_INFO("success to init ObTenantRestoreInfoMgr", K(MTL_ID()));
  }
  return ret;
}

int ObTenantRestoreInfoMgr::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantRestoreInfoMgr init twice", K(ret));
  }  else {
    backup_set_list_.set_tenant_id(tenant_id);
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantRestoreInfoMgr::start()
{
  int ret = OB_SUCCESS;
  const bool repeat = true;
  if (IS_NOT_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantRestoreInfoMgr is not init", K(ret));
  } else if (!is_user_tenant(tenant_id_)) {
    // do nothing
  } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(), refresh_info_task_, REFRESH_INFO_INTERVAL, repeat))) {
    LOG_WARN("failed to schedule tenant restore info mgr", K(ret));
  }
  return ret;
}

void ObTenantRestoreInfoMgr::wait()
{
  if (OB_LIKELY(is_inited_)) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), refresh_info_task_);
    LOG_INFO("wait tenant restore info refresh task", K_(tenant_id));
  }
}

void ObTenantRestoreInfoMgr::stop()
{
  if (OB_LIKELY(is_inited_)) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), refresh_info_task_);
    LOG_INFO("stop tenant restore info refresh task", K_(tenant_id));
  }
}

void ObTenantRestoreInfoMgr::destroy()
{
  stop();
  wait();
  LOG_INFO("tenant restore info mgr destroy", K_(tenant_id));
}

int ObTenantRestoreInfoMgr::refresh_restore_info()
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql can't null", K(ret), KP(sql_proxy));
  } else if (MTL_TENANT_ROLE_CACHE_IS_INVALID()) {
    // wait tenant role refresh
  } else if (MTL_TENANT_ROLE_CACHE_IS_CLONE()) {
    stop();
  } else if (MTL_TENANT_ROLE_CACHE_IS_RESTORE()) {
    // tenant in restore, get backup set list from table __all_restore_job
    share::ObPhysicalRestoreTableOperator restore_table_operator;
    const ObBackupDestType::TYPE backup_dest_type = ObBackupDestType::DEST_TYPE_RESTORE_DATA;
    int64_t dest_id = 0;
    HEAP_VAR(ObPhysicalRestoreJob, job) {
      if (OB_FAIL(restore_table_operator.init(sql_proxy, tenant_id_, share::OBCG_STORAGE /*group_id*/))) {
        LOG_WARN("fail to init restore table operator", K(ret));
      } else if (OB_FAIL(restore_table_operator.get_job_by_tenant_id(tenant_id_, job))) {
        LOG_WARN("fail to get restore job", K(ret), K_(tenant_id));
      } else if (OB_FAIL(share::ObBackupStorageInfoOperator::get_restore_dest_id(
            *sql_proxy, tenant_id_, backup_dest_type, dest_id))) {
        LOG_WARN("failed to get restore dest id", K(ret), K_(tenant_id), K(backup_dest_type));
      } else {
        lib::ObMutexGuard guard(mutex_);
        if (is_refreshed_) {
        } else if (OB_FAIL(job.get_multi_restore_path_list().get_backup_set_brief_info_list(backup_set_list_))) {
          LOG_WARN("fail to get backup set brief info list", K(ret), K(job));
        } else if (backup_set_list_.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("backup set list is empty", K(ret), K_(tenant_id));
        } else {
          restore_job_id_ = job.get_job_id();
          dest_id_ = dest_id;
          set_refreshed_();
          stop();
          LOG_INFO("get refresh restore info", K_(tenant_id), K_(backup_set_list));
        }
      }
    }
  } else {
    ObAllTenantInfo tenant_info;
    ObRestorePersistHelper persist_helper;
    ObPhysicalRestoreBackupDestList backup_dest_list;
    int64_t job_id = 0;
    if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, sql_proxy, false/*for_update*/, tenant_info))) {
      LOG_WARN("failed to load tenant info", K(ret), K_(tenant_id));
    } else if (!tenant_info.get_restore_data_mode().is_remote_mode()) {
      stop();
    } else if (OB_FAIL(persist_helper.init(tenant_id_, share::OBCG_STORAGE /*group_id*/))) {
      LOG_WARN("failed to init persist helper", K(ret), K_(tenant_id));
    } else if (OB_FAIL(persist_helper.get_backup_dest_list_from_restore_info(
                       *sql_proxy,
                       restore_job_id_,
                       backup_dest_list))) {
      LOG_WARN("failed to get backup dest list from restore info", K(ret), K_(tenant_id));
    } else {
      lib::ObMutexGuard guard(mutex_);
      if (is_refreshed_) {
      } else if (OB_FAIL(backup_dest_list.get_backup_set_brief_info_list(backup_set_list_))) {
        LOG_WARN("fail to get backup set brief info list", K(ret), K(backup_dest_list));
      } else if (backup_set_list_.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup set list is empty", K(ret), K_(tenant_id));
      } else {
        restore_job_id_ = job_id;
        set_refreshed_();
        stop();
        LOG_INFO("get refresh restore info", K_(tenant_id), K_(backup_set_list));
      }
    }
  }

  return ret;
}

int ObTenantRestoreInfoMgr::get_backup_dest(const int64_t backup_set_id, share::ObBackupDest &backup_dest)
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
  rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader *);
  share::ObRestoreDataMode restore_data_mode;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(tenant_info_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant info loader is null", K(ret), K_(tenant_id));
  } else if (OB_FAIL(tenant_info_loader->get_restore_data_mode(restore_data_mode))) {
    LOG_WARN("fail to get restore data mode", K(ret), K_(tenant_id));
  } else if (!restore_data_mode.is_remote_mode()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    LOG_WARN("restore data mode is not REMOTE, tenant should not have any backup IO", K(ret), K_(tenant_id));
  } else  if (!is_refreshed_) {
    ret = OB_EAGAIN;
    LOG_WARN("restore info has not been refreshed", K(ret), K(backup_set_id));
  } else if (OB_FAIL(get_restore_backup_set_brief_info_(backup_set_id, idx))) {
    LOG_WARN("failed to get restore backup set brief info", K(ret), K(backup_set_id));
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

int ObTenantRestoreInfoMgr::get_backup_type(const int64_t backup_set_id, ObBackupType &backup_type)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  int64_t idx = -1;
  if (!is_refreshed_) {
    ret = OB_EAGAIN;
    LOG_WARN("restore info has not been refreshed", K(ret), K(backup_set_id));
  } else if (OB_FAIL(get_restore_backup_set_brief_info_(backup_set_id, idx))) {
    LOG_WARN("failed to get restore backup set brief info", K(ret), K(backup_set_id));
  } else {
    backup_type = backup_set_list_.at(idx).backup_set_desc_.backup_type_;
    LOG_INFO("get backup type", K(backup_set_id), K(backup_type));
  }
  return ret;
}

int ObTenantRestoreInfoMgr::get_restore_dest_id(int64_t &dest_id)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  int64_t idx = -1;
  if (!is_refreshed_) {
    ret = OB_EAGAIN;
    LOG_WARN("restore info has not been refreshed", K(ret));
  } else {
    dest_id_ = dest_id;
    LOG_INFO("get dest id", K(dest_id));
  }
  return ret;
}

int ObTenantRestoreInfoMgr::get_restore_backup_set_brief_info_(
    const int64_t backup_set_id, int64_t &idx)
{
  int ret = OB_SUCCESS;
  idx = -1;
  ARRAY_FOREACH_X(backup_set_list_, i, cnt, OB_SUCC(ret)) {
    const share::ObRestoreBackupSetBriefInfo &brief_info = backup_set_list_.at(i);
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

void ObTenantRestoreInfoMgr::RestoreInfoRefresher::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mgr_.refresh_restore_info())) {
    LOG_WARN("failed to refresh restore info", K(ret));
  } else {
    LOG_INFO("refresh restore info");
  }
}

}
}