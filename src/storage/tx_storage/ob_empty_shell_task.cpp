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

#include "ob_empty_shell_task.h"
#include "storage/tx_storage/ob_ls_map.h"     // ObLSIterator
#include "storage/tx_storage/ob_ls_service.h" // ObLSService
#include "storage/tablet/ob_tablet.h" // ObTablet
#include "storage/tablet/ob_tablet_iterator.h"
#include "share/ob_tenant_info_proxy.h"
#include "rootserver/ob_tenant_info_loader.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"

namespace oceanbase
{
using namespace share;
namespace storage
{
namespace checkpoint
{

// The time interval for checking deleted tablet trigger is 5s
const int64_t ObEmptyShellTask::GC_EMPTY_TABLET_SHELL_INTERVAL = 5 * 1000 * 1000L;

// The time interval for tablet become empty shell is 24 * 720 * 5s = 1d
const int64_t ObEmptyShellTask::GLOBAL_EMPTY_CHECK_INTERVAL_TIMES = 24 * 720;

void ObEmptyShellTask::runTimerTask()
{
  STORAGE_LOG(INFO, "====== [emptytablet] empty shell timer task ======", K(GC_EMPTY_TABLET_SHELL_INTERVAL));
  int ret = OB_SUCCESS;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*);
  bool skip_empty_shell_task = false;
  RLOCAL_STATIC(int64_t, times) = 0;
  times = (times + 1) % GLOBAL_EMPTY_CHECK_INTERVAL_TIMES;

  skip_empty_shell_task = (OB_SUCCESS != (OB_E(EventTable::EN_TABLET_EMPTY_SHELL_TASK_FAILED) OB_SUCCESS));

  if (!ObServerCheckpointSlogHandler::get_instance().is_started()) {
    // do nothing
    STORAGE_LOG(DEBUG, "ob block manager has not started");
  } else if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "mtl ObLSService should not be null", KR(ret));
  } else if (OB_UNLIKELY(skip_empty_shell_task)) {
    // do nothing
  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {
    STORAGE_LOG(WARN, "get log stream iter failed", KR(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "iter is NULL", KR(ret));
  } else {
    ObLS *ls = NULL;
    int ls_cnt = 0;
    for (; OB_SUCC(ret) && OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      ObTabletEmptyShellHandler *tablet_empty_shell_handler = NULL;
      if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "ls is NULL", KR(ret));
      } else if (FALSE_IT(tablet_empty_shell_handler = ls->get_tablet_empty_shell_handler())) {
      } else if (tablet_empty_shell_handler->check_stop()) {
        STORAGE_LOG(INFO, "[emptytablet] tablet_gc_handler is stop", K(ls->get_ls_id()));
      } else if (0 == times || tablet_empty_shell_handler->get_empty_shell_trigger()) {
        STORAGE_LOG(INFO, "[emptytablet] task check ls", "ls_id", ls->get_ls_id(), K(tablet_empty_shell_handler));
        tablet_empty_shell_handler->set_empty_shell_trigger(false);
        obsys::ObRLockGuard lock(tablet_empty_shell_handler->wait_lock_);
        bool need_retry = false;
        common::ObTabletIDArray empty_shell_tablet_ids;
        if (OB_FAIL(tablet_empty_shell_handler->get_empty_shell_tablet_ids(empty_shell_tablet_ids, need_retry))) {
          need_retry = true;
          STORAGE_LOG(WARN, "[emptytablet] tablet_empty_shell_handler get empty shell tablet ids failed", K(ret), "ls_id", ls->get_ls_id());
        } else if (empty_shell_tablet_ids.empty()) {
          // do nothing
        } else if (OB_FAIL(tablet_empty_shell_handler->update_tablets_to_empty_shell(ls, empty_shell_tablet_ids))) {
          need_retry = true;
          STORAGE_LOG(WARN, "update tablet to empty shell failed", KR(ret), "ls_id", ls->get_ls_id());
        }
        if (need_retry) {
          STORAGE_LOG(INFO, "[emptytablet] tablet become empty shell error, need try", KR(ret), KPC(ls), K(empty_shell_tablet_ids));
          if (!tablet_empty_shell_handler->get_empty_shell_trigger()) {
            tablet_empty_shell_handler->set_empty_shell_trigger(true);
          }
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (ls_cnt > 0) {
        STORAGE_LOG(INFO, "[emptytablet] succeed to change tablet to empty shell", KR(ret), K(ls_cnt), K(times));
      } else {
        STORAGE_LOG(INFO, "[emptytablet] no logstream", KR(ret), K(ls_cnt), K(times));
      }
    }
  }
}

int ObTabletEmptyShellHandler::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTabletGCHandler init twice", KR(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret));
  } else {
    ls_ = ls;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletEmptyShellHandler::get_empty_shell_tablet_ids(common::ObTabletIDArray &empty_shell_tablet_ids, bool &need_retry)
{
  int ret = OB_SUCCESS;
  ObLSTabletIterator tablet_iter(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tablet empty shell handler is not inited", KR(ret));
  } else if (OB_FAIL(ls_->get_tablet_svr()->build_tablet_iter(tablet_iter))) {
    STORAGE_LOG(WARN, "failed to build ls tablet iter", KR(ret), KPC(this));
  } else {
    ObTabletHandle tablet_handle;
    ObTablet *tablet = NULL;
    bool can_become_shell = false;
    bool is_deleted = false;
    bool is_locked = false;
    while (OB_SUCC(ret)) {
      if (check_stop()) {
        ret = OB_EAGAIN;
        STORAGE_LOG(INFO, "tablet empty shell handler stop", KR(ret), KPC(this), K(tablet_handle), KPC(ls_), K(ls_->get_ls_meta()));
      } else if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "failed to get tablet", KR(ret), KPC(this), K(tablet_handle));
        }
      } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid tablet handle", KR(ret), KPC(this), K(tablet_handle));
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "tablet is NULL", KR(ret));
      } else if (tablet->is_ls_inner_tablet()) {
        // skip ls inner tablet
      } else if (OB_FAIL(tablet->is_locked_by_others<ObTabletCreateDeleteMdsUserData>(is_locked))) {
        STORAGE_LOG(WARN, "failed to get is locked", KR(ret), KPC(tablet));
      } else if (is_locked) {
        STORAGE_LOG(INFO, "tablet_status is changing", KR(ret), KPC(tablet));
        need_retry = true;
      } else if (OB_FAIL(check_tablet_deleted_(tablet, is_deleted))) {
        STORAGE_LOG(WARN, "fail to check_tablet_deleted", KR(ret));
      } else if (!is_deleted) {
      } else if (OB_FAIL(check_can_become_empty_shell_(*tablet, can_become_shell, need_retry))) {
        STORAGE_LOG(WARN, "check tablet can become empty shell failed", KR(ret), KPC(tablet));
      } else if (!can_become_shell) {
        STORAGE_LOG(INFO, "tablet can not become shell", KR(ret), "tablet_meta", tablet->get_tablet_meta());
      } else if (OB_FAIL(empty_shell_tablet_ids.push_back(tablet->get_tablet_meta().tablet_id_))) {
        STORAGE_LOG(WARN, "update tablet to empty shell failed", KR(ret),"tablet_meta", tablet->get_tablet_meta());
      }

    }
  }

  return ret;
}

int ObTabletEmptyShellHandler::update_tablets_to_empty_shell(ObLS *ls, const common::ObIArray<common::ObTabletID> &tablet_ids) {
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
    const ObTabletID &tablet_id = tablet_ids.at(i);
    if (OB_FAIL(ls->get_tablet_svr()->update_tablet_to_empty_shell(tablet_id))) {
      STORAGE_LOG(WARN, "failed to update tablet to shell", K(ret), K(ls->get_ls_id()), K(tablet_id));
    } else {
    #ifdef ERRSIM
      const uint64_t tenant_id = MTL_ID();
      SERVER_EVENT_ADD("gc", "turn_into_empty_shell", "tenant_id", tenant_id, "ls_id", ls->get_ls_id(), "tablet_id", tablet_id);
    #endif
    }
  }

  return ret;
}

int ObTabletEmptyShellHandler::check_tablet_deleted_(ObTablet *tablet, bool &is_deleted)
{
  int ret = OB_SUCCESS;
  is_deleted = false;
  bool is_finish = false;
  ObTabletCreateDeleteMdsUserData data;
  if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "tablet is null", KR(ret));
  } else if (tablet->is_empty_shell()) {
    // do nothing
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    if (OB_EMPTY_RESULT == ret) {
      STORAGE_LOG(INFO, "tablet_status is null", KR(ret), KPC(tablet));
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "failed to get latest tablet status", K(ret), KP(tablet));
    }
  } else if (ObTabletStatus::DELETED == data.tablet_status_
             || ObTabletStatus::TRANSFER_OUT_DELETED == data.tablet_status_) {
    is_deleted = true;
    STORAGE_LOG(INFO, "get tablet for deleting", "ls_id", ls_->get_ls_id(), "tablet_id", tablet->get_tablet_meta().tablet_id_, K(data));
  }
  return ret;
}

int ObTabletEmptyShellHandler::check_can_become_empty_shell_(const ObTablet &tablet, bool &can, bool &need_retry)
{
  int ret = OB_SUCCESS;
  can = false;
  bool is_committed = false;
  ObTabletCreateDeleteMdsUserData user_data;
  const uint64_t tenant_id = MTL_ID();
  bool is_shell = false;
  const ObTabletID tablet_id(tablet.get_tablet_meta().tablet_id_);
  if (!is_user_tenant(tenant_id)) {
    can = true;
    STORAGE_LOG(INFO, "tenant is not a user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tablet.ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    if (OB_EMPTY_RESULT == ret) {
      can = false;
      STORAGE_LOG(INFO, "tablet_status is null", KR(ret), "ls_id", ls_->get_ls_id(), K(tablet_id));
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "failed to get latest tablet status", K(ret), "tablet_meta", tablet.get_tablet_meta());
    }
  } else if (OB_FAIL(check_tablet_empty_shell_(tablet, user_data, can, need_retry))) {
    STORAGE_LOG(WARN, "failed to check tablet empty shell", K(ret), "ls_id", ls_->get_ls_id(), K(tablet_id));
  }
  return ret;
}

int ObTabletEmptyShellHandler::check_tablet_empty_shell_(
    const ObTablet &tablet,
    const ObTabletCreateDeleteMdsUserData &user_data,
    bool &can,
    bool &need_retry)
{
  int ret = OB_SUCCESS;
  can = false;
  bool not_depend_on = false;
  ObTabletStatus::Status tablet_status = user_data.get_tablet_status();
  rootserver::ObTenantInfoLoader *info = MTL(rootserver::ObTenantInfoLoader*);
  SCN readable_scn = SCN::base_scn();
  const ObTabletID tablet_id(tablet.get_tablet_meta().tablet_id_);
  if (!user_data.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(user_data));
  } else if (OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tenant info is null", K(ret), K(user_data));
  } else if (OB_FAIL(info->get_readable_scn(readable_scn))) {
    STORAGE_LOG(WARN, "failed to get readable scn", K(ret), K(readable_scn), K(user_data));
  } else if (!readable_scn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The tenant is belong to the primary database or readable_scn is invalid",
        K(ret), K(readable_scn), K(user_data));
  } else if (ObTabletStatus::TRANSFER_OUT_DELETED == tablet_status
      && OB_FAIL(check_transfer_out_deleted_tablet_(tablet, user_data, not_depend_on, need_retry))) {
    STORAGE_LOG(WARN, "failed to check transfer out deleted tablet", K(ret), K(readable_scn), K(user_data));
  } else if (ObTabletStatus::DELETED == tablet_status || not_depend_on) {
    if (user_data.delete_commit_scn_.is_valid() && user_data.delete_commit_scn_ <= readable_scn) {
      can = true;
      STORAGE_LOG(INFO, "readable_scn is bigger than finish_scn",
        "ls_id", ls_->get_ls_id(), K(tablet_id), K(readable_scn), K(user_data));
    } else {
      need_retry = true;
      if (REACH_TENANT_TIME_INTERVAL(1 * 1000 * 1000/*1s*/)) {
        STORAGE_LOG(INFO, "readable_scn is smaller than finish_scn",
          "ls_id", ls_->get_ls_id(), K(tablet_id), K(readable_scn), K(user_data), KPC(info));
      }
    }
  }
  return ret;
}

int ObTabletEmptyShellHandler::check_transfer_out_deleted_tablet_(
    const ObTablet &tablet,
    const ObTabletCreateDeleteMdsUserData &user_data,
    bool &can,
    bool &need_retry)
{
  int ret = OB_SUCCESS;
  can = false;
  SCN decided_scn;
  ObMigrationStatus migration_status;
  ObLSService *ls_service = NULL;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  const ObTabletID tablet_id(tablet.get_tablet_meta().tablet_id_);
  if (!user_data.is_valid() || !user_data.transfer_ls_id_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "arguments are invalid", K(ret), K(user_data));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(user_data.transfer_ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    if (OB_LS_NOT_EXIST == ret) {
      can = true;
      STORAGE_LOG(INFO, "transfer dest ls not exist, src tablet can become empty shell", K(ret), "ls_id", ls_->get_ls_id(), K(user_data));
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "failed to get ls", K(ret), K(user_data));
    }
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls should not be NULL", K(ret),K(user_data));
  } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
    STORAGE_LOG(WARN, "ls is get migration status", K(ret),K(user_data));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_ADD == migration_status
      || ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE == migration_status
      || ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD == migration_status) {
    can = true;
    STORAGE_LOG(INFO, "migration_status is OB_MIGRATION_STATUS_MIGRATE", K(ret),
        "ls_id", ls_->get_ls_id(), K(tablet_id), K(can), K(user_data), K(migration_status));
  } else if (tablet.get_tablet_meta().ha_status_.is_expected_status_deleted()) {
    can = true;
    STORAGE_LOG(INFO, "tablet is expected status deleted", KR(ret), "tablet_meta", tablet.get_tablet_meta());
  } else if (OB_FAIL(ls->get_max_decided_scn(decided_scn))) {
    STORAGE_LOG(WARN, "failed to get max decided scn", K(ret), K(user_data));
  } else if (decided_scn >= user_data.delete_commit_scn_) {
    can = true;
    STORAGE_LOG(INFO, "decided_scn is bigger than transfer finish scn", K(ret),
      "ls_id", ls_->get_ls_id(), K(tablet_id), K(can), K(user_data), K(decided_scn));
  } else {
    need_retry = true;
    if (REACH_TENANT_TIME_INTERVAL(1 * 1000 * 1000/*1s*/)) {
      STORAGE_LOG(INFO, "decided_scn is smaller than tablet delete commit scn",
        "ls_id", ls_->get_ls_id(), K(tablet_id), K(user_data), K(decided_scn));
    }
  }
  return ret;
}

bool ObTabletEmptyShellHandler::get_empty_shell_trigger() const
{
  return ATOMIC_LOAD(&is_trigger_);
}

void ObTabletEmptyShellHandler::set_empty_shell_trigger(bool is_trigger)
{
  ATOMIC_STORE(&is_trigger_, is_trigger);
  STORAGE_LOG(INFO, "[emptytablet] set empty shell trigger", "ls_id", ls_->get_ls_id(), K(is_trigger));
}

int ObTabletEmptyShellHandler::offline()
{
  int ret = OB_SUCCESS;
  set_stop();
  if (!is_finish()) {
    ret = OB_EAGAIN;
    STORAGE_LOG(INFO, "tablet empty shell handler not finish, retry", KR(ret), KPC(this), KPC(ls_), K(ls_->get_ls_meta()));
  } else {
    STORAGE_LOG(INFO, "tablet empty shell handler offline", KPC(this), KPC(ls_), K(ls_->get_ls_meta()));
  }
  return ret;
}

void ObTabletEmptyShellHandler::online()
{
  set_empty_shell_trigger(true);
  set_start();
  STORAGE_LOG(INFO, "empty shell handler online", KPC(this), KPC(ls_), K(ls_->get_ls_meta()));
}


} // checkpoint
} // storage
} // oceanbase
