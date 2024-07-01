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
#include "ob_ls_restore_task_mgr.h"
#include "storage/ls/ob_ls.h"
#include "lib/lock/ob_mutex.h"
#include "lib/atomic/ob_atomic.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_iterator.h"

using namespace oceanbase;
using namespace share;
using namespace common;
using namespace storage;
using namespace backup;
using namespace logservice;

int ObLSRestoreTaskMgr::ToRestoreTabletGroup::assign(const ToRestoreTabletGroup &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tablet_list_.assign(other.tablet_list_))) {
    LOG_WARN("fail to assign tablet list", K(ret));
  } else {
    action_ = other.action_;
    from_q_type_ = other.from_q_type_;
    need_redo_failed_tablets_ = other.need_redo_failed_tablets_;
    task_type_ = other.task_type_;
  }
  return ret;
}


ObLSRestoreTaskMgr::ObLSRestoreTaskMgr()
  : is_inited_(false),
    mtx_(ObLatchIds::RESTORE_LOCK),
    tablet_map_(),
    schedule_tablet_set_(),
    wait_tablet_set_(),
    high_pri_wait_tablet_set_(),
    restore_state_handler_(nullptr),
    force_reload_(false),
    final_reload_(false),
    has_checked_leader_done_(false)
{
}

ObLSRestoreTaskMgr::~ObLSRestoreTaskMgr()
{
}

int ObLSRestoreTaskMgr::init(ObILSRestoreState *state_handler, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(MTL_ID(), "RestoreTaskMgr");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(tablet_map_.create(OB_RESTORE_MAX_DAG_NET_NUM, attr))) {
    LOG_WARN("fail to create tablet_map_", K(ret));
  } else if (OB_FAIL(schedule_tablet_set_.create(OB_LS_RESTORE_MAX_TABLET_NUM, attr))) {
    LOG_WARN("fail to create schedule_tablet_set_", K(ret));
  } else if (OB_FAIL(wait_tablet_set_.create(OB_LS_RESTORE_MAX_TABLET_NUM, attr))) {
    LOG_WARN("fail to create wait_tablet_set_", K(ret));
  } else if (OB_FAIL(high_pri_wait_tablet_set_.create(OB_LS_RESTORE_MAX_TABLET_NUM, attr))) {
    LOG_WARN("fail to create high_pri_wait_tablet_set_", K(ret));
  } else {
    restore_state_handler_ = state_handler;
    ls_id_ = ls_id;
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void ObLSRestoreTaskMgr::destroy()
{
  if (tablet_map_.created()) {
    tablet_map_.destroy();
  }
  if (schedule_tablet_set_.created()) {
    schedule_tablet_set_.destroy();
  }
  if (wait_tablet_set_.created()) {
    wait_tablet_set_.destroy();
  }
  if (high_pri_wait_tablet_set_.created()) {
    high_pri_wait_tablet_set_.destroy();
  }
}

int ObLSRestoreTaskMgr::add_tablet_in_wait_set(const ObIArray<common::ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (tablet_ids.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_ids can't empty", K(ret));
  } else {
    lib::ObMutexGuard guard(mtx_);
    int tmp_ret = OB_SUCCESS;
    ARRAY_FOREACH_NORET(tablet_ids, i) {
      const ObTabletID &tablet_id = tablet_ids.at(i);
      if (OB_TMP_FAIL(wait_tablet_set_.set_refactored(tablet_id))) {
        LOG_WARN("fail to insert tablet id into wait tablet set", K(tmp_ret), K_(ls_id), K(tablet_id));
      }
    }

    LOG_INFO("add tablet in wait set", K_(ls_id), K(tablet_ids));
  }
  return ret;
}

int ObLSRestoreTaskMgr::record_one_tablet_to_restore(const common::ObTabletID& tablet_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(high_pri_wait_tablet_set_.set_refactored(tablet_id))) {
    // EMPTY tablets with transfer table should be restored as soon as possible.
    set_force_reload();
    LOG_WARN("fail to add high pri tablet, set force reload", K(ret), K_(ls_id), K(tablet_id));
  } else {
    LOG_INFO("record one high pri tablet", K_(ls_id), K(tablet_id));
  }
  return ret;
}

int ObLSRestoreTaskMgr::choose_tablets_to_restore(
    ObLSRestoreTaskMgr::ToRestoreTabletGroup &tablet_group)
{
  int ret = OB_SUCCESS;
  bool reload = false;
  ObLS *ls = nullptr;
  ObLSRestoreStatus ls_restore_status = restore_state_handler_->get_restore_status();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(ls = restore_state_handler_->get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret));
  } else if (OB_FAIL(check_need_reload_tablets_(reload))) {
    LOG_WARN("failed to check need reload tablets", K(ret));
  } else if (!reload) {
  } else if (OB_FAIL(reload_tablets_())) {
    LOG_WARN("failed to reload tablets", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (ls_restore_status.is_quick_restore()) {
    // During QUICK_RESTORE, EMPTY tablets with transfer table need to be restored first.
    if (OB_FAIL(choose_tablets_from_high_pri_tablet_set_(*ls, tablet_group))) {
      LOG_WARN("failed to choose tablets from high pri tablet set", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!tablet_group.empty()) {
  } else if (OB_FAIL(choose_tablets_from_wait_set_(*ls, tablet_group))) {
    LOG_WARN("failed to choose tablets from wait set", K(ret));
  }
  return ret;
}

int ObLSRestoreTaskMgr::remove_restored_tablets(ObIArray<common::ObTabletID> &restored_tablets)
{
  int ret = OB_SUCCESS;
  restored_tablets.reset();

  ObLS *ls = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(ls = restore_state_handler_->get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret));
  } else {
    ObArray<ObTaskId> finish_task;
    ObArray<ObTabletID> high_pri_tablet_need_redo;
    ObArray<ObTabletID> wait_tablet_need_redo;
    bool is_task_doing = false;
    lib::ObMutexGuard guard(mtx_);
    FOREACH_X(iter, tablet_map_, OB_SUCC(ret)) {
      if (OB_FAIL(check_task_exist_(iter->first, is_task_doing))) {
        LOG_WARN("fail to check task exist", K(ret), "taks_id", iter->first);
      } else if (is_task_doing) {
        LOG_INFO("task is still doing, wait later", "task_id", iter->first);
      } else if (OB_FAIL(finish_task.push_back(iter->first))) {
        LOG_WARN("fail to push back finished task id", K(ret));
      } else {
        const ToRestoreTabletGroup &restored_tg = iter->second;
        LOG_INFO("task is finished", "task_id", iter->first, K_(ls_id), K(restored_tg), KP(&high_pri_wait_tablet_set_), KP(&wait_tablet_set_));
        if (!restored_tg.is_tablet_group_task()) {
        } else if (OB_FAIL(handle_task_finish_(ls, restored_tg, restored_tablets, high_pri_tablet_need_redo, wait_tablet_need_redo))) {
          LOG_WARN("fail to handle finish task", K(ret), "task_id", iter->first);
        }
      }
    }

    if (FAILEDx(redo_failed_tablets_(high_pri_tablet_need_redo, wait_tablet_need_redo))) {
      LOG_WARN("failed to redo failed tablets", K(ret));
    } else {
      remove_finished_task_(finish_task);
      LOG_INFO("succeed remove restored tablets", K_(ls_id), K(high_pri_tablet_need_redo), K(wait_tablet_need_redo), K(restored_tablets));
    }
  }
  return ret;
}

int ObLSRestoreTaskMgr::reload_get_unfinished_tablets(
    ObIArray<common::ObTabletID> &unfinished_high_pri_tablets,
    ObIArray<common::ObTabletID> &unfinished_tablets) const
{
  int ret = OB_SUCCESS;
  ObLSTabletService *ls_tablet_svr = nullptr;
  ObLS *ls = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(ls = restore_state_handler_->get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret));
  } else if (OB_ISNULL(ls_tablet_svr = ls->get_tablet_svr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_tablet_svr is nullptr", K(ret));
  } else {
    ObLSRestoreStatus ls_restore_status = restore_state_handler_->get_restore_status();
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    ObLSTabletIterator iterator(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
    if (OB_FAIL(ls_tablet_svr->build_tablet_iter(iterator))) {
      LOG_WARN("fail to build tablet iterator", K(ret), KPC(ls));
    }

    while (OB_SUCC(ret)) {
      if (OB_FAIL(iterator.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get next tablet", K(ret));
        }
        break;
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet is nullptr", K(ret), K(tablet_handle));
      } else {
        const ObTabletMeta &tablet_meta = tablet->get_tablet_meta();
        const ObTabletID &tablet_id = tablet_meta.tablet_id_;
        bool is_restored = false;
        if (tablet_id.is_ls_inner_tablet()) {
          // do nothing
        } else if (tablet->is_empty_shell()) {
          // do nothing
        } else if (OB_FAIL(is_tablet_restore_finish_(ls_restore_status, tablet_handle, is_restored))) {
          LOG_WARN("failed to check tablet restore finish", K(ret), K(ls_restore_status), K(tablet_meta));
        } else if (is_restored) {
        } else if (tablet_meta.has_transfer_table()) {
          if (OB_FAIL(unfinished_high_pri_tablets.push_back(tablet_id))) {
            LOG_WARN("failed to push back tablet", K(ret));
          } else {
            LOG_INFO("find an unfinished tablet with transfer table", K(tablet));
          }
        } else if (OB_FAIL(unfinished_tablets.push_back(tablet_id))) {
          LOG_WARN("failed to push back tablet", K(ret));
        } else {
          LOG_INFO("find an unfinished tablet", KPC(tablet));
        }
      }
    }
  }

  return ret;
}

int ObLSRestoreTaskMgr::schedule_ls_restore(
    const share::ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  ToRestoreTabletGroup ls_restore_task;
  bool is_exist = false;
  lib::ObMutexGuard guard(mtx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (task_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task id", K(ret), K(task_id));
  } else if (OB_FAIL(check_task_exist_(task_id, is_exist))) {
    LOG_WARN("fail to check task exist", K(ret), K(task_id));
  } else if (is_exist) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task id exist", K(ret), K(task_id));
  } else if (OB_FALSE_IT(ls_restore_task.from_q_type_ = ToRestoreFromQType::FROM_NONE)) {
  } else if (OB_FALSE_IT(ls_restore_task.task_type_ = TaskType::LS_RESTORE_TASK)) {
  } else if (OB_FAIL(tablet_map_.set_refactored(task_id, ls_restore_task))) {
    LOG_WARN("fail to set task id map", K(ret), K(task_id));
  }

  return ret;
}

int ObLSRestoreTaskMgr::schedule_tablet_group_restore(
    const share::ObTaskId &task_id,
    const ToRestoreTabletGroup &to_restore_tg,
    bool &reach_dag_limit)
{
  int ret = OB_SUCCESS;
  reach_dag_limit = false;
  bool is_exist = false;
  lib::ObMutexGuard guard(mtx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (task_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task id", K(ret), K(task_id));
  } else if (!to_restore_tg.is_tablet_group_task() || to_restore_tg.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("to_restore_tg is invalid", K(ret), K(task_id), K(to_restore_tg));
  } else if (OB_FAIL(check_task_exist_(task_id, is_exist))) {
    LOG_WARN("fail to check task exist", K(ret), K(task_id));
  } else if (is_exist) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task id exist", K(ret), K(task_id), K(to_restore_tg));
  } else if (OB_FAIL(tablet_map_.set_refactored(task_id, to_restore_tg))) {
    LOG_WARN("fail to set task id map", K(ret), K(task_id), K(to_restore_tg));
  } else {
    // move to DOING set
    int64_t succeed_cnt = 0;
    const ObSArray<ObTabletID> &tablet_need_restore = to_restore_tg.get_tablet_list();
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_need_restore.count(); ++i) {
      const ObTabletID &tablet_id = tablet_need_restore.at(i);
      if (OB_FAIL(schedule_tablet_set_.set_refactored(tablet_id))) {
        LOG_WARN("fail to set into schedule_tablet_set_", K(ret), K(tablet_id), K(succeed_cnt));
      } else {
        ++succeed_cnt;
      }
    }

    if (OB_FAIL(ret)) {
      // rollback, remove from DOING set
      int tmp_ret = OB_SUCCESS;
      for (int64_t i = 0; i < succeed_cnt; ++i) {
        const ObTabletID &tablet_id = tablet_need_restore.at(i);
        if (OB_TMP_FAIL(schedule_tablet_set_.erase_refactored(tablet_id))) {
          LOG_ERROR("fail to remove from schedule_tablet_set_ ", K(tmp_ret), K(tablet_id), K(i), K(succeed_cnt));
        }
      }

      // remove from task map
      if (OB_TMP_FAIL(tablet_map_.erase_refactored(task_id))) {
        LOG_ERROR("fail to remove from task map", K(tmp_ret), K(task_id));
      }
    } else {
      // remove from TODO set
      int tmp_ret = OB_SUCCESS;
      TabletSet *todo_set = to_restore_tg.from_q_type() == ToRestoreFromQType::FROM_WAIT_TABLETS_Q ? &wait_tablet_set_ : &high_pri_wait_tablet_set_;
      for (int64_t i = 0; i < succeed_cnt; ++i) {
        const ObTabletID &tablet_id = tablet_need_restore.at(i);
        if (OB_TMP_FAIL(todo_set->erase_refactored(tablet_id))) {
          LOG_ERROR("fail to remove from wait_tablet_set_ ", K(tmp_ret), K(tablet_id), K(i), K(succeed_cnt));
        }
      }
    }
  }
  return ret;
}

int ObLSRestoreTaskMgr::cancel_task()
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    lib::ObMutexGuard guard(mtx_);
    TaskMap::iterator iter = tablet_map_.begin();
    for (; OB_SUCC(ret) && iter != tablet_map_.end(); ++iter) {
      is_exist = false;
      if (OB_FAIL(check_task_exist_(iter->first, is_exist))) {
        LOG_WARN("fail to check task exist", K(ret), "task_id", iter->first);
      } else if (is_exist) {
        ObTenantDagScheduler *scheduler = nullptr;
        if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret), KP(scheduler));
        } else if (OB_FAIL(scheduler->cancel_dag_net(iter->first))) {
          LOG_WARN("failed to check dag net exist", K(ret), "task_id", iter->first);
        }
      }
    }

    int64_t start_ts = ObTimeUtil::current_time();
    iter = tablet_map_.begin();
    for (; OB_SUCC(ret) && iter != tablet_map_.end(); ++iter) {
      is_exist = true;
      do {
        if (OB_FAIL(check_task_exist_(iter->first, is_exist))) {
          LOG_WARN("fail to check task exist", K(ret), "task_id", iter->first);
        } else if (is_exist && REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
          ret = OB_EAGAIN; // dag may hold ls lock. return OB_EAGAIN , ls offline can retry in next time.
          LOG_WARN("[RESTORE]cancel dag task cost too much time", K(ret), "task_id", iter->first,
              "cost_time", ObTimeUtil::current_time() - start_ts);
        }
      } while (is_exist && OB_SUCC(ret));
    }

    if (OB_SUCC(ret)) {
      clear_all();
    }
  }
  return ret;
}

void ObLSRestoreTaskMgr::switch_to_leader()
{
  // Reload tablets from local tablet service.
  clear_tablets_to_restore();
  set_force_reload();
  final_reload_ = false;
  has_checked_leader_done_ = false;
  LOG_INFO("handle switch to leader", K_(ls_id));
}

void ObLSRestoreTaskMgr::switch_to_follower()
{
  // Clear tablets to restore, and prepare to receive tablets from new leader.
  clear_tablets_to_restore();
  set_noneed_redo_failed_tablets_();
  final_reload_ = false;
  has_checked_leader_done_ = false;
  LOG_INFO("handle switch to follower", K_(ls_id));
}

void ObLSRestoreTaskMgr::leader_switched()
{
  // Clear tablets to restore, and prepare to receive tablets from new leader.
  clear_tablets_to_restore();
  set_noneed_redo_failed_tablets_();
  final_reload_ = false;
  has_checked_leader_done_ = false;
  LOG_INFO("handle leader switched", K_(ls_id));
}

int ObLSRestoreTaskMgr::check_task_exist_(
    const share::ObTaskId &task_id,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("LS restore task mgr do not init", K(ret));
  } else if (task_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check task exist get invalid argument", K(ret), K(task_id));
  } else {
    ObTenantDagScheduler *scheduler = nullptr;
    if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret), KP(scheduler));
    } else if (OB_FAIL(scheduler->check_dag_net_exist(task_id, is_exist))) {
      LOG_WARN("failed to check dag net exist", K(ret), K(task_id));
    }
  }
  return ret;
}

int ObLSRestoreTaskMgr::check_transfer_start_finish_(const ObTabletHandle &tablet_handle, bool &is_finish) const
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData user_data;
  ObTablet *tablet = nullptr;
  if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet is nullptr", K(ret));
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(
    share::SCN::max_scn(), user_data, -1))) {
    if (OB_EMPTY_RESULT == ret) {
      // No committed user data exist, indicate that transfer start transaction is not finish.
      ret = OB_SUCCESS;
      is_finish = false;
    } else {
      LOG_WARN("failed to get user data", K(ret), KPC(tablet));
    }
  } else {
    is_finish = true;
  }

  return ret;
}

int ObLSRestoreTaskMgr::check_transfer_start_finish_(const common::ObTabletID &tablet_id, bool &is_finish) const
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObLS *ls = nullptr;

  if (OB_ISNULL(ls = restore_state_handler_->get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id));
  } else if (OB_FAIL(check_transfer_start_finish_(tablet_handle, is_finish))) {
    LOG_WARN("fail to check transfer start finish", K(ret), K(tablet_id));
  }

  return ret;
}

void ObLSRestoreTaskMgr::set_force_reload()
{
  LOG_INFO("set force reload tablets", KPC_(restore_state_handler));
  ATOMIC_STORE(&force_reload_, true);
}

void ObLSRestoreTaskMgr::clear_tablets_to_restore()
{
  wait_tablet_set_.reuse();
  high_pri_wait_tablet_set_.reuse();
}

void ObLSRestoreTaskMgr::clear_all()
{
  wait_tablet_set_.reuse();
  high_pri_wait_tablet_set_.reuse();
  schedule_tablet_set_.reuse();
  tablet_map_.reuse();
}

bool ObLSRestoreTaskMgr::is_restore_completed() const
{
  return has_no_tablets_to_restore() && has_no_tablets_restoring() && final_reload_;
}

int ObLSRestoreTaskMgr::reload_tablets_()
{
  int ret = OB_SUCCESS;
  ObLSTabletService *ls_tablet_svr = nullptr;
  ObLS *ls = nullptr;
  bool is_follower = is_follower_();
  if (OB_ISNULL(ls = restore_state_handler_->get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret));
  } else if (OB_ISNULL(ls_tablet_svr = ls->get_tablet_svr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_tablet_svr is nullptr", K(ret));
  } else {
    ObLSRestoreStatus ls_restore_status = restore_state_handler_->get_restore_status();
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    ObLSTabletIterator iterator(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
    if (OB_FAIL(ls_tablet_svr->build_tablet_iter(iterator))) {
      LOG_WARN("fail to build tablet iterator", K(ret), KPC(ls));
    }

    while (OB_SUCC(ret)) {
      bool discard = false;
      if (OB_FAIL(iterator.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get next tablet", K(ret));
        }
        break;
      } else if (OB_FAIL(check_tablet_need_discard_when_reload_(tablet_handle, discard))) {
        LOG_WARN("fail to check tablet need discard when reload", K(ret), K(tablet_handle));
      } else if (discard) {
        LOG_DEBUG("this tablet will discard", K(tablet_handle), K(ls_restore_status));
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet is nullptr", K(ret), K(tablet_handle));
      } else {
        const ObTabletMeta &tablet_meta = tablet->get_tablet_meta();
        const ObTabletID &tablet_id = tablet_meta.tablet_id_;
        if (tablet_meta.has_transfer_table() && ls_restore_status.is_quick_restore()) {
          // restore status must be FULL or EMPTY.
          // These tablets which have transfer info have higher priority to restore minor.
          if (OB_FAIL(schedule_tablet_set_.exist_refactored(tablet_id))) {
            if (OB_HASH_NOT_EXIST == ret) {
              if (OB_FAIL(high_pri_wait_tablet_set_.set_refactored(tablet_id))) {
                LOG_WARN("fail to add tablet to high_pri_wait_tablet_set_", K(ret), K(tablet_id), K(ls_restore_status), K(tablet_meta));
              } else {
                LOG_INFO("add one tablet to high_pri_wait_tablet_set_", K(tablet_id), K(ls_restore_status));
              }
            } else if (OB_HASH_EXIST == ret) {
              ret = OB_SUCCESS;
              LOG_INFO("tablet is restoring, skip", K(tablet_id), K(ls_restore_status), K(tablet_meta));
            } else {
              LOG_WARN("fail to check tablet exist in schedule_tablet_set_", K(ret), K(tablet_id), K(ls_restore_status));
            }
          }
        } else if (OB_FAIL(schedule_tablet_set_.exist_refactored(tablet_id))) {
          if (OB_HASH_NOT_EXIST == ret) {
            if (OB_FAIL(wait_tablet_set_.set_refactored(tablet_id))) {
              LOG_WARN("fail to add tablet to wait_tablet_set_", K(ret), K(tablet_id));
            } else {
              LOG_INFO("add one tablet to wait_tablet_set_", K(tablet_id));
            }
          } else if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
            LOG_INFO("tablet is restoring, skip", K(tablet_id));
          } else {
            LOG_WARN("fail to check tablet exist in schedule_tablet_set_", K(ret), K(tablet_id));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    // If no tablets to restore are found, mark 'final_reload_' true.
    if (has_no_tablets_to_restore()
        && (!is_follower || (is_follower && has_checked_leader_done_))) {
      final_reload_ = true;
      LOG_INFO("no tablets to restore are found, set final reload", K_(ls_id), K(is_follower));
    } else {
      final_reload_ = false;
    }
    unset_force_reload_();
  } else {
    final_reload_ = false;
  }

  LOG_INFO("reload tablets", K(ret), K_(ls_id), K(is_follower),
    "wait_tablet_set size", wait_tablet_set_.size(),
    "high_pri_wait_tablet_set size", high_pri_wait_tablet_set_.size(),
    "schedule_tablet_set size", schedule_tablet_set_.size(),
    "task_map size", tablet_map_.size());

  return ret;
}


int ObLSRestoreTaskMgr::check_need_reload_tablets_(bool &reload)
{
  int ret = OB_SUCCESS;
  ObLSRestoreStatus ls_restore_status = restore_state_handler_->get_restore_status();
  reload = false;
  // Reload tablets is only allowed if restore status is RESTORE_TABLETS_META, QUICK_RESTORE, or RESTORE_MAJOR_DATA
  if (!ls_restore_status.is_restore_tablets_meta()
      && !ls_restore_status.is_quick_restore()
      && !ls_restore_status.is_restore_major_data()) {
    LOG_DEBUG("no need reload", K_(ls_id), K(ls_restore_status), "is_follower", is_follower_());
  } else if (ATOMIC_LOAD(&force_reload_)) {
    reload = true;
  } else if (!has_no_tablets_to_restore() || !has_no_tablets_restoring()) {
  } else if (final_reload_) {
    LOG_DEBUG("final reload is set, need not reload", K_(ls_id), K(ls_restore_status), "is_follower", is_follower_());
  } else if (is_follower_()) {
    // follower can reload tablets only if leader has been restored except at QUICK_RESTORE or RESTORE_MAJOR.
    bool finish = true;
    if (!ls_restore_status.is_restore_tablets_meta()
        && !ls_restore_status.is_quick_restore()
        && !ls_restore_status.is_restore_major_data()
        && OB_FAIL(restore_state_handler_->check_leader_restore_finish(finish))) {
      LOG_WARN("fail to check leader restore finish", K(ret), KPC_(restore_state_handler));
    } else if (!finish) {
      has_checked_leader_done_ = false;
      LOG_DEBUG("wait leader restore finish", K_(ls_id), K(ls_restore_status));
    } else {
      has_checked_leader_done_ = true;
      reload = true;
      LOG_INFO("follower need reload tablets", K_(ls_id), K(ls_restore_status));
    }
  } else {
    reload = true;
    LOG_INFO("leader need reload tablets", K_(ls_id), K(ls_restore_status));
  }

  return ret;
}

int ObLSRestoreTaskMgr::check_tablet_need_discard_when_reload_(
    const ObTabletHandle &tablet_handle,
    bool &discard) const
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  discard = false;
  if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is nullptr", K(ret));
  } else {
    // the following tablets will be discarded during reload.
    // 1. inner tablet
    // 2. restored tablet
    // 3. empty shell tablet
    // 4. this is follower, but leader has not been restored
    bool is_follower = is_follower_();
    bool is_finish = false;
    const ObTabletMeta &tablet_meta = tablet->get_tablet_meta();
    const ObTabletID &tablet_id = tablet_meta.tablet_id_;
    const ObLSRestoreStatus ls_restore_status = restore_state_handler_->get_restore_status();
    if (!tablet_meta.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet meta", K(ret), K(tablet_meta));
    } else if (tablet_meta.tablet_id_.is_ls_inner_tablet()) {
      discard = true;
    } else if (tablet->is_empty_shell()) {
      discard = true;
      LOG_INFO("find a empty shell tablet", K(tablet_id));
    } else if (OB_FAIL(is_tablet_restore_finish_(ls_restore_status, tablet_handle, is_finish))) {
      LOG_WARN("failed to check tablet restore finish", K(ret), K(ls_restore_status), K(tablet_handle));
    } else if (is_finish) {
      discard = true;
      LOG_DEBUG("skip restored tablet", K(tablet_id), K(ls_restore_status), "ha_status", tablet_meta.ha_status_);
    } else if (ls_restore_status.is_restore_tablets_meta()) {
    } else if (ls_restore_status.is_quick_restore()) {
    } else if (ls_restore_status.is_restore_major_data()) {
    } else if (is_follower && !has_checked_leader_done_) {
      // The follower does not load tablets to restore before leader has been restored.
      discard = true;
      LOG_DEBUG("skip tablet to restore before leader has been restored.", K(tablet_id), K(ls_restore_status), "ha_status", tablet_meta.ha_status_);
    }
  }

  return ret;
}

int ObLSRestoreTaskMgr::check_tablet_is_deleted_(
    const ObTabletHandle &tablet_handle,
    bool &is_deleted) const
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObTabletCreateDeleteMdsUserData data;

  is_deleted = false;

  if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet is null", K(ret));
  } else if (tablet->is_empty_shell()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet is empty shell", K(ret), KPC(tablet));
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    if (OB_EMPTY_RESULT == ret || OB_ERR_SHARED_LOCK_CONFLICT == ret) {
      LOG_WARN("tablet_status is null or not committed", K(ret), KPC(tablet));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get latest tablet status", K(ret), KPC(tablet));
    }
  } else if (ObTabletStatus::DELETED == data.tablet_status_
             || ObTabletStatus::TRANSFER_OUT_DELETED == data.tablet_status_) {
    is_deleted = true;
  }
  return ret;
}

int ObLSRestoreTaskMgr::check_need_discard_transfer_tablet_(
    const ObTabletHandle &tablet_handle,
    bool &discard) const
{
  int ret = OB_SUCCESS;
  bool is_finish = false;
  discard = false;
  if (OB_FAIL(check_transfer_start_finish_(tablet_handle, is_finish))) {
    if (OB_ERR_SHARED_LOCK_CONFLICT == ret) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(restore_state_handler_->check_recover_finish(is_finish))) {
        LOG_WARN("fail to check recover finish", K(ret), K(tmp_ret), KPC_(restore_state_handler), K_(ls_id));
      } else if (!is_finish) {
        // overwrite error code if transfer start has stepped into 2-phase transaction before recover finished.
        ret = OB_SUCCESS;
      } else {
        discard = true;
        ObTablet *tablet = tablet_handle.get_obj();
        LOG_INFO("uncommitted tablet created by transfer, but log has been recovered, "
                 "discard this tablet from restore task mgr", KPC(tablet), K(discard));
      }
    } else {
      LOG_WARN("fail to check transfer start finish", K(ret), K_(ls_id), K(tablet_handle));
    }
  } else if (!is_finish && OB_FAIL(check_need_discard_uncommit_transfer_tablet_(discard))) {
    LOG_WARN("fail to check need discard uncommit transfer tablet", K(ret));
  }
  return ret;
}

int ObLSRestoreTaskMgr::check_need_discard_uncommit_transfer_tablet_(
    bool &discard) const
{
  int ret = OB_SUCCESS;
  bool is_finish = false;
  discard = false;
  if (OB_FAIL(restore_state_handler_->check_recover_finish(is_finish))) {
    LOG_WARN("fail to check recover finish", K(ret), KPC_(restore_state_handler), K_(ls_id));
  } else if (is_finish) {
    discard = true;
  }
  return ret;
}

bool ObLSRestoreTaskMgr::is_follower_() const
{
  return is_follower(restore_state_handler_->get_role());
}

int ObLSRestoreTaskMgr::is_tablet_restore_finish_(
    const share::ObLSRestoreStatus &ls_restore_status,
    const ObTabletHandle &tablet_handle,
    bool &is_finish) const
{
  int ret = OB_SUCCESS;
  bool discard = false;
  const ObTabletMeta &tablet_meta = tablet_handle.get_obj()->get_tablet_meta();
  const ObTabletHAStatus &ha_status = tablet_meta.ha_status_;

  is_finish = true;
  switch (ls_restore_status.get_status()) {
    case ObLSRestoreStatus::RESTORE_TABLETS_META :
    case ObLSRestoreStatus::WAIT_RESTORE_TABLETS_META : {
      is_finish = !ha_status.is_restore_status_pending();
      break;
    }

    case ObLSRestoreStatus::RESTORE_TO_CONSISTENT_SCN :
    case ObLSRestoreStatus::WAIT_RESTORE_TO_CONSISTENT_SCN : {
      is_finish = !(ha_status.is_restore_status_full() && tablet_meta.has_transfer_table());
      break;
    }

    case ObLSRestoreStatus::QUICK_RESTORE:
    case ObLSRestoreStatus::WAIT_QUICK_RESTORE:
    case ObLSRestoreStatus::QUICK_RESTORE_FINISH: {
      if (ha_status.is_restore_status_undefined()) {
        bool is_deleted = true;
        // UNDEFINED should be deleted after log has recovered.
        if (ls_restore_status.is_quick_restore()) {
          is_finish = true;
        } else if (OB_FAIL(check_tablet_is_deleted_(tablet_handle, is_deleted))) {
          LOG_WARN("failed to check tablet is deleted", K(ret), K_(ls_id), K(tablet_meta));
        } else if (is_deleted) {
          is_finish = true;
          LOG_INFO("UNDEFINED tablet is deleted", K_(ls_id), K(tablet_meta));
        } else {
          is_finish = false;
          LOG_INFO("UNDEFINED tablet is not deleted", K_(ls_id), K(tablet_meta));
        }
      } else {
        is_finish = ha_status.is_restore_status_minor_and_major_meta();
        if (!ha_status.is_restore_status_full()) {
        } else if (!tablet_meta.has_transfer_table()) {
          is_finish = true;
        } else if (OB_FAIL(check_need_discard_transfer_tablet_(tablet_handle, discard))) {
          LOG_WARN("failed to check tablet need discard", K(ret), K_(ls_id), K(tablet_meta));
        } else if (discard) {
          // uncommitted tablet created by transfer, but log has been recovered.
          is_finish = true;
        } else {
          // FULL tablet with transfer table, need wait the table be replaced.
          is_finish = false;
        }
      }
      break;
    }

    case ObLSRestoreStatus::RESTORE_MAJOR_DATA :
    case ObLSRestoreStatus::WAIT_RESTORE_MAJOR_DATA : {
      is_finish = ha_status.is_restore_status_full();
      if (ls_restore_status.is_restore_major_data()) {
        is_finish |= ha_status.is_restore_status_undefined();
        LOG_INFO("skip UNDEFINED tablet, whose major need not to be restored", K_(ls_id), K(tablet_meta));
      }

      if (!is_finish) {
        // If tablet is deleted, major is no need to be restored.
        bool is_deleted = true;
        if (OB_FAIL(check_tablet_is_deleted_(tablet_handle, is_deleted))) {
          LOG_WARN("failed to check tablet is deleted", K(ret), K_(ls_id), K(tablet_meta));
        } else if (is_deleted) {
          is_finish = true;
          LOG_INFO("ignore deleted tablet during restore major", K_(ls_id), K(tablet_meta));
        }
      }
      break;
    }

    default: {
      is_finish = true;
      break;
    }
  }

  return ret;
}

int ObLSRestoreTaskMgr::choose_tablets_from_wait_set_(
    storage::ObLS &ls,
    ObLSRestoreTaskMgr::ToRestoreTabletGroup &tablet_group)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mtx_);
  bool is_exist = false;
  bool is_restored = false;
  bool is_restoring = false;
  ObTabletRestoreStatus::STATUS restore_status = ObTabletRestoreStatus::RESTORE_STATUS_MAX;
  ObArray<ObTabletID> need_remove_tablet;
  ObLSRestoreStatus ls_restore_status = restore_state_handler_->get_restore_status();
  tablet_group.action_ = get_common_restore_action_(ls_restore_status);
  tablet_group.from_q_type_ = ToRestoreFromQType::FROM_WAIT_TABLETS_Q;
  tablet_group.task_type_ = TaskType::TABLET_GROUP_RESTORE_TASK;
  FOREACH_X(iter, wait_tablet_set_, OB_SUCC(ret)) {
    const ObTabletID &tablet_id = iter->first;
    if (OB_FAIL(check_tablet_is_restoring_(tablet_id, is_restoring))) {
      LOG_WARN("failed to check tablet is restoring", K(ret), K_(ls_id), K(tablet_id));
    } else if (is_restoring) {
      LOG_INFO("tablet is restoring, skip it this time", K_(ls_id), K(tablet_id));
    } else if (OB_FAIL(check_tablet_status_(ls, iter->first, is_exist, is_restored, restore_status))) {
      LOG_WARN("failed to check tablet status", K(ret), K_(ls_id), K(tablet_id));
    } else if (!is_exist || is_restored || ObTabletRestoreStatus::is_full(restore_status)) {
      if (OB_FAIL(need_remove_tablet.push_back(iter->first))) {
        LOG_WARN("failed to push back tablet", K(ret));
      } else {
        LOG_INFO("remove not exist or restored tablet or full tablet", K(ls), K(tablet_id), K(is_exist), K(is_restored), K(restore_status));
      }
    } else if (OB_FAIL(tablet_group.tablet_list_.push_back(iter->first))) {
      LOG_WARN("fail to push backup tablet", K(ret));
    } else if (tablet_group.count() >= LOW_PRI_TABLETS_BATCH_NUM) {
      break;
    }
  }

  if(!need_remove_tablet.empty()) {
    int tmp_ret = OB_SUCCESS;
    ARRAY_FOREACH(need_remove_tablet, i) {
      if (OB_TMP_FAIL(wait_tablet_set_.erase_refactored(need_remove_tablet.at(i)))) {
        LOG_WARN("failed to erase from wait_tablet_set_", K(tmp_ret));
      }
    }
  }

  if (OB_SUCC(ret) && !tablet_group.empty()) {
    LOG_INFO("succeed choose tablets from wait set", K_(ls_id), K(tablet_group), KP(&high_pri_wait_tablet_set_), KP(&wait_tablet_set_));
  }

  return ret;
}

int ObLSRestoreTaskMgr::choose_tablets_from_high_pri_tablet_set_(
    storage::ObLS &ls,
    ObLSRestoreTaskMgr::ToRestoreTabletGroup &tablet_group)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mtx_);

  bool is_exist = false;
  bool is_restored = false;
  bool is_restoring = false;
  ObTabletRestoreStatus::STATUS restore_status = ObTabletRestoreStatus::RESTORE_STATUS_MAX;
  ObArray<ObTabletID> need_remove_tablet;
  tablet_group.action_ = ObTabletRestoreAction::RESTORE_MINOR;
  tablet_group.from_q_type_ = ToRestoreFromQType::FROM_HIGH_PRI_WAIT_TABLETS_Q;
  tablet_group.task_type_ = TaskType::TABLET_GROUP_RESTORE_TASK;
  FOREACH_X(iter, high_pri_wait_tablet_set_, OB_SUCC(ret)) {
    const ObTabletID &tablet_id = iter->first;
    if (OB_FAIL(check_tablet_is_restoring_(tablet_id, is_restoring))) {
      LOG_WARN("failed to check tablet is restoring", K(ret), K_(ls_id), K(tablet_id));
    } else if (is_restoring) {
      LOG_INFO("tablet is restoring, skip it this time", K_(ls_id), K(tablet_id));
    } else if (OB_FAIL(check_tablet_status_(ls, tablet_id, is_exist, is_restored, restore_status))) {
      LOG_WARN("failed to check tablet status", K(ret), K_(ls_id), K(tablet_id));
    } else if (!is_exist || is_restored) {
      if (OB_FAIL(need_remove_tablet.push_back(tablet_id))) {
        LOG_WARN("failed to push back tablet", K(ret));
      } else {
        LOG_INFO("remove not exist or restored tablet", K_(ls_id), K(tablet_id), K(is_exist), K(is_restored), K(restore_status));
      }
    } else if (ObTabletRestoreStatus::is_full(restore_status)) {
      LOG_INFO("this tablet need wait transfer replace", K_(ls_id), K(tablet_id));
    } else if (OB_FAIL(tablet_group.tablet_list_.push_back(tablet_id))) {
      LOG_WARN("fail to push backup tablet", K(ret));
    } else if (tablet_group.count() >= HIGH_PRI_TABLETS_BATCH_NUM) {
      break;
    }
  }

  if(!need_remove_tablet.empty()) {
    int tmp_ret = OB_SUCCESS;
    ARRAY_FOREACH(need_remove_tablet, i) {
      if (OB_TMP_FAIL(high_pri_wait_tablet_set_.erase_refactored(need_remove_tablet.at(i)))) {
        LOG_WARN("failed to erase from high_pri_wait_tablet_set_", K(tmp_ret));
      }
    }
  }

  if (OB_SUCC(ret) && !tablet_group.empty()) {
    LOG_INFO("succeed to choose tablets from high pri tablet set", K_(ls_id), K(tablet_group), KP(&high_pri_wait_tablet_set_), KP(&wait_tablet_set_));
  }

  return ret;
}

int ObLSRestoreTaskMgr::check_tablet_status_(
    storage::ObLS &ls,
    const common::ObTabletID &tablet_id,
    bool &is_exist,
    bool &is_restored,
    ObTabletRestoreStatus::STATUS &restore_status) const
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObLSRestoreStatus ls_restore_status = restore_state_handler_->get_restore_status();

  is_exist = false;
  is_restored = false;

  if (OB_FAIL(ls.ha_get_tablet(tablet_id, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      is_exist = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id));
    }
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nullptr tablet", K(ret));
  } else if (OB_FALSE_IT(is_exist = true)) {
  } else if (OB_FAIL(tablet->get_restore_status(restore_status))) {
    LOG_WARN("failed to get restore status", K(ret), KPC(tablet));
  } else if (OB_FAIL(is_tablet_restore_finish_(ls_restore_status, tablet_handle, is_restored))) {
    LOG_WARN("fail to check tablet restore finish", K(ret), K(ls_restore_status), K(tablet_handle));
  }
  return ret;
}

ObTabletRestoreAction::ACTION ObLSRestoreTaskMgr::get_common_restore_action_(
    const share::ObLSRestoreStatus &ls_restore_status) const
{
  ObTabletRestoreAction::ACTION action = ObTabletRestoreAction::RESTORE_NONE;
  switch (ls_restore_status.get_status()) {
    case ObLSRestoreStatus::RESTORE_TABLETS_META : {
      action = ObTabletRestoreAction::RESTORE_TABLET_META;
      break;
    }

    case ObLSRestoreStatus::RESTORE_TO_CONSISTENT_SCN : {
      action = ObTabletRestoreAction::RESTORE_TABLET_META;
      break;
    }

    case ObLSRestoreStatus::QUICK_RESTORE: {
      action = ObTabletRestoreAction::RESTORE_MINOR;
      break;
    }

    case ObLSRestoreStatus::RESTORE_MAJOR_DATA : {
      action = ObTabletRestoreAction::RESTORE_MAJOR;
      break;
    }

    default: {
      action = ObTabletRestoreAction::RESTORE_NONE;
      break;
    }
  }

  return action;
}

void ObLSRestoreTaskMgr::unset_force_reload_()
{
  ATOMIC_STORE(&force_reload_, false);
}

bool ObLSRestoreTaskMgr::has_no_tablets_to_restore() const
{
  return wait_tablet_set_.empty() && high_pri_wait_tablet_set_.empty();
}

bool ObLSRestoreTaskMgr::has_no_tablets_restoring() const
{
  return tablet_map_.empty();
}

int ObLSRestoreTaskMgr::check_tablet_is_restoring_(
    const common::ObTabletID &tablet_id,
    bool &is_restoring) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(schedule_tablet_set_.exist_refactored(tablet_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      is_restoring = false;
      ret = OB_SUCCESS;
    } else if (OB_HASH_EXIST == ret) {
      is_restoring = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check tablet exist in schedule set", K(ret), K_(ls_id), K(tablet_id));
    }
  }

  return ret;
}

void ObLSRestoreTaskMgr::set_noneed_redo_failed_tablets_()
{
  lib::ObMutexGuard guard(mtx_);
  FOREACH(iter, tablet_map_) {
    iter->second.need_redo_failed_tablets_ = false;
  }
}

int ObLSRestoreTaskMgr::handle_task_finish_(
    ObLS *ls,
    const ToRestoreTabletGroup &restored_tg,
    ObIArray<common::ObTabletID> &restored_tablets,
    ObIArray<common::ObTabletID> &high_pri_tablet_need_redo,
    ObIArray<common::ObTabletID> &wait_tablet_need_redo)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  bool is_restored = false;
  ObTabletRestoreStatus::STATUS restore_status = ObTabletRestoreStatus::RESTORE_STATUS_MAX;
  const ObIArray<ObTabletID> &tablet_id_array = restored_tg.get_tablet_list();
  // find all finished task, and group all tablet by restored or not.
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_array.count(); ++i) {
    const ObTabletID &tablet_id = tablet_id_array.at(i);
    if (OB_FAIL(check_tablet_status_(*ls, tablet_id, is_exist, is_restored, restore_status))) {
      LOG_WARN("fail to check tablet status", K(ret), KPC(ls), K(tablet_id));
    } else if (!is_exist) {
      LOG_INFO("this tablet is not exist, may be deleted", K_(ls_id), K(tablet_id));
    } else if (is_restored) {
      // if tablet is restored by leader, then it will be send to follower to restore.
      if (OB_FAIL(restored_tablets.push_back(tablet_id))) {
        LOG_WARN("fail to push tablet id", K(ret), K(tablet_id));
      }
    } else if (!restored_tg.need_redo_failed_tablets()) {
      final_reload_ = false;
      LOG_INFO("skip the failed tablet when need_redo_failed_tablets marked true", K_(ls_id), K(tablet_id));
    } else if (restored_tg.from_q_type() == ToRestoreFromQType::FROM_HIGH_PRI_WAIT_TABLETS_Q) {
      if (OB_FAIL(high_pri_tablet_need_redo.push_back(tablet_id))) {
        LOG_WARN("fail to push back tablet to high_pri_tablet_need_redo", K(ret));
      } else {
        LOG_INFO("this tablet need redo", K_(ls_id), K(tablet_id), K(restore_status));
      }
    } else if (OB_FAIL(wait_tablet_need_redo.push_back(tablet_id))) {
      LOG_WARN("fail to push tablet id to wait_tablet_need_redo", K(ret));
    } else {
      LOG_INFO("this tablet need redo", K_(ls_id), K(tablet_id), K(restore_status));
    }
  }

  return ret;
}

int ObLSRestoreTaskMgr::redo_failed_tablets_(
    ObIArray<common::ObTabletID> &high_pri_tablet_need_redo,
    ObIArray<common::ObTabletID> &wait_tablet_need_redo)
{
  int ret = OB_SUCCESS;
  // add redo tablets back to todo set.
  ARRAY_FOREACH(high_pri_tablet_need_redo, i) {
    const ObTabletID &tablet_id = high_pri_tablet_need_redo.at(i);
    if (OB_FAIL(high_pri_wait_tablet_set_.set_refactored(tablet_id))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to insert tablet id to high_pri_wait_tablet_set_", K(ret));
      }
    }
  }

  ARRAY_FOREACH(wait_tablet_need_redo, i) {
    const ObTabletID &tablet_id = wait_tablet_need_redo.at(i);
    if (OB_FAIL(wait_tablet_set_.set_refactored(tablet_id))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to insert tablet id to wait_tablet_set_", K(ret));
      }
    }
  }

  return ret;
}

void ObLSRestoreTaskMgr::remove_finished_task_(const ObIArray<ObTaskId> &finish_task)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_NORET(finish_task, i) {
    const ObTaskId &task_id = finish_task.at(i);
    const ToRestoreTabletGroup *restored_tg = nullptr;
    if (OB_ISNULL(restored_tg = tablet_map_.get(task_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task id not exist", K(ret), K(task_id));
    } else if (!restored_tg->is_tablet_group_task()) {
    } else {
      const ObIArray<ObTabletID> &tablet_id_array = restored_tg->get_tablet_list();
      ARRAY_FOREACH_NORET(tablet_id_array, i) {
        // overwite ret
        const ObTabletID &tablet_id = tablet_id_array.at(i);
        // remove from DOING set
        if (OB_FAIL(schedule_tablet_set_.erase_refactored(tablet_id))) {
          LOG_WARN("fail to erase tablet id", K(ret), K(tablet_id));
        }
      }
    }

    // remove from task map
    if (OB_FAIL(tablet_map_.erase_refactored(task_id))) {
      // overwrite ret
      LOG_WARN("fail to erase task id", K(ret), K(task_id));
    }
  }

  if (tablet_map_.empty()) {
    schedule_tablet_set_.reuse();
  }
}
