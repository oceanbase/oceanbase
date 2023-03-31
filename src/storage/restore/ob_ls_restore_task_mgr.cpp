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

using namespace oceanbase;
using namespace share;
using namespace common;
using namespace storage;
using namespace backup;
using namespace logservice;

ObLSRestoreTaskMgr::ObLSRestoreTaskMgr()
  : is_inited_(false),
    mtx_(),
    tablet_map_(),
    schedule_tablet_set_(),
    wait_tablet_set_()
{
}

ObLSRestoreTaskMgr::~ObLSRestoreTaskMgr()
{
}

int ObLSRestoreTaskMgr::init()
{
  int ret = OB_SUCCESS;
  const char *tablet_dag_net_task = "tabletDagNetTask";

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(tablet_map_.create(OB_RESTORE_MAX_DAG_NET_NUM, tablet_dag_net_task))) {
    LOG_WARN("fail to create tablet_map_", K(ret));
  } else if (OB_FAIL(schedule_tablet_set_.create(OB_LS_RESTORE_MAX_TABLET_NUM))) {
    LOG_WARN("fail to create schedule_tablet_set_", K(ret));
  } else if (OB_FAIL(wait_tablet_set_.create(OB_LS_RESTORE_MAX_TABLET_NUM))) {
    LOG_WARN("fail to create schedule_tablet_set_", K(ret));
  } else {
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (tablet_map_.created() && OB_SUCCESS != (tmp_ret = tablet_map_.destroy())) {
      LOG_WARN("fail to destroy tablet map", K(tmp_ret));
    }
    if (schedule_tablet_set_.created() && OB_SUCCESS != (tmp_ret = schedule_tablet_set_.destroy())) {
      LOG_WARN("fail to destroy schedule_tablet_set_", K(tmp_ret));
    }
  }
  return ret;
}

void ObLSRestoreTaskMgr::destroy()
{
  tablet_map_.destroy();
  wait_tablet_set_.destroy();
  schedule_tablet_set_.destroy();
}

int ObLSRestoreTaskMgr::pop_need_restore_tablets(
    storage::ObLS &ls, ObIArray<ObTabletID> &tablet_need_restore)
{
  int ret = OB_SUCCESS;
  tablet_need_restore.reset();
  ObArray<ObTabletID> need_remove_tablet;
  lib::ObMutexGuard guard(mtx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (0 == wait_tablet_set_.size()) {
    LOG_INFO("no tablet need to schedule, wait later");
  } else {
    TabletSet::iterator iter = wait_tablet_set_.begin();
    while (OB_SUCC(ret) && iter != wait_tablet_set_.end()) {
      bool is_deleted = false;
      bool is_restored = false;
      if (OB_FAIL(check_tablet_deleted_or_restored_(ls, iter->first, is_deleted, is_restored))) {
        LOG_WARN("failed to check tablet deleted or restored", K(ret));
      } else if (is_deleted || is_restored) {
        if (OB_FAIL(need_remove_tablet.push_back(iter->first))) {
          LOG_WARN("failed to push back tablet", K(ret));
        } else {
          ++iter;
        }
      } else if (OB_FAIL(tablet_need_restore.push_back(iter->first))) {
        LOG_WARN("fail to push backup tablet", K(ret));
      } else if (tablet_need_restore.count() >= OB_LS_RESOTRE_TABLET_DAG_NET_BATCH_NUM) {
        break;
      } else {
        ++iter;
      }
    }

    if(!need_remove_tablet.empty()) {
      ARRAY_FOREACH(need_remove_tablet, i) {
        if (OB_FAIL(wait_tablet_set_.erase_refactored(need_remove_tablet.at(i)))) {
          LOG_WARN("failed to erase from set", K(ret));
        }
      }
      LOG_INFO("tablets may be deleted or restored and removed from wait set.", K(ls), K(need_remove_tablet));
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("succeed pop need restore tablets", K(tablet_need_restore));
    }
  }
  return ret;
}


int ObLSRestoreTaskMgr::pop_restored_tablets(storage::ObLS &ls, ObIArray<common::ObTabletID> &tablet_send_to_follower)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  tablet_send_to_follower.reset();
  ObArray<ObTaskId> finish_task;
  ObArray<ObTabletID> tablet_need_redo;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    lib::ObMutexGuard guard(mtx_);
    TaskMap::iterator iter = tablet_map_.begin();
    for (; OB_SUCC(ret) && iter != tablet_map_.end(); ++iter) {
      if (OB_FAIL(check_task_exist_(iter->first, is_exist))) {
        LOG_WARN("fail to check task exist", K(ret), "taks_id", iter->first);
      } else if (is_exist) {
        LOG_INFO("task exist, wait later", "taks_id", iter->first);
      } else if (OB_FAIL(finish_task.push_back(iter->first))) {
        LOG_WARN("fail to push back task id", K(ret));
      } else {
        ObIArray<ObTabletID> &tablet_id_array = iter->second;
        for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_array.count(); ++i) {
          const ObTabletID &tablet_id = tablet_id_array.at(i);
          bool is_restored = false;
          bool is_deleted = false;
          if (OB_FAIL(check_tablet_deleted_or_restored_(ls, tablet_id, is_deleted, is_restored))) {
            LOG_WARN("fail to check tabelt restored", K(ret), K(ls), K(tablet_id));
          } else if (is_deleted) {// if tablet is deleted, it is no need to send it to follower to restore.
          } else if (is_restored) {
            // if tablet is restored by leader, then it will be send to follower to restore.
            if (OB_FAIL(tablet_send_to_follower.push_back(tablet_id))) {
              LOG_WARN("fail to push tablet id to tablet send to follower", K(ret), K(tablet_id));
            }
          } else if (OB_FAIL(tablet_need_redo.push_back(tablet_id))) {
            LOG_WARN("fail to push tablet id to tablet_need_redo", K(ret), K(tablet_id));
          }
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_need_redo.count(); ++i) {
      const ObTabletID &tablet_id = tablet_need_redo.at(i);
      if (OB_FAIL(wait_tablet_set_.set_refactored(tablet_id))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to insert tabelt id", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(schedule_tablet_set_.erase_refactored(tablet_id))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_WARN("tablet not exist, no need to erase", K(ret));
        } else {
          LOG_WARN("fail to erase tablet id", K(ret));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < finish_task.count(); ++i) {
      const ObTaskId &task_id = finish_task.at(i);
      if (OB_FAIL(tablet_map_.erase_refactored(task_id))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_WARN("task id not exist, no need to erase", K(ret));
        } else {
          LOG_WARN("fail to erase task id", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("succeed pop restored tablets", K(ret), K(tablet_need_redo), K(tablet_send_to_follower));
    }
  }
  return ret;
}

int ObLSRestoreTaskMgr::schedule_tablet(const ObTaskId &task_id, const ObSArray<ObTabletID> &tablet_need_restore, bool &reach_dag_limit)
{
  int ret = OB_SUCCESS;
  reach_dag_limit = false;
  lib::ObMutexGuard guard(mtx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (task_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id), K(tablet_need_restore));
  } else if (tablet_map_.size() >= OB_RESTORE_MAX_DAG_NET_NUM) {
    reach_dag_limit = true;
    LOG_INFO("two many restore dag net, wait later", "dag_net num", tablet_map_.size());
  } else if (OB_FAIL(tablet_map_.set_refactored(task_id, tablet_need_restore))) {
    LOG_WARN("fail to set task id map", K(ret), K(task_id), K(tablet_need_restore));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_need_restore.count(); ++i) {
      const ObTabletID &tablet_id = tablet_need_restore.at(i);
      if (OB_FAIL(wait_tablet_set_.erase_refactored(tablet_id))) {
        LOG_WARN("fail to erase tablet id", K(ret), K(tablet_id));
      } else if (OB_FAIL(schedule_tablet_set_.set_refactored(tablet_id))) {
        LOG_WARN("fail to set tabelt set", K(ret), K(tablet_id));
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
        LOG_WARN("fail to check task exist", K(ret), "taks_id", iter->first);
      } else if (is_exist) {
        ObTenantDagScheduler *scheduler = nullptr;
        if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret), KP(scheduler));
        } else if (OB_FAIL(scheduler->cancel_dag_net(iter->first))) {
          LOG_WARN("failed to check dag net exist", K(ret), K(iter->first));
        }
      }
    }

    int64_t start_ts = ObTimeUtil::current_time();
    for (; OB_SUCC(ret) && iter != tablet_map_.end(); ++iter) {
      is_exist = true;
      do {
        if (OB_FAIL(check_task_exist_(iter->first, is_exist))) {
          LOG_WARN("fail to check task exist", K(ret), "taks_id", iter->first);
        } else if (is_exist && REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
          LOG_WARN("cancel dag next task cost too much time", K(ret), "task_id", iter->first,
              "cost_time", ObTimeUtil::current_time() - start_ts);
        }
      } while (is_exist && OB_SUCC(ret));
    }

    if (OB_SUCC(ret)) {
      reuse_set();
      tablet_map_.reuse();
    }
  }
  return ret;
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
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      const ObTabletID &tablet_id = tablet_ids.at(i);
      if (!tablet_id.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet id", K(ret), K(tablet_id));
      } else if (OB_FAIL(schedule_tablet_set_.exist_refactored(tablet_id))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
          continue;
        } else {
          LOG_WARN("fail to check tablet exist in schedule set", K(ret), K(tablet_id));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(wait_tablet_set_.set_refactored(tablet_id))) {
        LOG_WARN("fail to insert tablet id into wait tablet set", K(ret), K(tablet_id));
      }
    }
    if(OB_SUCC(ret)) {
      LOG_INFO("success to add tablet in wait set", K(tablet_ids));
    }
  }
  return ret;
}

int ObLSRestoreTaskMgr::add_tablet_in_schedule_set(const ObIArray<common::ObTabletID> &tablet_ids)
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
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      const ObTabletID &tablet_id = tablet_ids.at(i);
      if (!tablet_id.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet id", K(ret), K(tablet_id));
      } else if (OB_FAIL(schedule_tablet_set_.set_refactored(tablet_id))) {
        LOG_WARN("fail to insert tablet id into schedule tablet set", K(ret), K(tablet_id));
      }
    }
    if(OB_SUCC(ret)) {
      LOG_INFO("success to add tablet in schedule set", K(tablet_ids));
    }
  }
  return ret;
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

int ObLSRestoreTaskMgr::check_tablet_deleted_or_restored_(storage::ObLS &ls, const common::ObTabletID &tablet_id,
    bool &is_deleted, bool &is_restored)
{
  // tablets may be deleted while replaying clog.
  // restore task mgr should skip the deleted tablet.
  int ret = OB_SUCCESS;
  is_deleted = false;
  is_restored = false;
  ObTabletHandle tablet_handle;
  ObLSRestoreStatus ls_restore_status;
  ObTablet *tablet;
  const int64_t timeout_us = ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US;
  if (OB_FAIL(ls.get_restore_status(ls_restore_status))) {
    LOG_WARN("fail to get ls restore status", K(ret), K(ls));
  } else if (OB_FAIL(ls.get_tablet(tablet_id, tablet_handle, timeout_us))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      is_deleted = true;
      ret = OB_SUCCESS;
      LOG_INFO("[LS_RESTORE]tablet has been deleted, no need to check is it restored", K(tablet_id), K(ls));
    } else {
      LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id), K(timeout_us));
    }
  } else if (nullptr == (tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nullptr tablet", K(ret));
  } else if (OB_FAIL(ObLSRestoreHandler::check_tablet_deleted(tablet_handle, is_deleted))) {
    LOG_WARN("failed to check tablet deleted", K(ret), K(tablet_handle));
  } else if (is_deleted) { // do nothing
  } else {
    const ObTabletMeta &tablet_meta = tablet->get_tablet_meta();
    if (OB_FAIL(ObLSRestoreHandler::check_tablet_restore_finish_(ls_restore_status, tablet_meta, is_restored))) {
      LOG_WARN("fail to check tablet restore finish", K(ret), K(tablet_meta), K(ls));
    }
  }
  return ret;
}

