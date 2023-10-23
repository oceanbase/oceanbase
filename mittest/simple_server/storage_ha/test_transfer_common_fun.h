/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/ob_errno.h"
#include "rootserver/ob_tenant_transfer_service.h" // ObTenantTransferService
#include "share/transfer/ob_transfer_task_operator.h" // ObTransferTaskOperator
#include "storage/high_availability/ob_transfer_handler.h"  //ObTransferHandler
#include "lib/utility/utility.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_service.h"

#include <thread>

namespace oceanbase
{
namespace storage
{

int wait_transfer_out_deleted_tablet_gc(
    const uint64_t tenant_id,
    const ObTransferTask &task)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    const ObLSID &ls_id = task.src_ls_;
    ObLS *ls = nullptr;
    ObLSHandle ls_handle;
    ObLSService *ls_svr = nullptr;
    const int64_t current_time = ObTimeUtil::current_time();
    const int64_t MAX_WAIT_TIME = 5 * 60 * 1000 * 1000; //5min
    const int64_t SLEEP_INTERVAL = 200 * 1000; // 200ms

    if (!task.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wait transfer out deleted tablet gc get invalid argument", K(ret), K(task));
    } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service should not be NULL", K(ret), K(task));
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id));
    } else {
      int64_t index = 0;
      while (OB_SUCC(ret) && index < task.tablet_list_.count()) {
        ObTabletHandle tablet_handle;
        const ObTransferTabletInfo &transfer_info = task.tablet_list_.at(index);
        if (OB_FAIL(ls->get_tablet(transfer_info.tablet_id_, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
          if (OB_TABLET_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            index++;
          } else {
            LOG_WARN("failed to get tablet", K(ret), K(transfer_info));
          }
        } else if (ObTimeUtil::current_time() - current_time >= MAX_WAIT_TIME) {
          ret = OB_TIMEOUT;
          LOG_WARN("wait transfer out delete tablet gc timeout", K(ret), K(transfer_info));
        } else {
          usleep(SLEEP_INTERVAL);
        }
      }
    }
  }
  return ret;
}

int wait_transfer_task(
    const uint64_t tenant_id,
    const ObTransferTaskID task_id,
    const ObTransferStatus &expected_status,
    const bool &is_from_his,
    ObMySQLProxy &inner_sql_proxy,
    ObTransferTask &task)
{
  int ret = OB_SUCCESS;
  task.reset();
  int64_t create_time = OB_INVALID_TIMESTAMP;
  int64_t finish_time = OB_INVALID_TIMESTAMP;
  const int64_t current_time = ObTimeUtil::current_time();
  const int64_t MAX_WAIT_TIME = 5 * 60 * 1000 * 1000; //5min
  const int64_t SLEEP_INTERVAL = 200 * 1000; // 200ms
  bool get_task = false;

  if (!task_id.is_valid() || !expected_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wait transfer task get invalid argument", K(ret), K(task_id), K(expected_status));
  } else if (is_from_his) {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ObTransferTaskOperator::get_history_task(inner_sql_proxy, tenant_id, task_id, task, create_time, finish_time))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          if (ObTimeUtil::current_time() - current_time >= MAX_WAIT_TIME) {
            LOG_WARN("cannot wait transfer task", K(ret), K(task_id), K(expected_status));
          } else {
            ret = OB_SUCCESS;
            usleep(SLEEP_INTERVAL);
          }
        } else {
          LOG_WARN("failed to get history task", K(ret), K(task_id), K(expected_status));
        }
      } else {
        break;
      }
    }
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ObTransferTaskOperator::get_task_with_time(inner_sql_proxy, tenant_id, task_id, false/*for_update*/,
          task, create_time, finish_time))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          get_task = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get history task", K(ret), K(task_id), K(expected_status));
        }
      } else if (task.get_status() != expected_status) {
        get_task = false;
      } else {
        get_task = true;
        break;
      }

      if (OB_SUCC(ret) && !get_task) {
        if (ObTimeUtil::current_time() - current_time >= MAX_WAIT_TIME) {
          LOG_WARN("cannot wait transfer task", K(ret), K(task_id), K(expected_status));
        } else {
          ret = OB_SUCCESS;
          usleep(SLEEP_INTERVAL);
        }
      }
    }
  }
  return ret;
}

int wait_transfer_task_after_restart(
    const uint64_t tenant_id,
    ObMySQLProxy &inner_sql_proxy)
{
  int ret = OB_SUCCESS;
  int64_t create_time = OB_INVALID_TIMESTAMP;
  int64_t finish_time = OB_INVALID_TIMESTAMP;
  const int64_t current_time = ObTimeUtil::current_time();
  const int64_t MAX_WAIT_TIME = 5 * 60 * 1000 * 1000; //5min
  const int64_t SLEEP_INTERVAL = 200 * 1000; // 200ms
  bool get_task = false;
  common::ObArray<ObTransferTask::TaskStatus> task_status;

  while (OB_SUCC(ret)) {
    if (OB_FAIL(ObTransferTaskOperator::get_all_task_status(inner_sql_proxy, tenant_id, task_status))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        get_task = true;
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get all task status", K(ret));
      }
    } else {
      get_task = false;
    }

    if (!get_task) {
      if (ObTimeUtil::current_time() - current_time >= MAX_WAIT_TIME) {
        LOG_WARN("cannot wait transfer task", K(ret));
      } else {
        ret = OB_SUCCESS;
        usleep(SLEEP_INTERVAL);
      }
    }
  }
  return ret;
}

int wait_start_transfer_tablet_mds_flush(
    const uint64_t tenant_id,
    const ObTransferTask &task)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    const ObLSID &ls_id = task.dest_ls_;
    ObLS *ls = nullptr;
    ObLSHandle ls_handle;
    ObLSService *ls_svr = nullptr;
    const int64_t current_time = ObTimeUtil::current_time();
    const int64_t MAX_WAIT_TIME = 5 * 60 * 1000 * 1000; //5min
    const int64_t SLEEP_INTERVAL = 200 * 1000; // 200ms

    if (!task.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wait transfer out deleted tablet gc get invalid argument", K(ret), K(task));
    } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service should not be NULL", K(ret), K(task));
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id));
    } else {
      int64_t index = 0;
      const SCN &scn = task.start_scn_;
      while (OB_SUCC(ret) && index < task.tablet_list_.count()) {
        ObTabletHandle tablet_handle;
        const ObTransferTabletInfo &transfer_info = task.tablet_list_.at(index);
        ObTablet *tablet = nullptr;
        ObTabletCreateDeleteMdsUserData user_data;
        bool unused_committed_flag = false;
        if (OB_FAIL(ls->get_tablet(transfer_info.tablet_id_, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
          LOG_WARN("failed to get tablet", K(ret), K(transfer_info));
        } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet should not be NULL", K(ret), K(transfer_info));
        } else if (tablet->get_tablet_meta().mds_checkpoint_scn_ <= scn) {
          if (ObTimeUtil::current_time() - current_time >= MAX_WAIT_TIME) {
            ret = OB_TIMEOUT;
            LOG_WARN("wait transfer out delete tablet gc timeout", K(ret), K(transfer_info));
          } else {
            usleep(SLEEP_INTERVAL);
          }
        } else {
          index++;
        }
      }
    }
  }
  return ret;
}

int wait_finish_transfer_tablet_mds_flush(
    const uint64_t tenant_id,
    const ObTransferTask &task)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    const ObLSID &ls_id = task.dest_ls_;
    ObLS *ls = nullptr;
    ObLSHandle ls_handle;
    ObLSService *ls_svr = nullptr;
    const int64_t current_time = ObTimeUtil::current_time();
    const int64_t MAX_WAIT_TIME = 5 * 60 * 1000 * 1000; //5min
    const int64_t SLEEP_INTERVAL = 200 * 1000; // 200ms

    if (!task.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("wait transfer out deleted tablet gc get invalid argument", K(ret), K(task));
    } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service should not be NULL", K(ret), K(task));
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id));
    } else {
      int64_t index = 0;
      const SCN &scn = task.finish_scn_;
      while (OB_SUCC(ret) && index < task.tablet_list_.count()) {
        ObTabletHandle tablet_handle;
        const ObTransferTabletInfo &transfer_info = task.tablet_list_.at(index);
        ObTablet *tablet = nullptr;
        ObTabletCreateDeleteMdsUserData user_data;
        bool unused_committed_flag = false;
        if (OB_FAIL(ls->get_tablet(transfer_info.tablet_id_, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
          LOG_WARN("failed to get tablet", K(ret), K(transfer_info));
        } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet should not be NULL", K(ret), K(transfer_info));
        } else if (tablet->get_tablet_meta().mds_checkpoint_scn_ < scn) {
          if (OB_FAIL(ObTabletCreateDeleteMdsUserData::set_tablet_gc_trigger(task.dest_ls_))) {
            LOG_WARN("failed to set_tablet_gc_trigger", K(ret), K(task));
          } else if (ObTimeUtil::current_time() - current_time >= MAX_WAIT_TIME) {
            ret = OB_TIMEOUT;
            LOG_WARN("wait transfer out delete tablet gc timeout", K(ret), K(transfer_info), KPC(tablet));
          } else {
            usleep(SLEEP_INTERVAL);
          }
        } else {
          index++;
        }
      }
    }
  }
  return ret;
}


}
}
