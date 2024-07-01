// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include "lib/container/ob_bit_set.h"
#include "ob_dup_table_lease.h"
#include "ob_dup_table_tablets.h"
#include "ob_dup_table_ts_sync.h"
#include "ob_dup_table_util.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_tx_log_adapter.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{

using namespace storage;
using namespace common;
using namespace share;

namespace transaction
{

typedef ObSEArray<common::ObTabletID, 100> TabletIDArray;

//*************************************************************************************************************
//**** ObDupTabletScanTask
//*************************************************************************************************************
void ObDupTabletScanTask::reset()
{
  tenant_id_ = 0;
  dup_table_scan_timer_ = nullptr;
  dup_loop_worker_ = nullptr;
  min_dup_ls_status_info_.reset();
  tenant_schema_dup_tablet_set_.destroy();
  scan_task_execute_interval_ = ObDupTabletScanTask::DUP_TABLET_SCAN_INTERVAL;
  last_dup_ls_refresh_time_ = 0;
  last_dup_schema_refresh_time_ = 0;
  last_scan_task_succ_time_ = 0;
  max_execute_interval_ = ObDupTabletScanTask::DUP_TABLET_SCAN_INTERVAL;
}

int ObDupTabletScanTask::make(const int64_t tenant_id,
                              ObDupTableLeaseTimer *scan_timer,
                              ObDupTableLoopWorker *loop_worker)
{
  int ret = OB_SUCCESS;
  if (tenant_id <= 0 || OB_ISNULL(loop_worker) || OB_ISNULL(scan_timer)) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid arguments", K(ret), K(tenant_id), KP(loop_worker), KP(scan_timer));
  } else {
    tenant_id_ = tenant_id;
    dup_table_scan_timer_ = scan_timer;
    dup_loop_worker_ = loop_worker;
    min_dup_ls_status_info_.reset();
    tenant_schema_dup_tablet_set_.reuse();
    scan_task_execute_interval_ = ObDupTabletScanTask::DUP_TABLET_SCAN_INTERVAL;
    // ObTransTask::make(ObTransRetryTaskType::DUP_TABLET_SCAN_TASK);
    // set_retry_interval_us(DUP_TABLET_SCAN_INTERVAL, DUP_TABLET_SCAN_INTERVAL);
  }
  return ret;
}

void ObDupTabletScanTask::runTimerTask()
{
  int tmp_ret = OB_SUCCESS;

  if (tenant_id_ <= 0 || OB_ISNULL(dup_loop_worker_) || OB_ISNULL(dup_table_scan_timer_)) {
    tmp_ret = OB_NOT_INIT;
    DUP_TABLE_LOG_RET(WARN, tmp_ret, "invalid arguments", K(tmp_ret), K(tenant_id_),
                      KP(dup_loop_worker_), KP(dup_table_scan_timer_));
  } else {
    if (OB_TMP_FAIL(execute_for_dup_ls_())) {
      DUP_TABLE_LOG_RET(WARN, tmp_ret, "execute dup ls scan failed", K(tmp_ret));
    }

    dup_table_scan_timer_->unregister_timeout_task(*this);
    dup_table_scan_timer_->register_timeout_task(*this, scan_task_execute_interval_);
  }
}

int ObDupTabletScanTask::refresh_dup_ls_(const int64_t cur_time)
{
  int ret = OB_SUCCESS;

  bool need_refresh = true;

  if (min_dup_ls_status_info_.is_valid() && min_dup_ls_status_info_.is_duplicate_ls()) {
    if (cur_time - last_dup_ls_refresh_time_ < MAX_DUP_LS_REFRESH_INTERVAL) {
      need_refresh = false;
    } else {
      need_refresh = true;
    }
  } else {
    need_refresh = true;
  }

  if (need_refresh && OB_SUCC(ret)) {
    share::ObLSStatusInfo tmp_dup_ls_status_info;
    share::ObLSStatusOperator ls_status_op;
    if (OB_FAIL(ls_status_op.get_duplicate_ls_status_info(MTL_ID(), *GCTX.sql_proxy_,
                                                          tmp_dup_ls_status_info, share::OBCG_STORAGE))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        DUP_TABLE_LOG(DEBUG, "no duplicate ls", K(tmp_dup_ls_status_info));
        ret = OB_SUCCESS;
      } else {
        DUP_TABLE_LOG(WARN, "get duplicate ls status info failed", K(ret),
                      K(tmp_dup_ls_status_info));
      }
    } else {
      DUP_TABLE_LOG(DEBUG, "find a duplicate ls", K(ret), K(tmp_dup_ls_status_info));

      if (!tmp_dup_ls_status_info.is_valid() || !tmp_dup_ls_status_info.is_duplicate_ls()) {
        ret = OB_ERR_UNEXPECTED;
        DUP_TABLE_LOG(ERROR, "invalid tmp_dup_ls_status_info", K(ret), K(tmp_dup_ls_status_info));
      } else {
        if (min_dup_ls_status_info_.is_valid() && min_dup_ls_status_info_.is_duplicate_ls()
            && tmp_dup_ls_status_info.get_ls_id() != min_dup_ls_status_info_.get_ls_id()) {
          DUP_TABLE_LOG(ERROR, "The min_dup_ls has already changed", K(ret),
                        K(tmp_dup_ls_status_info), K(min_dup_ls_status_info_));
        }
        if (OB_FAIL(min_dup_ls_status_info_.assign(tmp_dup_ls_status_info))) {
          DUP_TABLE_LOG(WARN, "rewrite min_dup_ls_status_info_ failed", K(ret),
                        K(tmp_dup_ls_status_info), K(min_dup_ls_status_info_));
        } else {
          last_dup_ls_refresh_time_ = cur_time;
        }
      }
    }
  }

  return ret;
}

int ObDupTabletScanTask::refresh_dup_tablet_schema_(const int64_t cur_time)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && min_dup_ls_status_info_.is_valid()
      && min_dup_ls_status_info_.is_duplicate_ls()) {
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!tenant_schema_dup_tablet_set_.created()) {
      if (OB_FAIL(tenant_schema_dup_tablet_set_.create(512))) {
        DUP_TABLE_LOG(WARN, "init dup tablet cache failed", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (cur_time <= last_dup_schema_refresh_time_) {
      // do nothing
    } else if (OB_FAIL(
                   dup_schema_helper_.refresh_and_get_tablet_set(tenant_schema_dup_tablet_set_))) {
      DUP_TABLE_LOG(WARN, "refresh dup tablet set failed", K(ret));
      tenant_schema_dup_tablet_set_.clear();
    } else {
      last_dup_schema_refresh_time_ = cur_time;
    }
  }

  return ret;
}

bool ObDupTabletScanTask::has_valid_dup_schema_() const
{
  bool dup_schema_is_valid = false;

  dup_schema_is_valid = min_dup_ls_status_info_.is_valid()
                        && min_dup_ls_status_info_.is_duplicate_ls()
                        && !tenant_schema_dup_tablet_set_.empty();

  return dup_schema_is_valid;
}

int ObDupTabletScanTask::execute_for_dup_ls_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  TabletIDArray tablet_id_array;
  bool need_refreh_dup_schema = true;
  ObLSHandle ls_handle;

  // compute scan task max execute interval
  const int64_t cur_time = ObTimeUtility::fast_current_time();
  if (cur_time - last_scan_task_succ_time_ > 0) {
    if (0 != last_scan_task_succ_time_) {
      if (max_execute_interval_ / 2 >= (cur_time - last_scan_task_succ_time_)) {
        // Avoid residual excessive execution intervals in exceptional circumstances
        ATOMIC_STORE(&max_execute_interval_, cur_time - last_scan_task_succ_time_);
      } else {
        ATOMIC_STORE(&max_execute_interval_,
                     max(max_execute_interval_, cur_time - last_scan_task_succ_time_));
      }
    } else {
    }
  }

  if (OB_TMP_FAIL(refresh_dup_ls_(cur_time))) {
    DUP_TABLE_LOG(WARN, "refresh dup ls failed", K(tmp_ret), KPC(this));
  } else if (OB_TMP_FAIL(refresh_dup_tablet_schema_(cur_time))) {
    DUP_TABLE_LOG(WARN, "refresh dup schema failed", K(tmp_ret), KPC(this));
  }

  if (OB_ISNULL(MTL(ObLSService *)) || OB_ISNULL(dup_loop_worker_)) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid arguments", K(ret));
  } else if (!has_valid_dup_schema_()) {
    DUP_TABLE_LOG(DEBUG, "refresh dup table schema failed", K(ret), KPC(this));
    // do nothing
  } else if (OB_FAIL(
                 MTL(ObLSService *)
                     ->get_ls(min_dup_ls_status_info_.ls_id_, ls_handle, ObLSGetMod::TRANS_MOD))) {
    DUP_TABLE_LOG(WARN, "get dup ls failed", K(ret), KPC(this));
  } else {

    ObLS *cur_ls_ptr = ls_handle.get_ls();
    if (OB_ISNULL(cur_ls_ptr)) {
      ret = OB_INVALID_ARGUMENT;
      DUP_TABLE_LOG(WARN, "invalid ls ptr", K(ret), KP(cur_ls_ptr));
    } else if (!cur_ls_ptr->get_dup_table_ls_handler()->is_master()) {
#ifndef NDEBUG
      DUP_TABLE_LOG(INFO, "ls not leader", K(cur_ls_ptr->get_ls_id()));
#endif
    } else {
      storage::ObHALSTabletIDIterator ls_tablet_id_iter(cur_ls_ptr->get_ls_id(), true);
      if (OB_FAIL(cur_ls_ptr->build_tablet_iter(ls_tablet_id_iter))) {
        DUP_TABLE_LOG(WARN, "build ls tablet iter failed", K(cur_ls_ptr->get_ls_id()));
      } else if (!ls_tablet_id_iter.is_valid()) {
        DUP_TABLE_LOG(WARN, "invalid tablet id iterator", K(cur_ls_ptr->get_ls_id()));
      } else {
        ObTabletID tmp_tablet_id;
        bool is_dup_tablet = false;
        int64_t refresh_time = ObTimeUtility::fast_current_time();
        while (OB_SUCC(ls_tablet_id_iter.get_next_tablet_id(tmp_tablet_id))) {
          is_dup_tablet = false;
          ret = tenant_schema_dup_tablet_set_.exist_refactored(tmp_tablet_id);
          if (OB_HASH_EXIST == ret) {
            is_dup_tablet = true;
            ret = OB_SUCCESS;
          } else if (OB_HASH_NOT_EXIST == ret) {
            is_dup_tablet = false;
            ret = OB_SUCCESS;
          } else {
            DUP_TABLE_LOG(WARN,
                          "Failed to check whether the tablet exists in the tenant_dup_tablet_set",
                          K(ret), K(cur_ls_ptr->get_ls_id()), K(tmp_tablet_id));
          }

          if (OB_FAIL(ret)) {
            // do nothing
          } else if (!cur_ls_ptr->get_dup_table_ls_handler()->is_inited() && !is_dup_tablet) {
            // do nothing
          } else if (OB_FAIL(cur_ls_ptr->get_dup_table_ls_handler()->init(is_dup_tablet))
                     && OB_INIT_TWICE != ret) {
            DUP_TABLE_LOG(WARN, "init dup tablet ls handler", K(ret));
          } else if (OB_FAIL(cur_ls_ptr->get_dup_table_ls_handler()->refresh_dup_table_tablet(
                         tmp_tablet_id, is_dup_tablet, refresh_time))) {
            if (is_dup_tablet || OB_NOT_INIT != ret) {
              DUP_TABLE_LOG(WARN, "refresh ls dup table tablets failed", K(ret), K(tmp_tablet_id),
                            K(is_dup_tablet));
            } else {
              ret = OB_SUCCESS;
            }
          }
        }

        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }

    // refresh dup_table_ls on leader and follower
    if (!cur_ls_ptr->get_dup_table_ls_handler()->is_inited()
        || !cur_ls_ptr->get_dup_table_ls_handler()->check_tablet_set_exist()) {
      // do nothing
    } else if (OB_TMP_FAIL(dup_loop_worker_->append_dup_table_ls(cur_ls_ptr->get_ls_id()))) {
      DUP_TABLE_LOG(WARN, "refresh dup_table ls failed", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    DUP_TABLE_LOG(WARN, "scan dup ls to find dup_tablet failed", KR(ret));
  } else {
    ATOMIC_STORE(&last_scan_task_succ_time_, cur_time);
  }

#ifndef NDEBUG
  DUP_TABLE_LOG(INFO, "execute dup table scan task", KPC(this));
#else

#endif
  return ret;
}

//*************************************************************************************************************
//**** ObDupTableLSHandler
//*************************************************************************************************************

int ObDupTableLSHandler::init(bool is_dup_table)
{
  int ret = OB_SUCCESS;
  bool is_constructed_ = false;
  if (is_dup_table) {
    if (is_inited()) {
      ret = OB_INIT_TWICE;
    } else {
      SpinWLockGuard init_w_guard(init_rw_lock_);
      // init by dup_tablet_scan_task_.

      ObDupTableLogOperator *tmp_log_operator = static_cast<ObDupTableLogOperator *>(
          share::mtl_malloc(sizeof(ObDupTableLogOperator), "DUP_LOG_OP"));

      ObDupTableLSLeaseMgr *tmp_lease_mgr_ptr =
          static_cast<ObDupTableLSLeaseMgr *>(ob_malloc(sizeof(ObDupTableLSLeaseMgr), "DupTable"));
      ObDupTableLSTsSyncMgr *tmp_ts_sync_mgr_ptr = static_cast<ObDupTableLSTsSyncMgr *>(
          ob_malloc(sizeof(ObDupTableLSTsSyncMgr), "DupTable"));
      ObLSDupTabletsMgr *tmp_tablets_mgr_ptr =
          static_cast<ObLSDupTabletsMgr *>(ob_malloc(sizeof(ObLSDupTabletsMgr), "DupTable"));

      if (OB_ISNULL(tmp_log_operator) || OB_ISNULL(tmp_lease_mgr_ptr)
          || OB_ISNULL(tmp_ts_sync_mgr_ptr) || OB_ISNULL(tmp_tablets_mgr_ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        DUP_TABLE_LOG(WARN, "alloc memory in ObDupTableLSHandler::init failed", K(ret),
                      KP(tmp_log_operator), KP(lease_mgr_ptr_), KP(ts_sync_mgr_ptr_),
                      KP(tablets_mgr_ptr_));
      } else {
        new (tmp_lease_mgr_ptr) ObDupTableLSLeaseMgr();
        new (tmp_ts_sync_mgr_ptr) ObDupTableLSTsSyncMgr();
        new (tmp_tablets_mgr_ptr) ObLSDupTabletsMgr();
        new (tmp_log_operator)
            ObDupTableLogOperator(ls_id_, log_handler_, &dup_ls_ckpt_, tmp_lease_mgr_ptr,
                                  tmp_tablets_mgr_ptr, &interface_stat_);
        is_constructed_ = true;

        if (OB_FAIL(tmp_lease_mgr_ptr->init(this))) {
          DUP_TABLE_LOG(WARN, "init lease_mgr failed", K(ret));
        } else if (OB_FAIL(tmp_ts_sync_mgr_ptr->init(this))) {
          DUP_TABLE_LOG(WARN, "init ts_sync_mgr failed", K(ret));
        } else if (OB_FAIL(tmp_tablets_mgr_ptr->init(this))) {
          DUP_TABLE_LOG(WARN, "init tablets_mgr failed", K(ret));
        } else {
          lease_mgr_ptr_ = tmp_lease_mgr_ptr;
          ts_sync_mgr_ptr_ = tmp_ts_sync_mgr_ptr;
          tablets_mgr_ptr_ = tmp_tablets_mgr_ptr;
          log_operator_ = tmp_log_operator;
          if (ls_state_helper_.is_leader()
              && OB_FAIL(leader_takeover_(true /*is_resume*/, true /*is_initing*/))) {
            DUP_TABLE_LOG(WARN, "leader takeover in init failed", K(ret));
          } else {
            is_inited_ = true;
          }
        }
      }

      if (OB_FAIL(ret)) {
        lease_mgr_ptr_ = nullptr;
        ts_sync_mgr_ptr_ = nullptr;
        tablets_mgr_ptr_ = nullptr;
        log_operator_ = nullptr;
        if (OB_NOT_NULL(tmp_lease_mgr_ptr)) {
          if (is_constructed_) {
            tmp_lease_mgr_ptr->~ObDupTableLSLeaseMgr();
          }
          ob_free(tmp_lease_mgr_ptr);
        }
        if (OB_NOT_NULL(tmp_ts_sync_mgr_ptr)) {
          if (is_constructed_) {
            tmp_ts_sync_mgr_ptr->~ObDupTableLSTsSyncMgr();
          }
          ob_free(tmp_ts_sync_mgr_ptr);
        }
        if (OB_NOT_NULL(tmp_tablets_mgr_ptr)) {
          if (is_constructed_) {
            tmp_tablets_mgr_ptr->~ObLSDupTabletsMgr();
          }
          ob_free(tmp_tablets_mgr_ptr);
        }
        if (OB_NOT_NULL(tmp_log_operator)) {
          if (is_constructed_) {
            tmp_log_operator->~ObDupTableLogOperator();
          }
          ob_free(tmp_log_operator);
        }
      }
      DUP_TABLE_LOG(INFO, "ls handler init", K(ret), KPC(this));
    }
  }
  return ret;
}

void ObDupTableLSHandler::start()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObDupTableLSRoleStateContainer restore_state_container;
  if (OB_FAIL(ls_state_helper_.prepare_state_change(ObDupTableLSRoleState::LS_START_SUCC,
                                                    restore_state_container))) {
    DUP_TABLE_LOG(WARN, "prepare state change failed", K(ret), KPC(this));
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ls_state_helper_.state_change_succ(ObDupTableLSRoleState::LS_START_SUCC,
                                                   restore_state_container))) {
      DUP_TABLE_LOG(ERROR, "change ls role state error", K(ret), KPC(this));
    }
  }
}

void ObDupTableLSHandler::stop()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObDupTableLSRoleStateContainer restore_state_container;
  if (OB_FAIL(ls_state_helper_.prepare_state_change(ObDupTableLSRoleState::LS_STOP_SUCC,
                                                    restore_state_container))) {
    DUP_TABLE_LOG(WARN, "prepare state change failed", K(ret), KPC(this));
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ls_state_helper_.state_change_succ(ObDupTableLSRoleState::LS_STOP_SUCC,
                                                   restore_state_container))) {
      DUP_TABLE_LOG(ERROR, "change ls role state error", K(ret), KPC(this));
    }
  }
}

int ObDupTableLSHandler::offline()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObDupTableLSRoleStateContainer restore_state_container;
  if (OB_FAIL(ls_state_helper_.prepare_state_change(ObDupTableLSRoleState::LS_OFFLINE_SUCC,
                                                    restore_state_container))) {
    DUP_TABLE_LOG(WARN, "prepare state change failed", K(ret), KPC(this));
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    }
  } else {
    SpinRLockGuard r_init_guard(init_rw_lock_);
    if (is_inited_) {
      if (OB_NOT_NULL(log_operator_) && log_operator_->is_busy()) {
        ret = OB_EAGAIN;
        DUP_TABLE_LOG(WARN, "wait log synced before offline", K(ret), KPC(this));
      } else if (OB_NOT_NULL(tablets_mgr_ptr_) && OB_FAIL(tablets_mgr_ptr_->offline())) {
        DUP_TABLE_LOG(WARN, "dup tablets mgr offline failed", K(ret), KPC(this));
      } else if (OB_NOT_NULL(lease_mgr_ptr_) && OB_FAIL(lease_mgr_ptr_->offline())) {
        DUP_TABLE_LOG(WARN, "dup lease mgr offline failed", K(ret), KPC(this));
      } else if (OB_NOT_NULL(ts_sync_mgr_ptr_) && OB_FAIL(ts_sync_mgr_ptr_->offline())) {
        DUP_TABLE_LOG(WARN, "dup ts mgr offline failed", K(ret), KPC(this));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ls_state_helper_.state_change_succ(ObDupTableLSRoleState::LS_OFFLINE_SUCC,
                                                   restore_state_container))) {
      DUP_TABLE_LOG(ERROR, "change ls role state error", K(ret), KPC(this));
    }
    interface_stat_.reset();
  }
  return ret;
}

int ObDupTableLSHandler::online()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObDupTableLSRoleStateContainer restore_state_container;
  if (OB_FAIL(ls_state_helper_.prepare_state_change(ObDupTableLSRoleState::LS_ONLINE_SUCC,
                                                    restore_state_container))) {
    DUP_TABLE_LOG(WARN, "prepare state change failed", K(ret), KPC(this));
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(dup_ls_ckpt_.online())) {
    DUP_TABLE_LOG(WARN, "dup table checkpoint online failed", K(ret), KPC(this));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ls_state_helper_.state_change_succ(ObDupTableLSRoleState::LS_ONLINE_SUCC,
                                                   restore_state_container))) {
      DUP_TABLE_LOG(ERROR, "change ls role state error", K(ret), KPC(this));
    }
  }

  return ret;
}

int ObDupTableLSHandler::safe_to_destroy(bool &is_dup_table_handler_safe)
{
  int ret = OB_SUCCESS;
  is_dup_table_handler_safe = false;
  //can not submit log after the offline
  if (is_inited()) {
    if (OB_NOT_NULL(log_operator_) && log_operator_->is_busy()) {
      ret = OB_EAGAIN;
      DUP_TABLE_LOG(WARN, "wait log synced before destroy", K(ret), KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    is_dup_table_handler_safe = true;
  }

  return ret;
}

void ObDupTableLSHandler::destroy() { reset(); }

void ObDupTableLSHandler::reset()
{
  // ATOMIC_STORE(&is_inited_, false);
  SpinWLockGuard w_init_guard(init_rw_lock_);
  ls_state_helper_.reset();

  dup_ls_ckpt_.reset();

  if (is_inited_) {
    if (OB_NOT_NULL(lease_mgr_ptr_)) {
      lease_mgr_ptr_->~ObDupTableLSLeaseMgr();
      ob_free(lease_mgr_ptr_);
    }
    if (OB_NOT_NULL(ts_sync_mgr_ptr_)) {
      ts_sync_mgr_ptr_->~ObDupTableLSTsSyncMgr();
      ob_free(ts_sync_mgr_ptr_);
    }
    if (OB_NOT_NULL(tablets_mgr_ptr_)) {
      tablets_mgr_ptr_->~ObLSDupTabletsMgr();
      ob_free(tablets_mgr_ptr_);
    }
    if (OB_NOT_NULL(log_operator_)) {
      log_operator_->~ObDupTableLogOperator();
      share::mtl_free(log_operator_);
    }
  }

  is_inited_ = false;

  lease_mgr_ptr_ = nullptr;
  ts_sync_mgr_ptr_ = nullptr;
  tablets_mgr_ptr_ = nullptr;
  log_operator_ = nullptr;

  total_block_confirm_ref_ = 0;
  self_max_replayed_scn_.reset();

  interface_stat_.reset();
  for (int i = 0; i < DupTableDiagStd::TypeIndex::MAX_INDEX; i++) {
    last_diag_info_print_us_[i] = 0;
  }
}

bool ObDupTableLSHandler::is_inited()
{
  SpinRLockGuard r_init_guard(init_rw_lock_);
  return is_inited_;
}

bool ObDupTableLSHandler::is_master() { return ls_state_helper_.is_leader_serving(); }

int ObDupTableLSHandler::ls_loop_handle()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!is_inited() || OB_ISNULL(lease_mgr_ptr_) || OB_ISNULL(tablets_mgr_ptr_)
      || OB_ISNULL(ts_sync_mgr_ptr_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "dup table ls handle not init", K(ret));
  } else if (!ls_state_helper_.is_active_ls()) {
    ret = OB_LS_OFFLINE;
    DUP_TABLE_LOG(WARN, "the ls is not active", K(ret), KPC(this));
  } else if (!check_tablet_set_exist()) {
    // if tablet set not exist,
    // return OB_NO_TABLET and remove ls id form map
    // else do ls loop handle
    ret = OB_NO_TABLET;
    DUP_TABLE_LOG(INFO, "no dup tablet, no need to do loop worker", K(ret),
                  KPC(tablets_mgr_ptr_));
  } else {
    if (ls_state_helper_.is_leader()) {
      if (OB_ISNULL(log_operator_) || !log_operator_->is_busy()) {
        // handle lease request and collect follower info
        DupTableTsInfo min_lease_ts_info;
        if (OB_TMP_FAIL(get_min_lease_ts_info_(min_lease_ts_info))) {
          DUP_TABLE_LOG(WARN, "get min lease ts info failed", K(tmp_ret), K(min_lease_ts_info));
          // try confirm tablets and check tablet need log
        } else if (OB_TMP_FAIL(try_to_confirm_tablets_(min_lease_ts_info.max_replayed_scn_))) {
          DUP_TABLE_LOG(WARN, "try confirm tablets failed", K(tmp_ret), K(min_lease_ts_info));
        }

        if (OB_TMP_FAIL(tablets_mgr_ptr_->scan_readable_set_for_gc(
                ATOMIC_LOAD(&interface_stat_.dup_table_ls_leader_takeover_ts_)))) {
          DUP_TABLE_LOG(WARN, "scan readable set failed", K(tmp_ret));
        }

        // submit lease log
        if(OB_FAIL(ret))
        {
        } else
          if (OB_ISNULL(log_operator_)) {
            ret = OB_INVALID_ARGUMENT;
            DUP_TABLE_LOG(WARN, "invalid log operator ptr", K(ret), KP(log_operator_));
        } else if (OB_FAIL(log_operator_->submit_log_entry())) {
          DUP_TABLE_LOG(WARN, "submit dup table log entry failed", K(ret));
        }
      }

      // update ts info cache
      if (OB_TMP_FAIL(ts_sync_mgr_ptr_->update_all_ts_info_cache())) {
        DUP_TABLE_LOG(WARN, "update all ts info cache failed", K(tmp_ret));
      }

    } else if (ls_state_helper_.is_follower()) {
      if (OB_FAIL(lease_mgr_ptr_->follower_handle())) {
        DUP_TABLE_LOG(WARN, "follower lease handle failed", K(ret));
      }
    } else {
      DUP_TABLE_LOG(INFO, "undefined role state for dup table ls, skip this loop", K(ret),
                    K(ls_id_), K(ls_state_helper_));
    }
    DUP_TABLE_LOG(DEBUG, "loop running : dup table ls handler", K(ret), K(ls_id_),
                  K(ls_state_helper_), KP(lease_mgr_ptr_), KP(tablets_mgr_ptr_),
                  KP(log_operator_));

    const int64_t fast_cur_time = ObTimeUtility::fast_current_time();
    const bool is_leader = ls_state_helper_.is_leader();

    if (fast_cur_time - last_diag_info_print_us_[DupTableDiagStd::TypeIndex::LEASE_INDEX]
        >= DupTableDiagStd::DUP_DIAG_PRINT_INTERVAL[DupTableDiagStd::TypeIndex::LEASE_INDEX]) {
      _DUP_TABLE_LOG(INFO, "[%sDup Interface Stat] tenant: %lu, ls: %lu, is_master: %s, %s",
                     DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, MTL_ID(), ls_id_.id(),
                     to_cstring(is_leader), to_cstring(interface_stat_));
    }

    if (fast_cur_time - last_diag_info_print_us_[DupTableDiagStd::TypeIndex::LEASE_INDEX]
        >= DupTableDiagStd::DUP_DIAG_PRINT_INTERVAL[DupTableDiagStd::TypeIndex::LEASE_INDEX]) {
      lease_mgr_ptr_->print_lease_diag_info_log(is_leader);
      last_diag_info_print_us_[DupTableDiagStd::TypeIndex::LEASE_INDEX] = fast_cur_time;
    }

    if (fast_cur_time - last_diag_info_print_us_[DupTableDiagStd::TypeIndex::TABLET_SET_INDEX]
        >= DupTableDiagStd::DUP_DIAG_PRINT_INTERVAL[DupTableDiagStd::TypeIndex::TABLET_SET_INDEX]) {
      tablets_mgr_ptr_->print_tablet_diag_info_log(is_leader);
      last_diag_info_print_us_[DupTableDiagStd::TypeIndex::TABLET_SET_INDEX] = fast_cur_time;
    }

    if (fast_cur_time - last_diag_info_print_us_[DupTableDiagStd::TypeIndex::TS_SYNC_INDEX]
        >= DupTableDiagStd::DUP_DIAG_PRINT_INTERVAL[DupTableDiagStd::TypeIndex::TS_SYNC_INDEX]) {
      ts_sync_mgr_ptr_->print_ts_sync_diag_info_log(is_leader);
      last_diag_info_print_us_[DupTableDiagStd::TypeIndex::TS_SYNC_INDEX] = fast_cur_time;
    }
  }

  return ret;
}

int ObDupTableLSHandler::refresh_dup_table_tablet(common::ObTabletID tablet_id,
                                                  bool is_dup_table,
                                                  int64_t refresh_time)
{
  int ret = OB_SUCCESS;

  if (!is_inited() || OB_ISNULL(tablets_mgr_ptr_)) {
    ret = OB_NOT_INIT;
    if (is_dup_table) {
      DUP_TABLE_LOG(WARN, "ObDupTableLSHandler not init", K(ret), K(is_inited_), K(is_dup_table),
                    KPC(this));
    }
  } else if (!ls_state_helper_.is_leader()) {
    ret = OB_NOT_MASTER;
    DUP_TABLE_LOG(WARN, "No need to refresh dup table schema", K(ret), K(tablet_id),
                  K(is_dup_table), KPC(this));
  } else if (OB_FAIL(tablets_mgr_ptr_->refresh_dup_tablet(tablet_id, is_dup_table, refresh_time))) {
    if (ret != OB_NOT_MASTER) {
      DUP_TABLE_LOG(WARN, "refresh dup table tablet failed", K(ret), K(tablet_id), K(is_dup_table),
                    KPC(this));
    } else {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObDupTableLSHandler::receive_lease_request(const ObDupTableLeaseRequest &lease_req)
{
  int ret = OB_SUCCESS;
  if (!is_inited() || OB_ISNULL(lease_mgr_ptr_) || OB_ISNULL(ts_sync_mgr_ptr_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "DupTableLSHandler not init", K(ret), K(is_inited_), KP(lease_mgr_ptr_));
  } else if (!ls_state_helper_.is_leader_serving()) {
    ret = OB_NOT_MASTER;
    DUP_TABLE_LOG(WARN, "No need to receive lease request", K(ret), K(lease_req), KPC(this));
  } else if (OB_FAIL(ts_sync_mgr_ptr_->handle_ts_sync_response(lease_req))) {
    DUP_TABLE_LOG(WARN, "handle ts sync response failed", K(ret));
  } else if (OB_FAIL(lease_mgr_ptr_->receive_lease_request(lease_req))) {
    DUP_TABLE_LOG(WARN, "receive lease request failed", K(ret), K(lease_req));
  }
  return ret;
}

int ObDupTableLSHandler::handle_ts_sync_response(const ObDupTableTsSyncResponse &ts_sync_resp)
{
  int ret = OB_SUCCESS;

  if (!is_inited() || OB_ISNULL(ts_sync_mgr_ptr_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "DupTableLSHandler not init", K(ret), K(is_inited_), KP(ts_sync_mgr_ptr_));
  } else if (!ls_state_helper_.is_leader_serving()) {
    ret = OB_NOT_MASTER;
    DUP_TABLE_LOG(WARN, "No need to handle ts sync response", K(ret), K(ts_sync_resp), KPC(this));
  } else if (OB_FAIL(ts_sync_mgr_ptr_->handle_ts_sync_response(ts_sync_resp))) {
    DUP_TABLE_LOG(WARN, "handle ts sync response failed", K(ret));
  }

  return ret;
}

int ObDupTableLSHandler::handle_ts_sync_request(const ObDupTableTsSyncRequest &ts_sync_req)
{
  int ret = OB_SUCCESS;

  if (!is_inited() || OB_ISNULL(ts_sync_mgr_ptr_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "DupTableLSHandler not init", K(ret), K(is_inited_), KP(ts_sync_mgr_ptr_));
  } else if (!ls_state_helper_.is_follower_serving()) {
    ret = OB_NOT_FOLLOWER;
    DUP_TABLE_LOG(WARN, "No need to handle ts sync request", K(ret), K(ts_sync_req), KPC(this));
  } else if (OB_FAIL(ts_sync_mgr_ptr_->handle_ts_sync_request(ts_sync_req))) {
    DUP_TABLE_LOG(WARN, "handle ts sync request failed", K(ret));
  }

  return ret;
}

int ObDupTableLSHandler::check_redo_sync_completed(const ObTransID &tx_id,
                                                   const share::SCN &redo_completed_scn,
                                                   bool &redo_sync_finish,
                                                   share::SCN &total_max_read_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LeaseAddrArray lease_addrs;
  redo_sync_finish = false;
  int64_t redo_sync_succ_cnt = 0;
  total_max_read_version.set_invalid();
  share::SCN tmp_max_read_version;
  tmp_max_read_version.set_invalid();

  const int64_t GET_GTS_TIMEOUT = 100 * 1000; // 100ms
  share::SCN before_prepare_gts;
  before_prepare_gts.set_invalid();
  ObDupTableBeforePrepareRequest::BeforePrepareScnSrc before_prepare_src =
      ObDupTableBeforePrepareRequest::BeforePrepareScnSrc::UNKNOWN;
  int64_t start_us = OB_INVALID_TIMESTAMP;

  if (!is_inited() || OB_ISNULL(lease_mgr_ptr_) || OB_ISNULL(ts_sync_mgr_ptr_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "ObDupTableLSHandler not init", K(ret), K(is_inited_), KP(lease_mgr_ptr_),
                  KP(ts_sync_mgr_ptr_));
  } else if (OB_FAIL(lease_mgr_ptr_->get_lease_valid_array(lease_addrs))) {
    DUP_TABLE_LOG(WARN, "get lease valid array failed", K(ret));
  } else if (lease_addrs.count() == 0) {
    redo_sync_finish = true;
    total_max_read_version.set_min(); // min scn
    DUP_TABLE_LOG(INFO, "no follower with valid lease, redo sync finish", K(ret), K(tx_id),
                  K(ls_id_), K(redo_completed_scn), K(redo_sync_finish), K(total_max_read_version));
  } else {
    tmp_max_read_version.set_min();
    for (int i = 0; OB_SUCC(ret) && i < lease_addrs.count(); i++) {
      bool replay_all_redo = false;
      share::SCN max_read_version;
      max_read_version.set_invalid();
      if (OB_FAIL(ts_sync_mgr_ptr_->validate_replay_ts(lease_addrs[i], redo_completed_scn, tx_id,
                                                       replay_all_redo, max_read_version))) {
        DUP_TABLE_LOG(WARN, "validate replay ts failed", K(ret), K(lease_addrs[i]),
                      K(redo_completed_scn));
      } else if (replay_all_redo) {
        if (!max_read_version.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          DUP_TABLE_LOG(WARN, "unexpected max read version", K(ret), K(replay_all_redo),
                        K(max_read_version));
        } else if (tmp_max_read_version.is_valid()) {
          tmp_max_read_version = share::SCN::max(tmp_max_read_version, max_read_version);
        } else {
          tmp_max_read_version = max_read_version;
        }
        if (OB_SUCC(ret)) {
          redo_sync_succ_cnt++;
        }
      }

      // get_gts && retry to post before_prepare request
      if ((OB_SUCC(ret) && !replay_all_redo) || OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (!before_prepare_gts.is_valid()) {
          share::SCN tmp_gts;
          tmp_gts.set_invalid();
          start_us = ObTimeUtility::fast_current_time();
          const MonotonicTs stc = MonotonicTs(start_us);
          MonotonicTs rts(0);
          if (OB_TMP_FAIL(MTL(ObTransService *)
                              ->get_ts_mgr()
                              ->get_gts(MTL_ID(), stc, NULL, tmp_gts, rts))) {
            if (OB_EAGAIN != tmp_ret) {
              DUP_TABLE_LOG(WARN, "get gts failed", K(tmp_ret), K(start_us));
            } else {
              if (OB_TMP_FAIL(
                      MTL(ObTransService *)->get_ts_mgr()->get_gts(MTL_ID(), NULL, tmp_gts))) {

                DUP_TABLE_LOG(WARN, "get gts from the cache failed", K(tmp_ret), K(start_us));
              }
            }
          }

          if (OB_TMP_FAIL(tmp_ret) || !tmp_gts.is_valid_and_not_min()) {
            if (OB_ISNULL(log_handler_)) {
              tmp_ret = OB_INVALID_ARGUMENT;
              DUP_TABLE_LOG(WARN, "invalid log handler ptr", K(tmp_ret), K(ls_id_),
                            KP(log_handler_));
            } else if (OB_TMP_FAIL(log_handler_->get_max_decided_scn(before_prepare_gts))) {
              DUP_TABLE_LOG(WARN, "get max decided scn failed", K(tmp_ret), K(ls_id_),
                            K(before_prepare_gts));
              before_prepare_gts.set_invalid();
            } else {
              before_prepare_src =
                  ObDupTableBeforePrepareRequest::BeforePrepareScnSrc::MAX_DECIDED_SCN;
            }
          } else {
            before_prepare_gts = tmp_gts;
            before_prepare_src = ObDupTableBeforePrepareRequest::BeforePrepareScnSrc::GTS;
          }

          if (OB_SUCCESS == tmp_ret && before_prepare_gts > redo_completed_scn) {
            const common::ObAddr self_addr = MTL(ObTransService *)->get_server();
            ObDupTableBeforePrepareRequest before_prepare_req(tx_id, before_prepare_gts,
                                                              before_prepare_src);
            before_prepare_req.set_header(self_addr, lease_addrs[i], self_addr, ls_id_);
            if (before_prepare_req.get_before_prepare_scn_src()
                <= ObDupTableBeforePrepareRequest::BeforePrepareScnSrc::UNKNOWN) {
              // ignore ret
              DUP_TABLE_LOG(ERROR, "UNKOWN before prepare src", K(ret), K(tmp_ret),
                            K(before_prepare_req), K(redo_completed_scn), K(tx_id), K(ls_id_));
            }

            if (OB_TMP_FAIL(MTL(ObTransService *)
                                ->get_dup_table_rpc_impl()
                                .post_msg(lease_addrs[i], before_prepare_req))) {
              DUP_TABLE_LOG(WARN, "post ts sync request failed", K(tmp_ret));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret) && redo_sync_succ_cnt == lease_addrs.count()) {
      redo_sync_finish = true;
      total_max_read_version = tmp_max_read_version;

      DUP_TABLE_LOG(INFO, "redo sync finish with lease valid follower", K(ret), K(ls_id_), K(tx_id),
                    K(redo_completed_scn), K(redo_sync_finish), K(total_max_read_version),
                    K(lease_addrs.count()), K(lease_addrs));
    }
  }

  if (redo_sync_finish) {
    interface_stat_.dup_table_redo_sync_succ_cnt_++;
  } else {
    interface_stat_.dup_table_redo_sync_fail_cnt_++;
  }

  return ret;
}

int ObDupTableLSHandler::block_confirm_with_dup_tablet_change_snapshot(
    share::SCN &dup_tablet_change_snapshot)
{
  int ret = OB_SUCCESS;

  ATOMIC_INC(&total_block_confirm_ref_);

  if (!is_inited()) {
    // do nothing
    ret = OB_SUCCESS;
  } else {
  }

  return ret;
}

int ObDupTableLSHandler::gc_temporary_dup_tablets(const int64_t gc_ts, const int64_t max_task_interval)
{
  int ret = OB_SUCCESS;

  if (!is_inited() || !ls_state_helper_.is_leader_serving()) {
    // do nothing
  } else if (OB_ISNULL(tablets_mgr_ptr_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "ObDupTableLSHandler not init", K(ret), K(is_inited_),
                  KP(tablets_mgr_ptr_));
  } else if (0 > gc_ts || 0 > max_task_interval) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid gc_time", K(ret), K(gc_ts), K(max_task_interval));
  } else if (OB_FAIL(tablets_mgr_ptr_->gc_tmporary_dup_tablets(gc_ts, max_task_interval))) {
    DUP_TABLE_LOG(WARN, "lose dup tablet failed", KR(ret), K(gc_ts));
  }

  return ret;
}

int ObDupTableLSHandler::try_to_confirm_tablets_(const share::SCN &confirm_scn)
{
  int ret = OB_SUCCESS;

  if (!is_inited() || OB_ISNULL(tablets_mgr_ptr_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "ObDupTableLSHandler not init", K(ret), K(is_inited_),
                  KP(tablets_mgr_ptr_));
  } else if (!confirm_scn.is_valid() || share::SCN::max_scn() == confirm_scn) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid confrim_time", K(ret), K(confirm_scn));
  } else if (OB_FAIL(tablets_mgr_ptr_->try_to_confirm_tablets(confirm_scn))) {
    DUP_TABLE_LOG(WARN, "confirm tablets failed", K(ret), K(confirm_scn));
  }
  // for debug
  DUP_TABLE_LOG(DEBUG, "ls finish confirm tablets", K(ret), K(confirm_scn));
  return ret;
}

int ObDupTableLSHandler::recover_ckpt_into_memory_()
{
  int ret = OB_SUCCESS;

  if (dup_ls_ckpt_.is_useful_meta()) {
    const bool is_dup_table = true;
    if (OB_SUCC(ret) && OB_FAIL(init(is_dup_table))) {
      if (OB_INIT_TWICE == ret) {
        ret = OB_SUCCESS;
      } else {
        DUP_TABLE_LOG(WARN, "init dup ls handler failed", K(ret), KPC(this));
      }
    }

    if (OB_SUCC(ret)) {
      ObDupTableLSCheckpoint::ObLSDupTableMeta dup_ls_meta;
      if (OB_ISNULL(lease_mgr_ptr_)) {
        ret = OB_ERR_UNEXPECTED;
        DUP_TABLE_LOG(WARN, "unexpected lease mgr ptr after inited", K(ret), KPC(lease_mgr_ptr_),
                      KPC(this));
      } else if (OB_FAIL(dup_ls_ckpt_.get_dup_ls_meta(dup_ls_meta))) {
        DUP_TABLE_LOG(WARN, "copy a dup table ls meta failed", K(ret), K(dup_ls_meta),
                      K(dup_ls_ckpt_));
      } else if (OB_FAIL(lease_mgr_ptr_->recover_lease_from_ckpt(dup_ls_meta))) {
        DUP_TABLE_LOG(WARN, "recover lease array failed", K(ret), KPC(this), KPC(lease_mgr_ptr_));
      }
    }
  } else {
    DUP_TABLE_LOG(DEBUG, "unuseful dup table checkpoint, no need to recover", K(ret), K(ls_id_),
                  K(dup_ls_ckpt_));
  }

  return ret;
}

int ObDupTableLSHandler::unblock_confirm_with_prepare_scn(
    const share::SCN &dup_tablet_change_snapshot,
    const share::SCN &redo_scn)
{
  int ret = OB_SUCCESS;

  return ret;
}

int ObDupTableLSHandler::check_dup_tablet_in_redo(const ObTabletID &tablet_id,
                                                  bool &is_dup_tablet,
                                                  const share::SCN &base_snapshot,
                                                  const share::SCN &redo_scn)
{
  int ret = OB_SUCCESS;
  is_dup_tablet = false;

  if (!tablet_id.is_valid() || !base_snapshot.is_valid() || !redo_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid argument", K(ret), K(tablet_id), K(base_snapshot), K(redo_scn));
  } else if (!is_inited() || OB_ISNULL(lease_mgr_ptr_) || OB_ISNULL(tablets_mgr_ptr_)) {
    is_dup_tablet = false;
  } else if (!has_dup_tablet()) {
    is_dup_tablet = false;
  } else if (OB_FAIL(tablets_mgr_ptr_->search_dup_tablet_in_redo_log(tablet_id, is_dup_tablet,
                                                                     base_snapshot, redo_scn))) {
    DUP_TABLE_LOG(WARN, "check dup tablet failed", K(ret), K(tablet_id), K(base_snapshot),
                  K(redo_scn));
  }
  return ret;
}

int ObDupTableLSHandler::check_dup_tablet_readable(const ObTabletID &tablet_id,
                                                   const share::SCN &read_snapshot,
                                                   const bool read_from_leader,
                                                   const share::SCN &max_replayed_scn,
                                                   bool &readable)
{
  int ret = OB_SUCCESS;

  share::SCN tmp_max_replayed_scn = max_replayed_scn;
  readable = false;
  if (!tablet_id.is_valid() || !read_snapshot.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid argument", K(ret), K(tablet_id), K(read_snapshot), K(readable));
  } else if (!is_inited() || OB_ISNULL(lease_mgr_ptr_) || OB_ISNULL(tablets_mgr_ptr_)) {
    // no dup tablet in ls
    readable = false;
  } else if (!ls_state_helper_.is_active_ls()) {
    readable = false;
    DUP_TABLE_LOG(INFO, "the ls is not active", K(ret), KPC(this));
  } else if (!has_dup_tablet()) {
    readable = false;
    interface_stat_.dup_table_follower_read_tablet_not_exist_cnt_++;
    // use read_from_leader to validate lease;
    DUP_TABLE_LOG(INFO, "no dup tablet can be read", K(ret), KPC(tablets_mgr_ptr_),
                  K(read_from_leader), K(tmp_max_replayed_scn));
  } else if (!tmp_max_replayed_scn.is_valid()
             && (OB_ISNULL(log_handler_)
                 || OB_FAIL(log_handler_->get_max_decided_scn(tmp_max_replayed_scn)))) {
    DUP_TABLE_LOG(WARN, "get max replayed scn for dup table read failed", K(ret), K(ls_id_),
                  K(tablet_id), K(read_snapshot), KP(log_handler_), K(tmp_max_replayed_scn));
    // rewrite ret code when get max replayed scn failed to drive retry
    ret = OB_NOT_MASTER;
  } else if (OB_FAIL(check_and_update_max_replayed_scn(max_replayed_scn))) {
    DUP_TABLE_LOG(WARN, "invalid max_replayed_scn", K(ret), K(tablet_id), K(read_snapshot),
                  K(read_from_leader));
  } else if (false
             == lease_mgr_ptr_->check_follower_lease_serving(read_from_leader,
                                                             tmp_max_replayed_scn)) {
    readable = false;
    interface_stat_.dup_table_follower_read_lease_expired_cnt_++;
    DUP_TABLE_LOG(INFO, "lease is expired for read", K(ret), K(tablet_id), K(read_snapshot),
                  K(read_from_leader), K(tmp_max_replayed_scn));
  } else if (OB_FAIL(tablets_mgr_ptr_->check_readable(tablet_id, readable, read_snapshot,
                                                      interface_stat_))) {
    DUP_TABLE_LOG(WARN, "check dup tablet failed", K(ret), K(tablet_id), K(read_snapshot));
  }

  if (readable) {
    interface_stat_.dup_table_follower_read_succ_cnt_++;
  }

  return ret;
}

bool ObDupTableLSHandler::is_dup_table_lease_valid()
{
  bool is_dup_lease_ls = false;
  const bool is_election_leader = false;

  if (has_dup_tablet()) {
    if (!is_inited() || OB_ISNULL(lease_mgr_ptr_)) {
      is_dup_lease_ls = false;
    } else if (ls_state_helper_.is_leader()) {
      is_dup_lease_ls = false;
      DUP_TABLE_LOG(INFO, "None valid lease on dup ls leader", K(is_dup_lease_ls),
                    KPC(this));
    } else {
      is_dup_lease_ls = lease_mgr_ptr_->is_follower_lease_valid();
    }
  } else {
    is_dup_lease_ls = false;
  }

  DUP_TABLE_LOG(DEBUG,
                "is dup table lease valid",
                KP(lease_mgr_ptr_),
                K(ls_state_helper_.is_leader()),
                K(has_dup_tablet()),
                K(is_dup_lease_ls));

  return is_dup_lease_ls;
}

int64_t ObDupTableLSHandler::get_dup_tablet_count()
{
  int64_t dup_tablet_cnt = 0;

  if (!is_inited() || OB_ISNULL(tablets_mgr_ptr_)) {
    dup_tablet_cnt = 0;
  } else {
    dup_tablet_cnt = tablets_mgr_ptr_->get_dup_tablet_count();
  }

  return dup_tablet_cnt;
}

bool ObDupTableLSHandler::has_dup_tablet()
{
  bool has_dup = false;
  if (!is_inited() || OB_ISNULL(tablets_mgr_ptr_)) {
    has_dup = false;
  } else {
    has_dup = tablets_mgr_ptr_->has_dup_tablet();
  }
  return has_dup;
}

bool ObDupTableLSHandler::is_dup_tablet(const common::ObTabletID &tablet_id)
{
  bool is_dup_tablet = false;
  int ret = OB_SUCCESS;

  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid argument", K(ret), K(tablet_id));
  } else if (!is_inited() || OB_ISNULL(tablets_mgr_ptr_)) {
    is_dup_tablet = false;
  } else if (OB_FAIL(tablets_mgr_ptr_->search_dup_tablet_for_read(tablet_id, is_dup_tablet))) {
    DUP_TABLE_LOG(WARN, "check dup tablet failed", K(ret), K(tablet_id), K(is_dup_tablet));
    is_dup_tablet = false;
  }

  return is_dup_tablet;
}

// if return false, there are no tablets and tablet set need log
bool ObDupTableLSHandler::check_tablet_set_exist()
{
  bool bool_ret = false;

  if (!is_inited() || OB_ISNULL(tablets_mgr_ptr_)) {
    bool_ret = false;
  } else {
    int64_t readable_and_need_confirm_set_count =
              tablets_mgr_ptr_->get_readable_tablet_set_count()
              + tablets_mgr_ptr_->get_need_confirm_tablet_set_count();

    // if readable and need confirm set count > 0, return true
    if (readable_and_need_confirm_set_count > 0 ) {
      bool_ret = true;
    } else {
      // if changing new and removing set exist return true
      bool chaning_and_removing_tablet_exist =
          tablets_mgr_ptr_->check_changing_new_tablet_exist()
          || tablets_mgr_ptr_->check_removing_tablet_exist();
      if (chaning_and_removing_tablet_exist) {
        bool_ret = true;
      } else {
        bool_ret = false;
      }
    }
  }

  return bool_ret;
}

int ObDupTableLSHandler::get_local_ts_info(DupTableTsInfo &ts_info)
{
  int ret = OB_SUCCESS;

  if (!is_inited() || OB_ISNULL(ts_sync_mgr_ptr_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "DupTableLSHandler not init", K(ret), K(is_inited_), KP(ts_sync_mgr_ptr_));
  } else if (!ls_state_helper_.is_active_ls()) {
    ret = OB_LS_OFFLINE;
    DUP_TABLE_LOG(WARN, "the ls is not active", K(ret), KPC(this));
  } else if (OB_FAIL(ts_sync_mgr_ptr_->get_local_ts_info(ts_info))) {
    DUP_TABLE_LOG(WARN, "get local ts sync info failed", K(ret));
  }

  return ret;
}

int ObDupTableLSHandler::get_cache_ts_info(const common::ObAddr &addr, DupTableTsInfo &ts_info)
{
  int ret = OB_SUCCESS;

  if (!is_inited() || OB_ISNULL(ts_sync_mgr_ptr_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "DupTableLSHandler not init", K(ret), K(is_inited_), KP(ts_sync_mgr_ptr_));
  } else if (!ls_state_helper_.is_leader_serving()) {
    ret = OB_NOT_MASTER;
    DUP_TABLE_LOG(WARN, "we can not get cached ts info from a follower", K(ret), K(ts_info),
                  KPC(this));
  } else if (OB_FAIL(ts_sync_mgr_ptr_->get_cache_ts_info(addr, ts_info))) {
    DUP_TABLE_LOG(WARN, "get cache ts info failed", K(ret), K(addr), K(ts_info));
  }
  return ret;
}

int ObDupTableLSHandler::replay(const void *buffer,
                                const int64_t nbytes,
                                const palf::LSN &lsn,
                                const share::SCN &ts_ns)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const bool no_dup_tablet_before_replay = !has_dup_tablet();

  // cover lease list and tablets list
  if (!is_inited() && OB_FAIL(init(true))) {
    DUP_TABLE_LOG(WARN, "init dup_ls_handle in replay failed", K(ret));
  } else if (OB_FALSE_IT(log_operator_->set_logging_scn(ts_ns))) {

  } else if (OB_FAIL(
                 log_operator_->merge_replay_block(static_cast<const char *>(buffer), nbytes))) {
    if (OB_SUCCESS == ret) {
      DUP_TABLE_LOG(INFO, "merge replay buf success, may be completed", K(ret));
    } else if (OB_START_LOG_CURSOR_INVALID == ret) {
      DUP_TABLE_LOG(WARN, "start replay from the middle of log entry, skip this dup_table log",
                    K(ts_ns), K(lsn));
      // ret = OB_SUCCESS;
    } else {
      DUP_TABLE_LOG(WARN, "merge replay buf failed", K(ret));
    }
  } else if (OB_FAIL(log_operator_->deserialize_log_entry())) {
    DUP_TABLE_LOG(WARN, "deserialize log block failed", K(ret), K(ts_ns));
  } else if (OB_FAIL(lease_mgr_ptr_->follower_try_acquire_lease(ts_ns))) {
    DUP_TABLE_LOG(WARN, "acquire lease from lease log error", K(ret), K(ts_ns));
  } else {
    ret = log_operator_->replay_succ();
    DUP_TABLE_LOG(DEBUG, "replay dup_table log success", K(ret), K(nbytes), K(lsn), K(ts_ns),
                  KPC(tablets_mgr_ptr_), KPC(lease_mgr_ptr_));
    // log_operator_->reuse();V
    interface_stat_.dup_table_log_entry_cnt_ += 1;
    interface_stat_.dup_table_log_entry_total_size_ += nbytes;
  }

  // start require lease instantly
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(log_operator_)) {
      log_operator_->reuse();
    }
  } else if (no_dup_tablet_before_replay && check_tablet_set_exist()
             && OB_TMP_FAIL(
                 MTL(ObTransService *)->get_dup_table_loop_worker().append_dup_table_ls(ls_id_))) {
    DUP_TABLE_LOG(WARN, "refresh dup table ls failed", K(tmp_ret), K(ls_id_), K(lsn), K(ts_ns));
  }

  return ret;
}
void ObDupTableLSHandler::switch_to_follower_forcedly()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  SpinRLockGuard r_init_guard(init_rw_lock_);

  ObDupTableLSRoleStateContainer restore_state_container;
  if (OB_FAIL(ls_state_helper_.prepare_state_change(ObDupTableLSRoleState::LS_REVOKE_SUCC,
                                                    restore_state_container))) {
    DUP_TABLE_LOG(WARN, "prepare state change failed", K(ret), KPC(this));
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(leader_revoke_(true /*forcedly*/))) {
    DUP_TABLE_LOG(ERROR, "switch to follower forcedly failed for dup table", K(ret), KPC(this));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ls_state_helper_.state_change_succ(ObDupTableLSRoleState::LS_REVOKE_SUCC,
                                                   restore_state_container))) {
      DUP_TABLE_LOG(ERROR, "change ls role state error", K(ret), KPC(this));
    }
  }
}

int ObDupTableLSHandler::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  SpinRLockGuard r_init_guard(init_rw_lock_);

  ObDupTableLSRoleStateContainer restore_state_container;
  if (OB_FAIL(ls_state_helper_.prepare_state_change(ObDupTableLSRoleState::LS_REVOKE_SUCC,
                                                    restore_state_container))) {
    DUP_TABLE_LOG(WARN, "prepare state change failed", K(ret), KPC(this));
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(leader_revoke_(false /*forcedly*/))) {
    DUP_TABLE_LOG(WARN, "switch to follower gracefully failed for dup table", K(ret), KPC(this));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ls_state_helper_.state_change_succ(ObDupTableLSRoleState::LS_REVOKE_SUCC,
                                                   restore_state_container))) {
      DUP_TABLE_LOG(ERROR, "change ls role state error", K(ret), KPC(this));
    }
  } else {
    tmp_ret = OB_SUCCESS;

    if (OB_TMP_FAIL(leader_takeover_(true /*is_resume*/))) {
      DUP_TABLE_LOG(WARN, "resume leader failed", K(ret), K(tmp_ret), KPC(this));
    }
    if (OB_SUCCESS != tmp_ret) {
      ret = OB_LS_NEED_REVOKE;
      DUP_TABLE_LOG(WARN, "resume leader failed, need revoke", K(ret), K(tmp_ret), KPC(this));
    } else if (OB_TMP_FAIL(ls_state_helper_.restore_state(ObDupTableLSRoleState::LS_REVOKE_SUCC,
                                                          restore_state_container))) {
      DUP_TABLE_LOG(ERROR, "restore ls role state error", K(ret), KPC(this));
    }
  }

  return ret;
}

int ObDupTableLSHandler::resume_leader()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const bool is_resume = true;

  SpinRLockGuard r_init_guard(init_rw_lock_);

  ObDupTableLSRoleStateContainer restore_state_container;
  if (OB_FAIL(ls_state_helper_.prepare_state_change(ObDupTableLSRoleState::LS_TAKEOVER_SUCC,
                                                    restore_state_container))) {
    DUP_TABLE_LOG(WARN, "prepare state change failed", K(ret), KPC(this));
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(leader_takeover_(is_resume))) {
    DUP_TABLE_LOG(WARN, "resume leader failed for dup table", K(ret), KPC(this));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ls_state_helper_.state_change_succ(ObDupTableLSRoleState::LS_TAKEOVER_SUCC,
                                                   restore_state_container))) {
      DUP_TABLE_LOG(ERROR, "change ls role state error", K(ret), KPC(this));
    }
  } else {
    tmp_ret = OB_SUCCESS;

    if (OB_TMP_FAIL(leader_revoke_(true /*forcedly*/))) {
      DUP_TABLE_LOG(WARN, "leader revoke failed", K(ret), K(tmp_ret), KPC(this));
    } else if (OB_TMP_FAIL(ls_state_helper_.restore_state(ObDupTableLSRoleState::LS_TAKEOVER_SUCC,
                                                          restore_state_container))) {
      DUP_TABLE_LOG(ERROR, "restore ls role state error", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObDupTableLSHandler::switch_to_leader()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const bool is_resume = false;

  SpinRLockGuard r_init_guard(init_rw_lock_);

  ObDupTableLSRoleStateContainer restore_state_container;
  if (OB_FAIL(ls_state_helper_.prepare_state_change(ObDupTableLSRoleState::LS_TAKEOVER_SUCC,
                                                    restore_state_container))) {
    DUP_TABLE_LOG(WARN, "prepare state change failed", K(ret), KPC(this));
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    }
  } else if (dup_ls_ckpt_.is_useful_meta() && !is_inited_) {
    ret = OB_LS_NEED_REVOKE;
    DUP_TABLE_LOG(WARN, "switch to leader failed  without ckpt recovery", K(ret), KPC(this));
  } else if (OB_FAIL(leader_takeover_(is_resume))) {
    DUP_TABLE_LOG(WARN, "leader takeover failed for dup table", K(ret), KPC(this));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ls_state_helper_.state_change_succ(ObDupTableLSRoleState::LS_TAKEOVER_SUCC,
                                                   restore_state_container))) {
      DUP_TABLE_LOG(ERROR, "change ls role state error", K(ret), KPC(this));
    } else {
      ATOMIC_STORE(&interface_stat_.dup_table_ls_leader_takeover_ts_,
                   ObTimeUtility::fast_current_time());
    }
  } else {
    tmp_ret = OB_SUCCESS;

    if (OB_TMP_FAIL(leader_revoke_(true /*forcedly*/))) {
      DUP_TABLE_LOG(WARN, "leader revoke failed", K(ret), K(tmp_ret), KPC(this));
    } else if (OB_TMP_FAIL(ls_state_helper_.restore_state(ObDupTableLSRoleState::LS_TAKEOVER_SUCC,
                                                          restore_state_container))) {
      DUP_TABLE_LOG(ERROR, "restore ls role state error", K(ret), KPC(this));
    }
  }
  return ret;
}

OB_NOINLINE int ObDupTableLSHandler::errsim_leader_revoke_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ret)) {
    DUP_TABLE_LOG(WARN, "errsim leader revoke", K(ret), K(ls_id_));
  }
  return ret;
}

int ObDupTableLSHandler::leader_revoke_(const bool is_forcedly)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  bool is_logging = false;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(errsim_leader_revoke_())) {
      DUP_TABLE_LOG(WARN, "errsim for dup table leader revoke", K(ret), K(ls_id_), K(is_forcedly));
    }
  }

  if (is_inited_) {
    if (OB_NOT_NULL(log_operator_)) {
      log_operator_->rlock_for_log();
      is_logging = log_operator_->check_is_busy_without_lock();
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(tablets_mgr_ptr_))
      if(OB_TMP_FAIL(tablets_mgr_ptr_->leader_revoke(is_logging))) {
      DUP_TABLE_LOG(WARN, "tablets_mgr switch to follower failed", K(ret), K(tmp_ret), KPC(this));
      if (!is_forcedly) {
        ret = tmp_ret;
      }
    }
    if (OB_NOT_NULL(log_operator_)) {
      log_operator_->unlock_for_log();
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(ts_sync_mgr_ptr_)) {
      if (OB_TMP_FAIL(ts_sync_mgr_ptr_->leader_revoke())) {
        DUP_TABLE_LOG(WARN, "ts_sync_mgr switch to follower failed", K(ret), K(tmp_ret), KPC(this));
        if (!is_forcedly) {
          ret = tmp_ret;
        }
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(lease_mgr_ptr_)) {
      if (OB_TMP_FAIL(lease_mgr_ptr_->leader_revoke())) {
        DUP_TABLE_LOG(WARN, "lease_mgr switch to follower failed", K(ret), K(tmp_ret), KPC(this));
        if (!is_forcedly) {
          ret = tmp_ret;
        }
      }
    }
  }

  DUP_TABLE_LOG(INFO, "Leader Revoke", K(ret), K(is_forcedly), KPC(this));
  return ret;
}

int ObDupTableLSHandler::leader_takeover_(const bool is_resume, const bool is_initing)
{
  int ret = OB_SUCCESS;

  if (is_inited_ || is_initing) {
    // clean ts info cache
    if (OB_NOT_NULL(ts_sync_mgr_ptr_)) {
      ts_sync_mgr_ptr_->leader_takeover();
    }
    // extend lease_expired_time
    if (OB_NOT_NULL(lease_mgr_ptr_)) {
      lease_mgr_ptr_->leader_takeover(is_resume);
    }

    if (OB_NOT_NULL(tablets_mgr_ptr_)) {
      if (OB_FAIL(tablets_mgr_ptr_->leader_takeover(
              is_resume, dup_ls_ckpt_.contain_all_readable_on_replica()))) {
        DUP_TABLE_LOG(WARN, "clean unreadable tablet set failed", K(ret));
      }
    }
  }

  DUP_TABLE_LOG(INFO, "Leader Takeover", K(ret), K(is_resume), KPC(this));
  return ret;
}

int ObDupTableLSHandler::prepare_log_operator_()
{
  int ret = OB_SUCCESS;

  // need release in reset()
  if (OB_ISNULL(log_operator_)) {
    if (OB_ISNULL(log_operator_ = static_cast<ObDupTableLogOperator *>(
                      share::mtl_malloc(sizeof(ObDupTableLogOperator), "DUP_LOG_OP")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DUP_TABLE_LOG(WARN, "malloc log operator failed", K(ret));
    } else {
      new (log_operator_) ObDupTableLogOperator(ls_id_, log_handler_, &dup_ls_ckpt_, lease_mgr_ptr_,
                                                tablets_mgr_ptr_, &interface_stat_);
    }
  }

  return ret;
}

int ObDupTableLSHandler::set_dup_table_ls_meta(
    const ObDupTableLSCheckpoint::ObLSDupTableMeta &dup_ls_meta,
    bool need_flush_slog)
{
  int ret = OB_SUCCESS;

  DUP_TABLE_LOG(INFO, "try to recover dup table ls meta", K(ret), K(need_flush_slog),
                K(dup_ls_meta), KPC(this));

  if (OB_FAIL(dup_ls_ckpt_.set_dup_ls_meta(dup_ls_meta))) {
    DUP_TABLE_LOG(WARN, "set dup ls meta failed", K(ret), K(need_flush_slog), K(dup_ls_meta),
                  KPC(this));
  } else if (need_flush_slog && OB_FAIL(dup_ls_ckpt_.flush())) {
    DUP_TABLE_LOG(WARN, "flush slog failed", K(ret), K(need_flush_slog), K(dup_ls_meta), KPC(this));
  } else if (dup_ls_ckpt_.is_useful_meta() && OB_FAIL(init(true/*is_dup_table*/))) {
    DUP_TABLE_LOG(WARN, "init dup table ls handler in recovery  failed", K(ret), KPC(this));
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObDupTableLSHandler::flush(share::SCN &rec)
{
  int ret = OB_SUCCESS;

  if (!ls_state_helper_.is_started()) {
    ret = OB_LS_NOT_EXIST;
    DUP_TABLE_LOG(WARN, "this ls is not started", K(ret), K(rec), KPC(this));

  } else {
    ret = dup_ls_ckpt_.flush();
  }

  return ret;
}

int ObDupTableLSHandler::check_and_update_max_replayed_scn(const share::SCN &max_replayed_scn)
{
  int ret = OB_SUCCESS;
  if (!max_replayed_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid max_replayed_scn", K(ret), K(max_replayed_scn));
  } else if (!self_max_replayed_scn_.atomic_get().is_valid()) {
    self_max_replayed_scn_.atomic_set(max_replayed_scn);
    last_max_replayed_scn_update_ts_ = ObTimeUtility::fast_current_time();
  } else if (max_replayed_scn >= self_max_replayed_scn_.atomic_get()) {
    self_max_replayed_scn_.atomic_set(max_replayed_scn);
    last_max_replayed_scn_update_ts_ = ObTimeUtility::fast_current_time();
  } else if (max_replayed_scn < self_max_replayed_scn_.atomic_get()
             && self_max_replayed_scn_.atomic_get().convert_to_ts(true)
                        - max_replayed_scn.convert_to_ts(true)
                    > 100 * 1000) {
    // ret = OB_ERR_UNEXPECTED;
    DUP_TABLE_LOG(WARN, "the max_replayed_scn has been rollbacked", K(ret), K(ls_id_),
                  K(max_replayed_scn), K(self_max_replayed_scn_),
                  K(last_max_replayed_scn_update_ts_));
  }

  return ret;
}

int64_t ObDupTableLSHandler::get_committing_dup_trx_cnt()
{
  ObSpinLockGuard guard(committing_dup_trx_lock_);
  int64_t cnt = committing_dup_trx_set_.size();
  if (cnt > 0) {
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
      TRANS_LOG(INFO, "print committing dup trx cnt", K(cnt), K(committing_dup_trx_set_));
    }
  }
  return cnt;
}

int ObDupTableLSHandler::add_commiting_dup_trx(const ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(committing_dup_trx_lock_);
  if (!committing_dup_trx_set_.created()) {
    if (OB_FAIL(committing_dup_trx_set_.create(128, "DupTrxBucket", "DupTrxCnt", MTL_ID()))) {
      TRANS_LOG(WARN, "create dup trx set failed", K(ret), K(ls_id_), K(tx_id));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(committing_dup_trx_set_.set_refactored(tx_id))) {
      TRANS_LOG(WARN, "insert into committing_dup_trx_cnt_ failed", K(ret), K(ls_id_), K(tx_id));
    }
  }
  return ret;
}

int ObDupTableLSHandler::remove_commiting_dup_trx(const ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(committing_dup_trx_lock_);

  if (OB_SUCC(ret)) {
    if (!committing_dup_trx_set_.created()) {
      ret = OB_SUCCESS;
      TRANS_LOG(WARN, "removing a dup table trx with the empty set", K(ret), K(ls_id_), K(tx_id));
    } else if (OB_FAIL(committing_dup_trx_set_.erase_refactored(tx_id))) {
      if (OB_HASH_NOT_EXIST != ret) {
        TRANS_LOG(WARN, "remove from committing_dup_trx_set_ failed", K(ret), K(ls_id_), K(tx_id));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }

  return  ret;
}

int ObDupTableLSHandler::get_min_lease_ts_info_(DupTableTsInfo &min_ts_info)
{
  int ret = OB_SUCCESS;

  int ts_info_err_ret = OB_SUCCESS;

  LeaseAddrArray lease_valid_array;
  min_ts_info.reset();

  if (!is_inited() || OB_ISNULL(lease_mgr_ptr_) || OB_ISNULL(ts_sync_mgr_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid arguments", K(is_inited_), KP(lease_mgr_ptr_),
                  KP(ts_sync_mgr_ptr_));
  } else if (OB_FAIL(ts_sync_mgr_ptr_->get_local_ts_info(min_ts_info))) {
    DUP_TABLE_LOG(WARN, "get local ts info failed", K(ret));
  } else if (!min_ts_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid local ts info", K(ret), K(min_ts_info));
  } else if (OB_FAIL(lease_mgr_ptr_->get_lease_valid_array(lease_valid_array))) {
    DUP_TABLE_LOG(WARN, "get lease valid array failed", K(ret));
  } else {
    DupTableTsInfo tmp_ts_info;
    for (int64_t i = 0; OB_SUCC(ret) && i < lease_valid_array.count(); i++) {
      if (OB_FAIL(ts_sync_mgr_ptr_->get_cache_ts_info(lease_valid_array[i], tmp_ts_info))) {
        DUP_TABLE_LOG(WARN, "get cache ts info failed", K(ret), K(lease_valid_array[i]));
        if (OB_HASH_NOT_EXIST == ret) {
          ts_info_err_ret = ret;
          ret = OB_SUCCESS;
        }
      } else {
        min_ts_info.max_replayed_scn_ =
            share::SCN::min(min_ts_info.max_replayed_scn_, tmp_ts_info.max_replayed_scn_);
        min_ts_info.max_read_version_ =
            share::SCN::min(min_ts_info.max_read_version_, tmp_ts_info.max_read_version_);
        min_ts_info.max_commit_version_ =
            share::SCN::min(min_ts_info.max_commit_version_, tmp_ts_info.max_commit_version_);
      }
    }
  }

  if (OB_SUCC(ret) && ts_info_err_ret != OB_SUCCESS) {
    ret = ts_info_err_ret;
  }

  if (OB_FAIL(ret)) {
    DUP_TABLE_LOG(WARN, "get min lease ts info failed", K(ret), K(ts_info_err_ret), K(min_ts_info),
                  K(lease_valid_array));
  }
  return ret;
}

int ObDupTableLSHandler::get_lease_mgr_stat(ObDupLSLeaseMgrStatIterator &collect_iter)
{
  int ret = OB_SUCCESS;
  FollowerLeaseMgrStatArr collect_arr;

  // collect all leader info
  if (ls_state_helper_.is_leader_serving()) {
    if (!is_inited() || OB_ISNULL(lease_mgr_ptr_) || OB_ISNULL(ts_sync_mgr_ptr_)) {
      ret = OB_NOT_INIT;
      DUP_TABLE_LOG(WARN, "not init", K(ret), KPC(lease_mgr_ptr_), KP(ts_sync_mgr_ptr_));
    } else if(OB_FAIL(lease_mgr_ptr_->get_lease_mgr_stat(collect_arr))) {
      DUP_TABLE_LOG(WARN, "get lease mgr stat from lease_mgr failed", K(ret));
    } else if(OB_FAIL(ts_sync_mgr_ptr_->get_lease_mgr_stat(collect_iter, collect_arr))) {
      DUP_TABLE_LOG(WARN, "get lease mgr stat from ts_sync_mgr failed", K(ret));
    }
    DUP_TABLE_LOG(DEBUG, "get lease mgr stat", K(ret), K(collect_arr));
  }

  return ret;
}

int ObDupTableLSHandler::get_ls_tablets_stat(ObDupLSTabletsStatIterator &collect_iter)
{
  int ret = OB_SUCCESS;
  const share::ObLSID ls_id = ls_id_;

  if (!is_inited() || OB_ISNULL(tablets_mgr_ptr_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "tablets_mgr not init", K(ret), KP(tablets_mgr_ptr_));
  } else if(OB_FAIL(tablets_mgr_ptr_->get_tablets_stat(collect_iter, ls_id_))) {
    DUP_TABLE_LOG(WARN, "get tablets stat failed", K(ret));
  }

  return ret;
}

int ObDupTableLSHandler::get_ls_tablet_set_stat(ObDupLSTabletSetStatIterator &collect_iter)
{
  int ret = OB_SUCCESS;
  const share::ObLSID ls_id = get_ls_id();

  if (!is_inited() || OB_ISNULL(tablets_mgr_ptr_)) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "not init", K(ret), KPC(tablets_mgr_ptr_));
  } else if (OB_FAIL(tablets_mgr_ptr_->get_tablet_set_stat(collect_iter, ls_id))) {
    DUP_TABLE_LOG(WARN, "get tablet set stat failed", K(ret));
  }

  return ret;
}

//*************************************************************************************************************
//**** ObDupTableLoopWorker
//*************************************************************************************************************

int ObDupTableLoopWorker::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    DUP_TABLE_LOG(WARN, "init dup_loop_worker twice", K(ret));
  } else {
    if (OB_FAIL(dup_ls_id_set_.create(8, "DUP_LS_SET", "DUP_LS_ID", MTL_ID()))) {
      DUP_TABLE_LOG(WARN, "create dup_ls_map_ error", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  DUP_TABLE_LOG(INFO, "init ObDupTableLoopWorker");
  return ret;
}

int ObDupTableLoopWorker::start()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(WARN, "dup_loop_worker has not inited", K(ret));
  } else {
    lib::ThreadPool::set_run_wrapper(MTL_CTX());
    ret = lib::ThreadPool::start();
  }
  DUP_TABLE_LOG(INFO, "start ObDupTableLoopWorker", KR(ret));
  return ret;
}

void ObDupTableLoopWorker::stop()
{
  if (!has_set_stop()) {
    DUP_TABLE_LOG(INFO, "stop ObDupTableLoopWorker");
  }
  lib::ThreadPool::stop();
}

void ObDupTableLoopWorker::wait()
{
  lib::ThreadPool::wait();
  DUP_TABLE_LOG(INFO, "wait ObDupTableLoopWorker");
}

void ObDupTableLoopWorker::destroy()
{
  lib::ThreadPool::destroy();
  (void)dup_ls_id_set_.destroy();
  DUP_TABLE_LOG(INFO, "destroy ObDupTableLoopWorker");
}

void ObDupTableLoopWorker::reset()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dup_ls_id_set_.clear())) {
    DUP_TABLE_LOG(WARN, "clear dup_ls_set failed", KR(ret));
  }
  is_inited_ = false;
}

void ObDupTableLoopWorker::run1()
{
  int ret = OB_SUCCESS;
  int64_t start_time = 0;
  int64_t time_used = 0;
  DupLSIDSet_Spin::iterator iter;
  ObSEArray<share::ObLSID, 2> remove_ls_list;

  lib::set_thread_name("DupLoop");
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    DUP_TABLE_LOG(ERROR, "dup_loop_worker has not inited", K(ret));
  } else {
    while (!has_set_stop()) {
      start_time = ObTimeUtility::current_time();

      remove_ls_list.reuse();

      for (iter = dup_ls_id_set_.begin(); iter != dup_ls_id_set_.end(); iter++) {
        const share::ObLSID cur_ls_id = iter->first;
        ObLSHandle ls_handle;

        if (OB_ISNULL(MTL(ObLSService *))
            || (OB_FAIL(MTL(ObLSService *)->get_ls(cur_ls_id, ls_handle, ObLSGetMod::TRANS_MOD))
                || !ls_handle.is_valid())) {
          if (OB_SUCC(ret)) {
            ret = OB_INVALID_ARGUMENT;
          }
          DUP_TABLE_LOG(WARN, "get ls error", K(ret), K(cur_ls_id));
        } else if (OB_FAIL(ls_handle.get_ls()->get_dup_table_ls_handler()->ls_loop_handle())) {
          DUP_TABLE_LOG(WARN, "ls loop handle error", K(ret), K(cur_ls_id));
        }

        if (OB_LS_NOT_EXIST == ret || OB_NOT_INIT == ret || OB_NO_TABLET == ret) {
          remove_ls_list.push_back(cur_ls_id);
          TRANS_LOG(INFO, "try to remove invalid dup ls id", K(ret), K(cur_ls_id),
                    K(remove_ls_list));
        }
      }

      for (int index = 0; index < remove_ls_list.count(); index++) {
        if (OB_FAIL(dup_ls_id_set_.erase_refactored(remove_ls_list[index]))) {
          DUP_TABLE_LOG(WARN, "remove from dup_ls_id_set_ failed", K(ret), K(index),
                        K(remove_ls_list[index]));
        }
      }

      time_used = ObTimeUtility::current_time() - start_time;
      if (time_used < LOOP_INTERVAL) {
        usleep(LOOP_INTERVAL - time_used);
      }
    }
  }
}

int ObDupTableLoopWorker::append_dup_table_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObDupTableLSHandler *tmp_ls_handle = nullptr;

  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid ls id", K(ls_id));
  } else if (OB_FAIL(dup_ls_id_set_.set_refactored(ls_id, 0))) {
    if (OB_HASH_EXIST == ret) {
      // do nothing
    } else {
      DUP_TABLE_LOG(WARN, "insert dup_ls_handle into hash_set failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    DUP_TABLE_LOG(INFO, "append dup table ls success", K(ret), K(ls_id));
  } else if (OB_HASH_EXIST == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret) && !dup_ls_id_set_.empty() && has_set_stop()) {
    start();
  }

  return ret;
}

int ObDupTableLoopWorker::CopyDupLsIdFunctor::operator()(common::hash::HashSetTypes<share::ObLSID>::pair_type &kv)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_id_array_.push_back(kv.first))) {
    DUP_TABLE_LOG(WARN, "push back ls id failed", K(ret));
  }
  return ret;
}

// trans service -> dup worker -> ls service -> dup ls handler -> iterate
int ObDupTableLoopWorker::iterate_dup_ls(ObDupLSLeaseMgrStatIterator &collect_iter)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  common::ObArray<share::ObLSID> ls_id_array;
  CopyDupLsIdFunctor copy_dup_ls_id_functor(ls_id_array);

  if (OB_ISNULL(ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "get ls service failed", K(ret), KP(ls_service));
  } else if (OB_FAIL(dup_ls_id_set_.foreach_refactored(copy_dup_ls_id_functor))) {
    DUP_TABLE_LOG(WARN, "get dup ls id array failed", K(ret));
  } else if (ls_id_array.count() > 0) {
    ARRAY_FOREACH(ls_id_array, i) {
      const share::ObLSID cur_ls_id = ls_id_array.at(i);
      ObDupTableLSHandler *cur_dup_ls_handler = nullptr;
      ObLSHandle ls_handle;

      if (OB_TMP_FAIL(ls_service->get_ls(cur_ls_id, ls_handle, ObLSGetMod::TRANS_MOD))) {
        if (OB_LS_NOT_EXIST != tmp_ret) {
          ret = tmp_ret;
          DUP_TABLE_LOG(WARN, "get ls handler error", K(ret), K(cur_ls_id), KPC(ls_service));
        }
      } else if (!ls_handle.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        DUP_TABLE_LOG(WARN, "ls handler not valid", K(ret), K(cur_ls_id), KPC(ls_service));
      } else {
        cur_dup_ls_handler = ls_handle.get_ls()->get_dup_table_ls_handler();
        if (OB_ISNULL(cur_dup_ls_handler) || !cur_dup_ls_handler->is_inited()) {
          ret = OB_NOT_INIT;
          DUP_TABLE_LOG(WARN, "dup ls handler not init", K(ret), K(cur_ls_id),
                        KPC(cur_dup_ls_handler));
        } else if (OB_FAIL(cur_dup_ls_handler->get_lease_mgr_stat(collect_iter))) {
          DUP_TABLE_LOG(WARN, "collect lease mgr stat failed", K(ret), K(cur_ls_id),
                        KPC(cur_dup_ls_handler));
        }
      }
    }
  }

  return ret;
}

int ObDupTableLoopWorker::iterate_dup_ls(ObDupLSTabletSetStatIterator &collect_iter)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  common::ObArray<share::ObLSID> ls_id_array;
  CopyDupLsIdFunctor copy_dup_ls_id_functor(ls_id_array);

  if (OB_ISNULL(ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "get ls service failed", K(ret), KP(ls_service));
  } else if (OB_FAIL(dup_ls_id_set_.foreach_refactored(copy_dup_ls_id_functor))) {
    DUP_TABLE_LOG(WARN, "get dup ls id array failed", K(ret));
  } else if (ls_id_array.count() > 0) {
    ARRAY_FOREACH(ls_id_array, i) {
      const share::ObLSID cur_ls_id = ls_id_array.at(i);
      ObDupTableLSHandler *cur_dup_ls_handler = nullptr;
      ObLSHandle ls_handle;

      if (OB_TMP_FAIL(ls_service->get_ls(cur_ls_id, ls_handle, ObLSGetMod::TRANS_MOD))) {
        if (OB_LS_NOT_EXIST != tmp_ret) {
          ret = tmp_ret;
          DUP_TABLE_LOG(WARN, "get ls handler error", K(ret), K(cur_ls_id), KPC(ls_service));
        }
      } else if (!ls_handle.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        DUP_TABLE_LOG(WARN, "ls handler not valid", K(ret), K(cur_ls_id), KPC(ls_service));
      } else {
        cur_dup_ls_handler = ls_handle.get_ls()->get_dup_table_ls_handler();
        if (OB_ISNULL(cur_dup_ls_handler) || !cur_dup_ls_handler->is_inited()) {
          ret = OB_NOT_INIT;
          DUP_TABLE_LOG(WARN, "dup ls handler not init", K(ret), K(cur_ls_id),
                        KPC(cur_dup_ls_handler));
        } else if (OB_FAIL(cur_dup_ls_handler->get_ls_tablet_set_stat(collect_iter))) {
          DUP_TABLE_LOG(WARN, "collect tablet set stat failed", K(ret), K(cur_ls_id),
                        KPC(cur_dup_ls_handler));
        }
      }
    }
  }

  return ret;
}

int ObDupTableLoopWorker::iterate_dup_ls(ObDupLSTabletsStatIterator &collect_iter)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  common::ObArray<share::ObLSID> ls_id_array;
  CopyDupLsIdFunctor copy_dup_ls_id_functor(ls_id_array);

  if (OB_ISNULL(ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "get ls service failed", K(ret), KP(ls_service));
  } else if (OB_FAIL(dup_ls_id_set_.foreach_refactored(copy_dup_ls_id_functor))) {
    DUP_TABLE_LOG(WARN, "get dup ls id array failed", K(ret));
  } else if (ls_id_array.count() > 0) {
    ARRAY_FOREACH(ls_id_array, i) {
      const share::ObLSID cur_ls_id = ls_id_array.at(i);
      ObDupTableLSHandler *cur_dup_ls_handler = nullptr;
      ObLSHandle ls_handle;

      if (OB_TMP_FAIL(ls_service->get_ls(cur_ls_id, ls_handle, ObLSGetMod::TRANS_MOD))) {
        if (OB_LS_NOT_EXIST != tmp_ret) {
          ret = tmp_ret;
          DUP_TABLE_LOG(WARN, "get ls handler error", K(ret), K(cur_ls_id), KPC(ls_service));
        }
      } else if (!ls_handle.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        DUP_TABLE_LOG(WARN, "ls handler not valid", K(ret), K(cur_ls_id), KPC(ls_service));
      } else {
        cur_dup_ls_handler = ls_handle.get_ls()->get_dup_table_ls_handler();
        if (OB_ISNULL(cur_dup_ls_handler) || !cur_dup_ls_handler->is_inited()) {
          ret = OB_NOT_INIT;
          DUP_TABLE_LOG(WARN, "dup ls handler not init", K(ret), K(cur_ls_id),
                        KPC(cur_dup_ls_handler));
        } else if (OB_FAIL(cur_dup_ls_handler->get_ls_tablets_stat(collect_iter))) {
          DUP_TABLE_LOG(WARN, "collect tablets stat failed", K(ret), K(cur_ls_id),
                        KPC(cur_dup_ls_handler));
        }
      }
    }
  }

  return ret;
}


bool ObDupTableLoopWorker::is_useful_dup_ls(const share::ObLSID ls_id)
{
  bool is_useful = false;

  if (OB_HASH_EXIST == (dup_ls_id_set_.exist_refactored(ls_id))) {
    is_useful = true;
  }

  return is_useful;
}

} // namespace transaction
} // namespace oceanbase
