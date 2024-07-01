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

#include "ob_storage_ha_diagnose_service.h"
#include "ob_storage_ha_diagnose_mgr.h"
#include "observer/ob_server_struct.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

ObStorageHADiagService::ObStorageHADiagService()
  : is_inited_(false),
    thread_cond_(),
    wakeup_cnt_(0),
    op_(),
    sql_proxy_(nullptr),
    task_keys_(),
    lock_(),
    err_diag_end_timestamp_(0),
    perf_diag_end_timestamp_(0)
{
}

int ObStorageHADiagService::init(common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("storage ha diagnose service is aleady init", K(ret));
  } else if (OB_FAIL(op_.init())) {
    LOG_WARN("failed to init op", K(ret));
  } else if (OB_FAIL(thread_cond_.init(ObWaitEventIds::HA_SERVICE_COND_WAIT))) {
    LOG_WARN("failed to init ha service thread cond", K(ret));
  } else {
    lib::ObMemAttr attr(OB_SERVER_TENANT_ID, "StorageHADiag");
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(task_keys_.create(REPORT_KEY_MAX_NUM, attr))) {
      LOG_WARN("failed to init hash map", K(ret));
    } else {
      sql_proxy_ = sql_proxy;
      is_inited_ = true;
    }
  }

  return ret;
}

void ObStorageHADiagService::wakeup()
{
  ObThreadCondGuard cond_guard(thread_cond_);
  wakeup_cnt_++;
  thread_cond_.signal();
}

void ObStorageHADiagService::destroy()
{
  COMMON_LOG(INFO, "ObStorageHADiagService starts to destroy");
  is_inited_ = false;
  thread_cond_.destroy();
  wakeup_cnt_ = 0;
  sql_proxy_ = nullptr;
  common::SpinWLockGuard guard(lock_);
  task_keys_.destroy();
  err_diag_end_timestamp_ = 0;
  perf_diag_end_timestamp_ = 0;
  COMMON_LOG(INFO, "ObStorageHADiagService destroyed");
}

void ObStorageHADiagService::stop()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObStorageHADiagService starts to stop");
    lib::ThreadPool::stop();
    wakeup();
    COMMON_LOG(INFO, "ObStorageHADiagService stopped");
  }
}

void ObStorageHADiagService::wait()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObStorageHADiagService starts to wait");
    lib::ThreadPool::wait();
    COMMON_LOG(INFO, "ObStorageHADiagService finish to wait");
  }
}

int ObStorageHADiagService::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha monitor service do not init", K(ret));
  } else {
    if (OB_FAIL(lib::ThreadPool::start())) {
      COMMON_LOG(WARN, "ObStorageHADiagService start thread failed", K(ret));
    } else {
      COMMON_LOG(INFO, "ObStorageHADiagService start");
    }
  }
  return ret;
}

void ObStorageHADiagService::run1()
{
  int ret = OB_SUCCESS;
  lib::set_thread_name("ObStorageHADiagService");
  uint64_t data_version = 0;
  while (!has_set_stop()) {
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (observer::ObServiceStatus::SS_SERVING != GCTX.status_) {
      ret = OB_SERVER_IS_INIT;
      LOG_WARN("server is not serving", K(ret), K(GCTX.status_));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, data_version))) {
      LOG_WARN("failed to get data version", K(ret));
    } else if (!((data_version >= DATA_VERSION_4_2_2_0 && data_version < DATA_VERSION_4_3_0_0)
        || data_version >= DATA_VERSION_4_3_1_0)) { /* [4.2.2, 4.3.0) && [4.3.1, ~) */
      //do nothing
    } else if (OB_FAIL(do_clean_history_(ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE,
        err_diag_end_timestamp_))) {
      LOG_WARN("failed to do clean error diagnose info", K(ret), K(err_diag_end_timestamp_));
    } else if (OB_FAIL(do_clean_history_(ObStorageHADiagModule::TRANSFER_PERF_DIAGNOSE,
        perf_diag_end_timestamp_))) {
      LOG_WARN("failed to do clean perf diagnose info", K(ret), K(perf_diag_end_timestamp_));
    } else if (OB_FAIL(do_report_())) {
      LOG_WARN("failed to do report", K(ret));
    } else if (OB_FAIL(do_clean_transfer_related_info_())) {
      LOG_WARN("failed to do clean transfer related info", K(ret));
    }

    ObThreadCondGuard guard(thread_cond_);
    if (has_set_stop() || wakeup_cnt_ > 0) {
      wakeup_cnt_ = 0;
    } else {
      int64_t wait_time_ms = 5 * 60 * 1000L;
#ifdef ERRSIM
      wait_time_ms = GCONF.errsim_transfer_diagnose_server_wait_time / 1000;
#endif
      thread_cond_.wait(wait_time_ms);
    }
  }
}

ObStorageHADiagService &ObStorageHADiagService::instance()
{
  static ObStorageHADiagService storage_ha_diag_service;
  return storage_ha_diag_service;
}

int ObStorageHADiagService::get_info_from_type_(const ObStorageHADiagTaskKey &key, ObStorageHADiagInfo *&info,
                                                ObTransferErrorDiagInfo &transfer_err_diag,
                                                ObTransferPerfDiagInfo &transfer_perf_diag,
                                                ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  info = nullptr;
  if (key.is_valid()) {
    switch(key.module_) {
      case ObStorageHADiagModule::TRANSFER_ERROR_DIAGNOSE: {
        info = &transfer_err_diag;
        break;
      }
      case ObStorageHADiagModule::TRANSFER_PERF_DIAGNOSE: {
        if (!transfer_perf_diag.is_inited() && OB_FAIL(transfer_perf_diag.init(&alloc, OB_SERVER_TENANT_ID))) {
          LOG_WARN("fail to init info", K(ret));
        } else {
          info = &transfer_perf_diag;
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid module", K(ret), K(key.module_));
        break;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(key));
  }

  return ret;
}

int ObStorageHADiagService::add_task(const ObStorageHADiagTaskKey &key)
{
  int ret =OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else {
    common::SpinWLockGuard guard(lock_);
    int64_t now = ObTimeUtility::current_time();
    if (REPORT_KEY_MAX_NUM == task_keys_.size()) {
      if (OB_FAIL(remove_oldest_without_lock_())) {
        LOG_WARN("failed to remove first", K(ret), K(task_keys_.size()));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(task_keys_.set_refactored(key, now))) {
      LOG_WARN("failed to add key to hash map", K(ret), K(key), K(now));
    }
  }
  return ret;
}

int ObStorageHADiagService::get_oldest_key_without_lock_(ObStorageHADiagTaskKey &key) const
{
  int ret = OB_SUCCESS;
  key.reset();
  int64_t old_timestamp = INT64_MAX;
  TaskKeyMap::const_iterator it = task_keys_.begin();
  for (; OB_SUCC(ret) && it != task_keys_.end(); ++it) {
    int64_t timestamp = it->second;
    if (timestamp < old_timestamp) {
      old_timestamp = timestamp;
      key = it->first;
    }
  }
  if (!key.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("key is invalid", K(ret), K(key));
  }
  return ret;
}

int ObStorageHADiagService::del_task_in_mgr_(const ObStorageHADiagTaskKey &task_key) const
{
  int ret = OB_SUCCESS;
  ObStorageHADiagMgr *mgr = nullptr;
  if (!task_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_key));
  } else {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (OB_FAIL(guard.switch_to(task_key.tenant_id_, false))) {
      if (OB_TENANT_NOT_IN_SERVER == ret) {
        // overwrite ret
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to switch tenant", K(ret), K(task_key.tenant_id_));
      }
    } else {
      if (OB_ISNULL(mgr = MTL(ObStorageHADiagMgr *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get ObStorageHADiagMgr", K(ret), K(task_key.tenant_id_));
      } else if (OB_FAIL(mgr->del(task_key))) {
        LOG_WARN("fail to del task key", K(ret), K(task_key));
      }
    }
  }
  return ret;
}

int ObStorageHADiagService::remove_oldest_without_lock_()
{
  int ret = OB_SUCCESS;
  ObStorageHADiagTaskKey key;
  if (OB_FAIL(get_oldest_key_without_lock_(key))) {
    LOG_WARN("failed to get oldest key", K(ret), K(key));
  } else if (OB_FAIL(clean_task_key_without_lock_(key))) {
    LOG_WARN("failed to clean task", K(ret), K(key));
  } else if (OB_FAIL(del_task_in_mgr_(key))) {
    LOG_WARN("failed to del task in mgr", K(ret), K(key));
  } else {
    LOG_INFO("succeed to remove task key", K(ret), K(key));
  }
  return ret;
}

int ObStorageHADiagService::do_clean_history_(const ObStorageHADiagModule module, int64_t &end_timestamp)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<int64_t> timestamp_array;
  int64_t delete_index = 0;
  while (OB_SUCC(ret)) {
    const int64_t now = ObTimeUtility::current_time();
    const int64_t delete_timestamp = now - GCONF._ha_diagnose_history_recycle_interval;
    if (OB_FAIL(op_.get_batch_row_keys(*sql_proxy_, OB_SYS_TENANT_ID,
        module, end_timestamp, timestamp_array))) {
      LOG_WARN("failed to get row keys", K(ret), K(end_timestamp), K(timestamp_array));
    } else if (0 == timestamp_array.count()) {
      break;
      LOG_INFO("have not item need to clean ", K(ret), K(timestamp_array.count()));
    } else if (timestamp_array.at(0) > delete_timestamp) {
      break;
      LOG_INFO("have not item need to clean ", K(ret), K(delete_timestamp), K(timestamp_array.at(0)));
    } else if (OB_FAIL(op_.do_batch_delete(*sql_proxy_, OB_SYS_TENANT_ID,
        module, timestamp_array, delete_timestamp, delete_index))) {
      LOG_WARN("failed to delete batch row", K(ret), K(timestamp_array), K(delete_timestamp));
    } else {
      end_timestamp = timestamp_array.at(delete_index);
    }
  }
  end_timestamp = 0;
  return ret;
}

int ObStorageHADiagService::insert_inner_table_(
    ObMySQLTransaction &trans,
    ObStorageHADiagMgr *mgr,
    const ObStorageHADiagTaskKey &task_key,
    ObStorageHADiagInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr) || !task_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_key), KP(mgr));
  } else if (OB_FAIL(mgr->get(task_key, info))) {
    //overwrite ret
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
    LOG_INFO("info has already been cleaned", K(ret), K(task_key));
  } else if (OB_FAIL(op_.insert_row(trans, OB_SYS_TENANT_ID, task_key.tenant_id_, info, info.module_))) {
    LOG_WARN("failed to insert row", K(ret), K(info));
  }
  return ret;
}

int ObStorageHADiagService::deep_copy_keys_(TaskKeyArray &do_report_keys) const
{
  int ret =OB_SUCCESS;
  do_report_keys.reset();
  common::SpinRLockGuard guard(lock_);
  TaskKeyMap::const_iterator it = task_keys_.begin();
  for (int64_t i = 0; OB_SUCC(ret) && it != task_keys_.end() && i < ONCE_REPORT_KEY_MAX_NUM; ++it, ++i) {
    const ObStorageHADiagTaskKey &key = it->first;
    if (OB_FAIL(do_report_keys.push_back(key))) {
      LOG_WARN("failed to add key to report keys", K(ret), K(key), K(do_report_keys));
    }
  }
  return ret;
}

int ObStorageHADiagService::clean_task_key_without_lock_(const ObStorageHADiagTaskKey &task_key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_keys_.erase_refactored(task_key))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to erase_refactored", KR(ret), K(task_key));
    }
  }
  return ret;
}

int ObStorageHADiagService::report_to_inner_table_(
    ObMySQLTransaction &trans,
    ObStorageHADiagMgr *mgr,
    const ObStorageHADiagTaskKey &task_key)
{
  int ret = OB_SUCCESS;
  ObStorageHADiagInfo *info = nullptr;
  ObArenaAllocator alloc;
  ObTransferErrorDiagInfo transfer_err_diag;
  ObTransferPerfDiagInfo transfer_perf_diag;
  if (OB_ISNULL(mgr) || !task_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_key));
  } else {
    if (OB_FAIL(get_info_from_type_(task_key, info, transfer_err_diag, transfer_perf_diag, alloc))) {
      LOG_WARN("failed to get info", K(ret), K(task_key), K(transfer_err_diag), K(transfer_perf_diag));
    } else if (OB_FAIL(insert_inner_table_(trans, mgr, task_key, *info))) {
      LOG_WARN("failed to insert inner table", K(ret), K(task_key));
    }
  }
  return ret;
}

int ObStorageHADiagService::start_trans_(
    ObTimeoutCtx &timeout_ctx,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t stmt_timeout = 10_s;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageHADiagService do not init", K(ret));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", K(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("set timeout context failed", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to start trans", K(ret));
  }
  return ret;
}

int ObStorageHADiagService::commit_trans_(
    const int32_t result,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageHADiagService do not init", K(ret));
  } else if (OB_FAIL(trans.end(OB_SUCCESS == result))) {
    LOG_WARN("end transaction failed", K(ret));
  }

  return ret;
}

int ObStorageHADiagService::report_batch_task_key_(ObMySQLTransaction &trans, const TaskKeyArray &task_keys)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObStorageHADiagMgr *mgr = nullptr;
  for (int64_t cur_index = 0; OB_SUCC(ret) && cur_index < task_keys.count(); cur_index++) {
    const ObStorageHADiagTaskKey &task_key = task_keys.at(cur_index);
    if (!task_key.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument!", K(ret), K(task_key));
    } else {
      MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
      if (task_key.tenant_id_ != tenant_id) {
        if (OB_FAIL(guard.switch_to(task_key.tenant_id_, false))) {
          if (OB_TENANT_NOT_IN_SERVER == ret) {
            // overwrite ret
            ret = OB_SUCCESS;
            continue;
          } else {
            LOG_WARN("fail to switch tenant", K(ret), K(task_key.tenant_id_));
          }
        } else {
          tenant_id = task_key.tenant_id_;
          if (OB_ISNULL(mgr = MTL(ObStorageHADiagMgr *))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get ObStorageHADiagMgr", K(ret), K(tenant_id));
          }
        }
      }
      if (OB_FAIL(ret)) {
        //do nothing
      } else if (OB_FAIL(report_to_inner_table_(trans, mgr, task_key))) {
        LOG_WARN("fail to report to inner table", K(ret), K(task_key));
      }
    }
  }
  return ret;
}

int ObStorageHADiagService::batch_del_task_in_mgr_(const TaskKeyArray &task_keys)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObStorageHADiagMgr *mgr = nullptr;
  for (int64_t cur_index = 0; OB_SUCC(ret) && cur_index < task_keys.count(); cur_index++) {
    const ObStorageHADiagTaskKey &task_key = task_keys.at(cur_index);
    if (!task_key.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument!", K(ret), K(task_key));
    } else {
      MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
      if (task_key.tenant_id_ != tenant_id) {
        if (OB_FAIL(guard.switch_to(task_key.tenant_id_, false))) {
          if (OB_TENANT_NOT_IN_SERVER == ret) {
            // overwrite ret
            ret = OB_SUCCESS;
            continue;
          } else {
            LOG_WARN("fail to switch tenant", K(ret), K(task_key.tenant_id_));
          }
        } else {
          tenant_id = task_key.tenant_id_;
          if (OB_ISNULL(mgr = MTL(ObStorageHADiagMgr *))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get ObStorageHADiagMgr", K(ret), K(tenant_id));
          }
        }
      }
      if (OB_FAIL(ret)) {
        //do nothing
      } else if (OB_FAIL(mgr->del(task_key))) {
        LOG_WARN("fail to del task key", K(ret), K(task_key));
      } else {
        common::SpinWLockGuard guard(lock_);
        if (OB_FAIL(clean_task_key_without_lock_(task_key))) {
          LOG_WARN("failed to clean task keys", K(ret), K(task_key));
        }
      }
    }
  }
  return ret;
}

int ObStorageHADiagService::report_process_(const TaskKeyArray &task_keys)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ObMySQLTransaction trans;
  ObTimeoutCtx timeout_ctx;
  ObStorageHADiagMgr *mgr = nullptr;
  if (OB_FAIL(start_trans_(timeout_ctx, trans))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(report_batch_task_key_(trans, task_keys))) {
      LOG_WARN("failed to batch report task key", K(ret), K(task_keys));
    }
    if (OB_TMP_FAIL(commit_trans_(ret, trans))) {
      LOG_WARN("failed to commit trans", K(tmp_ret), K(ret));
      if (OB_SUCCESS == ret) {
        ret = tmp_ret;
      }
    }
    if (OB_SUCCESS == ret && OB_SUCCESS == tmp_ret) {
      if (OB_FAIL(batch_del_task_in_mgr_(task_keys))) {
        LOG_WARN("failed to delete tasks", K(ret), K(task_keys));
      }
    }
  }
  return ret;
}

int ObStorageHADiagService::do_report_()
{
  int ret = OB_SUCCESS;
  TaskKeyArray do_report_keys;
  if (OB_FAIL(deep_copy_keys_(do_report_keys))) {
    LOG_WARN("failed to copy keys", K(ret), K(do_report_keys));
  } else {
    lib::ob_sort(do_report_keys.begin(), do_report_keys.end());
    if (OB_FAIL(report_process_(do_report_keys))) {
      LOG_WARN("failed to do report", K(ret), K(do_report_keys));
    }
  }

  return ret;
}

int ObStorageHADiagService::do_clean_related_info_(ObLSService *ls_service, const share::ObLSID &ls_id, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  share::ObTransferTaskID task_id;
  int64_t result_count = 0;
  if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // overwrite ret
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    }
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id));
  } else if (OB_FAIL(ls->get_transfer_handler()->get_related_info_task_id(task_id))) {
    LOG_WARN("failed to get transfer task id", K(ret), K(task_id));
  } else if (!task_id.is_valid()) {
    // do nothing
  } else if (OB_FAIL(op_.check_transfer_task_exist(*sql_proxy_, tenant_id, task_id, result_count))) {
    if (OB_TABLE_NOT_EXIST == ret) {
      // overwrite ret
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to check transfer task exist", K(ret), K(tenant_id), K(task_id), K(result_count));
    }
  } else if (0 != result_count) {
    // do not clean related info
  } else if (OB_FAIL(ls->get_transfer_handler()->reset_related_info(task_id))) {
    LOG_WARN("failed to reset transfer related info", K(ret), K(task_id));
  }
  return ret;
}

int ObStorageHADiagService::scheduler_clean_related_info_(ObLSService *ls_service, const ObIArray<share::ObLSID> &ls_id_array, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (ls_id_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id array empty", K(ret), K(ls_id_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_id_array.count(); ++i) {
      const share::ObLSID &ls_id = ls_id_array.at(i);
      if (OB_FAIL(do_clean_related_info_(ls_service, ls_id, tenant_id))) {
        LOG_WARN("failed to do clean related info", K(ret), K(ls_id), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObStorageHADiagService::get_ls_id_array_(ObLSService *ls_service, ObIArray<share::ObLSID> &ls_id_array)
{
  int ret = OB_SUCCESS;
  ls_id_array.reset();
  common::ObSharedGuard<ObLSIterator> ls_iter_guard;
  ObLSIterator *ls_iter = nullptr;
  if (OB_FAIL(ls_service->get_ls_iter(ls_iter_guard, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls iter", K(ret));
  } else if (OB_ISNULL(ls_iter = ls_iter_guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls iter should not be NULL", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      ObLS *ls = nullptr;
      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls should not be NULL", K(ret), KP(ls));
      } else if (OB_FAIL(ls_id_array.push_back(ls->get_ls_id()))) {
        LOG_WARN("failed to push ls id into array", K(ret), KP(ls));
      }
    }
  }
  return ret;
}

int ObStorageHADiagService::do_clean_related_info_in_ls_(ObLSService *ls_service, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObLSID> ls_id_array;
  if (OB_FAIL(get_ls_id_array_(ls_service, ls_id_array))) {
    LOG_WARN("failed to get ls id array", K(ret));
  } else if (ls_id_array.empty()) {
    // do nothing
  } else if (OB_FAIL(scheduler_clean_related_info_(ls_service, ls_id_array, tenant_id))) {
    LOG_WARN("failed to scheduler clean related info", K(ret), K(ls_id_array), K(tenant_id));
  }
  return ret;
}

int ObStorageHADiagService::do_clean_transfer_related_info_()
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> tenant_ids;
  ObLSService * ls_service = nullptr;
  if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get_mtl_tenant_ids", K(ret));
  } else {
    for (int64_t i = 0; i < tenant_ids.size(); i++) {
      uint64_t tenant_id = tenant_ids.at(i);
      if (GCTX.omt_->is_available_tenant(tenant_id)) {
        MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
        if (OB_FAIL(guard.switch_to(tenant_id, false))) {
          if (OB_TENANT_NOT_IN_SERVER == ret) {
            // overwrite ret
            ret = OB_SUCCESS;
            continue;
          } else {
            LOG_WARN("fail to switch tenant", K(ret), K(tenant_id));
          }
        } else {
          if (OB_ISNULL(ls_service = (MTL(ObLSService *)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ls service should not be NULL", K(ret), KP(ls_service));
          } else if (OB_FAIL(do_clean_related_info_in_ls_(ls_service, tenant_id))) {
            LOG_WARN("fail to clean related info", K(ret), K(tenant_id));
          }
        }
      }
    }
  }
  return ret;
}

}
}
