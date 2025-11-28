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

#include "storage/tx/ob_tx_loop_worker.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_leak_checker.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "close_modules/shared_storage/storage/incremental/garbage_collector/ob_ss_garbage_collector_service.h"
#endif


namespace oceanbase
{

using namespace share;
using namespace logservice;

namespace transaction
{

int ObTxLoopWorker::mtl_init(ObTxLoopWorker *& ka)
{
  return ka->init();
}

int ObTxLoopWorker::init()
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  TRANS_LOG(INFO, "[Tx Loop Worker] init");

  lib::ThreadPool::set_run_wrapper(MTL_CTX());

  return ret;
}

int ObTxLoopWorker::start()
{
  int ret = OB_SUCCESS;

  TRANS_LOG(INFO, "[Tx Loop Worker] start");
  if (OB_FAIL(lib::ThreadPool::start())) {
    TRANS_LOG(WARN, "[Tx Loop Worker] start tx loop worker thread failed", K(ret));
  } else {
    // TRANS_LOG(INFO, "[Tx Loop Worker] start keep alive thread succeed", K(ret));
  }

  return ret;
}

void ObTxLoopWorker::stop()
{
  TRANS_LOG(INFO, "[Tx Loop Worker] stop");
  lib::ThreadPool::stop();
}

void ObTxLoopWorker::wait()
{
  TRANS_LOG(INFO, "[Tx Loop Worker] wait");
  lib::ThreadPool::wait();
}

void ObTxLoopWorker::destroy()
{
  TRANS_LOG(INFO, "[Tx Loop Worker] destroy");
  lib::ThreadPool::destroy();
  reset();
}

void ObTxLoopWorker::reset()
{
  last_tx_gc_ts_ = 0;
  last_retain_ctx_gc_ts_ = 0;
  last_check_start_working_retry_ts_ = 0;
  last_log_cb_pool_adjust_ts_ = 0;
  last_tenant_config_refresh_ts_ = 0;
  last_palf_kv_gc_interval_ts_ = 0;
}

void ObTxLoopWorker::run1()
{
  int ret = OB_SUCCESS;
  int64_t start_time_us = 0;
  int64_t time_used = 0;
  lib::set_thread_name("TxLoopWorker");
  bool can_gc_tx = false;
  bool can_gc_retain_ctx = false;
  bool can_check_and_retry_start_working = false;
  bool can_adjust_log_cb_pool =  false;
  bool can_gc_palf_kv = false;

  while (!has_set_stop()) {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    const int64_t KEEPALIVE_INTERVAL = tenant_config.is_valid() ?
        min(tenant_config->_keepalive_interval, LOOP_INTERVAL) : LOOP_INTERVAL;
    start_time_us = ObTimeUtility::current_time();
    if (REACH_TIME_INTERVAL(60000000)) {
      ObLeakChecker::dump();
    }

    // tx gc, interval = 5s
    if (common::ObClockGenerator::getClock() - last_tx_gc_ts_ > TX_GC_INTERVAL) {
      TRANS_LOG(INFO, "tx gc loop thread is running", K(MTL_ID()));
      last_tx_gc_ts_ = common::ObClockGenerator::getClock();
      can_gc_tx = true;
    }

    //retain ctx gc, interval = 5s
    if (common::ObClockGenerator::getClock() - last_retain_ctx_gc_ts_ > TX_RETAIN_CTX_GC_INTERVAL) {
      TRANS_LOG(INFO, "try gc retain ctx");
      last_retain_ctx_gc_ts_ = common::ObClockGenerator::getClock();
      can_gc_retain_ctx = true;
    }

    if (common::ObClockGenerator::getClock() - last_check_start_working_retry_ts_
        > TX_START_WORKING_RETRY_INTERVAL) {
      TRANS_LOG(INFO, "try to retry start_working");
      last_check_start_working_retry_ts_ = common::ObClockGenerator::getClock();
      can_check_and_retry_start_working = true;
    }

    if (common::ObClockGenerator::getClock() - last_log_cb_pool_adjust_ts_
        > TX_LOG_CB_POOL_ADJUST_INTERVAL) {
      TRANS_LOG(INFO, "try to adjust log cb pool");
      last_log_cb_pool_adjust_ts_ = common::ObClockGenerator::getClock();
      can_adjust_log_cb_pool = true;
    }

    // refresh tx tenant config
    if (common::ObClockGenerator::getClock() -
          last_tenant_config_refresh_ts_ > LOOP_INTERVAL * 50 /*5s*/) {
      refresh_tenant_config_();
      last_tenant_config_refresh_ts_ = common::ObClockGenerator::getClock();
    }

    if (common::ObClockGenerator::getClock() - last_palf_kv_gc_interval_ts_
        > CLUSTER_PALF_KV_GC_INTERVAL) {
      last_palf_kv_gc_interval_ts_ = common::ObClockGenerator::getClock();
      can_gc_palf_kv = true;
    }

    (void)scan_all_ls_(can_gc_tx, can_gc_retain_ctx, can_check_and_retry_start_working, can_adjust_log_cb_pool, can_gc_palf_kv);

    // TODO shanyan.g
    // 1) We use max(max_commit_ts, gts_cache) as read snapshot,
    //    but now we adopt updating max_commit_ts periodly to avoid getting gts cache cost
    // 2) Some time later, we will revert current modification when performance problem solved;
      update_max_commit_ts_();

    time_used = ObTimeUtility::current_time() - start_time_us;

    if (time_used < KEEPALIVE_INTERVAL) {
      ob_usleep(KEEPALIVE_INTERVAL - time_used, true/*is_idle_sleep*/);
    }
    can_gc_tx = false;
    can_gc_retain_ctx = false;
    can_check_and_retry_start_working = false;
    can_adjust_log_cb_pool = false;
    can_gc_palf_kv = false;
  }
}

int ObTxLoopWorker::scan_all_ls_(bool can_tx_gc,
                                 bool can_gc_retain_ctx,
                                 bool can_check_and_retry_start_working,
                                 bool can_adjust_log_cb_pool,
                                 bool can_gc_palf_kv)
{
  int ret = OB_SUCCESS;
  int iter_ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObSharedGuard<ObLSIterator> ls_iter_guard;
  ObLSIterator *iter_ptr = nullptr;
  ObLS *cur_ls_ptr = nullptr;

  int64_t ls_cnt = 0;

  if (OB_ISNULL(MTL(ObLSService *))
      || OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter_guard, ObLSGetMod::TRANS_MOD))
      || !ls_iter_guard.is_valid()) {
    if (OB_SUCCESS == ret) {
      ret = OB_INVALID_ARGUMENT;
    }
    TRANS_LOG(WARN, "[Tx Loop Worker] get ls iter failed", K(ret), KP(MTL(ObLSService *)));
  } else if (OB_ISNULL(iter_ptr = ls_iter_guard.get_ptr())) {
    TRANS_LOG(WARN, "[Tx Loop Worker] ls iter_ptr is nullptr", KP(iter_ptr));
  } else {
    iter_ret = OB_SUCCESS;
    cur_ls_ptr = nullptr;
    while (OB_SUCCESS == (iter_ret = iter_ptr->get_next(cur_ls_ptr))) {
      SCN min_start_scn = SCN::invalid_scn();
      SCN max_decided_scn = SCN::invalid_scn();
      MinStartScnStatus status = MinStartScnStatus::UNKOWN;
      common::ObRole role = common::ObRole::INVALID_ROLE;
      int64_t base_proposal_id, proposal_id;

      if (ObReplicaTypeCheck::is_log_replica(cur_ls_ptr->get_replica_type())) {
        // do nothing
      } else {
        if (OB_TMP_FAIL(cur_ls_ptr->get_log_handler()->get_role(role, base_proposal_id))) {
          TRANS_LOG(WARN, "get role failed", K(tmp_ret), K(cur_ls_ptr->get_ls_id()));
          status = MinStartScnStatus::UNKOWN;
        } else if (role == common::ObRole::FOLLOWER) {
          status = MinStartScnStatus::UNKOWN;
        }

        // tx gc, interval = 15s
        if (can_tx_gc) {
          // TODO shanyan.g close ctx gc temporarily because of logical bug
          //

          // ATTENTION : get_max_decided_scn must before iterating all trans ctx.
          // set max_decided_scn as default value
          if (OB_TMP_FAIL(cur_ls_ptr->get_log_handler()->get_max_decided_scn(max_decided_scn))) {
            TRANS_LOG(WARN, "get max decided scn failed", KR(tmp_ret), K(min_start_scn));
            max_decided_scn.set_invalid();
          } else {
            (void)cur_ls_ptr->update_min_start_scn_info(max_decided_scn);
          }
          min_start_scn = max_decided_scn;
          do_tx_gc_(cur_ls_ptr, min_start_scn, status);
        }

        if (MinStartScnStatus::UNKOWN == status) {
          // do nothing
        } else if (OB_TMP_FAIL(cur_ls_ptr->get_log_handler()->get_role(role, proposal_id))) {
          TRANS_LOG(WARN, "get role failed", K(tmp_ret), K(cur_ls_ptr->get_ls_id()));
          status = MinStartScnStatus::UNKOWN;
        } else if (role == common::ObRole::FOLLOWER) {
          status = MinStartScnStatus::UNKOWN;
        } else if (base_proposal_id != proposal_id) {
          status = MinStartScnStatus::UNKOWN;
        }

        // During the transfer, we should not update min_start_scn, otherwise we
        // will ignore the ctx that has been transferred in. So we check whether
        // transfer is going on there.
        //
        // TODO(handora.qc): while after we have checked the transfer and later
        // submitted the log, the transfer may also happens during these two
        // operations. So we need double check it in the log application/replay.
        if(MinStartScnStatus::UNKOWN == status) {
          // do nothing
        } else if (cur_ls_ptr->get_transfer_status().get_transfer_prepare_enable()) {
          TRANS_LOG(INFO, "ignore min start scn during transfer prepare enabled",
                    K(cur_ls_ptr->get_transfer_status()), K(status), K(min_start_scn));
          status = MinStartScnStatus::UNKOWN;
        }

        if (MinStartScnStatus::UNKOWN == status) {
          min_start_scn.reset();
        } else if (MinStartScnStatus::NO_CTX == status) {
          min_start_scn.set_min();
        }

        // keep alive, interval = 100ms
        do_keep_alive_(cur_ls_ptr, min_start_scn, status);

        if (can_gc_retain_ctx) {
          do_retain_ctx_gc_(cur_ls_ptr);
        }

        if (can_check_and_retry_start_working) {
          do_start_working_retry_(cur_ls_ptr);
        }
        // ignore ret
        (void)cur_ls_ptr->get_tx_svr()->check_all_readonly_tx_clean_up();

        if (can_adjust_log_cb_pool) {
          do_log_cb_pool_adjust_(cur_ls_ptr, role);
        }
      }

      if (can_gc_palf_kv) {
        do_palf_kv_gc_(cur_ls_ptr, role);
      }
    }
  }

  return ret;
}

void ObTxLoopWorker::do_keep_alive_(ObLS *ls_ptr, const SCN &min_start_scn, MinStartScnStatus status)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ls_ptr->get_keep_alive_ls_handler()->try_submit_log(min_start_scn, status))) {
    TRANS_LOG(WARN, "[Tx Loop Worker] try submit keep alive log failed", K(ret));
  } else if (REACH_TIME_INTERVAL(KEEP_ALIVE_PRINT_INFO_INTERVAL)) {
    ls_ptr->get_keep_alive_ls_handler()->print_stat_info();
  } else {
    // do nothing
  }

  UNUSED(ret);
}

void ObTxLoopWorker::do_tx_gc_(ObLS *ls_ptr, SCN &min_start_scn, MinStartScnStatus &status)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ls_ptr->get_tx_svr()->check_scheduler_status(min_start_scn, status))) {
    TRANS_LOG(WARN, "[Tx Loop Worker] check tx scheduler failed", K(ret), K(MTL_ID()), K(*ls_ptr));
  } else {
    TRANS_LOG(INFO, "[Tx Loop Worker] check tx scheduler success", K(MTL_ID()), K(*ls_ptr));
  }

  UNUSED(ret);
}

void ObTxLoopWorker::refresh_tenant_config_()
{
  int ret = OB_SUCCESS;
  ObTransService *txs = NULL;
  if (OB_ISNULL(txs = MTL(ObTransService *))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected transaction service", K(ret), KP(txs));
  } else {
    txs->get_tx_elr_util().check_and_update_tx_elr_info();
  }
}

void ObTxLoopWorker::update_max_commit_ts_()
{
  int ret = OB_SUCCESS;
  SCN snapshot;
  const int64_t expire_ts = ObClockGenerator::getClock() + 1000000; // 1s
  ObTransService *txs = NULL;

  do {
    int64_t n = ObClockGenerator::getClock();
    if (n >= expire_ts) {
      ret = OB_TIMEOUT;
    } else if (OB_FAIL(OB_TS_MGR.get_gts(MTL_ID(), NULL, snapshot))) {
      if (OB_EAGAIN == ret) {
        ob_usleep(500, true/*is_idle_sleep*/);
      } else {
        TRANS_LOG(WARN, "get gts fail", "tenant_id", MTL_ID());
      }
    } else if (OB_UNLIKELY(!snapshot.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "invalid snapshot from gts", K(snapshot));
    } else if (OB_ISNULL(txs = MTL(ObTransService *))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected transaction service", K(ret), KP(txs));
    } else {
      txs->get_tx_version_mgr().update_max_commit_ts(snapshot, false);
    }
  } while (OB_EAGAIN == ret);
  // TODO, sys tenant
  if (is_meta_tenant(MTL_ID()) && GCTX.is_shared_storage_mode()) {
    update_max_commit_ts_for_sslog_();
  }
}

void ObTxLoopWorker::update_max_commit_ts_for_sslog_()
{
  int ret = OB_SUCCESS;
  SCN snapshot;
  const int64_t expire_ts = ObClockGenerator::getClock() + 1000000; // 1s
  ObTransService *txs = NULL;

  do {
    int64_t n = ObClockGenerator::getClock();
    if (n >= expire_ts) {
      ret = OB_TIMEOUT;
    } else if (!GCTX.is_shared_storage_mode()) {
      // do nothing
      ret = OB_SUCCESS;
    } else if (!is_meta_tenant(MTL_ID())) {
      // do nothing
      ret = OB_SUCCESS;
    } else if (OB_FAIL(OB_TS_MGR.get_gts(get_sslog_gts_tenant_id(MTL_ID()), NULL, snapshot))) {
      if (OB_EAGAIN == ret) {
        ob_usleep(500, true/*is_idle_sleep*/);
      } else {
        TRANS_LOG(WARN, "get gts fail", "tenant_id", MTL_ID());
      }
    } else if (OB_UNLIKELY(!snapshot.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "invalid snapshot from gts", K(snapshot));
    } else if (OB_ISNULL(txs = MTL(ObTransService *))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected transaction service", K(ret), KP(txs));
    } else {
      txs->get_tx_version_mgr_for_sslog().update_max_commit_ts(snapshot, false);
    }
  } while (OB_EAGAIN == ret);
}

void ObTxLoopWorker::do_retain_ctx_gc_(ObLS *ls_ptr)
{
  int ret = OB_SUCCESS;

  ObTxRetainCtxMgr *retain_ctx_mgr = ls_ptr->get_tx_svr()->get_retain_ctx_mgr();
  if (OB_ISNULL(retain_ctx_mgr)) {
    // ignore ret
    TRANS_LOG(WARN, "[Tx Loop Worker] retain_ctx_mgr  is not inited", K(ret), K(MTL_ID()),
              K(*ls_ptr));

  } else if (OB_FAIL(retain_ctx_mgr->try_gc_retain_ctx(ls_ptr))) {
    TRANS_LOG(WARN, "[Tx Loop Worker] retain_ctx_mgr try to gc retain ctx failed", K(ret),
              K(MTL_ID()), K(*ls_ptr));
  } else {
    TRANS_LOG(DEBUG, "[Tx Loop Worker] retain_ctx_mgr try to gc retain ctx success", K(ret),
              K(MTL_ID()), K(*ls_ptr));
  }

  retain_ctx_mgr->print_retain_ctx_info(ls_ptr->get_ls_id());
  retain_ctx_mgr->try_advance_retain_ctx_gc(ls_ptr->get_ls_id());

  UNUSED(ret);
}

void ObTxLoopWorker::do_start_working_retry_(ObLS *ls_ptr)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ls_ptr->retry_apply_start_working_log())) {
    TRANS_LOG(WARN, "retry to apply start working log failed", K(ret), KPC(ls_ptr));
  }
}

void ObTxLoopWorker::do_log_cb_pool_adjust_(ObLS *ls_ptr, const common::ObRole role)
{
  int ret = OB_SUCCESS;
  int64_t active_tx_cnt = 0;
  (void)ls_ptr->get_tx_svr()->get_active_tx_count(active_tx_cnt);
  if (common::is_strong_leader(role)) {
    if (OB_FAIL(ls_ptr->get_tx_svr()->get_log_cb_pool_mgr()->adjust_log_cb_pool(active_tx_cnt))) {
      TRANS_LOG(WARN, "adjust log cb pool failed", K(ret), K(role), KPC(ls_ptr));
    }
  } else {
    // log handler follower, not tx follower
    if (OB_FAIL(
            ls_ptr->get_tx_svr()->get_log_cb_pool_mgr()->clear_log_cb_pool(false /*for replay*/))) {
      TRANS_LOG(WARN, "clear log cb pools on a follower  failed", K(ret), K(role), KPC(ls_ptr));
    }
  }
}

void ObTxLoopWorker::do_palf_kv_gc_(ObLS *ls_ptr, const common::ObRole role)
{
  int ret = OB_SUCCESS;
  share::SCN max_gc_scn;

  const uint64_t tenant_id = MTL_ID();
  const share::ObLSID ls_id = ls_ptr->get_ls_id();

#ifdef OB_BUILD_SHARED_STORAGE
  if (!is_sys_tenant(tenant_id)) {
    // do nothing
  } else if (!ls_id.is_sslog_ls() || role != common::ObRole::LEADER) {
    // do nothing
  } else {
    if (OB_FAIL(MTL(ObSSGarbageCollectorService *)->get_min_ss_gc_last_succ_scn(true /*is_for_sslog_table */, max_gc_scn))) {
      TRANS_LOG(WARN, "get max gc scn failed", K(ret), K(max_gc_scn), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(MTL(ObTransService *)->push_palf_kv_gc_task(max_gc_scn))) {
      TRANS_LOG(WARN, "push palf kv gc task failed", K(ret), K(max_gc_scn), K(tenant_id), K(ls_id));
    }

    TRANS_LOG(INFO, "do palf kv gc", K(ret), K(tenant_id), K(ls_id), K(max_gc_scn));
  }
#endif
}
}
}

