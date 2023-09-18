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

#include "logservice/ob_log_base_header.h"
#include "logservice/ob_log_handler.h"
#include "storage/tx/ob_tx_loop_worker.h"
#include "storage/tx/ob_tx_retain_ctx_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/tx/ob_trans_service.h"

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
  last_tx_gc_ts_ = false;
  last_retain_ctx_gc_ts_ = 0;
}

void ObTxLoopWorker::run1()
{
  int ret = OB_SUCCESS;
  int64_t start_time_us = 0;
  int64_t time_used = 0;
  lib::set_thread_name("TxLoopWorker");
  bool can_gc_tx = false;
  bool can_gc_retain_ctx = false;

  while (!has_set_stop()) {
    start_time_us = ObTimeUtility::current_time();

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

    (void)scan_all_ls_(can_gc_tx, can_gc_retain_ctx);

    // TODO shanyan.g
    // 1) We use max(max_commit_ts, gts_cache) as read snapshot,
    //    but now we adopt updating max_commit_ts periodly to avoid getting gts cache cost
    // 2) Some time later, we will revert current modification when performance problem solved;
      update_max_commit_ts_();

    time_used = ObTimeUtility::current_time() - start_time_us;

    if (time_used < LOOP_INTERVAL) {
      ob_usleep(LOOP_INTERVAL- time_used);
    }
    can_gc_tx = false;
    can_gc_retain_ctx = false;
  }
}

int ObTxLoopWorker::scan_all_ls_(bool can_tx_gc, bool can_gc_retain_ctx)
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

      SCN min_start_scn;
      MinStartScnStatus status = MinStartScnStatus::UNKOWN;
      common::ObRole role;
      int64_t base_proposal_id, proposal_id;

      if (OB_TMP_FAIL(cur_ls_ptr->get_log_handler()->get_role(role, base_proposal_id))) {
        TRANS_LOG(WARN, "get role failed", K(tmp_ret), K(cur_ls_ptr->get_ls_id()));
        status  = MinStartScnStatus::UNKOWN;
      } else if (role == common::ObRole::FOLLOWER) {
        status = MinStartScnStatus::UNKOWN;
      }

      // tx gc, interval = 15s
      if (can_tx_gc) {
        // TODO shanyan.g close ctx gc temporarily because of logical bug
        //
        do_tx_gc_(cur_ls_ptr, min_start_scn, status);
      }

      if(MinStartScnStatus::UNKOWN == status) {
        // do nothing
      } else if (OB_TMP_FAIL(cur_ls_ptr->get_log_handler()->get_role(role, proposal_id))) {
        TRANS_LOG(WARN, "get role failed", K(tmp_ret), K(cur_ls_ptr->get_ls_id()));
        status = MinStartScnStatus::UNKOWN;
      } else if (role == common::ObRole::FOLLOWER) {
        status = MinStartScnStatus::UNKOWN;
      } else if (base_proposal_id != proposal_id) {
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
    }
  }

  return ret;
}

void ObTxLoopWorker::do_keep_alive_(ObLS *ls_ptr, const SCN &min_start_scn, MinStartScnStatus status)
{
  int ret = OB_SUCCESS;

  if (ls_ptr->get_keep_alive_ls_handler()->try_submit_log(min_start_scn, status)) {
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
        ob_usleep(500);
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
}

void ObTxLoopWorker::do_retain_ctx_gc_(ObLS *ls_ptr)
{
  int ret = OB_SUCCESS;

  ObTxRetainCtxMgr *retain_ctx_mgr = ls_ptr->get_tx_svr()->get_retain_ctx_mgr();
  if (OB_ISNULL(retain_ctx_mgr)) {
    TRANS_LOG(WARN, "[Tx Loop Worker] retain_ctx_mgr  is not inited", K(ret), K(MTL_ID()),
              K(*ls_ptr));

  } else if (retain_ctx_mgr->try_gc_retain_ctx(ls_ptr)) {
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

}
}

