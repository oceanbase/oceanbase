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

#include <functional>
#include "ob_trans_service.h"

#include "lib/profile/ob_perf_event.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/ob_name_id_def.h"
#include "lib/ob_running_mode.h"
#include "ob_trans_ctx.h"
#include "ob_trans_factory.h"
#include "ob_trans_functor.h"
#include "ob_trans_part_ctx.h"
#include "ob_trans_result.h"
#include "ob_tx_retain_ctx_mgr.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/ob_i_store.h"
#include "wrs/ob_i_weak_read_service.h"           // ObIWeakReadService
#include "sql/session/ob_basic_session_info.h"
#include "wrs/ob_weak_read_util.h"               // ObWeakReadUtil
#include "storage/memtable/ob_memtable_context.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "common/storage/ob_sequence.h"
#include "observer/ob_srv_network_frame.h"
#include "share/rc/ob_tenant_module_init_ctx.h"
#include "storage/tx_storage/ob_tenant_freezer.h"

namespace oceanbase
{

using namespace obrpc;
using namespace common;
using namespace lib;
using namespace share;
using namespace storage;
//using namespace memtable;
using namespace sql;
using namespace observer;

namespace transaction
{
ObTransService::ObTransService()
    : is_inited_(false),
      is_running_(false),
      use_def_(true),
      rpc_(&rpc_def_),
      location_adapter_(&location_adapter_def_),
      schema_service_(NULL),
      ts_mgr_(NULL),
      server_tracer_(NULL),
      input_queue_count_(0),
      output_queue_count_(0),
#ifdef ENABLE_DEBUG_LOG
      defensive_check_mgr_(NULL),
#endif
      tx_desc_mgr_(*this)
{
  check_env_();
}

int ObTransService::mtl_init(ObTransService *&it)
{
  int ret = OB_SUCCESS;
  const ObAddr &self = GCTX.self_addr();
  share::ObLocationService *location_service = GCTX.location_service_;
  share::schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  obrpc::ObBatchRpc *batch_rpc = GCTX.batch_rpc_;
  obrpc::ObSrvRpcProxy *rpc_proxy = GCTX.srv_rpc_proxy_;
  share::ObAliveServerTracer *server_tracer = GCTX.server_tracer_;
  ObSrvNetworkFrame *net_frame = GCTX.net_frame_;
  auto req_transport = net_frame->get_req_transport();
  if (OB_FAIL(it->rpc_def_.init(it, req_transport, self, batch_rpc))) {
    TRANS_LOG(ERROR, "rpc init error", KR(ret));
  } else if (OB_FAIL(it->dup_table_rpc_def_.init(it, req_transport, self))) {
    TRANS_LOG(ERROR, "dup table rpc init error", KR(ret));
  } else if (OB_FAIL(it->dup_table_rpc_impl_.init(req_transport,self))) {
    TRANS_LOG(ERROR, "dup table rpc init error", KR(ret));
  } else if (OB_FAIL(it->location_adapter_def_.init(schema_service, location_service))) {
    TRANS_LOG(ERROR, "location adapter init error", KR(ret));
  } else if (OB_FAIL(it->gti_source_def_.init(self, req_transport))) {
    TRANS_LOG(ERROR, "gti source init error", KR(ret));
  } else if (OB_FAIL(it->init(self,
                              &it->rpc_def_,
                              &it->dup_table_rpc_def_,
                              &it->location_adapter_def_,
                              &it->gti_source_def_,
                              &OB_TS_MGR,
                              rpc_proxy,
                              schema_service,
                              server_tracer))) {
    TRANS_LOG(ERROR, "trans-service init error", KR(ret), KPC(it));
  }
  return ret;
}

int ObTransService::init(const ObAddr &self,
                         ObITransRpc *rpc,
                         ObIDupTableRpc *dup_table_rpc,
                         ObILocationAdapter *location_adapter,
                         ObIGtiSource *gti_source,
                         ObITsMgr *ts_mgr,
                         obrpc::ObSrvRpcProxy *rpc_proxy,
                         share::schema::ObMultiVersionSchemaService *schema_service,
                         share::ObAliveServerTracer *server_tracer)
{
  int ret = OB_SUCCESS;
  ObSimpleThreadPool::set_run_wrapper(MTL_CTX());
  const int64_t tenant_id = MTL_ID();
  const int64_t tenant_memory_limit = lib::get_tenant_memory_limit(tenant_id);
  int64_t msg_task_cnt = MSG_TASK_CNT_PER_GB * (tenant_memory_limit / (1024 * 1024 * 1024));
  if (msg_task_cnt < MSG_TASK_CNT_PER_GB) {
    msg_task_cnt = MSG_TASK_CNT_PER_GB;
  }
  if (msg_task_cnt > MAX_MSG_TASK_CNT) {
    msg_task_cnt = MAX_MSG_TASK_CNT;
  }
  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransService inited twice", KPC(this));
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(!self.is_valid())
             || OB_ISNULL(rpc)
             || OB_ISNULL(dup_table_rpc)
             || OB_ISNULL(location_adapter)
             || OB_ISNULL(gti_source)
             || OB_ISNULL(ts_mgr)
             || OB_ISNULL(rpc_proxy)
             || OB_ISNULL(schema_service)
             || OB_ISNULL(server_tracer)) {
    TRANS_LOG(WARN, "invalid argument", K(self),
              KP(location_adapter), KP(rpc), K(dup_table_rpc),
              KP(location_adapter), KP(ts_mgr),
              KP(rpc_proxy), KP(schema_service), KP(server_tracer));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(timer_.init("TransTimeWheel"))) {
    TRANS_LOG(ERROR, "timer init error", KR(ret));
  } else if (OB_FAIL(dup_table_scan_timer_.init())) {
    TRANS_LOG(ERROR, "dup table scan timer init error", K(ret));
  } else if (OB_FAIL(ObSimpleThreadPool::init(2, msg_task_cnt, "TransService", tenant_id))) {
    TRANS_LOG(WARN, "thread pool init error", KR(ret), K(msg_task_cnt));
  } else if (OB_FAIL(tx_desc_mgr_.init(std::bind(&ObTransService::gen_trans_id,
                                                 this, std::placeholders::_1),
                                       lib::ObMemAttr(tenant_id, "TxDescMgr")))) {
    TRANS_LOG(WARN, "ObTxDescMgr init error", K(ret));
  } else if (OB_FAIL(tx_ctx_mgr_.init(tenant_id, ts_mgr, this))) {
    TRANS_LOG(WARN, "tx_ctx_mgr_ init error", KR(ret));
  } else if (OB_FAIL(dup_table_loop_worker_.init())) {
    TRANS_LOG(WARN, "init dup table loop worker failed", K(ret));
  } else if (OB_FAIL(dup_tablet_scan_task_.make(tenant_id,
                                                &dup_table_scan_timer_,
                                                &dup_table_loop_worker_))) {
    TRANS_LOG(WARN, "init dup_tablet_scan_task_ failed",K(ret));
  } else {
    self_ = self;
    tenant_id_ = tenant_id;
    gti_source_ = gti_source;
    rpc_ = rpc;
    dup_table_rpc_ = dup_table_rpc;
    location_adapter_ = location_adapter;
    rpc_proxy_ = rpc_proxy;
    schema_service_ = schema_service;
    ts_mgr_ = ts_mgr;
    server_tracer_ = server_tracer;
    is_inited_ = true;
    TRANS_LOG(INFO, "transaction service inited success", KP(this), K(tenant_memory_limit));
  }
  if (OB_SUCC(ret)) {
#ifdef ENABLE_DEBUG_LOG
    void *p = NULL;
    if (!GCONF.enable_defensive_check()) {
      // do nothing
    } else if (NULL == (p = ob_malloc(sizeof(ObDefensiveCheckMgr),
                                      lib::ObMemAttr(tenant_id, "ObDefenCheckMgr")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "memory alloc failed", KR(ret));
    } else {
      defensive_check_mgr_ = new(p) ObDefensiveCheckMgr();
      if (OB_FAIL(defensive_check_mgr_->init(lib::ObMemAttr(tenant_id, "ObDefenCheckMgr")))) {
        TRANS_LOG(ERROR, "defensive check mgr init failed", K(ret), KP(defensive_check_mgr_));
        defensive_check_mgr_->destroy();
        ob_free(defensive_check_mgr_);
        defensive_check_mgr_ = NULL;
      } else {
        // do nothing
      }
    }
#endif
  } else {
    TRANS_LOG(WARN, "transaction service inited failed", K(ret), K(tenant_memory_limit));
  }
  return ret;
}

int ObTransService::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_running_)) {
    TRANS_LOG(WARN, "ObTransService is already running");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(timer_.start())) {
    TRANS_LOG(WARN, "ObTransTimer start error", K(ret));
  } else if (OB_FAIL(dup_table_scan_timer_.start())) {
    TRANS_LOG(WARN, "dup_table_scan_timer_ start error", K(ret));
  } else if (OB_FAIL(dup_table_scan_timer_.register_timeout_task(
                     dup_tablet_scan_task_,
                     ObDupTabletScanTask::DUP_TABLET_SCAN_INTERVAL))) {
    TRANS_LOG(WARN, "register dup table scan task error", K(ret));
  } else if (OB_FAIL(rpc_->start())) {
    TRANS_LOG(WARN, "ObTransRpc start error", KR(ret));
  // } else if (OB_FAIL(dup_table_rpc_->start())) {
  //   TRANS_LOG(WARN, "ObDupTableRpc start error", K(ret));
  } else if (OB_FAIL(gti_source_->start())) {
    TRANS_LOG(WARN, "ObGtiSource start error", KR(ret));
  } else if (OB_FAIL(tx_ctx_mgr_.start())) {
    TRANS_LOG(WARN, "tx_ctx_mgr_ start error", KR(ret));
  } else if (OB_FAIL(tx_desc_mgr_.start())) {
    TRANS_LOG(WARN, "tx_desc_mgr_ start error", KR(ret));
  } else {
    is_running_ = true;

    TRANS_LOG(INFO, "transaction service start success", KPC(this));
  }

  return ret;
}

void ObTransService::stop()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited", KPC(this));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService already has stopped", KPC(this));
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(tx_ctx_mgr_.stop())) {
    TRANS_LOG(WARN, "tx_ctx_mgr_ stop error", KR(ret));
  } else if (OB_FAIL(tx_desc_mgr_.stop())) {
    TRANS_LOG(WARN, "tx_desc_mgr stop error", KR(ret));
  } else if (OB_FAIL(timer_.stop())) {
    TRANS_LOG(WARN, "ObTransTimer stop error", K(ret));
  } else if (OB_FAIL(dup_table_scan_timer_.stop())) {
    TRANS_LOG(WARN, "dup_table_scan_timer_ stop error", K(ret));
  } else {
    rpc_->stop();
    dup_table_rpc_->stop();
    gti_source_->stop();
    dup_table_loop_worker_.stop();
    ObSimpleThreadPool::stop();
    is_running_ = false;
    TRANS_LOG(INFO, "transaction service stop success", KPC(this));
  }
}

int ObTransService::wait_()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited", KPC(this));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_running_)) {
    TRANS_LOG(WARN, "ObTransService is running");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(tx_ctx_mgr_.wait())) {
    TRANS_LOG(WARN, "tx_ctx_mgr_ wait error", KR(ret));
  } else if (OB_FAIL(tx_desc_mgr_.wait())) {
  TRANS_LOG(WARN, "tx_desc_mgr_ wait error", KR(ret));
  } else if (OB_FAIL(timer_.wait())) {
    TRANS_LOG(WARN, "ObTransTimer wait error", K(ret));
  } else if (OB_FAIL(dup_table_scan_timer_.wait())) {
    TRANS_LOG(WARN, "dup_table_scan_timer_ wait error", K(ret));
  } else {
    rpc_->wait();
    // dup_table_rpc_->wait();
    gti_source_->wait();
    dup_table_loop_worker_.wait();
    TRANS_LOG(INFO, "transaction service wait success", KPC(this));
  }
  return ret;
}

void ObTransService::destroy()
{
  if (is_inited_) {
    if (is_running_) {
      stop();
      wait();
    }
    timer_.destroy();
    dup_table_scan_timer_.destroy();
    dup_tablet_scan_task_.destroy();
    dup_table_loop_worker_.destroy();
    if (use_def_) {
      rpc_->destroy();
      location_adapter_->destroy();
      use_def_ = false;
    }
    gti_source_->destroy();
    tx_ctx_mgr_.destroy();
    tx_desc_mgr_.destroy();
    dup_table_rpc_->destroy();
#ifdef ENABLE_DEBUG_LOG
    if (NULL != defensive_check_mgr_) {
      defensive_check_mgr_->destroy();
      ob_free(defensive_check_mgr_);
      defensive_check_mgr_ = NULL;
    }
#endif
    is_inited_ = false;
    TRANS_LOG(INFO, "transaction service destroyed", KPC(this));
  }
}

int ObTransService::get_gts_(
    SCN &snapshot_version,
    MonotonicTs &receive_gts_ts,
    const int64_t trans_expired_time,
    const int64_t stmt_expire_time,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // FIXME: its better to use transaction start time
  // and only get gts once during transaction lifecyle
  const MonotonicTs request_ts = MonotonicTs::current_time();
  const int64_t WAIT_GTS_US = 500;
  SCN gts;
  MonotonicTs tmp_receive_gts_ts;
  if (OB_ISNULL(ts_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid ts_mgr", KR(ret), K(ts_mgr_));
  } else {
    const int64_t GET_GTS_AHEAD_INTERVAL = GCONF._ob_get_gts_ahead_interval;
    const MonotonicTs stc_ahead = (request_ts - MonotonicTs(GET_GTS_AHEAD_INTERVAL));
    do {
      if (ObClockGenerator::getClock() >= trans_expired_time) {
        ret = OB_TRANS_TIMEOUT;
      } else if (ObClockGenerator::getClock() >= stmt_expire_time) {
        ret = OB_TRANS_STMT_TIMEOUT;
      } else if (OB_FAIL(ts_mgr_->get_gts(tenant_id, stc_ahead, NULL, gts, tmp_receive_gts_ts))) {
        if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "get gts failed", KR(ret));
        } else {
          ob_usleep(WAIT_GTS_US);
        }
      } else if (!gts.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "invalid gts, unexpected error", KR(ret), K(gts));
      } else {
        const SCN max_commit_ts = tx_version_mgr_.get_max_commit_ts(true);
        snapshot_version = SCN::max(max_commit_ts, gts);
        receive_gts_ts = tmp_receive_gts_ts;
      }
    } while (OB_EAGAIN == ret);
  }
  return ret;
}

int ObTransService::iterate_trans_memory_stat(ObTransMemStatIterator &mem_stat_iter)
{
  int ret = OB_SUCCESS;
  ObTransMemoryStat rpc_mem_stat;
  ObTransMemoryStat log_mutator_mem_stat;
  ObTransMemoryStat version_token_mem_stat;
  ObTransMemoryStat clog_buf_mem_stat;
  ObTransMemoryStat trans_ctx_mem_stat;
  ObTransMemoryStat tx_ctx_mgr_mem_stat;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
    /*
  } else if (OB_FAIL(rpc_mem_stat.init(self_, TransRpcTaskFactory::get_mod_type(),
      TransRpcTaskFactory::get_alloc_count(), TransRpcTaskFactory::get_release_count()))) {
    TRANS_LOG(WARN, "rpc memory stat init error", KR(ret), "alloc_count",
      TransRpcTaskFactory::get_alloc_count(), "release_count", TransRpcTaskFactory::get_release_count());
    */
  } else if (OB_FAIL(mem_stat_iter.push(rpc_mem_stat))) {
    TRANS_LOG(WARN, "rpc memory stat push error", KR(ret));
  } else if (OB_FAIL(log_mutator_mem_stat.init(self_, MutatorBufFactory::get_mod_type(),
      MutatorBufFactory::get_alloc_count(), MutatorBufFactory::get_release_count()))) {
    TRANS_LOG(WARN, "log mutator memory stat init error", KR(ret), "alloc_count",
        MutatorBufFactory::get_alloc_count(), "release_count", MutatorBufFactory::get_release_count());
  } else if (OB_FAIL(mem_stat_iter.push(log_mutator_mem_stat))) {
    TRANS_LOG(WARN, "log mutator memory stat push error", KR(ret));
  } else if (OB_FAIL(mem_stat_iter.push(version_token_mem_stat))) {
    TRANS_LOG(WARN, "version token memory stat push error", KR(ret));
  } else if (OB_FAIL(clog_buf_mem_stat.init(self_, ClogBufFactory::get_mod_type(),
      ClogBufFactory::get_alloc_count(), ClogBufFactory::get_release_count()))) {
    TRANS_LOG(WARN, "clog buf memory stat init error", KR(ret), "alloc_count",
        ClogBufFactory::get_alloc_count(), "release_count", ClogBufFactory::get_release_count());
  } else if (OB_FAIL(mem_stat_iter.push(clog_buf_mem_stat))) {
    TRANS_LOG(WARN, "clog buf memory stat push error", KR(ret));
  } else if (OB_FAIL(trans_ctx_mem_stat.init(self_, ObTransCtxFactory::get_mod_type(),
      ObTransCtxFactory::get_alloc_count(), ObTransCtxFactory::get_release_count()))) {
    TRANS_LOG(WARN, "transaction context memory stat init error", KR(ret), "alloc_count",
        ObTransCtxFactory::get_alloc_count(), "release_count", ObTransCtxFactory::get_release_count());
  } else if (OB_FAIL(mem_stat_iter.push(trans_ctx_mem_stat))) {
    TRANS_LOG(WARN, "transaction context memory stat push error", KR(ret));
  } else if (OB_FAIL(tx_ctx_mgr_mem_stat.init(self_, ObLSTxCtxMgrFactory::get_mod_type(),
      ObLSTxCtxMgrFactory::get_alloc_count(), ObLSTxCtxMgrFactory::get_release_count()))) {
    TRANS_LOG(WARN, "ls context mananger memory stat init error", KR(ret), "alloc_count",
        ObLSTxCtxMgrFactory::get_alloc_count(), "release_count", ObLSTxCtxMgrFactory::get_release_count());
  } else if (OB_FAIL(mem_stat_iter.push(tx_ctx_mgr_mem_stat))) {
    TRANS_LOG(WARN, "partition context mananger memory stat push error", KR(ret));
  } else if (OB_FAIL(mem_stat_iter.set_ready())) {
    TRANS_LOG(WARN, "transaction factory statistics set ready error", KR(ret));
  } else {
    // do nothing
  }

  return ret;
}


int ObTransService::end_1pc_trans(ObTxDesc &trans_desc,
                                  ObITxCallback *endTransCb,
                                  const bool is_rollback,
                                  const int64_t expire_ts)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!trans_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(trans_desc));
  } else if (is_rollback) {
    interrupt(trans_desc, ObTxAbortCause::EXPLICIT_ROLLBACK);
    if (OB_FAIL(rollback_tx(trans_desc))) {
      TRANS_LOG(WARN, "rollback 1pc trans fail", KR(ret));
    }
  } else if (OB_FAIL(submit_commit_tx(trans_desc, expire_ts, *endTransCb))) {
      TRANS_LOG(WARN, "1pc end trans failed", KR(ret));
  }
  TRANS_LOG(INFO, "end 1pc trans", KR(ret), K(trans_desc.tid()));

  return ret;
}

void ObTransService::check_env_()
{
  // do nothing now
}

int ObTransService::push(void *task)
{
  ATOMIC_FAA(&input_queue_count_, 1);
  return ObSimpleThreadPool::push(task);
}

void ObTransService::handle(void *task)
{
  int ret = OB_SUCCESS;
  ObTransTask *trans_task = NULL;
  bool need_release_task = true;
  ATOMIC_FAA(&output_queue_count_, 1);

  if (OB_ISNULL(task)) {
    TRANS_LOG(ERROR, "task is null", KP(task));
  } else {
    trans_task = static_cast<ObTransTask*>(task);
    if (!trans_task->ready_to_handle()) {
      if (OB_FAIL(push(trans_task))) {
        TRANS_LOG(WARN, "transaction service push task error", KR(ret), K(*trans_task));
        //TransRpcTaskFactory::release(static_cast<TransRpcTask*>(trans_task));
      }
    } else if (ObTransRetryTaskType::END_TRANS_CB_TASK == trans_task->get_task_type()) {
      bool has_cb = false;
      ObTxCommitCallbackTask *commit_cb_task = static_cast<ObTxCommitCallbackTask*>(task);
      int64_t need_wait_us = commit_cb_task->get_need_wait_us();
      if (need_wait_us > 0) {
        ob_usleep(need_wait_us);
      }
      if (OB_FAIL(commit_cb_task->callback(has_cb))) {
        TRANS_LOG(WARN, "end trans cb task callback error", KR(ret), KPC(commit_cb_task));
      }
      if (has_cb) {
        ObTxCommitCallbackTaskFactory::release(commit_cb_task);
      } else if (OB_FAIL(push(commit_cb_task))) {
        TRANS_LOG(WARN, "transaction service push task error", KR(ret), KPC(commit_cb_task));
      } else {
        // do nothing
      }
    } else if (ObTransRetryTaskType::ADVANCE_LS_CKPT_TASK == trans_task->get_task_type()) {
      ObAdvanceLSCkptTask *advance_ckpt_task = static_cast<ObAdvanceLSCkptTask *>(trans_task);
      if (OB_ISNULL(advance_ckpt_task)) {
        TRANS_LOG(WARN, "advance ckpt task is null", KP(advance_ckpt_task));
      } else if (OB_FAIL(advance_ckpt_task->try_advance_ls_ckpt_ts())) {
        TRANS_LOG(WARN, "advance ls ckpt ts failed", K(ret));
      }

      if (OB_NOT_NULL(advance_ckpt_task)) {
        mtl_free(advance_ckpt_task);
        advance_ckpt_task = nullptr;
      }
    } else if (ObTransRetryTaskType::STANDBY_CLEANUP_TASK == trans_task->get_task_type()) {
      ObTxStandbyCleanupTask *standby_cleanup_task = static_cast<ObTxStandbyCleanupTask *>(trans_task);
      if (OB_ISNULL(standby_cleanup_task)) {
        TRANS_LOG(WARN, "standby cleanup task is null");
      } else if (OB_FAIL(do_standby_cleanup())) {
        TRANS_LOG(WARN, "do standby cleanup failed", K(ret));
      }
      if (OB_NOT_NULL(standby_cleanup_task)) {
        mtl_free(standby_cleanup_task);
        standby_cleanup_task = nullptr;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected trans task type!!!", KR(ret), K(*trans_task));
    }

    // print task queue status periodically
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      int64_t queue_num = get_queue_num();
      TRANS_LOG(INFO, "[statisic] trans service task queue statisic : ", K(queue_num), K_(input_queue_count), K_(output_queue_count));
      ATOMIC_STORE(&input_queue_count_, 0);
      ATOMIC_STORE(&output_queue_count_, 0);
      TRANS_LOG(INFO, "[statisic] tx desc statisic : ",
                "alloc_count", tx_desc_mgr_.get_alloc_count(),
                "total_count", tx_desc_mgr_.get_total_count());
    }
  }
  UNUSED(ret); //make compiler happy
}

int ObTransService::get_ls_min_uncommit_prepare_version(const ObLSID &ls_id, SCN &min_prepare_version)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!ls_id.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(ls_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(tx_ctx_mgr_.get_ls_min_uncommit_tx_prepare_version(ls_id, min_prepare_version))) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr set memstore version error", KR(ret), K(ls_id));
  } else if (!min_prepare_version.is_valid()) {
    TRANS_LOG(ERROR, "invalid min prepare version, unexpected error", K(ls_id), K(min_prepare_version));
    ret = OB_ERR_UNEXPECTED;
  } else {
    TRANS_LOG(DEBUG, "get min uncommit prepare version success", K(ls_id), K(min_prepare_version));
  }
  return ret;
}

int ObTransService::get_min_undecided_log_ts(const ObLSID &ls_id, SCN &log_ts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!ls_id.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(ls_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(tx_ctx_mgr_.get_min_undecided_scn(ls_id, log_ts))) {
    TRANS_LOG(WARN, "get min undecided log ts failed", KR(ret), K(ls_id), K(log_ts));
  }
  return ret;
}

int ObTransService::get_max_decided_scn(const share::ObLSID &ls_id, share::SCN &scn)
{
  int ret = OB_SUCCESS;

  ObLSTxCtxMgr *ls_tx_mgr_ptr = nullptr;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!ls_id.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(ls_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(tx_ctx_mgr_.get_ls_tx_ctx_mgr(ls_id, ls_tx_mgr_ptr))) {
    TRANS_LOG(WARN, "get ls tx ctx mgr failed", K(ret));
  } else {
    if (OB_FAIL(ls_tx_mgr_ptr->get_max_decided_scn(scn))) {
    TRANS_LOG(WARN, "get max decided scn failed", K(ret));
    }
    tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_mgr_ptr);
  }
  return ret;
}

int ObTransService::check_dup_table_lease_valid(const ObLSID ls_id,
                                                bool &is_dup_ls,
                                                bool &is_lease_valid)
{
  int ret = OB_SUCCESS;

  ObLSHandle ls_handle;
  is_dup_ls = false;
  is_lease_valid = false;

  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(ls_id));
  } else if (!dup_table_loop_worker_.is_useful_dup_ls(ls_id)) {
    is_dup_ls = false;
    ret = OB_SUCCESS;
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD))) {
    is_dup_ls = false;
    TRANS_LOG(WARN, "get ls failed", K(ret), K(ls_id), K(ls_handle));
  } else if (!ls_handle.is_valid()) {
    is_dup_ls = false;
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid ls handle", K(ret), K(ls_id), K(ls_handle));
  } else if (ls_handle.get_ls()->get_dup_table_ls_handler()->is_dup_table_lease_valid()) {
    is_dup_ls = true;
    is_lease_valid = true;
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObTransService::handle_redo_sync_task_(ObDupTableRedoSyncTask *task, bool &need_release_task)
{
  UNUSED(task);
  UNUSED(need_release_task);
  return OB_NOT_SUPPORTED;
}

int ObTransService::remove_callback_for_uncommited_txn(
  const ObLSID ls_id, const memtable::ObMemtableSet *memtable_set)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
  } else if (OB_ISNULL(memtable_set)) {
    TRANS_LOG(WARN, "memtable is NULL");
    ret = OB_INVALID_ARGUMENT;
  } else if (!ls_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected ls id", KR(ret), K(ls_id), KPC(memtable_set));
  } else if (OB_FAIL(tx_ctx_mgr_.remove_callback_for_uncommited_tx(ls_id, memtable_set))) {
    TRANS_LOG(WARN, "participant remove callback for uncommitt txn failed", KR(ret), K(ls_id), KP(memtable_set));
  } else {
    TRANS_LOG(DEBUG, "participant remove callback for uncommitt txn success", K(ls_id), KP(memtable_set));
  }

  return ret;
}

/**
 * get snapshot_version for stmt
 *
 * NOTE: this function only handle *strong-consistency* read
 * @pkey : not NULL if this is a single local partition stmt,
 *         will try local publish version
 */
int ObTransService::get_weak_read_snapshot(const uint64_t tenant_id, SCN &snapshot_version)
{
  int ret = OB_SUCCESS;
  ObIWeakReadService *wrs = GCTX.weak_read_service_;
  if (OB_ISNULL(wrs)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "weak read service is invalid", K(ret), KP(wrs));
  } else {
    ret = wrs->get_cluster_version(tenant_id, snapshot_version);
  }
  return ret;
}

/*
 * get snapshot for CURRENT_READ consistency
 * @snapshot_epoch: for pkey != NULL, need record snapshot's epoch
 *                  and validate at get_store_ctx
 */

int ObTransService::dump_elr_statistic()
{
  int ret = OB_NOT_SUPPORTED;
  return ret;
}


// only for ob_admin use
int ObTransService::handle_part_trans_ctx(const ObTrxToolArg &arg, ObTrxToolRes &res)
{
  UNUSED(arg);
  UNUSED(res);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObTransService::check_dup_table_ls_readable()
{
  int ret = OB_NOT_MASTER;

  return ret;
}

int ObTransService::check_dup_table_tablet_readable()
{
  int ret = OB_NOT_MASTER;

  return ret;
}

int ObTransService::register_mds_into_tx(ObTxDesc &tx_desc,
                                         const ObLSID &ls_id,
                                         const ObTxDataSourceType &type,
                                         const char *buf,
                                         const int64_t buf_len,
                                         const int64_t request_id,
                                         const ObRegisterMdsFlag &register_flag)
{
  const int64_t MAX_RETRY_CNT = 5;
  const int64_t RETRY_INTERVAL = 400 * 1000;

  ObTimeGuard time_guard("register mds", 1 * 1000 * 1000);

  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObString str;
  obrpc::ObRegisterTxDataArg arg;
  obrpc::ObRegisterTxDataResult result;
  ObAddr ls_leader_addr;
  str.assign_ptr(buf, buf_len);
  int64_t local_retry_cnt = 0;
  int64_t retry_cnt = 0;
  ObTxExecResult tx_result;
  ObTxParam tx_param;
  tx_param.cluster_id_ = tx_desc.cluster_id_;
  tx_param.access_mode_ = tx_desc.access_mode_;
  tx_param.isolation_ = tx_desc.isolation_;
  tx_param.timeout_us_ = tx_desc.timeout_us_;
  ObTxSEQ savepoint;
  if (OB_UNLIKELY(!tx_desc.is_valid() || !ls_id.is_valid() || type <= ObTxDataSourceType::UNKNOWN
                  || type >= ObTxDataSourceType::MAX_TYPE || OB_ISNULL(buf) || buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tx_desc), K(ls_id), K(type), KP(buf),
              K(buf_len));
  } else if (!tx_desc.is_tx_active()) {
    ret = OB_TRANS_IS_EXITING;
    TRANS_LOG(WARN, "txn must in active for register", K(ret));
  } else if (OB_ISNULL(rpc_proxy_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "rpc proxy not inited", KR(ret), K(tx_desc), K(ls_id), K(type));
  } else if (OB_FAIL(arg.init(tx_desc.tenant_id_, tx_desc, ls_id, type, str, request_id,
                              register_flag))) {
    TRANS_LOG(WARN, "rpc arg init failed", KR(ret), K(tx_desc), K(ls_id), K(type));
  } else if (OB_FAIL(create_implicit_savepoint(tx_desc, tx_param, savepoint))) {
    TRANS_LOG(WARN, "create implicit savepoint failed", K(ret), K(tx_desc));
  } else {
    time_guard.click("start register");
    do {
      result.reset();
      tx_result.reset();
      if (OB_NOT_MASTER == ret) {
        ob_usleep(RETRY_INTERVAL);
        retry_cnt += 1;
      }

      if (ObTimeUtil::current_time() >= tx_desc.expire_ts_) {
        ret = OB_TIMEOUT;
        TRANS_LOG(WARN, "register tx data timeout", KR(ret), K(tx_desc), K(ls_id), K(type),
                  K(retry_cnt));
      } else if (OB_ISNULL(location_adapter_)
                 || OB_FAIL(location_adapter_->nonblock_get_leader(
                     tx_desc.cluster_id_, tx_desc.tenant_id_, ls_id, ls_leader_addr))) {
        TRANS_LOG(WARN, "get leader failed", KR(ret), K(ls_id));
      } else if (ls_leader_addr == self_) {
        local_retry_cnt = 0;

        time_guard.click("register in ctx begin");
        do {
          if (OB_FAIL(register_mds_into_ctx_(*(arg.tx_desc_), ls_id, type, buf, buf_len, register_flag))) {
            TRANS_LOG(WARN, "register msd into ctx failed", K(ret));
            if (OB_EAGAIN == ret) {
              if (ObTimeUtil::current_time() >= tx_desc.expire_ts_) {
                ret = OB_TIMEOUT;
                TRANS_LOG(WARN, "register tx data timeout in this participant", KR(ret), K(tx_desc),
                          K(ls_id), K(type), K(retry_cnt), K(local_retry_cnt));
              } else if (local_retry_cnt > MAX_RETRY_CNT) {
                ret = OB_NOT_MASTER;
                TRANS_LOG(WARN, "local retry too many times, need retry by the scheduler", K(ret),
                          K(local_retry_cnt), K(retry_cnt));
              } else {
                local_retry_cnt++;
              }
            }
          }
        } while (OB_EAGAIN == ret);
        time_guard.click("register in ctx end");

        // collect participants regardless of register error
        if (OB_TMP_FAIL(collect_tx_exec_result(*(arg.tx_desc_), result.tx_result_))) {
          TRANS_LOG(WARN, "collect tx exec result failed", K(ret));
        } else if (OB_TMP_FAIL(tx_result.merge_result(result.tx_result_))) {
          TRANS_LOG(WARN, "merge tx result failed", K(ret), K(result));
        }

        if (OB_TMP_FAIL(tmp_ret)) {
          if (OB_SUCC(ret)) {
            ret = tmp_ret;
          }
          TRANS_LOG(WARN, "set exec result failed when register mds", K(ret), K(tmp_ret),
                    K(*(arg.tx_desc_)));
        }
      } else if (this->self_ != tx_desc.addr_) {
        ret = OB_NOT_MASTER;
        TRANS_LOG(INFO,
                  "The follower receive a register request. we will return err_code to scheduler",
                  K(ret), K(tx_desc), K(ls_id), K(type), K(buf_len), K(request_id));
      } else if (OB_FALSE_IT(arg.inc_request_id(-1))) {
      } else if (OB_FALSE_IT(time_guard.click("register by rpc begin"))) {
      } else if (OB_FAIL(rpc_proxy_->to(ls_leader_addr)
                             .by(tx_desc.tenant_id_)
                             .timeout(tx_desc.expire_ts_)
                             .register_tx_data(arg, result))) {
        TRANS_LOG(WARN, "register_tx_fata failed", KR(ret), K(ls_leader_addr), K(arg), K(tx_desc),
                  K(ls_id), K(result));
        time_guard.click("register by rpc end");
      } else if (OB_FALSE_IT(time_guard.click("register by rpc end"))) {
      } else if (OB_FAIL(result.result_)) {
        TRANS_LOG(WARN, "register tx data failed in remote", KR(ret), K(tx_desc), K(ls_id),
                  K(type));
      } else if (OB_FAIL(tx_result.merge_result(result.tx_result_))) {
        TRANS_LOG(WARN, "merge tx result failed", KR(ret), K(result));
      }

      if (OB_NOT_MASTER == ret) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS
            != (tmp_ret = location_adapter_->nonblock_renew(tx_desc.cluster_id_, tx_desc.tenant_id_,
                                                            ls_id))) {
          TRANS_LOG(WARN, "refresh location cache failed", KR(tmp_ret), K(tx_desc), K(ls_id),
                    K(type));
        }
      }
    } while (OB_NOT_MASTER == ret && this->self_ == tx_desc.addr_);

    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_tx_exec_result(tx_desc, tx_result))) {
        TRANS_LOG(WARN, "add tx exec result failed", K(ret), K(tx_desc), K(tx_result));
      }
    }
    if (OB_FAIL(ret) && OB_NOT_MASTER != ret) {
      share::ObLSArray ls_list;
      ls_list.push_back(ls_id);
      int tmp_ret = OB_SUCCESS;
      int64_t expire_ts = tx_desc.expire_ts_; // FIXME: use a rational expire_ts
      if (OB_TMP_FAIL(rollback_to_implicit_savepoint(tx_desc, savepoint, expire_ts, &ls_list))) {
        TRANS_LOG(WARN, "rollback to savepoint fail", K(tmp_ret), K(savepoint), K(expire_ts));
      }
    }
  }

  TRANS_LOG(INFO, "register multi data source result", KR(ret), K(arg), K(result), K(tx_desc),
            K(local_retry_cnt), K(retry_cnt), K(request_id), K(time_guard));
  return ret;
}

int ObTransService::register_mds_into_ctx_(ObTxDesc &tx_desc,
                                           const ObLSID &ls_id,
                                           const ObTxDataSourceType &type,
                                           const char *buf,
                                           const int64_t buf_len,
                                           const ObRegisterMdsFlag &register_flag)
{
  int ret = OB_SUCCESS;
  ObStoreCtx store_ctx;
  ObTxReadSnapshot snapshot;
  snapshot.init_none_read();
  concurrent_control::ObWriteFlag write_flag;
  write_flag.set_is_mds();
  if (OB_UNLIKELY(!tx_desc.is_valid() || !ls_id.is_valid() || OB_ISNULL(buf) || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tx_desc), K(ls_id), KP(buf), K(buf_len));
  } else if (FALSE_IT(store_ctx.ls_id_ = ls_id)) {
  } else if (OB_FAIL(get_write_store_ctx(tx_desc, snapshot, write_flag, store_ctx, ObTxSEQ::INVL(), true))) {
    TRANS_LOG(WARN, "get store ctx failed", KR(ret), K(tx_desc), K(ls_id));
  } else {
    ObPartTransCtx *ctx = store_ctx.mvcc_acc_ctx_.tx_ctx_;
    if (OB_ISNULL(ctx)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected null ptr", KR(ret), K(tx_desc), K(ls_id), K(type));
    } else if (OB_FAIL(ctx->register_multi_data_source(type, buf, buf_len, false /*try lock*/, register_flag))) {
      TRANS_LOG(WARN, "register multi source data failed", KR(ret), K(tx_desc), K(ls_id), K(type), K(register_flag));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = revert_store_ctx(store_ctx))) {
      TRANS_LOG(WARN, "revert store ctx failed", KR(tmp_ret), K(tx_desc), K(ls_id), K(type));
    }
  }
  TRANS_LOG(DEBUG, "register multi source data on participant", KR(ret), K(tx_desc), K(ls_id),
            K(type));
  return ret;
}

int ObTransService::get_max_commit_version(SCN &commit_version) const
{
 int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else {
    commit_version = tx_version_mgr_.get_max_commit_ts(false);
    TRANS_LOG(DEBUG, "get publish version success", K(commit_version));
  }
  return ret;
}
} // transaction
} // oceanbase
