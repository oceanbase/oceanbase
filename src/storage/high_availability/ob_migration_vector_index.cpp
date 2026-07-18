/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_migration_vector_index.h"

#include "ob_migration_sliding_window_controller.h"
#include "ob_migration_tenant_window_mgr.h"
#include "ob_migration_vector_index_processor.h"
#include "ob_storage_ha_dag.h"
#include "ob_storage_ha_service.h"
#include "ob_storage_ha_utils.h"

#include "storage/ob_storage_rpc.h"
#include "storage/tx_storage/ob_ls_service.h"

#include "share/ob_debug_sync.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "share/vector_index/ob_plugin_vector_index_scheduler.h"
#include "share/vector_index/ob_plugin_vector_index_serialize.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/vector_index/ob_vector_index_segment.h"
#include "share/vector_index/ob_vector_index_util.h"

#include "observer/ob_server.h"
#include "observer/ob_server_event_history_table_operator.h"

#include "lib/container/ob_array.h"
#include "lib/stat/ob_latch_define.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/vector/ob_vsag_adaptor.h"
#include "share/ob_cluster_version.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;

// ======================== ObVecIdxFetchRpcCtx ========================

ObVecIdxFetchRpcCtx::ObVecIdxFetchRpcCtx()
  : rpc_proxy_(nullptr),
    src_addr_(),
    cluster_id_(0),
    tenant_id_(OB_INVALID_ID),
    ls_id_(),
    processor_id_(-1),
    rpc_timeout_us_(0),
    ls_rebuild_seq_(-1)
{
}

void ObVecIdxFetchRpcCtx::reset()
{
  rpc_proxy_ = nullptr;
  src_addr_.reset();
  cluster_id_ = 0;
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  processor_id_ = -1;
  rpc_timeout_us_ = 0;
  ls_rebuild_seq_ = -1;
}

bool ObVecIdxFetchRpcCtx::is_valid() const
{
  return OB_NOT_NULL(rpc_proxy_)
         && src_addr_.is_valid()
         && ls_id_.is_valid()
         && OB_INVALID_ID != tenant_id_
         && rpc_timeout_us_ > 0;
}

ERRSIM_POINT_DEF(EN_VEC_INDEX_MIGRATION_FETCH_SEGMENT_DATA_CALLBACK_FAILED);
// ======================== ObFetchVecIdxSegDataCB ========================
// Async RPC callback. Holds the driver alive via a refcounted handle (which
// transitively keeps the dest controller alive through driver_->dest_controller_handle_).
class ObFetchVecIdxSegDataCB
    : public obrpc::ObStorageRpcProxy::AsyncCB<obrpc::OB_HA_FETCH_VECTOR_INDEX_MIGRATION_SEGMENT_DATA>
{
  using BaseCB = obrpc::ObStorageRpcProxy::AsyncCB<
      obrpc::OB_HA_FETCH_VECTOR_INDEX_MIGRATION_SEGMENT_DATA>;
public:
  ObFetchVecIdxSegDataCB()
      : driver_handle_(), seq_idx_(-1) {}

  ~ObFetchVecIdxSegDataCB() = default;

  void set_args(const BaseCB::Request &args) override {
    seq_idx_ = args.seq_idx_;
  }

  rpc::frame::ObReqTransport::AsyncCB *clone(
      const rpc::frame::SPAlloc &alloc) const override {
    void *buf = alloc(sizeof(*this));
    ObFetchVecIdxSegDataCB *new_cb = nullptr;
    if (OB_NOT_NULL(buf)) {
      new_cb = new (buf) ObFetchVecIdxSegDataCB();
      new_cb->driver_handle_ = driver_handle_;  // copy: ref += 1
      new_cb->seq_idx_ = seq_idx_;
    }
    return new_cb;
  }

  int process() override {
    int ret = OB_SUCCESS;
#ifdef ERRSIM
    int32_t errsim_ret = OB_SUCCESS;
    if (OB_SUCCESS != (errsim_ret = EN_VEC_INDEX_MIGRATION_FETCH_SEGMENT_DATA_CALLBACK_FAILED)) {
      rcode_.rcode_ = errsim_ret;
      LOG_INFO("[MIG VEC] errsim inject fetch segment data callback failure",
          K(seq_idx_), "rcode", rcode_.rcode_);
      if (driver_handle_.is_valid()) {
        const ObVecIdxFetchRpcCtx &rpc_ctx = driver_handle_->get_rpc_ctx();
        SERVER_EVENT_ADD("storage_ha", "vec_mig_errsim_fetch_segment_data_callback_failed",
                         "tenant_id", rpc_ctx.tenant_id_,
                         "ls_id", rpc_ctx.ls_id_.id(),
                         "processor_id", rpc_ctx.processor_id_,
                         "seq_idx", seq_idx_,
                         "ret", errsim_ret);
      }
    }
#endif
    if (!driver_handle_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN_RET(ret, "driver handle is invalid in async cb");
    } else if (OB_EAGAIN == rcode_.rcode_) {
      if (OB_FAIL(driver_handle_->enqueue_retry(seq_idx_))) {
        LOG_WARN("enqueue retry failed in async cb", K(ret), K(seq_idx_));
      }
      driver_handle_.reset();
    } else if (rcode_.rcode_ != OB_SUCCESS) {
      if (OB_ITER_END == rcode_.rcode_) {
        driver_handle_->notify_src_exhausted(seq_idx_);
      } else {
        LOG_WARN_RET(rcode_.rcode_, "rpc error in async fetch cb", K(seq_idx_), "rcode", rcode_.rcode_);
        driver_handle_->abort_segment_fetch(rcode_.rcode_);
      }
    } else if (result_.data_.empty()) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected zero data len in async cb", K(seq_idx_));
      driver_handle_->abort_segment_fetch(OB_ERR_UNEXPECTED);
    } else {
      common::ObInOutBandwidthThrottle *throttle = GCTX.bandwidth_throttle_;
      if (OB_NOT_NULL(throttle) && result_.data_.length() > 0) {
        const int64_t bytes = result_.data_.length();
        int64_t last_send_ts = ObTimeUtility::current_time();
        lib::Thread::WaitGuard guard(lib::Thread::WAIT_FOR_IO_EVENT);
        int tmp_ret = throttle->limit_in_and_sleep(bytes, last_send_ts, INT64_MAX);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("[MIG VEC] failed to limit in band", K(tmp_ret), K(bytes));
        }
      }
      const int fill_ret = driver_handle_->fill_data(seq_idx_, result_.data_.ptr(), result_.data_.length());
      if (OB_SUCCESS != fill_ret) {
        LOG_WARN_RET(fill_ret, "fill data failed in async cb",
            K(seq_idx_), K(fill_ret));
        driver_handle_->abort_segment_fetch(fill_ret);
      }
    }
    return ret;
  }

  ObVecIdxSegFetchDriverHandle driver_handle_;
  int64_t seq_idx_;
};


int ObVecIdxSegFetchDriver::send_range_(const int64_t start_seq, const int64_t count)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    const int64_t seq = start_seq + i;
    if (OB_FAIL(send_one_(seq))) {
      LOG_WARN("[MIG VEC] failed to send async rpc", K(ret), K(seq));
      abort_segment_fetch(ret);
    }
  }
  return ret;
}

int ObVecIdxSegFetchDriver::send_retry_(const int64_t seq_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(send_one_(seq_idx))) {
    LOG_WARN("[MIG VEC] failed to send retry rpc", K(ret), K(seq_idx));
    abort_segment_fetch(ret);
  }
  return ret;
}

int ObVecIdxSegFetchDriver::send_one_(const int64_t seq_idx)
{
  ObFetchVecIdxSegDataCB cb;
  // Self-referencing handle: cb keeps the driver alive while the RPC is in
  // flight; original handle's dtor on stack unwind drops its ref.
  cb.driver_handle_ = ObVecIdxSegFetchDriverHandle(this);
  cb.seq_idx_ = seq_idx;
  obrpc::ObFetchVecIndexMigrationSegmentDataArg arg;
  arg.tenant_id_ = rpc_ctx_.tenant_id_;
  arg.ls_id_ = rpc_ctx_.ls_id_;
  arg.processor_id_ = rpc_ctx_.processor_id_;
  arg.seq_idx_ = seq_idx;
  arg.ls_rebuild_seq_ = rpc_ctx_.ls_rebuild_seq_;
  return rpc_ctx_.rpc_proxy_->to(rpc_ctx_.src_addr_)
      .dst_cluster_id(rpc_ctx_.cluster_id_)
      .by(rpc_ctx_.tenant_id_)
      .timeout(rpc_ctx_.rpc_timeout_us_)
      .group_id(share::OBCG_STORAGE)
      .fetch_vec_index_migration_segment_data(arg, &cb, obrpc::ObRpcOpts());
}

ObVecIdxSegFetchDriver::ObVecIdxSegFetchDriver()
  : is_inited_(false),
    rpc_ctx_(),
    dest_controller_handle_(),
    retry_lock_(common::ObLatchIds::OB_STORAGE_HA_STRUCT_LOCK),
    pending_retry_seqs_(),
    stopped_(false),
    fetch_abort_reason_(OB_SUCCESS),
    ref_count_(0)
{
}

int64_t ObVecIdxSegFetchDriver::dec_ref_()
{
  const int64_t new_ref = ATOMIC_SAF(&ref_count_, 1);
  if (new_ref < 0) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "driver ref count underflow",
                  K(new_ref), KP(this));
  }
  return new_ref;
}

int ObVecIdxSegFetchDriver::create(
    const ObVecIdxFetchRpcCtx &rpc_ctx,
    ObMigrationTenantWindowMgr *window_mgr,
    const share::ObDagPrio::ObDagPrioEnum dag_prio,
    ObVecIdxSegFetchDriverHandle &out_handle)
{
  int ret = OB_SUCCESS;
  ObVecIdxSegFetchDriver *driver = nullptr;
  if (OB_ISNULL(driver = OB_NEW(ObVecIdxSegFetchDriver, "MigVecIdxDrv"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[MIG VEC] alloc driver failed", KR(ret));
  } else if (OB_FAIL(driver->init(rpc_ctx, window_mgr, dag_prio))) {
    LOG_WARN("[MIG VEC] init driver failed", KR(ret), K(rpc_ctx), K(dag_prio));
    OB_DELETE(ObVecIdxSegFetchDriver, "MigVecIdxDrv", driver);
    driver = nullptr;
  } else {
    out_handle = ObVecIdxSegFetchDriverHandle(driver);  // ref -> 1
  }
  return ret;
}

void ObVecIdxSegFetchDriver::stop()
{
  {
    common::ObSpinLockGuard guard(retry_lock_);
    if (!stopped_) {
      stopped_ = true;
      pending_retry_seqs_.reset();
    }
  }
  if (dest_controller_handle_.is_valid()) {
    dest_controller_handle_->stop();
    dest_controller_handle_->wakeup_waiters();
  }
}

int ObVecIdxSegFetchDriver::init(
    const ObVecIdxFetchRpcCtx &rpc_ctx,
    ObMigrationTenantWindowMgr *window_mgr,
    const share::ObDagPrio::ObDagPrioEnum dag_prio)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[MIG VEC] vec idx seg fetch driver init twice", K(ret));
  } else if (!rpc_ctx.is_valid() || OB_ISNULL(window_mgr) || rpc_ctx.processor_id_ < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[MIG VEC] invalid argument for driver init", K(ret),
        K(rpc_ctx), KP(window_mgr));
  } else if (OB_FAIL(ObMigrationSlidingWindowDestController::create(
                 window_mgr, this, dag_prio, dest_controller_handle_))) {
    LOG_WARN("[MIG VEC] failed to create dest controller", K(ret), K(rpc_ctx), K(dag_prio));
  } else {
    rpc_ctx_ = rpc_ctx;
    {
      common::ObSpinLockGuard guard(retry_lock_);
      stopped_ = false;
    }
    ATOMIC_STORE(&fetch_abort_reason_, OB_SUCCESS);
    is_inited_ = true;
  }
  return ret;
}

int ObVecIdxSegFetchDriver::start_initial_window()
{
  int ret = OB_SUCCESS;
  int64_t head_seq = 0;
  int64_t init_window_size = 0;
  if (!is_inited_ || !dest_controller_handle_.is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] driver not inited in start initial window", K(ret));
  } else if (OB_FAIL(dest_controller_handle_->get_runtime_snapshot(head_seq, init_window_size))) {
    LOG_WARN("[MIG VEC] failed to get runtime snapshot", K(ret));
  } else if (OB_FAIL(send_range_(head_seq, init_window_size))) {
    LOG_WARN("[MIG VEC] initial rpc send failed", K(ret),
        K(head_seq), K(init_window_size));
  }
  return ret;
}

int ObVecIdxSegFetchDriver::fill_data(
    const int64_t seq_idx,
    const char *data,
    const int64_t data_len)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || !dest_controller_handle_.is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN_RET(ret, "driver not inited in fill data");
  } else {
    ret = dest_controller_handle_->fill_data(seq_idx, data, data_len);
  }
  return ret;
}

void ObVecIdxSegFetchDriver::notify_src_exhausted(const int64_t seq_idx)
{
  if (is_inited_ && dest_controller_handle_.is_valid()) {
    dest_controller_handle_->shrink_total_task_count(seq_idx);
  }
}

void ObVecIdxSegFetchDriver::abort_segment_fetch(const int failure_ret)
{
  if (!dest_controller_handle_.is_valid()) {
  } else {
    if (OB_SUCCESS != failure_ret && OB_CANCELED != failure_ret) {
      ATOMIC_BCAS(&fetch_abort_reason_, static_cast<int64_t>(OB_SUCCESS), static_cast<int64_t>(failure_ret));
    }
    this->stop();
  }
}

int ObVecIdxSegFetchDriver::enqueue_retry(const int64_t seq_idx)
{
  int ret = OB_SUCCESS;
  bool need_stop = false;
  {
    common::ObSpinLockGuard guard(retry_lock_);
    if (stopped_) {
      // nothing to do
    } else if (OB_FAIL(pending_retry_seqs_.push_back(seq_idx))) {
      LOG_WARN("enqueue retry failed to push, stopping", K(ret), K(seq_idx));
      stopped_ = true;
      need_stop = true;
    }
  }
  if (need_stop) {
    stop();
  }
  return ret;
}

int ObVecIdxSegFetchDriver::do_retry_()
{
  int ret = OB_SUCCESS;
  common::ObArray<int64_t> local_queue;
  bool has_work = false;
  {
    common::ObSpinLockGuard guard(retry_lock_);
    if (!stopped_ && !pending_retry_seqs_.empty()) {
      has_work = true;
      if (OB_FAIL(local_queue.assign(pending_retry_seqs_))) {
        LOG_WARN("failed to move retry seqs to local queue", KR(ret),
            "queue_count", pending_retry_seqs_.count());
        stopped_ = true;
      }
      pending_retry_seqs_.reset();
    }
  }
  if (OB_FAIL(ret)) {
    stop();
  } else if (has_work) {
    for (int64_t i = 0; OB_SUCC(ret) && i < local_queue.count(); ++i) {
      if (OB_FAIL(send_retry_(local_queue.at(i)))) {
        LOG_WARN("send retry failed", KR(ret), K(i),
            "queue_count", local_queue.count());
      }
    }
  }
  return ret;
}

void ObVecIdxSegFetchDriver::on_window_slid(
    const int64_t granted_start_seq,
    const int64_t granted_slot_count)
{
  if (granted_slot_count > 0 && is_inited_ && dest_controller_handle_.is_valid()) {
    const int ret = send_range_(granted_start_seq, granted_slot_count);
    if (OB_SUCCESS != ret) {
      LOG_WARN_RET(ret, "send rpc in window slid cb failed",
          K(granted_start_seq), K(granted_slot_count));
    }
  }
}

int64_t ObVecIdxSegFetchDriver::get_slot_buf_size()
{
  int64_t slot_buf_size = 0;
  if (!dest_controller_handle_.is_valid()) {
  } else {
    const int gret = dest_controller_handle_->get_slot_buf_size(slot_buf_size);
    if (OB_SUCCESS != gret) {
      LOG_WARN_RET(gret, "get_slot_buf_size failed on dest_ctrl", K(slot_buf_size));
    }
  }
  return slot_buf_size;
}

int ObVecIdxSegFetchDriver::wait_and_get_data(
    char *out_buf,
    const int64_t out_buf_len,
    int64_t &data_len)
{
  int ret = OB_SUCCESS;
  const int64_t deadline_us = ObTimeUtility::current_time() + ObStorageHAUtils::get_rpc_timeout();
  bool done = false;
  if (!is_inited_ || !dest_controller_handle_.is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] driver not inited in get next consume data", KR(ret));
  }
  while (OB_SUCC(ret) && !done) {
    if (OB_FAIL(do_retry_())) {
      LOG_WARN("do retry failed", K(ret));
      break;
    }
    const int64_t remain = deadline_us - ObTimeUtility::current_time();
    if (remain <= 0) {
      ret = OB_TIMEOUT;
      break;
    }
    const int64_t poll_timeout_us = MIN(CONSUME_POLL_INTERVAL_US, remain);
    const int poll_ret = dest_controller_handle_->get_next_consume_data(out_buf, out_buf_len, data_len, poll_timeout_us);
    if (OB_TIMEOUT == poll_ret) {
      // loop back to do_retry_()
    } else {
      ret = poll_ret;
      if (OB_CANCELED == ret) {
        const int64_t propagated = ATOMIC_LOAD(&fetch_abort_reason_);
        if (OB_SUCCESS != propagated) {
          ret = static_cast<int>(propagated);
        }
      }
      done = true;
    }
  }
  return ret;
}


static int notify_src_processor_done(
    ObLSCompleteMigrationCtx &ls_ctx,
    obrpc::ObStorageRpcProxy *rpc_proxy,
    ObVectorIndexAdaptorMigrationDag &dag,
    const int64_t processor_id,
    const int notify_code)
{
  int ret = OB_SUCCESS;
  if (!ObVectorIndexMigrationProcessorMgr::is_valid_processor_id(processor_id)) {
    // No processor registered yet; nothing to notify.
  } else if (OB_ISNULL(rpc_proxy)) {
    LOG_WARN("[MIG VEC] notify src processor skipped, rpc proxy null",
        K(processor_id), K(notify_code));
  } else {
    obrpc::ObNotifyVecIndexMigrationProcessorDoneArg arg;
    arg.tenant_id_ = ls_ctx.tenant_id_;
    arg.ls_id_ = ls_ctx.arg_.ls_id_;
    arg.adaptor_handle_id_ = dag.get_adaptor_handle_id();
    arg.failure_code_ = static_cast<int32_t>(notify_code);
    arg.ls_rebuild_seq_ = ls_ctx.src_ls_rebuild_seq_;
    arg.processor_id_ = processor_id;
    obrpc::Int64 res;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(rpc_proxy->to(ls_ctx.chosen_src_.src_addr_)
        .dst_cluster_id(ls_ctx.chosen_src_.cluster_id_)
        .by(ls_ctx.tenant_id_)
        .timeout(ObStorageHAUtils::get_rpc_timeout())
        .group_id(share::OBCG_STORAGE)
        .notify_vec_index_migration_processor_done(arg, res))) {
      LOG_WARN_RET(tmp_ret, "notify src processor done rpc failed",
          K(processor_id), K(notify_code));
    } else {
      LOG_INFO("[MIG VEC] notified src of processor done",
              K(processor_id), K(notify_code));
    }
  }
  return ret;
}

int ObVectorIndexAdaptorMigrationDag::set_vec_index_meta(const share::ObVectorIndexMeta &meta)
{
  int ret = OB_SUCCESS;
  vec_index_meta_.release();
  vec_index_meta_.header_ = meta.header_;
  vec_index_meta_.flags_ = meta.flags_;
  for (int64_t i = 0; OB_SUCC(ret) && i < meta.bases_.count(); ++i) {
    if (OB_FAIL(vec_index_meta_.add_base_seg_meta(meta.bases_.at(i)))) {
      LOG_WARN("[MIG VEC] failed to add base seg meta", K(ret), K(i));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < meta.incrs_.count(); ++i) {
    if (OB_FAIL(vec_index_meta_.add_incr_seg_meta(meta.incrs_.at(i)))) {
      LOG_WARN("[MIG VEC] failed to add incr seg meta", K(ret), K(i));
    }
  }
  return ret;
}

int ObVectorIndexAdaptorMigrationDag::release_src_adaptor_handle()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSCompleteMigrationDagNet *complete_dag_net = nullptr;
  ObLSCompleteMigrationCtx *ctx = nullptr;
  obrpc::ObStorageRpcProxy *rpc_proxy = nullptr;

  if (!is_inited_ || !ObVectorIndexMigrationProcessorMgr::is_valid_adaptor_handle_id(adaptor_handle_id_)) {
  } else if (OB_ISNULL(dag_net = get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] dag net is null when release src adaptor handle", K(ret), KP(this));
  } else if (ObDagNetType::DAG_NET_TYPE_COMPLETE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] dag net type unexpected when release src adaptor handle",
        K(ret), KPC(dag_net));
  } else if (FALSE_IT(complete_dag_net = static_cast<ObLSCompleteMigrationDagNet *>(dag_net))) {
  } else if (OB_ISNULL(ctx = complete_dag_net->get_ctx())
      || OB_ISNULL(rpc_proxy = complete_dag_net->get_storage_rpc_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ctx or rpc proxy null when release src adaptor handle",
        K(ret), KP(ctx), KP(rpc_proxy));
  } else {
    obrpc::ObReleaseVectorIndexAdaptorHandleArg arg;
    arg.tenant_id_ = ctx->tenant_id_;
    arg.ls_id_ = ctx->arg_.ls_id_;
    arg.adaptor_handle_id_ = adaptor_handle_id_;
    arg.ls_rebuild_seq_ = ctx->src_ls_rebuild_seq_;
    obrpc::Int64 res;
    if (OB_FAIL(rpc_proxy->to(ctx->chosen_src_.src_addr_)
        .dst_cluster_id(ctx->chosen_src_.cluster_id_)
        .by(ctx->tenant_id_)
        .group_id(share::OBCG_STORAGE)
        .release_vector_index_adaptor_handle(arg, res))) {
      LOG_WARN("[MIG VEC] release src adaptor handle rpc failed",
          K(ret), K_(adaptor_handle_id));
    } else {
      LOG_INFO("[MIG VEC] released src adaptor handle OK", K_(adaptor_handle_id));
      adaptor_handle_id_ = -1;
    }
  }
  return ret;
}

// ======================== ObVectorIndexMigrationDag ========================

ObVectorIndexMigrationDag::ObVectorIndexMigrationDag()
  : ObCompleteMigrationDag(ObDagType::DAG_TYPE_VECTOR_INDEX_MIGRATION),
    is_inited_(false)
{
}

ObVectorIndexMigrationDag::~ObVectorIndexMigrationDag()
{
}

int ObVectorIndexMigrationDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[MIG VEC] vector index migration dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[MIG VEC] init vector index migration dag get invalid argument", K(ret), KP(dag_net));
  } else if (OB_FAIL(ObCompleteMigrationDag::prepare_ctx(dag_net))) {
    LOG_WARN("[MIG VEC] failed to prepare ctx", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObVectorIndexMigrationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObLSCompleteMigrationCtx *self_ctx = nullptr;
  ObCStringHelper helper;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] vector index migration dag do not init", K(ret));
  } else if (ObIHADagNetCtx::LS_COMPLETE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(self_ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag_net_ctx_))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
        "ObVectorIndexMigrationDag: ls_id = %s, migration_type = %s, dag_prio = %s",
        helper.convert(self_ctx->arg_.ls_id_), ObMigrationOpType::get_str(self_ctx->arg_.type_),
        ObIDag::get_dag_prio_str(this->get_priority())))) {
    LOG_WARN("[MIG VEC] failed to fill comment", K(ret), K(*self_ctx));
  }
  return ret;
}

int ObVectorIndexMigrationDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObLSCompleteMigrationCtx *ctx = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] vector index migration dag not init", K(ret));
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ha dag net ctx is null", K(ret));
  } else if (ObIHADagNetCtx::LS_COMPLETE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag_net_ctx_))) {
  } else {
    int64_t adaptor_count = 0;
    if (ctx->has_vecidx_ctx_) {
      if (OB_FAIL(ctx->vecidx_ctx_.get_total_count(adaptor_count))) {
        LOG_WARN("[MIG VEC] failed to get vector index migration adaptor count", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  static_cast<int64_t>(ctx->tenant_id_),
                                  ctx->arg_.ls_id_.id(),
                                  static_cast<int64_t>(ctx->task_id_.hash()),
                                  adaptor_count))) {
        LOG_WARN("[MIG VEC] failed to fill info param", K(ret));
      }
    }
  }
  return ret;
}

int ObVectorIndexMigrationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObVectorIndexMigrationTask *task = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] vector index migration dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("[MIG VEC] Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("[MIG VEC] failed to init vector index migration task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("[MIG VEC] Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

int ObVectorIndexMigrationDag::generate_next_dag(ObIDag *&dag)
{
  // This controller DAG does not chain children; adaptor DAGs chain themselves.
  int ret = OB_ITER_END;
  dag = nullptr;
  return ret;
}

int ObVectorIndexMigrationDag::inner_reset_status_for_retry()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSCompleteMigrationCtx *ctx = nullptr;
  int32_t result = OB_SUCCESS;
  int32_t retry_count = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] vector index migration dag do not init", K(ret));
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ha dag net ctx is null", K(ret), KP(ha_dag_net_ctx_));
  } else if (ObIHADagNetCtx::LS_COMPLETE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag_net_ctx_))) {
  } else if (ctx->is_failed()) {
    if (OB_TMP_FAIL(ctx->get_result(ret))) {
      LOG_WARN("[MIG VEC] failed to get result", K(tmp_ret), KPC(ctx));
      ret = tmp_ret;
    } else {
      LOG_INFO("[MIG VEC] set inner set status for retry failed", K(ret), KPC(ctx));
    }
  } else if (OB_FAIL(result_mgr_.get_result(result))) {
    LOG_WARN("[MIG VEC] failed to get result", K(ret));
  } else if (OB_FAIL(result_mgr_.get_retry_count(retry_count))) {
    LOG_WARN("[MIG VEC] failed to get retry count", K(ret));
  } else {
    LOG_INFO("[MIG VEC] vector index migration dag start retry", KPC(this),
        K(result), K(retry_count));
    if (ctx->has_vecidx_ctx_) {
      if (OB_FAIL(ObPluginVectorIndexUtils::cleanup_adaptor_shells(
              ctx->arg_.ls_id_, ctx->vecidx_ctx_.get_adaptor_metas()))) {
        LOG_WARN("[MIG VEC] failed to cleanup adaptor shells before outer retry",
            K(ret), K(ctx->arg_.ls_id_));
      }
    }

    result_mgr_.reuse();
    if (ctx->has_vecidx_ctx_) {
      ctx->vecidx_ctx_.reset();
      ctx->has_vecidx_ctx_ = false;
    }

    if (OB_SUCC(ret)) {
      ObSqlString extra_info_str;
      if (OB_FAIL(extra_info_str.append_fmt("retry_count:%d;", retry_count))) {
        LOG_WARN("[MIG VEC] failed to append vecidx mig event extra info", K(ret), K(retry_count));
      } else {
        SERVER_EVENT_ADD("storage_ha", "vector_index_migration_retry", "tenant_id", ctx->tenant_id_,
            "ls_id", ctx->arg_.ls_id_.id(), "src", ctx->arg_.src_.get_server(), "dst", ctx->arg_.dst_.get_server(),
            "task_id", ctx->task_id_, "result", result, extra_info_str.ptr());
      }
      if (FAILEDx(create_first_task())) {
        LOG_WARN("[MIG VEC] failed to create first task for retry", K(ret));
      }
    }
  }
  return ret;
}

// ======================== ObVectorIndexMigrationTask ========================

ObVectorIndexMigrationTask::ObVectorIndexMigrationTask()
  : ObITask(TASK_TYPE_VEC_INDEX_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr),
    rpc_proxy_(nullptr)
{
}

ObVectorIndexMigrationTask::~ObVectorIndexMigrationTask()
{
}

int ObVectorIndexMigrationTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSCompleteMigrationDagNet *complete_dag_net = nullptr;
  ObVectorIndexMigrationDag *vec_idx_dag = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[MIG VEC] vector index migration task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_COMPLETE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(complete_dag_net = static_cast<ObLSCompleteMigrationDagNet*>(dag_net))) {
  } else if (OB_ISNULL(ctx_ = complete_dag_net->get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ctx is nullptr", K(ret), KPC(complete_dag_net));
  } else if (OB_ISNULL(this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] dag should not be NULL", K(ret));
  } else if (ObDagType::DAG_TYPE_VECTOR_INDEX_MIGRATION != this->get_dag()->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] dag type is unexpected", K(ret), KPC(this->get_dag()));
  } else if (FALSE_IT(vec_idx_dag = static_cast<ObVectorIndexMigrationDag *>(this->get_dag()))) {
  } else {
    dag_net_ = dag_net;
    rpc_proxy_ = complete_dag_net->get_storage_rpc_proxy();
    is_inited_ = true;
    LOG_INFO("[MIG VEC] succeed init vector index migration task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_VEC_INDEX_MIGRATION_TASK_FAILED);
int ObVectorIndexMigrationTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_INFO("[MIG VEC] vector index migration task begin",
      "ls_id", ctx_->arg_.ls_id_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] vector index migration task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    LOG_INFO("[MIG VEC] ls complete migration already failed, skip vector index migration", KPC(ctx_));
  } else if (OB_FAIL(fetch_adaptor_list_from_src_())) {
    LOG_WARN("[MIG VEC] failed to fetch adaptor list from src", K(ret));
  } else if (OB_FAIL(batch_create_adaptor_shells_())) {
    LOG_WARN("[VEC MIG] failed to batch create adaptor shells", K(ret));
  } else if (FALSE_IT(DEBUG_SYNC(AFTER_VECTOR_INDEX_MIGRATION_CREATE_ADAPTOR))) {
  } else {
#ifdef ERRSIM
    if (OB_FAIL(EN_VEC_INDEX_MIGRATION_TASK_FAILED)) {
      LOG_INFO("[MIG VEC] errsim inject migration task failure", K(ret),
          "ls_id", ctx_->arg_.ls_id_);
      SERVER_EVENT_ADD("storage_ha", "vec_mig_errsim_migration_task_failed", "tenant_id", ctx_->tenant_id_,
          "ls_id", ctx_->arg_.ls_id_.id(), "src", ctx_->arg_.src_.get_server(), "dst", ctx_->arg_.dst_.get_server(),
          "task_id", ctx_->task_id_, "ret", ret);
    }
#endif
    if (OB_SUCC(ret) && OB_FAIL(generate_first_adaptor_dag_())) {
      LOG_WARN("[MIG VEC] failed to generate first adaptor migration dag", K(ret));
    }
  }

  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("[MIG VEC] failed to record server event", K(tmp_ret), K(ret));
  }

  if (OB_FAIL(ret)) {
    LOG_INFO("[MIG VEC] vector index migration task failed, calling deal_with_fo",
        K(ret), "ls_id", ctx_->arg_.ls_id_);
    // No notify here: the outer PrepareTask only runs fetch_adaptor_list;
    // session is allocated per adaptor attempt by the inner PrepareTask
    // (fetch_segment_metas), so no src-side session exists to kill at this
    // stage. Inner-level failures handle their own notify.
    if (OB_TMP_FAIL(ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("[MIG VEC] failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }

  LOG_INFO("[MIG VEC] vector index migration task end",
      K(ret), "ls_id", ctx_->arg_.ls_id_);
  return ret;
}

int ObVectorIndexMigrationTask::fetch_adaptor_list_from_src_()
{
  int ret = OB_SUCCESS;
  ObArray<ObMigrationVectorIndexAdaptorMeta> adaptor_metas;
  uint64_t min_data_version = 0;
  if (OB_ISNULL(rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] rpc proxy is null", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(ctx_->tenant_id_, min_data_version))) {
    LOG_WARN("[MIG VEC] failed to get min data version", K(ret), "tenant_id", ctx_->tenant_id_);
  } else if (min_data_version < DATA_VERSION_4_6_0_1) {
    LOG_INFO("[MIG VEC] min data version below 4.6.0.1, skip vector index migration",
        K(min_data_version), "tenant_id", ctx_->tenant_id_, "ls_id", ctx_->arg_.ls_id_);
  } else {
    ObFetchVectorIndexAdaptorListArg arg;
    ObFetchVectorIndexAdaptorListRes res;
    arg.tenant_id_ = ctx_->tenant_id_;
    arg.ls_id_ = ctx_->arg_.ls_id_;
    arg.ls_rebuild_seq_ = ctx_->src_ls_rebuild_seq_;

    if (OB_FAIL(rpc_proxy_->to(ctx_->chosen_src_.src_addr_)
            .dst_cluster_id(ctx_->chosen_src_.cluster_id_)
            .by(ctx_->tenant_id_)
            .group_id(share::OBCG_STORAGE)
            .fetch_vector_index_adaptor_list(arg, res))) {
      LOG_WARN("[MIG VEC] failed to fetch vector index adaptor list from src", K(ret), K(arg));
    } else if (OB_FAIL(check_vsag_version_match_(res.vsag_version_.str()))) {
      if (OB_STATE_NOT_MATCH == ret) {
        // Mismatched vsag: skip in-memory copy; dest rebuilds from migrated tables.
        LOG_WARN("[MIG VEC] vsag version mismatch, skip adaptor copy, dest will rebuild",
            K(ret), "src_vsag_version", res.vsag_version_);
        ret = OB_SUCCESS;
        adaptor_metas.reset();
      } else {
        LOG_WARN("[MIG VEC] vsag version check failed", K(ret), "src_vsag_version", res.vsag_version_);
      }
    } else if (OB_FAIL(adaptor_metas.assign(res.adaptor_metas_))) {
      LOG_WARN("[MIG VEC] failed to assign adaptor metas", K(ret));
    } else {
      LOG_INFO("[MIG VEC] fetch adaptor list RPC success",
          "ls_id", ctx_->arg_.ls_id_, "adaptor_count", adaptor_metas.count());
      for (int64_t i = 0; i < adaptor_metas.count(); ++i) {
        LOG_INFO("[MIG VEC] adaptor meta detail", "idx", i,
            "data_tablet_id", adaptor_metas.at(i).data_tablet_id_,
            "inc_tablet_id", adaptor_metas.at(i).inc_tablet_id_,
            "snapshot_tablet_id", adaptor_metas.at(i).snapshot_tablet_id_,
            "vbitmap_tablet_id", adaptor_metas.at(i).vbitmap_tablet_id_);
      }
    }
  }

  if (OB_SUCC(ret) && adaptor_metas.empty()) {
    LOG_INFO("[MIG VEC] no vector index adaptor to copy, skip", "ls_id", ctx_->arg_.ls_id_);
  } else if (OB_SUCC(ret)) {
    ctx_->vecidx_ctx_.reset();
    ctx_->has_vecidx_ctx_ = false;
    if (OB_FAIL(ctx_->vecidx_ctx_.init(adaptor_metas))) {
      LOG_WARN("[MIG VEC] failed to init vi ctx", K(ret));
    } else {
      ctx_->has_vecidx_ctx_ = true;
      LOG_INFO("[MIG VEC] succeed to init vector index migration ctx",
          "ls_id", ctx_->arg_.ls_id_,
          "adaptor_count", adaptor_metas.count());
    }
  }
  return ret;
}

int ObVectorIndexMigrationTask::check_vsag_version_match_(const ObString &src_vsag_version)
{
  int ret = OB_SUCCESS;
  const std::string local_std = common::obvsag::version();
  const ObString local_vsag_version(static_cast<int32_t>(local_std.size()), local_std.c_str());

  if (src_vsag_version.empty()) {
    // Old src (pre-guard build) will not populate this field. Pass through to keep
    // backward compatibility; rely on higher-level cluster-version gating.
    LOG_INFO("[MIG VEC] src did not report vsag version, skip check",
        "local_vsag_version", local_vsag_version);
  } else if (0 != local_vsag_version.compare(src_vsag_version)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[MIG VEC] vsag library version mismatch, refuse migration", K(ret),
        K(src_vsag_version), "local_vsag_version", local_vsag_version);
    ObSqlString extra_info_str;
    if (OB_FAIL(extra_info_str.append_fmt("local_vsag_version:%.*s;",
            local_vsag_version.length(), local_vsag_version.ptr()))) {
      LOG_WARN("[MIG VEC] failed to append vsag version mismatch event extra info", K(ret),
          "local_vsag_version", local_vsag_version);
    } else {
      SERVER_EVENT_ADD("storage_ha", "vector_index_migration_vsag_version_mismatch", "tenant_id", ctx_->tenant_id_,
          "ls_id", ctx_->arg_.ls_id_.id(), "src", ctx_->arg_.src_.get_server(), "dst", ctx_->arg_.dst_.get_server(),
          "task_id", ctx_->task_id_, "src_vsag_version", src_vsag_version, extra_info_str.ptr());
    }
  } else {
    LOG_INFO("[MIG VEC] vsag version match", K(src_vsag_version));
  }
  return ret;
}

int ObVectorIndexMigrationTask::batch_create_adaptor_shells_()
{
  int ret = OB_SUCCESS;
  if (!ctx_->has_vecidx_ctx_) {
    // nothing to do
  } else if (OB_FAIL(share::ObPluginVectorIndexUtils::batch_create_adaptor_shells(
          ctx_->arg_.ls_id_, ctx_->vecidx_ctx_.get_adaptor_metas()))) {
    LOG_WARN("[VEC MIG] failed to batch create adaptor shells", K(ret));
  }
  return ret;
}

int ObVectorIndexMigrationTask::generate_first_adaptor_dag_()
{
  int ret = OB_SUCCESS;
  ObVectorIndexMigrationCtx &vi_ctx = ctx_->vecidx_ctx_;

  bool vi_empty = true;
  if (ctx_->has_vecidx_ctx_) {
    if (OB_FAIL(vi_ctx.check_adaptor_metas_empty(vi_empty))) {
      LOG_WARN("[MIG VEC] failed to check vector index migration ctx empty", K(ret));
    }
  }
  if (OB_SUCC(ret) && (!ctx_->has_vecidx_ctx_ || vi_empty)) {
    LOG_INFO("[MIG VEC] no adaptor to migrate, skip generating adaptor dag",
        "ls_id", ctx_->arg_.ls_id_, "has_vi_ctx", ctx_->has_vecidx_ctx_);
  } else if (OB_SUCC(ret)) {
    ObTenantDagScheduler *scheduler = nullptr;
    ObVectorIndexAdaptorMigrationDag *adaptor_dag = nullptr;
    ObMigrationVectorIndexAdaptorMeta adaptor_meta;
    ObDagPrio::ObDagPrioEnum prio = ObDagPrio::DAG_PRIO_MAX;
    ObDagId dag_id;

    if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[MIG VEC] failed to get ObTenantDagScheduler from MTL", K(ret));
    } else if (OB_FAIL(ObMigrationUtils::get_dag_priority(ctx_->arg_.type_, prio))) {
      LOG_WARN("[MIG VEC] failed to get dag priority", K(ret));
    } else {
      if (OB_FAIL(vi_ctx.get_next_adaptor_meta(adaptor_meta))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("[MIG VEC] all adaptor metas are invalid, skip", "ls_id", ctx_->arg_.ls_id_);
        } else {
          LOG_WARN("[MIG VEC] failed to get next adaptor meta", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && adaptor_meta.is_valid()) {
      ObIDag *vec_idx_dag = this->get_dag();
      if (OB_FAIL(scheduler->alloc_dag_with_priority(prio, adaptor_dag))) {
        LOG_WARN("[MIG VEC] failed to alloc vector index adaptor migration dag", K(ret));
      } else if (OB_FAIL(adaptor_dag->init(dag_net_, adaptor_meta))) {
        LOG_WARN("[MIG VEC] failed to init vector index adaptor migration dag", K(ret));
      } else if (FALSE_IT(dag_id.init(MYADDR))) {
      } else if (OB_FAIL(adaptor_dag->set_dag_id(dag_id))) {
        LOG_WARN("[MIG VEC] failed to set dag id", K(ret));
      } else if (OB_FAIL(vec_idx_dag->add_child(*adaptor_dag))) {
        // add_child links adaptor DAG between vi DAG and wait_data_ready.
        LOG_WARN("[MIG VEC] failed to add adaptor dag as child of vi dag", K(ret));
      } else if (OB_FAIL(adaptor_dag->create_first_task())) {
        LOG_WARN("[MIG VEC] failed to create first task for adaptor dag", K(ret));
      } else if (OB_FAIL(scheduler->add_dag(adaptor_dag))) {
        LOG_WARN("[MIG VEC] failed to add adaptor dag to scheduler", K(ret));
      } else {
        LOG_INFO("[MIG VEC] succeed to generate first adaptor migration dag",
            "ls_id", ctx_->arg_.ls_id_);
        adaptor_dag = nullptr;
      }

      if (OB_NOT_NULL(adaptor_dag)) {
        scheduler->free_dag(*adaptor_dag);
        adaptor_dag = nullptr;
      }
    }
  }

  return ret;
}

int ObVectorIndexMigrationTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ctx should not be null", K(ret));
  } else {
    int64_t adaptor_count = 0;
    if (ctx_->has_vecidx_ctx_) {
      if (OB_FAIL(ctx_->vecidx_ctx_.get_total_count(adaptor_count))) {
        LOG_WARN("[MIG VEC] failed to get vector index migration adaptor count", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      SERVER_EVENT_ADD("storage_ha", "vector_index_migration_task", "tenant_id", ctx_->tenant_id_,
          "ls_id", ctx_->arg_.ls_id_.id(), "src", ctx_->arg_.src_.get_server(), "dst", ctx_->arg_.dst_.get_server(),
          "task_id", ctx_->task_id_, "adaptor_count", adaptor_count);
    }
  }
  return ret;
}

// ======================== ObVectorIndexAdaptorMigrationDag ========================

ObVectorIndexAdaptorMigrationDag::ObVectorIndexAdaptorMigrationDag()
  : ObCompleteMigrationDag(ObDagType::DAG_TYPE_VECTOR_INDEX_ADAPTOR_MIGRATION),
    is_inited_(false),
    adaptor_meta_(),
    adaptor_handle_id_(-1),
    finish_task_(nullptr)
{
}

ObVectorIndexAdaptorMigrationDag::~ObVectorIndexAdaptorMigrationDag()
{
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(release_src_adaptor_handle())) {
    LOG_WARN_RET(tmp_ret, "[MIG VEC] failed to release src adaptor handle in dtor",
        K_(adaptor_handle_id));
  }
  if (OB_SUCCESS != get_dag_ret()) {
    record_migration_failed_event(get_dag_ret());
  }
}

int ObVectorIndexAdaptorMigrationDag::init(
    ObIDagNet *dag_net,
    const ObMigrationVectorIndexAdaptorMeta &adaptor_meta)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[MIG VEC] vector index adaptor migration dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net) || !adaptor_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[MIG VEC] init vector index adaptor migration dag get invalid argument",
        K(ret), KP(dag_net));
  } else if (OB_FAIL(ObCompleteMigrationDag::prepare_ctx(dag_net))) {
    LOG_WARN("[MIG VEC] failed to prepare ctx", K(ret));
  } else {
    adaptor_meta_ = adaptor_meta;
    is_inited_ = true;
  }
  return ret;
}

int ObVectorIndexAdaptorMigrationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObLSCompleteMigrationCtx *self_ctx = nullptr;
  ObCStringHelper helper;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] vector index adaptor migration dag do not init", K(ret));
  } else if (ObIHADagNetCtx::LS_COMPLETE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(self_ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag_net_ctx_))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
        "ObVectorIndexAdaptorMigrationDag: ls_id = %s, data_tablet_id = %ld, dag_prio = %s",
        helper.convert(self_ctx->arg_.ls_id_),
        adaptor_meta_.data_tablet_id_.id(),
        ObIDag::get_dag_prio_str(this->get_priority())))) {
    LOG_WARN("[MIG VEC] failed to fill comment", K(ret), K(*self_ctx), K_(adaptor_meta));
  }
  return ret;
}

int ObVectorIndexAdaptorMigrationDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObLSCompleteMigrationCtx *ctx = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] adaptor migration dag not init", K(ret));
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ha dag net ctx is null", K(ret));
  } else if (ObIHADagNetCtx::LS_COMPLETE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag_net_ctx_))) {
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                      static_cast<int64_t>(ctx->tenant_id_),
                                      ctx->arg_.ls_id_.id(),
                                      static_cast<int64_t>(adaptor_meta_.data_tablet_id_.id()),
                                      static_cast<int64_t>(ctx->task_id_.hash())))) {
    LOG_WARN("[MIG VEC] failed to fill info param", K(ret));
  }
  return ret;
}

bool ObVectorIndexAdaptorMigrationDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObStorageHADag &ha_dag = static_cast<const ObStorageHADag &>(other);
    if (OB_ISNULL(ha_dag_net_ctx_) || OB_ISNULL(ha_dag.get_ha_dag_net_ctx())) {
      is_same = false;
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ctx should not be NULL",
          KP(ha_dag_net_ctx_), KP(ha_dag.get_ha_dag_net_ctx()));
    } else if (ha_dag_net_ctx_->get_dag_net_ctx_type() != ha_dag.get_ha_dag_net_ctx()->get_dag_net_ctx_type()) {
      is_same = false;
    } else {
      ObLSCompleteMigrationCtx *self_ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag_net_ctx_);
      ObLSCompleteMigrationCtx *other_ctx =
          static_cast<ObLSCompleteMigrationCtx *>(ha_dag.get_ha_dag_net_ctx());
      if (self_ctx->arg_.ls_id_ != other_ctx->arg_.ls_id_) {
        is_same = false;
      } else {
        const ObVectorIndexAdaptorMigrationDag &other_dag =
            static_cast<const ObVectorIndexAdaptorMigrationDag &>(other);
        // Multiple vector indexes can be attached to the same user table (same data_tablet_id),
        // each index corresponds to one adaptor distinguished by inc_tablet_id;
        // must compare inc_tablet_id as well, otherwise scheduler hash will incorrectly dedup
        if (adaptor_meta_.data_tablet_id_ != other_dag.adaptor_meta_.data_tablet_id_
            || adaptor_meta_.inc_tablet_id_ != other_dag.adaptor_meta_.inc_tablet_id_) {
          is_same = false;
        }
      }
    }
  }
  return is_same;
}

uint64_t ObVectorIndexAdaptorMigrationDag::hash() const
{
  uint64_t hash_value = 0;
  ObLSCompleteMigrationCtx *ctx = nullptr;
  if (OB_ISNULL(ha_dag_net_ctx_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ctx should not be NULL", KP(ha_dag_net_ctx_));
  } else if (ObIHADagNetCtx::LS_COMPLETE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ha dag net ctx type is unexpected", KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag_net_ctx_))) {
  } else {
    hash_value = common::murmurhash(
        &ctx->arg_.ls_id_, sizeof(ctx->arg_.ls_id_), hash_value);
    const common::ObTabletID &data_tablet_id = adaptor_meta_.data_tablet_id_;
    hash_value = common::murmurhash(
        &data_tablet_id, sizeof(data_tablet_id), hash_value);
    // Multiple vector indexes under the same data_tablet_id share dag type + data_tablet_id,
    // must mix inc_tablet_id into hash, otherwise scheduler dag_net hash map will dedup the 2nd+ adaptor
    const common::ObTabletID &inc_tablet_id = adaptor_meta_.inc_tablet_id_;
    hash_value = common::murmurhash(
        &inc_tablet_id, sizeof(inc_tablet_id), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObVectorIndexAdaptorMigrationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObVectorIndexAdaptorMigrationTask *prepare_task = nullptr;
  ObVecIndexFinishTask *finish_task = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] vector index adaptor migration dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(finish_task))) {
    LOG_WARN("[MIG VEC] Fail to alloc finish task", K(ret));
  } else if (OB_FAIL(finish_task->init())) {
    LOG_WARN("[MIG VEC] failed to init finish task", K(ret));
  } else if (OB_FAIL(alloc_task(prepare_task))) {
    LOG_WARN("[MIG VEC] Fail to alloc prepare task", K(ret));
  } else if (OB_FAIL(prepare_task->init(finish_task))) {
    LOG_WARN("[MIG VEC] failed to init vector index adaptor migration task", K(ret), K_(adaptor_meta));
  } else if (OB_FAIL(prepare_task->add_child(*finish_task))) {
    LOG_WARN("[MIG VEC] failed to add finish task as child of prepare task", K(ret));
  } else if (OB_FAIL(add_task(*prepare_task))) {
    LOG_WARN("[MIG VEC] Fail to add prepare task", K(ret));
  } else if (OB_FAIL(add_task(*finish_task))) {
    LOG_WARN("[MIG VEC] Fail to add finish task", K(ret));
  } else {
    finish_task_ = finish_task;
    LOG_INFO("[MIG VEC] success to create first task", K(ret), KPC(this));
  }
  return ret;
}

int ObVectorIndexAdaptorMigrationDag::generate_next_dag(ObIDag *&dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObVectorIndexAdaptorMigrationDag *adaptor_dag = nullptr;
  ObMigrationVectorIndexAdaptorMeta adaptor_meta;
  ObLSCompleteMigrationCtx *ctx = nullptr;
  ObDagPrio::ObDagPrioEnum prio = ObDagPrio::DAG_PRIO_MAX;
  ObDagId dag_id;
  bool need_set_failed_result = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] vector index adaptor migration dag do not init", K(ret));
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ha dag net ctx is null", K(ret), KP(ha_dag_net_ctx_));
  } else if (ObIHADagNetCtx::LS_COMPLETE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag_net_ctx_))) {
  } else if (!ctx->has_vecidx_ctx_) {
    ret = OB_ITER_END;
    need_set_failed_result = false;
    LOG_INFO("[MIG VEC] vector index adaptor migration has no next dag, no vi ctx", KPC(this));
  } else if (ctx->is_failed()) {
    if (OB_TMP_FAIL(ctx->get_result(ret))) {
      LOG_WARN("[MIG VEC] failed to get result", K(tmp_ret), KPC(ctx));
      ret = tmp_ret;
    } else {
      LOG_INFO("[MIG VEC] generate_next_dag: dag-net ctx already failed, skip next adaptor",
          K(ret), KPC(ctx));
    }
  } else if (OB_FAIL(ObMigrationUtils::get_dag_priority(ctx->arg_.type_, prio))) {
    LOG_WARN("[MIG VEC] failed to get dag priority", K(ret));
  } else {
    if (OB_FAIL(ctx->vecidx_ctx_.get_next_adaptor_meta(adaptor_meta))) {
      if (OB_ITER_END == ret) {
        need_set_failed_result = false;
      } else {
        LOG_WARN("[MIG VEC] failed to get next adaptor meta", K(ret));
      }
    } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[MIG VEC] failed to get ObTenantDagScheduler from MTL", K(ret));
    } else if (OB_FAIL(scheduler->alloc_dag_with_priority(prio, adaptor_dag))) {
      LOG_WARN("[MIG VEC] failed to alloc vector index adaptor migration dag", K(ret));
    } else if (OB_FAIL(adaptor_dag->init(this->get_dag_net(), adaptor_meta))) {
      LOG_WARN("[MIG VEC] failed to init vector index adaptor migration dag", K(ret));
    } else if (FALSE_IT(dag_id.init(MYADDR))) {
    } else if (OB_FAIL(adaptor_dag->set_dag_id(dag_id))) {
      LOG_WARN("[MIG VEC] failed to set dag id", K(ret));
    } else {
      LOG_INFO("[MIG VEC] succeed generate next adaptor migration dag");
      dag = adaptor_dag;
      adaptor_dag = nullptr;
    }
  }

  if (OB_NOT_NULL(adaptor_dag) && OB_NOT_NULL(scheduler)) {
    scheduler->free_dag(*adaptor_dag);
    adaptor_dag = nullptr;
  }

  if (OB_FAIL(ret)) {
    const bool need_retry = false;
    if (need_set_failed_result && OB_TMP_FAIL(ha_dag_net_ctx_->set_result(ret, need_retry, get_type()))) {
      LOG_WARN("[MIG VEC] failed to set result", K(ret), KPC(ha_dag_net_ctx_));
    }
  }

  return ret;
}

int ObVectorIndexAdaptorMigrationDag::inner_reset_status_for_retry()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSCompleteMigrationCtx *ctx = nullptr;
  int32_t result = OB_SUCCESS;
  int32_t retry_count = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] vector index adaptor migration dag do not init", K(ret));
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ha dag net ctx is null", K(ret), KP(ha_dag_net_ctx_));
  } else if (ObIHADagNetCtx::LS_COMPLETE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag_net_ctx_))) {
  } else if (ctx->is_failed()) {
    if (OB_TMP_FAIL(ctx->get_result(ret))) {
      LOG_WARN("[MIG VEC] failed to get result", K(tmp_ret), KPC(ctx));
      ret = tmp_ret;
    } else {
      LOG_INFO("[MIG VEC] set inner set status for retry failed, dag-net ctx already failed",
          K(ret), KPC(ctx));
    }
  } else if (OB_FAIL(result_mgr_.get_result(result))) {
    LOG_WARN("[MIG VEC] failed to get result", K(ret));
  } else if (OB_FAIL(result_mgr_.get_retry_count(retry_count))) {
    LOG_WARN("[MIG VEC] failed to get retry count", K(ret));
  } else {
    LOG_INFO("[MIG VEC] vector index adaptor migration dag start retry", KPC(this),
        K(result), K(retry_count), K_(adaptor_meta));
    if (OB_TMP_FAIL(share::ObPluginVectorIndexUtils::set_adaptor_mig_state(
        ctx->arg_.ls_id_, adaptor_meta_.inc_tablet_id_, share::ObAdaptorMigState::MIG_FAIL))) {
      LOG_WARN("[MIG VEC] failed to set MIG_FAIL before retry", K(tmp_ret), K_(adaptor_meta));
    }
    if (OB_TMP_FAIL(release_src_adaptor_handle())) {
      LOG_WARN("[MIG VEC] failed to release src adaptor handle before retry",
          K(tmp_ret), K(result), K_(adaptor_handle_id));
    }
    record_migration_failed_event(result);
    result_mgr_.reuse();
    adaptor_handle_id_ = -1;
    vec_index_meta_.release();
    finish_task_ = nullptr;
    if (OB_FAIL(clear_migrated_segments_from_dest_adaptor_())) {
      LOG_WARN("[MIG VEC] failed to clear migrated segments before retry, "
          "abort retry to avoid duplicate segments", K(ret), K_(adaptor_meta));
    } else {
      if (OB_FAIL(share::ObPluginVectorIndexUtils::cleanup_tenant_adaptor_shell_for_retry(
              ctx->arg_.ls_id_, adaptor_meta_.inc_tablet_id_))) {
        LOG_WARN("[MIG VEC] failed to cleanup tenant adaptor before inner retry, "
            "abort retry to avoid empty adaptor on next attempt",  K(ret), K_(adaptor_meta));
      }
      if (OB_SUCC(ret)) {
        ObSqlString extra_info_str;
        if (OB_FAIL(extra_info_str.append_fmt("data_tablet_id:%ld;retry_count:%d;",
                adaptor_meta_.data_tablet_id_.id(), retry_count))) {
          LOG_WARN("[MIG VEC] failed to append vector index adaptor migration retry event extra info",
              K(ret), K(adaptor_meta_), K(retry_count));
        } else {
          SERVER_EVENT_ADD("storage_ha", "vector_index_adaptor_migration_retry", "tenant_id", ctx->tenant_id_,
              "ls_id", ctx->arg_.ls_id_.id(), "src", ctx->arg_.src_.get_server(), "dst", ctx->arg_.dst_.get_server(),
              "task_id", ctx->task_id_, "result", result, extra_info_str.ptr());
        }
        if (FAILEDx(create_first_task())) {
          LOG_WARN("[MIG VEC] failed to create first task for retry", K(ret));
        }
      }
    }
  }
  return ret;
}

bool ObVectorIndexAdaptorMigrationDag::is_migration_cancelled() const
{
  bool cancelled = false;
  if (is_failed()) {
    cancelled = true;
  } else {
    ObIHADagNetCtx *const dag_net_ctx = get_ha_dag_net_ctx();
    if (OB_NOT_NULL(dag_net_ctx) && dag_net_ctx->is_failed()) {
      cancelled = true;
    }
  }
  return cancelled;
}

void ObVectorIndexAdaptorMigrationDag::record_migration_failed_event(const int32_t result)
{
  int32_t retry_count = 0;
  int32_t root_result = result;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(ha_dag_net_ctx_)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "ha dag net ctx is null, skip failed event");
  } else if (ObIHADagNetCtx::LS_COMPLETE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "ha dag net ctx type is unexpected, skip failed event");
  } else {
    ObLSCompleteMigrationCtx *ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag_net_ctx_);
    (void) result_mgr_.get_retry_count(retry_count);
    int32_t first_result = OB_SUCCESS;
    if (OB_SUCCESS == result_mgr_.get_result(first_result) && OB_SUCCESS != first_result) {
      root_result = first_result;
    }
    ObSqlString extra_info_str;
    if (OB_TMP_FAIL(extra_info_str.append_fmt("data_tablet_id:%ld;inc_tablet_id:%ld;retry_count:%d;",
            adaptor_meta_.data_tablet_id_.id(), adaptor_meta_.inc_tablet_id_.id(), retry_count))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "[MIG VEC] failed to append vector index adaptor migration failed event extra info",
          K(ret), K(adaptor_meta_), K(root_result), K(retry_count));
    } else {
      SERVER_EVENT_ADD("storage_ha", "vector_index_adaptor_migration_failed", "tenant_id", ctx->tenant_id_,
          "ls_id", ctx->arg_.ls_id_.id(), "src", ctx->arg_.src_.get_server(), "dst", ctx->arg_.dst_.get_server(),
          "task_id", ctx->task_id_, "result", root_result, extra_info_str.ptr());
    }
  }
}
int ObVectorIndexAdaptorMigrationDag::clear_migrated_segments_from_dest_adaptor_()
{
  // Best-effort clear dest segments before retry (avoid duplicate install).
  int ret = OB_SUCCESS;
  ObLSCompleteMigrationCtx *ctx = nullptr;
  share::ObPluginVectorIndexService *vi_service = nullptr;
  share::ObPluginVectorIndexAdapterGuard adaptor_guard;
  if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ha dag net ctx is null", K(ret), KP(ha_dag_net_ctx_));
  } else if (ObIHADagNetCtx::LS_COMPLETE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(ctx = static_cast<ObLSCompleteMigrationCtx *>(ha_dag_net_ctx_))) {
  } else if (OB_ISNULL(vi_service = MTL(share::ObPluginVectorIndexService *))) {
    LOG_INFO("[MIG VEC] vector index service not available, skip clear",
        "ls_id", ctx->arg_.ls_id_);
  } else if (OB_FAIL(vi_service->get_adapter_inst_guard(
                ctx->arg_.ls_id_, adaptor_meta_.inc_tablet_id_, adaptor_guard))) {
    if (OB_HASH_NOT_EXIST == ret) {
      LOG_INFO("[MIG VEC] adaptor not available on dest, skip clear",
        K(ret), "ls_id", ctx->arg_.ls_id_,
        "inc_tablet_id", adaptor_meta_.inc_tablet_id_);
        ret = OB_SUCCESS;
    } else {
      LOG_WARN("[MIG VEC] failed to acquire adaptor guard on dest", K(ret), "ls_id", ctx->arg_.ls_id_,
        "inc_tablet_id", adaptor_meta_.inc_tablet_id_);
    }
  } else {
    share::ObPluginVectorIndexAdaptor *adaptor = adaptor_guard.get_adatper();
    if (OB_ISNULL(adaptor)) {
      LOG_INFO("[MIG VEC] adaptor null after get_adapter_inst_guard, skip clear",
          "ls_id", ctx->arg_.ls_id_,
          "inc_tablet_id", adaptor_meta_.inc_tablet_id_);
    } else if (OB_FAIL(adaptor->clear_migrated_segments())) {
      LOG_WARN("[MIG VEC] failed to clear migrated segments on dest adaptor",
          K(ret), "ls_id", ctx->arg_.ls_id_,
          "inc_tablet_id", adaptor_meta_.inc_tablet_id_);
    } else {
      LOG_INFO("[MIG VEC] cleared dest adaptor migrated segments before retry",
          "ls_id", ctx->arg_.ls_id_,
          "data_tablet_id", adaptor_meta_.data_tablet_id_,
          "inc_tablet_id", adaptor_meta_.inc_tablet_id_);
    }
  }
  return ret;
}

// ======================== ObVectorIndexAdaptorMigrationTask ========================

ObVectorIndexAdaptorMigrationTask::ObVectorIndexAdaptorMigrationTask()
  : ObITask(TASK_TYPE_VEC_INDEX_ADAPTOR_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr),
    finish_task_(nullptr),
    rpc_proxy_(nullptr)
{
}

ObVectorIndexAdaptorMigrationTask::~ObVectorIndexAdaptorMigrationTask()
{
}

int ObVectorIndexAdaptorMigrationTask::init(ObVecIndexFinishTask *finish_task)
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSCompleteMigrationDagNet *complete_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[MIG VEC] vector index adaptor migration task init twice", K(ret));
  } else if (OB_ISNULL(finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[MIG VEC] finish task is null", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_COMPLETE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(complete_dag_net = static_cast<ObLSCompleteMigrationDagNet*>(dag_net))) {
  } else if (OB_ISNULL(ctx_ = complete_dag_net->get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ctx is nullptr", K(ret), KPC(complete_dag_net));
  } else {
    dag_net_ = dag_net;
    finish_task_ = finish_task;
    rpc_proxy_ = complete_dag_net->get_storage_rpc_proxy();
    is_inited_ = true;
    LOG_INFO("[MIG VEC] succeed init vector index adaptor migration task",
        "ls_id", ctx_->arg_.ls_id_,
        "data_tablet_id", get_adaptor_dag_()->get_adaptor_meta().data_tablet_id_,
        "dag_id", *ObCurTraceId::get_trace_id(),
        "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObVectorIndexAdaptorMigrationTask::try_skip_fetch_by_tenant_map_(
        const ObMigrationVectorIndexAdaptorMeta &adaptor_meta,
        bool &skip_fetch)
{
  int ret = OB_SUCCESS;
  skip_fetch = false;
  if (OB_FAIL(share::ObPluginVectorIndexUtils::try_reuse_adaptor_from_tenant_map(
          ctx_->arg_.ls_id_, adaptor_meta.inc_tablet_id_, skip_fetch))) {
    LOG_WARN("[MIG VEC] failed to try reuse adaptor from tenant map",
        K(ret), K_(ctx), K(adaptor_meta));
  } else if (!skip_fetch) {
    if (OB_FAIL(share::ObPluginVectorIndexUtils::set_adaptor_mig_state(
        ctx_->arg_.ls_id_, adaptor_meta.inc_tablet_id_, share::ObAdaptorMigState::MIG_DOING))) {
      LOG_WARN("[MIG VEC] failed to set MIG_DOING", K(ret), K(adaptor_meta));
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_VEC_INDEX_MIGRATION_ADAPTOR_PREPARE_TASK_FAILED);
int ObVectorIndexAdaptorMigrationTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  bool skip_fetch = false;
  ObVectorIndexAdaptorMigrationDag *dag = get_adaptor_dag_();
  const ObMigrationVectorIndexAdaptorMeta &adaptor_meta = dag->get_adaptor_meta();
  LOG_INFO("[MIG VEC] vector index adaptor migration prepare task begin",
      "ls_id", ctx_->arg_.ls_id_,
      "data_tablet_id", adaptor_meta.data_tablet_id_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] vector index adaptor migration task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    LOG_INFO("[MIG VEC] ls complete migration already failed, skip adaptor migration",
        KPC(ctx_));
  } else {
    LOG_INFO("[MIG VEC] adaptor migration prepare begin",
        "ls_id", ctx_->arg_.ls_id_,
        "data_tablet_id", adaptor_meta.data_tablet_id_,
        "inc_tablet_id", adaptor_meta.inc_tablet_id_);
    if (OB_FAIL(try_skip_fetch_by_tenant_map_(adaptor_meta, skip_fetch))) {
      LOG_WARN("[MIG VEC] failed to try skip fetch by tenant map", K(ret), K(adaptor_meta), "ls_id", ctx_->arg_.ls_id_);
    } else if (skip_fetch) {
      LOG_INFO("[MIG VEC] skip fetch by tenant map, skip hold adapter and fetch segment metas from src", K(ret), K(adaptor_meta), "ls_id", ctx_->arg_.ls_id_);
    } else if (OB_FAIL(hold_adapter_and_fetch_segment_metas_from_src_())) {
      LOG_WARN("[MIG VEC] failed to hold adapter and fetch segment metas from src",
          K(ret), K(adaptor_meta), "ls_id", ctx_->arg_.ls_id_);
    }
    DEBUG_SYNC(AFTER_VECTOR_INDEX_MIGRATION_HOLD_SRC_ADAPTOR);
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      if (OB_FAIL(EN_VEC_INDEX_MIGRATION_ADAPTOR_PREPARE_TASK_FAILED)) {
        LOG_INFO("[MIG VEC] errsim vector index adaptor prepare task failed", K(ret),
            "adaptor_handle_id", dag->get_adaptor_handle_id());
        ObSqlString extra_info_str;
        if (OB_TMP_FAIL(extra_info_str.append_fmt("adaptor_handle_id:%ld;", dag->get_adaptor_handle_id()))) {
          LOG_WARN("[MIG VEC] failed to append errsim adaptor prepare task failed event extra info",
              K(tmp_ret), K(ret), "adaptor_handle_id", dag->get_adaptor_handle_id());
        } else {
          SERVER_EVENT_ADD("storage_ha", "vec_mig_errsim_adaptor_prepare_task_failed", "tenant_id", ctx_->tenant_id_,
              "ls_id", ctx_->arg_.ls_id_.id(), "src", ctx_->arg_.src_.get_server(), "dst", ctx_->arg_.dst_.get_server(),
              "task_id", ctx_->task_id_, "ret", ret, extra_info_str.ptr());
        }
      }
    }
#endif
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_copy_and_finish_tasks_())) {
      LOG_WARN("[MIG VEC] failed to create copy and finish tasks", K(ret));
    } else {
      LOG_INFO("[MIG VEC] PrepareTask done, CopyTasks and FinishTask created",
          "ls_id", ctx_->arg_.ls_id_,
          "data_tablet_id", dag->get_adaptor_meta().data_tablet_id_,
          "adaptor_handle_id", dag->get_adaptor_handle_id());
    }
  }

  if (OB_TMP_FAIL(record_server_event_(skip_fetch))) {
    LOG_WARN("[MIG VEC] failed to record server event", K(tmp_ret), K(ret));
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("[MIG VEC] failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }

  LOG_INFO("[MIG VEC] vector index adaptor migration prepare task end",
      K(ret), "ls_id", ctx_->arg_.ls_id_,
      "data_tablet_id", dag->get_adaptor_meta().data_tablet_id_,
      "cost_us", ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObVectorIndexAdaptorMigrationTask::hold_adapter_and_fetch_segment_metas_from_src_()
{
  int ret = OB_SUCCESS;
  ObVectorIndexAdaptorMigrationDag *dag = get_adaptor_dag_();
  const ObMigrationVectorIndexAdaptorMeta &adaptor_meta = dag->get_adaptor_meta();

  if (OB_ISNULL(rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] rpc proxy is null", K(ret));
  } else {
    obrpc::ObFetchVectorIndexSegmentMetasArg arg;
    obrpc::ObFetchVectorIndexSegmentMetasRes res;
    arg.tenant_id_ = ctx_->tenant_id_;
    arg.ls_id_ = ctx_->arg_.ls_id_;
    arg.inc_tablet_id_ = adaptor_meta.inc_tablet_id_;
    arg.ls_rebuild_seq_ = ctx_->src_ls_rebuild_seq_;
    if (OB_FAIL(rpc_proxy_->to(ctx_->chosen_src_.src_addr_)
            .dst_cluster_id(ctx_->chosen_src_.cluster_id_)
            .by(ctx_->tenant_id_)
            .group_id(share::OBCG_STORAGE)
            .fetch_vector_index_segment_metas(arg, res))) {
      LOG_WARN("[MIG VEC] failed to fetch segment metas from src", K(ret), K(arg));
    } else if (OB_INVALID_ID == res.adaptor_handle_id_) {
      // src returns OB_SUCCESS with adaptor_handle_id_=OB_INVALID_ID to signal that the
      // adaptor no longer exists (e.g. index dropped during migration window).
      // Skip this adaptor gracefully; no hold was established on src so no release needed.
      LOG_INFO("[MIG VEC] src adaptor not exist, index may have been dropped, skip",
          "ls_id", ctx_->arg_.ls_id_,
          "data_tablet_id", adaptor_meta.data_tablet_id_,
          "inc_tablet_id", adaptor_meta.inc_tablet_id_);
    } else {
      dag->set_adaptor_handle_id(res.adaptor_handle_id_);
      if (OB_FAIL(dag->set_vec_index_meta(res.vec_index_meta_))) {
        LOG_WARN("[MIG VEC] failed to set vec index meta on dag", K(ret),
            "adaptor_handle_id", res.adaptor_handle_id_);
      } else if (OB_FAIL(ctx_->vecidx_ctx_.update_vec_index_right_boundary_scn(
                   res.vec_index_right_boundary_scn_))) {
        LOG_WARN("[MIG VEC] failed to update vec index right boundary scn", K(ret));
      } else {
        ctx_->vecidx_ctx_.clear_all_adaptor_skip_fetch();
        LOG_INFO("[MIG VEC] hold adaptor and fetch segment metas from src success",
            "ls_id", ctx_->arg_.ls_id_,
            "data_tablet_id", adaptor_meta.data_tablet_id_,
            "adaptor_handle_id", dag->get_adaptor_handle_id(),
            "segment_meta_count", res.vec_index_meta_.segment_count(),
            "adaptor_boundary_scn", res.vec_index_right_boundary_scn_);
        ObSqlString extra_info_str;
        if (OB_FAIL(extra_info_str.append_fmt("inc_tablet_id:%ld;right_boundary_scn:%lu;index_num:%ld;",
                adaptor_meta.inc_tablet_id_.id(), res.vec_index_right_boundary_scn_.get_val_for_inner_table_field(),
                res.vec_index_meta_.segment_count()))) {
          LOG_WARN("[MIG VEC] failed to append fetch adaptor info event extra info", K(ret), K(adaptor_meta));
        } else {
          SERVER_EVENT_ADD("storage_ha", "vector_index_migration_fetch_adaptor_info", "tenant_id", ctx_->tenant_id_,
              "ls_id", ctx_->arg_.ls_id_.id(), "src", ctx_->arg_.src_.get_server(), "dst", ctx_->arg_.dst_.get_server(),
              "task_id", ctx_->task_id_, "data_tablet_id", adaptor_meta.data_tablet_id_.id(), extra_info_str.ptr());
        }
      }
    }
  }
  return ret;
}

int ObVectorIndexAdaptorMigrationTask::create_copy_and_finish_tasks_()
{
  int ret = OB_SUCCESS;
  ObVectorIndexAdaptorMigrationDag *dag = get_adaptor_dag_();
  const ObMigrationVectorIndexAdaptorMeta &adaptor_meta = dag->get_adaptor_meta();
  const int64_t total = dag->get_total_segment_count();

  // copy snapshot segment_idx order: bases_ then incrs_
  if (OB_ISNULL(finish_task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] finish task is null", K(ret));
  } else if (0 == total) {
    LOG_INFO("[MIG VEC] no vector index segments need to copy",
        "ls_id", ctx_->arg_.ls_id_, "data_tablet_id", adaptor_meta.data_tablet_id_);
  } else {
    // Create the first copy task; the chain is extended via generate_next_task
    ObVecIndexSegmentCopyTask *copy_task = nullptr;
    if (OB_FAIL(dag->alloc_task(copy_task))) {
      LOG_WARN("[MIG VEC] failed to alloc copy task", K(ret));
    } else if (OB_FAIL(copy_task->init(0 /*segment_idx*/))) {
      LOG_WARN("[MIG VEC] failed to init copy task", K(ret));
    } else if (OB_FAIL(copy_task->add_child(*finish_task_, false /*check_child_task_status*/))) {
      // FinishTask is already in WAITING status (added to DAG in create_first_task),
      // so we skip the child status check. This is safe because FinishTask's indegree > 0
      // (from PrepareTask), so it won't be scheduled prematurely.
      LOG_WARN("[MIG VEC] failed to add finish task as child of copy task", K(ret));
    } else if (OB_FAIL(dag->add_task(*copy_task))) {
      LOG_WARN("[MIG VEC] failed to add copy task to dag", K(ret));
    } else {
      LOG_INFO("[MIG VEC] created first copy task, chain will extend via generate next task",
          "ls_id", ctx_->arg_.ls_id_,
          "data_tablet_id", adaptor_meta.data_tablet_id_,
          "total_segments", total);
    }
  }
  return ret;
}

int ObVectorIndexAdaptorMigrationTask::record_server_event_(const bool skip_fetch)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ctx should not be null", K(ret));
  } else {
    const ObMigrationVectorIndexAdaptorMeta &adaptor_meta = get_adaptor_dag_()->get_adaptor_meta();
    ObSqlString extra_info_str;
    if (OB_FAIL(extra_info_str.append_fmt("inc_tablet_id:%ld;adaptor_handle_id:%ld;segment_meta_count:%ld;mode:%s;",
            adaptor_meta.inc_tablet_id_.id(), get_adaptor_dag_()->get_adaptor_handle_id(),
            get_adaptor_dag_()->get_total_segment_count(), skip_fetch ? "skip_fetch" : "normal"))) {
      LOG_WARN("[MIG VEC] failed to append adaptor prepare finish event extra info", K(ret), K(adaptor_meta),
          K(skip_fetch));
    } else {
      SERVER_EVENT_ADD("storage_ha", "vector_index_migration_adaptor_prepare_finish", "tenant_id", ctx_->tenant_id_,
          "ls_id", ctx_->arg_.ls_id_.id(), "src", ctx_->arg_.src_.get_server(), "dst", ctx_->arg_.dst_.get_server(),
          "task_id", ctx_->task_id_, "data_tablet_id", adaptor_meta.data_tablet_id_.id(), extra_info_str.ptr());
    }
  }
  return ret;
}

// ======================== ObVecIndexSegmentCopyTask ========================

ObVecIndexSegmentCopyTask::ObVecIndexSegmentCopyTask()
  : ObITask(TASK_TYPE_VEC_INDEX_SEGMENT_COPY),
    is_inited_(false),
    segment_idx_(OB_INVALID_INDEX_INT64),
    ctx_(nullptr),
    dag_net_(nullptr),
    rpc_proxy_(nullptr),
    processor_id_(-1),
    fetch_handle_()
{
}

ObVecIndexSegmentCopyTask::~ObVecIndexSegmentCopyTask()
{
}

int ObVecIndexSegmentCopyTask::init(const int64_t segment_idx)
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSCompleteMigrationDagNet *complete_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[MIG VEC] vec index segment copy task init twice", K(ret));
  } else if (segment_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[MIG VEC] invalid segment idx", K(ret), K(segment_idx));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] dag net should not be NULL", K(ret));
  } else if (ObDagNetType::DAG_NET_TYPE_COMPLETE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] dag net type is unexpected", K(ret));
  } else if (FALSE_IT(complete_dag_net = static_cast<ObLSCompleteMigrationDagNet *>(dag_net))) {
  } else if (OB_ISNULL(ctx_ = complete_dag_net->get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ctx is null", K(ret));
  } else {
    segment_idx_ = segment_idx;
    dag_net_ = dag_net;
    rpc_proxy_ = complete_dag_net->get_storage_rpc_proxy();
    processor_id_ = -1;
    fetch_handle_.reset();
    is_inited_ = true;
  }
  return ret;
}

int ObVecIndexSegmentCopyTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObVectorIndexAdaptorMigrationDag *const adaptor_dag = get_adaptor_dag_();
  const ObMigrationVectorIndexAdaptorMeta &adaptor_meta = adaptor_dag->get_adaptor_meta();
  LOG_INFO("[MIG VEC] vec index segment copy task begin", "ls_id", ctx_->arg_.ls_id_,
      "data_tablet_id", adaptor_meta.data_tablet_id_, K_(segment_idx),
      "total_segments", adaptor_dag->get_total_segment_count());

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] vec index segment copy task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    LOG_INFO("[MIG VEC] ls complete migration already failed, skip segment copy", K_(segment_idx), KPC(ctx_));
  } else if (adaptor_dag->is_failed()) {
    LOG_INFO("[MIG VEC] adaptor dag already failed, skip segment copy", K(ret), K_(segment_idx), KPC(adaptor_dag));
  } else {
    if (OB_FAIL(migrate_vec_index_segment_())) {
      LOG_WARN("[MIG VEC] failed to migrate vector index segment (snap/incr)", K(ret), K_(segment_idx));
    }
    ObSqlString extra_info_str;
    if (OB_TMP_FAIL(extra_info_str.append_fmt(
            "data_tablet_id:%ld;inc_tablet_id:%ld;total_segments:%ld;segment_idx:%ld;",
            adaptor_meta.data_tablet_id_.id(), adaptor_meta.inc_tablet_id_.id(),
            adaptor_dag->get_total_segment_count(), segment_idx_))) {
      LOG_WARN("[MIG VEC] failed to append segment copy event extra info", K(tmp_ret), K(adaptor_meta),
          K(segment_idx_));
    } else {
      SERVER_EVENT_ADD("storage_ha", "vector_index_migration_segment_copy", "tenant_id", ctx_->tenant_id_,
          "ls_id", ctx_->arg_.ls_id_.id(), "src", ctx_->arg_.src_.get_server(), "dst", ctx_->arg_.dst_.get_server(),
          "task_id", ctx_->task_id_, "result", ret, extra_info_str.ptr());
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("[MIG VEC] failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }

  LOG_INFO("[MIG VEC] vec index segment copy task end", K(ret), "ls_id", ctx_->arg_.ls_id_,
          "data_tablet_id", adaptor_meta.data_tablet_id_, K_(segment_idx),
          "cost_us", ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObVecIndexSegmentCopyTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  const int64_t next_segment_idx = segment_idx_ + 1;
  ObVectorIndexAdaptorMigrationDag *dag = get_adaptor_dag_();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] vec index segment copy task not init", K(ret));
  } else if (next_segment_idx >= dag->get_total_segment_count()) {
    // Reached the end of the chain
    ret = OB_ITER_END;
    LOG_INFO("[MIG VEC] no more segments, generate next task returns ITER END",
        K_(segment_idx), "total", dag->get_total_segment_count());
  } else {
    ObVecIndexSegmentCopyTask *copy_task = nullptr;
    if (OB_FAIL(dag->alloc_task(copy_task))) {
      LOG_WARN("[MIG VEC] failed to alloc copy task", K(ret));
    } else if (OB_FAIL(copy_task->init(next_segment_idx))) {
      LOG_WARN("[MIG VEC] failed to init copy task", K(ret), K(next_segment_idx));
    } else {
      next_task = copy_task;
      LOG_INFO("[MIG VEC] generate next task created new copy task", K(next_segment_idx));
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_VEC_INDEX_MIGRATION_COPY_TASK_FAILED);
ERRSIM_POINT_DEF(EN_VEC_INDEX_MIGRATION_DESERIALIZE_FAILED);
int ObVecIndexSegmentCopyTask::migrate_vec_index_segment_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObMigrationVectorIndexAdaptorMeta &adaptor_meta = get_adaptor_dag_()->get_adaptor_meta();
  const share::ObVectorIndexMeta &vec_index_meta = get_adaptor_dag_()->get_vec_index_meta();
  share::ObPluginVectorIndexAdapterGuard adaptor_guard;
  share::ObPluginVectorIndexAdaptor *adaptor = nullptr;
  share::ObVectorIndexSegmentHandle dest_segment_handle;
  int64_t deser_buf_size = 0;
  ObArenaAllocator deser_alloc("VIMigDeser");

  if (OB_FAIL(register_src_segment_processor_())) {
    LOG_WARN("[MIG VEC] failed to register src segment processor", K(ret), K_(segment_idx));
  } else if (OB_ISNULL(rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] rpc proxy is null", K(ret));
#ifdef ERRSIM
  } else if (OB_FAIL(EN_VEC_INDEX_MIGRATION_COPY_TASK_FAILED)) {
      LOG_INFO("[MIG VEC] errsim inject copy task failure", K(ret), K_(segment_idx), K_(processor_id));
      ObSqlString extra_info_str;
      if (OB_TMP_FAIL(extra_info_str.append_fmt("data_tablet_id:%ld;segment_idx:%ld;processor_id:%ld;",
              adaptor_meta.data_tablet_id_.id(), segment_idx_, processor_id_))) {
        LOG_WARN("[MIG VEC] failed to append errsim copy task failed event extra info",
            K(tmp_ret), K(ret), K(adaptor_meta), K(segment_idx_), K(processor_id_));
      } else {
        SERVER_EVENT_ADD("storage_ha", "vec_mig_errsim_copy_task_failed", "tenant_id", ctx_->tenant_id_,
            "ls_id", ctx_->arg_.ls_id_.id(), "src", ctx_->arg_.src_.get_server(), "dst", ctx_->arg_.dst_.get_server(),
            "task_id", ctx_->task_id_, "ret", ret, extra_info_str.ptr());
      }
#endif
  } else {
    bool skip_while_adaptor_deleted = false;
    LOG_INFO("[MIG VEC] fetch and consume segment begin",
        K_(segment_idx), K_(processor_id),
        "data_tablet_id", adaptor_meta.data_tablet_id_);
    if (OB_FAIL(create_fetch_handle_(deser_buf_size))) {
    } else if (OB_FAIL(get_dest_adaptor_guard_(adaptor_guard, adaptor, skip_while_adaptor_deleted))) {
    } else if (skip_while_adaptor_deleted) {
      LOG_INFO("[MIG VEC] adaptor not exist on dest, skip", K(ret), "ls_id", ctx_->arg_.ls_id_, K(adaptor_meta));
    } else if (OB_FAIL(validate_and_create_dest_segment_(adaptor, dest_segment_handle))) {
      LOG_WARN("[MIG VEC] validate and create dest segment failed", K(ret));
#ifdef ERRSIM
    } else if (OB_FAIL(EN_VEC_INDEX_MIGRATION_DESERIALIZE_FAILED)) {
      LOG_INFO("[MIG VEC] errsim inject deserialize failure", K(ret), K_(segment_idx));
      ObSqlString extra_info_str;
      if (OB_TMP_FAIL(extra_info_str.append_fmt("adaptor_handle_id:%ld;", get_adaptor_dag_()->get_adaptor_handle_id()))) {
        LOG_WARN("[MIG VEC] failed to append errsim deserialize failed event extra info",
            K(tmp_ret), K(ret), "adaptor_handle_id", get_adaptor_dag_()->get_adaptor_handle_id());
      } else {
        SERVER_EVENT_ADD("storage_ha", "vec_mig_errsim_deserialize_failed", "tenant_id", ctx_->tenant_id_,
            "ls_id", ctx_->arg_.ls_id_.id(), "src", ctx_->arg_.src_.get_server(), "dst", ctx_->arg_.dst_.get_server(),
            "task_id", ctx_->task_id_, "ret", ret, extra_info_str.ptr());
      }
#endif
    }

    if (OB_FAIL(ret) || skip_while_adaptor_deleted) {
    } else if (OB_FAIL(read_and_deserialize_meta_(fetch_handle_, deser_buf_size, deser_alloc, dest_segment_handle))) {
      LOG_WARN("[MIG VEC] read and deserialize meta failed", K(ret));
    } else if (OB_FAIL(stream_deserialize_vsag_(fetch_handle_, deser_buf_size, deser_alloc,
                   dest_segment_handle, segment_idx_))) {
      LOG_WARN("[MIG VEC] stream deserialize vsag failed", K(ret));
    } else if (OB_FAIL(dest_segment_handle->immutable_optimize())) {
      LOG_WARN("[MIG VEC] immutable optimize failed after deserialize", K(ret), K_(segment_idx));
    } else if (OB_FAIL(adaptor->install_migrated_segment_handle(
                   segment_idx_, vec_index_meta.flat_seg_meta_at(segment_idx_),
                   dest_segment_handle, get_adaptor_dag_()->get_meta_header(),
                   adaptor_meta.has_complete_))) {
      LOG_WARN("[MIG VEC] install migrated segment failed", K(ret), K_(segment_idx));
    } else {
      LOG_INFO("[MIG VEC] segment meta and data deserialization success", K_(segment_idx));
    }

    if (fetch_handle_.is_valid()) {
      fetch_handle_->stop();
      fetch_handle_.reset();
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("[MIG VEC] failed to fetch and consume segment", K(ret), K_(segment_idx), K_(processor_id));
    } else {
      LOG_INFO("[MIG VEC] fetch and consume segment done", K_(segment_idx), K_(processor_id),
        "data_tablet_id", adaptor_meta.data_tablet_id_);
    }
  }

  // release no matter success or failure
  if (OB_TMP_FAIL(notify_src_processor_done(*ctx_, rpc_proxy_, *get_adaptor_dag_(),
      processor_id_, ret))) {
    LOG_WARN("[MIG VEC] failed to notify src processor of migration done",
        K(tmp_ret), K(ret), K_(processor_id), K_(segment_idx));
  }
  return ret;
}

int ObVecIndexSegmentCopyTask::create_fetch_handle_(int64_t &deser_buf_size)
{
  int ret = OB_SUCCESS;
  deser_buf_size = 0;
  ObStorageHAService *ha_service = MTL(ObStorageHAService *);
  if (OB_ISNULL(ha_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ha service is null", K(ret));
  } else {
    ObMigrationTenantWindowMgr &dest_mgr = ha_service->get_vector_index_migration_window_mgr();
    ObVecIdxFetchRpcCtx rpc_ctx;
    rpc_ctx.rpc_proxy_ = rpc_proxy_;
    rpc_ctx.src_addr_ = ctx_->chosen_src_.src_addr_;
    rpc_ctx.cluster_id_ = ctx_->chosen_src_.cluster_id_;
    rpc_ctx.tenant_id_ = ctx_->tenant_id_;
    rpc_ctx.ls_id_ = ctx_->arg_.ls_id_;
    rpc_ctx.processor_id_ = processor_id_;
    rpc_ctx.rpc_timeout_us_ = ObStorageHAUtils::get_rpc_timeout();
    rpc_ctx.ls_rebuild_seq_ = ctx_->src_ls_rebuild_seq_;

    if (OB_FAIL(ObVecIdxSegFetchDriver::create( rpc_ctx, &dest_mgr,
                     get_adaptor_dag_()->get_priority(), fetch_handle_))) {
      LOG_WARN("[MIG VEC] failed to create fetch driver", K(ret), K(rpc_ctx));
    } else if (OB_FAIL(fetch_handle_->start_initial_window())) {
      LOG_WARN("[MIG VEC] initial rpc send failed", K(ret));
    } else if (OB_UNLIKELY(!fetch_handle_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[MIG VEC] fetch handle invalid after create", K(ret));
    } else if (FALSE_IT(deser_buf_size = fetch_handle_->get_slot_buf_size())) {
    } else if (OB_UNLIKELY(deser_buf_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[MIG VEC] invalid slot buf size", K(ret), K(deser_buf_size));
    }
  }
  return ret;
}

int ObVecIndexSegmentCopyTask::get_dest_adaptor_guard_(
    share::ObPluginVectorIndexAdapterGuard &adaptor_guard,
    share::ObPluginVectorIndexAdaptor *&adaptor,
    bool &skip_while_adaptor_deleted)
{
  int ret = OB_SUCCESS;
  skip_while_adaptor_deleted = false;
  adaptor = nullptr;
  share::ObPluginVectorIndexService *vi_service = MTL(share::ObPluginVectorIndexService *);
  const ObMigrationVectorIndexAdaptorMeta &adaptor_meta = get_adaptor_dag_()->get_adaptor_meta();
  if (OB_ISNULL(vi_service)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[MIG VEC] vector index service not available on dest", K(ret));
  } else if (OB_FAIL(vi_service->get_adapter_inst_guard(
                 ctx_->arg_.ls_id_, adaptor_meta.inc_tablet_id_, adaptor_guard))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("[MIG VEC] adaptor not exist on dest, skip", K(ret), "ls_id", ctx_->arg_.ls_id_, K(adaptor_meta));
      skip_while_adaptor_deleted = true;
    } else {
      LOG_WARN("[MIG VEC] failed to acquire adaptor guard on dest", K(ret),
        "ls_id", ctx_->arg_.ls_id_, K(adaptor_meta));
    }
  } else if (OB_ISNULL(adaptor = adaptor_guard.get_adatper())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] adaptor is null on dest", K(ret));
  }
  return ret;
}

int ObVecIndexSegmentCopyTask::register_src_segment_processor_()
{
  int ret = OB_SUCCESS;
  ObVectorIndexAdaptorMigrationDag *dag = get_adaptor_dag_();
  const ObMigrationVectorIndexAdaptorMeta &adaptor_meta = dag->get_adaptor_meta();

  if (OB_ISNULL(rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] rpc proxy is null", K(ret));
  } else {
    const int64_t register_timeout_us = ObVectorIndexMigrationProcessorMgr::PROCESSOR_AND_ADAPTOR_TIMEOUT_US;
    const int64_t register_start_ts = ObTimeUtil::current_time();
    while (OB_SUCC(ret)) {
      obrpc::ObRegisterVecIndexMigrationProcessorArg arg;
      obrpc::ObRegisterVecIndexMigrationProcessorRes res;
      arg.tenant_id_ = ctx_->tenant_id_;
      arg.ls_id_ = ctx_->arg_.ls_id_;
      arg.adaptor_handle_id_ = dag->get_adaptor_handle_id();
      arg.segment_idx_ = segment_idx_;
      arg.ls_rebuild_seq_ = ctx_->src_ls_rebuild_seq_;
      if (OB_FAIL(rpc_proxy_->to(ctx_->chosen_src_.src_addr_)
              .dst_cluster_id(ctx_->chosen_src_.cluster_id_)
              .by(ctx_->tenant_id_)
              .timeout(ObStorageHAUtils::get_rpc_timeout())
              .group_id(share::OBCG_STORAGE)
              .register_vec_index_migration_processor(arg, res))) {
        if (OB_EAGAIN == ret) {
          if (ObTimeUtil::current_time() - register_start_ts >= register_timeout_us) {
            ret = OB_TIMEOUT;
            LOG_WARN("[MIG VEC] register src processor timeout after retries", K(ret),
                K_(segment_idx), K(register_timeout_us));
          } else {
            ret = OB_SUCCESS;
            LOG_INFO("[MIG VEC] src vector index controller cap reached, retry after backoff", K_(segment_idx));
            ob_usleep(ObVectorIndexAdaptorMigrationDag::RETRY_BACKOFF_US);
          }
        } else {
          LOG_WARN("[MIG VEC] failed to register src processor for segment", K(ret), K(arg),
                K_(segment_idx));
        }
      } else {
        processor_id_ = res.processor_id_;
        LOG_INFO("[MIG VEC] register src segment processor success",
            "ls_id", ctx_->arg_.ls_id_,
            "data_tablet_id", adaptor_meta.data_tablet_id_,
            K_(segment_idx), "processor_id", res.processor_id_);
        break;
      }
    }
  }
  return ret;
}

// ======================== Stream deserialization context & helpers ========================
bool ObVecIdxMigVsagStreamCbParam::is_valid() const
{
  return driver_handle_.is_valid()
      && OB_NOT_NULL(slot_buf_)
      && slot_cap_ > 0
      && OB_INVALID_ID != expected_processor_id_
      && OB_INVALID_TENANT_ID != expected_tenant_id_
      && expected_ls_id_.is_valid()
      && OB_NOT_NULL(adaptor_dag_);
}

int ObVecIdxMigVsagStreamReader::operator()(
    char *&data,
    const int64_t /*data_size*/,
    int64_t &read_size,
    share::ObIStreamBuf::CbParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(share::ObIStreamBuf::CbParam::CbParamType::VEC_MIG_VSAG_STREAM != param.get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[VEC MIG] unexpected CbParam type", K(ret), "type", static_cast<int64_t>(param.get_type()));
  } else {
    ObVecIdxMigVsagStreamCbParam &cb_param = static_cast<ObVecIdxMigVsagStreamCbParam &>(param);
    if (OB_UNLIKELY(!cb_param.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[VEC MIG] invalid vsag stream cb param", K(ret), K(cb_param.driver_handle_),
              KP(cb_param.slot_buf_), K(cb_param.slot_cap_), K(cb_param.expected_processor_id_),
              K(cb_param.expected_tenant_id_), K(cb_param.expected_ls_id_));
    } else if (cb_param.adaptor_dag_->is_migration_cancelled()) {
      ret = OB_CANCELED;
      LOG_INFO("[MIG VEC] vsag stream cancelled due to dag/dag-net failure",
          K(ret), KP(cb_param.adaptor_dag_));
    } else {
      int64_t chunk_len = 0;
      if (OB_FAIL(cb_param.driver_handle_->wait_and_get_data(cb_param.slot_buf_, cb_param.slot_cap_, chunk_len))) {
        LOG_WARN("[VEC MIG] failed to get next consume data", K(ret));
      } else {
        // Strip per-chunk header from each fresh chunk.
        ObVecIdxMigChunkHeader header;
        int64_t header_size = 0;
        if (OB_FAIL(header.read(cb_param.slot_buf_, chunk_len,
                                cb_param.expected_processor_id_,
                                cb_param.expected_tenant_id_,
                                cb_param.expected_ls_id_,
                                header_size))) {
          LOG_WARN("[VEC MIG] read vsag chunk header fail", K(ret), K(chunk_len));
        } else if (OB_UNLIKELY(ObVecIdxMigChunkHeader::ChunkType::VSAG != header.chunk_type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[VEC MIG] unexpected chunk type during vsag phase", K(ret), K(header));
        } else {
          data = cb_param.slot_buf_ + header_size;
          read_size = chunk_len - header_size;
        }
      }
    }
  }
  return ret;
}

int ObVecIndexSegmentCopyTask::validate_and_create_dest_segment_(
    share::ObPluginVectorIndexAdaptor *adaptor,
    share::ObVectorIndexSegmentHandle &dest_segment_handle)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = ctx_->tenant_id_;
  int max_degree = 0;
  share::ObVectorIndexAlgorithmType build_type = share::VIAT_MAX;

  ObVectorIndexAdaptorMigrationDag *dag = get_adaptor_dag_();
  const share::ObVectorIndexMeta &vec_index_meta = dag->get_vec_index_meta();

  if (segment_idx_ < 0 || segment_idx_ >= vec_index_meta.segment_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[MIG VEC] segment idx out of range", K(ret), K_(segment_idx),
        "seg_metas_count", vec_index_meta.segment_count());
  } else {
    // Get build_type from source segment meta (fetched via fetch_vector_index_segment_metas RPC)
    const share::ObVectorIndexSegmentMeta &seg_meta = vec_index_meta.flat_seg_meta_at(segment_idx_);
    build_type = seg_meta.index_type_;
    const uint16_t build_type_u = static_cast<uint16_t>(build_type);
    if (build_type_u >= static_cast<uint16_t>(share::VIAT_MAX)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[MIG VEC] invalid segment build type from source", K(ret), K(build_type_u));
    }
  }

  // Compute max_degree locally from dest adaptor's hnsw_param + build_type
  share::ObVectorIndexParam *hnsw_param = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(adaptor->get_hnsw_param(hnsw_param))) {
    LOG_WARN("[MIG VEC] failed to get hnsw param on dest", K(ret));
  } else if (OB_ISNULL(hnsw_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] hnsw param null", K(ret));
  } else {
    max_degree = share::ObVectorIndexUtil::get_hnsw_max_degree(
        hnsw_param->type_, build_type, hnsw_param->m_);

    if (OB_FAIL(share::ObVectorIndexSegment::create(
        dest_segment_handle, tenant_id, *adaptor->get_allocator(),
        *hnsw_param, build_type, max_degree, adaptor))) {
      LOG_WARN("[MIG VEC] create dest segment with source params failed", K(ret),
          K(build_type), K(max_degree));
    }
  }
  return ret;
}

int ObVecIndexSegmentCopyTask::read_and_deserialize_meta_(
    ObVecIdxSegFetchDriverHandle &handle,
    int64_t buf_size,
    common::ObIAllocator &alloc,
    share::ObVectorIndexSegmentHandle &dest_segment_handle)
{
  int ret = OB_SUCCESS;
  share::ObVectorIndexSegment *seg = dest_segment_handle.get();
  if (OB_ISNULL(seg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] dest segment is null", K(ret));
  } else {
    char *chunk_buf = static_cast<char *>(alloc.alloc(buf_size));
    if (OB_ISNULL(chunk_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[MIG VEC] failed to alloc chunk buf", K(ret), K(buf_size));
    } else {
      ObVecIdxMigMetaAccumulator accumulator(alloc);
      ObVectorIndexAdaptorMigrationDag *const dag = get_adaptor_dag_();
      ObVecIdxMigMetaChunkHeader header;

      // Loop terminates when accumulator has received exactly meta_len_ bytes.
      while (OB_SUCC(ret) && !accumulator.is_complete()) {
        header.reset();
        int64_t header_size = 0;
        int64_t chunk_len = 0;
        if (dag->is_migration_cancelled()) {
          ret = OB_CANCELED;
          LOG_INFO("[MIG VEC] meta loop cancelled due to dag/dag-net failure", K(ret), KP(dag));
        } else if (OB_FAIL(handle->wait_and_get_data(chunk_buf, buf_size, chunk_len))) {
          LOG_WARN("[MIG VEC] failed to get next chunk in meta loop", K(ret));
        } else if (OB_UNLIKELY(chunk_len <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[MIG VEC] stream exhausted unexpectedly", K(ret), K(chunk_len));
        } else if (OB_FAIL(header.read(chunk_buf, chunk_len, processor_id_,
                   ctx_->tenant_id_, ctx_->arg_.ls_id_, header_size))) {
          LOG_WARN("[MIG VEC] read chunk header fail in meta loop", K(ret), K(chunk_len));
        } else if (OB_UNLIKELY(ObVecIdxMigChunkHeader::ChunkType::META != header.chunk_type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[MIG VEC] expected meta chunk in meta loop", K(ret), K(header));
        } else if (OB_FAIL(accumulator.feed(header, chunk_buf + header_size, chunk_len - header_size))) {
          LOG_WARN("[MIG VEC] feed meta accumulator fail", K(ret), K(header));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t deserialize_pos = 0;
        if (OB_FAIL(seg->deserialize_meta(accumulator.meta_buf(), accumulator.meta_size(), deserialize_pos))) {
          LOG_WARN("[MIG VEC] failed to deserialize meta", K(ret), "meta_size", accumulator.meta_size(), K(deserialize_pos));
        } else if (deserialize_pos != accumulator.meta_size()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[MIG VEC] meta deserialize size mismatch", K(ret), K(deserialize_pos), "meta_size", accumulator.meta_size());
        }
      }
    }
  }

  return ret;
}

int ObVecIdxMigMetaAccumulator::feed(const ObVecIdxMigMetaChunkHeader &header,
                                     const char *body, int64_t body_len)
{
  int ret = OB_SUCCESS;
  if (!header.is_valid() || body_len <= 0 || OB_ISNULL(body)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[MIG VEC] invalid args feeding meta accumulator", K(ret), K(header), KP(body), K(body_len));
  } else if (OB_ISNULL(meta_buf_)) { // First meta chunk: allocate exactly meta_len_ bytes per source header.
    if (OB_UNLIKELY(body_len > header.meta_len_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[MIG VEC] first meta chunk body exceeds meta_len", K(ret), K(header), K(body_len));
    } else if (OB_ISNULL(meta_buf_ = static_cast<char *>(alloc_.alloc(header.meta_len_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[MIG VEC] failed to alloc meta buf", K(ret), "meta_len", header.meta_len_);
    } else {
      meta_len_ = header.meta_len_;
    }
  } else if (OB_UNLIKELY(header.meta_len_ != meta_len_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] meta_len mismatch across chunks", K(ret), K(header), K_(meta_len));
  }

  if (OB_SUCC(ret)) {
    const int64_t new_size = meta_size_ + body_len;
    if (OB_UNLIKELY(new_size > meta_len_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[MIG VEC] accumulated meta exceeds meta_len", K(ret), K_(meta_size), K(body_len), K_(meta_len));
    } else {
      MEMCPY(meta_buf_ + meta_size_, body, body_len);
      meta_size_ = new_size;
    }
  }
  return ret;
}

int ObVecIndexSegmentCopyTask::stream_deserialize_vsag_(
    ObVecIdxSegFetchDriverHandle &handle,
    int64_t buf_size,
    common::ObIAllocator &alloc,
    share::ObVectorIndexSegmentHandle &dest_segment_handle,
    const int64_t segment_idx)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = ctx_->tenant_id_;

  ObVecIdxMigVsagStreamCbParam vsag_cb_param;
  vsag_cb_param.driver_handle_ = handle;
  vsag_cb_param.slot_buf_ = static_cast<char *>(alloc.alloc(buf_size));
  vsag_cb_param.slot_cap_ = buf_size;
  vsag_cb_param.expected_processor_id_ = processor_id_;
  vsag_cb_param.expected_tenant_id_ = ctx_->tenant_id_;
  vsag_cb_param.expected_ls_id_ = ctx_->arg_.ls_id_;
  vsag_cb_param.adaptor_dag_ = get_adaptor_dag_();
  if (OB_ISNULL(vsag_cb_param.slot_buf_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[MIG VEC] alloc vsag slot fail", K(ret));
  } else {
    ObVecIdxMigVsagStreamReader vsag_reader;
    share::ObIStreamBuf::Callback vsag_cb = vsag_reader;
    share::ObVectorIndexSerializer serializer(alloc);
    if (OB_FAIL(serializer.deserialize(dest_segment_handle, vsag_cb_param, vsag_cb, tenant_id))) {
      LOG_WARN("[MIG VEC] failed to deserialize vsag after meta", K(ret), K(segment_idx));
    } else {
      LOG_INFO("[MIG VEC] segment deserialization success", K(segment_idx));
    }
  }
  return ret;
}

// ======================== ObVecIndexFinishTask ========================

ObVecIndexFinishTask::ObVecIndexFinishTask()
  : ObITask(TASK_TYPE_VEC_INDEX_ADAPTOR_FINISH),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr),
    rpc_proxy_(nullptr)
{
}

ObVecIndexFinishTask::~ObVecIndexFinishTask()
{
}

int ObVecIndexFinishTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSCompleteMigrationDagNet *complete_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[MIG VEC] vec index finish task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] dag net should not be NULL", K(ret));
  } else if (ObDagNetType::DAG_NET_TYPE_COMPLETE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] dag net type is unexpected", K(ret));
  } else if (FALSE_IT(complete_dag_net = static_cast<ObLSCompleteMigrationDagNet*>(dag_net))) {
  } else if (OB_ISNULL(ctx_ = complete_dag_net->get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ctx is null", K(ret));
  } else {
    dag_net_ = dag_net;
    rpc_proxy_ = complete_dag_net->get_storage_rpc_proxy();
    is_inited_ = true;
  }
  return ret;
}

int ObVecIndexFinishTask::enqueue_mem_sync_task_(
  const ObMigrationVectorIndexAdaptorMeta &adaptor_meta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(share::ObPluginVectorIndexUtils::enqueue_mem_sync_task(
          ctx_->arg_.ls_id_, adaptor_meta.inc_tablet_id_, adaptor_meta.inc_table_id_))) {
    LOG_WARN("[VEC MIG] failed to enqueue mem sync task",
        K(ret), K(adaptor_meta));
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_VEC_INDEX_MIGRATION_FINISH_TASK_FAILED);
int ObVecIndexFinishTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObVectorIndexAdaptorMigrationDag *dag = get_adaptor_dag_();
  const ObMigrationVectorIndexAdaptorMeta &adaptor_meta = dag->get_adaptor_meta();
  const int64_t adaptor_handle_id = dag->get_adaptor_handle_id();
  LOG_INFO("[MIG VEC] vec index finish task begin",
      "ls_id", ctx_->arg_.ls_id_,
      "data_tablet_id", adaptor_meta.data_tablet_id_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[MIG VEC] vec index finish task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    LOG_INFO("[MIG VEC] ls complete migration already failed, skip finish task",
        KPC(ctx_));
  } else {
#ifdef ERRSIM
    bool is_in_retry = false;
    if (OB_TMP_FAIL(dag->check_is_in_retry(is_in_retry))) {
      LOG_WARN("[MIG VEC] errsim failed to check is in retry", K(tmp_ret));
    } else if (!is_in_retry) {
      if (OB_FAIL(EN_VEC_INDEX_MIGRATION_FINISH_TASK_FAILED)) {
        LOG_INFO("[MIG VEC] ERRSIM EN_VEC_INDEX_MIGRATION_FINISH_TASK_FAILED", K(ret),
            K(adaptor_handle_id));
        ObSqlString extra_info_str;
        if (OB_TMP_FAIL(extra_info_str.append_fmt("adaptor_handle_id:%ld;", adaptor_handle_id))) {
          LOG_WARN("[MIG VEC] failed to append errsim finish task failed event extra info",
              K(tmp_ret), K(ret), K(adaptor_handle_id));
        } else {
          SERVER_EVENT_ADD("storage_ha", "vec_mig_errsim_finish_task_failed", "tenant_id", ctx_->tenant_id_,
              "ls_id", ctx_->arg_.ls_id_.id(), "src", ctx_->arg_.src_.get_server(), "dst", ctx_->arg_.dst_.get_server(),
              "task_id", ctx_->task_id_, "ret", ret, extra_info_str.ptr());
        }
      }
    }
#endif
    if (OB_SUCC(ret)) {
      if (OB_FAIL(enqueue_mem_sync_task_(adaptor_meta))) {
        LOG_WARN("[MIG VEC] failed to enqueue mem sync task", K(ret));
      } else if (OB_TMP_FAIL(record_server_event_())) {
        LOG_WARN("[MIG VEC] failed to record server event", K(tmp_ret), K(ret));
      }
    }
  }

  if (OB_TMP_FAIL(dag->release_src_adaptor_handle())) {
    LOG_WARN("[MIG VEC] failed to release src adaptor handle in finish task", K(tmp_ret), K(ret), K(adaptor_handle_id));
  }

  if (OB_FAIL(ret)) {
    LOG_INFO("[MIG VEC] finish task failed, calling deal_with_fo",
        K(ret), "ls_id", ctx_->arg_.ls_id_, K(adaptor_handle_id),
        "inc_tablet_id", adaptor_meta.inc_tablet_id_,
        "data_tablet_id", adaptor_meta.data_tablet_id_);
    if (OB_TMP_FAIL(ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("[MIG VEC] failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  } else if (OB_FAIL(share::ObPluginVectorIndexUtils::set_adaptor_mig_state(
                 ctx_->arg_.ls_id_, adaptor_meta.inc_tablet_id_, share::ObAdaptorMigState::MIG_SUCC))) {
    LOG_WARN("[MIG VEC] failed to set MIG_SUCC", K(ret), K(adaptor_meta));
  }

  LOG_INFO("[MIG VEC] vec index finish task end", K(ret), "ls_id", ctx_->arg_.ls_id_,
          "data_tablet_id", adaptor_meta.data_tablet_id_, "cost_us", ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObVecIndexFinishTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] ctx should not be null", K(ret));
  } else {
    const ObMigrationVectorIndexAdaptorMeta &adaptor_meta = get_adaptor_dag_()->get_adaptor_meta();
    ObSqlString extra_info_str;
    if (OB_FAIL(extra_info_str.append_fmt("inc_tablet_id:%ld;adaptor_handle_id:%ld;",
            adaptor_meta.inc_tablet_id_.id(), get_adaptor_dag_()->get_adaptor_handle_id()))) {
      LOG_WARN("[MIG VEC] failed to append adaptor copy finish event extra info", K(ret), K(adaptor_meta));
    } else {
      SERVER_EVENT_ADD("storage_ha", "vector_index_migration_adaptor_copy_finish", "tenant_id", ctx_->tenant_id_,
          "ls_id", ctx_->arg_.ls_id_.id(), "src", ctx_->arg_.src_.get_server(), "dst", ctx_->arg_.dst_.get_server(),
          "task_id", ctx_->task_id_, "data_tablet_id", adaptor_meta.data_tablet_id_.id(), extra_info_str.ptr());
    }
  }
  return ret;
}
