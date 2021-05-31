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

#include "ob_trans_service.h"

#include "lib/profile/ob_perf_event.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/ob_name_id_def.h"
#include "lib/ob_running_mode.h"
#include "ob_trans_coord_ctx.h"
#include "ob_trans_ctx.h"
#include "ob_trans_factory.h"
#include "ob_trans_functor.h"
#include "ob_trans_log.h"
#include "ob_trans_msg_type.h"
#include "ob_trans_msg_type2.h"
#include "ob_trans_part_ctx.h"
#include "ob_trans_result.h"
#include "ob_trans_sche_ctx.h"
#include "ob_trans_slave_ctx.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_multi_cluster_util.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_i_store.h"
#include "clog/ob_clog_mgr.h"
#include "ob_trans_elr_task.h"
#include "ob_trans_task_worker.h"
#include "ob_i_weak_read_service.h"  // ObIWeakReadService
#include "sql/session/ob_basic_session_info.h"
#include "ob_weak_read_util.h"  // ObWeakReadUtil
#include "storage/memtable/ob_memtable_context.h"
#include "ob_trans_msg2.h"

namespace oceanbase {

using namespace obrpc;
using namespace common;
using namespace share;
using namespace storage;
using namespace memtable;
using namespace sql;
using namespace observer;

namespace transaction {
ObTransService::ObTransService()
    : is_inited_(false),
      is_running_(false),
      use_def_(true),
      rpc_(&rpc_def_),
      location_adapter_(&location_adapter_def_),
      trans_migrate_worker_(&trans_migrate_worker_def_),
      clog_adapter_(&clog_adapter_def_),
      partition_service_(NULL),
      schema_service_(NULL),
      ts_mgr_(NULL),
      server_tracer_(NULL),
      input_queue_count_(0),
      output_queue_count_(0)
{
  check_env_();
}

int ObTransService::init(const ObAddr& self, ObIPartitionLocationCache* location_cache,
    ObPartitionService* partition_service, ObTransRpcProxy* rpc_proxy, obrpc::ObBatchRpc* batch_rpc,
    ObDupTableRpcProxy* dup_table_rpc_proxy, ObXARpcProxy* xa_proxy,
    share::schema::ObMultiVersionSchemaService* schema_service, ObITsMgr* ts_mgr,
    share::ObAliveServerTracer& server_tracer)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransService inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(!self.is_valid()) || OB_ISNULL(location_cache) || OB_ISNULL(partition_service) ||
             OB_ISNULL(rpc_proxy) || OB_ISNULL(batch_rpc) || OB_ISNULL(schema_service) || OB_ISNULL(ts_mgr)) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(self),
        KP(location_cache),
        KP(schema_service),
        KP(partition_service),
        KP(rpc_proxy),
        KP(batch_rpc),
        KP(ts_mgr));
    ret = OB_INVALID_ARGUMENT;
    // init transaction timer
  } else if (OB_FAIL(timer_.init())) {
    TRANS_LOG(ERROR, "timer init error", KR(ret));
    // init rpc
  } else if (OB_FAIL(dup_table_lease_timer_.init())) {
    TRANS_LOG(ERROR, "dup table lease timer init error", K(ret));
  } else if (OB_FAIL(rpc_def_.init(rpc_proxy, this, self, batch_rpc))) {
    TRANS_LOG(ERROR, "rpc init error", KR(ret));
    // init dup table rpc
  } else if (OB_FAIL(dup_table_rpc_.init(this, dup_table_rpc_proxy))) {
    TRANS_LOG(ERROR, "dup table rpc init error", KR(ret));
    // init xa rpc
  } else if (OB_FAIL(xa_rpc_.init(xa_proxy, self))) {
    TRANS_LOG(WARN, "xa rpc init error", KR(ret));
    // init location adapter
  } else if (OB_FAIL(location_adapter_def_.init(location_cache, schema_service))) {
    TRANS_LOG(ERROR, "location adapter init error", KR(ret));
    // init clog adapter
  } else if (OB_FAIL(clog_adapter_def_.init(partition_service))) {
    TRANS_LOG(ERROR, "clog adapter init error", KR(ret));
  } else if (OB_FAIL(trans_migrate_worker_def_.init())) {
    TRANS_LOG(ERROR, "trans migrate worker init error", KR(ret));
  } else if (OB_FAIL(xa_trans_heartbeat_worker_.init(this))) {
    TRANS_LOG(ERROR, "xa trans relocate worker init error", KR(ret));
  } else if (OB_FAIL(xa_inner_table_gc_worker_.init(this))) {
    TRANS_LOG(WARN, "xa inner table gc worker init error", KR(ret));
    // init scheduler transaction context manager
  } else if (OB_FAIL(sche_trans_ctx_mgr_.init(ts_mgr, partition_service))) {
    TRANS_LOG(WARN, "scheduler transaction context manager init error", KR(ret));
    // init coordinator transaction context manager
  } else if (OB_FAIL(coord_trans_ctx_mgr_.init(ts_mgr, partition_service))) {
    TRANS_LOG(WARN, "coordinator transaction context manager init error", KR(ret));
    // init participant transaction context manager
  } else if (OB_FAIL(part_trans_ctx_mgr_.init(ObTransCtxType::PARTICIPANT, ts_mgr, partition_service))) {
    TRANS_LOG(WARN, "participant transaction context manager init error", KR(ret));
  } else if (OB_FAIL(slave_part_trans_ctx_mgr_.init(ObTransCtxType::SLAVE_PARTICIPANT, ts_mgr, partition_service))) {
    TRANS_LOG(WARN, "slave participant transaction context manager init error", KR(ret));
  } else if (OB_FAIL(ObSimpleThreadPool::init(
                 6, !lib::is_mini_mode() ? MAX_MSG_TASK : MINI_MODE_MAX_MSG_TASK, "TransService"))) {
    TRANS_LOG(WARN, "thread pool init error", KR(ret));
  } else if (OB_FAIL(big_trans_worker_.init())) {
    TRANS_LOG(WARN, "big trans worker init failed", KR(ret));
  } else if (OB_FAIL(msg_handler_.init(this, &part_trans_ctx_mgr_, &coord_trans_ctx_mgr_, location_adapter_, rpc_))) {
    TRANS_LOG(WARN, "ObTransMsgHandler init error", K(ret));
  } else if (OB_FAIL(light_trans_ctx_mgr_.init(&part_trans_ctx_mgr_))) {
    TRANS_LOG(WARN, "ObLightTransCtxMgr init error", K(ret));
  } else {
    partition_service_ = partition_service;
    schema_service_ = schema_service;
    self_ = self;
    ts_mgr_ = ts_mgr;
    server_tracer_ = &server_tracer;
    is_inited_ = true;
    TRANS_LOG(INFO, "transaction service inited success", KP(this));
  }

  return ret;
}

// for test only
int ObTransService::init(const ObAddr& self, ObITransRpc* rpc, ObILocationAdapter* location_adapter,
    ObIClogAdapter* clog_adapter, ObPartitionService* partition_service,
    share::schema::ObMultiVersionSchemaService* schema_service, ObITsMgr* ts_mgr)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransService inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(!self.is_valid()) || OB_ISNULL(rpc) || OB_ISNULL(location_adapter) ||
             OB_ISNULL(clog_adapter) || OB_ISNULL(partition_service) || OB_ISNULL(schema_service) ||
             OB_ISNULL(ts_mgr)) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(self),
        KP(rpc),
        KP(location_adapter),
        KP(clog_adapter),
        KP(partition_service),
        KP(schema_service),
        KP(ts_mgr));
    ret = OB_INVALID_ARGUMENT;
    // init transaction timer
  } else if (OB_FAIL(timer_.init())) {
    TRANS_LOG(ERROR, "timer init error", K(ret));
  } else if (OB_FAIL(dup_table_lease_timer_.init())) {
    TRANS_LOG(ERROR, "dup table lease timer init error", K(ret));
  } else if (OB_FAIL(sche_trans_ctx_mgr_.init(ts_mgr, partition_service))) {
    TRANS_LOG(WARN, "scheduler transaction context manager init error", KR(ret));
  } else if (OB_FAIL(coord_trans_ctx_mgr_.init(ts_mgr, partition_service))) {
    TRANS_LOG(WARN, "coordinator transaction context manager init error", KR(ret));
  } else if (OB_FAIL(part_trans_ctx_mgr_.init(ObTransCtxType::PARTICIPANT, ts_mgr, partition_service))) {
    TRANS_LOG(WARN, "participant transaction context manager init error", KR(ret));
  } else if (OB_FAIL(slave_part_trans_ctx_mgr_.init(ObTransCtxType::SLAVE_PARTICIPANT, ts_mgr, partition_service))) {
    TRANS_LOG(WARN, "slave participant transaction context manager init error", KR(ret));
  } else if (OB_FAIL(trans_migrate_worker_def_.init())) {
    TRANS_LOG(ERROR, "trans migrate worker init error", KR(ret));
  } else if (OB_FAIL(xa_trans_heartbeat_worker_.init(this))) {
    TRANS_LOG(ERROR, "xa trans relocate worker init error", KR(ret));
  } else if (OB_FAIL(xa_inner_table_gc_worker_.init(this))) {
    TRANS_LOG(WARN, "xa inner table gc worker init error", KR(ret));
  } else if (OB_FAIL(ObSimpleThreadPool::init(2, MAX_MSG_TASK, "TransService"))) {
    TRANS_LOG(WARN, "thread pool init error", KR(ret));
  } else if (OB_FAIL(big_trans_worker_.init())) {
    TRANS_LOG(WARN, "big trans worker init failed", KR(ret));
  } else if (OB_FAIL(light_trans_ctx_mgr_.init(&part_trans_ctx_mgr_))) {
    TRANS_LOG(WARN, "ObLightTransCtxMgr init error", K(ret));
  } else {
    use_def_ = false;
    rpc_ = rpc;
    location_adapter_ = location_adapter;
    clog_adapter_ = clog_adapter;
    partition_service_ = partition_service;
    schema_service_ = schema_service;
    self_ = self;
    ts_mgr_ = ts_mgr;
    is_inited_ = true;
    TRANS_LOG(INFO, "transaction service inited success", KP(this));
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
  } else if (OB_FAIL(dup_table_lease_timer_.start())) {
    TRANS_LOG(ERROR, "dup table lease timer start error", K(ret));
  } else if (OB_FAIL(rpc_->start())) {
    TRANS_LOG(WARN, "ObTransRpc start error", KR(ret));
  } else if (OB_FAIL(dup_table_rpc_.start())) {
    TRANS_LOG(WARN, "ObDupTableRpc start error", K(ret));
  } else if (OB_FAIL(xa_rpc_.start())) {
    TRANS_LOG(WARN, "ObXARpc start error", K(ret));
  } else if (OB_FAIL(clog_adapter_->start())) {
    TRANS_LOG(WARN, "ObClogAdapter start error", KR(ret));
  } else if (OB_FAIL(trans_migrate_worker_->start())) {
    TRANS_LOG(WARN, "ObTransMigrateWorker start error", KR(ret));
  } else if (OB_FAIL(xa_trans_heartbeat_worker_.start())) {
    TRANS_LOG(WARN, "ObXATransHeartbeatWorker start error", KR(ret));
  } else if (OB_FAIL(big_trans_worker_.start())) {
    TRANS_LOG(WARN, "big trans worker start error", KR(ret));
  } else if (OB_FAIL(sche_trans_ctx_mgr_.start())) {
    TRANS_LOG(WARN, "ObScheTransCtxMgr start error", KR(ret));
  } else if (OB_FAIL(coord_trans_ctx_mgr_.start())) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr start error", KR(ret));
  } else if (OB_FAIL(part_trans_ctx_mgr_.start())) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr start error", KR(ret));
  } else if (OB_FAIL(slave_part_trans_ctx_mgr_.start())) {
    TRANS_LOG(WARN, "slave ObPartTransCtxMgr start error", KR(ret));
  } else if (OB_FAIL(xa_inner_table_gc_worker_.start())) {
    TRANS_LOG(WARN, "xa inner table gc worker start error", KR(ret));
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "transaction service start success");
  }

  return ret;
}

void ObTransService::stop()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService already has stopped");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(sche_trans_ctx_mgr_.stop())) {
    TRANS_LOG(WARN, "ObScheTransCtxMgr stop error", KR(ret));
  } else if (OB_FAIL(coord_trans_ctx_mgr_.stop())) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr stop error", KR(ret));
  } else if (OB_FAIL(part_trans_ctx_mgr_.stop())) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr stop error", KR(ret));
  } else if (OB_FAIL(slave_part_trans_ctx_mgr_.stop())) {
    TRANS_LOG(WARN, "slave ObPartTransCtxMgr stop error", KR(ret));
  } else if (OB_FAIL(timer_.stop())) {
    TRANS_LOG(WARN, "ObTransTimer stop error", K(ret));
  } else if (OB_FAIL(dup_table_lease_timer_.stop())) {
    TRANS_LOG(ERROR, "dup table lease timer stop error", K(ret));
  } else {
    rpc_->stop();
    dup_table_rpc_.stop();
    clog_adapter_->stop();
    trans_migrate_worker_->stop();
    xa_trans_heartbeat_worker_.stop();
    ObSimpleThreadPool::stop();
    big_trans_worker_.stop();
    xa_inner_table_gc_worker_.stop();
    is_running_ = false;
    TRANS_LOG(INFO, "transaction service stop success");
  }
}

void ObTransService::wait()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_running_)) {
    TRANS_LOG(WARN, "ObTransService is running");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(sche_trans_ctx_mgr_.wait())) {
    TRANS_LOG(WARN, "ObScheTransCtxMgr wait error", KR(ret));
  } else if (OB_FAIL(coord_trans_ctx_mgr_.wait())) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr wait error", KR(ret));
  } else if (OB_FAIL(part_trans_ctx_mgr_.wait())) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr wait error", KR(ret));
  } else if (OB_FAIL(slave_part_trans_ctx_mgr_.wait())) {
    TRANS_LOG(WARN, "slave ObPartTransCtxMgr wait error", KR(ret));
  } else if (OB_FAIL(timer_.wait())) {
    TRANS_LOG(WARN, "ObTransTimer wait error", K(ret));
  } else if (OB_FAIL(dup_table_lease_timer_.wait())) {
    TRANS_LOG(ERROR, "dup table lease timer wait error", K(ret));
  } else {
    rpc_->wait();
    dup_table_rpc_.wait();
    clog_adapter_->wait();
    trans_migrate_worker_->wait();
    xa_trans_heartbeat_worker_.wait();
    big_trans_worker_.wait();
    xa_inner_table_gc_worker_.wait();
    TRANS_LOG(INFO, "transaction service wait success");
  }
}

void ObTransService::destroy()
{
  if (is_inited_) {
    if (is_running_) {
      stop();
      wait();
    }
    timer_.destroy();
    dup_table_lease_timer_.destroy();
    if (use_def_) {
      rpc_def_.destroy();
      location_adapter_def_.destroy();
      clog_adapter_def_.destroy();
      use_def_ = false;
    }
    trans_migrate_worker_def_.destroy();
    xa_trans_heartbeat_worker_.destroy();
    xa_inner_table_gc_worker_.destroy();
    sche_trans_ctx_mgr_.destroy();
    coord_trans_ctx_mgr_.destroy();
    part_trans_ctx_mgr_.destroy();
    slave_part_trans_ctx_mgr_.destroy();
    dup_table_rpc_.destroy();
    big_trans_worker_.destroy();
    part_trans_same_leader_batch_rpc_mgr_.destroy();
    light_trans_ctx_mgr_.destroy();
    is_inited_ = false;
    TRANS_LOG(INFO, "transaction service destroyed");
  }
}

memtable::ObIMemtableCtxFactory* ObTransService::get_mem_ctx_factory()
{
  return &mt_ctx_factory_;
}

int ObTransService::start_trans(const uint64_t tenant_id, const uint64_t cluster_id, const ObStartTransParam& req,
    const int64_t expired_time, const uint32_t session_id, const uint64_t proxy_session_id, ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  ObTransTraceLog& tlog = trans_desc.get_tlog();
  const ObTransID trans_id(self_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(!req.is_valid()) ||
             OB_UNLIKELY(expired_time <= 0) ||
             OB_UNLIKELY(ObStartTransParam::INVALID_CLUSTER_VERSION == req.get_cluster_version())) {
    TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(req), K(expired_time), K(req.get_cluster_version()));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObStandaloneStmtDesc& desc = trans_desc.get_standalone_stmt_desc();
    const int64_t stmt_snapshot_version = desc.get_snapshot_version();
    const bool is_stmt_snapshot_version_valid = desc.is_snapshot_version_valid();
    // reset transaction desc, it may be dirty
    trans_desc.reset();
    ObTransStatistic::get_instance().add_trans_start_count(tenant_id, 1);
    if (ObClockGenerator::getClock() >= expired_time) {
      TRANS_LOG(WARN, "transaction is timeout", K(trans_id), K(expired_time));
      ret = OB_TRANS_TIMEOUT;
    } else if (OB_FAIL(trans_desc.set_cluster_id(cluster_id))) {
      TRANS_LOG(WARN, "set cluster id error", KR(ret), K(cluster_id));
    } else if (OB_FAIL(trans_desc.set_trans_id(trans_id))) {
      TRANS_LOG(WARN, "set transaction id error", KR(ret), K(trans_id));
    } else if (OB_FAIL(trans_desc.set_trans_param(req))) {
      TRANS_LOG(WARN, "set transaction param error", KR(ret), K(trans_id));
    } else if (OB_FAIL(trans_desc.set_scheduler(self_))) {
      TRANS_LOG(WARN, "set scheduler error", KR(ret), K(trans_id));
    } else if (OB_FAIL(trans_desc.set_trans_expired_time(expired_time))) {
      TRANS_LOG(WARN, "set transaction expired time error", K(ret), K(trans_id));
    } else if (OB_FAIL(trans_desc.set_tenant_id(tenant_id))) {
      TRANS_LOG(WARN, "set tenant id error", K(ret), K(trans_id));
    } else if (OB_FAIL(trans_desc.set_cluster_version(req.get_cluster_version()))) {
      TRANS_LOG(WARN, "set cluster version error", KR(ret), K(trans_id), K(req.get_cluster_version()));
    } else if (OB_FAIL(trans_desc.set_session_id(session_id))) {
      TRANS_LOG(WARN, "set session id error", K(session_id), K(trans_id));
    } else if (OB_FAIL(trans_desc.set_proxy_session_id(proxy_session_id))) {
      TRANS_LOG(WARN, "set proxy session id error", K(proxy_session_id), K(trans_id));
    } else {
      // do nothing
    }
    if (OB_SUCC(ret) && trans_desc.is_serializable_isolation()) {
      if (OB_FAIL(set_trans_snapshot_version_for_serializable_(
              trans_desc, stmt_snapshot_version, is_stmt_snapshot_version_valid))) {
        TRANS_LOG(WARN,
            "set trans snapshot version for serializable isolation failed",
            KR(ret),
            K(trans_desc),
            K(req),
            K(cluster_id),
            K(session_id));
      }
    }

    int64_t cur_time = ObTimeUtility::current_time();
    const int64_t refresh_interval = 5000000;  // 5s
    bool need_refresh = cur_time - trans_desc.get_tenant_config_refresh_time() > refresh_interval;
    if (OB_SUCC(ret) && OB_UNLIKELY(need_refresh)) {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      bool enable_elr = false;
      if (OB_LIKELY(tenant_config.is_valid())) {
        const int64_t max_dependent_trans_count = GCONF._max_elr_dependent_trx_count;
        if (!trans_desc.is_readonly() && max_dependent_trans_count > 0 && OB_SYS_TENANT_ID != tenant_id) {
          enable_elr = tenant_config->enable_early_lock_release;
        }
        // need_record_rollback_trans_log = tenant_config->_enable_record_rollback_trans_log;
      }
      trans_desc.set_can_elr(enable_elr);
      trans_desc.set_tenant_config_refresh_time(cur_time);
      if (REACH_TIME_INTERVAL(10000000 /* 10s */)) {
        TRANS_LOG(INFO, "refresh tenant config success", K(enable_elr), K(tenant_id));
      }
    }

    if (OB_SUCC(ret)) {
      const int32_t trans_type = req.get_type();
      if (ObTransType::TRANS_SYSTEM == trans_type) {
        ObTransStatistic::get_instance().add_sys_trans_count(tenant_id, 1);
      } else if (ObTransType::TRANS_USER == trans_type) {
        ObTransStatistic::get_instance().add_user_trans_count(tenant_id, 1);
      } else {
        TRANS_LOG(ERROR, "invalid transaction type", K(trans_id), K(req));
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  REC_TRACE_EXT(tlog, start_trans, Y(ret), OB_ID(trans_id), trans_id);
  if (OB_FAIL(ret)) {
    trans_desc.reset();
    trans_desc.set_need_print_trace_log();
    TRANS_LOG(WARN, "start transaction failed", KR(ret), K(trans_id), K(req), K(session_id), K(trans_desc));
  }
  NG_TRACE_EXT(start_trans, OB_ID(trans_id), trans_id);

  return ret;
}

int ObTransService::half_stmt_commit(const ObTransDesc& trans_desc, const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  const bool for_replay = false;
  const bool is_readonly = false;
  const bool is_bounded_staleness_read = false;
  const bool need_completed_dirty_txn = false;
  bool alloc = false;
  ObTransCtx* ctx = NULL;
  ObPartTransCtx* part_ctx = NULL;
  const ObTransID& trans_id = trans_desc.get_trans_id();

  if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
          trans_id,
          for_replay,
          is_readonly,
          is_bounded_staleness_read,
          need_completed_dirty_txn,
          alloc,
          ctx))) {
    if (OB_TRANS_CTX_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(WARN, "get trans ctx error", K(ret), K(trans_desc), K(partition));
    }
  } else {
    part_ctx = static_cast<ObPartTransCtx*>(ctx);
    if (OB_FAIL(part_ctx->half_stmt_commit())) {
      TRANS_LOG(WARN, "half stmt commit error", K(ret), K(trans_desc), K(partition), K(*part_ctx));
    }
    (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
  }

  return ret;
}

int ObTransService::check_duplicate_partition_status_(const ObPartitionKey& partition, int& status)
{
  int ret = OB_SUCCESS;
  bool is_duplicated = false;
  bool is_serving = false;

  if (OB_FAIL(check_duplicated_partition(partition, is_duplicated))) {
    TRANS_LOG(WARN, "check duplicate partition failed", K(ret), K(partition));
  } else if (is_duplicated) {
    TRANS_LOG(DEBUG, "parition is duplicated", K(partition));
    if (OB_FAIL(part_trans_ctx_mgr_.is_dup_table_partition_serving(partition, false, is_serving))) {
      TRANS_LOG(WARN, "check dup table serving error", K(ret), K(partition));
    } else if (is_serving) {
      // duplicate table replica avaiable for read
      status = OB_SUCCESS;
    } else {
      status = OB_NOT_MASTER;
    }
  } else {
    status = OB_NOT_MASTER;
  }

  return ret;
}

int ObTransService::get_stmt_snapshot_info(
    const bool is_cursor_or_nested, ObTransDesc& trans_desc, ObTransSnapInfo& snapshot_info)
{
  int ret = OB_SUCCESS;
  int64_t snapshot_version = 0;

  if (trans_desc.is_serializable_isolation()) {
    snapshot_version = trans_desc.get_trans_snapshot_version();
  } else if (!trans_desc.is_bounded_staleness_read()) {
    if (!ts_mgr_->is_external_consistent(trans_desc.get_tenant_id())) {
      ret = OB_NOT_SUPPORTED;
      TRANS_LOG(WARN, "fast select need gts", K(ret));
    } else if (OB_FAIL(get_gts_for_snapshot_version_(trans_desc,
                   snapshot_version,
                   trans_desc.get_trans_expired_time(),
                   trans_desc.get_cur_stmt_expired_time(),
                   trans_desc.get_tenant_id()))) {
      TRANS_LOG(WARN, "get gts for snapshot version error", K(ret), K(trans_desc));
    } else {
      // do nothing
    }
  } else {
    ObIWeakReadService* wrs = GCTX.weak_read_service_;
    if (OB_ISNULL(wrs)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "weak read service is invalid", K(ret), KP(wrs));
    } else {
      if (OB_FAIL(wrs->get_cluster_version(trans_desc.get_tenant_id(), snapshot_version))) {
        TRANS_LOG(WARN, "get weak read cluster version fail", K(ret));
      }
    }
  }
  if (OB_SUCCESS == ret) {
    if (0 >= snapshot_version) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected trans snapshot version", K(ret), K(trans_desc));
    } else {
      snapshot_info.set_snapshot_version(snapshot_version);
      trans_desc.set_snapshot_gene_type(ObTransSnapshotGeneType::APPOINT);
      if (is_cursor_or_nested) {
        snapshot_info.set_trans_id(trans_desc.get_trans_id());
        snapshot_info.set_read_sql_no(trans_desc.get_sql_no());
        snapshot_info.set_is_cursor_or_nested(true);
      }
    }
  }

  return ret;
}

int ObTransService::check_partition_status(
    const ObTransDesc& trans_desc, const ObPartitionKey& partition, int64_t& leader_epoch)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* pg = NULL;
  const bool check_election = true;
  int status = OB_SUCCESS;
  ObTsWindows changing_leader_windows;

  if (trans_desc.is_bounded_staleness_read()) {
    // do nothing
  } else if (OB_FAIL(get_partition_group_(partition, guard, pg))) {
    TRANS_LOG(WARN, "get partition group fail", K(ret), K(partition), KP(pg));
  } else if (OB_ISNULL(pg)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid partition group", K(ret), K(partition), KP(pg));
  } else if (OB_FAIL(clog_adapter_->get_status(pg, check_election, status, leader_epoch, changing_leader_windows))) {
    TRANS_LOG(WARN, "get participant status error", K(ret), K(trans_desc), K(partition));
  } else if (OB_SUCCESS != status) {
    ret = status;
  } else {
    // do nothing
  }

  if (OB_NOT_MASTER == ret) {
    int tmp_ret = OB_SUCCESS;
    int duplicate_partition_status = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = check_duplicate_partition_status_(partition, duplicate_partition_status))) {
      TRANS_LOG(WARN, "check duplicate partition status failed", K(tmp_ret), K(partition));
    } else if (OB_SUCCESS == duplicate_partition_status) {
      ret = duplicate_partition_status;
      leader_epoch = 0;
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObTransService::generate_transaction_snapshot_(ObTransDesc& trans_desc, int64_t& snapshot_version)
{
  // in stage of transaction start, stmt_expired_time is unknown
  int64_t stmt_expire_time = INT64_MAX;
  int64_t trans_expire_time = trans_desc.get_trans_expired_time();
  return get_gts_for_snapshot_version_(
      trans_desc, snapshot_version, trans_expire_time, stmt_expire_time, trans_desc.get_tenant_id());
}

int ObTransService::get_gts_for_snapshot_version_(ObTransDesc& trans_desc, int64_t& snapshot_version,
    const int64_t trans_expired_time, const int64_t stmt_expire_time, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  MonotonicTs tmp_receive_gts_ts;

  if (OB_FAIL(get_gts_(snapshot_version, tmp_receive_gts_ts, trans_expired_time, stmt_expire_time, tenant_id))) {
    TRANS_LOG(WARN, "get gts fail", KR(ret), K(trans_desc));
  } else {
    const int64_t GET_GTS_AHEAD_INTERVAL = GCONF._ob_get_gts_ahead_interval;
    trans_desc.set_trans_need_wait_wrap(tmp_receive_gts_ts, GET_GTS_AHEAD_INTERVAL);
  }

  return ret;
}

int ObTransService::get_gts_(int64_t& snapshot_version, MonotonicTs& receive_gts_ts, const int64_t trans_expired_time,
    const int64_t stmt_expire_time, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // FIXME: its better to use transaction start time
  // and only get gts once during transaction lifecyle
  const MonotonicTs request_ts = MonotonicTs::current_time();
  const int64_t WAIT_GTS_US = 500;
  int64_t gts = OB_INVALID_TIMESTAMP;
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
      } else if (OB_FAIL(ts_mgr_->get_local_trans_version(tenant_id, stc_ahead, NULL, gts, tmp_receive_gts_ts))) {
        if (OB_EAGAIN != ret) {
          TRANS_LOG(WARN, "get gts failed", KR(ret));
        } else {
          usleep(WAIT_GTS_US);
        }
      } else if (0 >= gts) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "invalid gts, unexpected error", KR(ret), K(gts));
      } else {
        snapshot_version = gts;
        receive_gts_ts = tmp_receive_gts_ts;
      }
    } while (OB_EAGAIN == ret);
  }
  return ret;
}

int ObTransService::end_trans_callback_(sql::ObIEndTransCallback& cb, const int cb_param, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObEndTransCallback end_trans_cb;

  if (OB_FAIL(end_trans_cb.init(tenant_id, &cb))) {
    TRANS_LOG(WARN, "end trans callback init error", K(tenant_id), KR(ret));
  } else if (OB_FAIL(end_trans_cb.callback(cb_param))) {
    TRANS_LOG(WARN, "end trans callback error", KR(ret), K(cb_param), K(tenant_id), K(end_trans_cb));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransService::handle_sp_end_trans_(const bool is_rollback, ObTransDesc& trans_desc, sql::ObIEndTransCallback& cb,
    const int64_t stmt_expired_time, const MonotonicTs commit_time, bool& need_convert_to_dist_trans)
{
  int ret = OB_SUCCESS;
  int save_ret = OB_SUCCESS;
  static ObNullEndTransCallback null_cb;
  const uint64_t tenant_id = trans_desc.get_tenant_id();
  const bool is_trans_timeout = trans_desc.is_trans_timeout();
  const bool is_bounded_staleness_read = trans_desc.is_bounded_staleness_read();
  const bool is_readonly = trans_desc.is_readonly();
  const ObStmtRollbackInfo& stmt_rollback_info = trans_desc.get_stmt_rollback_info();
  bool need_rollback = is_rollback;
  need_convert_to_dist_trans = false;
  ObTransCtx* part_ctx = NULL;
  sql::ObIEndTransCallback* callback = &cb;

  // condider trans_timeout only for sp_trans, don't care abount stmt_timeout
  if (is_trans_timeout) {
    TRANS_LOG(WARN, "transaction is timeout", K(trans_desc));
    ObTransStatistic::get_instance().add_trans_timeout_count(tenant_id, 1);
    need_rollback = true;
    callback = &null_cb;
    save_ret = OB_TRANS_TIMEOUT;
  } else if (!is_rollback && trans_desc.need_rollback()) {
    need_rollback = true;
    save_ret = OB_TRANS_ROLLBACKED;
    callback = &null_cb;
  }

  TRANS_STAT_COMMIT_ABORT_TRANS_INC(need_rollback, tenant_id);
  // start into transaction termination flow
  if ((!is_bounded_staleness_read && trans_desc.get_participants().count() == 0) ||
      (is_bounded_staleness_read && trans_desc.get_participants_pla().count() == 0)) {
    ObTransStatistic::get_instance().add_zero_partition_count(tenant_id, 1);
  }
  if (OB_ISNULL(part_ctx = const_cast<ObTransDesc&>(trans_desc).get_part_ctx())) {
    TRANS_LOG(DEBUG, "transaction ctx is null, may be stmt fail or rollback", K(trans_desc));
    (void)end_trans_callback_(*callback, OB_SUCCESS, tenant_id);
  } else if (OB_FAIL(part_ctx->commit(need_rollback,
                 callback,
                 is_readonly,
                 commit_time,
                 stmt_expired_time,
                 stmt_rollback_info,
                 trans_desc.get_trace_info().get_app_trace_info(),
                 need_convert_to_dist_trans))) {
    TRANS_LOG(WARN, "sp commit failed", K(ret), K(stmt_expired_time), K(trans_desc));
    if (!need_convert_to_dist_trans) {
      if (is_bounded_staleness_read) {
        (void)slave_part_trans_ctx_mgr_.revert_trans_ctx(part_ctx);
      } else {
        (void)part_trans_ctx_mgr_.revert_trans_ctx(part_ctx);
      }
    }
  } else {
    if (is_bounded_staleness_read) {
      (void)slave_part_trans_ctx_mgr_.revert_trans_ctx(part_ctx);
    } else {
      (void)part_trans_ctx_mgr_.revert_trans_ctx(part_ctx);
    }
  }

  if (OB_SUCC(ret)) {
    ret = save_ret;
  }

  return ret;
}

int ObTransService::start_nested_stmt(ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  ObTransSnapInfo snapshot_info;

  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!trans_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(trans_desc));
  } else if (trans_desc.cluster_version_before_2271()) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "nested stmt not support during upgrade", K(ret), K(trans_desc));
  } else if (OB_UNLIKELY(trans_desc.is_trans_timeout())) {
    ret = OB_TRANS_TIMEOUT;
    TRANS_LOG(WARN, "transaction is timeout", K(ret), K(trans_desc));
  } else if (OB_UNLIKELY(trans_desc.is_stmt_timeout())) {
    ret = OB_TRANS_STMT_TIMEOUT;
    TRANS_LOG(WARN, "statement is timeout", K(ret), K(trans_desc));
  } else if (OB_UNLIKELY(trans_desc.need_rollback())) {
    ret = OB_TRANS_NEED_ROLLBACK;
    TRANS_LOG(WARN, "transaction need rollback", K(ret), K(trans_desc));
  } else if (FALSE_IT(trans_desc.start_nested_stmt())) {
  } else if (OB_FAIL(get_stmt_snapshot_info(true, trans_desc, snapshot_info))) {
    TRANS_LOG(WARN, "get stmt snapshot error", K(ret), K(trans_desc), K(snapshot_info));
  } else {
    trans_desc.set_snapshot_version(snapshot_info.get_snapshot_version());
    trans_desc.set_stmt_snapshot_info(snapshot_info);
  }

  TRANS_LOG(DEBUG, "nested start stmt", K(ret), K(trans_desc));

  return ret;
}

int ObTransService::end_nested_stmt(
    ObTransDesc& trans_desc, const ObPartitionArray& participants, const bool is_rollback)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!trans_desc.is_valid() || !trans_desc.is_nested_stmt()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(trans_desc));
  } else if (OB_UNLIKELY(trans_desc.is_trans_timeout())) {
    ret = OB_TRANS_TIMEOUT;
    TRANS_LOG(WARN, "transaction is timeout", K(ret), K(trans_desc));
  } else if (OB_UNLIKELY(trans_desc.need_rollback())) {
    ret = OB_TRANS_NEED_ROLLBACK;
    TRANS_LOG(WARN, "transaction need rollback", K(ret), K(trans_desc));
  } else if (is_rollback &&
             OB_FAIL(do_dist_rollback_(trans_desc, trans_desc.get_stmt_min_sql_no() - 1, participants))) {
    TRANS_LOG(WARN, "fail to do dist rollback", K(ret), K(trans_desc), K(participants));
  }

  TRANS_LOG(DEBUG, "nested end stmt", K(ret), K(trans_desc), K(is_rollback), K(participants));

  return ret;
}

int ObTransService::do_dist_rollback_(
    ObTransDesc& trans_desc, const int64_t sql_no, const ObPartitionArray& rollback_partitions)
{
  int ret = OB_SUCCESS;

  bool use_tmp_sche_ctx = false;
  ObScheTransCtx* sche_ctx = NULL;

  if (OB_FAIL(alloc_tmp_sche_ctx_(trans_desc, use_tmp_sche_ctx))) {
    TRANS_LOG(WARN, "fail to get tmp scheduler", K(ret), K(trans_desc));
  } else if (NULL == (sche_ctx = trans_desc.get_sche_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "scheduler not found", K(ret), K(trans_desc));
  } else if (OB_FAIL(sche_ctx->start_savepoint_rollback(trans_desc, sql_no, rollback_partitions))) {
    TRANS_LOG(WARN, "start savepoint rollback error", K(ret), K(trans_desc), K(sql_no));
  } else if (OB_FAIL(
                 sche_ctx->wait_savepoint_rollback(trans_desc.get_cur_stmt_expired_time() + END_STMT_MORE_TIME_US))) {
    TRANS_LOG(WARN, "wait savepoint rollback error", K(ret), K(trans_desc));
  }

  if (OB_FAIL(ret)) {
    trans_desc.set_need_rollback();
  }

  if (use_tmp_sche_ctx) {
    free_tmp_sche_ctx_(trans_desc);
  }

  return ret;
}

int ObTransService::alloc_tmp_sche_ctx_(ObTransDesc& trans_desc, bool& use_tmp_sche_ctx)
{
  int ret = OB_SUCCESS;

  if (!trans_desc.is_nested_stmt()) {
    // only nested stmt need create temp scheduler
  } else if (NULL == trans_desc.get_sche_ctx()) {
    const ObTransID& trans_id = trans_desc.get_trans_id();
    const bool for_replay = false;
    bool alloc = true;
    const bool is_readonly = false;
    ObTransCtx* ctx = NULL;

    if (OB_FAIL(sche_trans_ctx_mgr_.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
      TRANS_LOG(WARN, "get transaction context error", K(ret), K(trans_id));
    } else {
      ObScheTransCtx* sche_ctx = static_cast<ObScheTransCtx*>(ctx);

      if (OB_FAIL(sche_ctx->init(trans_desc.get_tenant_id(),
              trans_id,
              trans_desc.get_trans_expired_time(),
              SCHE_PARTITION_ID,
              &sche_trans_ctx_mgr_,
              trans_desc.get_trans_param(),
              trans_desc.get_cluster_version(),
              this,
              trans_desc.is_can_elr()))) {
        TRANS_LOG(WARN, "transaction context init error", K(ret), K(trans_id));
      } else {
        (void)sche_ctx->set_trans_app_trace_id_str(trans_desc.get_trans_app_trace_id_str());
        if (OB_FAIL(sche_ctx->set_scheduler(self_))) {
          TRANS_LOG(WARN, "set scheduler error", K(ret), K_(self), K(trans_id));
        } else if (OB_FAIL(sche_ctx->set_sql_no(trans_desc.get_sql_no()))) {
          TRANS_LOG(WARN, "scheduler set sql no error", K(ret), K(trans_id));
        } else if (OB_FAIL(sche_ctx->set_session_id(trans_desc.get_session_id()))) {
          TRANS_LOG(
              WARN, "scheduler set session id error", K(ret), K(trans_id), "session_id", trans_desc.get_session_id());
        } else if (OB_FAIL(sche_ctx->set_proxy_session_id(trans_desc.get_proxy_session_id()))) {
          TRANS_LOG(WARN,
              "scheduler set proxy session id error",
              KR(ret),
              K(trans_id),
              "proxy_session_id",
              trans_desc.get_proxy_session_id());
        }
      }

      if (OB_FAIL(ret) || OB_FAIL(trans_desc.set_sche_ctx(sche_ctx))) {
        sche_ctx->set_exiting();
        (void)sche_trans_ctx_mgr_.revert_trans_ctx(sche_ctx);
      } else {
        use_tmp_sche_ctx = true;
        TRANS_LOG(DEBUG, "create temp scheduler success", K(SCHE_PARTITION_ID), K(trans_desc));
      }
    }
  }

  return ret;
}

void ObTransService::free_tmp_sche_ctx_(ObTransDesc& trans_desc)
{
  ObScheTransCtx* sche_ctx = NULL;

  if (NULL != (sche_ctx = trans_desc.get_sche_ctx())) {
    trans_desc.set_sche_ctx(NULL);
    sche_ctx->set_exiting();
    (void)sche_trans_ctx_mgr_.revert_trans_ctx(sche_ctx);
  }
}

/*
 * convention with SQL-Layer:
 * Transaction-Layer promise the callback passed to end_trans will be called
 */
int ObTransService::end_trans(
    const bool is_rollback, ObTransDesc& trans_desc, sql::ObIEndTransCallback& cb, const int64_t stmt_expired_time)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTransTraceLog& tlog = trans_desc.get_tlog();
  const int64_t WARN_THRESHOLD_US = 100 * 1000;
  const ObTransID& trans_id = trans_desc.get_trans_id();
  const bool is_bounded_staleness_read = trans_desc.is_bounded_staleness_read();
  const int64_t participants_commit_count =
      (is_bounded_staleness_read ? trans_desc.get_participants_pla().count() : trans_desc.get_participants().count());
  ObExclusiveEndTransCallback* exclusive_cb = NULL;
  MonotonicTs commit_time = MonotonicTs::current_time();
  /*
  if (trans_desc.is_autocommit() && trans_desc.get_trans_id().get_server() == self_) {
    commit_time = trans_desc.get_trans_id().get_timestamp();
  } else {
    commit_time = ObClockGenerator::getRealClock();
    }*/
  if (!cb.is_shared()) {
    exclusive_cb = static_cast<ObExclusiveEndTransCallback*>(&cb);
    exclusive_cb->set_is_txs_end_trans_called(true);  // indicate end_trans interface was called
  } else {
    // do nothing for unshared callback, which was stateless
  }
  if (IS_NOT_INIT) {
    TRANS_LOG(ERROR, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!trans_desc.is_valid()) || OB_UNLIKELY(stmt_expired_time <= 0)) {
    TRANS_LOG(ERROR, "invalid argument", K(trans_desc), K(stmt_expired_time));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(!trans_desc.test_and_set_trans_end())) {
    // repeated call was rejected
    TRANS_LOG(WARN, "transaction already ended, forbidden repeated operation", K(trans_desc), K(is_rollback));
    ret = OB_NOT_SUPPORTED;
  } else if (OB_UNLIKELY(trans_desc.is_trx_idle_timeout())) {
    TRANS_LOG(WARN, "transaction is idle timeout", K(trans_desc), K(is_rollback));
    if (is_rollback) {
      (void)end_trans_callback_(cb, OB_SUCCESS, trans_desc.get_tenant_id());
    } else {
      TRANS_LOG(WARN, "transaction is idle timeout, report transaction rollbacked", K(trans_desc), K(is_rollback));
      ret = OB_TRANS_ROLLBACKED;
    }
  } else {
    // staticstics of 0/1/multi partition transaction count
    if (0 == participants_commit_count) {
      ObTransStatistic::get_instance().add_zero_partition_count(trans_desc.get_tenant_id(), 1);
    } else if (1 == participants_commit_count) {
      ObTransStatistic::get_instance().add_single_partition_count(trans_desc.get_tenant_id(), 1);
    } else if (participants_commit_count > 1) {
      ObTransStatistic::get_instance().add_multi_partition_count(trans_desc.get_tenant_id(), 1);
    } else {
      // do nothing
    }
    if (trans_desc.is_mini_sp_trans() || trans_desc.is_sp_trans()) {
      bool need_convert_to_dist_trans = false;
      if (OB_FAIL(handle_sp_end_trans_(
              is_rollback, trans_desc, cb, stmt_expired_time, commit_time, need_convert_to_dist_trans))) {
        TRANS_LOG(WARN, "handle sp end trans error", KR(ret), K(is_rollback), K(trans_desc), K(stmt_expired_time));
        if (need_convert_to_dist_trans) {
          // rewrite ret
          ret = OB_SUCCESS;
          ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(trans_desc.get_part_ctx());
          trans_desc.set_trans_type(TransType::DIST_TRANS);
          if (OB_ISNULL(part_ctx)) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "part ctx is NULL", KR(ret));
          } else if (OB_FAIL(recover_sche_ctx_(trans_desc, part_ctx))) {
            TRANS_LOG(WARN, "recover scheduler context error", KR(ret), K(trans_desc));
            (void)static_cast<ObTransDesc&>(trans_desc).set_sche_ctx(NULL);
          } else {
            part_ctx->recover_dist_trans(self_);
            (void)part_trans_ctx_mgr_.revert_trans_ctx(part_ctx);
            TRANS_LOG(INFO, "switch sp_trans to dist trans success", K(trans_desc));
            if (OB_FAIL(end_trans_(is_rollback, trans_desc, cb, stmt_expired_time, commit_time))) {
              TRANS_LOG(WARN, "end trans failed", KR(ret), K(trans_desc));
            }
          }
        }
      }
    } else {
      const int64_t start_time_us = ObClockGenerator::getClock();
      ret = end_trans_(is_rollback, trans_desc, cb, stmt_expired_time, commit_time);
      const int64_t end_time_us = ObClockGenerator::getClock();
      // print slow query
      if (end_time_us - start_time_us >= WARN_THRESHOLD_US) {
        TRANS_LOG(WARN, "transaction 2pc use too much time", K(trans_id), "used_time_us", end_time_us - start_time_us);
      }
      (void)trans_desc.set_sche_ctx(NULL);
    }
  }
  // special handle for OB_TRANS_TIMEOUT
  const int64_t tenant_id = ((trans_desc.get_tenant_id()) > 0 ? (trans_desc.get_tenant_id()) : OB_SYS_TENANT_ID);
  if (OB_FAIL(ret)) {
    if (is_rollback && OB_TRANS_TIMEOUT == ret) {
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = end_trans_callback_(cb, OB_SUCCESS, tenant_id)))) {
        ret = tmp_ret;
      }
    } else {
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = end_trans_callback_(cb, ret, tenant_id)))) {
        ret = tmp_ret;
      }
      TRANS_LOG(WARN, "end transaction error", KR(ret), K(trans_desc), K(is_rollback));
    }
  }
  REC_TRACE_EXT(tlog, end_trans, Y(ret));
  if (OB_FAIL(ret)) {
    trans_desc.set_need_print_trace_log();
    TRANS_LOG(WARN, "end transaction failed", KR(ret), K(trans_desc));
  }
  NG_TRACE_EXT(end_trans, Y(ret));

  return ret;
}

int ObTransService::handle_dist_start_stmt_(const ObStmtParam& stmt_param, const ObPartitionLeaderArray& pla,
    const ObStmtDesc& stmt_desc, ObTransDesc& trans_desc, ObPartitionArray& unreachable_partitions)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTransCtx* ctx = NULL;
  ObScheTransCtx* sche_ctx = NULL;
  const ObPartitionArray& participants = pla.get_partitions();
  const int64_t expired_time = stmt_param.get_stmt_expired_time();
  const ObTransID& trans_id = trans_desc.get_trans_id();
  // XA transaction already allocate scheduler in xa_start
  bool alloc = trans_desc.is_xa_local_trans() ? false : true;
  const bool for_replay = false;
  const bool is_readonly = trans_desc.is_readonly();

  if (NULL != (ctx = trans_desc.get_sche_ctx())) {
    sche_ctx = static_cast<ObScheTransCtx*>(ctx);
  } else if (OB_FAIL(
                 sche_trans_ctx_mgr_.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
    TRANS_LOG(DEBUG, "get transaction context error", KR(ret), K(trans_id));
  } else {
    if (OB_UNLIKELY(!alloc) && !trans_desc.is_xa_local_trans()) {
      TRANS_LOG(ERROR, "transaction context already exist", K(trans_id));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_ISNULL(ctx)) {
      TRANS_LOG(ERROR, "transaction context is null", K(trans_id));
      ret = OB_ERR_UNEXPECTED;
    } else {
      sche_ctx = static_cast<ObScheTransCtx*>(ctx);
      if (OB_FAIL(sche_ctx->init(trans_desc.get_tenant_id(),
              trans_id,
              trans_desc.get_trans_expired_time(),
              SCHE_PARTITION_ID,
              &sche_trans_ctx_mgr_,
              trans_desc.get_trans_param(),
              trans_desc.get_cluster_version(),
              this,
              trans_desc.is_can_elr()))) {
        TRANS_LOG(WARN, "transaction context init error", KR(ret), K(trans_id));
      } else {
        (void)sche_ctx->set_trans_app_trace_id_str(trans_desc.get_trans_app_trace_id_str());
        if (OB_FAIL(sche_ctx->set_scheduler(self_))) {
          TRANS_LOG(WARN, "set scheduler error", KR(ret), K_(self), K(trans_id));
        } else if (OB_FAIL(sche_ctx->set_sql_no(trans_desc.get_sql_no()))) {
          TRANS_LOG(WARN, "scheduler set sql no error", KR(ret), K(trans_id));
        } else if (OB_FAIL(sche_ctx->set_session_id(trans_desc.get_session_id()))) {
          TRANS_LOG(
              WARN, "scheduler set session id error", KR(ret), K(trans_id), "session_id", trans_desc.get_session_id());
        } else if (OB_FAIL(sche_ctx->set_proxy_session_id(trans_desc.get_proxy_session_id()))) {
          TRANS_LOG(WARN,
              "scheduler set proxy session id error",
              KR(ret),
              K(trans_id),
              "proxy_session_id",
              trans_desc.get_proxy_session_id());
        } else if (OB_FAIL(sche_ctx->start_trans(trans_desc))) {
          TRANS_LOG(WARN, "start transaction error", KR(ret), K(trans_id));
        } else {
          TRANS_LOG(DEBUG, "start transaction success", K(SCHE_PARTITION_ID), K(trans_desc));
        }
      }
    }
    // revert transaction context in end_trans when set ctx success
    if (OB_FAIL(ret) || OB_FAIL(trans_desc.set_sche_ctx(sche_ctx))) {
      if (OB_NOT_NULL(sche_ctx)) {
        sche_ctx->set_exiting();
        (void)sche_trans_ctx_mgr_.revert_trans_ctx(sche_ctx);
      }
    }
  }
  if (OB_SUCC(ret)) {
    //    if (!trans_desc.is_all_select_stmt()) {
    //      sche_ctx->set_readonly(false);
    //    }
    // if (!trans_desc.is_bounded_staleness_read()) {
    //  sche_ctx->set_readonly(false);
    //}
    ObWaitEventGuard wait_guard(ObWaitEventIds::START_STMT_WAIT,
        expired_time,
        trans_desc.get_trans_id().hash(),
        trans_desc.get_cur_stmt_desc().phy_plan_type_,
        participants.count());
    int32_t xa_trans_state = ObXATransState::NON_EXISTING;
    if (OB_FAIL(sche_ctx->get_xa_trans_state(xa_trans_state))) {
      TRANS_LOG(WARN, "get xa trans state error", K(ret), K(trans_desc));
    } else if (ObXATransState::IDLE == xa_trans_state || ObXATransState::PREPARED == xa_trans_state) {
      ret = OB_TRANS_XA_RMFAIL;
      LOG_USER_ERROR(OB_TRANS_XA_RMFAIL, ObXATransState::to_string(xa_trans_state));
      TRANS_LOG(WARN, "invalid state", K(trans_desc), K(xa_trans_state));
    } else if (OB_FAIL(sche_ctx->start_stmt(pla, trans_desc, stmt_desc))) {
      TRANS_LOG(WARN, "start statement error", KR(ret), K(trans_desc), K(stmt_desc));
    } else if (OB_FAIL(sche_ctx->wait_start_stmt(trans_desc, expired_time))) {
      TRANS_LOG(WARN, "wait start stmt error", K(ret), K(trans_desc));
      if (OB_TRANS_STMT_TIMEOUT == ret) {
        if (trans_desc.is_bounded_staleness_read()) {
          ObTransStatistic::get_instance().add_slave_read_stmt_timeout_count(trans_desc.get_tenant_id(), 1);
        } else {
          ObTransStatistic::get_instance().add_stmt_timeout_count(trans_desc.get_tenant_id(), 1);
        }
      } else if (trans_desc.is_bounded_staleness_read()) {
        // sql engine can do stmt retry for these errors
        if (OB_TRANS_RPC_TIMEOUT == ret || OB_REPLICA_NOT_READABLE == ret || OB_PARTITION_NOT_EXIST == ret ||
            OB_PARTITION_IS_STOPPED == ret || OB_PARTITION_IS_BLOCKED == ret) {
          ObTransStatistic::get_instance().add_slave_read_stmt_retry_count(trans_desc.get_tenant_id(), 1);
          if (OB_SUCCESS != (tmp_ret = sche_ctx->get_cur_stmt_unreachable_partitions(unreachable_partitions))) {
            TRANS_LOG(WARN, "unreachable partition assign error", "ret", tmp_ret, K(trans_desc));
            ret = tmp_ret;
          }
        }
      } else {
        // do nothing
      }
      (void)trans_desc.merge_participants_pla();
      (void)sche_ctx->merge_participants_pla();
      sche_ctx->clear_stmt_participants_pla();
      trans_desc.clear_stmt_participants_pla();
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObTransService::recover_sche_ctx_(ObTransDesc& trans_desc, ObPartTransCtx* part_ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTransID& trans_id = trans_desc.get_trans_id();
  const bool for_replay = false;
  const bool is_readonly = trans_desc.is_readonly();
  bool alloc = true;
  ObTransCtx* ctx = NULL;

  if (OB_ISNULL(part_ctx)) {
    TRANS_LOG(WARN, "invalid argument", KP(part_ctx));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(
                 sche_trans_ctx_mgr_.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
    TRANS_LOG(DEBUG, "get transaction context error", KR(ret), K(trans_id));
  } else if (OB_UNLIKELY(!alloc)) {
    TRANS_LOG(ERROR, "transaction context already exist", K(trans_id));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(ctx)) {
    TRANS_LOG(ERROR, "transaction context is null", K(trans_id));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObScheTransCtx* sche_ctx = static_cast<ObScheTransCtx*>(ctx);
    if (OB_FAIL(sche_ctx->init(trans_desc.get_tenant_id(),
            trans_id,
            trans_desc.get_trans_expired_time(),
            SCHE_PARTITION_ID,
            &sche_trans_ctx_mgr_,
            trans_desc.get_trans_param(),
            trans_desc.get_cluster_version(),
            this,
            trans_desc.is_can_elr()))) {
      TRANS_LOG(WARN, "scheduler transaction context init error", KR(ret), K(trans_id));
    } else if (OB_FAIL(sche_ctx->set_session_id(trans_desc.get_session_id()))) {
      TRANS_LOG(
          WARN, "scheduler set session id error", KR(ret), K(trans_id), "session_id", trans_desc.get_session_id());
    } else if (OB_FAIL(sche_ctx->set_proxy_session_id(trans_desc.get_proxy_session_id()))) {
      TRANS_LOG(WARN,
          "scheduler set proxy session id error",
          KR(ret),
          K(trans_id),
          "proxy_session_id",
          trans_desc.get_proxy_session_id());
    } else {
      (void)sche_ctx->set_dup_table_trans(trans_desc.is_dup_table_trans());
      (void)sche_ctx->set_trans_app_trace_id_str(part_ctx->get_trans_app_trace_id_str());
      if (OB_FAIL(sche_ctx->set_scheduler(self_))) {
        TRANS_LOG(WARN, "set scheduler error", KR(ret), K_(self), K(trans_id));
      } else if (OB_FAIL(sche_ctx->set_sql_no(trans_desc.get_sql_no()))) {
        TRANS_LOG(WARN, "scheduler set sql no error", KR(ret), K(trans_id));
        // participants count should be 0 or 1
      } else if (trans_desc.get_participants().count() > 1) {
        TRANS_LOG(ERROR, "participants count is great to 1, unexpected error", K(trans_desc));
        ret = OB_ERR_UNEXPECTED;
      } else if (trans_desc.is_bounded_staleness_read()) {
        if (OB_FAIL(sche_ctx->merge_participants_pla(trans_desc.get_participants_pla()))) {
          TRANS_LOG(WARN, "scheduler merge participants pla error", KR(ret), K(trans_desc));
        }
      } else {
        if (OB_FAIL(sche_ctx->merge_participants(trans_desc.get_participants()))) {
          TRANS_LOG(WARN, "scheduler merge participants error", KR(ret), K(trans_desc));
        } else {
          ObPartitionLeaderArray pla;
          if (OB_SUCCESS != (tmp_ret = pla.push(part_ctx->get_partition(), self_))) {
            TRANS_LOG(WARN, "partition leader array push error", K(tmp_ret), "part_ctx", *part_ctx);
          } else if (OB_SUCCESS != (tmp_ret = sche_ctx->merge_partition_leader_info(pla))) {
            TRANS_LOG(WARN, "merge trans location leader info error", K(tmp_ret), K(pla), "part_ctx", *part_ctx);
          } else {
            // do nothing
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sche_ctx->start_trans(trans_desc))) {
        TRANS_LOG(WARN, "start transaction error", KR(ret), K(trans_id));
      } else {
        TRANS_LOG(DEBUG, "start transaction success", K(SCHE_PARTITION_ID), K(trans_desc));
      }
    }
    if (OB_FAIL(ret) || OB_FAIL(trans_desc.set_sche_ctx(sche_ctx))) {
      (void)sche_trans_ctx_mgr_.revert_trans_ctx(sche_ctx);
    }
  }
  if (OB_SUCC(ret)) {
    TRANS_LOG(DEBUG, "recover scheduler context success", K(trans_desc));
  }

  return ret;
}

bool ObTransService::need_switch_to_dist_trans_(const ObTransDesc& trans_desc, const ObPartitionKey& cur_pkey,
    const ObPartitionLeaderArray& pla, const bool is_external_consistent)
{
  bool bool_ret = false;
  uint64_t tenant_id = trans_desc.get_tenant_id();
  const ObStmtDesc& stmt_desc = trans_desc.get_cur_stmt_desc();
  const ObPartitionArray& participants = pla.get_partitions();
  bool multi_tenant_stmt = pla.count() > 0 ? !pla.is_single_tenant() : false;

  // special optimization: read-only participant don't count as real participants
  //
  // constraint: single-tenant, gts was enabled
  //
  // @note
  // 1. multi-tenant must consult snapshot, need create scheCtx
  // 2. gts:
  //    1) when enable gts, SP_TRANS don't need consult snapshot
  //    2) when disable gts, multi-partition stmt have to consult snapshot
  if (multi_tenant_stmt || (!is_external_consistent && participants.count() > 1)) {
    bool_ret = true;
  } else {
    for (int64_t i = 0; i < participants.count(); i++) {
      if (!(trans_desc.is_not_create_ctx_participant(participants.at(i)) && is_external_consistent)) {
        if (cur_pkey != participants.at(i)) {
          bool_ret = true;
          break;
        }
      }
    }
  }

  if (bool_ret) {
    TRANS_LOG(DEBUG,
        "need switch sp trans to dist trans",
        K(tenant_id),
        K(multi_tenant_stmt),
        K(is_external_consistent),
        K(trans_desc),
        K(stmt_desc),
        K(pla));
  }

  return bool_ret;
}

bool ObTransService::verify_duplicate_partition_(const ObTransDesc& trans_desc, const ObPartitionLeaderArray& pla)
{
  bool bool_ret = false;
  const ObPartitionArray& participants = trans_desc.get_participants();
  const ObPartitionArray& pla_partitions = pla.get_partitions();
  const ObPartitionTypeArray& type_array = pla.get_types();

  for (int64_t i = 0; !bool_ret && i < type_array.count(); i++) {
    if (type_array.at(i) == ObPartitionType::DUPLICATE_FOLLOWER_PARTITION) {
      for (int64_t j = 0; j < participants.count(); j++) {
        if (participants.at(j) == pla_partitions.at(i)) {
          bool_ret = true;
          break;
        }
      }
    }
  }

  return bool_ret;
}

// definition of sp_trans:
// 1. autocommit=1, local/remote and single partition read/write transaction
// 2. autocommit=0, local and single partition transaction
int ObTransService::start_stmt(const ObStmtParam& stmt_param, ObTransDesc& trans_desc,
    const ObPartitionLeaderArray& pla, ObPartitionArray& unreachable_partitions)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTransTraceLog& tlog = trans_desc.get_tlog();
  unreachable_partitions.reset();
  const bool is_readonly = trans_desc.is_readonly();
  const ObPartitionArray& participants = pla.get_partitions();
  const int64_t expired_time = stmt_param.get_stmt_expired_time();
  const bool is_dup_table_stmt = pla.has_duplicate_partition();
  const int64_t tenant_id = trans_desc.get_tenant_id();
  const ObStmtDesc& stmt_desc = trans_desc.get_cur_stmt_desc();
  const int32_t read_snapshot_type = trans_desc.get_trans_param().get_read_snapshot_type();
  bool xa_need_release_lock = false;
  bool need_delete_xa_all_tightly_branch = false;

  TRANS_STAT_STMT_INTERVAL_TIME(tenant_id, trans_desc.get_last_end_stmt_ts());
  TRANS_STAT_STMT_TOTAL_COUNT_INC(tenant_id);

  // mark current time used to calculate stmt interval
  trans_desc.set_last_end_stmt_ts(ObClockGenerator::getClock());
  // when do non-readonly operate on duplicated table leader, mark transaction type as dup_table_trans
  if (!trans_desc.is_dup_table_trans()) {
    trans_desc.set_dup_table_trans(!stmt_desc.is_readonly_stmt() && pla.has_duplicate_leader_partition());
  }

#ifdef ERRSIM
  ret = E(EventTable::EN_START_STMT_INTERFACE_ERROR) OB_SUCCESS;
  if (OB_ERR_UNEXPECTED == ret) {
    return ret;
  }
#endif

  if (trans_desc.is_xa_local_trans()) {
    // sync state from original scheduler
    TRANS_LOG(INFO, "xa trans start stmt", K(trans_desc));
    ObXATransID xid = trans_desc.get_xid();
    ObScheTransCtx* sche_ctx = trans_desc.get_sche_ctx();
    if (OB_UNLIKELY(NULL == sche_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "sche ctx is null", K(trans_desc));
    } else if (!sche_ctx->is_xa_tightly_coupled()) {
      // do nothing
    } else if (self_ == trans_desc.get_orig_scheduler()) {
      if (OB_FAIL(sche_ctx->check_for_xa_execution(false /*is_new_branch*/, xid))) {
        if (OB_TRANS_XA_BRANCH_FAIL == ret) {
          TRANS_LOG(INFO, "xa trans has terminated", K(ret), K(trans_desc));
        } else {
          TRANS_LOG(WARN, "unexpected scheduler for xa execution", K(ret), K(*sche_ctx));
        }
      } else if (OB_SUCCESS != (ret = sche_ctx->xa_try_global_lock(xid))) {
        // ret = OB_TRANS_STMT_NEED_RETRY;
        TRANS_LOG(INFO, "xa trans try get global lock failed, retry stmt", K(ret), K(trans_desc));
      } else {
        sche_ctx->set_tmp_trans_desc(trans_desc);
        if (OB_FAIL(sche_ctx->xa_sync_status_response(*(sche_ctx->get_trans_desc()), true))) {
          TRANS_LOG(WARN, "local stmt pull failed", K(ret), K(trans_desc));
        } else {
          trans_desc.set_sche_ctx(sche_ctx);
          trans_desc.set_xid(xid);
          sche_ctx->set_xid(xid);
          xa_need_release_lock = true;
          TRANS_LOG(INFO, "local deep copy", K(trans_desc));
        }
      }
    } else {
      TRANS_LOG(INFO, "remote exclusive", K(trans_desc));
      if (sche_ctx->is_terminated()) {
        ret = OB_TRANS_XA_BRANCH_FAIL;
        TRANS_LOG(INFO, "xa trans has terminated", K(ret), K(trans_desc));
      } else if (OB_FAIL(xa_try_remote_lock_(trans_desc))) {
        if (OB_TRANS_STMT_NEED_RETRY == ret) {
          TRANS_LOG(INFO, "xa try remote lock failed, try again", K(ret), K(trans_desc));
        } else if (OB_TRANS_XA_BRANCH_FAIL == ret || OB_TRANS_CTX_NOT_EXIST == ret) {
          TRANS_LOG(INFO, "xa trans has terminated", K(ret), K(trans_desc));
          need_delete_xa_all_tightly_branch = true;
          sche_ctx->set_terminated();
          ret = OB_TRANS_XA_BRANCH_FAIL;
        } else {
          TRANS_LOG(WARN, "xa try remote lock failed", K(ret), K(trans_desc));
        }
      } else {
        xa_need_release_lock = true;
      }
    }
    (void)trans_desc.get_trans_param().reset_read_snapshot_type_for_isolation();
  }

  bool is_external_consistent = (NULL == ts_mgr_ ? false : ts_mgr_->is_external_consistent(tenant_id));
  if (trans_desc.is_xa_local_trans() && OB_SUCCESS != ret) {
    TRANS_LOG(WARN, "xa trans start stmt failed", K(ret), K(trans_desc));
  } else if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!stmt_param.is_valid()) || OB_UNLIKELY(!stmt_desc.is_valid()) ||
             OB_UNLIKELY(!trans_desc.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(stmt_param), K(stmt_desc), K(trans_desc), K(pla));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(stmt_desc.stmt_type_ != stmt::T_SELECT && pla.count() > 0 && !pla.is_single_tenant())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "multi tenant statement not supported", KR(ret), K(trans_desc), K(pla), K(stmt_desc));
  } else if (OB_UNLIKELY(multi_tenant_uncertain_phy_plan_(stmt_desc, participants))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "multi tenant uncertain phy plan not supported", KR(ret), K(trans_desc), K(pla), K(stmt_desc));
  } else if (OB_UNLIKELY(is_readonly && !stmt_desc.is_readonly_stmt())) {
    TRANS_LOG(WARN, "can not execute dml in readonly transaction", K(trans_desc));
    ret = OB_ERR_READ_ONLY_TRANSACTION;
  } else if (OB_UNLIKELY(is_readonly && stmt_desc.is_sfu_)) {
    ret = OB_ERR_READ_ONLY_TRANSACTION;
    TRANS_LOG(WARN, "select for update is not readonly transaction", K(trans_desc));
    // if a transaction both timeout on transaction and stmt level,
    // return transaction timeout preferentially
  } else if (OB_UNLIKELY(trans_desc.is_trx_idle_timeout())) {
    TRANS_LOG(WARN, "transaction is idle timeout", K(trans_desc));
    ret = OB_TRANS_NEED_ROLLBACK;
  } else if (OB_UNLIKELY(trans_desc.is_trans_timeout())) {
    TRANS_LOG(WARN, "transaction is timeout", K(trans_desc));
    ret = OB_TRANS_TIMEOUT;
  } else if (OB_UNLIKELY(ObClockGenerator::getClock() >= expired_time)) {
    TRANS_LOG(WARN, "statement is timeout", K(trans_desc), K(expired_time));
    ret = OB_TRANS_STMT_TIMEOUT;
    if (trans_desc.is_bounded_staleness_read()) {
      ObTransStatistic::get_instance().add_slave_read_stmt_timeout_count(tenant_id, 1);
    } else {
      ObTransStatistic::get_instance().add_stmt_timeout_count(tenant_id, 1);
    }
    // cannot support cross server strong consitency read, when disable gts
    // and
    // 1. cross server insert can be supported
    // 2. weak consitency read also can be supported
  } else if (OB_UNLIKELY(pla.count() > 0 && trans_desc.get_trans_param().need_strong_consistent_snapshot() &&
                         sql::stmt::T_INSERT != stmt_desc.stmt_type_ && !pla.is_single_leader() &&
                         !is_external_consistent)) {
    TRANS_LOG(WARN, "violate transaction consistency", K(trans_desc), K(stmt_desc), K(pla));
    ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_ERR_DISTRIBUTED_NOT_SUPPORTED, "strong consistency across distributed node");
  } else if (OB_UNLIKELY(trans_desc.need_rollback())) {
    TRANS_LOG(WARN, "transaction need rollback", K(trans_desc));
    ret = OB_TRANS_NEED_ROLLBACK;
  } else if (OB_FAIL(trans_desc.set_cur_stmt_expired_time(expired_time))) {
    TRANS_LOG(WARN, "set statement expired time error", KR(ret), K(trans_desc), K(expired_time));
    // transaction level trace_id, generated at first stmtment start
  } else if (OB_UNLIKELY(0 == trans_desc.get_sql_no() &&
                         OB_FAIL(trans_desc.set_trans_app_trace_id_str(stmt_desc.app_trace_id_str_)))) {
    TRANS_LOG(WARN, "set app trace id error", KR(ret), K(stmt_desc), K(trans_desc));
  } else if (OB_FAIL(trans_desc.set_stmt_participants(participants))) {
    TRANS_LOG(WARN, "set statement participants error", KR(ret), K(trans_desc), K(participants));
  } else if (OB_FAIL(trans_desc.set_stmt_participants_pla(pla))) {
    TRANS_LOG(WARN, "set statement participants leader array error", KR(ret), K(trans_desc), K(pla));
  } else if (OB_UNLIKELY(is_dup_table_stmt && !is_external_consistent)) {
    // duplicated table stmt need GTS to be enabled
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "dup table need gts", KR(ret), K(trans_desc));
  } else if (OB_UNLIKELY(is_dup_table_stmt && verify_duplicate_partition_(trans_desc, pla))) {
    // duplicated table stmt can't read follower when transaction has modifications at its partitions
    ret = OB_USE_DUP_FOLLOW_AFTER_DML;
    TRANS_LOG(WARN, "verify duplicate partition error", KR(ret), K(trans_desc));
  } else if (OB_PHY_PLAN_UNCERTAIN == stmt_desc.phy_plan_type_ && !is_external_consistent &&
             ObTransReadSnapshotType::PARTICIPANT_SNAPSHOT != read_snapshot_type) {
    // GTS must be enabled when SQL-Plan was UNCERTAIN
    TRANS_LOG(WARN, "uncertain plan not supported when violating external consistency", K(trans_desc));
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "uncertain plan violating external consistency");
  } else if (OB_FAIL(check_and_set_trans_consistency_type_(trans_desc))) {
    TRANS_LOG(WARN, "check and set trans consistency type fail", KR(ret), K(trans_desc));
  } else {
    // statistics of update stmt count for non-system tenants
    if (check_is_multi_partition_update_stmt_(trans_desc, participants, stmt_desc)) {
      TRANS_MULTI_PARTITION_UPDATE_STMT_COUNT_INC(trans_desc.get_tenant_id());
    }
    // decide transaction-type
    if (OB_FAIL(decide_trans_type_(trans_desc, stmt_desc, pla, is_external_consistent))) {
      TRANS_LOG(WARN, "decide trans type fail", KR(ret), K(stmt_desc), K(trans_desc), K(pla));
    } else {
      if (trans_desc.cluster_version_before_2271()) {
        trans_desc.inc_sub_sql_no();
      } else {
        trans_desc.start_main_stmt();
      }
      // if (1 == trans_desc.get_sub_sql_no()) {
      // ELR enabled when both of transaction level and tenant level config enabled
      if (!stmt_param.is_trx_elr()) {
        // TODO enable it in future
        // trans_desc.set_can_elr(false);
      }
      if (trans_desc.is_can_elr()) {
        TRANS_LOG(DEBUG, "current trans can early lock release", K(trans_desc));
      }
      //}
      // decide snapshot generation type by transaction type and tenant config
      // generate snapshot for generation type `APPOINT`
      if (OB_FAIL(decide_read_snapshot_(
              stmt_param, pla, stmt_desc, is_external_consistent, trans_desc, unreachable_partitions))) {
        TRANS_LOG(WARN, "decide read snapshot fail", KR(ret), K(stmt_desc), K(trans_desc), K(pla));
      } else {
        if (trans_desc.is_mini_sp_trans() || trans_desc.is_sp_trans()) {
          // don't create sche_ctx for SP_TRANS type
          if (OB_UNLIKELY(ObTransSnapshotGeneType::CONSULT == trans_desc.get_snapshot_gene_type())) {
            ret = OB_NOT_SUPPORTED;
            TRANS_LOG(WARN, "snapshot gene type should not be CONSULT", KR(ret), K(trans_desc));
          }
        }
        // for non SP_TRANS, create sche_ctx and consult snapshot for snapshot generation type `CONSULT`
        else if (OB_FAIL(handle_dist_start_stmt_(stmt_param, pla, stmt_desc, trans_desc, unreachable_partitions))) {
          TRANS_LOG(WARN, "handle distribute start stmt error", KR(ret), K(stmt_desc), K(trans_desc), K(stmt_param));
        } else {
          // success
        }
      }
    }

    // snaity check snapshot
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_snapshot_for_start_stmt_(trans_desc, pla))) {
        TRANS_LOG(WARN, "check snapshot after start stmt fail", KR(ret), K(trans_desc));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (0 != unreachable_partitions.count()) {
      TRANS_LOG(ERROR, "unexpected unreachable partitions count", KR(ret), "count", unreachable_partitions.count());
      ret = OB_ERR_UNEXPECTED;
    }
  }
  REC_TRACE_EXT(tlog, start_stmt, Y(ret), OB_ID(sql_no), trans_desc.get_sql_no());
  if (OB_FAIL(ret)) {
    trans_desc.set_need_print_trace_log();
    TRANS_LOG(WARN, "start statement failed", KR(ret), K(trans_desc), K(participants), K(stmt_param));
  }
  if (OB_FAIL(ret)) {
    if (xa_need_release_lock) {
      if (OB_SUCCESS != (tmp_ret = xa_release_lock_(trans_desc))) {
        if (OB_TRANS_XA_BRANCH_FAIL == ret || OB_TRANS_CTX_NOT_EXIST == ret) {
          trans_desc.get_sche_ctx()->set_terminated();
          need_delete_xa_all_tightly_branch = true;
          ret = OB_TRANS_XA_BRANCH_FAIL;
          TRANS_LOG(INFO, "original scheduler has terminated", K(ret), K(trans_desc));
        } else {
          TRANS_LOG(WARN, "release xa lock failed", K(tmp_ret), K(trans_desc));
        }
      }
    }
    if (OB_TRANS_XA_BRANCH_FAIL == ret) {
      (void)clear_branch_for_xa_terminate_(trans_desc, trans_desc.get_sche_ctx(), need_delete_xa_all_tightly_branch);
    }
  }

  return ret;
}

int ObTransService::decide_trans_type_(ObTransDesc& trans_desc, const ObStmtDesc& stmt_desc,
    const ObPartitionLeaderArray& pla, const bool is_external_consistent)
{
  int ret = OB_SUCCESS;
  const ObPartitionArray& participants = pla.get_partitions();

  const bool is_this_stmt_local_trans =
      (trans_desc.is_local_trans() && participants.count() > 0 && pla.is_single_leader() &&
          pla.get_leaders().at(0) == self_ && OB_PHY_PLAN_UNCERTAIN != stmt_desc.phy_plan_type_);

  if (trans_desc.is_current_read()) {
    ret = decide_trans_type_for_current_read_(trans_desc, stmt_desc, pla, is_external_consistent);
  } else if (trans_desc.is_bounded_staleness_read()) {
    ret = decide_trans_type_for_bounded_staleness_read_(trans_desc, stmt_desc, pla);
  } else {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "unknown trans consistency type, not supported", KR(ret), K(trans_desc));
  }

  trans_desc.set_local_trans(is_this_stmt_local_trans);

  return ret;
}

int ObTransService::convert_sp_trans_to_dist_trans_(ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(trans_desc.get_part_ctx());
  if (NULL == part_ctx) {
    TRANS_LOG(INFO, "no need to recover scheduler context", K(trans_desc));
    // it maybe the stmt has rollbacked
  } else if (OB_FAIL(recover_sche_ctx_(trans_desc, part_ctx))) {
    TRANS_LOG(WARN, "recover scheduler context error", KR(ret), K(trans_desc));
    (void)trans_desc.set_sche_ctx(NULL);
  } else {
    // recover participants
    part_ctx->recover_dist_trans(self_);
    (void)part_trans_ctx_mgr_.revert_trans_ctx(part_ctx);
    TRANS_LOG(INFO, "switch sp_trans to dist trans success", K(trans_desc));
  }

  if (OB_SUCCESS == ret) {
    trans_desc.set_trans_type(TransType::DIST_TRANS);
  }
  return ret;
}

int ObTransService::decide_trans_type_for_bounded_staleness_read_(
    ObTransDesc& trans_desc, const ObStmtDesc& stmt_desc, const ObPartitionLeaderArray& pla)
{
  int ret = OB_SUCCESS;
  if (trans_desc.is_autocommit() && OB_PHY_PLAN_DISTRIBUTED != stmt_desc.phy_plan_type_ &&
      OB_PHY_PLAN_UNCERTAIN != stmt_desc.phy_plan_type_ && trans_desc.is_readonly() && pla.is_single_partition()) {
    // optmization for MIN_SP_TRANS
    trans_desc.set_trans_type(TransType::MINI_SP_TRANS);
  } else if (trans_desc.is_sp_trans()) {
    // don't optmize SP_TRANS, convert to DIST_TRANS
    if (OB_FAIL(convert_sp_trans_to_dist_trans_(trans_desc))) {
      TRANS_LOG(WARN, "convert sp trans to dist trans fail", KR(ret), K(trans_desc), K(stmt_desc));
    } else {
      // success
    }
  } else {
    trans_desc.set_trans_type(TransType::DIST_TRANS);
  }
  return ret;
}

int ObTransService::decide_trans_type_for_current_read_(ObTransDesc& trans_desc, const ObStmtDesc& stmt_desc,
    const ObPartitionLeaderArray& pla, const bool is_external_consistent)
{
  int ret = OB_SUCCESS;
  if (stmt_desc.has_valid_read_snapshot() || OB_PHY_PLAN_UNCERTAIN == stmt_desc.phy_plan_type_) {
    // force to handle with DIST_TRANS
    if (trans_desc.is_sp_trans()) {
      if (OB_FAIL(convert_sp_trans_to_dist_trans_(trans_desc))) {
        TRANS_LOG(WARN, "convert sp trans to dist trans fail", KR(ret), K(trans_desc), K(stmt_desc), K(pla));
      }
    } else {
      trans_desc.set_trans_type(TransType::DIST_TRANS);
    }
  } else {  // has nont appoint snapshot, decide snapshot and transaction type
    // optmize single partition transaction
    if (trans_desc.is_autocommit() && OB_PHY_PLAN_DISTRIBUTED != stmt_desc.phy_plan_type_ && trans_desc.is_readonly() &&
        pla.is_single_partition()) {
      trans_desc.set_trans_type(TransType::MINI_SP_TRANS);
    } else {
      if (0 == trans_desc.get_sql_no()) {
        if (trans_desc.is_xa_local_trans()) {
          trans_desc.set_trans_type(TransType::DIST_TRANS);
          // create participant context at start stmt:
          //(1) ac = 1, plan_type = LOCAL/REMOTE, cur_desc.is_single_partition() = 1;
          //(2) ac = 0, plan_type = LOCAL, cur_desc.is_single_partition() = 1;
        } else if ((trans_desc.is_autocommit() && OB_PHY_PLAN_DISTRIBUTED != stmt_desc.phy_plan_type_ &&
                       pla.is_single_partition()) ||
                   (!trans_desc.is_autocommit() && OB_PHY_PLAN_LOCAL == stmt_desc.phy_plan_type_ &&
                       pla.is_single_partition())) {
          trans_desc.set_trans_type(TransType::SP_TRANS);
        } else {
          // other case, set trans_type to DIST_TRANS
          // @note: can't assume first sql's trans_type is DIST_TRANS, because if first stmt start failed
          // but trans_type already set to SP_TRANS during its starting flow.
          trans_desc.set_trans_type(TransType::DIST_TRANS);
        }
        // if previouse sql has decide transaction is SP_TRANS
      } else if (trans_desc.is_sp_trans()) {
        ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(trans_desc.get_part_ctx());
        if (NULL == part_ctx) {
          // it can happen when stmt execution occurred -6001 error then ctx was released
          if ((trans_desc.is_autocommit() && OB_PHY_PLAN_DISTRIBUTED != stmt_desc.phy_plan_type_ &&
                  pla.is_single_partition()) ||
              (!trans_desc.is_autocommit() && OB_PHY_PLAN_LOCAL == stmt_desc.phy_plan_type_ &&
                  pla.is_single_partition())) {
            if (0 != trans_desc.get_participants().count()) {
              if (OB_UNLIKELY(!trans_desc.is_sp_trans_exiting())) {
                TRANS_LOG(WARN, "participants count no equal to 0, unexpected error", K(trans_desc));
                ret = OB_ERR_UNEXPECTED;
              } else {
                // trans_desc's part_ctx was NULL, but participants.count() != 0,
                // which indicate transaction has executed stmt successfully, and transaction was killed.(bug:10626110)
                ret = OB_TRANS_KILLED;
              }
            } else {
              trans_desc.set_trans_type(TransType::SP_TRANS);
            }
          } else {
            // two possible reason:
            // 1. first stmt was retrying, and hasn't create transaction context successfully
            // 2. transaction was killed
            // distinguish these situations by `is_sp_trans_exiting`
            if (trans_desc.is_sp_trans_exiting()) {
              ret = OB_TRANS_KILLED;
            } else {
              trans_desc.set_trans_type(TransType::DIST_TRANS);
            }
          }
        } else {
          const ObPartitionKey& partition = part_ctx->get_partition();
          if (!need_switch_to_dist_trans_(trans_desc, partition, pla, is_external_consistent) &&
              !part_ctx->is_changing_leader()) {
            // transaction type is SP_TRANS here
            trans_desc.set_trans_type(TransType::SP_TRANS);
            if (part_ctx->is_exiting()) {
              ret = OB_TRANS_KILLED;
            } else if (part_ctx->is_for_replay()) {
              ret = OB_NOT_MASTER;
            } else {
              // do nothing
            }
          } else {
            // convert single part transaction to distributed transaction type
            trans_desc.set_trans_type(TransType::DIST_TRANS);
            if (OB_FAIL(recover_sche_ctx_(trans_desc, part_ctx))) {
              TRANS_LOG(WARN, "recover scheduler context error", KR(ret), K(trans_desc));
              (void)static_cast<ObTransDesc&>(trans_desc).set_sche_ctx(NULL);
            } else {
              // recover participant
              part_ctx->recover_dist_trans(self_);
              (void)part_trans_ctx_mgr_.revert_trans_ctx(part_ctx);
              TRANS_LOG(DEBUG, "switch sp_trans to dist trans success", K(trans_desc), K(pla), K(stmt_desc));
            }
          }
        }
      } else {
        // do nothing
      }
    }

    if (OB_SUCC(ret)) {
      if (sql::stmt::T_SELECT != stmt_desc.stmt_type_ || stmt_desc.is_sfu_) {
        trans_desc.set_all_select_stmt(false);
      }
    }
  }
  return ret;
}

// at the end of start_stmt, check snapshot is valid and satisfy relative constraints
int ObTransService::check_snapshot_for_start_stmt_(const ObTransDesc& trans_desc, const ObPartitionLeaderArray& pla)
{
  int ret = OB_SUCCESS;
  int32_t snapshot_gene_type = trans_desc.get_snapshot_gene_type();
  int64_t snapshot_version = trans_desc.get_snapshot_version();

  // snapshot must already decided for APPOINT and CONSULT
  if (ObTransSnapshotGeneType::APPOINT == snapshot_gene_type ||
      ObTransSnapshotGeneType::CONSULT == snapshot_gene_type) {
    if (OB_UNLIKELY(snapshot_version <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "snapshot version should be valid", K(snapshot_gene_type), K(snapshot_version), K(trans_desc));
    } else {
      // success
    }
  } else if (ObTransSnapshotGeneType::NOTHING == snapshot_gene_type) {
    // snapshot must not ready for NOTHING
    if (OB_UNLIKELY(ObTransVersion::INVALID_TRANS_VERSION != snapshot_version)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "snapshot version should be invalid", K(snapshot_gene_type), K(snapshot_version), K(trans_desc));
    }
    // in NOTHING case, can't support consistent snapshot for multiple partition stmt
    else if (trans_desc.get_trans_param().need_consistent_snapshot() && OB_UNLIKELY(!pla.is_single_partition())) {
      ret = OB_NOT_SUPPORTED;
      TRANS_LOG(WARN,
          "NOTHING snapshot gene type is not supported when "
          "transaction need consistent snapshot and has multi-participants",
          K(ret),
          K(snapshot_gene_type),
          K(snapshot_version),
          K(trans_desc),
          K(pla));
    } else {
      // success
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN,
        "unknown snapshot gene type, not suppport",
        K(ret),
        K(snapshot_gene_type),
        K(snapshot_version),
        K(trans_desc));
  }

  return ret;
}

// veirfy and decide ObTransConsistencyType
int ObTransService::check_and_set_trans_consistency_type_(ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  int32_t trans_consistency_type = trans_desc.get_trans_param().get_consistency_type();
  if (0 == trans_desc.get_sql_no()) {
    // save to trans_desc and verify at each stmt start
    if (OB_FAIL(trans_desc.set_trans_consistency_type(trans_consistency_type))) {
      TRANS_LOG(WARN, "set transaction consistency type error", KR(ret), K(trans_desc));
    }
  } else if (OB_UNLIKELY(!trans_desc.is_identical_consistency_type(trans_consistency_type))) {
    TRANS_LOG(WARN,
        "different consistency type in current transaction, unexpected error",
        K(trans_desc),
        K(trans_consistency_type));
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "different consistency type in current transaction");
  } else {
    // do nothing
  }
  return ret;
}

/* decide ObTransSnapshotGenType by SQL-Layer's  ObTransConsistencyType and ObTransReadSnapshotType parameter
 * ObTransSnapshotGenType:
 * . APPOINT appoint snapshot, generated just now
 * . CONSULT consult snapshot by ObScheTransCtx
 * . NOTHING don't need to generate snapshot, snapshot was generated at participants side
 *
 * @note for semantic of interface exported by Transaction-Layer see ObTransConsistencyType
 */
int ObTransService::decide_read_snapshot_(const ObStmtParam& stmt_param, const ObPartitionLeaderArray& pla,
    const ObStmtDesc& stmt_desc, const bool is_external_consistent, ObTransDesc& trans_desc,
    ObPartitionArray& unreachable_partitions)
{
  int ret = OB_SUCCESS;
  int32_t snapshot_gene_type = ObTransSnapshotGeneType::UNKNOWN;
  int64_t snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;

  const ObStartTransParam& trans_param = trans_desc.get_trans_param();
  const int32_t consistency_type = trans_param.get_consistency_type();
  const int32_t read_snapshot_type = trans_param.get_read_snapshot_type();
  const bool stmt_specific_read_snapshot_is_valid = stmt_desc.has_valid_read_snapshot();

  trans_desc.reset_snapshot_version();
  trans_desc.reset_snapshot_gene_type();

  if (ObTransReadSnapshotType::TRANSACTION_SNAPSHOT == read_snapshot_type) {
    // only support SI isolation level
    if (trans_desc.is_serializable_isolation()) {
      ret = decide_read_snapshot_for_serializable_trans_(stmt_desc, trans_desc, snapshot_gene_type, snapshot_version);
    } else {
      ret = OB_NOT_SUPPORTED;
      TRANS_LOG(
          ERROR, "TRANSACTION_SNAPSHOT only supports SERIALIZABLE transaction", KR(ret), K(trans_desc), K(stmt_desc));
    }
  } else if (OB_UNLIKELY(trans_desc.is_serializable_isolation())) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(
        ERROR, "SERIALIZABLE transaction only supports TRANSACTION_SNAPSHOT", KR(ret), K(trans_desc), K(stmt_desc));
  } else if (ObTransReadSnapshotType::PARTICIPANT_SNAPSHOT == read_snapshot_type) {
    // only used by inner table access
    if (stmt_specific_read_snapshot_is_valid) {
      snapshot_gene_type = ObTransSnapshotGeneType::APPOINT;
      snapshot_version = stmt_desc.cur_stmt_specified_snapshot_version_;
    } else {
      snapshot_gene_type = ObTransSnapshotGeneType::NOTHING;
      snapshot_version = ObTransVersion::ObTransVersion::INVALID_TRANS_VERSION;
    }
  } else if (ObTransReadSnapshotType::STATEMENT_SNAPSHOT == read_snapshot_type) {
    // user specified snapshot, eg: sql used by global index building routine
    if (stmt_specific_read_snapshot_is_valid) {
      snapshot_gene_type = ObTransSnapshotGeneType::APPOINT;
      snapshot_version = stmt_desc.cur_stmt_specified_snapshot_version_;
    } else {
      if (ObTransConsistencyType::CURRENT_READ == consistency_type) {
        // read latest data
        ret = decide_statement_snapshot_for_current_read_(
            pla, stmt_desc, trans_desc, is_external_consistent, snapshot_gene_type, snapshot_version);
      } else if (ObTransConsistencyType::BOUNDED_STALENESS_READ == consistency_type) {
        // read stale data
        ret = decide_statement_snapshot_for_bounded_staleness_read_(
            stmt_param, pla, stmt_desc, trans_desc, unreachable_partitions, snapshot_gene_type, snapshot_version);
      } else {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "unknown trans consistency type", KR(ret), K(consistency_type), K(stmt_desc), K(trans_desc));
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "unknown trans read snapshot type", KR(ret), K(read_snapshot_type), K(stmt_desc), K(trans_desc));
  }

  if (OB_SUCCESS == ret) {
    if (OB_UNLIKELY(!ObTransSnapshotGeneType::is_valid(snapshot_gene_type))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN,
          "snapshot generate type is invalid",
          KR(ret),
          K(snapshot_gene_type),
          K(snapshot_version),
          K(trans_desc),
          K(stmt_desc));
    } else {
      trans_desc.set_snapshot_gene_type(snapshot_gene_type);
      if (ObTransSnapshotGeneType::APPOINT == snapshot_gene_type) {
        if (OB_UNLIKELY(snapshot_version <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN,
              "snapshot version is invalid under APPOINT snapshot_gene_type",
              KR(ret),
              K(snapshot_gene_type),
              K(snapshot_version),
              K(trans_desc),
              K(stmt_desc));
        } else {
          (void)trans_desc.set_snapshot_version(snapshot_version);
        }
      } else {
        // other case, don't need set snapshot
      }
    }
  }

  TRANS_LOG(DEBUG,
      "ObTransService::start_stmt::decide_read_snapshot",
      KR(ret),
      K(snapshot_gene_type),
      K(snapshot_version),
      K(trans_desc),
      K(stmt_desc));
  return ret;
}

int ObTransService::decide_read_snapshot_for_serializable_trans_(
    const ObStmtDesc& stmt_desc, const ObTransDesc& trans_desc, int32_t& snapshot_gene_type, int64_t& snapshot_version)
{
  int ret = OB_SUCCESS;
  const int32_t consistency_type = trans_desc.get_trans_param().get_consistency_type();
  const int32_t read_snapshot_type = trans_desc.get_trans_param().get_read_snapshot_type();

  // SI don't support user specified snapshot
  if (OB_UNLIKELY(stmt_desc.has_valid_read_snapshot())) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(
        ERROR, "not allow specific snapshot version of serializable transaction", KR(ret), K(trans_desc), K(stmt_desc));
  }
  // only support read latest data
  else if (OB_UNLIKELY(ObTransConsistencyType::CURRENT_READ != consistency_type)) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(ERROR,
        "statement of weak consistency is not allowed under SERIALIZABLE isolation",
        KR(ret),
        K(trans_desc),
        K(stmt_desc));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "weak consistency under SERIALIZABLE isolation level");
  }
  // read_snapshot_type must be TRANSACTION_SNAPSHOT
  else if (OB_UNLIKELY(ObTransReadSnapshotType::TRANSACTION_SNAPSHOT != read_snapshot_type)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR,
        "unexpected error, read snapshot type is not TRANSACTION_SNAPSHOT "
        "under serializable transaction",
        KR(ret),
        K(trans_desc),
        K(stmt_desc));
  }
  // check trans_snapshot_version was valid, which shoul be acquired from gts in start_transaction phase
  else if (OB_UNLIKELY(trans_desc.get_trans_snapshot_version() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR,
        "unexpected transaction snapshot version under serializable transaction",
        KR(ret),
        K(trans_desc),
        K(stmt_desc));
  } else {
    snapshot_gene_type = ObTransSnapshotGeneType::APPOINT;
    snapshot_version = trans_desc.get_trans_snapshot_version();
  }
  return ret;
}

int ObTransService::decide_statement_snapshot_for_current_read_(const ObPartitionLeaderArray& pla,
    const ObStmtDesc& stmt_desc, ObTransDesc& trans_desc, const bool is_external_consistent,
    int32_t& snapshot_gene_type, int64_t& snapshot_version)
{
  int ret = OB_SUCCESS;
  int32_t consistency_type = trans_desc.get_trans_param().get_consistency_type();
  int32_t read_snapshot_type = trans_desc.get_trans_param().get_read_snapshot_type();

  bool multi_tenant_stmt = pla.count() > 0 ? !pla.is_single_tenant() : false;
  ObPhyPlanType phy_plan_type = stmt_desc.phy_plan_type_;
  const ObPartitionArray& participants = pla.get_partitions();
  uint64_t tenant_id =
      (OB_PHY_PLAN_UNCERTAIN == phy_plan_type) ? stmt_desc.stmt_tenant_id_ : pla.get_partitions().at(0).get_tenant_id();

  if (OB_ISNULL(ts_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid ts_mgr", KR(ret), K(ts_mgr_), K(stmt_desc), K(trans_desc));
  } else if (OB_UNLIKELY(ObTransReadSnapshotType::STATEMENT_SNAPSHOT != read_snapshot_type)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN,
        "invalid snapshot type which is not STATEMENT_SNAPSHOT",
        KR(ret),
        K(consistency_type),
        K(stmt_desc),
        K(trans_desc));
  } else if (multi_tenant_stmt) {
    if (share::is_oracle_mode()) {
      ret = OB_NOT_SUPPORTED;
      TRANS_LOG(ERROR, "multi tenant statement not supported", KR(ret), K(stmt_desc), K(trans_desc));
    } else {
      // must consult snapshot for multi-tenant stmt
      TRANS_LOG(INFO,
          "multi tenant transaction need consult snapshot version",
          K(multi_tenant_stmt),
          K(stmt_desc),
          K(trans_desc));

      if (OB_UNLIKELY(trans_desc.is_sp_trans() || trans_desc.is_mini_sp_trans())) {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(
            WARN, "sp trans is not supported in multi-tenant trans", KR(ret), K(trans_desc), K(stmt_desc), K(pla));
      } else {
        snapshot_gene_type = ObTransSnapshotGeneType::CONSULT;
        snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;
      }
    }
  } else if (!is_external_consistent) {
    // for SP_TRANS, if gts was disabled, snapshot can be generated by participant
    if (trans_desc.is_sp_trans() || trans_desc.is_mini_sp_trans()) {
      const ObPartitionKey& pg_key = participants.at(0);
      int64_t part_snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;
      bool is_not_create_ctx_participant = trans_desc.is_not_create_ctx_participant(pg_key);
      if (OB_UNLIKELY(!pla.is_single_partition())) {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN,
            "sp trans of multi-participants is not supported when GTS is disabled",
            KR(ret),
            K(pla),
            K(trans_desc));
      } else if (OB_FAIL(generate_part_snapshot_for_current_read_(trans_desc.is_can_elr(),
                     trans_desc.is_readonly(),
                     trans_desc.get_cur_stmt_expired_time(),
                     trans_desc.get_trans_expired_time(),
                     is_not_create_ctx_participant,
                     pg_key,
                     part_snapshot_version))) {
        TRANS_LOG(WARN,
            "generate part snapshot for current read fail",
            KR(ret),
            K(pg_key),
            K(participants),
            K(trans_desc),
            K(pla),
            K(self_));
      } else {
        snapshot_gene_type = ObTransSnapshotGeneType::APPOINT;
        snapshot_version = part_snapshot_version;
        TRANS_LOG(DEBUG,
            "generate snapshot version for local single partition trans success",
            K(snapshot_version),
            K(snapshot_gene_type),
            K(trans_desc),
            K(stmt_desc),
            K(participants),
            K(pla),
            K(self_));
      }
    } else {
      snapshot_gene_type = ObTransSnapshotGeneType::CONSULT;
      snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;
    }
  } else {
    // *************** GTS enabled *****************
    // 1. snapshot was generated at start_stmt phase
    // 2. SP_TRANS won't create sche_ctx, won't consult snapshot
    // 3. APPOINT type contains: acquired from gts or from local server' gts-cache (aka. publish_version)

    bool need_gts = true;
    bool is_local_trans = (participants.count() > 0 && pla.is_single_leader() && pla.get_leaders().at(0) == self_);
    bool is_local_single_part_trans = (is_local_trans && participants.count() == 1);

    if (OB_PHY_PLAN_UNCERTAIN == phy_plan_type) {
      need_gts = true;
    } else if (is_local_single_part_trans) {
      need_gts = false;
    } else if (trans_desc.is_sp_trans() || trans_desc.is_mini_sp_trans()) {
      need_gts = true;
    } else if (is_local_trans) {
      need_gts = true;
    } else {
      need_gts = true;
    }

    if (need_gts) {
      int64_t gts = 0;
      int64_t trans_expire_time = trans_desc.get_trans_expired_time();
      int64_t stmt_expire_time = trans_desc.get_cur_stmt_expired_time();
      if (OB_FAIL(get_gts_for_snapshot_version_(trans_desc, gts, trans_expire_time, stmt_expire_time, tenant_id))) {
        TRANS_LOG(WARN, "get gts fail", KR(ret), K(tenant_id), K(trans_desc), K(stmt_desc));
      } else {
        snapshot_gene_type = ObTransSnapshotGeneType::APPOINT;
        snapshot_version = gts;
        TRANS_LOG(DEBUG,
            "get gts while start statement success",
            KR(ret),
            K(gts),
            K(participants),
            K(trans_desc),
            K(stmt_desc));
      }
    } else if (is_local_single_part_trans) {
      const ObPartitionKey& pg_key = participants.at(0);
      int64_t part_snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;
      bool is_not_create_ctx_participant = trans_desc.is_not_create_ctx_participant(pg_key);
      if (OB_FAIL(generate_part_snapshot_for_current_read_(trans_desc.is_can_elr(),
              trans_desc.is_readonly(),
              trans_desc.get_cur_stmt_expired_time(),
              trans_desc.get_trans_expired_time(),
              is_not_create_ctx_participant,
              pg_key,
              part_snapshot_version))) {
        TRANS_LOG(WARN,
            "generate part snapshot for current read fail",
            KR(ret),
            K(pg_key),
            K(participants),
            K(trans_desc),
            K(pla),
            K(self_));
      } else {
        snapshot_gene_type = ObTransSnapshotGeneType::APPOINT;
        snapshot_version = part_snapshot_version;
        TRANS_LOG(DEBUG,
            "generate snapshot version for local single partition trans success",
            K(snapshot_version),
            K(snapshot_gene_type),
            K(trans_desc),
            K(stmt_desc),
            K(participants),
            K(pla),
            K(self_));
      }
    } else {
      snapshot_gene_type = ObTransSnapshotGeneType::CONSULT;
      snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;
    }
  }
  return ret;
}

/*
 * generate snapshot for weak read
 * principles:
 * 1. if seting monotonic read(cause-consistency), use WRS to decide snapshot
 * 2. otherwise, consult snapshot (by ask all participants ot this stmt)
 */
int ObTransService::decide_statement_snapshot_for_bounded_staleness_read_(const ObStmtParam& stmt_param,
    const ObPartitionLeaderArray& pla, const ObStmtDesc& stmt_desc, const ObTransDesc& trans_desc,
    ObPartitionArray& unreachable_partitions, int32_t& snapshot_gene_type, int64_t& snapshot_version)
{
  int ret = OB_SUCCESS;
  ObIWeakReadService* wrs = GCTX.weak_read_service_;
  ObPhyPlanType phy_plan_type = stmt_desc.phy_plan_type_;
  uint64_t tenant_id = stmt_desc.stmt_tenant_id_;
  bool enable_monotonic_weak_read = ObWeakReadUtil::enable_monotonic_weak_read(tenant_id);

  // defens: ObSqlTransControl promise inner sql would not need generate snapshot
  //  (they will use NOTHING generate snapshot type)
  if (OB_UNLIKELY(stmt_desc.is_contain_inner_table_)) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "inner table SQL should not generate statement snapshot", KR(ret), K(stmt_desc), K(trans_desc));
  } else if (OB_ISNULL(wrs)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "weak read service is invalid", KR(ret), K(wrs));
  } else {
    snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;
    snapshot_gene_type = ObTransSnapshotGeneType::CONSULT;

    if (enable_monotonic_weak_read) {
      snapshot_gene_type = ObTransSnapshotGeneType::APPOINT;
    }

    else if (OB_PHY_PLAN_UNCERTAIN == phy_plan_type) {
      snapshot_gene_type = ObTransSnapshotGeneType::APPOINT;
    } else {
      snapshot_gene_type = ObTransSnapshotGeneType::CONSULT;
    }

    if (ObTransSnapshotGeneType::APPOINT == snapshot_gene_type) {
      int64_t cur_read_snapshot = 0;
      // get cluster level monotonic snapshot
      // TODO: SQL-Layer add error-code retry handle
      if (OB_FAIL(wrs->get_cluster_version(tenant_id, cur_read_snapshot))) {
        TRANS_LOG(WARN,
            "get weak read cluster version fail",
            KR(ret),
            K(tenant_id),
            K(stmt_desc),
            K(trans_desc),
            K(cur_read_snapshot));
      } else {
        ret = check_snapshot_version_for_bounded_staleness_read_(
            cur_read_snapshot, stmt_param, pla, stmt_desc, trans_desc, unreachable_partitions);
      }
      if (OB_SUCCESS == ret) {
        if (OB_UNLIKELY(cur_read_snapshot <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "current read snapshot version is invalid", KR(ret), K(cur_read_snapshot));
        } else {
          snapshot_version = cur_read_snapshot;
        }
      }
    } else {
      if (trans_desc.is_sp_trans() || trans_desc.is_mini_sp_trans()) {
        if (OB_UNLIKELY(!pla.is_single_partition())) {
          ret = OB_NOT_SUPPORTED;
          TRANS_LOG(WARN, "sp trans of multi-participants is not supported", KR(ret), K(pla), K(trans_desc));
        } else {
          const ObPartitionKey& pkey = pla.get_partitions().at(0);
          ObIPartitionGroupGuard guard;
          ObIPartitionGroup* pg = NULL;
          if (OB_FAIL(get_partition_group_(pkey, guard, pg))) {
            TRANS_LOG(WARN, "get partition group fail", KR(ret), K(pkey), K(pg));
          } else if (OB_ISNULL(pg)) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(WARN, "invalid partition group", K(ret), K(pkey), K(pg));
          } else if (OB_FAIL(generate_part_snapshot_for_bounded_staleness_read_(
                         pkey, *pg, snapshot_version, unreachable_partitions))) {
            TRANS_LOG(WARN, "generate snapshot for bounded staleness read failed", K(ret), K(unreachable_partitions));
          } else {
            snapshot_gene_type = ObTransSnapshotGeneType::APPOINT;
          }
        }
      }
    }
  }
  WRS_CLUSTER_LOG(DEBUG,
      "[WRS] decide_statement_snapshot_for_bounded_staleness_read get cluster version",
      K(tenant_id),
      K(snapshot_version),
      K(snapshot_gene_type),
      K(enable_monotonic_weak_read),
      K(stmt_desc),
      K(trans_desc),
      K(stmt_param),
      K(pla));
  return ret;
}

int ObTransService::check_snapshot_version_for_bounded_staleness_read_(int64_t& cur_read_snapshot,
    const ObStmtParam& stmt_param, const ObPartitionLeaderArray& pla, const ObStmtDesc& stmt_desc,
    const ObTransDesc& trans_desc, ObPartitionArray& unreachable_partitions)
{
  UNUSED(pla);
  UNUSED(unreachable_partitions);
  int ret = OB_SUCCESS;
  const int64_t last_read_snapshot = stmt_param.get_safe_weak_read_snapshot();
  const int64_t last_read_snapshot_source = stmt_param.get_safe_weak_read_snapshot_source();

  // verify relation between current snapshot and previous snapshot,
  // if current snapshot less than previous, use previous snapshot,
  // this promise monotonic read consistency in session scope
  if (cur_read_snapshot < last_read_snapshot) {
    TRANS_LOG(ERROR,
        "last weak read snapshot is great than curent weak read snapshot",
        K(cur_read_snapshot),
        K(last_read_snapshot),
        K(last_read_snapshot_source),
        "last_read_snapshot_source_str",
        ObBasicSessionInfo::source_to_string(last_read_snapshot_source),
        K(stmt_param),
        K(trans_desc),
        K(stmt_desc));
    cur_read_snapshot = last_read_snapshot;
  }

  // check weak read snapshot is stale
  const int64_t snapshot_version_barrier =
      ObTimeUtility::current_time() - ObWeakReadUtil::max_stale_time_for_weak_consistency(trans_desc.get_tenant_id());
  if (cur_read_snapshot < snapshot_version_barrier) {
    TRANS_LOG(WARN,
        "weak consistent read the old data",
        K(cur_read_snapshot),
        "snapshot_barrier",
        snapshot_version_barrier,
        "delta",
        (snapshot_version_barrier - cur_read_snapshot),
        "trans_id",
        trans_desc.get_trans_id());
    ret = OB_REPLICA_NOT_READABLE;
    ObTransStatistic::get_instance().add_slave_read_stmt_retry_count(trans_desc.get_tenant_id(), 1);
  }
  return ret;
}

bool ObTransService::check_participant_epoch_exit_(const ObPartitionKey& pkey, const ObPartitionEpochArray& epoch_arr)
{
  bool bool_ret = false;
  for (int64_t i = 0; i < epoch_arr.count(); i++) {
    if (epoch_arr.at(i).get_partition() == pkey) {
      bool_ret = true;
      break;
    }
  }

  return bool_ret;
}

int ObTransService::end_stmt(bool is_rollback, bool is_incomplete, const ObPartitionArray& cur_stmt_all_participants,
    const ObPartitionEpochArray& epoch_arr, const ObPartitionArray& discard_participants,
    const ObPartitionLeaderArray& pla, ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool tmp_rollback = is_rollback;
  bool need_delete_xa_all_tightly_branch = false;
  ObTransTraceLog& tlog = trans_desc.get_tlog();
  // real commit stmt's participants
  ObPartitionArray real_stmt_participants;
  // participants has create participants
  ObPartitionLeaderArray real_stmt_pla;
  // participants has epoch, these has create context also
  ObPartitionArray epoch_participants;
#ifdef ERRSIM
  ret = E(EventTable::EN_END_STMT_INTERFACE_ERROR) OB_SUCCESS;
  if (OB_ERR_UNEXPECTED == ret) {
    return ret;
  }
#endif
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!trans_desc.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(trans_desc));
    ret = OB_INVALID_ARGUMENT;
  } else if (trans_desc.is_readonly() && !trans_desc.get_cur_stmt_desc().is_readonly_stmt()) {
    TRANS_LOG(WARN, "can not execute dml in readonly transaction", K(trans_desc));
    ret = OB_ERR_READ_ONLY_TRANSACTION;
  } else if (trans_desc.need_rollback()) {
    TRANS_LOG(WARN, "transaction need rollback", K(trans_desc));
    ret = OB_TRANS_NEED_ROLLBACK;
  } else if (trans_desc.is_trans_timeout()) {
    TRANS_LOG(WARN, "transaction is timeout", K(trans_desc));
    ret = OB_TRANS_TIMEOUT;
  } else if (is_incomplete) {
    // participants not complete, transaction must be rollbacked
    ret = OB_TRANS_NEED_ROLLBACK;
    trans_desc.set_need_rollback();
    TRANS_LOG(WARN, "participants is incomplete", KR(ret), K(cur_stmt_all_participants), K(trans_desc));
  } else if (trans_desc.is_bounded_staleness_read()) {
    // weak read no need for check stmt participants
  } else if (cur_stmt_all_participants.count() <= 0) {
    if (is_rollback) {
      TRANS_LOG(INFO, "current statement is empty", K(cur_stmt_all_participants), K(trans_desc));
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected cur stmt participants count", KR(ret), K(cur_stmt_all_participants), K(trans_desc));
    }
  } else if (OB_FAIL(check_stmt_participants_and_epoch_(epoch_arr, epoch_participants))) {
    TRANS_LOG(WARN, "check stmt participants eand epoch error", KR(ret), K(epoch_arr), K(trans_desc));
  } else {
    // transaction whether involve duplicated table can been determined at end_stmt only
    // for uncertain plan stmt
    if (!trans_desc.is_dup_table_trans() &&
        sql::OB_PHY_PLAN_UNCERTAIN == trans_desc.get_cur_stmt_desc().phy_plan_type_) {
      // conditions: stmt not read-only and operate leader replica
      trans_desc.set_dup_table_trans(
          !trans_desc.get_cur_stmt_desc().is_readonly_stmt() && pla.has_duplicate_leader_partition());
    }
    // filter cur_stmt_all_participant by partition, which in discard participants
    // or which no need for creating context (has_epoch)
    if (!is_rollback) {
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_stmt_all_participants.count(); i++) {
        if (is_contain(epoch_participants, cur_stmt_all_participants.at(i)) &&
            !is_contain(discard_participants, cur_stmt_all_participants.at(i))) {
          if (OB_FAIL(real_stmt_participants.push_back(cur_stmt_all_participants.at(i)))) {
            TRANS_LOG(WARN, "push back error", KR(ret), K(cur_stmt_all_participants), K(trans_desc));
          }
        }
      }
    } else {
      // for stmt rollback, filter participants by partition which no need for creating context
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_stmt_all_participants.count(); i++) {
        if (!trans_desc.is_not_create_ctx_participant(cur_stmt_all_participants.at(i))) {
          if (OB_FAIL(real_stmt_participants.push_back(cur_stmt_all_participants.at(i)))) {
            TRANS_LOG(WARN, "push back error", K(ret), K(cur_stmt_all_participants), K(trans_desc));
          }
        }
      }
    }
    // 2. generate PLA({partition, leader_address}) list on which current stmt has create context (has epoch)
    for (int64_t i = 0; OB_SUCC(ret) && i < pla.count(); i++) {
      if (is_contain(epoch_participants, pla.get_partitions().at(i))) {
        if (OB_FAIL(real_stmt_pla.push(pla.get_partitions().at(i), pla.get_leaders().at(i), pla.get_types().at(i)))) {
          TRANS_LOG(WARN, "push back error", KR(ret), K(trans_desc));
        }
      }
    }
  }
  if (trans_desc.is_xa_local_trans()) {
    ObScheTransCtx* sche_ctx = trans_desc.get_sche_ctx();
    if (NULL == sche_ctx) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "scheduler ctx is NULL", K(ret));
    } else if (sche_ctx->is_terminated()) {
      ret = OB_TRANS_XA_BRANCH_FAIL;
      TRANS_LOG(INFO, "xa trans has terminated", K(ret), K(trans_desc));
    }
  }

  if (OB_TRANS_XA_BRANCH_FAIL != ret) {
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans_desc.set_stmt_participants(real_stmt_participants))) {
        TRANS_LOG(WARN, "set stmt participants error", KR(ret), K(cur_stmt_all_participants), K(trans_desc));
      } else if (OB_FAIL(trans_desc.set_stmt_participants_pla(
                     trans_desc.is_bounded_staleness_read() ? pla : real_stmt_pla))) {
        TRANS_LOG(WARN, "set statement participants leader array error", KR(ret), K(trans_desc), K(real_stmt_pla));
      } else if (is_rollback && OB_FAIL(trans_desc.merge_gc_participants(real_stmt_participants))) {
        TRANS_LOG(WARN, "merge gc participants error", KR(ret), K(trans_desc), K(real_stmt_participants));
      } else if (OB_FAIL(handle_end_stmt_(is_rollback, trans_desc))) {
        TRANS_LOG(WARN, "handle end stmt error", KR(ret), K(is_rollback), K(cur_stmt_all_participants), K(epoch_arr));
      } else if (!is_rollback &&
                 OB_FAIL(trans_desc.update_savepoint_partition_info(real_stmt_participants, trans_desc.get_sql_no()))) {
        TRANS_LOG(WARN, "update savepoint partition info error", KR(ret), K(trans_desc));
      } else {
        // do nothing
      }
      if (OB_FAIL(ret)) {
        trans_desc.set_need_rollback();
      }
    } else {
      // starting to rollback stmt
      tmp_rollback = true;
      if (OB_SUCCESS != (tmp_ret = trans_desc.set_stmt_participants(cur_stmt_all_participants))) {
        TRANS_LOG(WARN, "set stmt participants error", "ret", tmp_ret, K(cur_stmt_all_participants), K(trans_desc));
      } else if (OB_PHY_PLAN_UNCERTAIN == trans_desc.get_cur_stmt_desc().phy_plan_type_ &&
                 OB_SUCCESS != (tmp_ret = trans_desc.set_stmt_participants_pla(pla))) {
        TRANS_LOG(WARN, "set statement participants leader array error", "ret", tmp_ret, K(trans_desc), K(pla));
      } else if (OB_SUCCESS != (tmp_ret = trans_desc.merge_gc_participants(real_stmt_participants))) {
        TRANS_LOG(WARN, "merge gc participants error", "ret", tmp_ret, K(trans_desc), K(real_stmt_participants));
      } else if (OB_SUCCESS != (tmp_ret = handle_end_stmt_(tmp_rollback, trans_desc))) {
        TRANS_LOG(WARN, "handle end stmt error", "ret", tmp_ret, K(trans_desc));
      } else {
        // do nothing
      }
      // error NOT_MASTER can be retried, for other errors transaction must be rollbacked
      if (OB_NOT_MASTER != ret || OB_SUCCESS != tmp_ret) {
        trans_desc.set_need_rollback();
      }
    }
  }

  REC_TRACE_EXT(tlog, end_stmt, Y(ret));
  if (OB_FAIL(ret)) {
    trans_desc.set_need_print_trace_log();
    TRANS_LOG(WARN, "end statement failed", KR(ret), K(trans_desc), K(is_rollback), K(tmp_rollback));
  }
  if (trans_desc.is_xa_local_trans()) {
    if (OB_FAIL(xa_release_lock_(trans_desc))) {
      if (OB_TRANS_XA_BRANCH_FAIL == ret || OB_TRANS_CTX_NOT_EXIST == ret) {
        trans_desc.get_sche_ctx()->set_terminated();
        need_delete_xa_all_tightly_branch = true;
        ret = OB_TRANS_XA_BRANCH_FAIL;
        TRANS_LOG(INFO, "original scheduler has terminated", K(ret), K(trans_desc));
      } else {
        TRANS_LOG(WARN, "xa releasee lock failed", K(ret), K(trans_desc));
      }
    }
  }
  if (OB_TRANS_XA_BRANCH_FAIL == ret) {
    // don't override return code
    (void)clear_branch_for_xa_terminate_(trans_desc, trans_desc.get_sche_ctx(), need_delete_xa_all_tightly_branch);
  }

  return ret;
}

int ObTransService::handle_end_stmt_(bool is_rollback, ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObScheTransCtx* sche_ctx = NULL;
  bool rollback_done = false;
  int64_t expired_time = trans_desc.get_cur_stmt_expired_time();
  if (trans_desc.is_bounded_staleness_read()) {
    rollback_done = true;
    // do nothing
  } else if (trans_desc.get_stmt_participants().count() == 0) {
    // it need for merge discard participants
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = trans_desc.merge_participants_pla()))) {
      TRANS_LOG(WARN, "trans desc merge participants pla", K(tmp_ret), K(trans_desc));
    }
    rollback_done = true;
  } else if (trans_desc.is_sp_trans() || trans_desc.is_mini_sp_trans()) {
    if (!is_rollback) {
      if (OB_FAIL(trans_desc.merge_participants())) {
        TRANS_LOG(WARN, "merge_participants failed", KR(ret), K(trans_desc));
      }
    }
    rollback_done = true;
    // rollback sp_trans stmt participant
    if (is_rollback && trans_desc.is_sp_trans()) {
      rollback_done = false;
      ObTransCtx* trans_ctx;
      if (OB_ISNULL(trans_ctx = trans_desc.get_part_ctx())) {
        // maybe start_participant fail
        if (trans_desc.is_sp_trans()) {
          rollback_done = true;
          TRANS_LOG(WARN, "sp trans part_ctx is NULL, please check start_participant fail", K(ret), K(trans_desc));
        } else {
          TRANS_LOG(WARN, "transaction already convert to dis-trans", K(trans_desc));
        }
      } else {
        ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(trans_ctx);
        int retry_cnt = 0;
        do {
          if (OB_FAIL(part_ctx->rollback_stmt(trans_desc.get_sql_no(), trans_desc.get_stmt_min_sql_no()))) {
            if (ret == OB_TRANS_STMT_NEED_RETRY && ObTimeUtility::current_time() < expired_time) {
              usleep(500);
            } else if (ret == OB_TRANS_STMT_NEED_RETRY || ret == OB_NOT_MASTER) {
              // leader shift, convert to dist-trans
              if (OB_FAIL(convert_sp_trans_to_dist_trans_(trans_desc))) {
                TRANS_LOG(WARN, "try convert sp-trans to dist-trans fail", K(ret), K(trans_desc));
                trans_desc.set_need_rollback();
              } else {
                trans_desc.get_sche_ctx()->mark_stmt_started();
                TRANS_LOG(INFO, "convert to dist-trans succeed", K(trans_desc));
              }
            } else {
              // other error, transaction need rollback
              TRANS_LOG(
                  WARN, "try sp-trans rollback fail, transaction need rollback", K(ret), K(trans_desc), K(*part_ctx));
              trans_desc.set_need_rollback();
            }
          } else {
            rollback_done = true;
            trans_desc.clear_stmt_participants();
            TRANS_LOG(DEBUG, "rollback sp trans stmt succeed", K(trans_desc), K(*part_ctx));
          }
        } while (ret == OB_TRANS_STMT_NEED_RETRY);
      }
    }

    // accumulate audit info : lock_for_read
    ObAuditRecordData* audit_record;
    transaction::ObTransCtx* trans_ctx;
    if (trans_desc.is_sp_trans() && GCONF.enable_sql_audit &&
        OB_NOT_NULL(audit_record = trans_desc.get_audit_record()) &&
        OB_NOT_NULL(trans_ctx = trans_desc.get_part_ctx())) {
      static_cast<transaction::ObPartTransCtx*>(trans_ctx)->get_audit_info(audit_record->trx_lock_for_read_elapse_);
      TRANS_LOG(DEBUG, "audit record: ", K(trans_desc), K(audit_record->trx_lock_for_read_elapse_));
    }
  }

  if (rollback_done) {
  } else if (OB_ISNULL(trans_desc.get_sche_ctx())) {
    TRANS_LOG(WARN, "scheduler ctx is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else {  // handle dist-trans-stmt-rollback
    sche_ctx = trans_desc.get_sche_ctx();
    (void)sche_ctx->set_dup_table_trans(trans_desc.is_dup_table_trans());
    bool need_merge_commit_list = false;
    ObWaitEventGuard wait_guard(ObWaitEventIds::END_STMT_WAIT,
        expired_time,
        trans_desc.need_rollback(),
        trans_desc.get_trans_id().hash(),
        trans_desc.get_cur_stmt_desc().phy_plan_type_);
    if (OB_FAIL(sche_ctx->end_stmt(is_rollback, trans_desc))) {
      TRANS_LOG(WARN, "end statement error", KR(ret), K(trans_desc));
    } else if (OB_FAIL(sche_ctx->wait_end_stmt(expired_time + END_STMT_MORE_TIME_US))) {
      TRANS_LOG(WARN, "wait end stmt error", KR(ret), K(trans_desc));
      if (OB_TIMEOUT == ret) {
        ret = OB_TRANS_STMT_TIMEOUT;
        ObTransStatistic::get_instance().add_stmt_timeout_count(trans_desc.get_tenant_id(), 1);
      }
    } else {
      TRANS_LOG(DEBUG, "sche_ctx end stmt succeed", K(trans_desc));
    }
    if (!is_rollback) {
      need_merge_commit_list = true;
    } else if (OB_FAIL(ret)) {
      need_merge_commit_list = true;
    } else {
      need_merge_commit_list = false;
    }
    if (need_merge_commit_list) {
      if (OB_SUCCESS != (tmp_ret = sche_ctx->merge_participants(trans_desc.get_stmt_participants()))) {
        ret = tmp_ret;
      } else if (OB_SUCCESS != (tmp_ret = trans_desc.merge_participants())) {
        ret = tmp_ret;
      } else if (OB_PHY_PLAN_DISTRIBUTED == trans_desc.get_cur_stmt_desc().phy_plan_type_) {
        if (OB_SUCCESS != (tmp_ret = trans_desc.merge_participants_pla())) {
          ret = tmp_ret;
        }
      } else if (OB_PHY_PLAN_UNCERTAIN == trans_desc.get_cur_stmt_desc().phy_plan_type_) {
        if (OB_SUCCESS != (tmp_ret = sche_ctx->merge_partition_leader_info(trans_desc.get_stmt_participants_pla()))) {
          ret = tmp_ret;
        } else if (OB_SUCCESS != (tmp_ret = trans_desc.merge_participants_pla())) {
          ret = tmp_ret;
        } else {
          // do nothing
        }
      } else {
        // do nothing
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = trans_desc.merge_participants_pla())) {
        ret = tmp_ret;
      }
    }
    sche_ctx->clear_stmt_participants();
    trans_desc.clear_stmt_participants();
  }  // end handle dist-trans stmt-rollback

  return ret;
}

int ObTransService::check_stmt_participants_and_epoch_(
    const ObPartitionEpochArray& epoch_arr, ObPartitionArray& epoch_participants)
{
  int ret = OB_SUCCESS;
  // partition leader epoch must unique, we don't support leader shift during stmt execution
  for (int64_t i = 0; OB_SUCC(ret) && i < epoch_arr.count(); ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < epoch_arr.count(); ++j) {
      if (epoch_arr.at(i).get_partition() == epoch_arr.at(j).get_partition() &&
          epoch_arr.at(i).get_epoch() != epoch_arr.at(j).get_epoch()) {
        TRANS_LOG(WARN, "different leader epoch, maybe partition leader revoke", "epoch_info", epoch_arr.at(i));
        ret = OB_NOT_MASTER;
      }
    }
    if (!is_contain(epoch_participants, epoch_arr.at(i).get_partition()) &&
        OB_FAIL(epoch_participants.push_back(epoch_arr.at(i).get_partition()))) {
      TRANS_LOG(WARN, "push epoch partition error", KR(ret), K(epoch_arr));
    }
  }

  return ret;
}

OB_INLINE int ObTransService::handle_sp_bounded_staleness_trans_(const ObTransDesc& trans_desc,
    const ObPartitionArray& participants, storage::ObIPartitionArrayGuard& pkey_guard_arr)
{
  UNUSED(pkey_guard_arr);
  int ret = OB_SUCCESS;
  UNUSED(pkey_guard_arr);

  const ObPartitionKey& partition = participants.at(0);
  const ObTransID& trans_id = trans_desc.get_trans_id();
  const bool is_bounded_staleness_read = trans_desc.is_bounded_staleness_read();
  const bool is_readonly = trans_desc.is_readonly();
  const bool for_replay = false;
  const bool need_completed_dirty_txn = false;
  bool alloc = true;
  ObTransCtx* ctx = NULL;
  ObSlaveTransCtx* slave_ctx = NULL;
  // read only stmt: autocommit=1, single partition
  if (NULL != (ctx = const_cast<ObTransDesc&>(trans_desc).get_part_ctx())) {
    slave_ctx = static_cast<ObSlaveTransCtx*>(ctx);
  } else if (OB_FAIL(slave_part_trans_ctx_mgr_.get_trans_ctx(partition,
                 trans_id,
                 for_replay,
                 is_readonly,
                 is_bounded_staleness_read,
                 need_completed_dirty_txn,
                 alloc,
                 ctx))) {
    TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_desc));
  } else {
    slave_ctx = static_cast<ObSlaveTransCtx*>(ctx);
    if (alloc) {
      slave_ctx->set_participants(participants);
      slave_ctx->set_readonly(true);
      slave_ctx->set_trans_type(TransType::MINI_SP_TRANS);
      slave_ctx->set_session_id(trans_desc.get_session_id());
      slave_ctx->set_proxy_session_id(trans_desc.get_proxy_session_id());
      if (OB_FAIL(slave_ctx->init(trans_desc.get_tenant_id(),
              trans_id,
              trans_desc.get_trans_expired_time(),
              partition,
              &slave_part_trans_ctx_mgr_,
              trans_desc.get_trans_param(),
              trans_desc.get_cluster_version(),
              this))) {
        TRANS_LOG(WARN, "init transaction context error", KR(ret), K(partition), K(trans_desc));
        (void)slave_part_trans_ctx_mgr_.revert_trans_ctx(slave_ctx);
      } else if (OB_FAIL(slave_ctx->start_trans())) {
        TRANS_LOG(WARN, "start participant transaction error", KR(ret), K(partition), K(trans_desc));
      } else {
        const_cast<ObTransDesc&>(trans_desc).set_part_ctx(slave_ctx);
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;
    if (ObTransSnapshotGeneType::CONSULT == trans_desc.get_snapshot_gene_type()) {
      // do nothing
    } else if (ObTransSnapshotGeneType::APPOINT == trans_desc.get_snapshot_gene_type()) {
      if (trans_desc.get_snapshot_version() < 0) {
        TRANS_LOG(ERROR, "unexpected safe weak read snapshot version", K(trans_desc));
        ret = OB_ERR_UNEXPECTED;
      } else {
        snapshot_version = trans_desc.get_snapshot_version();
      }
    } else {
      TRANS_LOG(ERROR, "invalid snapshot gene type, unexpected error", K(trans_desc));
      ret = OB_ERR_UNEXPECTED;
    }

    if (OB_SUCC(ret) &&
        OB_FAIL(slave_ctx->start_task(trans_desc, snapshot_version, trans_desc.get_snapshot_gene_type(), true))) {
      TRANS_LOG(DEBUG, "start participant task error", KR(ret), K(trans_desc), K(partition));
    }
  }

  return ret;
}

OB_INLINE int ObTransService::handle_sp_trans_(const ObTransDesc& trans_desc, const ObPartitionArray& participants,
    const int64_t leader_epoch, storage::ObIPartitionArrayGuard& pkey_guard_arr)
{
  int ret = OB_SUCCESS;

  ObPartitionKey* sp_partition = NULL;
  int64_t pkey_guard_arr_index = 0;
  const ObTransID& trans_id = trans_desc.get_trans_id();
  const bool is_bounded_staleness_read = trans_desc.is_bounded_staleness_read();
  const bool is_readonly = trans_desc.is_readonly();
  const bool for_replay = false;
  const bool need_completed_dirty_txn = false;
  bool alloc = false;
  ObTransCtx* ctx = NULL;
  ObPartTransCtx* part_ctx = NULL;
  const ObStmtDesc& stmt_desc = trans_desc.get_cur_stmt_desc();
  int64_t user_specified_snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;
  if (OB_FAIL(trans_desc.find_first_not_ro_participant(participants, sp_partition, pkey_guard_arr_index))) {
    TRANS_LOG(ERROR, "find first not read only participant error", KR(ret), K(trans_desc));
  } else if (is_readonly) {
    alloc = true;
  } else {
    alloc = is_bounded_staleness_read ? !trans_desc.has_create_ctx(*sp_partition, self_)
                                      : !trans_desc.has_create_ctx(*sp_partition);
  }

  if (OB_SUCC(ret)) {
    const ObPartitionKey& partition = *sp_partition;
    ObIPartitionGroup* pg = pkey_guard_arr.at(pkey_guard_arr_index);

    // autocommit=1, single partition and select stmt
    if (trans_desc.is_mini_sp_trans()) {
      if (NULL != (ctx = const_cast<ObTransDesc&>(trans_desc).get_part_ctx())) {
        part_ctx = static_cast<ObPartTransCtx*>(ctx);
      } else if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
                     trans_id,
                     for_replay,
                     is_readonly,
                     is_bounded_staleness_read,
                     need_completed_dirty_txn,
                     alloc,
                     ctx))) {
        TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_desc));
      } else {
        part_ctx = static_cast<ObPartTransCtx*>(ctx);
        if (alloc) {
          part_ctx->set_participant(partition);
          part_ctx->set_readonly(true);
          part_ctx->set_trans_type(TransType::MINI_SP_TRANS);
          part_ctx->set_session_id(trans_desc.get_session_id());
          part_ctx->set_proxy_session_id(trans_desc.get_proxy_session_id());
          if (OB_FAIL(part_ctx->init(trans_desc.get_tenant_id(),
                  trans_id,
                  trans_desc.get_trans_expired_time(),
                  partition,
                  &part_trans_ctx_mgr_,
                  trans_desc.get_trans_param(),
                  trans_desc.get_cluster_version(),
                  this,
                  trans_desc.get_cluster_id(),
                  leader_epoch,
                  trans_desc.is_can_elr()))) {
            TRANS_LOG(WARN, "init transaction context error", KR(ret), K(partition), K(trans_desc));
            (void)part_trans_ctx_mgr_.revert_trans_ctx(part_ctx);
          } else if (OB_FAIL(part_ctx->start_trans())) {
            TRANS_LOG(WARN, "start participant transaction error", KR(ret), K(partition), K(trans_desc));
          } else {
            (void)part_ctx->set_trans_app_trace_id_str(trans_desc.get_trans_app_trace_id_str());
            const_cast<ObTransDesc&>(trans_desc).set_part_ctx(part_ctx);
          }
        }
      }
      if (OB_SUCC(ret) && NULL != part_ctx) {
        int64_t snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;
        (void)part_ctx->set_cur_query_start_time(stmt_desc.cur_query_start_time_);
        part_ctx->set_local_trans(trans_desc.is_local_trans());

        ret = decide_participant_snapshot_version_(
            trans_desc, partition, pg, user_specified_snapshot_version, snapshot_version);

        if (OB_SUCC(ret) && OB_FAIL(part_ctx->start_task(trans_desc, snapshot_version, true, pg))) {
          TRANS_LOG(DEBUG, "start participant task error", KR(ret), K(trans_desc), K(partition));
        }
      }
    } else {
      // single partition, multi stmt transaction
      if (NULL != (ctx = const_cast<ObTransDesc&>(trans_desc).get_part_ctx())) {
        part_ctx = static_cast<ObPartTransCtx*>(ctx);
      } else if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
                     trans_id,
                     for_replay,
                     is_readonly,
                     is_bounded_staleness_read,
                     need_completed_dirty_txn,
                     alloc,
                     ctx))) {
        TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_desc));
      } else {
        part_ctx = static_cast<ObPartTransCtx*>(ctx);
        if (alloc) {
          part_ctx->set_trans_type(TransType::SP_TRANS);
          part_ctx->set_session_id(trans_desc.get_session_id());
          part_ctx->set_proxy_session_id(trans_desc.get_proxy_session_id());
          part_ctx->set_participant(partition);
          if (OB_FAIL(part_ctx->init(trans_desc.get_tenant_id(),
                  trans_id,
                  trans_desc.get_trans_expired_time(),
                  partition,
                  &part_trans_ctx_mgr_,
                  trans_desc.get_trans_param(),
                  trans_desc.get_cluster_version(),
                  this,
                  trans_desc.get_cluster_id(),
                  leader_epoch,
                  trans_desc.is_can_elr()))) {
            TRANS_LOG(WARN, "init transaction context error", KR(ret), K(partition), K(trans_desc));
            (void)part_trans_ctx_mgr_.revert_trans_ctx(part_ctx);
          } else {
            (void)part_ctx->set_trans_app_trace_id_str(trans_desc.get_trans_app_trace_id_str());
            (void)const_cast<ObTransDesc&>(trans_desc).set_part_ctx(part_ctx);
            if (OB_FAIL(part_ctx->start_trans())) {
              TRANS_LOG(WARN, "start participant transaction error", K(trans_desc));
            }
          }
        } else {
          // the ctx is created by previous stmt which already rollbacked
          (void)const_cast<ObTransDesc&>(trans_desc).set_part_ctx(part_ctx);
        }
      }
      if (OB_SUCC(ret) && NULL != part_ctx) {
        int64_t snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;
        (void)part_ctx->set_cur_query_start_time(stmt_desc.cur_query_start_time_);
        part_ctx->set_local_trans(trans_desc.is_local_trans());

        ret = decide_participant_snapshot_version_(
            trans_desc, partition, pg, user_specified_snapshot_version, snapshot_version);

        if (OB_SUCC(ret) && OB_FAIL(part_ctx->start_task(trans_desc, snapshot_version, true, pg))) {
          TRANS_LOG(WARN, "start participant task error", KR(ret), K(trans_desc), K(partition));
        }
      }
    }
  }  // OB_SUCC(ret)

  return ret;
}

OB_INLINE int ObTransService::handle_start_participant_for_bounded_staleness_read_(const ObTransDesc& trans_desc,
    const ObPartitionArray& participants, storage::ObIPartitionArrayGuard& pkey_guard_arr)
{
  int ret = OB_SUCCESS;
  const ObTransID& trans_id = trans_desc.get_trans_id();
  const bool for_replay = false;
  const bool is_readonly = trans_desc.is_readonly();
  const bool is_bounded_staleness_read = trans_desc.is_bounded_staleness_read();
  const bool need_completed_dirty_txn = false;

  if (trans_desc.is_mini_sp_trans()) {
    if (OB_FAIL(handle_sp_bounded_staleness_trans_(trans_desc, participants, pkey_guard_arr))) {
      TRANS_LOG(DEBUG, "handle sp transaction error", KR(ret), K(trans_desc), K(participants));
    }
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < participants.count(); ++index) {
      const ObPartitionKey& partition = participants.at(index);
      ObTransCtx* ctx = NULL;
      bool alloc = true;
      if (OB_FAIL(slave_part_trans_ctx_mgr_.get_trans_ctx(partition,
              trans_id,
              for_replay,
              is_readonly,
              is_bounded_staleness_read,
              need_completed_dirty_txn,
              alloc,
              ctx))) {
        TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_desc));
      } else {
        ObSlaveTransCtx* slave_ctx = static_cast<ObSlaveTransCtx*>(ctx);
        if (alloc) {
          if (trans_desc.is_sp_trans()) {
            slave_ctx->set_trans_type(TransType::SP_TRANS);
          }
          if (OB_FAIL(slave_ctx->init(trans_desc.get_tenant_id(),
                  trans_id,
                  trans_desc.get_trans_expired_time(),
                  partition,
                  &slave_part_trans_ctx_mgr_,
                  trans_desc.get_trans_param(),
                  trans_desc.get_cluster_version(),
                  this))) {
            TRANS_LOG(WARN, "init transaction context error", KR(ret), K(partition), K(trans_desc));
          } else {
            slave_ctx->set_session_id(trans_desc.get_session_id());
            slave_ctx->set_proxy_session_id(trans_desc.get_proxy_session_id());
            slave_ctx->set_scheduler(trans_id.get_addr());
            slave_ctx->set_coordinator(partition);
            slave_ctx->set_participants(participants);
            if (OB_FAIL(slave_ctx->start_trans())) {
              TRANS_LOG(WARN, "start participant transaction error", KR(ret), K(partition), K(trans_desc));
            }
          }
        }
        if (OB_SUCC(ret)) {
          const int64_t snapshot_version = trans_desc.get_snapshot_version();
          if (OB_UNLIKELY(snapshot_version < 0)) {
            TRANS_LOG(ERROR, "unexpected safe weak read snapshot version", K(trans_desc));
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(slave_ctx->start_task(
                         trans_desc, snapshot_version, trans_desc.get_snapshot_gene_type(), 0 == index))) {
            TRANS_LOG(DEBUG, "start participant task error", KR(ret), K(trans_desc), K(partition));
          } else {
            // do nothing
          }
        }
        (void)slave_part_trans_ctx_mgr_.revert_trans_ctx(ctx);
      }
    }  // for
  }
  return ret;
}

int ObTransService::set_replay_checkpoint_(const ObPartitionKey& pkey, const int64_t checkpoint)
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard pkey_guard;
  if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey), K(checkpoint));
  } else if (OB_FAIL(partition_service_->get_partition(pkey, pkey_guard))) {
    TRANS_LOG(WARN, "get partition failed", KR(ret));
  } else if (OB_ISNULL(pkey_guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "partition is null, unexpected error", KR(ret), KP(pkey_guard.get_partition_group()));
  } else if (OB_FAIL(pkey_guard.get_partition_group()->set_replay_checkpoint(checkpoint))) {
    TRANS_LOG(WARN, "set replay checkpoint failed", KR(ret));
  } else {
    // do nothing
  }
  if (OB_SUCCESS != ret) {
    TRANS_LOG(ERROR, "set replay checkpoint failed", KR(ret), K(pkey), K(checkpoint));
    // rewrite ret
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObTransService::get_partition_group_(
    const ObPartitionKey& pkey, ObIPartitionGroupGuard& guard, ObIPartitionGroup*& pg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "invalid partition service", K(partition_service_));
  } else if (OB_FAIL(partition_service_->get_partition(pkey, guard))) {
    TRANS_LOG(WARN, "get partition group guard failed", K(pkey), KR(ret));
  } else if (OB_ISNULL(pg = guard.get_partition_group())) {
    TRANS_LOG(WARN, "partition group not exist", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
  }
  return ret;
}

// decide snapshot for participant
//
// 1. use user speicified
// 2. generate snapshot if trans_desc.snapshot_gen_type is NOTHING
// 3. snapshot must be already decided at stmt start
int ObTransService::decide_participant_snapshot_version_(const ObTransDesc& trans_desc,
    const common::ObPartitionKey& pkey, storage::ObIPartitionGroup* pg, const int64_t user_specified_snapshot,
    int64_t& part_snapshot_version, const char* module /* = "start_participant"*/)
{
  int ret = OB_SUCCESS;
  int32_t snapshot_gene_type = trans_desc.get_snapshot_gene_type();
  ObIPartitionGroupGuard guard;

  if (user_specified_snapshot > 0) {
    TRANS_LOG(INFO,
        "user speicified snapshot version is valid to be participant snapshot",
        K(user_specified_snapshot),
        K(trans_desc),
        K(pkey));
    part_snapshot_version = user_specified_snapshot;
  } else {
    if (trans_desc.is_fast_select()) {
      part_snapshot_version = trans_desc.get_stmt_snapshot_info().get_snapshot_version();
    } else {
      part_snapshot_version = trans_desc.get_snapshot_version();
    }

    if (ObTransSnapshotGeneType::APPOINT == snapshot_gene_type ||
        ObTransSnapshotGeneType::CONSULT == snapshot_gene_type) {
      if (OB_UNLIKELY(part_snapshot_version <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR,
            "snapshot version should be valid",
            KR(ret),
            K(part_snapshot_version),
            K(snapshot_gene_type),
            K(trans_desc));
      }
    } else if (OB_UNLIKELY(ObTransSnapshotGeneType::NOTHING != snapshot_gene_type)) {
      ret = OB_NOT_SUPPORTED;
      TRANS_LOG(ERROR, "unknown snapshot generate type", KR(ret), K(snapshot_gene_type), K(trans_desc));
    } else {
      if (OB_UNLIKELY(ObTransVersion::INVALID_TRANS_VERSION != part_snapshot_version)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR,
            "snapshot version should be invalid under NOTHING snapshot_gene_type",
            KR(ret),
            K(part_snapshot_version),
            K(snapshot_gene_type),
            K(trans_desc));
      } else if (NULL == pg && OB_FAIL(get_partition_group_(pkey, guard, pg))) {
        TRANS_LOG(WARN, "get partition group fail", KR(ret), K(pkey));
      } else if (trans_desc.is_bounded_staleness_read()) {
        // partitions return to SQL-Layer, which help SQL-Engine retry other servers
        ObPartitionArray unreachable_partitions;
        ret = generate_part_snapshot_for_bounded_staleness_read_(
            pkey, *pg, part_snapshot_version, unreachable_partitions);
      } else if (trans_desc.is_current_read()) {
        ret = generate_part_snapshot_for_current_read_(trans_desc.is_can_elr(),
            trans_desc.is_readonly(),
            trans_desc.get_cur_stmt_expired_time(),
            trans_desc.get_trans_expired_time(),
            trans_desc.is_not_create_ctx_participant(pkey),
            pkey,
            *pg,
            part_snapshot_version);
      } else {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(ERROR, "unknown consistency type", KR(ret), K(trans_desc));
      }
    }

    if (OB_SUCCESS == ret) {
      if (trans_desc.is_bounded_staleness_read()) {
        // must check partition is readable for decided snapshot
        if (NULL == pg && OB_FAIL(get_partition_group_(pkey, guard, pg))) {
          TRANS_LOG(WARN, "get partition group fail", KR(ret), K(pkey));
        } else if (OB_FAIL(pg->check_replica_ready_for_bounded_staleness_read(part_snapshot_version))) {
          TRANS_LOG(WARN,
              "replica is not readable",
              KR(ret),
              K(part_snapshot_version),
              K(user_specified_snapshot),
              K(trans_desc));
        } else {
          // do nothing
        }
      }
    }
  }

  TRANS_LOG(DEBUG,
      "decide participant snapshot",
      KR(ret),
      K(module),
      K(part_snapshot_version),
      K(user_specified_snapshot),
      K(trans_desc),
      K(pkey));
  return ret;
}

int ObTransService::generate_part_snapshot_for_bounded_staleness_read_(const common::ObPartitionKey& pkey,
    storage::ObIPartitionGroup& ob_partition, int64_t& part_snapshot_version, ObPartitionArray& unreachable_partitions)
{
  int ret = OB_SUCCESS;
  const int64_t snapshot_version_barrier =
      ObTimeUtility::current_time() - ObWeakReadUtil::max_stale_time_for_weak_consistency(pkey.get_tenant_id());
  int64_t snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;

  if (OB_FAIL(ob_partition.get_weak_read_timestamp(snapshot_version))) {
    TRANS_LOG(WARN, "get weak read timestamp error", KR(ret), K(snapshot_version), K(pkey));
    if (OB_STATE_NOT_MATCH == ret) {
      // SQL-Layer will retry
      ret = OB_REPLICA_NOT_READABLE;
      TRANS_LOG(WARN, "replica not readable, change ret to OB_REPLICA_NOT_READABLE", KR(ret));
    }
  } else {
    if (snapshot_version < snapshot_version_barrier) {
      TRANS_LOG(WARN,
          "snapshot version is too old to match max_stale_time_for_weak_consistency",
          K(snapshot_version),
          "snapshot_barrier",
          snapshot_version_barrier,
          "delta",
          (snapshot_version_barrier - snapshot_version),
          K(pkey));
      ret = OB_REPLICA_NOT_READABLE;
      ObTransStatistic::get_instance().add_slave_read_stmt_retry_count(pkey.get_tenant_id(), 1);
    } else {
      part_snapshot_version = snapshot_version;
    }
  }

  if (OB_REPLICA_NOT_READABLE == ret) {
    (void)unreachable_partitions.push_back(pkey);
  }
  return ret;
}

int ObTransService::generate_part_snapshot_for_bounded_staleness_read_(
    const common::ObPartitionKey& pkey, int64_t& part_snapshot_version, ObPartitionArray& unreachable_partitions)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* pg = NULL;
  if (OB_FAIL(get_partition_group_(pkey, guard, pg))) {
    TRANS_LOG(WARN, "get partition group fail", KR(ret), K(pkey), K(pg), K(self_));
  } else if (OB_ISNULL(pg)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid partition group", KR(ret), K(pkey), K(pg), K(self_));
  } else {
    ret = generate_part_snapshot_for_bounded_staleness_read_(pkey, *pg, part_snapshot_version, unreachable_partitions);
  }
  return ret;
}

int ObTransService::generate_part_snapshot_for_current_read_(const bool can_elr, const bool is_readonly,
    const int64_t stmt_expired, const int64_t trans_expired, const bool is_not_create_ctx_participant,
    const common::ObPartitionKey& pkey, storage::ObIPartitionGroup& ob_partition, int64_t& part_snapshot_version)
{
  int ret = OB_SUCCESS;
  const bool check_election = true;
  int clog_status = OB_SUCCESS;
  int64_t before_leader_epoch = 0;
  int64_t after_leader_epoch = 0;
  ObTsWindows changing_leader_windows;
  int64_t snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;
  MonotonicTs tmp_receive_gts_ts;
  const bool is_external_consistent = ts_mgr_->is_external_consistent(pkey.get_tenant_id());

  if (OB_ISNULL(ts_mgr_) || OB_ISNULL(clog_adapter_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(ERROR, "invalid ts_mgr or clog adaptor", KR(ret), K(ts_mgr_), K(clog_adapter_));
  } else if (OB_FAIL(clog_adapter_->get_status(
                 &ob_partition, check_election, clog_status, before_leader_epoch, changing_leader_windows))) {
    TRANS_LOG(WARN, "clog adapter get status error", K(ret), K(pkey));
  } else {
    // 1. for LTS, use publish_version
    // 2. for read-only Tx, use publish_version, a correct and smaller snapshot is better
    // 3. if ELR open, and not previous case, snapshot need acquired from local_trans_version
    if (can_elr && !is_readonly && is_external_consistent) {
      if (OB_FAIL(get_gts_(snapshot_version, tmp_receive_gts_ts, stmt_expired, trans_expired, pkey.get_tenant_id()))) {
        TRANS_LOG(WARN, "get publish version error", K(ret), K(pkey));
      }
    } else {
      if (OB_FAIL(ts_mgr_->get_publish_version(pkey.get_tenant_id(), snapshot_version))) {
        TRANS_LOG(WARN, "get publish version error", K(ret), K(pkey));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(clog_adapter_->get_status(
                          &ob_partition, check_election, clog_status, after_leader_epoch, changing_leader_windows))) {
    TRANS_LOG(WARN, "clog adapter get status error", KR(ret), K(pkey));
  }

  if (OB_SUCCESS == ret) {
    const int64_t cur_ts = ObTimeUtility::current_time();

    if (OB_NOT_MASTER == clog_status) {
      ret = clog_status;
      if (is_not_create_ctx_participant) {
        int tmp_ret = OB_SUCCESS;
        int duplicate_partition_status = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = check_duplicate_partition_status_(pkey, duplicate_partition_status))) {
          TRANS_LOG(WARN, "check duplicate partition status failed", K(tmp_ret), K(pkey));
        } else if (OB_SUCCESS == duplicate_partition_status) {
          ret = duplicate_partition_status;
        } else {
          // do nothing
        }
      }
    }
    // double check partition is leader
    else if (before_leader_epoch != after_leader_epoch) {
      TRANS_LOG(
          WARN, "leader epoch not identical when double check", K(before_leader_epoch), K(after_leader_epoch), K(pkey));
      ret = OB_NOT_MASTER;
    } else if (GCONF.enable_smooth_leader_switch &&
               cur_ts > changing_leader_windows.get_start() + changing_leader_windows.get_left_size() / 3 &&
               cur_ts < changing_leader_windows.get_end()) {
      ret = OB_NOT_MASTER;
    } else {
      // do nothing
    }

    if (OB_SUCCESS == ret) {
      part_snapshot_version = snapshot_version;
    }
  }
  return ret;
}

int ObTransService::generate_part_snapshot_for_current_read_(const bool can_elr, const bool is_readonly,
    const int64_t stmt_expired, const int64_t trans_expired, const bool is_not_create_ctx_participant,
    const common::ObPartitionKey& pkey, int64_t& part_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* pg = NULL;
  int64_t tmp_snapshot_version = 0;
  if (OB_FAIL(get_partition_group_(pkey, guard, pg))) {
    TRANS_LOG(WARN, "get partition group fail", KR(ret), K(pkey), K(pg), K(self_));
  } else if (OB_ISNULL(pg)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid partition group", KR(ret), K(pkey), K(pg), K(self_));
  } else if (OB_FAIL(generate_part_snapshot_for_current_read_(can_elr,
                 is_readonly,
                 stmt_expired,
                 trans_expired,
                 is_not_create_ctx_participant,
                 pkey,
                 *pg,
                 tmp_snapshot_version))) {
    TRANS_LOG(WARN, "generate part snapshot for current read fail", KR(ret), K(pkey), K(pg), K(self_));
  } else {
    part_snapshot_version = tmp_snapshot_version;
  }

  return ret;
}

int ObTransService::handle_snapshot_for_read_only_participant_(const ObTransDesc& trans_desc,
    const common::ObPartitionKey& pg_key, const int64_t user_specified_snapshot_version, int64_t& snapshot_version)
{
  int ret = OB_SUCCESS;
  bool is_user_specified_snapshot_valid = true;
  const char* module = "table_scan";
  if (OB_FAIL(check_user_specified_snapshot_version(
          trans_desc, user_specified_snapshot_version, is_user_specified_snapshot_valid))) {
    TRANS_LOG(WARN,
        "failed to check user specified snapshot versions",
        KR(ret),
        K(trans_desc),
        K(user_specified_snapshot_version));
  } else if (OB_FAIL(decide_participant_snapshot_version_(
                 trans_desc, pg_key, NULL, user_specified_snapshot_version, snapshot_version, module))) {
    TRANS_LOG(WARN,
        "decide part snapshot when start participant fail",
        KR(ret),
        K(trans_desc),
        K(pg_key),
        K(user_specified_snapshot_version));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_QUERY_TIMESTAMP;
    TRANS_LOG(WARN, "invalid snapshot version", KR(ret), K(snapshot_version), K(trans_desc), K(pg_key));
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    bool updated = false;
    if (OB_ISNULL(ts_mgr_)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid ts mgr", K(ts_mgr_));
    } else if (OB_FAIL(ts_mgr_->update_local_trans_version(pg_key.get_tenant_id(), snapshot_version, updated))) {
      TRANS_LOG(WARN, "update gts failed", KR(ret), K(pg_key));
    } else {
      // do nothing
    }
  }

  return ret;
}

OB_INLINE int ObTransService::handle_start_participant_(const ObTransDesc& trans_desc,
    const ObPartitionArray& participants, const ObLeaderEpochArray& leader_epoch_arr,
    storage::ObIPartitionArrayGuard& pkey_guard_arr)
{
  int ret = OB_SUCCESS;
  const ObTransID& trans_id = trans_desc.get_trans_id();
  const bool for_replay = false;
  const bool is_readonly = trans_desc.is_readonly();
  const bool is_bounded_staleness_read = trans_desc.is_bounded_staleness_read();
  const bool need_completed_dirty_txn = false;
  int64_t leader_epoch = 0;

  if (trans_desc.is_sp_trans() || trans_desc.is_mini_sp_trans()) {
    if (leader_epoch_arr.count() <= 0) {
      TRANS_LOG(DEBUG, "need not create part ctx to start task", K(trans_desc), K(leader_epoch_arr));
    } else {
      leader_epoch = leader_epoch_arr.at(0);
      if (OB_FAIL(handle_sp_trans_(trans_desc, participants, leader_epoch, pkey_guard_arr))) {
        TRANS_LOG(DEBUG, "handle sp transaction error", KR(ret), K(trans_desc), K(participants), K(leader_epoch_arr));
      }
    }
  } else {
    int64_t epoch_index = 0;
    int64_t index = 0;
    int64_t participant_count = participants.count();

    for (index = 0; OB_SUCC(ret) && index < participant_count; ++index) {
      const ObPartitionKey& partition = participants.at(index);
      ObTransCtx* ctx = NULL;
      bool alloc = !trans_desc.has_create_ctx(partition) || is_readonly;

      if (trans_desc.is_not_create_ctx_participant(partition)) {
        TRANS_LOG(DEBUG, "need not create part ctx to start task", K(trans_desc), K(partition), K(participant_count));
      } else if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
                     trans_id,
                     for_replay,
                     is_readonly,
                     is_bounded_staleness_read,
                     need_completed_dirty_txn,
                     alloc,
                     ctx))) {
        TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_desc));
      } else {
        ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(ctx);
        leader_epoch = leader_epoch_arr.at(epoch_index++);
        if (alloc) {
          if (OB_FAIL(part_ctx->init(trans_desc.get_tenant_id(),
                  trans_id,
                  trans_desc.get_trans_expired_time(),
                  partition,
                  &part_trans_ctx_mgr_,
                  trans_desc.get_trans_param(),
                  trans_desc.get_cluster_version(),
                  this,
                  trans_desc.get_cluster_id(),
                  leader_epoch,
                  trans_desc.is_can_elr()))) {
            TRANS_LOG(WARN, "init transaction context error", KR(ret), K(partition), K(trans_desc));
          } else {
            part_ctx->set_session_id(trans_desc.get_session_id());
            part_ctx->set_proxy_session_id(trans_desc.get_proxy_session_id());
            part_ctx->set_scheduler(trans_id.get_addr());
            (void)part_ctx->set_trans_app_trace_id_str(trans_desc.get_trans_app_trace_id_str());
            if (OB_FAIL(part_ctx->start_trans())) {
              TRANS_LOG(WARN, "start participant transaction error", KR(ret), K(partition), K(trans_desc));
            }
          }
        }
        if (OB_SUCC(ret)) {
          int64_t snapshot_version = trans_desc.get_snapshot_version();
          const ObStmtDesc& stmt_desc = trans_desc.get_cur_stmt_desc();
          int64_t user_specified_snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;
          (void)part_ctx->set_cur_query_start_time(stmt_desc.cur_query_start_time_);
          part_ctx->set_local_trans(trans_desc.is_local_trans());
          snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;
          ret = decide_participant_snapshot_version_(
              trans_desc, partition, pkey_guard_arr.at(index), user_specified_snapshot_version, snapshot_version);

          if (OB_SUCC(ret) &&
              OB_FAIL(part_ctx->start_task(trans_desc, snapshot_version, true, pkey_guard_arr.at(index)))) {
            TRANS_LOG(DEBUG, "start participant task error", K(ret), K(trans_desc), K(partition));
          }
        }
        (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
      }
    }  // for
    if (OB_FAIL(ret)) {
      // need clear tasks started.
      // only rollback the tasks have succussfully started. all the tasks start failed need clear the state itself.
      int64_t tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS !=
          (tmp_ret = handle_end_participant_(
               true /*is_rollback, rollback all what we may have done*/, trans_desc, participants, index))) {
        TRANS_LOG(WARN, "handle participant rollback error", K(tmp_ret), K(trans_desc), K(participants), K(index));
      }
    }
  }
  return ret;
}

int ObTransService::check_partition_status(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  ObTsWindows changing_leader_windows;
  int clog_status = OB_SUCCESS;
  int64_t leader_epoch = 0;

  if (OB_FAIL(clog_adapter_->get_status_unsafe(partition, clog_status, leader_epoch, changing_leader_windows))) {
    TRANS_LOG(WARN, "get participant status error", K(ret), K(partition));
  } else if (OB_SUCCESS != clog_status) {
    ret = clog_status;
  } else {
    // do nothing
  }

  return ret;
}

int ObTransService::get_cached_pg_guard(const ObPartitionKey& partition, storage::ObIPartitionGroupGuard*& pg_guard)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(part_trans_ctx_mgr_.get_cached_pg_guard(partition, pg_guard))) {
    if (OB_PARTITION_NOT_EXIST != ret) {
      TRANS_LOG(WARN, "get cached pg guard error", K(ret), K(partition));
    } else {
      // convert to pg_key and retry
      ObPGKey pg_key;
      if (OB_SUCCESS != (tmp_ret = partition_service_->get_pg_key(partition, pg_key))) {
        ret = tmp_ret;
        TRANS_LOG(TRACE, "get pg key error", K(tmp_ret), K(partition));
      } else if (partition != pg_key) {
        if (OB_FAIL(part_trans_ctx_mgr_.get_cached_pg_guard(pg_key, pg_guard))) {
          if (OB_PARTITION_NOT_EXIST != ret) {
            TRANS_LOG(WARN, "get cached pg guard error", K(ret), K(partition), K(pg_key));
          }
        }
      } else {
        // partition not exist
      }
    }
  }
  return ret;
}

int ObTransService::check_trans_partition_leader_unsafe(const ObPartitionKey& partition, bool& is_leader)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_FAIL(part_trans_ctx_mgr_.check_trans_partition_leader_unsafe(partition, is_leader))) {
    if (OB_PARTITION_NOT_EXIST != ret) {
      TRANS_LOG(WARN, "check trans partition leader unsafe error", K(ret), K(partition), K(is_leader));
    } else {
      // convert to pg_key and retry
      ObPGKey pg_key;
      if (OB_SUCCESS != (tmp_ret = partition_service_->get_pg_key(partition, pg_key))) {
        ret = tmp_ret;
        TRANS_LOG(TRACE, "get pg key error", K(tmp_ret), K(partition));
      } else if (partition != pg_key) {
        if (OB_FAIL(part_trans_ctx_mgr_.check_trans_partition_leader_unsafe(pg_key, is_leader))) {
          if (OB_PARTITION_NOT_EXIST != ret) {
            TRANS_LOG(WARN, "check trans partition leader unsafe error", K(ret), K(partition), K(pg_key), K(is_leader));
          }
        }
      } else {
        // partition not exit
      }
    }
  }
  return ret;
}

int ObTransService::handle_batch_msg(const int type, const ObTrxMsgBase& base_msg)
{
  int ret = OB_SUCCESS;
  const int real_type = type - OB_TRX_NEW_MSG_TYPE_BASE;
  ObTrxMsgBase* base_message = const_cast<ObTrxMsgBase*>(&base_msg);
  switch (real_type) {
    case OB_TRX_2PC_LOG_ID_REQUEST: {
      break;
    }
    case OB_TRX_2PC_LOG_ID_RESPONSE: {
      break;
    }
    case OB_TRX_2PC_PRE_PREPARE_REQUEST: {
      break;
    }
    case OB_TRX_2PC_PRE_PREPARE_RESPONSE: {
      break;
    }
    case OB_TRX_2PC_PREPARE_REQUEST: {
      ObTrx2PCPrepareRequest* msg = static_cast<ObTrx2PCPrepareRequest*>(base_message);
      if (OB_FAIL(msg_handler_.part_ctx_handle_request(*msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc prepare request failed", K(ret), K(*msg));
      }
      break;
    }
    case OB_TRX_2PC_PREPARE_RESPONSE: {
      ObTrx2PCPrepareResponse* msg = static_cast<ObTrx2PCPrepareResponse*>(base_message);
      if (OB_FAIL(msg_handler_.coord_ctx_handle_response(*msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc prepare response failed", K(ret), K(*msg));
      }
      break;
    }
    case OB_TRX_2PC_PRE_COMMIT_REQUEST:
    case OB_TRX_2PC_COMMIT_REQUEST: {
      ObTrx2PCCommitRequest* msg = static_cast<ObTrx2PCCommitRequest*>(base_message);
      if (OB_FAIL(msg_handler_.part_ctx_handle_request(*msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc clear request failed", K(ret), K(*msg));
      }
      break;
    }
    case OB_TRX_2PC_PRE_COMMIT_RESPONSE: {
      ObTrx2PCPreCommitResponse* msg = static_cast<ObTrx2PCPreCommitResponse*>(base_message);
      if (OB_FAIL(msg_handler_.coord_ctx_handle_response(*msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc pre-commit response failed", K(ret), K(*msg));
      }
      break;
    }
    case OB_TRX_2PC_COMMIT_RESPONSE: {
      ObTrx2PCCommitResponse* msg = static_cast<ObTrx2PCCommitResponse*>(base_message);
      if (OB_FAIL(msg_handler_.coord_ctx_handle_response(*msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc commit response failed", K(ret), K(*msg));
      }
      break;
    }
    case OB_TRX_2PC_ABORT_REQUEST: {
      ObTrx2PCAbortRequest* msg = static_cast<ObTrx2PCAbortRequest*>(base_message);
      if (OB_FAIL(msg_handler_.part_ctx_handle_request(*msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc abort request failed", K(ret), K(*msg));
      } else {
        // do nothing
      }
      break;
    }
    case OB_TRX_2PC_ABORT_RESPONSE: {
      ObTrx2PCAbortResponse* msg = static_cast<ObTrx2PCAbortResponse*>(base_message);
      if (OB_FAIL(msg_handler_.coord_ctx_handle_response(*msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc abort response failed", K(ret), K(*msg));
      }
      break;
    }
    case OB_TRX_2PC_CLEAR_REQUEST: {
      ObTrx2PCClearRequest* msg = static_cast<ObTrx2PCClearRequest*>(base_message);
      if (OB_FAIL(msg_handler_.part_ctx_handle_request(*msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc clear request failed", K(ret), K(*msg));
      }
      break;
    }
    case OB_TRX_2PC_CLEAR_RESPONSE: {
      ObTrx2PCClearResponse* msg = static_cast<ObTrx2PCClearResponse*>(base_message);
      if (OB_FAIL(msg_handler_.coord_ctx_handle_response(*msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc clear response failed", K(ret), K(*msg));
      }
      break;
    }
    case OB_TRX_2PC_COMMIT_CLEAR_REQUEST: {
      break;
    }
    case OB_TRX_2PC_COMMIT_CLEAR_RESPONSE: {
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      break;
    }
  }

  return ret;
}

int ObTransService::handle_batch_msg_(const int type, const char* buf, const int32_t size)
{
  UNUSED(buf);
  UNUSED(size);
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const int real_type = type - OB_TRX_NEW_MSG_TYPE_BASE;
  switch (real_type) {
    case OB_TRX_2PC_LOG_ID_REQUEST: {
      break;
    }
    case OB_TRX_2PC_LOG_ID_RESPONSE: {
      break;
    }
    case OB_TRX_2PC_PRE_PREPARE_REQUEST: {
      break;
    }
    case OB_TRX_2PC_PRE_PREPARE_RESPONSE: {
      break;
    }
    case OB_TRX_2PC_PREPARE_REQUEST: {
      ObTrx2PCPrepareRequest msg;
      if (OB_FAIL(msg.deserialize(buf, size, pos))) {
        TRANS_LOG(WARN, "deserialize msg failed", K(ret), K(real_type));
      } else if (OB_FAIL(msg_handler_.part_ctx_handle_request(msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc prepare request failed", K(ret), K(msg));
      } else {
        // do nothing
      }
      break;
    }
    case OB_TRX_2PC_PREPARE_RESPONSE: {
      ObTrx2PCPrepareResponse msg;
      if (OB_FAIL(msg.deserialize(buf, size, pos))) {
        TRANS_LOG(WARN, "deserialize msg failed", K(ret), K(real_type));
      } else if (OB_FAIL(msg_handler_.coord_ctx_handle_response(msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc prepare response failed", K(ret), K(msg));
      } else {
        // do nothing
      }
      break;
    }
    case OB_TRX_2PC_PRE_COMMIT_REQUEST:
    case OB_TRX_2PC_COMMIT_REQUEST: {
      ObTrx2PCCommitRequest msg;
      if (OB_FAIL(msg.deserialize(buf, size, pos))) {
        TRANS_LOG(WARN, "deserialize msg failed", K(ret), K(real_type));
      } else if (OB_FAIL(msg_handler_.part_ctx_handle_request(msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc clear request failed", K(ret), K(msg));
      } else {
        // do nothing
      }
      break;
    }
    case OB_TRX_2PC_PRE_COMMIT_RESPONSE: {
      ObTrx2PCPreCommitResponse msg;
      if (OB_FAIL(msg.deserialize(buf, size, pos))) {
        TRANS_LOG(WARN, "deserialize msg failed", K(ret), K(real_type));
      } else if (OB_FAIL(msg_handler_.coord_ctx_handle_response(msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc pre-commit response failed", K(ret), K(msg));
      } else {
        // do nothing
      }
      break;
    }
    case OB_TRX_2PC_COMMIT_RESPONSE: {
      ObTrx2PCCommitResponse msg;
      if (OB_FAIL(msg.deserialize(buf, size, pos))) {
        TRANS_LOG(WARN, "deserialize msg failed", K(ret), K(real_type));
      } else if (OB_FAIL(msg_handler_.coord_ctx_handle_response(msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc commit response failed", K(ret), K(msg));
      } else {
        // do nothing
      }
      break;
    }
    case OB_TRX_2PC_ABORT_REQUEST: {
      ObTrx2PCAbortRequest msg;
      if (OB_FAIL(msg.deserialize(buf, size, pos))) {
        TRANS_LOG(WARN, "deserialize msg failed", K(ret), K(real_type));
      } else if (OB_FAIL(msg_handler_.part_ctx_handle_request(msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc clear request failed", K(ret), K(msg));
      } else {
        // do nothing
      }
      break;
    }
    case OB_TRX_2PC_ABORT_RESPONSE: {
      ObTrx2PCAbortResponse msg;
      if (OB_FAIL(msg.deserialize(buf, size, pos))) {
        TRANS_LOG(WARN, "deserialize msg failed", K(ret), K(real_type));
      } else if (OB_FAIL(msg_handler_.coord_ctx_handle_response(msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc abort response failed", K(ret), K(msg));
      } else {
        // do nothing
      }
      break;
    }
    case OB_TRX_2PC_CLEAR_REQUEST: {
      ObTrx2PCClearRequest msg;
      if (OB_FAIL(msg.deserialize(buf, size, pos))) {
        TRANS_LOG(WARN, "deserialize msg failed", K(ret), K(real_type));
      } else if (OB_FAIL(msg_handler_.part_ctx_handle_request(msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc clear request failed", K(ret), K(msg));
      } else {
        // do nothing
      }
      break;
    }
    case OB_TRX_2PC_CLEAR_RESPONSE: {
      ObTrx2PCClearResponse msg;
      if (OB_FAIL(msg.deserialize(buf, size, pos))) {
        TRANS_LOG(WARN, "deserialize msg failed", K(ret), K(real_type));
      } else if (OB_FAIL(msg_handler_.coord_ctx_handle_response(msg, real_type))) {
        TRANS_LOG(WARN, "handle 2pc clear response failed", K(ret), K(msg));
      } else {
        // do nothing
      }
      break;
    }
    case OB_TRX_2PC_COMMIT_CLEAR_REQUEST: {
      break;
    }
    case OB_TRX_2PC_COMMIT_CLEAR_RESPONSE: {
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      break;
    }
  }
  return ret;
}

int ObTransService::start_participant(const ObTransDesc& trans_desc, const ObPartitionArray& participants,
    ObPartitionEpochArray& partition_epoch_arr, storage::ObIPartitionArrayGuard& pkey_guard_arr)
{
  int ret = OB_SUCCESS;
  ObTransTraceLog& tlog = (const_cast<ObTransDesc&>(trans_desc)).get_tlog();
  const ObTransID& trans_id = trans_desc.get_trans_id();
  const bool is_readonly = trans_desc.is_readonly();
  const bool is_bounded_staleness_read = trans_desc.is_bounded_staleness_read();
  ObLeaderEpochArray leader_epoch_arr;
  (const_cast<ObTransDesc&>(trans_desc)).reset_start_participant_ts();
  int64_t start_participant_ts = ObTimeUtility::current_time();
#ifdef ERRSIM
  ret = E(EventTable::EN_START_PARTICIPANT_INTERFACE_ERROR) OB_SUCCESS;
  if (OB_ERR_UNEXPECTED == ret) {
    return ret;
  }
#endif
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!trans_desc.is_valid()) || OB_UNLIKELY(0 == participants.count()) ||
             OB_UNLIKELY(participants.count() != pkey_guard_arr.count())) {
    TRANS_LOG(WARN, "invalid argument", K(trans_desc), K(participants));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_readonly && !trans_desc.get_cur_stmt_desc().is_readonly_stmt()) {
    TRANS_LOG(WARN, "can not execute dml in readonly transaction", K(trans_desc));
    ret = OB_ERR_READ_ONLY_TRANSACTION;
  } else if (OB_UNLIKELY(multi_tenant_uncertain_phy_plan_(trans_desc.get_cur_stmt_desc(), participants))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "multi tenant uncertain phy plan unsupport", KR(ret), K(trans_desc), K(participants));
    // return transaction timeout, if both of stmt and transaction is timeout
  } else if (OB_UNLIKELY(trans_desc.is_trans_timeout())) {
    TRANS_LOG(WARN, "transaction is timeout", K(trans_desc));
    ret = OB_TRANS_TIMEOUT;
  } else if (OB_UNLIKELY(trans_desc.is_stmt_timeout())) {
    TRANS_LOG(WARN, "statement is timeout", K(trans_desc));
    ret = OB_TRANS_STMT_TIMEOUT;
  } else if (OB_UNLIKELY(trans_desc.need_rollback())) {
    TRANS_LOG(WARN, "transaction need rollback", K(trans_desc));
    ret = OB_TRANS_NEED_ROLLBACK;
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    if (is_bounded_staleness_read) {
      for (int64_t i = 0; OB_SUCC(ret) && i < participants.count(); ++i) {
        const ObPartitionKey& pkey = participants.at(i);
        if (OB_UNLIKELY(!trans_desc.is_not_create_ctx_participant(pkey))) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "bounded staleness read should not create ctx", KR(ret), K(pkey));
        }
      }
    } else {
      const bool check_election = true;
      int status = OB_SUCCESS;
      ObTsWindows changing_leader_windows;
      int64_t leader_epoch = 0;
      ObPartitionEpochInfo epoch_info;
      for (int64_t i = 0; OB_SUCC(ret) && i < participants.count(); ++i) {
        const ObPartitionKey& pkey = participants.at(i);
        if (OB_ISNULL(pkey_guard_arr.at(i)) || OB_UNLIKELY(pkey != pkey_guard_arr.at(i)->get_partition_key())) {
          TRANS_LOG(ERROR, "unexpected partition guard arr", K(i), K(pkey), K(trans_id));
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(clog_adapter_->get_status(
                       pkey_guard_arr.at(i), check_election, status, leader_epoch, changing_leader_windows))) {
          TRANS_LOG(WARN, "get participant status error", KR(ret), K(pkey), K(trans_id));
        } else if (OB_SUCCESS == status) {
          if (!trans_desc.has_create_ctx(pkey) && !can_create_ctx_(trans_id.get_timestamp(), changing_leader_windows)) {
            ret = OB_NOT_MASTER;
          }
        } else {
          ret = status;
        }
        if (OB_SUCC(ret)) {
          if (trans_desc.is_not_create_ctx_participant(pkey)) {
            // read-only participant
          } else if (OB_FAIL(epoch_info.generate(pkey, leader_epoch))) {
            TRANS_LOG(WARN, "generate epoch info error", KR(ret), K(pkey), K(leader_epoch), K(trans_id));
          } else if (OB_FAIL(partition_epoch_arr.push_back(epoch_info))) {
            TRANS_LOG(WARN, "partition epoch push back error", KR(ret), K(pkey), K(leader_epoch), K(trans_id));
          } else if (OB_FAIL(leader_epoch_arr.push_back(leader_epoch))) {
            TRANS_LOG(WARN, "leader epoch push back error", KR(ret), K(pkey), K(trans_id));
          } else {
            // do nothing
          }
        } else if (OB_NOT_MASTER == ret) {
          if (trans_desc.is_not_create_ctx_participant(pkey)) {
            int tmp_ret = OB_SUCCESS;
            int duplicate_partition_status = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = check_duplicate_partition_status_(pkey, duplicate_partition_status))) {
              TRANS_LOG(WARN, "check duplicate partition status failed", K(tmp_ret), K(pkey));
            } else if (OB_SUCCESS == duplicate_partition_status) {
              ret = duplicate_partition_status;
            } else {
              // do nothing
            }
          } else {
            // need check access follower replica after write leader replica of duplicated table
            // return error OB_USE_DUP_FOLLOW_AFTER_DML
            int tmp_ret = OB_SUCCESS;
            bool is_duplicated = false;
            if (OB_SUCCESS != (tmp_ret = check_duplicated_partition(pkey, is_duplicated))) {
              TRANS_LOG(WARN, "check duplicate partition failed", K(tmp_ret), K(pkey), K(trans_desc));
            } else if (is_duplicated && is_contain(trans_desc.get_participants(), pkey)) {
              // rewrite ret
              ret = OB_USE_DUP_FOLLOW_AFTER_DML;
              TRANS_LOG(
                  WARN, "verify duplicate partition when start participants fail", KR(ret), K(trans_desc), K(pkey));
            } else {
              // do nothing
            }
          }
        } else {
          // do nothing
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL((const_cast<ObTransDesc&>(trans_desc)).set_start_participant_ts(start_participant_ts))) {
          TRANS_LOG(WARN, "set start participant ts error", K(ret), K(trans_desc), K(start_participant_ts));
        } else if (OB_FAIL(handle_start_participant_(trans_desc, participants, leader_epoch_arr, pkey_guard_arr))) {
          TRANS_LOG(WARN, "handle start participant error", KR(ret), K(trans_desc));
        } else {
          // do nothing
        }
      }
    }
  }
  REC_TRACE_EXT(tlog, start_participant, Y(ret));
  if (OB_FAIL(ret)) {
    (const_cast<ObTransDesc&>(trans_desc)).set_need_print_trace_log();
    TRANS_LOG(WARN, "start participant failed", KR(ret), K(trans_desc), K(participants));
  }
  return ret;
}

int ObTransService::handle_end_bounded_staleness_participant_(
    bool is_rollback, const ObTransDesc& trans_desc, const ObPartitionArray& participants)
{
  int ret = OB_SUCCESS;
  const ObTransID& trans_id = trans_desc.get_trans_id();
  const bool is_readonly = trans_desc.is_readonly();
  const bool for_replay = false;
  const ObPhyPlanType phy_plan_type = trans_desc.get_cur_stmt_desc().phy_plan_type_;
  const bool is_bounded_staleness_read = trans_desc.is_bounded_staleness_read();
  const bool need_completed_dirty_txn = false;
  ObTransCtx* ctx = NULL;
  ObSlaveTransCtx* slave_ctx = NULL;

  if (trans_desc.is_mini_sp_trans()) {
    if (OB_ISNULL(ctx = const_cast<ObTransDesc&>(trans_desc).get_part_ctx())) {
      TRANS_LOG(ERROR, "sp ctx is null, unexpected error", KP(slave_ctx), K(trans_desc));
      ret = OB_ERR_UNEXPECTED;
    } else {
      slave_ctx = static_cast<ObSlaveTransCtx*>(ctx);
      if (OB_FAIL(slave_ctx->end_task(is_rollback, phy_plan_type, trans_desc.get_sql_no()))) {
        TRANS_LOG(WARN, "end participant task error", KR(ret), K(trans_desc));
      }
      if (trans_desc.is_trans_timeout()) {
        ret = OB_TRANS_TIMEOUT;
      } else if (trans_desc.is_stmt_timeout()) {
        ret = OB_TRANS_STMT_TIMEOUT;
      } else {
        // do nothing
      }
      if (slave_ctx->is_exiting()) {
        const_cast<ObTransDesc&>(trans_desc).set_sp_trans_exiting();
        (void)const_cast<ObTransDesc&>(trans_desc).set_part_ctx(NULL);
        // slave_ctx->print_trace_log();
        (void)slave_part_trans_ctx_mgr_.revert_trans_ctx(slave_ctx);
        slave_ctx = NULL;
      }
    }
  } else if (OB_PHY_PLAN_LOCAL == phy_plan_type || OB_PHY_PLAN_REMOTE == phy_plan_type ||
             OB_PHY_PLAN_DISTRIBUTED == phy_plan_type || OB_PHY_PLAN_UNCERTAIN == phy_plan_type) {
    ObPartitionArray* pptr = const_cast<ObPartitionArray*>(&participants);
    for (ObPartitionArray::iterator it = pptr->begin(); it != pptr->end() && OB_SUCCESS == ret; it++) {
      bool alloc = false;
      const ObPartitionKey& partition = *it;
      if (OB_FAIL(slave_part_trans_ctx_mgr_.get_trans_ctx(partition,
              trans_id,
              for_replay,
              is_readonly,
              is_bounded_staleness_read,
              need_completed_dirty_txn,
              alloc,
              ctx))) {
        if (trans_desc.is_trans_timeout()) {
          TRANS_LOG(WARN, "transaction is timeout", K(trans_desc));
          ret = OB_TRANS_TIMEOUT;
        }
        TRANS_LOG(DEBUG, "get transaction context error", KR(ret), K(partition), K(trans_id));
      } else {
        slave_ctx = static_cast<ObSlaveTransCtx*>(ctx);
        if (OB_FAIL(slave_ctx->end_task(is_rollback, phy_plan_type, trans_desc.get_sql_no()))) {
          TRANS_LOG(WARN, "end participant task error", KR(ret), K(partition), K(trans_id));
        }
        (void)slave_part_trans_ctx_mgr_.revert_trans_ctx(ctx);
      }
    }
    if (trans_desc.is_trans_timeout()) {
      ret = OB_TRANS_TIMEOUT;
    } else if (trans_desc.is_stmt_timeout()) {
      ret = OB_TRANS_STMT_TIMEOUT;
    } else {
      // do nothing
    }
  } else {
    TRANS_LOG(ERROR, "invalid phy plan type", K(phy_plan_type));
    ret = OB_ERR_UNEXPECTED;
  }

  return ret;
}

int ObTransService::handle_end_participant_(bool is_rollback, const ObTransDesc& trans_desc,
    const ObPartitionArray& participants, const int64_t participant_cnt)
{
  int ret = OB_SUCCESS;
  int safe_ret = OB_SUCCESS;
  const ObTransID& trans_id = trans_desc.get_trans_id();
  const bool is_readonly = trans_desc.is_readonly();
  const bool for_replay = false;
  const ObPhyPlanType phy_plan_type = trans_desc.get_cur_stmt_desc().phy_plan_type_;
  const bool is_bounded_staleness_read = trans_desc.is_bounded_staleness_read();
  const bool need_completed_dirty_txn = false;
  ObTransCtx* ctx = NULL;
  ObPartTransCtx* part_ctx = NULL;

  if (trans_desc.is_mini_sp_trans()) {
    if (trans_desc.is_all_ro_participants(participants, participant_cnt)) {
      TRANS_LOG(DEBUG, "duplicate read only end partition", K(trans_desc));
      int64_t epoch = 0;
      if (!is_rollback && participants.count() > 0 && trans_desc.need_check_at_end_participant()) {
        if (OB_FAIL(check_partition_status(trans_desc, participants.at(0), epoch))) {
          if (OB_NOT_MASTER != ret) {
            TRANS_LOG(WARN, "check partition status error", K(ret), K(participants), K(trans_desc));
          }
        } else if (epoch >= trans_desc.get_start_participant_ts()) {
          ret = OB_NOT_MASTER;
          TRANS_LOG(WARN, "maybe change leader twice (ABA)", K(ret), K(epoch), K(trans_desc), K(participants));
        } else {
          // do nothing
        }
      }
    } else if (OB_ISNULL(ctx = const_cast<ObTransDesc&>(trans_desc).get_part_ctx())) {
      TRANS_LOG(ERROR, "sp ctx is null, unexpected error", KP(part_ctx), K(trans_desc));
      ret = OB_ERR_UNEXPECTED;
    } else {
      part_ctx = static_cast<ObPartTransCtx*>(ctx);
      if (OB_FAIL(
              part_ctx->end_task(is_rollback, trans_desc, trans_desc.get_sql_no(), trans_desc.get_stmt_min_sql_no()))) {
        TRANS_LOG(WARN, "end participant task error", KR(ret), K(trans_desc));
      }
      if (trans_desc.is_trans_timeout()) {
        ret = OB_TRANS_TIMEOUT;
      } else if (trans_desc.is_stmt_timeout()) {
        ret = OB_TRANS_STMT_TIMEOUT;
      } else {
        // do nothing
      }
      if (part_ctx->is_exiting()) {
        const_cast<ObTransDesc&>(trans_desc).set_sp_trans_exiting();
        (void)const_cast<ObTransDesc&>(trans_desc).set_part_ctx(NULL);
        // part_ctx->print_trace_log();
        (void)part_trans_ctx_mgr_.revert_trans_ctx(part_ctx);
        part_ctx = NULL;
      }
    }
  } else if (trans_desc.is_sp_trans()) {
    if (trans_desc.is_all_ro_participants(participants, participant_cnt)) {
      TRANS_LOG(DEBUG, "duplicate read only end partition", K(trans_desc));
      int64_t epoch = 0;
      if (!is_rollback && participants.count() > 0 && trans_desc.need_check_at_end_participant()) {
        if (OB_FAIL(check_partition_status(trans_desc, participants.at(0), epoch))) {
          // consistency read on follower, let sql retry
          if (OB_NOT_MASTER != ret) {
            TRANS_LOG(WARN, "check partition status error", K(ret), K(participants), K(trans_desc));
          }
          // prevent ABA leader switch
        } else if (epoch >= trans_desc.get_start_participant_ts()) {
          ret = OB_NOT_MASTER;
          TRANS_LOG(WARN, "maybe change leader twice (ABA)", K(ret), K(epoch), K(trans_desc), K(participants));
        } else {
          // do nothing
        }
      }
    } else if (OB_ISNULL(ctx = const_cast<ObTransDesc&>(trans_desc).get_part_ctx())) {
      TRANS_LOG(ERROR, "sp ctx is null, unexpected error", KP(part_ctx), K(trans_desc));
      ret = OB_ERR_UNEXPECTED;
    } else {
      part_ctx = static_cast<ObPartTransCtx*>(ctx);
      if (OB_FAIL(
              part_ctx->end_task(is_rollback, trans_desc, trans_desc.get_sql_no(), trans_desc.get_stmt_min_sql_no()))) {
        TRANS_LOG(WARN, "end participant task error", KR(ret), K(trans_desc));
      }
      if (trans_desc.is_trans_timeout()) {
        ret = OB_TRANS_TIMEOUT;
      } else if (trans_desc.is_stmt_timeout()) {
        ret = OB_TRANS_STMT_TIMEOUT;
      } else {
        // do nothing
      }
      if (part_ctx->is_exiting()) {
        const_cast<ObTransDesc&>(trans_desc).set_sp_trans_exiting();
        // part_ctx->print_trace_log();
        (void)part_trans_ctx_mgr_.revert_trans_ctx(part_ctx);
        (void)const_cast<ObTransDesc&>(trans_desc).set_part_ctx(NULL);
      }
    }
  } else if (OB_PHY_PLAN_LOCAL == phy_plan_type || OB_PHY_PLAN_REMOTE == phy_plan_type ||
             OB_PHY_PLAN_DISTRIBUTED == phy_plan_type || OB_PHY_PLAN_UNCERTAIN == phy_plan_type) {
    int64_t index = 0;
    for (index = 0; index < participants.count() && index < participant_cnt; ++index) {
      bool alloc = false;
      const ObPartitionKey& partition = participants.at(index);
      if (trans_desc.is_not_create_ctx_participant(partition)) {
        TRANS_LOG(DEBUG, "not create ctx participant end participant", K(trans_desc));
        int64_t epoch = 0;
        if (!is_rollback && participants.count() > 0 && trans_desc.need_check_at_end_participant()) {
          if (OB_FAIL(check_partition_status(trans_desc, partition, epoch))) {
            if (OB_NOT_MASTER != ret) {
              TRANS_LOG(WARN, "check partition status error", K(ret), K(participants), K(trans_desc));
            }
          } else if (epoch >= trans_desc.get_start_participant_ts()) {
            ret = OB_NOT_MASTER;
            TRANS_LOG(WARN, "maybe change leader twice (ABA)", K(ret), K(epoch), K(trans_desc), K(participants));
          } else {
            // do nothing
          }
        }
      } else if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
                     trans_id,
                     for_replay,
                     is_readonly,
                     is_bounded_staleness_read,
                     need_completed_dirty_txn,
                     alloc,
                     ctx))) {
        TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_id));
      } else {
        part_ctx = static_cast<ObPartTransCtx*>(ctx);
        if (OB_FAIL(part_ctx->end_task(
                is_rollback, trans_desc, trans_desc.get_sql_no(), trans_desc.get_stmt_min_sql_no()))) {
          TRANS_LOG(WARN, "end participant task error", K(ret), K(partition), K(trans_id));
        }
        (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
      }
      if (OB_FAIL(ret)) {
        safe_ret = ret;
      }
    }
    if (OB_SUCC(ret)) {
      ret = safe_ret;
    }
    if (trans_desc.is_trans_timeout()) {
      ret = OB_TRANS_TIMEOUT;
    } else if (trans_desc.is_stmt_timeout()) {
      ret = OB_TRANS_STMT_TIMEOUT;
    } else {
      // do nothing
    }
  } else {
    TRANS_LOG(ERROR, "invalid phy plan type", K(phy_plan_type));
    ret = OB_ERR_UNEXPECTED;
  }

  return ret;
}

int ObTransService::end_participant(
    bool is_rollback, const ObTransDesc& trans_desc, const ObPartitionArray& participants)
{
  int ret = OB_SUCCESS;
  ObTransTraceLog& tlog = (const_cast<ObTransDesc&>(trans_desc)).get_tlog();
  const bool is_readonly = trans_desc.is_readonly();
#ifdef ERRSIM
  ret = E(EventTable::EN_END_PARTICIPANT_INTERFACE_ERROR) OB_SUCCESS;
  if (OB_ERR_UNEXPECTED == ret) {
    return ret;
  }
#endif

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (is_readonly && !trans_desc.get_cur_stmt_desc().is_readonly_stmt()) {
    TRANS_LOG(WARN, "can not execute dml in readonly transaction", K(trans_desc));
    ret = OB_ERR_READ_ONLY_TRANSACTION;
  } else if (!trans_desc.is_valid() || 0 == participants.count()) {
    TRANS_LOG(WARN, "invalid argument", K(trans_desc), K(participants));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (trans_desc.get_trans_param().is_bounded_staleness_read()) {
      // do nothing
    } else {
      if (OB_FAIL(handle_end_participant_(is_rollback, trans_desc, participants, participants.count()))) {
        TRANS_LOG(WARN, "handle end participant error", K(ret), K(is_rollback), K(trans_desc), K(participants));
      }
    }
  }

  REC_TRACE_EXT(tlog, end_participant, Y(ret));
  if (OB_FAIL(ret)) {
    (const_cast<ObTransDesc&>(trans_desc)).set_need_print_trace_log();
    TRANS_LOG(WARN, "end participant failed", KR(ret), K(trans_desc), K(participants), K(is_rollback));
  }
  return ret;
}

int ObTransService::can_replay_redo_(
    const char* buf, const int64_t len, const bool is_xa_redo, ObPartTransCtx* part_ctx, bool& can_replay_redo)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || len < 0 || (len == 0 && !is_xa_redo) || OB_ISNULL(part_ctx)) {
    TRANS_LOG(WARN, "invalid argument", KP(buf), K(len), KP(part_ctx), K(is_xa_redo));
    ret = OB_INVALID_ARGUMENT;
  } else if (len == 0 && is_xa_redo) {
    // handle this case specially for XA
    can_replay_redo = true;
  } else {
    ObMemtableMutatorMeta meta;
    bool need_replay_redo = true;
    int64_t pos = 0;
    if (OB_FAIL(meta.deserialize(buf, len, pos))) {
      TRANS_LOG(WARN, "meta deserialize error", KR(ret), KP(buf), K(len));
    } else {
      if (ObTransRowFlag::is_normal_row(meta.get_flags())) {
        need_replay_redo = true;
      } else if (ObTransRowFlag::is_big_row_start(meta.get_flags())) {
        need_replay_redo = true;
      } else if (ObTransRowFlag::is_big_row_mid(meta.get_flags()) || ObTransRowFlag::is_big_row_end(meta.get_flags())) {
        if (part_ctx->has_write_or_replay_mutator_redo_log()) {
          need_replay_redo = true;
        } else {
          need_replay_redo = false;
          TRANS_LOG(INFO, "no need to replay this big row redo log", K(meta), K(*part_ctx));
        }
      } else {
        TRANS_LOG(ERROR, "invalid row flag, unexpected error", K(meta), K(*part_ctx));
        ret = OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret)) {
      can_replay_redo = need_replay_redo;
    }
  }

  return ret;
}

int ObTransService::can_replay_mutator_(
    const char* buf, const int64_t len, ObPartTransCtx* part_ctx, bool& can_replay_mutator)
{
  const bool is_xa_redo = false;
  return can_replay_redo_(buf, len, is_xa_redo, part_ctx, can_replay_mutator);
}

int ObTransService::can_replay_elr_log_(const int64_t tenant_id, bool& can_replay)
{
  int ret = OB_SUCCESS;

  if (ObTransCtxFactory::get_active_part_ctx_cunt() > MAX_PART_CTX_COUNT / 2) {
    can_replay = false;
    if (EXECUTE_COUNT_PER_SEC(10)) {
      TRANS_LOG(WARN,
          "too many active part context, need retry current log",
          K(tenant_id),
          "part_ctx_cnt",
          ObTransCtxFactory::get_active_part_ctx_cunt());
    }
  } else {
    int64_t active_memstore_used = 0;
    int64_t total_memstore_used = 0;
    int64_t major_freeze_trigger = 0;
    int64_t memstore_limit = 0;
    int64_t freeze_cnt = 0;
    if (OB_FAIL(ObTenantManager::get_instance().get_tenant_memstore_cond(
            tenant_id, active_memstore_used, total_memstore_used, major_freeze_trigger, memstore_limit, freeze_cnt))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        can_replay = false;
        TRANS_LOG(WARN,
            "get tenant memstore cond error",
            K(ret),
            K(active_memstore_used),
            K(total_memstore_used),
            K(major_freeze_trigger),
            K(memstore_limit),
            K(freeze_cnt));
      } else {
        can_replay = true;
      }
      ret = OB_SUCCESS;
    } else {
      // for hotspot-update memory limit:
      // 1. don't affect tenant minor freeze
      // 2. limit >= 80%
      const int64_t limit = ((major_freeze_trigger < 80) ? 80 : (major_freeze_trigger + 5));
      if (total_memstore_used > (memstore_limit * (double)(limit / 100))) {
        TRANS_LOG(WARN,
            "nearly no memstore limit, current log need retry",
            K(total_memstore_used),
            K(memstore_limit),
            K(active_memstore_used),
            K(major_freeze_trigger),
            K(freeze_cnt));
        can_replay = false;
      } else {
        can_replay = true;
      }
    }
  }
  return ret;
}

int ObTransService::replay(const ObPartitionKey& partition, const char* logbuf, const int64_t size,
    const int64_t timestamp, const uint64_t log_id, const int64_t save_slave_read_timestamp, const bool batch_committed,
    const int64_t checkpoint, int64_t& log_table_version)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t log_type = storage::OB_LOG_UNKNOWN;
  int64_t tmp_idx = 0;
  ObTransCtx* ctx = NULL;
  ObPartTransCtx* part_ctx = NULL;
  const bool for_replay = true;
  const bool is_readonly = false;
  // maybe change to log.get_trans_timeXXX
  const int64_t trans_expired_time = ObClockGenerator::getClock();
  const int64_t leader_epoch = -1;
  const bool is_bounded_staleness_read = false;
  // for stop recovery and partition migrate situation, transaction
  // state maybe already determined before replay
  const bool need_completed_dirty_txn = true;
  bool replay_redo_log_succ = false;
  ObTransID tmp_trans_id;
  const uint64_t real_tenant_id = partition.get_tenant_id();

  // don't limit memory usage during replay
  get_ignore_mem_limit() = true;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid() || OB_ISNULL(logbuf) || size <= 0 || timestamp < 0 || 0 > checkpoint) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(partition),
        "logbuf",
        OB_P(logbuf),
        K(size),
        K(timestamp),
        K(log_id),
        K(checkpoint));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i64(logbuf, size, pos, &log_type))) {
    TRANS_LOG(WARN, "decode log type error", KR(ret), K(partition));
  } else if (OB_FAIL(serialization::decode_i64(logbuf, size, pos, &tmp_idx))) {
    TRANS_LOG(WARN, "decode index error", KR(ret), K(partition));
  } else if (!ObTransLogType::is_valid(log_type)) {
    TRANS_LOG(ERROR, "unknown log type", K(log_type));
    ret = OB_TRANS_INVALID_LOG_TYPE;
  } else if ((log_type & OB_LOG_SP_TRANS_REDO) != 0) {
    ObSpTransRedoLog log;
    if (OB_FAIL(log.get_mutator().init())) {
      TRANS_LOG(WARN, "sp redo log init fail", KR(ret), K(partition));
    } else if (OB_FAIL(log.deserialize(logbuf, size, pos))) {
      TRANS_LOG(WARN, "sp log deserialize error", KR(ret), K(partition));
    } else if (real_tenant_id != log.get_tenant_id() && OB_FAIL(log.replace_tenant_id(real_tenant_id))) {
      TRANS_LOG(WARN, "replace_tenant_id failed", KR(ret), K(partition));
    } else {
      const ObTransID& trans_id = log.get_trans_id();
      const uint64_t tenant_id = log.get_tenant_id();
      // bool alloc = (0 == log.get_log_no());
      bool alloc = true;
      if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
              trans_id,
              for_replay,
              is_readonly,
              is_bounded_staleness_read,
              need_completed_dirty_txn,
              alloc,
              ctx))) {
        TRANS_LOG(DEBUG, "get transaction context error", KR(ret), K(partition), K(trans_id));
      } else {
        part_ctx = static_cast<ObPartTransCtx*>(ctx);
        part_ctx->set_trans_type(TransType::SP_TRANS);
        const bool can_elr = log.is_can_elr();
        bool can_replay_redo = true;
        if (alloc && OB_FAIL(part_ctx->init(tenant_id,
                         trans_id,
                         trans_expired_time,
                         partition,
                         &part_trans_ctx_mgr_,
                         log.get_trans_param(),
                         GET_MIN_CLUSTER_VERSION(),
                         this,
                         log.get_cluster_id(),
                         leader_epoch,
                         can_elr))) {
          TRANS_LOG(WARN, "transaction context init error", KR(ret), K(partition), K(trans_id));
        } else if (OB_FAIL(can_replay_redo_(log.get_mutator().get_data(),
                       log.get_mutator().get_position(),
                       false,
                       part_ctx,
                       can_replay_redo))) {
          TRANS_LOG(WARN, "check can replay redo error", KR(ret), K(log));
        } else if (!can_replay_redo) {
          TRANS_LOG(WARN, "ignore sp redo log", K(log), K(timestamp), K(log_id));
        } else if (OB_FAIL(part_ctx->replay_sp_redo_log(log, timestamp, log_id, log_table_version))) {
          if (OB_ALLOCATE_MEMORY_FAILED != ret || EXECUTE_COUNT_PER_SEC(100)) {
            TRANS_LOG(WARN, "replay sp redo log error", KR(ret), K(log), K(timestamp), K(log_id));
          }
        } else {
          // do nothing
        }
        (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
      }
    }
  } else if ((log_type & OB_LOG_SP_TRANS_COMMIT) != 0 || (log_type & OB_LOG_SP_ELR_TRANS_COMMIT) != 0) {
    ObSpTransCommitLog log;
    ObSpTransRedoLog& sp_redo_log = log;
    bool can_relay_elr_log = true;
    if (OB_FAIL(log.get_mutator().init())) {
      TRANS_LOG(WARN, "sp redo log init error", KR(ret), K(partition), K(log_type), K(log_id));
    } else if (OB_FAIL(log.deserialize(logbuf, size, pos))) {
      TRANS_LOG(WARN, "log deserialize error", KR(ret), K(partition));
    } else if (real_tenant_id != log.get_tenant_id() && OB_FAIL(log.replace_tenant_id(real_tenant_id))) {
      TRANS_LOG(WARN, "replace_tenant_id failed", K(ret), K(partition));
    } else if ((log_type & OB_LOG_SP_ELR_TRANS_COMMIT) != 0 &&
               OB_FAIL(can_replay_elr_log_(log.get_tenant_id(), can_relay_elr_log))) {
      // limit active part_ctx and memory usage, prevent out of memory during recovery replay
      if (EXECUTE_COUNT_PER_SEC(10)) {
        TRANS_LOG(WARN, "check can replay elr log error", K(ret), K(log));
      }
    } else if (!can_relay_elr_log) {
      ret = OB_EAGAIN;
    } else {
      const ObTransID& trans_id = log.get_trans_id();
      const uint64_t tenant_id = log.get_tenant_id();
      bool alloc = true;
      if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
              trans_id,
              for_replay,
              is_readonly,
              is_bounded_staleness_read,
              need_completed_dirty_txn,
              alloc,
              ctx))) {
        if (OB_TRANS_CTX_NOT_EXIST != ret) {
          TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_id));
        } else {
          // rewrite ret
          ret = OB_SUCCESS;
          TRANS_LOG(WARN, "ignore commit log", K(log), K(timestamp), K(log_id));
        }
      } else {
        bool is_empty_redo_log = log.is_empty_redo_log();
        const int64_t log_checkpoint = log.get_checkpoint();
        part_ctx = static_cast<ObPartTransCtx*>(ctx);
        part_ctx->set_trans_type(TransType::SP_TRANS);
        const bool can_elr = log.is_can_elr();
        if (alloc && OB_FAIL(part_ctx->init(tenant_id,
                         trans_id,
                         trans_expired_time,
                         partition,
                         &part_trans_ctx_mgr_,
                         log.get_trans_param(),
                         GET_MIN_CLUSTER_VERSION(),
                         this,
                         log.get_cluster_id(),
                         leader_epoch,
                         can_elr))) {
          TRANS_LOG(WARN, "transaction context init error", KR(ret), K(partition), K(trans_id));
          // replay redo log in sp-commit log
        } else if (!is_empty_redo_log &&
                   OB_FAIL(part_ctx->replay_sp_redo_log(sp_redo_log, timestamp, log_id, log_table_version))) {
          if (OB_ALLOCATE_MEMORY_FAILED != ret || EXECUTE_COUNT_PER_SEC(100)) {
            TRANS_LOG(WARN, "replay sp redo log error", KR(ret), K(log), K(timestamp), K(log_id));
          }
        } else {
          if (!is_empty_redo_log) {
            replay_redo_log_succ = true;
            tmp_trans_id = trans_id;
          }
          if (0 != save_slave_read_timestamp) {
            if (timestamp <= save_slave_read_timestamp && timestamp > part_ctx->get_max_durable_log_ts()) {
              TRANS_LOG(ERROR,
                  "unexpected row compact version when replaying",
                  K(save_slave_read_timestamp),
                  K(log),
                  K(timestamp));
            }
            part_ctx->get_memtable_ctx()->set_read_snapshot(save_slave_read_timestamp);
          }
          if (OB_SUCC(ret) && OB_FAIL(set_replay_checkpoint_(partition, log_checkpoint))) {
            TRANS_LOG(WARN, "set replay checkpoint failed", KR(ret), K(partition), K(log));
          }
          if (OB_SUCC(ret) && OB_FAIL(part_ctx->replay_sp_commit_log(log, timestamp, log_id))) {
            if (OB_ALLOCATE_MEMORY_FAILED != ret || EXECUTE_COUNT_PER_SEC(100)) {
              TRANS_LOG(WARN, "replay sp commit log error", KR(ret), K(log), K(timestamp), K(log_id));
            }
          }
        }
        (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
      }
    }
  } else if ((log_type & OB_LOG_SP_TRANS_ABORT) != 0) {
    ObSpTransAbortLog log;
    if (OB_FAIL(log.deserialize(logbuf, size, pos))) {
      TRANS_LOG(WARN, "log deserialize error", KR(ret), K(partition));
    } else if (real_tenant_id != log.get_tenant_id() && OB_FAIL(log.replace_tenant_id(real_tenant_id))) {
      TRANS_LOG(WARN, "replace_tenant_id failed", K(ret), K(partition));
    } else {
      const ObTransID& trans_id = log.get_trans_id();
      bool alloc = false;
      if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
              trans_id,
              for_replay,
              is_readonly,
              is_bounded_staleness_read,
              need_completed_dirty_txn,
              alloc,
              ctx))) {
        if (OB_TRANS_CTX_NOT_EXIST != ret) {
          TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_id));
        } else {
          // rewrite ret
          ret = OB_SUCCESS;
          TRANS_LOG(WARN, "ignore abort log", K(log), K(timestamp), K(log_id));
        }
      } else {
        part_ctx = static_cast<ObPartTransCtx*>(ctx);
        part_ctx->set_trans_type(TransType::SP_TRANS);
        if (OB_FAIL(part_ctx->replay_sp_abort_log(log, timestamp, log_id))) {
          if (OB_ALLOCATE_MEMORY_FAILED != ret || EXECUTE_COUNT_PER_SEC(100)) {
            TRANS_LOG(WARN, "replay abort log error", KR(ret), K(log), K(timestamp), K(log_id));
          }
        }
        (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
      }
    }
  } else {
    if (OB_SUCCESS == ret && ((OB_LOG_MUTATOR & log_type) != 0)) {
      ObTransMutatorLog log;
      if (OB_FAIL(log.get_mutator().init())) {
        TRANS_LOG(WARN, "redo log init fail", KR(ret), K(partition));
      } else if (OB_FAIL(log.deserialize(logbuf, size, pos))) {
        TRANS_LOG(WARN, "log deserialize error", KR(ret), K(partition));
      } else if (real_tenant_id != log.get_tenant_id() && OB_FAIL(log.replace_tenant_id(real_tenant_id))) {
        TRANS_LOG(WARN, "replace_tenant_id failed", K(ret), K(partition));
      } else {
        const ObTransID& trans_id = log.get_trans_id();
        const uint64_t tenant_id = log.get_tenant_id();
        const uint64_t cluster_version =
            (log.get_cluster_version() > 0 ? log.get_cluster_version() : GET_MIN_CLUSTER_VERSION());
        bool alloc = true;
        if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
                trans_id,
                for_replay,
                is_readonly,
                is_bounded_staleness_read,
                need_completed_dirty_txn,
                alloc,
                ctx))) {
          TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_id));
        } else {
          part_ctx = static_cast<ObPartTransCtx*>(ctx);
          bool can_replay_mutator = true;
          if (alloc && OB_FAIL(part_ctx->init(tenant_id,
                           trans_id,
                           log.get_trans_expired_time(),
                           partition,
                           &part_trans_ctx_mgr_,
                           log.get_trans_param(),
                           cluster_version,
                           this,
                           log.get_cluster_id(),
                           leader_epoch,
                           log.is_can_elr()))) {
          } else if (OB_FAIL(can_replay_mutator_(log.get_mutator().get_data(),
                         log.get_mutator().get_position(),
                         part_ctx,
                         can_replay_mutator))) {
            TRANS_LOG(WARN, "check can replay mutator error", KR(ret), K(log));
          } else if (!can_replay_mutator) {
            TRANS_LOG(WARN, "ignore mutator log", K(log), K(timestamp), K(log_id));
          } else if (OB_FAIL(part_ctx->replay_trans_mutator_log(log, timestamp, log_id, log_table_version))) {
            if (OB_ALLOCATE_MEMORY_FAILED != ret || EXECUTE_COUNT_PER_SEC(100)) {
              TRANS_LOG(WARN, "replay trans mutator log error", KR(ret), K(log), K(timestamp), K(log_id));
            }
          } else {
            // do nothing
          }
          (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
        }
      }
    }
    if (OB_SUCCESS == ret && ((OB_LOG_TRANS_STATE & log_type) != 0)) {
      ObTransStateLog log;
      if (OB_FAIL(log.deserialize(logbuf, size, pos))) {
        TRANS_LOG(WARN, "log deserialize error", KR(ret), K(partition));
      } else if (real_tenant_id != log.get_tenant_id() && OB_FAIL(log.replace_tenant_id(real_tenant_id))) {
        TRANS_LOG(WARN, "replace_tenant_id failed", K(ret), K(partition));
      } else {
        const ObTransID& trans_id = log.get_trans_id();
        const uint64_t tenant_id = log.get_tenant_id();
        const uint64_t cluster_version =
            (log.get_cluster_version() > 0 ? log.get_cluster_version() : GET_MIN_CLUSTER_VERSION());
        bool alloc = true;
        if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
                trans_id,
                for_replay,
                is_readonly,
                is_bounded_staleness_read,
                need_completed_dirty_txn,
                alloc,
                ctx))) {
          TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_id));
        } else {
          part_ctx = static_cast<ObPartTransCtx*>(ctx);
          if (alloc && OB_FAIL(part_ctx->init(tenant_id,
                           trans_id,
                           log.get_trans_expired_time(),
                           partition,
                           &part_trans_ctx_mgr_,
                           log.get_trans_param(),
                           cluster_version,
                           this,
                           log.get_cluster_id(),
                           leader_epoch,
                           log.is_can_elr()))) {
            TRANS_LOG(WARN, "transaction context init error", KR(ret), K(partition), K(trans_id));
          } else if (OB_FAIL(part_ctx->replay_trans_state_log(log, timestamp, log_id))) {
            if (OB_ALLOCATE_MEMORY_FAILED != ret || EXECUTE_COUNT_PER_SEC(100)) {
              TRANS_LOG(WARN, "replay trans state log error", KR(ret), K(log), K(timestamp), K(log_id));
            }
          } else {
            // do nothing
          }
          (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
        }
      }
    }
    if (OB_SUCCESS == ret && ((OB_LOG_MUTATOR_ABORT & log_type) != 0)) {
      ObTransMutatorAbortLog log;
      if (OB_FAIL(log.deserialize(logbuf, size, pos))) {
        TRANS_LOG(WARN, "log deserialize error", KR(ret), K(partition));
      } else if (real_tenant_id != log.get_tenant_id() && OB_FAIL(log.replace_tenant_id(real_tenant_id))) {
        TRANS_LOG(WARN, "replace_tenant_id failed", K(ret), K(partition));
      } else {
        const ObTransID& trans_id = log.get_trans_id();
        bool alloc = false;
        if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
                trans_id,
                for_replay,
                is_readonly,
                is_bounded_staleness_read,
                need_completed_dirty_txn,
                alloc,
                ctx))) {
          if (OB_TRANS_CTX_NOT_EXIST != ret) {
            TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_id));
          } else {
            // rewrite ret
            ret = OB_SUCCESS;
            TRANS_LOG(WARN, "ignore mutator abort log", K(log), K(timestamp), K(log_id));
          }
        } else {
          part_ctx = static_cast<ObPartTransCtx*>(ctx);
          if (OB_FAIL(part_ctx->replay_mutator_abort_log(log, timestamp, log_id))) {
            if (OB_ALLOCATE_MEMORY_FAILED != ret || EXECUTE_COUNT_PER_SEC(100)) {
              TRANS_LOG(WARN, "replay trans mutator abort log error", KR(ret), K(log), K(timestamp), K(log_id));
            }
          }
          (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
        }
      }
    }
    if (OB_SUCCESS == ret && ((log_type & OB_LOG_TRANS_REDO) != 0)) {
      ObTransRedoLogHelper helper;
      ObTransRedoLog log(helper);
      bool can_relay_elr_log = true;
      if (OB_FAIL(log.init())) {
        TRANS_LOG(WARN, "redo log init fail", KR(ret), K(partition));
      } else if (OB_FAIL(log.deserialize(logbuf, size, pos))) {
        TRANS_LOG(WARN, "log deserialize error", KR(ret), K(partition));
      } else if (log.get_prev_trans_arr().count() > 0 &&
                 OB_FAIL(can_replay_elr_log_(log.get_tenant_id(), can_relay_elr_log))) {
        if (EXECUTE_COUNT_PER_SEC(10)) {
          TRANS_LOG(WARN, "check can replay elr log error", K(ret), K(log));
        }
      } else if (!can_relay_elr_log) {
        ret = OB_EAGAIN;
      } else if (real_tenant_id != log.get_tenant_id() && OB_FAIL(log.replace_tenant_id(real_tenant_id))) {
        TRANS_LOG(WARN, "replace_tenant_id failed", K(ret), K(partition));
      } else {
        const ObTransID& trans_id = log.get_trans_id();
        const uint64_t tenant_id = log.get_tenant_id();
        const bool is_xa_redo = !log.get_xid().empty();
        // bool alloc = (0 == log.get_log_no());
        bool alloc = true;
        bool light_mgr_ret = false;
        if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
                trans_id,
                for_replay,
                is_readonly,
                is_bounded_staleness_read,
                need_completed_dirty_txn,
                alloc,
                ctx))) {
          TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_id));
        } else {
          const bool with_prepare = ((log_type & OB_LOG_TRANS_PREPARE) != 0);
          part_ctx = static_cast<ObPartTransCtx*>(ctx);
          const bool can_elr = log.is_can_elr();
          bool can_replay_redo = true;
          if (alloc && OB_FAIL(part_ctx->init(tenant_id,
                           trans_id,
                           trans_expired_time,
                           partition,
                           &part_trans_ctx_mgr_,
                           log.get_trans_param(),
                           GET_MIN_CLUSTER_VERSION(),
                           this,
                           log.get_cluster_id(),
                           leader_epoch,
                           can_elr))) {
            TRANS_LOG(WARN, "transaction context init error", KR(ret), K(partition), K(trans_id));
          } else {
            if (OB_FAIL(can_replay_redo_(log.get_mutator().get_data(),
                    log.get_mutator().get_position(),
                    is_xa_redo,
                    part_ctx,
                    can_replay_redo))) {
              TRANS_LOG(WARN, "check can replay redo error", KR(ret), K(log));
            } else if (!can_replay_redo) {
              TRANS_LOG(WARN, "ignore redo log", K(log), K(timestamp), K(log_id));
            } else if (OB_FAIL(part_ctx->replay_redo_log(log, timestamp, log_id, with_prepare, log_table_version))) {
              if (OB_ALLOCATE_MEMORY_FAILED != ret || EXECUTE_COUNT_PER_SEC(100)) {
                TRANS_LOG(WARN, "replay redo log error", KR(ret), K(log), K(timestamp), K(log_id));
              }
            } else {
              replay_redo_log_succ = true;
              tmp_trans_id = trans_id;
            }
            if (light_trans_ctx_mgr_.add_trans_ctx(ctx)) {
              light_mgr_ret = true;
            }
          }
          if (!light_mgr_ret) {
            (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
          }
        }
      }
    }
    if (OB_SUCCESS == ret && ((log_type & OB_LOG_TRANS_PREPARE) != 0)) {
      ObTransPrepareLogHelper helper;
      ObTransPrepareLog log(helper);
      if (OB_FAIL(log.deserialize(logbuf, size, pos))) {
        TRANS_LOG(WARN, "log deserialize error", KR(ret), K(partition));
      } else if (real_tenant_id != log.get_tenant_id() && OB_FAIL(log.replace_tenant_id(real_tenant_id))) {
        TRANS_LOG(WARN, "replace_tenant_id failed", K(ret), K(partition));
      } else {
        const ObTransID& trans_id = log.get_trans_id();
        const uint64_t tenant_id = log.get_tenant_id();
        bool alloc = true;
        bool light_mgr_ret = true;
        if (!light_trans_ctx_mgr_.get_trans_ctx(partition, trans_id, ctx)) {
          light_mgr_ret = false;
          if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
                  trans_id,
                  for_replay,
                  is_readonly,
                  is_bounded_staleness_read,
                  need_completed_dirty_txn,
                  alloc,
                  ctx))) {
            TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_id), K(alloc));
          }
        } else {
          alloc = false;
        }
        // ret must be true if light_mgr_ret is ture
        if (OB_SUCC(ret)) {
          part_ctx = static_cast<ObPartTransCtx*>(ctx);
          const int64_t log_checkpoint = log.get_checkpoint();
          const bool can_elr = log.is_can_elr();
          if (alloc && OB_FAIL(part_ctx->init(tenant_id,
                           trans_id,
                           trans_expired_time,
                           partition,
                           &part_trans_ctx_mgr_,
                           log.get_trans_param(),
                           GET_MIN_CLUSTER_VERSION(),
                           this,
                           log.get_cluster_id(),
                           leader_epoch,
                           can_elr))) {
            TRANS_LOG(WARN, "transaction context init error", KR(ret), K(partition), K(trans_id));
          } else if (log_checkpoint > checkpoint) {
            if (OB_FAIL(set_replay_checkpoint_(partition, log_checkpoint))) {
              TRANS_LOG(WARN, "set replay checkpoint failed", KR(ret), K(partition), K(log));
            }
          } else {
            // do nothing
          }
          if (OB_SUCC(ret) &&
              OB_FAIL(part_ctx->replay_prepare_log(log, timestamp, log_id, batch_committed, checkpoint))) {
            TRANS_LOG(WARN, "replay prepare log error", KR(ret), K(log), K(timestamp), K(log_id));
          }
          if (!light_mgr_ret) {
            (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
          } else if (log.is_batch_commit_trans()) {
            if (!light_trans_ctx_mgr_.remove_trans_ctx(partition, trans_id)) {
              TRANS_LOG(WARN,
                  "[prepare] fail to remove ctx from lightweight trans ctx mgr",
                  K(ret),
                  K(partition),
                  K(trans_id));
            }
            (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
          }
        }
      }
    }
    if (OB_SUCCESS == ret && ((log_type & OB_LOG_TRANS_COMMIT) != 0)) {
      PartitionLogInfoArray partition_log_info_arr;
      ObTransCommitLog log(partition_log_info_arr);
      if (OB_FAIL(log.deserialize(logbuf, size, pos))) {
        TRANS_LOG(WARN, "log deserialize error", KR(ret), K(partition));
      } else if (real_tenant_id != log.get_tenant_id() && OB_FAIL(log.replace_tenant_id(real_tenant_id))) {
        TRANS_LOG(WARN, "replace_tenant_id failed", K(ret), K(partition));
      } else {
        const ObTransID& trans_id = log.get_trans_id();
        bool alloc = false;
        bool light_mgr_ret = true;
        int tmp_ret = OB_SUCCESS;
        if (!light_trans_ctx_mgr_.get_trans_ctx(partition, trans_id, ctx)) {
          light_mgr_ret = false;
          if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
                  trans_id,
                  for_replay,
                  is_readonly,
                  is_bounded_staleness_read,
                  need_completed_dirty_txn,
                  alloc,
                  ctx))) {
            tmp_ret = ret;
            if (OB_TRANS_CTX_NOT_EXIST != ret) {
              TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_id));
            } else {
              // rewrite ret
              ret = OB_SUCCESS;
              TRANS_LOG(WARN, "ignore commit log", K(log), K(timestamp), K(log_id));
            }
          }
        }
        if (OB_SUCCESS == tmp_ret) {
          part_ctx = static_cast<ObPartTransCtx*>(ctx);
          if (0 != save_slave_read_timestamp) {
            if (log.get_global_trans_version() <= save_slave_read_timestamp &&
                timestamp > part_ctx->get_max_durable_log_ts()) {
              TRANS_LOG(ERROR,
                  "unexpected row compact version when replaying",
                  K(save_slave_read_timestamp),
                  K(log),
                  K(timestamp));
            }
            part_ctx->get_memtable_ctx()->set_read_snapshot(save_slave_read_timestamp);
          }
          if (OB_SUCC(ret) && OB_FAIL(part_ctx->replay_commit_log(log, timestamp, log_id))) {
            TRANS_LOG(WARN, "replay commit log error", KR(ret), K(log), K(timestamp), K(log_id));
          }
          if (!light_mgr_ret) {
            (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
          }
        }
      }
    }
    if (OB_SUCCESS == ret && ((log_type & OB_LOG_TRANS_ABORT) != 0)) {
      ObTransAbortLog log;
      if (OB_FAIL(log.deserialize(logbuf, size, pos))) {
        TRANS_LOG(WARN, "log deserialize error", KR(ret), K(partition));
      } else if (real_tenant_id != log.get_tenant_id() && OB_FAIL(log.replace_tenant_id(real_tenant_id))) {
        TRANS_LOG(WARN, "replace_tenant_id failed", K(ret), K(partition));
      } else {
        const ObTransID& trans_id = log.get_trans_id();
        bool alloc = false;
        bool light_mgr_ret = true;
        int tmp_ret = OB_SUCCESS;
        if (!light_trans_ctx_mgr_.get_trans_ctx(partition, trans_id, ctx)) {
          light_mgr_ret = false;
          if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
                  trans_id,
                  for_replay,
                  is_readonly,
                  is_bounded_staleness_read,
                  need_completed_dirty_txn,
                  alloc,
                  ctx))) {
            tmp_ret = ret;
            if (OB_TRANS_CTX_NOT_EXIST != ret) {
              TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_id));
            } else {
              // rewrite ret
              ret = OB_SUCCESS;
              TRANS_LOG(WARN, "ignore abort log", K(log), K(timestamp), K(log_id));
            }
          }
        }
        if (OB_SUCCESS == tmp_ret) {
          part_ctx = static_cast<ObPartTransCtx*>(ctx);
          if (OB_FAIL(part_ctx->replay_abort_log(log, timestamp, log_id))) {
            TRANS_LOG(WARN, "replay abort log error", KR(ret), K(log), K(timestamp), K(log_id));
          }
          if (!light_mgr_ret) {
            (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
          }
        }
      }
    }
    if (OB_SUCCESS == ret && ((log_type & OB_LOG_TRANS_CLEAR) != 0)) {
      ObTransClearLog log;
      if (OB_FAIL(log.deserialize(logbuf, size, pos))) {
        TRANS_LOG(WARN, "log deserialize error", KR(ret), K(partition));
      } else if (real_tenant_id != log.get_tenant_id() && OB_FAIL(log.replace_tenant_id(real_tenant_id))) {
        TRANS_LOG(WARN, "replace_tenant_id failed", K(ret), K(partition));
      } else {
        const ObTransID& trans_id = log.get_trans_id();
        bool alloc = false;
        int tmp_ret = OB_SUCCESS;
        if (!light_trans_ctx_mgr_.get_and_remove_trans_ctx(partition, trans_id, ctx)) {
          if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
                  trans_id,
                  for_replay,
                  is_readonly,
                  is_bounded_staleness_read,
                  need_completed_dirty_txn,
                  alloc,
                  ctx))) {
            tmp_ret = ret;
            if (OB_TRANS_CTX_NOT_EXIST != ret) {
              TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_id));
            } else {
              // rewrite ret
              ret = OB_SUCCESS;
              TRANS_LOG(WARN, "ignore clear log", K(log), K(timestamp), K(log_id));
            }
          }
        }
        if (OB_SUCCESS == tmp_ret) {
          part_ctx = static_cast<ObPartTransCtx*>(ctx);
          if (OB_FAIL(part_ctx->replay_clear_log(log, timestamp, log_id))) {
            TRANS_LOG(WARN, "replay clear log error", KR(ret), K(log), K(timestamp), K(log_id));
          }
          (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
        }
      }
    }
  }

  // replay failed after redo log replay success,
  // stmt need to be rollbacked
  if (OB_FAIL(ret) && replay_redo_log_succ) {
    int tmp_ret = OB_SUCCESS;
    bool alloc = false;
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = part_trans_ctx_mgr_.get_trans_ctx(partition,
                                       tmp_trans_id,
                                       for_replay,
                                       is_readonly,
                                       is_bounded_staleness_read,
                                       need_completed_dirty_txn,
                                       alloc,
                                       ctx)))) {
      TRANS_LOG(WARN, "get transaction context error", K(tmp_ret), K(partition), K(tmp_trans_id));
    } else {
      part_ctx = static_cast<ObPartTransCtx*>(ctx);
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = part_ctx->get_memtable_ctx()->sub_trans_end(false)))) {
        TRANS_LOG(WARN, "sub trans abort error", K(tmp_ret), K(partition), K(log_id), K(*part_ctx));
      }
      (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
    }
  }

  // reset mem list
  get_ignore_mem_limit() = false;
  return ret;
}

int ObTransService::replay_start_working_log(
    const common::ObPartitionKey& pkey, const int64_t timestamp, const uint64_t log_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_FAIL(part_trans_ctx_mgr_.replay_start_working_log(pkey, timestamp, log_id))) {
    TRANS_LOG(WARN, "replay start working log error", KR(ret), K(pkey), K(timestamp), K(log_id));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransService::check_and_handle_orphan_msg_(const int ret_code, const int64_t leader_epoch, const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  const int64_t msg_type = msg.get_msg_type();
  const ObPartitionKey& partition = msg.get_receiver();
  const ObTransID& trans_id = msg.get_trans_id();
  const bool for_replay = false;
  const bool is_readonly = msg.get_trans_param().is_readonly();
  const bool is_bounded_staleness_read = false;
  const bool need_completed_dirty_txn = false;
  const int64_t trans_expired_time = msg.get_trans_time();
  ObTransCtx* ctx = NULL;
  ObPartTransCtx* part_ctx = NULL;

  if (need_handle_orphan_msg_(ret_code, msg)) {
    // double check partition is leader, otherwise don't reponse with prepare_unknown
    const bool check_election = true;
    int64_t new_leader_epoch = -1;
    ObTsWindows changing_leader_windows;
    int clog_status = OB_SUCCESS;
    if ((OB_TRANS_STMT_ROLLBACK_REQUEST == msg_type || OB_TRANS_SAVEPOINT_ROLLBACK_REQUEST == msg_type) &&
        (OB_PARTITION_NOT_EXIST == ret_code || OB_PARTITION_IS_BLOCKED == ret_code ||
            OB_PARTITION_IS_STOPPED == ret_code)) {
      // do nothing
    } else if (OB_FAIL(clog_adapter_->get_status(
                   partition, check_election, clog_status, new_leader_epoch, changing_leader_windows))) {
      TRANS_LOG(WARN, "get participant status error", KR(ret), K(partition), K(trans_id));
    } else if (OB_SUCCESS != clog_status) {
      ret = clog_status;
    } else if (leader_epoch != new_leader_epoch) {
      ret = OB_NOT_MASTER;
    } else {
      // do nothing
    }
    if (OB_SUCC(ret) && OB_FAIL(handle_orphan_msg_(msg, ret_code))) {
      TRANS_LOG(WARN, "handle orphan message error", KR(ret), K(msg));
    }
    // rollback request come first before stmt read/write task
    // mark part_context stmt rollbacked, used to reject futher task
  } else if (OB_TRANS_STMT_ROLLBACK_REQUEST == msg.get_msg_type() && OB_TRANS_CTX_NOT_EXIST == ret_code &&
             0 != msg.get_cluster_version()) {
    bool alloc = msg.need_create_ctx();
    if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
            trans_id,
            for_replay,
            is_readonly,
            is_bounded_staleness_read,
            need_completed_dirty_txn,
            alloc,
            ctx))) {
      TRANS_LOG(WARN,
          "get transaction context error",
          KR(ret),
          K(partition),
          K(trans_id),
          "msg_type",
          msg.get_msg_type(),
          "sender_addr",
          msg.get_sender_addr());
    } else {
      part_ctx = static_cast<ObPartTransCtx*>(ctx);
      if (alloc) {
        if (OB_FAIL(part_ctx->init(msg.get_tenant_id(),
                trans_id,
                trans_expired_time,
                partition,
                &part_trans_ctx_mgr_,
                msg.get_trans_param(),
                msg.get_trans_param().get_cluster_version(),
                this,
                msg.get_cluster_id(),
                leader_epoch,
                msg.is_can_elr()))) {
          TRANS_LOG(WARN, "context init error", KR(ret), K(partition), K(trans_id), K(msg));
          part_ctx = NULL;
        } else if (OB_FAIL(part_ctx->construct_context(msg))) {
          TRANS_LOG(WARN, "participant construct transaction context error", KR(ret), K(trans_id), K(msg));
        } else if (OB_FAIL(part_ctx->handle_message(msg))) {
          TRANS_LOG(WARN, "participant handle message error", KR(ret), K(trans_id), K(msg));
        } else {
          // do nothing
        }
      } else if (OB_FAIL(part_ctx->handle_message(msg))) {
        TRANS_LOG(WARN, "participant handle message error", KR(ret), K(trans_id), K(msg));
      } else {
        // do nothing
      }
      (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
    }
    if (EXECUTE_COUNT_PER_SEC(16)) {
      int tmp_ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "handle pre stmt rollback finished", "ret", tmp_ret, "real_ret", ret, K(msg));
    }
    // CLOG state is leader, but part_ctx not switch to leader active state
    // in this case, schduler need do retry later
    if (OB_SUCCESS != ret && OB_NOT_MASTER != ret) {
      // if stmt rollback fail and not cause by leader switch, transaction must rollback
      ret = OB_SUCCESS;
    }
  } else {
    // other error will handle by caller
    ret = ret_code;
  }

  return ret;
}

int ObTransService::handle_participant_msg_(
    const ObTransMsg& msg, const ObPartitionKey& partition, const bool ctx_alloc)
{
  int ret = OB_SUCCESS;

  bool alloc = ctx_alloc;
  const ObTransID& trans_id = msg.get_trans_id();
  const bool for_replay = false;
  const bool is_readonly = msg.get_trans_param().is_readonly();
  const bool is_bounded_staleness_read = false;
  const bool need_completed_dirty_txn = false;
  const int64_t msg_type = msg.get_msg_type();
  ObTransCtx* ctx = NULL;
  ObPartTransCtx* part_ctx = NULL;

  ObTsWindows changing_leader_windows;
  int clog_status = OB_SUCCESS;
  int64_t leader_epoch = 0;

  // start stmt(consult snapshot), don't create transaction context
  if (OB_TRANS_START_STMT_REQUEST == msg_type && msg.is_not_create_ctx_participant() &&
      !msg.get_trans_param().is_bounded_staleness_read()) {
    if (OB_FAIL(handle_start_stmt_request_(msg))) {
      TRANS_LOG(WARN, "handle start stmt request error", KR(ret), K(msg));
    }
  } else {
    if (OB_TRANS_ASK_SCHEDULER_STATUS_RESPONSE != msg_type) {
      if (OB_FAIL(clog_adapter_->get_status_unsafe(partition, clog_status, leader_epoch, changing_leader_windows))) {
        TRANS_LOG(WARN, "get participant status error", KR(ret), K(partition), K(trans_id));
      } else if (OB_SUCCESS == clog_status) {
        if (alloc && OB_TRANS_START_STMT_REQUEST == msg_type &&
            !can_create_ctx_(trans_id.get_timestamp(), changing_leader_windows)) {
          ret = OB_NOT_MASTER;
        }
      } else {
        ret = clog_status;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
              trans_id,
              for_replay,
              is_readonly,
              is_bounded_staleness_read,
              need_completed_dirty_txn,
              alloc,
              ctx))) {
        TRANS_LOG(WARN,
            "get transaction context error",
            KR(ret),
            K(partition),
            K(trans_id),
            "msg_type",
            msg.get_msg_type(),
            "sender_addr",
            msg.get_sender_addr());
      } else {
        // do nothing
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_FAIL(check_and_handle_orphan_msg_(ret, leader_epoch, msg))) {
        TRANS_LOG(WARN, "check and handle orphan msg error", KR(ret), K(leader_epoch), K(msg));
      }
    } else {
      part_ctx = static_cast<ObPartTransCtx*>(ctx);
      const int64_t trans_expired_time = msg.get_trans_time();
      if (alloc) {
        if (OB_FAIL(part_ctx->init(msg.get_tenant_id(),
                trans_id,
                trans_expired_time,
                partition,
                &part_trans_ctx_mgr_,
                msg.get_trans_param(),
                msg.get_cluster_version(),
                this,
                msg.get_cluster_id(),
                leader_epoch,
                msg.is_can_elr()))) {
          TRANS_LOG(WARN, "context init error", KR(ret), K(partition), K(trans_id), K(msg));
          part_ctx = NULL;
        } else if (OB_FAIL(part_ctx->construct_context(msg))) {
          TRANS_LOG(WARN, "participant construct transaction context error", KR(ret), K(trans_id), K(msg));
        } else if (OB_FAIL(part_ctx->handle_message(msg))) {
          TRANS_LOG(WARN, "participant handle message error", KR(ret), K(trans_id), K(msg));
        } else {
          // do nothing
        }
      } else if (OB_FAIL(part_ctx->handle_message(msg))) {
        TRANS_LOG(WARN, "participant handle message error", KR(ret), K(trans_id), K(msg));
      } else {
        // do nothing
      }
      (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
    }
  }

  return ret;
}

int ObTransService::handle_participant_bounded_staleness_msg_(const ObTransMsg& msg, const bool alloc)
{
  int ret = OB_SUCCESS;
  if (msg.is_not_create_ctx_participant()) {
    int msg_status = OB_SUCCESS;
    int64_t snapshot_version = ObClockGenerator::getClock();
    const int msg_type = OB_TRANS_START_STMT_RESPONSE;
    ObTransMsg ret_msg;
    ObPartitionArray unreachable_partitions;
    if (OB_FAIL(generate_part_snapshot_for_bounded_staleness_read_(
            msg.get_receiver(), snapshot_version, unreachable_partitions))) {
      TRANS_LOG(WARN, "generate snapshot version failed", K(ret), K(msg), K(unreachable_partitions));
      msg_status = ret;
      // rewrite ret
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            SCHE_PARTITION_ID,
            msg.get_trans_param(),
            self_,
            msg.get_sql_no(),
            snapshot_version,
            msg_status,
            msg.get_request_id()))) {
      TRANS_LOG(WARN, "message init error", KR(ret));
    } else if (OB_FAIL(rpc_->post_trans_msg(msg.get_tenant_id(), msg.get_scheduler(), ret_msg, msg_type))) {
      TRANS_LOG(WARN, "post transaction message error", KR(ret));
    } else {
      // do nothing
    }
  } else {
    bool tmp_alloc = alloc;
    const ObPartitionKey& partition = msg.get_receiver();
    const ObTransID& trans_id = msg.get_trans_id();
    const bool for_replay = false;
    const bool is_readonly = true;
    const bool is_bounded_staleness_read = true;
    const bool need_completed_dirty_txn = false;
    ObTransCtx* ctx = NULL;
    ObSlaveTransCtx* slave_ctx = NULL;

    if (OB_FAIL(slave_part_trans_ctx_mgr_.get_trans_ctx(partition,
            trans_id,
            for_replay,
            is_readonly,
            is_bounded_staleness_read,
            need_completed_dirty_txn,
            tmp_alloc,
            ctx))) {
      TRANS_LOG(WARN,
          "get transaction context error",
          KR(ret),
          K(partition),
          K(trans_id),
          "msg_type",
          msg.get_msg_type(),
          "sender_addr",
          msg.get_sender_addr());
      if (need_handle_orphan_msg_(ret, msg)) {
        // rewrite ret
        if (OB_FAIL(handle_orphan_msg_(msg, ret))) {
          TRANS_LOG(WARN, "handle orphan message error", KR(ret), K(msg));
        }
      }
    } else {
      slave_ctx = static_cast<ObSlaveTransCtx*>(ctx);
      const uint64_t tenant_id = msg.get_tenant_id();
      const int64_t trans_expired_time = msg.get_trans_time();
      if (tmp_alloc) {
        if (OB_FAIL(slave_ctx->init(tenant_id,
                trans_id,
                trans_expired_time,
                partition,
                &slave_part_trans_ctx_mgr_,
                msg.get_trans_param(),
                msg.get_cluster_version(),
                this))) {
          TRANS_LOG(WARN, "context init error", KR(ret), K(partition), K(trans_id), K(msg));
          slave_ctx = NULL;
        } else if (OB_FAIL(slave_ctx->construct_context(msg))) {
          TRANS_LOG(WARN, "participant construct transaction context error", KR(ret), K(trans_id), K(msg));
        } else if (OB_FAIL(slave_ctx->handle_message(msg))) {
          TRANS_LOG(WARN, "participant handle message error", KR(ret), K(trans_id), K(msg));
        } else {
          // do nothing
        }
      } else if (OB_FAIL(slave_ctx->handle_message(msg))) {
        TRANS_LOG(WARN, "participant handle message error", KR(ret), K(trans_id), K(msg));
      } else {
        // do nothing
      }
      (void)slave_part_trans_ctx_mgr_.revert_trans_ctx(ctx);
    }
  }

  return ret;
}

int ObTransService::handle_trans_msg(const ObTransMsg& msg, ObTransRpcResult& result)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool alloc = false;
  const bool for_replay = false;
  const bool is_readonly = msg.get_trans_param().is_readonly();
  int64_t ctx_type = ObTransCtxType::UNKNOWN;
  const uint64_t tenant_id = msg.get_tenant_id();
  ObTransCtx* ctx = NULL;
  int64_t leader_epoch = 0;
  const bool is_bounded_staleness_read = msg.get_trans_param().is_bounded_staleness_read();
  int64_t commit_times = -1;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!msg.is_valid()) {
    TRANS_LOG(WARN, "invalid message", K(msg));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObPartitionKey& partition = msg.get_receiver();
    const ObTransID& trans_id = msg.get_trans_id();
    const int64_t trans_expired_time = msg.get_trans_time();
    const int64_t msg_type = msg.get_msg_type();
    // get transaction context
    switch (msg_type) {
      case OB_TRANS_START_STMT_RESPONSE:
      // go through
      case OB_TRANS_STMT_ROLLBACK_RESPONSE:
      // go through
      case OB_TRANS_SAVEPOINT_ROLLBACK_RESPONSE:
      // go through
      case OB_TRANS_COMMIT_RESPONSE:
      // go through
      case OB_TRANS_ABORT_RESPONSE:
      // go through
      case OB_TRANS_CLEAR_RESPONSE:
      // go through
      case OB_TRANS_XA_PREPARE_RESPONSE:
      // go through
      case OB_TRANS_XA_ROLLBACK_RESPONSE:
      // go through
      case OB_TRANS_ASK_SCHEDULER_STATUS_REQUEST: {
        alloc = false;
        ctx_type = ObTransCtxType::SCHEDULER;
        break;
      }
      // get transaction context for coordinator
      case OB_TRANS_XA_COMMIT_REQUEST:
      // go through
      case OB_TRANS_XA_ROLLBACK_REQUEST:
      // go through
      case OB_TRANS_XA_PREPARE_REQUEST:
      // go through
      case OB_TRANS_COMMIT_REQUEST:
      // go through
      case OB_TRANS_ABORT_REQUEST: {
        commit_times = msg.get_commit_times();
        alloc = true;
        ctx_type = ObTransCtxType::COORDINATOR;
        break;
      }
      case OB_TRANS_2PC_LOG_ID_RESPONSE:
      // go through
      case OB_TRANS_2PC_PREPARE_RESPONSE:
      // go through
      case OB_TRANS_2PC_PRE_COMMIT_RESPONSE:
      // go through
      case OB_TRANS_2PC_COMMIT_RESPONSE:
      // go through
      case OB_TRANS_2PC_COMMIT_CLEAR_RESPONSE:
      // go through
      case OB_TRANS_2PC_ABORT_RESPONSE:
      // go through
      case OB_TRANS_2PC_CLEAR_RESPONSE: {
        // equals to 0 when created by participant's response message
        commit_times = 0;
        alloc = true;
        ctx_type = ObTransCtxType::COORDINATOR;
        break;
      }
      case OB_TRANS_START_STMT_REQUEST: {
        alloc = msg.need_create_ctx() || is_readonly;
        ctx_type = ObTransCtxType::PARTICIPANT;
        break;
      }
      case OB_TRANS_STMT_ROLLBACK_REQUEST:
      // go through
      case OB_TRANS_SAVEPOINT_ROLLBACK_REQUEST:
      // go through
      case OB_TRANS_CLEAR_REQUEST:
      // go through
      case OB_TRANS_DISCARD_REQUEST:
      // go through
      case OB_TRANS_2PC_LOG_ID_REQUEST:
      // go through
      case OB_TRANS_2PC_PREPARE_REQUEST:
      // go through
      case OB_TRANS_2PC_PRE_COMMIT_REQUEST:
      // go through
      case OB_TRANS_2PC_COMMIT_REQUEST:
      // go through
      case OB_TRANS_2PC_COMMIT_CLEAR_REQUEST:
      // go through
      case OB_TRANS_2PC_ABORT_REQUEST:
      // go through
      case OB_TRANS_2PC_CLEAR_REQUEST:
      // go through
      case OB_TRANS_ASK_SCHEDULER_STATUS_RESPONSE: {
        alloc = false;
        ctx_type = ObTransCtxType::PARTICIPANT;
        break;
      }
      default:
        TRANS_LOG(WARN, "unknown message type", K(msg_type));
        ret = OB_UNKNOWN_PACKET;
        break;
    }

    if (ObTransCtxType::SCHEDULER == ctx_type) {
      ObScheTransCtx* sche_ctx = NULL;
      // check stmt rollback request is timeout and drop it
      if (OB_TRANS_STMT_ROLLBACK_RESPONSE == msg_type && ObTimeUtility::current_time() > msg.get_msg_timeout()) {
        TRANS_LOG(INFO, "current message timeout, discard it", K(msg));
      } else if (OB_FAIL(
                     sche_trans_ctx_mgr_.get_trans_ctx(SCHE_PARTITION_ID, trans_id, false, is_readonly, alloc, ctx))) {
        TRANS_LOG(WARN, "get transaction context error", KR(ret), K(trans_id), K(msg));
        ctx = NULL;
        if (OB_TRANS_ASK_SCHEDULER_STATUS_REQUEST == msg_type) {
          // for sp_trans, scheduler not exist, check part_ctx's status instead
          if (OB_SUCCESS != (tmp_ret = part_trans_ctx_mgr_.get_trans_ctx(
                                 msg.get_sender(), trans_id, for_replay, is_readonly, alloc, ctx))) {
            TRANS_LOG(WARN, "get transaction context error", KR(ret), K(trans_id), K(msg));
            // do nothing
          } else {
            if (ctx->is_sp_trans()) {
              TRANS_LOG(INFO, "sp trans", KP(ctx), K(msg));
              // rewrite ret
              ret = OB_SUCCESS;
            } else {
              TRANS_LOG(INFO, "not sp trans, need to double check scheduler context", KR(ret), K(msg));
            }
            (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
            if (OB_FAIL(ret)) {
              ctx = NULL;
              // race condition when sp-trans convert to dist-trans:
              // 1) check scheduler context exist->no;
              // 2) switch sp_trans to dist_trans, create scheduler context;
              // 3) check participant context not sp trans
              // solution: check session state, double check scheduler context is a temporary method
              if (OB_SUCCESS != (tmp_ret = sche_trans_ctx_mgr_.get_trans_ctx(
                                     SCHE_PARTITION_ID, trans_id, false, is_readonly, alloc, ctx))) {
                TRANS_LOG(WARN, "get transaction context error", KR(tmp_ret), K(trans_id), K(msg));
              } else {
                // rewrite retcode
                ret = OB_SUCCESS;
                (void)sche_trans_ctx_mgr_.revert_trans_ctx(ctx);
              }
            }
          }
          if (OB_FAIL(handle_trans_ask_scheduler_status_request_(msg, ret))) {
            TRANS_LOG(WARN, "handle ask scheduler status request error", K(ret), K(msg));
          }
        }
      } else {
        sche_ctx = static_cast<ObScheTransCtx*>(ctx);
        if (OB_TRANS_ASK_SCHEDULER_STATUS_REQUEST == msg_type) {
          if (OB_FAIL(handle_trans_ask_scheduler_status_request_(msg, ret))) {
            TRANS_LOG(WARN, "handle ask scheduler status request error", KR(ret), K(msg), K(sche_ctx));
          }
        } else if (OB_FAIL(sche_ctx->handle_message(msg))) {
          TRANS_LOG(WARN, "scheduler handle message error", KR(ret), K(msg));
        }
        (void)sche_trans_ctx_mgr_.revert_trans_ctx(ctx);
      }
    } else if (ObTransCtxType::COORDINATOR == ctx_type) {
      const bool check_election = true;
      ObCoordTransCtx* coord_ctx = NULL;
      int clog_status = OB_SUCCESS;
      ObTsWindows changing_leader_windows;
      // coordinator does not check in_changing_leader_windows
      if (!is_bounded_staleness_read) {
        if (OB_FAIL(clog_adapter_->get_status(
                partition, check_election, clog_status, leader_epoch, changing_leader_windows))) {
          TRANS_LOG(WARN, "get participant status error", KR(ret), K(partition), K(trans_id));
        } else if (OB_SUCCESS != clog_status) {
          ret = clog_status;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(coord_trans_ctx_mgr_.get_trans_ctx(partition, trans_id, false, is_readonly, alloc, ctx))) {
          TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_id), K(msg));
          ctx = NULL;
        } else if (OB_ISNULL(ctx)) {
          TRANS_LOG(WARN, "context is null", K(partition), K(trans_id), K(msg));
          ret = OB_ERR_UNEXPECTED;
        } else {
          coord_ctx = static_cast<ObCoordTransCtx*>(ctx);
          if (alloc) {
            if (OB_FAIL(coord_ctx->init(tenant_id,
                    trans_id,
                    trans_expired_time,
                    partition,
                    &coord_trans_ctx_mgr_,
                    msg.get_trans_param(),
                    GET_MIN_CLUSTER_VERSION(),
                    this,
                    commit_times))) {
              TRANS_LOG(
                  WARN, "transaction context init error", KR(ret), K(partition), K(trans_id), K(msg), K(commit_times));
              coord_ctx = NULL;
            } else if (OB_FAIL(coord_ctx->construct_context(msg))) {
              if (OB_TRANS_XA_COMMIT_REQUEST == msg_type || OB_TRANS_XA_ROLLBACK_REQUEST == msg_type) {
                // scheduler create coordinator fail, need to wait coordinator
                // created by participant's request, and scheduler will retry commit or rollback
                // this is because of scheduler don't known participants info
                TRANS_LOG(INFO, "coordinator construct transaction context failed", KR(ret), K(trans_id), K(msg));
              } else {
                TRANS_LOG(WARN, "coordinator construct transaction context error", KR(ret), K(trans_id), K(msg));
              }
              coord_ctx = NULL;
            } else if (OB_FAIL(coord_ctx->handle_message(msg))) {
              TRANS_LOG(WARN, "coordinator handle message error", KR(ret), K(trans_id), K(msg));
              // coodrinator don't reponse to scheduler when handle message fail
              ret = OB_SUCCESS;
            } else {
              // do nothing
            }
          } else if (OB_FAIL(coord_ctx->handle_message(msg))) {
            TRANS_LOG(WARN, "coordinator handle message error", KR(ret), K(trans_id), K(msg));
            // coodrinator don't reponse to scheduler when handle message fail
            ret = OB_SUCCESS;
          } else {
            // do nothing
          }
          (void)coord_trans_ctx_mgr_.revert_trans_ctx(ctx);
        }
      }
    } else if (ObTransCtxType::PARTICIPANT == ctx_type) {
      // if message is stale, drop it
      if (OB_TRANS_STMT_ROLLBACK_REQUEST == msg_type && ObTimeUtility::current_time() > msg.get_msg_timeout()) {
        TRANS_LOG(INFO, "current message timeout, discard it", K(msg));
      } else if (is_bounded_staleness_read) {
        if (OB_FAIL(handle_participant_bounded_staleness_msg_(msg, alloc))) {
          TRANS_LOG(WARN, "handle participant bounded_staleness msg error", KR(ret), K(msg));
        }
      } else if (0 == msg.get_batch_same_leader_partitions().count()) {
        if (OB_FAIL(handle_participant_msg_(msg, msg.get_receiver(), alloc))) {
          TRANS_LOG(WARN, "handle participant msg error", K(ret), K(msg));
        }
      } else {
        const ObPartitionArray& tmp_arr = msg.get_batch_same_leader_partitions();
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp_arr.count(); i++) {
          if (OB_FAIL(handle_participant_msg_(msg, tmp_arr.at(i), alloc))) {
            TRANS_LOG(WARN, "handle participant msg error", K(ret), K(msg));
          }
        }
      }
    } else {
      TRANS_LOG(WARN, "invalid transaction context type", K(ctx_type));
      ret = OB_ERR_UNEXPECTED;
    }

    // response error message
    if (OB_FAIL(ret)) {
      ObTransMsg resp_msg;
      if (OB_SUCCESS != (tmp_ret = resp_msg.init(OB_TRANS_ERROR_MSG,
                             msg.get_msg_type(),
                             msg.get_tenant_id(),
                             msg.get_trans_id(),
                             msg.get_receiver(),
                             self_,
                             ret,
                             msg.get_sql_no(),
                             msg.get_request_id()))) {
        TRANS_LOG(WARN, "response message init error", "ret", tmp_ret, "status", ret, K(msg));
      } else if (OB_SUCCESS != (tmp_ret = resp_msg.set_msg_timeout(msg.get_msg_timeout()))) {
        TRANS_LOG(WARN, "error msg set msg start ts error", K(tmp_ret), K(msg));
      } else if (OB_SUCCESS !=
                 (tmp_ret = post_trans_error_response_(msg.get_tenant_id(), resp_msg, msg.get_sender_addr()))) {
        TRANS_LOG(WARN, "post error response error", "ret", tmp_ret, K(resp_msg), "sender", msg.get_sender_addr());
      } else {
        // do nothing
      }
    }
  }
  result.reset();
  result.init(ret, msg.get_timestamp());

  return ret;
}

bool ObTransService::need_handle_orphan_msg_(const int retcode, const ObTransMsg& msg)
{
  bool is_need = false;  // whether need to handle as orphan msg
  const ObPartitionKey& partition = msg.get_receiver();
  const ObTransLocationCache& trans_location_cache = msg.get_trans_location_cache();
  const bool is_bounded_staleness_read = msg.get_trans_param().is_bounded_staleness_read();

  if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
  } else if (OB_TRANS_CTX_NOT_EXIST == retcode) {
    is_need = true;
    if (!is_bounded_staleness_read && OB_TRANS_STMT_ROLLBACK_REQUEST == msg.get_msg_type() && msg.need_create_ctx() &&
        0 != msg.get_cluster_version()) {
      is_need = false;
    }
  } else if (OB_TRANS_KILLED == retcode || OB_TRANS_IS_EXITING == retcode) {
    is_need = true;
  } else if (OB_TRANS_STMT_ROLLBACK_REQUEST == msg.get_msg_type() &&
             (OB_PARTITION_NOT_EXIST == retcode || OB_PARTITION_IS_BLOCKED == retcode ||
                 OB_PARTITION_IS_STOPPED == retcode)) {
    // scheduler's location cache contains this partition,
    // its ok to response rollback success
    for (int64_t i = 0; i < trans_location_cache.count(); ++i) {
      if (trans_location_cache.at(i).get_partition() == partition && trans_location_cache.at(i).get_addr() == self_) {
        is_need = true;
      }
    }
  } else {
    // do nothing
  }

  return is_need;
}

int ObTransService::post_trans_error_response_(const uint64_t tenant_id, ObTransMsg& msg, const ObAddr& server)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(tenant_id) || !msg.is_valid() || !server.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(tenant_id), K(msg), K(server));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(rpc_->post_trans_resp_msg(tenant_id, server, msg))) {
    TRANS_LOG(WARN, "post trans response message error", KR(ret), "sender", server, K(msg));
  }

  return ret;
}

int ObTransService::handle_trans_msg_callback(const ObPartitionKey& partition, const ObTransID& trans_id,
    const int64_t msg_type, const int status, const ObAddr& addr, const int64_t sql_no, const int64_t request_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid() || !trans_id.is_valid() || !ObTransMsgTypeChecker::is_valid_msg_type(msg_type) ||
             !addr.is_valid()) {
    TRANS_LOG(
        WARN, "invalid argument", K(partition), K(trans_id), K(msg_type), K(status), K(addr), K(sql_no), K(request_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_TRANS_START_STMT_REQUEST == msg_type || OB_TRANS_STMT_ROLLBACK_REQUEST == msg_type ||
             OB_TRANS_CLEAR_REQUEST == msg_type) {
    bool alloc = false;
    const bool for_replay = false;
    const bool is_readonly = false;
    ObTransCtx* ctx = NULL;
    ObScheTransCtx* sche_ctx = NULL;
    if (OB_FAIL(sche_trans_ctx_mgr_.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
      TRANS_LOG(WARN, "get transaction context error", KR(ret), K(trans_id));
      ctx = NULL;
    } else if (OB_ISNULL(ctx)) {
      TRANS_LOG(WARN, "context is null", KP(ctx), K(trans_id), K(msg_type));
      ret = OB_ERR_UNEXPECTED;
    } else {
      sche_ctx = static_cast<ObScheTransCtx*>(ctx);
      if (OB_FAIL(sche_ctx->handle_trans_msg_callback(partition, msg_type, status, addr, sql_no, request_id))) {
        TRANS_LOG(WARN,
            "handle transaction message callback error",
            KR(ret),
            K(partition),
            K(trans_id),
            K(msg_type),
            K(status),
            K(addr),
            K(sql_no));
      } else {
        TRANS_LOG(DEBUG,
            "handle transaction message callback success",
            K(partition),
            K(trans_id),
            K(msg_type),
            K(status),
            K(sql_no));
      }
      (void)sche_trans_ctx_mgr_.revert_trans_ctx(ctx);
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObTransService::handle_trx_req(int type, ObPartitionKey& pkey, const char* buf, int32_t size)
{
  UNUSED(pkey);
  int ret = OB_SUCCESS;
  if (type < OB_TRX_NEW_MSG_TYPE_BASE) {
    int64_t pos = 0;
    ObTransMsg msg;
    ObTransRpcResult result;
    if (OB_FAIL(msg.deserialize(buf, size, pos))) {
      TRANS_LOG(WARN, "deserialize msg failed", K(ret));
    } else {
      ret = handle_trans_msg(msg, result);
    }
  } else {
    ret = handle_batch_msg_(type, buf, size);
  }

  return ret;
}

int ObTransService::handle_trans_response(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!msg.is_valid()) {
    TRANS_LOG(WARN, "invalid message", K(msg));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int status = msg.get_status();
    const ObPartitionKey& sender = msg.get_sender();
    const ObTransID& trans_id = msg.get_trans_id();
    const int64_t err_msg_type = msg.get_err_msg_type();
    const ObAddr& sender_addr = msg.get_sender_addr();
    const int64_t sql_no = msg.get_sql_no();
    const int64_t request_id = msg.get_request_id();

    if (OB_SUCCESS != status) {
      // if partition leader shifted or partition migrated, refresh location cache
      // and retry operation
      if (OB_PARTITION_NOT_EXIST == status || OB_NOT_MASTER == status || OB_PARTITION_IS_STOPPED == status ||
          OB_PARTITION_IS_BLOCKED == status || OB_TRANS_STMT_NEED_RETRY == status) {
        const int64_t expire_renew_time = 0;
        if (OB_FAIL(location_adapter_->nonblock_renew(sender, expire_renew_time))) {
          TRANS_LOG(WARN, "refresh location cache error", KR(ret), "partition", sender, K(expire_renew_time));
        } else {
          if (EXECUTE_COUNT_PER_SEC(16)) {
            TRANS_LOG(INFO, "refresh location cache success", "partition", sender, K(lbt()));
          }
        }

        if (OB_TRANS_START_STMT_REQUEST == err_msg_type || OB_TRANS_STMT_ROLLBACK_REQUEST == err_msg_type ||
            OB_TRANS_COMMIT_REQUEST == err_msg_type || OB_TRANS_ABORT_REQUEST == err_msg_type ||
            OB_TRANS_CLEAR_REQUEST == err_msg_type || OB_TRANS_SAVEPOINT_ROLLBACK_REQUEST == err_msg_type) {
          if (OB_SUCCESS != (tmp_ret = handle_trans_err_response_(
                                 err_msg_type, trans_id, sender, sender_addr, status, sql_no, request_id))) {
            TRANS_LOG(WARN, "handle error reseponse msg error", K(msg));
            ret = tmp_ret;
          }
        }
      }
    }
    if (EXECUTE_COUNT_PER_SEC(16)) {
      TRANS_LOG(INFO, "handle transaction response msg success", KR(ret), K(msg));
    }
  }

  return ret;
}

int ObTransService::refresh_location_cache(const ObPartitionKey& partition, const bool need_clear_cache)
{
  UNUSED(need_clear_cache);
  int ret = OB_SUCCESS;

  // expire_renew_time > 0: clear cache
  // expire_renew_time = 0: refresh but don't clear cache
  // here we don't clear, it can prevent refreshed result was cleared by other thread
  const int64_t expire_renew_time = 0;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(location_adapter_->nonblock_renew(partition, expire_renew_time))) {
    TRANS_LOG(WARN, "refresh location cache error", KR(ret), K(partition), K(expire_renew_time));
  } else {
    if (EXECUTE_COUNT_PER_SEC(16)) {
      TRANS_LOG(INFO, "refresh location cache success", K(partition), K(lbt()));
    }
  }

  return ret;
}

int ObTransService::leader_revoke(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(coord_trans_ctx_mgr_.leader_revoke(partition))) {
    TRANS_LOG(ERROR, "coordinator partition leader revoke error", KR(ret), K(partition));
  } else if (OB_FAIL(part_trans_ctx_mgr_.leader_revoke(partition))) {
    if (OB_NOT_RUNNING != ret) {
      TRANS_LOG(ERROR, "participant partition leader revoke error", KR(ret), K(partition));
    } else {
      TRANS_LOG(WARN, "participant partition leader revoke failed", KR(ret), K(partition));
    }
  } else {
    TRANS_LOG(INFO, "leader revoke success", K(partition));
  }

  return ret;
}

int ObTransService::leader_takeover(
    const ObPartitionKey& partition, const LeaderActiveArg& arg, const int64_t checkpoint)
{
  UNUSED(arg);
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
    // } else if (OB_FAIL(part_trans_ctx_mgr_.clear_trans_after_restore(partition))) {
    //   TRANS_LOG(WARN, "clear trans after restore log error", K(ret), K(partition));
  } else if (OB_FAIL(coord_trans_ctx_mgr_.leader_takeover(partition, checkpoint))) {
    TRANS_LOG(ERROR, "coordinator partition leader takeover error", KR(ret), K(partition));
  } else if (OB_FAIL(part_trans_ctx_mgr_.leader_takeover(partition, checkpoint))) {
    TRANS_LOG(ERROR, "participant partition leader takeover error", KR(ret), K(partition));
  } else if (OB_FAIL(light_trans_ctx_mgr_.leader_takeover(partition))) {
    TRANS_LOG(ERROR, "light trans ctx manager leader takeover error", KR(ret), K(partition));
  } else if (OB_FAIL(update_publish_version(partition, ObClockGenerator::getRealClock(), true))) {
    TRANS_LOG(ERROR, "update publish version error", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "leader takeover success", K(partition), K(arg));
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(ERROR, "leader takeover error", KR(ret), K(partition), K(arg));
  }

  return ret;
}

int ObTransService::leader_active(const ObPartitionKey& partition, const LeaderActiveArg& arg)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "obtransservice is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(coord_trans_ctx_mgr_.leader_active(partition, arg))) {
    TRANS_LOG(ERROR, "coordinator partition leader active error", KR(ret), K(partition));
  } else if (OB_FAIL(part_trans_ctx_mgr_.leader_active(partition, arg))) {
    TRANS_LOG(ERROR, "participant partition leader active error", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "leader active success", K(partition), K(arg));
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_TRANS_LEADER_ACTIVE) OB_SUCCESS;
  }
#endif
  if (OB_FAIL(ret)) {
    TRANS_LOG(ERROR, "leader active error", KR(ret), K(partition), K(arg));
  }

  return ret;
}

// during partition creation, transaction-layer only concern major version(bug:11194272)
int ObTransService::add_partition(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  int64_t unused_gts = 0;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(coord_trans_ctx_mgr_.add_partition(partition))) {
    TRANS_LOG(WARN, "coordinator add partition error", KR(ret), K(partition));
  } else if (OB_FAIL(part_trans_ctx_mgr_.add_partition(partition))) {
    (void)coord_trans_ctx_mgr_.remove_partition(partition, false);
    TRANS_LOG(WARN, "participant add partition error", KR(ret), K(partition));
    (void)part_trans_ctx_mgr_.remove_partition(partition, false);
    TRANS_LOG(WARN, "sp participant add partition error", KR(ret), K(partition));
  } else if (OB_FAIL(slave_part_trans_ctx_mgr_.add_partition(partition))) {
    (void)coord_trans_ctx_mgr_.remove_partition(partition, false);
    TRANS_LOG(WARN, "participant add partition error", KR(ret), K(partition));
    (void)part_trans_ctx_mgr_.remove_partition(partition, false);
    TRANS_LOG(WARN, "sp participant add partition error", KR(ret), K(partition));
    (void)slave_part_trans_ctx_mgr_.remove_partition(partition, false);
    TRANS_LOG(WARN, "slave participant add partition error", KR(ret), K(partition));
  } else {
    // trigger gts tenant creation
    (void)OB_TS_MGR.get_gts(partition.get_tenant_id(), NULL, unused_gts);
    TRANS_LOG(INFO, "add partition success", K(partition));
  }

  return ret;
}

int ObTransService::remove_partition(const ObPartitionKey& partition, const bool graceful)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObWaitEventGuard wait_guard(ObWaitEventIds::REMOVE_PARTITION_WAIT,
        0,
        partition.get_tenant_id(),
        partition.get_table_id(),
        partition.get_partition_id());
    if (OB_FAIL(slave_part_trans_ctx_mgr_.remove_partition(partition, graceful))) {
      if (OB_ENTRY_NOT_EXIST != ret && OB_PARTITION_NOT_EXIST != ret) {
        TRANS_LOG(WARN, "slave participant remove partition error", KR(ret), K(partition), K(graceful));
      } else {
        TRANS_LOG(INFO, "slave participant remove partition error", KR(ret), K(partition), K(graceful));
      }
    } else if (OB_FAIL(light_trans_ctx_mgr_.remove_partition(partition))) {
      TRANS_LOG(WARN, "remove partition from lightweight trans ctx manager error", KR(ret), K(partition), K(graceful));
    } else if (OB_FAIL(part_trans_ctx_mgr_.remove_partition(partition, graceful))) {
      TRANS_LOG(WARN, "participant remove partition error", KR(ret), K(partition), K(graceful));
    } else if (OB_FAIL(coord_trans_ctx_mgr_.remove_partition(partition, graceful))) {
      TRANS_LOG(WARN, "coordinator remove partition error", KR(ret), K(partition), K(graceful));
    } else if (OB_FAIL(dup_table_lease_task_map_.del(partition))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        TRANS_LOG(INFO, "remove partition success", K(partition), K(graceful));
      } else {
        TRANS_LOG(WARN, "erase lease task from hashmap error", KR(ret), K(partition));
      }
    } else {
      TRANS_LOG(INFO, "remove partition success", K(partition), K(graceful));
    }
  }

  return ret;
}

int ObTransService::block_partition(const ObPartitionKey& partition, bool& is_all_trans_clear)
{
  int ret = OB_SUCCESS;
  bool is_slave_part_clear = false;
  bool is_part_clear = false;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(slave_part_trans_ctx_mgr_.block_partition(partition, is_slave_part_clear))) {
    TRANS_LOG(WARN, "slave participant block partition error", KR(ret), K(partition));
  } else if (OB_FAIL(part_trans_ctx_mgr_.block_partition(partition, is_part_clear))) {
    TRANS_LOG(WARN, "participant block partition error", KR(ret), K(partition));
  } else {
    is_all_trans_clear = (is_slave_part_clear && is_part_clear);
  }
  if (OB_SUCCESS != ret) {
    TRANS_LOG(WARN, "block partition failed", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "block partition success", K(partition), K(is_slave_part_clear), K(is_part_clear));
  }

  return ret;
}

int ObTransService::kill_all_trans(const ObPartitionKey& partition, const KillTransArg& arg, bool& is_all_trans_clear)
{
  int ret = OB_SUCCESS;
  bool is_slave_part_clear = false;
  bool is_part_clear = false;
  bool is_coord_part_clear = false;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(slave_part_trans_ctx_mgr_.kill_all_trans(partition, arg, is_slave_part_clear))) {
    TRANS_LOG(WARN, "slave participant kill all trans failed", KR(ret), K(partition));
  } else if (OB_FAIL(part_trans_ctx_mgr_.kill_all_trans(partition, arg, is_part_clear))) {
    TRANS_LOG(WARN, "participant kill all trans failed", KR(ret), K(partition));
  } else if (OB_FAIL(coord_trans_ctx_mgr_.kill_all_trans(partition, arg, is_coord_part_clear))) {
    TRANS_LOG(WARN, "coordinator kill all trans failed", KR(ret), K(partition));
  } else {
    is_all_trans_clear = (is_slave_part_clear && is_part_clear && is_coord_part_clear);
  }
  if (OB_SUCCESS != ret) {
    TRANS_LOG(WARN, "kill all trans failed", KR(ret), K(partition), K(arg));
  } else {
    TRANS_LOG(INFO, "kill all trans success", K(partition), K(arg));
  }

  return ret;
}

int ObTransService::calculate_trans_cost(const ObTransID& tid, uint64_t& cost)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(part_trans_ctx_mgr_.calculate_trans_cost(tid, cost))) {
    TRANS_LOG(WARN, "part_trans_ctx_mgr_ calculate trans cost error", KR(ret), K(tid));
  }

  return ret;
}

/**
 * note:
 * 1. for leader, called in logic split, target partition can not been written before split complete,
 *    and active transaction will not increase
 * 2. for follower, called in log replay, checkpoint routine will be called before replay, this promise
 *    1PC transaction already terminated before start to replay
 */
int ObTransService::wait_1pc_trx_end(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(slave_part_trans_ctx_mgr_.wait_1pc_trx_end(partition))) {
    TRANS_LOG(WARN, "slave participant wait 1pc trx end failed", KR(ret), K(partition));
  } else if (OB_FAIL(part_trans_ctx_mgr_.wait_1pc_trx_end(partition))) {
    TRANS_LOG(WARN, "participant wait 1pc trx end failed", KR(ret), K(partition));
  } else if (OB_FAIL(coord_trans_ctx_mgr_.wait_1pc_trx_end(partition))) {
    TRANS_LOG(WARN, "coordinator wait 1pc trx end failed", KR(ret), K(partition));
  } else {
  }
  if (OB_SUCCESS != ret) {
    TRANS_LOG(WARN, "wait all 1pc trx end failed", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "wait all 1pc trx end success", K(partition));
  }

  return ret;
}

int ObTransService::wait_all_trans_clear(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(slave_part_trans_ctx_mgr_.wait_all_trans_clear(partition))) {
    TRANS_LOG(WARN, "slave participant wait all trans clear failed", KR(ret), K(partition));
  } else if (OB_FAIL(part_trans_ctx_mgr_.wait_all_trans_clear(partition))) {
    TRANS_LOG(WARN, "participant wait all trans clear failed", KR(ret), K(partition));
  } else if (OB_FAIL(coord_trans_ctx_mgr_.wait_all_trans_clear(partition))) {
    TRANS_LOG(WARN, "coordinator wait all trans clear failed", KR(ret), K(partition));
  } else {
  }
  if (OB_SUCCESS != ret) {
    TRANS_LOG(WARN, "wait all trans clear failed", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "wait all trans clear success", K(partition));
  }

  return ret;
}

int ObTransService::check_all_trans_in_trans_table_state(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(slave_part_trans_ctx_mgr_.wait_all_trans_clear(partition))) {
    TRANS_LOG(WARN, "slave participant wait all trans clear failed", KR(ret), K(partition));
  } else if (OB_FAIL(part_trans_ctx_mgr_.check_all_trans_in_trans_table_state(partition))) {
    TRANS_LOG(WARN, "participant check all trans in trans table stat failed", KR(ret), K(partition));
  } else if (OB_FAIL(coord_trans_ctx_mgr_.wait_all_trans_clear(partition))) {
    TRANS_LOG(WARN, "coordinator wait all trans clear failed", KR(ret), K(partition));
  } else {
  }
  if (OB_SUCCESS != ret) {
    TRANS_LOG(WARN, "check all trans in trans table stat failed", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "check all trans in trans table stat success", K(partition));
  }

  return ret;
}

int ObTransService::mark_dirty_trans(const ObPartitionKey& pkey, const ObMemtable* const frozen_memtable,
    const ObMemtable* const active_memtable, int64_t& cb_cnt, int64_t& applied_log_ts)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(part_trans_ctx_mgr_.mark_dirty_trans(
                 pkey, frozen_memtable, active_memtable, cb_cnt, applied_log_ts))) {
    TRANS_LOG(WARN, "mark dirty trans failed", K(ret), K(pkey), KP(frozen_memtable), KP(active_memtable));
  } else {
    TRANS_LOG(INFO, "mark dirty trans success", K(pkey), K(cb_cnt));
  }

  return ret;
}

int ObTransService::get_applied_log_ts(const ObPartitionKey& pkey, int64_t& applied_log_ts)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(part_trans_ctx_mgr_.get_applied_log_ts(pkey, applied_log_ts))) {
    TRANS_LOG(WARN, "fail to get applied log id", K(ret), K(pkey));
  }

  return ret;
}

int ObTransService::init_memtable_ctx_(ObMemtableCtx* mem_ctx, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  MemtableIDMap& id_map = mt_ctx_factory_.get_id_map();
  ObIAllocator& malloc_allocator = mt_ctx_factory_.get_malloc_allocator();
  if (OB_FAIL(mem_ctx->init(tenant_id, id_map, malloc_allocator))) {
    if (OB_INIT_TWICE != ret) {
      TRANS_LOG(WARN, "memtable context init error", KR(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    uint32_t ctx_descriptor = IDMAP_INVALID_ID;
    if (OB_FAIL(id_map.assign(mem_ctx, ctx_descriptor))) {
      TRANS_LOG(WARN, "id map assign fail", KR(ret));
    } else if (0 == ctx_descriptor) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected ctx descriptor", K(ctx_descriptor), "context", *mem_ctx);
    } else {
      mem_ctx->set_ctx_descriptor(ctx_descriptor);
    }
  }
  return ret;
}

int ObTransService::alloc_memtable_ctx_(
    const ObPartitionKey& pkey, const bool is_fast_select, const uint64_t tenant_id, ObMemtableCtx*& ctx)
{
  int ret = OB_SUCCESS;

  ObMemtableCtx* memtable_ctx = NULL;
  if (is_fast_select) {
    memtable_ctx = static_cast<ObMemtableCtx*>(mt_ctx_factory_.alloc(tenant_id));
    if (NULL != memtable_ctx) {
      memtable_ctx->set_self_alloc_ctx(true);
    }
  } else {
    memtable_ctx = alloc_tc_memtable_ctx_();
    if (NULL != memtable_ctx) {
      if (memtable_ctx->get_thread_local_ctx()->state_ == ObThreadLocalTransCtxState::OB_THREAD_LOCAL_CTX_RUNNING) {
        memtable_ctx = static_cast<ObMemtableCtx*>(mt_ctx_factory_.alloc(tenant_id));
        if (NULL != memtable_ctx) {
          memtable_ctx->set_self_alloc_ctx(true);
        }
      } else {
        memtable_ctx->get_thread_local_ctx()->state_ = ObThreadLocalTransCtxState::OB_THREAD_LOCAL_CTX_RUNNING;
      }
    }
  }
  if (NULL != memtable_ctx) {
    bool is_valid = false;
    if (OB_FAIL(memtable_ctx->get_trans_table_guard()->check_trans_table_valid(pkey, is_valid))) {
      TRANS_LOG(INFO, "check trans table valid error", K(ret), K(pkey));
      // presume non reference to transaction state table here
    } else if (is_valid) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected trans table", K(ret), K(pkey));
    } else if (OB_FAIL(part_trans_ctx_mgr_.get_partition_trans_ctx_mgr_with_ref(
                   pkey, *(memtable_ctx->get_trans_table_guard())))) {
      TRANS_LOG(WARN, "get trans part ctx mgr ref error", K(ret), K(pkey), KP(memtable_ctx));
    } else {
      ctx = memtable_ctx;
    }
  } else {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "allocate memory failed", K(ret), K(pkey), K(is_fast_select));
  }
  return ret;
}

void ObTransService::release_memtable_ctx_(const ObPartitionKey& pkey, ObMemtableCtx* mt_ctx)
{
  int ret = OB_SUCCESS;

  if (mt_ctx->is_self_alloc_ctx()) {
    mt_ctx_factory_.free(mt_ctx);
  } else {
    mt_ctx->get_thread_local_ctx()->state_ = ObThreadLocalTransCtxState::OB_THREAD_LOCAL_CTX_READY;
    bool is_valid = true;
    if (OB_FAIL(mt_ctx->get_trans_table_guard()->check_trans_table_valid(pkey, is_valid))) {
      TRANS_LOG(ERROR, "check trans table valid error", K(ret), K(pkey));
    } else if (is_valid) {
      // revert ref
      mt_ctx->get_trans_table_guard()->reset();
    } else {
      TRANS_LOG(INFO, "invalid trans table, maybe alloc memtable ctx failed", K(pkey));
    }
  }
  UNUSED(ret);
}

ObMemtableCtx* ObTransService::alloc_tc_memtable_ctx_()
{
  ObMemtableCtx* memtable_ctx = NULL;
  ObThreadLocalTransCtx* thread_local_ctx = GET_TSI_MULT(ObThreadLocalTransCtx, 0);
  if (NULL != thread_local_ctx) {
    thread_local_ctx->memtable_ctx_.set_thread_local_ctx(thread_local_ctx);
    memtable_ctx = &thread_local_ctx->memtable_ctx_;
  }
  return memtable_ctx;
}

int ObTransService::get_store_ctx(const ObTransDesc& trans_desc, const ObPartitionKey& pg_key, ObStoreCtx& store_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t user_specified_snapshot = -1;
  ObMemtableCtx* mt_ctx = NULL;
  const ObTransID& trans_id = trans_desc.get_trans_id();
  const bool is_bounded_staleness_read = trans_desc.is_bounded_staleness_read();
  const int64_t trx_lock_timeout = trans_desc.get_cur_stmt_desc().trx_lock_timeout_;
  const ObStandaloneStmtDesc& desc = trans_desc.get_standalone_stmt_desc();
  const bool is_weak_read = (is_bounded_staleness_read || desc.is_bounded_staleness_read());

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!is_weak_read  // check it is standby cluster for strong-consistency read
             && !ObMultiClusterUtil::is_cluster_allow_strong_consistency_read_write(pg_key.get_table_id())) {
    ret = OB_STANDBY_WEAK_READ_ONLY;
  } else if (!trans_desc.is_all_select_stmt() && !ObMultiClusterUtil::is_cluster_private_table(pg_key.get_table_id()) &&
             GCTX.is_in_disabled_state()) {
    // in cluster disabled state, cannot start non-readonly trx
    ret = OB_ERR_DB_READ_ONLY;
  } else {
    if (desc.is_valid()) {
      if (OB_FAIL(get_store_ctx_(desc, pg_key, user_specified_snapshot, store_ctx))) {
        TRANS_LOG(WARN, "get store ctx failed", K(ret), K(desc), K(pg_key));
      }
    } else if (!trans_desc.is_valid() || !pg_key.is_valid()) {
      TRANS_LOG(WARN, "invalid argument", K(trans_desc), K(pg_key));
      ret = OB_INVALID_ARGUMENT;
    } else if (trans_desc.is_trans_timeout()) {
      TRANS_LOG(WARN, "transaction is timeout", K(trans_desc), K(pg_key));
      ret = OB_TRANS_TIMEOUT;
    } else if (trans_desc.is_stmt_timeout()) {
      TRANS_LOG(WARN, "statement is timeout", K(trans_desc), K(pg_key));
      ret = OB_TRANS_STMT_TIMEOUT;
    } else {
      if (trans_desc.is_fast_select() || trans_desc.is_not_create_ctx_participant(pg_key, user_specified_snapshot)) {
        int64_t part_snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;
        store_ctx.trans_id_ = trans_desc.get_trans_id();
        if (OB_FAIL(handle_snapshot_for_read_only_participant_(
                trans_desc, pg_key, user_specified_snapshot, part_snapshot_version))) {
          TRANS_LOG(WARN,
              "handle snapshot for read only participant fail",
              KR(ret),
              K(trans_desc),
              K(pg_key),
              K(user_specified_snapshot));
        } else if (OB_FAIL(alloc_memtable_ctx_(pg_key, trans_desc.is_fast_select(), pg_key.get_tenant_id(), mt_ctx))) {
          TRANS_LOG(WARN, "allocate memory failed", K(ret), K(pg_key));
        } else if (!mt_ctx->is_self_alloc_ctx() && OB_FAIL(init_memtable_ctx_(mt_ctx, pg_key.get_tenant_id()))) {
          TRANS_LOG(WARN, "init mem ctx failed", K(ret), K(pg_key));
          release_memtable_ctx_(pg_key, mt_ctx);
        } else if (OB_FAIL(mt_ctx->trans_begin())) {
          TRANS_LOG(WARN, "trans begin failed", K(ret));
          release_memtable_ctx_(pg_key, mt_ctx);
        } else if (!trans_desc.is_fast_select() &&
                   OB_FAIL(mt_ctx->sub_trans_begin(
                       part_snapshot_version, trans_desc.get_cur_stmt_expired_time(), false, trx_lock_timeout))) {
          TRANS_LOG(WARN, "sub trans begin failed", K(ret));
          release_memtable_ctx_(pg_key, mt_ctx);
        } else if (trans_desc.is_fast_select() &&
                   OB_FAIL(mt_ctx->fast_select_trans_begin(
                       part_snapshot_version, trans_desc.get_cur_stmt_expired_time(), trx_lock_timeout))) {
          TRANS_LOG(WARN, "fast select trans begin failed", K(ret));
          release_memtable_ctx_(pg_key, mt_ctx);
        } else if (OB_FAIL(mt_ctx->get_trans_table_guard()
                               ->get_trans_state_table()
                               .get_partition_trans_ctx_mgr()
                               ->check_and_inc_read_only_trx_count())) {
          TRANS_LOG(WARN, "check and inc read only trx count error", K(ret), K(pg_key));
          release_memtable_ctx_(pg_key, mt_ctx);
        } else {
          bool is_user_specified_snapshot_valid = false;
          mt_ctx->is_standalone_ = true;
          mt_ctx->set_read_only();
          if (trans_desc.is_fast_select() || trans_desc.is_nested_stmt()) {
            store_ctx.snapshot_info_ = trans_desc.get_stmt_snapshot_info();
          }
          store_ctx.mem_ctx_ = mt_ctx;
          store_ctx.trans_table_guard_ = mt_ctx->get_trans_table_guard();
          store_ctx.cur_pkey_ = pg_key;
          store_ctx.trans_id_ = trans_desc.get_trans_id();
          store_ctx.sql_no_ = trans_desc.get_sql_no();
          store_ctx.stmt_min_sql_no_ = trans_desc.get_stmt_min_sql_no();

          if (OB_FAIL(check_user_specified_snapshot_version(
                  trans_desc, user_specified_snapshot, is_user_specified_snapshot_valid))) {
            TRANS_LOG(WARN,
                "failed to check user specified snapshot versions",
                K(ret),
                K(trans_desc),
                K(user_specified_snapshot));
          } else if (is_user_specified_snapshot_valid) {
            store_ctx.snapshot_info_.set_snapshot_version(user_specified_snapshot);
          }
        }
      } else {  // part ctx exist
        store_ctx.trans_id_ = trans_desc.get_trans_id();
        bool is_sp_trans = false;
        if (trans_desc.is_mini_sp_trans() || trans_desc.is_sp_trans()) {
          ObTransCtx* ctx = NULL;
          is_sp_trans = true;
          if (OB_ISNULL(ctx = const_cast<ObTransDesc&>(trans_desc).get_part_ctx())) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "ctx is null, unexpected error", KR(ret), K(trans_desc), KP(ctx));
          } else if (is_bounded_staleness_read) {
            mt_ctx = static_cast<ObSlaveTransCtx*>(ctx)->get_memtable_ctx();
          } else {
            mt_ctx = static_cast<ObPartTransCtx*>(ctx)->get_memtable_ctx();
          }
        } else {
          if (is_bounded_staleness_read) {
            if (OB_FAIL(
                    slave_part_trans_ctx_mgr_.get_memtable_ctx(pg_key, trans_id, is_bounded_staleness_read, mt_ctx))) {
              TRANS_LOG(WARN, "get memtable context error", KR(ret), K(pg_key), K(trans_id));
            }
          } else {
            if (OB_FAIL(part_trans_ctx_mgr_.get_memtable_ctx(pg_key, trans_id, is_bounded_staleness_read, mt_ctx))) {
              TRANS_LOG(WARN, "get memtable context error", KR(ret), K(pg_key), K(trans_id));
            }
          }
        }

        if (OB_SUCC(ret)) {
          mt_ctx->inc_ref();
          if (trans_desc.is_nested_stmt()) {
            const int64_t snapshot_version =
                static_cast<ObPartTransCtx*>(mt_ctx->get_trans_ctx())->get_snapshot_version();
            if (snapshot_version != trans_desc.get_stmt_snapshot_info().get_snapshot_version()) {
              TRANS_LOG(ERROR,
                  "snapshot_version not equal",
                  K(snapshot_version),
                  K(trans_desc.get_stmt_snapshot_info().get_snapshot_version()));
            }
            store_ctx.snapshot_info_ = trans_desc.get_stmt_snapshot_info();
          }
          store_ctx.mem_ctx_ = mt_ctx;
          store_ctx.warm_up_ctx_ = trans_desc.get_warm_up_ctx();
          store_ctx.is_sp_trans_ = is_sp_trans;
          store_ctx.isolation_ = trans_desc.get_trans_param().get_isolation();
          store_ctx.sql_no_ = trans_desc.get_sql_no();
          store_ctx.stmt_min_sql_no_ = trans_desc.get_stmt_min_sql_no();
          store_ctx.trans_table_guard_ = mt_ctx->get_trans_table_guard();
        }
      }
    }
  }

  return ret;
}

int ObTransService::revert_store_ctx(const ObTransDesc& trans_desc, const ObPartitionKey& pg_key, ObStoreCtx& store_ctx)
{
  int ret = OB_SUCCESS;
  const ObStandaloneStmtDesc& desc = trans_desc.get_standalone_stmt_desc();
  bool valid = true;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
    //} else if (OB_UNLIKELY(!is_running_)) {
    //  TRANS_LOG(WARN, "ObTransService is not running");
    //  ret = OB_NOT_RUNNING;
  } else if (OB_ISNULL(store_ctx.mem_ctx_)) {
    ret = OB_ERR_SYS;
    TRANS_LOG(ERROR, "store_ctx.mem_ctx_ must not null", KR(ret), K(trans_desc), K(pg_key));
  } else if (OB_FAIL(store_ctx.mem_ctx_->get_trans_table_guard()->get_trans_state_table().check_trans_table_valid(
                 pg_key, valid))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(
        ERROR, "invalid trans table pkey", K(ret), K(trans_desc), K(desc), K(pg_key), "mt_ctx", *(store_ctx.mem_ctx_));
  } else {
    ObPartitionTransCtxMgr* part_mgr = NULL;
    if (valid) {
      store_ctx.trans_table_guard_ = NULL;
      part_mgr = store_ctx.mem_ctx_->get_trans_table_guard()->get_trans_state_table().get_partition_trans_ctx_mgr();
    }
    if (desc.is_valid()) {
      if (OB_FAIL(revert_store_ctx_(desc, pg_key, store_ctx, part_mgr))) {
        TRANS_LOG(WARN, "revert store ctx failed", K(ret), K(desc), K(pg_key));
      }
    } else if (OB_UNLIKELY(!trans_desc.is_valid())) {
      TRANS_LOG(WARN, "invalid argument", K(trans_desc));
      ret = OB_INVALID_ARGUMENT;
    } else if (static_cast<ObMemtableCtx*>(store_ctx.mem_ctx_)->is_standalone_) {
      if (NULL != part_mgr) {
        if (OB_FAIL(part_mgr->check_and_dec_read_only_trx_count())) {
          TRANS_LOG(WARN, "check and dec read only trx count fail", K(ret), K(pg_key));
        }
      } else if (OB_FAIL(part_trans_ctx_mgr_.check_and_dec_read_only_trx_count(pg_key))) {
        TRANS_LOG(WARN, "check and dec read only trx count fail", K(ret), K(pg_key));
      } else {
        // do nothing
      }
      ObMemtableCtx* mem_ctx = static_cast<ObMemtableCtx*>(store_ctx.mem_ctx_);
      if (mem_ctx->has_read_relocated_row()) {
        const_cast<ObTransDesc*>(&trans_desc)->set_need_check_at_end_participant(true);
        mem_ctx->reset_has_read_relocated_row();
      }
      release_memtable_ctx_(pg_key, mem_ctx);
      store_ctx.mem_ctx_ = NULL;
    } else {
      store_ctx.mem_ctx_->dec_ref();
      store_ctx.warm_up_ctx_ = NULL;
      if (trans_desc.is_mini_sp_trans() || trans_desc.is_sp_trans()) {
        // do nothing
      } else if (store_ctx.is_sp_trans_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected sp trans", KR(ret), K(trans_desc), K(pg_key));
      } else {
        if (trans_desc.is_bounded_staleness_read()) {
          ret = slave_part_trans_ctx_mgr_.revert_memtable_ctx(store_ctx.mem_ctx_);
        } else {
          ret = part_trans_ctx_mgr_.revert_memtable_ctx(store_ctx.mem_ctx_);
        }
        if (OB_FAIL(ret)) {
          TRANS_LOG(WARN, "revert memtable context error", KR(ret), K(trans_desc));
        }
      }
    }
  }

  return ret;
}

/*
 * schema version not comparable between system table and user table, also between different tenant
 * this need caller provide comparable schema_version for the partition
 */
int ObTransService::check_schema_version_elapsed(const ObPartitionKey& partition, const int64_t schema_version,
    const int64_t refreshed_schema_ts, int64_t& max_commit_version)
{
  int ret = OB_SUCCESS;
  int64_t tmp_max_commit_version = 0;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid() || schema_version <= 0 || refreshed_schema_ts < 0) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(schema_version), K(refreshed_schema_ts));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(
                 part_trans_ctx_mgr_.check_schema_version_elapsed(partition, schema_version, refreshed_schema_ts))) {
    if (OB_EAGAIN != ret) {
      TRANS_LOG(
          WARN, "check schema version elapsed error", KR(ret), K(partition), K(schema_version), K(refreshed_schema_ts));
    }
  } else if (OB_FAIL(ts_mgr_->get_publish_version(partition.get_tenant_id(), tmp_max_commit_version))) {
    TRANS_LOG(WARN, "get publish version error", KR(ret));
  } else {
    max_commit_version = tmp_max_commit_version;
  }

  return ret;
}

int ObTransService::check_ctx_create_timestamp_elapsed(const ObPartitionKey& partition, const int64_t ts)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTransService not inited", KR(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTransService is not running", KR(ret));
  } else if (!partition.is_valid() || ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(partition), K(ts));
  } else if (OB_FAIL(part_trans_ctx_mgr_.check_ctx_create_timestamp_elapsed(partition, ts))) {
    if (OB_EAGAIN != ret) {
      TRANS_LOG(WARN, "check ctx create timestamp elapsed error", KR(ret), K(partition), K(ts));
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObTransService::get_publish_version(const ObPartitionKey& pkey, int64_t& publish_version)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_FAIL(ts_mgr_->get_publish_version(pkey.get_tenant_id(), publish_version))) {
    TRANS_LOG(WARN, "get publish version error", KR(ret), K(pkey));
  } else {
    TRANS_LOG(DEBUG, "get publish version success", K(publish_version));
  }

  return ret;
}

int ObTransService::get_max_trans_version(const ObPartitionKey& pkey, int64_t& max_trans_version)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_FAIL(part_trans_ctx_mgr_.get_max_trans_version(pkey, max_trans_version))) {
    TRANS_LOG(WARN, "get publish version error", KR(ret), K(pkey));
  } else {
    TRANS_LOG(DEBUG, "get publish version success", K(pkey), K(max_trans_version));
  }

  return ret;
}

int ObTransService::check_scheduler_status(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_FAIL(part_trans_ctx_mgr_.check_scheduler_status(pkey))) {
    TRANS_LOG(WARN, "check_scheduler_status error", KR(ret), K(pkey));
  } else if (OB_FAIL(slave_part_trans_ctx_mgr_.check_scheduler_status(pkey))) {
    TRANS_LOG(WARN, "slave check_scheduler_status error", KR(ret), K(pkey));
  } else {
    TRANS_LOG(DEBUG, "check_scheduler_status success", K(pkey));
  }

  return ret;
}

int ObTransService::update_publish_version(
    const ObPartitionKey& partition, const int64_t publish_version, const bool for_replay)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid() || !is_valid_trans_version(publish_version)) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(publish_version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ts_mgr_->update_publish_version(partition.get_tenant_id(), publish_version, for_replay))) {
    TRANS_LOG(WARN, "update publish version error", KR(ret), K(partition), K(publish_version), K(lbt()));
  } else if (OB_FAIL(part_trans_ctx_mgr_.update_max_trans_version(partition, publish_version))) {
    TRANS_LOG(WARN, "update max trans version error", KR(ret), K(partition), K(publish_version), K(lbt()));
  } else if (!ts_mgr_->is_external_consistent(partition.get_tenant_id()) &&
             OB_FAIL(ts_mgr_->update_publish_version(
                 partition.get_tenant_id(), publish_version + MAX_TIME_INTERVAL_BETWEEN_MACHINE_US, false))) {
    TRANS_LOG(WARN, "update publish version fail", KR(ret), K(partition), K(publish_version), K(for_replay));
  } else {
    TRANS_LOG(INFO, "update publish version success", K(partition), K(publish_version), K(lbt()));
  }

  return ret;
}

int ObTransService::iterate_partition(ObPartitionIterator& partition_iter)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(part_trans_ctx_mgr_.iterate_partition(partition_iter))) {
    TRANS_LOG(WARN, "iterate partition error", KR(ret));
  } else if (OB_FAIL(partition_iter.set_ready())) {
    TRANS_LOG(WARN, "ObPartitionIterator set ready error", KR(ret));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransService::iterate_partition_mgr_stat(ObTransPartitionMgrStatIterator& partition_mgr_stat_iter)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(part_trans_ctx_mgr_.iterate_partition_mgr_stat(partition_mgr_stat_iter, self_))) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr iterate partition error", KR(ret), K_(self));
  } else if (OB_FAIL(slave_part_trans_ctx_mgr_.iterate_partition_mgr_stat(partition_mgr_stat_iter, self_))) {
    TRANS_LOG(WARN, "slave ObPartTransCtxMgr iterate partition error", KR(ret), K_(self));
  } else if (OB_FAIL(partition_mgr_stat_iter.set_ready())) {
    TRANS_LOG(WARN, "ObTransPartitionMgrStatIterator set ready error", KR(ret));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransService::clear_all_ctx(const common::ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(coord_trans_ctx_mgr_.clear_all_ctx(partition))) {
    TRANS_LOG(WARN, "clear all coordinator ctx error", KR(ret), K(partition));
  } else if (OB_FAIL(part_trans_ctx_mgr_.clear_all_ctx(partition))) {
    TRANS_LOG(WARN, "clear all participant ctx error", KR(ret), K(partition));
  } else if (OB_FAIL(slave_part_trans_ctx_mgr_.clear_all_ctx(partition))) {
    TRANS_LOG(WARN, "clear all slave participant ctx error", KR(ret), K(partition));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransService::iterate_trans_stat(const common::ObPartitionKey& partition, ObTransStatIterator& trans_stat_iter)
{
  int ret = OB_SUCCESS;
  const int64_t PRINT_SCHE_COUNT = 128;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(part_trans_ctx_mgr_.iterate_trans_stat(partition, trans_stat_iter))) {
    TRANS_LOG(WARN, "iterate transaction stat error", KR(ret), K(partition));
  } else if (OB_FAIL(slave_part_trans_ctx_mgr_.iterate_trans_stat(partition, trans_stat_iter))) {
    TRANS_LOG(WARN, "iterate slave transaction stat error", KR(ret), K(partition));
  } else if (OB_FAIL(trans_stat_iter.set_ready())) {
    TRANS_LOG(WARN, "ObTransStatIterator set ready error", KR(ret), K(partition));
  } else {
    // do nothing
  }
  sche_trans_ctx_mgr_.print_all_ctx(PRINT_SCHE_COUNT);

  return ret;
}

int ObTransService::print_all_trans_ctx(const common::ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(part_trans_ctx_mgr_.print_all_trans_ctx(partition))) {
    TRANS_LOG(WARN, "print all trans ctx error", K(ret), K(partition));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransService::iterate_trans_lock_stat(
    const common::ObPartitionKey& partition, ObTransLockStatIterator& trans_lock_stat_iter)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(part_trans_ctx_mgr_.iterate_trans_lock_stat(partition, trans_lock_stat_iter))) {
    TRANS_LOG(WARN, "iterate transaction lock stat error", KR(ret), K(partition));
  } else if (OB_FAIL(trans_lock_stat_iter.set_ready())) {
    TRANS_LOG(WARN, "ObTransLockStatIterator set ready error", KR(ret), K(partition));
  } else {
    // do nothing
    TRANS_LOG(INFO, "ObTransLockStatIterator set ready succ", KR(ret), K(partition));
  }

  return ret;
}

int ObTransService::iterate_trans_result_info_in_TRIM(
    const common::ObPartitionKey& partition, ObTransResultInfoStatIterator& iter)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(part_trans_ctx_mgr_.iterate_trans_result_info_in_TRIM(partition, iter))) {
    TRANS_LOG(WARN, "iterate transaction result info stat error", KR(ret), K(partition));
  } else if (OB_FAIL(iter.set_ready())) {
    TRANS_LOG(WARN, "ObTransResultInfoStatIterator set ready error", KR(ret), K(partition));
  } else {
    // do nothing
    TRANS_LOG(INFO, "ObTransResultInfoStatIterator set ready succ", KR(ret), K(partition));
  }

  return ret;
}

int ObTransService::iterate_trans_memory_stat(ObTransMemStatIterator& mem_stat_iter)
{
  int ret = OB_SUCCESS;
  ObTransMemoryStat rpc_mem_stat;
  ObTransMemoryStat log_mutator_mem_stat;
  ObTransMemoryStat version_token_mem_stat;
  ObTransMemoryStat clog_buf_mem_stat;
  ObTransMemoryStat trans_ctx_mem_stat;
  ObTransMemoryStat partition_ctx_mgr_mem_stat;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(rpc_mem_stat.init(self_,
                 TransRpcTaskFactory::get_mod_type(),
                 TransRpcTaskFactory::get_alloc_count(),
                 TransRpcTaskFactory::get_release_count()))) {
    TRANS_LOG(WARN,
        "rpc memory stat init error",
        KR(ret),
        "alloc_count",
        TransRpcTaskFactory::get_alloc_count(),
        "release_count",
        TransRpcTaskFactory::get_release_count());
  } else if (OB_FAIL(mem_stat_iter.push(rpc_mem_stat))) {
    TRANS_LOG(WARN, "rpc memory stat push error", KR(ret));
  } else if (OB_FAIL(log_mutator_mem_stat.init(self_,
                 MutatorBufFactory::get_mod_type(),
                 MutatorBufFactory::get_alloc_count(),
                 MutatorBufFactory::get_release_count()))) {
    TRANS_LOG(WARN,
        "log mutator memory stat init error",
        KR(ret),
        "alloc_count",
        MutatorBufFactory::get_alloc_count(),
        "release_count",
        MutatorBufFactory::get_release_count());
  } else if (OB_FAIL(mem_stat_iter.push(log_mutator_mem_stat))) {
    TRANS_LOG(WARN, "log mutator memory stat push error", KR(ret));
  } else if (OB_FAIL(mem_stat_iter.push(version_token_mem_stat))) {
    TRANS_LOG(WARN, "version token memory stat push error", KR(ret));
  } else if (OB_FAIL(clog_buf_mem_stat.init(self_,
                 ClogBufFactory::get_mod_type(),
                 ClogBufFactory::get_alloc_count(),
                 ClogBufFactory::get_release_count()))) {
    TRANS_LOG(WARN,
        "clog buf memory stat init error",
        KR(ret),
        "alloc_count",
        ClogBufFactory::get_alloc_count(),
        "release_count",
        ClogBufFactory::get_release_count());
  } else if (OB_FAIL(mem_stat_iter.push(clog_buf_mem_stat))) {
    TRANS_LOG(WARN, "clog buf memory stat push error", KR(ret));
  } else if (OB_FAIL(trans_ctx_mem_stat.init(self_,
                 ObTransCtxFactory::get_mod_type(),
                 ObTransCtxFactory::get_alloc_count(),
                 ObTransCtxFactory::get_release_count()))) {
    TRANS_LOG(WARN,
        "transaction context memory stat init error",
        KR(ret),
        "alloc_count",
        ObTransCtxFactory::get_alloc_count(),
        "release_count",
        ObTransCtxFactory::get_release_count());
  } else if (OB_FAIL(mem_stat_iter.push(trans_ctx_mem_stat))) {
    TRANS_LOG(WARN, "transaction context memory stat push error", KR(ret));
  } else if (OB_FAIL(partition_ctx_mgr_mem_stat.init(self_,
                 ObPartitionTransCtxMgrFactory::get_mod_type(),
                 ObPartitionTransCtxMgrFactory::get_alloc_count(),
                 ObPartitionTransCtxMgrFactory::get_release_count()))) {
    TRANS_LOG(WARN,
        "partition context mananger memory stat init error",
        KR(ret),
        "alloc_count",
        ObPartitionTransCtxMgrFactory::get_alloc_count(),
        "release_count",
        ObPartitionTransCtxMgrFactory::get_release_count());
  } else if (OB_FAIL(mem_stat_iter.push(partition_ctx_mgr_mem_stat))) {
    TRANS_LOG(WARN, "partition context mananger memory stat push error", KR(ret));
  } else if (OB_FAIL(mem_stat_iter.set_ready())) {
    TRANS_LOG(WARN, "transaction factory statistics set ready error", KR(ret));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransService::end_trans_(const bool is_rollback, ObTransDesc& trans_desc, ObIEndTransCallback& cb,
    const int64_t stmt_expired_time, const MonotonicTs commit_time)
{
  int ret = OB_SUCCESS;
  int save_ret = OB_SUCCESS;
  ObScheTransCtx* sche_ctx = trans_desc.get_sche_ctx();
  ;
  const uint64_t tenant_id = trans_desc.get_tenant_id();
  const int64_t trans_expired_time = trans_desc.get_trans_expired_time();
  const bool is_xa_trans = trans_desc.is_xa_local_trans();
  static ObNullEndTransCallback null_cb;
  sql::ObIEndTransCallback* callback = &cb;
  bool need_rollback = is_rollback;

  if (NULL == sche_ctx) {
    TRANS_LOG(INFO, "transaction ctx is null, may be stmt fail or rollback", K(trans_desc));
    (void)end_trans_callback_(*callback, OB_SUCCESS, tenant_id);
  } else {
    if (trans_desc.is_trans_timeout()) {
      TRANS_LOG(WARN, "transaction is timeout", K(trans_desc));
      ObTransStatistic::get_instance().add_trans_timeout_count(tenant_id, 1);
      save_ret = OB_TRANS_TIMEOUT;
    } else {
      // 1. caculate participant will be rollbacked
      if (OB_FAIL(check_abort_participants_(trans_desc, sche_ctx))) {
        TRANS_LOG(WARN, "check abrot participants error", KR(ret), K(is_rollback), K(trans_desc));
        ret = OB_SUCCESS;
      }
      // 2. commit/rollback transaction
      if (ObClockGenerator::getClock() >= stmt_expired_time) {
        need_rollback = true;
        save_ret = OB_TRANS_STMT_TIMEOUT;
        TRANS_LOG(WARN, "statement is timeout", K(save_ret), K(trans_desc));
        ObTransStatistic::get_instance().add_trans_timeout_count(tenant_id, 1);
        callback = &null_cb;
      } else {
        need_rollback = (trans_desc.need_rollback() || is_rollback);
        if (!is_rollback && need_rollback) {
          save_ret = OB_TRANS_ROLLBACKED;
          callback = &null_cb;
        }
      }

      const int64_t expired_time = ((trans_expired_time < stmt_expired_time) ? trans_expired_time : stmt_expired_time);
      ObWaitEventGuard wait_guard(ObWaitEventIds::END_TRANS_WAIT,
          expired_time,
          need_rollback,
          trans_desc.get_trans_id().hash(),
          trans_desc.get_participants().count());
      if (is_xa_trans && is_rollback) {
        // xa rollback caused by connection close
        // if (OB_FAIL(sche_ctx->xa_rollback_session_terminate())) {
        if (OB_FAIL(handle_terminate_for_xa_branch_(trans_desc))) {
          TRANS_LOG(WARN, "rollback xa trans failed", K(ret), K(trans_desc));
        } else {
          (void)end_trans_callback_(*callback, ret, tenant_id);
        }
      } else if (OB_FAIL(sche_ctx->end_trans(
                     trans_desc, need_rollback, callback, trans_expired_time, stmt_expired_time, commit_time))) {
        TRANS_LOG(WARN, "end trans failed", KR(ret), K(is_rollback), K(trans_desc));
      }
    }
    // xa transaction revert sche_ctx in xa_end
    if (!is_xa_trans) {
      // always revert transaction context
      (void)sche_trans_ctx_mgr_.revert_trans_ctx(sche_ctx);
    }
  }
  if (OB_SUCC(ret)) {
    ret = save_ret;
  }

  return ret;
}

// calculate partitions actually need to rollback directly
int ObTransService::check_abort_participants_(ObTransDesc& trans_desc, ObScheTransCtx* sche_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sche_ctx)) {
    TRANS_LOG(WARN, "invalid argument", KP(sche_ctx));
    ret = OB_INVALID_ARGUMENT;
  } else if (trans_desc.is_bounded_staleness_read()) {
    // weak read would not create part_ctx
  } else {
    ObPartitionArray gc_participants;
    for (int64_t i = 0; OB_SUCC(ret) && i < trans_desc.get_gc_participants().count(); ++i) {
      const ObPartitionKey& pkey = trans_desc.get_gc_participants().at(i);

      int64_t j = 0;
      for (; OB_SUCC(ret) && j < trans_desc.get_participants().count(); ++j) {
        if (pkey == trans_desc.get_participants().at(j)) {
          break;
        }
      }

      if (j == trans_desc.get_participants().count()) {
        if (OB_FAIL(gc_participants.push_back(pkey))) {
          TRANS_LOG(WARN, "abort participants push error", KR(ret), K(pkey));
        }
      }
    }
    if (OB_SUCC(ret) && gc_participants.count() > 0) {
      if (OB_FAIL(sche_ctx->merge_gc_participants(gc_participants))) {
        TRANS_LOG(WARN, "scheduer context merge abort participants error", KR(ret));
      }
    }
  }
  return ret;
}

void ObTransService::check_env_()
{
  // do nothing now
}

// handle message when Participant transaction context don't exist
int ObTransService::handle_orphan_msg_(const ObTransMsg& msg, const int ctx_ret)
{
  int ret = OB_SUCCESS;
  int32_t msg_status = OB_SUCCESS;
  int64_t msg_type = OB_TRANS_MSG_UNKNOWN;
  int64_t trans_version = ObClockGenerator::getClock();
  bool need_response = true;
  ObTransMsg ret_msg;
  ObAddr server;

  // return success for stmt rollback
  if (OB_TRANS_STMT_ROLLBACK_REQUEST == msg.get_msg_type()) {
    TRANS_LOG(INFO, "discard statement rollback message which is an orphan", K(msg));
    msg_type = OB_TRANS_STMT_ROLLBACK_RESPONSE;
    msg_status = OB_SUCCESS;
    need_response = false;
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            msg.get_sender(),
            msg.get_trans_param(),
            self_,
            msg.get_sql_no(),
            msg_status,
            msg.get_request_id()))) {
      TRANS_LOG(WARN, "ObTransMsg init error", KR(ret));
    } else if (OB_FAIL(ret_msg.set_msg_timeout(msg.get_msg_timeout()))) {
      TRANS_LOG(WARN, "set message start timestamp error", K(ret), K(msg));
    } else if (OB_FAIL(rpc_->post_trans_msg(msg.get_tenant_id(), msg.get_sender_addr(), ret_msg, msg_type))) {
      TRANS_LOG(WARN, "post transaction message error", KR(ret));
    } else {
      // do nothing
    }
  } else if (OB_TRANS_SAVEPOINT_ROLLBACK_REQUEST == msg.get_msg_type()) {
    TRANS_LOG(INFO, "savepoint rollback request message which is an orphan", K(msg));
    msg_type = OB_TRANS_SAVEPOINT_ROLLBACK_RESPONSE;
    msg_status = OB_SUCCESS;
    need_response = false;
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            msg.get_sender(),
            msg.get_trans_param(),
            self_,
            msg.get_sql_no(),
            msg_status,
            msg.get_request_id()))) {
      TRANS_LOG(WARN, "ObTransMsg init error", KR(ret));
    } else if (OB_FAIL(rpc_->post_trans_msg(msg.get_tenant_id(), msg.get_sender_addr(), ret_msg, msg_type))) {
      TRANS_LOG(WARN, "post transaction message error", KR(ret));
    } else {
      // do nothing
    }
  } else if (OB_TRANS_2PC_LOG_ID_REQUEST == msg.get_msg_type()) {
    const int64_t prepare_log_id = ObClockGenerator::getClock();
    msg_type = OB_TRANS_2PC_LOG_ID_RESPONSE;
    msg_status = OB_TRANS_STATE_UNKNOWN;
    PartitionLogInfoArray arr;
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            msg.get_sender(),
            msg.get_trans_param(),
            prepare_log_id,
            ObClockGenerator::getClock(),
            self_,
            msg_status,
            msg.get_request_id(),
            arr))) {
      TRANS_LOG(WARN, "ObTransMsg init error", KR(ret));
    }
  } else if (OB_TRANS_2PC_PREPARE_REQUEST == msg.get_msg_type()) {
    msg_type = OB_TRANS_2PC_PREPARE_RESPONSE;
    uint64_t prepare_log_id = ObClockGenerator::getClock();
    int64_t prepare_log_timestamp = 0;
    const PartitionLogInfoArray& arr = msg.get_partition_log_info_arr();
    if (arr.count() != msg.get_participants().count()) {
      if (ctx_ret == OB_TRANS_KILLED) {
        msg_status = OB_TRANS_KILLED;
      } else {
        msg_status = OB_TRANS_STATE_UNKNOWN;
      }
      TRANS_LOG(INFO, "handle orphan 2pc prepare request", K(msg_status), K(msg));
    } else {
      // participant had participant in batch commit,
      // need check partition's logid is persistented
      uint64_t log_id = OB_INVALID_ID;
      int64_t ts = OB_INVALID_TIMESTAMP;
      for (int64_t i = 0; i < arr.count(); ++i) {
        if (msg.get_receiver() == arr.at(i).get_partition()) {
          log_id = arr.at(i).get_log_id();
          ts = arr.at(i).get_log_timestamp();
          break;
        }
      }
      if (OB_INVALID_ID == log_id || OB_INVALID_TIMESTAMP == ts) {
        TRANS_LOG(ERROR, "unexpected log id or timestamp", K(msg));
        ret = OB_ERR_UNEXPECTED;
      } else {
        const ObPartitionKey& partition = msg.get_receiver();
        storage::ObIPartitionGroupGuard guard;
        clog::ObIPartitionLogService* log_service = NULL;
        ObTransID trans_id;
        int64_t submit_timestamp = OB_INVALID_TIMESTAMP;

        if (OB_FAIL(partition_service_->get_partition(partition, guard))) {
          TRANS_LOG(WARN, "get partition failed", K(partition), KR(ret));
        } else if (NULL == guard.get_partition_group()) {
          TRANS_LOG(WARN, "partition not exist", K(partition));
          ret = OB_PARTITION_NOT_EXIST;
        } else if (NULL == (log_service = guard.get_partition_group()->get_log_service())) {
          TRANS_LOG(WARN, "get partiton log service error", K(partition));
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(log_service->query_confirmed_log(log_id, trans_id, submit_timestamp))) {
          if (OB_NOT_MASTER == ret || OB_EAGAIN == ret) {
            // retryable
          } else if (OB_ENTRY_NOT_EXIST == ret) {
            msg_status = OB_TRANS_KILLED;
            ret = OB_SUCCESS;
          } else {
            TRANS_LOG(ERROR, "unexpected ret code", KR(ret), K(msg));
            ret = OB_ERR_UNEXPECTED;
          }
          TRANS_LOG(INFO, "handle query confirmed log", KR(ret), K(msg_status), K(msg));
        } else {
          if (trans_id == msg.get_trans_id()) {
            if (ts == submit_timestamp) {
              prepare_log_id = log_id;
              prepare_log_timestamp = ts;
              trans_version = prepare_log_timestamp;
              msg_status = OB_SUCCESS;
              if (EXECUTE_COUNT_PER_SEC(64)) {
                TRANS_LOG(INFO, "query confirmed log success", K(prepare_log_id), K(prepare_log_timestamp), K(msg));
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              TRANS_LOG(
                  ERROR, "submit_timestamp not match, unexpected", K(log_id), K(trans_id), K(submit_timestamp), K(ts));
            }
          } else {
            TRANS_LOG(WARN, "different trans_id", K(trans_id), K(msg));
            msg_status = OB_TRANS_KILLED;
            ret = OB_SUCCESS;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
              msg.get_trans_id(),
              msg_type,
              msg.get_trans_time(),
              msg.get_receiver(),
              msg.get_sender(),
              msg.get_scheduler(),
              msg.get_coordinator(),
              msg.get_participants(),
              msg.get_trans_param(),
              prepare_log_id,
              prepare_log_timestamp,
              self_,
              msg_status,
              Ob2PCState::PREPARE,
              trans_version,
              msg.get_request_id(),
              msg.get_partition_log_info_arr(),
              0,
              msg.get_app_trace_info(),
              msg.get_xid(),
              msg.is_xa_prepare()))) {
        TRANS_LOG(WARN, "ObTransMsg init error", KR(ret));
      }
    }
  } else if (OB_TRANS_2PC_PRE_COMMIT_REQUEST == msg.get_msg_type()) {
    msg_type = OB_TRANS_2PC_PRE_COMMIT_RESPONSE;
    trans_version = msg.get_trans_version();
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            msg.get_sender(),
            msg.get_scheduler(),
            msg.get_coordinator(),
            msg.get_participants(),
            msg.get_trans_param(),
            trans_version,
            self_,
            msg.get_request_id(),
            msg.get_partition_log_info_arr(),
            msg_status))) {
      TRANS_LOG(WARN, "ObTransMsg init error", KR(ret));
    }
  } else if (OB_TRANS_2PC_COMMIT_REQUEST == msg.get_msg_type()) {
    msg_type = OB_TRANS_2PC_COMMIT_RESPONSE;
    trans_version = msg.get_trans_version();
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            msg.get_sender(),
            msg.get_scheduler(),
            msg.get_coordinator(),
            msg.get_participants(),
            msg.get_trans_param(),
            trans_version,
            self_,
            msg.get_request_id(),
            msg.get_partition_log_info_arr(),
            msg_status))) {
      TRANS_LOG(WARN, "ObTransMsg init error", KR(ret));
    }
  } else if (OB_TRANS_2PC_ABORT_REQUEST == msg.get_msg_type()) {
    msg_type = OB_TRANS_2PC_ABORT_RESPONSE;
    msg_status = OB_TRANS_STATE_UNKNOWN;
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            msg.get_sender(),
            msg.get_scheduler(),
            msg.get_coordinator(),
            msg.get_participants(),
            msg.get_trans_param(),
            self_,
            msg.get_request_id(),
            msg_status))) {
      TRANS_LOG(WARN, "ObTransMsg init error", KR(ret));
    }
  } else if (OB_TRANS_2PC_CLEAR_REQUEST == msg.get_msg_type()) {
    msg_type = OB_TRANS_2PC_CLEAR_RESPONSE;
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            msg.get_sender(),
            msg.get_scheduler(),
            msg.get_coordinator(),
            msg.get_participants(),
            msg.get_trans_param(),
            self_,
            msg_status,
            msg.get_request_id(),
            MonotonicTs(0)))) {
      TRANS_LOG(WARN, "ObTransMsg init error", K(ret));
    }
  } else if (OB_TRANS_2PC_COMMIT_CLEAR_REQUEST == msg.get_msg_type()) {
    msg_type = OB_TRANS_2PC_COMMIT_CLEAR_RESPONSE;
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            msg.get_sender(),
            msg.get_scheduler(),
            msg.get_coordinator(),
            msg.get_participants(),
            msg.get_trans_param(),
            self_,
            msg_status,
            msg.get_request_id(),
            MonotonicTs(0)))) {
      TRANS_LOG(WARN, "ObTransMsg init error", K(ret));
    }
  } else if (OB_TRANS_CLEAR_REQUEST == msg.get_msg_type()) {
    need_response = false;
    msg_type = OB_TRANS_CLEAR_RESPONSE;
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            msg.get_sender(),
            msg.get_scheduler(),
            msg.get_participants(),
            msg.get_trans_param(),
            self_,
            msg.get_request_id(),
            OB_SUCCESS))) {
      TRANS_LOG(WARN, "transaction clear response message init error", KR(ret));
    } else if (OB_FAIL(rpc_->post_trans_msg(msg.get_tenant_id(), msg.get_scheduler(), ret_msg, msg_type))) {
      TRANS_LOG(WARN, "post transaction message error", KR(ret));
    } else {
      // do nothing
    }
  } else if (OB_TRANS_START_STMT_REQUEST == msg.get_msg_type()) {
    need_response = false;
    msg_type = OB_TRANS_START_STMT_RESPONSE;
    int64_t snapshot_version = ObClockGenerator::getClock();
    msg_status = ctx_ret;
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            SCHE_PARTITION_ID,
            msg.get_trans_param(),
            self_,
            msg.get_sql_no(),
            snapshot_version,
            msg_status,
            msg.get_request_id()))) {
      TRANS_LOG(WARN, "message init error", KR(ret));
    } else if (OB_FAIL(rpc_->post_trans_msg(msg.get_tenant_id(), msg.get_scheduler(), ret_msg, msg_type))) {
      TRANS_LOG(WARN, "post transaction message error", KR(ret));
    } else {
      // do nothing
    }
  } else if (OB_TRANS_DISCARD_REQUEST == msg.get_msg_type()) {
    need_response = false;
  } else if (OB_TRANS_ASK_SCHEDULER_STATUS_RESPONSE == msg.get_msg_type()) {
    need_response = false;
  } else {
    TRANS_LOG(ERROR, "invalid message type", K(msg));
    ret = OB_ERR_UNEXPECTED;
  }
  if (OB_SUCC(ret) && need_response) {
    if (OB_FAIL(location_adapter_->nonblock_get_strong_leader(msg.get_sender(), server))) {
      TRANS_LOG(WARN, "get leader failed", K(ret), "partition", msg.get_sender());
      server = msg.get_sender_addr();
      // rewrite ret
      ret = OB_SUCCESS;
      const int64_t expire_renew_time = 0;
      (void)location_adapter_->nonblock_renew(msg.get_sender(), expire_renew_time);
    }
    if (OB_SUCCESS == ret) {
      if (OB_FAIL(rpc_->post_trans_msg(msg.get_sender().get_tenant_id(), server, ret_msg, msg_type))) {
        TRANS_LOG(WARN, "post transaction message error", KR(ret), K(server), K(ret_msg));
      }
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "handle orphan message error", KR(ret), K(msg));
  } else {
    TRANS_LOG(DEBUG, "handle orphan message success", K(msg));
  }

  return ret;
}

int ObTransService::handle_start_stmt_request_(const ObTransMsg& msg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const int64_t msg_type = OB_TRANS_START_STMT_RESPONSE;
  int64_t snapshot_version = 0;
  ObTransMsg ret_msg;
  const bool can_elr = false;
  const bool is_readonly = msg.get_trans_param().is_readonly();
  int msg_status = OB_SUCCESS;
  if (OB_FAIL(generate_part_snapshot_for_current_read_(can_elr,
          is_readonly,
          msg.get_stmt_expired_time(),
          msg.get_stmt_expired_time(),
          true,
          msg.get_receiver(),
          snapshot_version))) {
    TRANS_LOG(WARN, "generate snapshot version error", K(ret), K(msg));
    if (OB_EAGAIN == ret || OB_GTS_NOT_READY == ret || OB_NOT_MASTER == ret) {
      msg_status = OB_NOT_MASTER;
    } else {
      msg_status = ret;
    }
  }

  if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = ret_msg.init(msg.get_tenant_id(),
                                     msg.get_trans_id(),
                                     msg_type,
                                     msg.get_trans_time(),
                                     msg.get_receiver(),
                                     SCHE_PARTITION_ID,
                                     msg.get_trans_param(),
                                     self_,
                                     msg.get_sql_no(),
                                     snapshot_version,
                                     msg_status,
                                     msg.get_request_id())))) {
    TRANS_LOG(WARN, "message init error", K(tmp_ret));
  } else if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = rpc_->post_trans_msg(
                                            msg.get_tenant_id(), msg.get_scheduler(), ret_msg, msg_type)))) {
    TRANS_LOG(WARN, "post transaction message error", K(tmp_ret));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransService::handle_trans_err_response_(const int64_t err_msg_type, const ObTransID& trans_id,
    const ObPartitionKey& partition, const ObAddr& sender_addr, const int status, const int64_t sql_no,
    const int64_t request_id)
{
  int ret = OB_SUCCESS;
  bool alloc = false;
  const bool for_replay = false;
  const bool is_readonly = false;
  ObTransCtx* ctx = NULL;
  ObScheTransCtx* sche_ctx = NULL;

  if (OB_FAIL(sche_trans_ctx_mgr_.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
    TRANS_LOG(DEBUG, "get transaction context error", KR(ret), K(trans_id));
    ctx = NULL;
  } else if (OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "context is null", KP(ctx), K(trans_id));
    ret = OB_ERR_UNEXPECTED;
  } else {
    sche_ctx = static_cast<ObScheTransCtx*>(ctx);
    if (OB_FAIL(sche_ctx->handle_err_response(err_msg_type, partition, sender_addr, status, sql_no, request_id))) {
      TRANS_LOG(WARN,
          "handle transaction err response error",
          KR(ret),
          K(err_msg_type),
          K(partition),
          K(trans_id),
          K(status),
          K(sql_no));
    } else {
      TRANS_LOG(
          DEBUG, "handle transaction err response success", K(partition), K(err_msg_type), K(trans_id), K(status));
    }
    (void)sche_trans_ctx_mgr_.revert_trans_ctx(ctx);
  }

  return ret;
}

int ObTransService::retry_trans_rpc_(const int64_t msg_type, const ObPartitionKey& partition, const ObTransID& trans_id,
    const int64_t request_id, const int64_t sql_no, const int64_t msg_timeout)
{
  int ret = OB_SUCCESS;
  bool alloc = false;
  const bool for_replay = false;
  const bool is_readonly = false;
  ObTransCtx* ctx = NULL;
  ObScheTransCtx* sche_ctx = NULL;

  if (OB_TRANS_STMT_ROLLBACK_REQUEST == msg_type || OB_TRANS_SAVEPOINT_ROLLBACK_REQUEST == msg_type) {
    if (OB_FAIL(sche_trans_ctx_mgr_.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
      TRANS_LOG(WARN, "get transaction context error", KR(ret), K(trans_id));
      ctx = NULL;
    } else if (OB_ISNULL(ctx)) {
      TRANS_LOG(WARN, "context is null", KP(ctx), K(trans_id));
      ret = OB_ERR_UNEXPECTED;
    } else {
      sche_ctx = static_cast<ObScheTransCtx*>(ctx);
      if (OB_FAIL(sche_ctx->submit_stmt_rollback_request(partition, msg_type, request_id, sql_no, msg_timeout))) {
        TRANS_LOG(
            WARN, "submit stmt rollback request fail", KR(ret), K(partition), K(trans_id), K(msg_type), K(sql_no));
      } else {
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          TRANS_LOG(
              INFO, "submit stmt rollback request success", KR(ret), K(partition), K(trans_id), K(msg_type), K(sql_no));
        }
      }
      (void)sche_trans_ctx_mgr_.revert_trans_ctx(ctx);
    }
  }

  return ret;
}

int ObTransService::inactive_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!is_valid_tenant_id(tenant_id)) {
    TRANS_LOG(WARN, "invalid argument", K(tenant_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(sche_trans_ctx_mgr_.inactive_tenant(tenant_id))) {
    TRANS_LOG(WARN, "scheduler context mgr inactive tenant error", KR(ret), K(tenant_id));
  } else {
    TRANS_LOG(INFO, "scheduler context mgr inactive tenant success", K(tenant_id));
  }

  return ret;
}

int ObTransService::checkpoint(
    const ObPartitionKey& pkey, const int64_t checkpoint_base_ts, storage::ObPartitionLoopWorker* lp_worker)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_FAIL(part_trans_ctx_mgr_.checkpoint(pkey, checkpoint_base_ts, lp_worker))) {
    TRANS_LOG(WARN, "checkpoint transaction failed", KR(ret), K(checkpoint_base_ts), K(pkey));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransService::push(void* task)
{
  ATOMIC_FAA(&input_queue_count_, 1);
  return ObSimpleThreadPool::push(task);
}

void ObTransService::handle(void* task)
{
  int ret = OB_SUCCESS;
  TransRpcTask* rpc_task = NULL;
  ObTransTask* trans_task = NULL;
  bool need_release_task = true;
  ATOMIC_FAA(&output_queue_count_, 1);

  if (OB_ISNULL(task)) {
    TRANS_LOG(ERROR, "task is null", KP(task));
  } else {
    trans_task = static_cast<ObTransTask*>(task);
    if (!trans_task->ready_to_handle()) {
      if (OB_FAIL(push(trans_task))) {
        TRANS_LOG(WARN, "transaction service push task error", KR(ret), K(*trans_task));
        TransRpcTaskFactory::release(static_cast<TransRpcTask*>(trans_task));
      }
    } else if (ObTransRetryTaskType::CALLBACK_TASK == trans_task->get_task_type() ||
               ObTransRetryTaskType::NOTIFY_TASK == trans_task->get_task_type()) {
      rpc_task = static_cast<TransRpcTask*>(task);
      const ObPartitionKey& partition = rpc_task->get_partition();
      const ObTransID& trans_id = rpc_task->get_trans_id();
      const int64_t msg_type = rpc_task->get_msg_type();
      const int status = rpc_task->get_status();
      const ObAddr& addr = rpc_task->get_addr();
      const int64_t sql_no = rpc_task->get_sql_no();
      const int64_t request_id = rpc_task->get_request_id();
      if (OB_FAIL(handle_trans_msg_callback(partition, trans_id, msg_type, status, addr, sql_no, request_id))) {
        TRANS_LOG(WARN, "handle transaction msg callback error", KR(ret), "rpc_task", *rpc_task);
      } else {
        TRANS_LOG(INFO, "handle transaction msg callback", "rpc_task", *rpc_task);
      }
      TransRpcTaskFactory::release(rpc_task);
      rpc_task = NULL;
    } else if (ObTransRetryTaskType::ERROR_MSG_TASK == trans_task->get_task_type()) {
      rpc_task = static_cast<TransRpcTask*>(task);
      const ObTransMsg& msg = rpc_task->get_msg();
      if (OB_FAIL(handle_trans_response(msg))) {
        TRANS_LOG(WARN, "handle transaction msg error", KR(ret), "rpc_task", *rpc_task);
      } else {
        TRANS_LOG(DEBUG, "handle transaction msg", "rpc_task", *rpc_task);
      }
      TransRpcTaskFactory::release(rpc_task);
      rpc_task = NULL;
    } else if (ObTransRetryTaskType::RETRY_STMT_ROLLBACK_TASK == trans_task->get_task_type()) {
      rpc_task = static_cast<TransRpcTask*>(task);
      const ObPartitionKey& partition = rpc_task->get_partition();
      const ObTransID& trans_id = rpc_task->get_trans_id();
      const int64_t request_id = rpc_task->get_request_id();
      const int64_t msg_type = rpc_task->get_msg_type();
      const int64_t sql_no = rpc_task->get_sql_no();
      const int64_t task_timeout = rpc_task->get_task_timeout();
      if (OB_FAIL(retry_trans_rpc_(msg_type, partition, trans_id, request_id, sql_no, task_timeout))) {
        TRANS_LOG(WARN, "retry trans rpc fail", KR(ret), "rpc_task", *rpc_task);
        if (OB_LOCATION_NOT_EXIST == ret || OB_LOCATION_LEADER_NOT_EXIST == ret) {
          if (OB_FAIL(push(rpc_task))) {
            TRANS_LOG(WARN, "transaction service push task error", KR(ret), "rpc_task", *rpc_task);
          } else {
            need_release_task = false;
            TRANS_LOG(INFO, "re-push task succ", K(trans_id));
          }
        }
      } else {
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          TRANS_LOG(INFO, "retry trans rpc success", KR(ret), "rpc_task", *rpc_task);
        }
      }
      if (need_release_task) {
        TransRpcTaskFactory::release(rpc_task);
      }
      rpc_task = NULL;
    } else if (ObTransRetryTaskType::CALLBACK_PREV_TRANS_TASK == trans_task->get_task_type() ||
               ObTransRetryTaskType::CALLBACK_NEXT_TRANS_TASK == trans_task->get_task_type()) {
      CallbackTransTask* callback_trans_task = static_cast<CallbackTransTask*>(task);
      bool need_release = true;
      if (OB_FAIL(handle_elr_callback_(trans_task->get_task_type(),
              callback_trans_task->get_partition(),
              callback_trans_task->get_trans_id(),
              callback_trans_task->get_prev_trans_id(),
              callback_trans_task->get_status()))) {
        if (EXECUTE_COUNT_PER_SEC(1)) {
          TRANS_LOG(WARN, "handle_elr_callback error", K(ret), K(*trans_task));
        }
        if (OB_EAGAIN == ret) {
          if (OB_FAIL(push(task))) {
            TRANS_LOG(ERROR, "push retry task error", K(ret), K(*trans_task));
          } else {
            need_release = false;
          }
        }
      }
      if (need_release) {
        CallbackTransTaskFactory::release(callback_trans_task);
        callback_trans_task = NULL;
      }
    } else if (ObTransRetryTaskType::REDO_SYNC_TASK == trans_task->get_task_type()) {
      bool need_release_task = true;
      ObDupTableRedoSyncTask* redo_sync_task = static_cast<ObDupTableRedoSyncTask*>(task);
      if (OB_FAIL(handle_redo_sync_task_(redo_sync_task, need_release_task))) {
        TRANS_LOG(WARN, "handle redo sync task error", KR(ret), K(*redo_sync_task));
      }
      if (need_release_task) {
        redo_sync_task = NULL;
      } else if (OB_FAIL(push(redo_sync_task))) {
        TRANS_LOG(WARN, "transaction service push task error", KR(ret), K(*redo_sync_task));
      } else {
        // do nothing
      }
    } else if (ObTransRetryTaskType::END_TRANS_CB_TASK == trans_task->get_task_type()) {
      bool has_cb = false;
      EndTransCallbackTask* end_trans_cb_task = static_cast<EndTransCallbackTask*>(task);
      int64_t need_wait_us = end_trans_cb_task->get_need_wait_us();
      if (need_wait_us > 0) {
        usleep(need_wait_us);
      }
      if (OB_FAIL(end_trans_cb_task->callback(has_cb))) {
        TRANS_LOG(WARN, "end trans cb task callback error", KR(ret), K(*end_trans_cb_task));
      }
      if (has_cb) {
        EndTransCallbackTaskFactory::release(end_trans_cb_task);
      } else if (OB_FAIL(push(end_trans_cb_task))) {
        TRANS_LOG(WARN, "transaction service push task error", KR(ret), K(*end_trans_cb_task));
      } else {
        // do nothing
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected trans task type!!!", KR(ret), K(*trans_task));
    }

    // print task queue status periodically
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      int64_t queue_num = get_queue_num();
      TRANS_LOG(INFO,
          "[statisic] trans service task queue statisic : ",
          K(queue_num),
          K_(input_queue_count),
          K_(output_queue_count));
      ATOMIC_STORE(&input_queue_count_, 0);
      ATOMIC_STORE(&output_queue_count_, 0);
    }
  }
  UNUSED(ret);  // make compiler happy
}

int ObTransService::get_min_uncommit_prepare_version(const ObPartitionKey& partition, int64_t& min_prepare_version)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(part_trans_ctx_mgr_.get_min_uncommit_prepare_version(partition, min_prepare_version))) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr set memstore version error", KR(ret), K(partition));
  } else if (min_prepare_version <= 0) {
    TRANS_LOG(ERROR, "invalid min prepare version, unexpected error", K(partition), K(min_prepare_version));
    ret = OB_ERR_UNEXPECTED;
  } else {
    TRANS_LOG(DEBUG, "get min uncommit prepare version success", K(partition), K(min_prepare_version));
  }

  return ret;
}

/*
 * get minimum prepare version of transaction whose commit version greate than lg_ts
 * */
int ObTransService::get_min_prepare_version(
    const ObPartitionKey& partition, const int64_t log_ts, int64_t& min_prepare_version)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(part_trans_ctx_mgr_.get_min_prepare_version(partition, log_ts, min_prepare_version))) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr get min prepare version error", KR(ret), K(partition), K(log_ts));
  } else if (min_prepare_version <= 0) {
    TRANS_LOG(ERROR, "invalid min prepare version, unexpected error", K(partition), K(min_prepare_version));
    ret = OB_ERR_UNEXPECTED;
  } else {
    TRANS_LOG(DEBUG, "get min prepare version success", K(partition), K(log_ts), K(min_prepare_version));
  }

  return ret;
}

int ObTransService::get_min_uncommit_log(
    const ObPartitionKey& pkey, uint64_t& min_uncommit_log_id, int64_t& min_uncommit_log_ts)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!pkey.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(pkey));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(part_trans_ctx_mgr_.get_min_uncommit_log(pkey, min_uncommit_log_id, min_uncommit_log_ts))) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr set memstore version error", KR(ret), K(pkey));
  } else if (min_uncommit_log_id <= 0) {
    TRANS_LOG(ERROR, "min_uncommit_log_id is negative", K(pkey), K(min_uncommit_log_id));
    ret = OB_ERR_UNEXPECTED;
  } else {
    TRANS_LOG(DEBUG, "get min_uncommit_log_id success", K(pkey), K(min_uncommit_log_id));
  }

  return ret;
}

int ObTransService::gc_trans_result_info(const ObPartitionKey& pkey, const int64_t checkpoint_ts)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!pkey.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(pkey));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(part_trans_ctx_mgr_.gc_trans_result_info(pkey, checkpoint_ts))) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr gc transaction result info error", K(ret), K(pkey));
  } else {
    TRANS_LOG(DEBUG, "gc trans_result_info success", K(pkey), K(checkpoint_ts));
  }

  return ret;
}

int ObTransService::internal_kill_trans(ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(ERROR, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!trans_desc.is_valid())) {
    TRANS_LOG(ERROR, "invalid argument", K(trans_desc));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(trans_desc.is_trans_end())) {
    TRANS_LOG(ERROR, "unxpected trans_end", K(trans_desc));
    ret = OB_ERR_UNEXPECTED;
  } else if (trans_desc.is_mini_sp_trans() || trans_desc.is_sp_trans()) {
    ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(trans_desc.get_part_ctx());
    bool need_convert_to_dist_trans = false;
    if (NULL != part_ctx && OB_FAIL(part_ctx->kill_trans(need_convert_to_dist_trans))) {
      if (need_convert_to_dist_trans) {
        ret = OB_SUCCESS;
        trans_desc.set_trans_type(TransType::DIST_TRANS);
        if (OB_FAIL(recover_sche_ctx_(trans_desc, part_ctx))) {
          TRANS_LOG(WARN, "recover scheduler context error", KR(ret), K(trans_desc));
          (void)trans_desc.set_sche_ctx(NULL);
        } else {
          part_ctx->recover_dist_trans(self_);
          TRANS_LOG(INFO, "switch sp_trans to dist trans success", K(trans_desc));
          if (OB_FAIL(trans_desc.get_sche_ctx()->kill_trans(trans_desc))) {
            TRANS_LOG(WARN, "kill_dist_trans fail", K(ret), K(trans_desc));
          }
          (void)sche_trans_ctx_mgr_.revert_trans_ctx(trans_desc.get_sche_ctx());
          (void)trans_desc.set_sche_ctx(NULL);
        }
      } else {
        TRANS_LOG(WARN, "kill_sp_trans fail", K(ret), K(trans_desc));
      }
    }
    if (part_ctx != NULL) {
      trans_desc.set_trx_idle_timeout();
      (void)part_trans_ctx_mgr_.revert_trans_ctx(part_ctx);
      trans_desc.set_part_ctx(NULL);
    }
  } else {
    ObScheTransCtx* sche_ctx = trans_desc.get_sche_ctx();
    if (NULL != sche_ctx) {
      if (OB_FAIL(sche_ctx->kill_trans(trans_desc))) {
        TRANS_LOG(WARN, "kill_dist_trans fail", K(ret), K(trans_desc));
      }
      trans_desc.set_trx_idle_timeout();
      (void)sche_trans_ctx_mgr_.revert_trans_ctx(sche_ctx);
      (void)trans_desc.set_sche_ctx(NULL);
    }
  }
  return ret;
}

int ObTransService::kill_query_session(const ObTransDesc& trans_desc, const int status)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (trans_desc.is_valid() && trans_desc.get_trans_type() == TransType::DIST_TRANS) {
    ObTransCtx* ctx = NULL;
    ObScheTransCtx* sche_ctx = NULL;
    const ObTransID& trans_id = trans_desc.get_trans_id();
    bool alloc = false;
    const bool for_replay = false;
    const bool is_readonly = false;

    if (OB_FAIL(sche_trans_ctx_mgr_.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
      TRANS_LOG(WARN, "get transaction context error", KR(ret), K(trans_id));
      ctx = NULL;
    } else if (OB_ISNULL(ctx)) {
      TRANS_LOG(WARN, "context is null", KP(ctx), K(trans_id));
      ret = OB_ERR_UNEXPECTED;
    } else {
      sche_ctx = static_cast<ObScheTransCtx*>(ctx);
      if (OB_FAIL(sche_ctx->kill_query_session(status))) {
        TRANS_LOG(WARN, "kill query session error", KR(ret), K(trans_id), K(status));
      }
      (void)sche_trans_ctx_mgr_.revert_trans_ctx(ctx);
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObTransService::relocate_data(const ObPartitionKey& partition, memtable::ObIMemtable* memtable)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid() || OB_ISNULL(memtable)) {
    TRANS_LOG(WARN, "invalid argument", K(partition), KP(memtable));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(part_trans_ctx_mgr_.relocate_data(partition, memtable))) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr relocate data error", K(partition), KP(memtable));
  } else {
    if (EXECUTE_COUNT_PER_SEC(8)) {
      TRANS_LOG(INFO, "ObPartTransCtxMgr relocate data success", K(partition), KP(memtable));
    }
  }

  return ret;
}

int ObTransService::handle_coordinator_orphan_msg(const ObTransMsg& msg, ObTransMsg& ret_msg)
{
  int ret = OB_SUCCESS;
  int32_t msg_status = OB_SUCCESS;
  int64_t msg_type = OB_TRANS_MSG_UNKNOWN;
  int64_t trans_version = ObClockGenerator::getClock();

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_TRANS_2PC_LOG_ID_REQUEST == msg.get_msg_type()) {
    const int64_t prepare_log_id = ObClockGenerator::getClock();
    msg_type = OB_TRANS_2PC_LOG_ID_RESPONSE;
    msg_status = OB_TRANS_STATE_UNKNOWN;
    PartitionLogInfoArray arr;
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            msg.get_sender(),
            msg.get_trans_param(),
            prepare_log_id,
            ObClockGenerator::getClock(),
            self_,
            msg_status,
            msg.get_request_id(),
            arr))) {
      TRANS_LOG(WARN, "ObTransMsg init error", KR(ret));
    }
  } else if (OB_TRANS_2PC_PREPARE_REQUEST == msg.get_msg_type()) {
    msg_type = OB_TRANS_2PC_PREPARE_RESPONSE;
    uint64_t prepare_log_id = ObClockGenerator::getClock();
    int64_t prepare_log_timestamp = ObClockGenerator::getClock();
    msg_status = OB_TRANS_STATE_UNKNOWN;
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            msg.get_sender(),
            msg.get_scheduler(),
            msg.get_coordinator(),
            msg.get_participants(),
            msg.get_trans_param(),
            prepare_log_id,
            prepare_log_timestamp,
            self_,
            msg_status,
            Ob2PCState::PREPARE,
            trans_version,
            msg.get_request_id(),
            msg.get_partition_log_info_arr(),
            0,
            msg.get_app_trace_info(),
            msg.get_xid(),
            msg.is_xa_prepare()))) {
      TRANS_LOG(WARN, "ObTransMsg init error", KR(ret));
    }
  } else if (OB_TRANS_2PC_PRE_COMMIT_REQUEST == msg.get_msg_type()) {
    msg_type = OB_TRANS_2PC_PRE_COMMIT_RESPONSE;
    trans_version = msg.get_trans_version();
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            msg.get_sender(),
            msg.get_scheduler(),
            msg.get_coordinator(),
            msg.get_participants(),
            msg.get_trans_param(),
            trans_version,
            self_,
            msg.get_request_id(),
            msg.get_partition_log_info_arr(),
            msg_status))) {
      TRANS_LOG(WARN, "ObTransMsg init error", KR(ret));
    }
  } else if (OB_TRANS_2PC_COMMIT_REQUEST == msg.get_msg_type()) {
    msg_type = OB_TRANS_2PC_COMMIT_RESPONSE;
    trans_version = msg.get_trans_version();
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            msg.get_sender(),
            msg.get_scheduler(),
            msg.get_coordinator(),
            msg.get_participants(),
            msg.get_trans_param(),
            trans_version,
            self_,
            msg.get_request_id(),
            msg.get_partition_log_info_arr(),
            msg_status))) {
      TRANS_LOG(WARN, "ObTransMsg init error", KR(ret));
    }
  } else if (OB_TRANS_2PC_ABORT_REQUEST == msg.get_msg_type()) {
    msg_type = OB_TRANS_2PC_ABORT_RESPONSE;
    msg_status = OB_TRANS_STATE_UNKNOWN;
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            msg.get_sender(),
            msg.get_scheduler(),
            msg.get_coordinator(),
            msg.get_participants(),
            msg.get_trans_param(),
            self_,
            msg.get_request_id(),
            msg_status))) {
      TRANS_LOG(WARN, "ObTransMsg init error", KR(ret));
    }
  } else if (OB_TRANS_2PC_CLEAR_REQUEST == msg.get_msg_type()) {
    msg_type = OB_TRANS_2PC_CLEAR_RESPONSE;
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            msg.get_sender(),
            msg.get_scheduler(),
            msg.get_coordinator(),
            msg.get_participants(),
            msg.get_trans_param(),
            self_,
            msg_status,
            msg.get_request_id(),
            MonotonicTs(0)))) {
      TRANS_LOG(WARN, "ObTransMsg init error", K(ret));
    }
  } else if (OB_TRANS_2PC_COMMIT_CLEAR_REQUEST == msg.get_msg_type()) {
    msg_type = OB_TRANS_2PC_COMMIT_CLEAR_RESPONSE;
    if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
            msg.get_trans_id(),
            msg_type,
            msg.get_trans_time(),
            msg.get_receiver(),
            msg.get_sender(),
            msg.get_scheduler(),
            msg.get_coordinator(),
            msg.get_participants(),
            msg.get_trans_param(),
            self_,
            msg_status,
            msg.get_request_id(),
            MonotonicTs(0)))) {
      TRANS_LOG(WARN, "ObTransMsg init error", K(ret));
    }
  } else {
    TRANS_LOG(ERROR, "invalid message type", K(msg));
    ret = OB_ERR_UNEXPECTED;
  }

  return ret;
}

int ObTransService::register_prepare_changing_leader_task(
    const ObPartitionKey& pkey, const ObAddr& proposal_leader, const int64_t ts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!pkey.is_valid() || !proposal_leader.is_valid() || 0 >= ts) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey), K(proposal_leader), K(ts));
  } else {
    ObPrepareChangingLeaderTask* task = NULL;
    const int64_t delay = ts - ObTimeUtility::current_time();
    int64_t txn_cnt = 0;
    if (delay <= 0) {
      TRANS_LOG(WARN, "changing leader task expired, ignore it", K(pkey), K(ts));
    } else if (OB_FAIL(part_trans_ctx_mgr_.get_active_read_write_count(pkey, txn_cnt))) {
      TRANS_LOG(WARN, "get active read write count failed", K(ret), K(pkey));
    } else {
      int64_t round = MAX(1, (txn_cnt + CHANGING_LEADER_TXN_PER_ROUND - 1) / CHANGING_LEADER_TXN_PER_ROUND);

      for (int64_t i = 0; i < round && OB_SUCC(ret); i++) {
        if (OB_ISNULL(task = op_reclaim_alloc(ObPrepareChangingLeaderTask))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TRANS_LOG(ERROR, "alloc ObPrepareChangingLeaderTask failed", KR(ret));
        } else if (OB_FAIL(task->init(ts, this, pkey, proposal_leader, round, i))) {
          TRANS_LOG(WARN, "changing leader task init failed", KR(ret), K(round), K(i));
        } else if (OB_FAIL(timer_.register_timeout_task(*task, delay))) {
          TRANS_LOG(WARN, "register changing leader task failed", KR(ret), K(delay));
        }

        if (OB_FAIL(ret) && NULL != task) {
          op_reclaim_free(task);
          task = NULL;
        }
      }

      STORAGE_LOG(INFO, "register prepare changing leader task success", K(pkey), K(ts), K(round), K(txn_cnt));
    }
  }
  return ret;
}

int ObTransService::prepare_changing_leader(
    const ObPartitionKey& pkey, const ObAddr& proposal_leader, const int64_t round, const int64_t cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!pkey.is_valid() || !proposal_leader.is_valid() || round < 1 || cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey), K(proposal_leader), K(round), K(cnt));
  } else {
    TRANS_LOG(INFO, "prepare changing leader start", KR(ret), K(pkey), K(proposal_leader), K(round), K(cnt));
    if (OB_FAIL(part_trans_ctx_mgr_.prepare_changing_leader(pkey, proposal_leader, round, cnt))) {
      TRANS_LOG(WARN, "prepare changing leader failed", KR(ret), K(pkey), K(proposal_leader));
    }
  }

  return ret;
}

bool ObTransService::check_is_multi_partition_update_stmt_(
    const ObTransDesc& trans_desc, const ObPartitionArray& paritions, const ObStmtDesc& stmt_desc)
{
  int bool_ret = false;

  if (sql::stmt::T_UPDATE == stmt_desc.stmt_type_ && paritions.count() > 1 &&
      OB_SYS_TENANT_ID != trans_desc.get_tenant_id()) {
    bool_ret = true;
  }

  return bool_ret;
}

bool ObTransService::can_create_ctx_(const int64_t trx_start_ts, const ObTsWindows& changing_leader_windows)
{
  bool bool_ret = true;
  const int64_t start_ts = changing_leader_windows.get_start();
  const int64_t end_ts = changing_leader_windows.get_end();
  const int64_t size = changing_leader_windows.get_left_size();
  const int64_t cur_ts = ObTimeUtility::current_time();
  if (!GCONF.enable_smooth_leader_switch) {
    bool_ret = (trx_start_ts < changing_leader_windows.get_start() || trx_start_ts > changing_leader_windows.get_end());
  } else {
    if (trx_start_ts > start_ts && trx_start_ts < end_ts) {
      bool_ret = false;
    } else {
      if (cur_ts >= start_ts + size / 3 && cur_ts < end_ts) {
        bool_ret = false;
      }
    }
  }

  return bool_ret;
}

int ObTransService::handle_trans_ask_scheduler_status_request_(const ObTransMsg& msg, const int status)
{
  int ret = OB_SUCCESS;
  ObTransMsg ret_msg;

  if (OB_FAIL(ret_msg.init(msg.get_tenant_id(),
          msg.get_trans_id(),
          OB_TRANS_ASK_SCHEDULER_STATUS_RESPONSE,
          msg.get_trans_time(),
          SCHE_PARTITION_ID,
          msg.get_sender(),
          msg.get_trans_param(),
          self_,
          self_,
          status))) {
    TRANS_LOG(WARN, "trans msg init error", KR(ret), K(msg));
  } else if (OB_FAIL(rpc_->post_trans_msg(
                 msg.get_tenant_id(), msg.get_sender_addr(), ret_msg, OB_TRANS_ASK_SCHEDULER_STATUS_RESPONSE))) {
    TRANS_LOG(WARN, "post transaction message error", KR(ret), K(msg), K(ret_msg));
  } else {
    TRANS_LOG(DEBUG, "handle ask scheduler request", K(msg), K(status));
  }

  return ret;
}

int ObTransService::handle_elr_callback_(const int64_t task_type, const ObPartitionKey& partition,
    const ObTransID& trans_id, const ObTransID& prev_or_next_trans_id, const int state)
{
  int ret = OB_SUCCESS;
  const bool for_replay = false;
  const bool is_readonly = false;
  const bool is_bounded_staleness_read = false;
  const bool need_completed_dirty_txn = false;
  bool alloc = false;
  ObTransCtx* ctx = NULL;
  ObPartTransCtx* part_ctx = NULL;

  if (OB_UNLIKELY(!partition.is_valid() || !trans_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(partition), K(trans_id));
  } else if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
                 trans_id,
                 for_replay,
                 is_readonly,
                 is_bounded_staleness_read,
                 need_completed_dirty_txn,
                 alloc,
                 ctx))) {
    if (EXECUTE_COUNT_PER_SEC(1)) {
      TRANS_LOG(WARN,
          "get trans ctx error",
          KR(ret),
          K(partition),
          "called_trans_id",
          trans_id,
          "caller_trans_id",
          prev_or_next_trans_id);
    }
  } else {
    part_ctx = static_cast<ObPartTransCtx*>(ctx);
    if (OB_FAIL(part_ctx->elr_next_trans_callback(task_type, prev_or_next_trans_id, state))) {
      TRANS_LOG(WARN,
          "elr prev trans callback error",
          KR(ret),
          K(partition),
          K(trans_id),
          K(prev_or_next_trans_id),
          K(state),
          K(*part_ctx));
    }
    (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
  }

  return ret;
}

int ObTransService::fetch_trans_ctx_by_ctx_id(const uint32_t ctx_id, ObTransCtx*& ctx)
{
  int ret = OB_SUCCESS;

  memtable::MemtableIDMap& id_map = mt_ctx_factory_.get_id_map();
  memtable::ObIMemtableCtx* memtable_ctx = NULL;

  if (OB_UNLIKELY(NULL == (memtable_ctx = id_map.fetch(ctx_id)))) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_UNLIKELY(NULL == (ctx = memtable_ctx->get_trans_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans ctx not exit", KR(ret), K(ctx_id));
  } else {
    // do nothing
  }

  return ret;
}

void ObTransService::revert_trans_ctx_by_ctx_id(const uint32_t ctx_id)
{
  memtable::MemtableIDMap& id_map = mt_ctx_factory_.get_id_map();
  id_map.revert(ctx_id);
}

int ObTransService::get_partition_audit_info(const common::ObPartitionKey& partition, ObPartitionAuditInfo& info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(part_trans_ctx_mgr_.get_partition_audit_info(partition, info))) {
    TRANS_LOG(WARN, "get partition audit info", KR(ret), K(partition));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransService::set_partition_audit_base_row_count(const ObPartitionKey& partition, const int64_t count)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(part_trans_ctx_mgr_.set_partition_audit_base_row_count(partition, count))) {
    TRANS_LOG(WARN, "get partition audit info", KR(ret), K(partition));
  } else {
    // do nothing
  }

  return ret;
}

bool ObTransService::multi_tenant_uncertain_phy_plan_(const ObStmtDesc& stmt_desc, const ObPartitionArray& participants)
{
  int bool_ret = false;

  if (stmt_desc.phy_plan_type_ == OB_PHY_PLAN_UNCERTAIN) {
    int64_t tenant_id = stmt_desc.stmt_tenant_id_;
    for (int i = 0; i < participants.count(); i++) {
      if (participants.at(i).get_tenant_id() != tenant_id) {
        bool_ret = true;
        break;
      }
    }
  }

  return bool_ret;
}

int ObTransService::handle_redo_sync_task_(ObDupTableRedoSyncTask* task, bool& need_release_task)
{
  int ret = OB_SUCCESS;
  const bool for_replay = false;
  const bool is_readonly = false;
  const bool is_bounded_staleness_read = false;
  const bool need_completed_dirty_txn = false;
  bool alloc = false;
  const bool batch_committed = false;
  ObTransCtx* ctx = NULL;
  ObPartTransCtx* part_ctx = NULL;
  need_release_task = true;

  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(task));
  } else if (OB_UNLIKELY(!task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(*task));
  } else if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(task->get_partition(),
                 task->get_trans_id(),
                 for_replay,
                 is_readonly,
                 is_bounded_staleness_read,
                 need_completed_dirty_txn,
                 alloc,
                 ctx))) {
    TRANS_LOG(ERROR, "unexpected get trans ctx error", K(ret), K(*task));
  } else {
    part_ctx = static_cast<ObPartTransCtx*>(ctx);
    // when this replica prepare leader revoke, need callback transaction
    // layer log sync success, otherwise leader revoke will be blocked
    if (part_ctx->is_prepare_leader_revoke() || (task->is_mask_set_ready() && part_ctx->is_redo_log_sync_finish())) {
      if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
        TRANS_LOG(INFO, "[DUP TABLE]duplicate table redo sync statisic", K(task->get_used_time()));
      }
      if (OB_FAIL(part_ctx->on_sync_log_success(
              task->get_log_type(), task->get_log_id(), task->get_timestamp(), batch_committed))) {
        TRANS_LOG(WARN, "on_sync_log_success error", KR(ret), K(*task));
      }
    } else if (OB_FAIL(part_ctx->retry_redo_sync_task(
                   task->get_log_id(), task->get_log_type(), task->get_timestamp(), false))) {
      TRANS_LOG(WARN, "retry redo sync task error", K(ret), K(*task));
      // retry when send request fail
      need_release_task = false;
    } else {
      need_release_task = false;
    }
    (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
  }

  return ret;
}

int ObTransService::update_dup_table_partition_info(const ObPartitionKey& pkey, const bool is_duplicated)
{
  int ret = OB_SUCCESS;
  ObDupTableLeaseTask* task = NULL;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!pkey.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(pkey));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCC(dup_table_lease_task_map_.get(pkey, task))) {
    if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "tup table lease task is NULL", KR(ret));
    } else if (!is_duplicated && OB_FAIL(dup_table_lease_task_map_.del(pkey))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        TRANS_LOG(WARN, "erase lease task from hashmap error", KR(ret), K(pkey));
      }
    } else {
      // do nothing
    }
    dup_table_lease_task_map_.revert(task);
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    TRANS_LOG(WARN, "get dup table lease task error", KR(ret), K(pkey));
  } else if (is_duplicated) {
    if (OB_FAIL(part_trans_ctx_mgr_.init_dup_table_mgr(pkey))) {
      TRANS_LOG(WARN, "failed to init dup table mgr", KR(ret), K(pkey));
    } else if (OB_FAIL(dup_table_lease_task_map_.create(pkey, task))) {
      TRANS_LOG(WARN, "create dup table lease task error", KR(ret), K(pkey));
    } else {
      int64_t delay = 1;
      if (OB_FAIL(task->init(pkey, this))) {
        TRANS_LOG(WARN, "dup table lease task init error", K(ret), K(pkey));
      } else if (OB_FAIL(dup_table_lease_timer_.register_timeout_task(*task, delay))) {
        TRANS_LOG(WARN, "register dup table lease task failed", K(ret), K(delay));
      } else {
        TRANS_LOG(DEBUG, "register dup table lease task success", K(delay));
      }
      if (OB_FAIL(ret)) {
        dup_table_lease_task_map_.revert(task);
        (void)dup_table_lease_task_map_.del(pkey);
      }
    }
  } else {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObTransService::send_dup_table_lease_request_msg(const ObPartitionKey& pkey, ObDupTableLeaseTask* task)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!pkey.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(pkey));
    ret = OB_INVALID_ARGUMENT;
  } else {
    storage::ObIPartitionGroupGuard pkey_guard;
    storage::ObIPartitionGroup* pg = NULL;
    common::ObAddr leader_addr;
    common::ObTimeGuard tg("dup_table_lease_send", 10 * 1000);

    if (GCTX.is_standby_cluster()) {
      // do nothing
    } else if (OB_FAIL(partition_service_->get_partition(pkey, pkey_guard))) {
      TRANS_LOG(WARN, "get partition failed", K(ret), K(pkey));
    } else if (OB_ISNULL(pg = pkey_guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "partition is null, unexpected error");
    } else if (OB_FAIL(location_adapter_->nonblock_get_strong_leader(pkey, leader_addr))) {
      TRANS_LOG(WARN, "get partition leader failed", K(ret), K(pkey));
      const int64_t expire_renew_time = 0;
      (void)location_adapter_->nonblock_renew(pkey, expire_renew_time);
    } else {
      ObDupTableLeaseRequestMsg msg;
      uint64_t cur_log_id = pg->get_min_replayed_log_id();
      const int64_t request_ts = ObTimeUtility::current_time();
      bool need_renew_lease = false;
      bool need_refresh_location = false;
      if (OB_FAIL(part_trans_ctx_mgr_.send_dup_table_lease_request_msg(
              pkey, cur_log_id, need_renew_lease, need_refresh_location))) {
        TRANS_LOG(WARN, "send dup table lease request msg error", K(ret), K(pkey));
      } else {
        if (need_renew_lease) {
          if (OB_FAIL(msg.init(
                  request_ts, pkey, self_, cur_log_id, ObTransService::DEFAULT_DUP_TABLE_LEASE_TIMEOUT_INTERVAL_US))) {
            TRANS_LOG(WARN, "ObDupTableLeaseRequestMsg init errir", K(ret), K(msg));
          } else if (OB_FAIL(msg.set_header(self_, self_, leader_addr))) {
            TRANS_LOG(WARN, "set header error", K(ret), K(msg));
          } else if (OB_FAIL(dup_table_rpc_.post_dup_table_lease_request(pkey.get_tenant_id(), leader_addr, msg))) {
            TRANS_LOG(WARN, "post dup table lease message error", K(ret), K(msg));
          } else {
            TRANS_LOG(DEBUG, "send dup table lease request", K(msg));
          }
        }

        if (need_refresh_location) {
          const int64_t expire_renew_time = 0;
          (void)location_adapter_->nonblock_renew(pkey, expire_renew_time);
        }
      }
    }
    tg.click();

    (void)dup_table_lease_timer_.unregister_timeout_task(*task);
    dup_table_lease_task_map_.revert(task);
    ObDupTableLeaseTask* tmp_task = NULL;
    int64_t delay = DUP_TABLE_LEASE_INTERVAL_US;
    if (OB_FAIL(dup_table_lease_task_map_.get(pkey, tmp_task))) {
      TRANS_LOG(WARN, "get dup table lease task error", K(ret), K(pkey));
    } else if (OB_FAIL(dup_table_lease_timer_.register_timeout_task(*tmp_task, delay))) {
      TRANS_LOG(WARN, "register dup table lease task failed", K(ret), K(delay));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObTransService::check_duplicated_partition(const ObPartitionKey& pkey, bool& is_duplicated_partition)
{
  int ret = dup_table_lease_task_map_.contains_key(pkey);

  if (OB_ENTRY_EXIST == ret) {
    is_duplicated_partition = true;
    ret = OB_SUCCESS;
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    is_duplicated_partition = false;
    ret = OB_SUCCESS;
  } else {
    is_duplicated_partition = false;
  }

  return ret;
}

int ObTransService::iterate_duplicate_partition_stat(
    const common::ObPartitionKey& partition, ObDuplicatePartitionStatIterator& iter)
{
  int ret = OB_SUCCESS;
  bool is_duplicated_partition = false;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "partition is invalid", KR(ret), K(partition));
  } else if (OB_FAIL(check_duplicated_partition(partition, is_duplicated_partition))) {
    TRANS_LOG(WARN, "check duplicated partition error", KR(ret), K(partition));
  } else if (is_duplicated_partition &&
             OB_FAIL(part_trans_ctx_mgr_.iterate_duplicate_partition_stat(partition, iter))) {
    TRANS_LOG(WARN, "iterate duplicate partition stat error", KR(ret), K(partition));
  } else if (OB_FAIL(iter.set_ready())) {
    TRANS_LOG(WARN, "ObDuplicatePartitionStatIterator set ready error", KR(ret), K(partition));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransService::iterate_trans_table(
    const ObPartitionKey& pg_key, const uint64_t end_log_id, blocksstable::ObMacroBlockWriter& writer)
{
  int ret = OB_SUCCESS;
  transaction::ObPartitionTransCtxMgr* trans_ctx_mgr = NULL;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(trans_ctx_mgr = part_trans_ctx_mgr_.get_partition_trans_ctx_mgr(pg_key))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get partition trans ctx mgr", K(ret));
  } else if (OB_FAIL(trans_ctx_mgr->iterate_trans_table(end_log_id, writer))) {
    STORAGE_LOG(WARN, "failed to iterate trans table", K(ret), K(pg_key));
  }
  return ret;
}

/*
 * safety check: user specified snapshot less than gts
 * FIXME: this will hang current thread during get gts value,
 *        its better to do this job at start_stmt phase
 */
int ObTransService::check_user_specified_snapshot_version(
    const ObTransDesc& trans_desc, const int64_t user_specified_snapshot_version, bool& is_snapshot_valid)
{
  int ret = OB_SUCCESS;

  if (user_specified_snapshot_version > 0) {
    int64_t gts = 0;
    uint64_t tenant_id = trans_desc.get_tenant_id();
    int64_t trans_expire_time = trans_desc.get_trans_expired_time();
    int64_t stmt_expire_time = trans_desc.get_cur_stmt_expired_time();
    MonotonicTs receive_gts_ts;
    is_snapshot_valid = true;

    if (OB_FAIL(get_gts_(gts, receive_gts_ts, trans_expire_time, stmt_expire_time, tenant_id))) {
      STORAGE_LOG(WARN, "get gts failed", K(ret), K(tenant_id), K(trans_expire_time), K(stmt_expire_time));
    } else {
      if (user_specified_snapshot_version > gts) {
        ret = OB_INVALID_QUERY_TIMESTAMP;
        STORAGE_LOG(
            WARN, "snapshot version is too new, cannot read", KR(ret), K(user_specified_snapshot_version), K(gts));
      }
    }
  }
  return ret;
}

int ObTransService::check_user_specified_snapshot_version(
    const ObStandaloneStmtDesc& desc, const int64_t user_specified_snapshot_version)
{
  int ret = OB_SUCCESS;
  int64_t gts = 0;
  const uint64_t tenant_id = desc.get_tenant_id();
  // disable trans_expire_time verify for read-only stmt
  const int64_t trans_expire_time = INT64_MAX;
  const int64_t stmt_expire_time = desc.get_stmt_expired_time();
  MonotonicTs receive_gts_ts;

  if (user_specified_snapshot_version > 0) {

    if (OB_FAIL(get_gts_(gts, receive_gts_ts, trans_expire_time, stmt_expire_time, tenant_id))) {
      STORAGE_LOG(WARN, "get gts failed", K(ret), K(tenant_id), K(trans_expire_time), K(stmt_expire_time));
    } else {
      if (user_specified_snapshot_version > gts) {
        ret = OB_INVALID_QUERY_TIMESTAMP;
        STORAGE_LOG(
            WARN, "snapshot version is too new, cannot read", KR(ret), K(user_specified_snapshot_version), K(gts));
      }
    }
  }
  return ret;
}

int ObTransService::savepoint(ObTransDesc& trans_desc, const common::ObString& sp_id)
{
  int ret = OB_SUCCESS;
  ObTransTraceLog& tlog = trans_desc.get_tlog();
  if (trans_desc.is_trx_idle_timeout()) {
    TRANS_LOG(WARN, "transaction idle timeout", K(ret), K(sp_id), K(trans_desc));
    ret = OB_TRANS_NEED_ROLLBACK;
  } else if (OB_FAIL(trans_desc.push_savepoint(sp_id, trans_desc.get_sql_no()))) {
    TRANS_LOG(WARN, "push savepoint error", K(ret), K(sp_id), K(trans_desc));
  }
  if (EXECUTE_COUNT_PER_SEC(10)) {
    TRANS_LOG(INFO, "create savepoint", K(ret), K(trans_desc));
  }

  REC_TRACE_EXT(tlog, savepoint, Y(ret), OB_ID(sql_no), trans_desc.get_sql_no());
  return ret;
}

int ObTransService::do_savepoint_rollback_(
    ObTransDesc& trans_desc, const int64_t sql_no, const ObPartitionArray& rollback_partitions)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const bool for_replay = false;
  const bool is_readonly = false;
  bool alloc = false;
  ObTransCtx* ctx = NULL;
  ObPartTransCtx* part_ctx = NULL;
  const bool is_bounded_staleness_read = false;
  const bool need_completed_dirty_txn = false;

  if (rollback_partitions.count() > 0) {
    if (trans_desc.is_sp_trans()) {
      if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(rollback_partitions.at(0),
              trans_desc.get_trans_id(),
              for_replay,
              is_readonly,
              is_bounded_staleness_read,
              need_completed_dirty_txn,
              alloc,
              ctx))) {
        TRANS_LOG(WARN, "get transaction context error", KR(ret), K(rollback_partitions), K(trans_desc));
      } else {
        part_ctx = static_cast<ObPartTransCtx*>(ctx);
        if (OB_ISNULL(part_ctx)) {
          tmp_ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "part ctx is NULL", KR(tmp_ret));
        } else if (OB_FAIL(part_ctx->handle_savepoint_rollback_request(sql_no, trans_desc.get_sql_no(), false))) {
          // switch transaction type to DIST_TRANS if partition leader has shifted
          if (OB_NOT_MASTER == ret) {
            trans_desc.set_trans_type(TransType::DIST_TRANS);
            if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = recover_sche_ctx_(trans_desc, part_ctx)))) {
              TRANS_LOG(WARN, "recover scheduler context error", KR(tmp_ret), K(trans_desc));
              (void)static_cast<ObTransDesc&>(trans_desc).set_sche_ctx(NULL);
            } else {
              // recover participants info
              part_ctx->recover_dist_trans(self_);
              (void)part_trans_ctx_mgr_.revert_trans_ctx(part_ctx);
              TRANS_LOG(INFO, "switch sp_trans to dist trans success", K(trans_desc));
            }
          }
        } else {
          // do nothing
        }
        // retry for logging and leader switched participant state
        if (OB_UNLIKELY(OB_SUCCESS != ret && OB_NOT_MASTER != ret && OB_TRANS_STMT_NEED_RETRY != ret)) {
          trans_desc.set_need_rollback();
        }
        (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
      }
    } else if (OB_FAIL(do_dist_rollback_(trans_desc, sql_no, rollback_partitions))) {
      TRANS_LOG(WARN, "fail to do rollback", KR(ret), K(trans_desc), K(sql_no), K(rollback_partitions));
    }
  }

  TRANS_LOG(DEBUG, "do savepoint rollback", K(ret), K(trans_desc), K(sql_no), K(rollback_partitions));
  return ret;
}

int ObTransService::rollback_savepoint(
    ObTransDesc& trans_desc, const common::ObString& sp_id, const ObStmtParam& stmt_param)
{
  int ret = OB_SUCCESS;
  const int64_t expired_time = stmt_param.get_stmt_expired_time();
  ObTransTraceLog& tlog = trans_desc.get_tlog();
  int64_t sql_no = -1;
  ObPartitionArray rollback_partitions;
  if (trans_desc.is_trx_idle_timeout()) {
    TRANS_LOG(WARN, "transaction idle timeout", K(ret), K(sp_id), K(trans_desc));
    ret = OB_TRANS_NEED_ROLLBACK;
  } else if (OB_FAIL(trans_desc.set_cur_stmt_expired_time(expired_time))) {
    TRANS_LOG(WARN, "set statement expired time error", KR(ret), K(trans_desc), K(expired_time));
  } else if (OB_FAIL(trans_desc.get_savepoint_rollback_info(sp_id, sql_no, rollback_partitions))) {
    TRANS_LOG(WARN, "get savepoint rollback info failed", K(ret));
  } else if (OB_FAIL(do_savepoint_rollback_(trans_desc, sql_no, rollback_partitions))) {
    TRANS_LOG(WARN, "do savepoint rollback error", K(ret));
  } else if (OB_FAIL(trans_desc.truncate_savepoint(sp_id))) {
    TRANS_LOG(WARN, "truncate savepoint failed", K(ret));
  } else {
    TRANS_LOG(DEBUG, "rollback savepoint success", K(trans_desc), K(sp_id));
  }
  if (EXECUTE_COUNT_PER_SEC(10)) {
    TRANS_LOG(INFO, "rollback savepoint", K(ret), K(trans_desc), K(sp_id));
  }

  REC_TRACE_EXT(tlog, rollback_savepoint, Y(ret), OB_ID(sql_no), trans_desc.get_sql_no());
  return ret;
}

int ObTransService::release_savepoint(ObTransDesc& trans_desc, const common::ObString& sp_id)
{
  int ret = OB_SUCCESS;
  ObTransTraceLog& tlog = trans_desc.get_tlog();
  if (trans_desc.is_trx_idle_timeout()) {
    TRANS_LOG(WARN, "transaction idle timeout", K(ret), K(sp_id), K(trans_desc));
    ret = OB_TRANS_NEED_ROLLBACK;
  } else if (OB_FAIL(trans_desc.truncate_savepoint(sp_id))) {
    TRANS_LOG(WARN, "truncate savepoint error", K(ret), K(sp_id), K(trans_desc));
  } else if (OB_FAIL(trans_desc.del_savepoint(sp_id))) {
    TRANS_LOG(WARN, "del savepoint error", KR(ret), K(sp_id), K(trans_desc));
  } else {
    // do nothing
  }
  if (EXECUTE_COUNT_PER_SEC(10)) {
    TRANS_LOG(INFO, "release savepoint success", K(trans_desc), K(sp_id));
  }

  REC_TRACE_EXT(tlog, release_savepoint, Y(ret), OB_ID(sql_no), trans_desc.get_sql_no());
  return ret;
}

int ObTransService::xa_rollback_all_changes(ObTransDesc& trans_desc, const ObStmtParam& stmt_param)
{
  int ret = OB_SUCCESS;
  const int64_t expired_time = stmt_param.get_stmt_expired_time();
  const ObPartitionArray& rollback_partitions = trans_desc.get_participants();
  const int64_t sql_no = 0;
  ObScheTransCtx* sche_ctx = trans_desc.get_sche_ctx();
  const ObXATransID& xid = trans_desc.get_xid();

  if (!trans_desc.is_valid() || !trans_desc.is_xa_local_trans()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(trans_desc));
  } else if (OB_ISNULL(sche_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa trans sche ctx is null", K(ret), K(*sche_ctx), K(trans_desc));
  } else if (OB_FAIL(trans_desc.set_cur_stmt_expired_time(expired_time))) {
    TRANS_LOG(WARN, "set statement expired time error", KR(ret), K(trans_desc), K(expired_time));
  } else if (sche_ctx->is_xa_tightly_coupled()) {
    // tight couple
    if (self_ == trans_desc.get_orig_scheduler()) {
      if (OB_FAIL(sche_ctx->check_for_xa_execution(false /*is_new_branch*/, xid))) {
        if (OB_TRANS_XA_BRANCH_FAIL == ret) {
          TRANS_LOG(INFO, "xa trans has terminated", K(ret), K(trans_desc));
        } else {
          TRANS_LOG(WARN, "unexpected scheduler for xa execution", K(ret), K(*sche_ctx));
        }
      } else {
        int64_t retry_times = 0;
        while (OB_FAIL(sche_ctx->xa_try_global_lock(xid)) && retry_times++ < 100) {
          usleep(1000);  // 1ms
        }
        if (OB_SUCC(ret)) {
          sche_ctx->set_tmp_trans_desc(trans_desc);
          if (OB_FAIL(sche_ctx->xa_sync_status_response(*(sche_ctx->get_trans_desc()), true))) {
            TRANS_LOG(WARN, "local pull trans desc failed", K(ret), K(trans_desc));
          } else {
            trans_desc.set_xid(xid);
            trans_desc.set_sche_ctx(sche_ctx);
            sche_ctx->set_xid(xid);
            if (OB_FAIL(do_savepoint_rollback_(trans_desc, sql_no, rollback_partitions))) {
              TRANS_LOG(WARN, "do savepoint rollback error", K(ret), K(trans_desc));
            }
          }
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = sche_ctx->xa_release_global_lock(xid))) {
            TRANS_LOG(WARN, "release xa global lock failed", K(tmp_ret), K(trans_desc));
            ret = tmp_ret;
          }
        } else {
          TRANS_LOG(WARN, "try xa global lock failed", K(ret), K(trans_desc));
        }
      }
    } else {
      if (sche_ctx->is_terminated()) {
        ret = OB_TRANS_XA_BRANCH_FAIL;
        TRANS_LOG(INFO, "xa trans has terminated", K(ret), K(trans_desc));
      } else {
        int64_t retry_times = 0;
        do {
          if (OB_SUCCESS != (ret = xa_try_remote_lock_(trans_desc))) {
            if (OB_TRANS_STMT_NEED_RETRY == ret) {
              usleep(1000);  // 1ms
            } else if (OB_TRANS_XA_BRANCH_FAIL == ret || OB_TRANS_CTX_NOT_EXIST == ret) {
              TRANS_LOG(INFO, "original scheduler has terminated", K(ret), K(trans_desc));
              sche_ctx->set_terminated();
              ret = OB_TRANS_XA_BRANCH_FAIL;
              break;
            } else {
              TRANS_LOG(WARN, "try remote lock failed", K(ret), K(trans_desc));
            }
          }
        } while (OB_TRANS_STMT_NEED_RETRY == ret && retry_times++ < 100);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(do_savepoint_rollback_(trans_desc, sql_no, rollback_partitions))) {
            TRANS_LOG(WARN, "do savepoint rollback error", K(ret), K(trans_desc));
          }
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = xa_release_remote_lock_(trans_desc))) {
            TRANS_LOG(WARN, "release remote lock failed", K(tmp_ret), K(trans_desc));
            ret = tmp_ret;
          }
        } else {
          TRANS_LOG(WARN, "try xa global lock failed", K(ret), K(trans_desc));
        }
      }
    }
    if (OB_TRANS_XA_BRANCH_FAIL == ret) {
      // (void)clear_branch_for_xa_terminate_(trans_desc, sche_ctx, xid);
      ret = OB_SUCCESS;
    }
  } else {
    // loose couple
    if (OB_FAIL(do_savepoint_rollback_(trans_desc, sql_no, rollback_partitions))) {
      TRANS_LOG(WARN, "do savepoint rollback error", K(ret), K(trans_desc));
    }
  }
  return ret;
}

int ObTransService::remove_callback_for_uncommited_txn(ObMemtable* mt)
{
  int ret = OB_SUCCESS;
  ObPartitionKey pkey;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_ISNULL(mt)) {
    TRANS_LOG(WARN, "memtable is NULL");
    ret = OB_INVALID_ARGUMENT;
  } else if (FALSE_IT(pkey = mt->get_key().pkey_)) {
  } else if (!pkey.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected partition key", KR(ret), K(pkey), KP(mt));
  } else if (OB_FAIL(part_trans_ctx_mgr_.remove_callback_for_uncommited_txn(pkey, mt))) {
    TRANS_LOG(WARN, "participant remove callback for uncommitt txn failed", KR(ret), K(pkey), KP(mt));
  } else {
    TRANS_LOG(DEBUG, "participant remove callback for uncommitt txn success", K(pkey), KP(mt));
  }

  return ret;
}

int ObTransService::remove_mem_ctx_for_trans_ctx(memtable::ObMemtable* mt)
{
  int ret = OB_SUCCESS;
  ObPartitionKey pkey;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_ISNULL(mt)) {
    TRANS_LOG(WARN, "memtable is NULL");
    ret = OB_INVALID_ARGUMENT;
  } else if (FALSE_IT(pkey = mt->get_key().pkey_)) {
  } else if (!pkey.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected partition key", KR(ret), K(pkey), KP(mt));
  } else if (OB_FAIL(part_trans_ctx_mgr_.remove_mem_ctx_for_trans_ctx(pkey, mt))) {
    TRANS_LOG(WARN, "participant remove mem ctx for txn ctx failed", KR(ret), K(pkey), KP(mt));
  } else {
    TRANS_LOG(DEBUG, "participant remove mem ctx for txn ctx success", K(pkey), KP(mt));
  }

  return ret;
}

int ObTransService::mark_trans_forbidden_sql_no(
    const ObTransID& trans_id, const ObPartitionKey& partition, const int64_t sql_no, bool& forbid_succ)
{
  int ret = OB_SUCCESS;
  UNUSED(trans_id);
  UNUSED(partition);
  UNUSED(sql_no);
  forbid_succ = true;
  /*
  const bool for_replay = false;
  const bool is_readonly = false;
  bool alloc = true;
  ObTransCtx *ctx = NULL;

  if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
                                                trans_id,
                                                for_replay,
                                                is_readonly,
                                                alloc,
                                                ctx))) {
    TRANS_LOG(WARN, "get trans ctx error", K(ret), K(partition), K(trans_id), K(sql_no));
  } else {
    ObPartTransCtx *part_ctx = static_cast<ObPartTransCtx*>(ctx);
    if (OB_FAIL(part_ctx->set_forbidden_sql_no(sql_no, forbid_succ))) {
      TRANS_LOG(WARN, "set forbidden sql no fail", K(ret), K(trans_id), K(partition), K(*part_ctx));
    }
    (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
    }*/

  return ret;
}

int ObTransService::is_trans_forbidden_sql_no(
    const ObTransID& trans_id, const common::ObPartitionKey& partition, const int64_t sql_no, bool& is_forbidden)
{
  int ret = OB_SUCCESS;
  UNUSED(trans_id);
  UNUSED(partition);
  UNUSED(sql_no);
  is_forbidden = false;
  /*
  const bool for_replay = false;
  const bool is_readonly = false;
  bool alloc = false;
  ObTransCtx *ctx = NULL;

  is_forbidden = false;
  if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
                                                trans_id,
                                                for_replay,
                                                is_readonly,
                                                alloc,
                                                ctx))) {
    if (OB_TRANS_CTX_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(WARN, "get trans ctx error", K(ret), K(partition), K(trans_id), K(sql_no));
    }
  } else {
    ObPartTransCtx *part_ctx = static_cast<ObPartTransCtx*>(ctx);
    if (part_ctx->get_forbidden_sql_no() > sql_no) {
      ret = OB_TRANS_SQL_SEQUENCE_ILLEGAL;
    } else if (part_ctx->get_forbidden_sql_no() == sql_no) {
      is_forbidden = true;
    } else {
      // do nothing
    }
    (void)part_trans_ctx_mgr_.revert_trans_ctx(ctx);
    }*/

  return ret;
}

int ObTransService::get_max_trans_version_before_given_log_ts(
    const ObPartitionKey& pkey, const int64_t log_ts, int64_t& max_trans_version, bool& is_all_rollback_trans)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "trans_service is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTranService is not running", K(ret));
  } else if (OB_FAIL(part_trans_ctx_mgr_.get_max_trans_version_before_given_log_ts(
                 pkey, log_ts, max_trans_version, is_all_rollback_trans))) {
    TRANS_LOG(WARN, "failed to get_max_trans_version_before_given_log_id", K(ret), K(pkey), K(log_ts));
  }
  return ret;
}

int ObTransService::clear_unused_trans_status(const ObPartitionKey& pg_key, const int64_t max_cleanout_log_ts)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(part_trans_ctx_mgr_.clear_unused_trans_status(pg_key, max_cleanout_log_ts))) {
    STORAGE_LOG(WARN, "failed to clear unused trans status", K(ret), K(pg_key), K(max_cleanout_log_ts));
  }
  return ret;
}

int ObTransService::has_terminated_trx_in_given_log_ts_range(
    const ObPartitionKey& pkey, const int64_t start_log_ts, const int64_t end_log_ts, bool& has_terminated_trx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "trans_service is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTranService is not running", K(ret));
  } else if (OB_FAIL(part_trans_ctx_mgr_.has_terminated_trx_in_given_log_ts_range(
                 pkey, start_log_ts, end_log_ts, has_terminated_trx))) {
    TRANS_LOG(WARN,
        "failed to check has_terminated_trx_in_given_log_id_range",
        K(ret),
        K(pkey),
        K(start_log_ts),
        K(end_log_ts));
  }
  return ret;
}

int ObTransService::get_stmt_snapshot_info(ObTransDesc& trans_desc, const int64_t specified_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObStandaloneStmtDesc& desc = trans_desc.get_standalone_stmt_desc();

  if (OB_UNLIKELY(desc.is_transaction_snapshot() && specified_snapshot_version > 0)) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "not support snapshot generation", K(desc), K(specified_snapshot_version));
  } else if (specified_snapshot_version > 0) {
    desc.set_snapshot_version(specified_snapshot_version);
  } else if (desc.is_transaction_snapshot() && desc.get_snapshot_version() != OB_INVALID_TIMESTAMP) {
    // reuse snapshot for transaction level snapshot
  } else if (desc.is_bounded_staleness_read()) {
    ObIWeakReadService* wrs = GCTX.weak_read_service_;
    int64_t snapshot_version = 0;
    if (OB_ISNULL(wrs)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "weak read service is invalid", K(ret), KP(wrs));
    } else {
      if (OB_FAIL(wrs->get_cluster_version(desc.get_tenant_id(), snapshot_version))) {
        TRANS_LOG(WARN, "get weak read cluster version fail", K(ret));
      } else {
        desc.set_snapshot_version(snapshot_version);
      }
    }
  } else if (desc.is_current_read()) {
    // only support gts
    if (desc.is_local_single_partition_stmt()) {
      if (!desc.get_first_pkey().is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected partition key", K(ret), K(desc));
      } else {
        // don't check partition epoch, delay to get_store_ctx routine
        // need get publish version
        const int64_t WAIT_GTS_US = 500;
        const uint64_t tenant_id = desc.get_tenant_id();
        int64_t gts = -1;
        int64_t begin_ts = ObClockGenerator::getClock();
        do {
          if (ObClockGenerator::getClock() >= desc.get_stmt_expired_time()) {
            ret = OB_TRANS_STMT_TIMEOUT;
          } else if (OB_FAIL(ts_mgr_->get_publish_version(tenant_id, gts))) {
            if (OB_EAGAIN != ret) {
              TRANS_LOG(WARN, "get gts failed", KR(ret));
            } else {
              usleep(WAIT_GTS_US);
            }
          } else if (0 >= gts) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(WARN, "invalid gts, unexpected error", KR(ret), K(gts));
          } else {
            desc.set_snapshot_version(gts);
          }
        } while (OB_EAGAIN == ret);
        // verify leader epoch
        if (OB_SUCC(ret)) {
          bool is_dup_table = false;
          int64_t leader_epoch = 0;
          if (OB_FAIL(check_partition_status_(desc, desc.get_first_pkey(), is_dup_table, leader_epoch, NULL))) {
            TRANS_LOG(WARN, "check partition status failed", K(ret), K(desc));
          } else if (is_dup_table) {
            // do nothing
          } else if (leader_epoch > begin_ts) {
            ret = OB_NOT_MASTER;
            TRANS_LOG(WARN, "get stmt snapshot version error", K(ret), K(is_dup_table), K(leader_epoch), K(desc));
          } else {
            // do nothing
          }
        }
      }
    } else if (!ts_mgr_->is_external_consistent(desc.get_tenant_id())) {
      ret = OB_NOT_SUPPORTED;
      TRANS_LOG(WARN, "fast select need gts", K(ret));
    } else {
      const int64_t WAIT_GTS_US = 500;
      const uint64_t tenant_id = desc.get_tenant_id();
      const MonotonicTs stc = MonotonicTs::current_time();
      int64_t gts = -1;
      MonotonicTs tmp_receive_gts_ts;
      do {
        if (ObClockGenerator::getClock() >= desc.get_stmt_expired_time()) {
          ret = OB_TRANS_STMT_TIMEOUT;
        } else if (OB_FAIL(ts_mgr_->get_gts(tenant_id, stc, NULL, gts, tmp_receive_gts_ts))) {
          if (OB_EAGAIN != ret) {
            TRANS_LOG(WARN, "get gts failed", KR(ret));
          } else {
            usleep(WAIT_GTS_US);
          }
        } else if (0 >= gts) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "invalid gts, unexpected error", KR(ret), K(gts));
        } else {
          desc.set_snapshot_version(gts);
        }
      } while (OB_EAGAIN == ret);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }

  return ret;
}

int ObTransService::check_partition_status_(const ObStandaloneStmtDesc& desc, const ObPartitionKey& partition,
    bool& is_dup_partition, int64_t& leader_epoch, ObIPartitionGroup* pg)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  const bool check_election = true;
  int status = OB_SUCCESS;
  ObTsWindows changing_leader_windows;
  is_dup_partition = false;

  if (desc.is_bounded_staleness_read()) {
    // do nothing
  } else if (desc.is_current_read()) {
    if (NULL == pg && OB_FAIL(get_partition_group_(partition, guard, pg))) {
      TRANS_LOG(WARN, "get partition group fail", K(ret), K(partition), KP(pg));
    } else if (OB_ISNULL(pg)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "invalid partition group", K(ret), K(partition), KP(pg));
    } else if (OB_FAIL(clog_adapter_->get_status(pg, check_election, status, leader_epoch, changing_leader_windows))) {
      TRANS_LOG(WARN, "get participant status error", K(ret), K(desc), K(partition));
    } else if (OB_SUCCESS != status) {
      ret = status;
    } else {
      // do nothing
    }
    if (OB_NOT_MASTER == ret) {
      int tmp_ret = OB_SUCCESS;
      int duplicate_partition_status = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = check_duplicate_partition_status_(partition, duplicate_partition_status))) {
        TRANS_LOG(WARN, "check duplicate partition status failed", K(tmp_ret), K(partition));
      } else if (OB_SUCCESS == duplicate_partition_status) {
        ret = duplicate_partition_status;
        is_dup_partition = true;
      } else {
        // do nothing
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }

  return ret;
}

int ObTransService::check_bounded_staleness_read_version_(
    const ObStandaloneStmtDesc& desc, const ObPartitionKey& pg_key)
{
  int ret = OB_SUCCESS;
  // check: specified snapshot greate than partition's max weak read snapshot
  storage::ObIPartitionGroupGuard pkey_guard;
  if (desc.get_snapshot_version() < 0 || !pg_key.is_valid() || OB_ISNULL(partition_service_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(pg_key), K(desc));
  } else if (OB_FAIL(partition_service_->get_partition(pg_key, pkey_guard))) {
    TRANS_LOG(WARN, "get partition failed", K(ret), K(pg_key), K(desc));
  } else if (OB_ISNULL(pkey_guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "partition is null, unexpected error", K(ret), KP(pkey_guard.get_partition_group()));
  } else if (OB_FAIL(pkey_guard.get_partition_group()->check_replica_ready_for_bounded_staleness_read(
                 desc.get_snapshot_version()))) {
    TRANS_LOG(WARN, "check replica ready for weak read failed", K(ret), K(pg_key), K(desc));
  } else {
    // do nothing
  }
  return ret;
}

int ObTransService::get_store_ctx_(const ObStandaloneStmtDesc& desc, const ObPartitionKey& pg_key,
    const int64_t user_specified_snapshot, ObStoreCtx& store_ctx)
{
  int ret = OB_SUCCESS;
  ObMemtableCtx* mt_ctx = NULL;
  const ObTransID& trans_id = desc.get_trans_id();
  const bool is_bounded_staleness_read = desc.is_bounded_staleness_read();
  const int64_t trx_lock_timeout = desc.get_trx_lock_timeout();
  const int64_t snapshot_version = desc.get_snapshot_version();
  bool updated = false;
  bool is_dup_table = false;
  int64_t leader_epoch = 0;

  if (OB_UNLIKELY(!desc.is_valid() || !pg_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(desc), K(pg_key));
  } else if (OB_UNLIKELY(desc.is_stmt_timeout())) {
    ret = OB_TRANS_STMT_TIMEOUT;
    TRANS_LOG(WARN, "statement is timeout", K(ret), K(desc), K(pg_key));
    // single partition read stmt check partition leader status when generate snapshot
  } else if (!desc.is_local_single_partition_stmt() &&
             OB_FAIL(check_partition_status_(desc, pg_key, is_dup_table, leader_epoch, NULL))) {
    TRANS_LOG(WARN, "check partition status failed", K(ret), K(desc), K(pg_key));
  } else if (user_specified_snapshot > 0 &&
             OB_FAIL(check_user_specified_snapshot_version(desc, user_specified_snapshot))) {
    // specified snapshot must greate than gts
    TRANS_LOG(WARN, "user specified snapshot too new", K(ret), K(desc));
  } else if (desc.is_bounded_staleness_read() && OB_FAIL(check_bounded_staleness_read_version_(desc, pg_key))) {
    TRANS_LOG(WARN, "check bounded staleness read version error", K(ret), K(pg_key), K(desc));
  } else if (!desc.is_local_single_partition_stmt() &&
             OB_FAIL(ts_mgr_->update_local_trans_version(pg_key.get_tenant_id(), snapshot_version, updated))) {
    TRANS_LOG(WARN, "update gts failed", KR(ret), K(pg_key));
  } else {
    if (OB_FAIL(alloc_memtable_ctx_(pg_key, false, pg_key.get_tenant_id(), mt_ctx))) {
      TRANS_LOG(WARN, "allocate memory failed", K(ret), K(pg_key));
    } else if (!mt_ctx->is_self_alloc_ctx() && OB_FAIL(init_memtable_ctx_(mt_ctx, pg_key.get_tenant_id()))) {
      TRANS_LOG(WARN, "init mem ctx failed", K(ret), K(pg_key));
      release_memtable_ctx_(pg_key, mt_ctx);
    } else if (OB_FAIL(mt_ctx->trans_begin())) {
      TRANS_LOG(WARN, "trans begin failed", K(ret));
      release_memtable_ctx_(pg_key, mt_ctx);
    } else if (OB_FAIL(mt_ctx->sub_trans_begin(user_specified_snapshot > 0 ? user_specified_snapshot : snapshot_version,
                   desc.get_stmt_expired_time(),
                   false,
                   desc.get_trx_lock_timeout()))) {
      TRANS_LOG(WARN, "sub trans begin failed", K(ret));
      release_memtable_ctx_(pg_key, mt_ctx);
      // account read-only transaction count, prevent partition
      // was removed during read-only transaction execution
    } else if (OB_FAIL(mt_ctx->get_trans_table_guard()
                           ->get_trans_state_table()
                           .get_partition_trans_ctx_mgr()
                           ->check_and_inc_read_only_trx_count())) {
      TRANS_LOG(WARN, "check and inc read only trx count error", K(ret), K(pg_key));
      release_memtable_ctx_(pg_key, mt_ctx);
    } else {
      mt_ctx->is_standalone_ = true;
      mt_ctx->set_read_only();
      store_ctx.trans_id_ = desc.get_trans_id();
      store_ctx.mem_ctx_ = mt_ctx;
      store_ctx.trans_table_guard_ = mt_ctx->get_trans_table_guard();
      store_ctx.cur_pkey_ = pg_key;
      store_ctx.sql_no_ = 1;
    }
  }

  return ret;
}

int ObTransService::revert_store_ctx_(const ObStandaloneStmtDesc& desc, const ObPartitionKey& pg_key,
    ObStoreCtx& store_ctx, ObPartitionTransCtxMgr* part_mgr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!desc.is_valid() || !pg_key.is_valid() || OB_ISNULL(part_mgr))) {
    TRANS_LOG(WARN, "invalid argument", K(desc), K(pg_key));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (NULL != part_mgr) {
      if (OB_FAIL(part_mgr->check_and_dec_read_only_trx_count())) {
        TRANS_LOG(WARN, "check and dec read only trx count fail", K(ret), K(pg_key));
      }
    } else if (OB_FAIL(part_trans_ctx_mgr_.check_and_dec_read_only_trx_count(pg_key))) {
      TRANS_LOG(WARN, "check and dec read only trx count fail", K(ret), K(pg_key));
    } else {
      // do nothing
    }
    ObMemtableCtx* mem_ctx = static_cast<ObMemtableCtx*>(store_ctx.mem_ctx_);
    release_memtable_ctx_(pg_key, mem_ctx);
    store_ctx.mem_ctx_ = NULL;
  }

  return ret;
}

int ObTransService::dump_elr_statistic()
{
  int ret = OB_SUCCESS;
  ObELRStatSummary elr_stat;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(part_trans_ctx_mgr_.iterate_partition(elr_stat))) {
    TRANS_LOG(WARN, "iterate partition error", K(ret));
  } else {
    TRANS_LOG(INFO, "ELR statistic summary", K(elr_stat));
    elr_stat.reset();
  }

  return ret;
}

int ObTransService::set_trans_snapshot_version_for_serializable_(
    ObTransDesc& trans_desc, const int64_t stmt_snapshot_version, const bool is_stmt_snapshot_version_valid)
{
  int ret = OB_SUCCESS;
  int64_t snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;
  const uint64_t tenant_id = trans_desc.get_tenant_id();

  if (!ts_mgr_->is_external_consistent(tenant_id)) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "serializable isolation need gts", KR(ret));
  } else if (is_stmt_snapshot_version_valid) {
    trans_desc.set_trans_snapshot_version(stmt_snapshot_version);
    TRANS_LOG(DEBUG, "set snapshot for serializable transaction", K(stmt_snapshot_version), K(trans_desc));
  } else if (OB_FAIL(generate_transaction_snapshot_(trans_desc, snapshot_version))) {
    TRANS_LOG(WARN, "generate snapshot version error", KR(ret));
  } else {
    trans_desc.set_trans_snapshot_version(snapshot_version);
    TRANS_LOG(DEBUG, "generate snapshot for serializable transaction", K(snapshot_version), K(trans_desc));
  }

  return ret;
}

int ObTransService::set_restore_snapshot_version(const ObPartitionKey& pkey, const int64_t restore_snapshot_version)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(part_trans_ctx_mgr_.set_restore_snapshot_version(pkey, restore_snapshot_version))) {
    TRANS_LOG(WARN, "failed to set_restore_snapshot_version", KR(ret), K(pkey), K(restore_snapshot_version));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransService::set_last_restore_log_id(const ObPartitionKey& pkey, const uint64_t last_restore_log_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(part_trans_ctx_mgr_.set_last_restore_log_id(pkey, last_restore_log_id))) {
    TRANS_LOG(WARN, "set last restore log id error", K(ret), K(pkey), K(last_restore_log_id));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransService::update_restore_replay_info(
    const ObPartitionKey& pkey, const int64_t restore_snapshot_version, const uint64_t last_restore_log_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(
                 part_trans_ctx_mgr_.update_restore_replay_info(pkey, restore_snapshot_version, last_restore_log_id))) {
    TRANS_LOG(WARN,
        "failed to update_restore_replay_info",
        K(ret),
        K(pkey),
        K(restore_snapshot_version),
        K(last_restore_log_id));
  } else {
    // do nothing
  }

  return ret;
}

// only for ob_admin use
int ObTransService::kill_part_trans_ctx(const ObPartitionKey& partition, const ObTransID& trans_id)
{
  int ret = OB_SUCCESS;
  bool alloc = false;
  ObTransCtx* ctx = nullptr;
  if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(partition), K(trans_id));
  } else if (OB_FAIL(part_trans_ctx_mgr_.get_trans_ctx(partition,
                 trans_id,
                 false, /*for_replay*/
                 false, /*is_readonly*/
                 false, /*is_bounded_staleness_read*/
                 true,  /*need_completed_active_txn*/
                 alloc,
                 ctx))) {
    TRANS_LOG(WARN, "failed to get part ctx", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ctx is null", K(ret), K(partition), K(trans_id));
  } else {
    ObPartTransCtx* part_ctx = reinterpret_cast<ObPartTransCtx*>(ctx);
    KillTransArg arg(false, false, false);
    ObEndTransCallbackArray cb_array;
    if (OB_FAIL(part_ctx->kill(arg, cb_array))) {
      TRANS_LOG(WARN, "failed to kill part ctx", K(ret));
    }
    part_trans_ctx_mgr_.revert_trans_ctx(ctx);
  }

  return ret;
}

int ObTransService::submit_log_for_split(const ObPartitionKey& partition, bool& log_finished)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(part_trans_ctx_mgr_.submit_log_for_split(partition, log_finished))) {
    TRANS_LOG(WARN, "submit log for split failed", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "submit log for split success", K(partition));
  }

  return ret;
}

int ObTransService::copy_trans_table(const ObPartitionKey& partition, const ObIArray<ObPartitionKey>& dest_array)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(part_trans_ctx_mgr_.copy_trans_table(this, partition, dest_array))) {
    TRANS_LOG(WARN, "copy trans table failed", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "copy trans table success", K(partition));
  }

  return ret;
}

int ObTransService::start_cursor_stmt(ObTransDesc& trans_desc, const int64_t cur_stmt_expired_time)
{
  int ret = OB_SUCCESS;
  ObTransSnapInfo snapshot;
  trans_desc.inc_sub_sql_no();
  trans_desc.set_cur_stmt_expired_time(cur_stmt_expired_time);
  ret = get_stmt_snapshot_info(true, trans_desc, snapshot);
  trans_desc.set_stmt_snapshot_info(snapshot);
  trans_desc.set_fast_select();
  TRANS_LOG(DEBUG, "get stmt snapshot info", K(ret), K(snapshot));
  return ret;
}

void ObThreadLocalTransCtx::destroy()
{
  int ret = OB_SUCCESS;
  state_ = ObThreadLocalTransCtxState::OB_THREAD_LOCAL_CTX_INVALID;
  if (memtable_ctx_.get_ctx_descriptor() > 0) {
    if (OB_ISNULL(memtable_ctx_.get_ctx_map())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected mem ctx", K(ret));
    } else {
      memtable_ctx_.get_ctx_map()->erase(memtable_ctx_.get_ctx_descriptor());
      TRANS_LOG(INFO, "thread local mem ctx erase id", K(memtable_ctx_.get_ctx_descriptor()));
    }
  }
}

}  // namespace transaction
}  // namespace oceanbase
