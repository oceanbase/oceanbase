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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_SERVICE_
#define OCEANBASE_TRANSACTION_OB_TRANS_SERVICE_

#include <stdlib.h>
#include <time.h>
#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/memtable/ob_memtable_context.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_light_hashmap.h"
#include "sql/ob_end_trans_callback.h"
#include "lib/utility/utility.h"
#include "ob_trans_define.h"
#include "ob_trans_timer.h"
#include "ob_location_adapter.h"
#include "ob_trans_rpc.h"
#include "ob_trans_ctx_mgr.h"
#include "ob_dup_table_rpc.h"
#include "ob_dup_table_base.h"
#include "ob_trans_memory_stat.h"
#include "ob_trans_event.h"
#include "ob_dup_table.h"
#include "ob_gts_rpc.h"
#include "ob_gti_source.h"
#include "ob_tx_version_mgr.h"
#include "ob_tx_standby_cleanup.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/container/ob_iarray.h"
#include "observer/ob_server_struct.h"
#include "common/storage/ob_sequence.h"
#include "ob_tx_elr_util.h"
#include "storage/tx/ob_dup_table_util.h"
#include "ob_tx_free_route.h"
#include "ob_tx_free_route_msg.h"
#include "ob_tablet_to_ls_cache.h"

#define MAX_REDO_SYNC_TASK_COUNT 10

namespace oceanbase
{

namespace obrpc
{
class ObTransRpcProxy;
class ObTransRpcResult;
class ObDupTableRpcProxy;
class ObSrvRpcProxy;
}

namespace common
{
class ObAddr;
}

namespace share
{
class ObAliveServerTracer;
}

namespace storage
{
class ObIMemtable;
}

namespace memtable
{
class ObMemtableCtx;
}

namespace obrpc
{
class ObSrvRpcProxy;
}

namespace transaction
{
class ObITsMgr;
class ObTimestampService;
class ObITxLogParam;

// iterate transaction module memory usage status
typedef common::ObSimpleIterator<ObTransMemoryStat,
    ObModIds::OB_TRANS_VIRTUAL_TABLE_MEMORY_STAT, 16> ObTransMemStatIterator;
// cache scaned duplicated table partition and its lease request task
class KillTransArg
{
public:
  KillTransArg(const bool graceful, const bool ignore_ro_trans = true, const bool need_kill_coord_ctx = true)
    : graceful_(graceful), ignore_ro_trans_(ignore_ro_trans), need_kill_coord_ctx_(need_kill_coord_ctx) {}
  ~KillTransArg() {}
  TO_STRING_KV(K_(graceful), K_(ignore_ro_trans), K_(need_kill_coord_ctx));
public:
  bool graceful_;
  bool ignore_ro_trans_;
  bool need_kill_coord_ctx_;
};

enum class ObThreadLocalTransCtxState : int
{
  OB_THREAD_LOCAL_CTX_INVALID,
  OB_THREAD_LOCAL_CTX_READY,
  OB_THREAD_LOCAL_CTX_RUNNING,
  OB_THREAD_LOCAL_CTX_BLOCKING
};

class ObThreadLocalTransCtx
{
public:
  static const int64_t MAX_BIG_TRANS_TASK = 100 * 1000;
public:
  ObThreadLocalTransCtx() : state_(ObThreadLocalTransCtxState::OB_THREAD_LOCAL_CTX_READY) {}
  ~ObThreadLocalTransCtx() { destroy(); }
  void reset()
  {
    state_ = ObThreadLocalTransCtxState::OB_THREAD_LOCAL_CTX_INVALID;
  }
  void destroy();
public:
  memtable::ObMemtableCtx memtable_ctx_;
  ObThreadLocalTransCtxState state_;
} CACHE_ALIGNED;

class ObRollbackSPMsgGuard final : public share::ObLightHashLink<ObRollbackSPMsgGuard>
{
public:
  ObRollbackSPMsgGuard(ObCommonID tx_msg_id, ObTxDesc &tx_desc, ObTxDescMgr &tx_desc_mgr)
  : tx_msg_id_(tx_msg_id), tx_desc_(tx_desc), tx_desc_mgr_(tx_desc_mgr) {
    tx_desc_.inc_ref(1);
  }
  ~ObRollbackSPMsgGuard() {
    if (0 == tx_desc_.dec_ref(1)) {
      tx_desc_mgr_.free(&tx_desc_);
    }
    tx_msg_id_.reset();
  }
  ObTxDesc &get_tx_desc() { return tx_desc_; }
  bool contain(ObCommonID tx_msg_id) { return tx_msg_id == tx_msg_id_; }
private:
  ObCommonID tx_msg_id_;
  ObTxDesc &tx_desc_;
  ObTxDescMgr &tx_desc_mgr_;
};

class ObRollbackSPMsgGuardAlloc
{
public:
  static ObRollbackSPMsgGuard* alloc_value()
  {
    return (ObRollbackSPMsgGuard*)ob_malloc(sizeof(ObRollbackSPMsgGuard), "RollbackSPMsg");
  }
  static void free_value(ObRollbackSPMsgGuard *p)
  {
    if (NULL != p) {
      p->~ObRollbackSPMsgGuard();
      ob_free(p);
      p = NULL;
    }
  }
};

class ObTransService : public common::ObSimpleThreadPool
{
public:
  ObTransService();
  virtual ~ObTransService() { destroy(); }
  static int mtl_init(ObTransService* &trans_service);
  int init(const ObAddr &self,
           ObITransRpc *rpc,
           ObIDupTableRpc *dup_table_rpc,
           ObILocationAdapter *location_adapter,
           ObIGtiSource *gti_source,
           ObITsMgr *ts_mgr,
           obrpc::ObSrvRpcProxy *rpc_proxy,
           share::schema::ObMultiVersionSchemaService *schema_service,
           share::ObAliveServerTracer *server_tracer);
  int start();
  void stop();
  void wait() { wait_(); }
  int wait_();
  void destroy();
  int push(void *task);
  virtual void handle(void *task) override;
public:
  int check_trans_partition_leader_unsafe(const share::ObLSID &ls_id, bool &is_leader);
  int get_weak_read_snapshot(const uint64_t tenant_id, share::SCN &snapshot_version);
  int calculate_trans_cost(const ObTransID &tid, uint64_t &cost);
  int get_ls_min_uncommit_prepare_version(const share::ObLSID &ls_id, share::SCN &min_prepare_version);
  int get_min_undecided_log_ts(const share::ObLSID &ls_id, share::SCN &log_ts);
  int check_dup_table_lease_valid(const share::ObLSID ls_id, bool &is_dup_ls, bool &is_lease_valid);
  //get the memory used condition of transaction module
  int iterate_trans_memory_stat(ObTransMemStatIterator &mem_stat_iter);
  int get_trans_start_session_id(const share::ObLSID &ls_id, const ObTransID &tx_id, uint32_t &session_id);
  int dump_elr_statistic();
  int remove_callback_for_uncommited_txn(
    const share::ObLSID ls_id,
    const memtable::ObMemtableSet *memtable_set);
  int64_t get_tenant_id() const { return tenant_id_; }
  const common::ObAddr &get_server() { return self_; }
  ObTransTimer &get_trans_timer() { return timer_; }
  ObITransRpc *get_trans_rpc() { return rpc_; }
  ObIDupTableRpc *get_dup_table_rpc() { return dup_table_rpc_; }
  ObDupTableRpc &get_dup_table_rpc_impl() { return dup_table_rpc_impl_; }
  ObDupTableLoopWorker &get_dup_table_loop_worker() { return dup_table_loop_worker_; }
  const ObDupTabletScanTask &get_dup_table_scan_task() { return dup_tablet_scan_task_; }
  ObILocationAdapter *get_location_adapter() { return location_adapter_; }
  common::ObMySQLProxy *get_mysql_proxy() { return GCTX.sql_proxy_; }
  bool is_running() const { return is_running_; }
  ObITsMgr *get_ts_mgr() { return ts_mgr_; }
  share::ObAliveServerTracer *get_server_tracer() { return server_tracer_; }
  share::schema::ObMultiVersionSchemaService *get_schema_service() { return schema_service_; }
  ObTxVersionMgr &get_tx_version_mgr() { return tx_version_mgr_; }
  int handle_part_trans_ctx(const obrpc::ObTrxToolArg &arg, obrpc::ObTrxToolRes &res);
  int register_mds_into_tx(ObTxDesc &tx_desc,
                           const share::ObLSID &ls_id,
                           const ObTxDataSourceType &type,
                           const char *buf,
                           const int64_t buf_len,
                           const int64_t request_id = 0,
                           const ObRegisterMdsFlag &register_flag = ObRegisterMdsFlag(),
                           const transaction::ObTxSEQ seq_no = transaction::ObTxSEQ());
  ObTxELRUtil &get_tx_elr_util() { return elr_util_; }
  int create_tablet(const common::ObTabletID &tablet_id, const share::ObLSID &ls_id)
  {
    return tablet_to_ls_cache_.create_tablet(tablet_id, ls_id);
  }
  int remove_tablet(const common::ObTabletID &tablet_id, const share::ObLSID &ls_id)
  {
    return tablet_to_ls_cache_.remove_tablet(tablet_id, ls_id);
  }
  int remove_tablet(const share::ObLSID &ls_id)
  {
    return tablet_to_ls_cache_.remove_ls_tablets(ls_id);
  }
  int check_and_get_ls_info(const common::ObTabletID &tablet_id,
                            share::ObLSID &ls_id,
                            bool &is_local_leader)
  {
    return tablet_to_ls_cache_.check_and_get_ls_info(tablet_id, ls_id, is_local_leader);
  }
#ifdef ENABLE_DEBUG_LOG
  transaction::ObDefensiveCheckMgr *get_defensive_check_mgr() { return defensive_check_mgr_; }
#endif
private:
  void check_env_();
  bool can_create_ctx_(const int64_t trx_start_ts, const common::ObTsWindows &changing_leader_windows);
  int register_mds_into_ctx_(ObTxDesc &tx_desc,
                             const share::ObLSID &ls_id,
                             const ObTxDataSourceType &type,
                             const char *buf,
                             const int64_t buf_len,
                             const transaction::ObTxSEQ seq_no,
                             const ObRegisterMdsFlag &register_flag);
private:
  int handle_redo_sync_task_(ObDupTableRedoSyncTask *task, bool &need_release_task);
  int handle_dup_pre_commit_task_(ObPreCommitTask *task, bool &need_release_task);
  int get_gts_(share::SCN &snapshot_version,
      MonotonicTs &receive_gts_ts,
      const int64_t trans_expired_time,
      const int64_t stmt_expired_time,
      const uint64_t tenant_id);
  int handle_batch_msg_(const int type, const char *buf, const int32_t size);
  int64_t fetch_rollback_sp_sequence_() { return ATOMIC_AAF(&rollback_sp_msg_sequence_, 1); }
public:
  int check_dup_table_ls_readable();
  int check_dup_table_tablet_readable();

  int retry_redo_sync_by_task(ObTransID tx_id, share::ObLSID ls_id);
public:
  int end_1pc_trans(ObTxDesc &trans_desc,
                    ObITxCallback *endTransCb,
                    const bool is_rollback,
                    const int64_t expire_ts);
  int get_max_commit_version(share::SCN &commit_version) const;
  int get_max_decided_scn(const share::ObLSID &ls_id, share::SCN & scn);
  #include "ob_trans_service_v4.h"
  #include "ob_tx_free_route_api.h"
private:
  static const int64_t END_STMT_MORE_TIME_US = 100 * 1000;
  // max task count in message process queue
  static const int64_t MAX_MSG_TASK_CNT = 1000 * 1000;
  static const int64_t MSG_TASK_CNT_PER_GB = 50 * 1000;
  static const int64_t MAX_BIG_TRANS_WORKER = 8;
  static const int64_t MAX_BIG_TRANS_TASK = 100 * 1000;
  // max time bias between any two machine
  static const int64_t MAX_TIME_INTERVAL_BETWEEN_MACHINE_US = 200 * 1000;
  static const int64_t CHANGING_LEADER_TXN_PER_ROUND = 200;
public:
  ObIGtiSource *gti_source_;
  ObGtiSource gti_source_def_;
  // send lease renew request interval for duplicated table partition
  static const int64_t DUP_TABLE_LEASE_INTERVAL_US = 1 * 1000 * 1000;  // 1s
  // default duplicated table partition lease timeout
  static const int64_t DEFAULT_DUP_TABLE_LEASE_TIMEOUT_INTERVAL_US = 11 * 1000 * 1000; // 11s
  static const int64_t DUP_TABLE_LEASE_START_RENEW_INTERVAL_US = 4 * 1000 * 1000; // 4s
protected:
  bool is_inited_;
  bool is_running_;
  // for ObTransID
  common::ObAddr self_;
  int64_t tenant_id_;
  ObTransRpc rpc_def_;
  ObLocationAdapter location_adapter_def_;
  // transaction timer
  ObTransTimer timer_;
  ObDupTableLeaseTimer dup_table_scan_timer_;
  ObTxVersionMgr tx_version_mgr_;
protected:
  bool use_def_;
  ObITransRpc *rpc_;
  ObIDupTableRpc *dup_table_rpc_;
  ObDupTableRpc_old dup_table_rpc_def_;
  // the adapter between transaction and location cache
  ObILocationAdapter *location_adapter_;
  // the adapter between transaction and clog
  share::schema::ObMultiVersionSchemaService *schema_service_;
private:
  ObITsMgr *ts_mgr_;
  // server alive tracker
  share::ObAliveServerTracer *server_tracer_;
  // account task qeuue's inqueue and dequeue
  uint32_t input_queue_count_;
  uint32_t output_queue_count_;
#ifdef ENABLE_DEBUG_LOG
  transaction::ObDefensiveCheckMgr *defensive_check_mgr_;
#endif
  // in order to pass the mittest, tablet_to_ls_cache_ must be declared before tx_desc_mgr_
  ObTabletToLSCache tablet_to_ls_cache_;
  // txDesc's manager
  ObTxDescMgr tx_desc_mgr_;

  //4.0 dup_table
  ObDupTabletScanTask dup_tablet_scan_task_;
  ObDupTableLoopWorker dup_table_loop_worker_;
  ObDupTableRpc dup_table_rpc_impl_;
  ObTxRedoSyncRetryTask redo_sync_task_array_[MAX_REDO_SYNC_TASK_COUNT];

  obrpc::ObSrvRpcProxy *rpc_proxy_;
  ObTxELRUtil elr_util_;
  // for rollback-savepoint request-id
  int64_t rollback_sp_msg_sequence_;
  // for rollback-savepoint msg resp callback to find tx_desc
  share::ObLightHashMap<ObCommonID, ObRollbackSPMsgGuard, ObRollbackSPMsgGuardAlloc, common::SpinRWLock, 1 << 16 /*bucket_num*/> rollback_sp_msg_mgr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransService);
};

} // transaction
} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_TRANS_SERVICE_
