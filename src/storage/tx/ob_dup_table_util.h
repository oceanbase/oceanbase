// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_TRANSACTION_DUP_TABLE_UTIL_H
#define OCEANBASE_TRANSACTION_DUP_TABLE_UTIL_H

// #include "lib/hash/ob_hashset.h"
#include "logservice/ob_log_base_type.h"
#include "storage/tx/ob_dup_table_tablets.h"
#include "storage/tx/ob_dup_table_lease.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_dup_table_stat.h"
#include "share/ls/ob_ls_status_operator.h"

namespace oceanbase
{

namespace logservice
{
class ObLogHandler;
}

namespace transaction
{
class ObDupTableLSLeaseMgr;
class ObDupTableLSTsSyncMgr;
class ObLSDupTabletsMgr;
class ObDupTableLoopWorker;
class DupTableTsInfo;
// class ObDupTableICallback;
class ObITxLogAdapter;

class ObDupTableLeaseRequest;
class ObDupTableTsSyncRequest;
class ObDupTableTsSyncResponse;

class MockDupTableChecker
{
public:
  static int check_dup_table_tablet(common::ObTabletID id, bool &is_dup_table)
  {
    int ret = OB_SUCCESS;

    is_dup_table = false;

    return ret;
  }
};

// scan all tablets to find dup_table tablets on LS leader
class ObDupTabletScanTask : public ObITimeoutTask
{
public:
  static const int64_t DUP_TABLET_SCAN_INTERVAL = 10 * 1000 * 1000;          // 10s
  static const int64_t MAX_DUP_LS_REFRESH_INTERVAL = 60 * 60 * 1000 * 1000L; // 60min
public:
  ObDupTabletScanTask() { reset(); }
  ~ObDupTabletScanTask() { destroy(); }
  void reset();
  void destroy() { reset(); }
  int make(const int64_t tenant_id,
           ObDupTableLeaseTimer *scan_timer,
           ObDupTableLoopWorker *loop_worker);

  void runTimerTask();
  uint64_t hash() const { return tenant_id_; }

  int64_t get_max_exec_interval() const { return ATOMIC_LOAD(&max_execute_interval_); }
  int64_t get_last_scan_task_succ_ts() const { return ATOMIC_LOAD(&last_scan_task_succ_time_); }

  TO_STRING_KV(K(tenant_id_),
               KP(dup_table_scan_timer_),
               KP(dup_loop_worker_),
               K(min_dup_ls_status_info_),
               K(tenant_schema_dup_tablet_set_.size()),
               K(scan_task_execute_interval_),
               K(last_dup_ls_refresh_time_),
               K(last_dup_schema_refresh_time_),
               K(last_scan_task_succ_time_),
               K(max_execute_interval_));

private:
  int execute_for_dup_ls_();
  int refresh_dup_ls_(const int64_t cur_time);
  int refresh_dup_tablet_schema_(const int64_t cur_time);

  bool has_valid_dup_schema_() const;

private:
  ObTenantDupTabletSchemaHelper dup_schema_helper_;

  int64_t tenant_id_;
  ObDupTableLeaseTimer *dup_table_scan_timer_;
  ObDupTableLoopWorker *dup_loop_worker_;

  share::ObLSStatusInfo min_dup_ls_status_info_; // min_ls_id
  ObTenantDupTabletSchemaHelper::TabletIDSet tenant_schema_dup_tablet_set_;

  int64_t scan_task_execute_interval_;

  int64_t last_dup_ls_refresh_time_;
  int64_t last_dup_schema_refresh_time_;

  int64_t last_scan_task_succ_time_;
  int64_t max_execute_interval_;
};

// LS-level
// manage lease and ts_sync for dup_table
// register log_handler in each ls but not alloc inside memory without dup_table tablet
// alloc inside memory when called by ObDupTabletScanTask
class ObDupTableLSHandler : public logservice::ObIReplaySubHandler,
                            public logservice::ObIRoleChangeSubHandler,
                            public logservice::ObICheckpointSubHandler
{
public:
  ObDupTableLSHandler()
      : ls_state_helper_("DupTableLSHandler"), lease_mgr_ptr_(nullptr), ts_sync_mgr_ptr_(nullptr),
        tablets_mgr_ptr_(nullptr), log_operator_(nullptr)
  {
    reset();
  }
  ~ObDupTableLSHandler() { destroy(); }
  // init by ObDupTabletScanTask or replay
  int init(bool is_dup_table);

  void start();
  void stop();    // TODO  stop submit log before the log handler is invalid.
  int safe_to_destroy(bool &is_dup_table_handler_safe);    // TODO
  void destroy(); // remove from dup_table_loop_worker
  int offline();
  int online();
  void reset();

  const share::ObLSID &get_ls_id() { return ls_id_; }
  // must set when init log_stream
  void default_init(const share::ObLSID &ls_id, logservice::ObLogHandler *log_handler)
  {
    ls_id_ = ls_id;
    log_handler_ = log_handler;
    dup_ls_ckpt_.default_init(ls_id);
  }

  bool is_master();
  // bool is_follower();
  bool is_inited();
  // bool is_online();

  TO_STRING_KV(K(ls_id_),
               K(ls_state_helper_),
               K(dup_ls_ckpt_),
               KP(lease_mgr_ptr_),
               KP(tablets_mgr_ptr_),
               KP(ts_sync_mgr_ptr_),
               KP(log_operator_),
               K(interface_stat_));

public:
  int ls_loop_handle();

  //------ leader interface
  // called by DupTabletScanTask => ObDupTableLoopWorker , alloc memory for first dup_table tablet
  int refresh_dup_table_tablet(common::ObTabletID tablet_id,
                               bool is_dup_table,
                               int64_t refresh_time);
  // called by rpc thread
  int receive_lease_request(const ObDupTableLeaseRequest &lease_req);
  int handle_ts_sync_response(const ObDupTableTsSyncResponse &ts_sync_reps);
  // called by part_ctx
  // int validate_dup_table_tablet(const ObTabletID &tablet_id, bool &is_dup_tablet);
  // int validate_replay_ts(int64_t log_ts);
  // int validate_commit_version(int64_t commit_version);
  int check_redo_sync_completed(const ObTransID &tx_id,
                                const share::SCN &redo_completed_scn,
                                bool &redo_sync_finish,
                                share::SCN &total_max_read_version);

  int block_confirm_with_dup_tablet_change_snapshot(share::SCN &dup_tablet_change_snapshot);
  int unblock_confirm_with_prepare_scn(const share::SCN &dup_tablet_change_snapshot,
                                       const share::SCN &prepare_scn);
  int check_dup_tablet_in_redo(const ObTabletID &tablet_id,
                               bool &is_dup_tablet,
                               const share::SCN &base_snapshot,
                               const share::SCN &redo_scn);

  //------ follower interface
  // called by rpc thread
  int handle_ts_sync_request(const ObDupTableTsSyncRequest &ts_sync_req); // post ts_sync response
  // called by trans_service
  int validate_readable_tablet();
  int validate_lease_valid();
  int validate_in_trans_read();
  int check_dup_tablet_readable(const ObTabletID &tablet_id,
                                const share::SCN &read_snapshot,
                                const bool read_from_leader,
                                const share::SCN &max_replayed_scn,
                                bool &readable);

  bool is_dup_table_lease_valid();

public:
  int64_t get_dup_tablet_count();
  bool check_tablet_set_exist();
  bool has_dup_tablet();
  bool is_dup_tablet(const common::ObTabletID &tablet_id);
  int gc_temporary_dup_tablets(const int64_t gc_ts, const int64_t max_task_interval);
  int get_local_ts_info(DupTableTsInfo &ts_info);
  int get_cache_ts_info(const common::ObAddr &addr, DupTableTsInfo &ts_info);
  int get_lease_mgr_stat(ObDupLSLeaseMgrStatIterator &collect_iter);
  int get_ls_tablets_stat(ObDupLSTabletsStatIterator &collect_iter);
  int get_ls_tablet_set_stat(ObDupLSTabletSetStatIterator &collect_iter);
  // int retry_submit_log();
  // ObLSDupTabletsMgr *get_tablets_mgr() { return tablets_mgr_ptr_; }
  // ObITxLogAdapter *get_log_adapter() { return log_adapter_ptr_; }
public:
  int replay(const void *buffer,
             const int64_t nbytes,
             const palf::LSN &lsn,
             const share::SCN &ts_ns);
  void switch_to_follower_forcedly();
  int switch_to_follower_gracefully();
  int resume_leader();
  int switch_to_leader();

  int set_dup_table_ls_meta(const ObDupTableLSCheckpoint::ObLSDupTableMeta &dup_ls_meta,
                            bool need_flush_slog = false);
  int get_dup_table_ls_meta(ObDupTableLSCheckpoint::ObLSDupTableMeta &dup_ls_meta) const
  {
    return dup_ls_ckpt_.get_dup_ls_meta(dup_ls_meta);
  }

  share::SCN get_rec_scn() { return dup_ls_ckpt_.get_lease_log_rec_scn(); }
  int flush(share::SCN &rec);

  logservice::ObLogHandler *get_log_handler() { return log_handler_; }

  // void inc_committing_dup_trx_cnt() { ATOMIC_INC(&committing_dup_trx_cnt_); }
  // void dec_committing_dup_trx_cnt() { ATOMIC_DEC(&committing_dup_trx_cnt_); }
public:
  int64_t get_total_block_confirm_ref() { return ATOMIC_LOAD(&total_block_confirm_ref_); }

  int check_and_update_max_replayed_scn(const share::SCN &max_replayed_scn);

  int64_t get_committing_dup_trx_cnt();
  int add_commiting_dup_trx(const ObTransID &tx_id);
  int remove_commiting_dup_trx(const ObTransID &tx_id);

private:
  DISALLOW_COPY_AND_ASSIGN(ObDupTableLSHandler);

  int prepare_log_operator_();
  int get_min_lease_ts_info_(DupTableTsInfo &min_ts_info);
  int leader_takeover_(const bool is_resume, const bool is_initing = false);
  int leader_revoke_(const bool is_forcedly);

  int try_to_confirm_tablets_(const share::SCN &confirm_ts);

  int recover_ckpt_into_memory_();

  int errsim_leader_revoke_();

private:
  share::ObLSID ls_id_;

  bool is_inited_;
  SpinRWLock init_rw_lock_;

  // set these flag for a normal ls without dup_table
  ObDupTableLSRoleStateHelper ls_state_helper_;

  int64_t total_block_confirm_ref_; // block new dup tablet confirmed

  ObSpinLock committing_dup_trx_lock_;
  common::hash::ObHashSet<ObTransID> committing_dup_trx_set_;

  share::SCN self_max_replayed_scn_;
  int64_t last_max_replayed_scn_update_ts_;

  ObDupTableLSCheckpoint dup_ls_ckpt_;

  // lease
  ObDupTableLSLeaseMgr *lease_mgr_ptr_;
  // ts sync
  ObDupTableLSTsSyncMgr *ts_sync_mgr_ptr_;
  // dup_table_tablets
  ObLSDupTabletsMgr *tablets_mgr_ptr_;

  logservice::ObLogHandler *log_handler_;

  ObDupTableLogOperator *log_operator_;

  DupTableInterfaceStat interface_stat_;

  int64_t last_diag_info_print_us_[DupTableDiagStd::TypeIndex::MAX_INDEX];
};

typedef common::hash::ObHashSet<ObDupTableLSHandler *, common::hash::SpinReadWriteDefendMode>
    DupTableLSHandlerSet;

typedef common::hash::
    ObHashMap<share::ObLSID, ObDupTableLSHandler *, common::hash::SpinReadWriteDefendMode>
        DupTableLSHandlerMap;

typedef common::hash::ObHashSet<share::ObLSID, common::hash::SpinReadWriteDefendMode>
    DupLSIDSet_Spin;


// tenant-level
// a thread for  handle lease request and writer log
// start by ObDupTabletScanTask
// stop by trans_service or ObDupTabletScanTask
class ObDupTableLoopWorker : public lib::ThreadPool
{
public:
  const static int64_t LOOP_INTERVAL = 100 * 1000; // 100ms
public:
  ObDupTableLoopWorker() { is_inited_ = false; }
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  void reset();
  void run1();

public:
  // called by ObDupTabletScanTask
  // set dup_ls_handle and dup_tablet
  int refresh_dup_table_tablet(ObDupTableLSHandler *dup_ls_handle,
                               const common::ObTabletID tablet_id,
                               bool is_dup_table,
                               int64_t refresh_time);
  int append_dup_table_ls(const share::ObLSID &ls_id);
  // int control_thread();

  // int remove_stopped_dup_ls(share::ObLSID ls_id);
  // iterate all ls for collect dup ls info
  int iterate_dup_ls(ObDupLSLeaseMgrStatIterator &collect_iter);
  int iterate_dup_ls(ObDupLSTabletSetStatIterator &collect_iter);
  int iterate_dup_ls(ObDupLSTabletsStatIterator &collect_iter);

  bool is_useful_dup_ls(const share::ObLSID ls_id);

  TO_STRING_KV(K(is_inited_), K(dup_ls_id_set_.size()));

private:
  class CopyDupLsIdFunctor{
  public:
    CopyDupLsIdFunctor(common::ObArray<share::ObLSID> &ls_id_array) : ls_id_array_(ls_id_array)
    {}
    ~CopyDupLsIdFunctor() {};
    int operator()(common::hash::HashSetTypes<share::ObLSID>::pair_type &kv);
  private:
    common::ObArray<share::ObLSID> &ls_id_array_;
  };
private:
  bool is_inited_;
  // SpinRWLock lock_;
  // // dup_table ls map which need to handle
  // // DupTableLSHandlerSet dup_ls_set_;
  // DupTableLSHandlerMap dup_ls_map_;
  DupLSIDSet_Spin dup_ls_id_set_;
};

} // namespace transaction
} // namespace oceanbase

#endif
