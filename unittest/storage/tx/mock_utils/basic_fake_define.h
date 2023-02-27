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

#ifndef OCEANBASE_TRANSACTION_TEST_BASIC_FAKE_DEFINE_
#define OCEANBASE_TRANSACTION_TEST_BASIC_FAKE_DEFINE_

#include "storage/tx/ob_trans_define.h"
#include "storage/tx_table/ob_tx_table.h"
#include "lib/utility/ob_defer.h"
#include "storage/tx/ob_location_adapter.h"
#include "storage/tx/ob_tx_log_adapter.h"
#include "storage/tx/ob_dup_table_rpc.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/tx/ob_gti_source.h"
#include "storage/tx/ob_tx_replay_executor.h"
#include "storage/tx/ob_trans_part_ctx.h"

namespace oceanbase {
using namespace share;
using namespace memtable;
namespace transaction {

class ObFakeTxDataTable : public ObTxDataTable {
public:
  ObFakeTxDataTable() : map_() { IGNORE_RETURN map_.init(); }
  virtual int init(ObLS *ls, ObTxCtxTable *tx_ctx_table) override
  { return OB_SUCCESS; }
  virtual int start() override { return OB_SUCCESS; }
  virtual void stop() override {}
  virtual void reset() override {}
  virtual void destroy() override {}
  virtual int alloc_tx_data(ObTxData *&tx_data) override
  {
    return map_.alloc_value(tx_data);
  }
  virtual int deep_copy_tx_data(ObTxData *from, ObTxData *&to) override
  {
    int ret = OB_SUCCESS;
    OZ (map_.alloc_value(to));
    OX (*to = *from);
    OZ (deep_copy_undo_status_list_(from->undo_status_list_, to->undo_status_list_));
    return ret;
  }
  virtual void free_tx_data(ObTxData *tx_data) override
  {
    map_.free_value(tx_data);
  }
  virtual int alloc_undo_status_node(ObUndoStatusNode *&undo_status_node) override
  {
    undo_status_node = new ObUndoStatusNode();
    return OB_SUCCESS;
  }
  virtual int free_undo_status_node(ObUndoStatusNode *&undo_status_node) override
  {
    delete undo_status_node;
    return OB_SUCCESS;
  }
  virtual int insert(ObTxData *&tx_data) override
  {
    int ret = OB_SUCCESS;
    ObTxData *old = NULL;
    if (OB_SUCC(map_.get(tx_data->tx_id_, old))) {
      OX (map_.revert(old));
      OZ (map_.del(tx_data->tx_id_));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
    OZ (map_.insert_and_get(tx_data->tx_id_, tx_data));
    OX (map_.revert(tx_data));
    return ret;
  }
  virtual int check_with_tx_data(const ObTransID tx_id, ObITxDataCheckFunctor &fn) override
  {
    int ret = OB_SUCCESS;
    ObTxData *tx_data = NULL;
    OZ (map_.get(tx_id, tx_data));
    OZ (fn(*tx_data));
    if (OB_NOT_NULL(tx_data)) { map_.revert(tx_data); }
    if (OB_ENTRY_NOT_EXIST == ret) { ret = OB_TRANS_CTX_NOT_EXIST; }
    return ret;
  }
  common::ObLinkHashMap<ObTransID, ObTxData> map_;
};

class ObFakeTxTable : public ObTxTable {
public:
  ObFakeTxTable() : ObTxTable(tx_data_table_) {}
private:
  ObFakeTxDataTable tx_data_table_;
};

class ObFakeDupTableRpc : public ObIDupTableRpc
{
public:
  int start() { return OB_SUCCESS; }
  int stop() { return OB_SUCCESS; }
  int wait() { return OB_SUCCESS; }
  void destroy() {}
  int post_dup_table_lease_request(const uint64_t tenant_id,
                                   const common::ObAddr &server,
                                   const ObDupTableLeaseRequestMsg &msg) { return OB_SUCCESS; }
  int post_dup_table_lease_response(const uint64_t tenant_id,
                                    const common::ObAddr &server,
                                    const ObDupTableLeaseResponseMsg &msg) { return OB_SUCCESS; }
  int post_redo_log_sync_request(const uint64_t tenant_id,
                                 const common::ObAddr &server,
                                 const ObRedoLogSyncRequestMsg &msg) { return OB_SUCCESS; }
  int post_redo_log_sync_response(const uint64_t tenant_id,
                                  const common::ObAddr &server,
                                  const ObRedoLogSyncResponseMsg &msg) { return OB_SUCCESS; }
  int post_pre_commit_request(const uint64_t tenant_id,
                              const common::ObAddr &server,
                              const ObPreCommitRequestMsg &msg) { return OB_SUCCESS; }
  int post_pre_commit_response(const uint64_t tenant_id,
                               const common::ObAddr &addr,
                               const ObPreCommitResponseMsg &msg) { return OB_SUCCESS; }
};

class ObFakeLocationAdapter : public ObILocationAdapter
{
public:
  ObFakeLocationAdapter() {
    ls_addr_table_.create(16, ObModIds::TEST);
  }
  int init(share::schema::ObMultiVersionSchemaService *schema_service,
                   share::ObLocationService *location_service) { return OB_SUCCESS; }
  void destroy() { }
  void reset() { ls_addr_table_.reuse(); }
  int get_leader(const int64_t cluster_id,
                 const int64_t tenant_id,
                 const share::ObLSID &ls_id,
                 common::ObAddr &leader)
  {
    int ret = OB_SUCCESS;
    const common::ObAddr *a = ls_addr_table_.get(ls_id);
    if (a == NULL) { ret = OB_ENTRY_NOT_EXIST; }
    else { leader = *a; }
    return ret;
  }
  int nonblock_get_leader(const int64_t cluster_id,
                          const int64_t tenant_id,
                          const share::ObLSID &ls_id,
                          common::ObAddr &leader)
  { return get_leader(cluster_id, tenant_id, ls_id, leader); }
  int nonblock_renew(const int64_t cluster_id,
                     const int64_t tenant_id,
                     const share::ObLSID &ls_id)
  { return OB_SUCCESS; }
  int nonblock_get(const int64_t cluster_id,
                   const int64_t tenant_id,
                   const share::ObLSID &ls_id,
                   share::ObLSLocation &location)
  {
    int ret = OB_SUCCESS;
    common::ObAddr leader;
    OZ(get_leader(cluster_id, tenant_id, ls_id, leader));
    ObLSReplicaLocation rep_loc;
    ObLSRestoreStatus restore_status(ObLSRestoreStatus::Status::RESTORE_NONE);
    auto p = ObReplicaProperty::create_property(100);
    OZ(rep_loc.init(leader, ObRole::LEADER, 10000, ObReplicaType::REPLICA_TYPE_FULL, p, restore_status));
    OZ(location.add_replica_location(rep_loc));
    return ret;
  }
public:
  // maintains functions
  int fill(const share::ObLSID ls_id, const common::ObAddr &addr)
  {
    return ls_addr_table_.set_refactored(ls_id, addr);
  }
  int remove(const share::ObLSID ls_id)
  {
    return ls_addr_table_.erase_refactored(ls_id);
  }
  int update_localtion(const share::ObLSID ls_id, const common::ObAddr &addr)
  {
    ls_addr_table_.erase_refactored(ls_id);
    return ls_addr_table_.set_refactored(ls_id, addr);
  }

private:
  common::hash::ObHashMap<share::ObLSID, common::ObAddr> ls_addr_table_;
};

class ObFakeGtiSource : public ObIGtiSource
{
  int get_trans_id(int64_t &trans_id) {
    trans_id = ATOMIC_AAF(&tx_id, 1);
    return OB_SUCCESS;
  }
  int64_t tx_id = 66;
};

class ObFakeTsMgr : public ObITsMgr
{
private:
  int get_gts_error_ = OB_SUCCESS;
public:
  void inject_get_gts_error(int error) {
    get_gts_error_ = error;
  }
  void repair_get_gts_error() {
    get_gts_error_ = OB_SUCCESS;
  }
public:
  int update_gts(const uint64_t tenant_id, const int64_t gts, bool &update) { return OB_SUCCESS; }
  int get_gts(const uint64_t tenant_id,
              const MonotonicTs stc,
              ObTsCbTask *task,
              int64_t &gts,
              MonotonicTs &receive_gts_ts)
  {
    int ret = OB_SUCCESS;
    TRANS_LOG(INFO, "get gts begin", K(gts_), K(&gts_));
    if (get_gts_error_) {
      ret = get_gts_error_;
    } else {
      gts = ATOMIC_AAF(&gts_, 1);
      if (task != nullptr && ATOMIC_LOAD(&get_gts_waiting_mode_)) {
        get_gts_waiting_queue_.push(task);
        ret = OB_EAGAIN;
      }
    }
    TRANS_LOG(INFO, "get gts end", K(ret), K(gts_), K(gts), K(&gts_));
    return ret;
  }

  int get_gts(const uint64_t tenant_id, ObTsCbTask *task, int64_t &gts) {
    if (get_gts_error_) { return get_gts_error_; }
    return OB_SUCCESS;
  }
  int get_ts_sync(const uint64_t tenant_id, const int64_t timeout_ts,
                  int64_t &ts, bool &is_external_consistent) { return OB_SUCCESS; }
  int wait_gts_elapse(const uint64_t tenant_id, const int64_t ts, ObTsCbTask *task,
                      bool &need_wait)
  {
    TRANS_LOG(INFO, "wait_gts_elapse begin", K(gts_), K(ts));
    int ret = OB_SUCCESS;
    if (task != nullptr && ATOMIC_LOAD(&elapse_waiting_mode_)) {
      elapse_queue_.push(task);
      callback_gts_ = ts+1;
      need_wait = true;
    } else {
      update_fake_gts(ts);
    }
    TRANS_LOG(INFO, "wait_gts_elapse end", K(gts_), K(ts));
    return ret;
  }

  int wait_gts_elapse(const uint64_t tenant_id, const int64_t ts)
  {
    int ret = OB_SUCCESS;
    if (ts > gts_) {
      ret = OB_EAGAIN;
    }
    return ret;
  }

  int update_base_ts(const int64_t base_ts) { return OB_SUCCESS; }
  int get_base_ts(int64_t &base_ts) { return OB_SUCCESS; }
  bool is_external_consistent(const uint64_t tenant_id) { return true; }
  int get_gts_and_type(const uint64_t tenant_id, const MonotonicTs stc, int64_t &gts,
                       int64_t &ts_type) { return OB_SUCCESS; }
  int64_t gts_ = 100;
  int64_t callback_gts_ = 100;

public:
  void update_fake_gts(int64_t gts)
  {
    TRANS_LOG(INFO, "update fake gts", K(gts_), K(gts), K(&gts_));
    gts_ = gts;
  }

  void set_elapse_waiting_mode() { ATOMIC_SET(&elapse_waiting_mode_, true); }
  void clear_elapse_waiting_mode() { ATOMIC_SET(&elapse_waiting_mode_, false); }

  void set_get_gts_waiting_mode() { ATOMIC_SET(&get_gts_waiting_mode_, true); }
  void clear_get_gts_waiting_mode() { ATOMIC_SET(&get_gts_waiting_mode_, false); }

  void elapse_callback()
  {
    if (callback_gts_ > gts_) {
      update_fake_gts(callback_gts_);
    }
    while(true) {
      ObLink *task = elapse_queue_.pop();
      if (task) {
        const MonotonicTs srr(MonotonicTs::current_time());
        const int64_t ts = gts_;
        const MonotonicTs receive_gts_ts(gts_);
        const ObGTSCacheTaskType task_type = WAIT_GTS_ELAPSING;
        static_cast<ObTsCbTask*>(task)->gts_elapse_callback(srr, ts);
      } else {
        break;
      }
    }
  }

  void get_gts_callback()
  {
    while(true) {
      ObLink *task = get_gts_waiting_queue_.pop();
      if (task) {
        const MonotonicTs srr(MonotonicTs::current_time());
        const int64_t ts = gts_;
        const MonotonicTs receive_gts_ts(gts_);
        const ObGTSCacheTaskType task_type = GET_GTS;
        static_cast<ObTsCbTask*>(task)->get_gts_callback(srr, ts, receive_gts_ts);
      } else {
        break;
      }
    }
  }

public:
  ObSpScLinkQueue elapse_queue_;
  ObSpScLinkQueue get_gts_waiting_queue_;
  bool elapse_waiting_mode_ = false;
  bool get_gts_waiting_mode_ = false;
};

class ObFakeTxLogAdapter : public ObITxLogAdapter, public share::ObThreadPool
{
public:
  virtual int start() {
    int ret = OB_SUCCESS;
    ObThreadPool::set_run_wrapper(MTL_CTX());
    ret = ObThreadPool::start();
    stop_ = false;
    TRANS_LOG(INFO, "start.FakeTxLogAdapter", KP(this));
    return ret;
  }

  void stop() {
    stop_ = true;
    cond_.signal();
    TRANS_LOG(INFO, "stop.FakeTxLogAdapter", KP(this));
    ObThreadPool::stop();
  }

  struct ApplyCbTask : public common::ObLink
  {
    int64_t replay_hint_;
    logservice::AppendCb *cb_;
  };

  struct ReplayCbTask : public common::ObLink
  {
    int64_t replay_hint_;
    char* log_buf_;
    int64_t log_size_;
    palf::LSN lsn_;
    int64_t log_ts_;
  };

  const static int64_t TASK_QUEUE_CNT = 128;
  ObSpScLinkQueue apply_task_queue_arr[TASK_QUEUE_CNT];
  ObSpScLinkQueue replay_task_queue_arr[TASK_QUEUE_CNT];
  int64_t max_submit_scn_ =  OB_INVALID_TIMESTAMP;

  void run1() {
    while(true) {
      int64_t process_cnt = 0;
      bool stop = stop_;
      if (!ATOMIC_LOAD(&pause_)) {
        for (int64_t i = 0; i < TASK_QUEUE_CNT; ++i) {
          while(true) {
            ObLink *task = apply_task_queue_arr[i].pop();
            if (task) {
              ++process_cnt;
              static_cast<ApplyCbTask*>(task)->cb_->on_success();
              delete task;
              ATOMIC_DEC(&inflight_cnt_);
            } else {
              break;
            }
          }
        }
      }
      if (!pause_ && 0 == process_cnt && stop) {
        break;
      } else if (0 == process_cnt && ATOMIC_BCAS(&is_sleeping_, false, true)) {
        auto key = cond_.get_key();
        cond_.wait(key, 10 * 1000);
      }
    }
  }
  void wakeup() { if (ATOMIC_BCAS(&is_sleeping_, true, false)) { cond_.signal(); }}
  template <typename Function> int replay_all(Function& fn)
  {
    LOG_INFO("replay all begin");
    int ret = OB_SUCCESS;

    for (int64_t i = 0; i < TASK_QUEUE_CNT; ++i) {
      while(OB_SUCC(ret)) {
        ReplayCbTask *task = static_cast<ReplayCbTask*>(replay_task_queue_arr[i].pop());
        if (task) {
          ret = fn(task->log_buf_, task->log_size_, task->lsn_, task->log_ts_);
          delete task->log_buf_;
          delete task;
          ATOMIC_DEC(&unreplay_cnt_);
        } else {
          break;
        }
      }
    }
    LOG_INFO("replay all end", K(ret));
    return ret;
  }

  int submit_log(const char *buf,
                 const int64_t size,
                 const int64_t base_ts,
                 ObTxBaseLogCb *cb,
                 const bool need_nonblock)
  {
    int ret = OB_SUCCESS;
    logservice::ObLogBaseHeader base_header;
    int64_t tmp_pos = 0;
    if (OB_FAIL(base_header.deserialize(buf, size, tmp_pos))) {
      LOG_WARN("log base header deserialize error", K(ret));
    } else {
      const int64_t replay_hint = base_header.get_replay_hint();
      const int64_t ts = std::max(base_ts, ts_) + 1;
      int64_t queue_idx = replay_hint % TASK_QUEUE_CNT;
      if (!ATOMIC_LOAD(&log_drop_)) {
        const palf::LSN lsn = palf::LSN(++lsn_);
        cb->set_log_ts(ts);
        cb->set_lsn(lsn);
        ts_ = ts;
        ApplyCbTask *apply_task = new ApplyCbTask();
        apply_task->replay_hint_ = replay_hint;
        apply_task->cb_ = cb;
        max_submit_scn_ = MAX(max_submit_scn_, ts);

        apply_task_queue_arr[queue_idx].push(apply_task);
        ATOMIC_INC(&inflight_cnt_);
        wakeup();

        ReplayCbTask *replay_task = new ReplayCbTask();
        replay_task->replay_hint_ = replay_hint;
        replay_task->log_buf_ = new char[size];
        memcpy(replay_task->log_buf_, buf, size);
        replay_task->log_size_ = size;
        replay_task->lsn_ = lsn;
        replay_task->log_ts_ = ts;
        replay_task_queue_arr[queue_idx].push(replay_task);
        ATOMIC_INC(&unreplay_cnt_);

        TRANS_LOG(INFO, "submit_log",
            K(replay_hint), K(inflight_cnt_), K(unreplay_cnt_), K(queue_idx));
        hex_dump(buf, size, true, OB_LOG_LEVEL_INFO);
      } else {
        TRANS_LOG(INFO, "drop_log",
            K(replay_hint), K(inflight_cnt_), K(unreplay_cnt_), K(queue_idx));
        hex_dump(buf, size, true, OB_LOG_LEVEL_INFO);
      }
    }
    return ret;
  }

  int get_role(bool &is_leader, int64_t &epoch) {
    is_leader = true;
    epoch = 1;
    return OB_SUCCESS;
  }

  int get_max_decided_scn(int64_t &scn)
  {
    int ret = OB_SUCCESS;
    int64_t min_unreplayed_scn = OB_INVALID_TIMESTAMP;
    int64_t min_unapplyed_scn = OB_INVALID_TIMESTAMP;

    for (int64_t i = 0; i < TASK_QUEUE_CNT; ++i) {
      if (!replay_task_queue_arr[i].empty()) {
        int64_t tmp_scn = static_cast<ReplayCbTask *>(replay_task_queue_arr[i].top())->log_ts_;
        if (min_unreplayed_scn != OB_INVALID_TIMESTAMP && tmp_scn != OB_INVALID_TIMESTAMP) {
          min_unreplayed_scn = MIN(tmp_scn, min_unreplayed_scn);
        } else if (tmp_scn != OB_INVALID_TIMESTAMP) {
          min_unreplayed_scn = tmp_scn;
        }
      }
    }

    for (int64_t i = 0; i < TASK_QUEUE_CNT; ++i) {
      if (!apply_task_queue_arr[i].empty()) {
        int64_t tmp_scn = (static_cast<ObTxBaseLogCb *>(
                               (static_cast<ApplyCbTask *>(apply_task_queue_arr[i].top()))->cb_))
                              ->get_log_ts();
        if (min_unapplyed_scn != OB_INVALID_TIMESTAMP && tmp_scn != OB_INVALID_TIMESTAMP) {
          min_unapplyed_scn = MIN(tmp_scn, min_unapplyed_scn);
        } else if (tmp_scn != OB_INVALID_TIMESTAMP) {
          min_unapplyed_scn = tmp_scn;
        }
      }
    }

    if (min_unapplyed_scn != OB_INVALID_TIMESTAMP && min_unreplayed_scn != OB_INVALID_TIMESTAMP) {
      scn = MAX(min_unapplyed_scn, min_unreplayed_scn);
    } else {
      scn = max_submit_scn_;
    }
    if (scn >= 0) {
      scn = scn - 1;
    }
    return OB_SUCCESS;
  }

  int get_inflight_cnt() {
    return ATOMIC_LOAD(&inflight_cnt_);
  }

  int get_unreplay_cnt() {
    return ATOMIC_LOAD(&unreplay_cnt_);
  }

  void set_pause() {
    ATOMIC_SET(&pause_, true);
  }

  void clear_pause() {
    ATOMIC_SET(&pause_, false);
  }

  void set_log_drop() {
    ATOMIC_SET(&log_drop_, true);
  }

  void clear_log_drop() {
    ATOMIC_SET(&log_drop_, false);
  }

  bool pause_ = false;
  bool log_drop_ = false;
  int64_t ts_ = 1;
  int64_t lsn_ = 1;
  int64_t inflight_cnt_ = 0;
  int64_t unreplay_cnt_ = 0;
  bool is_sleeping_ = false;
  common::SimpleCond cond_;
};

class ObFakeTxReplayExecutor : public ObTxReplayExecutor
{
public:
  ObFakeTxReplayExecutor(storage::ObLS *ls,
                         storage::ObLSTxService *ls_tx_srv,
                         const palf::LSN &lsn,
                         const int64_t &log_timestamp)
      : ObTxReplayExecutor(ls, ls_tx_srv, lsn, log_timestamp) {memtable_ = nullptr;}

  ~ObFakeTxReplayExecutor() {}

  static int execute(storage::ObLS *ls,
                     storage::ObLSTxService *ls_tx_srv,
                     const char *buf,
                     const int64_t size,
                     const int skip_pos,
                     const palf::LSN &lsn,
                     const int64_t &log_timestamp,
                     const int64_t &replay_hint,
                     const share::ObLSID &ls_id,
                     const int64_t &tenant_id,
                     memtable::ObMemtable* memtable)
  {
    int ret = OB_SUCCESS;
    ObFakeTxReplayExecutor replay_executor(ls, ls_tx_srv, lsn, log_timestamp);
    if (OB_ISNULL(ls) || OB_ISNULL(ls_tx_srv) || OB_ISNULL(buf) || size <= 0
        || 0 >= log_timestamp || INT64_MAX == log_timestamp || !lsn.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(ERROR, "invaild arguments", K(replay_executor), K(buf), K(size));
    } else if (replay_executor.set_memtable(memtable)) {
    } else if (OB_FAIL(replay_executor.do_replay_(buf,
                                                  size,
                                                  skip_pos,
                                                  replay_hint,
                                                  ls_id,
                                                  tenant_id))) {
      TRANS_LOG(ERROR, "replay_executor.do_replay failed",
          K(replay_executor), K(buf), K(size), K(skip_pos), K(replay_hint), K(ls_id), K(tenant_id));
      hex_dump(buf, size, true, OB_LOG_LEVEL_INFO);
    }
    return ret;
  }

private:
  int set_memtable(memtable::ObMemtable* memtable)
  {
    memtable_ = memtable;
    return OB_SUCCESS;
  }

  int replay_one_row_in_memtable_(memtable::ObMutatorRowHeader& row_head,
                                  memtable::ObMemtableMutatorIterator *mmi_ptr,
                                  memtable::ObEncryptRowBuf &row_buf) override
  {
    int ret = OB_SUCCESS;
    storage::ObStoreCtx storeCtx;
    storeCtx.ls_id_ = ctx_->get_ls_id();
    storeCtx.mvcc_acc_ctx_.tx_ctx_ = ctx_;
    storeCtx.mvcc_acc_ctx_.mem_ctx_ = mt_ctx_;
    storeCtx.log_ts_ = log_ts_ns_;
    storeCtx.tablet_id_ = row_head.tablet_id_;
    storeCtx.mvcc_acc_ctx_.tx_id_ = ctx_->get_trans_id();

    switch (row_head.mutator_type_) {
    case memtable::MutatorType::MUTATOR_ROW: {
      if (OB_FAIL(memtable_->replay_row(storeCtx, mmi_ptr_, row_buf))) {
        TRANS_LOG(WARN, "[Replay Tx] replay row error", K(ret));
      } else {
        TRANS_LOG(INFO, "[Replay Tx] replay row in memtable success");
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "[Replay Tx] Unknown mutator_type", K(row_head.mutator_type_));
    } // default
    } // switch

    return ret;
  }

public:
  memtable::ObMemtable* memtable_;
};

class ObFakeLSTxService : public ObLSTxService
{
public:
  ObFakeLSTxService(ObLS *parent) : ObLSTxService(parent) { }
  ~ObFakeLSTxService() {}
  virtual int64_t get_ls_weak_read_ts() override {
    return 0;
  }
};

} // transaction
} // oceanbase

#endif //OCEANBASE_TRANSACTION_TEST_BASIC_FAKE_DEFINE_
