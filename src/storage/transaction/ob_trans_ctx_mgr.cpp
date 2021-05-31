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

#include "ob_trans_ctx_mgr.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_worker.h"
#include "lib/list/ob_list.h"
#include "lib/container/ob_array.h"
#include "lib/profile/ob_perf_event.h"
#include "observer/ob_server.h"
#include "storage/ob_storage_log_type.h"
#include "ob_trans_factory.h"
#include "ob_trans_functor.h"
#include "ob_trans_result_info_mgr.h"
#include "ob_dup_table.h"

namespace oceanbase {

using namespace common;
using namespace share;
using namespace common::hash;
using namespace storage;
using namespace memtable;
using namespace observer;

namespace transaction {
int ObPartitionTransCtxMgr::init_trans_result_info_mgr_()
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);
  if (OB_ISNULL(trans_result_info_mgr_)) {
    ObTransResultInfoMgr* ptr = NULL;
    if (OB_ISNULL(ptr = ObTransResultInfoMgrFactory::alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(ERROR, "alloc trans result info mgr error", KR(ret), K(partition_));
    } else if (OB_FAIL(ptr->init(&ObPartitionService::get_instance(), partition_))) {
      TRANS_LOG(ERROR, "trans result info mgr init error", KR(ret), K(partition_));
    } else {
      ATOMIC_STORE(&trans_result_info_mgr_, ptr);
    }
  }
  return ret;
}

int ObPartitionTransCtxMgr::init(const ObPartitionKey& partition, const int64_t ctx_type, ObITsMgr* ts_mgr,
    storage::ObPartitionService* partition_service, CtxMap* ctx_map)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(!partition.is_valid()) || OB_UNLIKELY(!ObTransCtxType::is_valid(ctx_type)) ||
             OB_ISNULL(ts_mgr) || OB_ISNULL(partition_service) || OB_ISNULL(ctx_map)) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(ctx_type), KP(ts_mgr), KP(partition_service));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ctx_map_mgr_.init(partition, ctx_map))) {
    TRANS_LOG(WARN, "ctx_map_mgr init fail", KR(ret));
  } else {
    if (ObTransCtxType::PARTICIPANT == ctx_type) {
      bool is_dup_table = false;
      ObTransService* txs = NULL;
      if (NULL == (txs = partition_service->get_trans_service())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "transaction service is null, unexpected error", K(ret), KP(txs));
      } else if (OB_FAIL(txs->check_duplicated_partition(partition_, is_dup_table))) {
        TRANS_LOG(WARN, "check duplicated partition serving error", KR(ret), K(partition_));
      } else if (is_dup_table && OB_FAIL(init_dup_table_mgr())) {
        TRANS_LOG(WARN, "failed to init dup table", K(ret), K(partition));
      } else if (OB_FAIL(aggre_log_container_.init(partition))) {
        TRANS_LOG(WARN, "aggre log container init error", K(ret), K(partition));
      } else {
        // do nothing
      }
    }
    if (OB_SUCC(ret) && (ObTransCtxType::PARTICIPANT == ctx_type)) {
      if (OB_ISNULL(core_local_partition_audit_info_ = ObCoreLocalPartitionAuditInfoFactory::alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "alloc partition audit info error", KR(ret), K(partition));
      } else if (OB_FAIL(core_local_partition_audit_info_->init(OB_PARTITION_AUDIT_LOCAL_STORAGE_COUNT))) {
        TRANS_LOG(WARN, "partition audit info init error", KR(ret), K(partition));
      } else {
        // do nothing
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
      ctx_type_ = ctx_type;
      state_ = ObTransCtxType::SCHEDULER == ctx_type ? State::L_WORKING : State::F_WORKING;
      partition_ = partition;
      ts_mgr_ = ts_mgr;
      TRANS_LOG(INFO, "ObPartitionTransCtxMgr inited success", KP(this), K(partition));
    } else {
      TRANS_LOG(WARN, "ObPartitionTransCtxMgr inited fail", KR(ret), KP(this), K(partition));
    }
  }

  return ret;
}

void ObPartitionTransCtxMgr::destroy()
{
  WLockGuard guard(rwlock_);
  if (IS_INIT) {
    ctx_map_mgr_.destroy();
    if (NULL != trans_result_info_mgr_) {
      ObTransResultInfoMgrFactory::release(static_cast<ObTransResultInfoMgr*>(trans_result_info_mgr_));
      trans_result_info_mgr_ = NULL;
    }
    if (NULL != core_local_partition_audit_info_) {
      ObCoreLocalPartitionAuditInfoFactory::release(core_local_partition_audit_info_);
      core_local_partition_audit_info_ = NULL;
    }
    if (NULL != dup_table_partition_info_) {
      ObDupTablePartitionInfoFactory::release(dup_table_partition_info_);
      dup_table_partition_info_ = NULL;
    }
    if (NULL != dup_table_partition_mgr_) {
      ObDupTablePartitionMgrFactory::release(dup_table_partition_mgr_);
      dup_table_partition_mgr_ = NULL;
    }
    aggre_log_container_.destroy();
    pg_guard_.reset();
    is_inited_ = false;
    is_dup_table_ = false;
    TRANS_LOG(INFO, "ObPartitionTransCtxMgr destroyed", K_(partition), K_(ctx_type));
  }
}

void ObPartitionTransCtxMgr::reset()
{
  is_inited_ = false;
  ctx_type_ = ObTransCtxType::UNKNOWN;
  state_ = State::INVALID;
  ctx_map_mgr_.reset();
  partition_.reset();
  read_only_count_ = 0;
  active_read_write_count_ = 0;
  total_ctx_count_ = 0;
  min_uncommit_prepare_version_ = INT64_MAX;
  max_trans_version_ = 0;
  leader_takeover_ts_.reset();
  max_replay_batch_commit_version_ = 0;
  is_leader_serving_ = false;
  ts_mgr_ = NULL;
  pg_guard_.reset();
  trans_result_info_mgr_ = NULL;
  core_local_partition_audit_info_ = NULL;
  dup_table_partition_info_ = NULL;
  dup_table_partition_mgr_ = NULL;
  compact_mode_ = (int)(ObWorker::CompatMode::INVALID);
  reset_elr_statistic();
  clog_aggregation_buffer_amount_ = 0;
  last_refresh_tenant_config_ts_ = 0;
  restore_snapshot_version_ = OB_INVALID_TIMESTAMP;
  last_restore_log_id_ = OB_INVALID_ID;
  aggre_log_container_.reset();
  is_dup_table_ = false;
}

bool ObPartitionTransCtxMgr::has_valid_compact_mode_()
{
  return (int)(ObWorker::CompatMode::INVALID) != compact_mode_;
}

// compact mode is not allowed to change once created
int ObPartitionTransCtxMgr::set_compact_mode_(const int compact_mode)
{
  int ret = OB_SUCCESS;

  if ((int)(ObWorker::CompatMode::INVALID) != compact_mode_) {
    if (compact_mode_ != compact_mode) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "changing compact mode, unexpected error", KR(ret), K(compact_mode), K(*this));
    }
  } else if ((int)(ObWorker::CompatMode::INVALID) == compact_mode) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguemnt", K(compact_mode), K(*this));
  } else {
    ATOMIC_STORE(&compact_mode_, compact_mode);
  }

  return ret;
}

int ObPartitionTransCtxMgr::process_callback_(const ObEndTransCallbackArray& cb_array) const
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCCESS == ret && i < cb_array.size(); i++) {
    const ObEndTransCallbackItem& item = cb_array[i];
    if (OB_ISNULL(item.cb_)) {
      TRANS_LOG(WARN, "callback pointer is null", K(item));
      ret = OB_ERR_UNEXPECTED;
    } else {
      item.cb_->callback(item.retcode_);
    }
  }

  return ret;
}

void ObPartitionTransCtxMgr::print_all_ctx(const int64_t max_print, const bool verbose)
{
  RLockGuard guard(rwlock_);
  print_all_ctx_(max_print, verbose);
}

void ObPartitionTransCtxMgr::print_all_ctx_(const int64_t max_print, const bool verbose)
{
  PrintFunctor print_fn(max_print, verbose);
  // ignore ret
  ctx_map_mgr_.foreach_ctx(print_fn);
}

//          START      LEADER_REVOKE LEADER_ACTIVE BLOCK     STOP    WAIT  REMOVE
// INIT      F_WORKING  N             N             N         N       N     N
// F_WORKING N          N             L_WORKING     F_BLOCKED STOPPED N     N
// L_WORKING N          F_WORKING     N             L_BLOCKED STOPPED N     N
// F_BLOCKED N          N             L_BLOCKED     F_BLOCKED STOPPED N     N
// L_BLOCKED N          F_BLOCKED     N             L_BLOCKED STOPPED N     N
// STOPPED   N          N             N             N         STOPPED WAIT  N
// WAIT      N          N             N             N         N       N     END
// END       N          N             N             N         N       N     N
int ObPartitionTransCtxMgr::StateHelper::switch_state(const int64_t op)
{
  int ret = OB_SUCCESS;
  static const int64_t N = State::INVALID;
  static const int64_t STATE_MAP[State::MAX][Ops::MAX] = {{State::F_WORKING, N, N, N, N, N, N},
      {N, N, State::L_WORKING, State::F_BLOCKED, State::STOPPED, N, N},
      {N, State::F_WORKING, N, State::L_BLOCKED, State::STOPPED, N, N},
      {N, N, State::L_BLOCKED, State::F_BLOCKED, State::STOPPED, N, N},
      {N, State::F_BLOCKED, N, State::L_BLOCKED, State::STOPPED, N, N},
      {N, State::STOPPED, N, N, State::STOPPED, State::WAIT, N},
      {N, N, N, N, N, N, State::END},
      {N, N, N, N, N, N, N}};

  if (OB_UNLIKELY(!Ops::is_valid(op))) {
    TRANS_LOG(WARN, "invalid argument", K(op));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(!State::is_valid(state_))) {
    TRANS_LOG(WARN, "partition current state is invalid", K_(state), K(op));
    ret = OB_ERR_UNEXPECTED;
  } else {
    const int64_t new_state = STATE_MAP[state_][op];
    if (OB_UNLIKELY(!State::is_valid(new_state))) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      last_state_ = state_;
      state_ = new_state;
      is_switching_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    TRANS_LOG(INFO, "partition switch state success", K_(last_state), K_(state), K(op));
  } else {
    TRANS_LOG(ERROR, "partition switch state error", KR(ret), K_(state), K(op));
  }

  return ret;
}

void ObPartitionTransCtxMgr::StateHelper::restore_state()
{
  if (is_switching_) {
    is_switching_ = false;
    state_ = last_state_;
  }
}

int ObPartitionTransCtxMgr::get_trans_ctx(const ObTransID& trans_id, const bool for_replay, const bool is_readonly,
    const bool is_bounded_staleness_read, const bool need_completed_dirty_txn, bool& alloc, ObTransCtx*& ctx)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);

  if (OB_FAIL(get_trans_ctx_(
          trans_id, for_replay, is_readonly, is_bounded_staleness_read, need_completed_dirty_txn, alloc, ctx))) {
    TRANS_LOG(DEBUG, "get transaction context error", K(trans_id), K(for_replay), K(is_readonly), K(alloc));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionTransCtxMgr::get_partition_state_()
{
  int ret = OB_SUCCESS;

  if (is_stopped_()) {
    ret = OB_PARTITION_IS_STOPPED;
  } else if (is_blocked_()) {
    ret = OB_PARTITION_IS_BLOCKED;
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionTransCtxMgr::check_and_inc_read_only_trx_count()
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);
  if (OB_FAIL(get_partition_state_())) {
    TRANS_LOG(WARN, "partition state no success", KR(ret), K_(partition));
  } else {
    inc_total_ctx_count();
    inc_read_only_count();
  }

  return ret;
}

int ObPartitionTransCtxMgr::check_and_dec_read_only_trx_count()
{
  RLockGuard guard(rwlock_);
  desc_total_ctx_count();
  desc_read_only_count();
  return OB_SUCCESS;
}

int ObPartitionTransCtxMgr::remove_callback_for_uncommited_txn(ObMemtable* mt)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("remove callback for uncommited txn", 10L * 1000L);

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited", K_(partition));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(mt)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable is null", K_(partition));
  } else {
    ObRemoveCallbackFunctor fn(mt);
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
      TRANS_LOG(WARN, "for each transaction context error", KR(ret), KP(mt));
    } else {
      TRANS_LOG(INFO, "remove callback for uncommited txn success", KP(mt));
    }
  }

  return ret;
}

int ObPartitionTransCtxMgr::remove_mem_ctx_for_trans_ctx(memtable::ObMemtable* mt)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("remove mem ctx for trans ctx", 1000L);

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited", K_(partition));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(mt)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable is null", K_(partition));
  } else {
    ObRemoveMemCtxFunctor fn(mt);
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
      TRANS_LOG(WARN, "for each transaction context error", KR(ret), KP(mt));
    } else {
      TRANS_LOG(INFO, "remove callback for uncommited txn success", KP(mt));
    }
  }

  return ret;
}

int ObPartitionTransCtxMgr::get_trans_ctx_(const ObTransID& trans_id, const bool for_replay, const bool is_readonly,
    const bool is_bounded_staleness_read, const bool need_completed_dirty_txn, bool& alloc, ObTransCtx*& ctx)
{
  int ret = OB_SUCCESS;
  ObTransCtx* tmp_ctx = NULL;
  const int64_t MAX_LOOP_COUNT = 100;
  int64_t count = 0;
  int64_t gts = 0;
  MonotonicTs unused_ts = MonotonicTs::current_time();

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!trans_id.is_valid()) || OB_ISNULL(ts_mgr_)) {
    TRANS_LOG(WARN, "invalid argument", K_(partition), K(trans_id), KP(ts_mgr_));
    ret = OB_INVALID_ARGUMENT;
    // TODO, here needs to check the gts status, if false, then the following checks are not necessary.
  } else if (alloc && is_master_() && !is_leader_serving_ && (ObTransCtxType::SCHEDULER != ctx_type_) &&
             (OB_FAIL(ts_mgr_->get_gts(partition_.get_tenant_id(), leader_takeover_ts_, NULL, gts, unused_ts)) ||
                 (max_replay_batch_commit_version_ > 0 && OB_FAIL(ts_mgr_->wait_gts_elapse(partition_.get_tenant_id(),
                                                              max_replay_batch_commit_version_))))) {
    if (OB_EAGAIN != ret) {
      TRANS_LOG(WARN,
          "get gts error",
          KR(ret),
          K_(leader_takeover_ts),
          K(gts),
          K_(partition),
          K_(max_replay_batch_commit_version));
    } else {
      // rewrite error no, retry the stmt
      ret = OB_NOT_MASTER;
      TRANS_LOG(WARN, "gts is not ready", KR(ret), "tenant_id", partition_.get_tenant_id(), K_(leader_takeover_ts));
    }
  } else {
    if (OB_SUCC(ctx_map_mgr_.get(trans_id, tmp_ctx))) {
      if (OB_ISNULL(tmp_ctx)) {
        TRANS_LOG(WARN, "ctx is NULL", "ctx", OB_P(tmp_ctx));
        ret = OB_ERR_UNEXPECTED;
      } else {
        // FIXME
        bool is_inited = tmp_ctx->is_inited();
        while (!is_inited && count < MAX_LOOP_COUNT) {
          count++;
          ObTransCond::usleep(1);
          is_inited = tmp_ctx->is_inited();
          if (1 == count) {
            tmp_ctx->set_partition(partition_);
          }
        }
        if (!is_inited) {
          TRANS_LOG(WARN, "get transaction context not inited", K(trans_id));
        }

        // for trans with is_exiting, we consider the thans has been ended, but the trans status may be needed.
        if (!need_completed_dirty_txn && tmp_ctx->is_exiting() && tmp_ctx->is_dirty()) {
          ret = OB_TRANS_CTX_NOT_EXIST;
          ctx_map_mgr_.revert(tmp_ctx);
          tmp_ctx = NULL;
        }

        ctx = tmp_ctx;
        alloc = false;
      }
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      TRANS_LOG(ERROR, "get transaction context error", KR(ret), K(trans_id));
    } else {
      tmp_ctx = NULL;
      if (!alloc) {
        ret = OB_TRANS_CTX_NOT_EXIST;
      } else if ((!is_bounded_staleness_read) && (!is_master_()) && (!for_replay)) {
        TRANS_LOG(WARN,
            "partition is not master",
            K(is_bounded_staleness_read),
            K_(partition),
            K(trans_id),
            K_(state),
            K(for_replay));
        ret = OB_NOT_MASTER;
      } else if (is_stopped_()) {
        TRANS_LOG(WARN, "partition is stopped", K_(partition), K(trans_id));
        ret = OB_PARTITION_IS_STOPPED;
      } else if (!for_replay && is_blocked_()) {
        TRANS_LOG(WARN, "partition is blocked", K_(partition), K(trans_id));
        ret = OB_PARTITION_IS_BLOCKED;
      } else if (OB_ISNULL(tmp_ctx = ObTransCtxFactory::alloc(ctx_type_))) {
        TRANS_LOG(WARN, "alloc transaction context error", K_(partition), K(trans_id), K_(ctx_type));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(tmp_ctx->set_partition_trans_ctx_mgr(this))) {
        TRANS_LOG(WARN, "set partition trans ctx mgr error", KR(ret), K_(partition), K(trans_id));
        ObTransCtxFactory::release(tmp_ctx);
        tmp_ctx = NULL;
        // insert new transaction context into context map
      } else if (OB_FAIL(ctx_map_mgr_.insert_and_get(trans_id, tmp_ctx))) {
        ObTransCtxFactory::release(tmp_ctx);
        tmp_ctx = NULL;
        if (OB_ENTRY_EXIST != ret) {
          TRANS_LOG(WARN, "insert transaction context error", KR(ret), K_(partition), K(trans_id));
          // the case may occur if context are created concurrently.
        } else if (OB_SUCC(ctx_map_mgr_.get(trans_id, tmp_ctx))) {
          if (OB_ISNULL(tmp_ctx)) {
            TRANS_LOG(WARN, "ctx is NULL", "ctx", OB_P(tmp_ctx));
            ret = OB_ERR_UNEXPECTED;
          } else {
            // FIXME
            bool is_inited = tmp_ctx->is_inited();
            while (!is_inited && count < MAX_LOOP_COUNT) {
              count++;
              ObTransCond::usleep(1);
              is_inited = tmp_ctx->is_inited();
            }
            if (!is_inited) {
              TRANS_LOG(WARN, "get transaction context not inited", K(trans_id));
            }
            ctx = tmp_ctx;
            alloc = false;
          }
        } else {
          // do nothing
        }
      } else {
        ctx = tmp_ctx;
        ctx->set_for_replay(for_replay);
        ctx->set_readonly(is_readonly);
        ctx->set_partition(partition_);
        ctx->set_bounded_staleness_read(is_bounded_staleness_read);
        inc_total_ctx_count();
        if (is_master_() && !is_leader_serving_) {
          is_leader_serving_ = true;
        }
      }
    }
    if (REACH_TIME_INTERVAL(OB_TRANS_STATISTICS_INTERVAL)) {
      TRANS_LOG(INFO,
          "transaction statistics",
          K_(partition),
          "total_count",
          get_ctx_count_(),
          K_(read_only_count),
          K_(active_read_write_count));
    }
  }

  return ret;
}

int ObPartitionTransCtxMgr::leader_takeover(const int64_t checkpoint)
{
  int ret = OB_SUCCESS;

  ObTimeGuard timeguard("ctxmgr leader takeover");
  WLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited", K_(partition));
    ret = OB_NOT_INIT;
  } else if (is_master_()) {
    TRANS_LOG(ERROR, "partition is master already", "manager", *this);
    ret = OB_ERR_UNEXPECTED;
  } else {
    leader_takeover_ts_ = MonotonicTs::current_time();
    const bool verbose = true;
    print_all_ctx_(MAX_HASH_ITEM_PRINT, verbose);
    LeaderTakeoverFunctor fn(partition_, checkpoint);
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
      TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
    } else {
      TRANS_LOG(INFO, "partition leader takeover success", K(checkpoint), "manager", *this);
    }
  }
  if (timeguard.get_diff() > 3 * 1000000) {
    TRANS_LOG(WARN, "leader takeover use too much time", K(timeguard), "manager", *this);
  }

  return ret;
}

int ObPartitionTransCtxMgr::recover_pg_guard()
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("recover_pg_guard");
  WLockGuard guard(rwlock_);

  if (NULL == pg_guard_.get_partition_group()) {
    if (OB_FAIL(ObPartitionService::get_instance().get_partition(partition_, pg_guard_))) {
      TRANS_LOG(WARN, "get partition group error", K(ret), K(partition_));
    }
  }

  return ret;
}

int ObPartitionTransCtxMgr::leader_active(const LeaderActiveArg& arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t gts = 0;
  StateHelper state_helper(state_);

  ObTimeGuard timeguard("ctxmgr leader active");
  WLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited", K_(partition));
    ret = OB_NOT_INIT;
    //} else if (is_stopped_()) {
    //  TRANS_LOG(WARN, "partition is stopped", K_(partition));
    //  ret = OB_PARTITION_IS_STOPPED;
  } else if (OB_FAIL(state_helper.switch_state(Ops::LEADER_ACTIVE))) {
    TRANS_LOG(WARN, "switch state error", KR(ret), "manager", *this);
  } else if (OB_ISNULL(ts_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ts source is null");
    // cache the pointer of pg when leader takes over, this is used to optimize performance
  } else if (NULL == pg_guard_.get_partition_group() &&
             OB_FAIL(ObPartitionService::get_instance().get_partition(partition_, pg_guard_))) {
    TRANS_LOG(WARN, "get partition group error", K(ret), K(partition_));
  } else {
    MonotonicTs unused_ts = MonotonicTs::current_time();
    // Here, according to gts only, check whether result is newest and is_leader_serving is modified
    // Do not affect the logic of leader active
    if (OB_SUCCESS !=
        (tmp_ret = ts_mgr_->get_gts(partition_.get_tenant_id(), leader_takeover_ts_, NULL, gts, unused_ts))) {
      if (OB_EAGAIN != tmp_ret) {
        TRANS_LOG(WARN, "get gts error", "ret", tmp_ret, K_(leader_takeover_ts), K(gts), K_(partition));
      }
    } else if (max_replay_batch_commit_version_ > 0 &&
               OB_SUCCESS !=
                   (tmp_ret = ts_mgr_->wait_gts_elapse(partition_.get_tenant_id(), max_replay_batch_commit_version_))) {
      if (OB_EAGAIN != tmp_ret) {
        TRANS_LOG(WARN, "wait gts error", "ret", tmp_ret, K_(partition), K_(max_replay_batch_commit_version));
      }
    } else {
      is_leader_serving_ = true;
    }
    timeguard.click();
    if (NULL != dup_table_partition_mgr_) {
      (void)dup_table_partition_mgr_->leader_active(
          dup_table_partition_info_->get_replay_log_id(), arg.is_elected_by_changing_leader_);
    }
    const bool verbose = true;
    print_all_ctx_(MAX_HASH_ITEM_PRINT, verbose);
    LeaderActiveFunctor fn(partition_, arg);
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
      TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
    } else {
      TRANS_LOG(INFO, "partition leader active success", "manager", *this);
    }
    if (OB_FAIL(ret)) {
      state_helper.restore_state();
    }
  }
  if (timeguard.get_diff() > 3 * 1000000) {
    TRANS_LOG(WARN, "leader active use too much time", K(timeguard), "manager", *this);
  }

  return ret;
}

int ObPartitionTransCtxMgr::active(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(!is_stopped_() || partition_ != partition)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected partition state", KR(ret), K(partition), K(this));
  } else {
    state_ = State::F_WORKING;
  }

  return ret;
}

// stop operation kills the trans gracefully (gracefule = true)
// meanwhile callback sql for trans which has called end_trans
int ObPartitionTransCtxMgr::stop(const bool graceful)
{
  int ret = OB_SUCCESS;
  // const bool graceful = true;
  StateHelper state_helper(state_);
  ObEndTransCallbackArray cb_array;
  const KillTransArg arg(graceful, false);
  ObTimeGuard timeguard("ctxmgr stop");
  {
    WLockGuard guard(rwlock_);
    if (OB_FAIL(cb_array.reserve(ctx_map_mgr_.estimate_count()))) {
      TRANS_LOG(WARN, "reserve callback array error", KR(ret));
    } else {
      {
        WLockGuard guard(minor_merge_lock_);
        if (OB_FAIL(state_helper.switch_state(Ops::STOP))) {
          TRANS_LOG(WARN, "switch state error", KR(ret), "manager", *this);
        }
      }

      if (OB_SUCC(ret)) {
        KillTransCtxFunctor fn(arg, cb_array);
        fn.set_release_audit_mgr_lock(true);
        if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
          TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
        }
        if (OB_FAIL(ret)) {
          state_helper.restore_state();
        }
      }
    }
  }
  if (timeguard.get_diff() > 3 * 1000000) {
    TRANS_LOG(WARN, "stop trans use too much time", K(timeguard), "manager", *this);
  }
  // run callback out of lock, ignore ret
  (void)process_callback_(cb_array);

  return ret;
}

int ObPartitionTransCtxMgr::inactive_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("ctxmgr inactive_tenant");
  ObEndTransCallbackArray cb_array;
  {
    WLockGuard guard(rwlock_);
    if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
      TRANS_LOG(WARN, "invalid argument", K(tenant_id));
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(cb_array.reserve(ctx_map_mgr_.estimate_count()))) {
      TRANS_LOG(WARN, "reserve callback array error", KR(ret));
    } else {
      InactiveCtxFunctor fn(tenant_id, cb_array);
      if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
        TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
      }
    }
  }
  if (timeguard.get_diff() > 3 * 1000000) {
    TRANS_LOG(WARN, "inactive_tenant use too much time", K(timeguard), "manager", *this);
  }
  // run callback out of lock, ignore ret
  (void)process_callback_(cb_array);

  return ret;
}

int ObPartitionTransCtxMgr::kill_all_trans(const KillTransArg& arg, bool& is_all_trans_clear)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("ctxmgr kill_all_trans");
  ObEndTransCallbackArray cb_array;
  {
    WLockGuard guard(rwlock_);
    KillTransCtxFunctor fn(arg, cb_array);
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
      TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
    }
    is_all_trans_clear = (get_ctx_count_() == 0);
  }
  if (timeguard.get_diff() > 3 * 1000000) {
    TRANS_LOG(WARN, "kill_all_trans use too much time", K(timeguard), "manager", *this);
  }
  (void)process_callback_(cb_array);
  return ret;
}

int ObPartitionTransCtxMgr::calculate_trans_cost(const ObTransID& tid, uint64_t& cost)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTransCtx* ctx = NULL;
  ObPartTransCtx* part_ctx = NULL;

  RLockGuard guard(rwlock_);

  if (!tid.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(tid));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ctx_map_mgr_.get(tid, ctx))) {
    ret = OB_SUCCESS;
  } else {
    if (OB_ISNULL(part_ctx = static_cast<ObPartTransCtx*>(ctx))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "cast ctx_base to trans_part_ctx error", KR(ret), K(*ctx));
    } else if (OB_FAIL(part_ctx->calculate_trans_ctx_cost(cost))) {
      TRANS_LOG(WARN, "calculate trans ctx cost error", KR(ret), K(tid), K(*part_ctx));
    }
    if (OB_SUCCESS != (tmp_ret = revert_trans_ctx_(ctx))) {
      TRANS_LOG(WARN, "revert trans_ctx error", KR(ret), K(*ctx));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

int ObPartitionTransCtxMgr::block(bool& is_all_trans_clear)
{
  int ret = OB_SUCCESS;
  StateHelper state_helper(state_);
  WLockGuard guard(rwlock_);

  if (OB_FAIL(state_helper.switch_state(Ops::BLOCK))) {
    TRANS_LOG(WARN, "switch state error", KR(ret), "manager", *this);
  } else {
    // is_all_trans_clear = (get_ctx_count_() == 0);
    is_all_trans_clear = (get_read_only_count() + get_active_read_write_count() == 0);
    if (get_ctx_count_() != get_read_only_count() + get_active_read_write_count()) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        TRANS_LOG(INFO,
            "some zombie ctx not released",
            K_(total_ctx_count),
            K_(read_only_count),
            K_(active_read_write_count));
      }
    }
    if (OB_FAIL(ret)) {
      state_helper.restore_state();
    }
  }

  return ret;
}

int ObPartitionTransCtxMgr::release_all_trans_ctx_()
{
  int ret = OB_SUCCESS;
  ReleaseAllTransCtxFunctor fn(*this);
  if (OB_FAIL(ctx_map_mgr_.remove_if(fn))) {
    TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
  } else {
    TRANS_LOG(
        DEBUG, "for each transaction context success", "manager", *this, "released_count", fn.get_released_count());
  }

  return ret;
}

int ObPartitionTransCtxMgr::wait_1pc_trx_end(bool& all_1pc_trx_end)
{
  int ret = OB_SUCCESS;
  all_1pc_trx_end = false;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else {
    WaitAll1PCTrxEndFunctor functor;
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(functor))) {
      TRANS_LOG(WARN, "foreach wait 1pc trx end error", KR(ret), K(*this));
    } else {
      all_1pc_trx_end = true;
    }
  }

  return ret;
}

int ObPartitionTransCtxMgr::check_all_trans_in_trans_table_state()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else {
    CheckAllTransInTransTableStateFunctor functor;
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(functor))) {
      TRANS_LOG(WARN, "foreach check all trans in trans table state error", KR(ret), K(*this));
    } else if (!functor.is_all_in_trans_table_state()) {
      ret = OB_EAGAIN;
      TRANS_LOG(WARN, "check all trans in trans table state is not all finished", K(ret), K(*this));
    }
  }

  return ret;
}

int ObPartitionTransCtxMgr::submit_log_for_split(bool& log_finished)
{
  int ret = OB_SUCCESS;
  log_finished = false;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else {
    SubmitLogForSplitFunctor functor;
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(functor))) {
      TRANS_LOG(WARN, "foreach wait 1pc trx end error", KR(ret), K(*this));
    } else {
      log_finished = functor.is_log_finished();
    }
  }

  return ret;
}

int ObPartitionTransCtxMgr::leader_revoke(const bool first_check)
{
  int ret = OB_SUCCESS;
  StateHelper state_helper(state_);
  ObEndTransCallbackArray cb_array;
  ObTimeGuard timeguard("ctxmgr leader revoke");

  {
    WLockGuard guard(rwlock_);

    if (IS_NOT_INIT) {
      TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
      ret = OB_NOT_INIT;
      //} else if (is_stopped_()) {
      //  TRANS_LOG(WARN, "partition is stopped", K_(partition));
      //  ret = OB_PARTITION_IS_STOPPED;
    } else if (OB_FAIL(cb_array.reserve(ctx_map_mgr_.estimate_count()))) {
      TRANS_LOG(WARN, "reserve callback array error", KR(ret));
      ret = OB_EAGAIN;
    } else if (OB_FAIL(state_helper.switch_state(Ops::LEADER_REVOKE))) {
      TRANS_LOG(WARN, "switch state error", KR(ret), "manager", *this);
    } else {
      int64_t retry_count = 0;
      const bool verbose = true;
      is_leader_serving_ = false;
      if (NULL != dup_table_partition_mgr_) {
        (void)dup_table_partition_mgr_->leader_revoke();
      }
      print_all_ctx_(MAX_HASH_ITEM_PRINT, verbose);
      LeaderRevokeFunctor fn(first_check, *this, cb_array, retry_count);
      if (OB_FAIL(ctx_map_mgr_.remove_if((fn)))) {
        TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
      } else if (0 < retry_count) {
        TRANS_LOG(WARN, "partition leader revoke need retry", "manager", *this, K(first_check), K(retry_count));
        ret = OB_EAGAIN;
      } else {
        ret = aggre_log_container_.leader_revoke();
      }

      if (OB_FAIL(ret)) {
        state_helper.restore_state();
      }
    }
  }
  if (timeguard.get_diff() > 3 * 1000000) {
    TRANS_LOG(WARN, "leader revoke use too much time", K(timeguard), "manager", *this);
  }
  // run callback out of lock, ignore ret
  (void)process_callback_(cb_array);

  return ret;
}

int ObPartitionTransCtxMgr::replay_start_working_log(const int64_t timestamp, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_TIMESTAMP != restore_snapshot_version_ && OB_INVALID_ID != last_restore_log_id_) {
    ObClearTransAfterRestoreLog fn(restore_snapshot_version_, last_restore_log_id_, timestamp);
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
      ret = fn.get_ret();
      TRANS_LOG(WARN, "for each transaction context error", K(ret), "manager", *this);
    } else {
      TRANS_LOG(INFO, "success to clear_trans_after_restore", K(ret), "manager", *this);
      last_restore_log_id_ = OB_INVALID_ID;
      restore_snapshot_version_ = OB_INVALID_TIMESTAMP;
    }
  } else {
    TRANS_LOG(INFO,
        "skip restore when replay start working",
        K(restore_snapshot_version_),
        K(last_restore_log_id_),
        K(timestamp));
  }

  if (OB_SUCC(ret)) {
    ReplayStartWorkingLogFunctor functor(timestamp, log_id);
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(functor))) {
      TRANS_LOG(WARN, "foreach replay start working log error", KR(ret), K(timestamp), K(log_id));
    }
  }

  return ret;
}

// get min prepare version of trans module in current partition
// cache min_uncommit_prepare_version_,
// due to the disordered replay, version number for slave read may fallback
int ObPartitionTransCtxMgr::get_min_uncommit_prepare_version(int64_t& min_prepare_version)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);

  IterateMinPrepareVersionFunctor fn;
  if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
    TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
  } else {
    min_prepare_version = fn.get_min_prepare_version();
  }

  return ret;
}

// get min prepare version of uncommitted trans or committed trans
// whose prepare log ts less than log_ts and commit log ts greater than log_ts in current partition
int ObPartitionTransCtxMgr::get_min_prepare_version(const int64_t log_ts, int64_t& min_prepare_version)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);

  IterateMinPrepareVersionBeforeLogtsFunctor fn(log_ts);
  if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
    TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
  } else {
    min_prepare_version = fn.get_min_prepare_version();
  }

  return ret;
}

int ObPartitionTransCtxMgr::check_schema_version_elapsed(
    const int64_t schema_version, const int64_t refreshed_schema_ts)
{
  int ret = OB_SUCCESS;

  ObTimeGuard timeguard("ctxmgr check_schema_version_elapsed");
  WLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_master_()) {
    ret = OB_NOT_MASTER;
    TRANS_LOG(WARN, "partition is not master", K(ret), K(partition_), K(schema_version), K(refreshed_schema_ts));
  } else {
    IterateTransSchemaVersionStat fn(schema_version, refreshed_schema_ts);
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
      ret = fn.get_ret();
      TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
    }
  }
  if (timeguard.get_diff() > 3 * 1000000) {
    TRANS_LOG(WARN, "check_schema_version_elapsed use too much time", K(timeguard), "manager", *this);
  }

  return ret;
}

int ObPartitionTransCtxMgr::get_min_uncommit_log(uint64_t& min_uncommit_log_id, int64_t& min_uncommit_log_ts)
{
  int ret = OB_SUCCESS;
  uint64_t tmp_min_log_id = UINT64_MAX;
  int64_t tmp_min_log_ts = INT64_MAX;
  RLockGuard guard(rwlock_);

  // The traverse order can't be changed, first active transaction, then transaction status table!
  IterateMinLogIdFunctor fn;
  if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
    TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
  } else if (OB_ISNULL(trans_result_info_mgr_)) {
    min_uncommit_log_id = UINT64_MAX;
    min_uncommit_log_ts = INT64_MAX;
  } else if (OB_FAIL(trans_result_info_mgr_->get_min_log(tmp_min_log_id, tmp_min_log_ts))) {
    TRANS_LOG(WARN, "trans result info mgr get min log id error", KR(ret), K_(partition));
  } else {
    min_uncommit_log_id = std::min(tmp_min_log_id, fn.get_min_uncommit_log_id());
    min_uncommit_log_ts = std::min(tmp_min_log_ts, fn.get_min_uncommit_log_ts());
    if (0 == min_uncommit_log_id) {
      TRANS_LOG(ERROR,
          "unexpected min uncommit log id",
          K(min_uncommit_log_id),
          "elr_min_log_id",
          tmp_min_log_id,
          "trx_min_log_id",
          fn.get_min_uncommit_log_id(),
          "manager",
          *this);
    }
  }

  return ret;
}

int ObPartitionTransCtxMgr::gc_trans_result_info(const int64_t checkpoint_ts)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);

  if (checkpoint_ts <= 0) {
    // do nothing
  } else if (OB_ISNULL(trans_result_info_mgr_)) {
    // lazy init , null permited
  } else if (OB_FAIL(trans_result_info_mgr_->try_gc_trans_result_info(checkpoint_ts))) {
    TRANS_LOG(WARN, "trans result info mgr gc error", K(ret), K_(partition));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionTransCtxMgr::check_ctx_create_timestamp_elapsed(const int64_t ts)
{
  int ret = OB_SUCCESS;

  ObTimeGuard timeguard("ctxmgr check_ctx_create_timestamp_elapsed");
  WLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
    ret = OB_NOT_INIT;
    // discuss with @, there is no need to check whether it is master
    // this interface is called by leader or follower
    //} else if (!is_master_()) {
    //  ret = OB_NOT_MASTER;
    //  TRANS_LOG(WARN, "partition is not master", K(ret), K(partition_), K(ts));
  } else {
    IterateCtxCreateTimestamp fn(ts);
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
      if (OB_EAGAIN != ret) {
        TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
      }
    }
  }
  if (timeguard.get_diff() > 3 * 1000000) {
    TRANS_LOG(WARN, "check_ctx_create_timestamp_elapsed use too much time", K(timeguard), "manager", *this);
  }

  return ret;
}

void ObPartitionTransCtxMgr::reset_elr_statistic()
{
  ATOMIC_STORE(&with_dependency_trx_count_, 0);
  ATOMIC_STORE(&without_dependency_trx_count_, 0);
  ATOMIC_STORE(&end_trans_by_prev_count_, 0);
  ATOMIC_STORE(&end_trans_by_checkpoint_count_, 0);
  ATOMIC_STORE(&end_trans_by_self_count_, 0);
}

int ObPartitionTransCtxMgr::iterate_trans_stat(ObTransStatIterator& trans_stat_iter)
{
  int ret = OB_SUCCESS;

  RLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else {
    IterateTransStatFunctor fn(trans_stat_iter);
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
      TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
    }
  }

  return ret;
}

int ObPartitionTransCtxMgr::set_last_restore_log_id(const uint64_t last_restore_log_id)
{
  int ret = OB_SUCCESS;
  if (ATOMIC_BCAS(&last_restore_log_id_, OB_INVALID_ID, last_restore_log_id)) {
    TRANS_LOG(INFO, "set_last_restore_log_id", K(last_restore_log_id), "manager", *this);
  } else if (last_restore_log_id != ATOMIC_LOAD(&last_restore_log_id_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid last_restore_log_id", KR(ret), K(last_restore_log_id), "manager", *this);
  }

  return ret;
}

int ObPartitionTransCtxMgr::set_restore_snapshot_version(const int64_t restore_snapshot_version)
{
  int ret = OB_SUCCESS;
  if (ATOMIC_BCAS(&restore_snapshot_version_, OB_INVALID_TIMESTAMP, restore_snapshot_version)) {
    TRANS_LOG(INFO, "set_restore_snapshot_version", K(restore_snapshot_version), "manager", *this);
  } else if (restore_snapshot_version != ATOMIC_LOAD(&restore_snapshot_version_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid restore_snapshot_version", KR(ret), K(restore_snapshot_version), "manager", *this);
  }

  return ret;
}

int ObPartitionTransCtxMgr::update_restore_replay_info(
    const int64_t restore_snapshot_version, const uint64_t last_restore_log_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_restore_snapshot_version(restore_snapshot_version))) {
    TRANS_LOG(WARN,
        "faile to set_restore_snapshot_version",
        KR(ret),
        K(restore_snapshot_version),
        K(last_restore_log_id),
        "manager",
        *this);
  } else if (OB_FAIL(set_last_restore_log_id(last_restore_log_id))) {
    TRANS_LOG(WARN,
        "faile to set_last_restore_log_id",
        KR(ret),
        K(restore_snapshot_version),
        K(last_restore_log_id),
        "manager",
        *this);
  } else {
    TRANS_LOG(INFO,
        "success to update_restore_replay_info",
        KR(ret),
        K(restore_snapshot_version),
        K(last_restore_log_id),
        "manager",
        *this);
  }
  return ret;
}

bool ObPartitionTransCtxMgr::is_clog_aggregation_enabled()
{
  const int64_t cur_ts = ObClockGenerator::getClock();
  if (OB_UNLIKELY(cur_ts - ATOMIC_LOAD(&last_refresh_tenant_config_ts_) > 5000000)) {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(partition_.get_tenant_id()));
    ATOMIC_STORE(&clog_aggregation_buffer_amount_, tenant_config->_clog_aggregation_buffer_amount);
    ATOMIC_STORE(&last_refresh_tenant_config_ts_, cur_ts);
  }
  return (ATOMIC_LOAD(&clog_aggregation_buffer_amount_) > 0);
}

int ObPartitionTransCtxMgr::iterate_trans_lock_stat(ObTransLockStatIterator& trans_lock_stat_iter)
{
  int ret = OB_SUCCESS;

  RLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else {
    IterateTransLockStatFunctor fn(trans_lock_stat_iter);
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
      TRANS_LOG(WARN, "for each trans context error", KR(ret), "manager", *this);
    }
  }

  return ret;
}

int ObPartitionTransCtxMgr::iterate_trans_result_info_in_TRIM(ObTransResultInfoStatIterator& iter)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(trans_result_info_mgr_)) {
    // lazy init trans_result_info_mgr_
  } else {
    IterateTransResultInfoFunctor functor(iter, partition_, GCTX.self_addr_);
    ObTransResultInfoMgr* mgr = static_cast<ObTransResultInfoMgr*>(trans_result_info_mgr_);
    if (OB_FAIL(mgr->for_each<IterateTransResultInfoFunctor>(functor))) {
      TRANS_LOG(WARN, "for each trans result info error", KR(ret), K(*this));
    }
  }

  return ret;
}

int ObPartitionTransCtxMgr::iterate_duplicate_partition_stat(ObDuplicatePartitionStatIterator& iter)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_dup_table_) {
    // do nothing
  } else if (OB_ISNULL(dup_table_partition_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "dup table partition mgr is NULL", KR(ret), K_(partition));
  } else if (!is_master_()) {
    // do nothing
  } else {
    ObDuplicatePartitionStat stat;
    if (OB_FAIL(stat.init(GCTX.self_addr_,
            partition_,
            dup_table_partition_mgr_->get_cur_log_id(),
            dup_table_partition_mgr_->is_master(),
            dup_table_partition_mgr_->get_dup_table_lease_info_hashmap()))) {
      TRANS_LOG(WARN, "ObDuplicatePartitionStat init error", KR(ret), K_(partition));
    } else if (OB_FAIL(iter.push(stat))) {
      TRANS_LOG(WARN, "ObDuplicatePartitionStat push error", KR(ret), K_(partition));
    } else {
      // do nothing
    }
  }

  return ret;
}

// to increase the ctx ref
int ObPartitionTransCtxMgr::get_trans_ctx_(const ObTransID& trans_id)
{
  int ret = OB_SUCCESS;
  ObTransCtx* ctx = NULL;

  if (OB_UNLIKELY(!trans_id.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(trans_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ctx_map_mgr_.get(trans_id, ctx))) {
    TRANS_LOG(WARN, "get transaction context from hashmap error", KR(ret), K(trans_id));
  } else {
    // do nothing
  }
  UNUSED(ctx);

  return ret;
}

int ObPartitionTransCtxMgr::get_trans_ctx_(const ObTransID& trans_id, ObTransCtx*& ctx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!trans_id.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(trans_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ctx_map_mgr_.get(trans_id, ctx))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TRANS_CTX_NOT_EXIST;
    } else {
      TRANS_LOG(WARN, "get transaction context from hashmap error", KR(ret), K(trans_id));
    }
  }

  return ret;
}

// to decrease the ctx ref
int ObPartitionTransCtxMgr::revert_trans_ctx_(ObTransCtx* ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "invalid argument", KP(ctx));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ctx_map_mgr_.revert(ctx);
  }

  return ret;
}

int ObPartitionTransCtxMgr::erase_trans_ctx_(const ObTransID& trans_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!trans_id.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(trans_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ctx_map_mgr_.del(trans_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(WARN, "erase transaction context from hashmap error", KR(ret), K_(partition), K(trans_id));
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionTransCtxMgr::revert_trans_ctx(ObTransCtx* ctx)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "invalid argument", KP(ctx));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ctx_map_mgr_.revert(ctx);
  }

  return ret;
}

// The checkpoint process can be unlocked:
// (1) the security of accessing memory can be ensured by hashmap;
// (2) because PartTransCtxMgr is protected by read-write lock,
//     the partition_trans_ctx_mgr accessed here is not released
int ObPartitionTransCtxMgr::checkpoint(const int64_t checkpoint_base_version, storage::ObPartitionLoopWorker* lp_worker)
{
  int ret = OB_SUCCESS;
  int64_t total_ckp_cnt = 0;
  int64_t ckp_succ_cnt = 0;
  ObTimeGuard timeguard("checkpoint ctxmgr");
  // RLockGuard guard(rwlock_);

  if (!is_master_()) {
    timeguard.click();
    const int64_t start_ckp_ts = ObTimeUtility::current_time();
    CheckpointFunctor functor(lp_worker, start_ckp_ts, checkpoint_base_version, total_ckp_cnt, ckp_succ_cnt);
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(functor))) {
      TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
    }
  }
  if (timeguard.get_diff() > 100 * 1000) {
    TRANS_LOG(WARN, "checkpoint use too much time", K(total_ckp_cnt), K(ckp_succ_cnt), K(timeguard), K_(partition));
  } else if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
    TRANS_LOG(INFO,
        "checkpoint successfully on ctx mgr",
        K(total_ckp_cnt),
        K(ckp_succ_cnt),
        K(timeguard),
        K_(partition),
        K(checkpoint_base_version));
  }

  return ret;
}

int ObPartitionTransCtxMgr::update_max_trans_version(const int64_t trans_version)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
  } else if (0 > trans_version) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_version));
  } else {
    (void)inc_update(&max_trans_version_, trans_version);
  }
  return ret;
}

int ObPartitionTransCtxMgr::update_max_replay_batch_commit_version(const int64_t batch_commit_version)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
  } else if (batch_commit_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "batch_commit_version invalid", KR(ret), K(batch_commit_version));
  } else {
    (void)inc_update(&max_replay_batch_commit_version_, batch_commit_version);
  }

  return ret;
}

int ObPartitionTransCtxMgr::get_max_trans_version(int64_t& max_trans_version) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
  } else {
    max_trans_version = ATOMIC_LOAD(&max_trans_version_);
  }
  return ret;
}

int ObPartitionTransCtxMgr::relocate_data(memtable::ObIMemtable* memtable)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(memtable)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(memtable));
  } else {
    ObTransDataRelocateFunctor functor(memtable);
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(functor))) {
      TRANS_LOG(WARN, "for each transaction context error", KR(ret), KP(memtable), "manager", *this);
    }
  }

  return ret;
}

int ObPartitionTransCtxMgr::prepare_changing_leader(
    const ObAddr& proposal_leader, const int64_t round, const int64_t cnt)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("prepare changing leader", 10 * 1000);
  RLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_master_()) {
    TRANS_LOG(WARN, "partition is not master", KR(ret), "manager", *this);
  } else {
    timeguard.click();
    ObPrepareChangingLeaderFunctor functor(proposal_leader, round, cnt);
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(functor))) {
      TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
    }
    timeguard.click();
  }
  if (OB_SUCC(ret)) {
    TRANS_LOG(DEBUG, "prepare changing leader success", "manager", *this, K(timeguard));
  } else {
    TRANS_LOG(WARN, "prepare changing leader failed", KR(ret), "manager", *this, K(timeguard));
  }

  return ret;
}

int ObPartitionTransCtxMgr::check_scheduler_status()
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else {
    IteratePartCtxAskSchedulerStatusFunctor functor;
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(functor))) {
      TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
    }
  }

  return ret;
}

int ObPartitionTransCtxMgr::insert_in_TRIM(ObTransResultInfo* info, bool& registered)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ObTransCtxType::PARTICIPANT != ctx_type_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "only part ctx has trans result info mgr", KR(ret), K_(ctx_type), K_(partition));
  } else if (OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans result info is NULL", K(ret), KP(info), K_(partition));
  } else if (OB_ISNULL(trans_result_info_mgr_) && OB_FAIL(init_trans_result_info_mgr_())) {
    TRANS_LOG(ERROR, "initialize trans_result_info_mgr_ fail", K(ret), K_(partition));
  } else if (OB_FAIL(trans_result_info_mgr_->insert(info, registered))) {
    TRANS_LOG(WARN, "trans result info mgr insert error", K(ret), K(info), K_(partition));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionTransCtxMgr::get_state_in_TRIM(const ObTransID& trans_id, int& state)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ObTransCtxType::PARTICIPANT != ctx_type_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "only part ctx has trans result info mgr", KR(ret), K_(ctx_type), K_(partition));
  } else if (OB_ISNULL(trans_result_info_mgr_) && OB_FAIL(init_trans_result_info_mgr_())) {
    TRANS_LOG(WARN, "trans result info mgr initialize fail", KR(ret), K_(partition));
  } else if (OB_FAIL(trans_result_info_mgr_->get_state(trans_id, state))) {
    TRANS_LOG(WARN, "trans result info mgr get state error", KR(ret), K_(partition));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionTransCtxMgr::audit_partition(ObPartitionAuditInfoCache& cache, bool commit)
{
  int ret = OB_SUCCESS;
  ObPartitionAuditInfo* partition_audit_info = NULL;

  if (!GCONF.enable_sql_audit) {
    // do nothing
  } else if (OB_UNLIKELY(ObTransCtxType::PARTICIPANT != ctx_type_ && ObTransCtxType::SLAVE_PARTICIPANT != ctx_type_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "audit partition type unexpected", KR(ret), K_(partition));
  } else if (OB_ISNULL(core_local_partition_audit_info_)) {
    if (ObTransCtxType::PARTICIPANT == ctx_type_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "core local partition audit info is NULL", KR(ret), K_(partition));
    }
  } else if (OB_FAIL(core_local_partition_audit_info_->get_value(partition_audit_info))) {
    TRANS_LOG(WARN, "core local partition audit info get value error", KR(ret), K_(partition));
  } else if (OB_ISNULL(partition_audit_info)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "partition audit info is null", KR(ret), K_(partition));
  } else if (OB_FAIL(partition_audit_info->update_audit_info(cache, commit))) {
    TRANS_LOG(WARN, "update audit info error", KR(ret), K(commit), K_(partition));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionTransCtxMgr::get_partition_audit_info(ObPartitionAuditInfo& info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ObTransCtxType::PARTICIPANT != ctx_type_ && ObTransCtxType::SLAVE_PARTICIPANT != ctx_type_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "audit partition type unexpected", KR(ret), K_(partition));
  } else if (OB_ISNULL(core_local_partition_audit_info_)) {
    if (ObTransCtxType::PARTICIPANT == ctx_type_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "core local partition audit info is NULL", KR(ret), K_(partition));
    }
  } else {
    ObPartitionAuditInfo* partition_audit_info = NULL;
    for (int i = 0; OB_SUCC(ret) && i < OB_PARTITION_AUDIT_LOCAL_STORAGE_COUNT; i++) {
      if (OB_FAIL(core_local_partition_audit_info_->get_value(i, partition_audit_info))) {
        TRANS_LOG(WARN, "core local partition audit info get value error", KR(ret), K_(partition));
      } else if (OB_ISNULL(partition_audit_info)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "partition audit info is null", KR(ret), K_(partition));
      } else {
        info += *partition_audit_info;
      }
    }
  }

  return ret;
}

int ObPartitionTransCtxMgr::set_partition_audit_base_row_count(const int64_t count)
{
  int ret = OB_SUCCESS;
  ObPartitionAuditInfo* partition_audit_info = NULL;

  if (!GCONF.enable_sql_audit) {
    // do nothing
  } else if (OB_UNLIKELY(ObTransCtxType::PARTICIPANT != ctx_type_ && ObTransCtxType::SLAVE_PARTICIPANT != ctx_type_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "audit partition type unexpected", KR(ret), K_(partition));
  } else if (OB_ISNULL(core_local_partition_audit_info_)) {
    if (ObTransCtxType::PARTICIPANT == ctx_type_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "core local partition audit info is NULL", KR(ret), K_(partition));
    }
  } else if (OB_FAIL(core_local_partition_audit_info_->get_value(partition_audit_info))) {
    TRANS_LOG(WARN, "core local partition audit info get value error", KR(ret), K_(partition));
  } else if (OB_ISNULL(partition_audit_info)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "partition audit info is null", KR(ret), K_(partition));
  } else {
    partition_audit_info->set_base_row_count(count);
  }

  return ret;
}

int ObPartitionTransCtxMgr::print_dup_table_partition_mgr()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(dup_table_partition_mgr_)) {
    (void)dup_table_partition_mgr_->print_lease_info();
  }

  return ret;
}

int ObPartitionTransCtxMgr::is_dup_table_partition_serving(const bool is_master, bool& is_serving)
{
  int ret = OB_SUCCESS;

  if (!is_dup_table_) {
    is_serving = false;
  } else if (OB_ISNULL(dup_table_partition_mgr_) || OB_ISNULL(dup_table_partition_info_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected state", KR(ret), K_(partition));
  } else if (is_master) {
    is_serving = dup_table_partition_mgr_->is_serving();
  } else {
    is_serving = dup_table_partition_info_->is_serving();
  }

  return ret;
}

int ObPartitionTransCtxMgr::mark_dirty_trans(const ObMemtable* const frozen_memtable,
    const ObMemtable* const active_memtable, int64_t& cb_cnt, int64_t& applied_log_ts)
{
  int ret = OB_SUCCESS;

  ObDirtyTransMarkerFunctor fn(frozen_memtable, active_memtable);
  if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
    TRANS_LOG(WARN, "for each mark dirty trans failed", KR(ret), "manager", *this);
  }

  cb_cnt = fn.get_pending_cb_cnt();
  applied_log_ts = fn.get_applied_log_ts();

  return ret;
}

int ObPartitionTransCtxMgr::get_applied_log_ts(int64_t& applied_log_ts)
{
  int ret = OB_SUCCESS;

  ObGetAppliedLogTsFunctor fn;
  if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
    TRANS_LOG(WARN, "get applied log ts failed", KR(ret), "manager", *this);
  }

  applied_log_ts = fn.get_applied_log_ts();

  return ret;
}

int ObPartitionTransCtxMgr::get_max_trans_version_before_given_log_ts(
    const int64_t log_ts, int64_t& max_trans_version, bool& is_all_rollback_trans)
{
  int ret = OB_SUCCESS;
  max_trans_version = INT64_MAX;

  ObGetMaxTransVersionBeforeLogFunctor fn(log_ts);
  if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
    TRANS_LOG(
        WARN, "failed to iterate each ctx and get max_trans_version before given log_id", K(ret), "manager", *this);
  } else if (is_partition_stopped()) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    max_trans_version = fn.get_max_trans_version();
    is_all_rollback_trans = fn.is_all_rollback_trans();
    TRANS_LOG(TRACE, "get_max_trans_version_before_given_log", K(max_trans_version), K(partition_));
  }

  return ret;
}

int ObPartitionTransCtxMgr::clear_unused_trans_status(const int64_t max_cleanout_log_ts)
{
  int ret = OB_SUCCESS;

  ObCleanTransTableFunctor fn(max_cleanout_log_ts);
  if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
    TRANS_LOG(WARN, "failed to clean trans table", K(ret), "manager", *this);
  } else {
    TRANS_LOG(DEBUG, "succ to clean trans table", K(max_cleanout_log_ts), K(partition_));
  }
  return ret;
}

int ObPartitionTransCtxMgr::has_terminated_trx_in_given_log_ts_range(
    const int64_t start_log_ts, const int64_t end_log_ts, bool& has_terminated_trx)
{
  int ret = OB_SUCCESS;
  has_terminated_trx = true;

  ObCheckHasTerminatedTrxBeforeLogFunction fn(start_log_ts, end_log_ts);
  if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
    // ignore ret since we rewrite errcode to OB_ITER_END once we found terminated trx
    // to stop traversal early
    ret = OB_SUCCESS;
  } else {
    has_terminated_trx = fn.has_terminated_trx();
  }
  return ret;
}

int ObPartitionTransCtxMgr::init_dup_table_mgr()
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);

  if (is_dup_table_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "the partition is already dup table", KR(ret), K_(partition));
  } else if (OB_ISNULL(dup_table_partition_info_ = ObDupTablePartitionInfoFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "dup table partition info alloc error", KR(ret), K_(partition));
  } else if (OB_FAIL(dup_table_partition_info_->init(this))) {
    TRANS_LOG(WARN, "dup table partition info init error", KR(ret), K_(partition));
  } else if (OB_ISNULL(dup_table_partition_mgr_ = ObDupTablePartitionMgrFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc dup table partition mgr failed", KR(ret), K_(partition));
  } else if (OB_FAIL(
                 dup_table_partition_mgr_->init(&ObPartitionService::get_instance(), partition_, this, is_master_()))) {
    TRANS_LOG(WARN, "dup table partition mgr init error", KR(ret), K_(partition));
  } else {
    is_dup_table_ = true;
  }
  return ret;
}

int ObPartitionTransCtxMgr::CtxMapMgr::init(const ObPartitionKey& partition, CtxMap* ctx_map)
{
  partition_ = partition;
  ctx_map_ = ctx_map;
  return OB_SUCCESS;
}

void ObPartitionTransCtxMgr::CtxMapMgr::destroy()
{
  if (OB_NOT_NULL(ctx_map_)) {
    ctx_map_ = nullptr;
  }
}

void ObPartitionTransCtxMgr::CtxMapMgr::reset()
{
  if (OB_NOT_NULL(ctx_map_)) {
    ObRemoveAllCtxFunctor fn;
    remove_if(fn);
    ctx_map_ = nullptr;
  }
}

template <class Fn>
bool ObPartitionTransCtxMgr::CtxMapMgr::ObPartitionForEachFilterFunctor<Fn>::operator()(
    const ObTransKey& trans_key, ObTransCtx* ctx_base)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  if (!trans_key.is_valid() || OB_ISNULL(ctx_base)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(trans_key), KP(ctx_base));
  } else if (partition_ != trans_key.get_partition_key()) {
    // not current partition, do nothing
    bool_ret = true;
  } else {
    bool_ret = fn_(trans_key.get_trans_id(), ctx_base);
  }
  UNUSED(ret);
  return bool_ret;
}

template <class Fn>
bool ObPartitionTransCtxMgr::CtxMapMgr::ObPartitionRemoveIfFilterFunctor<Fn>::operator()(
    const ObTransKey& trans_key, ObTransCtx* ctx_base)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  if (!trans_key.is_valid() || OB_ISNULL(ctx_base)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(trans_key), KP(ctx_base));
  } else if (partition_ != trans_key.get_partition_key()) {
    // not current partition, do nothing
  } else {
    bool_ret = fn_(trans_key.get_trans_id(), ctx_base);
  }
  UNUSED(ret);
  return bool_ret;
}

int ObPartitionTransCtxMgr::CtxMapMgr::get(const ObTransID& trans_id, ObTransCtx*& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ctx_map_)) {
    ret = ctx_map_->get(ObTransKey(partition_, trans_id), ctx);
  } else {
    ret = OB_NOT_INIT;
  }
  return ret;
}

int ObPartitionTransCtxMgr::CtxMapMgr::insert_and_get(const ObTransID& trans_id, ObTransCtx* ctx)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ctx_map_)) {
    ret = ctx_map_->insert_and_get(ObTransKey(partition_, trans_id), ctx);
  } else {
    ret = OB_NOT_INIT;
  }
  return ret;
}

void ObPartitionTransCtxMgr::CtxMapMgr::revert(ObTransCtx* ctx)
{
  if (OB_NOT_NULL(ctx_map_)) {
    ctx_map_->revert(ctx);
  }
}

int ObPartitionTransCtxMgr::CtxMapMgr::del(const ObTransID& trans_id)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ctx_map_)) {
    ret = ctx_map_->del(ObTransKey(partition_, trans_id));
  } else {
    ret = OB_NOT_INIT;
  }
  return ret;
}

int ObPartitionTransCtxMgr::lock_for_read(
    const ObLockForReadArg& lock_for_read_arg, bool& can_read, int64_t& trans_version, bool& is_determined_state)
{
  int ret = OB_SUCCESS;

  // check whether the partition is stopped by rebuild or other action,
  // prevent accessing idle data
  if (is_partition_stopped()) {
    TRANS_LOG(WARN, "partition ctx mgr has been stooped", K(ret));
    ret = OB_REPLICA_NOT_READABLE;
  } else {
    ret = lock_for_read_(lock_for_read_arg, can_read, trans_version, is_determined_state);
  }

  // double check, prevent accessing idle data
  if (is_partition_stopped()) {
    TRANS_LOG(WARN, "partition ctx mgr has been stooped", K(ret));
    ret = OB_REPLICA_NOT_READABLE;
  }

  return ret;
}

int ObPartitionTransCtxMgr::lock_for_read_(
    const ObLockForReadArg& lock_for_read_arg, bool& can_read, int64_t& trans_version, bool& is_determined_state)
{
  int ret = OB_SUCCESS;
  transaction::ObTransCtx* trans_ctx = NULL;
  transaction::ObPartTransCtx* part_trans_ctx = NULL;
  const ObTransID& data_trans_id = lock_for_read_arg.data_trans_id_;
  if (OB_FAIL(get_trans_ctx_(data_trans_id, trans_ctx))) {
    TRANS_LOG(WARN, "failed to get trans ctx", K(ret));
  } else {
    if (OB_ISNULL(part_trans_ctx = dynamic_cast<transaction::ObPartTransCtx*>(trans_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected error", K(ret));
    } else if (OB_FAIL(
                   part_trans_ctx->lock_for_read(lock_for_read_arg, can_read, trans_version, is_determined_state))) {
      TRANS_LOG(WARN, "failed to check transaction status", K(ret));
    }
    revert_trans_ctx_(trans_ctx);
  }
  return ret;
}

int ObPartitionTransCtxMgr::get_transaction_status_with_log_ts(const transaction::ObTransID& data_trans_id,
    const int64_t log_ts, ObTransTableStatusType& status, int64_t& trans_version)
{
  int ret = OB_SUCCESS;

  // check whether the partition is stopped by rebuild or other action,
  // prevent accessing idle data
  if (is_partition_stopped()) {
    TRANS_LOG(WARN, "partition ctx mgr has been stooped", K(ret));
    ret = OB_REPLICA_NOT_READABLE;
  } else {
    ret = get_transaction_status_with_log_ts_(data_trans_id, log_ts, status, trans_version);
  }

  // double check, prevent accessing idle data
  if (is_partition_stopped()) {
    TRANS_LOG(WARN, "partition ctx mgr has been stooped", K(ret));
    ret = OB_REPLICA_NOT_READABLE;
  }

  return ret;
}

int ObPartitionTransCtxMgr::get_transaction_status_with_log_ts_(const transaction::ObTransID& data_trans_id,
    const int64_t log_ts, ObTransTableStatusType& status, int64_t& trans_version)
{
  int ret = OB_SUCCESS;
  transaction::ObTransCtx* trans_ctx = NULL;
  transaction::ObPartTransCtx* part_trans_ctx = NULL;
  if (OB_FAIL(get_trans_ctx_(data_trans_id, trans_ctx))) {
    TRANS_LOG(WARN, "failed to get trans ctx", K(ret));
  } else {
    if (OB_ISNULL(part_trans_ctx = dynamic_cast<transaction::ObPartTransCtx*>(trans_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected error", K(ret));
    } else if (OB_FAIL(part_trans_ctx->get_transaction_status_with_log_ts(log_ts, status, trans_version))) {
      TRANS_LOG(WARN, "failed to check transaction status", K(ret));
    }
    revert_trans_ctx_(trans_ctx);
  }
  return ret;
}

int ObPartitionTransCtxMgr::check_sql_sequence_can_read(
    const transaction::ObTransID& data_trans_id, const int64_t sql_sequence, bool& can_read)
{
  int ret = OB_SUCCESS;

  // check whether the partition is stopped by rebuild or other action,
  // prevent accessing idle data
  if (is_partition_stopped()) {
    TRANS_LOG(WARN, "partition ctx mgr has been stooped", K(ret));
    ret = OB_REPLICA_NOT_READABLE;
  } else {
    ret = check_sql_sequence_can_read_(data_trans_id, sql_sequence, can_read);
  }

  // double check, prevent accessing idle data
  if (is_partition_stopped()) {
    TRANS_LOG(WARN, "partition ctx mgr has been stooped", K(ret));
    ret = OB_REPLICA_NOT_READABLE;
  }

  return ret;
}

int ObPartitionTransCtxMgr::check_sql_sequence_can_read_(
    const transaction::ObTransID& data_trans_id, const int64_t sql_sequence, bool& can_read)
{
  int ret = OB_SUCCESS;
  transaction::ObTransCtx* trans_ctx = NULL;
  transaction::ObPartTransCtx* part_trans_ctx = NULL;
  if (OB_FAIL(get_trans_ctx_(data_trans_id, trans_ctx))) {
    TRANS_LOG(WARN, "failed to get trans ctx", K(ret));
  } else {
    if (OB_ISNULL(part_trans_ctx = dynamic_cast<transaction::ObPartTransCtx*>(trans_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected error", K(ret));
    } else if (OB_FAIL(part_trans_ctx->check_sql_sequence_can_read(sql_sequence, can_read))) {
      TRANS_LOG(WARN, "failed to check transaction status", K(ret));
    }
    revert_trans_ctx_(trans_ctx);
  }
  return ret;
}

int ObPartitionTransCtxMgr::check_row_locked(const ObStoreRowkey& key, ObIMvccCtx& ctx,
    const transaction::ObTransID& read_trans_id, const transaction::ObTransID& data_trans_id,
    const int32_t sql_sequence, ObStoreRowLockState& lock_state)
{
  int ret = OB_SUCCESS;

  // check whether the partition is stopped by rebuild or other action,
  // prevent accessing idle data
  if (is_partition_stopped()) {
    TRANS_LOG(WARN, "partition ctx mgr has been stooped", K(ret));
    ret = OB_REPLICA_NOT_READABLE;
  } else {
    ret = check_row_locked_(key, ctx, read_trans_id, data_trans_id, sql_sequence, lock_state);
  }

  // double check, prevent accessing idle data
  if (is_partition_stopped()) {
    TRANS_LOG(WARN, "partition ctx mgr has been stooped", K(ret));
    ret = OB_REPLICA_NOT_READABLE;
  }

  return ret;
}

int ObPartitionTransCtxMgr::check_row_locked_(const ObStoreRowkey& key, ObIMvccCtx& ctx,
    const transaction::ObTransID& read_trans_id, const transaction::ObTransID& data_trans_id,
    const int32_t sql_sequence, ObStoreRowLockState& lock_state)
{
  int ret = OB_SUCCESS;
  transaction::ObTransCtx* trans_ctx = NULL;
  transaction::ObPartTransCtx* part_trans_ctx = NULL;
  bool alloc = false;

  if (is_partition_stopped()) {
    TRANS_LOG(WARN, "partition ctx mgr has been stooped", K(ret));
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(get_trans_ctx_(data_trans_id, trans_ctx))) {
    TRANS_LOG(WARN, "failed to get trans ctx", K(ret));
  } else {
    if (OB_ISNULL(part_trans_ctx = dynamic_cast<transaction::ObPartTransCtx*>(trans_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected error", K(ret));
    } else {
      ret = part_trans_ctx->check_row_locked(key, ctx, read_trans_id, data_trans_id, sql_sequence, lock_state);
    }
    revert_trans_ctx_(trans_ctx);
  }

  if (is_partition_stopped()) {
    TRANS_LOG(WARN, "partition ctx mgr has been stooped", K(ret));
    ret = OB_STATE_NOT_MATCH;
  }

  return ret;
}

int ObPartitionTransCtxMgr::iterate_trans_table(const uint64_t end_log_id, blocksstable::ObMacroBlockWriter& writer)
{
  int ret = OB_SUCCESS;

  RLockGuard guard(rwlock_);

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_ID == end_log_id) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "end log id is invalid", K(end_log_id));
  } else {
    IterateTransTableFunctor fn(writer);
    if (OB_FAIL(fn.init(end_log_id))) {
      TRANS_LOG(WARN, "failed to init trans table functor", K(ret), K(*this));
    } else if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
      TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
    } else if (is_partition_stopped()) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      TRANS_LOG(INFO,
          "succ to iterate trans table",
          K_(partition),
          "dirty_trx_cnt",
          fn.get_dirty_trx_count(),
          "clean_trx_cnt",
          fn.get_clean_trx_count());
    }
  }
  return ret;
}

int ObPartitionTransCtxMgr::copy_trans_table(ObTransService* txs, const ObIArray<ObPartitionKey>& dest_array)
{
  int ret = OB_SUCCESS;

  RLockGuard guard(rwlock_);

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else {
    CopyTransTableFunctor fn;
    if (OB_FAIL(fn.init(txs, dest_array))) {
      TRANS_LOG(WARN, "failed to init trans table functor", K(ret), K(*this));
    } else if (OB_FAIL(ctx_map_mgr_.foreach_ctx(fn))) {
      TRANS_LOG(WARN, "for each transaction context error", KR(ret), "manager", *this);
    } else {
      TRANS_LOG(INFO, "succ to copy trans table", K(ret), "trx_count", fn.get_trx_count());
    }
  }
  return ret;
}

int ObPartitionTransCtxMgr::xa_scheduler_hb_req()
{
  int ret = OB_SUCCESS;
  // TODO, check whether the lock is required
  RLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr not inited", K(*this));
  } else {
    ObXASchedulerHbFunctor functor;
    if (OB_FAIL(ctx_map_mgr_.foreach_ctx(functor))) {
      TRANS_LOG(WARN, "for each transaction context error", K(ret), "manager", *this);
    }
  }
  return ret;
}

void ObTransCtxMgrImpl::reset()
{
  ctx_type_ = ObTransCtxType::UNKNOWN;
  partition_ctx_map_.reset();
  mgr_cache_.reset();
  if (OB_NOT_NULL(ctx_map_)) {
    for (int64_t i = 0; i < CONTEXT_MAP_COUNT; ++i) {
      ctx_map_[i].reset();
    }
    ob_free(ctx_map_);
    ctx_map_ = nullptr;
  }
  access_count_ = 0;
  hit_count_ = 0;
  ts_mgr_ = NULL;
  partition_service_ = NULL;
  partition_alloc_cnt_ = 0;
  partition_del_cnt_ = 0;
}

void ObTransCtxMgrImpl::destroy()
{
  int tmp_ret = OB_SUCCESS;

  if (OB_SUCCESS != (tmp_ret = remove_all_partition())) {
    TRANS_LOG(WARN, "remove all partition error", K(tmp_ret));
  }
  ctx_type_ = ObTransCtxType::UNKNOWN;
  partition_ctx_map_.destroy();
  mgr_cache_.destroy();
  if (OB_NOT_NULL(ctx_map_)) {
    for (int64_t i = 0; i < CONTEXT_MAP_COUNT; ++i) {
      ctx_map_[i].destroy();
    }
    ob_free(ctx_map_);
    ctx_map_ = nullptr;
  }
}

int ObTransCtxMgrImpl::init(const int64_t ctx_type, ObITsMgr* ts_mgr, storage::ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;

  if (OB_FAIL(partition_ctx_map_.init(ObModIds::OB_HASH_BUCKET_PARTITION_TRANS_CTX))) {
    TRANS_LOG(WARN, "transaction context map create error", KR(ret), K(ctx_type));
  } else if (OB_FAIL(mgr_cache_.init())) {
    TRANS_LOG(WARN, "manager cache init error", KR(ret));
  } else if (OB_ISNULL(ts_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ts mgr is null");
  } else if (OB_ISNULL(partition_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "partition service is null");
  } else if (OB_ISNULL(ctx_map_ = (CtxMap*)ob_malloc(
                           sizeof(CtxMap) * CONTEXT_MAP_COUNT, ObModIds::OB_HASH_BUCKET_TRANS_CTX))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc ctx_map failed", K(sizeof(CtxMap)), K(CONTEXT_MAP_COUNT));
  } else {
    for (i = 0; i < CONTEXT_MAP_COUNT && OB_SUCC(ret); ++i) {
      new (ctx_map_ + i) CtxMap(1 << 10);
      ret = ctx_map_[i].init(ObModIds::OB_HASH_BUCKET_TRANS_CTX);
    }
    if (OB_FAIL(ret)) {
      TRANS_LOG(WARN, "trans ctx hash map create error", KR(ret), K(ctx_type), K(i));
    } else {
      ctx_type_ = ctx_type;
      access_count_ = 0;
      hit_count_ = 0;
      ts_mgr_ = ts_mgr;
      partition_service_ = partition_service;
    }
  }

  return ret;
}

int ObTransCtxMgrImpl::add_partition(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  const uint64_t tenant_id = partition.get_tenant_id();

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL != (ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    // partition mgr already exists.
    // if partition is in stop status, active the partition; otherwise, error no is returned.
    if (OB_UNLIKELY(!ctx_mgr->is_stopped())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected partition mgr state", K(ctx_mgr));
    } else if (OB_FAIL(ctx_mgr->active(partition))) {
      TRANS_LOG(WARN, "reactive partition mgr error", KR(ret), K(partition), K(ctx_mgr));
    } else {
      TRANS_LOG(INFO, "add partition for reactive success", K(partition), K(ctx_mgr));
    }
  } else if (OB_ISNULL(ctx_mgr = ObPartitionTransCtxMgrFactory::alloc(tenant_id))) {
    TRANS_LOG(WARN, "alloc partition transaction context manager error", K(partition));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(ctx_mgr->init(partition,
                 ctx_type_,
                 ts_mgr_,
                 partition_service_,
                 ctx_map_ + (partition.hash() % CONTEXT_MAP_COUNT)))) {
    TRANS_LOG(WARN, "partition transaction context manager inited error", KR(ret), K(partition));
    ObPartitionTransCtxMgrFactory::release(ctx_mgr);
    ctx_mgr = NULL;
  } else if (OB_FAIL(partition_ctx_map_.insert_and_get(partition, ctx_mgr))) {
    TRANS_LOG(WARN, "partition transaction context manager insert error", KR(ret), K(partition));
    ObPartitionTransCtxMgrFactory::release(ctx_mgr);
    ctx_mgr = NULL;
  } else {
    ATOMIC_STORE(&partition_alloc_cnt_, 1);
    // need to revert the trans ctx ref explicitly
    partition_ctx_map_.revert(ctx_mgr);
    TRANS_LOG(INFO,
        "add partition success",
        "total_alloc",
        partition_alloc_cnt_,
        "total_del",
        partition_del_cnt_,
        K(partition),
        KP(ctx_mgr));
  }

  return ret;
}

int ObTransCtxMgrImpl::remove_all_partition()
{
  int ret = OB_SUCCESS;

  RemovePartitionFunctor fn;
  if (OB_FAIL(remove_if(fn))) {
    TRANS_LOG(WARN, "remove_if partition error", KR(ret));
  }

  return ret;
}

int ObTransCtxMgrImpl::remove_partition(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
    // check transaction context before removing partition
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "partition transaction context not exist", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_UNLIKELY(!ctx_mgr->is_stopped())) {
    TRANS_LOG(WARN, "partition has not been stopped", K(partition));
    ret = OB_PARTITION_IS_NOT_STOPPED;
  } else {
    if (0 != (count = ctx_mgr->get_ctx_count())) {
      TRANS_LOG(WARN, "partition transaction context not empty", K(partition), K(count), "lbt", lbt());
    }
    // remove partition transaction context from map
    if (OB_FAIL(partition_ctx_map_.del(partition))) {
      TRANS_LOG(WARN, "remove partition error", KR(ret), K(partition));
    } else if (OB_FAIL(mgr_cache_.remove(partition.hash()))) {
      TRANS_LOG(WARN, "remove mgr cache error", KR(ret), K(partition));
    } else {
      ATOMIC_STORE(&partition_del_cnt_, 1);
      TRANS_LOG(INFO,
          "remove partition success",
          "total_alloc",
          partition_alloc_cnt_,
          "total_del",
          partition_del_cnt_,
          K(partition));
    }
  }

  return ret;
}

int ObTransCtxMgrImpl::stop_all_partition()
{
  int ret = OB_SUCCESS;
  StopPartitionFunctor fn;

  if (OB_FAIL(foreach_partition(fn))) {
    TRANS_LOG(WARN, "foreach partition to stop error", KR(ret));
  }

  return ret;
}

int ObTransCtxMgrImpl::stop_partition(const ObPartitionKey& partition, const bool graceful)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "partition transaction context manager not exist", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->stop(graceful))) {
    TRANS_LOG(WARN, "stop partition error", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "stop partition success", K(partition), "ctx_count", ctx_mgr->get_ctx_count());
  }

  return ret;
}

int ObTransCtxMgrImpl::block_partition(const ObPartitionKey& partition, bool& is_all_trans_clear)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "partition transaction context manager not exist", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->block(is_all_trans_clear))) {
    TRANS_LOG(WARN, "block partition error", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "block partition success", K(partition), "ctx_count", ctx_mgr->get_ctx_count());
  }

  return ret;
}

int ObTransCtxMgrImpl::print_all_partition_ctx()
{
  int ret = OB_SUCCESS;

  PrintAllPartitionTransCtxFunctor fn;

  if (OB_FAIL(foreach_partition(fn))) {
    TRANS_LOG(WARN, "print partition transaction context error", KR(ret));
  }

  return ret;
}

int ObTransCtxMgrImpl::wait_all_partition()
{
  int ret = OB_SUCCESS;
  int64_t retry_count = 0;

  WaitPartitionFunctor fn(retry_count);
  if (OB_FAIL(foreach_partition(fn))) {
    TRANS_LOG(WARN, "foreach partition error", KR(ret));
  } else if (retry_count > 0) {
    TRANS_LOG(WARN, "partition need retry to wait", "part_count", retry_count, "lbt", lbt());
    ret = OB_EAGAIN;
  } else {
    // do nothing
  }

  return ret;
}

int ObTransCtxMgrImpl::wait_partition(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  const int64_t PRINT_LOG_INTERVAL = 1000 * 1000;

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
    // check transaction context before removing partition
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "partition transaction context not exist", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_UNLIKELY(!ctx_mgr->is_stopped())) {
    TRANS_LOG(WARN, "partition has not been stopped", K(partition));
    ret = OB_PARTITION_IS_NOT_STOPPED;
  } else if ((count = ctx_mgr->get_ctx_count()) > 0) {
    if (REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      TRANS_LOG(WARN,
          "transaction context not empty, try again",
          KP(ctx_mgr),
          K(partition),
          K(count),
          "partition_type",
          ctx_mgr->get_partition_type());
    }
    ret = OB_EAGAIN;
  } else {
    TRANS_LOG(DEBUG, "wait partition success", K(partition));
  }

  return ret;
}

int ObTransCtxMgrImpl::leader_revoke(const ObPartitionKey& partition, const bool first_check)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context mgr error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    ret = ctx_mgr->leader_revoke(first_check);
  }

  return ret;
}

int ObTransCtxMgrImpl::clear_all_ctx(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  const KillTransArg arg(false, false);
  bool is_all_trans_clear = false;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context mgr error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->kill_all_trans(arg, is_all_trans_clear))) {
    TRANS_LOG(WARN, "kill all trans failed", KR(ret), K(partition), K(arg));
  } else {
    // ignore is_all_trans_clear, OB_SUCCESS is returned.
  }

  return ret;
}

int ObTransCtxMgrImpl::kill_all_trans(
    const ObPartitionKey& partition, const KillTransArg& arg, bool& is_all_trans_clear)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context mgr error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    ret = ctx_mgr->kill_all_trans(arg, is_all_trans_clear);
  }

  return ret;
}

int ObTransCtxMgrImpl::calculate_trans_cost(const ObTransID& tid, uint64_t& cost)
{
  int ret = OB_SUCCESS;

  if (!tid.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(tid));
    ret = OB_INVALID_ARGUMENT;
  } else {
    CalculateCostFunctor fn(tid);
    if (OB_FAIL(foreach_partition(fn))) {
      TRANS_LOG(WARN, "foreach trasaction context mgr error", KR(ret));
    } else {
      cost += fn.get_total_cost();
    }
  }

  return ret;
}

int ObTransCtxMgrImpl::wait_1pc_trx_end(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  bool all_1pc_trx_end = false;

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context mgr error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->wait_1pc_trx_end(all_1pc_trx_end))) {
    TRANS_LOG(WARN, "wait 1pc trx end error", K(ret), K(partition), "ctx_cnt", ctx_mgr->get_ctx_count());
  } else if (!all_1pc_trx_end) {
    ret = OB_EAGAIN;
  } else {
    // do nothing
  }

  return ret;
}

int ObTransCtxMgrImpl::wait_all_trans_clear(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context mgr error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (ctx_mgr->get_ctx_count() > 0) {
    ret = OB_EAGAIN;
  } else {
    // do nothing
  }

  return ret;
}

int ObTransCtxMgrImpl::check_all_trans_in_trans_table_state(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context mgr error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->check_all_trans_in_trans_table_state())) {
    TRANS_LOG(WARN, "check_all_trans_in_trans_table_state error", K(partition));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransCtxMgrImpl::leader_takeover(const ObPartitionKey& partition, const int64_t checkpoint)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->leader_takeover(checkpoint))) {
    TRANS_LOG(ERROR, "leader takeover error", KR(ret), K(partition));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransCtxMgrImpl::leader_active(const ObPartitionKey& partition, const LeaderActiveArg& arg)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context mgr error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->leader_active(arg))) {
    TRANS_LOG(WARN, "coordinator leader active error", KR(ret), K(partition), "lbt", lbt());
  } else {
    // do nothing
  }

  return ret;
}

int ObTransCtxMgrImpl::revert_partition_trans_ctx_mgr_with_ref(ObPartitionTransCtxMgr* mgr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(mgr)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(mgr));
  } else {
    partition_ctx_map_.revert(mgr);
  }

  return ret;
}

int ObTransCtxMgrImpl::get_partition_trans_ctx_mgr_with_ref(
    const ObPartitionKey& partition, ObTransStateTableGuard& guard)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  if (OB_UNLIKELY(!partition.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(partition));
  } else if (OB_FAIL(partition_ctx_map_.get(partition, ctx_mgr))) {
    TRANS_LOG(WARN, "get partition context manager error", KR(ret), K(partition));
  } else if (OB_FAIL(guard.set_trans_state_table(ctx_mgr))) {
    TRANS_LOG(WARN, "set partition trans ctx mgr error", K(ret), K(partition));
  } else {
    // do nothing
  }
  if (OB_FAIL(ret) && NULL != ctx_mgr) {
    partition_ctx_map_.revert(ctx_mgr);
  }
  return ret;
}

ObPartitionTransCtxMgr* ObTransCtxMgrImpl::get_partition_trans_ctx_mgr(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL != (ctx_mgr = mgr_cache_.get(partition.hash())) && ctx_mgr->get_partition() == partition) {
    (void)ATOMIC_FAA(&access_count_, 1);
    (void)ATOMIC_FAA(&hit_count_, 1);
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      TRANS_LOG(INFO,
          "mgr cache hit rate",
          K(partition),
          K_(hit_count),
          K_(access_count),
          "hit_rate",
          (double)hit_count_ / (double)(access_count_ + 1));
    }
  } else if (OB_FAIL(partition_ctx_map_.get(partition, ctx_mgr))) {
    if (OB_ENTRY_NOT_EXIST != ret && OB_PARTITION_NOT_EXIST != ret) {
      TRANS_LOG(WARN, "get partition context manager error", KR(ret), K(partition));
    } else {
      TRANS_LOG(TRACE, "get partition context manager error", KR(ret), K(partition));
    }
    ctx_mgr = NULL;
  } else {
    (void)mgr_cache_.set(partition.hash(), ctx_mgr);
    (void)ATOMIC_FAA(&access_count_, 1);
    partition_ctx_map_.revert(ctx_mgr);
  }

  return ctx_mgr;
}

int ObScheTransCtxMgr::init(ObITsMgr* ts_mgr, storage::ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObScheTransCtxMgr inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(ts_mgr)) {
    TRANS_LOG(WARN, "ts mgr is null");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(partition_service)) {
    TRANS_LOG(WARN, "partition service is null");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ctx_map_.init(ObModIds::OB_HASH_BUCKET_TRANS_CTX))) {
    TRANS_LOG(WARN, "trans ctx hash map create error", KR(ret));
  } else if (OB_FAIL(partition_trans_ctx_mgr_.init(
                 SCHE_PARTITION_ID, ObTransCtxType::SCHEDULER, ts_mgr, partition_service, &ctx_map_))) {
    TRANS_LOG(WARN, "partition transaction context manager init error", KR(ret), "partition", SCHE_PARTITION_ID);
  } else {
    TRANS_LOG(INFO, "ObScheTransCtxMgr inited success", "partition", SCHE_PARTITION_ID);
    ts_mgr_ = ts_mgr;
    is_inited_ = true;
  }

  return ret;
}

int ObScheTransCtxMgr::start()
{
  int ret = OB_SUCCESS;

  DRWLock::WRLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtxMgr is not inited");
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObScheTransCtxMgr is already running");
    ret = OB_ERR_UNEXPECTED;
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "ObScheTransCtxMgr start success");
  }

  return ret;
}

int ObScheTransCtxMgr::stop()
{
  int ret = OB_SUCCESS;
  const bool graceful = true;

  DRWLock::WRLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtxMgr is not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    TRANS_LOG(WARN, "ObScheTransCtxMgr already has been stopped");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(partition_trans_ctx_mgr_.stop(graceful))) {
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr stop error", KR(ret), "manager", partition_trans_ctx_mgr_);
  } else {
    is_running_ = false;
    TRANS_LOG(INFO, "ObScheTransCtxMgr stop success", "ctx_count", partition_trans_ctx_mgr_.get_ctx_count());
  }

  return ret;
}

int ObScheTransCtxMgr::wait()
{
  int ret = OB_SUCCESS;
  const int64_t SLEEP_US = 100 * 1000;
  const int64_t MAX_WAIT_RETRY_TIMES = 10;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtxMgr is not inited");
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObScheTransCtxMgr is running");
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t ctx_count = 0;
    int64_t retry = 0;
    for (; OB_SUCCESS == ret && retry < MAX_WAIT_RETRY_TIMES; ++retry) {
      {
        DRWLock::RDLockGuard guard(rwlock_);
        ctx_count = partition_trans_ctx_mgr_.get_ctx_count();
        if (0 == ctx_count) {
          break;
        }
      }
      ObTransCond::usleep(SLEEP_US);
    }
    if (OB_SUCCESS != ret || MAX_WAIT_RETRY_TIMES == retry) {
      DRWLock::RDLockGuard guard(rwlock_);
      const bool verbose = true;
      partition_trans_ctx_mgr_.print_all_ctx(ObPartitionTransCtxMgr::MAX_HASH_ITEM_PRINT, verbose);
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(INFO, "ObScheTransCtxMgr wait error", KR(ret), "manager", partition_trans_ctx_mgr_);
  } else {
    TRANS_LOG(INFO, "ObScheTransCtxMgr wait success");
  }

  return ret;
}

void ObScheTransCtxMgr::destroy()
{
  DRWLock::WRLockGuard guard(rwlock_);

  if (is_inited_) {
    partition_trans_ctx_mgr_.destroy();
    ctx_map_.destroy();
    is_inited_ = false;
    TRANS_LOG(INFO, "ObScheTransCtxMgr destroyed");
  }
}

void ObScheTransCtxMgr::reset()
{
  is_inited_ = false;
  is_running_ = false;
  ts_mgr_ = NULL;
  ctx_map_.reset();
  partition_trans_ctx_mgr_.reset();
}

int ObScheTransCtxMgr::inactive_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!is_valid_tenant_id(tenant_id)) {
    TRANS_LOG(WARN, "invalid argument", K(tenant_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObPartitionTransCtxMgr* ctx_mgr = get_partition_trans_ctx_mgr_(SCHE_PARTITION_ID);
    if (OB_ISNULL(ctx_mgr)) {
      TRANS_LOG(WARN, "get partition transaction context mgr error");
      ret = OB_PARTITION_NOT_EXIST;
    } else if (OB_FAIL(ctx_mgr->inactive_tenant(tenant_id))) {
      TRANS_LOG(WARN, "ObPartitionTransCtxMgr inactive tenant error", KR(ret), K(tenant_id));
    } else {
      TRANS_LOG(DEBUG, "ObPartitionTransCtxMgr inactive tenant success", K(tenant_id));
    }
  }

  return ret;
}

int ObScheTransCtxMgr::get_trans_ctx(const ObPartitionKey& partition, const ObTransID& trans_id, const bool for_replay,
    const bool is_readonly, bool& alloc, ObTransCtx*& ctx)
{
  int ret = OB_SUCCESS;
  const bool is_bounded_staleness_read = false;
  const bool need_completed_dirty_txn = false;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid() || !trans_id.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(trans_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObPartitionTransCtxMgr* ctx_mgr = get_partition_trans_ctx_mgr_(partition);
    if (OB_ISNULL(ctx_mgr)) {
      TRANS_LOG(WARN, "get partition transaction context mgr error", K(partition));
      ret = OB_PARTITION_NOT_EXIST;
    } else if (OB_FAIL(ctx_mgr->get_trans_ctx(trans_id,
                   for_replay,
                   is_readonly,
                   is_bounded_staleness_read,
                   need_completed_dirty_txn,
                   alloc,
                   ctx))) {
      TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_id), K(for_replay), K(alloc));
    } else if (OB_ISNULL(ctx)) {
      TRANS_LOG(WARN, "transaction context is null", K(partition), K(trans_id));
      ret = OB_ERR_UNEXPECTED;
    } else {
      TRANS_LOG(DEBUG, "get transaction context success", K(partition), K(trans_id));
    }
  }

  return ret;
}

int ObScheTransCtxMgr::revert_trans_ctx(ObTransCtx* ctx)
{
  int ret = OB_SUCCESS;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObScheTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "invalid argument", KP(ctx));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // reference cannot be used here, otherwise context memory will be released
    // and then core dump occurs when printiing partition
    const ObPartitionKey partition = ctx->get_partition();
    ObPartitionTransCtxMgr* ctx_mgr = get_partition_trans_ctx_mgr_(partition);
    if (OB_ISNULL(ctx_mgr)) {
      TRANS_LOG(WARN, "get partition transaction context mgr error", K(partition));
      ret = OB_PARTITION_NOT_EXIST;
    } else if (OB_FAIL(ctx_mgr->revert_trans_ctx(ctx))) {
      TRANS_LOG(WARN, "revert transaction context error", KR(ret), K(partition), "context", *ctx);
    } else {
      TRANS_LOG(DEBUG, "revert transaction context success", K(partition));
    }
  }

  return ret;
}

void ObScheTransCtxMgr::print_all_ctx(const int64_t count)
{
  if (count > 0) {
    DRWLock::RDLockGuard guard(rwlock_);
    const bool verbose = true;
    partition_trans_ctx_mgr_.print_all_ctx(count, verbose);
  }
}

int ObScheTransCtxMgr::xa_scheduler_hb_req()
{
  int ret = OB_SUCCESS;
  // TODO, check whether the lock is required.
  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObScheTransCtxMgr not inited");
  } else {
    ObPartitionTransCtxMgr* ctx_mgr = get_partition_trans_ctx_mgr_(SCHE_PARTITION_ID);
    if (OB_ISNULL(ctx_mgr)) {
      ret = OB_PARTITION_NOT_EXIST;
      TRANS_LOG(WARN, "get partition transaction context mgr error");
    } else if (OB_FAIL(ctx_mgr->xa_scheduler_hb_req())) {
      TRANS_LOG(WARN, "xa scheduler hb failed", K(ret));
    }
  }
  return ret;
}

int ObCoordTransCtxMgr::init(ObITsMgr* ts_mgr, storage::ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(ObTransCtxMgrImpl::init(ObTransCtxType::COORDINATOR, ts_mgr, partition_service))) {
    TRANS_LOG(WARN, "ObTransCtxMgrImpl init error", KR(ret));
  } else {
    is_inited_ = true;
    TRANS_LOG(INFO, "ObCoordTransCtxMgr inited success");
  }

  return ret;
}

int ObCoordTransCtxMgr::start()
{
  int ret = OB_SUCCESS;

  DRWLock::WRRetryLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr is not inited");
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr is already running");
    ret = OB_ERR_UNEXPECTED;
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "ObCoordTransCtxMgr start success");
  }

  return ret;
}

int ObCoordTransCtxMgr::stop()
{
  int ret = OB_SUCCESS;

  DRWLock::WRRetryLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr is not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr already has been stopped");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(ObTransCtxMgrImpl::stop_all_partition())) {
    TRANS_LOG(WARN, "stop all partition error", KR(ret));
  } else {
    is_running_ = false;
    TRANS_LOG(INFO, "ObCoordTransCtxMgr stop success");
  }

  return ret;
}

int ObCoordTransCtxMgr::wait()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t SLEEP_US = 100 * 1000;
  const int64_t MAX_WAIT_RETRY_TIMES = 10;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr is not inited");
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr is running");
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t retry = 0;
    for (; OB_SUCCESS == ret && retry < MAX_WAIT_RETRY_TIMES; ++retry) {
      {
        DRWLock::RDLockGuard guard(rwlock_);
        if (OB_FAIL(ObTransCtxMgrImpl::wait_all_partition())) {
          if (OB_EAGAIN == ret) {
            ret = OB_SUCCESS;
          } else {
            TRANS_LOG(WARN, "wait all partition error", KR(ret), K(retry));
          }
        } else {
          break;
        }
      }
      ObTransCond::usleep(SLEEP_US);
    }
    if (OB_FAIL(ret) || MAX_WAIT_RETRY_TIMES == retry) {
      DRWLock::RDLockGuard guard(rwlock_);
      if (OB_SUCCESS != (tmp_ret = ObTransCtxMgrImpl::print_all_partition_ctx())) {
        TRANS_LOG(WARN, "print all partition context error", "ret", tmp_ret);
      }
    }
  }

  return ret;
}

void ObCoordTransCtxMgr::destroy()
{
  DRWLock::WRRetryLockGuard guard(rwlock_);

  if (is_inited_) {
    is_inited_ = false;
    ObTransCtxMgrImpl::destroy();
    TRANS_LOG(INFO, "ObCoordTransCtxMgr destroyed");
  }
}

void ObCoordTransCtxMgr::reset()
{
  is_inited_ = false;
  is_running_ = false;
  ObTransCtxMgrImpl::reset();
}

int ObCoordTransCtxMgr::get_trans_ctx(const ObPartitionKey& partition, const ObTransID& trans_id, const bool for_replay,
    const bool is_readonly, bool& alloc, ObTransCtx*& ctx)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  const bool is_bounded_staleness_read = false;
  const bool need_completed_dirty_txn = false;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid() || !trans_id.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(trans_id), K(for_replay), K(alloc));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context mgr error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->get_trans_ctx(
                 trans_id, for_replay, is_readonly, is_bounded_staleness_read, need_completed_dirty_txn, alloc, ctx))) {
    TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_id));
  } else if (OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "transaction context is null", K(partition), K(trans_id));
    ret = OB_ERR_UNEXPECTED;
  } else {
    TRANS_LOG(DEBUG, "get transaction context success", K(partition), K(trans_id));
  }

  return ret;
}

int ObCoordTransCtxMgr::add_partition(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransCtxMgrImpl::add_partition(partition))) {
    TRANS_LOG(WARN, "coordinator add partition error", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "coordinator add partition success", K(partition));
  }

  return ret;
}

int ObCoordTransCtxMgr::remove_partition(const ObPartitionKey& partition, const bool graceful)
{
  int ret = OB_SUCCESS;
  bool need_retry = true;
  const int64_t SLEEP_US = 20000;                  // 20ms
  const int64_t PRINT_LOG_INTERVAL = 1000 * 1000;  // 1s
  const int64_t MAX_RETRY_NUM = 50;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  const KillTransArg arg(graceful, false);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else {
    do {
      DRWLock::RDLockGuard guard(rwlock_);
      if (OB_FAIL(ObTransCtxMgrImpl::stop_partition(partition, graceful))) {
        TRANS_LOG(WARN, "coordinator stop partition error", KR(ret), K(partition));
      }
    } while (0);
    if (OB_SUCC(ret)) {
      // there is no limit for retry times
      // trans ctx are required to be released
      for (int64_t retry = 0; need_retry && is_running_ && OB_SUCC(ret); ++retry) {
        need_retry = false;
        do {
          bool is_all_trans_clear = false;
          DRWLock::RDLockGuard guard(rwlock_);
          // if partition has been removed, OB_SUCCESS will be returned
          if (NULL != (ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
            if (OB_FAIL(ObTransCtxMgrImpl::wait_partition(partition))) {
              if (REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
                TRANS_LOG(WARN, "coordinator wait partition error", KR(ret), K(retry), K(partition), K(*ctx_mgr));
              }
              need_retry = (OB_EAGAIN == ret);
              if (need_retry && MAX_RETRY_NUM == retry && NULL != ctx_mgr) {
                // kill all trans if reach MAX_RETRY_NUM
                if (OB_FAIL(ctx_mgr->kill_all_trans(arg, is_all_trans_clear))) {
                  TRANS_LOG(WARN, "kill all transaction context error", KR(ret), K(arg));
                } else if (!is_all_trans_clear) {
                  const bool verbose = true;
                  ctx_mgr->print_all_ctx(ObPartitionTransCtxMgr::MAX_HASH_ITEM_PRINT, verbose);
                  // here context has exited, therefore no need wait partition operation
                  if (0 == ctx_mgr->get_active_read_write_count() && 0 == ctx_mgr->get_read_only_count()) {
                    if (0 != ctx_mgr->get_ctx_count()) {
                      TRANS_LOG(
                          WARN, "maybe some context memory not free, please attention", K(partition), K(*ctx_mgr));
                    }
                    need_retry = false;
                    // OB_SUCCESS is not returned here.
                    // This is because partition mgr in hashmap can not be removed.
                    ret = OB_EAGAIN;
                  }
                } else {
                  need_retry = false;
                }
              }
            }
          }
        } while (0);
        if (need_retry) {
          ret = OB_SUCCESS;
          // retry after 20ms
          ObTransCond::usleep(SLEEP_US);
        }
      }  // end for
      if (OB_SUCC(ret)) {
        DRWLock::WRRetryLockGuard guard(rwlock_);

        // if partition has been removed, OB_SUCCESS is returned.
        if (NULL != get_partition_trans_ctx_mgr(partition)) {
          if (OB_FAIL(ObTransCtxMgrImpl::remove_partition(partition))) {
            TRANS_LOG(WARN, "coordinator remove partition error", KR(ret), K(partition));
          } else {
            TRANS_LOG(INFO, "coordinator remove partition success", K(partition));
          }
        }
      } else if (OB_EAGAIN == ret) {
        TRANS_LOG(WARN, "remove partition error, but return OB_SUCCESS", KR(ret), K(partition));
        ret = OB_SUCCESS;
      } else {
        // do nothing
      }
    }  // end if
  }
  UNUSED(MAX_RETRY_NUM);

  return ret;
}

int ObCoordTransCtxMgr::block_partition(const ObPartitionKey& partition, bool& is_all_trans_clear)
{
  int ret = OB_SUCCESS;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransCtxMgrImpl::block_partition(partition, is_all_trans_clear))) {
    TRANS_LOG(WARN, "coordinator block partition error", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "coordinator block partition success", K(partition));
  }

  return ret;
}

int ObCoordTransCtxMgr::revert_trans_ctx(ObTransCtx* ctx)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "invalid argument", KP(ctx));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // Reference cannot be used here, otherwise context memory will be released
    // and core dump may occur when printing partition
    const ObPartitionKey partition = ctx->get_partition();
    if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
      TRANS_LOG(WARN, "get partition transaction context mgr error", K(partition));
      ret = OB_PARTITION_NOT_EXIST;
    } else if (OB_FAIL(ctx_mgr->revert_trans_ctx(ctx))) {
      TRANS_LOG(WARN, "revert transaction context error", KR(ret), K(partition), "context", *ctx);
    } else {
      TRANS_LOG(DEBUG, "revert transaction context success", K(partition));
    }
  }

  return ret;
}

int ObCoordTransCtxMgr::leader_revoke(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  const int64_t SLEEP_US = 20000;  // 20ms
  bool need_retry = true;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t retry = 0; need_retry && OB_SUCC(ret); ++retry) {
      need_retry = false;
      do {
        DRWLock::RDLockGuard guard(rwlock_);
        if (OB_FAIL(ObTransCtxMgrImpl::leader_revoke(partition, 0 == retry))) {
          TRANS_LOG(WARN, "coordinator leader revoke error", KR(ret), K(retry), K(partition));
          need_retry = (OB_EAGAIN == ret);
        }
      } while (0);
      if (!is_running_) {
        need_retry = false;
        ret = OB_NOT_RUNNING;
      } else if (need_retry) {
        ret = OB_SUCCESS;
        ObTransCond::usleep(SLEEP_US);
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

// Clean up all ctx replayed in the follower.
// Currently, it is used for conversion F -> L
int ObCoordTransCtxMgr::clear_all_ctx(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(rwlock_);

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransCtxMgrImpl::clear_all_ctx(partition))) {
    TRANS_LOG(WARN, "coordinator kill trans ctx error", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "coordinator kill trans ctx success", K(partition));
  }

  return ret;
}

int ObCoordTransCtxMgr::kill_all_trans(
    const ObPartitionKey& partition, const KillTransArg& arg, bool& is_all_trans_clear)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(rwlock_);

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (arg.need_kill_coord_ctx_) {
    if (OB_FAIL(ObTransCtxMgrImpl::kill_all_trans(partition, arg, is_all_trans_clear))) {
      TRANS_LOG(WARN, "coordinator kill all trans error", KR(ret), K(partition));
    } else {
      TRANS_LOG(INFO, "coordinator kill all trans success", K(partition));
    }
  } else {
    is_all_trans_clear = true;
  }

  return ret;
}

int ObCoordTransCtxMgr::wait_1pc_trx_end(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(rwlock_);

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransCtxMgrImpl::wait_1pc_trx_end(partition))) {
    TRANS_LOG(WARN, "coordinator wait all 1pc trx end error", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "coordinator wait all 1pc trx end success", K(partition));
  }

  return ret;
}

int ObCoordTransCtxMgr::wait_all_trans_clear(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(rwlock_);

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransCtxMgrImpl::wait_all_trans_clear(partition))) {
    TRANS_LOG(WARN, "coordinator wait all trans clear error", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "coordinator wait all trans clear success", K(partition));
  }

  return ret;
}

int ObCoordTransCtxMgr::leader_takeover(const ObPartitionKey& partition, const int64_t checkpoint)
{
  int ret = OB_SUCCESS;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransCtxMgrImpl::leader_takeover(partition, checkpoint))) {
    TRANS_LOG(ERROR, "coordinator leader takeover error", KR(ret), K(partition));
  } else {
    // do nothing
  }

  return ret;
}

int ObCoordTransCtxMgr::leader_active(const ObPartitionKey& partition, const LeaderActiveArg& arg)
{
  int ret = OB_SUCCESS;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObCoordTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransCtxMgrImpl::leader_active(partition, arg))) {
    TRANS_LOG(WARN, "coordinator leader active error", KR(ret), K(partition));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::init(const int ctx_type, ObITsMgr* ts_mgr, storage::ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(ObTransCtxMgrImpl::init(ctx_type, ts_mgr, partition_service))) {
    TRANS_LOG(WARN, "ObTransCtxMgrImpl init error", KR(ret));
  } else {
    is_inited_ = true;
    TRANS_LOG(INFO, "ObPartTransCtxMgr inited success");
  }

  return ret;
}

int ObPartTransCtxMgr::start()
{
  int ret = OB_SUCCESS;

  DRWLock::WRRetryLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr is not inited");
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr is already running");
    ret = OB_ERR_UNEXPECTED;
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "ObPartTransCtxMgr start success");
  }

  return ret;
}

int ObPartTransCtxMgr::stop()
{
  int ret = OB_SUCCESS;

  DRWLock::WRRetryLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr is not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr already has been stopped");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(ObTransCtxMgrImpl::stop_all_partition())) {
    TRANS_LOG(WARN, "stop all partition error", KR(ret));
  } else {
    is_running_ = false;
    TRANS_LOG(INFO, "ObPartTransCtxMgr stop success");
  }

  return ret;
}

int ObPartTransCtxMgr::wait()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t SLEEP_US = 100 * 1000;
  const int64_t MAX_WAIT_RETRY_TIMES = 10;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr is not inited");
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr is running");
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t retry = 0;
    for (; OB_SUCCESS == ret && retry < MAX_WAIT_RETRY_TIMES; ++retry) {
      {
        DRWLock::RDLockGuard guard(rwlock_);
        if (OB_FAIL(ObTransCtxMgrImpl::wait_all_partition())) {
          if (OB_EAGAIN == ret) {
            ret = OB_SUCCESS;
          } else {
            TRANS_LOG(WARN, "wait all partition error", KR(ret), K(retry));
          }
        } else {
          break;
        }
      }
      ObTransCond::usleep(SLEEP_US);
    }
    if (OB_FAIL(ret) || MAX_WAIT_RETRY_TIMES == retry) {
      DRWLock::RDLockGuard guard(rwlock_);
      if (OB_SUCCESS != (tmp_ret = ObTransCtxMgrImpl::print_all_partition_ctx())) {
        TRANS_LOG(WARN, "print all partition context error", "ret", tmp_ret);
      }
    }
  }

  return ret;
}

void ObPartTransCtxMgr::destroy()
{
  DRWLock::WRRetryLockGuard guard(rwlock_);

  if (is_inited_) {
    is_inited_ = false;
    ObTransCtxMgrImpl::destroy();
    TRANS_LOG(INFO, "ObPartTransCtxMgr destroyed");
  }
}

void ObPartTransCtxMgr::reset()
{
  is_inited_ = false;
  is_running_ = false;
  ObTransCtxMgrImpl::reset();
}

int ObPartTransCtxMgr::get_trans_ctx(const ObPartitionKey& partition, const ObTransID& trans_id, const bool for_replay,
    const bool is_readonly, bool& alloc, ObTransCtx*& ctx)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  const bool is_bounded_staleness_read = false;
  const bool need_completed_dirty_txn = false;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid() || !trans_id.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(trans_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->get_trans_ctx(
                 trans_id, for_replay, is_readonly, is_bounded_staleness_read, need_completed_dirty_txn, alloc, ctx))) {
    TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_id));
  } else if (OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "transaction context is null", K(partition), K(trans_id));
    ret = OB_ERR_UNEXPECTED;
  } else {
    TRANS_LOG(DEBUG, "get transaction context success", K(partition), K(trans_id));
  }

  return ret;
}

int ObPartTransCtxMgr::get_trans_ctx(const ObPartitionKey& partition, const ObTransID& trans_id, const bool for_replay,
    const bool is_readonly, const bool is_bounded_staleness_read, const bool need_completed_dirty_txn, bool& alloc,
    ObTransCtx*& ctx)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid() || !trans_id.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(trans_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->get_trans_ctx(
                 trans_id, for_replay, is_readonly, is_bounded_staleness_read, need_completed_dirty_txn, alloc, ctx))) {
    TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_id), K(alloc));
  } else if (OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "transaction context is null", K(partition), K(trans_id));
    ret = OB_ERR_UNEXPECTED;
  } else {
    TRANS_LOG(DEBUG, "get transaction context success", K(partition), K(trans_id));
  }

  return ret;
}

int ObPartTransCtxMgr::get_cached_pg_guard(const ObPartitionKey& partition, storage::ObIPartitionGroupGuard*& pg_guard)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(TRACE, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    // Due to the process design of partition, the follower may not guarantee that
    // there must be an initialized pg_guard.
    // Therefore, you need to have a comprehensive recovery method
    if (OB_ISNULL(ctx_mgr->get_pg_guard().get_partition_group())) {
      ret = ctx_mgr->recover_pg_guard();
    }
    if (OB_SUCC(ret)) {
      pg_guard = &(ctx_mgr->get_pg_guard());
    }
  }
  return ret;
}

int ObPartTransCtxMgr::check_trans_partition_leader_unsafe(const ObPartitionKey& partition, bool& is_leader)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(TRACE, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    is_leader = ctx_mgr->is_trans_partition_leader();
  }
  return ret;
}

int ObPartTransCtxMgr::add_partition(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransCtxMgrImpl::add_partition(partition))) {
    TRANS_LOG(WARN, "participant add partition error", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "participant add partition success", K(partition));
  }

  return ret;
}

int ObPartTransCtxMgr::remove_partition(const ObPartitionKey& partition, const bool graceful)
{
  int ret = OB_SUCCESS;
  bool need_retry = true;
  const int64_t SLEEP_US = 20000;                  // 20ms
  const int64_t PRINT_LOG_INTERVAL = 1000 * 1000;  // 1s
  const int64_t MAX_RETRY_NUM = 50;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  const KillTransArg arg(graceful, false);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else {
    do {
      DRWLock::RDLockGuard guard(rwlock_);
      if (OB_FAIL(ObTransCtxMgrImpl::stop_partition(partition, graceful))) {
        if (OB_ENTRY_NOT_EXIST != ret && OB_PARTITION_NOT_EXIST != ret) {
          TRANS_LOG(WARN, "participant stop partition error", KR(ret), K(partition));
        } else {
          TRANS_LOG(INFO, "participant stop partition error", KR(ret), K(partition));
        }
      }
    } while (0);
    if (OB_SUCC(ret)) {
      // there is no limit for retry times
      // trans ctx are required to be released
      for (int64_t retry = 0; need_retry && is_running_ && OB_SUCC(ret); ++retry) {
        need_retry = false;
        do {
          bool is_all_trans_clear = false;
          DRWLock::RDLockGuard guard(rwlock_);
          // if partition has been removed, OB_SUCCESS will be returned
          if (NULL != (ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
            if (OB_FAIL(ObTransCtxMgrImpl::wait_partition(partition))) {
              if (REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
                TRANS_LOG(WARN, "participant wait partition error", KR(ret), K(retry), K(partition), K(*ctx_mgr));
              }
              need_retry = (OB_EAGAIN == ret);
              if (need_retry && MAX_RETRY_NUM == retry && NULL != ctx_mgr) {
                // kill all trans if reach MAX_RETRY_NUM
                if (OB_FAIL(ctx_mgr->kill_all_trans(arg, is_all_trans_clear))) {
                  TRANS_LOG(WARN, "kill all transaction context error", KR(ret), K(arg));
                } else if (!is_all_trans_clear) {
                  const bool verbose = true;
                  ctx_mgr->print_all_ctx(ObPartitionTransCtxMgr::MAX_HASH_ITEM_PRINT, verbose);
                  // here context has exited, therefore no need wait partition operation
                  if (0 == ctx_mgr->get_active_read_write_count() && 0 == ctx_mgr->get_read_only_count()) {
                    if (0 != ctx_mgr->get_ctx_count()) {
                      TRANS_LOG(
                          ERROR, "maybe some context memory not free, please attention", K(partition), K(*ctx_mgr));
                    }
                    need_retry = false;
                    // OB_SUCCESS is not returned here.
                    // This is because partition mgr in hashmap can not be removed.
                    ret = OB_EAGAIN;
                  }
                } else {
                  need_retry = false;
                }
              }
            }
          }
        } while (0);
        if (need_retry) {
          ret = OB_SUCCESS;
          // retry after 20ms
          ObTransCond::usleep(SLEEP_US);
        }
      }  // end for
      if (OB_SUCC(ret)) {
        DRWLock::WRRetryLockGuard guard(rwlock_);
        // if partition has been removed, OB_SUCCESS is returned.
        if (NULL != get_partition_trans_ctx_mgr(partition)) {
          if (OB_FAIL(ObTransCtxMgrImpl::remove_partition(partition))) {
            TRANS_LOG(WARN, "participant remove partition error", KR(ret), K(partition));
          } else {
            TRANS_LOG(INFO, "participant remove partition success", K(partition));
          }
        }
      } else if (OB_EAGAIN == ret) {
        TRANS_LOG(WARN, "remove partition error, but return OB_SUCCESS", KR(ret), K(partition));
        ret = OB_SUCCESS;
      } else {
        // do nothing
      }
    }  // end if
  }
  UNUSED(MAX_RETRY_NUM);

  return ret;
}

int ObPartTransCtxMgr::block_partition(const ObPartitionKey& partition, bool& is_all_trans_clear)
{
  int ret = OB_SUCCESS;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransCtxMgrImpl::block_partition(partition, is_all_trans_clear))) {
    TRANS_LOG(WARN, "participant block partition error", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "participant block partition success", K(partition));
  }

  return ret;
}

int ObPartTransCtxMgr::revert_trans_ctx(ObTransCtx* ctx)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "invalid argument", KP(ctx));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // reference cannot be used here, otherwise context memory will be released
    // and core dump may occur when printing partition
    const ObPartitionKey partition = ctx->get_partition();
    if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
      TRANS_LOG(WARN, "get partition transaction context mgr error", K(partition));
      ret = OB_PARTITION_NOT_EXIST;
    } else if (OB_FAIL(ctx_mgr->revert_trans_ctx(ctx))) {
      TRANS_LOG(WARN, "revert transaction context error", KR(ret), K(partition), "context", *ctx);
    } else {
      TRANS_LOG(DEBUG, "revert transaction context success", K(partition));
    }
  }

  return ret;
}

int ObPartTransCtxMgr::replay_start_working_log(
    const ObPartitionKey& pkey, const int64_t timestamp, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    ret = OB_PARTITION_NOT_EXIST;
    TRANS_LOG(WARN, "get partition transaction context mgr error", KR(ret), K(pkey));
  } else if (OB_FAIL(ctx_mgr->replay_start_working_log(timestamp, log_id))) {
    TRANS_LOG(WARN, "replay start working log error", KR(ret), K(pkey), K(timestamp), K(log_id));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::leader_revoke(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  const int64_t SLEEP_US = 20000;  // 20ms
  bool need_retry = true;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else {
    for (int64_t retry = 0; need_retry && OB_SUCC(ret); ++retry) {
      need_retry = false;
      do {
        DRWLock::RDLockGuard guard(rwlock_);
        if (OB_FAIL(ObTransCtxMgrImpl::leader_revoke(partition, 0 == retry))) {
          TRANS_LOG(WARN, "participant leader revoke error", KR(ret), K(retry), K(partition));
          need_retry = (OB_EAGAIN == ret);
        }
      } while (0);
      if (!is_running_) {
        need_retry = false;
        ret = OB_NOT_RUNNING;
      } else if (need_retry) {
        ret = OB_SUCCESS;
        ObTransCond::usleep(SLEEP_US);
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

// Clean up all ctx replayed in the follower.
// Currently, it is used for conversion F -> L
int ObPartTransCtxMgr::clear_all_ctx(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(rwlock_);

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransCtxMgrImpl::clear_all_ctx(partition))) {
    TRANS_LOG(WARN, "participant kill trans ctx error", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "participant kill trans ctx success", K(partition));
  }

  return ret;
}

int ObPartTransCtxMgr::kill_all_trans(
    const ObPartitionKey& partition, const KillTransArg& arg, bool& is_all_trans_clear)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(rwlock_);

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (ObTransCtxType::SLAVE_PARTICIPANT == ctx_type_ && arg.ignore_ro_trans_) {
    is_all_trans_clear = true;
    TRANS_LOG(INFO, "slave participant need not kill trans", K(partition), K(arg));
  } else if (OB_FAIL(ObTransCtxMgrImpl::kill_all_trans(partition, arg, is_all_trans_clear))) {
    TRANS_LOG(WARN, "participant kill all trans error", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "participant kill all trans success", K(partition));
  }

  return ret;
}

int ObPartTransCtxMgr::calculate_trans_cost(const ObTransID& tid, uint64_t& cost)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(rwlock_);

  if (!tid.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(tid));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransCtxMgrImpl::calculate_trans_cost(tid, cost))) {
    TRANS_LOG(WARN, "calculate txn cost error", KR(ret), K(tid));
  }

  return ret;
}

int ObPartTransCtxMgr::wait_1pc_trx_end(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(rwlock_);

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransCtxMgrImpl::wait_1pc_trx_end(partition))) {
    TRANS_LOG(WARN, "participant wait all 1pc trx end error", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "participant wait all 1pc trx end success", K(partition));
  }

  return ret;
}

int ObPartTransCtxMgr::wait_all_trans_clear(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(rwlock_);

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransCtxMgrImpl::wait_all_trans_clear(partition))) {
    TRANS_LOG(WARN, "participant wait all trans clear error", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "participant wait all trans clear success", K(partition));
  }

  return ret;
}

int ObPartTransCtxMgr::check_all_trans_in_trans_table_state(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(rwlock_);

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransCtxMgrImpl::check_all_trans_in_trans_table_state(partition))) {
    TRANS_LOG(WARN, "participant check all trans in trans table state error", KR(ret), K(partition));
  } else {
    TRANS_LOG(INFO, "participant check all trans in trans table state success", K(partition));
  }

  return ret;
}

int ObPartTransCtxMgr::leader_takeover(const ObPartitionKey& partition, const int64_t checkpoint)
{
  int ret = OB_SUCCESS;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ObTransCtxMgrImpl::leader_takeover(partition, checkpoint))) {
    TRANS_LOG(ERROR, "participant leader takeover error", KR(ret), K(partition));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::leader_active(const ObPartitionKey& partition, const LeaderActiveArg& arg)
{
  int ret = OB_SUCCESS;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ObTransCtxMgrImpl::leader_active(partition, arg))) {
    TRANS_LOG(WARN, "participant leader active error", KR(ret), K(partition), K(arg));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::get_memtable_ctx(const ObPartitionKey& partition, const ObTransID& trans_id,
    const bool is_bounded_staleness_read, ObMemtableCtx*& mt_ctx)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  ObTransCtx* ctx = NULL;
  const bool for_replay = false;
  const bool is_readonly = false;
  const bool need_completed_dirty_txn = false;
  bool alloc = false;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid() || !trans_id.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(trans_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(partition), K(trans_id));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->get_trans_ctx(
                 trans_id, for_replay, is_readonly, is_bounded_staleness_read, need_completed_dirty_txn, alloc, ctx))) {
    TRANS_LOG(WARN, "get transaction context error", KR(ret), K(partition), K(trans_id));
  } else if (OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "transaction context is null", K(partition), K(trans_id));
    ret = OB_ERR_UNEXPECTED;
  } else if (is_bounded_staleness_read) {
    mt_ctx = static_cast<ObSlaveTransCtx*>(ctx)->get_memtable_ctx();
  } else {
    mt_ctx = static_cast<ObPartTransCtx*>(ctx)->get_memtable_ctx();
  }

  return ret;
}

int ObPartTransCtxMgr::revert_memtable_ctx(memtable::ObIMemtableCtx*& mt_ctx)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(mt_ctx)) {
    TRANS_LOG(WARN, "invalid argument", KP(mt_ctx));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObTransCtx* ctx = mt_ctx->get_trans_ctx();
    if (OB_ISNULL(ctx)) {
      TRANS_LOG(WARN, "ctx is null", KP(ctx));
      ret = OB_ERR_UNEXPECTED;
    } else {
      // Ref cannot be used here, otherwise context memory will be released
      // and then core dump may occur when printing partition
      const ObPartitionKey partition = ctx->get_partition();
      if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
        TRANS_LOG(WARN, "get partition transaction context manager error", K(partition));
        ret = OB_PARTITION_NOT_EXIST;
      } else if (OB_FAIL(ctx_mgr->revert_trans_ctx(ctx))) {
        TRANS_LOG(WARN, "revert transaction context error", KR(ret), "context", *ctx);
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

// After schema splitting, there is no cluster level schema_version concept.
// System table and user table schema_version are not comparable,
// and schemas between different tenants are not comparable.
// The schema versions are comparable, which is guaranteed by the upper caller
int ObPartTransCtxMgr::check_schema_version_elapsed(
    const ObPartitionKey& partition, const int64_t schema_version, const int64_t refreshed_schema_ts)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->check_schema_version_elapsed(schema_version, refreshed_schema_ts))) {
    TRANS_LOG(WARN, "check schema version elapsed error", KR(ret), K(partition), K(schema_version));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::check_ctx_create_timestamp_elapsed(const ObPartitionKey& partition, const int64_t ts)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->check_ctx_create_timestamp_elapsed(ts))) {
    if (OB_EAGAIN != ret) {
      TRANS_LOG(WARN, "check ctx create timestamp elapsed error", KR(ret), K(partition), K(ts));
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::iterate_trans_stat(const ObPartitionKey& partition, ObTransStatIterator& trans_stat_iter)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->iterate_trans_stat(trans_stat_iter))) {
    TRANS_LOG(WARN, "iterate transaction stat error", KR(ret), K(partition));
  } else {
    TRANS_LOG(DEBUG, "ObTransStatIterator set ready success", K(partition));
  }

  return ret;
}

int ObPartTransCtxMgr::print_all_trans_ctx(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    const bool verbose = true;
    ctx_mgr->print_all_ctx(ObPartitionTransCtxMgr::MAX_HASH_ITEM_PRINT, verbose);
  }

  return ret;
}

int ObPartTransCtxMgr::iterate_trans_lock_stat(
    const ObPartitionKey& partition, ObTransLockStatIterator& trans_lock_stat_iter)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->iterate_trans_lock_stat(trans_lock_stat_iter))) {
    TRANS_LOG(WARN, "iterate transaction lock stat error", KR(ret), K(partition));
  } else {
    TRANS_LOG(DEBUG, "ObTransLockStatIterator set ready success", K(partition));
  }

  return ret;
}

int ObPartTransCtxMgr::iterate_trans_result_info_in_TRIM(
    const common::ObPartitionKey& partition, ObTransResultInfoStatIterator& iter)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->iterate_trans_result_info_in_TRIM(iter))) {
    TRANS_LOG(WARN, "iterate transaction result info stat error", KR(ret), K(partition));
  } else {
    TRANS_LOG(DEBUG, "ObTransResultInfoStatIterator set ready success", K(partition));
  }

  return ret;
}

int ObPartTransCtxMgr::iterate_duplicate_partition_stat(
    const common::ObPartitionKey& partition, ObDuplicatePartitionStatIterator& iter)
{
  int ret = OB_SUCCESS;

  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->iterate_duplicate_partition_stat(iter))) {
    TRANS_LOG(WARN, "iterate duplicate partition stat error", KR(ret), K(partition));
  } else {
    TRANS_LOG(DEBUG, "iterate duplicate partition stat success", K(partition));
  }

  return ret;
}

int ObPartTransCtxMgr::update_max_trans_version(const ObPartitionKey& partition, const int64_t trans_version)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid() || 0 > trans_version)) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(trans_version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->update_max_trans_version(trans_version))) {
    TRANS_LOG(WARN, "update max trans version failed", KR(ret), K(partition));
  } else {
    TRANS_LOG(DEBUG, "update max trans version success", K(partition));
  }
  return ret;
}

int ObPartTransCtxMgr::get_active_read_write_count(const ObPartitionKey& partition, int64_t& count)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);
  if (!is_inited_) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    count = ctx_mgr->get_active_read_write_count();
  }

  return ret;
}

int ObPartTransCtxMgr::get_max_trans_version(const ObPartitionKey& partition, int64_t& max_trans_version)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->get_max_trans_version(max_trans_version))) {
    TRANS_LOG(WARN, "update max trans version failed", KR(ret), K(partition));
  } else {
    TRANS_LOG(DEBUG, "update max trans version success", K(partition));
  }
  return ret;
}

int ObPartTransCtxMgr::check_scheduler_status(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->check_scheduler_status())) {
    TRANS_LOG(WARN, "check_scheduler_status failed", KR(ret), K(partition));
  } else {
    TRANS_LOG(DEBUG, "check_scheduler_status success", K(partition));
  }
  return ret;
}

int ObPartTransCtxMgr::iterate_partition(ObPartitionIterator& partition_iter)
{
  int ret = OB_SUCCESS;

  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else {
    IteratePartitionFunctor fn(partition_iter);
    if (OB_FAIL(foreach_partition(fn))) {
      TRANS_LOG(WARN, "for each all partition error", KR(ret));
      //} else if (OB_FAIL(partition_iter.set_ready())) {
      //  TRANS_LOG(WARN, "ObPartitionIterator set ready error", KR(ret));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObPartTransCtxMgr::iterate_partition(ObELRStatSummary& elr_stat)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else {
    ObELRStatSummaryFunctor fn(elr_stat);
    if (OB_FAIL(foreach_partition(fn))) {
      TRANS_LOG(WARN, "for each all partition error", K(ret));
    }
  }
  return ret;
}

int ObPartTransCtxMgr::iterate_partition_mgr_stat(
    ObTransPartitionMgrStatIterator& partition_mgr_stat_iter, const ObAddr& addr)
{
  int ret = OB_SUCCESS;

  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else {
    IteratePartitionMgrStatFunctor fn(partition_mgr_stat_iter, addr);
    if (OB_FAIL(foreach_partition(fn))) {
      TRANS_LOG(WARN, "for each all partition error", KR(ret));
    }
  }

  return ret;
}

int ObPartTransCtxMgr::get_min_uncommit_prepare_version(const ObPartitionKey& partition, int64_t& min_prepare_version)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->get_min_uncommit_prepare_version(min_prepare_version))) {
    TRANS_LOG(WARN, "get min uncommit prepare version error", KR(ret), K(partition));
  } else {
    TRANS_LOG(
        DEBUG, "ObPartTransCtxMgr get min uncommit prepare version success", K(partition), K(min_prepare_version));
  }

  return ret;
}

/*
 * get the min prepare version of commit log ts greater than log_ts
 * */
int ObPartTransCtxMgr::get_min_prepare_version(
    const ObPartitionKey& partition, const int64_t log_ts, int64_t& min_prepare_version)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->get_min_prepare_version(log_ts, min_prepare_version))) {
    TRANS_LOG(WARN, "get min prepare version error", KR(ret), K(partition), K(log_ts));
  } else {
    TRANS_LOG(
        DEBUG, "ObPartTransCtxMgr get min prepare version success", K(partition), K(log_ts), K(min_prepare_version));
  }

  return ret;
}

int ObPartTransCtxMgr::gc_trans_result_info(const ObPartitionKey& pkey, const int64_t checkpoint_ts)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  ObTimeGuard tg("ObPartTransCtxMgr trans_result_info_gc ", 300000);
  DRWLock::RDLockGuard guard(rwlock_);
  tg.click();

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(pkey));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->gc_trans_result_info(checkpoint_ts))) {
    TRANS_LOG(WARN, "trans_result_info gc failed", K(ret), K(pkey));
  } else {
    TRANS_LOG(DEBUG, "ObPartTransCtxMgr handle trans_result_info gc success", K(pkey), K(checkpoint_ts));
  }
  tg.click();

  return ret;
}

int ObPartTransCtxMgr::get_min_uncommit_log(
    const ObPartitionKey& pkey, uint64_t& min_uncommit_log_id, int64_t& min_uncommit_log_ts)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(pkey));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->get_min_uncommit_log(min_uncommit_log_id, min_uncommit_log_ts))) {
    TRANS_LOG(WARN, "get min_uncommit_log_id failed", KR(ret), K(pkey));
  } else {
    TRANS_LOG(DEBUG, "ObPartTransCtxMgr get min_uncommit_log_id success", K(pkey), K(min_uncommit_log_id));
  }

  return ret;
}

int ObPartTransCtxMgr::checkpoint(
    const ObPartitionKey& pkey, const int64_t checkpoint_base_ts, storage::ObPartitionLoopWorker* lp_worker)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (ctx_mgr->get_active_read_write_count() <= 0) {
    // do nothing
  } else if (OB_FAIL(ctx_mgr->checkpoint(checkpoint_base_ts, lp_worker))) {
    TRANS_LOG(WARN, "checkpoint failed", KR(ret), K(pkey));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::relocate_data(const ObPartitionKey& pkey, memtable::ObIMemtable* memtable)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid() || OB_ISNULL(memtable))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey), KP(memtable));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->relocate_data(memtable))) {
    TRANS_LOG(WARN, "relocate data error", KR(ret), K(pkey));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::prepare_changing_leader(
    const ObPartitionKey& pkey, const ObAddr& proposal_leader, const int64_t round, const int64_t cnt)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid() || !proposal_leader.is_valid() || round < 1 || cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey), K(proposal_leader));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    ret = ctx_mgr->prepare_changing_leader(proposal_leader, round, cnt);
  }
  return ret;
}

int ObPartTransCtxMgr::get_state_in_TRIM(const common::ObPartitionKey& pkey, const ObTransID& trans_id, int& state)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    ret = ctx_mgr->get_state_in_TRIM(trans_id, state);
  }

  return ret;
}

int ObPartTransCtxMgr::get_partition_audit_info(const ObPartitionKey& pkey, ObPartitionAuditInfo& info)
{
  int ret = OB_SUCCESS;

  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->get_partition_audit_info(info))) {
    TRANS_LOG(WARN, "get partition audit info error", KR(ret), K(pkey));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::set_partition_audit_base_row_count(const ObPartitionKey& pkey, const int64_t count)
{
  int ret = OB_SUCCESS;

  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->set_partition_audit_base_row_count(count))) {
    TRANS_LOG(WARN, "set partition audit base row count error", KR(ret), K(pkey));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::handle_dup_lease_request(const ObPartitionKey& pkey, const ObDupTableLeaseRequestMsg& request)
{
  int ret = OB_SUCCESS;

  ObDupTablePartitionMgr* dup_table_mgr = NULL;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  ObTimeGuard tg("ObPartTransCtxMgr handle_dup_lease_request", 30000);
  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_ISNULL(dup_table_mgr = ctx_mgr->get_dup_table_partition_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "get dup table partition mgr error", K(ret), K(pkey));
  } else if (OB_FAIL(dup_table_mgr->handle_lease_request(request))) {
    TRANS_LOG(WARN, "handle lease request", K(ret), K(pkey));
  } else {
    // do nothing
  }
  tg.click();

  return ret;
}

int ObPartTransCtxMgr::handle_dup_redo_log_sync_response(
    const ObPartitionKey& pkey, const ObRedoLogSyncResponseMsg& msg)
{
  int ret = OB_SUCCESS;

  ObDupTablePartitionMgr* dup_table_mgr = NULL;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  ObTimeGuard tg("ObPartTransCtxMgr handle_dup_redo_log_sync_response", 30000);
  DRWLock::RDLockGuard guard(rwlock_);
  tg.click();
  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_ISNULL(dup_table_mgr = ctx_mgr->get_dup_table_partition_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "get dup table partition mgr error", K(ret), K(pkey));
  } else if (OB_FAIL(dup_table_mgr->handle_redo_log_sync_response(msg))) {
    TRANS_LOG(WARN, "handle redo log sync response error", K(ret), K(pkey));
  } else {
    // do nothing
  }
  tg.click();

  return ret;
}

int ObPartTransCtxMgr::handle_dup_lease_response(const ObPartitionKey& pkey, const ObDupTableLeaseResponseMsg& msg)
{
  int ret = OB_SUCCESS;

  ObDupTablePartitionInfo* dup_table_info = NULL;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  ObTimeGuard tg("ObPartTransCtxMgr handle_dup_lease_response", 30000);
  DRWLock::RDLockGuard guard(rwlock_);
  tg.click();
  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_ISNULL(dup_table_info = ctx_mgr->get_dup_table_partition_info())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "get dup table partition info error", K(ret), K(pkey));
  } else if (OB_FAIL(dup_table_info->handle_lease_response(msg))) {
    TRANS_LOG(WARN, "handle dup lease response error", K(ret), K(msg));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::handle_dup_redo_log_sync_request(
    const ObPartitionKey& pkey, const ObRedoLogSyncRequestMsg& msg, storage::ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;

  ObDupTablePartitionInfo* dup_table_info = NULL;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  ObTimeGuard tg("ObPartTransCtxMgr handle_dup_redo_log_sync_request", 30000);
  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_ISNULL(dup_table_info = ctx_mgr->get_dup_table_partition_info())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "get dup table partition info error", K(ret), K(pkey));
  } else if (OB_FAIL(dup_table_info->handle_redo_log_sync_request(msg, partition_service))) {
    TRANS_LOG(WARN, "handle dup redo log sync request error", K(ret), K(msg));
  } else {
    // do nothing
  }
  tg.click();

  return ret;
}

int ObPartTransCtxMgr::send_dup_table_lease_request_msg(
    const ObPartitionKey& pkey, const uint64_t min_replay_log_id, bool& need_renew_lease, bool& need_refresh_location)
{
  int ret = OB_SUCCESS;
  need_renew_lease = false;
  need_refresh_location = false;

  ObDupTablePartitionInfo* dup_table_info = NULL;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;
  ObTimeGuard tg("ObPartTransCtxMgr send_dup_table_lease_request_msg", 30000);
  DRWLock::RDLockGuard guard(rwlock_);
  tg.click();
  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_ISNULL(dup_table_info = ctx_mgr->get_dup_table_partition_info())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "get dup table partition info error", K(ret), K(pkey));
  } else if (OB_FAIL(dup_table_info->update_replay_log_id(min_replay_log_id))) {
    TRANS_LOG(WARN, "update replay log id error", K(ret), K(pkey), K(min_replay_log_id));
  } else {
    need_renew_lease = dup_table_info->need_renew_lease();
    if (need_renew_lease) {
      dup_table_info->get_lease_request_statistics().inc_request_count();
      dup_table_info->statistics(pkey);
      need_refresh_location = dup_table_info->need_refresh_location();
    }
  }

  return ret;
}

int ObPartTransCtxMgr::print_dup_table_partition_mgr(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->print_dup_table_partition_mgr())) {
    TRANS_LOG(WARN, "print dup table partition mgr error", KR(ret), K(pkey));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::is_dup_table_partition_serving(
    const ObPartitionKey& pkey, const bool is_master, bool& is_serving)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->is_dup_table_partition_serving(is_master, is_serving))) {
    TRANS_LOG(WARN, "check dup table partition serving error", KR(ret), K(pkey));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::check_and_inc_read_only_trx_count(const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->check_and_inc_read_only_trx_count())) {
    TRANS_LOG(WARN, "check and inc read only trx count error", KR(ret), K(pkey));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::check_and_dec_read_only_trx_count(const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->check_and_dec_read_only_trx_count())) {
    TRANS_LOG(WARN, "check and dec read only trx count error", KR(ret), K(pkey));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::remove_callback_for_uncommited_txn(const ObPartitionKey& partition, ObMemtable* mt)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid()) || OB_ISNULL(mt)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(partition), KP(mt));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(INFO, "partition is removed", K(partition), KP(mt));
  } else if (OB_FAIL(ctx_mgr->remove_callback_for_uncommited_txn(mt))) {
    TRANS_LOG(WARN, "remove callback for uncommited txn failed", KR(ret), K(partition), KP(mt));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::remove_mem_ctx_for_trans_ctx(const ObPartitionKey& partition, memtable::ObMemtable* mt)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!partition.is_valid()) || OB_ISNULL(mt)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(partition), KP(mt));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(INFO, "partition is removed", K(partition), KP(mt));
  } else if (OB_FAIL(ctx_mgr->remove_mem_ctx_for_trans_ctx(mt))) {
    TRANS_LOG(WARN, "remove mem ctx for trans ctx failed", KR(ret), K(partition), KP(mt));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::mark_dirty_trans(const ObPartitionKey& pkey, const ObMemtable* const frozen_memtable,
    const ObMemtable* const active_memtable, int64_t& cb_cnt, int64_t& applied_log_ts)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    ret = OB_PARTITION_NOT_EXIST;
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
  } else if (OB_FAIL(ctx_mgr->mark_dirty_trans(frozen_memtable, active_memtable, cb_cnt, applied_log_ts))) {
    TRANS_LOG(WARN, "mark dirty trans failed", KR(ret));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::get_applied_log_ts(const ObPartitionKey& pkey, int64_t& applied_log_ts)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    ret = OB_PARTITION_NOT_EXIST;
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
  } else if (OB_FAIL(ctx_mgr->get_applied_log_ts(applied_log_ts))) {
    TRANS_LOG(WARN, "get applied log id failed", KR(ret));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartTransCtxMgr::get_max_trans_version_before_given_log_ts(
    const ObPartitionKey& pkey, const int64_t log_ts, int64_t& max_trans_version, bool& is_all_rollback_trans)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = nullptr;

  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    ret = OB_PARTITION_NOT_EXIST;
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
  } else if (OB_FAIL(ctx_mgr->get_max_trans_version_before_given_log_ts(
                 log_ts, max_trans_version, is_all_rollback_trans))) {
    TRANS_LOG(WARN, "failed to get_max_trans_version_before_given_log_id", K(ret), K(pkey), K(log_ts));
  }
  return ret;
}

int ObPartTransCtxMgr::clear_unused_trans_status(const ObPartitionKey& pkey, const int64_t max_cleanout_log_ts)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = nullptr;

  DRWLock::RDLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    ret = OB_PARTITION_NOT_EXIST;
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
  } else if (OB_FAIL(ctx_mgr->clear_unused_trans_status(max_cleanout_log_ts))) {
    TRANS_LOG(WARN, "failed to clear unused trans status", K(ret), K(pkey), K(max_cleanout_log_ts));
  }
  return ret;
}

int ObPartTransCtxMgr::has_terminated_trx_in_given_log_ts_range(
    const ObPartitionKey& pkey, const int64_t start_log_ts, const int64_t end_log_ts, bool& has_terminated_trx)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = nullptr;

  DRWLock::RDLockGuard guard(rwlock_);
  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    ret = OB_PARTITION_NOT_EXIST;
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
  } else if (OB_FAIL(ctx_mgr->has_terminated_trx_in_given_log_ts_range(start_log_ts, end_log_ts, has_terminated_trx))) {
    TRANS_LOG(WARN,
        "failed to check has_terminated_trx_in_given_log_id_range",
        K(ret),
        K(pkey),
        K(start_log_ts),
        K(end_log_ts));
  }
  return ret;
}

int ObPartTransCtxMgr::set_last_restore_log_id(const common::ObPartitionKey& pkey, const uint64_t last_restore_log_id)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  ObTimeGuard tg("ObPartTransCtxMgr set_last_restore_log_id", 30000);
  DRWLock::RDLockGuard guard(rwlock_);
  tg.click();
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->set_last_restore_log_id(last_restore_log_id))) {
    TRANS_LOG(WARN, "failed to set_last_restore_log_id", KR(ret), K(pkey), K(last_restore_log_id));
  } else { /*do nothing*/
  }
  tg.click();

  return ret;
}

int ObPartTransCtxMgr::set_restore_snapshot_version(
    const common::ObPartitionKey& pkey, const int64_t restore_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  ObTimeGuard tg("ObPartTransCtxMgr set_last_restore_log_id", 30000);
  DRWLock::RDLockGuard guard(rwlock_);
  tg.click();
  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey), K(restore_snapshot_version));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->set_restore_snapshot_version(restore_snapshot_version))) {
    TRANS_LOG(WARN, "failed to set_last_restore_log_id", KR(ret), K(pkey), K(restore_snapshot_version));
  } else { /*do nothing*/
  }
  tg.click();

  return ret;
}

int ObPartTransCtxMgr::update_restore_replay_info(
    const common::ObPartitionKey& pkey, const int64_t restore_snapshot_version, const uint64_t last_restore_log_id)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  ObTimeGuard tg("ObPartTransCtxMgr set_last_restore_log_id", 30000);
  DRWLock::RDLockGuard guard(rwlock_);
  tg.click();
  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN,
        "get partition transaction context manager error",
        K(pkey),
        K(restore_snapshot_version),
        K(last_restore_log_id));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->update_restore_replay_info(restore_snapshot_version, last_restore_log_id))) {
    TRANS_LOG(WARN,
        "failed to update_restore_replay_info",
        KR(ret),
        K(pkey),
        K(restore_snapshot_version),
        K(last_restore_log_id));
  } else { /*do nothing*/
  }
  tg.click();

  return ret;
}

int ObPartTransCtxMgr::submit_log_for_split(const common::ObPartitionKey& pkey, bool& log_finished)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  ObTimeGuard tg("ObPartTransCtxMgr submit_log_for_split", 30000);
  DRWLock::RDLockGuard guard(rwlock_);
  tg.click();
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->submit_log_for_split(log_finished))) {
    TRANS_LOG(WARN, "failed to submit_log_for_split", KR(ret), K(pkey));
  } else { /*do nothing*/
  }
  tg.click();

  return ret;
}

void ObTransStateTable::release_ref()
{
  int ret = OB_SUCCESS;

  ObTransService* txs = NULL;
  if (NULL != partition_trans_ctx_mgr_) {
    if (NULL == (txs = storage::ObPartitionService::get_instance().get_trans_service())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "transaction service is null, unexpected error", KP(txs));
    } else {
      txs->get_part_trans_ctx_mgr().revert_partition_trans_ctx_mgr_with_ref(partition_trans_ctx_mgr_);
    }
  }
  UNUSED(ret);
}

int ObTransStateTable::check_trans_table_valid(const common::ObPartitionKey& pkey, bool& valid) const
{
  int ret = OB_SUCCESS;

  if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OB_ISNULL(partition_trans_ctx_mgr_)) {
    // mt_ctx may be reset
    valid = false;
  } else {
    const ObPartitionKey& trans_table_key = partition_trans_ctx_mgr_->get_partition();
    valid = (pkey == trans_table_key);
    if (!valid) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "different partition key", K(ret), K(pkey), K(trans_table_key));
    }
  }

  return ret;
}

int ObPartTransCtxMgr::copy_trans_table(
    ObTransService* txs, const common::ObPartitionKey& pkey, const ObIArray<ObPartitionKey>& dest_array)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  ObTimeGuard tg("ObPartTransCtxMgr copy_trans_table", 30000);
  DRWLock::RDLockGuard guard(rwlock_);
  tg.click();
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObPartTransCtxMgr not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(pkey))) {
    TRANS_LOG(WARN, "get partition transaction context manager error", K(pkey));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->copy_trans_table(txs, dest_array))) {
    TRANS_LOG(WARN, "failed to submit_log_for_split", KR(ret), K(pkey));
  } else { /*do nothing*/
  }
  tg.click();

  return ret;
}

int ObPartTransCtxMgr::init_dup_table_mgr(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* ctx_mgr = NULL;

  if (OB_UNLIKELY(!partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx_mgr = get_partition_trans_ctx_mgr(partition))) {
    TRANS_LOG(WARN, "partition transaction context not exist", K(partition));
    ret = OB_PARTITION_NOT_EXIST;
  } else if (OB_FAIL(ctx_mgr->init_dup_table_mgr())) {
    TRANS_LOG(WARN, "failed to init dup table mgr", K(ret));
  }
  return ret;
}

int ObTransStateTable::check_row_locked(const ObStoreRowkey& key, ObIMvccCtx& ctx,
    const transaction::ObTransID& read_trans_id, const transaction::ObTransID& data_trans_id,
    const int32_t sql_sequence, ObStoreRowLockState& lock_state)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(partition_trans_ctx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "partition transaction ctx mgr is null, unexpected error", K(ret), KP_(partition_trans_ctx_mgr));
  } else {
    ret = partition_trans_ctx_mgr_->check_row_locked(key, ctx, read_trans_id, data_trans_id, sql_sequence, lock_state);
  }

  if (OB_TRANS_CTX_NOT_EXIST == ret) {
    TRANS_LOG(ERROR,
        "sstable check row locked encounter trans ctx not exist",
        K(read_trans_id),
        K(data_trans_id),
        K(sql_sequence),
        K(ctx),
        K(key));
  }

  return ret;
}

int ObTransStateTable::lock_for_read(
    const ObLockForReadArg& lock_for_read_arg, bool& can_read, int64_t& trans_version, bool& is_determined_state)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(partition_trans_ctx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "partition transaction ctx mgr is null, unexpected error", K(ret), KP_(partition_trans_ctx_mgr));
  } else {
    ret = partition_trans_ctx_mgr_->lock_for_read(lock_for_read_arg, can_read, trans_version, is_determined_state);
  }

  if (OB_TRANS_CTX_NOT_EXIST == ret) {
    const ObTransID& read_trans_id = lock_for_read_arg.read_trans_id_;
    const ObTransID& data_trans_id = lock_for_read_arg.data_trans_id_;
    int32_t read_sql_sequence = lock_for_read_arg.read_sql_sequence_;
    int32_t data_sql_sequence = lock_for_read_arg.data_sql_sequence_;
    bool read_latest = lock_for_read_arg.read_latest_;
    TRANS_LOG(ERROR,
        "sstable lock for read encounter trans ctx not exist",
        K(read_trans_id),
        K(data_trans_id),
        K(read_sql_sequence),
        K(data_sql_sequence),
        K(read_latest));
  }

  return ret;
}

int ObTransStateTable::get_transaction_status_with_log_ts(const transaction::ObTransID& data_trans_id,
    const int64_t log_ts, ObTransTableStatusType& status, int64_t& trans_version)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(partition_trans_ctx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "partition transaction ctx mgr is null, unexpected error", K(ret), KP_(partition_trans_ctx_mgr));
  } else {
    ret = partition_trans_ctx_mgr_->get_transaction_status_with_log_ts(data_trans_id, log_ts, status, trans_version);
  }

  if (OB_TRANS_CTX_NOT_EXIST == ret) {
    TRANS_LOG(ERROR, "sstable get transaction status encounter trans ctx not exist", K(data_trans_id), K(log_ts));
  }

  return ret;
}

int ObTransStateTable::check_sql_sequence_can_read(
    const transaction::ObTransID& data_trans_id, const int64_t sql_sequence, bool& can_read)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(partition_trans_ctx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "partition transaction ctx mgr is null, unexpected error", K(ret), KP_(partition_trans_ctx_mgr));
  } else {
    ret = partition_trans_ctx_mgr_->check_sql_sequence_can_read(data_trans_id, sql_sequence, can_read);
  }

  if (OB_TRANS_CTX_NOT_EXIST == ret) {
    TRANS_LOG(
        ERROR, "sstable check sql sequence can read encounter trans ctx not exist", K(data_trans_id), K(sql_sequence));
  }

  return ret;
}

int ObTransStateTableGuard::set_trans_state_table(ObPartitionTransCtxMgr* part_mgr)
{
  int ret = OB_SUCCESS;

  if (NULL == part_mgr) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KP(part_mgr));
  } else {
    trans_state_table_.acquire_ref(part_mgr);
  }

  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
