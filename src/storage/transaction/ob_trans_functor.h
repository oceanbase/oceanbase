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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_FUNCTOR_
#define OCEANBASE_TRANSACTION_OB_TRANS_FUNCTOR_
#include "storage/ob_callback_queue_thread.h"
#include "storage/ob_partition_loop_worker.h"
#include "common/ob_simple_iterator.h"
#include "ob_trans_ctx.h"
#include "ob_trans_part_ctx.h"
#include "ob_trans_slave_ctx.h"
#include "ob_trans_sche_ctx.h"
#include "ob_trans_stat.h"
#include "ob_trans_partition_stat.h"
#include "ob_trans_version_mgr.h"
#include "ob_trans_result_info_mgr.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "share/ob_force_print_log.h"
#include "storage/transaction/ob_trans_ctx_mgr.h"

namespace oceanbase {
/*
namespace blocksstable
{
class ObMacroBlockWriter;
}
*/
namespace common {
class ObPartitionKey;
}

namespace transaction {
class ObTransCtx;
}

namespace transaction {

class LeaderActiveFunctor {
public:
  LeaderActiveFunctor(const common::ObPartitionKey& partition, const storage::LeaderActiveArg& arg)
      : partition_(partition), leader_active_arg_(arg)
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    bool bool_ret = false;
    int tmp_ret = common::OB_SUCCESS;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K_(partition), K(trans_id), "ctx", OB_P(ctx_base));
    } else if (common::OB_SUCCESS != (tmp_ret = ctx_base->leader_active(leader_active_arg_))) {
      TRANS_LOG(WARN, "leader active error", "ret", tmp_ret, K_(partition), K(trans_id), K_(leader_active_arg));
    } else {
      bool_ret = true;
    }

    return bool_ret;
  }

private:
  common::ObPartitionKey partition_;
  storage::LeaderActiveArg leader_active_arg_;
};

class LeaderTakeoverFunctor {
public:
  explicit LeaderTakeoverFunctor(const common::ObPartitionKey& partition, const int64_t checkpoint)
      : partition_(partition), checkpoint_(checkpoint)
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    bool bool_ret = false;
    int tmp_ret = common::OB_SUCCESS;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K_(partition), K(trans_id), "ctx", OB_P(ctx_base));
    } else if (common::OB_SUCCESS != (tmp_ret = ctx_base->leader_takeover(checkpoint_))) {
      TRANS_LOG(WARN, "leader takeover error", "ret", tmp_ret, K_(partition), K(trans_id));
    } else {
      bool_ret = true;
    }

    return bool_ret;
  }

private:
  common::ObPartitionKey partition_;
  int64_t checkpoint_;
};

class ReleaseAllTransCtxFunctor {
public:
  explicit ReleaseAllTransCtxFunctor(ObPartitionTransCtxMgr& ctx_mgr) : ctx_mgr_(ctx_mgr), released_count_(0)
  {}
  int64_t get_released_count() const
  {
    return released_count_;
  }
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    bool bool_ret = false;
    int ret = OB_SUCCESS;
    ObEndTransCallbackArray cb_array;
    KillTransArg arg(false, false);

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), KP(ctx_base));
    } else {
      if (OB_FAIL(ctx_base->kill(arg, cb_array))) {
        TRANS_LOG(WARN, "release kill transaction error", KR(ret), K(*ctx_base));
      } else {
        TRANS_LOG(WARN, "release kill transaction success", K(*ctx_base));
      }
      ++released_count_;
      bool_ret = true;
    }

    return bool_ret;
  }

private:
  ObPartitionTransCtxMgr& ctx_mgr_;
  int64_t released_count_;
};

class InactiveCtxFunctor {
public:
  InactiveCtxFunctor(const uint64_t tenant_id, ObEndTransCallbackArray& cb_array)
      : tenant_id_(tenant_id), cb_array_(cb_array)
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int tmp_ret = common::OB_SUCCESS;
    bool bool_ret = false;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), "ctx", OB_P(ctx_base));
      tmp_ret = common::OB_INVALID_ARGUMENT;
    } else {
      ObScheTransCtx* ctx = static_cast<ObScheTransCtx*>(ctx_base);
      if (tenant_id_ == ctx->get_tenant_id()) {
        if (OB_SUCCESS != (tmp_ret = ctx->inactive(cb_array_))) {
          TRANS_LOG(WARN, "inactive context error", "ret", tmp_ret, "context", *ctx);
        }
      }
    }
    if (OB_SUCCESS == tmp_ret) {
      bool_ret = true;
    }

    return bool_ret;
  }

private:
  uint64_t tenant_id_;
  ObEndTransCallbackArray& cb_array_;
};

class KillTransCtxFunctor {
public:
  KillTransCtxFunctor(const KillTransArg& arg, ObEndTransCallbackArray& cb_array)
      : arg_(arg), cb_array_(cb_array), freeze_version_()
  {}
  KillTransCtxFunctor(
      const common::ObVersion& freeze_version, const KillTransArg& arg, ObEndTransCallbackArray& cb_array)
      : arg_(arg), cb_array_(cb_array), freeze_version_(freeze_version)
  {}
  void set_release_audit_mgr_lock(const bool release_audit_mgr_lock)
  {
    release_audit_mgr_lock_ = release_audit_mgr_lock;
  }
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int ret = OB_SUCCESS;
    int tmp_ret = common::OB_SUCCESS;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), "ctx", OB_P(ctx_base));
      tmp_ret = common::OB_INVALID_ARGUMENT;
    } else {
      if (OB_FAIL(ctx_base->kill(arg_, cb_array_))) {
        TRANS_LOG(INFO, "kill transaction success", K(trans_id), K_(arg));
      } else if (common::OB_TRANS_CANNOT_BE_KILLED == ret) {
        TRANS_LOG(INFO, "transaction can not be killed", K(trans_id), "context", *ctx_base);
      } else {
        TRANS_LOG(WARN, "kill transaction error", "ret", ret, K(trans_id), "context", *ctx_base);
      }

      // explicitly release audit mgr lock
      if (release_audit_mgr_lock_) {
        if (OB_SUCCESS != (tmp_ret = ctx_base->reset_trans_audit_record())) {
          TRANS_LOG(WARN, "reset trans audot record failed", KR(tmp_ret));
        }
        ctx_base->get_record_mgr_guard().destroy();
      }
    }

    return OB_SUCCESS == ret;
  }

private:
  KillTransArg arg_;
  ObEndTransCallbackArray& cb_array_;
  ObVersion freeze_version_;
  bool release_audit_mgr_lock_;
};

class StopPartitionFunctor {
public:
  StopPartitionFunctor()
  {}
  ~StopPartitionFunctor()
  {}
  bool operator()(const ObPartitionKey& partition, ObPartitionTransCtxMgr* part_trans_ctx_mgr)
  {
    int tmp_ret = common::OB_SUCCESS;
    bool bool_ret = false;
    const bool graceful = false;

    if (!partition.is_valid() || OB_ISNULL(part_trans_ctx_mgr)) {
      TRANS_LOG(WARN, "invalid argument", K(partition), KP(part_trans_ctx_mgr));
      tmp_ret = OB_INVALID_ARGUMENT;
    } else if (OB_SUCCESS != (tmp_ret = part_trans_ctx_mgr->stop(graceful))) {
      TRANS_LOG(WARN, "ObPartitionTransCtxMgr stop error", K(tmp_ret), K(partition));
    } else {
      bool_ret = true;
    }

    return bool_ret;
  }
};

class WaitPartitionFunctor {
public:
  explicit WaitPartitionFunctor(int64_t& retry_count) : retry_count_(retry_count)
  {}
  ~WaitPartitionFunctor()
  {}
  bool operator()(const ObPartitionKey& partition, ObPartitionTransCtxMgr* ctx_mgr)
  {
    int tmp_ret = common::OB_SUCCESS;
    bool bool_ret = true;

    if (!partition.is_valid() || OB_ISNULL(ctx_mgr)) {
      TRANS_LOG(WARN, "invalid argument", K(partition), KP(ctx_mgr));
      tmp_ret = OB_INVALID_ARGUMENT;
    } else if (!ctx_mgr->is_stopped()) {
      TRANS_LOG(WARN, "partition has not been stopped", K(partition));
      tmp_ret = OB_PARTITION_IS_NOT_STOPPED;
    } else if (ctx_mgr->get_ctx_count() > 0) {
      // if there are unfinished transactions at the partition,
      // increase retry_count by 1
      ++retry_count_;
    } else {
      // do nothing
    }
    if (common::OB_SUCCESS != tmp_ret) {
      bool_ret = false;
    }

    return bool_ret;
  }

private:
  int64_t& retry_count_;
};

class RemovePartitionFunctor {
public:
  RemovePartitionFunctor()
  {}
  ~RemovePartitionFunctor()
  {}
  bool operator()(const ObPartitionKey& partition, ObPartitionTransCtxMgr* ctx_mgr)
  {
    int tmp_ret = common::OB_SUCCESS;
    bool bool_ret = false;

    if (!partition.is_valid() || OB_ISNULL(ctx_mgr)) {
      TRANS_LOG(WARN, "invalid argument", K(partition), KP(ctx_mgr));
      tmp_ret = OB_INVALID_ARGUMENT;
    } else if (!ctx_mgr->is_stopped()) {
      TRANS_LOG(WARN, "partition has not been stopped", K(partition));
      tmp_ret = OB_PARTITION_IS_NOT_STOPPED;
    } else {
      // Release all ctx memory on the partition
      ctx_mgr->destroy();
      // ObPartitionTransCtxMgrFactory::release(ctx_mgr);
      ctx_mgr = NULL;
      bool_ret = true;
    }
    UNUSED(tmp_ret);
    return bool_ret;
  }
};

class CalculateCostFunctor {
public:
  explicit CalculateCostFunctor(const ObTransID& tid) : tid_(tid), total_cost_(0)
  {}
  ~CalculateCostFunctor()
  {}
  bool operator()(const ObPartitionKey& partition, ObPartitionTransCtxMgr* ctx_mgr)
  {
    int tmp_ret = common::OB_SUCCESS;
    bool bool_ret = false;
    uint64_t part_cost = 0;

    if (!partition.is_valid() || OB_ISNULL(ctx_mgr)) {
      TRANS_LOG(WARN, "invalid argument", K(partition), KP(ctx_mgr));
      tmp_ret = OB_INVALID_ARGUMENT;
    } else if (ctx_mgr->is_stopped()) {
      TRANS_LOG(WARN, "partition has been stopped", K(partition));
      tmp_ret = OB_PARTITION_IS_STOPPED;
    } else if (OB_SUCCESS != (tmp_ret = ctx_mgr->calculate_trans_cost(tid_, part_cost))) {
      TRANS_LOG(WARN, "calculate trans cost from ctx_mgr error", K(tmp_ret), K(tid_));
      bool_ret = true;
    } else {
      total_cost_ += part_cost;
      bool_ret = true;
    }

    return bool_ret;
  }
  uint64_t get_total_cost() const
  {
    return total_cost_;
  }

private:
  ObTransID tid_;
  uint64_t total_cost_;
};

class IteratePartitionFunctor {
public:
  explicit IteratePartitionFunctor(ObPartitionIterator& partition_iter) : partition_iter_(partition_iter)
  {}
  bool operator()(const ObPartitionKey& partition, ObPartitionTransCtxMgr* part_trans_ctx_mgr)
  {
    int tmp_ret = common::OB_SUCCESS;
    bool bool_ret = false;

    if (!partition.is_valid() || OB_ISNULL(part_trans_ctx_mgr)) {
      TRANS_LOG(WARN, "invalid argument", K(partition), KP(part_trans_ctx_mgr));
      tmp_ret = OB_INVALID_ARGUMENT;
    } else if (OB_SUCCESS != (tmp_ret = partition_iter_.push(partition))) {
      TRANS_LOG(WARN, "ObPartitionIterator push partition error", K(tmp_ret), K(partition));
    } else {
      bool_ret = true;
    }

    return bool_ret;
  }

private:
  ObPartitionIterator& partition_iter_;
};

class ObELRStatSummaryFunctor {
public:
  explicit ObELRStatSummaryFunctor(ObELRStatSummary& elr_stat) : elr_stat_(elr_stat)
  {}
  bool operator()(const ObPartitionKey& partition, ObPartitionTransCtxMgr* part_trans_ctx_mgr)
  {
    int tmp_ret = common::OB_SUCCESS;
    bool bool_ret = false;

    if (!partition.is_valid() || OB_ISNULL(part_trans_ctx_mgr)) {
      TRANS_LOG(WARN, "invalid argument", K(partition), KP(part_trans_ctx_mgr));
      tmp_ret = OB_INVALID_ARGUMENT;
    } else {
      elr_stat_.with_dependency_trx_count_ += part_trans_ctx_mgr->get_with_dependency_trx_count();
      elr_stat_.without_dependency_trx_count_ += part_trans_ctx_mgr->get_without_dependency_trx_count();
      elr_stat_.end_trans_by_prev_count_ += part_trans_ctx_mgr->get_end_trans_by_prev_count();
      elr_stat_.end_trans_by_checkpoint_count_ += part_trans_ctx_mgr->get_end_trans_by_checkpoint_count();
      elr_stat_.end_trans_by_self_count_ += part_trans_ctx_mgr->get_end_trans_by_self_count();
      part_trans_ctx_mgr->reset_elr_statistic();
      bool_ret = true;
    }

    return bool_ret;
  }

private:
  ObELRStatSummary& elr_stat_;
};

class IteratePartitionMgrStatFunctor {
public:
  IteratePartitionMgrStatFunctor(ObTransPartitionMgrStatIterator& partition_mgr_stat_iter, const ObAddr& addr)
      : partition_mgr_stat_iter_(partition_mgr_stat_iter), addr_(addr)
  {}
  bool operator()(const ObPartitionKey& partition, ObPartitionTransCtxMgr* part_trans_ctx_mgr)
  {
    int tmp_ret = common::OB_SUCCESS;
    bool bool_ret = false;
    // FIXME
    const bool is_frozen = false;
    ObTransPartitionStat trans_partition_stat;

    if (!partition.is_valid() || OB_ISNULL(part_trans_ctx_mgr)) {
      TRANS_LOG(WARN, "invalid argument", K(partition), KP(part_trans_ctx_mgr));
      tmp_ret = OB_INVALID_ARGUMENT;
    } else if (OB_SUCCESS != (tmp_ret = trans_partition_stat.init(addr_,
                                  part_trans_ctx_mgr->partition_,
                                  part_trans_ctx_mgr->ctx_type_,
                                  part_trans_ctx_mgr->is_master_(),
                                  is_frozen,
                                  part_trans_ctx_mgr->is_stopped_(),
                                  part_trans_ctx_mgr->read_only_count_,
                                  part_trans_ctx_mgr->active_read_write_count_,
                                  part_trans_ctx_mgr->total_ctx_count_,
                                  (int64_t)(&(*part_trans_ctx_mgr)),
                                  part_trans_ctx_mgr->get_with_dependency_trx_count(),
                                  part_trans_ctx_mgr->get_without_dependency_trx_count(),
                                  part_trans_ctx_mgr->get_end_trans_by_prev_count(),
                                  part_trans_ctx_mgr->get_end_trans_by_checkpoint_count(),
                                  part_trans_ctx_mgr->get_end_trans_by_self_count()))) {
      TRANS_LOG(WARN, "ObTransPartitionStat init error", K_(addr), "manager", *part_trans_ctx_mgr);
    } else if (OB_SUCCESS != (tmp_ret = partition_mgr_stat_iter_.push(trans_partition_stat))) {
      TRANS_LOG(WARN,
          "ObPartitionMgrStatIterator push partition error",
          K(tmp_ret),
          K(partition),
          "partition_mgr",
          *part_trans_ctx_mgr);
    } else {
      bool_ret = true;
    }

    return bool_ret;
  }

private:
  ObTransPartitionMgrStatIterator& partition_mgr_stat_iter_;
  const ObAddr& addr_;
};

class IterateTransSchemaVersionStat {
public:
  explicit IterateTransSchemaVersionStat(const int64_t schema_version, const int64_t refreshed_schema_ts)
      : base_schema_version_(schema_version), refreshed_schema_ts_(refreshed_schema_ts), ret_(common::OB_SUCCESS)
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int tmp_ret = common::OB_SUCCESS;
    bool bool_ret = false;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), "ctx", OB_P(ctx_base));
      tmp_ret = OB_INVALID_ARGUMENT;
    } else {
      ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);
      if (OB_SUCCESS != (tmp_ret = ctx->check_schema_version_elapsed(base_schema_version_, refreshed_schema_ts_))) {
        if (OB_EAGAIN != tmp_ret) {
          TRANS_LOG(WARN, "check schema version error", K(tmp_ret), "ctx", *ctx);
        }
      } else {
        bool_ret = true;
      }
    }

    if (common::OB_SUCCESS != tmp_ret && common::OB_SUCCESS == ret_) {
      // return the first error
      ret_ = tmp_ret;
    }

    return bool_ret;
  }
  int get_ret() const
  {
    return ret_;
  }

private:
  int64_t base_schema_version_;
  int64_t refreshed_schema_ts_;
  int ret_;
};

class IterateCtxCreateTimestamp {
public:
  explicit IterateCtxCreateTimestamp(const int64_t ts) : base_ctx_create_ts_(ts)
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int tmp_ret = common::OB_SUCCESS;
    bool bool_ret = false;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), "ctx", OB_P(ctx_base));
      tmp_ret = OB_INVALID_ARGUMENT;
    } else {
      ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);
      if (OB_SUCCESS != (tmp_ret = ctx->check_ctx_create_timestamp_elapsed(base_ctx_create_ts_))) {
        if (OB_EAGAIN != tmp_ret) {
          TRANS_LOG(WARN, "check ctx create timestamp elapsed error", K(tmp_ret), "ctx", *ctx);
        }
      } else {
        bool_ret = true;
      }
    }
    UNUSED(tmp_ret);

    return bool_ret;
  }

private:
  int64_t base_ctx_create_ts_;
};

class IterateMinPrepareVersionFunctor {
public:
  explicit IterateMinPrepareVersionFunctor() : min_prepare_version_(INT64_MAX)
  {}
  int64_t get_min_prepare_version() const
  {
    return min_prepare_version_;
  }
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int tmp_ret = common::OB_SUCCESS;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      tmp_ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(tmp_ret), K(trans_id), "ctx", OB_P(ctx_base));
    } else {
      bool is_prepared = false;
      int64_t prepare_version = 0;
      ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);
      if (OB_SUCCESS != (tmp_ret = ctx->get_prepare_version_if_prepared(is_prepared, prepare_version))) {
        TRANS_LOG(WARN, "get prepare version if prepared failed", K(tmp_ret), K(*ctx));
      } else if (!is_prepared || prepare_version >= min_prepare_version_) {
        // do nothing
      } else {
        min_prepare_version_ = prepare_version;
      }
    }

    return (OB_SUCCESS == tmp_ret);
  }

private:
  int64_t min_prepare_version_;
};

class IterateMinPrepareVersionBeforeLogtsFunctor {
public:
  explicit IterateMinPrepareVersionBeforeLogtsFunctor(const int64_t log_ts)
      : min_prepare_version_(INT64_MAX), min_logts_(log_ts)
  {}
  int64_t get_min_prepare_version() const
  {
    return min_prepare_version_;
  }
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int tmp_ret = common::OB_SUCCESS;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      tmp_ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(tmp_ret), K(trans_id), "ctx", OB_P(ctx_base));
    } else {
      bool has_prepared = false;
      int64_t prepare_version = 0;
      ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);
      if (OB_SUCCESS != (tmp_ret = ctx->get_prepare_version_before_logts(min_logts_, has_prepared, prepare_version))) {
        TRANS_LOG(WARN, "get prepare version if prepared failed", K(tmp_ret), K(*ctx));
      } else if (!has_prepared || prepare_version >= min_prepare_version_) {
        // do nothing
      } else {
        min_prepare_version_ = prepare_version;
      }
    }

    return (OB_SUCCESS == tmp_ret);
  }

private:
  int64_t min_prepare_version_;
  int64_t min_logts_;
};

class IterateMinLogIdFunctor {
public:
  IterateMinLogIdFunctor() : min_uncommit_log_id_(UINT64_MAX), min_uncommit_log_ts_(INT64_MAX)
  {}
  uint64_t get_min_uncommit_log_id() const
  {
    return min_uncommit_log_id_;
  }
  int64_t get_min_uncommit_log_ts() const
  {
    return min_uncommit_log_ts_;
  }
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int tmp_ret = OB_SUCCESS;
    bool bool_ret = false;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), "ctx", OB_P(ctx_base));
    } else {
      ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);
      uint64_t tmp_log_id = UINT64_MAX;
      int64_t tmp_log_ts = INT64_MAX;
      if (OB_SUCCESS != (tmp_ret = ctx->get_min_log(tmp_log_id, tmp_log_ts))) {
        if (OB_NOT_INIT != tmp_ret) {
          TRANS_LOG(WARN, "get min log id error", "ret", tmp_ret, "context", *ctx);
        } else {
          bool_ret = true;
        }
      } else {
        if (0 == tmp_log_id) {
          TRANS_LOG(ERROR, "unexpected min log id", K(tmp_log_id), K(*ctx));
        }
        // If the transaction is a read-only transaction, return INT64_MAX as log_id by default
        if (tmp_log_id < min_uncommit_log_id_) {
          min_uncommit_log_id_ = tmp_log_id;
          min_uncommit_log_ts_ = tmp_log_ts;
        }
        bool_ret = true;
      }
    }

    return bool_ret;
  }

private:
  uint64_t min_uncommit_log_id_;
  int64_t min_uncommit_log_ts_;
};

class IteratePartCtxAskSchedulerStatusFunctor {
public:
  IteratePartCtxAskSchedulerStatusFunctor()
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int ret = OB_SUCCESS;

    if (OB_UNLIKELY(!trans_id.is_valid() || OB_ISNULL(ctx_base))) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id), "ctx", OB_P(ctx_base));
      // If you encounter a situation where part_ctx has not been init yet,
      // skip it directly, there will be a background thread retry
    } else if (!ctx_base->is_inited()) {
      // not inited, do nothing
    } else if (!ctx_base->get_trans_param().is_bounded_staleness_read()) {
      ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(ctx_base);
      if (OB_FAIL(part_ctx->check_scheduler_status())) {
        TRANS_LOG(WARN, "check scheduler status error", KR(ret), "ctx", *part_ctx);
      }
    } else {
      ObSlaveTransCtx* slave_ctx = static_cast<ObSlaveTransCtx*>(ctx_base);
      if (OB_FAIL(slave_ctx->check_scheduler_status())) {
        TRANS_LOG(WARN, "slave check scheduler status error", KR(ret), "ctx", *slave_ctx);
      }
    }

    return true;
  }
};

class IterateTransStatFunctor {
public:
  explicit IterateTransStatFunctor(ObTransStatIterator& trans_stat_iter) : trans_stat_iter_(trans_stat_iter)
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int tmp_ret = common::OB_SUCCESS;
    bool bool_ret = false;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), "ctx", OB_P(ctx_base));
      tmp_ret = OB_INVALID_ARGUMENT;
      // If you encounter a situation where part_ctx has not been init yet,
      // skip it directly, there will be a background thread retry
    } else if (!ctx_base->is_inited()) {
      // not inited, don't need to traverse
    } else {
      ObTransStat trans_stat;
      const int64_t ctx_ref = ctx_base->get_uref();
      // Judge whether the transaction has been decided by state
      bool has_decided = false;
      bool is_dirty = false;
      int64_t sql_no = 0;
      uint64_t lock_for_read_retry_count = 0;
      ObElrTransInfoArray prev_trans_arr;
      ObElrTransInfoArray next_trans_arr;
      int32_t prev_trans_commit_count = 0;
      uint32_t ctx_id = 0;
      int64_t already_synced_size = 0;
      int64_t remaining_data_size = 0;
      ObDistTransCtx* part_ctx = static_cast<ObDistTransCtx*>(ctx_base);
      if (Ob2PCState::is_valid(part_ctx->state_.get_state())) {
        if (Ob2PCState::INIT < part_ctx->state_.get_state()) {
          has_decided = true;
        }
      }
      if (part_ctx->is_bounded_staleness_read()) {
        ObSlaveTransCtx* ctx = static_cast<ObSlaveTransCtx*>(part_ctx);
        sql_no = ctx->stmt_info_.get_sql_no();
        lock_for_read_retry_count = ctx->get_lock_for_read_retry_count();
      } else {
        ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(part_ctx);
        sql_no = ctx->stmt_info_.get_sql_no();
        lock_for_read_retry_count = ctx->get_lock_for_read_retry_count();
        prev_trans_commit_count = ctx->get_ctx_dependency_wrap().get_prev_trans_commit_count();
        ctx_id = ctx->get_ctx_id();
        is_dirty = ctx->is_dirty_trans();
        remaining_data_size = ctx->get_pending_log_size();
        already_synced_size = ctx->get_flushed_log_size();
        ObElrTransArrGuard prev_trans_guard;
        ObElrTransArrGuard next_trans_guard;
        if (OB_UNLIKELY(
                OB_SUCCESS != (tmp_ret = ctx->get_ctx_dependency_wrap().get_prev_trans_arr_guard(prev_trans_guard)))) {
          TRANS_LOG(WARN, "get prev trans arr copy error", K(tmp_ret));
        } else if (OB_SUCCESS != (tmp_ret = prev_trans_arr.assign(prev_trans_guard.get_elr_trans_arr()))) {
          TRANS_LOG(WARN, "prev trans arr assign error", K(tmp_ret));
        } else if (OB_UNLIKELY(OB_SUCCESS !=
                               (tmp_ret = ctx->get_ctx_dependency_wrap().get_next_trans_arr_guard(next_trans_guard)))) {
          TRANS_LOG(WARN, "get next trans arr copy error", K(tmp_ret));
        } else if (OB_SUCCESS != (tmp_ret = next_trans_arr.assign(next_trans_guard.get_elr_trans_arr()))) {
          TRANS_LOG(WARN, "next trans arr assign error", K(tmp_ret));
        } else {
          // do nothing
        }
        //(void)ctx->check_memtable_ctx_ref();
      }
      if (part_ctx->is_too_slow_transaction()) {
        // If the transaction has not completed in 600 seconds, print its trace log
        part_ctx->print_trace_log();
      }
      if (OB_SUCCESS == tmp_ret) {
        ObPartitionArray participants_arr(
            common::ObModIds::OB_TRANS_PARTITION_ARRAY, common::OB_MALLOC_NORMAL_BLOCK_SIZE);
        if (OB_SUCCESS != (tmp_ret = part_ctx->get_participants_copy(participants_arr))) {
          TRANS_LOG(WARN, "ObTransStat get participants copy error", K(tmp_ret));
        } else if (OB_SUCCESS != (tmp_ret = trans_stat.init(part_ctx->addr_,
                                      trans_id,
                                      part_ctx->tenant_id_,
                                      part_ctx->is_exiting_,
                                      part_ctx->is_readonly_,
                                      has_decided,
                                      is_dirty,
                                      part_ctx->self_,
                                      participants_arr,
                                      part_ctx->trans_param_,
                                      part_ctx->ctx_create_time_,
                                      part_ctx->trans_expired_time_,
                                      ctx_ref,
                                      sql_no,
                                      part_ctx->state_.get_state(),
                                      part_ctx->get_session_id(),
                                      part_ctx->get_proxy_session_id(),
                                      part_ctx->get_trans_type(),
                                      part_ctx->get_part_trans_action(),
                                      lock_for_read_retry_count,
                                      (int64_t)(&(*part_ctx)),
                                      prev_trans_arr,
                                      next_trans_arr,
                                      prev_trans_commit_count,
                                      ctx_id,
                                      remaining_data_size,
                                      already_synced_size))) {
          TRANS_LOG(WARN,
              "ObTransStat init error",
              K(tmp_ret),
              K(has_decided),
              K(trans_id),
              "addr",
              part_ctx->addr_,
              "tenant_id",
              part_ctx->tenant_id_,
              "is_exiting",
              part_ctx->is_exiting_,
              "is_readonly",
              part_ctx->is_readonly_,
              "partition",
              part_ctx->self_,
              "participants",
              participants_arr,
              "trans_param",
              part_ctx->trans_param_,
              "ctx_create_time",
              part_ctx->ctx_create_time_,
              "trans_expired_time",
              part_ctx->trans_expired_time_,
              "sql_no",
              sql_no,
              "state",
              part_ctx->state_.get_state(),
              "session_id",
              part_ctx->get_session_id(),
              "proxy_session_id",
              part_ctx->get_proxy_session_id(),
              "trans_type",
              part_ctx->get_trans_type(),
              "part_trans_action",
              part_ctx->get_part_trans_action(),
              "lock_for_read_retry_count",
              lock_for_read_retry_count,
              "pending_log_size",
              remaining_data_size,
              "flushed_log_size",
              already_synced_size);
        } else if (OB_SUCCESS != (tmp_ret = trans_stat_iter_.push(trans_stat))) {
          TRANS_LOG(WARN, "ObTransStatIterator push trans stat error", K(tmp_ret));
        } else {
          // do nothing
        }
      }
    }
    if (OB_SUCCESS == tmp_ret) {
      bool_ret = true;
    }

    return bool_ret;
  }

private:
  ObTransStatIterator& trans_stat_iter_;
};

class IterateTransTableFunctor {
public:
  explicit IterateTransTableFunctor(blocksstable::ObMacroBlockWriter& writer)
      : writer_(writer),
        store_row_(),
        end_log_ts_(0),
        allocator_("trans_table"),
        tmp_buf_(NULL),
        pre_trans_key_(),
        dirty_trx_cnt_(0),
        clean_trx_cnt_(0)
  {}
  int init(const int64_t end_log_ts)
  {
    int ret = OB_SUCCESS;
    if (0 == end_log_ts) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "end log id is invalid", K(end_log_ts));
    } else {
      end_log_ts_ = end_log_ts;
      store_row_.row_val_.cells_ = cells_;
      store_row_.row_val_.count_ = COLUMN_CNT;
      store_row_.dml_ = storage::T_DML_INSERT;
      store_row_.flag_ = ObActionFlag::OP_ROW_EXIST;
    }
    return ret;
  }
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int ret = common::OB_SUCCESS;
    bool bool_ret = false;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), "ctx", OB_P(ctx_base));
      ret = OB_INVALID_ARGUMENT;
    } else {
      ObTransKey cur_trans_key(ctx_base->get_partition(), trans_id);
      if (pre_trans_key_.is_valid()) {
        if (pre_trans_key_.compare_trans_table(cur_trans_key) >= 0) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(
              WARN, "pre_trans_key should be smaller than cur_trans_key", K(ret), K_(pre_trans_key), K(cur_trans_key));
        }
      }
      if (OB_SUCC(ret)) {
        pre_trans_key_ = cur_trans_key;

        int64_t pos = 0;
        ObTransSSTableDurableCtxInfo ctx_info;
        ObPartTransCtx* part_ctx = NULL;

        if (OB_ISNULL(part_ctx = dynamic_cast<ObPartTransCtx*>(ctx_base))) {
          ret = OB_ERR_UNEXPECTED;
        } else if (!part_ctx->is_dirty_trans()) {
          // do nothing
          clean_trx_cnt_++;
        } else if (OB_FAIL(part_ctx->get_trans_sstable_durable_ctx_info(end_log_ts_, ctx_info))) {
          TRANS_LOG(WARN, "failed to get trans table status info", K(ret));
        } else {
          dirty_trx_cnt_++;
          int64_t serialize_size = trans_id.get_serialize_size() + ctx_info.get_serialize_size();
          if (serialize_size > BUF_LENGTH) {
            allocator_.reuse();
            if (OB_ISNULL(tmp_buf_ = static_cast<char*>(allocator_.alloc(serialize_size)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              STORAGE_LOG(WARN, "failed to allocate memory", K(serialize_size));
            }
          } else {
            tmp_buf_ = buf_;
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(trans_id.serialize(tmp_buf_, serialize_size, pos))) {
              TRANS_LOG(WARN, "failed to serialize trans_id", K(ret), K(ctx_info), K(pos));
            } else if (OB_FAIL(ctx_info.serialize(tmp_buf_, serialize_size, pos))) {
              TRANS_LOG(WARN, "failed to serialize status info", K(ret), K(ctx_info), K(pos));
            } else {
              store_row_.row_val_.cells_[0].set_binary(ObString(trans_id.get_serialize_size(), tmp_buf_));
              store_row_.row_val_.cells_[1].set_binary(
                  ObString(ctx_info.get_serialize_size(), tmp_buf_ + trans_id.get_serialize_size()));

              if (OB_FAIL(writer_.append_row(store_row_))) {
                TRANS_LOG(WARN, "failed to append row", K(ret));
              }
            }
          }
        }
      }
    }
    if (OB_SUCCESS == ret) {
      bool_ret = true;
    }

    return bool_ret;
  }
  int64_t get_dirty_trx_count()
  {
    return dirty_trx_cnt_;
  }
  int64_t get_clean_trx_count()
  {
    return clean_trx_cnt_;
  }

private:
  static const int64_t COLUMN_CNT = 2;
  static const int64_t BUF_LENGTH = 1000;

private:
  blocksstable::ObMacroBlockWriter& writer_;
  storage::ObStoreRow store_row_;
  ObObj cells_[COLUMN_CNT];
  char buf_[BUF_LENGTH];
  int64_t end_log_ts_;
  ObArenaAllocator allocator_;
  char* tmp_buf_;
  ObTransKey pre_trans_key_;
  int64_t dirty_trx_cnt_;
  int64_t clean_trx_cnt_;
};

class CopyTransTableFunctor {
public:
  explicit CopyTransTableFunctor() : trans_table_guards_(), txs_(NULL), trx_count_(0)
  {}
  int init(ObTransService* txs, const common::ObIArray<common::ObPartitionKey>& pkeys)
  {
    int ret = OB_SUCCESS;
    ObTransStateTableGuard* trans_table_guard = NULL;
    if (OB_ISNULL(txs) || pkeys.count() < 2) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkeys));
    } else if (OB_FAIL(trans_table_guards_.prepare_allocate(pkeys.count()))) {
      STORAGE_LOG(WARN, "failed to reserve count", K(ret), K(pkeys));
    } else {
      txs_ = txs;
      for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); i++) {
        const ObPartitionKey& pkey = pkeys.at(i);
        if (OB_FAIL(
                txs->get_part_trans_ctx_mgr().get_partition_trans_ctx_mgr_with_ref(pkey, trans_table_guards_.at(i)))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "failed to get partition trans ctx mgr", K(ret), K(pkey));
        }
      }
    }
    return ret;
  }
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int ret = common::OB_SUCCESS;
    bool bool_ret = false;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(trans_id), "ctx", OB_P(ctx_base));
    } else {
      trx_count_++;
      bool alloc = true;
      ObTransSSTableDurableCtxInfo ctx_info;
      ObTransCtx* ctx = NULL;
      ObPartTransCtx* src_part_ctx = NULL;
      ObPartTransCtx* part_ctx = NULL;

      if (OB_ISNULL(src_part_ctx = dynamic_cast<ObPartTransCtx*>(ctx_base))) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(src_part_ctx->get_trans_sstable_durable_ctx_info(INT64_MAX - 1 /*end_log_ts*/, ctx_info))) {
        TRANS_LOG(WARN, "failed to get trans table status info", K(ret));
      } else {
        for (int i = 0; OB_SUCC(ret) && i < trans_table_guards_.count(); i++) {
          ObPartitionTransCtxMgr* ctx_mgr =
              trans_table_guards_.at(i).get_trans_state_table().get_partition_trans_ctx_mgr();
          if (OB_FAIL(ctx_mgr->get_trans_ctx(trans_id,
                  true,  /*for_replay*/
                  false, /*is_readonly*/
                  false, /*is_staleness_read*/
                  true,  /*need_completed_dirty_txn*/
                  alloc,
                  ctx))) {
            STORAGE_LOG(WARN, "failed to get trans ctx", K(ret), K(trans_id));
          } else if (OB_ISNULL(part_ctx = dynamic_cast<ObPartTransCtx*>(ctx))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "part ctx should not be NULL", K(ret), K(trans_id));
          } else if (part_ctx->is_inited()) {
            continue;
          } else {
            if (OB_FAIL(part_ctx->init(ctx_info.tenant_id_,
                    trans_id,
                    ctx_info.trans_expired_time_,
                    ctx_mgr->get_partition(),
                    &txs_->get_part_trans_ctx_mgr(),
                    ctx_info.trans_param_,
                    GET_MIN_CLUSTER_VERSION(),
                    txs_,
                    ctx_info.cluster_id_,
                    -1,
                    ctx_info.can_elr_))) {
              TRANS_LOG(WARN, "part ctx init error", K(ret), K(*part_ctx));
            } else if (OB_FAIL(part_ctx->recover_from_trans_sstable_durable_ctx_info(ctx_info))) {
              TRANS_LOG(WARN, "recover from trans sstable durable ctx info failed", K(ret), K(*part_ctx));
            }

            if (OB_FAIL(ret)) {
              part_ctx->reset();
            }
          }
          if (NULL != ctx) {
            int tmp_ret = 0;
            if (OB_SUCCESS != (tmp_ret = ctx_mgr->revert_trans_ctx(ctx))) {
              TRANS_LOG(WARN, "failed to revert trans ctx", K(ret), K(trans_id));
            }
            ctx = NULL;
          }
        }
      }
    }
    if (OB_SUCCESS == ret) {
      bool_ret = true;
    }

    return bool_ret;
  }
  int64_t get_trx_count() const
  {
    return trx_count_;
  }

private:
  ObSEArray<ObTransStateTableGuard, 2> trans_table_guards_;
  ObTransService* txs_;
  int64_t trx_count_;
};

class IterateTransLockStatFunctor {
public:
  explicit IterateTransLockStatFunctor(ObTransLockStatIterator& trans_lock_stat_iter)
      : trans_lock_stat_iter_(trans_lock_stat_iter)
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int ret = common::OB_SUCCESS;
    bool bool_ret = false;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id), "ctx", OB_P(ctx_base));
    } else {
      ObMemtableKeyArray memtable_key_arr;
      ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(ctx_base);
      if (OB_ISNULL(part_ctx)) {
        ret = OB_INVALID_ARGUMENT;
        TRANS_LOG(WARN, "part_ctx is null", KR(ret));
      } else if (OB_SUCCESS != (ret = part_ctx->get_memtable_key_arr(memtable_key_arr))) {
        TRANS_LOG(WARN, "get memtable key arr fail", KR(ret), K(memtable_key_arr));
      } else {
        int64_t count = memtable_key_arr.count();
        for (int i = 0; OB_SUCC(ret) && i < count; i++) {
          ObTransLockStat trans_lock_stat;
          if (OB_SUCCESS != (ret = trans_lock_stat.init(part_ctx->get_addr(),
                                 part_ctx->get_tenant_id(),
                                 part_ctx->get_partition(),
                                 memtable_key_arr.at(i),
                                 part_ctx->get_session_id(),
                                 part_ctx->get_proxy_session_id(),
                                 trans_id,
                                 part_ctx->get_ctx_create_time(),
                                 part_ctx->get_trans_expired_time()))) {
            TRANS_LOG(WARN,
                "trans lock stat init fail",
                KR(ret),
                "part_ctx",
                *(part_ctx),
                K(trans_id),
                "memtable key info",
                memtable_key_arr.at(i));
          } else if (OB_SUCCESS != (ret = trans_lock_stat_iter_.push(trans_lock_stat))) {
            TRANS_LOG(WARN, "trans_lock_stat_iter push item fail", KR(ret), K(trans_lock_stat));
          } else {
            // do nothing
          }
        }
      }
    }

    if (common::OB_SUCCESS == ret) {
      bool_ret = true;
    }

    return bool_ret;
  }

private:
  ObTransLockStatIterator& trans_lock_stat_iter_;
};

class IterateTransResultInfoFunctor {
public:
  IterateTransResultInfoFunctor(
      ObTransResultInfoStatIterator& iter, common::ObPartitionKey& partition, common::ObAddr& addr)
      : trans_result_info_stat_iter_(iter), partition_(partition), addr_(addr)
  {}
  ~IterateTransResultInfoFunctor()
  {}
  bool operator()(ObTransResultInfo* info)
  {
    int ret = OB_SUCCESS;
    ObTransResultInfoStat stat;

    if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "iterate trans result info is NULL", KR(ret));
    } else if (OB_FAIL(stat.init(info->get_state(),
                   info->get_commit_version(),
                   info->get_min_log_id(),
                   info->get_trans_id(),
                   partition_,
                   addr_))) {
      TRANS_LOG(WARN, "stat init error", KR(ret), K(*info));
    } else if (OB_FAIL(trans_result_info_stat_iter_.push(stat))) {
      TRANS_LOG(WARN, "trans result info stat push error", KR(ret), K(stat));
    } else {
      // do nothing
    }

    return true;
  }

private:
  ObTransResultInfoStatIterator& trans_result_info_stat_iter_;
  common::ObPartitionKey& partition_;
  common::ObAddr& addr_;
};

class ReplayStartWorkingLogFunctor {
public:
  ReplayStartWorkingLogFunctor(const int64_t timestamp, const uint64_t log_id) : timestamp_(timestamp), log_id_(log_id)
  {}
  ~ReplayStartWorkingLogFunctor()
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int ret = OB_SUCCESS;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      ret = common::OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id), "ctx", OB_P(ctx_base));
    } else if (ctx_base->is_bounded_staleness_read() || ctx_base->is_readonly() || !ctx_base->is_for_replay()) {
      // do nothing
    } else {
      ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(ctx_base);
      if (OB_FAIL(part_ctx->replay_start_working_log(timestamp_, log_id_))) {
        TRANS_LOG(WARN, "replay start working log error", KR(ret), K(*part_ctx));
      }
    }

    return true;
  }

private:
  int64_t timestamp_;
  uint64_t log_id_;
};

class WaitAll1PCTrxEndFunctor {
public:
  WaitAll1PCTrxEndFunctor()
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int tmp_ret = common::OB_SUCCESS;
    bool bool_ret = false;
    bool trx_end = false;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), "ctx", OB_P(ctx_base));
      tmp_ret = common::OB_INVALID_ARGUMENT;
    } else {
      ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);
      if (common::OB_SUCCESS != (tmp_ret = ctx->wait_1pc_trx_end_in_spliting(trx_end))) {
        TRANS_LOG(WARN, "wait 1pc trx end error", "ret", tmp_ret, K(trans_id));
        // If the transaction is not completed, the local wait fails and needs to be retried
      } else if (trx_end) {
        bool_ret = true;
      } else {
        // do nothing
      }
    }

    return bool_ret;
  }
};

class CheckAllTransInTransTableStateFunctor {
public:
  CheckAllTransInTransTableStateFunctor() : all_in_trans_table_state_(true)
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      all_in_trans_table_state_ = false;
      TRANS_LOG(WARN, "invalid argument", K(trans_id), "ctx", OB_P(ctx_base));
    } else {
      ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);
      if (!ctx->is_in_trans_table_state()) {
        all_in_trans_table_state_ = false;
        TRANS_LOG(WARN, "check all in trans table state failed", K(*ctx));
      }
    }

    return true;
  }

  bool is_all_in_trans_table_state() const
  {
    return all_in_trans_table_state_;
  }

private:
  bool all_in_trans_table_state_;
};

class SubmitLogForSplitFunctor {
public:
  SubmitLogForSplitFunctor() : log_finished_(true)
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int tmp_ret = common::OB_SUCCESS;
    bool tmp_log_finished = false;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), "ctx", OB_P(ctx_base));
      log_finished_ = false;
      tmp_ret = common::OB_INVALID_ARGUMENT;
    } else {
      ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);
      if (common::OB_SUCCESS != (tmp_ret = ctx->submit_log_for_split(tmp_log_finished))) {
        log_finished_ = false;
        TRANS_LOG(WARN, "submit_log_for_split error", "ret", tmp_ret, K(trans_id));
        // If the transaction is not completed, the local wait fails and needs to be retried
      } else if (!tmp_log_finished) {
        log_finished_ = false;
      } else {
        // do nothing
      }
    }

    return true;
  }
  bool is_log_finished()
  {
    return log_finished_;
  }

private:
  bool log_finished_;
};

class LeaderRevokeFunctor {
public:
  LeaderRevokeFunctor(
      const bool first_check, ObPartitionTransCtxMgr& ctx_mgr, ObEndTransCallbackArray& cb_array, int64_t& retry_count)
      : first_check_(first_check), ctx_mgr_(ctx_mgr), cb_array_(cb_array), retry_count_(retry_count)
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int tmp_ret = common::OB_SUCCESS;
    bool bool_ret = false;
    bool need_release = false;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), "ctx", OB_P(ctx_base));
      tmp_ret = common::OB_INVALID_ARGUMENT;
    } else if (common::OB_SUCCESS != (tmp_ret = ctx_base->leader_revoke(first_check_, need_release, cb_array_))) {
      if (OB_EAGAIN == tmp_ret || OB_NOT_INIT == tmp_ret) {
        // the transaction has not entered 2pc, and the ctx is not released,
        // the next round of leader_revoke operation needs to be retried and checked
        ++retry_count_;
      }
    } else if (need_release) {
      bool_ret = true;
    } else {
      // do nothing
    }
    if (common::OB_SUCCESS != tmp_ret) {
      TRANS_LOG(WARN, "transaction leader revoke error", "ret", tmp_ret, K(trans_id));
    } else {
      if (!ctx_base->is_bounded_staleness_read() || !ctx_base->is_readonly()) {
        TRANS_LOG(INFO, "transaction leader revoke success", K(trans_id));
      }
    }

    return bool_ret;
  }

private:
  bool first_check_;
  ObPartitionTransCtxMgr& ctx_mgr_;
  ObEndTransCallbackArray& cb_array_;
  int64_t& retry_count_;
};

class PrintFunctor {
public:
  PrintFunctor(const int64_t max_print_count, const bool verbose)
      : max_print_count_(max_print_count), print_count_(0), verbose_(verbose)
  {
    //  TRANS_LOG(INFO, "begin print hashmap item", K(max_print_count));
  }
  ~PrintFunctor()
  {}
  // just print, no need to check parameters
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    bool bool_ret = false;
    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), KP(ctx_base));
    } else if (print_count_++ < max_print_count_) {
      TRANS_LOG(INFO, "hashmap item", K(trans_id), "context", *ctx_base);
      bool_ret = true;
      if (verbose_) {
        ctx_base->print_trace_log();
      }
    } else {
      // do nothing
    }
    return bool_ret;
  }

private:
  int64_t max_print_count_;
  int64_t print_count_;
  bool verbose_;
};

class PrintAllPartitionTransCtxFunctor {
public:
  PrintAllPartitionTransCtxFunctor()
  {}
  ~PrintAllPartitionTransCtxFunctor()
  {}
  bool operator()(const ObPartitionKey& partition, ObPartitionTransCtxMgr* ctx_mgr)
  {
    int tmp_ret = common::OB_SUCCESS;
    bool bool_ret = false;
    const bool verbose = true;

    if (!partition.is_valid() || OB_ISNULL(ctx_mgr)) {
      TRANS_LOG(WARN, "invalid argument", K(partition), KP(ctx_mgr));
      tmp_ret = OB_INVALID_ARGUMENT;
    } else {
      ctx_mgr->print_all_ctx(ObPartitionTransCtxMgr::MAX_HASH_ITEM_PRINT, verbose);
      bool_ret = true;
    }
    UNUSED(tmp_ret);
    return bool_ret;
  }
};

class CheckpointFunctor {
public:
  explicit CheckpointFunctor(storage::ObPartitionLoopWorker* lp_worker, const int64_t start_ckp_ts,
      const int64_t checkpoint_base_version, int64_t& ckp_total_cnt, int64_t& ckp_succ_cnt)
      : worker_(lp_worker),
        start_ckp_ts_(start_ckp_ts),
        checkpoint_base_version_(checkpoint_base_version),
        ckp_total_cnt_(ckp_total_cnt),
        ckp_succ_cnt_(ckp_succ_cnt)
  {}
  ~CheckpointFunctor()
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int tmp_ret = common::OB_SUCCESS;
    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), KP(ctx_base));
      // If you encounter a situation where part_ctx has not been init yet,
      // skip it directly, there will be a background thread retry
    } else if (!ctx_base->is_inited() || start_ckp_ts_ < ctx_base->get_ctx_create_time()) {
      // do nothing
    } else {
      ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);
      bool checkpoint_succ = false;
      int64_t ckp_version = 0;
      int64_t safe_slave_read_timestamp = 0;
      if (NULL == worker_) {
        // Use a fixed timestamp for checkpoint
        ckp_version = checkpoint_base_version_;
        safe_slave_read_timestamp = checkpoint_base_version_;
      } else {
        worker_->get_checkpoint_info(ckp_version, safe_slave_read_timestamp);
      }
      if (OB_SUCCESS != (tmp_ret = ctx->checkpoint(ckp_version, safe_slave_read_timestamp, checkpoint_succ))) {
        TRANS_LOG(WARN, "checkpoint transaction failed", K(tmp_ret), "ctx", *ctx);
      } else if (checkpoint_succ) {
        ckp_succ_cnt_++;
      } else {
        // do nothing
      }
    }
    ckp_total_cnt_++;
    return true;
  }
  int64_t get_checkpoint_total_ctx_count() const
  {
    return ckp_total_cnt_;
  }
  int64_t get_checkpoint_succ_count() const
  {
    return ckp_succ_cnt_;
  }

private:
  storage::ObPartitionLoopWorker* worker_;
  // Record the start time of the checkpoint, those contexts created after this point
  // will be operated in the next round to avoid long-term occupation of the checkpoint thread
  int64_t start_ckp_ts_;
  int64_t checkpoint_base_version_;
  // The total number of ctx traversed
  int64_t& ckp_total_cnt_;
  // The number of ctx successfully checkpointed
  int64_t& ckp_succ_cnt_;
};

class ObTransDataRelocateFunctor {
public:
  explicit ObTransDataRelocateFunctor(memtable::ObIMemtable* memtable) : memtable_(memtable)
  {}
  ~ObTransDataRelocateFunctor()
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    bool bool_ret = false;
    int tmp_ret = common::OB_SUCCESS;
    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), KP(ctx_base));
    } else {
      ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);
      if (OB_SUCCESS != (tmp_ret = ctx->relocate_data(memtable_))) {
        TRANS_LOG(WARN, "relocate data failed", K(tmp_ret), "ctx", *ctx);
      } else {
        bool_ret = true;
      }
    }
    return bool_ret;
  }

private:
  memtable::ObIMemtable* memtable_;
};

class ObXASchedulerHbFunctor {
public:
  ObXASchedulerHbFunctor()
  {}
  ~ObXASchedulerHbFunctor()
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    // always return true so as to traverse all ctx
    int tmp_ret = common::OB_SUCCESS;
    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), KP(ctx_base));
    } else {
      ObScheTransCtx* sche_ctx = static_cast<ObScheTransCtx*>(ctx_base);
      if (sche_ctx->is_xa_local_trans()) {
        if (OB_SUCCESS != (tmp_ret = sche_ctx->xa_scheduler_hb_req())) {
          TRANS_LOG(WARN, "xa scheduler hb failed", K(tmp_ret), K(*sche_ctx));
        }
      }
    }
    return true;
  }
};

class ObPrepareChangingLeaderFunctor {
public:
  explicit ObPrepareChangingLeaderFunctor(const common::ObAddr& proposal_leader, const int64_t round, const int64_t cnt)
      : proposal_leader_(proposal_leader), round_(round), cnt_(cnt)
  {}
  ~ObPrepareChangingLeaderFunctor()
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    bool bool_ret = false;
    int tmp_ret = common::OB_SUCCESS;
    if (!trans_id.is_valid() || OB_ISNULL(ctx_base) || round_ < 1 || cnt_ < 0) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), KP(ctx_base));
    } else {
      ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);
      if (ctx->hash() % round_ != cnt_) {
        bool_ret = true;
      } else if (OB_SUCCESS != (tmp_ret = ctx->prepare_changing_leader(proposal_leader_))) {
        TRANS_LOG(WARN, "prepare changing leader failed", K(tmp_ret), "ctx", *ctx);
      } else {
        bool_ret = true;
      }
    }
    return bool_ret;
  }

private:
  common::ObAddr proposal_leader_;
  int64_t round_;
  int64_t cnt_;
};

class ObRemoveAllCtxFunctor {
public:
  explicit ObRemoveAllCtxFunctor()
  {}
  ~ObRemoveAllCtxFunctor()
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    bool bool_ret = false;
    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), KP(ctx_base));
    } else {
      bool_ret = true;
    }
    return bool_ret;
  }
};

class ObRemoveCallbackFunctor {
public:
  explicit ObRemoveCallbackFunctor(memtable::ObMemtable* mt) : mt_(mt)
  {}
  ~ObRemoveCallbackFunctor()
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    bool bool_ret = true;
    int tmp_ret = OB_SUCCESS;
    ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), KP(ctx_base));
    } else if (OB_ISNULL(mt_) || OB_ISNULL(ctx)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "invalid argument", K(tmp_ret), K(trans_id), KP(ctx_base));
    } else if (OB_SUCCESS != (tmp_ret = ctx->remove_callback_for_uncommited_txn(mt_))) {
      TRANS_LOG(WARN, "remove callback for unncommitted txn failed", K(tmp_ret), K(trans_id), KP(ctx_base));
    }

    if (OB_SUCCESS != tmp_ret) {
      bool_ret = false;
    }

    return bool_ret;
  }

private:
  memtable::ObMemtable* mt_;
};

class ObRemoveMemCtxFunctor {
public:
  explicit ObRemoveMemCtxFunctor(memtable::ObMemtable* mt) : mt_(mt)
  {}
  ~ObRemoveMemCtxFunctor()
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    bool bool_ret = true;
    int tmp_ret = OB_SUCCESS;
    ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), KP(ctx_base));
    } else if (OB_ISNULL(mt_) || OB_ISNULL(ctx)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "invalid argument", K(tmp_ret), K(trans_id), KP(ctx_base));
    } else if (OB_SUCCESS != (tmp_ret = ctx->remove_mem_ctx_for_trans_ctx(mt_))) {
      TRANS_LOG(WARN, "remove mem ctx for trans ctx failed", K(tmp_ret), K(trans_id), KP(ctx_base));
    }

    if (OB_SUCCESS != tmp_ret) {
      bool_ret = false;
    }

    return bool_ret;
  }

private:
  memtable::ObMemtable* mt_;
};

class ObDirtyTransMarkerFunctor {
public:
  explicit ObDirtyTransMarkerFunctor(
      const memtable::ObMemtable* const frozen_memtable, const memtable::ObMemtable* const active_memtable)
      : frozen_memtable_(frozen_memtable), active_memtable_(active_memtable), cb_cnt_(0), applied_log_ts_(INT64_MAX)
  {}

  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int ret = common::OB_SUCCESS;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), "ctx", OB_P(ctx_base));
    } else {
      int64_t cb_cnt = 0;
      ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);
      if (OB_FAIL(ctx->mark_frozen_data(frozen_memtable_, active_memtable_, cb_cnt))) {
        TRANS_LOG(WARN, "mark frozen data failed", K(trans_id));
      } else {
        cb_cnt_ += cb_cnt;
        applied_log_ts_ = MIN(applied_log_ts_, ctx->get_applying_log_ts() - 1);
      }
    }

    return OB_SUCC(ret);
  }

  // Returns the number of newly discovered callbacks
  // that need to be synchronized during this traversal
  int64_t get_pending_cb_cnt() const
  {
    return cb_cnt_;
  }
  // Return the minimum log_id - 1 of the log that the partition is applying asynchronously,
  // and return INT64_MAX if there is no such value
  int64_t get_applied_log_ts() const
  {
    return applied_log_ts_;
  }

private:
  const memtable::ObMemtable* const frozen_memtable_;
  const memtable::ObMemtable* const active_memtable_;
  int64_t cb_cnt_;
  int64_t applied_log_ts_;
};

class ObGetAppliedLogTsFunctor {
public:
  ObGetAppliedLogTsFunctor() : applied_log_ts_(INT64_MAX)
  {}

  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int ret = common::OB_SUCCESS;

    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      TRANS_LOG(WARN, "invalid argument", K(trans_id), "ctx", OB_P(ctx_base));
    } else {
      ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);
      applied_log_ts_ = MIN(applied_log_ts_, ctx->get_applying_log_ts() - 1);
    }

    return OB_SUCC(ret);
  }

  // Return the minimum log_id - 1 of the log that the partition is applying asynchronously,
  // and return INT64_MAX if there is no such value
  int64_t get_applied_log_ts() const
  {
    return applied_log_ts_;
  }

private:
  int64_t applied_log_ts_;
};

class ObGetMaxTransVersionBeforeLogFunctor {
public:
  explicit ObGetMaxTransVersionBeforeLogFunctor(const int64_t log_ts)
      : log_ts_(log_ts), max_trans_version_(0), is_all_rollback_trans_(true), has_related_trans_(false)
  {}

  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int ret = common::OB_SUCCESS;
    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), K(trans_id), "ctx", OB_P(ctx_base));
    } else {
      ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);
      int64_t trans_version = 0;
      bool is_releated_trans = false;
      bool is_rollback_trans = false;
      if (OB_FAIL(
              ctx->check_log_ts_and_get_trans_version(log_ts_, trans_version, is_releated_trans, is_rollback_trans))) {
        TRANS_LOG(WARN, "failed to check_log_ts_and_get_trans_version", K(ret), K(trans_id));
      } else {
        if (trans_version > max_trans_version_) {
          max_trans_version_ = trans_version;
        }
        if (is_releated_trans && !is_rollback_trans) {
          is_all_rollback_trans_ = false;
        }
        if (is_releated_trans) {
          has_related_trans_ = true;
        }
      }
    }
    return OB_SUCC(ret);
  }

  int64_t get_max_trans_version() const
  {
    return max_trans_version_;
  }
  bool is_all_rollback_trans()
  {
    return is_all_rollback_trans_ && has_related_trans_;
  }

private:
  int64_t log_ts_;
  int64_t max_trans_version_;
  bool is_all_rollback_trans_;
  bool has_related_trans_;
};

class ObCleanTransTableFunctor {
public:
  explicit ObCleanTransTableFunctor(const int64_t max_cleanout_log_ts) : max_cleanout_log_ts_(max_cleanout_log_ts)
  {}
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int ret = common::OB_SUCCESS;
    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(trans_id), "ctx", OB_P(ctx_base));
    } else {
      ObTransStatusInfo trans_info;
      ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);
      if (OB_FAIL(ctx->get_trans_state_and_version_without_lock(trans_info))) {
        TRANS_LOG(WARN, "failed to get trans table status info", K(ret));
      } else if ((ObTransTableStatusType::COMMIT == trans_info.status_ ||
                     ObTransTableStatusType::ABORT == trans_info.status_) &&
                 ctx->is_exiting()) {
        if (trans_info.end_log_ts_ <= max_cleanout_log_ts_) {
          if (ctx->is_dirty_trans()) {
            ctx->remove_trans_table();
          }
        }
      }
    }
    return OB_SUCC(ret);
  }

private:
  int64_t max_cleanout_log_ts_;
};

class ObCheckHasTerminatedTrxBeforeLogFunction {
public:
  explicit ObCheckHasTerminatedTrxBeforeLogFunction(const int64_t start_log_ts, const int64_t end_log_ts)
      : start_log_ts_(start_log_ts), end_log_ts_(end_log_ts), has_terminated_trx_(false)
  {}

  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int ret = common::OB_SUCCESS;
    if (!trans_id.is_valid() || OB_ISNULL(ctx_base)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), K(trans_id), "ctx", OB_P(ctx_base));
    } else {
      ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);
      bool is_terminated = false;
      if (OB_FAIL(ctx->check_if_terminated_in_given_log_range(start_log_ts_, end_log_ts_, is_terminated))) {
        TRANS_LOG(WARN, "failed to check_if_terminated_before_given_log_id", K(ret), K_(start_log_ts), K_(end_log_ts));
      } else if (is_terminated) {
        has_terminated_trx_ = true;
        ret = common::OB_ITER_END;
      }
    }
    return OB_SUCC(ret);
  }

  bool has_terminated_trx() const
  {
    return has_terminated_trx_;
  }

private:
  int64_t start_log_ts_;
  int64_t end_log_ts_;
  bool has_terminated_trx_;
};

// For physical backup and restore, after pulling the log,
// use this interface to garbage collect unfinished transactions
class ObClearTransAfterRestoreLog {
public:
  explicit ObClearTransAfterRestoreLog(
      const int64_t restore_snapshot_version, const uint64_t last_restore_log_id, const int64_t fake_terminate_log_ts)
      : ret_(OB_SUCCESS),
        restore_snapshot_version_(restore_snapshot_version),
        last_restore_log_id_(last_restore_log_id),
        fake_terminate_log_ts_(fake_terminate_log_ts)
  {}
  int get_ret() const
  {
    return ret_;
  }
  bool operator()(const ObTransID& trans_id, ObTransCtx* ctx_base)
  {
    int ret = OB_SUCCESS;
    UNUSED(trans_id);
    ObPartTransCtx* ctx = static_cast<ObPartTransCtx*>(ctx_base);
    if (OB_ISNULL(ctx)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR,
          "ctx is NULL",
          K(ret),
          K_(restore_snapshot_version),
          K_(last_restore_log_id),
          K_(fake_terminate_log_ts));
    } else if (OB_FAIL(ctx->clear_trans_after_restore(
                   restore_snapshot_version_, last_restore_log_id_, fake_terminate_log_ts_))) {
      TRANS_LOG(WARN, "failed to clear trans after restore", K(ret), K(*ctx));
    }

    return OB_SUCCESS == ret;
  }

private:
  int ret_;
  int64_t restore_snapshot_version_;
  uint64_t last_restore_log_id_;
  int64_t fake_terminate_log_ts_;  // fake terminate log ts for killing transaction
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_FUNCTOR_
