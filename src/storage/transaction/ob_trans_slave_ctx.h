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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_SLAVE_CTX_
#define OCEANBASE_TRANSACTION_OB_TRANS_SLAVE_CTX_

#include "ob_trans_ctx.h"

namespace oceanbase {

namespace common {
class ObPartitionKey;
class ObAddr;
class ObMaskSet;
}  // namespace common

namespace transaction {
class ObITransCtxMgr;
class ObTransMsg;
}  // namespace transaction

namespace memtable {
class ObIMemtableCtxFactory;
class ObIMemtableCtx;
};  // namespace memtable

namespace storage {
class ObIPartitionGroupGuard;
}

namespace transaction {
// slave participant transaction context
class ObSlaveTransCtx : public ObDistTransCtx {
  friend class IterateTransStatFunctor;

public:
  ObSlaveTransCtx() : ObDistTransCtx("slave_participant", ObTransCtxType::SLAVE_PARTICIPANT), mt_ctx_()
  {
    reset();
  }
  ~ObSlaveTransCtx()
  {
    destroy();
  }
  int init(const uint64_t tenant_id, const ObTransID& trans_id, const int64_t trans_expired_time,
      const common::ObPartitionKey& self, ObITransCtxMgr* ctx_mgr, const ObStartTransParam& trans_param,
      const uint64_t cluster_version, ObTransService* trans_service);
  bool is_inited() const;
  virtual void destroy();
  void reset();
  int construct_context(const ObTransMsg& msg);
  int start_trans();
  int start_task(const ObTransDesc& trans_desc, const int64_t snapshot_version, const int snapshot_gene_type,
      const bool need_update_gts);
  int end_task(const bool is_rollback, const sql::ObPhyPlanType plan_type, const int64_t sql_no);
  int commit(const bool is_rollback, sql::ObIEndTransCallback* cb, bool is_readonly, const MonotonicTs commit_time,
      const int64_t stmt_expired_time, const ObStmtRollbackInfo& stmt_rollback_info, bool& need_convert_to_dist_trans);
  // @fixme
  virtual int commit(const bool, sql::ObIEndTransCallback*, bool, const MonotonicTs, const int64_t,
      const ObStmtRollbackInfo&, const common::ObString&, bool&)
  {
    return common::OB_SUCCESS;
  }
  int handle_message(const ObTransMsg& msg);
  int handle_timeout(const int64_t delay);
  bool can_be_freezed() const
  {
    return true;
  }
  bool is_prepared() const
  {
    return false;
  }
  int kill(const KillTransArg& arg, ObEndTransCallbackArray& cb_array);
  int leader_revoke(const bool first_check, bool& need_release, ObEndTransCallbackArray& cb_array)
  {
    UNUSED(first_check);
    need_release = false;
    UNUSED(cb_array);
    return OB_SUCCESS;
  }
  int leader_takeover(const int64_t checkpoint)
  {
    UNUSED(checkpoint);
    return common::OB_SUCCESS;
  }
  int leader_active(const storage::LeaderActiveArg& arg)
  {
    UNUSED(arg);
    return OB_SUCCESS;
  }
  memtable::ObMemtableCtx* get_memtable_ctx()
  {
    return &mt_ctx_;
  }
  uint64_t get_lock_for_read_retry_count() const
  {
    return mt_ctx_.get_lock_for_read_retry_count();
  }
  int check_scheduler_status();
  uint32_t get_ctx_id() const
  {
    return mt_ctx_.get_ctx_descriptor();
  }

  INHERIT_TO_STRING_KV(
      "ObDistTransCtx", ObDistTransCtx, K_(snapshot_version), K_(stmt_info), K_(session_id), K_(proxy_session_id));

private:
  int trans_end_(const bool commit, const int64_t commit_version);
  int trans_clear_();
  int do_clear_();
  int init_memtable_ctx_(const uint64_t tenant_id);
  int start_stmt_(const int64_t sql_no);
  int start_bounded_staleness_task_(const int64_t sql_no, const int64_t snapshot_version,
      storage::ObIPartitionGroupGuard& pkey_guard, const int snapshot_gene_type);
  int end_stmt_(const bool is_rollback, bool& need_response);
  int post_stmt_response_(const int64_t msg_type, const int64_t sql_no);
  int post_trans_response_(const int64_t msg_type);
  int handle_trans_clear_request_(const ObTransMsg& msg);
  int handle_trans_discard_request_(const ObTransMsg& msg);
  int handle_start_stmt_request_(const ObTransMsg& msg);
  int handle_stmt_rollback_request_(const ObTransMsg& msg);
  int handle_trans_ask_scheduler_status_response_(const ObTransMsg& msg);
  bool need_to_ask_scheduler_status_();
  int check_rs_scheduler_is_alive_(bool& is_alive);
  int force_kill_();
  int post_ask_scheduler_status_msg_();
  DISALLOW_COPY_AND_ASSIGN(ObSlaveTransCtx);

private:
  // 0x0078746374726170 means reset partctx
  static const int64_t PART_CTX_MAGIC_NUM = 0x0078746374726170;
  static const int64_t REPLAY_PRINT_TRACE_THRESHOLD = 10 * 1000;  // 10 ms
  static const int64_t OP_LOCAL_NUM = 16;

private:
  bool is_inited_;
  memtable::ObMemtableCtx mt_ctx_;
  memtable::ObIMemtableCtxFactory* mt_ctx_factory_;
  storage::ObPartitionService* partition_service_;
  int64_t snapshot_version_;
  int64_t commit_task_count_;
  ObTransStmtInfo stmt_info_;
  int64_t last_ask_scheduler_status_ts_;
  int64_t last_ask_scheduler_status_response_ts_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_SLAVE_CTX_
