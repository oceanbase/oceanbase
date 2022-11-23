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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_STAT_
#define OCEANBASE_TRANSACTION_OB_TRANS_STAT_

#include "ob_trans_define.h"
#include "common/ob_range.h"
#include "ob_dup_table.h"

namespace oceanbase
{
namespace transaction
{
class ObTransStat
{
public:
  ObTransStat() { reset(); }
  ~ObTransStat() { }
  void reset();
  int init(const common::ObAddr &addr, const ObTransID &trans_id, const uint64_t tenant_id,
      const bool is_exiting, const bool is_readonly, const bool has_readonly, const bool is_dirty,
      const ObStartTransParam &trans_param, const int64_t ctx_create_time, const int64_t expired,
      const int64_t refer, const int64_t sql_no, const int64_t state, const uint32_t session_id,
      const uint64_t proxy_session_id, const int trans_type, const int64_t part_trans_action,
      const uint64_t lock_for_read_retry_count, const int64_t ctx_addr,
      const ObElrTransInfoArray &prev_trans_arr, const ObElrTransInfoArray &next_trans_arr,
      int32_t prev_trans_commit_count, uint32_t ctx_id, const int64_t pending_log_size,
      const int64_t flushed_log_size);
  common::ObAddr &get_addr() { return addr_; }
  ObTransID &get_trans_id() { return trans_id_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  bool is_exiting() const { return is_exiting_; }
  bool is_readonly() const { return is_readonly_; }
  bool has_decided() const { return has_decided_; }
  bool is_dirty() const { return is_dirty_; }
  common::ObVersion &get_active_memstore_version() { return active_memstore_version_; }
  uint32_t get_session_id() { return session_id_; }
  uint64_t get_proxy_session_id() { return proxy_session_id_; }
  ObStartTransParam &get_trans_param() { return trans_param_; }
  int64_t get_ctx_create_time() const { return ctx_create_time_; }
  int64_t get_trans_expired_time() const { return expired_time_; }
  int64_t get_trans_refer_cnt() const { return refer_; }
  int64_t get_sql_no() const { return sql_no_; }
  int64_t get_state() const { return state_; }
  int get_trans_type() const { return trans_type_; }
  int64_t get_part_trans_action() const { return part_trans_action_; }
  uint64_t get_lock_for_read_retry_count() const { return lock_for_read_retry_count_; }
  int64_t get_ctx_addr() const { return ctx_addr_; }
  ObElrTransInfoArray &get_prev_trans_arr() { return prev_trans_arr_; }
  ObElrTransInfoArray &get_next_trans_arr() { return next_trans_arr_; }
  int32_t get_prev_trans_commit_count() const { return prev_trans_commit_count_; }
  uint32_t get_ctx_id() const { return ctx_id_; }
  int64_t get_pending_log_size() const { return pending_log_size_; }
  int64_t get_flushed_log_size() const { return flushed_log_size_; }

  TO_STRING_KV(K_(addr), K_(trans_id), K_(tenant_id), K_(is_exiting), K_(is_readonly),
      K_(has_decided), K_(is_dirty), K_(active_memstore_version),
      K_(trans_param), K_(ctx_create_time), K_(expired_time), K_(refer),
      K_(sql_no), K_(state), K_(session_id), K_(proxy_session_id), K_(trans_type),
      K_(ctx_addr), K_(prev_trans_arr), K_(next_trans_arr), K_(prev_trans_commit_count), K_(ctx_id),
      K_(pending_log_size), K_(flushed_log_size));
private:
  bool is_inited_;
  common::ObAddr addr_;
  ObTransID trans_id_;
  uint64_t tenant_id_;
  bool is_exiting_;
  bool is_readonly_;
  bool has_decided_;
  bool is_dirty_;
  common::ObVersion active_memstore_version_;
  ObStartTransParam trans_param_;
  int64_t ctx_create_time_;
  int64_t expired_time_;
  int64_t refer_;
  int64_t sql_no_;
  int64_t state_;
  uint32_t session_id_;
  uint64_t proxy_session_id_;
  //SP_TRANS,MINI_SP_TRANS,DIST_TRANS
  int trans_type_;
  int64_t part_trans_action_;
  uint64_t lock_for_read_retry_count_;
  int64_t ctx_addr_;
  ObElrTransInfoArray prev_trans_arr_;
  ObElrTransInfoArray next_trans_arr_;
  // < 0 if has aborted transaction
  int32_t prev_trans_commit_count_;
  uint32_t ctx_id_;
  int64_t pending_log_size_;
  int64_t flushed_log_size_;
};

class ObTransLockStat
{
public:
  ObTransLockStat() { reset(); }
  ~ObTransLockStat() {}
  int init(const common::ObAddr &addr,
            uint64_t tenant_id,
            const ObMemtableKeyInfo &memtable_key,
            uint32_t session_id,
            uint64_t proxy_session_id,
            const ObTransID &trans_id,
            int64_t ctx_create_time,
            int64_t expired_time);
  void reset();
  const common::ObAddr &get_addr() const { return addr_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  const ObMemtableKeyInfo &get_memtable_key() const { return memtable_key_; }
  uint32_t get_session_id() const { return session_id_; }
  uint64_t get_proxy_session_id() const { return proxy_session_id_; }
  const ObTransID &get_trans_id() const { return trans_id_; }
  int64_t get_ctx_create_time() const { return ctx_create_time_; }
  int64_t get_trans_expired_time() const { return expired_time_; }

  TO_STRING_KV(K_(addr),
               K_(tenant_id),
               K_(memtable_key),
               K_(session_id),
               K_(proxy_session_id),
               K_(trans_id),
               K_(ctx_create_time),
               K_(expired_time));

private:
  bool is_inited_;
  common::ObAddr addr_;
  uint64_t tenant_id_;
  ObMemtableKeyInfo memtable_key_;
  uint32_t session_id_;
  uint64_t proxy_session_id_;
  ObTransID trans_id_;
  int64_t ctx_create_time_;
  int64_t expired_time_;
};

class ObDuplicatePartitionStat
{
public:
  ObDuplicatePartitionStat() { reset(); }
  ~ObDuplicatePartitionStat() { destroy(); }
  int init(const common::ObAddr &addr,
           const uint64_t cur_log_id,
           const bool is_master,
           const DupTableLeaseInfoHashMap &hashmap);
  void reset();
  void destroy() { reset(); }
  const common::ObAddr &get_addr() const { return addr_; }
  uint64_t get_cur_log_id() const { return cur_log_id_; }
  bool is_master() const { return is_master_; }
  const ObDupTableLeaseInfoArray &get_lease_list() const { return lease_list_; }
  TO_STRING_KV(K_(addr), K_(cur_log_id), K_(is_master), K_(lease_list));
private:
  common::ObAddr addr_;
  uint64_t cur_log_id_;
  bool is_master_;
  ObDupTableLeaseInfoArray lease_list_;
};

} // transaction
} // oceanbase
#endif // OCEANABAE_TRANSACTION_OB_TRANS_STAT_
