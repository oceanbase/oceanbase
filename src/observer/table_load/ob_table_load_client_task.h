/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/hash/ob_link_hashmap.h"
#include "observer/table_load/ob_table_load_object_allocator.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "share/table/ob_table_load_define.h"
#include "sql/session/ob_sql_session_mgr.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadClientExecCtx;
class ObTableLoadTableCtx;
class ObTableLoadTask;
class ObITableLoadTaskScheduler;

class ObTableLoadClientTask
{
public:
  ObTableLoadClientTask();
  ~ObTableLoadClientTask();
  int init(uint64_t tenant_id, uint64_t user_id, uint64_t database_id, uint64_t table_id,
           int64_t timeout_us, int64_t heartbeat_timeout_us);
  bool is_inited() const { return is_inited_; }
  int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }
  int64_t inc_ref_count() { return ATOMIC_AAF(&ref_count_, 1); }
  int64_t dec_ref_count() { return ATOMIC_SAF(&ref_count_, 1); }
  int set_table_ctx(ObTableLoadTableCtx *table_ctx);
  int get_table_ctx(ObTableLoadTableCtx *&table_ctx);
  OB_INLINE sql::ObSQLSessionInfo *get_session_info() { return session_info_; }
  OB_INLINE ObTableLoadClientExecCtx *get_exec_ctx() { return exec_ctx_; }
  int add_trans_id(const table::ObTableLoadTransId &trans_id);
  int get_next_trans_id(table::ObTableLoadTransId &trans_id);
  int64_t get_next_batch_id() { return ATOMIC_FAA(&next_batch_id_, 1); }
  OB_INLINE const common::ObIArray<table::ObTableLoadTransId> &get_trans_ids() const
  {
    return trans_ids_;
  }
  int set_status_running();
  int set_status_committing();
  int set_status_commit();
  int set_status_error(int error_code);
  void set_status_abort();
  table::ObTableLoadClientStatus get_status() const;
  void get_status(table::ObTableLoadClientStatus &client_status, int &error_code) const;
  int check_status(table::ObTableLoadClientStatus client_status);
  int alloc_task(ObTableLoadTask *&task);
  void free_task(ObTableLoadTask *task);
  int add_task(ObTableLoadTask *task);
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(database_id), K_(table_id), K_(ddl_param),
               K_(column_names), K_(column_idxs), K_(result_info), KP_(session_info),
               K_(free_session_ctx), KP_(exec_ctx), KP_(task_scheduler), K_(trans_ids),
               K_(next_trans_idx), KP_(table_ctx), K_(client_status), K_(error_code),
               K_(ref_count));
private:
  int create_session_info(uint64_t user_id, uint64_t database_id, uint64_t table_id,
                                 sql::ObSQLSessionInfo *&session_info,
                                 sql::ObFreeSessionCtx &free_session_ctx);
  int init_column_names_and_idxs();
  int init_exec_ctx(int64_t timeout_us, int64_t heartbeat_timeout_us);
public:
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t database_id_;
  uint64_t table_id_;
  ObTableLoadDDLParam ddl_param_;
  common::ObArray<ObString> column_names_;
  common::ObArray<int64_t> column_idxs_;
  table::ObTableLoadResultInfo result_info_;
private:
  ObArenaAllocator allocator_;
  sql::ObSQLSessionInfo *session_info_;
  sql::ObFreeSessionCtx free_session_ctx_;
  ObTableLoadClientExecCtx *exec_ctx_;
  ObTableLoadObjectAllocator<ObTableLoadTask> task_allocator_;
  ObITableLoadTaskScheduler *task_scheduler_;
  common::ObArray<table::ObTableLoadTransId> trans_ids_;
  int64_t next_trans_idx_;
  int64_t next_batch_id_ CACHE_ALIGNED;
  mutable obsys::ObRWLock rw_lock_;
  ObTableLoadTableCtx *table_ctx_;
  table::ObTableLoadClientStatus client_status_;
  int error_code_;
  int64_t ref_count_ CACHE_ALIGNED;
  bool is_inited_;
};

struct ObTableLoadClientTaskBrief : public common::LinkHashValue<ObTableLoadUniqueKey>
{
public:
  ObTableLoadClientTaskBrief()
    : table_id_(common::OB_INVALID_ID),
      dest_table_id_(common::OB_INVALID_ID),
      task_id_(0),
      client_status_(table::ObTableLoadClientStatus::MAX_STATUS),
      error_code_(common::OB_SUCCESS),
      active_time_(0)
  {
  }
  TO_STRING_KV(K_(table_id), K_(dest_table_id), K_(task_id), K_(client_status), K_(error_code),
               K_(active_time));
public:
  uint64_t table_id_;
  uint64_t dest_table_id_;
  int64_t task_id_;
  table::ObTableLoadClientStatus client_status_;
  int error_code_;
  int64_t active_time_;
};

} // namespace observer
} // namespace oceanbase
