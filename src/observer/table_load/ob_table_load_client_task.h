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
#include "observer/table_load/ob_table_load_exec_ctx.h"
#include "observer/table_load/ob_table_load_instance.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "share/table/ob_table_load_define.h"
#include "share/table/ob_table_load_row_array.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "storage/direct_load/ob_direct_load_struct.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTableCtx;
class ObTableLoadTask;
class ObITableLoadTaskScheduler;
class ObTableLoadInstance;

struct ObTableLoadClientTaskParam
{
public:
  ObTableLoadClientTaskParam();
  ~ObTableLoadClientTaskParam();
  void reset();
  int assign(const ObTableLoadClientTaskParam &other);
  bool is_valid() const;

#define DEFINE_VAR_GETTER_AND_SETTER(type, name)        \
  OB_INLINE type get_##name() const { return name##_; } \
  OB_INLINE void set_##name(type name) { name##_ = name; }

#define DEFINE_STR_GETTER_AND_SETTER(type, name)               \
  OB_INLINE const type &get_##name() const { return name##_; } \
  OB_INLINE int set_##name(const type &name) { return set_string(name, name##_); }

#define DEFINE_STR_ARRAY_GETTER_AND_SETTER(type, name)                   \
  OB_INLINE const ObIArray<type> &get_##name() const { return name##_; } \
  OB_INLINE int set_##name(const ObIArray<type> &name) { return set_string_array(name, name##_); }

  DEFINE_VAR_GETTER_AND_SETTER(ObAddr, client_addr);
  DEFINE_VAR_GETTER_AND_SETTER(uint64_t, task_id);
  DEFINE_VAR_GETTER_AND_SETTER(uint64_t, tenant_id);
  DEFINE_VAR_GETTER_AND_SETTER(uint64_t, user_id);
  DEFINE_VAR_GETTER_AND_SETTER(uint64_t, database_id);
  DEFINE_STR_GETTER_AND_SETTER(ObString, table_name);
  DEFINE_VAR_GETTER_AND_SETTER(int64_t, parallel);
  DEFINE_VAR_GETTER_AND_SETTER(uint64_t, max_error_row_count);
  DEFINE_VAR_GETTER_AND_SETTER(sql::ObLoadDupActionType, dup_action);
  DEFINE_VAR_GETTER_AND_SETTER(uint64_t, timeout_us);
  DEFINE_VAR_GETTER_AND_SETTER(uint64_t, heartbeat_timeout_us);
  DEFINE_STR_GETTER_AND_SETTER(ObString, load_method);
  DEFINE_STR_ARRAY_GETTER_AND_SETTER(ObString, column_names);

#undef DEFINE_VAR_GETTER_AND_SETTER
#undef DEFINE_STR_GETTER_AND_SETTER

  TO_STRING_KV(K_(client_addr),
               K_(task_id),
               K_(tenant_id),
               K_(user_id),
               K_(database_id),
               K_(table_name),
               K_(parallel),
               K_(max_error_row_count),
               K_(dup_action),
               K_(timeout_us),
               K_(heartbeat_timeout_us),
               K_(load_method),
               K_(column_names));

private:
  int set_string(const ObString &src, ObString &dest);
  int set_string_array(const ObIArray<ObString> &src, ObIArray<ObString> &dest);

private:
  ObArenaAllocator allocator_;
  int64_t task_id_;
  ObAddr client_addr_;
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t database_id_;
  ObString table_name_;
  int64_t parallel_;
  uint64_t max_error_row_count_;
  sql::ObLoadDupActionType dup_action_;
  int64_t timeout_us_;
  int64_t heartbeat_timeout_us_;
  ObString load_method_;
  common::ObArray<ObString> column_names_;
};

class ObTableLoadClientTask
{
public:
  ObTableLoadClientTask();
  ~ObTableLoadClientTask();
  int init(const ObTableLoadClientTaskParam &param);
  int start();
  int write(table::ObTableLoadObjRowArray &obj_rows);
  int commit();
  void abort();
  OB_INLINE int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }
  OB_INLINE int64_t inc_ref_count() { return ATOMIC_AAF(&ref_count_, 1); }
  OB_INLINE int64_t dec_ref_count() { return ATOMIC_SAF(&ref_count_, 1); }
  OB_INLINE int64_t get_task_id() const { return param_.get_task_id(); }
  OB_INLINE uint64_t get_table_id() const { return 0; }
  void heart_beat();
  int check_status();

  int set_status_initializing();
  int set_status_waitting();
  int set_status_running();
  int set_status_commit();
  int set_status_error(int error_code);
  void set_status_abort(int error_code = OB_CANCELED);
  table::ObTableLoadClientStatus get_status() const;
  void get_status(table::ObTableLoadClientStatus &client_status, int &error_code) const;
  int check_status(table::ObTableLoadClientStatus client_status);

  OB_INLINE const table::ObTableLoadResultInfo &get_result_info() const { return result_info_; }
  TO_STRING_KV(K_(param),
               KP_(session_info),
               K_(free_session_ctx),
               K_(client_exec_ctx),
               KP_(task_scheduler),
               K_(client_status),
               K_(error_code),
               K_(result_info),
               K_(ref_count));

private:
  int create_session_info();
  int init_exec_ctx();
  int init_task_scheduler();

  int advance_status_nolock(const table::ObTableLoadClientStatus expected,
                            const table::ObTableLoadClientStatus updated);
  int advance_status(const table::ObTableLoadClientStatus expected,
                     const table::ObTableLoadClientStatus updated);

  int init_instance(ObTableLoadParam &load_param, const ObIArray<uint64_t> &column_ids);
  int commit_instance();
  void destroy_instance();

private:
  class ClientTaskExectueProcessor;
  class ClientTaskExectueCallback;

private:
  ObArenaAllocator allocator_;
  ObTableLoadClientTaskParam param_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  sql::ObSQLSessionInfo *session_info_;
  sql::ObFreeSessionCtx free_session_ctx_;
  sql::ObSqlCtx sql_ctx_;
  sql::ObPhysicalPlanCtx plan_ctx_;
  ObExecContext exec_ctx_;
  ObTableLoadClientExecCtx client_exec_ctx_;
  ObITableLoadTaskScheduler *task_scheduler_;
  int64_t session_count_;
  ObTableLoadInstance instance_;
  ObTableLoadInstance::TransCtx trans_ctx_;
  int64_t next_batch_id_ CACHE_ALIGNED;
  mutable obsys::ObRWLock rw_lock_;
  table::ObTableLoadClientStatus client_status_;
  int error_code_;
  table::ObTableLoadResultInfo result_info_;
  int64_t ref_count_ CACHE_ALIGNED;
  bool is_inited_;
};

struct ObTableLoadClientTaskBrief : public common::LinkHashValue<ObTableLoadUniqueKey>
{
public:
  ObTableLoadClientTaskBrief()
    : task_id_(common::OB_INVALID_ID),
      table_id_(common::OB_INVALID_ID),
      client_status_(table::ObTableLoadClientStatus::MAX_STATUS),
      error_code_(common::OB_SUCCESS),
      active_time_(0)
  {
  }
  TO_STRING_KV(K_(task_id), K_(table_id), K_(client_status), K_(error_code), K_(result_info),
               K_(active_time));

public:
  int64_t task_id_;
  uint64_t table_id_;
  table::ObTableLoadClientStatus client_status_;
  int error_code_;
  table::ObTableLoadResultInfo result_info_;
  int64_t active_time_;
};

} // namespace observer
} // namespace oceanbase
