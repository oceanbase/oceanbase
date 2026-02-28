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

#ifndef OCEANBASE_ROUTINE_LOAD_TABLE_OPERATOR_H_
#define OCEANBASE_ROUTINE_LOAD_TABLE_OPERATOR_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
class ObIAllocator;

namespace sqlclient
{
class ObMySQLResult;
}
}

namespace share
{
class ObDMLSqlSplicer;
}

namespace storage
{

enum class ObRoutineLoadJobState : int64_t
{
  RUNNING = 0,
  PAUSED,
  STOPPED,
  CANCELLED,
  MAX,
};

struct ObRLoadDynamicFields {
  ObString progress_;
  ObString lag_;
  ObString tmp_progress_;
  ObString tmp_lag_;
  TO_STRING_KV(K_(progress), K_(lag),
               K_(tmp_progress), K_(tmp_lag));
};

struct ObRoutineLoadJob {
  ObRoutineLoadJob() : tenant_id_(OB_INVALID_TENANT_ID),
                       job_id_(OB_INVALID_ID),
                       job_name_(),
                       create_time_(OB_INVALID_TIMESTAMP),
                       pause_time_(OB_INVALID_TIMESTAMP),
                       end_time_(OB_INVALID_TIMESTAMP),
                       database_id_(OB_INVALID_ID),
                       table_id_(OB_INVALID_ID),
                       state_(ObRoutineLoadJobState::MAX),
                       job_properties_(),
                       dynamic_fields_(),
                       err_infos_(),
                       trace_id_(),
                       ret_code_(OB_SUCCESS) {}
  ~ObRoutineLoadJob() {}
  int init(const uint64_t tenant_id,
           const int64_t job_id,
           const ObString &job_name,
           const int64_t create_time,
           const int64_t pause_time,
           const int64_t end_time,
           const uint64_t database_id,
           const uint64_t table_id,
           const ObRoutineLoadJobState state,
           const ObString &job_properties,
           const ObRLoadDynamicFields &dynamic_fields,
           const ObString &err_infos,
           const common::ObCurTraceId::TraceId &trace_id,
           const int ret_code);
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(job_id), K_(job_name),
               K_(create_time), K_(pause_time),
               K_(end_time), K_(database_id),
               K_(table_id), K_(state),
               K_(job_properties), K_(dynamic_fields),
               K_(err_infos), K_(trace_id), K_(ret_code));
public:
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_job_id() const { return job_id_; }
  const ObString &get_job_name() const { return job_name_; }
  int64_t get_create_time() const { return create_time_; }
  int64_t get_pause_time() const { return pause_time_; }
  int64_t get_end_time() const { return end_time_; }
  uint64_t get_database_id() const { return database_id_; }
  uint64_t get_table_id() const { return table_id_; }
  ObRoutineLoadJobState get_state() const { return state_; }
  const ObString &get_job_properties() const { return job_properties_; }
  const ObRLoadDynamicFields &get_dynamic_fields() const { return dynamic_fields_; }
  const ObString &get_err_infos() const { return err_infos_; }
  const common::ObCurTraceId::TraceId &get_trace_id() const { return trace_id_; }
  int get_ret_code() const { return ret_code_; }

private:
  uint64_t tenant_id_;
  int64_t job_id_;
  ObString job_name_;
  int64_t create_time_;
  int64_t pause_time_;
  int64_t end_time_;
  uint64_t database_id_;
  uint64_t table_id_;
  ObRoutineLoadJobState state_;
  ObString job_properties_;
  ObRLoadDynamicFields dynamic_fields_;
  ObString err_infos_;
  common::ObCurTraceId::TraceId trace_id_;
  int ret_code_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRoutineLoadJob);
};

class ObRoutineLoadTableOperator
{
public:
  ObRoutineLoadTableOperator();
  bool is_inited() const { return is_inited_; }
public:
  ObRoutineLoadJobState str_to_job_state(const ObString &status_str);
  const char* job_state_to_str(const ObRoutineLoadJobState &status);
public:
  int init(const uint64_t user_tenant_id, ObISQLClient *proxy);
  int insert_routine_load_job(const ObRoutineLoadJob &job);
  int get_dynamic_fields(const int64_t job_id,
                         const bool need_lock,
                         common::ObIAllocator &allocator,
                         ObRLoadDynamicFields &dynamic_fields);
  int update_offsets_from_tmp_results(const int64_t job_id,
                                      const ObRLoadDynamicFields &dynamic_fields,
                                      const ObRLoadDynamicFields &origin_dynamic_fields);
  int update_tmp_results(const int64_t job_id,
                         const ObString &tmp_progress,
                         const ObString &tmp_lag);
  int reset_tmp_results(const int64_t job_id);
  int update_info_when_failed(const int64_t job_id, const int ret_code);
  int update_trace_id(const int64_t job_id, const common::ObCurTraceId::TraceId &trace_id);
  int update_status(const int64_t job_id,
                    const ObRoutineLoadJobState &old_status,
                    const ObRoutineLoadJobState &new_status);
  int update_pause_time(const int64_t job_id, const int64_t pause_time);
  int update_job_name_and_stop_time(const int64_t job_id, const ObString &job_name, const int64_t end_time);
  int get_status(const ObString &job_name, ObRoutineLoadJobState &status);
  int get_job_id_by_name(const ObString &job_name, int64_t &job_id);
  int update_error_msg(const int64_t job_id, const ObString &error_msg);

private:
  int build_insert_dml_(const ObRoutineLoadJob &job,
                        share::ObDMLSqlSplicer &dml);
private:
  static const char* JOB_STATE_ARRAY[];
  static constexpr const char* INVALID_STR = "invalid";
private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObISQLClient *proxy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRoutineLoadTableOperator);
};

} // end namespace share
} // end namespace oceanbase
#endif
