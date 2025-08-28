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

#ifndef OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_COLLECTOR_H_
#define OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_COLLECTOR_H_
#include "share/wr/ob_wr_snapshot_rpc_processor.h"
#include "sql/plan_cache/ob_i_lib_cache_object.h"

namespace oceanbase
{
namespace share
{

enum class ObWrSnapshotStatus : int64_t;

struct ObWrSysstat
{
  ObWrSysstat() : svr_ip_("\0"), svr_port_(0), stat_id_(0), value_(0)
  {}

public:
  TO_STRING_KV(K_(svr_ip), K_(svr_port), K_(stat_id), K_(value));
  char svr_ip_[OB_IP_STR_BUFF];
  int64_t svr_port_;
  int64_t stat_id_;
  int64_t value_;
};

struct ObWrResMgrSysstat
{
  ObWrResMgrSysstat() : svr_ip_("\0"), svr_port_(0), group_id_(0), stat_id_(0), value_(0)
  {}

public:
  TO_STRING_KV(K_(svr_ip), K_(svr_port), K_(group_id), K_(stat_id), K_(value));
  char svr_ip_[OB_IP_STR_BUFF];
  int64_t svr_port_;
  int64_t group_id_;
  int64_t stat_id_;
  int64_t value_;
};

struct ObWrSystemEvent
{
public:
  ObWrSystemEvent()
      : svr_ip_("\0"),
        svr_port_(0),
        event_id_(0),
        total_waits_(0),
        total_timeouts_(0),
        time_waited_micro_(0)
  {}
  TO_STRING_KV(K_(svr_ip), K_(svr_port), K_(event_id), K_(total_waits), K_(total_timeouts),
      K_(time_waited_micro));
  char svr_ip_[OB_IP_STR_BUFF];
  int64_t svr_port_;
  int64_t event_id_;
  int64_t total_waits_;
  int64_t total_timeouts_;
  int64_t time_waited_micro_;
};

struct ObWrAsh
{
public:
  ObWrAsh() {
    svr_ip_[0] = '\0';
    svr_port_ = 0;
    sample_id_ = 0;
    session_id_ = 0;
    sample_time_ = 0;
    user_id_ = 0;
    session_type_ = 0;
    sql_id_[0] = '\0';
    trace_id_[0] = '\0';
    event_no_ = -1;
    time_waited_ = -1;
    p1_ = -1;
    p2_ = -1;
    p3_ = -1;
    sql_plan_line_id_ = -1;
    group_id_ = 0;
    time_model_ = 0;
    module_[0] = '\0';
    action_[0] = '\0';
    client_id_[0] = '\0';
    backtrace_[0] = '\0';
    plan_id_ = 0;
    program_[0] = '\0';
    tm_delta_time_ = -1;
    tm_delta_cpu_time_ = -1;
    tm_delta_db_time_ = -1;
    plan_hash_ = 0;
    thread_id_ = -1;
    stmt_type_ = -1;
    tablet_id_ = -1;
    blocking_session_id_ = -1;
    proxy_sid_ = 0;
    delta_read_io_requests_ = 0;
    delta_read_io_bytes_ = 0;
    delta_write_io_requests_ = 0;
    delta_write_io_bytes_ = 0;
  };

  TO_STRING_KV(K_(svr_ip), K_(svr_port), K_(sample_id), K_(session_id), K_(sample_time),
      K_(user_id), K_(session_type), K_(sql_id), K_(trace_id), K_(event_no), K_(event_id),
      K_(time_waited), K_(p1), K_(p2), K_(p3), K_(sql_plan_line_id), K_(group_id), K_(time_model),
      K_(module), K_(action), K_(client_id), K_(plan_id), K_(top_level_sql_id),
      K_(plsql_entry_object_id), K_(plsql_entry_subprogram_id), K_(plsql_entry_subprogram_name),
      K_(plsql_object_id), K_(plsql_subprogram_id), K_(plsql_subprogram_name), K_(plan_hash),
      K_(thread_id), K_(stmt_type), K_(tablet_id), K_(blocking_session_id), K_(proxy_sid), 
      K_(delta_read_io_requests), K_(delta_read_io_bytes), K_(delta_write_io_requests), K_(delta_write_io_bytes));
  char svr_ip_[OB_IP_STR_BUFF];
  int64_t svr_port_;
  int64_t sample_id_;
  int64_t session_id_;
  int64_t sample_time_;
  int64_t user_id_;
  bool session_type_;
  char sql_id_[OB_MAX_SQL_ID_LENGTH + 1];  // + 1 for '/0'
  char trace_id_[OB_MAX_TRACE_ID_BUFFER_SIZE + 1];
  int64_t event_no_;
  int64_t event_id_;
  int64_t time_waited_;
  int64_t p1_;
  int64_t p2_;
  int64_t p3_;
  int64_t sql_plan_line_id_;
  int64_t group_id_;
  uint64_t time_model_;
  char module_[ASH_MODULE_STR_LEN + 1];
  char action_[ASH_ACTION_STR_LEN + 1];
  char client_id_[ASH_CLIENT_ID_STR_LEN + 1];
  char backtrace_[ASH_BACKTRACE_STR_LEN + 1];
  int64_t plan_id_;
  char program_[ASH_PROGRAM_STR_LEN + 1];
  int64_t tm_delta_time_;
  int64_t tm_delta_cpu_time_;
  int64_t tm_delta_db_time_;
  char top_level_sql_id_[OB_MAX_SQL_ID_LENGTH + 1];  // + 1 for '/0'
  int64_t plsql_entry_object_id_;
  int64_t plsql_entry_subprogram_id_;
  char plsql_entry_subprogram_name_[common::OB_MAX_ASH_PL_NAME_LENGTH + 1];
  int64_t plsql_object_id_;
  int64_t plsql_subprogram_id_;
  char plsql_subprogram_name_[common::OB_MAX_ASH_PL_NAME_LENGTH + 1];
  uint64_t plan_hash_;
  int64_t thread_id_;
  int64_t stmt_type_;
  int64_t tx_id_;
  int64_t tablet_id_;
  int64_t blocking_session_id_;
  int64_t proxy_sid_;
  int64_t delta_read_io_requests_;
  int64_t delta_read_io_bytes_;
  int64_t delta_write_io_requests_;
  int64_t delta_write_io_bytes_;
};


struct ObWrSqlStat
{
public:
  ObWrSqlStat() {
    svr_ip_[0] = '\0';
    svr_port_ = 0;
    sql_id_[0] = '\0';
    plan_hash_ = 0;
    plan_id_ = 0;
    plan_type_ = 0;
    module_[0] = '\0';
    action_[0] = '\0';
    parsing_db_id_ = 0;
    parsing_db_name_[0] = '\0';
    parsing_user_id_ = 0;
    executions_total_ = 0;
    executions_delta_ = 0;
    disk_reads_total_ = 0;
    disk_reads_delta_ = 0;
    buffer_gets_total_ = 0;
    buffer_gets_delta_ = 0;
    elapsed_time_total_ = 0;
    elapsed_time_delta_ = 0;
    cpu_time_total_ = 0;
    cpu_time_delta_ = 0;
    ccwait_total_ = 0;
    ccwait_delta_ = 0;
    userio_wait_total_ = 0;
    userio_wait_delta_ = 0;
    apwait_total_ = 0;
    apwait_delta_ = 0;
    physical_read_requests_total_ = 0;
    physical_read_requests_delta_ = 0;
    physical_read_bytes_total_ = 0;
    physical_read_bytes_delta_ = 0;
    write_throttle_total_ = 0;
    write_throttle_delta_ = 0;
    rows_processed_total_ = 0;
    rows_processed_delta_ = 0;
    memstore_read_rows_total_ = 0;
    memstore_read_rows_delta_ = 0;
    minor_ssstore_read_rows_total_ = 0;
    minor_ssstore_read_rows_delta_ = 0;
    major_ssstore_read_rows_total_ = 0;
    major_ssstore_read_rows_delta_ = 0;
    rpc_total_ = 0;
    rpc_delta_ = 0;
    fetches_total_ = 0;
    fetches_delta_ = 0;
    retry_total_ = 0;
    retry_delta_ = 0;
    partition_total_ = 0;
    partition_delta_ = 0;
    nested_sql_total_ = 0;
    nested_sql_delta_ = 0;
    source_ip_[0] = '\0';
    source_ip_[46] = '\0';
    source_port_ = 0;
    route_miss_total_ = 0;
    route_miss_delta_ = 0;
    first_load_time_ = 0;
    plan_cache_hit_total_ = 0;
    plan_cache_hit_delta_ = 0;
  };

  TO_STRING_KV(K_(svr_ip), K_(svr_port), K_(sql_id), K_(plan_hash), K_(plan_id));
  char svr_ip_[OB_IP_STR_BUFF];
  int64_t svr_port_;
  char sql_id_[OB_MAX_SQL_ID_LENGTH + 1];  // + 1 for '/0'
  uint64_t plan_hash_;
  int64_t plan_id_;
  int64_t plan_type_;
  char module_[64 + 1];
  char action_[64 + 1];
  int64_t parsing_db_id_;
  char parsing_db_name_[OB_MAX_DATABASE_NAME_LENGTH+1];
  int64_t parsing_user_id_;
  int64_t executions_total_;
  int64_t executions_delta_;
  int64_t disk_reads_total_;
  int64_t disk_reads_delta_;
  int64_t buffer_gets_total_;
  int64_t buffer_gets_delta_;
  int64_t elapsed_time_total_;
  int64_t elapsed_time_delta_;
  int64_t cpu_time_total_;
  int64_t cpu_time_delta_;
  int64_t ccwait_total_;
  int64_t ccwait_delta_;
  int64_t userio_wait_total_;
  int64_t userio_wait_delta_;
  int64_t apwait_total_;
  int64_t apwait_delta_;
  int64_t physical_read_requests_total_;
  int64_t physical_read_requests_delta_;
  int64_t physical_read_bytes_total_;
  int64_t physical_read_bytes_delta_;
  int64_t write_throttle_total_;
  int64_t write_throttle_delta_;
  int64_t rows_processed_total_;
  int64_t rows_processed_delta_;
  int64_t memstore_read_rows_total_;
  int64_t memstore_read_rows_delta_;
  int64_t minor_ssstore_read_rows_total_;
  int64_t minor_ssstore_read_rows_delta_;
  int64_t major_ssstore_read_rows_total_;
  int64_t major_ssstore_read_rows_delta_;
  int64_t rpc_total_;
  int64_t rpc_delta_;
  int64_t fetches_total_;
  int64_t fetches_delta_;
  int64_t retry_total_;
  int64_t retry_delta_;
  int64_t partition_total_;
  int64_t partition_delta_;
  int64_t nested_sql_total_;
  int64_t nested_sql_delta_;
  char source_ip_[46+1];
  int64_t source_port_;
  int64_t route_miss_total_;
  int64_t route_miss_delta_;
  int64_t first_load_time_;
  int64_t plan_cache_hit_total_;
  int64_t plan_cache_hit_delta_;
};


struct ObWrSqlText
{
public:
  ObWrSqlText() {
    sql_id_[0] = '\0';
    sql_id_[OB_MAX_SQL_ID_LENGTH] = '\0';
    query_sql_[0] = '\0';
    query_sql_[1024] = '\0';
    sql_type_ = 0;
  }
  TO_STRING_KV(K_(sql_type), K_(sql_id));
  char sql_id_[OB_MAX_SQL_ID_LENGTH + 1];  // + 1 for '/0'
  char query_sql_[1024+1];
  int64_t sql_type_;
};

struct ObWrSqlPlan
{
public:
  ObWrSqlPlan() {
    svr_ip_[0] = '\0';
    svr_ip_[OB_IP_STR_BUFF] = '\0';
    svr_port_ = 0;
    sql_id_[0] = '\0';
    sql_id_[OB_MAX_SQL_ID_LENGTH] = '\0';
    plan_hash_ = 0;
    plan_id_ = 0;
    id_ = 0;
    db_id_ = 0;
    gmt_create_= 0;
    operator_[0] = '\0';
    operator_[255] = '\0';
    option_[0] = '\0';
    option_[255] = '\0';
    object_node_[0] = '\0';
    object_node_[40] = '\0';
    object_id_ = 0;
    object_owner_[0] = '\0';
    object_owner_[128] = '\0';
    object_name_[0] = '\0';
    object_name_[128] = '\0';
    object_alias_[0] = '\0';
    object_alias_[261] = '\0';
    object_type_[0] = '\0';
    object_type_[20] = '\0';
    optimizer_[0] = '\0';
    optimizer_[4000] = '\0';
    parent_id_ = 0;
    depth_ = 0;
    position_ = 0;
    is_last_child_ = 0;
    cost_ = 0;
    real_cost_ = 0;
    cardinality_ = 0;
    real_cardinality_ = 0;
    bytes_ = 0;
    rowset_ = 0;
    other_tag_[0] = '\0';
    other_tag_[4000] = '\0';
    partition_start_[0] = '\0';
    partition_start_[4000] = '\0';
    other_[0] = '\0';
    other_[4000] = '\0';
    cpu_cost_ = 0;
    io_cost_ = 0;
    access_predicates_[0] = '\0';
    access_predicates_[4000] = '\0';
    filter_predicates_[0] = '\0';
    filter_predicates_[4000] = '\0';
    startup_predicates_[0] = '\0';
    startup_predicates_[4000] = '\0';
    projection_[0] = '\0';
    projection_[4000] = '\0';
    special_predicates_[0] = '\0';
    special_predicates_[4000] = '\0';
    qblock_name_[0] = '\0';
    qblock_name_[128] = '\0';
    remarks_[0] = '\0';
    remarks_[4000] = '\0';
    other_xml_[0] = '\0';
    other_xml_[4000] = '\0';
  }
  TO_STRING_KV(K_(sql_id), K_(plan_id), K_(id));
  char svr_ip_[OB_IP_STR_BUFF+1];
  int64_t svr_port_;
  char sql_id_[OB_MAX_SQL_ID_LENGTH + 1];  // + 1 for '/0'
  uint64_t plan_hash_;
  int64_t plan_id_;
  int64_t id_;
  int64_t db_id_;
  int64_t gmt_create_;
  char operator_[255 + 1];
  char option_[255+1];
  char object_node_[40 + 1];
  int64_t object_id_;
  char object_owner_[128+1];
  char object_name_[128+1];
  char object_alias_[261+1];
  char object_type_[20+1];
  char optimizer_[4000 + 1];
  int64_t parent_id_;
  int64_t depth_;
  int64_t position_;
  int64_t is_last_child_;
  int64_t cost_;
  int64_t real_cost_;
  int64_t cardinality_;
  int64_t real_cardinality_;
  int64_t bytes_;
  int64_t rowset_;
  char other_tag_[4000 + 1];
  char partition_start_[4000+1];
  char other_[4000+1];
  int64_t cpu_cost_;
  int64_t io_cost_;
  char access_predicates_[4000 + 1];
  char filter_predicates_[4000 + 1];
  char startup_predicates_[4000 + 1];
  char projection_[4000 + 1];
  char special_predicates_[4000 + 1];
  char qblock_name_[128 + 1];
  char remarks_[4000 + 1];
  char other_xml_[4000 + 1];
};

class ObWrCollector
{
public:
  explicit ObWrCollector(int64_t snap_id, int64_t snapshot_begin_time, int64_t snapshot_end_time,
      int64_t snapshot_timeout_ts);
  ~ObWrCollector() = default;
  DISABLE_COPY_ASSIGN(ObWrCollector);
  int init();
  int collect();
  int collect_ash();
  static int exec_read_sql_with_retry(common::ObCommonSqlProxy::ReadResult &res, const uint64_t tenant_id, const char *sql);
  static int exec_write_sql_with_retry(const uint64_t tenant_id, const char *sql, int64_t &affected_rows);
  template <typename T>
  static int exec_sql_with_retry(const uint64_t tenant_id, const char * sql, T function);
  static bool is_can_retry(const int err);
  TO_STRING_KV(K_(snap_id), K_(snapshot_begin_time), K_(snapshot_end_time), K_(timeout_ts));

private:
  int collect_sysstat();
  int collect_statname();
  int collect_eventname();
  int collect_system_event();
  int collect_sqlstat();
  int update_sqlstat();
  int collect_sqltext();
  int collect_sql_plan();
  int collect_res_mgr_sysstat();
  int write_to_wr(ObDMLSqlSplicer &dml_splicer, const char *table_name, int64_t tenant_id, bool ignore_error=false);
  int write_to_wr_sql_plan_and_aux(ObDMLSqlSplicer &dml_splicer, ObDMLSqlSplicer &dml_splicer_aux,
      const char *table_name, const char *table_name_aux, int64_t tenant_id, bool ignore_error = false);
  int fetch_snapshot_id_sequence_curval(int64_t &snap_id);
  int get_cur_snapshot_id_for_ahead_snapshot(int64_t &snap_id);
  int get_begin_interval_time(int64_t &begin_interval_time);
  int update_last_snapshot_end_time();

  int check_if_ignore_errorcode(int error_code);
  int64_t snap_id_;
  int64_t snapshot_begin_time_;
  int64_t snapshot_end_time_;
  int64_t timeout_ts_;
  bool snapshot_ahead_;
};

class ObWrDeleter
{
public:
  ObWrDeleter(const ObWrPurgeSnapshotArg &purge_arg) : purge_arg_(purge_arg)
  {}
  ~ObWrDeleter() = default;
  DISABLE_COPY_ASSIGN(ObWrDeleter);
  int do_delete();
  TO_STRING_KV(K_(purge_arg));

private:
  // delete expired data from wr tables
  //
  // @table_name [in] the name of the table whose data needs to be deleted
  // @tenant_id [in] the id of tenant
  // @cluster_id [in] the id of clusted
  // @snap_id [in] the id of snapshot
  // @return the error code.
  int delete_expired_data_from_wr_table(const char *const table_name, const uint64_t tenant_id,
      const int64_t cluster_id, const int64_t snap_id, const int64_t query_timeout);
  int delete_expired_data_from_wr_sql_plan_and_aux(const char *const table_name, const char *const table_name_aux, const uint64_t tenant_id,
      const int64_t cluster_id, const int64_t snap_id, const int64_t query_timeout);
  int modify_snapshot_status(const uint64_t tenant_id, const int64_t cluster_id,
    const int64_t snap_id, const int64_t query_timeout, const ObWrSnapshotStatus status);
  const ObWrPurgeSnapshotArg &purge_arg_;
};

typedef common::hash::ObHashMap<sql::ObSqlStatRecordKey, sql::ObExecutedSqlStatRecord*> TmpSqlStatMap;
struct ObUpdateSqlStatOp
{
public:
  ObUpdateSqlStatOp() {}
  void reset() {}
  int operator()(common::hash::HashMapPair<sql::ObCacheObjID, sql::ObILibCacheObject *> &entry);
};

}  // namespace share
}  // namespace oceanbase
#endif  // OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_COLLECTOR_H_
