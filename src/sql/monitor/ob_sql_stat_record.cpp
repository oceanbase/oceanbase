/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *         http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL

#include "sql/monitor/ob_sql_stat_record.h"
#include "sql/engine/ob_physical_plan.h"
#include "lib/stat/ob_diagnose_info.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/atomic/ob_atomic.h"
#include "sql/ob_sql_context.h"
#include "lib/allocator/page_arena.h"   // ObArenaAllocator
#include "lib/time/ob_tsc_timestamp.h"
#include "observer/ob_server.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
int ObSqlStatRecordKey::deep_copy(common::ObIAllocator &allocator, const ObILibCacheKey &other)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy(other))) {
    LOG_WARN("failed to deep copy sql stat record key", K(ret));
  }
  return ret;
}

int ObSqlStatRecordKey::deep_copy(const ObILibCacheKey &other)
{
  int ret = OB_SUCCESS;
  const ObSqlStatRecordKey &sql_stat_key = static_cast<const ObSqlStatRecordKey&>(other);
  MEMCPY(sql_id_, sql_stat_key.sql_id_, common::OB_MAX_SQL_ID_LENGTH);
  plan_hash_  = sql_stat_key.plan_hash_;
  namespace_ = sql_stat_key.namespace_;
  source_addr_ = sql_stat_key.source_addr_;
  return ret;
}

uint64_t ObSqlStatRecordKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = source_addr_.hash();
  hash_val = murmurhash(&plan_hash_, sizeof(plan_hash_), hash_val);
  hash_val = common::murmurhash(&sql_id_, common::OB_MAX_SQL_ID_LENGTH, hash_val);
  return hash_val;
}

int ObSqlStatRecordKey::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

bool ObSqlStatRecordKey::is_equal(const ObILibCacheKey &other) const
{
  const ObSqlStatRecordKey &sql_stat_key = static_cast<const ObSqlStatRecordKey&>(other);
  bool cmp_ret = (0 == MEMCMP(sql_id_, sql_stat_key.sql_id_, common::OB_MAX_SQL_ID_LENGTH))
                    && (sql_stat_key.plan_hash_  == plan_hash_)
                    && (sql_stat_key.source_addr_  == source_addr_);
  return cmp_ret;
}

int ObSqlStatInfo::init(
  const ObSqlStatRecordKey& key,
  const ObSQLSessionInfo &session_info,
  const ObString &sql,
  const ObPhysicalPlan *plan)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(key_.deep_copy(key))){
    LOG_WARN("failed to assign sql stat key", K(ret));
  } else {
    sql_type_ = session_info.get_stmt_type();
    sql_cs_type_ = session_info.get_local_collation_connection();
    parsing_db_id_ = session_info.get_database_id();
    parsing_user_id_= session_info.get_user_id() < 0 ? 0: session_info.get_user_id();
    ObString db_name = session_info.get_database_name();
    if (!db_name.empty()) {
      ObString::obstr_size_t size = MIN(static_cast<ObString::obstr_size_t>(STRLEN(db_name.ptr()) + 1), OB_MAX_DATABASE_NAME_LENGTH);
      MEMCPY(parsing_db_name_, db_name.ptr(), size);
      parsing_db_name_[size] = '\0';
    }

    if (OB_ISNULL(plan)) {
      if (!sql.empty()) {
        sql.to_string(query_sql_, OB_MAX_SQL_STAT_QUERY_SQL_LEN);
        int32_t size = MIN(sql.length(), OB_MAX_SQL_STAT_QUERY_SQL_LEN);
        query_sql_[size] = '\0';
      }
    } else {
      plan_id_ = plan->get_plan_id();
      plan_type_ = plan->get_plan_type();
      first_load_time_ = plan->stat_.gen_time_;
      ObString src_stmt;
      if (plan->need_param()) {
        src_stmt = plan->stat_.stmt_;
      } else {
        src_stmt = plan->stat_.raw_sql_;
      }
      if (!src_stmt.empty()) {
        src_stmt.to_string(query_sql_, OB_MAX_SQL_STAT_QUERY_SQL_LEN);
        int32_t size = MIN(src_stmt.length(), OB_MAX_SQL_STAT_QUERY_SQL_LEN);
        query_sql_[size] = '\0';
      }
    }
  }
  return ret;
}

void ObSqlStatInfo::reset()
{
  key_.reset();
  plan_id_ = 0;
  plan_type_ = 0;
  parsing_db_id_ = 0;
  query_sql_[0] = '\0';
  query_sql_[OB_MAX_SQL_STAT_QUERY_SQL_LEN] = '\0';
  sql_cs_type_ = common::CS_TYPE_INVALID;
  sql_type_ = 0;
  parsing_db_name_[0] = '\0';
  parsing_db_name_[OB_MAX_DATABASE_NAME_LENGTH] = '\0';
  first_load_time_ = 0;
}

int ObSqlStatInfo::assign(const ObSqlStatInfo& other)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator no_use;
  if (OB_FAIL(key_.deep_copy(no_use , other.get_key()))){
    LOG_WARN("failed to assign sql stat key", K(ret));
  } else {
    plan_id_ = other.get_plan_id();
    plan_type_ = other.get_plan_type();
    parsing_db_id_ = other.get_parsing_db_id();
    parsing_user_id_ = other.get_parsing_user_id();
    MEMCPY(query_sql_, other.get_query_sql(), static_cast<ObString::obstr_size_t>(STRLEN(other.get_query_sql()) + 1));
    MEMCPY(parsing_db_name_, other.get_parsing_db_name(), static_cast<ObString::obstr_size_t>(STRLEN(other.get_parsing_db_name()) + 1));
    sql_cs_type_ = other.get_sql_cs_type();
    sql_type_ = other.get_sql_type();
    first_load_time_ = other.get_first_load_time();
  }
  return ret;
}

ObExecutingSqlStatRecord::ObExecutingSqlStatRecord()
{
  is_in_retry_ = false;
  is_route_miss_ = false;
  is_plan_cache_hit_ = false;
#define DEF_SQL_STAT_ITEM_INIT(def_name)           \
  def_name##_start_ = 0;                           \
  def_name##_end_ = 0;
  DEF_SQL_STAT_ITEM_INIT(disk_reads);
  DEF_SQL_STAT_ITEM_INIT(buffer_gets);
  DEF_SQL_STAT_ITEM_INIT(elapsed_time);
  DEF_SQL_STAT_ITEM_INIT(cpu_time);
  DEF_SQL_STAT_ITEM_INIT(ccwait);
  DEF_SQL_STAT_ITEM_INIT(userio_wait);
  DEF_SQL_STAT_ITEM_INIT(apwait);
  DEF_SQL_STAT_ITEM_INIT(physical_read_requests);
  DEF_SQL_STAT_ITEM_INIT(physical_read_bytes);
  DEF_SQL_STAT_ITEM_INIT(write_throttle);
  DEF_SQL_STAT_ITEM_INIT(rows_processed);
  DEF_SQL_STAT_ITEM_INIT(memstore_read_rows);
  DEF_SQL_STAT_ITEM_INIT(minor_ssstore_read_rows);
  DEF_SQL_STAT_ITEM_INIT(major_ssstore_read_rows);
  DEF_SQL_STAT_ITEM_INIT(rpc);
  DEF_SQL_STAT_ITEM_INIT(fetches);
  DEF_SQL_STAT_ITEM_INIT(partition);
  DEF_SQL_STAT_ITEM_INIT(nested_sql);
#undef DEF_SQL_STAT_ITEM_INIT
}

#define RECORD_ITEM(se, di)                                                                        \
  do {                                                                                             \
    elapsed_time_##se##_ = static_cast<int64_t>(                                                   \
      static_cast<double>(rdtsc()) / OBSERVER_FREQUENCE.get_cpu_frequency_khz() * 1000);           \
    if (OB_NOT_NULL(di)) {                                                                         \
      ObStatEventAddStatArray &arr = di->get_add_stat_stats();                                     \
      disk_reads_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::IO_READ_COUNT);                     \
      buffer_gets_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::ROW_CACHE_HIT) * 2 +               \
                            EVENT_STAT_GET(arr, ObStatEventIds::FUSE_ROW_CACHE_HIT) * 2 +          \
                            EVENT_STAT_GET(arr, ObStatEventIds::BLOOM_FILTER_FILTS) * 2 +          \
                            EVENT_STAT_GET(arr, ObStatEventIds::BLOCK_CACHE_HIT) +                 \
                            disk_reads_##se##_;                                                    \
      cpu_time_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::SYS_TIME_MODEL_DB_CPU);               \
      ccwait_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::CCWAIT_TIME);                           \
      userio_wait_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::USER_IO_WAIT_TIME);                \
      apwait_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::APWAIT_TIME);                           \
      physical_read_requests_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::IO_READ_COUNT);         \
      physical_read_bytes_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::IO_READ_BYTES);            \
      write_throttle_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::STORAGE_WRITING_THROTTLE_TIME); \
      memstore_read_rows_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::MEMSTORE_READ_ROW_COUNT);   \
      minor_ssstore_read_rows_##se##_ =                                                            \
          EVENT_STAT_GET(arr, ObStatEventIds::MINOR_SSSTORE_READ_ROW_COUNT);                       \
      major_ssstore_read_rows_##se##_ =                                                            \
          EVENT_STAT_GET(arr, ObStatEventIds::MAJOR_SSSTORE_READ_ROW_COUNT);                       \
      rpc_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::RPC_PACKET_OUT);                           \
    }                                                                                              \
  } while (0);

void ObExecutingSqlStatRecord::reset()
{
  is_in_retry_ = false;
  is_route_miss_ = false;
  is_plan_cache_hit_ = false;
#define DEF_SQL_STAT_ITEM_INIT(def_name)           \
  def_name##_start_ = 0;                             \
  def_name##_end_ = 0;
  DEF_SQL_STAT_ITEM_INIT(disk_reads);
  DEF_SQL_STAT_ITEM_INIT(buffer_gets);
  DEF_SQL_STAT_ITEM_INIT(elapsed_time);
  DEF_SQL_STAT_ITEM_INIT(cpu_time);
  DEF_SQL_STAT_ITEM_INIT(ccwait);
  DEF_SQL_STAT_ITEM_INIT(userio_wait);
  DEF_SQL_STAT_ITEM_INIT(apwait);
  DEF_SQL_STAT_ITEM_INIT(physical_read_requests);
  DEF_SQL_STAT_ITEM_INIT(physical_read_bytes);
  DEF_SQL_STAT_ITEM_INIT(write_throttle);
  DEF_SQL_STAT_ITEM_INIT(rows_processed);
  DEF_SQL_STAT_ITEM_INIT(memstore_read_rows);
  DEF_SQL_STAT_ITEM_INIT(minor_ssstore_read_rows);
  DEF_SQL_STAT_ITEM_INIT(major_ssstore_read_rows);
  DEF_SQL_STAT_ITEM_INIT(rpc);
  DEF_SQL_STAT_ITEM_INIT(fetches);
  DEF_SQL_STAT_ITEM_INIT(partition);
  DEF_SQL_STAT_ITEM_INIT(nested_sql);
#undef DEF_SQL_STAT_ITEM_INIT
}

int ObExecutingSqlStatRecord::assign(const ObExecutingSqlStatRecord& other)
{
  is_in_retry_ = other.get_is_in_retry();
  is_route_miss_ = other.is_route_miss();
  is_plan_cache_hit_ = other.is_plan_cache_hit();
#define DEF_SQL_STAT_ITEM_COPY(def_name)           \
  def_name##_start_ = other.def_name##_start_;     \
  def_name##_end_ = other.def_name##_end_;
  DEF_SQL_STAT_ITEM_COPY(disk_reads);
  DEF_SQL_STAT_ITEM_COPY(buffer_gets);
  DEF_SQL_STAT_ITEM_COPY(elapsed_time);
  DEF_SQL_STAT_ITEM_COPY(cpu_time);
  DEF_SQL_STAT_ITEM_COPY(ccwait);
  DEF_SQL_STAT_ITEM_COPY(userio_wait);
  DEF_SQL_STAT_ITEM_COPY(apwait);
  DEF_SQL_STAT_ITEM_COPY(physical_read_requests);
  DEF_SQL_STAT_ITEM_COPY(physical_read_bytes);
  DEF_SQL_STAT_ITEM_COPY(write_throttle);
  DEF_SQL_STAT_ITEM_COPY(rows_processed);
  DEF_SQL_STAT_ITEM_COPY(memstore_read_rows);
  DEF_SQL_STAT_ITEM_COPY(minor_ssstore_read_rows);
  DEF_SQL_STAT_ITEM_COPY(major_ssstore_read_rows);
  DEF_SQL_STAT_ITEM_COPY(rpc);
  DEF_SQL_STAT_ITEM_COPY(fetches);
  DEF_SQL_STAT_ITEM_COPY(partition);
  DEF_SQL_STAT_ITEM_COPY(nested_sql);
#undef DEF_SQL_STAT_ITEM_COPY
  return OB_SUCCESS;
}

int ObExecutingSqlStatRecord::record_sqlstat_start_value()
{

  ObDiagnosticInfo *di_info = ObLocalDiagnosticInfo::get();
  RECORD_ITEM(start, di_info);
  return OB_SUCCESS;
}

int ObExecutingSqlStatRecord::record_sqlstat_end_value(ObDiagnoseSessionInfo* di /*= nullptr*/)
{
  if (OB_NOT_NULL(di)) {
    RECORD_ITEM(end, di);
  } else {
    ObDiagnosticInfo *di_info = ObLocalDiagnosticInfo::get();
    RECORD_ITEM(end, di_info);
  }
  return OB_SUCCESS;
}

#undef RECORD_ITEM

int ObExecutingSqlStatRecord::move_to_sqlstat_cache(
  ObSQLSessionInfo &session_info,
  ObString &cur_sql,
  ObPhysicalPlan *plan /*= nullptr*/,
  const bool is_px_remote_exec /*= false*/)
{
  int ret = OB_SUCCESS;
  // 1. init key
  ObSqlStatRecordKey key;
  session_info.get_cur_sql_id(key.sql_id_, sizeof(key.sql_id_));
  key.set_plan_hash(plan== nullptr? session_info.get_current_plan_hash(): plan->get_plan_hash_value());
  if (is_px_remote_exec) {
    key.set_source_addr(session_info.get_peer_addr());
  }
  LOG_DEBUG("view sqlstat cache key and query_sql", K(ret), K(key), K(cur_sql));

  if (key.is_valid() && OB_NOT_NULL(plan)) {
    ObExecutedSqlStatRecord *sql_stat_value = const_cast<ObExecutedSqlStatRecord *>(&(plan->sql_stat_record_value_));
    if (OB_ISNULL(sql_stat_value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_stat_value is NULL", KR(ret));
    } else {
      if (!sql_stat_value->get_key().is_valid()) {
        if (OB_FAIL(sql_stat_value->get_sql_stat_info().init(key, session_info, cur_sql, plan))) {
          LOG_WARN("failed to init sql stat info", K(ret));
        }
      }

      if (OB_SUCC(ret) && OB_FAIL(sql_stat_value->sum_stat_value(*this))) {
        LOG_WARN("sql_stat_value sum value failed", KR(ret));
      }
    }
  } else {
    LOG_WARN("the key is not valid which at plan cache mgr", KR(ret));
  }
  return ret;
}

int ObExecutedSqlStatRecord::copy_sql_stat_info(const ObSqlStatInfo& sql_stat_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql_stat_info_.assign(sql_stat_info))) {
    LOG_WARN("failed to assign sql stat info",K(ret), K(sql_stat_info));
  }
  return ret;
}

int ObExecutedSqlStatRecord::sum_stat_value(ObExecutingSqlStatRecord& executing_record)
{
  int ret = OB_SUCCESS;
  (void)ATOMIC_AAF(&executions_total_, executing_record.get_is_in_retry()? 0:1 );
  (void)ATOMIC_AAF(&retry_total_, executing_record.get_is_in_retry()? 1:0 );

  (void)ATOMIC_AAF(&disk_reads_total_, executing_record.get_disk_reads_delta());
  (void)ATOMIC_AAF(&buffer_gets_total_, executing_record.get_buffer_gets_delta());
  (void)ATOMIC_AAF(&elapsed_time_total_, executing_record.get_elapsed_time_delta());
  (void)ATOMIC_AAF(&cpu_time_total_, executing_record.get_cpu_time_delta());
  (void)ATOMIC_AAF(&ccwait_total_, executing_record.get_ccwait_delta());
  (void)ATOMIC_AAF(&userio_wait_total_, executing_record.get_userio_wait_delta());
  (void)ATOMIC_AAF(&apwait_total_, executing_record.get_apwait_delta());
  (void)ATOMIC_AAF(&physical_read_requests_total_, executing_record.get_physical_read_requests_delta());
  (void)ATOMIC_AAF(&physical_read_bytes_total_, executing_record.get_physical_read_bytes_delta());
  (void)ATOMIC_AAF(&write_throttle_total_, executing_record.get_write_throttle_delta());
  (void)ATOMIC_AAF(&rows_processed_total_, executing_record.get_rows_processed_delta());
  (void)ATOMIC_AAF(&memstore_read_rows_total_, executing_record.get_memstore_read_rows_delta());
  (void)ATOMIC_AAF(&minor_ssstore_read_rows_total_, executing_record.get_minor_ssstore_read_rows_delta());
  (void)ATOMIC_AAF(&major_ssstore_read_rows_total_, executing_record.get_major_ssstore_read_rows_delta());
  (void)ATOMIC_AAF(&rpc_total_, executing_record.get_rpc_delta());
  (void)ATOMIC_AAF(&fetches_total_, executing_record.get_fetches_delta());
  (void)ATOMIC_AAF(&partition_total_, executing_record.get_partition_delta());
  (void)ATOMIC_AAF(&nested_sql_total_, executing_record.get_nested_sql_delta());
  (void)ATOMIC_AAF(&route_miss_total_, executing_record.is_route_miss()? 1:0);
  (void)ATOMIC_AAF(&plan_cache_hit_total_, executing_record.is_plan_cache_hit());
  return OB_SUCCESS;
}

int ObExecutedSqlStatRecord::sum_stat_value(ObExecutedSqlStatRecord& executed_record)
{
  int ret = OB_SUCCESS;
  (void)ATOMIC_AAF(&executions_total_, executed_record.get_executions_total() );
  (void)ATOMIC_AAF(&retry_total_, executed_record.get_retry_total() );

  (void)ATOMIC_AAF(&disk_reads_total_, executed_record.get_disk_reads_total());
  (void)ATOMIC_AAF(&buffer_gets_total_, executed_record.get_buffer_gets_total());
  (void)ATOMIC_AAF(&elapsed_time_total_, executed_record.get_elapsed_time_total());
  (void)ATOMIC_AAF(&cpu_time_total_, executed_record.get_cpu_time_total());
  (void)ATOMIC_AAF(&ccwait_total_, executed_record.get_ccwait_total());
  (void)ATOMIC_AAF(&userio_wait_total_, executed_record.get_userio_wait_total());
  (void)ATOMIC_AAF(&apwait_total_, executed_record.get_apwait_total());
  (void)ATOMIC_AAF(&physical_read_requests_total_, executed_record.get_physical_read_requests_total());
  (void)ATOMIC_AAF(&physical_read_bytes_total_, executed_record.get_physical_read_bytes_total());
  (void)ATOMIC_AAF(&write_throttle_total_, executed_record.get_write_throttle_total());
  (void)ATOMIC_AAF(&rows_processed_total_, executed_record.get_rows_processed_total());
  (void)ATOMIC_AAF(&memstore_read_rows_total_, executed_record.get_memstore_read_rows_total());
  (void)ATOMIC_AAF(&minor_ssstore_read_rows_total_, executed_record.get_minor_ssstore_read_rows_total());
  (void)ATOMIC_AAF(&major_ssstore_read_rows_total_, executed_record.get_major_ssstore_read_rows_total());
  (void)ATOMIC_AAF(&rpc_total_, executed_record.get_rpc_total());
  (void)ATOMIC_AAF(&fetches_total_, executed_record.get_fetches_total());
  (void)ATOMIC_AAF(&partition_total_, executed_record.get_partition_total());
  (void)ATOMIC_AAF(&nested_sql_total_, executed_record.get_nested_sql_total());
  (void)ATOMIC_AAF(&route_miss_total_, executed_record.get_route_miss_total());
  return OB_SUCCESS;
}
int ObExecutedSqlStatRecord::assign(const ObExecutedSqlStatRecord& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql_stat_info_.assign(other.get_sql_stat_info()))){
    LOG_WARN("failed to assign sql stat info", K(ret));
  } else {
#define DEF_ASSIGN_FUNC(def_name)                                           \
    def_name##_total_ = other.get_##def_name##_total();                     \
    def_name##_last_snap_ = other.get_##def_name##_last_snap();
    DEF_ASSIGN_FUNC(executions);
    DEF_ASSIGN_FUNC(disk_reads);
    DEF_ASSIGN_FUNC(buffer_gets);
    DEF_ASSIGN_FUNC(elapsed_time);
    DEF_ASSIGN_FUNC(cpu_time);
    DEF_ASSIGN_FUNC(ccwait);
    DEF_ASSIGN_FUNC(userio_wait);
    DEF_ASSIGN_FUNC(apwait);
    DEF_ASSIGN_FUNC(physical_read_requests);
    DEF_ASSIGN_FUNC(physical_read_bytes);
    DEF_ASSIGN_FUNC(write_throttle);
    DEF_ASSIGN_FUNC(rows_processed);
    DEF_ASSIGN_FUNC(memstore_read_rows);
    DEF_ASSIGN_FUNC(minor_ssstore_read_rows);
    DEF_ASSIGN_FUNC(major_ssstore_read_rows);
    DEF_ASSIGN_FUNC(rpc);
    DEF_ASSIGN_FUNC(fetches);
    DEF_ASSIGN_FUNC(retry);
    DEF_ASSIGN_FUNC(partition);
    DEF_ASSIGN_FUNC(nested_sql);
    DEF_ASSIGN_FUNC(route_miss);
    DEF_ASSIGN_FUNC(plan_cache_hit);
#undef DEF_ASSIGN_FUNC
  }
  return ret;
}

int ObExecutedSqlStatRecord::reset()
{
  int ret = OB_SUCCESS;
  sql_stat_info_.reset();
#define DEF_RESET_FUNC(def_name)             \
    def_name##_total_ = 0;                   \
    def_name##_last_snap_ = 0;
    DEF_RESET_FUNC(executions);
    DEF_RESET_FUNC(disk_reads);
    DEF_RESET_FUNC(buffer_gets);
    DEF_RESET_FUNC(elapsed_time);
    DEF_RESET_FUNC(cpu_time);
    DEF_RESET_FUNC(ccwait);
    DEF_RESET_FUNC(userio_wait);
    DEF_RESET_FUNC(apwait);
    DEF_RESET_FUNC(physical_read_requests);
    DEF_RESET_FUNC(physical_read_bytes);
    DEF_RESET_FUNC(write_throttle);
    DEF_RESET_FUNC(rows_processed);
    DEF_RESET_FUNC(memstore_read_rows);
    DEF_RESET_FUNC(minor_ssstore_read_rows);
    DEF_RESET_FUNC(major_ssstore_read_rows);
    DEF_RESET_FUNC(rpc);
    DEF_RESET_FUNC(fetches);
    DEF_RESET_FUNC(retry);
    DEF_RESET_FUNC(partition);
    DEF_RESET_FUNC(nested_sql);
    DEF_RESET_FUNC(route_miss);
    DEF_RESET_FUNC(plan_cache_hit);
#undef DEF_RESET_FUNC

  return ret;
}

int ObExecutedSqlStatRecord::update_last_snap_record_value()
{
  int ret = OB_SUCCESS;
  #define DEF_UPDATE_LAST_SNAP_FUNC(def_name)                                           \
    def_name##_last_snap_ = def_name##_total_;
    DEF_UPDATE_LAST_SNAP_FUNC(executions);
    DEF_UPDATE_LAST_SNAP_FUNC(disk_reads);
    DEF_UPDATE_LAST_SNAP_FUNC(buffer_gets);
    DEF_UPDATE_LAST_SNAP_FUNC(elapsed_time);
    DEF_UPDATE_LAST_SNAP_FUNC(cpu_time);
    DEF_UPDATE_LAST_SNAP_FUNC(ccwait);
    DEF_UPDATE_LAST_SNAP_FUNC(userio_wait);
    DEF_UPDATE_LAST_SNAP_FUNC(apwait);
    DEF_UPDATE_LAST_SNAP_FUNC(physical_read_requests);
    DEF_UPDATE_LAST_SNAP_FUNC(physical_read_bytes);
    DEF_UPDATE_LAST_SNAP_FUNC(write_throttle);
    DEF_UPDATE_LAST_SNAP_FUNC(rows_processed);
    DEF_UPDATE_LAST_SNAP_FUNC(memstore_read_rows);
    DEF_UPDATE_LAST_SNAP_FUNC(minor_ssstore_read_rows);
    DEF_UPDATE_LAST_SNAP_FUNC(major_ssstore_read_rows);
    DEF_UPDATE_LAST_SNAP_FUNC(rpc);
    DEF_UPDATE_LAST_SNAP_FUNC(fetches);
    DEF_UPDATE_LAST_SNAP_FUNC(retry);
    DEF_UPDATE_LAST_SNAP_FUNC(partition);
    DEF_UPDATE_LAST_SNAP_FUNC(nested_sql);
    DEF_UPDATE_LAST_SNAP_FUNC(route_miss);
    DEF_UPDATE_LAST_SNAP_FUNC(plan_cache_hit);
  #undef DEF_UPDATE_LAST_SNAP_FUNC
  return ret;
}
/*************************************************************************************************/

int ObSqlStatRecordNode::inner_get_cache_obj(ObILibCacheCtx &ctx,
                                             ObILibCacheKey *key,
                                             ObILibCacheObject *&cache_obj)
{
  UNUSED(ctx);
  UNUSED(key);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_obj_)) {
    ret = OB_SQL_PC_NOT_EXIST;
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    cache_obj = cache_obj_;
  }
  return ret;
}

int ObSqlStatRecordNode::inner_add_cache_obj(ObILibCacheCtx &ctx,
                                             ObILibCacheKey *key,
                                             ObILibCacheObject *cache_obj)
{
  UNUSED(ctx);
  UNUSED(key);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache obj is null", K(ret));
  } else {
    cache_obj_ = cache_obj;
  }
  return ret;
}

int ObSqlStatRecordUtil::get_cache_obj(ObSqlStatRecordKey &key, ObCacheObjGuard& guard)
{
  int ret = OB_SUCCESS;
  sql::ObILibCacheCtx cache_ctx;
  ObPlanCache* lib_cache = nullptr;
  observer::ObReqTimeGuard req_timeinfo_guard;
  if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get multi tenant from GCTX", K(ret));
  } else if (OB_ISNULL(lib_cache = MTL(ObPlanCache*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get plan cache", K(ret));
  } else if (OB_FAIL(lib_cache->get_cache_obj(cache_ctx, &key, guard))) {
    if (ret == OB_SQL_PC_NOT_EXIST) {
      LOG_INFO("sql stat record not found",K(ret), K(key));
    } else {
      LOG_WARN("fail to get cache obj", K(ret), K(key));
    }
  }
  return ret;
}

int ObSqlStatRecordUtil::create_cache_obj(ObSqlStatRecordKey &key, ObCacheObjGuard& guard)
{
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  sql::ObILibCacheCtx cache_ctx;
  ObPlanCache* lib_cache = nullptr;
  ObSqlStatRecordObj *cache_obj = nullptr;
  observer::ObReqTimeGuard req_timeinfo_guard;
  if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get multi tenant from GCTX", K(ret));
  } else if (OB_ISNULL(lib_cache = MTL(ObPlanCache*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get plan cache", K(ret));
  } else if (OB_FAIL(ObCacheObjectFactory::alloc(guard,
                                                 ObLibCacheNameSpace::NS_SQLSTAT,
                                                 lib_cache->get_tenant_id()))) {
    LOG_WARN("fail to alloc new cache obj", K(ret));
  } else if (OB_ISNULL(cache_obj = static_cast<ObSqlStatRecordObj *>(guard.get_cache_obj()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get cache obj", K(ret));
  } else if (OB_FAIL(lib_cache->add_cache_obj(cache_ctx, &key, cache_obj))) {
    LOG_WARN( "fail to add cache obj to lib cache", K(ret), K(key));
  }
  return ret;
}
} // end of namespace sql
} // end of namespace oceanbase
