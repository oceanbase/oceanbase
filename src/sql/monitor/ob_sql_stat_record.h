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
#ifndef _OB_SQL_STAT_RECORD_H_
#define _OB_SQL_STAT_RECORD_H_
#include "sql/ob_sql_context.h"
#include "sql/plan_cache/ob_i_lib_cache_key.h"
#include "sql/plan_cache/ob_i_lib_cache_node.h"
#include "sql/plan_cache/ob_i_lib_cache_object.h"
#include "sql/plan_cache/ob_i_lib_cache_context.h"
#include "lib/utility/utility.h"
#include "sql/plan_cache/ob_lib_cache_register.h"
#include "lib/atomic/ob_atomic.h"

#define OB_MAX_SQL_STAT_QUERY_SQL_LEN 1024
namespace oceanbase
{
namespace sql
{

struct ObSqlStatRecordKey : public sql::ObILibCacheKey
{
public:
  ObSqlStatRecordKey()
    : sql::ObILibCacheKey(ObLibCacheNameSpace::NS_SQLSTAT),
      plan_hash_(0),
      source_addr_()
  {
    sql_id_[0] = '\0';
    sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
  }

  void reset()
  {
    namespace_ = ObLibCacheNameSpace::NS_SQLSTAT;
    sql_id_[0] = '\0';
    sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
    plan_hash_ = 0;
    source_addr_.reset();
  }
  virtual int deep_copy(common::ObIAllocator &allocator, const sql::ObILibCacheKey &other);
  int deep_copy(const sql::ObILibCacheKey &other);
  virtual uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  virtual bool is_equal(const sql::ObILibCacheKey &other) const;
  inline bool is_valid() const { return sql_id_[0] != '\0'; }
  void set_sql_id(const ObString& sql_id) { sql_id.to_string(sql_id_, OB_MAX_SQL_ID_LENGTH); } 
  void set_plan_hash(const uint64_t plan_hash_val) { ATOMIC_STORE(&plan_hash_,plan_hash_val); }
  void set_source_addr(const ObAddr &source_addr) { source_addr_ = source_addr; }
  TO_STRING_KV(K_(sql_id), K_(plan_hash), K_(source_addr));
public:
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  uint64_t plan_hash_;
  common::ObAddr source_addr_;
};

class ObSqlStatInfo
{
public:
  ObSqlStatInfo(): key_(), plan_id_(0), plan_type_(0), parsing_db_id_(0)
  {
    query_sql_[0] = '\0';
    query_sql_[OB_MAX_SQL_STAT_QUERY_SQL_LEN] = '\0';
    sql_cs_type_ = common::CS_TYPE_INVALID;
    sql_type_ = 0;
    parsing_db_name_[0] = '\0';
    parsing_db_name_[OB_MAX_DATABASE_NAME_LENGTH] = '\0';
    first_load_time_ = 0;
  }
  ~ObSqlStatInfo() = default;
  int init(const ObSqlStatRecordKey& key,
           const ObSQLSessionInfo &session_info,
           const ObString &sql,
           const ObPhysicalPlan *plan);
  void reset();
  int assign(const ObSqlStatInfo& other);
  const ObSqlStatRecordKey& get_key() const {return key_;}
  ObSqlStatRecordKey& get_key() {return key_;}
  void set_key(ObSqlStatRecordKey& key) { key_.deep_copy(key); }
  int64_t get_plan_id() const { return plan_id_; }
  int64_t get_plan_type() const { return plan_type_;}
  const char* get_query_sql() const {return query_sql_; }
  int64_t get_sql_type() const { return sql_type_;}
  ObCollationType get_sql_cs_type() const {return sql_cs_type_; }
  int64_t get_parsing_db_id() const { return parsing_db_id_; }
  const char* get_parsing_db_name() const { return parsing_db_name_; }
  int64_t get_parsing_user_id() const { return parsing_user_id_; }
  int64_t get_first_load_time() const { return first_load_time_; }
  void set_first_load_time(const int64_t first_load_time) { first_load_time_ = first_load_time; }
  TO_STRING_KV(K_(key), K_(plan_id), K_(plan_type), K_(query_sql), K_(sql_type), K_(parsing_db_id), K_(parsing_user_id));
private:
  ObSqlStatRecordKey key_;
  int64_t plan_id_;
  int64_t plan_type_;
  char query_sql_[OB_MAX_SQL_STAT_QUERY_SQL_LEN+1];
  common::ObCollationType sql_cs_type_;
  int64_t sql_type_;
  int64_t parsing_db_id_;
  char parsing_db_name_[OB_MAX_DATABASE_NAME_LENGTH+1];
  int64_t parsing_user_id_;
  int64_t first_load_time_;
};

struct ObSqlCtx;
class ObExecutingSqlStatRecord
{
public:
  ObExecutingSqlStatRecord();
  ~ObExecutingSqlStatRecord() = default;
  void reset();
  int assign(const ObExecutingSqlStatRecord& other);
  int record_sqlstat_start_value();
  /// WARN: current sression's di address can be changed by time. So please always using
  /// get_local_diagnose_info() to get latest di paramter.
  int record_sqlstat_end_value(ObDiagnoseSessionInfo* di = nullptr);
  // WARNNIGN!!! 
  // It is forbidden to use the cur_plan_ pointer on sql_ctx_, 
  // which can be modified and risks CORE. It is only safe to use the result_set pointer.
  int move_to_sqlstat_cache(ObSQLSessionInfo &session_info,
                            ObString &cur_sql,
                            const ObPhysicalPlan *plan = nullptr,
                            const bool is_px_remote_exec = false);

  bool get_is_in_retry() const { return is_in_retry_; }
  void set_is_in_retry(const bool is_in_retry) { is_in_retry_ = is_in_retry; }
  void set_rows_processed(int64_t rows_processed) { rows_processed_end_ = rows_processed; } 
  void inc_fetch_cnt() { fetches_end_ ++; }
  void set_partition_cnt(int64_t partition_cnt) { partition_end_ = partition_cnt; } 
  bool is_route_miss() const { return is_route_miss_; }
  void set_is_route_miss(const bool is_route_miss) { is_route_miss_ = is_route_miss; }
  bool is_plan_cache_hit() const { return is_plan_cache_hit_;}
  void set_is_plan_cache_hit(const bool is_plan_cache_hit) { is_plan_cache_hit_ = is_plan_cache_hit; }
  bool is_muti_query() const { return is_muti_query_; }
  void set_is_muti_query(const bool is_muti_query) { is_muti_query_ = is_muti_query; }
  bool is_muti_query_batch() const { return is_muti_query_batch_; }
  void set_is_muti_query_batch(const bool is_muti_query_batch) { is_muti_query_batch_ = is_muti_query_batch; }
  bool is_full_table_scan() const { return is_full_table_scan_; }
  void set_is_full_table_scan(const bool is_full_table_scan) { is_full_table_scan_ = is_full_table_scan; }
  bool is_failed() const { return is_failed_; }
  void set_is_failed(const bool failed) { is_failed_ = failed; }
#define DEF_SQL_STAT_ITEM_DELTA_FUNC(def_name)                 \
    int64_t get_##def_name##_delta() const {                   \
      int64_t delta = def_name##_end_ - def_name##_start_;     \
      return delta>0? delta: 0;                                \
    }
    DEF_SQL_STAT_ITEM_DELTA_FUNC(disk_reads);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(buffer_gets);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(elapsed_time);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(cpu_time);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(ccwait);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(userio_wait);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(apwait);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(physical_read_requests);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(physical_read_bytes);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(write_throttle);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(rows_processed);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(memstore_read_rows);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(minor_ssstore_read_rows);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(major_ssstore_read_rows);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(rpc);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(fetches);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(partition);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(nested_sql);
#undef DEF_SQL_STAT_ITEM_DELTA_FUNC
public:
  bool is_in_retry_;
  bool is_route_miss_;
  bool is_plan_cache_hit_;
  bool is_muti_query_;
  bool is_muti_query_batch_;
  bool is_full_table_scan_;
  bool is_failed_;
#define DEF_SQL_STAT_ITEM(def_name)           \
  int64_t def_name##_start_;                  \
  int64_t def_name##_end_;
  DEF_SQL_STAT_ITEM(disk_reads);
  DEF_SQL_STAT_ITEM(buffer_gets);
  DEF_SQL_STAT_ITEM(elapsed_time);
  DEF_SQL_STAT_ITEM(cpu_time);
  DEF_SQL_STAT_ITEM(ccwait);
  DEF_SQL_STAT_ITEM(userio_wait);
  DEF_SQL_STAT_ITEM(apwait);
  DEF_SQL_STAT_ITEM(physical_read_requests);
  DEF_SQL_STAT_ITEM(physical_read_bytes);
  DEF_SQL_STAT_ITEM(write_throttle);
  DEF_SQL_STAT_ITEM(rows_processed);
  DEF_SQL_STAT_ITEM(memstore_read_rows);
  DEF_SQL_STAT_ITEM(minor_ssstore_read_rows);
  DEF_SQL_STAT_ITEM(major_ssstore_read_rows);
  DEF_SQL_STAT_ITEM(rpc);
  DEF_SQL_STAT_ITEM(fetches);
  DEF_SQL_STAT_ITEM(partition);
  DEF_SQL_STAT_ITEM(nested_sql);
#undef DEF_SQL_STAT_ITEM
};

class ObExecutedSqlStatRecord
{
public:
  ObExecutedSqlStatRecord(): sql_stat_info_()
  {
#define DEF_SQL_STAT_ITEM_CONSTRUCT(def_name)           \
    def_name##_total_ = 0;                              \
    def_name##_last_snap_ = 0;
    DEF_SQL_STAT_ITEM_CONSTRUCT(executions);
    DEF_SQL_STAT_ITEM_CONSTRUCT(disk_reads);
    DEF_SQL_STAT_ITEM_CONSTRUCT(buffer_gets);
    DEF_SQL_STAT_ITEM_CONSTRUCT(elapsed_time);
    DEF_SQL_STAT_ITEM_CONSTRUCT(cpu_time);
    DEF_SQL_STAT_ITEM_CONSTRUCT(ccwait);
    DEF_SQL_STAT_ITEM_CONSTRUCT(userio_wait);
    DEF_SQL_STAT_ITEM_CONSTRUCT(apwait);
    DEF_SQL_STAT_ITEM_CONSTRUCT(physical_read_requests);
    DEF_SQL_STAT_ITEM_CONSTRUCT(physical_read_bytes);
    DEF_SQL_STAT_ITEM_CONSTRUCT(write_throttle);
    DEF_SQL_STAT_ITEM_CONSTRUCT(rows_processed);
    DEF_SQL_STAT_ITEM_CONSTRUCT(memstore_read_rows);
    DEF_SQL_STAT_ITEM_CONSTRUCT(minor_ssstore_read_rows);
    DEF_SQL_STAT_ITEM_CONSTRUCT(major_ssstore_read_rows);
    DEF_SQL_STAT_ITEM_CONSTRUCT(rpc);
    DEF_SQL_STAT_ITEM_CONSTRUCT(fetches);
    DEF_SQL_STAT_ITEM_CONSTRUCT(retry);
    DEF_SQL_STAT_ITEM_CONSTRUCT(partition);
    DEF_SQL_STAT_ITEM_CONSTRUCT(nested_sql);
    DEF_SQL_STAT_ITEM_CONSTRUCT(route_miss);
    DEF_SQL_STAT_ITEM_CONSTRUCT(plan_cache_hit);
    DEF_SQL_STAT_ITEM_CONSTRUCT(muti_query);
    DEF_SQL_STAT_ITEM_CONSTRUCT(muti_query_batch);
    DEF_SQL_STAT_ITEM_CONSTRUCT(full_table_scan);
    DEF_SQL_STAT_ITEM_CONSTRUCT(error_count);
#undef DEF_SQL_STAT_ITEM_CONSTRUCT
  }
  
  ~ObExecutedSqlStatRecord() = default;
  int copy_sql_stat_info(const ObSqlStatInfo& sql_stat_info);
  int sum_stat_value(ObExecutingSqlStatRecord& executing_record);
  int sum_stat_value(ObExecutedSqlStatRecord& executed_record);
  ObSqlStatInfo& get_sql_stat_info() {return sql_stat_info_;}
  const ObSqlStatInfo& get_sql_stat_info() const {return sql_stat_info_;}
  ObSqlStatRecordKey& get_key() {return sql_stat_info_.get_key();};
  const ObSqlStatRecordKey& get_key() const {return sql_stat_info_.get_key();};
  int assign(const ObExecutedSqlStatRecord& other);
  int reset();
  int update_last_snap_record_value();
  #define DEF_SQL_STAT_ITEM_DELTA_FUNC(def_name)                                           \
    int64_t get_##def_name##_total() const { return def_name##_total_;}                    \
    int64_t get_##def_name##_last_snap() const { return def_name##_last_snap_;}            \
    int64_t get_##def_name##_delta() const {                                               \
      int64_t delta = def_name##_total_ - def_name##_last_snap_;                           \
      return delta > 0 ? delta: 0;                                                         \
    }
    DEF_SQL_STAT_ITEM_DELTA_FUNC(executions);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(disk_reads);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(buffer_gets);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(elapsed_time);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(cpu_time);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(ccwait);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(userio_wait);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(apwait);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(physical_read_requests);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(physical_read_bytes);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(write_throttle);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(rows_processed);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(memstore_read_rows);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(minor_ssstore_read_rows);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(major_ssstore_read_rows);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(rpc);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(fetches);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(retry);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(partition);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(nested_sql);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(route_miss);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(plan_cache_hit);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(muti_query);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(muti_query_batch);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(full_table_scan);
    DEF_SQL_STAT_ITEM_DELTA_FUNC(error_count);
  #undef DEF_SQL_STAT_ITEM_DELTA_FUNC
  
  TO_STRING_KV(K_(sql_stat_info));
private:
  ObSqlStatInfo sql_stat_info_;
  #define DEF_SQL_STAT_ITEM(def_name)           \
    int64_t def_name##_total_;                  \
    int64_t def_name##_last_snap_;
    DEF_SQL_STAT_ITEM(executions);
    DEF_SQL_STAT_ITEM(disk_reads);
    DEF_SQL_STAT_ITEM(buffer_gets);
    DEF_SQL_STAT_ITEM(elapsed_time);
    DEF_SQL_STAT_ITEM(cpu_time);
    DEF_SQL_STAT_ITEM(ccwait);
    DEF_SQL_STAT_ITEM(userio_wait);
    DEF_SQL_STAT_ITEM(apwait);
    DEF_SQL_STAT_ITEM(physical_read_requests);
    DEF_SQL_STAT_ITEM(physical_read_bytes);
    DEF_SQL_STAT_ITEM(write_throttle);
    DEF_SQL_STAT_ITEM(rows_processed);
    DEF_SQL_STAT_ITEM(memstore_read_rows);
    DEF_SQL_STAT_ITEM(minor_ssstore_read_rows);
    DEF_SQL_STAT_ITEM(major_ssstore_read_rows);
    DEF_SQL_STAT_ITEM(rpc);
    DEF_SQL_STAT_ITEM(fetches);
    DEF_SQL_STAT_ITEM(retry);
    DEF_SQL_STAT_ITEM(partition);
    DEF_SQL_STAT_ITEM(nested_sql);
    DEF_SQL_STAT_ITEM(route_miss);
    DEF_SQL_STAT_ITEM(plan_cache_hit);
    DEF_SQL_STAT_ITEM(muti_query);
    DEF_SQL_STAT_ITEM(muti_query_batch);
    DEF_SQL_STAT_ITEM(full_table_scan);
    DEF_SQL_STAT_ITEM(error_count);
  #undef DEF_SQL_STAT_ITEM
};


/*************************************************************************************************/

class ObSqlStatRecordNode: public ObILibCacheNode
{
public:
  ObSqlStatRecordNode(ObPlanCache *lib_cache, lib::MemoryContext &mem_context)
      : ObILibCacheNode(lib_cache, mem_context),
        cache_obj_(nullptr) {}
  virtual ~ObSqlStatRecordNode() {}
  virtual int inner_get_cache_obj(sql::ObILibCacheCtx &ctx,
                                sql::ObILibCacheKey *key,
                                sql::ObILibCacheObject *&cache_obj) override;
  virtual int inner_add_cache_obj(sql::ObILibCacheCtx &ctx,
                                sql::ObILibCacheKey *key,
                                sql::ObILibCacheObject *cache_obj) override;
private:
  sql::ObILibCacheObject *cache_obj_;
};

class ObSqlStatRecordObj: public sql::ObILibCacheObject
{
public:
  ObSqlStatRecordObj(lib::MemoryContext &mem_context)
  : sql::ObILibCacheObject(ObLibCacheNameSpace::NS_SQLSTAT, mem_context),
    record_value_() {}

  virtual ~ObSqlStatRecordObj() = default;
  OB_INLINE sql::ObExecutedSqlStatRecord* get_record_value() { return &record_value_; }
  OB_INLINE const sql::ObExecutedSqlStatRecord* get_record_value() const { return &record_value_; }
private:
  sql::ObExecutedSqlStatRecord record_value_;
};

class ObSqlStatRecordUtil
{
public:
  static int get_cache_obj(ObSqlStatRecordKey &key, ObCacheObjGuard& guard);
  static int create_cache_obj(ObSqlStatRecordKey &key, ObCacheObjGuard& guard);
  int add_cache_obj(ObSqlStatRecordKey &key, ObCacheObjGuard& guard);
};
} // end sql
} // end oceanbase

#endif /* _OB_SQL_STAT_RECORD_H_ */
