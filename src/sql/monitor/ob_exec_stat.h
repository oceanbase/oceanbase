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

#ifdef EVENT_INFO
EVENT_INFO(WAIT_TIME, wait_time)
EVENT_INFO(WAIT_COUNT, wait_count)
EVENT_INFO(IO_READ_COUNT, io_read_count)
EVENT_INFO(USER_IO_TIME, user_io_time)
EVENT_INFO(APPLICATION_TIME, application_time)
EVENT_INFO(CONCURRENCY_TIME, concurrency_time)
EVENT_INFO(IO_WRITES_COUNT, io_write_count)
EVENT_INFO(RPC_PACKET_OUT, rpc_packet_out)
EVENT_INFO(ROW_CACHE_HIT, row_cache_hit)
EVENT_INFO(BLOCK_CACHE_HIT, block_cache_hit)
EVENT_INFO(BLOOM_FILTER_FILTS, bloom_filter_filts)
EVENT_INFO(MEMSTORE_READ_ROW_COUNT, memstore_read_row_count)
EVENT_INFO(SSSTORE_READ_ROW_COUNT, ssstore_read_row_count)
EVENT_INFO(DATA_BLOCK_READ_CNT, data_block_read_cnt)
EVENT_INFO(DATA_BLOCK_CACHE_HIT, data_block_cache_hit)
EVENT_INFO(INDEX_BLOCK_READ_CNT, index_block_read_cnt)
EVENT_INFO(INDEX_BLOCK_CACHE_HIT, index_block_cache_hit)
EVENT_INFO(BLOCKSCAN_BLOCK_CNT, blockscan_block_cnt)
EVENT_INFO(BLOCKSCAN_ROW_CNT, blockscan_row_cnt)
EVENT_INFO(PUSHDOWN_STORAGE_FILTER_ROW_CNT, pushdown_storage_filter_row_cnt)
EVENT_INFO(FUSE_ROW_CACHE_HIT, fuse_row_cache_hit)
#endif

#ifndef OCEANBASE_SQL_OB_EXEC_STAT_H
#define OCEANBASE_SQL_OB_EXEC_STAT_H
#include "lib/stat/ob_diagnose_info.h"
#include "lib/wait_event/ob_wait_event.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/net/ob_addr.h"
#include "sql/ob_sql_define.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
namespace oceanbase
{
namespace sql
{
struct ObExecRecord
{
 //max wait event during sql exec
  common::ObWaitEventDesc max_wait_event_;

#define EVENT_INFO(def, name) \
  int64_t name##_start_; \
  int64_t name##_end_;\
  int64_t name##_;
#include "ob_exec_stat.h"
#undef EVENT_INFO

  ObExecRecord()
  {
    MEMSET(this, 0, sizeof(*this));
  }

#define EVENT_INFO(def, name) \
  int64_t get_##name() const { return name##_end_ - name##_start_; }
#include "ob_exec_stat.h"
#undef EVENT_INFO


#define EVENT_STAT_GET(event_stats_array, stat_no)              \
 ({                                                            \
   int64_t ret = 0;                                            \
   oceanbase::common::ObStatEventAddStat *stat = NULL;         \
   if (NULL != (stat = event_stats_array.get(::oceanbase::common::stat_no))) { \
     ret = stat->get_stat_value();                             \
   }                                                           \
   ret;                                                        \
 })

#define RECORD(se, di) \
  do { \
    oceanbase::common::ObDiagnoseSessionInfo *diag_session_info = \
        (NULL != di) ? di : oceanbase::common::ObDiagnoseSessionInfo::get_local_diagnose_info(); \
    if (NULL != diag_session_info) { \
      oceanbase::common::ObStatEventAddStatArray &arr = diag_session_info->get_add_stat_stats(); \
      io_read_count_##se##_= EVENT_STAT_GET(arr, ObStatEventIds::IO_READ_COUNT); \
      block_cache_hit_##se##_= EVENT_STAT_GET(arr, ObStatEventIds::BLOCK_CACHE_HIT); \
      rpc_packet_out_##se##_= EVENT_STAT_GET(arr, ObStatEventIds::RPC_PACKET_OUT);   \
      row_cache_hit_##se##_= EVENT_STAT_GET(arr, ObStatEventIds::ROW_CACHE_HIT);     \
      bloom_filter_filts_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::BLOOM_FILTER_FILTS);            \
      memstore_read_row_count_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::MEMSTORE_READ_ROW_COUNT);  \
      ssstore_read_row_count_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::SSSTORE_READ_ROW_COUNT);    \
      data_block_read_cnt_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::DATA_BLOCK_READ_CNT);          \
      data_block_cache_hit_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::DATA_BLOCK_CACHE_HIT);        \
      index_block_read_cnt_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::INDEX_BLOCK_READ_CNT);        \
      index_block_cache_hit_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::INDEX_BLOCK_CACHE_HIT);      \
      blockscan_block_cnt_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::BLOCKSCAN_BLOCK_CNT);          \
      blockscan_row_cnt_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::BLOCKSCAN_ROW_CNT);              \
      pushdown_storage_filter_row_cnt_##se##_ = EVENT_STAT_GET(arr, ObStatEventIds::PUSHDOWN_STORAGE_FILTER_ROW_CNT); \
      fuse_row_cache_hit_##se##_= EVENT_STAT_GET(arr, ObStatEventIds::FUSE_ROW_CACHE_HIT);             \
    } \
  } while(0);

  #define UPDATE_EVENT(event) \
  do \
  { \
    event##_ += event##_end_ - event##_start_; \
  } while(0);

  void record_start(common::ObDiagnoseSessionInfo *di = NULL) {
    RECORD(start, di);
  }

  void record_end(common::ObDiagnoseSessionInfo *di = NULL) {
    RECORD(end, di);
  }

  void update_stat() {
    UPDATE_EVENT(wait_time);
    UPDATE_EVENT(wait_count);
    UPDATE_EVENT(io_read_count);
    UPDATE_EVENT(io_write_count);
    UPDATE_EVENT(block_cache_hit);
    UPDATE_EVENT(rpc_packet_out);
    UPDATE_EVENT(row_cache_hit);
    UPDATE_EVENT(bloom_filter_filts);
    UPDATE_EVENT(user_io_time);
    UPDATE_EVENT(concurrency_time);
    UPDATE_EVENT(application_time);
    UPDATE_EVENT(memstore_read_row_count);
    UPDATE_EVENT(ssstore_read_row_count);
    UPDATE_EVENT(data_block_read_cnt);
    UPDATE_EVENT(data_block_cache_hit);
    UPDATE_EVENT(index_block_read_cnt);
    UPDATE_EVENT(index_block_cache_hit);
    UPDATE_EVENT(blockscan_block_cnt);
    UPDATE_EVENT(blockscan_row_cnt);
    UPDATE_EVENT(pushdown_storage_filter_row_cnt);
  }
};

enum ExecType {
  InvalidType = 0,
  MpQuery,
  InnerSql,
  RpcProcessor,
  PLSql,
  PSCursor,
  DbmsCursor,
  CursorFetch
};

struct ObReqTimestamp
{
  ObReqTimestamp()
  {
    MEMSET(this, 0, sizeof(*this));
  }
  int64_t receive_timestamp_;
  int64_t run_timestamp_;
  int64_t enqueue_timestamp_;
};

struct ObExecTimestamp {
  ObExecTimestamp()
  {
    MEMSET(this, 0, sizeof(*this));
  }
  ExecType exec_type_;
  int64_t rpc_send_ts_; //发送rpc的时间戳
  int64_t receive_ts_;  //接收请求的时间戳, 其后面是net wait时间
  //***下面的时间戳在每次重试后需要更新***
  int64_t enter_queue_ts_;//进入队列时间戳
  int64_t run_ts_;      //开始run的时间戳, 其后面是decode时间
  int64_t before_process_ts_;  //before process 开始的时间戳
  int64_t single_process_ts_;  //单条sql do process的开始时间戳
  int64_t process_executor_ts_; //plan开始执行的时间戳
  int64_t executor_end_ts_; // plan执行结束的时间戳
  //*** 下面时间戳是特殊场景所需要用的***
  int64_t multistmt_start_ts_; // multistmt 场景下拆分后各个子 sql 开始执行时间戳

  int64_t elapsed_t_;
  //**** 下面只记录在第一次执行过程中耗时**
  int64_t net_t_;
  int64_t net_wait_t_;
  //***** 下面记录的是累计耗时 ***
  int64_t queue_t_;
  int64_t decode_t_;
  int64_t get_plan_t_;
  int64_t executor_t_;

  TO_STRING_KV(K(exec_type_), K(rpc_send_ts_), K(receive_ts_), K(enter_queue_ts_),
               K(run_ts_), K(before_process_ts_), K(single_process_ts_),
               K(process_executor_ts_), K(executor_end_ts_), K(multistmt_start_ts_),
               K(elapsed_t_), K(net_t_), K(net_wait_t_), K(queue_t_),
               K(decode_t_), K(get_plan_t_), K(executor_t_));

  //出现重试时时间累加
  void update_stage_time() {
    // elapsed_t_ 重试不需要累加，其他重试需要累加，且在 multistmt 场景下计算方式更改
    // multistmt 场景特殊处理，第二个及之后的 sql 的 queue_t_、decode_t_ 均为 0
    if (multistmt_start_ts_ > 0) {
      queue_t_ = 0;
      decode_t_ = 0;
      elapsed_t_ = executor_end_ts_ - multistmt_start_ts_;
    } else {
      queue_t_ += run_ts_ - enter_queue_ts_;
      decode_t_ += before_process_ts_ - run_ts_;
      elapsed_t_ = executor_end_ts_ - receive_ts_;
    }
    get_plan_t_ += process_executor_ts_ - single_process_ts_;
    executor_t_ += executor_end_ts_ - process_executor_ts_;
  }
};

class ObSchedInfo
{
public:
  ObSchedInfo() : sched_info_(NULL), sched_info_len_(0) {}
  void reset() 
  {
    MEMSET(this, 0 , sizeof(*this)); 
  }
  inline void assign(char *sched_info, int64_t info_len)
  {
    sched_info_ = sched_info;
    sched_info_len_ = info_len;
  }
  int append(common::ObIAllocator &allocator, const char *sched_info, int64_t info_len)
  {
    int ret = common::OB_SUCCESS;
    if (sched_info_len_ >= 0 && info_len > 0 && sched_info_len_ + info_len <= common::OB_MAX_SCHED_INFO_LENGTH) {
      void *ptr = NULL;
      if (OB_UNLIKELY(NULL == (ptr = allocator.alloc(info_len + sched_info_len_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_MONITOR_LOG(WARN, "fail to alloc sched info", K(ret));
      } else {
        char *str = static_cast<char *>(ptr);
        if (sched_info_ != NULL && sched_info_len_ > 0) {
          MEMCPY(str, sched_info_, sched_info_len_);
        }
        MEMCPY(str + sched_info_len_, sched_info, info_len);
        assign(str, info_len + sched_info_len_);
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      SQL_MONITOR_LOG(WARN, "sched info len is invalid", K(ret));
    }
    return ret;
  }
  inline const char *get_ptr() const { return sched_info_; }
  inline int64_t get_len() const { return sched_info_len_; }
private:
  char *sched_info_;
  int64_t sched_info_len_;
};

struct ObAuditRecordData {
  ObAuditRecordData()
  {
    reset();
  }
  ~ObAuditRecordData()
  {
  }
  int assign(const ObAuditRecordData &from)
  {
    MEMCPY((void*)this, (void*)&from, sizeof(from));
    return OB_SUCCESS;
  }
  void reset()
  {
    MEMSET((void*)this, 0, sizeof(*this));
    consistency_level_ = common::INVALID_CONSISTENCY;
    ps_stmt_id_ = OB_INVALID_STMT_ID;
    ps_inner_stmt_id_ = OB_INVALID_STMT_ID;
    trans_id_ = 0;
    request_type_ = EXECUTE_INVALID;
    is_batched_multi_stmt_ = false;
    plan_hash_ = 0;
    trx_lock_for_read_elapse_ = 0;
    params_value_len_ = 0;
    partition_hit_ = true;
    is_perf_event_closed_ = false;
  }

  int64_t get_elapsed_time() const
  {
    int64_t elapsed_time = 0;
    if (OB_UNLIKELY(exec_timestamp_.multistmt_start_ts_ > 0)) {
      elapsed_time = exec_timestamp_.executor_end_ts_ - exec_timestamp_.multistmt_start_ts_;
    } else {
      elapsed_time = exec_timestamp_.executor_end_ts_ - exec_timestamp_.receive_ts_;
    }
    return elapsed_time;
  }

  int64_t get_process_time() const
  {
    return exec_timestamp_.executor_end_ts_ - exec_timestamp_.single_process_ts_;
  }

  void update_event_stage_state() {
    exec_record_.update_stat();
    const int64_t cpu_time = MAX(exec_timestamp_.elapsed_t_ - exec_record_.wait_time_, 0);
    const int64_t elapsed_time = MAX(exec_timestamp_.elapsed_t_, 0);
    if(is_inner_sql_) {
      EVENT_ADD(SYS_TIME_MODEL_DB_INNER_TIME, elapsed_time);
      EVENT_ADD(SYS_TIME_MODEL_DB_INNER_CPU, cpu_time);
    } else {
      EVENT_ADD(SYS_TIME_MODEL_DB_TIME, elapsed_time);
      EVENT_ADD(SYS_TIME_MODEL_DB_CPU, cpu_time);
    }
  }

  bool is_timeout() const {
    return common::OB_TIMEOUT == status_
           || common::OB_TRANS_STMT_TIMEOUT == status_;
  }

  int64_t get_extra_size() const
  {
    return sql_len_ + tenant_name_len_ + user_name_len_ + db_name_len_;
  }

  share::SCN get_snapshot_version() const
  {
    return snapshot_.version_;
  }

  ObString get_snapshot_source() const
  {
    return ObString(snapshot_.source_);
  }

  int16_t seq_; //packet->get_packet_header().seq_; always 0 currently
  int status_; //error code
  common::ObCurTraceId::TraceId trace_id_;
  int64_t request_id_; //set by request_manager automatic when add record
  int64_t execution_id_;  //used to jion v$sql_plan_monitor
  uint64_t session_id_;
  uint64_t proxy_session_id_;
  uint64_t qc_id_;  //px框架下id
  int64_t dfo_id_;
  int64_t sqc_id_;
  int64_t worker_id_;
  common::ObAddr server_addr_;
  common::ObAddr client_addr_;
  common::ObAddr user_client_addr_;
  int64_t tenant_id_;
  int64_t effective_tenant_id_;
  char *tenant_name_;
  int64_t tenant_name_len_;
  int64_t user_id_;
  char *user_name_;
  int64_t user_name_len_;
  int user_group_; // user 所属 cgroup id，仅主线程展示
  uint64_t db_id_;
  char *db_name_;
  int64_t db_name_len_;
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  char *sql_; //该内存由allocate_分配, 在record被淘汰时释放；
  int64_t sql_len_;
  common::ObCollationType sql_cs_type_;
  int64_t plan_id_;
  int64_t affected_rows_;//delete,update,insert影响的行数,及select选出的行数
  int64_t return_rows_;
  int64_t partition_cnt_;//该请求涉及的所以partition个数
  int64_t expected_worker_cnt_; // px 预期分配线程数
  int64_t used_worker_cnt_; // px 实际分配线程数
  int64_t try_cnt_; //尝试执行次数
  ObPhyPlanType plan_type_;
  bool is_executor_rpc_;
  bool is_inner_sql_;
  bool is_hit_plan_cache_;
  bool is_multi_stmt_; //是否是multi sql
  bool table_scan_;
  common::ObConsistencyLevel consistency_level_;
  int64_t request_memory_used_;
  ObExecTimestamp exec_timestamp_;
  ObExecRecord exec_record_;
  ObTableScanStat table_scan_stat_;
  int64_t ps_stmt_id_;
  int64_t ps_inner_stmt_id_;
  int64_t request_type_;
  int64_t trans_id_;
  bool is_batched_multi_stmt_;
  uint64_t plan_hash_;
  int64_t trx_lock_for_read_elapse_;
  int64_t params_value_len_;
  char *params_value_;
  char *rule_name_;
  int64_t rule_name_len_;
  struct StmtSnapshot {
    share::SCN version_;      // snapshot version
    int64_t tx_id_;        // snapshot inner which txn
    int64_t scn_;          // snapshot's position in the txn
    char const* source_;   // snapshot's acquire source
  } snapshot_; // stmt's tx snapshot
  uint64_t txn_free_route_flag_; // flag contains txn free route meta
  uint64_t txn_free_route_version_; // the version of txn's state
  bool partition_hit_;// flag for need das partition route or not
  bool is_perf_event_closed_;
  char flt_trace_id_[OB_MAX_UUID_STR_LENGTH + 1];
};

} //namespace sql
} //namespace oceanbase
#endif


