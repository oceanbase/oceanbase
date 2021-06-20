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
EVENT_INFO(SCHED_TIME, sched_time)
EVENT_INFO(USER_IO_TIME, user_io_time)
EVENT_INFO(APPLICATION_TIME, application_time)
EVENT_INFO(CONCURRENCY_TIME, concurrency_time)
EVENT_INFO(IO_READ_COUNT, io_read_count)
EVENT_INFO(IO_WRITES_COUNT, io_write_count)
EVENT_INFO(IO_READ_BYTES, io_read_bytes)
EVENT_INFO(IO_WRITE_BYTES, io_write_bytes)
EVENT_INFO(RPC_PACKET_IN, rpc_packet_in)
EVENT_INFO(RPC_PACKET_IN_BYTES, rpc_packet_in_bytes)
EVENT_INFO(RPC_PACKET_OUT, rpc_packet_out)
EVENT_INFO(RPC_PACKET_OUT_BYTES, rpc_packet_out_bytes)
EVENT_INFO(ROW_CACHE_HIT, row_cache_hit)
EVENT_INFO(ROW_CACHE_MISS, row_cache_miss)
EVENT_INFO(BLOCK_INDEX_CACHE_HIT, block_index_cache_hit)
EVENT_INFO(BLOCK_INDEX_CACHE_MISS, block_index_cache_miss)
EVENT_INFO(BLOCK_CACHE_HIT, block_cache_hit)
EVENT_INFO(BLOCK_CACHE_MISS, block_cache_miss)
EVENT_INFO(BLOOM_FILTER_FILTS, bloom_filter_filts)
EVENT_INFO(LOCATION_CACHE_HIT, location_cache_hit)
EVENT_INFO(LOCATION_CACHE_MISS, location_cache_miss)
EVENT_INFO(MEMSTORE_READ_LOCK_SUCC_COUNT, memstore_read_lock_succ_count)
EVENT_INFO(MEMSTORE_WRITE_LOCK_SUCC_COUNT, memstore_write_lock_succ_count)
EVENT_INFO(MEMSTORE_WAIT_READ_LOCK_TIME, memstore_wait_read_lock_time)
EVENT_INFO(MEMSTORE_WAIT_WRITE_LOCK_TIME, memstore_wait_write_lock_time)
EVENT_INFO(TRANS_COMMIT_LOG_SYNC_TIME, trans_commit_log_sync_time)
EVENT_INFO(TRANS_COMMIT_LOG_SYNC_COUNT, trans_commit_log_sync_count)
EVENT_INFO(TRANS_COMMIT_LOG_SUBMIT_COUNT, trans_commit_log_submit_count)
EVENT_INFO(TRANS_COMMIT_TIME, trans_commit_time)
EVENT_INFO(MEMSTORE_READ_ROW_COUNT, memstore_read_row_count)
EVENT_INFO(SSSTORE_READ_ROW_COUNT, ssstore_read_row_count)
EVENT_INFO(FUSE_ROW_CACHE_HIT, fuse_row_cache_hit)
EVENT_INFO(FUSE_ROW_CACHE_MISS, fuse_row_cache_miss)
#endif

#ifndef OCEANBASE_SQL_OB_EXEC_STAT_H
#define OCEANBASE_SQL_OB_EXEC_STAT_H
#include "lib/stat/ob_diagnose_info.h"
#include "lib/wait_event/ob_wait_event.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/net/ob_addr.h"
#include "sql/ob_sql_define.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
namespace oceanbase {
namespace sql {
struct ObExecRecord {
  // max wait event during sql exec
  common::ObWaitEventDesc max_wait_event_;

#define EVENT_INFO(def, name) \
  int64_t name##_start_;      \
  int64_t name##_end_;        \
  int64_t name##_;
#include "ob_exec_stat.h"
#undef EVENT_INFO

  ObExecRecord()
  {
    MEMSET(this, 0, sizeof(*this));
  }

#define EVENT_INFO(def, name)           \
  int64_t get_##name() const            \
  {                                     \
    return name##_end_ - name##_start_; \
  }
#include "ob_exec_stat.h"
#undef EVENT_INFO

#define RECORD(se, di)                                                                                               \
  do {                                                                                                               \
    oceanbase::common::ObDiagnoseSessionInfo* diag_session_info =                                                    \
        (NULL != di) ? di : oceanbase::common::ObDiagnoseSessionInfo::get_local_diagnose_info();                     \
    if (NULL != diag_session_info) {                                                                                 \
      io_read_count_##se##_ = EVENT_GET(ObStatEventIds::IO_READ_COUNT, diag_session_info);                           \
      io_write_count_##se##_ = EVENT_GET(ObStatEventIds::IO_WRITE_COUNT, diag_session_info);                         \
      block_cache_hit_##se##_ = EVENT_GET(ObStatEventIds::BLOCK_CACHE_HIT, diag_session_info);                       \
      io_read_bytes_##se##_ = EVENT_GET(ObStatEventIds::IO_READ_BYTES, diag_session_info);                           \
      io_write_bytes_##se##_ = EVENT_GET(ObStatEventIds::IO_WRITE_BYTES, diag_session_info);                         \
      rpc_packet_in_##se##_ = EVENT_GET(ObStatEventIds::RPC_PACKET_IN, diag_session_info);                           \
      rpc_packet_in_bytes_##se##_ = EVENT_GET(ObStatEventIds::RPC_PACKET_IN_BYTES, diag_session_info);               \
      rpc_packet_out_##se##_ = EVENT_GET(ObStatEventIds::RPC_PACKET_OUT, diag_session_info);                         \
      rpc_packet_out_bytes_##se##_ = EVENT_GET(ObStatEventIds::RPC_PACKET_OUT_BYTES, diag_session_info);             \
      trans_commit_log_sync_time_##se##_ = EVENT_GET(ObStatEventIds::TRANS_COMMIT_LOG_SYNC_TIME, diag_session_info); \
      row_cache_hit_##se##_ = EVENT_GET(ObStatEventIds::ROW_CACHE_HIT, diag_session_info);                           \
      row_cache_miss_##se##_ = EVENT_GET(ObStatEventIds::ROW_CACHE_MISS, diag_session_info);                         \
      block_index_cache_hit_##se##_ = EVENT_GET(ObStatEventIds::BLOCK_INDEX_CACHE_HIT, diag_session_info);           \
      block_index_cache_miss_##se##_ = EVENT_GET(ObStatEventIds::BLOCK_INDEX_CACHE_MISS, diag_session_info);         \
      block_cache_hit_##se##_ = EVENT_GET(ObStatEventIds::BLOCK_CACHE_HIT, diag_session_info);                       \
      block_cache_miss_##se##_ = EVENT_GET(ObStatEventIds::BLOCK_CACHE_MISS, diag_session_info);                     \
      bloom_filter_filts_##se##_ = EVENT_GET(ObStatEventIds::BLOOM_FILTER_FILTS, diag_session_info);                 \
      location_cache_hit_##se##_ = EVENT_GET(ObStatEventIds::LOCATION_CACHE_HIT, diag_session_info);                 \
      location_cache_miss_##se##_ = EVENT_GET(ObStatEventIds::LOCATION_CACHE_MISS, diag_session_info);               \
      memstore_read_lock_succ_count_##se##_ =                                                                        \
          EVENT_GET(ObStatEventIds::MEMSTORE_READ_LOCK_SUCC_COUNT, diag_session_info);                               \
      memstore_write_lock_succ_count_##se##_ =                                                                       \
          EVENT_GET(ObStatEventIds::MEMSTORE_WRITE_LOCK_SUCC_COUNT, diag_session_info);                              \
      memstore_wait_read_lock_time_##se##_ =                                                                         \
          EVENT_GET(ObStatEventIds::MEMSTORE_WAIT_READ_LOCK_TIME, diag_session_info);                                \
      memstore_wait_write_lock_time_##se##_ =                                                                        \
          EVENT_GET(ObStatEventIds::MEMSTORE_WAIT_WRITE_LOCK_TIME, diag_session_info);                               \
      memstore_read_row_count_##se##_ = EVENT_GET(ObStatEventIds::MEMSTORE_READ_ROW_COUNT, diag_session_info);       \
      ssstore_read_row_count_##se##_ = EVENT_GET(ObStatEventIds::SSSTORE_READ_ROW_COUNT, diag_session_info);         \
      fuse_row_cache_hit_##se##_ = EVENT_GET(ObStatEventIds::FUSE_ROW_CACHE_HIT, diag_session_info);                 \
      fuse_row_cache_miss_##se##_ = EVENT_GET(ObStatEventIds::FUSE_ROW_CACHE_MISS, diag_session_info);               \
      for (int i = 0; i < oceanbase::common::ObWaitEventIds::WAIT_EVENT_END; ++i) {                                  \
        if (NULL != diag_session_info->get_event_stats().get(i)) {                                                   \
          uint64_t time = diag_session_info->get_event_stats().get(i)->time_waited_;                                 \
          switch (oceanbase::common::OB_WAIT_EVENTS[i].wait_class_) {                                                \
            case oceanbase::common::ObWaitClassIds::USER_IO: {                                                       \
              user_io_time_##se##_ += time;                                                                          \
              break;                                                                                                 \
            }                                                                                                        \
            case oceanbase::common::ObWaitClassIds::SCHEDULER: {                                                     \
              sched_time_##se##_ += time;                                                                            \
              break;                                                                                                 \
            }                                                                                                        \
            case oceanbase::common::ObWaitClassIds::CONCURRENCY: {                                                   \
              concurrency_time_##se##_ += time;                                                                      \
              break;                                                                                                 \
            }                                                                                                        \
            case oceanbase::common::ObWaitClassIds::APPLICATION: {                                                   \
              application_time_##se##_ += time;                                                                      \
              break;                                                                                                 \
            }                                                                                                        \
            default: {                                                                                               \
              /*do nothing*/                                                                                         \
              break;                                                                                                 \
            }                                                                                                        \
          }                                                                                                          \
        }                                                                                                            \
      } /*for end */                                                                                                 \
    }                                                                                                                \
  } while (0);

#define UPDATE_EVENT(event)                    \
  do {                                         \
    event##_ += event##_end_ - event##_start_; \
  } while (0);

  void record_start(common::ObDiagnoseSessionInfo* di = NULL)
  {
    RECORD(start, di);
  }

  void record_end(common::ObDiagnoseSessionInfo* di = NULL)
  {
    RECORD(end, di);
  }

  void update_stat()
  {
    UPDATE_EVENT(wait_time);
    UPDATE_EVENT(wait_count);
    UPDATE_EVENT(io_read_count);
    UPDATE_EVENT(io_write_count);
    UPDATE_EVENT(block_cache_hit);
    UPDATE_EVENT(io_read_bytes);
    UPDATE_EVENT(io_write_bytes);
    UPDATE_EVENT(rpc_packet_in);
    UPDATE_EVENT(rpc_packet_in_bytes);
    UPDATE_EVENT(rpc_packet_out);
    UPDATE_EVENT(rpc_packet_out_bytes);
    UPDATE_EVENT(trans_commit_log_sync_time);
    UPDATE_EVENT(row_cache_hit);
    UPDATE_EVENT(row_cache_miss);
    UPDATE_EVENT(block_index_cache_hit);
    UPDATE_EVENT(block_index_cache_miss);
    UPDATE_EVENT(bloom_filter_filts);
    UPDATE_EVENT(location_cache_hit);
    UPDATE_EVENT(location_cache_miss);
    UPDATE_EVENT(memstore_read_lock_succ_count);
    UPDATE_EVENT(memstore_write_lock_succ_count);
    UPDATE_EVENT(memstore_wait_read_lock_time);
    UPDATE_EVENT(memstore_wait_write_lock_time);
    UPDATE_EVENT(user_io_time);
    UPDATE_EVENT(sched_time);
    UPDATE_EVENT(concurrency_time);
    UPDATE_EVENT(application_time);
    UPDATE_EVENT(memstore_read_row_count);
    UPDATE_EVENT(ssstore_read_row_count);
  }
};

enum ExecType { InvalidType = 0, MpQuery, InnerSql, RpcProcessor, PLSql };

struct ObReqTimestamp {
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

  int64_t rpc_send_ts_;  // Send rpc timestamp
  int64_t receive_ts_;   // The timestamp of the received request, followed by the net wait time
  //***The timestamp below needs to be updated after each retry***
  int64_t enter_queue_ts_;       // Enter the queue timestamp
  int64_t run_ts_;               // The timestamp of the start of the run, followed by the decode time
  int64_t before_process_ts_;    // The timestamp of the beginning of the before process
  int64_t single_process_ts_;    // Start timestamp of a single sql do process
  int64_t process_executor_ts_;  // The time stamp when the plan started executing
  int64_t executor_end_ts_;      // The timestamp of the end of plan execution

  int64_t elapsed_t_;
  //**** The following records only time-consuming during the first execution**
  int64_t net_t_;
  int64_t net_wait_t_;
  //***** The following records are the cumulative time ***
  int64_t queue_t_;
  int64_t decode_t_;
  int64_t get_plan_t_;
  int64_t executor_t_;

  // Time accumulates when there is a retry
  void update_stage_time()
  {
    elapsed_t_ = executor_end_ts_ - receive_ts_;  // Retry does not need to accumulate
    queue_t_ += run_ts_ - enter_queue_ts_;
    decode_t_ += before_process_ts_ - run_ts_;
    get_plan_t_ += process_executor_ts_ - single_process_ts_;
    executor_t_ += executor_end_ts_ - process_executor_ts_;
  }
};

class ObSchedInfo {
public:
  ObSchedInfo() : sched_info_(NULL), sched_info_len_(0)
  {}
  void reset()
  {
    MEMSET(this, 0, sizeof(*this));
  }
  inline void assign(char* sched_info, int64_t info_len)
  {
    sched_info_ = sched_info;
    sched_info_len_ = info_len;
  }
  int append(common::ObIAllocator& allocator, const char* sched_info, int64_t info_len)
  {
    int ret = common::OB_SUCCESS;
    if (sched_info_len_ >= 0 && info_len > 0 && sched_info_len_ + info_len <= common::OB_MAX_SCHED_INFO_LENGTH) {
      void* ptr = NULL;
      if (OB_UNLIKELY(NULL == (ptr = allocator.alloc(info_len + sched_info_len_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_MONITOR_LOG(WARN, "fail to alloc sched info", K(ret));
      } else {
        char* str = static_cast<char*>(ptr);
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
  inline const char* get_ptr() const
  {
    return sched_info_;
  }
  inline int64_t get_len() const
  {
    return sched_info_len_;
  }

private:
  char* sched_info_;
  int64_t sched_info_len_;
};

struct ObAuditRecordData {
  ObAuditRecordData() : sched_info_()
  {
    reset();
  }
  ~ObAuditRecordData()
  {
    sched_info_.reset();
  }

  void reset()
  {
    MEMSET(this, 0, sizeof(*this));
    consistency_level_ = common::INVALID_CONSISTENCY;
    ps_stmt_id_ = OB_INVALID_STMT_ID;
    trans_hash_ = 0;
    request_type_ = EXECUTE_INVALID;
    is_batched_multi_stmt_ = false;
    plan_hash_ = 0;
    trx_lock_for_read_elapse_ = 0;
  }

  int64_t get_elapsed_time() const
  {
    return exec_timestamp_.executor_end_ts_ - exec_timestamp_.receive_ts_;
  }

  int64_t get_process_time() const
  {
    return exec_timestamp_.executor_end_ts_ - exec_timestamp_.single_process_ts_;
  }

  void update_stage_stat()
  {
    exec_timestamp_.update_stage_time();
    exec_record_.update_stat();
    const int64_t cpu_time = MAX(exec_timestamp_.elapsed_t_ - exec_record_.wait_time_, 0);
    const int64_t elapsed_time = MAX(exec_timestamp_.elapsed_t_, 0);
    if (is_inner_sql_) {
      EVENT_ADD(SYS_TIME_MODEL_DB_INNER_TIME, elapsed_time);
      EVENT_ADD(SYS_TIME_MODEL_DB_INNER_CPU, cpu_time);
    } else {
      EVENT_ADD(SYS_TIME_MODEL_DB_TIME, elapsed_time);
      EVENT_ADD(SYS_TIME_MODEL_DB_CPU, cpu_time);
    }
  }

  bool is_timeout() const
  {
    return common::OB_TIMEOUT == status_ || common::OB_TRANS_STMT_TIMEOUT == status_;
  }

  int64_t get_extra_size() const
  {
    return sql_len_ + tenant_name_len_ + user_name_len_ + db_name_len_;
  }

  int16_t seq_;  // packet->get_packet_header().seq_; always 0 currently
  int status_;   // error code
  uint64_t trace_id_[2];
  int64_t request_id_;    // set by request_manager automatic when add record
  int64_t execution_id_;  // used to jion v$sql_plan_monitor
  uint64_t session_id_;
  uint64_t qc_id_;
  int64_t dfo_id_;
  int64_t sqc_id_;
  int64_t worker_id_;
  common::ObAddr server_addr_;
  common::ObAddr client_addr_;
  common::ObAddr user_client_addr_;
  int64_t tenant_id_;
  int64_t effective_tenant_id_;
  char* tenant_name_;
  int64_t tenant_name_len_;
  int64_t user_id_;
  char* user_name_;
  int64_t user_name_len_;
  int user_group_;  // The cgroup id that the user belongs to, only displayed on the main thread
  uint64_t db_id_;
  char* db_name_;
  int64_t db_name_len_;
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  char* sql_;  // The memory is allocated by allocate_ and released when the record is eliminated;
  int64_t sql_len_;
  int64_t plan_id_;
  int64_t affected_rows_;  // The number of rows affected by delete, update, insert, and the number of rows selected by
                           // select
  int64_t return_rows_;
  int64_t partition_cnt_;        // The number of partitions involved in the request
  int64_t expected_worker_cnt_;  // px expected number of threads allocated
  int64_t used_worker_cnt_;      // px actual number of threads allocated
  int64_t try_cnt_;              // Number of attempts to execute
  ObPhyPlanType plan_type_;
  bool is_executor_rpc_;
  bool is_inner_sql_;
  bool is_hit_plan_cache_;
  bool is_multi_stmt_;  // Is it multi sql
  bool table_scan_;
  common::ObConsistencyLevel consistency_level_;
  int64_t request_memory_used_;
  ObExecTimestamp exec_timestamp_;
  ObExecRecord exec_record_;
  ObTableScanStat table_scan_stat_;
  ObSchedInfo sched_info_;  // px sched info
  int64_t ps_stmt_id_;
  int64_t request_type_;
  uint64_t trans_hash_;
  bool is_batched_multi_stmt_;
  ObString ob_trace_info_;
  uint64_t plan_hash_;
  int64_t trx_lock_for_read_elapse_;
};

}  // namespace sql
}  // namespace oceanbase
#endif
