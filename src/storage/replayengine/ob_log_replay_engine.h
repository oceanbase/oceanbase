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

#ifndef OCEANBASE_REPLAYENGINE_OB_LOG_REPLAY_ENGINE_
#define OCEANBASE_REPLAYENGINE_OB_LOG_REPLAY_ENGINE_

#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/allocator/ob_retire_station.h"
#include "lib/container/ob_se_array.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "common/ob_partition_key.h"
#include "common/ob_queue_thread.h"
#include "common/ob_range.h"
#include "clog/ob_partition_log_service.h"
#include "storage/ob_replay_status.h"
#include "storage/ob_storage_log_type.h"
#include "storage/replayengine/ob_i_log_replay_engine.h"

namespace oceanbase {
namespace transaction {
class ObTransService;
}

namespace clog {
class ObLogEntry;
}
namespace storage {
class ObPartitionService;
class ObIPartitionGroup;
class ObReplayStatus;
class ObReplayLogTask;
class ObReplayLogTaskQueue;
}  // namespace storage

namespace replayengine {
class ObLogReplayEngine : public ObILogReplayEngine, public lib::TGTaskHandler {
public:
  typedef storage::ObReplayLogTask ObReplayLogTask;
  ObLogReplayEngine();
  virtual ~ObLogReplayEngine();

public:
  virtual int init(transaction::ObTransService* trans_replay_service, storage::ObPartitionService* partition_service,
      const ObLogReplayEngineConfig& config);
  virtual int submit_replay_log_task_sequentially(const common::ObPartitionKey& pkey, const uint64_t log_id,
      const int64_t log_submit_ts, const bool need_replay, const clog::ObLogType log_type,
      const int64_t next_replay_log_ts);
  virtual int submit_replay_log_task_by_restore(
      const common::ObPartitionKey& pkey, const uint64_t log_id, const int64_t log_ts);

  virtual int add_partition(const common::ObPartitionKey& partition_key);
  virtual int remove_partition(const common::ObPartitionKey& pkey, storage::ObIPartitionGroup* partition);
  virtual int remove_partition(const common::ObPartitionKey& partition_key);
  virtual int reset_partition(const common::ObPartitionKey& partition_key);
  virtual int set_need_filter_trans_log(const common::ObPartitionKey& partition_key, const bool need_filter);

  virtual int is_replay_finished(const common::ObPartitionKey& partition_key, bool& is_finished) const;
  virtual int is_submit_finished(const common::ObPartitionKey& partition_key, bool& is_finished) const;
  virtual int check_can_receive_log(const common::ObPartitionKey& pkey, bool& can_receive_log) const;
  virtual int get_pending_submit_task_count(const common::ObPartitionKey& partition_key, int64_t& pending_count) const;
  virtual void handle(void* task);
  virtual int submit_task_into_queue(storage::ObReplayTask* task);
  virtual int is_tenant_out_of_memory(const common::ObPartitionKey& partition_key, bool& is_out_of_mem);
  virtual void stop();
  virtual void wait();
  virtual void destroy();

private:
  typedef common::ObSEArray<storage::ObReplayLogTaskEx, 16> ReplayLogTaskArray;
  struct ObTenantThreadKey {
  public:
    ObTenantThreadKey()
    {
      reset();
    }
    ObTenantThreadKey(const uint64_t tenant_id, const uint64_t thread_idx)
    {
      tenant_id_ = tenant_id;
      thread_idx_ = thread_idx;
    }
    ~ObTenantThreadKey()
    {
      reset();
    }
    bool operator==(const ObTenantThreadKey& other) const
    {
      return (tenant_id_ == other.tenant_id_ && thread_idx_ == other.thread_idx_);
    }
    uint64_t hash() const
    {
      uint64_t hash_val = 0;
      hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
      hash_val = murmurhash(&thread_idx_, sizeof(thread_idx_), hash_val);
      return hash_val;
    }
    void reset()
    {
      tenant_id_ = common::OB_INVALID_ID;
      thread_idx_ = common::OB_INVALID_ID;
    }
    TO_STRING_KV(K_(tenant_id), K_(thread_idx));

  public:
    uint64_t tenant_id_;
    uint64_t thread_idx_;
  };
  struct ObThrottleEndTime {
  public:
    ObThrottleEndTime()
    {
      reset();
    }
    ~ObThrottleEndTime()
    {
      reset();
    }
    void reset()
    {
      end_ts_ = 0;
    }
    TO_STRING_KV(K(end_ts_));

  public:
    union {
      int64_t end_ts_;      // Writing throttling end time
      int64_t fail_count_;  // count of consecutive failures
    };
  };

private:
  void add_task(storage::ObReplayStatus& replay_status, ObReplayLogTask& replay_task);
  void remove_task(storage::ObReplayStatus& replay_status, ObReplayLogTask& replay_task);
  void destroy_task(storage::ObReplayStatus& replay_status, ObReplayLogTask& replay_task);
  int push_task(storage::ObReplayStatus& replay_status, ObReplayLogTask& replay_task, uint64_t task_sign);
  void on_replay_error(ObReplayLogTask& replay_task, int ret);
  void on_replay_error();
  int get_log_replay_status(
      const common::ObPartitionKey& partition_key, uint64_t& start_log_id, uint64_t& min_unreplay_log_id);
  int submit_single_replay_task_(ObReplayLogTask& replay_task, storage::ObReplayStatus& replay_status);
  int submit_aggre_replay_task_(
      ObReplayLogTask& replay_task, storage::ObReplayStatus& replay_status, bool& is_first_task_pushed_in_aggr_log);

  int submit_trans_log_(ObReplayLogTask& replay_task, int64_t trans_id, storage::ObReplayStatus& replay_status);

  int submit_non_trans_log_(ObReplayLogTask& replay_task, storage::ObReplayStatus& replay_status);

  int check_condition_before_submit_non_trans_log_(ObReplayLogTask& replay_task);

  int prepare_replay_task_array_(
      ObReplayLogTask& replay_task, storage::ObReplayStatus& replay_status, ReplayLogTaskArray& task_array);

  int submit_aggre_trans_log_(
      storage::ObReplayStatus& replay_status, ReplayLogTaskArray& task_array, bool& is_first_task_pushed_in_aggr_log);
  void destroy_replay_task_array_(storage::ObReplayStatus& replay_status, ReplayLogTaskArray& task_array);

  bool is_valid_param(
      const common::ObPartitionKey& partition_key, const int64_t log_submit_timestamp, const uint64_t log_id) const;

  int handle_replay_log_task_(const common::ObPartitionKey& pkey, storage::ObIPartitionGroup* partition,
      storage::ObReplayLogTaskQueue* task_queue, bool& is_timeslice_run_out);

  int handle_submit_log_task_(const common::ObPartitionKey& pkey, storage::ObIPartitionGroup* partition,
      storage::ObSubmitReplayLogTask* submit_task, bool& is_timeslice_run_out);
  int fetch_and_submit_single_log_(const common::ObPartitionKey& pkey, storage::ObIPartitionGroup& partition,
      storage::ObReplayStatus& replay_status, storage::ObSubmitReplayLogTask* submit_task, uint64_t log_id,
      int64_t& accum_checksum, int64_t& submit_timestamp, int64_t& log_size, bool& is_first_task_pushed_in_aggr_log);
  int check_condition_after_fetch_log_(const common::ObPartitionKey& pkey, storage::ObSubmitReplayLogTask* submit_task,
      clog::ObLogEntry& log_entry, storage::ObStorageLogType& storage_log_type, bool& need_replay);
  int do_replay_task(storage::ObIPartitionGroup* partition, ObReplayLogTask* replay_task,
      const int64_t replay_queue_index, int64_t& table_version);
  void statistics_replay(const int64_t replay_task_used, const int64_t destroy_task_used, const int64_t retry_count);
  void statistics_submit(const int64_t submit_task_used, const int64_t log_size, const int64_t log_count);
  void process_replay_ret_code_(const int ret_code, const int64_t table_version, storage::ObReplayStatus& replay_status,
      storage::ObReplayLogTaskQueue& task_queue, ObReplayLogTask& replay_task);
  int pre_check_(const ObPartitionKey& pkey, storage::ObReplayStatus& replay_status, storage::ObReplayTask& task);
  int check_standby_cluster_schema_condition_(const common::ObPartitionKey& pkey, const int64_t table_version);

  int check_need_submit_current_log_(const ObPartitionKey& partition_key, storage::ObIPartitionGroup& partition,
      const uint64_t log_id, const int64_t log_submit_timestamp, storage::ObReplayStatus& replay_status,
      bool& can_submit);
  int check_need_replay_throttling_(const ObPartitionKey& pkey, storage::ObReplayTask& task,
      storage::ObReplayStatus& replay_status, bool& need_control);
  int update_replay_throttling_info_(const ObPartitionKey& pkey, storage::ObReplayStatus& replay_status);
  int update_replay_fail_info_(const ObPartitionKey& pkey, bool is_replay_succ);
  bool need_simple_thread_replay_table_(const uint64_t table_id) const;
  uint64_t get_replay_task_sign_(const ObPartitionKey& pkey, const int64_t trans_id) const;
  // ObThrottleEndTime corresponding to slot that 0 == tennat_id actully records the number of
  // consecutive failures of each thread, which is used to control cpu usage
  int get_throttle_end_time_(
      const uint64_t tenant_id, const bool create_if_not_exist_in_map, ObThrottleEndTime*& throttle_end_time);

private:
  const int64_t MAX_REPLAY_TIME_PER_ROUND = 10 * 1000;                             // 10ms
  const int64_t MAX_SUBMIT_TIME_PER_ROUND = 10 * 1000;                             // 10ms
  const int64_t TASK_QUEUE_WAIT_IN_GLOBAL_QUEUE_TIME_THRESHOLD = 5 * 1000 * 1000;  // 10s
  const int64_t REPLAY_TASK_WAIT_IN_QUEUE_TIME_THRESHOLD = 5 * 1000 * 1000;        // 5s
  const int64_t REPLAY_TASK_REPLAY_TIMEOUT = 50 * 1000;                            // 50ms
  const int64_t TASK_QUEUE_REPLAY_INTERVAL_ON_FAIL = 1000;                         // 1ms
  const int64_t FAILURE_TIME_THRESHOLD = 10 * 60 * 1000 * 1000;                    // 10m

  // Parameters of adaptive thread pool
  const int64_t LEAST_THREAD_NUM = 8;
  const int64_t ESTIMATE_TS = 200000;
  const int64_t EXPAND_RATE = 90;
  const int64_t SHRINK_RATE = 75;
  // For every 50 consecutive failures, sleep 500us
  const int64_t SLEEP_TRIGGER_CNT = 50;
  const int64_t SLEEP_INTERVAL = 500;  // sleep interval: 500us
  static const int64_t REPLAY_CONTROL_ARRAY_SIZE = 128 * 1024L;
  const uint64_t MAX_CACHE_TENANT_ID = 5000;

private:
  bool is_inited_;
  bool is_stopped_;
  int tg_id_;
  int64_t total_task_num_;
  transaction::ObTransService* trans_replay_service_;
  storage::ObPartitionService* partition_service_;
  common::ObSmallAllocator throttle_ts_allocator_;
  /*this map is used to record information of writing throttling, like waiting time point of each
   * thread for a certain tenant. At the same time, this map is reused, and the value obtained by
   * addressing with the key of (0, thread_id) records the number of consecutive replay failures
   * of the thread. It is used to control the CPU usage rate in scenarios such as writing throtting
   * or no memory scenarios, which will frequently placing tasks at the end of the queue. */
  common::ObLinearHashMap<ObTenantThreadKey, ObThrottleEndTime*> replay_control_map_;
  ObThrottleEndTime replay_control_array_[REPLAY_CONTROL_ARRAY_SIZE];

  DISALLOW_COPY_AND_ASSIGN(ObLogReplayEngine);
};

}  // namespace replayengine
}  // namespace oceanbase

#endif  // OCEANBASE_REPLAYENGINE_OB_LOG_REPLAY_ENGINE_
