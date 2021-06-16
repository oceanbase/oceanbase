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

#ifndef OCEANBASE_CLOG_OB_LOG_STRESS_
#define OCEANBASE_CLOG_OB_LOG_STRESS_

#include "lib/utility/utility.h"
#include "storage/ob_storage_log_type.h"
#include "ob_i_submit_log_cb.h"
#include "ob_partition_log_service.h"

namespace oceanbase {
namespace clog {
class FakeTask : public ObISubmitLogCb {
public:
  FakeTask(int64_t& finished_cnt, CountReporter& counter)
      : submit_time_(ObTimeUtility::current_time()), finished_cnt_(finished_cnt), counter_(counter)
  {}
  int on_success(const common::ObPartitionKey& partition_key, const clog::ObLogType log_type, const uint64_t log_id,
      const int64_t version, const bool batch_committed, const bool batch_last_succeed)
  {
    UNUSED(log_type);
    UNUSED(batch_committed);
    UNUSED(batch_last_succeed);
    int ret = common::OB_SUCCESS;
    CLOG_LOG(DEBUG, "submit_log success", K(partition_key), K(log_id), K(version));
    (void)ATOMIC_FAA(&finished_cnt_, 1);
    counter_.inc(submit_time_);
    op_reclaim_free(this);
    return ret;
  }
  void reset_submit_time()
  {
    submit_time_ = ObTimeUtility::current_time();
  }

private:
  int64_t submit_time_;
  int64_t& finished_cnt_;
  CountReporter& counter_;
};

class ObLogStressRunnable : public share::ObThreadPool {
public:
  enum { SIG_START_STRESS = 57 };
  ObLogStressRunnable() : submit_cnt_(0), finished_cnt_(0), counter_("submit", 1000000)
  {}
  virtual ~ObLogStressRunnable()
  {}
  static int register_signal_handler(storage::ObPartitionService* partition_service, ObILogEngine* log_engine)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(partition_service)) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      partition_service_ = partition_service;
      log_engine_ = log_engine;
      // signal(SIG_START_STRESS, handle_signal);
    }
    return ret;
  }
  void run1()
  {
    int ret = common::OB_SUCCESS;
    int64_t batch_cnt = 128;
    int64_t queue_limit = 8192 * 32;
    int64_t start = 0;
    static int64_t idx = 10;

    // bind_core();
    ObIPartitionLogService* pls = get_pls(ATOMIC_FAA(&idx, 1));
    if (OB_ISNULL(pls)) {
      ret = common::OB_ERR_UNEXPECTED;
    }
    while (!has_set_stop() && OB_SUCC(ret)) {
      usleep(1);
      if ((start = ATOMIC_LOAD(&submit_cnt_)) + batch_cnt > ATOMIC_LOAD(&finished_cnt_) + queue_limit) {
        PAUSE();
      } else {
        start = ATOMIC_FAA(&submit_cnt_, batch_cnt);
        for (int64_t i = 0; OB_SUCC(ret) && i < batch_cnt; i++) {
          ret = submit_log(pls);
          if (OB_FAIL(ret)) {
            CLOG_LOG(ERROR, "submit_log fail:", K(ret));
            ret = OB_SUCCESS;  // for test, just continue
          }
          usleep(10);
        }
      }
    }
  }

private:
  void start_stress()
  {
    set_thread_count(32);
    (void)start();
  }
  ObIPartitionLogService* get_pls(int64_t idx)
  {
    int ret = common::OB_SUCCESS;
    storage::ObIPartitionGroup* partition = NULL;
    storage::ObIPartitionGroupIterator* partition_iter = NULL;
    ObIPartitionLogService* pls = NULL;
    if (OB_ISNULL(partition_service_)) {
      ret = OB_NOT_INIT;
      CLOG_LOG(ERROR, "partition_service_ is null");
    } else if (NULL == (partition_iter = partition_service_->alloc_pg_iter())) {
      CLOG_LOG(WARN, "alloc_pg_iter failed");
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    }
    const uint64_t my_table_id = 1099511630777;
    static const bool my_table = false;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(partition_iter->get_next(partition))) {
        // do nothing
      } else if (!partition->is_valid() || (NULL == (pls = partition->get_log_service()))) {
        // do nothing
      } else if (my_table && (0 == idx)) {
        // disable my_table
        if (my_table_id == partition->get_partition_key().get_table_id()) {
          CLOG_LOG(INFO, "get my_table_id", "pkey", partition->get_partition_key());
          break;
        }
      } else if (idx-- == 0) {
        break;
      }
    }
    if (NULL != partition) {
      const ObPartitionKey& pkey = partition->get_partition_key();
      CLOG_LOG(INFO, "log_stress on partition", K(pkey));
    } else {
      CLOG_LOG(WARN, "partition is null");
    }
    if (NULL != partition_iter) {
      partition_service_->revert_pg_iter(partition_iter);
    }
    return pls;
  }

  int submit_log(ObIPartitionLogService* pls)
  {
    int ret = common::OB_SUCCESS;
    static common::ObVersion version;
    static int64_t total_count = 0;
    const int64_t data_len = 32;
    char data[data_len];
    int64_t log_type = storage::OB_LOG_TEST;
    int64_t pos = 0;
    uint64_t log_id = 0;
    int64_t log_timestamp = 0;
    const int64_t base_timestamp = 0;
    const bool is_trans_log = false;
    FakeTask* task = NULL;
    if (OB_ISNULL(pls)) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      if (OB_ISNULL(task = op_reclaim_alloc_args(FakeTask, finished_cnt_, counter_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        if (ATOMIC_FAA(&total_count, 1) % 70 == 0) {
          log_type = storage::OB_LOG_MAJOR_FREEZE;
          version.major_ += 1;
        }
        if (OB_FAIL(serialization::encode_i64(data, data_len, pos, log_type))) {
          CLOG_LOG(WARN, "set type error", K(ret), K(log_type), K(version));
        }
        if (OB_SUCC(ret)) {
          while (
              common::OB_EAGAIN ==
              (ret = pls->submit_log(data, sizeof(data), base_timestamp, task, is_trans_log, log_id, log_timestamp))) {
            PAUSE();
            usleep(1);
            task->reset_submit_time();
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      if (NULL != task) {
        op_reclaim_free(task);
        task = NULL;
      }
    }
    return ret;
  }
  static void handle_signal(int sig)
  {
    UNUSED(sig);
    CLOG_LOG(INFO, "start_stress");
    (new (std::nothrow) ObLogStressRunnable())->start_stress();
  }

private:
  int64_t submit_cnt_ CACHE_ALIGNED;
  int64_t finished_cnt_ CACHE_ALIGNED;
  CountReporter counter_;
  static storage::ObPartitionService* partition_service_;
  static clog::ObILogEngine* log_engine_;
};
storage::ObPartitionService* ObLogStressRunnable::partition_service_ = NULL;
clog::ObILogEngine* ObLogStressRunnable::log_engine_ = NULL;

};  // end namespace clog
};  // end namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_STRESS_
