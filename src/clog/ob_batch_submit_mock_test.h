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

#ifndef OCEANBASE_CLOG_OB_BATCH_SUBMIT_MOCK_TEST_H_
#define OCEANBASE_CLOG_OB_BATCH_SUBMIT_MOCK_TEST_H_

#include "lib/utility/utility.h"
#include "common/ob_clock_generator.h"
#include "ob_clog_mgr.h"
#include "ob_i_submit_log_cb.h"
#include "ob_partition_log_service.h"
#include "storage/ob_storage_log_type.h"
#include "storage/ob_partition_service.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
namespace clog {
static int64_t on_success_cnt = 0;
static int64_t batch_committed_cnt = 0;
static int64_t batch_failed_cnt = 0;
static int64_t batch_submitted_cnt = 0;
static char mock_log_data[1000];

static void statistics()
{
  if (REACH_TIME_INTERVAL(1000 * 1000)) {
    CLOG_LOG(INFO,
        "ob_batch_submit_mock_test: statistics",
        K(on_success_cnt),
        K(batch_committed_cnt),
        K(batch_failed_cnt),
        K(batch_submitted_cnt));
  }
}

class MockSubmitCb : public ObISubmitLogCb {
public:
  MockSubmitCb()
  {}
  int on_success(const common::ObPartitionKey& partition_key, const clog::ObLogType log_type, const uint64_t log_id,
      const int64_t version, const bool batch_committed, const bool batch_last_succeed)
  {
    UNUSED(partition_key);
    UNUSED(log_type);
    UNUSED(log_id);
    UNUSED(version);
    UNUSED(batch_last_succeed);

    int ret = common::OB_SUCCESS;
    ATOMIC_INC(&on_success_cnt);
    if (batch_committed) {
      ATOMIC_INC(&batch_committed_cnt);
    } else {
      ATOMIC_INC(&batch_failed_cnt);
      CLOG_LOG(INFO, "MockSubmitCb batch_failed_cnt", K(partition_key), K(log_id));
    }
    op_reclaim_free(this);
    return ret;
  }
};

class ObBatchSubmitMockTest : public share::ObThreadPool {
public:
  const static int64_t SIG_START_STRESS = 57;
  const static int64_t THREAD_NUM = 1;
  const static int64_t MAX_SUBMIT_CNT = 100 * 1000 * 1000;
  const static int64_t MAX_PARTITION_NUM = 16;
  ObBatchSubmitMockTest()
  {}
  virtual ~ObBatchSubmitMockTest()
  {}

  static int register_signal_handler(
      storage::ObPartitionService* partition_service, ObICLogMgr* clog_mgr, const common::ObAddr& self)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(partition_service) || OB_ISNULL(clog_mgr) || !self.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid arguments");
    } else {
      partition_service_ = partition_service;
      clog_mgr_ = clog_mgr;
      self_ = self;
    }
    return ret;
  }

  void run1()
  {
    int ret = OB_SUCCESS;

    int64_t log_type = storage::OB_LOG_TEST;
    int64_t pos = 0;
    if (OB_FAIL(serialization::encode_i64(mock_log_data, sizeof(mock_log_data), pos, log_type))) {
      CLOG_LOG(WARN, "set type error", K(ret), K(log_type));
    }

    bool need_submit = true;
    while (!has_set_stop() && OB_SUCC(ret)) {
      if (need_submit) {
        for (int64_t i = 0; OB_SUCC(ret) && i < MAX_SUBMIT_CNT; i++) {
          ret = batch_submit_log();
          usleep(1);
          statistics();
        }
        need_submit = false;
      }
      statistics();
      usleep(1000);
    }
  }

  static void handle_signal()
  {
    CLOG_LOG(INFO, "ObBatchSubmitMockTest handle_signal");
    (new (std::nothrow) ObBatchSubmitMockTest())->start_stress();
  }

private:
  int batch_submit_log()
  {
    int ret = OB_SUCCESS;
    transaction::ObTransID trans_id(self_);
    common::ObPartitionArray partition_array;
    ObLogInfoArray log_info_array;
    ObISubmitLogCbArray cb_array;
    bool can_batch = false;

    if (OB_FAIL(get_partition_array(partition_array))) {
      CLOG_LOG(WARN, "get_partition_array failed", K(ret));
    } else if (OB_FAIL(get_log_info_array(partition_array, log_info_array))) {
      CLOG_LOG(WARN, "get_log_info_array failed", K(ret));
    } else if (OB_FAIL(get_cb_array(partition_array, cb_array))) {
      CLOG_LOG(WARN, "get_cb_array failed", K(ret));
    } else if (OB_FAIL(clog_mgr_->check_can_batch_submit(partition_array, can_batch))) {
      CLOG_LOG(WARN, "check_can_batch_submit failed", K(ret));
    } else if (can_batch && OB_FAIL(clog_mgr_->batch_submit_log(trans_id, partition_array, log_info_array, cb_array))) {
      CLOG_LOG(WARN, "batch_submit_log failed", K(ret));
    } else if (can_batch) {
      ATOMIC_INC(&batch_submitted_cnt);
    }
    return ret;
  }

  int get_partition_array(common::ObPartitionArray& partition_array)
  {
    int ret = common::OB_SUCCESS;
    storage::ObIPartitionGroup* partition = NULL;
    storage::ObIPartitionGroupIterator* partition_iter = NULL;
    if (NULL == (partition_iter = partition_service_->alloc_pg_iter())) {
      CLOG_LOG(WARN, "alloc_pg_iter failed");
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    }

    int cnt = 0;
    while (OB_SUCC(ret) && cnt < MAX_PARTITION_NUM) {
      if (OB_FAIL(partition_iter->get_next(partition)) || NULL == partition || !partition->is_valid()) {
        // do nothing
      } else {
        common::ObPartitionKey partition_key = partition->get_partition_key();
        if (OB_FAIL(partition_array.push_back(partition_key))) {
          CLOG_LOG(WARN, "partition_array push_back failed", K(ret));
        }
        cnt++;
      }
    }

    if (NULL != partition_iter) {
      partition_service_->revert_pg_iter(partition_iter);
      partition_iter = NULL;
    }
    return ret;
  }

  int get_log_info_array(const common::ObPartitionArray& partition_array, ObLogInfoArray& log_info_array)
  {
    int ret = OB_SUCCESS;

    for (int64_t index = 0; OB_SUCC(ret) && index < partition_array.count(); index++) {
      const common::ObPartitionKey partition_key = partition_array[index];
      const int64_t base_timestamp = ObClockGenerator::getClock();
      ;
      ObLogMeta log_meta;
      ObLogInfo log_info;
      ObIPartitionLogService* log_service = NULL;
      storage::ObIPartitionGroupGuard guard;

      if (OB_FAIL(partition_service_->get_partition(partition_key, guard)) || NULL == guard.get_partition_group() ||
          NULL == (log_service = guard.get_partition_group()->get_log_service())) {
        ret = OB_PARTITION_NOT_EXIST;
        CLOG_LOG(WARN, "invalid partition", K(ret), K(partition_key));
      } else if (!guard.get_partition_group()->is_valid()) {
        ret = OB_INVALID_PARTITION;
        CLOG_LOG(WARN, "partition is invalid", K(ret), K(partition_key));
      } else {
        while (common::OB_EAGAIN == (ret = log_service->get_log_id_timestamp(base_timestamp, log_meta)) ||
               common::OB_TOO_LARGE_LOG_ID == ret) {
          PAUSE();
          usleep(1);
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(log_info.set(mock_log_data, sizeof(mock_log_data), log_meta, true))) {
            CLOG_LOG(WARN, "log_info set failed", K(ret));
          } else if (OB_FAIL(log_info_array.push_back(log_info))) {
            CLOG_LOG(WARN, "log_info_array push_back failed", K(ret));
          }
        }
      }
    }

    return ret;
  }

  int get_cb_array(const common::ObPartitionArray& partition_array, ObISubmitLogCbArray& cb_array)
  {
    int ret = OB_SUCCESS;

    for (int64_t index = 0; OB_SUCC(ret) && index < partition_array.count(); index++) {
      MockSubmitCb* submit_cb = NULL;
      if (NULL == (submit_cb = op_reclaim_alloc(MockSubmitCb))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        CLOG_LOG(WARN, "op_reclaim_alloc failed", K(ret));
      } else if (OB_FAIL(cb_array.push_back(submit_cb))) {
        CLOG_LOG(WARN, "cb_array push_back failed", K(ret));
      }
    }

    return ret;
  }

  void start_stress()
  {
    set_thread_count(THREAD_NUM);
    ObThreadPool::start();
  }

  static storage::ObPartitionService* partition_service_;
  static ObICLogMgr* clog_mgr_;
  static common::ObAddr self_;
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_BATCH_SUBMIT_MOCK_TEST_H_
