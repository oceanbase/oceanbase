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

#include "ob_partition_worker.h"
#include "storage/ob_partition_service.h"
#include "share/config/ob_server_config.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "lib/thread/ob_thread_name.h"

namespace oceanbase {
using namespace common;

namespace storage {
int ObPartitionWorker::init(ObPartitionService* ps)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
  } else {
    partition_service_ = ps;
    inited_ = true;
    STORAGE_LOG(INFO, "Partition loop worker thread init succ");
  }
  return ret;
}

void ObPartitionWorker::destroy()
{
  STORAGE_LOG(INFO, "Partition loop worker thread begin destroy");
  if (inited_) {
    stop();
    wait();
    inited_ = false;
    partition_service_ = NULL;
  }
  STORAGE_LOG(INFO, "Partition loop worker thread destroy succ");
}

int ObPartitionWorker::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(share::ObThreadPool::start())) {
    STORAGE_LOG(ERROR, "Partition loop worker thread start error", K(ret));
  } else {
    STORAGE_LOG(INFO, "Partition loop worker thread start");
  }
  return ret;
}

void ObPartitionWorker::stop()
{
  STORAGE_LOG(INFO, "Partition loop worker thread stop");
  share::ObThreadPool::stop();
}

void ObPartitionWorker::wait()
{
  STORAGE_LOG(INFO, "Partition loop worker thread wait");
  share::ObThreadPool::wait();
}

void ObPartitionWorker::run1()
{
  int ret = OB_SUCCESS;
  int64_t total_time = 0;
  int64_t loop_count = 0;
  static const int64_t INTERVAL_US = 50 * 1000;  // 50ms
  lib::set_thread_name("PartWorker");

  int64_t dump_elr_statistic_ts = 0;
  int64_t loop_worker_stat_ts = 0;
  while (!has_set_stop()) {
    int64_t start_time = ObTimeUtility::current_time();
    int64_t valid_user_part_count = 0;
    int64_t valid_inner_part_count = 0;
    loop_count++;
    if (OB_FAIL(scan_all_partitions_(valid_user_part_count, valid_inner_part_count))) {
      STORAGE_LOG(WARN, "scan all partition fail", K(ret));
    }
    int64_t cur_time = ObTimeUtility::current_time();
    int64_t time_used = cur_time - start_time;
    total_time += time_used;

    if ((cur_time - loop_worker_stat_ts) > 5000000 /* 5s */) {
      STORAGE_LOG(INFO,
          "Partition loop worker statistics",
          "avg_time",
          total_time / loop_count,
          K(valid_user_part_count),
          K(valid_inner_part_count));
      total_time = 0;
      loop_count = 0;
      loop_worker_stat_ts = cur_time;
    }
    if ((cur_time - dump_elr_statistic_ts) > 60000000 /* 1min */) {
      if (OB_FAIL(partition_service_->get_trans_service()->dump_elr_statistic())) {
        STORAGE_LOG(WARN, "dump elr statistic fail", K(ret));
      } else {
        dump_elr_statistic_ts = cur_time;
      }
    }

    if (time_used < INTERVAL_US) {
      usleep((uint32_t)(INTERVAL_US - time_used));
    }
  }
}

// scan all partitions
int ObPartitionWorker::scan_all_partitions_(int64_t& valid_user_part_count, int64_t& valid_inner_part_count)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupIterator* iter = NULL;
  valid_user_part_count = 0;
  valid_inner_part_count = 0;

  if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition_service_ is NULL", KP_(partition_service));
  } else if (OB_ISNULL(iter = partition_service_->alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "fail to alloc partition scan iter", K(ret));
  } else {
    int64_t iter_partition_cnt = 0;
    int64_t already_sleep_time = 0;
    while (OB_SUCCESS == ret) {
      ObIPartitionGroup* partition = NULL;
      if (OB_FAIL(iter->get_next(partition))) {
        if (OB_ITER_END == ret) {
          // iteration completed
        } else {
          STORAGE_LOG(WARN, "iterate next partition fail", K(ret));
        }
      } else if (OB_ISNULL(partition)) {
        ret = OB_PARTITION_NOT_EXIST;
        STORAGE_LOG(WARN, "iterate partition fail", K(partition));
      } else if (OB_FAIL(partition_service_->do_partition_loop_work((*partition)))) {
        STORAGE_LOG(WARN, "handle partition fail", K(ret), "pkey", partition->get_partition_key());
      } else {
        if (is_inner_table(partition->get_partition_key().get_table_id())) {
          valid_inner_part_count++;
        } else {
          valid_user_part_count++;
        }
      }
      // Attention:
      // after a single machine can support millions of partitions,
      // ObPartitionLoopWorker will use one CPU core.
      // in order to reduce the overhead of this part, considering that only
      // gene_checkpoint_/replay_checkpoint_ need to be executed at high frequency,
      // the execution cycle of do_partition_loop_work is reduced to 15s
      // when neither elr nor dumping uncommited transaction is on
      iter_partition_cnt++;
      if (iter_partition_cnt >= DIVISION_PARTITION_CNT && !GCONF.enable_one_phase_commit &&
          GCONF._max_elr_dependent_trx_count == 0) {
        iter_partition_cnt = 0;
        int64_t usleep_total_time = 15 * 1000 * 1000L;
        const int64_t partition_cnt =
            std::max(DIVISION_PARTITION_CNT, ObPartitionService::get_instance().get_total_partition_cnt());
        if (GCONF.enable_log_archive && GCONF.log_archive_checkpoint_interval < usleep_total_time) {
          usleep_total_time = GCONF.log_archive_checkpoint_interval;
        }
        const int64_t sleep_time = std::min(std::max(usleep_total_time - already_sleep_time, 0L),
            usleep_total_time / ((partition_cnt + DIVISION_PARTITION_CNT - 1) / DIVISION_PARTITION_CNT));
        usleep(sleep_time);
        already_sleep_time += sleep_time;
      }
    }  // while

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  if (NULL != iter) {
    partition_service_->revert_pg_iter(iter);
    iter = NULL;
  }

  return ret;
}

}  // namespace storage
}  // namespace oceanbase
