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

#include "ob_trans_checkpoint_worker.h"
#include "storage/ob_partition_service.h"
#include "share/config/ob_server_config.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "lib/thread/ob_thread_name.h"

namespace oceanbase {
using namespace common;

namespace storage {

int ObTransCheckpointAdapter::init(ObPartitionService* ps)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
  } else {
    inited_ = true;
    partition_service_ = ps;
    STORAGE_LOG(INFO, "transaction checkpoint adapter init succ");
  }
  return ret;
}

int ObTransCheckpointAdapter::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(share::ObThreadPool::start())) {
    STORAGE_LOG(ERROR, "transaction checkpoint adapter start error", K(ret));
  } else {
    STORAGE_LOG(INFO, "transaction checkpoint adapter start");
  }
  return ret;
}

void ObTransCheckpointAdapter::stop()
{
  STORAGE_LOG(INFO, "transaction checkpoint adapter stop");
  share::ObThreadPool::stop();
}

void ObTransCheckpointAdapter::wait()
{
  STORAGE_LOG(INFO, "transaction checkpoint adapter wait");
  share::ObThreadPool::wait();
}

void ObTransCheckpointAdapter::destroy()
{
  if (inited_) {
    stop();
    wait();
    inited_ = false;
    STORAGE_LOG(INFO, "transaction checkpoint adapter destroy succ");
  }
}

void ObTransCheckpointAdapter::run1()
{
  int ret = OB_SUCCESS;
  int64_t total_time = 0;
  int64_t loop_count = 0;
  static const int64_t INTERVAL_US = 50 * 1000;  // 50ms
  bool need_checkpoint_all_trx = false;
  const int64_t max_elr_dependent_trx_cnt = GCONF._max_elr_dependent_trx_count;
  lib::set_thread_name("TransCheckpointWorker");

  int64_t loop_worker_stat_ts = 0;
  while (!has_set_stop()) {
    int64_t start_time = ObTimeUtility::current_time();
    int64_t valid_user_part_count = 0;
    int64_t valid_inner_part_count = 0;
    loop_count++;
    if (need_checkpoint_all_trx              // force checkpoint
        || GCONF.enable_one_phase_commit     // 1PC
        || max_elr_dependent_trx_cnt > 0) {  // ELR
      if (OB_FAIL(scan_all_partitions_(valid_user_part_count, valid_inner_part_count))) {
        STORAGE_LOG(WARN, "scan all partition fail", K(ret));
      }
    }
    int64_t cur_time = ObTimeUtility::current_time();
    int64_t time_used = cur_time - start_time;
    total_time += time_used;
    need_checkpoint_all_trx = false;

    if ((cur_time - loop_worker_stat_ts) > 5000000 /* 5s */) {
      STORAGE_LOG(INFO,
          "transaction checkpoint worker statistics",
          "avg_time",
          total_time / loop_count,
          K(valid_user_part_count),
          K(valid_inner_part_count));
      total_time = 0;
      loop_count = 0;
      loop_worker_stat_ts = cur_time;
      // full checkpoint operation in case of checkpoint leak
      //
      // temporary remove full checkpoint operation
      // need_checkpoint_all_trx = true;
    }

    if (time_used < INTERVAL_US) {
      usleep((uint32_t)(INTERVAL_US - time_used));
    }
  }
}

int ObTransCheckpointAdapter::scan_all_partitions_(int64_t& valid_user_part_count, int64_t& valid_inner_part_count)
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
    while (OB_SUCCESS == ret) {
      ObIPartitionGroup* partition = NULL;
      if (OB_FAIL(iter->get_next(partition))) {
        if (OB_ITER_END == ret) {
          // do nothing
        } else {
          STORAGE_LOG(WARN, "iterate next partition fail", K(ret));
        }
      } else if (OB_ISNULL(partition)) {
        ret = OB_PARTITION_NOT_EXIST;
        STORAGE_LOG(WARN, "iterate partition fail", K(partition));
      } else if (NULL == partition_service_->get_trans_service() ||
                 !is_working_state(partition->get_partition_state()) ||
                 NULL == partition->get_partition_loop_worker()) {
        // do nothing
      } else {
        ObPartitionLoopWorker* lp_worker = partition->get_partition_loop_worker();
        if (OB_FAIL(
                partition_service_->get_trans_service()->checkpoint(partition->get_partition_key(), 0, lp_worker))) {
          STORAGE_LOG(WARN, "checkpoint failed", K(ret), "pkey", partition->get_partition_key());
        } else {
          if (is_inner_table(partition->get_partition_key().get_table_id())) {
            valid_inner_part_count++;
          } else {
            valid_user_part_count++;
          }
        }
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

int ObTransCheckpointWorker::init(ObPartitionService* ps)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < MAX_CHECKPOINT_NUM; ++i) {
      if (OB_FAIL(checkpoint_adapters_[i].init(ps))) {
        STORAGE_LOG(ERROR, "transaction checkpoint adapter init error", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      inited_ = true;
      STORAGE_LOG(INFO, "transaction checkpoint worker thread init succ");
    }
  }
  return ret;
}

void ObTransCheckpointWorker::destroy()
{
  STORAGE_LOG(INFO, "transaction checkpoint worker thread begin destroy");
  if (inited_) {
    stop();
    wait();
    inited_ = false;
  }
  STORAGE_LOG(INFO, "transaction checkpoint worker thread destroy succ");
}

int ObTransCheckpointWorker::start()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_CHECKPOINT_NUM; ++i) {
    if (OB_FAIL(checkpoint_adapters_[i].start())) {
      STORAGE_LOG(ERROR, "transaction checkpoint adapter start error", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "transaction checkpoint worker thread start");
  }
  return ret;
}

void ObTransCheckpointWorker::stop()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_CHECKPOINT_NUM; ++i) {
    (void)checkpoint_adapters_[i].stop();
  }
  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "transaction checkpoint worker thread stop");
  }
  UNUSED(ret);
}

void ObTransCheckpointWorker::wait()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_CHECKPOINT_NUM; ++i) {
    (void)checkpoint_adapters_[i].wait();
  }
  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "transaction checkpoint worker thread wait");
  }
  UNUSED(ret);
}

}  // namespace storage
}  // namespace oceanbase
