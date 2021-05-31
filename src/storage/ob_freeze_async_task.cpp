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

#include "storage/ob_freeze_async_task.h"

#include "storage/ob_partition_service.h"

namespace oceanbase {

namespace storage {

int ObFreezeAsyncWorker::init(ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "init twice", K(ret));
  } else if (OB_FAIL(timer_.init())) {
    OB_LOG(ERROR, "fail to init timer", K(ret));
  } else if (OB_FAIL(async_task_.init(partition_service))) {
    OB_LOG(ERROR, "fail to init freeze async task", K(ret));
  } else {
    inited_ = true;
  }

  return ret;
}

int ObFreezeAsyncWorker::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(timer_.schedule(async_task_, EXEC_INTERVAL, true))) {
    OB_LOG(ERROR, "fail to schedule task", K(ret));
  }

  return ret;
}

void ObFreezeAsyncWorker::wait()
{
  timer_.wait();
}

void ObFreezeAsyncWorker::stop()
{
  timer_.stop();
}

void ObFreezeAsyncWorker::destroy()
{
  timer_.destroy();
}

void ObFreezeAsyncWorker::before_minor_freeze()
{
  async_task_.before_minor_freeze();
}

void ObFreezeAsyncWorker::after_minor_freeze()
{
  async_task_.after_minor_freeze();
}

int ObFreezeAsyncWorker::ObFreezeAsyncTask::init(ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(partition_service)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(partition_service));
  } else {
    partition_service_ = partition_service;
    inited_ = true;
  }

  return ret;
}

void ObFreezeAsyncWorker::ObFreezeAsyncTask::before_minor_freeze()
{
  ObLatchWGuard guard(latch_, ObLatchIds::FREEZE_ASYNC_WORKER_LOCK);
  in_freezing_ += 1;
}

void ObFreezeAsyncWorker::ObFreezeAsyncTask::after_minor_freeze()
{
  ObLatchWGuard guard(latch_, ObLatchIds::FREEZE_ASYNC_WORKER_LOCK);
  in_freezing_ -= 1;
  if (in_freezing_ == 0) {
    if (in_marking_dirty_) {
      needed_round_after_freeze_ = 2;
    } else {
      needed_round_after_freeze_ = 1;
    }
  }
}

void ObFreezeAsyncWorker::ObFreezeAsyncTask::before_marking_dirty_()
{
  ObLatchWGuard guard(latch_, ObLatchIds::FREEZE_ASYNC_WORKER_LOCK);
  in_marking_dirty_ = true;
}

void ObFreezeAsyncWorker::ObFreezeAsyncTask::after_marking_dirty_()
{
  ObLatchWGuard guard(latch_, ObLatchIds::FREEZE_ASYNC_WORKER_LOCK);

  if (in_freezing_ == 0 && !has_error_) {
    needed_round_after_freeze_ -= 1;
  }

  in_marking_dirty_ = false;
}

void ObFreezeAsyncWorker::ObFreezeAsyncTask::fetch_task_with_lock_(int& in_freezing, int& needed_round)
{
  ObLatchRGuard guard(latch_, ObLatchIds::FREEZE_ASYNC_WORKER_LOCK);
  in_freezing = in_freezing_;
  needed_round = needed_round_after_freeze_;
}

void ObFreezeAsyncWorker::ObFreezeAsyncTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int in_freezing = 0;
  int needed_round_after_freeze = 0;

  fetch_task_with_lock_(in_freezing, needed_round_after_freeze);

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (in_freezing == 0 &&               /*whether the freeze is ongoing*/
             needed_round_after_freeze <= 0 && /*whether another round is needed after freeze*/
             !has_error_ &&                    /*whether an error is happened in the last round*/
             all_cleared_ /*whether all transactions are marked*/) {
    // skip
  } else {
    bool has_error = false;
    STORAGE_LOG(INFO,
        "freeze async task running...",
        K(in_freezing),
        K(needed_round_after_freeze),
        K(has_error_),
        K(all_cleared_));
    DEBUG_SYNC(BEFORE_FREEZE_ASYNC_TASK);
    before_marking_dirty_();
    ObIPartitionGroupIterator* iter = NULL;
    all_cleared_ = true;

    if (NULL == (iter = partition_service_->alloc_pg_iter())) {
      has_error_ = true;
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "fail to alloc partition iter");
    } else {
      ObIPartitionGroup* partition = NULL;
      ObPartitionKey pkey;
      bool cleared = false;

      while (true) {
        ret = OB_SUCCESS;  // skip error, and continue to do next

        if (OB_FAIL(iter->get_next(partition))) {
          if (OB_ITER_END != ret) {
            has_error_ = true;
            STORAGE_LOG(WARN, "scan next partition failed.", K(ret));
          }
          break;
        } else if (OB_UNLIKELY(NULL == partition)) {
          has_error = true;
          ret = OB_PARTITION_NOT_EXIST;
          STORAGE_LOG(WARN, "get partition failed", K(ret));
        } else if (OB_FAIL(partition->mark_dirty_trans(cleared))) {
          has_error = true;
          STORAGE_LOG(WARN, "fail to mark dirty trans", K(ret), K(pkey));
        } else {
          all_cleared_ = all_cleared_ && cleared;
        }
      }

      partition_service_->revert_pg_iter(iter);
    }

    has_error_ = has_error;
    after_marking_dirty_();
  }
}

}  // namespace storage
}  // namespace oceanbase
