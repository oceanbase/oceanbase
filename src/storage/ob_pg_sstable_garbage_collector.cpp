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

#define USING_LOG_PREFIX STORAGE

#include "ob_pg_sstable_garbage_collector.h"
#include "storage/ob_partition_service.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;

ObPGSSTableGCTask::ObPGSSTableGCTask()
{}

ObPGSSTableGCTask::~ObPGSSTableGCTask()
{}

void ObPGSSTableGCTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupIterator* partition_iter = nullptr;
  ObIPartitionGroup* partition_group = nullptr;
  int64_t left_recycle_cnt = ONE_ROUND_RECYCLE_COUNT_THRESHOLD;
  if (OB_ISNULL(partition_iter = ObPartitionService::get_instance().alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for partition group iterator", K(ret));
  } else {
    while (OB_SUCC(ret) && left_recycle_cnt > 0) {
      int64_t recycle_cnt = 0;
      if (OB_FAIL(partition_iter->get_next(partition_group))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next partition group", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (nullptr == partition_group) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys, partition group must not be null", K(ret));
      } else if (OB_FAIL(partition_group->recycle_unused_sstables(left_recycle_cnt, recycle_cnt))) {
        LOG_WARN("fail to recycle unsued sstable", K(ret));
      } else {
        left_recycle_cnt -= recycle_cnt;
      }
    }
  }
  if (nullptr != partition_iter) {
    ObPartitionService::get_instance().revert_pg_iter(partition_iter);
    partition_iter = nullptr;
  }
}

ObPGSSTableGarbageCollector::ObPGSSTableGarbageCollector() : is_inited_(false), timer_(), gc_task_()
{}

ObPGSSTableGarbageCollector::~ObPGSSTableGarbageCollector()
{}

int ObPGSSTableGarbageCollector::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPGSSTableGarbageCollector has already been inited", K(ret));
  } else if (OB_FAIL(timer_.init())) {
    LOG_WARN("fail to init sstable gc timer", K(ret));
  } else if (OB_FAIL(schedule_gc_task())) {
    LOG_WARN("fail to schedule gc task", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObPGSSTableGarbageCollector::schedule_gc_task()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(timer_.schedule(gc_task_, GC_INTERVAL_US, true /*repeat*/))) {
    LOG_WARN("fail to schedule gc task", K(ret));
  }
  return ret;
}

ObPGSSTableGarbageCollector& ObPGSSTableGarbageCollector::get_instance()
{
  static ObPGSSTableGarbageCollector instance;
  return instance;
}

void ObPGSSTableGarbageCollector::stop()
{
  timer_.stop();
}

void ObPGSSTableGarbageCollector::wait()
{
  timer_.wait();
}

void ObPGSSTableGarbageCollector::destroy()
{
  timer_.destroy();
}