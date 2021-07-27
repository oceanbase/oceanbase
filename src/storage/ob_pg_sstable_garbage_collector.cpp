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
  disable_timeout_check();
  if (OB_FAIL(ObPGSSTableGarbageCollector::get_instance().gc_free_sstable())) {
    LOG_WARN("fail to gc free sstable", K(ret));
  }
}

ObPGSSTableGarbageCollector::ObPGSSTableGarbageCollector()
  : is_inited_(false), timer_(), gc_task_(), free_sstables_queue_(), do_gc_cnt_by_queue_(0)
{
}

ObPGSSTableGarbageCollector::~ObPGSSTableGarbageCollector()
{
}

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

int ObPGSSTableGarbageCollector::gc_free_sstable()
{
  int ret = OB_SUCCESS;
  if (DO_ONE_ROUND_PG_ITER_RECYCLE_THRESHOLD == do_gc_cnt_by_queue_) {
    if (OB_FAIL(gc_free_sstable_by_pg_iter())) {
      LOG_WARN("fail to gc free sstable by pg iter", K(ret));
    } else {
      do_gc_cnt_by_queue_ = 0;
    }
  } else if (OB_FAIL(gc_free_sstable_by_queue())) {
    LOG_WARN("fail to gc free sstable by queue", K(ret));
  } else {
    do_gc_cnt_by_queue_++;
  }
  return ret;
}

int ObPGSSTableGarbageCollector::gc_free_sstable_by_pg_iter()
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupIterator *partition_iter = nullptr;
  ObIPartitionGroup *partition_group = nullptr;
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

  LOG_INFO("do one gc free sstable by pg iter", K(ret), "free sstable cnt",
      ONE_ROUND_RECYCLE_COUNT_THRESHOLD - left_recycle_cnt);
  return ret;
}

int ObPGSSTableGarbageCollector::gc_free_sstable_by_queue()
{
  int ret = OB_SUCCESS;
  int64_t left_recycle_cnt = ONE_ROUND_RECYCLE_COUNT_THRESHOLD;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGSSTableGarbageCollector not init", K(ret));
  } else {
    while (OB_SUCC(ret) && left_recycle_cnt > 0 && free_sstables_queue_.size() > 0) {
      ObLink *ptr = NULL;
      if (OB_FAIL(free_sstables_queue_.pop(ptr))) {
        LOG_WARN("fail to pop item", K(ret));
      } else if (OB_ISNULL(ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, ptr is nullptr", K(ret), KP(ptr));
      } else {
        ObSSTableGCItem *item = static_cast<ObSSTableGCItem *>(ptr);
        ObIPartitionGroupGuard pg_guard;
        ObIPartitionGroup *pg = NULL;
        if (OB_UNLIKELY(!item->is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, ptr is nullptr", K(ret), K(*item));
        } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(item->key_.pkey_,
                                                                            pg_guard))) {
          if (OB_PARTITION_NOT_EXIST == ret) {
            // skip not exist partition in pg mgr, because the sstables of this partition will be
            // gc by ObPGMemoryGarbageCollector.
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get partition", K(ret), "pkey", item->key_.pkey_);
          }
        } else if (OB_ISNULL(pg = pg_guard.get_partition_group())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, pg is nullptr", K(ret), KP(pg), "pkey", item->key_.pkey_);
        } else if (OB_FAIL(pg->recycle_sstable(item->key_))) {
          LOG_WARN("fail to recycle sstable", K(ret), "table key", item->key_);
        } else {
          left_recycle_cnt--;
        }

        if (OB_FAIL(ret) && OB_NOT_NULL(item)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = free_sstables_queue_.push(item))) {
            LOG_WARN("fail to push into free sstables queue", K(tmp_ret), K(ret), K(*item));
          } else {
            item = NULL;
          }
        }

        if (OB_NOT_NULL(item)) {
          free_sstable_gc_item(item);
        }
      }
    }
  }

  LOG_INFO("do one gc free sstable by queue", K(ret), "free sstable cnt",
      ONE_ROUND_RECYCLE_COUNT_THRESHOLD - left_recycle_cnt);
  return ret;
}

int ObPGSSTableGarbageCollector::push_sstable_into_gc_queue(ObITable::TableKey &key)
{
  int ret = OB_SUCCESS;
  ObSSTableGCItem *item = NULL;
  if (OB_FAIL(alloc_sstable_gc_item(item))) {
    LOG_WARN("fail to allocate sstable gc item", K(ret));
  } else if (OB_ISNULL(item))  {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, item is nullptr", K(ret), KP(item));
  } else if (FALSE_IT(item->key_ = key)) {
  } else if (OB_FAIL(push_sstable_gc_item(item))) {
    LOG_WARN("fail to push sstable gc item", K(ret), K(*item));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(item)) {
      free_sstable_gc_item(item);
    }
  }
  return ret;
}

int ObPGSSTableGarbageCollector::alloc_sstable_gc_item(ObSSTableGCItem *&item)
{
  int ret = OB_SUCCESS;
  int64_t size = sizeof(ObSSTableGCItem);
  ObMemAttr attr(common::OB_SERVER_TENANT_ID, "SSTableGCItem");
  if (OB_ISNULL(item = (ObSSTableGCItem *)ob_malloc(size, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_WARN("no memory, failed to allocate sstable gc item", K(ret), K(size));
    }
  }
  return ret;
}

int ObPGSSTableGarbageCollector::push_sstable_gc_item(const ObSSTableGCItem *item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(item)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), KP(item));
  } else if (OB_FAIL(free_sstables_queue_.push((ObLink *)item))) {
    LOG_WARN("fail to push back into free sstable queue", K(ret), K(*item));
  }
  return ret;
}

void ObPGSSTableGarbageCollector::free_sstable_gc_item(const ObSSTableGCItem *item)
{
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    tmp_ret = OB_NOT_INIT;
    LOG_ERROR("ObPGSSTableGarbageCollector not init", K(tmp_ret));
  } else if (OB_ISNULL(item)) {
    tmp_ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(tmp_ret), KP(item));
  } else {
    ob_free((void *)item);
  }
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
