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

#include "ob_pg_memory_garbage_collector.h"
#include "storage/ob_i_partition_component_factory.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;

void ObPGMemoryGCTask::runTimerTask()
{
  ObPGMemoryGarbageCollector::get_instance().recycle();
}

ObPGMemoryGarbageCollector::ObPGMemoryGarbageCollector()
    : pg_list_(),
      pg_node_pool_(),
      allocator_(ObModIds::OB_PG_RECYCLE_NODE),
      lock_(),
      timer_(),
      gc_task_(),
      cp_fty_(nullptr),
      is_inited_(false)
{}

int ObPGMemoryGarbageCollector::init(ObIPartitionComponentFactory* cp_fty)
{
  int ret = OB_SUCCESS;
  char* buf = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPGMemoryGarbageCollector has already been inited", K(ret));
  } else if (nullptr == cp_fty) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(cp_fty));
  } else if (OB_ISNULL(
                 buf = static_cast<char*>(allocator_.alloc(sizeof(ObPGRecycleNode) * OB_MAX_PG_NUM_PER_SERVER)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < OB_MAX_PG_NUM_PER_SERVER; ++i) {
      ObPGRecycleNode* pg_node = new (buf + i * sizeof(ObPGRecycleNode)) ObPGRecycleNode();
      if (OB_FAIL(pg_node_pool_.push_back(pg_node))) {
        LOG_WARN("fail to push back pg node", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(timer_.init())) {
        LOG_WARN("fail to init timer", K(ret));
      } else if (OB_FAIL(timer_.schedule(gc_task_, GC_INTERVAL_US, true /*repeat*/))) {
        LOG_WARN("fail to schedule timer", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      cp_fty_ = cp_fty;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObPGMemoryGarbageCollector::recycle()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMemoryGarbageCollector has not been inited", K(ret));
  } else {
    int64_t left_recycle_cnt = 10000L;
    TCWLockGuard guard(lock_);
    DLIST_FOREACH_REMOVESAFE(curr, pg_list_)
    {
      ObIPartitionGroup* pg = curr->get_pg();
      bool can_free = false;
      int64_t recycle_cnt = 0;
      if (OB_FAIL(pg->recycle_unused_sstables(left_recycle_cnt, recycle_cnt))) {
        LOG_WARN("fail to recycle unused sstables", K(ret));
      } else if (OB_FAIL(pg->check_can_free(can_free))) {
        LOG_WARN("fail to check can free pg", K(ret));
      } else if (can_free) {
        FLOG_INFO("recycle pg", "pg_key", pg->get_partition_key());
        cp_fty_->free(pg);
        pg_list_.remove(curr);
        if (OB_FAIL(pg_node_pool_.push_back(curr))) {
          LOG_WARN("fail to push back pg node pool", K(ret));
        }
      }
    }
  }
  return ret;
}

void ObPGMemoryGarbageCollector::add_pg(ObIPartitionGroup* pg)
{
  int ret = OB_SUCCESS;
  if (nullptr == pg) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, pg must not be null", K(ret));
  } else {
    TCWLockGuard guard(lock_);
    ObPGRecycleNode* pg_node = nullptr;
    if (OB_FAIL(pg_node_pool_.pop_back(pg_node))) {
      LOG_WARN("fail to pop back pg node", K(ret));
    } else {
      pg_node->set_pg(pg);
      pg_list_.add_last(pg_node);
      FLOG_INFO("add recycle pg", "pg_key", pg->get_partition_key());
    }
  }
}

int ObPGMemoryGarbageCollector::check_tenant_pg_exist(const uint64_t tenant_id, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  TCRLockGuard guard(lock_);
  DLIST_FOREACH_X(curr, pg_list_, OB_SUCC(ret) && !is_exist)
  {
    ObIPartitionGroup* pg = curr->get_pg();
    is_exist = pg->get_partition_key().get_tenant_id() == tenant_id;
  }
  return ret;
}

ObPGMemoryGarbageCollector& ObPGMemoryGarbageCollector::get_instance()
{
  static ObPGMemoryGarbageCollector instance;
  return instance;
}

void ObPGMemoryGarbageCollector::stop()
{
  timer_.stop();
}

void ObPGMemoryGarbageCollector::wait()
{
  timer_.wait();
}

void ObPGMemoryGarbageCollector::destroy()
{
  timer_.destroy();
}
