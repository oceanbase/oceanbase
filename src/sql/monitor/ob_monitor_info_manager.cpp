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

#define USING_LOG_PREFIX SQL_MONITOR
#include "sql/monitor/ob_monitor_info_manager.h"
#include "sql/monitor/ob_phy_plan_monitor_info.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
ObMonitorInfoManager::ObMonitorInfoManager()
    : allocator_(), slow_query_queue_(), timer_(), elimination_task_(),
    plan_execution_time_map_(), max_push_interval_(OB_MAX_PUSH_INTERVAL),
    operator_info_size_(0)
{
  memory_limit_ = min(
      MAX_MEMORY_SIZE,
      static_cast<int64_t>(static_cast<double>(GMEMCONF.get_server_memory_avail()) * MONITOR_MEM_FACTOR));
}

ObMonitorInfoManager::~ObMonitorInfoManager()
{
  destroy();
}

int ObMonitorInfoManager::init()
{
  int ret = OB_SUCCESS;
  int64_t delay = 5 * 1000 * 1000;
  if (OB_FAIL(allocator_.init(memory_limit_, memory_limit_, PAGE_SIZE))) {
    LOG_WARN("fail to init allocator", K(ret));
  } else if (OB_FAIL(slow_query_queue_.init(ObModIds::OB_SQL_PLAN_MONITOR, OB_MAX_QUEUE_SIZE))) {
    LOG_WARN("fail to init history info", K(ret));
  } else if (OB_FAIL(timer_.init("MonInfoEvict"))) {
    LOG_WARN("fail to init timer", K(ret));
  } else if (OB_FAIL(elimination_task_.init(this))){
    LOG_WARN("fail to init elimination task", K(ret));
  } else if (OB_FAIL(plan_execution_time_map_.init(ObModIds::OB_SQL_PLAN_MONITOR))) {
    LOG_WARN("fail to init plan execution time map", K(ret));
  } else {
    allocator_.set_label(ObModIds::OB_SQL_PLAN_MONITOR);
    if (timer_.schedule(elimination_task_, delay, true)) {
      LOG_WARN("fail to schedule timer task", K(ret));
    }
  }
  return ret;
}

void ObMonitorInfoManager::destroy()
{
  IGNORE_RETURN plan_execution_time_map_.destroy();
  timer_.destroy();
  clear_queue(INT64_MAX);
  slow_query_queue_.destroy();
  allocator_.destroy();
}

int ObMonitorInfoManager::get_by_request_id(int64_t request_id,
                                            int64_t &index,
                                            ObPhyPlanMonitorInfo *&plan_info,
                                            Ref* ref)
{
  int ret = OB_SUCCESS;
  index = -1;
  if (OB_UNLIKELY(request_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(request_id));
  } else {
    int64_t start_idx = slow_query_queue_.get_pop_idx();
    int64_t end_idx = slow_query_queue_.get_push_idx();
    void *tmp_info = NULL;
    ret = OB_ERROR;
    for (int64_t i = start_idx; i <= end_idx && OB_ERROR == ret; i++) {
      if (NULL == (tmp_info = slow_query_queue_.get(i, ref))) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (request_id == static_cast<ObPhyPlanMonitorInfo*>(tmp_info)->get_request_id()) {
        plan_info = static_cast<ObPhyPlanMonitorInfo*>(tmp_info);
        index = i;
        ret = OB_SUCCESS;
        break;
      }
      if (NULL != ref) {
        slow_query_queue_.revert(ref);
      }
    }
  }
  return ret;
}

int ObMonitorInfoManager::get_by_index(int64_t index,
                                       ObPhyPlanMonitorInfo *&plan_info,
                                       Ref* ref)
{
  int ret = OB_SUCCESS;
  plan_info = NULL;
  if (OB_UNLIKELY(index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index));
  } else if (index >= get_start_index() + get_size()) {
    ret = OB_ITER_END;
  } else if (NULL == (plan_info = static_cast<ObPhyPlanMonitorInfo*>(slow_query_queue_.get(index, ref)))) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObMonitorInfoManager::is_info_nearly_duplicated(const ObAddr &addr, int64_t plan_id, bool &is_duplicated)
{
  int ret = OB_SUCCESS;
  is_duplicated = false;
  int64_t last_execution_time = 0;
  int64_t current_time = common::ObTimeUtility::current_time();
  PlanKey key;
  key.plan_id_ = plan_id;
  key.addr_ = addr;
  if (OB_FAIL(plan_execution_time_map_.get(key, last_execution_time))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(plan_execution_time_map_.insert(key, current_time))) {
        LOG_WARN("fail to insert into plan execution time map", K(ret), K(key.plan_id_));
      }
    } else {
      LOG_WARN("fail to get last execution time", K(ret));
    }
  } else {
    if (current_time - last_execution_time < max_push_interval_) {
      is_duplicated = true;
    }
  }
  return ret;
}

int ObMonitorInfoManager::add_monitor_info(ObPhyPlanMonitorInfo *info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info));
  } else {
    int64_t retry_times = 3;
    while (retry_times > 0) {
      retry_times --;
      int64_t &req_id = info->get_request_id();
      int64_t cur_operator_info_size = info->get_operator_info_memory_size();
      if (OB_FAIL(slow_query_queue_.push_with_imme_seq((void*)info, req_id))) {
        if (OB_SIZE_OVERFLOW == ret) {
          clear_queue(OB_BATCH_GC_COUNT);
        }
      } else {
        operator_info_size_ += cur_operator_info_size;
        LOG_DEBUG("add monitor info", K(*info));
        break;
      }
    }
    if (OB_FAIL(ret) && NULL != info) {
      free(info);
      info = NULL;
    }
  }
  return ret;
}

void ObMonitorInfoManager::clear_queue(int64_t limit)
{
  int64_t pop_cnt = 0;
  ObPhyPlanMonitorInfo *poped = NULL;
  while(pop_cnt++ < limit && NULL != (poped = (ObPhyPlanMonitorInfo*)slow_query_queue_.pop())) {
    operator_info_size_ -= poped->get_operator_info_memory_size();
    poped->destroy();
  }
}

int ObMonitorInfoManager::alloc(int64_t request_id,
                                ObPhyPlanMonitorInfo *&info)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_UNLIKELY(request_id < 0)) {
     ret = OB_INVALID_ARGUMENT;
     LOG_WARN("invalid argument", K(ret), K(request_id));
  } else if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObPhyPlanMonitorInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc memory", K(ret), K(request_id), K(allocator_.allocated()), K_(memory_limit));
  } else {
    info = new(ptr) ObPhyPlanMonitorInfo(allocator_);
    info->set_request_id(request_id);
  }
  return ret;
}

int ObMonitorInfoManager::free(ObPhyPlanMonitorInfo *&info)
{
  if (OB_ISNULL(info)) {
  } else {
    info->~ObPhyPlanMonitorInfo();
    allocator_.free(info);
    info = NULL;
  }
  return OB_SUCCESS;
}

int ObMonitorInfoManager::reclain_map()
{
  int ret = OB_SUCCESS;
  int64_t timestamp = ObTimeUtility::current_time();
  ReclainCond cond(timestamp, max_push_interval_);
  if (OB_FAIL(plan_execution_time_map_.remove_if(cond))) {
    LOG_WARN("fail to remove map", K(ret));
  }
  return ret;
}

int ObMonitorInfoManager::gc()
{
  int ret = OB_SUCCESS;
  int64_t allocated_size = allocator_.allocated() + operator_info_size_;
  int64_t mem_limit = memory_limit_ * 8 / 10;
  if (mem_limit < allocated_size) {
    int64_t pop_count = 3;
    while (mem_limit / 2 < allocator_.allocated() + operator_info_size_ && pop_count > 0) {
      clear_queue(OB_BATCH_GC_COUNT);
      pop_count --;
    }
  }
  if (OB_FAIL(reclain_map())) {
    LOG_WARN("fail to reclaim map", K(ret));
  }
  return ret;
}
} //namespace sql
} //namespace oceanbase


