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

#define USING_LOG_PREFIX SERVER

#include "ob_eliminate_task.h"
#include "ob_mysql_request_manager.h"
#include "lib/time/tbtimeutil.h"

using namespace oceanbase::obmysql;

ObEliminateTask::ObEliminateTask() : request_manager_(NULL), config_mem_limit_(0)
{}

ObEliminateTask::~ObEliminateTask()
{}

int ObEliminateTask::init(const ObMySQLRequestManager* request_manager)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(request_manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(request_manager_), K(ret));
  } else {
    request_manager_ = const_cast<ObMySQLRequestManager*>(request_manager);
    // can't call ObMySQLRequestManager::get_mem_limit for now, tenant not inited
    // set config_mem_limit_ to 64M
    config_mem_limit_ = 64 * 1024 * 1024;  // 64M
    disable_timeout_check();
  }
  return ret;
}

// Check whether the configuration item sql_audit_memory_limit is changed
int ObEliminateTask::check_config_mem_limit(bool& is_change)
{
  const int64_t MINIMUM_LIMIT = 64 * 1024 * 1024;    // at lease 64M
  const int64_t MAXIMUM_LIMIT = 1024 * 1024 * 1024;  // 1G maximum
  int ret = OB_SUCCESS;
  is_change = false;
  int64_t mem_limit = config_mem_limit_;
  if (OB_ISNULL(request_manager_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(request_manager_), K(ret));
  } else if (request_manager_->get_tenant_id() > OB_SYS_TENANT_ID &&
             request_manager_->get_tenant_id() <= OB_MAX_RESERVED_TENANT_ID) {
    // If the 50x tenant dosn't have a corresponding tenant schema,
    // the query configuration must fail
  } else if (OB_FAIL(ObMySQLRequestManager::get_mem_limit(request_manager_->get_tenant_id(), mem_limit))) {
    LOG_WARN("failed to get mem limit", K(ret));

    // if memory limit is not retrivable
    // overwrite error code, set mem config to default value
    // so that total memory use of sql audit can be limited
    ret = OB_SUCCESS;
    mem_limit = MAXIMUM_LIMIT;
  } else {
    // do nothing
  }
  if (config_mem_limit_ != mem_limit) {
    LOG_INFO("before change config mem", K(config_mem_limit_));
    if (mem_limit < MINIMUM_LIMIT) {
      if (lib::is_mini_mode()) {
        // do nothing
      } else {
        config_mem_limit_ = MINIMUM_LIMIT;
      }
    } else if (mem_limit > MAXIMUM_LIMIT) {
      config_mem_limit_ = MAXIMUM_LIMIT;
    } else {
      config_mem_limit_ = mem_limit;
    }
    is_change = true;
    LOG_INFO("after change config mem", K(config_mem_limit_));
  }
  return ret;
}

// Remaining memory elimination curve, when mem_limit is [64M, 100M],
// it is eliminated when there is 20M remaining memory;
//    When mem_limit is [100M, 5G], memory is more than mem_limit*0.2 and eliminated;
//    When mem_limit is at [5G, +∞], will be eliminated when the memory is 1G remaining;
// High and low water mark memory difference curve graph,
// when mem_limit is [64M, 100M], the memory difference is: 20M;
//    When mem_limit is [100M, 5G], the memory is poor: mem_limit*0.2;
//    When mem_limit is [5G, +∞], the memory difference is: 1G,
//        ______
//       /
// _____/
//   100M 5G
int ObEliminateTask::calc_evict_mem_level(int64_t& low, int64_t& high)
{
  int ret = OB_SUCCESS;
  const double HIGH_LEVEL_PRECENT = 0.80;
  const double LOW_LEVEL_PRECENT = 0.60;
  const double HALF_PRECENT = 0.50;
  const int64_t BIG_MEMORY_LIMIT = 5368709120;           // 5G
  const int64_t SMALL_MEMORY_LIMIT = 100 * 1024 * 1024;  // 100M
  const int64_t LOW_CONFIG = 64 * 1024 * 1024;           // 64M
  if (OB_ISNULL(request_manager_) || config_mem_limit_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(request_manager_), K(config_mem_limit_), K(ret));
  } else {
    if (config_mem_limit_ > BIG_MEMORY_LIMIT) {  // mem_limit > 5G
      high = config_mem_limit_ - static_cast<int64_t>(BIG_MEMORY_LIMIT * (1.0 - HIGH_LEVEL_PRECENT));
      low = config_mem_limit_ - static_cast<int64_t>(BIG_MEMORY_LIMIT * (1.0 - LOW_LEVEL_PRECENT));
    } else if (config_mem_limit_ >= LOW_CONFIG && config_mem_limit_ < SMALL_MEMORY_LIMIT) {  // 64M =< mem_limit < 100M
      high = config_mem_limit_ - static_cast<int64_t>(SMALL_MEMORY_LIMIT * (1.0 - HIGH_LEVEL_PRECENT));
      low = config_mem_limit_ - static_cast<int64_t>(SMALL_MEMORY_LIMIT * (1.0 - LOW_LEVEL_PRECENT));
    } else if (config_mem_limit_ < LOW_CONFIG) {  // mem_limit < 64M
      high = static_cast<int64_t>(static_cast<double>(config_mem_limit_) * HALF_PRECENT);
      low = 0;
    } else {
      high = static_cast<int64_t>(static_cast<double>(config_mem_limit_) * HIGH_LEVEL_PRECENT);
      low = static_cast<int64_t>(static_cast<double>(config_mem_limit_) * LOW_LEVEL_PRECENT);
    }
  }
  return ret;
}

void ObEliminateTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  common::ObConcurrentFIFOAllocator* allocator = NULL;
  int64_t evict_high_level = 0;
  int64_t evict_low_level = 0;
  bool is_change = false;
  if (OB_ISNULL(request_manager_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(request_manager_), K(ret));
  } else if (OB_FAIL(check_config_mem_limit(is_change))) {
    LOG_WARN("fail to check mem limit stat", K(ret));
  } else if (OB_FAIL(calc_evict_mem_level(evict_low_level, evict_high_level))) {
    LOG_WARN("fail to get sql audit evict memory level", K(ret));
  } else {
    allocator = request_manager_->get_allocator();
    if (OB_ISNULL(allocator)) {
      ret = OB_NOT_INIT;
      LOG_WARN("fail to get sql audit evict memory level", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t start_time = obsys::CTimeUtil::getTime();
    int64_t evict_batch_count = 0;
    // Eliminate by memory
    if (evict_high_level < allocator->allocated()) {
      LOG_INFO("sql audit evict mem start",
          K(evict_low_level),
          K(evict_high_level),
          "size_used",
          request_manager_->get_size_used(),
          "mem_used",
          allocator->allocated());
      int64_t last_time_allocated = allocator->allocated();
      while (evict_low_level < allocator->allocated()) {
        request_manager_->release_old();
        evict_batch_count++;
        if ((evict_low_level < allocator->allocated()) && (last_time_allocated == allocator->allocated())) {
          LOG_INFO("release old cannot free more memory");
          break;
        }
        last_time_allocated = allocator->allocated();
      }
    }
    // Eliminate according to the number of sql audit records
    if (request_manager_->get_size_used() > ObMySQLRequestManager::HIGH_LEVEL_EVICT_SIZE) {
      evict_batch_count = (request_manager_->get_size_used() - ObMySQLRequestManager::LOW_LEVEL_EVICT_SIZE) /
                          ObMySQLRequestManager::BATCH_RELEASE_COUNT;
      LOG_INFO("sql audit evict record start",
          "size_used",
          request_manager_->get_size_used(),
          "mem_used",
          allocator->allocated());
      for (int i = 0; i < evict_batch_count; i++) {
        request_manager_->release_old();
      }
    }
    // if sql_audit_memory_limit changed, need refresh total_limit_ in ObConcurrentFIFOAllocator;
    if (true == is_change) {
      allocator->set_total_limit(config_mem_limit_);
    }
    int64_t end_time = obsys::CTimeUtil::getTime();
    LOG_INFO("sql audit evict task end",
        K(evict_high_level),
        K(evict_batch_count),
        "elapse_time",
        end_time - start_time,
        "size_used",
        request_manager_->get_size_used(),
        "mem_used",
        allocator->allocated());
  }
}
