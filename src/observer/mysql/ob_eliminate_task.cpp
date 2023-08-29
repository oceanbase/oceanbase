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

using namespace oceanbase::obmysql;

ObEliminateTask::ObEliminateTask()
    :request_manager_(NULL),
     config_mem_limit_(0)
{

}

ObEliminateTask::~ObEliminateTask()
{

}

int ObEliminateTask::init(const ObMySQLRequestManager *request_manager)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(request_manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(request_manager_), K(ret));
  } else {
    request_manager_ = const_cast<ObMySQLRequestManager*>(request_manager);
    // can't call ObMySQLRequestManager::get_mem_limit for now, tenant not inited
    // set config_mem_limit_ to 16M
    config_mem_limit_ = 16 * 1024 * 1024; // 16M
    common::ObConcurrentFIFOAllocator  *allocator = request_manager_->get_allocator();
    if (OB_ISNULL(allocator)) {
      ret = OB_NOT_INIT;
      LOG_WARN("request manager allocator not init", K(ret));
    } else {
      allocator->set_total_limit(config_mem_limit_);
    }
    disable_timeout_check();
  }
  return ret;
}

// 检查配置内存限时是否更改：mem_limit = tenant_mem_limit * ob_sql_audit_percentage
int ObEliminateTask::check_config_mem_limit(bool &is_change)
{
  const int64_t MINIMUM_LIMIT = 64 * 1024 * 1024;   // at lease 64M
  const int64_t MAXIMUM_LIMIT = 1024 * 1024 * 1024; // 1G maximum
  int ret = OB_SUCCESS;
  is_change = false;
  int64_t mem_limit = config_mem_limit_;
  int64_t tenant_id = OB_INVALID_TENANT_ID;
  if (OB_ISNULL(request_manager_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(request_manager_), K(ret));
  } else if (FALSE_IT(tenant_id = request_manager_->get_tenant_id())) {
  } else if (tenant_id > OB_SYS_TENANT_ID && tenant_id <= OB_MAX_RESERVED_TENANT_ID) {
    // 50x租户在没有对应的tenant schema，查询配置一定失败
    // do nothing
  } else if (OB_FAIL(ObMySQLRequestManager::get_mem_limit(tenant_id, mem_limit))) {
    // if memory limit is not retrivable
    // overwrite error code, not change the size of config_mem_limit_
    LOG_WARN("failed to get mem limit", K(ret), K(tenant_id), K(mem_limit), K(config_mem_limit_));
    ret = OB_SUCCESS;
  } else {
    if (config_mem_limit_ != mem_limit) {
      LOG_INFO("change config mem limit", K(config_mem_limit_), K(mem_limit), K(tenant_id));
      bool use_mini_mem = lib::is_mini_mode() || MTL_IS_MINI_MODE() || is_meta_tenant(tenant_id);
      config_mem_limit_ = mem_limit;
      if (mem_limit < MINIMUM_LIMIT && !use_mini_mem) {
        config_mem_limit_ = MINIMUM_LIMIT;
      }
      is_change = true;
    }
  }
  return ret;
}

//剩余内存淘汰曲线图,当mem_limit在[64M, 100M]时, 内存剩余20M时淘汰;
//               当mem_limit在[100M, 5G]时, 内存甚于mem_limit*0.2时淘汰;
//               当mem_limit在[5G, +∞]时, 内存剩余1G时淘汰;
//高低水位线内存差曲线图，当mem_limit在[64M, 100M]时, 内存差为:20M;
//                        当mem_limit在[100M, 5G]时，内存差：mem_limit*0.2;
//                        当mem_limit在[5G, +∞]时, 内存差是：1G,
//        ______
//       /
// _____/
//   100M 5G
int ObEliminateTask::calc_evict_mem_level(int64_t &low, int64_t &high)
{
  int ret = OB_SUCCESS;
  const double HIGH_LEVEL_PRECENT = 0.80;
  const double LOW_LEVEL_PRECENT = 0.60;
  const double HALF_PRECENT = 0.50;
  const int64_t BIG_MEMORY_LIMIT = 5368709120; //5G
  const int64_t SMALL_MEMORY_LIMIT = 100*1024*1024; //100M
  const int64_t LOW_CONFIG = 64*1024*1024; //64M
  if (OB_ISNULL(request_manager_) || config_mem_limit_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(request_manager_), K(config_mem_limit_), K(ret));
  } else {
    if (config_mem_limit_ > BIG_MEMORY_LIMIT) { // mem_limit > 5G
      high = config_mem_limit_ - static_cast<int64_t>(BIG_MEMORY_LIMIT * (1.0 - HIGH_LEVEL_PRECENT));
      low = config_mem_limit_ - static_cast<int64_t>(BIG_MEMORY_LIMIT * (1.0 - LOW_LEVEL_PRECENT)) ;
    } else if (config_mem_limit_ >= LOW_CONFIG &&  config_mem_limit_  < SMALL_MEMORY_LIMIT) { // 64M =< mem_limit < 100M
      high = config_mem_limit_ - static_cast<int64_t>(SMALL_MEMORY_LIMIT * (1.0 - HIGH_LEVEL_PRECENT));
      low = config_mem_limit_ - static_cast<int64_t>(SMALL_MEMORY_LIMIT * (1.0 - LOW_LEVEL_PRECENT));
    } else if (config_mem_limit_ < LOW_CONFIG) { //mem_limit < 64M
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
  common::ObConcurrentFIFOAllocator *allocator = NULL;
  bool is_change = false;
  int64_t release_cnt = 0;
  int64_t evict_high_mem_level = 0;
  int64_t evict_low_mem_level = 0;
  int64_t evict_high_size_level = 0;
  int64_t evict_low_size_level = 0;
  flt_mgr_ = MTL(ObFLTSpanMgr*);
  if (flt_mgr_->get_size() > (ObFLTSpanMgr::MAX_QUEUE_SIZE-ObFLTSpanMgr::RELEASE_QUEUE_SIZE)) {
    for (int i = 0; i < ObFLTSpanMgr::RELEASE_QUEUE_SIZE/ObFLTSpanMgr::BATCH_RELEASE_COUNT; i++) {
      flt_mgr_->release_old(ObFLTSpanMgr::BATCH_RELEASE_COUNT);
    }
  }
  if (OB_ISNULL(request_manager_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(request_manager_), K(ret));
  } else if (OB_FAIL(check_config_mem_limit(is_change))) {
    LOG_WARN("fail to check mem limit stat", K(ret));
  } else if (OB_FAIL(calc_evict_mem_level(evict_low_mem_level, evict_high_mem_level))) {
    LOG_WARN("fail to get sql audit evict memory level", K(ret));
  } else {
    int64_t queue_size = request_manager_->get_capacity();
    bool use_mini_queue = lib::is_mini_mode() || MTL_IS_MINI_MODE()
                          || is_meta_tenant(request_manager_->get_tenant_id());
    release_cnt = use_mini_queue
                  ? ObMySQLRequestManager::MINI_MODE_BATCH_RELEASE_SIZE
                  : ObMySQLRequestManager::BATCH_RELEASE_SIZE;
    evict_high_size_level = queue_size * ObMySQLRequestManager::HIGH_LEVEL_EVICT_PERCENTAGE;
    evict_low_size_level = queue_size * ObMySQLRequestManager::LOW_LEVEL_EVICT_PERCENTAGE;
    allocator = request_manager_->get_allocator();
    if (OB_ISNULL(allocator)) {
      ret = OB_NOT_INIT;
      LOG_WARN("fail to get sql audit evict memory level", K(ret));
    }
    if (OB_SUCC(ret) && REACH_TIME_INTERVAL(30 * 1000 * 1000)) { // 30s delay
      LOG_INFO("Eliminate task evict sql audit",
          K(request_manager_->get_tenant_id()), K(queue_size), K(config_mem_limit_),
          K(request_manager_->get_size_used()), K(evict_high_size_level), K(evict_low_size_level),
          K(allocator->allocated()), K(evict_high_mem_level), K(evict_low_mem_level));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t start_time = ObTimeUtility::current_time();
    int64_t evict_batch_count = 0;
    //按内存淘汰
    if (evict_high_mem_level < allocator->allocated()) {
      LOG_INFO("sql audit evict mem start",
               K(request_manager_->get_tenant_id()),
               K(evict_low_mem_level),
               K(evict_high_mem_level),
               "size_used",request_manager_->get_size_used(),
               "mem_used", allocator->allocated());
      int64_t last_time_allocated = allocator->allocated();
      while (evict_low_mem_level < allocator->allocated()) {
        request_manager_->release_old(release_cnt);
        evict_batch_count++;
        if ((evict_low_mem_level < allocator->allocated()) && (last_time_allocated == allocator->allocated())) {
          LOG_INFO("release old cannot free more memory");
          break;
        }
        last_time_allocated = allocator->allocated();
      }
    }
    //按记录数淘汰
    if (request_manager_->get_size_used() > evict_high_size_level) {
      evict_batch_count = (request_manager_->get_size_used() - evict_low_size_level) / release_cnt;
      LOG_INFO("sql audit evict record start",
               K(request_manager_->get_tenant_id()),
               K(evict_high_size_level),
               K(evict_low_size_level),
               "size_used",request_manager_->get_size_used(),
               "mem_used", allocator->allocated());
      for (int i = 0; i < evict_batch_count; i++) {
        request_manager_->release_old(release_cnt);
      }
    }
    //如果sql_audit_memory_limit改变, 则需要将ObConcurrentFIFOAllocator中total_limit_更新;
    if (true == is_change) {
      allocator->set_total_limit(config_mem_limit_);
    }
    int64_t end_time = ObTimeUtility::current_time();
    LOG_INFO("sql audit evict task end",
             K(request_manager_->get_tenant_id()),
             K(evict_high_mem_level),
             K(evict_high_size_level),
             K(evict_batch_count),
             "elapse_time", end_time - start_time,
             "size_used",request_manager_->get_size_used(),
             "mem_used", allocator->allocated());
  }
}
