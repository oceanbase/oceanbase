// Copyright 2010-2016 Alibaba Inc. All Rights Reserved.
// Author:
//   zhenling.zzg
// this file defines implementation of sql plan manager


#define USING_LOG_PREFIX SQL

#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/compress/zlib/zlib_src/zlib.h"
#include "sql/session/ob_sql_session_info.h"
#include "common/object/ob_object.h"
#include "lib/thread/thread_mgr.h"
#include "ob_sql_plan_manager.h"
#include "lib/ob_running_mode.h"
#include "util/easy_time.h"
#include "lib/rc/ob_rc.h"

namespace oceanbase
{
namespace sql
{


ObSqlPlanMgr::ObSqlPlanMgr()
  :allocator_(),
  task_(),
  plan_table_mgrs_(),
  plan_item_mgr_(nullptr),
  plan_real_info_mgr_(nullptr),
  destroyed_(false),
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  tg_id_(-1)
{
}

ObSqlPlanMgr::~ObSqlPlanMgr()
{
  if (inited_) {
    destroy();
  }
}

int ObSqlPlanMgr::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  tenant_id_ = tenant_id;
  int64_t queue_size = lib::is_mini_mode() ?
                        MINI_MODE_MAX_QUEUE_SIZE : MAX_QUEUE_SIZE;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(allocator_.init(SQL_PLAN_PAGE_SIZE,
                                     ObModIds::OB_SQL_PLAN,
                                     tenant_id,
                                     INT64_MAX))) {
    SERVER_LOG(WARN, "failed to init allocator", K(ret));
  } else if (OB_ISNULL(buf=alloc(sizeof(ObPlanItemMgr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for ObPlanItemMgr", K(ret));
  } else if (OB_FALSE_IT(plan_item_mgr_=new(buf)ObPlanItemMgr(get_allocator()))) {
  } else if (OB_FAIL(plan_item_mgr_->init(tenant_id, queue_size))) {
    LOG_WARN("failed to init plan item manager", K(ret));
  } else if (OB_ISNULL(buf=alloc(sizeof(ObPlanRealInfoMgr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for ObPlanRealInfoMgr", K(ret));
  } else if (OB_FALSE_IT(plan_real_info_mgr_=new(buf)ObPlanRealInfoMgr(get_allocator()))) {
  } else if (OB_FAIL(plan_real_info_mgr_->init(tenant_id, queue_size))) {
    LOG_WARN("failed to init plan real info manager", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::ReqMemEvict,
                                      tg_id_))) {
    SERVER_LOG(WARN, "create failed", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    SERVER_LOG(WARN, "init timer fail", K(ret));
  } else if (OB_FAIL(task_.init(this))) {
    SERVER_LOG(WARN, "fail to init sql plan timer task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, task_, EVICT_INTERVAL, true))) {
    SERVER_LOG(WARN, "start eliminate task failed", K(ret));
  } else {
    inited_ = true;
    destroyed_ = false;
  }
  if ((OB_FAIL(ret)) && (!inited_)) {
    destroy();
  }
  return ret;
}

void ObSqlPlanMgr::destroy()
{
  if (!destroyed_) {
    TG_DESTROY(tg_id_);
    if (plan_real_info_mgr_ != NULL) {
      plan_real_info_mgr_->destroy();
      free(plan_real_info_mgr_);
    }
    if (plan_item_mgr_ != NULL) {
      plan_item_mgr_->destroy();
      free(plan_item_mgr_);
    }
    for (int i = 0; i < plan_table_mgrs_.count(); ++i) {
      if (plan_table_mgrs_.at(i) != NULL) {
        plan_table_mgrs_.at(i)->destroy();
        free(plan_table_mgrs_.at(i));
      }
    }
    allocator_.destroy();
    inited_ = false;
    destroyed_ = true;
  }
}

ObConcurrentFIFOAllocator *ObSqlPlanMgr::get_allocator()
{
  return &allocator_;
}

void* ObSqlPlanMgr::alloc(const int64_t size)
{
  void * ret = allocator_.alloc(size);
  return ret;
}

void ObSqlPlanMgr::free(void *ptr)
{
  allocator_.free(ptr);
  ptr = NULL;
}

uint64_t ObSqlPlanMgr::get_tenant_id() const
{
  return tenant_id_;
}

bool ObSqlPlanMgr::is_valid() const
{
  return inited_ && !destroyed_;
}

int ObSqlPlanMgr::get_mem_limit(int64_t &mem_limit)
{
  int ret = OB_SUCCESS;
  int64_t tenant_mem_limit = lib::get_tenant_memory_limit(tenant_id_);
  // default mem limit
  mem_limit = static_cast<int64_t>(SQL_PLAN_MEM_FACTOR * tenant_mem_limit);
  // get mem_percentage from session info
  ObArenaAllocator alloc;
  ObObj obj_val;
  int64_t mem_pct = 0;
  if (OB_FAIL(ObBasicSessionInfo::get_global_sys_variable(tenant_id_,
                                                          alloc,
                                                          ObDataTypeCastParams(),
                                                          ObString(share::OB_SV_SQL_PLAN_MEMORY_PERCENTAGE),
                                                          obj_val))) {
    LOG_WARN("failed to get global sys variable", K(tenant_id_), K(ret));
  } else if (OB_FAIL(obj_val.get_int(mem_pct))) {
    LOG_WARN("failed to get int", K(ret), K(obj_val));
  } else if (mem_pct < 0 || mem_pct > 100) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid value of sql plan mem percentage", K(ret), K(mem_pct));
  } else {
    mem_limit = static_cast<int64_t>(tenant_mem_limit * mem_pct / 100.0);
    LOG_DEBUG("tenant sql plan memory limit", K_(tenant_id),
             K(tenant_mem_limit), K(mem_pct), K(mem_limit));
  }
  return ret;
}

int ObSqlPlanMgr::init_plan_table_manager(ObPlanItemMgr* &plan_table_mgr)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  plan_table_mgr = NULL;
  int64_t evict_high_level = 0;
  int64_t evict_low_level = 0;
  if (OB_FAIL(task_.calc_evict_mem_level(evict_low_level, evict_high_level))) {
    LOG_WARN("fail to get sql plan evict memory level", K(ret));
  } else if (evict_high_level <= allocator_.allocated()) {
    //do nothing
  } else if (OB_ISNULL(buf=alloc(sizeof(ObPlanItemMgr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for ObPlanRealInfoMgr", K(ret));
  } else if (OB_FALSE_IT(plan_table_mgr=new(buf)ObPlanItemMgr(get_allocator()))) {
  } else if (OB_FAIL(plan_table_mgr->init(tenant_id_, PLAN_TABLE_QUEUE_SIZE))) {
    LOG_WARN("failed to init plan real info manager", K(ret));
  } else {
    ObLockGuard<ObSpinLock> guard(plan_table_mgr_lock_);
    if (OB_FAIL(plan_table_mgrs_.push_back(plan_table_mgr))) {
      LOG_WARN("failed to push back plan table", K(ret));
    }
  }
  if (OB_FAIL(ret) && plan_table_mgr != NULL) {
    // cleanup
    free(plan_table_mgr);
    plan_table_mgr = NULL;
  }
  return ret;
}

int ObSqlPlanMgr::destroy_plan_table_manager(ObPlanItemMgr* &plan_table_mgr)
{
  int ret = OB_SUCCESS;
  if (plan_table_mgr != NULL) {
    bool find = false;
    int idx = 0;
    ObLockGuard<ObSpinLock> guard(plan_table_mgr_lock_);
    for (int i = 0; !find && i < plan_table_mgrs_.count(); ++i) {
      if (plan_table_mgrs_.at(i) == plan_table_mgr) {
        idx = i;
        find = true;
      }
    }
    if (find) {
      plan_table_mgrs_.remove(idx);
      plan_table_mgr->destroy();
      free(plan_table_mgr);
      plan_table_mgr = NULL;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("plan table not in this sql plan mgr", K(ret));
    }
  }
  return ret;
}

int ObSqlPlanMgr::mtl_init(ObSqlPlanMgr* &sql_plan_mgr)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = lib::current_resource_owner_id();
  sql_plan_mgr = OB_NEW(ObSqlPlanMgr, ObModIds::OB_SQL_PLAN);
  if (nullptr == sql_plan_mgr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for ObSqlPlanMgr", K(ret));
  } else if (OB_FAIL(sql_plan_mgr->init(tenant_id))) {
    LOG_WARN("failed to init request manager", K(ret));
  }
  if (OB_FAIL(ret) && sql_plan_mgr != nullptr) {
    // cleanup
    ob_delete(sql_plan_mgr);
    sql_plan_mgr = nullptr;
  }
  return ret;
}

void ObSqlPlanMgr::mtl_destroy(ObSqlPlanMgr* &sql_plan_mgr)
{
  if (sql_plan_mgr != nullptr) {
    ob_delete(sql_plan_mgr);
    sql_plan_mgr = nullptr;
  }
}

ObSqlPlanEliminateTask::ObSqlPlanEliminateTask()
    :sql_plan_manager_(NULL),
     config_mem_limit_(0)
{

}

ObSqlPlanEliminateTask::~ObSqlPlanEliminateTask()
{

}

int ObSqlPlanEliminateTask::init(const ObSqlPlanMgr *sql_plan_manager)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_plan_manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(sql_plan_manager_), K(ret));
  } else {
    sql_plan_manager_ = const_cast<ObSqlPlanMgr*>(sql_plan_manager);
    // can't call ObSqlPlanMgr::get_mem_limit for now, tenant not inited
    // set config_mem_limit_ to 64M
    config_mem_limit_ = 64 * 1024 * 1024; // 64M
    disable_timeout_check();
  }
  return ret;
}

//mem_limit = tenant_mem_limit * ob_sql_plan_percentage
int ObSqlPlanEliminateTask::check_config_mem_limit(bool &is_change)
{
  int ret = OB_SUCCESS;
  is_change = false;
  const int64_t MINIMUM_LIMIT = 64 * 1024 * 1024;   // at lease 64M
  const int64_t MAXIMUM_LIMIT = 1024 * 1024 * 1024; // 1G maximum
  int64_t mem_limit = config_mem_limit_;
  if (OB_ISNULL(sql_plan_manager_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(sql_plan_manager_), K(ret));
  } else if (sql_plan_manager_->get_tenant_id() > OB_SYS_TENANT_ID &&
             sql_plan_manager_->get_tenant_id() <= OB_MAX_RESERVED_TENANT_ID) {
    // 50x租户在没有对应的tenant schema，查询配置一定失败
  } else if (OB_FAIL(sql_plan_manager_->get_mem_limit(mem_limit))) {
    LOG_WARN("failed to get mem limit", K(ret));

    // if memory limit is not retrivable
    // overwrite error code, set mem config to default value
    // so that total memory use of sql plan can be limited
    ret = OB_SUCCESS;
    mem_limit = MAXIMUM_LIMIT;
  }
  if (config_mem_limit_ != mem_limit) {
    LOG_TRACE("before change config mem", K(config_mem_limit_));
    if (mem_limit < MINIMUM_LIMIT) {
      if (lib::is_mini_mode()) {
        config_mem_limit_ = mem_limit;
      } else {
        config_mem_limit_ = MINIMUM_LIMIT;
      }
    } else {
      config_mem_limit_ = mem_limit;
    }
    is_change = true;
    LOG_TRACE("after change config mem", K(config_mem_limit_));
  }
  return ret;
}

//剩余内存淘汰曲线图,当mem_limit在[64M, 100M]时, 内存剩余20M时淘汰;
//               当mem_limit在[100M, 5G]时, 内存剩余mem_limit*0.2时淘汰;
//               当mem_limit在[5G, +∞]时, 内存剩余1G时淘汰;
//高低水位线内存差曲线图，当mem_limit在[64M, 100M]时, 内存差为:20M;
//                        当mem_limit在[100M, 5G]时，内存差：mem_limit*0.2;
//                        当mem_limit在[5G, +∞]时, 内存差是：1G,
//        ______
//       /
// _____/
//   100M 5G
int ObSqlPlanEliminateTask::calc_evict_mem_level(int64_t &low, int64_t &high)
{
  int ret = OB_SUCCESS;
  const double HIGH_LEVEL_PRECENT = 0.80;
  const double LOW_LEVEL_PRECENT = 0.60;
  const double HALF_PRECENT = 0.50;
  const int64_t BIG_MEMORY_LIMIT = 5368709120; //5G
  const int64_t LOW_CONFIG = 10*1024*1024; //10M
  if (OB_ISNULL(sql_plan_manager_) || config_mem_limit_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(sql_plan_manager_), K(config_mem_limit_), K(ret));
  } else {
    if (config_mem_limit_ > BIG_MEMORY_LIMIT) {
      // mem_limit > 5G
      high = config_mem_limit_ - static_cast<int64_t>(BIG_MEMORY_LIMIT * (1.0 - HIGH_LEVEL_PRECENT));
      low = config_mem_limit_ - static_cast<int64_t>(BIG_MEMORY_LIMIT * (1.0 - LOW_LEVEL_PRECENT)) ;
    } else if (config_mem_limit_ > LOW_CONFIG) {
      //mem_limit between 10M and 5G
      high = static_cast<int64_t>(static_cast<double>(config_mem_limit_) * HIGH_LEVEL_PRECENT);
      low = static_cast<int64_t>(static_cast<double>(config_mem_limit_) * LOW_LEVEL_PRECENT);
    } else {
      //mem_limit < 10M
      high = static_cast<int64_t>(static_cast<double>(config_mem_limit_) * HALF_PRECENT);
      low = 0;
    }
  }
  return ret;
}

void ObSqlPlanEliminateTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObConcurrentFIFOAllocator *allocator = NULL;
  int64_t evict_high_level = 0;
  int64_t evict_low_level = 0;
  bool is_change = false;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t evict_batch_count = 0;
  if (OB_ISNULL(sql_plan_manager_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(sql_plan_manager_), K(ret));
  } else if (OB_FAIL(check_config_mem_limit(is_change))) {
    LOG_WARN("fail to check mem limit stat", K(ret));
  } else if (OB_FAIL(calc_evict_mem_level(evict_low_level, evict_high_level))) {
    LOG_WARN("fail to get sql plan evict memory level", K(ret));
  } else if (OB_ISNULL(allocator = sql_plan_manager_->get_allocator())) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to get sql plan evict memory level", K(ret));
  } else if (OB_FAIL(evict_queue_use(evict_batch_count))) {
    LOG_WARN("failed to evict queue use", K(ret));
  } else if (evict_high_level < allocator->allocated() &&
             OB_FAIL(evict_memory_use(evict_low_level,
                                      evict_high_level,
                                      evict_batch_count))) {
    LOG_WARN("failed to evict mempry use", K(ret));
  } else {
    //如果sql_plan_memory_limit改变, 则需要将ObConcurrentFIFOAllocator中total_limit_更新;
    if (true == is_change) {
      allocator->set_total_limit(config_mem_limit_);
    }
    int64_t end_time = ObTimeUtility::current_time();
    if (evict_batch_count > 0) {
      LOG_INFO("sql plan evict task end",
              K(sql_plan_manager_->get_tenant_id()),
              K(evict_high_level),
              K(evict_low_level),
              K(evict_batch_count),
              "elapse_time", end_time - start_time,
              K_(config_mem_limit),
              "mem_used", allocator->allocated());
    }
  }
}

int ObSqlPlanEliminateTask::evict_memory_use(int64_t evict_low_level,
                                             int64_t evict_high_level,
                                             int64_t &evict_batch_count)
{
  int ret = OB_SUCCESS;
  ObConcurrentFIFOAllocator *allocator = NULL;
  if (OB_ISNULL(sql_plan_manager_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(sql_plan_manager_), K(ret));
  } else if (OB_ISNULL(allocator = sql_plan_manager_->get_allocator())) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to get sql plan evict memory level", K(ret));
  } else {
    int64_t evict_count = 0;
    //evict plan table mgr
    {
      ObLockGuard<ObSpinLock> guard(sql_plan_manager_->plan_table_mgr_lock_);
      for (int i = 0; i < sql_plan_manager_->get_plan_table_mgrs().count(); ++i) {
        if (sql_plan_manager_->get_plan_table_mgrs().at(i) != NULL) {
          ObPlanItemMgr *plan_table_mgr = sql_plan_manager_->get_plan_table_mgrs().at(i);
          LOG_INFO("plan table evict mem start", K(evict_high_level), "mem_used", allocator->allocated());
          while (evict_low_level < allocator->allocated()) {
            evict_count = plan_table_mgr->release_old(ObSqlPlanMgr::BATCH_RELEASE_COUNT);
            evict_batch_count += evict_count;
            if (evict_count < ObSqlPlanMgr::BATCH_RELEASE_COUNT) {
              LOG_INFO("release old cannot free more memory");
              break;
            }
          }
        }
      }
    }
    //evict plan real info mgr
    if (sql_plan_manager_->get_plan_real_info_mgr() != NULL) {
      ObPlanRealInfoMgr *plan_real_info_mgr = sql_plan_manager_->get_plan_real_info_mgr();
      LOG_INFO("plan real info evict mem start", K(evict_high_level), "mem_used", allocator->allocated());
      while (evict_low_level < allocator->allocated()) {
        evict_count = plan_real_info_mgr->release_old(ObSqlPlanMgr::BATCH_RELEASE_COUNT);
        evict_batch_count += evict_count;
        if (evict_count < ObSqlPlanMgr::BATCH_RELEASE_COUNT) {
          LOG_INFO("release old cannot free more memory");
          break;
        }
      }
    }
    //evict plan item mgr
    if (sql_plan_manager_->get_plan_item_mgr() != NULL) {
      ObPlanItemMgr *plan_item_mgr = sql_plan_manager_->get_plan_item_mgr();
      LOG_INFO("plan item evict mem start", K(evict_high_level), "mem_used", allocator->allocated());
      while (evict_low_level < allocator->allocated()) {
        evict_count = plan_item_mgr->release_old(ObSqlPlanMgr::BATCH_RELEASE_COUNT);
        evict_batch_count += evict_count;
        if (evict_count < ObSqlPlanMgr::BATCH_RELEASE_COUNT) {
          LOG_INFO("release old cannot free more memory");
          break;
        }
      }
    }
  }
  return ret;
}

int ObSqlPlanEliminateTask::evict_queue_use(int64_t &evict_batch_count)
{
  int ret = OB_SUCCESS;
  ObConcurrentFIFOAllocator *allocator = NULL;
  if (OB_ISNULL(sql_plan_manager_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(sql_plan_manager_), K(ret));
  } else if (OB_ISNULL(allocator = sql_plan_manager_->get_allocator())) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to get sql plan evict memory level", K(ret));
  } else {
    //evict plan item mgr
    if (sql_plan_manager_->get_plan_item_mgr() != NULL) {
      ObPlanItemMgr *plan_item_mgr = sql_plan_manager_->get_plan_item_mgr();
      int64_t max_queue_size = plan_item_mgr->get_capacity();
      int64_t high_level_evict_size = max_queue_size * ObSqlPlanMgr::HIGH_LEVEL_EVICT_SIZE_PERCENT;
      int64_t low_level_evict_size = max_queue_size * ObSqlPlanMgr::LOW_LEVEL_EVICT_SIZE_PERCENT;
      if (plan_item_mgr->get_size_used() > high_level_evict_size) {
        int64_t batch_count = (plan_item_mgr->get_size_used() - low_level_evict_size)
                              / ObSqlPlanMgr::BATCH_RELEASE_COUNT;
        LOG_INFO("plan item evict record start", "size_used",plan_item_mgr->get_size_used());
        for (int i = 0; i < batch_count; i++) {
          evict_batch_count += plan_item_mgr->release_old(ObSqlPlanMgr::BATCH_RELEASE_COUNT);
        }
      }
    }
    //evict plan real into mgr
    if (sql_plan_manager_->get_plan_real_info_mgr() != NULL) {
      ObPlanRealInfoMgr *plan_real_info_mgr = sql_plan_manager_->get_plan_real_info_mgr();
      int64_t max_queue_size = plan_real_info_mgr->get_capacity();
      int64_t high_level_evict_size = max_queue_size * ObSqlPlanMgr::HIGH_LEVEL_EVICT_SIZE_PERCENT;
      int64_t low_level_evict_size = max_queue_size * ObSqlPlanMgr::LOW_LEVEL_EVICT_SIZE_PERCENT;
      if (plan_real_info_mgr->get_size_used() > high_level_evict_size) {
        int64_t batch_count = (plan_real_info_mgr->get_size_used() - low_level_evict_size)
                              / ObSqlPlanMgr::BATCH_RELEASE_COUNT;
        LOG_INFO("plan real info evict record start", "size_used",plan_real_info_mgr->get_size_used());
        for (int i = 0; i < batch_count; i++) {
          evict_batch_count += plan_real_info_mgr->release_old(ObSqlPlanMgr::BATCH_RELEASE_COUNT);
        }
      }
    }
    //evict plan table mgr
    {
      ObLockGuard<ObSpinLock> guard(sql_plan_manager_->plan_table_mgr_lock_);
      for (int i = 0; i < sql_plan_manager_->get_plan_table_mgrs().count(); ++i) {
        if (sql_plan_manager_->get_plan_table_mgrs().at(i) != NULL) {
          ObPlanItemMgr *plan_table_mgr = sql_plan_manager_->get_plan_table_mgrs().at(i);
          int64_t max_queue_size = plan_table_mgr->get_capacity();
          int64_t high_level_evict_size = max_queue_size * ObSqlPlanMgr::HIGH_LEVEL_EVICT_SIZE_PERCENT;
          int64_t low_level_evict_size = max_queue_size * ObSqlPlanMgr::LOW_LEVEL_EVICT_SIZE_PERCENT;
          if (plan_table_mgr->get_size_used() > high_level_evict_size) {
            int64_t batch_count = (plan_table_mgr->get_size_used() - low_level_evict_size)
                                  / ObSqlPlanMgr::BATCH_RELEASE_COUNT;
            LOG_INFO("plan table evict record start", "size_used",plan_table_mgr->get_size_used());
            for (int i = 0; i < batch_count; i++) {
              evict_batch_count += plan_table_mgr->release_old(ObSqlPlanMgr::BATCH_RELEASE_COUNT);
            }
          }
        }
      }
    }
  }
  return ret;
}

} // end of namespace sql
} // end of namespace oceanbase
