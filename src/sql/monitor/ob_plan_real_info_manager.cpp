// Copyright 2010-2016 Alibaba Inc. All Rights Reserved.
// Author:
//   zhenling.zzg
// this file defines implementation of plan real info manager


#define USING_LOG_PREFIX SQL
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/compress/zlib/zlib_src/zlib.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/thread/thread_mgr.h"
#include "common/object/ob_object.h"
#include "ob_plan_real_info_manager.h"
#include "lib/ob_running_mode.h"
#include "util/easy_time.h"
#include "lib/rc/ob_rc.h"

namespace oceanbase
{
namespace sql
{

ObPlanRealInfo::ObPlanRealInfo()
{
  reset();
}

ObPlanRealInfo::~ObPlanRealInfo()
{
}

void ObPlanRealInfo::reset()
{
  plan_id_ = 0;
  sql_id_ = NULL;
  sql_id_len_ = 0;
  plan_hash_ = 0;
  id_ = 0;
  real_cost_ = 0;
  real_cardinality_ = 0;
  cpu_cost_ = 0;
  io_cost_ = 0;
}

int64_t ObPlanRealInfo::get_extra_size() const
{
  return sql_id_len_;
}

ObPlanRealInfoRecord::ObPlanRealInfoRecord()
  :allocator_(NULL)
{

}

ObPlanRealInfoRecord::~ObPlanRealInfoRecord()
{
  destroy();
}

void ObPlanRealInfoRecord::destroy()
{
  if (NULL != allocator_) {
    allocator_->free(this);
  }
}

ObPlanRealInfoMgr::ObPlanRealInfoMgr()
  :allocator_(),
  task_(),
  queue_(),
  destroyed_(false),
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  tg_id_(-1)
{
}

ObPlanRealInfoMgr::~ObPlanRealInfoMgr()
{
  if (inited_) {
    destroy();
  }
}

int ObPlanRealInfoMgr::init(uint64_t tenant_id,
                      const int64_t queue_size)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(queue_.init(ObModIds::OB_SQL_PLAN,
                                 queue_size,
                                 tenant_id))) {
    SERVER_LOG(WARN, "Failed to init ObMySQLRequestQueue", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::ReqMemEvict,
                                      tg_id_))) {
    SERVER_LOG(WARN, "create failed", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    SERVER_LOG(WARN, "init timer fail", K(ret));
  } else if (OB_FAIL(allocator_.init(SQL_PLAN_PAGE_SIZE,
                                     ObModIds::OB_SQL_PLAN,
                                     tenant_id,
                                     INT64_MAX))) {
    SERVER_LOG(WARN, "failed to init allocator", K(ret));
  } else {
    //check FIFO mem used and plan real info every 1 seconds
    if (OB_FAIL(task_.init(this))) {
      SERVER_LOG(WARN, "fail to init plan real info timer task", K(ret));
    } else if (OB_FAIL(TG_SCHEDULE(tg_id_, task_, EVICT_INTERVAL, true))) {
      SERVER_LOG(WARN, "start eliminate task failed", K(ret));
    } else {
      tenant_id_ = tenant_id;
      inited_ = true;
      destroyed_ = false;
    }
  }
  if ((OB_FAIL(ret)) && (!inited_)) {
    destroy();
  }
  return ret;
}

void ObPlanRealInfoMgr::destroy()
{
  if (!destroyed_) {
    TG_DESTROY(tg_id_);
    clear_queue();
    queue_.destroy();
    allocator_.destroy();
    inited_ = false;
    destroyed_ = true;
  }
}

int ObPlanRealInfoMgr::handle_plan_info(int64_t id,
                                        const ObString& sql_id,
                                        uint64_t plan_id,
                                        uint64_t plan_hash,
                                        const ObMonitorNode &plan_info)
{
  int ret = OB_SUCCESS;
  ObPlanRealInfoRecord *record = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    char *buf = NULL;
    //alloc mem from allocator
    int64_t pos = sizeof(ObPlanRealInfoRecord);
    int64_t total_size = sizeof(ObPlanRealInfoRecord) +
                         sql_id.length();
    if (NULL == (buf = (char*)alloc(total_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        SERVER_LOG(WARN, "alloc mem failed", K(total_size), K(ret));
      }
    } else {
      uint64_t cpu_khz = get_cpufreq_khz();
      cpu_khz = cpu_khz < 1 ? 1 : cpu_khz;
      int64_t row_count = plan_info.output_row_count_;
      int64_t cpu_time = plan_info.db_time_*1000 / cpu_khz;
      int64_t io_time = plan_info.block_time_*1000 / cpu_khz;
      int64_t open_time = plan_info.open_time_;
      int64_t last_row_time = plan_info.last_row_time_;
      int64_t real_time = 0;
      if (last_row_time > open_time) {
        real_time = last_row_time - open_time;
      }
      record = new(buf)ObPlanRealInfoRecord();
      record->allocator_ = &allocator_;
      record->data_.id_ = id;
      record->data_.plan_id_ = plan_id;
      record->data_.plan_hash_ = plan_hash;
      record->data_.real_cost_ = real_time;
      record->data_.real_cardinality_ = row_count;
      record->data_.io_cost_ = io_time;
      record->data_.cpu_cost_ = cpu_time;
      if ((sql_id.length() > 0) && (NULL != sql_id.ptr())) {
        MEMCPY(buf + pos, sql_id.ptr(), sql_id.length());
        record->data_.sql_id_ = buf + pos;
        pos += sql_id.length();
        record->data_.sql_id_len_ = sql_id.length();
      }
    }
    //push into queue
    if (OB_SUCC(ret)) {
      int64_t req_id = 0;
      if (OB_FAIL(queue_.push(record, req_id))) {
        if (REACH_TIME_INTERVAL(2 * 1000 * 1000)) {
          SERVER_LOG(WARN, "push into queue failed", K(ret));
        }
        free(record);
        record = NULL;
      }
    }
  }
  return ret;
}

ObConcurrentFIFOAllocator *ObPlanRealInfoMgr::get_allocator()
{
  return &allocator_;
}

void* ObPlanRealInfoMgr::alloc(const int64_t size)
{
  void * ret = allocator_.alloc(size);
  return ret;
}

void ObPlanRealInfoMgr::free(void *ptr)
{
  allocator_.free(ptr);
  ptr = NULL;
}

int ObPlanRealInfoMgr::get(const int64_t idx, void *&record, Ref* ref)
{
  int ret = OB_SUCCESS;
  if (NULL == (record = queue_.get(idx, ref))) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObPlanRealInfoMgr::revert(Ref* ref)
{
  queue_.revert(ref);
  return OB_SUCCESS;
}

int ObPlanRealInfoMgr::release_old(int64_t limit)
{
  void* req = NULL;
  int64_t count = 0;
  while(count++ < limit && NULL != (req = queue_.pop())) {
      free(req);
  }
  return OB_SUCCESS;
}

void ObPlanRealInfoMgr::clear_queue()
{
  (void)release_old(INT64_MAX);
}

uint64_t ObPlanRealInfoMgr::get_tenant_id() const
{
  return tenant_id_;
}

bool ObPlanRealInfoMgr::is_valid() const
{
  return inited_ && !destroyed_;
}

int64_t ObPlanRealInfoMgr::get_start_idx() const
{
  return (int64_t)queue_.get_pop_idx();
}

int64_t ObPlanRealInfoMgr::get_end_idx() const
{
  return (int64_t)queue_.get_push_idx();
}

int64_t ObPlanRealInfoMgr::get_size_used()
{
  return (int64_t)(queue_.get_push_idx() - queue_.get_pop_idx());
}

int64_t ObPlanRealInfoMgr::get_size()
{
  return (int64_t)queue_.get_size();
}

int ObPlanRealInfoMgr::get_mem_limit(uint64_t tenant_id, int64_t &mem_limit)
{
  int ret = OB_SUCCESS;
  int64_t tenant_mem_limit = lib::get_tenant_memory_limit(tenant_id);
  // default mem limit
  mem_limit = static_cast<int64_t>(SQL_PLAN_MEM_FACTOR * tenant_mem_limit);

  // get mem_percentage from session info
  ObArenaAllocator alloc;
  ObObj obj_val;
  int64_t mem_pct = 0;
  if (OB_FAIL(ObBasicSessionInfo::get_global_sys_variable(tenant_id,
                                                          alloc,
                                                          ObDataTypeCastParams(),
                                                          ObString(share::OB_SV_SQL_PLAN_MEMORY_PERCENTAGE),
                                                          obj_val))) {
    LOG_WARN("failed to get global sys variable", K(ret));
  } else if (OB_FAIL(obj_val.get_int(mem_pct))) {
    LOG_WARN("failed to get int", K(ret), K(obj_val));
  } else if (mem_pct < 0 || mem_pct > 100) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid value of plan real info mem percentage", K(ret), K(mem_pct));
  } else {
    mem_limit = static_cast<int64_t>(tenant_mem_limit * mem_pct / 100.0);
    LOG_DEBUG("tenant plan real info memory limit",
             K(tenant_id), K(tenant_mem_limit), K(mem_pct), K(mem_limit));
  }
  return ret;
}

int ObPlanRealInfoMgr::mtl_init(ObPlanRealInfoMgr* &plan_real_info_mgr)
{
  int ret = OB_SUCCESS;
  plan_real_info_mgr = OB_NEW(ObPlanRealInfoMgr, ObModIds::OB_SQL_PLAN);
  if (nullptr == plan_real_info_mgr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for ObPlanRealInfoMgr", K(ret));
  } else {
    int64_t queue_size = lib::is_mini_mode() ?
                        MINI_MODE_MAX_QUEUE_SIZE : MAX_QUEUE_SIZE;
    uint64_t tenant_id = lib::current_resource_owner_id();
    if (OB_FAIL(plan_real_info_mgr->init(tenant_id,
                                   queue_size))) {
      LOG_WARN("failed to init request manager", K(ret));
    }
  }
  if (OB_FAIL(ret) && plan_real_info_mgr != nullptr) {
    // cleanup
    ob_delete(plan_real_info_mgr);
    plan_real_info_mgr = nullptr;
  }
  return ret;
}

void ObPlanRealInfoMgr::mtl_destroy(ObPlanRealInfoMgr* &plan_real_info_mgr)
{
  if (plan_real_info_mgr != nullptr) {
    ob_delete(plan_real_info_mgr);
    plan_real_info_mgr = nullptr;
  }
}

ObPlanRealInfoEliminateTask::ObPlanRealInfoEliminateTask()
    :plan_real_info_manager_(NULL),
     config_mem_limit_(0)
{

}

ObPlanRealInfoEliminateTask::~ObPlanRealInfoEliminateTask()
{

}

int ObPlanRealInfoEliminateTask::init(const ObPlanRealInfoMgr *plan_real_info_manager)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plan_real_info_manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(plan_real_info_manager_), K(ret));
  } else {
    plan_real_info_manager_ = const_cast<ObPlanRealInfoMgr*>(plan_real_info_manager);
    // can't call ObPlanRealInfoMgr::get_mem_limit for now, tenant not inited
    // set config_mem_limit_ to 64M
    config_mem_limit_ = 64 * 1024 * 1024; // 64M
    disable_timeout_check();
  }
  return ret;
}

//mem_limit = tenant_mem_limit * ob_sql_plan_percentage
int ObPlanRealInfoEliminateTask::check_config_mem_limit(bool &is_change)
{
  int ret = OB_SUCCESS;
  is_change = false;
  const int64_t MINIMUM_LIMIT = 64 * 1024 * 1024;   // at lease 64M
  const int64_t MAXIMUM_LIMIT = 1024 * 1024 * 1024; // 1G maximum
  int64_t mem_limit = config_mem_limit_;
  if (OB_ISNULL(plan_real_info_manager_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(plan_real_info_manager_), K(ret));
  } else if (plan_real_info_manager_->get_tenant_id() > OB_SYS_TENANT_ID &&
             plan_real_info_manager_->get_tenant_id() <= OB_MAX_RESERVED_TENANT_ID) {
    // 50x租户在没有对应的tenant schema，查询配置一定失败
  } else if (OB_FAIL(ObPlanRealInfoMgr::get_mem_limit(plan_real_info_manager_->get_tenant_id(),
                                                     mem_limit))) {
    LOG_WARN("failed to get mem limit", K(ret));

    // if memory limit is not retrivable
    // overwrite error code, set mem config to default value
    // so that total memory use of plan real info can be limited
    ret = OB_SUCCESS;
    mem_limit = MAXIMUM_LIMIT;
  }
  if (config_mem_limit_ != mem_limit) {
    LOG_TRACE("before change config mem", K(config_mem_limit_));
    if (mem_limit < MINIMUM_LIMIT) {
      if (lib::is_mini_mode()) {
        // do nothing
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
//               当mem_limit在[100M, 5G]时, 内存甚于mem_limit*0.2时淘汰;
//               当mem_limit在[5G, +∞]时, 内存剩余1G时淘汰;
//高低水位线内存差曲线图，当mem_limit在[64M, 100M]时, 内存差为:20M;
//                        当mem_limit在[100M, 5G]时，内存差：mem_limit*0.2;
//                        当mem_limit在[5G, +∞]时, 内存差是：1G,
//        ______
//       /
// _____/
//   100M 5G
int ObPlanRealInfoEliminateTask::calc_evict_mem_level(int64_t &low, int64_t &high)
{
  int ret = OB_SUCCESS;
  const double HIGH_LEVEL_PRECENT = 0.80;
  const double LOW_LEVEL_PRECENT = 0.60;
  const double HALF_PRECENT = 0.50;
  const int64_t BIG_MEMORY_LIMIT = 5368709120; //5G
  const int64_t SMALL_MEMORY_LIMIT = 100*1024*1024; //100M
  const int64_t LOW_CONFIG = 64*1024*1024; //64M
  if (OB_ISNULL(plan_real_info_manager_) || config_mem_limit_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(plan_real_info_manager_), K(config_mem_limit_), K(ret));
  } else {
    if (config_mem_limit_ > BIG_MEMORY_LIMIT) {
      // mem_limit > 5G
      high = config_mem_limit_ - static_cast<int64_t>(BIG_MEMORY_LIMIT * (1.0 - HIGH_LEVEL_PRECENT));
      low = config_mem_limit_ - static_cast<int64_t>(BIG_MEMORY_LIMIT * (1.0 - LOW_LEVEL_PRECENT)) ;
    } else if (config_mem_limit_ >= LOW_CONFIG &&  config_mem_limit_  < SMALL_MEMORY_LIMIT) {
      // 64M =< mem_limit < 100M
      high = config_mem_limit_ - static_cast<int64_t>(SMALL_MEMORY_LIMIT * (1.0 - HIGH_LEVEL_PRECENT));
      low = config_mem_limit_ - static_cast<int64_t>(SMALL_MEMORY_LIMIT * (1.0 - LOW_LEVEL_PRECENT));
    } else if (config_mem_limit_ < LOW_CONFIG) {
      //mem_limit < 64M
      high = static_cast<int64_t>(static_cast<double>(config_mem_limit_) * HALF_PRECENT);
      low = 0;
    } else {
      high = static_cast<int64_t>(static_cast<double>(config_mem_limit_) * HIGH_LEVEL_PRECENT);
      low = static_cast<int64_t>(static_cast<double>(config_mem_limit_) * LOW_LEVEL_PRECENT);
    }
  }
  return ret;
}

void ObPlanRealInfoEliminateTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObConcurrentFIFOAllocator *allocator = NULL;
  int64_t evict_high_level = 0;
  int64_t evict_low_level = 0;
  bool is_change = false;
  if (OB_ISNULL(plan_real_info_manager_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(plan_real_info_manager_), K(ret));
  } else if (OB_FAIL(check_config_mem_limit(is_change))) {
    LOG_WARN("fail to check mem limit stat", K(ret));
  } else if (OB_FAIL(calc_evict_mem_level(evict_low_level, evict_high_level))) {
    LOG_WARN("fail to get plan real info evict memory level", K(ret));
  } else if (OB_ISNULL(allocator = plan_real_info_manager_->get_allocator())) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to get plan real info evict memory level", K(ret));
  }
  if (OB_SUCC(ret)) {
    int64_t start_time = ObTimeUtility::current_time();
    int64_t evict_batch_count = 0;
    //按内存淘汰
    if (evict_high_level < allocator->allocated()) {
      LOG_INFO("plan real info evict mem start",
               K(evict_low_level),
               K(evict_high_level),
               "size_used",plan_real_info_manager_->get_size_used(),
               "mem_used", allocator->allocated());
      int64_t last_time_allocated = allocator->allocated();
      while (evict_low_level < allocator->allocated()) {
        plan_real_info_manager_->release_old();
        evict_batch_count++;
        if ((evict_low_level < allocator->allocated()) &&
            (last_time_allocated == allocator->allocated())) {
          LOG_INFO("release old cannot free more memory");
          break;
        }
        last_time_allocated = allocator->allocated();
      }
    }
    //按sql plan记录数淘汰
    int64_t high_level_evict_size = ObPlanRealInfoMgr::HIGH_LEVEL_EVICT_SIZE;
    int64_t low_level_evict_size = ObPlanRealInfoMgr::LOW_LEVEL_EVICT_SIZE;
    if (lib::is_mini_mode()) {
      high_level_evict_size = ObPlanRealInfoMgr::MINI_MODE_HIGH_LEVEL_EVICT_SIZE;
      low_level_evict_size = ObPlanRealInfoMgr::MINI_MODE_LOW_LEVEL_EVICT_SIZE;
    }
    if (plan_real_info_manager_->get_size_used() > high_level_evict_size) {
      evict_batch_count = (plan_real_info_manager_->get_size_used() - low_level_evict_size)
                            / ObPlanRealInfoMgr::BATCH_RELEASE_COUNT;
      LOG_INFO("plan real info evict record start",
               "size_used",plan_real_info_manager_->get_size_used(),
               "mem_used", allocator->allocated());
      for (int i = 0; i < evict_batch_count; i++) {
        plan_real_info_manager_->release_old();
      }
    }
    //如果sql_plan_memory_limit改变, 则需要将ObConcurrentFIFOAllocator中total_limit_更新;
    if (true == is_change) {
      allocator->set_total_limit(config_mem_limit_);
    }
    int64_t end_time = ObTimeUtility::current_time();
    LOG_TRACE("plan real info evict task end",
             K(evict_high_level),
             K(evict_batch_count),
             "elapse_time", end_time - start_time,
             "size_used",plan_real_info_manager_->get_size_used(),
             "mem_used", allocator->allocated());
  }
}

} // end of namespace sql
} // end of namespace oceanbase
