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

ObSqlPlanItem::ObSqlPlanItem()
{
  reset();
}

ObSqlPlanItem::~ObSqlPlanItem()
{
}

void ObSqlPlanItem::reset()
{
  plan_id_ = 0;
  db_id_ = 0;
  sql_id_ = NULL;
  sql_id_len_ = 0;
  plan_hash_ = 0;
  gmt_create_ = 0;
  operation_ = NULL;
  operation_len_ = 0;
  options_ = NULL;
  options_len_ = 0;
  object_node_ = NULL;
  object_node_len_ = 0;
  object_id_ = 0;
  object_owner_ = NULL;
  object_owner_len_ = 0;
  object_name_ = NULL;
  object_name_len_ = 0;
  object_alias_ = NULL;
  object_alias_len_ = 0;
  object_type_ = NULL;
  object_type_len_ = 0;
  optimizer_ = NULL;
  optimizer_len_ = 0;
  id_ = 0;
  parent_id_ = 0;
  depth_ = 0;
  position_ = 0;
  is_last_child_ = false;
  search_columns_ = 0;
  cost_ = 0;
  cardinality_ = 0;
  bytes_ = 0;
  rowset_ = 1;
  other_tag_ = NULL;
  other_tag_len_ = 0;
  partition_start_ = NULL;
  partition_start_len_ = 0;
  partition_stop_ = NULL;
  partition_stop_len_ = 0;
  partition_id_ = 0;
  other_ = NULL;
  other_len_ = 0;
  distribution_ = NULL;
  distribution_len_ = 0;
  cpu_cost_ = 0;
  io_cost_ = 0;
  temp_space_ = 0;
  access_predicates_ = NULL;
  access_predicates_len_ = 0;
  filter_predicates_ = NULL;
  filter_predicates_len_ = 0;
  startup_predicates_ = NULL;
  startup_predicates_len_ = 0;
  projection_ = NULL;
  projection_len_ = 0;
  special_predicates_ = NULL;
  special_predicates_len_ = 0;
  time_ = 0;
  qblock_name_ = NULL;
  qblock_name_len_ = 0;
  remarks_ = NULL;
  remarks_len_ = 0;
  other_xml_ = NULL;
  other_xml_len_ = 0;
}

int64_t ObSqlPlanItem::get_extra_size() const
{
  return sql_id_len_ +
        operation_len_ +
        options_len_ +
        object_node_len_ +
        object_owner_len_ +
        object_name_len_ +
        object_alias_len_ +
        object_type_len_ +
        optimizer_len_ +
        other_tag_len_ +
        partition_start_len_ +
        partition_stop_len_ +
        other_len_ +
        distribution_len_ +
        access_predicates_len_ +
        filter_predicates_len_ +
        startup_predicates_len_ +
        projection_len_ +
        special_predicates_len_ +
        qblock_name_len_ +
        remarks_len_ +
        other_xml_len_;
}

ObSqlPlanItemRecord::ObSqlPlanItemRecord()
  :allocator_(NULL)
{

}

ObSqlPlanItemRecord::~ObSqlPlanItemRecord()
{
  destroy();
}

void ObSqlPlanItemRecord::destroy()
{
  if (NULL != allocator_) {
    allocator_->free(this);
  }
}

ObSqlPlanMgr::ObSqlPlanMgr()
  :allocator_(),
  task_(),
  queue_(),
  plan_id_increment_(0),
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

int ObSqlPlanMgr::init(uint64_t tenant_id,
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
    //check FIFO mem used and sql plan every 1 seconds
    if (OB_FAIL(task_.init(this))) {
      SERVER_LOG(WARN, "fail to init sql plan timer task", K(ret));
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

void ObSqlPlanMgr::destroy()
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

int ObSqlPlanMgr::handle_plan_item(const ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  ObSqlPlanItemRecord *record = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    char *buf = NULL;
    //alloc mem from allocator
    int64_t pos = sizeof(ObSqlPlanItemRecord);
    int64_t total_size = sizeof(ObSqlPlanItemRecord) +
                          plan_item.get_extra_size();
    if (NULL == (buf = (char*)alloc(total_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        SERVER_LOG(WARN, "alloc mem failed", K(total_size), K(ret));
      }
    } else {
      record = new(buf)ObSqlPlanItemRecord();
      record->allocator_ = &allocator_;
      record->data_ = plan_item;
      #define DEEP_COPY_DATA(value)                                               \
      do {                                                                        \
        if (pos + plan_item.value##len_ > total_size) {                           \
          ret = OB_ERR_UNEXPECTED;                                                \
          LOG_WARN("unexpect record size", K(pos), K(plan_item.value##len_),      \
                                           K(total_size), K(ret));                \
        } else if ((plan_item.value##len_ > 0) && (NULL != plan_item.value)) {    \
          MEMCPY(buf + pos, plan_item.value, plan_item.value##len_);              \
          record->data_.value = buf + pos;                                        \
          pos += plan_item.value##len_;                                           \
        } else {                                                                  \
          record->data_.value = buf + pos;                                        \
        }                                                                         \
      } while(0);
      DEEP_COPY_DATA(sql_id_);
      DEEP_COPY_DATA(operation_);
      DEEP_COPY_DATA(options_);
      DEEP_COPY_DATA(object_node_);
      DEEP_COPY_DATA(object_owner_);
      DEEP_COPY_DATA(object_name_);
      DEEP_COPY_DATA(object_alias_);
      DEEP_COPY_DATA(object_type_);
      DEEP_COPY_DATA(optimizer_);
      DEEP_COPY_DATA(other_tag_);
      DEEP_COPY_DATA(partition_start_);
      DEEP_COPY_DATA(partition_stop_);
      DEEP_COPY_DATA(other_);
      DEEP_COPY_DATA(distribution_);
      DEEP_COPY_DATA(access_predicates_);
      DEEP_COPY_DATA(filter_predicates_);
      DEEP_COPY_DATA(startup_predicates_);
      DEEP_COPY_DATA(projection_);
      DEEP_COPY_DATA(special_predicates_);
      DEEP_COPY_DATA(qblock_name_);
      DEEP_COPY_DATA(remarks_);
      DEEP_COPY_DATA(other_xml_);
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

int ObSqlPlanMgr::get_plan(int64_t plan_id,
                           ObIArray<ObSqlPlanItem*> &plan)
{
  int ret = OB_SUCCESS;
  plan.reuse();
  int64_t start_idx = get_start_idx();
  int64_t end_idx = get_end_idx();
  void *rec = NULL;
  Ref ref;
  for (int64_t cur_id=start_idx;
       (OB_ENTRY_NOT_EXIST == ret || OB_SUCCESS == ret) && cur_id < end_idx;
       ++cur_id) {
    ref.reset();
    ret = get(cur_id, rec, &ref);
    if (OB_SUCC(ret) && NULL != rec) {
      ObSqlPlanItemRecord *record = static_cast<ObSqlPlanItemRecord*>(rec);
      if (record->data_.plan_id_ != plan_id) {
        //do nothing
      } else if (OB_FAIL(plan.push_back(&record->data_))) {
        LOG_WARN("failed to push back plan item", K(ret));
      }
    }
    if (ref.idx_ != -1) {
      revert(&ref);
    }
  }
  return ret;
}

int ObSqlPlanMgr::get_plan(const ObString &sql_id,
                           int64_t plan_id,
                           ObIArray<ObSqlPlanItem*> &plan)
{
  int ret = OB_SUCCESS;
  plan.reuse();
  int64_t start_idx = get_start_idx();
  int64_t end_idx = get_end_idx();
  void *rec = NULL;
  Ref ref;
  for (int64_t cur_id=start_idx;
       (OB_ENTRY_NOT_EXIST == ret || OB_SUCCESS == ret) && cur_id < end_idx;
       ++cur_id) {
    ref.reset();
    ret = get(cur_id, rec, &ref);
    if (OB_SUCC(ret) && NULL != rec) {
      ObSqlPlanItemRecord *record = static_cast<ObSqlPlanItemRecord*>(rec);
      if (record->data_.plan_id_ != plan_id ||
          sql_id.case_compare(ObString(record->data_.sql_id_len_,record->data_.sql_id_)) != 0) {
        //do nothing
      } else if (OB_FAIL(plan.push_back(&record->data_))) {
        LOG_WARN("failed to push back plan item", K(ret));
      }
    }
    if (ref.idx_ != -1) {
      revert(&ref);
    }
  }
  return ret;
}

int ObSqlPlanMgr::get_plan_by_hash(const ObString &sql_id,
                                   uint64_t plan_hash,
                                   ObIArray<ObSqlPlanItem*> &plan)
{
  int ret = OB_SUCCESS;
  plan.reuse();
  int64_t start_idx = get_start_idx();
  int64_t end_idx = get_end_idx();
  void *rec = NULL;
  Ref ref;
  for (int64_t cur_id=start_idx;
       (OB_ENTRY_NOT_EXIST == ret || OB_SUCCESS == ret) && cur_id < end_idx;
       ++cur_id) {
    ref.reset();
    ret = get(cur_id, rec, &ref);
    if (OB_SUCC(ret) && NULL != rec) {
      ObSqlPlanItemRecord *record = static_cast<ObSqlPlanItemRecord*>(rec);
      if (record->data_.plan_hash_ != plan_hash ||
          sql_id.case_compare(ObString(record->data_.sql_id_len_,record->data_.sql_id_)) != 0) {
        //do nothing
      } else if (OB_FAIL(plan.push_back(&record->data_))) {
        LOG_WARN("failed to push back plan item", K(ret));
      }
    }
    if (ref.idx_ != -1) {
      revert(&ref);
    }
  }
  return ret;
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

int ObSqlPlanMgr::get(const int64_t idx, void *&record, Ref* ref)
{
  int ret = OB_SUCCESS;
  if (NULL == (record = queue_.get(idx, ref))) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObSqlPlanMgr::revert(Ref* ref)
{
  queue_.revert(ref);
  return OB_SUCCESS;
}

int ObSqlPlanMgr::release_old(int64_t limit)
{
  void* req = NULL;
  int64_t count = 0;
  while(count++ < limit && NULL != (req = queue_.pop())) {
      free(req);
  }
  return OB_SUCCESS;
}

void ObSqlPlanMgr::clear_queue()
{
  (void)release_old(INT64_MAX);
}

uint64_t ObSqlPlanMgr::get_tenant_id() const
{
  return tenant_id_;
}

bool ObSqlPlanMgr::is_valid() const
{
  return inited_ && !destroyed_;
}

int64_t ObSqlPlanMgr::get_start_idx() const
{
  return (int64_t)queue_.get_pop_idx();
}

int64_t ObSqlPlanMgr::get_end_idx() const
{
  return (int64_t)queue_.get_push_idx();
}

int64_t ObSqlPlanMgr::get_size_used()
{
  return (int64_t)queue_.get_size();
}

int64_t ObSqlPlanMgr::get_capacity()
{
  return (int64_t)queue_.get_capacity();
}

int64_t ObSqlPlanMgr::get_next_plan_id()
{
  return ++plan_id_increment_;
}

int64_t ObSqlPlanMgr::get_last_plan_id()
{
  return plan_id_increment_;
}

int ObSqlPlanMgr::get_mem_limit(uint64_t tenant_id, int64_t &mem_limit)
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
    LOG_WARN("invalid value of sql plan mem percentage", K(ret), K(mem_pct));
  } else {
    mem_limit = static_cast<int64_t>(tenant_mem_limit * mem_pct / 100.0);
    LOG_DEBUG("tenant sql plan memory limit",
             K(tenant_id), K(tenant_mem_limit), K(mem_pct), K(mem_limit));
  }
  return ret;
}

int ObSqlPlanMgr::init_plan_manager(uint64_t tenant_id, ObSqlPlanMgr* &sql_plan_mgr)
{
  int ret = OB_SUCCESS;
  sql_plan_mgr = OB_NEW(ObSqlPlanMgr, ObModIds::OB_SQL_PLAN);
  if (nullptr == sql_plan_mgr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for ObSqlPlanMgr", K(ret));
  } else {
    int64_t queue_size = PLAN_TABLE_QUEUE_SIZE;
    if (OB_FAIL(sql_plan_mgr->init(tenant_id,
                                   queue_size))) {
      LOG_WARN("failed to init request manager", K(ret));
    }
  }
  if (OB_FAIL(ret) && sql_plan_mgr != nullptr) {
    // cleanup
    ob_delete(sql_plan_mgr);
    sql_plan_mgr = nullptr;
  }
  return ret;
}

int ObSqlPlanMgr::mtl_init(ObSqlPlanMgr* &sql_plan_mgr)
{
  int ret = OB_SUCCESS;
  sql_plan_mgr = OB_NEW(ObSqlPlanMgr, ObModIds::OB_SQL_PLAN);
  if (nullptr == sql_plan_mgr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for ObSqlPlanMgr", K(ret));
  } else {
    int64_t queue_size = lib::is_mini_mode() ?
                        MINI_MODE_MAX_QUEUE_SIZE : MAX_QUEUE_SIZE;
    uint64_t tenant_id = lib::current_resource_owner_id();
    if (OB_FAIL(sql_plan_mgr->init(tenant_id,
                                   queue_size))) {
      LOG_WARN("failed to init request manager", K(ret));
    }
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
  } else if (OB_FAIL(ObSqlPlanMgr::get_mem_limit(sql_plan_manager_->get_tenant_id(),
                                                     mem_limit))) {
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
  const int64_t SMALL_MEMORY_LIMIT = 100*1024*1024; //100M
  const int64_t LOW_CONFIG = 64*1024*1024; //64M
  if (OB_ISNULL(sql_plan_manager_) || config_mem_limit_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(sql_plan_manager_), K(config_mem_limit_), K(ret));
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

void ObSqlPlanEliminateTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObConcurrentFIFOAllocator *allocator = NULL;
  int64_t evict_high_level = 0;
  int64_t evict_low_level = 0;
  bool is_change = false;
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
  }
  if (OB_SUCC(ret)) {
    int64_t start_time = ObTimeUtility::current_time();
    int64_t evict_batch_count = 0;
    //按内存淘汰
    if (evict_high_level < allocator->allocated()) {
      LOG_INFO("sql plan evict mem start",
               K(evict_low_level),
               K(evict_high_level),
               "size_used",sql_plan_manager_->get_size_used(),
               "mem_used", allocator->allocated());
      int64_t last_time_allocated = allocator->allocated();
      while (evict_low_level < allocator->allocated()) {
        sql_plan_manager_->release_old();
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
    int64_t max_queue_size = sql_plan_manager_->get_capacity();
    int64_t high_level_evict_size = max_queue_size * ObSqlPlanMgr::HIGH_LEVEL_EVICT_SIZE_PERCENT;
    int64_t low_level_evict_size = max_queue_size * ObSqlPlanMgr::LOW_LEVEL_EVICT_SIZE_PERCENT;
    if (sql_plan_manager_->get_size_used() > high_level_evict_size) {
      evict_batch_count = (sql_plan_manager_->get_size_used() - low_level_evict_size)
                            / ObSqlPlanMgr::BATCH_RELEASE_COUNT;
      LOG_INFO("sql plan evict record start",
               "size_used",sql_plan_manager_->get_size_used(),
               "mem_used", allocator->allocated());
      for (int i = 0; i < evict_batch_count; i++) {
        sql_plan_manager_->release_old();
      }
    }
    //如果sql_plan_memory_limit改变, 则需要将ObConcurrentFIFOAllocator中total_limit_更新;
    if (true == is_change) {
      allocator->set_total_limit(config_mem_limit_);
    }
    int64_t end_time = ObTimeUtility::current_time();
    LOG_TRACE("sql plan evict task end",
             K(evict_high_level),
             K(evict_batch_count),
             "elapse_time", end_time - start_time,
             "size_used",sql_plan_manager_->get_size_used(),
             "mem_used", allocator->allocated());
  }
}

} // end of namespace sql
} // end of namespace oceanbase
