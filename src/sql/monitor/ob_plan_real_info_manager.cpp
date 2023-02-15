// Copyright 2010-2016 Alibaba Inc. All Rights Reserved.
// Author:
//   zhenling.zzg
// this file defines implementation of plan real info manager


#define USING_LOG_PREFIX SQL
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
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

ObPlanRealInfoMgr::ObPlanRealInfoMgr(ObConcurrentFIFOAllocator *allocator)
  :allocator_(allocator),
  queue_(),
  destroyed_(false),
  inited_(false)
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
  } else {
    inited_ = true;
    destroyed_ = false;
  }
  if ((OB_FAIL(ret)) && (!inited_)) {
    destroy();
  }
  return ret;
}

void ObPlanRealInfoMgr::destroy()
{
  if (!destroyed_) {
    clear_queue();
    queue_.destroy();
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
      record->allocator_ = allocator_;
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
  return allocator_;
}

void* ObPlanRealInfoMgr::alloc(const int64_t size)
{
  void *ret = NULL;
  if (allocator_ != NULL) {
    ret = allocator_->alloc(size);
  }
  return ret;
}

void ObPlanRealInfoMgr::free(void *ptr)
{
  if (allocator_ != NULL) {
    allocator_->free(ptr);
    ptr = NULL;
  }
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

int64_t ObPlanRealInfoMgr::release_old(int64_t limit)
{
  void* req = NULL;
  int64_t count = 0;
  while(count < limit && NULL != (req = queue_.pop())) {
    free(req);
    ++count;
  }
  return count;
}

void ObPlanRealInfoMgr::clear_queue()
{
  (void)release_old(INT64_MAX);
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
  return (int64_t)queue_.get_size();
}

int64_t ObPlanRealInfoMgr::get_capacity()
{
  return (int64_t)queue_.get_capacity();
}

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


ObPlanItemMgr::ObPlanItemMgr(ObConcurrentFIFOAllocator *allocator)
  :allocator_(allocator),
  queue_(),
  plan_id_increment_(0),
  destroyed_(false),
  inited_(false)
{
}

ObPlanItemMgr::~ObPlanItemMgr()
{
  if (inited_) {
    destroy();
  }
}

int ObPlanItemMgr::init(uint64_t tenant_id,
                        const int64_t queue_size)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(queue_.init(ObModIds::OB_SQL_PLAN,
                                 queue_size,
                                 tenant_id))) {
    SERVER_LOG(WARN, "Failed to init ObMySQLRequestQueue", K(ret));
  } else {
    inited_ = true;
    destroyed_ = false;
  }
  if ((OB_FAIL(ret)) && (!inited_)) {
    destroy();
  }
  return ret;
}

void ObPlanItemMgr::destroy()
{
  if (!destroyed_) {
    clear_queue();
    queue_.destroy();
    inited_ = false;
    destroyed_ = true;
  }
}

int ObPlanItemMgr::handle_plan_item(const ObSqlPlanItem &plan_item)
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
      record->allocator_ = allocator_;
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

int ObPlanItemMgr::get_plan(int64_t plan_id,
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

int ObPlanItemMgr::get_plan(const ObString &sql_id,
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

int ObPlanItemMgr::get_plan_by_hash(const ObString &sql_id,
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

ObConcurrentFIFOAllocator *ObPlanItemMgr::get_allocator()
{
  return allocator_;
}

void* ObPlanItemMgr::alloc(const int64_t size)
{
  void *ret = NULL;
  if (allocator_ != NULL) {
    ret = allocator_->alloc(size);
  }
  return ret;
}

void ObPlanItemMgr::free(void *ptr)
{
  if (allocator_ != NULL) {
    allocator_->free(ptr);
    ptr = NULL;
  }
}

int ObPlanItemMgr::get(const int64_t idx, void *&record, Ref* ref)
{
  int ret = OB_SUCCESS;
  if (NULL == (record = queue_.get(idx, ref))) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObPlanItemMgr::revert(Ref* ref)
{
  queue_.revert(ref);
  return OB_SUCCESS;
}

int64_t ObPlanItemMgr::release_old(int64_t limit)
{
  void* req = NULL;
  int64_t count = 0;
  while(count < limit && NULL != (req = queue_.pop())) {
    free(req);
    ++count;
  }
  return count;
}

void ObPlanItemMgr::clear_queue()
{
  (void)release_old(INT64_MAX);
}

bool ObPlanItemMgr::is_valid() const
{
  return inited_ && !destroyed_;
}

int64_t ObPlanItemMgr::get_start_idx() const
{
  return (int64_t)queue_.get_pop_idx();
}

int64_t ObPlanItemMgr::get_end_idx() const
{
  return (int64_t)queue_.get_push_idx();
}

int64_t ObPlanItemMgr::get_size_used()
{
  return (int64_t)queue_.get_size();
}

int64_t ObPlanItemMgr::get_capacity()
{
  return (int64_t)queue_.get_capacity();
}

int64_t ObPlanItemMgr::get_next_plan_id()
{
  return ++plan_id_increment_;
}

int64_t ObPlanItemMgr::get_last_plan_id()
{
  return plan_id_increment_;
}

} // end of namespace sql
} // end of namespace oceanbase
