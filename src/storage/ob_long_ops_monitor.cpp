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

#include "ob_long_ops_monitor.h"
#include "observer/ob_server_struct.h"
#include "lib/thread/thread_mgr.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share::schema;
using namespace oceanbase::storage;

ObILongOpsKey::ObILongOpsKey() : tenant_id_(OB_INVALID_ID), sid_(OB_INVALID_ID)
{
  MEMSET(name_, 0, sizeof(name_));
  MEMSET(target_, 0, sizeof(target_));
}

int64_t ObILongOpsKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&sid_, sizeof(sid_), hash_val);
  hash_val = murmurhash(name_, sizeof(name_), hash_val);
  hash_val = murmurhash(target_, sizeof(target_), hash_val);
  return hash_val;
}

bool ObILongOpsKey::operator==(const ObILongOpsKey& other) const
{
  return tenant_id_ == other.tenant_id_ && sid_ == other.sid_ && (0 == MEMCMP(name_, other.name_, sizeof(name_))) &&
         (0 == MEMCMP(target_, other.target_, sizeof(target_)));
}

bool ObILongOpsKey::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && '\0' != name_[0] && '\0' != target_[0];
}

int ObCreateIndexKey::to_key_string()
{
  int ret = OB_SUCCESS;
  int64_t name_pos = 0;
  int64_t target_pos = 0;
  if (OB_FAIL(databuff_printf(name_, MAX_LONG_OPS_NAME_LENGTH, name_pos, "CREATE INDEX"))) {
    LOG_WARN("fail to set name string", K(ret));
  } else if (OB_FAIL(databuff_printf(
                 target_, MAX_LONG_OPS_NAME_LENGTH, target_pos, "index_table_id=%ld, ", index_table_id_))) {
    LOG_WARN("fail to convert index_table_id to string", K(ret));
  } else if (OB_FAIL(
                 databuff_printf(target_, MAX_LONG_OPS_TARGET_LENGTH, target_pos, "partition_id=%ld", partition_id_))) {
    LOG_WARN("fail to convert partition_id to string", K(ret));
  }
  return ret;
}

int ObILongOpsTaskStat::assign(const ObILongOpsTaskStat& other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    task_id_ = other.task_id_;
    cpu_cost_ = other.cpu_cost_;
    io_cost_ = other.io_cost_;
    state_ = other.state_;
    type_ = other.type_;
  }
  return ret;
}

int ObCreateIndexScanTaskStat::assign(const ObILongOpsTaskStat& other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObILongOpsTaskStat::TaskType::SCAN != other.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(other));
  } else {
    const ObCreateIndexScanTaskStat& other_scan_stat = static_cast<const ObCreateIndexScanTaskStat&>(other);
    if (this != &other) {
      if (OB_FAIL(ObILongOpsTaskStat::assign(other))) {
        LOG_WARN("fail to assign task", K(ret));
      } else {
        macro_count_ = other_scan_stat.macro_count_;
      }
    }
  }
  return ret;
}

int ObCreateIndexSortTaskStat::assign(const ObILongOpsTaskStat& other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObILongOpsTaskStat::TaskType::SORT != other.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(other));
  } else {
    const ObCreateIndexSortTaskStat& other_sort_stat = static_cast<const ObCreateIndexSortTaskStat&>(other);
    if (this != &other) {
      if (OB_FAIL(ObILongOpsTaskStat::assign(other))) {
        LOG_WARN("fail to assign task", K(ret));
      } else {
        macro_count_ = other_sort_stat.macro_count_;
        run_count_ = other_sort_stat.run_count_;
      }
    }
  }
  return ret;
}

ObCommonOpsStatValue::ObCommonOpsStatValue()
    : start_time_(0),
      finish_time_(0),
      elapsed_time_(0),
      remaining_time_(0),
      percentage_(0),
      last_update_time_(0),
      is_updated_(false),
      message_(),
      lock_()
{
  memset(message_, 0, sizeof(message_));
}

ObCommonOpsStatValue& ObCommonOpsStatValue::operator=(const ObCommonOpsStatValue& other)
{
  if (this != &other) {
    start_time_ = other.start_time_;
    finish_time_ = other.finish_time_;
    elapsed_time_ = other.elapsed_time_;
    remaining_time_ = other.remaining_time_;
    percentage_ = other.percentage_;
    last_update_time_ = other.last_update_time_;
    is_updated_ = other.is_updated_;
    MEMCPY(message_, other.message_, sizeof(message_));
  }
  return *this;
}

void ObCommonOpsStatValue::reset()
{
  start_time_ = 0;
  finish_time_ = 0;
  elapsed_time_ = 0;
  remaining_time_ = 0;
  percentage_ = 0;
  last_update_time_ = 0;
  is_updated_ = 0;
  memset(message_, 0, sizeof(message_));
}

ObCreateIndexPartitionStat::ObCreateIndexPartitionStat()
    : ObILongOpsStat(), key_(), task_stats_(), allocator_(ObModIds::OB_SSTABLE_LONG_OPS_MONITOR)
{}

ObCreateIndexPartitionStat::~ObCreateIndexPartitionStat()
{
  reset();
}

int ObCreateIndexPartitionStat::update_task_stat(const ObILongOpsTaskStat& task_stat)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(common_value_.lock_);
  if (OB_UNLIKELY(!task_stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_stat));
  } else {
    int64_t i = 0;
    for (i = 0; OB_SUCC(ret) && i < task_stats_.count(); ++i) {
      if (task_stat.task_id_ == task_stats_.at(i)->task_id_) {
        break;
      }
    }
    if (i == task_stats_.count()) {
      ObILongOpsTaskStat* new_task_stat = NULL;
      if (OB_FAIL(copy_task(&task_stat, new_task_stat))) {
        LOG_WARN("fail to copy task", K(ret));
      } else if (OB_FAIL(task_stats_.push_back(new_task_stat))) {
        LOG_WARN("fail to push back task stat", K(ret));
      }
    } else {
      if (OB_FAIL(task_stats_.at(i)->assign(task_stat))) {
        LOG_WARN("fail to assign task", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      common_value_.is_updated_ = true;
      common_value_.last_update_time_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

int ObCreateIndexPartitionStat::copy_task(const ObILongOpsTaskStat* src, ObILongOpsTaskStat*& dest)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(src));
  } else {
    void* buf = NULL;
    ObCreateIndexScanTaskStat* scan_stat = NULL;
    ObCreateIndexSortTaskStat* sort_stat = NULL;
    switch (src->type_) {
      case ObILongOpsTaskStat::TaskType::SCAN:
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObCreateIndexScanTaskStat)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret));
        } else {
          scan_stat = new (buf) ObCreateIndexScanTaskStat();
          *scan_stat = *static_cast<const ObCreateIndexScanTaskStat*>(src);
          dest = scan_stat;
        }
        break;
      case ObILongOpsTaskStat::TaskType::SORT:
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObCreateIndexSortTaskStat)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret));
        } else {
          sort_stat = new (buf) ObCreateIndexSortTaskStat();
          *sort_stat = *static_cast<const ObCreateIndexSortTaskStat*>(src);
          dest = sort_stat;
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid task type", K(ret), K(src->type_));
    }
  }
  return ret;
}

void ObCreateIndexPartitionStat::reset()
{
  for (int64_t i = 0; i < task_stats_.count(); ++i) {
    ObILongOpsTaskStat* stat = task_stats_.at(i);
    if (NULL != stat) {
      stat->~ObILongOpsTaskStat();
      stat = NULL;
    }
  }
  task_stats_.reset();
  allocator_.reset();
}

int ObCreateIndexPartitionStat::estimate_cost()
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(common_value_.lock_);
  if (common_value_.is_updated_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_stats_.count(); ++i) {
      ObILongOpsTaskStat* task_stat = task_stats_.at(i);
      if (OB_ISNULL(task_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, task stat must not be NULL", K(ret));
      } else if (OB_FAIL(task_stat->estimate_cost())) {
        LOG_WARN("fail to estimate task cost", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      double completed_tasks_cost = 0;
      double uncompleted_tasks_cost = 0;
      bool is_finish = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < task_stats_.count(); ++i) {
        ObILongOpsTaskStat* task_stat = task_stats_.at(i);
        if (ObILongOpsTaskStat::TaskState::FAIL == task_stat->state_) {
          completed_tasks_cost = 1;
          uncompleted_tasks_cost = 1;
          is_finish = true;
          break;
        } else if (ObILongOpsTaskStat::TaskState::SUCCESS == task_stat->state_) {
          completed_tasks_cost += task_stat->get_cost();
        } else {
          uncompleted_tasks_cost += task_stat->get_cost();
          is_finish = false;
        }
      }

      if (OB_SUCC(ret)) {
        const int64_t now = ObTimeUtility::current_time();
        double total_task_cost = completed_tasks_cost + uncompleted_tasks_cost;
        int64_t estimate_total_time = 0;
        if (is_finish) {
          common_value_.percentage_ = 100;
        }
        if (total_task_cost > 0) {
          double tmp_percentage = static_cast<double>(completed_tasks_cost) / total_task_cost;
          common_value_.percentage_ = static_cast<int64_t>(tmp_percentage * 100);
        }
        if (100 == common_value_.percentage_) {
          common_value_.finish_time_ = common_value_.last_update_time_;
        }
        common_value_.elapsed_time_ = common_value_.finish_time_ > 0
                                          ? common_value_.finish_time_ - common_value_.start_time_
                                          : now - common_value_.start_time_;
        if (common_value_.percentage_ > 0 && common_value_.elapsed_time_ > 0) {
          estimate_total_time = 100 * common_value_.elapsed_time_ / common_value_.percentage_;
          common_value_.remaining_time_ = std::max(0L, estimate_total_time - common_value_.elapsed_time_);
        }
      }
    }
    common_value_.is_updated_ = false;
  }
  return ret;
}

int ObCreateIndexPartitionStat::deep_copy(char* buf, const int64_t buf_len, ObILongOpsStat*& value) const
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || buf_len < deep_copy_size) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len));
  } else {
    ObCreateIndexPartitionStat* pvalue = new (buf) ObCreateIndexPartitionStat();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_stats_.count(); ++i) {
      ObILongOpsTaskStat* src = task_stats_.at(i);
      ObILongOpsTaskStat* dest = NULL;
      if (OB_FAIL(pvalue->copy_task(src, dest))) {
        STORAGE_LOG(WARN, "fail to copy task", K(ret));
      } else if (OB_FAIL(pvalue->task_stats_.push_back(dest))) {
        STORAGE_LOG(WARN, "fail to push back task", K(ret));
      }

      if (OB_FAIL(ret) && NULL != dest) {
        dest->~ObILongOpsTaskStat();
        dest = NULL;
      }
    }

    if (OB_SUCC(ret)) {
      pvalue->key_ = key_;
      pvalue->common_value_ = common_value_;
      value = pvalue;
    }
  }
  return ret;
}

int ObCreateIndexPartitionStat::check_can_purge(bool& can_purge)
{
  int ret = OB_SUCCESS;
  can_purge = false;
  const int64_t index_table_id = key_.index_table_id_;
  const uint64_t tenant_id = extract_tenant_id(index_table_id);
  const ObTableSchema* index_schema = NULL;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_full_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(index_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(index_table_id, index_schema))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else if (OB_ISNULL(index_schema)) {
    // index has been dropped, need delete
    can_purge = true;
  } else {
    const int64_t now = ObTimeUtility::current_time();
    const share::schema::ObIndexStatus index_status = index_schema->get_index_status();
    const bool is_dropped_schema = index_schema->is_dropped_schema();
    can_purge = is_final_index_status(index_status, is_dropped_schema) &&
                (common_value_.last_update_time_ / 1000 + MAX_LIFE_TIME < now / 1000);
  }

  return ret;
}

ObILongOpsStatHandle::ObILongOpsStatHandle() : ObResourceHandle<ObILongOpsStat>()
{}

ObILongOpsStatHandle::~ObILongOpsStatHandle()
{
  reset();
}

void ObILongOpsStatHandle::reset()
{
  if (NULL != ptr_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObLongOpsMonitor::get_instance().dec_handle_ref(*this))) {
      LOG_WARN("fail to dec handle ref", K(tmp_ret));
    }
  }
}

ObLongOpsMonitor::ObLongOpsMonitor() : is_inited_(false), map_()
{}

ObLongOpsMonitor::~ObLongOpsMonitor()
{}

int ObLongOpsMonitor::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLongOpsMonitor has been inited twice", K(ret));
  } else if (OB_FAIL(map_.init(
                 DEFAULT_BUCKET_NUM, ObModIds::OB_SSTABLE_LONG_OPS_MONITOR, TOTAL_LIMIT, HOLD_LIMIT, PAGE_SIZE))) {
    LOG_WARN("fail to init map", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLongOpsMonitor::add_long_ops_stat(const ObILongOpsKey& key, ObILongOpsStat& stat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLongOpsMonitor has not been inited", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid() || !stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key), K(stat));
  } else if (OB_FAIL(map_.set(key, stat))) {
    LOG_WARN("fail to set to map", K(ret), K(key));
  }
  return ret;
}

int ObLongOpsMonitor::get_long_ops_stat(const ObILongOpsKey& key, ObILongOpsStatHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLongOpsMonitor has not been inited", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(map_.get(key, handle))) {
    LOG_WARN("fail to get partition stat", K(ret));
  }
  return ret;
}

int ObLongOpsMonitor::remove_long_ops_stat(const ObILongOpsKey& key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLongOpsMonitor has not been inited", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_FAIL(map_.erase(key))) {
    LOG_WARN("fail to get from map", K(ret), K(key));
  }
  return ret;
}

int ObLongOpsMonitor::update_task_stat(const ObILongOpsKey& key, const ObILongOpsTaskStat& task_stat)
{
  int ret = OB_SUCCESS;
  ObILongOpsStatHandle handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLongOpsMonitor has not been inited", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid() || !task_stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key), K(task_stat));
  } else if (OB_FAIL(map_.get(key, handle))) {
    LOG_WARN("fail to get from map", K(ret), K(key));
  } else if (OB_ISNULL(handle.get_resource_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, partition stat must not be NULL", K(ret));
  } else if (OB_FAIL(handle.get_resource_ptr()->update_task_stat(task_stat))) {
    LOG_WARN("fail to update task stat", K(ret), K(task_stat));
  }
  return ret;
}

template <typename Callback>
int ObLongOpsMonitor::foreach (Callback& callback)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLongOpsMonitor has not been inited", K(ret));
  } else if (OB_FAIL(map_.foreach (callback))) {
    LOG_WARN("fail to foreach map", K(ret));
  }
  return ret;
}

int ObLongOpsMonitor::dec_handle_ref(ObILongOpsStatHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLongOpsMonitor has not been inited", K(ret));
  } else if (OB_FAIL(map_.dec_handle_ref(handle.ptr_))) {
    LOG_WARN("fail to dec handle ref", K(ret));
  }
  return ret;
}

ObLongOpsMonitorIterator::ObKeySnapshotCallback::ObKeySnapshotCallback(ObIArray<ObILongOpsKey>& key_snapshot)
    : key_snapshot_(key_snapshot)
{}

int ObLongOpsMonitorIterator::ObKeySnapshotCallback::operator()(PAIR& pair)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(key_snapshot_.push_back(pair.first))) {
    LOG_WARN("fail to push back key", K(ret));
  }
  return ret;
}

ObLongOpsMonitor& ObLongOpsMonitor::get_instance()
{
  static ObLongOpsMonitor instance;
  return instance;
}

ObLongOpsMonitorIterator::ObLongOpsMonitorIterator() : is_inited_(false), key_snapshot_(), key_cursor_(0)
{}

ObLongOpsMonitorIterator::~ObLongOpsMonitorIterator()
{}

int ObLongOpsMonitorIterator::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLongOpsMonitorIterator has been inited twice", K(ret));
  } else {
    is_inited_ = true;
    key_cursor_ = 0;
    if (OB_FAIL(make_key_snapshot())) {
      LOG_WARN("fail to make key snapshot", K(ret));
    }
  }
  return ret;
}

int ObLongOpsMonitorIterator::make_key_snapshot()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLongOpsMonitorIterator has not been inited", K(ret));
  } else {
    ObKeySnapshotCallback callback(key_snapshot_);
    if (OB_FAIL(ObLongOpsMonitor::get_instance().foreach (callback))) {
      LOG_WARN("fail to do foreach map", K(ret));
    }
  }
  return ret;
}

int ObLongOpsMonitorIterator::get_next_stat(ObILongOpsStatHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLongOpsMonitorIterator has not been inited", K(ret));
  } else if (OB_UNLIKELY(key_cursor_ >= key_snapshot_.count())) {
    ret = OB_ITER_END;
  } else {
    bool need_retry = true;
    while (OB_SUCC(ret) && need_retry && key_cursor_ < key_snapshot_.count()) {
      const ObILongOpsKey& key = key_snapshot_.at(key_cursor_);
      if (OB_FAIL(ObLongOpsMonitor::get_instance().get_long_ops_stat(key, handle))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          LOG_WARN("fail to get parition stat", K(ret), K(key));
        } else {
          need_retry = true;
          ret = OB_SUCCESS;
        }
      } else {
        need_retry = false;
      }
      ++key_cursor_;
    }
  }
  return ret;
}

ObPurgeCompletedMonitorInfoTask::ObPurgeCompletedMonitorInfoTask() : is_inited_(false), schema_service_(nullptr)
{}

int ObPurgeCompletedMonitorInfoTask::init(share::schema::ObMultiVersionSchemaService* schema_service, int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPurgeCompletedMonitorInfoTask has already been inited", K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(schema_service));
  } else {
    schema_service_ = schema_service;
    is_inited_ = true;
    if (OB_FAIL(TG_SCHEDULE(tg_id, *this, SCHEDULE_INTERVAL, true /*schedule repeatly*/))) {
      LOG_WARN("fail to schedule task", K(ret));
    }
  }
  return ret;
}

void ObPurgeCompletedMonitorInfoTask::destroy()
{
  is_inited_ = false;
  schema_service_ = NULL;
}

void ObPurgeCompletedMonitorInfoTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPurgeCompletedMonitorInfoTask has not been inited", K(ret));
  } else {
    int64_t iterated_count = 0;
    ObLongOpsMonitorIterator iter;
    if (OB_FAIL(iter.init())) {
      LOG_WARN("fail to init iterator", K(ret));
    }
    while (OB_SUCC(ret) && iterated_count < MAX_BATCH_COUNT) {
      bool can_purge = false;
      ObILongOpsStatHandle handle;
      if (OB_FAIL(iter.get_next_stat(handle))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next stat", K(ret));
        }
      } else if (OB_FAIL(handle.get_resource_ptr()->check_can_purge(can_purge))) {
        LOG_WARN("fail to check can purge", K(ret));
      } else if (can_purge) {
        if (OB_FAIL(ObLongOpsMonitor::get_instance().remove_long_ops_stat(handle.get_resource_ptr()->get_key()))) {
          LOG_WARN("fail to remove long ops stat", K(ret));
        }
      }
      ++iterated_count;
    }
  }
}
