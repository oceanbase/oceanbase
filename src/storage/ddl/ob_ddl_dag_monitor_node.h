/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef __OB_STORAGE_DDL_DAG_MONITOR_NODE_H__
#define __OB_STORAGE_DDL_DAG_MONITOR_NODE_H__

#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_iarray.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h" // for share::ObITask
#include <type_traits>

namespace oceanbase
{
namespace common
{
class ObIAllocator;
} // namespace common
namespace share
{
class ObITask;
} // namespace share

namespace storage
{

class ObDDLDagMonitorEntry;

// Task-side monitor info interface (owned by ObDDLDagMonitorNode).
// Independent DAG tasks that need monitoring should implement a derived class and provide a conversion
// to ObDDLDagMonitorEntry for virtual table display.
class ObDDLDagMonitorInfo : public common::ObDLinkBase<ObDDLDagMonitorInfo>
{
public:
  explicit ObDDLDagMonitorInfo(common::ObIAllocator *allocator, share::ObITask *task);
  virtual ~ObDDLDagMonitorInfo() = default;

  common::ObIAllocator *get_allocator() const { return allocator_; }
  uint64_t get_format_version() const { return format_version_; }
  // NOTE: Caller must ensure the lifecycle of task_ while using the returned pointer.
  // The monitor info itself may outlive the task object.
  share::ObITask *get_task() const { return task_; }
  share::ObITask::ObITaskType get_task_type() const { return task_type_; }
  int64_t get_task_create_timestamp() const { return create_timestamp_; }
  int64_t get_task_finish_timestamp() const { return finish_timestamp_; }
  int64_t get_schedule_count() const { return schedule_count_; }
  int64_t get_exec_time_us() const { return exec_time_us_; }
  int get_ret_code() const { return ret_code_; }

  void mark_finished();
  void record_execute_stat(const int ret_code, const int64_t exec_time_us);

  // Default implementation provides basic task monitoring:
  // - task_info: task type string
  // - key/value + message: task register_time, finish_time(best-effort)
  // NOTE: dag_id/trace_id/create_time/finish_time at dag level are filled by virtual table iterator (node-level).
  virtual int convert_to_monitor_entry(ObDDLDagMonitorEntry &entry) const;
  TO_STRING_KV(KP_(allocator), KP_(task), K_(format_version), K_(create_timestamp), K_(finish_timestamp), K_(schedule_count), K_(exec_time_us), K_(ret_code));

protected:
  common::ObIAllocator *allocator_;
  share::ObITask *task_;
  share::ObITask::ObITaskType task_type_;
  uint64_t format_version_;
  // Task-side timestamps: independent from dag/node time.
  int64_t create_timestamp_;
  mutable int64_t finish_timestamp_;
  int64_t schedule_count_;
  int64_t exec_time_us_;
  int ret_code_;
};

// A monitor node is bound to one independent DAG execution (dag_ptr, trace_id).
// It owns multiple monitor infos, each representing one monitored DAG task.
class ObDDLDagMonitorNode
{
public:
  struct Key
  {
  public:
    Key();
    Key(const void *dag_ptr,
        const common::ObCurTraceId::TraceId &trace_id);

    bool is_valid() const;
    bool operator==(const Key &other) const;
    uint64_t hash() const;
    int hash(uint64_t &hash_val) const;
    TO_STRING_KV(K_(dag_ptr), K_(trace_id));

  public:
    uint64_t dag_ptr_;
    common::ObCurTraceId::TraceId trace_id_;
  };

  explicit ObDDLDagMonitorNode(common::ObIAllocator *allocator);
  ~ObDDLDagMonitorNode();

  void reset();

  // Reference counting:
  // - One base reference is held by ObDDLDagMonitorMgr when node is inserted into node_map_.
  // - Virtual table / other readers should call inc_ref() before using the pointer, and dec_ref() when done.
  void inc_ref();          // +1
  void dec_ref();          // -1, free node when ref_cnt_ reaches 0

  int init(const Key &key);

  // Mark node finished (DAG destroyed). Infos are NOT deleted here.
  void mark_finished();

  // Allocate a new task-side monitor info (derived from ObDDLDagMonitorInfo) owned by this node.
  // The concrete type should implement convert_to_monitor_entry().
  template <typename T>
  int alloc_monitor_info(share::ObITask *task, T *&info);

  // Collect all infos for display (snapshot pointers).
  int get_all_infos(common::ObIArray<ObDDLDagMonitorInfo *> &infos) const;

  // Clean task-side monitor infos.
  // - only_finished=true: remove finished infos
  // - only_finished=false: remove all infos
  int clean_infos(const bool only_finished);

  bool is_finished() const { return finish_timestamp_ > 0; }
  bool is_expired(const int64_t current_time, const int64_t ttl_us) const;

  const Key &get_key() const { return key_; }
  const void *get_dag_ptr() const { return reinterpret_cast<const void *>(key_.dag_ptr_); }
  const common::ObCurTraceId::TraceId &get_trace_id() const { return key_.trace_id_; }
  int64_t get_create_timestamp() const { return create_timestamp_; }
  int64_t get_finish_timestamp() const { return finish_timestamp_; }
  TO_STRING_KV(K_(is_inited), K_(ref_cnt), KP_(allocator), K_(key), K_(create_timestamp), K_(finish_timestamp), K(info_list_.get_size()));

private:
  bool is_inited_;
  int64_t ref_cnt_;
  common::ObIAllocator *allocator_;
  Key key_;
  int64_t create_timestamp_;
  int64_t finish_timestamp_;
  mutable common::SpinRWLock lock_;
  common::ObDList<ObDDLDagMonitorInfo> info_list_;
};

template <typename T>
int ObDDLDagMonitorNode::alloc_monitor_info(share::ObITask *task, T *&info)
{
  int ret = common::OB_SUCCESS;
  info = nullptr;
  static_assert(std::is_base_of<ObDDLDagMonitorInfo, T>::value, "T must derive from ObDDLDagMonitorInfo");
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "monitor node not init", K(ret));
  } else if (OB_ISNULL(info = OB_NEWx(T, allocator_, allocator_, task))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to allocate memory", K(ret));
  } else {
    {
      common::SpinWLockGuard guard(lock_);
      if (!info_list_.add_last(info)) {
        ret = common::OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "failed to add monitor info", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      info->~T();
      allocator_->free(info);
      info = nullptr;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

#endif // __OB_STORAGE_DDL_DAG_MONITOR_NODE_H__
