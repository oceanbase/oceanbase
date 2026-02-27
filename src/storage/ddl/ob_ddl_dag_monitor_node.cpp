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

#define USING_LOG_PREFIX STORAGE

#include "storage/ddl/ob_ddl_dag_monitor_node.h"
#include "storage/ddl/ob_ddl_dag_monitor_entry.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/container/ob_iarray.h"
#include "lib/oblog/ob_log_module.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"

namespace oceanbase
{
namespace storage
{

using namespace common;

ObDDLDagMonitorInfo::ObDDLDagMonitorInfo(common::ObIAllocator *allocator, share::ObITask *task)
  : allocator_(allocator),
    task_(task),
    task_type_(share::ObITask::TASK_TYPE_MAX),
    format_version_(0),
    create_timestamp_(0),
    finish_timestamp_(0),
    schedule_count_(0),
    exec_time_us_(0),
    ret_code_(common::OB_SUCCESS)
{
  // Assign timestamp in ctor body (not in default member initialization).
  create_timestamp_ = common::ObTimeUtility::current_time();

  // Cache immutable task attributes at construction time because task object may be destroyed
  // before virtual table reads the monitor info.
  if (OB_NOT_NULL(task_)) {
    task_type_ = task_->get_type();
  }
}

void ObDDLDagMonitorInfo::mark_finished()
{
  if (finish_timestamp_ <= 0) {
    finish_timestamp_ = common::ObTimeUtility::current_time();
  }
}

void ObDDLDagMonitorInfo::record_execute_stat(const int ret_code, const int64_t exec_time_us)
{
  ret_code_ = ret_code;
  exec_time_us_ += MAX(0, exec_time_us);
  ++schedule_count_;
}

int ObDDLDagMonitorInfo::convert_to_monitor_entry(ObDDLDagMonitorEntry &entry) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(entry.set_tenant_id(MTL_ID()))) {
    LOG_WARN("failed to set tenant id", K(ret));
  } else if (OB_FAIL(entry.set_task_id(task_))) {
    LOG_WARN("failed to set task id", K(ret));
  } else {
    entry.set_create_time(create_timestamp_);
    entry.set_finish_time(finish_timestamp_);
    // Default task_info: cached task type string.
    const char *type_str = nullptr;
    if (task_type_ >= 0 && task_type_ <= share::ObITask::TASK_TYPE_MAX) {
      type_str = share::ObITask::ObITaskTypeStr[task_type_];
    }
    const common::ObString task_info = common::ObString::make_string(OB_ISNULL(type_str) ? "UNKNOWN" : type_str);
    if (OB_FAIL(entry.set_task_info(task_info))) {
      LOG_WARN("failed to set task info", K(ret));
    } else if (OB_FAIL(entry.set_format_version(get_format_version()))) {
      LOG_WARN("failed to set format version", K(ret));
    } else {
      // Build JSON message
      ObJsonObject *json_obj = nullptr;
      ObJsonInt *schedule_cnt_node = nullptr;
      ObJsonInt *exec_time_node = nullptr;
      ObJsonInt *ret_code_node = nullptr;
      ObString json_str;
      common::ObArenaAllocator &allocator = entry.get_allocator();
      if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, json_obj))) {
        LOG_WARN("failed to get json object", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, schedule_count_, schedule_cnt_node))
                 || OB_FAIL(json_obj->add("schedule_count", schedule_cnt_node))) {
        LOG_WARN("failed to add schedule_count to json", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, exec_time_us_, exec_time_node))
                 || OB_FAIL(json_obj->add("exec_time_us", exec_time_node))) {
        LOG_WARN("failed to add exec_time_us to json", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, ret_code_, ret_code_node))
                 || OB_FAIL(json_obj->add("ret_code", ret_code_node))) {
        LOG_WARN("failed to add ret_code to json", K(ret));
      } else if (OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(allocator, json_obj, json_str))) {
        LOG_WARN("failed to print json to string", K(ret));
      } else if (OB_FAIL(entry.set_message(json_str))) {
        LOG_WARN("failed to set message", K(ret));
      }
    }
  }
  return ret;
}

ObDDLDagMonitorNode::Key::Key()
  : dag_ptr_(0),
    trace_id_()
{
}

ObDDLDagMonitorNode::Key::Key(const void *dag_ptr,
                                      const ObCurTraceId::TraceId &trace_id)
  : dag_ptr_(reinterpret_cast<uint64_t>(dag_ptr)),
    trace_id_(trace_id)
{
}

bool ObDDLDagMonitorNode::Key::is_valid() const
{
  return dag_ptr_ != 0 && trace_id_.is_valid();
}

bool ObDDLDagMonitorNode::Key::operator==(const Key &other) const
{
  return dag_ptr_ == other.dag_ptr_
      && trace_id_ == other.trace_id_;
}

uint64_t ObDDLDagMonitorNode::Key::hash() const
{
  uint64_t hash_val = 0;
  hash_val = common::murmurhash(&dag_ptr_, sizeof(dag_ptr_), hash_val);
  hash_val = common::murmurhash(&trace_id_, sizeof(trace_id_), hash_val);
  return hash_val;
}

int ObDDLDagMonitorNode::Key::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return common::OB_SUCCESS;
}

ObDDLDagMonitorNode::ObDDLDagMonitorNode(common::ObIAllocator *allocator)
  : is_inited_(false),
    ref_cnt_(0),
    allocator_(allocator),
    key_(),
    create_timestamp_(0),
    finish_timestamp_(0),
    lock_(common::ObLatchIds::DDL_DAG_MONITOR_LOCK),
    info_list_()
{
}

ObDDLDagMonitorNode::~ObDDLDagMonitorNode()
{
  reset();
}

void ObDDLDagMonitorNode::reset()
{
  key_ = Key();
  create_timestamp_ = 0;
  finish_timestamp_ = 0;
  // Free all task-side monitor infos. They are allocated from allocator_ (node's allocator).
  (void)clean_infos(false /*only_finished*/);
  allocator_ = nullptr;
  is_inited_ = false;
  ref_cnt_ = 0;
}

void ObDDLDagMonitorNode::inc_ref()
{
  (void)ATOMIC_AAF(&ref_cnt_, 1);
}

void ObDDLDagMonitorNode::dec_ref()
{
  const int64_t new_val = ATOMIC_SAF(&ref_cnt_, 1);
  if (OB_UNLIKELY(new_val < 0)) {
    // Should never happen; indicates ref counting imbalance.
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "independent dag monitor node ref_cnt_ became negative", K(new_val), "key", key_);
  } else if (0 == new_val) {
    // Last reference: release node and its infos.
    common::ObIAllocator *alloc = allocator_;
    this->~ObDDLDagMonitorNode();
    if (OB_NOT_NULL(alloc)) {
      alloc->free(this);
    }
  }
}

int ObDDLDagMonitorNode::init(const Key &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("monitor node init twice", K(ret));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node key is invalid", K(ret), K(key));
  } else {
    key_ = key;
    create_timestamp_ = ObTimeUtility::current_time();
    finish_timestamp_ = 0;
    is_inited_ = true;
  }
  return ret;
}

bool ObDDLDagMonitorNode::is_expired(const int64_t current_time, const int64_t ttl_us) const
{
  return is_finished() && (current_time - finish_timestamp_ > ttl_us);
}

void ObDDLDagMonitorNode::mark_finished()
{
  finish_timestamp_ = ObTimeUtility::current_time();
}

int ObDDLDagMonitorNode::get_all_infos(ObIArray<ObDDLDagMonitorInfo *> &infos) const
{
  int ret = OB_SUCCESS;
  infos.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("monitor node not init", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    DLIST_FOREACH_NORET(iter, info_list_) {
      if (OB_FAIL(infos.push_back(const_cast<ObDDLDagMonitorInfo *>(iter)))) {
        LOG_WARN("push back monitor info failed", K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObDDLDagMonitorNode::clean_infos(const bool only_finished)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("monitor node not init", K(ret));
  } else {
    SpinWLockGuard guard(lock_);
    DLIST_FOREACH_REMOVESAFE_NORET(iter, info_list_) {
      const bool need_remove = !only_finished || iter->get_task_finish_timestamp() > 0;
      if (need_remove) {
        info_list_.remove(iter);
        iter->~ObDDLDagMonitorInfo();
        if (OB_NOT_NULL(allocator_)) {
          allocator_->free(iter);
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
