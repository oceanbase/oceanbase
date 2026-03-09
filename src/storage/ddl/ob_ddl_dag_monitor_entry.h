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

#ifndef __OB_STORAGE_DDL_DAG_MONITOR_ENTRY_H__
#define __OB_STORAGE_DDL_DAG_MONITOR_ENTRY_H__

#include "lib/allocator/page_arena.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/string/ob_string.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace storage
{

// Monitor entry used for virtual table display. It is generated on the fly.
class ObDDLDagMonitorEntry
{
public:
  // NOTE: keep lengths aligned with `__all_virtual_ddl_dag_monitor` schema definition.
  static const int64_t MAX_ID_LEN = 64;              // dag_id/task_id
  static const int64_t MAX_INFO_LEN = 256;           // dag_info/task_info
  static const int64_t MAX_MESSAGE_LEN = 1024;
  static const int64_t MAX_TRACE_ID_LEN = 128;

  ObDDLDagMonitorEntry();
  ~ObDDLDagMonitorEntry();

  // Reuse arena pages and clear all fields for next use.
  void reuse();

  int set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; return common::OB_SUCCESS; }
  // dag_id is the pointer address of the independent dag object, printed as hex string (e.g. "0x7f...").
  int set_dag_id(const void *dag_ptr);
  int set_trace_id(const common::ObCurTraceId::TraceId &trace_id);
  int set_task_id(const void *task_ptr);

  int set_dag_info(const common::ObString &dag_info);
  int set_task_info(const common::ObString &task_info);
  int set_format_version(const uint64_t format_version) { format_version_ = format_version; return common::OB_SUCCESS; }
  int set_create_time(const int64_t create_time) { create_time_ = create_time; return common::OB_SUCCESS; }
  int set_finish_time(const int64_t finish_time) { finish_time_ = finish_time; return common::OB_SUCCESS; }

  int set_message(const common::ObString &message);

  uint64_t get_tenant_id() const { return tenant_id_; }
  const common::ObString &get_dag_id() const { return dag_id_; }
  const common::ObString &get_dag_info() const { return dag_info_; }
  const common::ObString &get_task_id() const { return task_id_; }
  const common::ObString &get_task_info() const { return task_info_; }
  uint64_t get_format_version() const { return format_version_; }
  const common::ObString &get_trace_id() const { return trace_id_; }
  int64_t get_create_time() const { return create_time_; }
  int64_t get_finish_time() const { return finish_time_; }
  const common::ObString &get_message() const { return message_; }
  common::ObArenaAllocator &get_allocator() { return arena_; }

  DECLARE_TO_STRING;

private:
  int deep_copy_string(const common::ObString &src, common::ObString &dest);

private:
  common::ObArenaAllocator arena_;
  uint64_t tenant_id_;
  common::ObString dag_id_;
  common::ObString dag_info_;
  common::ObString task_id_;
  common::ObString task_info_;
  uint64_t format_version_;
  common::ObString trace_id_;
  int64_t create_time_;
  int64_t finish_time_;
  common::ObString message_;
};

} // namespace storage
} // namespace oceanbase

#endif // __OB_STORAGE_DDL_DAG_MONITOR_ENTRY_H__
