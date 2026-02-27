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

#include <cstdio>
#include <cstring>
#include "storage/ddl/ob_ddl_dag_monitor_entry.h"
#include "lib/oblog/ob_log_module.h"
#include "share/rc/ob_tenant_base.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{

using namespace common;

ObDDLDagMonitorEntry::ObDDLDagMonitorEntry()
  : arena_(ObMemAttr(MTL_ID(), "IndDagMonEntry")),
    tenant_id_(OB_INVALID_TENANT_ID),
    dag_id_(),
    dag_info_(),
    task_id_(),
    task_info_(),
    format_version_(0),
    trace_id_(),
    create_time_(0),
    finish_time_(0)
{
}

ObDDLDagMonitorEntry::~ObDDLDagMonitorEntry()
{
}

void ObDDLDagMonitorEntry::reuse()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  dag_id_.reset();
  dag_info_.reset();
  task_id_.reset();
  task_info_.reset();
  format_version_ = 0;
  trace_id_.reset();
  create_time_ = 0;
  finish_time_ = 0;
  message_.reset();
  arena_.reuse();
}

static int trace_id_to_string_(const ObCurTraceId::TraceId &trace_id, char *buf, const int64_t buf_len, int64_t &out_len)
{
  int ret = OB_SUCCESS;
  out_len = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    const int64_t n = trace_id.to_string(buf, buf_len);
    if (n <= 0 || n >= buf_len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("format trace_id failed", K(ret), K(n), K(buf_len));
    } else {
      out_len = n;
    }
  }
  return ret;
}

int ObDDLDagMonitorEntry::set_dag_id(const void *dag_ptr)
{
  int ret = OB_SUCCESS;
  char buf[MAX_ID_LEN] = {0};
  const int64_t n = snprintf(buf, sizeof(buf), "%p", dag_ptr);
  if (n <= 0 || n >= static_cast<int64_t>(sizeof(buf))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("format dag_id failed", K(ret), K(n), KP(dag_ptr));
  } else {
    ret = deep_copy_string(ObString(static_cast<int32_t>(n), buf), dag_id_);
  }
  return ret;
}

int ObDDLDagMonitorEntry::set_trace_id(const ObCurTraceId::TraceId &trace_id)
{
  int ret = OB_SUCCESS;
  char buf[MAX_TRACE_ID_LEN] = {0};
  int64_t n = 0;
  if (OB_FAIL(trace_id_to_string_(trace_id, buf, sizeof(buf), n))) {
    LOG_WARN("format trace_id failed", K(ret));
  } else {
    ret = deep_copy_string(ObString(static_cast<int32_t>(n), buf), trace_id_);
  }
  return ret;
}

int ObDDLDagMonitorEntry::set_task_id(const void *task_ptr)
{
  int ret = OB_SUCCESS;
  if (nullptr != task_ptr) {
    char buf[MAX_ID_LEN] = {0};
    const int64_t n = snprintf(buf, sizeof(buf), "%p", task_ptr);
    if (n <= 0 || n >= static_cast<int64_t>(sizeof(buf))) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("format task_id failed", K(ret), K(n), KP(task_ptr));
    } else {
      ret = deep_copy_string(ObString(static_cast<int32_t>(n), buf), task_id_);
    }
  }
  return ret;
}

int ObDDLDagMonitorEntry::set_dag_info(const ObString &dag_info)
{
  int ret = OB_SUCCESS;
  if (dag_info.length() > MAX_INFO_LEN) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("dag_info too long", K(ret), K(dag_info.length()));
  } else {
    ret = deep_copy_string(dag_info, dag_info_);
  }
  return ret;
}

int ObDDLDagMonitorEntry::set_task_info(const ObString &task_info)
{
  int ret = OB_SUCCESS;
  if (task_info.length() > MAX_INFO_LEN) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("task_info too long", K(ret), K(task_info.length()));
  } else {
    ret = deep_copy_string(task_info, task_info_);
  }
  return ret;
}

int ObDDLDagMonitorEntry::set_message(const ObString &message)
{
  int ret = OB_SUCCESS;
  if (message.length() > MAX_MESSAGE_LEN) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("message too long", K(ret), K(message.length()));
  } else {
    ret = deep_copy_string(message, message_);
  }
  return ret;
}

int ObDDLDagMonitorEntry::deep_copy_string(const ObString &src, ObString &dest)
{
  int ret = OB_SUCCESS;
  if (src.empty()) {
    dest.reset();
  } else {
    char *buf = static_cast<char *>(arena_.alloc(src.length()));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(src.length()));
    } else {
      MEMCPY(buf, src.ptr(), src.length());
      dest.assign(buf, static_cast<int32_t>(src.length()));
    }
  }
  return ret;
}

DEF_TO_STRING(ObDDLDagMonitorEntry)
{
  int64_t pos = 0;
  J_KV(K_(tenant_id),
       K_(dag_id),
       K_(dag_info),
       K_(task_id),
       K_(task_info),
       K_(format_version),
       K_(trace_id),
       K_(create_time),
       K_(finish_time),
       K_(message),
       K_(arena));
  return pos;
}

} // namespace storage
} // namespace oceanbase
