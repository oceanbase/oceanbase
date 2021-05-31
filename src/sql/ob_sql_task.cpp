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

#define USING_LOG_PREFIX SQL

#include "lib/queue/ob_fixed_queue.h"
#include "ob_sql_task.h"
#include "ob_sql.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::obrpc;

void ObSqlTaskHandler::reset()
{
  task_ = NULL;
  sql_engine_ = NULL;
}

int ObSqlTaskHandler::init(observer::ObSrvTask* task, ObSql* sql_engine)
{
  int ret = OB_SUCCESS;
  if (NULL == task || NULL == sql_engine) {
    ret = OB_INVALID_ARGUMENT;
    ;
    SQL_LOG(WARN, "invalid argument", K(ret), KP(task), KP(sql_engine));
  } else {
    set_task_mark();
    task_ = task;
    sql_engine_ = sql_engine;
  }
  return ret;
}

int ObSqlTaskHandler::process()
{
  int ret = OB_SUCCESS;
  if (NULL == task_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "task is null, unexpected error", K(ret), KP_(task));
  } else if (NULL == sql_engine_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "sql engine is null, unexpected error", K(ret), KP_(sql_engine));
  } else {
    ObSqlTask* task = static_cast<ObSqlTask*>(task_);
    ObCurTraceId::set(task->get_trace_id());
    if (OB_FAIL(sql_engine_->handle_batch_req(
            task->get_msg_type(), task->get_req_ts(), task->get_buf(), task->get_size()))) {
      SQL_LOG(WARN, "handle sql task failed", K(ret), K(*task));
    }
  }
  return ret;
}

void ObSqlTask::reset()
{
  msg_type_ = 0;
  size_ = 0;
  handler_.reset();
}

int ObSqlTask::init(
    const int msg_type, const ObReqTimestamp& req_ts, const char* buf, const int64_t size, ObSql* sql_engine)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(0 >= size) || OB_UNLIKELY(MAX_SQL_TASK_SIZE < size) || OB_ISNULL(sql_engine)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "invalid argument", K(ret), KP(buf), K(size), KP(sql_engine), K(msg_type));
  } else if (OB_FAIL(handler_.init(this, sql_engine))) {
    SQL_LOG(WARN, "ObSqlTaskHandler init failed", K(ret));
  } else {
    set_type(ObRequest::OB_SQL_TASK);
    msg_type_ = msg_type;
    memcpy(buf_, buf, size);
    size_ = size;
    req_ts_ = req_ts;
    ObCurTraceId::TraceId* trace_id = ObCurTraceId::get_trace_id();
    if (OB_LIKELY(NULL != trace_id)) {
      trace_id_.set(*trace_id);
    }
  }
  return ret;
}

int ObSqlTaskFactory::init()
{
  int ret = OB_SUCCESS;
  int64_t fixed_num = 0;
  if (!lib::is_mini_mode()) {
    fixed_num = NORMAL_FIXED_TASK_NUM;
  } else {
    fixed_num = MINI_FIXED_TASK_NUM;
  }
  if (OB_FAIL(fixed_queue_.init(fixed_num))) {
    SQL_LOG(WARN, "init fixed queue failed", K(ret), K(fixed_num));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < fixed_num; i++) {
      ObSqlTask* task = alloc_(common::OB_SERVER_TENANT_ID);
      if (OB_ISNULL(task)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        task->set_fixed_alloc();
        if (OB_FAIL(fixed_queue_.push(task))) {
          free_(task);
          task = NULL;
        }
      }
    }
  }
  return ret;
}

void ObSqlTaskFactory::destroy()
{
  ObSqlTask* task = NULL;
  while (OB_SUCCESS == fixed_queue_.pop(task)) {
    free_(task);
    task = NULL;
  }
}

ObSqlTask* ObSqlTaskFactory::alloc(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlTask* task = NULL;
  if (OB_FAIL(fixed_queue_.pop(task))) {
    task = alloc_(tenant_id);
  }
  return task;
}

void ObSqlTaskFactory::free(ObSqlTask* task)
{
  int ret = OB_SUCCESS;
  if (NULL != task) {
    if (task->is_fixed_alloc()) {
      if (OB_FAIL(fixed_queue_.push(task))) {
        SQL_LOG(WARN, "free sql task failed", K(ret));
      }
    } else {
      free_(task);
    }
  }
}

ObSqlTaskFactory& ObSqlTaskFactory::get_instance()
{
  static ObSqlTaskFactory instance;
  return instance;
}

ObSqlTask* ObSqlTaskFactory::alloc_(const uint64_t tenant_id)
{
  ObMemAttr memattr(tenant_id, "OB_SQL_TASK");
  void* ptr = NULL;
  ObSqlTask* task = NULL;
  if (NULL != (ptr = ob_malloc(sizeof(ObSqlTask), memattr))) {
    task = new (ptr) ObSqlTask();
  }
  return task;
}

void ObSqlTaskFactory::free_(ObSqlTask* task)
{
  if (NULL != task) {
    task->~ObSqlTask();
    ob_free(task);
    task = NULL;
  }
}
