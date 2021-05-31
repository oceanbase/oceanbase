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

#define USING_LOG_PREFIX SHARE

#include "ob_event_history_table_operator.h"
#include "share/config/ob_server_config.h"

namespace oceanbase {
namespace share {
using namespace lib;
using namespace common;

ObEventTableClearTask::ObEventTableClearTask(ObEventHistoryTableOperator& rs_event_operator,
    ObEventHistoryTableOperator& server_event_operator, common::ObWorkQueue& work_queue)
    : ObAsyncTimerTask(work_queue), rs_event_operator_(rs_event_operator), server_event_operator_(server_event_operator)
{
  set_retry_times(0);  // don't retry when process failed
}

int ObEventTableClearTask::process()
{
  int ret = OB_SUCCESS;
  if (!rs_event_operator_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("rs_event_operator not init", K(ret));
  } else if (!server_event_operator_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("server_event_operator not init", K(ret));
  } else if (OB_FAIL(rs_event_operator_.async_delete())) {
    LOG_WARN("async_delete failed", K(ret));
  } else if (OB_FAIL(server_event_operator_.async_delete())) {
    LOG_WARN("async_delete failed", K(ret));
  }
  return ret;
}

ObAsyncTask* ObEventTableClearTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObEventTableClearTask* task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN("buffer not large enough", K(buf_size));
  } else {
    task = new (buf) ObEventTableClearTask(rs_event_operator_, server_event_operator_, work_queue_);
  }
  return task;
}

////////////////////////////////////////////////////////////////
ObEventHistoryTableOperator::ObEventTableUpdateTask::ObEventTableUpdateTask(
    ObEventHistoryTableOperator& table_operator, const bool is_delete)
    : IObDedupTask(T_RS_ET_UPDATE), table_operator_(table_operator), is_delete_(is_delete)
{}

int ObEventHistoryTableOperator::ObEventTableUpdateTask::init(const char* ptr, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr) || buf_size <= 0) {
    LOG_WARN("invalid argument", KP(ptr), K(buf_size));
    ret = OB_INVALID_ARGUMENT;
  } else {
    sql_.assign_ptr(ptr, static_cast<int32_t>(buf_size));
  }

  return ret;
}

bool ObEventHistoryTableOperator::ObEventTableUpdateTask::is_valid() const
{
  return table_operator_.is_inited() && !sql_.empty();
}

int64_t ObEventHistoryTableOperator::ObEventTableUpdateTask::hash() const
{
  int64_t hash_value = 0;
  if (!this->is_valid()) {
    LOG_WARN("invalid event table update task", "task", *this);
  } else {
    hash_value = reinterpret_cast<int64_t>(sql_.ptr());
  }
  return hash_value;
}

bool ObEventHistoryTableOperator::ObEventTableUpdateTask::operator==(const common::IObDedupTask& other) const
{
  bool is_equal = false;
  if (!this->is_valid()) {
    LOG_WARN("invalid event table update task", "task", *this);
  } else if (this->get_type() != other.get_type()) {
    is_equal = false;
  } else {
    const ObEventTableUpdateTask& o = static_cast<const ObEventTableUpdateTask&>(other);
    if (!o.is_valid()) {
      LOG_WARN("invalid event table update task", "task", o);
    } else if (this == &other) {
      is_equal = true;
    } else {
      is_equal = (&(this->table_operator_) == &(o.table_operator_)) && this->sql_ == o.sql_ &&
                 this->is_delete_ == o.is_delete_;
    }
  }
  return is_equal;
}

IObDedupTask* ObEventHistoryTableOperator::ObEventTableUpdateTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObEventTableUpdateTask* task = NULL;
  if (!this->is_valid()) {
    LOG_WARN("invalid event table update task", "task", *this);
  } else if (NULL == buf || buf_size < get_deep_copy_size()) {
    LOG_WARN("invalid argument", "buf", reinterpret_cast<int64_t>(buf), K(buf_size), "need size", get_deep_copy_size());
  } else {
    task = new (buf) ObEventTableUpdateTask(table_operator_, is_delete_);
    char* ptr = buf + sizeof(ObEventTableUpdateTask);
    MEMCPY(ptr, sql_.ptr(), sql_.length());
    task->assign_ptr(ptr, sql_.length());
  }
  return task;
}

int ObEventHistoryTableOperator::ObEventTableUpdateTask::process()
{
  int ret = OB_SUCCESS;
  if (!this->is_valid()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("invalid event task update task", "task", *this, K(ret));
  } else if (OB_FAIL(table_operator_.process_task(sql_, is_delete_))) {
    LOG_WARN("process_task failed", K_(sql), K_(is_delete), K(ret));
  }
  return ret;
}

ObEventHistoryTableOperator::ObEventHistoryTableOperator()
    : inited_(false),
      stopped_(false),
      last_event_ts_(0),
      lock_(ObLatchIds::RS_EVENT_TS_LOCK),
      proxy_(NULL),
      event_queue_(),
      event_table_name_(NULL),
      self_addr_(),
      is_rootservice_event_history_(false)
{}

ObEventHistoryTableOperator::~ObEventHistoryTableOperator()
{}

int ObEventHistoryTableOperator::init(common::ObMySQLProxy& proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    const int64_t thread_count = 1;
    if (OB_FAIL(event_queue_.init(
            thread_count, "EvtHisUpdTask", TASK_QUEUE_SIZE, TASK_MAP_SIZE, TOTAL_LIMIT, HOLD_LIMIT, PAGE_SIZE))) {
      LOG_WARN("task_queue_ init failed",
          K(thread_count),
          LITERAL_K(TASK_QUEUE_SIZE),
          LITERAL_K(TASK_MAP_SIZE),
          LITERAL_K(TOTAL_LIMIT),
          LITERAL_K(HOLD_LIMIT),
          LITERAL_K(PAGE_SIZE),
          K(ret));
    } else {
      event_queue_.set_label(ObModIds::OB_RS_EVENT_QUEUE);
      proxy_ = &proxy;
      inited_ = true;
      stopped_ = false;
    }
  }
  return ret;
}

void ObEventHistoryTableOperator::destroy()
{
  stopped_ = true;
  event_queue_.destroy();
  // allocator should destroy after event_queue_ destroy
  inited_ = false;
}

int ObEventHistoryTableOperator::add_event(const char* module, const char* event)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == module || NULL == event) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("neither module or event can be NULL", KP(module), KP(event), K(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_gmt_create(now)) || OB_FAIL(dml.add_column("module", module)) ||
        OB_FAIL(dml.add_column("event", event))) {
      LOG_WARN("add column failed", K(ret));
    } else {
      ObSqlString sql;
      if (OB_FAIL(dml.splice_insert_sql(event_table_name_, sql))) {
        LOG_WARN("splice_insert_sql failed", "table_name", event_table_name_, K(ret));
      } else if (OB_FAIL(add_task(sql))) {
        LOG_WARN("add_task failed", K(sql), K(ret));
      }
    }
  }
  return ret;
}

int ObEventHistoryTableOperator::gen_event_ts(int64_t& event_ts)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    ObMutexGuard guard(lock_);
    event_ts = (last_event_ts_ >= now ? last_event_ts_ + 1 : now);
    last_event_ts_ = event_ts;
  }
  return ret;
}

int ObEventHistoryTableOperator::add_task(const ObSqlString& sql, const bool is_delete)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql is empty", K(sql), K(ret));
  } else if (stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("observer is stopped, cancel task", K(sql), K(is_delete), K(ret));
  } else {
    ObEventTableUpdateTask task(*this, is_delete);
    if (OB_FAIL(task.init(sql.ptr(), sql.length() + 1))) {  // extra byte for '\0'
      LOG_WARN("task init error", K(ret));
    } else if (OB_FAIL(event_queue_.add_task(task))) {
      if (OB_EAGAIN == ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("duplicated task is not expected to exist", K(task), K(ret));
      } else {
        LOG_WARN("event_queue_ add_task failed", K(task), K(ret));
      }
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObEventHistoryTableOperator::process_task(const ObString& sql, const bool is_delete)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql is empty", K(sql), K(ret));
  } else {
    if (stopped_) {
      ret = OB_CANCELED;
      LOG_WARN("server is stopped, cancel task", K(ret));
    } else {
      int64_t affected_rows = 0;
      if (!is_delete) {
        if (OB_FAIL(proxy_->write(sql.ptr(), affected_rows))) {
          LOG_WARN("execute sql failed", K(sql), K(ret));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected_rows expected to be one", K(affected_rows), K(ret));
        }
      } else {
        int64_t cnt = 0;
        const int64_t MAX_DELETE_TIMES = 10;
        while (OB_SUCCESS == ret && !stopped_) {
          if (OB_FAIL(proxy_->write(sql.ptr(), affected_rows))) {
            LOG_WARN("execute sql failed", K(sql), K(ret));
          } else if (0 == affected_rows) {
            LOG_INFO("finished to delete from event history table", K(sql));
            break;
          } else if (cnt > MAX_DELETE_TIMES) {
            LOG_INFO("delete cnt reach limit, schedule next round", K(sql));
            break;
          } else {
            LOG_INFO("delete rows from event history table", K(affected_rows), K(sql));
            cnt++;
          }
        }
      }
    }
  }
  return ret;
}

}  // namespace share
}  // end namespace oceanbase
