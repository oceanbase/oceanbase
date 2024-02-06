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
#include "share/deadlock/ob_deadlock_inner_table_service.h"
#include "share/ob_debug_sync_point.h"
#include "share/ob_debug_sync.h"

namespace oceanbase
{
namespace share
{
using namespace lib;
using namespace common;

ObEventTableClearTask::ObEventTableClearTask(
    ObEventHistoryTableOperator &rs_event_operator,
    ObEventHistoryTableOperator &server_event_operator,
    ObEventHistoryTableOperator &deadlock_history_operator,
    common::ObWorkQueue &work_queue)
    : ObAsyncTimerTask(work_queue),
      rs_event_operator_(rs_event_operator),
      server_event_operator_(server_event_operator),
      deadlock_history_operator_(deadlock_history_operator)
{
  set_retry_times(0);  // don't retry when process failed
}

int ObEventTableClearTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!rs_event_operator_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("rs_event_operator not init", K(ret));
  } else if (!server_event_operator_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("server_event_operator not init", K(ret));
  } else if (!deadlock_history_operator_.is_inited()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("deadlock_history_operator_ not init", K(ret));
  } else {
    if (OB_TMP_FAIL(rs_event_operator_.async_delete())) {
      LOG_WARN("async_delete failed", KR(tmp_ret));
    }
    if (OB_TMP_FAIL(server_event_operator_.async_delete())) {
      LOG_WARN("async_delete failed", KR(tmp_ret));
    }
    if (OB_TMP_FAIL(deadlock_history_operator_.async_delete())) {
      LOG_WARN("async_delete failed", KR(tmp_ret));
    }
  }
  return ret;
}

ObAsyncTask *ObEventTableClearTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObEventTableClearTask *task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN_RET(OB_BUF_NOT_ENOUGH, "buffer not large enough", K(buf_size));
  } else {
    task = new(buf) ObEventTableClearTask(rs_event_operator_,
                                          server_event_operator_,
                                          deadlock_history_operator_,
                                          work_queue_);
  }
  return task;
}

////////////////////////////////////////////////////////////////
ObEventHistoryTableOperator::ObEventTableUpdateTask::ObEventTableUpdateTask(
    ObEventHistoryTableOperator &table_operator, const bool is_delete, const int64_t create_time)
  : IObDedupTask(T_RS_ET_UPDATE), table_operator_(table_operator), is_delete_(is_delete),
  create_time_(create_time)
{
}


int ObEventHistoryTableOperator::ObEventTableUpdateTask::init(const char *ptr,
    const int64_t buf_size)
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
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid event table update task", "task", *this);
  } else {
    hash_value = reinterpret_cast<int64_t>(sql_.ptr());
  }
  return hash_value;
}

bool ObEventHistoryTableOperator::ObEventTableUpdateTask::operator==(
    const common::IObDedupTask &other) const
{
  bool is_equal = false;
  if (!this->is_valid()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid event table update task", "task", *this);
  } else if (this->get_type() != other.get_type()) {
    is_equal = false;
  } else {
    const ObEventTableUpdateTask &o = static_cast<const ObEventTableUpdateTask &>(other);
    if (!o.is_valid()) {
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid event table update task", "task", o);
    } else if (this == &other) {
      is_equal = true;
    } else {
      is_equal = (&(this->table_operator_) == &(o.table_operator_))
          && this->sql_ == o.sql_ && this->is_delete_ == o.is_delete_;
      //no need take care of create_time
    }
  }
  return is_equal;
}

IObDedupTask *ObEventHistoryTableOperator::ObEventTableUpdateTask::deep_copy(
    char *buf, const int64_t buf_size) const
{
  ObEventTableUpdateTask *task = NULL;
  if (!this->is_valid()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid event table update task", "task", *this);
  } else if (NULL == buf || buf_size < get_deep_copy_size()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid argument", "buf", reinterpret_cast<int64_t>(buf), K(buf_size),
        "need size", get_deep_copy_size());
  } else {
    task = new (buf) ObEventTableUpdateTask(table_operator_, is_delete_, create_time_);
    char *ptr = buf + sizeof(ObEventTableUpdateTask);
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
  } else if (OB_FAIL(table_operator_.process_task(sql_, is_delete_, create_time_))) {
      LOG_WARN("process_task failed", K_(sql), K_(is_delete), KR(ret), K(create_time_));
  }
  return ret;
}

ObEventHistoryTableOperator::ObEventHistoryTableOperator()
  : inited_(false), stopped_(false), last_event_ts_(0),
    lock_(ObLatchIds::RS_EVENT_TS_LOCK), proxy_(NULL), event_queue_(),
    event_table_name_(NULL), self_addr_(), is_rootservice_event_history_(false),
    is_server_event_history_(false),
    timer_()
{
}

ObEventHistoryTableOperator::~ObEventHistoryTableOperator()
{
}

int ObEventHistoryTableOperator::init(common::ObMySQLProxy &proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    const int64_t thread_count = 1;
    const int64_t queue_size_square_of_2 = 10;
    if (OB_FAIL(event_queue_.init(thread_count, "EvtHisUpdTask", TASK_QUEUE_SIZE, TASK_MAP_SIZE,
        TOTAL_LIMIT, HOLD_LIMIT, PAGE_SIZE))) {
      LOG_WARN("task_queue_ init failed", K(thread_count), LITERAL_K(TASK_QUEUE_SIZE),
          LITERAL_K(TASK_MAP_SIZE), LITERAL_K(TOTAL_LIMIT), LITERAL_K(HOLD_LIMIT),
          LITERAL_K(PAGE_SIZE), K(ret));
    } else if (is_server_event_history_ &&
          OB_FAIL(timer_.init_and_start(thread_count, 5_s, "EventTimer", queue_size_square_of_2))) {
      LOG_WARN("int global event report timer failed", KR(ret));
    } else {
      event_queue_.set_attr(SET_USE_500(ObMemAttr(OB_SERVER_TENANT_ID, ObModIds::OB_RS_EVENT_QUEUE)));
      proxy_ = &proxy;
      inited_ = true;
      stopped_ = false;
    }
  }
  return ret;
}

void ObEventHistoryTableOperator::stop()
{
  stopped_ = true;
  event_queue_.stop();
  timer_.stop();
}

void ObEventHistoryTableOperator::wait()
{
  event_queue_.wait();
  timer_.wait();
}

void ObEventHistoryTableOperator::destroy()
{
  event_queue_.destroy();
  timer_.destroy();
  // allocator should destroy after event_queue_ destroy
  inited_ = false;
}

int ObEventHistoryTableOperator::gen_event_ts(int64_t &event_ts)
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

int ObEventHistoryTableOperator::default_async_delete()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", K(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    ObSqlString sql;
    const bool is_delete = true;
    const int64_t delete_timestap = now - GCONF.ob_event_history_recycle_interval;
    if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE gmt_create < usec_to_time(%ld) LIMIT 1024",
            event_table_name_, delete_timestap))) {
      SHARE_LOG(WARN, "assign_fmt failed", K(ret), K(event_table_name_));
    } else if (OB_FAIL(add_task(sql, is_delete, now))) {
      SHARE_LOG(WARN, "add_task failed", K(sql), K(is_delete), K(ret));
    }
  }
  return ret;
}

int ObEventHistoryTableOperator::add_task(const ObSqlString &sql, const bool is_delete, const int64_t create_time)
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
    int64_t new_create_time = OB_INVALID_TIMESTAMP == create_time ?
      ObTimeUtility::current_time() : create_time;
    ObEventTableUpdateTask task(*this, is_delete, new_create_time);
    if (OB_FAIL(task.init(sql.ptr(), sql.length() + 1))) { // extra byte for '\0'
      LOG_WARN("task init error", K(ret));
    } else if (OB_FAIL(event_queue_.add_task(task))) {
      if (OB_EAGAIN == ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("duplicated task is not expected to exist", K(task), K(ret));
      } else {
        LOG_WARN("event_queue_ add_task failed", K(task), K(ret), K(new_create_time));
      }
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObEventHistoryTableOperator::process_task(const ObString &sql, const bool is_delete, const int64_t create_time)
{
  int ret = OB_SUCCESS;

  DEBUG_SYNC(BEFORE_PROCESS_EVENT_TASK);
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
        int tmp_ret = OB_SUCCESS;
        while (OB_SUCCESS == ret && !stopped_) {
          if (OB_FAIL(proxy_->write(sql.ptr(), affected_rows))) {
            LOG_WARN("execute sql failed", K(sql), K(ret));
          } else if (0 == affected_rows) {
            LOG_INFO("finished to delete from event history table", K(sql), K(create_time));
            break;
          } else if (cnt > MAX_DELETE_TIMES) {
            LOG_INFO("delete cnt reach limit, schedule next round", K(sql), K(create_time));
            if (OB_INVALID_TIMESTAMP == create_time) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("create time is invalid", KR(ret), K(create_time), K(sql));
            } else if (ObTimeUtility::current_time() - create_time > EVENT_TABLE_CLEAR_INTERVAL) {
              //has new clear task, no need add task again
              LOG_INFO("maybe has new clear task, no need add task again", K(create_time));
            } else {
              ObSqlString new_sql;
              const bool is_delete = true;
              if (OB_TMP_FAIL(new_sql.assign(sql))) {
                LOG_WARN("failed to assign sql", KR(tmp_ret), K(sql));
              } else if (OB_TMP_FAIL(add_task(new_sql, is_delete, create_time))) {
                LOG_WARN("failed to add task", KR(tmp_ret), K(new_sql), K(create_time));
              } else {
                LOG_INFO("has event need delete, add task again", K(new_sql), K(create_time));
              }
            }
            break;
          } else {
            LOG_INFO("delete rows from event history table", K(affected_rows), K(sql), K(cnt));
            cnt++;
          }
        }
      }
    }
  }
  return ret;
}

int ObEventHistoryTableOperator::add_event_to_timer_(const common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  int retry_times = 0;
  ObAddr self_addr = self_addr_;
  common::ObMySQLProxy *proxy = proxy_;
  ObUniqueGuard<ObStringHolder> uniq_holder;
  if (OB_FAIL(ob_make_unique(uniq_holder, SET_USE_500("EventReHolder")))) {
    SHARE_LOG(WARN, "fail to make unique guard");
  } else if (OB_FAIL(uniq_holder->assign(sql.string()))) {
    SHARE_LOG(WARN, "fail to create unique ownership of string");
  } else if (OB_FAIL(timer_.schedule_task_ignore_handle_repeat_and_immediately(15_s, [retry_times, self_addr, uniq_holder, proxy]() mutable -> bool {
    int ret = OB_SUCCESS;
    bool stop_flag = false;
    char ip[64] = {0};
    int64_t affected_rows = 0;
    const char *sql = to_cstring(uniq_holder->get_ob_string());
    if (OB_ISNULL(proxy)) {
      SHARE_LOG(WARN, "proxy_ is NULL", KP(proxy));
    } else if (!self_addr.ip_to_string(ip, sizeof(ip))) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "ip to string failed");
    } else if (OB_FAIL(proxy->write(sql, affected_rows))) {
      SHARE_LOG(WARN, "sync execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "affected_rows expected to be one", K(affected_rows), K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      SHARE_LOG(INFO, "event table sync add event success", K(ret), K(sql));
    }
    if (++retry_times > MAX_RETRY_COUNT || OB_SUCC(ret)) {
      // this may happened cause inner sql may not work for a long time,
      // but i have tried my very best, so let it miss.
      if (retry_times > MAX_RETRY_COUNT) {
        SHARE_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "fail to schedule report event task cause retry too much times");
      }
      stop_flag = true;
    }
    return stop_flag;
  }))) {
    SHARE_LOG(ERROR, "fail to schedule report event task");
  }
  return ret;
}
}//end namespace rootserver
}//end namespace oceanbase
