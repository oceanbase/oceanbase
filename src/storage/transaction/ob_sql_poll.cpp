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

#include "ob_sql_poll.h"

namespace oceanbase {

namespace transaction {

int ObSqlTask::init(const ObSqlString& sql)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObSqlTask inited twice");
    ret = OB_INIT_TWICE;
  } else if (!sql.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(sql));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(sql_.assign(sql.string()))) {
      TRANS_LOG(WARN, "sql assign fail", KR(ret));
    } else {
      need_check_affected_row_ = false;
      is_inited_ = true;
      TRANS_LOG(DEBUG, "ObSqlTask inited success");
    }
  }

  return ret;
}

int ObSqlTask::init(const ObSqlString& sql, const int64_t expect_affected_row, const int64_t retry_time)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObSqlTask inited twice");
    ret = OB_INIT_TWICE;
  } else if (!sql.is_valid() || expect_affected_row <= 0 || retry_time <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(sql), K(expect_affected_row), K(retry_time));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(sql_.assign(sql.string()))) {
      TRANS_LOG(WARN, "sql assign fail", KR(ret));
    } else {
      max_retry_time_ = retry_time;
      expect_affected_row_ = expect_affected_row;
      need_check_affected_row_ = true;
      is_inited_ = true;
      TRANS_LOG(DEBUG, "ObSqlTask inited success");
    }
  }

  return ret;
}

bool ObSqlTask::is_valid() const
{
  return sql_.is_valid();
}

void ObSqlTask::reset()
{
  is_inited_ = false;
  sql_.reset();
  max_retry_time_ = 0;
  expect_affected_row_ = 0;
  need_check_affected_row_ = false;
}

void ObSqlTask::destroy()
{
  if (is_inited_) {
    sql_.reset();
    is_inited_ = false;
  }
}

ObSqlPool::ObSqlPool() : is_inited_(false), is_running_(false)
{}

int ObSqlPool::init(const int64_t n_threads, const common::ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObSqlPoll inited twice");
    ret = OB_INIT_TWICE;
  } else if (n_threads <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(n_threads));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObSimpleThreadPool::init(n_threads, TOTAL_TASK, "SqlPoll"))) {
    TRANS_LOG(WARN, "thread pool init error", K(n_threads), KR(ret));
  } else {
    sql_proxy_.assign(sql_proxy);
    is_inited_ = true;
    TRANS_LOG(INFO, "ObSqlPoll inited success");
  }

  return ret;
}

int ObSqlPool::start()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObSqlPoll is not inited");
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObSqlPoll is already running");
    ret = OB_ERR_UNEXPECTED;
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "ObSqlPoll start success");
  }

  return ret;
}

int ObSqlPool::stop()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObSqlPoll is not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    TRANS_LOG(WARN, "ObSqlPoll already has been stopped");
    ret = OB_NOT_RUNNING;
  } else {
    ObSimpleThreadPool::destroy();
    is_running_ = false;
    TRANS_LOG(INFO, "ObSqlPoll stop success");
  }

  return ret;
}

int ObSqlPool::wait()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObSqlPoll is not inited");
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObSqlPoll is running");
    ret = OB_ERR_UNEXPECTED;
  } else {
    TRANS_LOG(INFO, "ObSqlPoll wait success");
  }

  return ret;
}

void ObSqlPool::destroy()
{
  int tmp_ret = OB_SUCCESS;

  if (is_inited_) {
    if (is_running_) {
      if (OB_SUCCESS != (tmp_ret = stop())) {
        TRANS_LOG(WARN, "ObSqlPoll stop error", "ret", tmp_ret);
      } else if (OB_SUCCESS != (tmp_ret = wait())) {
        TRANS_LOG(WARN, "ObSqlPoll wait error", "ret", tmp_ret);
      } else {
        // do nothing
      }
    }
    is_inited_ = false;
    TRANS_LOG(INFO, "ObSqlPoll destroyed");
  }
}

int ObSqlPool::add_task(void* task)
{
  int ret = OB_SUCCESS;
  ObSqlTask* sql_task = NULL;

  if (NULL == task) {
    TRANS_LOG(ERROR, "task is null", KP(task));
    ret = OB_ERR_NULL_VALUE;
  } else if (!is_inited_) {
    TRANS_LOG(WARN, "ObSqlPoll is not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    TRANS_LOG(WARN, "ObSqlPoll is not running");
    ret = OB_NOT_RUNNING;
  } else {
    sql_task = reinterpret_cast<ObSqlTask*>(task);
    if (!sql_task->is_valid()) {
      TRANS_LOG(WARN, "invalid argument", K(task));
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(push(task))) {
      TRANS_LOG(WARN, "push sql task error", KR(ret));
    } else {
      // do nothing
    }
  }

  return ret;
}

void ObSqlPool::handle(void* task)
{
  // int ret = OB_SUCCESS;
  ObSqlTask* sql_task = NULL;

  if (task == NULL) {
    TRANS_LOG(ERROR, "task is null", KP(task));
  } else {
    sql_task = reinterpret_cast<ObSqlTask*>(task);
    // To be determined
    // Limit the number of executions per second
  }

  (void)sql_task;  // make compiler happy
}

}  // namespace transaction
}  // namespace oceanbase
