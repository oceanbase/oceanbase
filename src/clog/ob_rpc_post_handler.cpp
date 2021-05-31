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

#include "ob_rpc_post_handler.h"
#include "ob_log_rpc.h"

namespace oceanbase {
using namespace common;
namespace clog {
struct ObRpcPostHandler::Task : public QLink {
  Task(ObIBatchBufferTask* callback, ObAddr server, const int pcode, const char* buf, const int64_t len)
      : callback_(callback), server_(server), pcode_(pcode), buf_(buf), len_(len)
  {}
  ~Task()
  {}
  TO_STRING_KV(K_(callback), K_(server), K_(pcode), KP_(buf), K_(len));
  ObIBatchBufferTask* callback_;
  ObAddr server_;
  int pcode_;
  const char* buf_;
  int64_t len_;
};

void ObRpcPostHandler::stop()
{
  ObThreadPool::stop();
}

void ObRpcPostHandler::wait()
{
  ObThreadPool::wait();
}

void ObRpcPostHandler::destroy()
{
  stop();
  wait();
}

int ObRpcPostHandler::init(ObILogRpc* rpc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rpc)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    rpc_ = rpc;
    if (OB_FAIL(start())) {
      CLOG_LOG(WARN, "ObRpcPostHandler start thread failed", K(ret));
    }
  }
  return ret;
}

int ObRpcPostHandler::post(const common::ObAddr& server, const int pcode, const char* data, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (!server.is_valid() || pcode < 0 || OB_ISNULL(data) || len < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == rpc_) {
    ret = OB_NOT_INIT;
  } else {
    ret = rpc_->post(server, pcode, data, len);
  }
  return ret;
}

int ObRpcPostHandler::submit(
    ObIBatchBufferTask* task, const ObAddr& server, const int pcode, const char* buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  Task* post_task = NULL;
  if (OB_ISNULL(task) || !server.is_valid() || pcode < 0 || OB_ISNULL(buf) || len < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (post_task = (Task*)task->alloc(sizeof(*post_task)))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    new (post_task) Task(task, server, pcode, buf, len);
    ret = queue_.push(post_task);
    cond_.signal();
  }
  return ret;
}

int ObRpcPostHandler::handle_task()
{
  int ret = OB_SUCCESS;
  int pop_ret = OB_SUCCESS;
  int post_ret = OB_SUCCESS;
  Task* task = NULL;
  int64_t timeout_us = WAIT_TASK_TIMEOUT;
  uint32_t seq = cond_.get_seq();
  if (NULL == rpc_) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != (pop_ret = queue_.pop((QLink*&)task))) {
    cond_.wait(seq, timeout_us);
  } else if (OB_SUCCESS != (post_ret = rpc_->post(task->server_, task->pcode_, task->buf_, task->len_))) {
    CLOG_LOG(WARN, "post packet fail:", K(post_ret), K(task));
  }
  if (OB_SUCCESS == pop_ret && NULL != task && NULL != task->callback_) {
    task->callback_->after_consume();
  }
  return ret;
}

void ObRpcPostHandler::run1()
{
  int ret = OB_SUCCESS;
  while (!has_set_stop() && OB_SUCCESS == ret) {
    if (OB_FAIL(handle_task())) {
      CLOG_LOG(ERROR, "handle_task fail", K(ret));
    }
  }
}

};  // end namespace clog
};  // end namespace oceanbase
