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

#include "log_io_task_cb_thread_pool.h"
#include "share/ob_errno.h"                   // errno...
#include "share/ob_thread_define.h"           // TGDefIDs
#include "share/ob_thread_mgr.h"              // TG_START
#include "logservice/palf/log_io_task.h"      // LogIOTask
#include "palf_env_impl.h"                    // PalfEnvImpl

namespace oceanbase
{
namespace palf
{
LogIOTaskCbThreadPool::LogIOTaskCbThreadPool()
    : tg_id_(-1),
      palf_env_impl_(NULL),
      is_inited_(false)
{
}

LogIOTaskCbThreadPool::~LogIOTaskCbThreadPool()
{
  destroy();
}

int LogIOTaskCbThreadPool::init(const int64_t log_io_cb_num,
                                IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  const int tg_id = lib::TGDefIDs::LogIOTaskCbThreadPool;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "LogIOTaskCbThreadPool has inited!!!", K(ret));
  } else if (NULL == palf_env_impl) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), KPC(palf_env_impl));
  } else if (OB_FAIL(TG_CREATE_TENANT(tg_id, tg_id_, log_io_cb_num))) {
    PALF_LOG(WARN, "LogIOTaskCbThreadPool TG_CREATE failed", K(ret));
  } else {
    palf_env_impl_ = palf_env_impl;
    is_inited_ = true;
    PALF_LOG(INFO, "LogIOTaskCbThreadPool init success", K(ret),
        K(tg_id_), KP(palf_env_impl_), KP(palf_env_impl), K(log_io_cb_num));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

int LogIOTaskCbThreadPool::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogIOTaskCbThreadPool not inited!!!", K(ret));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    PALF_LOG(ERROR, "start LogIOTaskCbThreadPool failed", K(ret));
  } else {
    PALF_LOG(INFO, "start LogIOTaskCbThreadPool success", K(ret),
        K(tg_id_));
  }
  return ret;
}

int LogIOTaskCbThreadPool::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOTaskCbThreadPool not inited!!!", K(ret));
  } else {
    TG_STOP(tg_id_);
    PALF_LOG(INFO, "stop LogIOTaskCbThreadPool success", K(tg_id_));
  }
  return ret;
}

int LogIOTaskCbThreadPool::wait()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOTaskCbThreadPool not inited!!!", K(ret));
  } else {
    TG_WAIT(tg_id_);
    PALF_LOG(INFO, "wait LogIOTaskCbThreadPool success", K(tg_id_));
  }
  return ret;
}

void LogIOTaskCbThreadPool::destroy()
{
  stop();
  wait();
  is_inited_ = false;
  if (-1 != tg_id_) {
    TG_DESTROY(tg_id_);
  }
  tg_id_ = -1;
  PALF_LOG(INFO, "destroy LogIOTaskCbThreadPool success", K(tg_id_));
}

void LogIOTaskCbThreadPool::handle(void *task)
{
  int ret = OB_SUCCESS;
  LogIOTask *log_io_task = reinterpret_cast<LogIOTask*>(task);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogIOTaskCbThreadPool not inited!!!", K(ret));
  } else if (OB_ISNULL(log_io_task)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(log_io_task));
  } else if (OB_FAIL(log_io_task->after_consume(palf_env_impl_))) {
    PALF_LOG(WARN, "LogIOTask after_consume failed", K(ret), KP(log_io_task));
  } else {
    PALF_LOG(TRACE, "LogIOTaskCbThreadPool handle success");
  }
  if (OB_NOT_NULL(log_io_task)) {
    log_io_task->free_this(palf_env_impl_);
  }
}

int LogIOTaskCbThreadPool::get_tg_id() const
{
  return tg_id_;
}
} // end namespace palf
} // end namespace oceanbase
