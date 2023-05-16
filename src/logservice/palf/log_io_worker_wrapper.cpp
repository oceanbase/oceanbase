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

#define USING_LOG_PREFIX PALF
#include "log_io_worker_wrapper.h"
#include "lib/ob_define.h"
#include "log_define.h"

namespace oceanbase
{
namespace palf
{

LogIOWorkerWrapper::LogIOWorkerWrapper()
    : is_inited_(false),
    is_user_tenant_(false),
    sys_log_io_worker_(),
    user_log_io_worker_() {}

LogIOWorkerWrapper::~LogIOWorkerWrapper()
{
  destroy();
}

void LogIOWorkerWrapper::destroy()
{
  is_inited_ = false;
  is_user_tenant_ = false;
  sys_log_io_worker_.destroy();
  user_log_io_worker_.destroy();
}
int LogIOWorkerWrapper::init(const LogIOWorkerConfig &config,
                             const int64_t tenant_id,
                             int cb_thread_pool_tg_id,
                             ObIAllocator *allocaotor,
                             IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id));
  } else if(FALSE_IT(is_user_tenant_ = is_user_tenant(tenant_id))) {
  } else if (OB_FAIL(sys_log_io_worker_.init(config,
                                             tenant_id,
                                             cb_thread_pool_tg_id,
                                             allocaotor,
                                             palf_env_impl))) {
    LOG_WARN("failed to init sys_log_io_worker", K(ret));
  } else if (is_user_tenant_ && OB_FAIL(user_log_io_worker_.init(config,
                                                                 tenant_id,
                                                                 cb_thread_pool_tg_id,
                                                                 allocaotor, palf_env_impl))) {
    sys_log_io_worker_.destroy();
    LOG_WARN("failed to init user_log_io_worker");
  } else {
    is_inited_ = true;
    LOG_INFO("success to init LogIOWorkerWrapper", K(tenant_id), KPC(this));
  }
  return ret;
}
LogIOWorker *LogIOWorkerWrapper::get_log_io_worker(const int64_t palf_id)
{
  return is_sys_palf_id(palf_id) ? &sys_log_io_worker_ : &user_log_io_worker_;
}

int LogIOWorkerWrapper::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sys_log_io_worker_.start())) {
    LOG_WARN("failed to start sys_log_io_worker");
  } else if (is_user_tenant_ && OB_FAIL(user_log_io_worker_.start())) {
    LOG_WARN("failed to start user_log_io_worker");
  } else {
    LOG_INFO("success to start LogIOWorkerWrapper", KPC(this));
  }
  return ret;
}

void LogIOWorkerWrapper::stop()
{
  LOG_INFO("LogIOWorkerWrapper starts stopping", KPC(this));
  sys_log_io_worker_.stop();
  if (is_user_tenant_) {
    user_log_io_worker_.stop();
  }
  LOG_INFO("LogIOWorkerWrapper has finished stopping", KPC(this));
}

void LogIOWorkerWrapper::wait()
{
  LOG_INFO("LogIOWorkerWrapper starts waiting", KPC(this));
  sys_log_io_worker_.wait();
  if (is_user_tenant_) {
    user_log_io_worker_.wait();
  }
  LOG_INFO("LogIOWorkerWrapper has finished waiting", KPC(this));
}

int LogIOWorkerWrapper::notify_need_writing_throttling(const bool &need_throttling)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_user_tenant_) {
    //need no nothing
  } else if (OB_FAIL(user_log_io_worker_.notify_need_writing_throttling(need_throttling))) {
    LOG_WARN("failed to notify_need_writing_throttling", K(need_throttling));
  } else {
    if (need_throttling) {
      LOG_INFO("success to notify_need_writing_throttling True");
    }
  }
  return ret;
}

int64_t LogIOWorkerWrapper::get_last_working_time() const
{
  const int64_t sys_last_working_time = sys_log_io_worker_.get_last_working_time();
  const int64_t user_last_working_time = user_log_io_worker_.get_last_working_time();
  return MAX(sys_last_working_time, user_last_working_time);
}

}//end of namespace palf
}//end of namespace oceanbase
