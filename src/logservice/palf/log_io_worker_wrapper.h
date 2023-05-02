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

#ifndef OCEANBASE_LOGSERVIVE_LOG_IO_WORKER_WRAPPER_
#define OCEANBASE_LOGSERVIVE_LOG_IO_WORKER_WRAPPER_

#include "log_io_worker.h"

namespace oceanbase
{
namespace palf
{
class LogIOWorkerWrapper
{
public:
LogIOWorkerWrapper();
~LogIOWorkerWrapper();

int init(const LogIOWorkerConfig &config,
         const int64_t tenant_id,
         int cb_thread_pool_tg_id,
         ObIAllocator *allocaotr,
         IPalfEnvImpl *palf_env_impl);
void destroy();
int start();
void stop();
void wait();
LogIOWorker *get_log_io_worker(const int64_t palf_id);
int notify_need_writing_throttling(const bool &need_throtting);
int64_t get_last_working_time() const;
TO_STRING_KV(K_(is_inited), K_(is_user_tenant), K_(sys_log_io_worker), K_(user_log_io_worker));

private:
bool is_inited_;
bool is_user_tenant_;
//for log stream  NO.1
LogIOWorker sys_log_io_worker_;
//for log streams except NO.1
LogIOWorker user_log_io_worker_;
};

}//end of namespace palf
}//end of namespace oceanbase
#endif
