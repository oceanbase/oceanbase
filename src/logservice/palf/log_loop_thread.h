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

#ifndef OCEANBASE_LOGSERVICE_LOG_LOOP_THREAD_
#define OCEANBASE_LOGSERVICE_LOG_LOOP_THREAD_

#include "share/ob_thread_pool.h"
#include "log_define.h"

namespace oceanbase
{
namespace palf
{
class IPalfEnvImpl;
class LogLoopThread : public share::ObThreadPool
{
public:
  LogLoopThread();
  virtual ~LogLoopThread();
public:
  int init(IPalfEnvImpl *palf_env_impl);
  void destroy();
  void run1();
private:
  void log_loop_();
private:
  IPalfEnvImpl *palf_env_impl_;
  int64_t run_interval_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(LogLoopThread);
};

} // namespace palf
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_LOOP_THREAD_
