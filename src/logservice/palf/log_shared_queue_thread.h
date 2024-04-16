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

#ifndef OCEANBASE_LOGSERVICE_LOG_SHARED_QUEUE_THREAD_
#define OCEANBASE_LOGSERVICE_LOG_SHARED_QUEUE_THREAD_

#include "lib/thread/thread_mgr_interface.h"
#include "lib/utility/ob_print_utils.h"
#include "palf_callback.h"

namespace oceanbase
{
namespace palf
{
class IPalfEnvImpl;
class LogSharedTask;

class LogSharedQueueTh : public lib::TGTaskHandler
{
public:
  LogSharedQueueTh();
  ~LogSharedQueueTh();
public:
  int init(IPalfEnvImpl *palf_env_impl);
  int start();
  int stop();
  int wait();
  void destroy();
  int push_task(LogSharedTask *task);
  virtual void handle(void *task);
  int get_tg_id() const;
public:
  static constexpr int64_t THREAD_NUM = 1;
  static constexpr int64_t MINI_MODE_THREAD_NUM = 1;
  static constexpr int64_t MAX_LOG_HANDLE_TASK_NUM = 10 * OB_MAX_LS_NUM_PER_TENANT_PER_SERVER;
private:
  DISALLOW_COPY_AND_ASSIGN(LogSharedQueueTh);
private:
  int tg_id_;
  IPalfEnvImpl *palf_env_impl_;
  bool is_inited_;
};
} // end namespace palf
} // end namespace oceanbase

#endif
