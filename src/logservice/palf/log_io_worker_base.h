/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOGSERVICE_LOG_IO_WORKER_BASE_
#define OCEANBASE_LOGSERVICE_LOG_IO_WORKER_BASE_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_thread_pool.h"

namespace oceanbase
{
namespace palf
{
class LogIOTask;

// Common submit and lifecycle contract for legacy and async PALF IO workers.
class LogIOWorkerBase : public share::ObThreadPool
{
public:
  LogIOWorkerBase() {}
  virtual ~LogIOWorkerBase() {}

  // Start the worker thread, request it to stop, and join it. Concrete workers
  // define whether stop drains queued work before wait returns.
  virtual int start() { return share::ObThreadPool::start(); }
  virtual void stop() { share::ObThreadPool::stop(); }
  virtual void wait() { share::ObThreadPool::wait(); }
  virtual void destroy() = 0;
  // Success only means the worker owns the task. It does not mean the task has
  // been executed or persisted.
  virtual int submit_io_task(LogIOTask *io_task) = 0;
  // Return the oldest pending IO start time, or OB_INVALID_TIMESTAMP if idle.
  virtual int64_t get_oldest_pending_io_start_ts() const = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(LogIOWorkerBase);
};

} // end namespace palf
} // end namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_IO_WORKER_BASE_
