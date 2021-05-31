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

#include "share/ob_worker.h"

#include <stdlib.h>
#include "lib/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "lib/allocator/ob_malloc.h"
#include "share/scheduler/ob_dag_scheduler.h"

using namespace oceanbase::common;

using namespace oceanbase::lib;

namespace oceanbase {
namespace lib {

// Replacing function in library
void* alloc_worker()
{
  static RLOCAL(ByteBuf<sizeof(share::ObWorker)>, wbuf);
  return new (&wbuf[0]) share::ObWorker();
}
}  // namespace lib
}  // namespace oceanbase

namespace oceanbase {
namespace share {

ObWorker::ObWorker()
    : run_status_(RS_PAUSED),
      session_(nullptr),
      timeout_ts_(INT64_MAX),
      rpc_tenant_id_(0),
      tidx_(-1),
      large_token_expired_(0),
      disable_wait_(false)
{
  worker_node_.get_data() = this;
  lq_worker_node_.get_data() = this;
  lq_waiting_worker_node_.get_data() = this;
  if (OB_ISNULL(self_)) {
    self_ = this;
  } else {
    // Ideally, there won't be worker creating when a routine, or
    // thread, has a worker, i.e. self_ isn't null. Whereas ObThWorker
    // which derived from ObWorker doesn't create instance on the same
    // routine as it is. So we can't assert self_ is null when new
    // ObWorker is initializing right now.
  }
}

ObWorker::~ObWorker()
{
  if (self_ == this) {
    // We only remove this worker not other worker since the reason
    // described in SET stage.
    self_ = nullptr;
  }
}

ObWorker::Status ObWorker::check_wait()
{
  share::dag_yield();
  return WS_NOWAIT;
}

void ObWorker::set_timeout_ts(int64_t timeout_ts)
{
  timeout_ts_ = timeout_ts;
}

int64_t ObWorker::get_timeout_ts() const
{
  return timeout_ts_;
}

int64_t ObWorker::get_timeout_remain() const
{
  return timeout_ts_ - ObTimeUtility::current_time();
}

bool ObWorker::is_timeout() const
{
  return ObTimeUtility::current_time() >= timeout_ts_;
}

void ObWorker::set_rpc_tenant(uint64_t tenant_id)
{
  rpc_tenant_id_ = tenant_id;
}

void ObWorker::reset_rpc_tenant()
{
  rpc_tenant_id_ = 0;
}

uint64_t ObWorker::get_rpc_tenant() const
{
  return rpc_tenant_id_;
}

}  // namespace share
}  // namespace oceanbase
