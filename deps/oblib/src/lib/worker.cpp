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

#define USING_LOG_PREFIX LIB
#include "worker.h"
#include <stdlib.h>
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/allocator/ob_malloc.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;

OB_SERIALIZE_MEMBER(ObRuntimeContext, compat_mode_);

namespace oceanbase {
namespace lib {

void* __attribute__((weak)) alloc_worker()
{
  static RLOCAL(ByteBuf<sizeof(Worker)>, wbuf);
  return new (&wbuf[0]) Worker();
}

}  // namespace lib
}  // namespace oceanbase

// static variables
RLOCAL(Worker*, Worker::self_);

Worker::Worker() : allocator_(nullptr), req_flag_(false), worker_level_(INT32_MAX), curr_request_level_(0), group_id_(0)
{}

bool Worker::sched_wait()
{
  return true;
}

bool Worker::sched_run(int64_t waittime)
{
  UNUSED(waittime);
  check_status();
  return true;
}
