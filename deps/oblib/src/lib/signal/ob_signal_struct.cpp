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

#define USING_LOG_PREFIX COMMON

#include "lib/signal/ob_signal_struct.h"
#include <new>
#include "lib/atomic/ob_atomic.h"

namespace oceanbase {
namespace common {
const int MP_SIG = SIGURG;
const int SIG_STACK_SIZE = 16L << 10;

__thread char tl_trace_id_buf[sizeof(DTraceId)];
__thread DTraceId* tl_trace_id = nullptr;

DTraceId DTraceId::gen_trace_id()
{
  static int64_t seq = 0;
  DTraceId id;
  id.v_ = ATOMIC_AAF(&seq, 1);
  return id;
}

DTraceId& get_tl_trace_id()
{
  if (OB_UNLIKELY(nullptr == tl_trace_id)) {
    tl_trace_id = new (tl_trace_id_buf) DTraceId();
  }
  return *tl_trace_id;
}

DTraceId set_tl_trace_id(DTraceId& id)
{
  DTraceId orig = get_tl_trace_id();
  get_tl_trace_id() = id;
  return orig;
}

}  // namespace common
}  // namespace oceanbase
