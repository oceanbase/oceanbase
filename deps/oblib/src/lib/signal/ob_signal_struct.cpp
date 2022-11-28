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
#include "lib/charset/ob_mysql_global.h"
#include "lib/coro/co_var.h"
#include "lib/utility/ob_defer.h"

namespace oceanbase
{
namespace common
{
const int MP_SIG = SIGURG;
const int SIG_STACK_SIZE = 16L<<10;
uint64_t g_rlimit_core = 0;

DTraceId DTraceId::gen_trace_id()
{
  static int64_t seq = 0;
  DTraceId id;
  id.v_ = ATOMIC_AAF(&seq, 1);
  return id;
}

DTraceId &get_tl_trace_id()
{
  RLOCAL(DTraceId, tl_trace_id);
  return tl_trace_id;
}

DTraceId set_tl_trace_id(DTraceId &id)
{
  DTraceId orig = get_tl_trace_id();
  get_tl_trace_id() = id;
  return orig;
}

void ObProcMaps::load_maps()
{
  char fn[64];
  snprintf(fn, sizeof(fn), "/proc/%d/maps", getpid());
  FILE *file = fopen(fn, "rt");
  if (!file) {
    return;
  }
  DEFER(fclose(file));

  char line[1024];
  while (fgets(line, sizeof line, file) != NULL) {
    int len = strlen(line);
    if (len > 0 && line[len-1] == '\n') {
      line[--len] = '\0';
    }
    // line would be modified by strtok_r, copy it ahead of time
    int64_t n = snprintf(buf_ + pos_, sizeof(buf_) - pos_, "%s\n", line);
    char *tokens[8];
    int token_cnt = 0;
    char *saveptr = nullptr;
    for (char *value = strtok_r(line, " ", &saveptr);
         NULL != value;
         value = strtok_r(NULL, " ", &saveptr)) {
      tokens[token_cnt++] = value;
    }
    if (token_cnt < 6) {
      continue;
    }
    char *perms = tokens[1];
    if (strlen(perms) == 4 && perms[2] != 'x') {
      continue;
    }
    if (n < 0 || n >= (sizeof(buf_) - pos_)) break;
    pos_ += n;
  }
}

void ObSigHandlerCtx::atomic_set_req_id(int64_t req_id)
{
  lock();
  DEFER(unlock());
  req_id_ = req_id;
}

void ObSigHandlerCtx::lock()
{
  while (ATOMIC_TAS(&lock_, 1)) {
    PAUSE();
  }
}

void ObSigHandlerCtx::unlock()
{
  ATOMIC_STORE(&lock_, 0);
}

ObSigHandlerCtx g_sig_handler_ctx_;

} // namespace common
} // namespace oceanbase
