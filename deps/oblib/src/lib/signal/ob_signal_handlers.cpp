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

#include "lib/signal/ob_signal_handlers.h"
#include <sys/prctl.h>
#include "lib/profile/ob_trace_id.h"
#include "lib/utility/utility.h"
#include "lib/signal/ob_signal_struct.h"
#include "lib/signal/ob_signal_utils.h"
#include "lib/signal/ob_memory_cutter.h"
#include "common/ob_common_utility.h"

namespace oceanbase {
namespace common {
static const int SIG_SET[] = {SIGABRT, SIGBUS, SIGFPE, SIGSEGV, SIGURG};

static inline void handler(int sig, siginfo_t* s, void* p)
{
  if (tl_handler != nullptr) {
    tl_handler(sig, s, p);
  }
}

int install_ob_signal_handler()
{
  int ret = OB_SUCCESS;
  struct sigaction sa;
  sa.sa_flags = SA_SIGINFO | SA_RESTART | SA_NODEFER | SA_ONSTACK;
  sa.sa_sigaction = handler;
  sigemptyset(&sa.sa_mask);
  for (int i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(SIG_SET); i++) {
    if (-1 == sigaction(SIG_SET[i], &sa, nullptr)) {
      ret = OB_INIT_FAIL;
    }
  }
  return ret;
}

__thread signal_handler_t tl_handler = ob_signal_handler;

#define COMMON_FMT "timestamp=%ld, tid=%ld, tname=%s, trace_id=%lu-%lu, extra_info=(%s), lbt=%s"

void coredump_cb(int, siginfo_t*);
void ob_signal_handler(int sig, siginfo_t* si, void*)
{
  if (MP_SIG == sig) {
    ObISigHandler* handler = (ObISigHandler*)si->si_value.sival_ptr;
    handler->handle();
  } else {
    coredump_cb(sig, si);
  }
}

void coredump_cb(int sig, siginfo_t* si)
{
  send_request_and_wait(VERB_LEVEL_2, syscall(SYS_gettid) /*exclude_id*/);
#define MINICORE 1
#if MINICORE
  int pid = 0;
  if ((pid = fork()) != 0) {
#endif
    // parent or fork failed
    timespec time = {0, 0};
    clock_gettime(CLOCK_REALTIME, &time);
    int64_t ts = time.tv_sec * 1000000 + time.tv_nsec / 1000;
    // thread_name
    char tname[16];
    prctl(PR_GET_NAME, tname);
    // backtrace
    char bt[256];
    int64_t len = 0;
    safe_backtrace(bt, sizeof(bt) - 1, len);
    bt[len++] = '\0';
    // trace_id
    const uint64_t* trace_id = ObCurTraceId::get();
    uint64_t trace_id_0 = OB_ISNULL(trace_id) ? OB_INVALID_ID : trace_id[0];
    uint64_t trace_id_1 = OB_ISNULL(trace_id) ? OB_INVALID_ID : trace_id[1];
    // extra
    const ObFatalErrExtraInfoGuard* extra_info =
        nullptr;  // TODO: May deadlock, ObFatalErrExtraInfoGuard::get_thd_local_val_ptr();
    char print_buf[512];
    ssize_t print_len = safe_snprintf(print_buf,
        sizeof(print_buf),
        "CRASH ERROR!!! sig=%d, sig_code=%d, sig_addr=%p, " COMMON_FMT "\n",
        sig,
        si->si_code,
        si->si_addr,
        ts,
        GETTID(),
        tname,
        trace_id_0,
        trace_id_1,
        (NULL == extra_info) ? NULL : to_cstring(*extra_info),
        bt);
    write(STDERR_FILENO, print_buf, print_len);
#if MINICORE
  } else {
    // child
    int64_t total_size = 0;
    if (lib::g_mem_cutter != nullptr) {
      lib::g_mem_cutter->cut(total_size);
    }
    DLOG(INFO, "[MINICORE], TOTAL FREED: %ld", total_size);
  }
#endif
  // Reset back to the default handler
  signal(sig, SIG_DFL);
  raise(sig);
}

}  // namespace common
}  // namespace oceanbase
