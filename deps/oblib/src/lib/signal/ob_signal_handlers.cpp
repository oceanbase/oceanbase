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
#define _GNU_SOURCE 1
#include "lib/signal/ob_signal_handlers.h"
#include <sys/prctl.h>
#include <dirent.h>
#include <unistd.h>
#include <fstream>
#include <sys/wait.h>
#include "lib/profile/ob_trace_id.h"
#include "lib/utility/utility.h"
#include "lib/signal/ob_libunwind.h"
#include "lib/signal/ob_signal_struct.h"
#include "lib/signal/ob_signal_utils.h"
#include "lib/signal/ob_signal_worker.h"
#include "lib/utility/ob_hang_fatal_error.h"
#include "common/ob_common_utility.h"

namespace oceanbase
{
namespace common
{
static const int SIG_SET[] = {SIGABRT, SIGBUS, SIGFPE, SIGSEGV, SIGURG};
static constexpr char MINICORE_SHELL_PATH[] = "tools/minicore.sh";
static constexpr char FASTSTACK_SHELL_PATH[] = "tools/callstack.sh";
static constexpr char MINICORE_SCRIPT[] = "if [ -e bin/minicore.py ]; then\n"
"  python bin/minicore.py `cat $(pwd)/run/observer.pid` -c -o core.`cat $(pwd)/run/observer.pid`.mini\n"
"fi\n"
"[ $(ls -1 core.*.mini 2>/dev/null | wc -l) -gt 5 ] && ls -1 core.*.mini -t | tail -n 1 | xargs rm -f";

static constexpr char FASTSTACK_SCRIPT[] = "if [ -x \"$(command -v obstack)\" ]; then\n"
"  obstack `cat $(pwd)/run/observer.pid` > stack.`cat $(pwd)/run/observer.pid`.`date +%Y%m%d%H%M%S`\n"
"fi\n"
"[ $(ls -1 stack.* 2>/dev/null | wc -l) -gt 100 ] && ls -1 stack.* -t | tail -n 1 | xargs rm -f";

static inline void handler(int sig, siginfo_t *s, void *p)
{
  if (get_signal_handler() != nullptr) {
    get_signal_handler()(sig, s, p);
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

signal_handler_t &get_signal_handler()
{
  struct Wrapper {
    Wrapper() : v_(ob_signal_handler) {}
    signal_handler_t v_;
  };
  RLOCAL(Wrapper, tl_handler);
  return (&tl_handler)->v_;
}
bool g_redirect_handler = false;
static __thread int g_coredump_num = 0;

#define COMMON_FMT "timestamp=%ld, tid=%ld, tname=%s, trace_id=%lu-%lu-%lu-%lu, extra_info=(%s), lbt=%s"

void coredump_cb(int, siginfo_t*, void*);
void ob_signal_handler(int sig, siginfo_t *si, void *context)
{
  if (!g_redirect_handler) {
    signal(sig, SIG_DFL);
    raise(sig);
  } else {
    if (MP_SIG == sig) {
      auto &ctx = g_sig_handler_ctx_;
      ctx.lock();
      DEFER(ctx.unlock());
      int64_t req_id = (int64_t)si->si_value.sival_ptr;
      if (ctx.req_id_ != req_id) return;
      ctx.handler_->handle(ctx);
    } else {
      coredump_cb(sig, si, context);
    }
  }
}

void hook_sigsegv_msg(int sig, siginfo_t *si, void *context)
{
  if (mprotect_page(si->si_addr, 8, PROT_READ | PROT_WRITE, "release signal addr") != 0) {
    coredump_cb(sig, si, context);
  } else {
    // thread_name
    char tname[16];
    prctl(PR_GET_NAME, tname);
    _OB_LOG_RET(ERROR, OB_ERROR, "CRASH ERROR!!! sig=%d, sig_code=%d, sig_addr=%p, tid=%ld, tname=%s",
            sig, si->si_code, si->si_addr, GETTID(), tname);
  }
}

void close_socket_fd()
{
  char path[32];
  char name[32];
  char real_name[32];
  DIR *dir = nullptr;
  struct dirent *fd_file = nullptr;
  int fd = -1;
  int pid = getpid();

  safe_snprintf(path, 32, "/proc/%d/fd/", pid);
  if (NULL == (dir = opendir(path))) {
  } else {
    while(NULL != (fd_file = readdir(dir))) {
      if (0 != strcmp(fd_file->d_name, ".") && 0 != strcmp(fd_file->d_name, "..")
        && 0 != strcmp(fd_file->d_name, "0") && 0 != strcmp(fd_file->d_name, "1")
        && 0 != strcmp(fd_file->d_name, "2")) {
        safe_snprintf(name, 32, "/proc/%d/fd/%s", pid, fd_file->d_name);
        if (-1 == readlink(name, real_name, 32)) {
          DLOG(INFO, "[CLOSEFD], err read link %s, errno = %d", name, errno);
        } else {
          safe_snprintf(name, 32, "%s", real_name);
          if (NULL != strstr(name, "socket")) {
            fd = atoi(fd_file->d_name);
            close(fd);
          }
        }
      }
    }
    DLOG(INFO, "[CLOSEFD], close socket fd finish");
  }
  if (NULL != dir) {
    closedir(dir);
  }
}


void coredump_cb(int sig, siginfo_t *si, void *context)
{
  int ret = OB_SUCCESS;
  if (g_coredump_num++ < 1) {
    pid_t pid;
    close_socket_fd();
    ret = minicoredump(sig, GETTID(), pid);
    //send_request_and_wait(VERB_LEVEL_2,
    //                      syscall(SYS_gettid)/*exclude_id*/);
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
#ifdef __x86_64__
    safe_backtrace(bt, sizeof(bt) - 1, &len);
#endif
    bt[len++] = '\0';
    // extra
    const ObFatalErrExtraInfoGuard *extra_info = nullptr; // TODO: May deadlock, ObFatalErrExtraInfoGuard::get_thd_local_val_ptr();
    uint64_t uval[4] = {0};
    auto *trace_id = ObCurTraceId::get_trace_id();
    if (trace_id != nullptr) {
      trace_id->get_uval(uval);
    }
    char print_buf[512];
    const ucontext_t *con = (ucontext_t *)context;
#if defined(__x86_64__)
    int64_t ip = con->uc_mcontext.gregs[REG_RIP];
    int64_t bp = con->uc_mcontext.gregs[REG_RBP]; // stack base
#else
    // TODO: ARM
    int64_t ip = -1;
    int64_t bp = -1;
#endif
    char rlimit_core[32] = "unlimited";
    if (UINT64_MAX != g_rlimit_core) {
      safe_snprintf(rlimit_core, sizeof(rlimit_core), "%lu", g_rlimit_core);
    }
    char crash_info[128] = "CRASH ERROR!!!";
    int64_t fatal_error_thread_id = get_fatal_error_thread_id();
    if (-1 != fatal_error_thread_id) {
      safe_snprintf(crash_info, sizeof(crash_info),
                    "Right to Die or Duty to Live's Thread Existed before CRASH ERROR!!!"
                    "ThreadId=%ld,", fatal_error_thread_id);
    }
    ssize_t print_len = safe_snprintf(print_buf, sizeof(print_buf),
                                     "%s IP=%lx, RBP=%lx, sig=%d, sig_code=%d, sig_addr=%p, RLIMIT_CORE=%s, "COMMON_FMT", ",
                                     crash_info, ip, bp, sig, si->si_code, si->si_addr, rlimit_core,
                                     ts, GETTID(), tname, uval[0], uval[1], uval[2], uval[3],
                                     (NULL == extra_info) ? NULL : to_cstring(*extra_info), bt);
    const auto &si_guard = ObSqlInfoGuard::get_cur_guard();
    char sql[] = "SQL=";
    char end[] = "\n";
    struct iovec iov[4];
    memset(iov, 0, sizeof(iov));
    iov[0].iov_base = print_buf;
    iov[0].iov_len = print_len;
    iov[1].iov_base = sql;
    iov[1].iov_len = strlen(sql);
    iov[2].iov_base = NULL != si_guard ? si_guard->sql_.ptr() : NULL;
    iov[2].iov_len = NULL != si_guard ? si_guard->sql_.length() : 0;
    iov[3].iov_base = end;
    iov[3].iov_len = strlen(end);
    writev(STDERR_FILENO, iov, sizeof(iov) / sizeof(iov[0]));
    if (OB_SUCC(ret)) {
      int status = 0;
      waitpid(pid, &status, __WALL);
    }
  }
  // Reset back to the default handler
  signal(sig, SIG_DFL);
  raise(sig);
}

int minicoredump(int sig, int64_t tid, pid_t& pid)
{
  static constexpr int64_t MIN_INTERVAL = 5 * 60 * 1000 * 1000; // 5min
  static int64_t last_ts = 0;
  int64_t now = ObTimeUtility::fast_current_time();
  int64_t last = ATOMIC_LOAD(&last_ts);
  int ret = OB_SUCCESS;
  UNUSED(sig);
  UNUSED(tid);
  if (now - last < MIN_INTERVAL) {
    ret = OB_EAGAIN;
  } else if (!ATOMIC_BCAS(&last_ts, last, now)) {
    ret = OB_EAGAIN;
  } else if (-1 == access("bin/minicore.py", R_OK)) {
    ret = OB_FILE_NOT_EXIST;
  } else if (-1 == access(MINICORE_SHELL_PATH, R_OK)) {
    if (0 == (pid = syscall(__NR_clone, CLONE_VFORK, nullptr, nullptr, nullptr, nullptr))) {
      IGNORE_RETURN execlp("sh", "sh", "-c", MINICORE_SCRIPT, nullptr);
      _exit(EXIT_FAILURE);
    }
  } else if (-1 != access(MINICORE_SHELL_PATH, X_OK)) {
    if (0 == (pid = syscall(__NR_clone, CLONE_VFORK, nullptr, nullptr, nullptr, nullptr))) {
      IGNORE_RETURN execlp("sh", "sh", MINICORE_SHELL_PATH, nullptr);
      _exit(EXIT_FAILURE);
    }
  }
  return ret;
}

int faststack()
{
  static constexpr int64_t MIN_INTERVAL = 30 * 60 * 1000 * 1000; // 30min
  static int64_t last_ts = 0;
  int64_t now = ObTimeUtility::fast_current_time();
  int64_t last = ATOMIC_LOAD(&last_ts);
  int ret = OB_SUCCESS;
  if (now - last < MIN_INTERVAL) {
    ret = OB_EAGAIN;
  } else if (!ATOMIC_BCAS(&last_ts, last, now)) {
    ret = OB_EAGAIN;
  } else if (-1 == access(FASTSTACK_SHELL_PATH, R_OK)) {
    if (0 == syscall(__NR_clone, CLONE_VFORK | CLONE_PARENT, nullptr, nullptr, nullptr, nullptr)) {
      IGNORE_RETURN execlp("sh", "sh", "-c", FASTSTACK_SCRIPT, nullptr);
      _exit(EXIT_FAILURE);
    }
  } else if (-1 != access(FASTSTACK_SHELL_PATH, X_OK)) {
    if (0 == syscall(__NR_clone, CLONE_VFORK | CLONE_PARENT, nullptr, nullptr, nullptr, nullptr)) {
      IGNORE_RETURN execlp("sh", "sh", FASTSTACK_SHELL_PATH, nullptr);
      _exit(EXIT_FAILURE);
    }
  }
  LOG_WARN("faststack", K(now), K(ret));
  return ret;
}

} // namespace common
} // namespace oceanbase
