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
#include <unistd.h>

namespace oceanbase
{
namespace common
{

ObSigFaststack::ObSigFaststack()
    : min_interval_(30 * 60 * 1000 * 1000UL) // 30min
{
}

ObSigFaststack::~ObSigFaststack()
{
}

ObSigFaststack &ObSigFaststack::get_instance()
{
  static ObSigFaststack sig_faststack;
  return sig_faststack;
}

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
const char *const FASTSTACK_SCRIPT_ARGV[] = {"/bin/sh", "-c", FASTSTACK_SCRIPT, NULL};
const char *const FASTSTACK_SHELL_ARGV[] = {"/bin/sh", FASTSTACK_SHELL_PATH, NULL};
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

#define COMMON_FMT "timestamp=%ld, tid=%ld, tname=%s, trace_id=%s, extra_info=(%s), lbt=%s"

void coredump_cb(int, int, void*, void*);
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
      coredump_cb(sig, si->si_code, si->si_addr, context);
    }
  }
}

void hook_sigsegv_msg(int sig, siginfo_t *si, void *context)
{
  if (mprotect_page(si->si_addr, 8, PROT_READ | PROT_WRITE, "release signal addr") != 0) {
    coredump_cb(sig, si->si_code, si->si_addr, context);
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

  lnprintf(path, 32, "/proc/%d/fd/", pid);
  if (NULL == (dir = opendir(path))) {
  } else {
    while(NULL != (fd_file = readdir(dir))) {
      if (0 != strcmp(fd_file->d_name, ".") && 0 != strcmp(fd_file->d_name, "..")
        && 0 != strcmp(fd_file->d_name, "0") && 0 != strcmp(fd_file->d_name, "1")
        && 0 != strcmp(fd_file->d_name, "2")) {
        lnprintf(name, 32, "/proc/%d/fd/%s", pid, fd_file->d_name);
        if (-1 == readlink(name, real_name, 32)) {
          DLOG(INFO, "[CLOSEFD], err read link %s, errno = %d", name, errno);
        } else {
          lnprintf(name, 32, "%s", real_name);
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


void coredump_cb(volatile int sig, volatile int sig_code, void* volatile sig_addr, void *context)
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
    char bt[512] = {'\0'};
    int64_t len = 0;
    // extra
    const ObFatalErrExtraInfoGuard *extra_info = nullptr; // TODO: May deadlock, ObFatalErrExtraInfoGuard::get_thd_local_val_ptr();
    auto *trace_id = ObCurTraceId::get_trace_id();
    char trace_id_buf[128] = {'\0'};
    if (trace_id != nullptr) {
      int64_t pos = trace_id->to_string(trace_id_buf, sizeof(trace_id_buf));
      if (pos < sizeof(trace_id_buf)) {
        trace_id_buf[pos]= '\0';
      }
    }
    char print_buf[1024];
    const ucontext_t *con = (ucontext_t *)context;
#if defined(__x86_64__)
    int64_t ip = con->uc_mcontext.gregs[REG_RIP];
    int64_t bp = con->uc_mcontext.gregs[REG_RBP]; // stack base
    safe_backtrace(bt, sizeof(bt) - 1, &len);
#elif defined(__aarch64__)
    int64_t ip = con->uc_mcontext.regs[30];
    int64_t bp = con->uc_mcontext.regs[29];
    void* addrs[64];
    int n_addr = light_backtrace(addrs, ARRAYSIZEOF(addrs), bp);
    len += safe_parray(bt, sizeof(bt) - 1, (int64_t*)addrs, n_addr);
#else
    int64_t ip = -1;
    int64_t bp = -1;
#endif
    bt[len++] = '\0';
    char rlimit_core[32] = "unlimited";
    if (UINT64_MAX != g_rlimit_core) {
      lnprintf(rlimit_core, sizeof(rlimit_core), "%lu", g_rlimit_core);
    }
    char crash_info[128] = "CRASH ERROR!!!";
    int64_t fatal_error_thread_id = get_fatal_error_thread_id();
    if (-1 != fatal_error_thread_id) {
      lnprintf(crash_info, sizeof(crash_info),
               "Right to Die or Duty to Live's Thread Existed before CRASH ERROR!!!"
               "ThreadId=%ld,", fatal_error_thread_id);
    }
    ssize_t print_len = lnprintf(print_buf, sizeof(print_buf),
                                 "%s IP=%lx, RBP=%lx, sig=%d, sig_code=%d, sig_addr=%p, RLIMIT_CORE=%s, "COMMON_FMT", ",
                                  crash_info, ip, bp, sig, sig_code, sig_addr, rlimit_core,
                                  ts, GETTID(), tname, trace_id_buf,
                                  (NULL == extra_info) ? NULL : to_cstring(*extra_info), bt);
    ObSqlInfo sql_info = ObSqlInfoGuard::get_tl_sql_info();
    char sql_id[] = "SQL_ID=";
    char sql_string[] = ", SQL_STRING=";
    char end[] = "\n";
    struct iovec iov[6];
    memset(iov, 0, sizeof(iov));
    iov[0].iov_base = print_buf;
    iov[0].iov_len = print_len;
    iov[1].iov_base = sql_id;
    iov[1].iov_len = strlen(sql_id);
    iov[2].iov_base = sql_info.sql_id_.ptr();
    iov[2].iov_len = sql_info.sql_id_.length();
    iov[3].iov_base = sql_string;
    iov[3].iov_len = strlen(sql_string);
    iov[4].iov_base = sql_info.sql_string_.ptr();
    iov[4].iov_len = sql_info.sql_string_.length();
    iov[5].iov_base = end;
    iov[5].iov_len = strlen(end);
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
  static int64_t last_ts = 0;
  int64_t now = ObTimeUtility::fast_current_time();
  int64_t last = ATOMIC_LOAD(&last_ts);
  int ret = OB_SUCCESS;
  pid_t pid;
  if (now - last < ObSigFaststack::get_instance().get_min_interval()) {
    ret = OB_EAGAIN;
  } else if (!ATOMIC_BCAS(&last_ts, last, now)) {
    ret = OB_EAGAIN;
  } else if (-1 == access(FASTSTACK_SHELL_PATH, R_OK)) {
    if ((pid = vfork()) < 0) {
      LOG_WARN("fork first child failed");
    } else if (pid == 0) { /* first child */
      if ((pid = vfork()) < 0) {
        LOG_WARN("fork second child failed");
      } else if (pid > 0) {
        _exit(EXIT_SUCCESS); /* parent from second fork == first child */
      } else {
        IGNORE_RETURN syscall(SYS_execve, "/bin/sh", FASTSTACK_SCRIPT_ARGV, nullptr);
        _exit(EXIT_FAILURE);
      }
    }
    if (waitpid(pid, NULL, 0) != pid) {
      LOG_WARN("wait child process:FASTSTACK_SCRIPT failed");
    }
  } else if (-1 != access(FASTSTACK_SHELL_PATH, X_OK)) {
    if ((pid = vfork()) < 0) {
      LOG_WARN("fork first child failed");
    } else if (pid == 0) { /* first child */
      if ((pid = vfork()) < 0) {
        LOG_WARN("fork second child failed");
      } else if (pid > 0) {
        _exit(EXIT_SUCCESS); /* parent from second vfork == first child */
      } else {
        IGNORE_RETURN syscall(SYS_execve, "/bin/sh", FASTSTACK_SHELL_ARGV, nullptr);
        _exit(EXIT_FAILURE);
      }
    }
    if (waitpid(pid, NULL, 0) != pid) {
      LOG_WARN("wait child process:FASTSTACK_SHELL failed");
    }
  }
  LOG_WARN("faststack", K(now), K(ret));
  return ret;
}

} // namespace common
} // namespace oceanbase
