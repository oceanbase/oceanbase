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

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/prctl.h>
#include <dirent.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/mount.h>
#include <sched.h>
#include <stdarg.h>
#include <libminijail.h>
#include <poll.h>
#include <getopt.h>
#include <map>
#include <set>
#include <string>
#include <sstream>
#include "lib/oblog/ob_log.h"
#include "lib/ob_errno.h"
#include "lib/profile/ob_trace_id.h"
#include "share/ob_sandbox_protocol.h"
#include "share/ob_version.h"
#include "ob_sandbox_setup.h"

#define USING_LOG_PREFIX COMMON
using namespace oceanbase::observer;
using namespace oceanbase::common;

static int comm_fd = -1;
static int pass_fd = -1;
static bool g_support_userns = false;
static struct sock_fprog g_seccomp_prog = {0, nullptr};
static bool g_seccomp_prog_ready = false;

static void print_version()
{
  fprintf(stderr, "ob_sandbox (%s)\n", PACKAGE_STRING);
  fprintf(stderr, "REVISION: %s\n", build_version());
  fprintf(stderr, "BUILD_BRANCH: %s\n", build_branch());
  fprintf(stderr, "BUILD_TIME: %s %s\n", build_date(), build_time());
  fprintf(stderr, "BUILD_FLAGS: %s\n", build_flags());
  fprintf(stderr, "BUILD_INFO: %s\n", build_info());
  fprintf(stderr, "Copyright (c) 2011-present OceanBase Inc.\n");
}

struct SandboxOptions {
  int comm_fd_;
  int pass_fd_;
  std::string log_level_;
  std::string log_path_;

  SandboxOptions() : comm_fd_(-1), pass_fd_(-1), log_level_("WDIAG"), log_path_("log/ob_sandbox.log") {}

  void print_usage(const char* prog_name) {
    fprintf(stderr, "Usage: %s [options]\n", prog_name);
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "  --comm_fd=FD      Communication file descriptor (required)\n");
    fprintf(stderr, "  --pass_fd=FD      FD passing file descriptor (required)\n");
    fprintf(stderr, "  --log_level=LEVEL Log level (default: WDIAG)\n");
    fprintf(stderr, "  --log_path=PATH   Log file path (default: log/ob_sandbox.log)\n");
    fprintf(stderr, "  -V, --version     Show version information\n");
    fprintf(stderr, "  -h, --help        Show this help message\n");
  }

  int parse(int argc, char* argv[]) {
    int ret = OB_SUCCESS;
    static struct option long_options[] = {
      {"comm_fd",   required_argument, 0, 'c'},
      {"pass_fd",   required_argument, 0, 'p'},
      {"log_level", required_argument, 0, 'l'},
      {"log_path",  required_argument, 0, 'f'},
      {"version",   no_argument,       0, 'V'},
      {"help",      no_argument,       0, 'h'},
      {0, 0, 0, 0}
    };

    int opt;
    int option_index = 0;
    while ((opt = getopt_long(argc, argv, "c:p:l:f:Vh", long_options, &option_index)) != -1) {
      switch (opt) {
        case 'c':
          comm_fd_ = atoi(optarg);
          break;
        case 'p':
          pass_fd_ = atoi(optarg);
          break;
        case 'l':
          log_level_ = optarg;
          break;
        case 'f':
          log_path_ = optarg;
          break;
        case 'V':
          print_version();
          exit(0);
        case 'h':
          print_usage(argv[0]);
          exit(0);
        default:
          print_usage(argv[0]);
          ret = OB_INVALID_ARGUMENT;
          break;
      }
    }
    if (OB_SUCC(ret)) {
      if (comm_fd_ <= 0 || pass_fd_ <= 0) {
        fprintf(stderr, "Error: --comm_fd and --pass_fd are required and must be positive.\n");
        print_usage(argv[0]);
        ret = OB_INVALID_ARGUMENT;
      }
    }
    return ret;
  }
};

static int recv_fds(int sock, int *fds, int max_count) {
  int ret = 0;
  struct msghdr msg;
  memset(&msg, 0, sizeof(msg));
  struct iovec iov;
  char buf[1]; // dummy data
  char control[CMSG_SPACE(sizeof(int) * max_count)];

  iov.iov_base = buf;
  iov.iov_len = sizeof(buf);

  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  msg.msg_control = control;
  msg.msg_controllen = sizeof(control);

  if (recvmsg(sock, &msg, 0) < 0) {
    ret = -1;
  } else {
    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    if (cmsg && cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS) {
      int received_len = cmsg->cmsg_len - CMSG_LEN(0);
      int count = received_len / sizeof(int);
      if (count > max_count) count = max_count;
      memcpy(fds, CMSG_DATA(cmsg), count * sizeof(int));
      ret = count;
    }
  }
  return ret;
}

static char g_tname[16] = {0};
static pid_t g_tid = -1;

static void init_thread_info() {
  if (g_tid == -1) {
    prctl(PR_GET_NAME, g_tname);
    g_tid = syscall(SYS_gettid);
  }
}

// Probe whether the running kernel allows the current (unprivileged) process
// to create a new user namespace
static bool probe_user_namespace_support() {
  int ret = OB_SUCCESS;
  bool supported = false;
  pid_t pid = fork();
  if (pid < 0) {
    int err = errno;
    ret = OB_ERR_SYS;
    LOG_WARN("probe user namespace: fork failed", K(ret), K(err));
  } else if (pid == 0) {
    int exit_code = 0;
    if (unshare(CLONE_NEWUSER) != 0) {
      exit_code = (errno != 0) ? errno : 1;
    }
    _exit(exit_code);
  } else {
    int status = 0;
    pid_t w = -1;
    do {
      w = waitpid(pid, &status, 0);
    } while (w < 0 && errno == EINTR);
    if (w == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0) {
      supported = true;
    } else {
      int exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("probe user namespace: unshare failed in child",
               K(ret), K(status), K(exit_code));
    }
  }
  return supported;
}

// Simple logging wrapper
void log_msg(const char* fmt, ...) {
  init_thread_info();
  char buffer[1024];
  va_list args;
  va_start(args, fmt);

  // Get timestamp
  struct timeval tv;
  gettimeofday(&tv, NULL);
  struct tm* tm_info = localtime(&tv.tv_sec);

  int len = snprintf(buffer, sizeof(buffer), "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] [%d] [%s] ",
           tm_info->tm_year + 1900, tm_info->tm_mon + 1, tm_info->tm_mday,
           tm_info->tm_hour, tm_info->tm_min, tm_info->tm_sec, tv.tv_usec,
           g_tid, g_tname);

  if (len < sizeof(buffer)) {
    vsnprintf(buffer + len, sizeof(buffer) - len, fmt, args);
  }

  va_end(args);
  fprintf(stderr, "%s\n", buffer);
}

struct ProcessInfo {
  int status; // last known status
  bool running;
  ProcessInfo() : status(0), running(false) {}
};

std::map<pid_t, ProcessInfo> processes;

// Tracks binary paths whose runtime dependencies have already been copied
// into the sandbox root. Used to avoid repeating ld.so --list on every
// handle_create. Event loop is single-threaded so no locking is required.
static std::set<std::string> g_processed_binaries;

void reap_zombies() {
  int status;
  pid_t pid;
  int ret = 0; // for LOG_ERROR macro
  while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
    if (processes.find(pid) != processes.end()) {
      processes[pid].running = false;
      processes[pid].status = status;
      int exit_code = 0;
      int sig = 0;
      bool is_coredump = false;
      if (WIFEXITED(status)) {
        exit_code = WEXITSTATUS(status);
        LOG_WARN("Process exited normally", K(pid), K(status), K(exit_code));
      } else if (WIFSIGNALED(status)) {
        sig = WTERMSIG(status);
        is_coredump = WCOREDUMP(status);
        const char* sig_name = strsignal(sig);
        LOG_WARN("Process exited by signal", K(pid), K(status), K(sig), K(sig_name));
        if (is_coredump) {
          LOG_ERROR("Process coredumped", K(pid), K(sig));
        }
      } else {
        LOG_INFO("Process exited with unknown reason", K(pid), K(status));
      }
    }
  }
}

void reap_process(pid_t pid, ProcessInfo& info) {
  int status;
  if (waitpid(pid, &status, WNOHANG) == pid) {
    info.running = false;
    info.status = status;
  }
}

static int pre_execve_hook(void *payload) {
  int err = 0;
  prctl(PR_SET_PDEATHSIG, SIGKILL);
  pid_t ppid = getppid();
  if (ppid == 1) {
    fprintf(stderr, "Parent process has changed to init (pid=1), the sandbox will exit, pid=%d\n", ppid);
    err = -1;
  }
  return err;
}

static int setup_minijail(struct minijail *j, const SandboxMsgCreate& msg) {
  int ret = OB_SUCCESS;
  if (j) {
    minijail_namespace_user(j);
    minijail_namespace_vfs(j);
    // Use root_path_ from msg, or default to OB_SANDBOX_ROOT_PATH if empty
    const char* root_path = (msg.root_path_.length() > 0) ? msg.root_path_.ptr() : OB_SANDBOX_ROOT_PATH;
    minijail_enter_pivot_root(j, root_path);
    int err = 0;
    err = minijail_bind(j, msg.binary_path_.ptr(), msg.binary_path_.ptr(), 0);
    if (err != 0) {
      ret = OB_ERR_SYS;
      LOG_WARN("Failed to bind binary path", K(ret), K(err), K(msg.binary_path_.ptr()));
    }

    minijail_bind(j, "/proc/self", "/proc/self", 0);

    // Process mount infos from msg
    for (int64_t i = 0; OB_SUCC(ret) && i < msg.mount_infos_.count(); ++i) {
      const SandboxMountInfo& mount_info = msg.mount_infos_.at(i);
      if (mount_info.src_.length() > 0) {
        const char* src_path = mount_info.src_.ptr();
        const char* dest_path = (mount_info.dest_.length() > 0) ? mount_info.dest_.ptr() : src_path;
        const int writeable = (mount_info.flags_ & MS_RDONLY) ? 0 : 1;
        if ((ret = minijail_bind(j, src_path, dest_path, writeable)) != 0) {
          ret = OB_ERR_SYS;
          LOG_WARN("Failed to bind mount path", K(ret), K(i), K(src_path), K(dest_path), K(writeable), K(err));
        } else {
          LOG_TRACE("Bind mount path", K(i), K(src_path), K(dest_path), K(writeable));
        }
      }
    }

    // Mount minimal /dev for native extensions.
    // Cannot use minijail_mount_dev() because it calls mknod() internally
    // which is forbidden inside a user namespace (causes EPERM -> abort).
    // Instead, bind-mount individual device files from the host.
    // Mount points must already exist in sandbox root (created by prepare_sandbox_root).
    if (OB_SUCC(ret)) {
      struct DevNode { const char *path; int writable; };
      static const DevNode dev_nodes[] = {
        {"/dev/null", 1}, {"/dev/zero", 1}, {"/dev/full", 1},
        {"/dev/urandom", 0}, {"/dev/random", 0}, {NULL, 0}
      };
      for (int i = 0; dev_nodes[i].path != NULL; ++i) {
        int bind_err = minijail_bind(j, dev_nodes[i].path, dev_nodes[i].path, dev_nodes[i].writable);
        if (bind_err != 0) {
          LOG_WARN("Failed to bind device node (non-fatal)", K(bind_err), "dev", dev_nodes[i].path);
        } else {
          LOG_TRACE("Bind device node", K(i), K(dev_nodes[i].path));
        }
      }
      LOG_TRACE("Mounted minimal /dev via bind-mount");
    }

    if (OB_SUCC(ret)) {
      LOG_TRACE("Mount paths configured", "count", msg.mount_infos_.count(),
               K(msg.binary_path_.ptr()), K(root_path));
    }

    if (!msg.enable_net_) {
      minijail_namespace_net(j);
      LOG_TRACE("Network namespace enabled (network disabled)");
    } else {
      LOG_INFO("Network access enabled");
    }

    minijail_namespace_pids(j);
    minijail_no_new_privs(j);
    minijail_close_open_fds(j);
    minijail_run_as_init(j);
    minijail_add_hook(j, pre_execve_hook, NULL, MINIJAIL_HOOK_EVENT_PRE_EXECVE);
    minijail_preserve_fd(j, STDIN_FILENO, STDIN_FILENO);
    minijail_preserve_fd(j, STDOUT_FILENO, STDOUT_FILENO);
    minijail_preserve_fd(j, STDERR_FILENO, STDERR_FILENO);
    minijail_namespace_user_disable_setgroups(j);

    char uid_map[32];
    char gid_map[32];
    snprintf(uid_map, sizeof(uid_map), "0 %d 1", getuid());
    snprintf(gid_map, sizeof(gid_map), "0 %d 1", getgid());
    minijail_uidmap(j, uid_map);
    minijail_gidmap(j, gid_map);

    if (g_seccomp_prog_ready) {
      minijail_use_seccomp_filter(j);
      minijail_set_seccomp_filters(j, &g_seccomp_prog);
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "seccomp filter not initialized, skip installing");
    }
  }
  return ret;
}

// Validate that mount_infos only contain same-path entries. Path remapping
// (src != dest) cannot be emulated without a private mount namespace.
static int check_degraded_mount_infos(const SandboxMsgCreate& msg) {
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < msg.mount_infos_.count(); ++i) {
    const SandboxMountInfo& mi = msg.mount_infos_.at(i);
    if (mi.src_.length() == 0) {
      continue;
    }
    const bool has_dest = (mi.dest_.length() > 0);
    const bool same_path =
        !has_dest ||
        (mi.dest_.length() == mi.src_.length() &&
         0 == MEMCMP(mi.dest_.ptr(), mi.src_.ptr(), mi.src_.length()));
    if (!same_path) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("degraded sandbox does not support path remapping",
               K(ret), K(i), "src", mi.src_.ptr(), "dest", mi.dest_.ptr());
    } else if (mi.flags_ & MS_RDONLY) {
      LOG_WARN("degraded sandbox ignores read-only mount flag",
               K(i), "src", mi.src_.ptr());
    }
  }
  return ret;
}

// Close every fd > STDERR_FILENO listed under /proc/self/fd, keeping only
// stdio and up to two caller-specified fds (pass -1 to skip).
static void close_fds_except(int keep_a = -1, int keep_b = -1) {
  DIR *d = opendir("/proc/self/fd");
  if (d != nullptr) {
    int dir_fd = dirfd(d);
    struct dirent *ent = nullptr;
    while ((ent = readdir(d)) != nullptr) {
      if (ent->d_name[0] != '.') {
        int fd = atoi(ent->d_name);
        if (fd > STDERR_FILENO && fd != dir_fd && fd != keep_a && fd != keep_b) {
          close(fd);
        }
      }
    }
    closedir(d);
  }
}

// Degraded execution path used when user namespace is not available on the
// kernel. This is a plain fork + execve with minimal setup; no sandboxing
// is performed beyond PR_SET_PDEATHSIG to tie the child's lifetime to
// ob_sandbox.
static int run_degraded_process(const SandboxMsgCreate& msg, const char *path,
                                char *const argv[], int stdin_fd, int stdout_fd,
                                pid_t& child_pid) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_degraded_mount_infos(msg))) {
    LOG_WARN("check_degraded_mount_infos failed", K(ret));
  } else {
    pid_t pid = fork();
    if (pid < 0) {
      int err = errno;
      ret = OB_ERR_SYS;
      LOG_ERROR("fork failed in degraded path", K(ret), K(err));
    } else if (pid == 0) {
      // Child. Keep the work here minimal: libc locks may be in an
      // inconsistent state until execve, so stick to async-signal-safe
      // syscalls where possible.
      prctl(PR_SET_PDEATHSIG, SIGKILL);
      if (getppid() == 1) {
        _exit(127);
      }
      if (stdin_fd > 0 && stdin_fd != STDIN_FILENO) {
        dup2(stdin_fd, STDIN_FILENO);
      }
      if (stdout_fd > 0 && stdout_fd != STDOUT_FILENO) {
        dup2(stdout_fd, STDOUT_FILENO);
      }
      close_fds_except();
      execve(path, argv, environ);
      _exit(127);
    } else {
      child_pid = pid;
    }
  }
  return ret;
}

static int recv_process_fds(const SandboxMsgCreate& msg, int& stdin_fd, int& stdout_fd) {
  int ret = OB_SUCCESS;
  int msg_stdin = msg.stdin_fd_;
  int msg_stdout = msg.stdout_fd_;
  stdin_fd = msg_stdin;
  stdout_fd = msg_stdout;

  bool expect_fds = (msg_stdin >= 0) || (msg_stdout >= 0);
  if (expect_fds) {
    int received_fds[2] = {-1, -1};
    int cnt = recv_fds(pass_fd, received_fds, 2);
    if (cnt < 0) {
       ret = OB_ERR_SYS;
       LOG_WARN("recv_fds failed", K(ret), K(errno));
    } else {
      int current_fd_idx = 0;
      if (msg_stdin >= 0 && current_fd_idx < cnt) {
        stdin_fd = received_fds[current_fd_idx++];
      }
      if (msg_stdout >= 0 && current_fd_idx < cnt) {
        stdout_fd = received_fds[current_fd_idx++];
      }

      for (; current_fd_idx < cnt; ++current_fd_idx) {
        close(received_fds[current_fd_idx]);
      }
    }
  }
  return ret;
}

static int run_minijail_process(const SandboxMsgCreate& msg,
                                int stdin_fd, int stdout_fd, pid_t& child_pid) {
  int ret = OB_SUCCESS;

  // Construct path and argv from msg
  std::string path(msg.binary_path_.ptr(), msg.binary_path_.length());
  std::string arg_str(msg.arg_str_.ptr(), msg.arg_str_.length());
  std::stringstream ss(arg_str);
  std::string segment;
  std::vector<char*> argv;
  argv.push_back(strdup(path.c_str()));
  while (std::getline(ss, segment, ' ')) {
    if (!segment.empty()) {
      argv.push_back(strdup(segment.c_str()));
    }
  }
  argv.push_back(nullptr);

  if (!g_support_userns) {
    if (OB_FAIL(run_degraded_process(msg, path.c_str(), argv.data(),
                                     stdin_fd, stdout_fd, child_pid))) {
      LOG_WARN("run_degraded_process failed", K(ret));
    }
  } else {
    struct minijail *j = minijail_new();
    if (nullptr == j) {
      ret = OB_ERR_SYS;
      LOG_ERROR("minijail_new failed", K(ret));
    } else if (OB_FAIL(setup_minijail(j, msg))) {
      minijail_destroy(j);
      LOG_ERROR("setup_minijail failed", K(ret));
    } else {
      int mj_ret = 0;
      int stdin_fd_back = -1;
      int stdout_fd_back = -1;

      // Setup FDs
      if (stdin_fd > 0) {
        stdin_fd_back = dup(STDIN_FILENO);
        dup2(stdin_fd, STDIN_FILENO);
      }
      if (stdout_fd > 0) {
        stdout_fd_back = dup(STDOUT_FILENO);
        dup2(stdout_fd, STDOUT_FILENO);
      }

      mj_ret = minijail_run_pid_pipes_no_preload(
        j, path.c_str(), argv.data(), &child_pid,
        nullptr, nullptr, nullptr);

      // Restore FDs
      if (stdin_fd_back > 0) {
        dup2(stdin_fd_back, STDIN_FILENO);
        close(stdin_fd_back);
      }
      if (stdout_fd_back > 0) {
        dup2(stdout_fd_back, STDOUT_FILENO);
        close(stdout_fd_back);
      }
      if (mj_ret != 0) {
        ret = OB_ERR_SYS;
        LOG_ERROR("minijail_run_pid_pipes_no_preload failed", K(ret), K(mj_ret));
      }
      minijail_destroy(j);
    }
  }

  // Free argv memory
  for (auto p : argv) {
    if (p != nullptr) free(p);
  }

  return ret;
}

void handle_create(int fd, uint32_t len) {
  int ret = OB_SUCCESS;
  std::vector<char> buf(len);

  if (ObSandboxProtocolHelper::read_n(fd, buf.data(), len) < 0) {
    ret = OB_ERR_SYS;
    LOG_ERROR("handle_create read_n payload failed", K(ret), K(len));
  } else {
    SandboxMsgCreate msg;
    int64_t pos = 0;
    if (OB_FAIL(msg.deserialize(buf.data(), len, pos))) {
      LOG_ERROR("handle_create deserialize failed", K(ret), K(len));
      ObSandboxProtocolHelper::send_response(fd, SandboxResponse());
    } else {
      int32_t stdin_fd = -1;
      int32_t stdout_fd = -1;

      if (OB_FAIL(recv_process_fds(msg, stdin_fd, stdout_fd))) {
         LOG_WARN("recv_process_fds failed", K(ret));
         ObSandboxProtocolHelper::send_response(fd, SandboxResponse());
      } else {
        LOG_TRACE("handle_create", K(msg), K(stdin_fd), K(stdout_fd));

        // On first encounter with this binary path, copy its runtime shared
        // library dependencies (discovered via ld.so --list) into the sandbox
        // root. This is best-effort: failures are logged but do not abort the
        // request — if a library is truly missing, minijail's execve will fail
        // and the error will surface through the normal response path.
        {
          std::string bin_key(msg.binary_path_.ptr(), msg.binary_path_.length());
          if (!bin_key.empty()
              && g_processed_binaries.find(bin_key) == g_processed_binaries.end()) {
            copy_binary_dependencies(bin_key.c_str());
            g_processed_binaries.insert(bin_key);
          }
        }

        pid_t child_pid = -1;
        if (OB_FAIL(run_minijail_process(msg, stdin_fd, stdout_fd, child_pid))) {
           LOG_WARN("run_minijail_process failed", K(ret));
           ObSandboxProtocolHelper::send_response(fd, SandboxResponse());
        } else {
          if (child_pid > 0) {
            ProcessInfo info;
            info.running = true;
            processes[child_pid] = info;
          }

          LOG_TRACE("Created sandbox process", K(child_pid), K(msg.binary_path_.ptr()));

          SandboxResponse resp;
          resp.ret_code_ = 0;
          resp.pid_ = child_pid;
          resp.is_running_ = 1;
          ObSandboxProtocolHelper::send_response(fd, resp);
        }
        if (stdin_fd > 0) {
          close(stdin_fd);
        }
        if (stdout_fd > 0) {
          close(stdout_fd);
        }
      }
    }
  }
}

void handle_destroy(int fd, uint32_t len) {
  int ret = OB_SUCCESS;
  std::vector<char> buf(len);
  if (ObSandboxProtocolHelper::read_n(fd, buf.data(), len) < 0) {
    ret = OB_ERR_SYS;
    LOG_WARN("handle_destroy read_n failed", K(ret));
  } else {
    SandboxMsgDestroy msg;
    int64_t pos = 0;
    if (OB_FAIL(msg.deserialize(buf.data(), len, pos))) {
      LOG_WARN("handle_destroy deserialize failed", K(ret));
      ObSandboxProtocolHelper::send_response(fd, SandboxResponse());
    } else {
      int32_t target_pid = msg.pid_;
      SandboxResponse resp;
      resp.pid_ = target_pid;
      std::map<pid_t, ProcessInfo>::iterator it = processes.find(target_pid);
      if (it == processes.end()) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("handle_destroy target_pid not found", K(ret), K(target_pid));
        resp.ret_code_ = ret;
      } else {
        reap_process(target_pid, it->second);
        if (it->second.running) {
          LOG_INFO("kill process when destroying", K(target_pid));
          int kill_ret = kill(target_pid, SIGKILL);
          if (kill_ret == 0) {
            int status;
            waitpid(target_pid, &status, 0);
            it->second.running = false;
            it->second.status = status;
          } else {
            ret = OB_ERR_SYS;
            LOG_WARN("handle_destroy kill failed", K(ret), K(target_pid), K(kill_ret), K(errno));
            resp.ret_code_ = ret;
          }
        }
        if (OB_SUCC(ret)) {
          resp.ret_code_ = 0;
          resp.status_ = it->second.status;
          processes.erase(it);
        }
      }
      ObSandboxProtocolHelper::send_response(fd, resp);
    }
  }
}

void handle_check(int fd, uint32_t len) {
  int ret = OB_SUCCESS;
  std::vector<char> buf(len);
  if (ObSandboxProtocolHelper::read_n(fd, buf.data(), len) < 0) {
    ret = OB_ERR_SYS;
    LOG_WARN("handle_check read_n failed", K(ret));
  } else {
    SandboxMsgCheck msg;
    int64_t pos = 0;
    if (OB_FAIL(msg.deserialize(buf.data(), len, pos))) {
      LOG_WARN("handle_check deserialize failed", K(ret));
      ObSandboxProtocolHelper::send_response(fd, SandboxResponse());
    } else {
      int32_t target_pid = msg.pid_;
      SandboxResponse resp;
      resp.pid_ = target_pid;

      std::map<pid_t, ProcessInfo>::iterator it = processes.find(target_pid);
      if (it != processes.end()) {
        ProcessInfo &info = it->second;
        reap_process(target_pid, info);
        resp.ret_code_ = 0;
        resp.status_ = info.status;
        resp.is_running_ = info.running ? 1 : 0;
      } else {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("handle_check unknown pid", K(ret), K(target_pid));
        resp.ret_code_ = ret;
      }
      ObSandboxProtocolHelper::send_response(fd, resp);
    }
  }
}

static void run_event_loop(int comm_fd) {
  int ret = OB_SUCCESS;
  bool keep_running = true;

  struct pollfd pfd;
  pfd.fd = comm_fd;
  pfd.events = POLLIN;

  while (keep_running) {
    reap_zombies();

    // Wait for data or timeout (1000 ms)
    int poll_ret = poll(&pfd, 1, 1000);
    if (poll_ret < 0) {
      if (errno == EINTR) {
        continue;
      } else {
        ret = OB_ERR_SYS;
        LOG_ERROR("poll failed", K(errno));
        keep_running = false;
        break;
      }
    } else if (poll_ret == 0) {
      // Timeout, loop back to reap_zombies
      continue;
    }

    if (pfd.revents & POLLIN) {
      SandboxMsgHeader header;
      if (ObSandboxProtocolHelper::read_n(comm_fd, (char*)&header, sizeof(header)) < 0) {
        keep_running = false;
      } else {
        ObCurTraceId::set(header.trace_id_);
        if (header.magic_ != SANDBOX_MAGIC) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("Invalid magic", K(ret));
          keep_running = false;
        } else {
          switch (header.type_) {
            case SANDBOX_MSG_CREATE:
              handle_create(comm_fd, header.data_len_);
              break;
            case SANDBOX_MSG_DESTROY:
              handle_destroy(comm_fd, header.data_len_);
              break;
            case SANDBOX_MSG_CHECK:
              handle_check(comm_fd, header.data_len_);
              break;
            default:
              std::vector<char> dummy(header.data_len_);
              ObSandboxProtocolHelper::read_n(comm_fd, dummy.data(), header.data_len_);
              ObSandboxProtocolHelper::send_response(comm_fd, SandboxResponse());
              break;
          }
        }
      }
    } else if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
      ret = OB_ERR_SYS;
      LOG_WARN("poll error event", K(pfd.revents));
      keep_running = false;
    }
  }
}

int main(int argc, char* argv[]) {
  int ret = OB_SUCCESS;
  // Ignore SIGPIPE: when observer closes comm_fd unexpectedly, write() should
  // return EPIPE through read_n_write() instead of terminating the process.
  ::signal(SIGPIPE, SIG_IGN);
  init_thread_info();
  SandboxOptions options;
  if (OB_FAIL(options.parse(argc, argv))) {
    ret = 1;
  } else {
    comm_fd = options.comm_fd_;
    pass_fd = options.pass_fd_;

    OB_LOGGER.set_log_level(options.log_level_.c_str());
    bool no_redirect_log = (comm_fd <= 2);
    OB_LOGGER.set_file_name(options.log_path_.c_str(), no_redirect_log);

    LOG_INFO("ob_sandbox started", K(comm_fd), K(pass_fd),
             "log_level", options.log_level_.c_str(),
             "log_path", options.log_path_.c_str());

    g_support_userns = probe_user_namespace_support();
    if (!g_support_userns) {
      LOG_ERROR_RET(OB_NOT_SUPPORTED, "user namespace not supported, fallback to degraded sandbox "
               "(no isolation: runs as plain fork+exec; path remapping "
               "in mount_infos will be rejected)");
    }

    // Prepare sandbox root directory and copy required libraries
    if (OB_SUCCESS != (ret = prepare_sandbox_root())) {
      LOG_WARN("Failed to prepare sandbox root directory (non-fatal)", K(ret));
      // Continue anyway, as this is best-effort
    }

    // Build the blacklist seccomp BPF program once at startup; it is shared
    // across every sandbox invocation. Failure here is non-fatal — we fall
    // back to running without a filter rather than refusing all requests.
    int prog_ret = build_blacklist_bpf_program(&g_seccomp_prog);
    if (prog_ret != OB_SUCCESS) {
      LOG_ERROR("build_blacklist_bpf_program failed (non-fatal)", K(prog_ret));
    } else {
      g_seccomp_prog_ready = true;
      LOG_INFO("Built blacklist seccomp filter", "instr_count", g_seccomp_prog.len);
    }

    run_event_loop(comm_fd);
    close(comm_fd);
    for (auto &pair : processes) {
      if (pair.second.running) {
        LOG_INFO("kill process when destroying ob_sandbox", "pid", pair.first);
        kill(pair.first, SIGKILL);
      }
    }
  }
  return ret;
}
