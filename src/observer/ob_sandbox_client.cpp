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
#include "observer/ob_sandbox_client.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"
#include <sys/socket.h>
#include <unistd.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/wait.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <fcntl.h>
#include "share/ob_sandbox_protocol.h"
#include "observer/ob_sandbox_manager.h"

namespace oceanbase
{
namespace observer
{

static const int MAX_SEND_FDS_COUNT = 2;

static int set_fd_cloexec(int fd)
{
  int ret = 0;
  int flags = fcntl(fd, F_GETFD);
  if (flags < 0 || fcntl(fd, F_SETFD, flags | FD_CLOEXEC) < 0) {
    ret = -1;
  }
  return ret;
}

// Format process exit status (from waitpid) into a human-readable string.
static void format_sandbox_exit_status(int status, char *buf, size_t buf_len, bool &is_coredump)
{
  is_coredump = false;
  if (WIFEXITED(status)) {
    snprintf(buf, buf_len, "exited normally, exit_code=%d", WEXITSTATUS(status));
  } else if (WIFSIGNALED(status)) {
    int sig = WTERMSIG(status);
    if (WCOREDUMP(status)) {
      is_coredump = true;
      snprintf(buf, buf_len, "killed by signal %d(%s), coredumped", sig, strsignal(sig));
    } else {
      snprintf(buf, buf_len, "killed by signal %d(%s)", sig, strsignal(sig));
    }
  } else {
    snprintf(buf, buf_len, "unknown reason, raw_status=%d", status);
  }
}

static int send_fds(int sock, const int *fds, int count) {
  int ret = 0;
  struct msghdr msg;
  memset(&msg, 0, sizeof(msg));
  struct iovec iov;
  char buf[1] = {0};
  char control[CMSG_SPACE(sizeof(int) * MAX_SEND_FDS_COUNT)];

  if (count < 0 || count > MAX_SEND_FDS_COUNT) {
    ret = -1;
  } else {
    iov.iov_base = buf;
    iov.iov_len = sizeof(buf);

    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    if (count > 0) {
      msg.msg_control = control;
      msg.msg_controllen = CMSG_SPACE(sizeof(int) * count);

      struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
      cmsg->cmsg_level = SOL_SOCKET;
      cmsg->cmsg_type = SCM_RIGHTS;
      cmsg->cmsg_len = CMSG_LEN(sizeof(int) * count);
      memcpy(CMSG_DATA(cmsg), fds, sizeof(int) * count);
    }

    if (sendmsg(sock, &msg, 0) < 0) {
      ret = -1;
    }
  }
  return ret;
}

ObSandboxClient::ObSandboxClient()
  : server_pid_(-1), server_fd_(-1), fd_pass_sock_(-1),
    mutex_(common::ObLatchIds::SANDBOX_LOCK)
{
}

ObSandboxClient::~ObSandboxClient()
{
  destroy();
}

int ObSandboxClient::create_ob_sandbox()
{
  lib::ObMutexGuard guard(mutex_);
  return create_ob_sandbox_();
}
int ObSandboxClient::create_ob_sandbox_()
{
  int ret = common::OB_SUCCESS;

  bool process_running = false;
  // Check if process is still running
  if (server_pid_ > 0) {
    int status;
    pid_t result = waitpid(server_pid_, &status, WNOHANG);
    if (result == 0) {
      // Process is still running
      SERVER_LOG(INFO, "ob_sandbox process is still running", K(server_pid_));
      process_running = true;
    } else {
      SERVER_LOG(WARN, "ob_sandbox process exited or error", K(server_pid_));
      if (server_fd_ != -1) { close(server_fd_); server_fd_ = -1; }
      if (fd_pass_sock_ != -1) { close(fd_pass_sock_); fd_pass_sock_ = -1; }
      server_pid_ = -1;
    }
  }

  int sv[2];
  if (process_running) {
    // do nothing
  } else if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
    ret = common::OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "socketpair failed", K(errno));
  } else {
    int fd_pass_sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fd_pass_sv) < 0) {
      ret = common::OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "socketpair fd_pass failed", K(errno));
      close(sv[0]);
      close(sv[1]);
    } else {
      const char* sandbox_path = OB_SANDBOX_BINARY_PATH;
      if (set_fd_cloexec(sv[0]) != 0 || set_fd_cloexec(fd_pass_sv[0]) != 0) {
        ret = common::OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "failed to set parent-side sandbox fds close-on-exec",
                   K(errno), K(sv[0]), K(fd_pass_sv[0]));
        close(sv[0]);
        close(sv[1]);
        close(fd_pass_sv[0]);
        close(fd_pass_sv[1]);
      } else if (access(sandbox_path, X_OK) != 0) {
        ret = common::OB_FILE_NOT_EXIST;
        SERVER_LOG(WARN, "ob_sandbox not found or not executable", K(sandbox_path), K(errno));
        close(sv[0]);
        close(sv[1]);
        close(fd_pass_sv[0]);
        close(fd_pass_sv[1]);
      } else {
        // prepare arguments for ob_sandbox
        char comm_fd_arg[32];
        snprintf(comm_fd_arg, sizeof(comm_fd_arg), "--comm_fd=%d", sv[1]);
        char pass_fd_arg[32];
        snprintf(pass_fd_arg, sizeof(pass_fd_arg), "--pass_fd=%d", fd_pass_sv[1]);
        const char* argv[] = {"ob_sandbox", comm_fd_arg, pass_fd_arg, NULL};
        const char* envp[] = {NULL};

        pid_t pid = vfork();
        if (pid < 0) {
          ret = common::OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "vfork failed", K(errno));
          close(sv[0]);
          close(sv[1]);
          close(fd_pass_sv[0]);
          close(fd_pass_sv[1]);
        } else if (pid == 0) {
          // Child
          syscall(SYS_execve, OB_SANDBOX_BINARY_PATH, (char* const*)argv, (char* const*)envp);
          _exit(127);
        } else {
          // Parent
          close(sv[1]); // Close child end
          close(fd_pass_sv[1]);
          server_fd_ = sv[0];
          fd_pass_sock_ = fd_pass_sv[0];
          server_pid_ = pid;
          SERVER_LOG(INFO, "ObSandboxClient inited", K(server_pid_), K(server_fd_));
        }
      }
    }
  }
  return ret;
}

void ObSandboxClient::destroy()
{
  lib::ObMutexGuard guard(mutex_);
  if (server_fd_ != -1) {
    close(server_fd_);
    server_fd_ = -1;
  }
  if (fd_pass_sock_ != -1) {
    close(fd_pass_sock_);
    fd_pass_sock_ = -1;
  }
  if (server_pid_ != -1) {
    SERVER_LOG(INFO, "wait ob_sandbox exiting", K(server_pid_));
    // child process will exit after server_fd_ closed, unneed to kill
    waitpid(server_pid_, NULL, 0);
    server_pid_ = -1;
  }
}

int ObSandboxClient::create_sandbox_process(const ObSandboxProcess& process,
                                           const int64_t timeout_us,
                                           pid_t &pid,
                                           const int stdin_fd,
                                           const int stdout_fd)
{
  int ret = create_sandbox_process_(process, timeout_us, pid, stdin_fd, stdout_fd);
  if (OB_UNLIKELY(OB_IO_ERROR == ret)) {
    SERVER_LOG(WARN, "create sandbox process failed, the ob_sandbox process may not running,"
      "create ob_sandbox and try again");
    ret = try_recycle_sandbox_process();
    if (OB_SUCC(ret)) {
      ret = create_sandbox_process_(process, timeout_us, pid, stdin_fd, stdout_fd);
    }
  }
  return ret;
}

int ObSandboxClient::create_sandbox_process_(const ObSandboxProcess& process,
                                            const int64_t timeout_us,
                                            pid_t &pid,
                                            const int stdin_fd,
                                            const int stdout_fd)
{
  lib::ObMutexGuard guard(mutex_);
  int ret = common::OB_SUCCESS;
  if (server_pid_ <= 0 ) {
    ret = create_ob_sandbox_();
  }
  if (OB_SUCC(ret)) {
    common::ObString path = process.execute_path_;
    common::ObString arg_str = process.execute_arg_;
    if (!path) {
      ret = common::OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid path", K(path));
    } else {
      SandboxMsgCreate msg;
      msg.root_path_ = process.root_path_;
      msg.binary_path_ = common::ObString(path);
      msg.arg_str_ = common::ObString(arg_str);
      msg.stdin_fd_ = stdin_fd;
      msg.stdout_fd_ = stdout_fd;
      // Copy mount_infos_ from process
      msg.mount_infos_.reset();
      for (int64_t i = 0; i < process.mount_infos_.count() && OB_SUCC(ret); ++i) {
        SandboxMountInfo mount_info;
        mount_info.src_ = process.mount_infos_.at(i).src_;
        mount_info.dest_ = process.mount_infos_.at(i).dest_;
        mount_info.flags_ = process.mount_infos_.at(i).flags_;
        if (OB_FAIL(msg.mount_infos_.push_back(mount_info))) {
          SERVER_LOG(WARN, "push mount_infos_ failed", K(ret), K(i));
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(ObSandboxProtocolHelper::send_request(server_fd_, SANDBOX_MSG_CREATE, msg))) {
        SERVER_LOG(WARN, "send create request failed", K(ret));
      } else {
        // Send FDs
        int fd_send_ret = 0;
        if (stdin_fd >= 0 || stdout_fd >= 0) {
          int fds_to_send[2];
          int fds_cnt = 0;
          if (stdin_fd >= 0) fds_to_send[fds_cnt++] = stdin_fd;
          if (stdout_fd >= 0) fds_to_send[fds_cnt++] = stdout_fd;

          if (send_fds(fd_pass_sock_, fds_to_send, fds_cnt) < 0) {
            ret = common::OB_ERR_UNEXPECTED;
            SERVER_LOG(ERROR, "send_fds failed, close connection to avoid protocol inconsistency",
                K(errno), K(server_pid_));
            fd_send_ret = -1;
            // send_request succeeded but send_fds failed, the server side may be
            // blocked in recv_fds. The protocol state is now inconsistent.
            // Close the connection so ob_sandbox detects EOF and exits gracefully,
            // then reap the child process. Next call will rebuild ob_sandbox.
            close(server_fd_);
            server_fd_ = -1;
            close(fd_pass_sock_);
            fd_pass_sock_ = -1;
            if (server_pid_ > 0) {
              waitpid(server_pid_, nullptr, 0);
              server_pid_ = -1;
            }
          }
        }

        if (fd_send_ret == 0) {
          SandboxResponse resp;
          if (OB_FAIL(ObSandboxProtocolHelper::recv_response(server_fd_, resp))) {
            SERVER_LOG(WARN, "recv create response failed", K(ret));
          } else {
            if (resp.ret_code_ != 0) {
              ret = common::OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "sandbox process returned error", K(ret), K(resp.ret_code_));
            } else {
              pid = resp.pid_;
              SERVER_LOG(INFO, "create sandbox process success", K(pid), K(stdin_fd), K(stdout_fd));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSandboxClient::try_recycle_sandbox_process()
{
  lib::ObMutexGuard guard(mutex_);
  int ret = common::OB_SUCCESS;
  if (server_pid_ <= 0) {
    // ob_sandbox has been exited
  } else {
    int status;
    pid_t result = waitpid(server_pid_, &status, WNOHANG);
    if (result == 0) {
      // process is still running
    } else if (result == server_pid_) {
      ret = common::OB_ERR_UNEXPECTED;
      char exit_desc[128] = {0};
      bool is_coredump = false;
      format_sandbox_exit_status(status, exit_desc, sizeof(exit_desc), is_coredump);
      SERVER_LOG(ERROR, "ob_sandbox process exited", KR(ret), K(server_pid_), K(exit_desc), K(is_coredump));
      if (server_fd_ != -1) {
        close(server_fd_);
        server_fd_ = -1;
      }
      if (fd_pass_sock_ != -1) {
        close(fd_pass_sock_);
        fd_pass_sock_ = -1;
      }
      server_pid_ = -1;
    } else {
      ret = OB_ERR_SYS;
      SERVER_LOG(WARN, "wait ob_sandbox failed", K(server_pid_), K(status), K(result), K(errno));
    }
  }
  return ret;
}

int ObSandboxClient::destroy_sandbox_process(pid_t pid)
{
  lib::ObMutexGuard guard(mutex_);
  int ret = common::OB_SUCCESS;
  if (pid <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid pid", K(pid));
  } else if (server_pid_ == -1) {
    // ob_sandbox exit abnormally, In most cases, the sandbox process should then exit becuase of PR_SET_PDEATHSIG
    // But OBserver should also check the sandbox is exited
    if (kill(pid, 0) == 0) {
      SERVER_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "ob_sandbox exited abnormally, but the child sandbox process exists, "
        "you need to kill the child sandbox process manually", K(pid), K(server_pid_));
    } else {
      SERVER_LOG(INFO, "ob_sandbox process is not exist, sandbox process might be exited as the parent process is exited", K(pid));
    }
  } else {
    SandboxMsgDestroy msg;
    msg.pid_ = pid;

    if (OB_FAIL(ObSandboxProtocolHelper::send_request(server_fd_, SANDBOX_MSG_DESTROY, msg))) {
      SERVER_LOG(WARN, "send destroy request failed", K(ret));
    } else {
      SandboxResponse resp;
      if (OB_FAIL(ObSandboxProtocolHelper::recv_response(server_fd_, resp))) {
        SERVER_LOG(WARN, "recv destroy response failed", K(ret));
      } else {
        if (resp.ret_code_ != 0) {
          ret = resp.ret_code_;
          SERVER_LOG(WARN, "sandbox destroy returned error", K(ret), K(resp.ret_code_), K(pid), K(resp.status_));
        } else {
          char exit_desc[128] = {0};
          bool is_coredump = false;
          format_sandbox_exit_status(resp.status_, exit_desc, sizeof(exit_desc), is_coredump);
          if (is_coredump) {
            SERVER_LOG(ERROR, "sandbox process exited with coredump", K(pid), K(exit_desc));
          } else {
            SERVER_LOG(INFO, "destroy sandbox process success, process exited", K(pid), K(exit_desc));
          }
        }
      }
    }
  }
  return ret;
}

int ObSandboxClient::check_process_status(pid_t pid)
{
  lib::ObMutexGuard guard(mutex_);
  int ret = common::OB_SUCCESS;
  if (server_pid_ == -1) {
    ret = common::OB_NOT_INIT;
    SERVER_LOG(WARN, "ob_sandbox process is not exist", K(pid));
  } else {
    SandboxMsgCheck msg;
    msg.pid_ = pid;

    if (OB_FAIL(ObSandboxProtocolHelper::send_request(server_fd_, SANDBOX_MSG_CHECK, msg))) {
      SERVER_LOG(WARN, "send check request failed", K(ret));
    } else {
      SandboxResponse resp;
      if (OB_FAIL(ObSandboxProtocolHelper::recv_response(server_fd_, resp))) {
        SERVER_LOG(WARN, "recv check response failed", K(ret));
      } else {
        if (resp.ret_code_ != 0) {
          ret = common::OB_SEARCH_NOT_FOUND;
          SERVER_LOG(TRACE, "sandbox check process not found", K(ret), K(pid), K(resp.ret_code_));
        } else {
          if (resp.is_running_) {
            ret = common::OB_SUCCESS;
          } else {
            char exit_desc[128] = {0};
            bool is_coredump = false;
            format_sandbox_exit_status(resp.status_, exit_desc, sizeof(exit_desc), is_coredump);
            if (is_coredump) {
              SERVER_LOG(ERROR, "sandbox process exited with coredump", K(pid), K(exit_desc));
            } else {
              SERVER_LOG(INFO, "sandbox process exited", K(pid), K(exit_desc));
            }
            ret = common::OB_ENTRY_NOT_EXIST;
          }
        }
      }
    }
  }
  return ret;
}

} // end namespace observer
} // end namespace oceanbase
