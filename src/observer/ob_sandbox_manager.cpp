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

#define USING_LOG_PREFIX SERVER

#include "observer/ob_sandbox_manager.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/time/ob_time_utility.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>
#include <fstream>
#include <sstream>

namespace oceanbase {
namespace observer {

#define OB_SANDBOX_ROOT_PATH "run/ob_sandbox/"

// ---------------- ObSandboxProcess ----------------

ObSandboxProcess::ObSandboxProcess()
  : execute_path_(), execute_arg_(), root_path_(), running_timeout_(0), pid_(-1), tenant_id_(OB_SERVER_TENANT_ID),
    addr_(), state_(SandboxState::STATE_UNKNOWN),
    pipe_fd_stdin_(-1), pipe_fd_stdout_(-1),
    start_time_(0), cpu_time_(0), last_cpu_time_(0), last_check_time_(0), cpu_usage_(0), process_state_('\0'),
    memory_usage_(0), process_name_(),
    pid_limit_(-1), memory_limited_(false),
    placeholder_mem_ctx_(),
    memory_placeholder_(ObMemAttr(tenant_id_, "SandboxHolder", common::ObCtxIds::DEFAULT_CTX_ID)),
    string_allocator_("SbProcMem")
{
  int ret = set_root_path(OB_SANDBOX_ROOT_PATH);
  if (OB_FAIL(ret)) {
    LOG_WARN("set default root path failed", K(ret));
  }
}

ObSandboxProcess::ObSandboxProcess(const char *path, const char *arg_str)
  : execute_path_(), execute_arg_(), root_path_(), running_timeout_(0), pid_(-1), tenant_id_(OB_SERVER_TENANT_ID),
    addr_(), state_(SandboxState::STATE_UNKNOWN),
    pipe_fd_stdin_(-1), pipe_fd_stdout_(-1),
    start_time_(0), cpu_time_(0), last_cpu_time_(0), last_check_time_(0), cpu_usage_(0), process_state_('\0'),
    memory_usage_(0), process_name_(),
    pid_limit_(-1), memory_limited_(false),
    placeholder_mem_ctx_(),
    memory_placeholder_(ObMemAttr(tenant_id_, "SandboxHolder", common::ObCtxIds::DEFAULT_CTX_ID)),
    string_allocator_("SbProcMem")
{
  int ret = set_root_path(OB_SANDBOX_ROOT_PATH);
  if (OB_FAIL(ret)) {
    LOG_WARN("set default root path failed", K(ret));
  } else if (OB_FAIL(set_execute_path(path))) {
    LOG_WARN("set execute path failed", K(ret));
  } else if (OB_FAIL(set_execute_arg(arg_str))) {
    LOG_WARN("set execute arg failed", K(ret));
  }
}

ObSandboxProcess::ObSandboxProcess(const ObSandboxProcess &other)
  : execute_path_(), execute_arg_(), root_path_(), running_timeout_(0), pid_(-1), tenant_id_(OB_SERVER_TENANT_ID),
    addr_(), state_(SandboxState::STATE_UNKNOWN),
    pipe_fd_stdin_(-1), pipe_fd_stdout_(-1),
    start_time_(0), cpu_time_(0), last_cpu_time_(0), last_check_time_(0), cpu_usage_(0), process_state_('\0'),
    memory_usage_(0), process_name_(),
    pid_limit_(-1), memory_limited_(false),
    placeholder_mem_ctx_(),
    memory_placeholder_(ObMemAttr(tenant_id_, "SandboxHolder", common::ObCtxIds::DEFAULT_CTX_ID)),
    string_allocator_("SbProcMem")
{
  int ret = common::OB_SUCCESS;
  running_timeout_ = other.running_timeout_;
  pid_ = other.pid_;
  tenant_id_ = other.tenant_id_;
  addr_ = other.addr_;
  state_ = other.state_;
  pipe_fd_stdin_ = other.pipe_fd_stdin_;
  pipe_fd_stdout_ = other.pipe_fd_stdout_;
  start_time_ = other.start_time_;
  cpu_time_ = other.cpu_time_;
  last_cpu_time_ = other.last_cpu_time_;
  last_check_time_ = other.last_check_time_;
  cpu_usage_ = other.cpu_usage_;
  process_state_ = other.process_state_;
  memory_usage_ = other.memory_usage_;
  MEMCPY(process_name_, other.process_name_, sizeof(process_name_));
  pid_limit_ = other.pid_limit_;
  if (OB_FAIL(set_string_(other.execute_path_, execute_path_))) {
    LOG_WARN("copy execute path failed", K(ret));
  } else if (OB_FAIL(set_string_(other.execute_arg_, execute_arg_))) {
    LOG_WARN("copy execute arg failed", K(ret));
  } else if (OB_FAIL(set_string_(other.root_path_, root_path_))) {
    LOG_WARN("copy root path failed", K(ret));
  } else if (OB_FAIL(deep_copy_mount_infos_(other.mount_infos_))) {
    LOG_WARN("copy mount infos failed", K(ret));
  }
}

ObSandboxProcess &ObSandboxProcess::operator=(const ObSandboxProcess &other)
{
  if (this != &other) {
    int ret = common::OB_SUCCESS;
    IGNORE_RETURN destroy();
    reset_string_storage_();
    running_timeout_ = other.running_timeout_;
    pid_ = other.pid_;
    tenant_id_ = other.tenant_id_;
    addr_ = other.addr_;
    state_ = other.state_;
    pipe_fd_stdin_ = other.pipe_fd_stdin_;
    pipe_fd_stdout_ = other.pipe_fd_stdout_;
    start_time_ = other.start_time_;
    cpu_time_ = other.cpu_time_;
    last_cpu_time_ = other.last_cpu_time_;
    last_check_time_ = other.last_check_time_;
    cpu_usage_ = other.cpu_usage_;
    process_state_ = other.process_state_;
    memory_usage_ = other.memory_usage_;
    MEMCPY(process_name_, other.process_name_, sizeof(process_name_));
    pid_limit_ = other.pid_limit_;
    if (OB_FAIL(set_string_(other.execute_path_, execute_path_))) {
      LOG_WARN("assign execute path failed", K(ret));
    } else if (OB_FAIL(set_string_(other.execute_arg_, execute_arg_))) {
      LOG_WARN("assign execute arg failed", K(ret));
    } else if (OB_FAIL(set_string_(other.root_path_, root_path_))) {
      LOG_WARN("assign root path failed", K(ret));
    } else if (OB_FAIL(deep_copy_mount_infos_(other.mount_infos_))) {
      LOG_WARN("assign mount infos failed", K(ret));
    }
  }
  return *this;
}
ObSandboxProcess::~ObSandboxProcess()
{
  destroy();
}

int ObSandboxProcess::set_execute_path(const char *path)
{
  return set_string_(path, execute_path_);
}

int ObSandboxProcess::set_execute_arg(const char *arg_str)
{
  return set_string_(arg_str, execute_arg_);
}

int ObSandboxProcess::set_root_path(const char *root_path)
{
  return set_string_(root_path, root_path_);
}

int ObSandboxProcess::start(bool redirect_stdin_stdout /* = true */)
{
  int ret = common::OB_SUCCESS;
  if (state_ == SandboxState::STATE_RUNNING) {
    ret = common::OB_ENTRY_EXIST;
    LOG_WARN("sandbox process already running", K(pid_));
  } else if (!execute_path_.prefix_match("/")) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid execute path, path must be an absolute path", K(execute_path_));
  } else {
    // Ensure manager is initialized
    ObSandboxManager& manager = ObSandboxManager::get_instance();
    // Create pipes for communication
    int pipe_in[2] = {-1, -1};
    int pipe_out[2] = {-1, -1};
    if (redirect_stdin_stdout && (pipe2(pipe_in, O_CLOEXEC) < 0 || pipe2(pipe_out, O_CLOEXEC) < 0)) {
      ret = common::OB_IO_ERROR;
      LOG_WARN("create pipe failed", K(errno));
      if (pipe_in[0] >= 0) {
        close(pipe_in[0]);
        close(pipe_in[1]);
      }
      if (pipe_out[0] >= 0) {
        close(pipe_out[0]);
        close(pipe_out[1]);
      }
    } else {
      int child_stdin = pipe_in[0];
      int parent_write = pipe_in[1];
      int child_stdout = pipe_out[1];
      int parent_read = pipe_out[0];

      if (OB_FAIL(manager.create_sandbox_process(*this, child_stdin, child_stdout))) {
        LOG_WARN("create sandbox process via client failed", K(ret));
        close(pipe_in[0]);
        close(pipe_in[1]);
        close(pipe_out[0]);
        close(pipe_out[1]);
      } else {
        // Close child ends in parent
        close(child_stdin);
        close(child_stdout);

        set_pipes(parent_write, parent_read);
        set_state(SandboxState::STATE_RUNNING);
        start_time_ = common::ObTimeUtility::current_time();
        LOG_INFO("start sandbox process success", K(pid_), K(execute_path_), K(child_stdin), K(child_stdout), K(parent_write), K(parent_read));
      }
    }
  }
  return ret;
}

int ObSandboxProcess::destroy()
{
  int ret = common::OB_SUCCESS;
  if (pid_ > 0) {
    // Call manager to destroy
    ObSandboxManager::get_instance().destroy_sandbox_process(*this);
    pid_ = -1;
    state_ = SandboxState::STATE_EXITED;
  }
  if (pipe_fd_stdin_ >= 0) {
    close(pipe_fd_stdin_);
    pipe_fd_stdin_ = -1;
  }
  if (pipe_fd_stdout_ >= 0) {
    close(pipe_fd_stdout_);
    pipe_fd_stdout_ = -1;
  }
  memory_placeholder_.destroy();
  if (OB_NOT_NULL(placeholder_mem_ctx_.ref_context())) {
    DESTROY_CONTEXT(placeholder_mem_ctx_);
    placeholder_mem_ctx_ = nullptr;
  }
  return ret;
}

int ObSandboxProcess::init_placeholder_mem_context()
{
  int ret = common::OB_SUCCESS;
  if (OB_NOT_NULL(placeholder_mem_ctx_.ref_context())) {
    ret = common::OB_INIT_TWICE;
    LOG_WARN("placeholder memory context already inited", K(ret), K_(tenant_id), K_(pid));
  } else {
    lib::ContextParam param;
    param.set_mem_attr(tenant_id_, "SandboxHolder", common::ObCtxIds::DEFAULT_CTX_ID)
         .set_properties(lib::ALLOC_THREAD_SAFE);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(placeholder_mem_ctx_, param))) {
      LOG_WARN("create placeholder memory context failed", K(ret), K_(tenant_id));
    } else if (OB_ISNULL(placeholder_mem_ctx_.ref_context())) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("placeholder memory context is null after create", K(ret));
    } else {
      // get_malloc_allocator() returns ObIAllocator& but actually constructs ObAllocator /
      // ObParallelAllocator under the hood (see __MemoryContext__::init_alloc).
      memory_placeholder_.set_allocator(
          static_cast<common::ObAllocator *>(&placeholder_mem_ctx_->get_malloc_allocator()));
    }
  }
  return ret;
}

int ObSandboxProcess::mount_path(const char *src, const char *dest, unsigned long flags)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(src) || OB_ISNULL(dest)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    MountInfo info;
    if (OB_FAIL(set_string_(src, info.src_))) {
      LOG_WARN("copy mount source failed", K(ret));
    } else if (OB_FAIL(set_string_(dest, info.dest_))) {
      LOG_WARN("copy mount destination failed", K(ret));
    } else {
      info.flags_ = flags;
    }
    if (OB_SUCC(ret) && OB_FAIL(mount_infos_.push_back(info))) {
      LOG_WARN("push back mount info failed", K(ret));
    }
  }
  return ret;
}

int ObSandboxProcess::set_string_(const char *src, common::ObString &dst)
{
  int ret = common::OB_SUCCESS;
  common::ObString src_str;
  if (OB_ISNULL(src)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    src_str.assign_ptr(const_cast<char *>(src), static_cast<int32_t>(strlen(src)));
    ret = set_string_(src_str, dst);
  }
  return ret;
}

int ObSandboxProcess::set_string_(const common::ObString &src, common::ObString &dst)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(src.ptr()) || src.length() <= 0) {
    dst.reset();
  } else if (OB_FAIL(common::ob_write_string(string_allocator_, src, dst))) {
    LOG_WARN("deep copy string failed", K(ret), K(src));
  }
  return ret;
}

int ObSandboxProcess::deep_copy_mount_infos_(const common::ObIArray<ObSandboxProcess::MountInfo> &src_mount_infos)
{
  int ret = common::OB_SUCCESS;
  mount_infos_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < src_mount_infos.count(); ++i) {
    ObSandboxProcess::MountInfo info;
    info.flags_ = src_mount_infos.at(i).flags_;
    if (OB_FAIL(set_string_(src_mount_infos.at(i).src_, info.src_))) {
      LOG_WARN("copy mount src failed", K(ret), K(i));
    } else if (OB_FAIL(set_string_(src_mount_infos.at(i).dest_, info.dest_))) {
      LOG_WARN("copy mount dest failed", K(ret), K(i));
    } else if (OB_FAIL(mount_infos_.push_back(info))) {
      LOG_WARN("push mount info failed", K(ret), K(i));
    }
  }
  return ret;
}

void ObSandboxProcess::reset_string_storage_()
{
  string_allocator_.reset();
  execute_path_.reset();
  execute_arg_.reset();
  root_path_.reset();
  mount_infos_.reset();
}

int ObSandboxProcess::send_msg(const char* buf, size_t size)
{
  int ret = common::OB_SUCCESS;
  if (pipe_fd_stdin_ < 0) {
    ret = common::OB_NOT_INIT;
  } else {
    ssize_t written = 0;
    while (OB_SUCC(ret) && written < (ssize_t)size) {
      ssize_t n = write(pipe_fd_stdin_, buf + written, size - written);
      if (n < 0) {
        if (errno == EINTR) {
          continue;
        }
        int err = errno;
        if (err == EPIPE || err == EBADF) {
          ret = common::OB_ERR_SYS;
          LOG_WARN("write to sandbox stdin failed, sandbox exited",
                   K(ret), K(err), KPC(this));
        } else {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("write to sandbox stdin failed", K(ret), K(err));
        }
      } else {
        written += n;
      }
    }
  }
  return ret;
}

int ObSandboxProcess::read_msg(char* buf, size_t size, size_t &ret_size)
{
  int ret = common::OB_SUCCESS;
  ret_size = 0;
  if (pipe_fd_stdout_ < 0) {
    ret = common::OB_NOT_INIT;
  } else {
    while (OB_SUCC(ret)) {
      ssize_t n = read(pipe_fd_stdout_, buf, size);
      if (n < 0) {
        if (errno == EINTR) {
          continue;
        } else {
          int err = errno;
          if (err == EPIPE || err == EBADF) {
            ret = common::OB_ERR_SYS;
            LOG_WARN("read from sandbox stdout failed, sandbox exited",
                     K(ret), K(err), KPC(this));
          } else {
            ret = common::OB_ERR_SYS;
            LOG_WARN("read from sandbox stdout failed", K(ret), K(err), KPC(this));
          }
        }
      } else if (n == 0) {
        // EOF: write-end of pipe is fully closed, sandbox is no longer usable
        ret = common::OB_ERR_SYS;
        LOG_WARN("sandbox stdout pipe EOF, sandbox exited", K(ret), KPC(this));
      } else {
        ret_size = n;
      }
      break;
    }
  }
  return ret;
}

int ObSandboxProcess::read_msg_with_timeout(char *buf, size_t size,
                                             size_t &ret_size, int timeout_ms)
{
  int ret = common::OB_SUCCESS;
  ret_size = 0;
  if (pipe_fd_stdout_ < 0) {
    ret = common::OB_NOT_INIT;
  } else {
    struct pollfd pfd;
    pfd.fd     = pipe_fd_stdout_;
    pfd.events = POLLIN;
    // Retry poll on EINTR, adjusting remaining timeout each time
    int remaining_ms = timeout_ms;
    int rc = 0;
    while (true) {
      int64_t start_us = common::ObTimeUtility::current_time();
      rc = poll(&pfd, 1, remaining_ms);
      if (rc >= 0 || errno != EINTR) {
        break;
      }
      // EINTR: adjust remaining timeout and retry
      if (remaining_ms > 0) {
        int64_t elapsed_ms = (common::ObTimeUtility::current_time() - start_us) / 1000;
        remaining_ms -= static_cast<int>(elapsed_ms);
        if (remaining_ms <= 0) {
          rc = 0; // treat as timeout
          break;
        }
      }
      // remaining_ms < 0 means infinite wait, just retry
    }
    if (rc == 0) {
      ret = common::OB_TIMEOUT;
    } else if (rc < 0) {
      ret = common::OB_IO_ERROR;
      int err = errno;
      LOG_WARN("poll sandbox stdout failed", K(ret), K(err), KPC(this));
    } else {
      ret = read_msg(buf, size, ret_size);
    }
  }
  return ret;
}

void ObSandboxProcess::update_resource_usage()
{
  if (pid_ > 0) {
    char path[64];
    const int64_t now = common::ObTimeUtility::current_time();
    snprintf(path, sizeof(path), "/proc/%d/stat", pid_);
    int fd = open(path, O_RDONLY);
    if (fd >= 0) {
      char buf[1024];
      ssize_t len = read(fd, buf, sizeof(buf) - 1);
      close(fd);

      if (len > 0) {
        buf[len] = '\0';
        // Find the last ')' to skip filename
        char *p = strrchr(buf, ')');
        if (p) {
          char *name_begin = strchr(buf, '(');
          if (name_begin != nullptr && name_begin < p) {
            (void)snprintf(process_name_, sizeof(process_name_), "%.*s",
                           static_cast<int>(p - name_begin - 1), name_begin + 1);
          } else {
            process_name_[0] = '\0';
          }
          p += 2; // skip ") "
          // Scan fields after filename
          // state ppid pgrp session tty_nr tpgid flags minflt cminflt majflt cmajflt utime stime cutime cstime
          //  3     4    5     6      7      8     9     10     11      12     13     14    15    16     17
          // priority nice num_threads itrealvalue starttime vsize rss
          //   18      19      20          21          22      23   24

          char state;
          int ppid, pgrp, session, tty_nr, tpgid;
          unsigned int flags;
          unsigned long minflt, cminflt, majflt, cmajflt, utime, stime;
          long cutime, cstime, priority, nice, num_threads, itrealvalue;
          unsigned long long starttime;
          unsigned long vsize;
          long rss;

          int fields = sscanf(p, "%c %d %d %d %d %d %u %lu %lu %lu %lu %lu %lu %ld %ld %ld %ld %ld %ld %llu %lu %ld",
            &state, &ppid, &pgrp, &session, &tty_nr, &tpgid, &flags,
            &minflt, &cminflt, &majflt, &cmajflt, &utime, &stime, &cutime, &cstime,
            &priority, &nice, &num_threads, &itrealvalue, &starttime, &vsize, &rss);
          if (fields >= 1) {
            process_state_ = state;
          } else {
            process_state_ = '\0';
          }

          if (fields >= 22) { // up to rss
            static const long ticks_per_sec = sysconf(_SC_CLK_TCK);
            static const long page_size = sysconf(_SC_PAGESIZE);

            if (ticks_per_sec > 0) {
              last_cpu_time_ = cpu_time_;
              cpu_time_ = (utime + stime) * 1000000 / ticks_per_sec; // us
              if (last_check_time_ > 0 && now > last_check_time_) {
                const int64_t cpu_time_delta = cpu_time_ - last_cpu_time_;
                const int64_t wall_time_delta_us = now - last_check_time_;
                if (cpu_time_delta >= 0 && wall_time_delta_us > 0) {
                  cpu_usage_ = static_cast<double>(cpu_time_delta) * 100.0 / static_cast<double>(wall_time_delta_us);
                }
              }
              last_check_time_ = now;
            }
            if (page_size > 0) {
              memory_usage_ = rss * page_size; // bytes
            }
          }
        }
      }
    }
  }
}

void ObSandboxProcess::sync_memory_placeholder_()
{
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(memory_placeholder_.memory_placeholder_sync(memory_usage_))) {
    if (!memory_limited_) {
      LOG_WARN_RET(tmp_ret, "fail to sync memory placeholder, limit child process memory",
                   K_(tenant_id), K_(memory_usage), K_(pid));
      memory_limited_ = true;
    }
  } else if (memory_limited_) {
    LOG_INFO("discard memory limit, restore process memory limit", K_(tenant_id), K_(memory_usage), K_(pid));
    memory_limited_ = false;
  }
}

// ---------------- ObSandboxManager ----------------

ObSandboxManager::ObSandboxManager()
  : rwlock_(common::ObLatchIds::SANDBOX_LOCK),
    sandbox_client_(), monitor_task_(*this)
{
}

ObSandboxManager::~ObSandboxManager()
{
  destroy();
}

ObSandboxManager &ObSandboxManager::get_instance()
{
  static ObSandboxManager instance;
  return instance;
}

int ObSandboxManager::init()
{
  int ret = common::OB_SUCCESS;
  common::SpinWLockGuard guard(rwlock_);
  if (!timer_.inited()) {
    if (OB_FAIL(timer_.init("SandboxMgr"))) {
      LOG_WARN("init sandbox manager timer failed", K(ret));
    } else if (OB_FAIL(timer_.schedule(monitor_task_, MONITOR_INTERVAL_US, true))) {
      timer_.destroy();
      LOG_WARN("schedule sandbox manager monitor task failed", K(ret));
    }
  }
  return ret;
}

void ObSandboxManager::destroy()
{
  timer_.destroy();
  common::SpinWLockGuard guard(rwlock_);
  for (int64_t i = 0; i < sandbox_process_list_.count(); ++i) {
    ObSandboxProcess *p = sandbox_process_list_.at(i);
    if (p && p->get_pid() > 0) {
      int tmp_ret = common::OB_SUCCESS;
      if (OB_TMP_FAIL(sandbox_client_.destroy_sandbox_process(p->get_pid()))) {
        LOG_ERROR_RET(tmp_ret, "destroy sandbox process failed", K(tmp_ret), K(p->get_pid()));
      } else {
        p->set_pid(-1);
        p->set_state(SandboxState::STATE_EXITED);
      }
    }
  }
  sandbox_process_list_.reset();
  sandbox_client_.destroy();
}

int ObSandboxManager::create_sandbox_process(ObSandboxProcess& process, int child_stdin, int child_stdout)
{
  int ret = common::OB_SUCCESS;
  pid_t pid = -1;
  const int64_t timeout_us = 5 * 1000 * 1000; // 5s

  if (OB_FAIL(sandbox_client_.create_sandbox_process(process, timeout_us, pid, child_stdin, child_stdout))) {
    LOG_WARN("create sandbox process failed", K(ret));
  } else {
    process.set_pid(pid);
    if (OB_FAIL(register_process(&process))) {
      LOG_WARN("register process failed", K(ret), K(pid));
      // ob_sandbox exited abnormally, ignore return value
      IGNORE_RETURN sandbox_client_.destroy_sandbox_process(pid);
      process.set_pid(-1);
    }
  }

  return ret;
}

int ObSandboxManager::destroy_sandbox_process(ObSandboxProcess &process)
{
  int ret = common::OB_SUCCESS;
  pid_t pid = process.get_pid();
  if (pid > 0) {
    unregister_process(&process);
    // ob_sandbox exited abnormally, ignore return value
    IGNORE_RETURN sandbox_client_.destroy_sandbox_process(pid);
  }
  return ret;
}

int ObSandboxManager::register_process(ObSandboxProcess* process)
{
  int ret = common::OB_SUCCESS;
  common::SpinWLockGuard guard(rwlock_);
  if (!timer_.inited()) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("sandbox manager is not initialized", K(ret));
  } else if (OB_SUCCESS == ret) {
    ret = sandbox_process_list_.push_back(process);
  }
  return ret;
}

void ObSandboxManager::unregister_process(ObSandboxProcess* process)
{
  common::SpinWLockGuard guard(rwlock_);
  int ret = common::OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; i < sandbox_process_list_.count(); ++i) {
    if (sandbox_process_list_.at(i) == process) {
        sandbox_process_list_.remove(i);
        ret = common::OB_SUCCESS;
        break;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_ERROR("unregister process failed", K(ret), K(process));
  }
  // Release the array's internal buffer when empty to avoid being flagged
  // by the SQL memory leak checker (the buffer was allocated during SQL
  // execution but persists in this global singleton).
  if (sandbox_process_list_.empty()) {
    sandbox_process_list_.reset();
  }
}

ObSandboxProcessSnapshot::ObSandboxProcessSnapshot()
  : pid_(-1),
    tenant_id_(common::OB_INVALID_TENANT_ID),
    state_(SandboxState::STATE_UNKNOWN),
    memory_usage_(0),
    cpu_time_(0),
    cpu_usage_(0),
    process_state_('\0'),
    process_name_(),
    start_time_(0),
    execute_path_buf_(),
    execute_path_len_(0),
    root_path_buf_(),
    root_path_len_(0)
{
  execute_path_buf_[0] = '\0';
  root_path_buf_[0] = '\0';
}

int ObSandboxProcessSnapshot::fill_from(const ObSandboxProcess &process)
{
  int ret = common::OB_SUCCESS;
  pid_ = process.get_pid();
  tenant_id_ = process.get_tenant_id();
  state_ = process.get_state();
  memory_usage_ = process.get_memory_usage();
  cpu_time_ = process.get_cpu_time();
  cpu_usage_ = process.get_cpu_usage();
  process_state_ = process.get_process_state();
  const char *proc_name = process.get_process_name();
  if (OB_NOT_NULL(proc_name)) {
    int64_t name_len = MIN(static_cast<int64_t>(STRLEN(proc_name)),
                           static_cast<int64_t>(sizeof(process_name_) - 1));
    MEMCPY(process_name_, proc_name, name_len);
    process_name_[name_len] = '\0';
  } else {
    process_name_[0] = '\0';
  }
  start_time_ = process.get_start_time();

  const common::ObString &src_execute_path = process.get_execute_path();
  const common::ObString &src_root_path = process.get_root_path();

  if (src_execute_path.length() > 0 && src_execute_path.ptr() != nullptr) {
    int64_t copy_len = MIN(static_cast<int64_t>(src_execute_path.length()),
                          static_cast<int64_t>(common::MAX_PATH_SIZE - 1));
    MEMCPY(execute_path_buf_, src_execute_path.ptr(), copy_len);
    execute_path_buf_[copy_len] = '\0';
    execute_path_len_ = static_cast<int32_t>(copy_len);
  } else {
    execute_path_buf_[0] = '\0';
    execute_path_len_ = 0;
  }

  if (src_root_path.length() > 0 && src_root_path.ptr() != nullptr) {
    int64_t copy_len = MIN(static_cast<int64_t>(src_root_path.length()),
                          static_cast<int64_t>(common::MAX_PATH_SIZE - 1));
    MEMCPY(root_path_buf_, src_root_path.ptr(), copy_len);
    root_path_buf_[copy_len] = '\0';
    root_path_len_ = static_cast<int32_t>(copy_len);
  } else {
    root_path_buf_[0] = '\0';
    root_path_len_ = 0;
  }

  return ret;
}

int ObSandboxManager::get_sandbox_process_snapshots(common::ObIArray<ObSandboxProcessSnapshot> &snapshots)
{
  int ret = common::OB_SUCCESS;
  snapshots.reset();
  common::SpinRLockGuard guard(rwlock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < sandbox_process_list_.count(); ++i) {
    ObSandboxProcess *p = sandbox_process_list_.at(i);
    if (nullptr != p) {
      ObSandboxProcessSnapshot snapshot;
      if (OB_FAIL(snapshots.push_back(snapshot))) {
        LOG_WARN("push sandbox process snapshot failed", K(ret), K(i), KPC(p));
      } else if (OB_FAIL(snapshots.at(snapshots.count() - 1).fill_from(*p))) {
        LOG_WARN("fill sandbox process snapshot failed", K(ret), K(i), KPC(p));
      }
    }
  }
  return ret;
}

void ObSandboxManager::run1()
{
  // Periodically check the status of the ob_sandbox process
  sandbox_client_.try_recycle_sandbox_process();
  bool dump_process_info = false;
  if (REACH_TIME_INTERVAL(20 * 1000 * 1000)) { // 20s
    dump_process_info = true;
  }
  common::SpinRLockGuard guard(rwlock_);
  for (int64_t i = 0; i < sandbox_process_list_.count(); ++i) {
    ObSandboxProcess *p = sandbox_process_list_.at(i);
    if (p && p->get_pid() > 0 && p->get_state() == SandboxState::STATE_RUNNING) {
      // Check status
      int ret = sandbox_client_.check_process_status(p->get_pid());
      if (ret != common::OB_SUCCESS) {
        // Process exited
        p->set_state(SandboxState::STATE_EXITED);
        LOG_INFO("sandbox process exited", "pid", p->get_pid());
        // NOTICE: We don't remove it here automatically, let the owner decide when to destroy/cleanup
      } else {
        p->update_resource_usage();
        p->sync_memory_placeholder_();

        // CPU limit / statistics
        const uint64_t tenant_id = p->get_tenant_id();
        if (OB_NOT_NULL(GCTX.cgroup_ctrl_) && GCTX.cgroup_ctrl_->is_valid()) {
          // Scenario A: cgroup enabled, add sandbox process to tenant cgroup via cgroup.procs
          int tmp_ret = GCTX.cgroup_ctrl_->add_process_to_cgroup(p->get_pid(), tenant_id);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("fail to add sandbox process to cgroup", "pid", p->get_pid(), K(tenant_id), K(tmp_ret));
          }
        } else {
          // Scenario B: cgroup not enabled, accumulate cpu time to tenant
          omt::ObTenant *tenant = NULL;
          if (OB_NOT_NULL(GCTX.omt_)) {
            int tmp_ret = GCTX.omt_->get_tenant(tenant_id, tenant);
            if (OB_SUCCESS == tmp_ret && OB_NOT_NULL(tenant)) {
              tenant->add_cpu_time(p->get_cpu_time_delta());
            } else {
              LOG_WARN("fail to get tenant", K(tmp_ret), K(tenant_id));
            }
          }
        }

        // TODO: Check timeout

        if (dump_process_info) {
          LOG_INFO("dump ob_sandbox process info", K(i), KPC(p));
        }
      }
    }
  }
}

} // namespace observer
} // namespace oceanbase
