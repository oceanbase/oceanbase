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

#ifndef OCEANBASE_OBSERVER_OB_SANDBOX_MANAGER_H_
#define OCEANBASE_OBSERVER_OB_SANDBOX_MANAGER_H_

#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/task/ob_timer.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"
#include "observer/ob_sandbox_client.h"
#include "lib/alloc/ob_memory_placeholder.h"
#include "lib/rc/context.h"
#include <sys/resource.h>

namespace oceanbase {
namespace observer {

enum class SandboxState {
  STATE_UNKNOWN = 0,
  STATE_IDLE,
  STATE_RUNNING,
  STATE_EXITED
};

class ObSandboxManager;

class ObSandboxProcess {
public:
  static constexpr unsigned long MOUNT_RDWR = 0UL;
  static constexpr unsigned long MOUNT_RDONLY = 1UL;

  ObSandboxProcess();
  ObSandboxProcess(const char *path, const char *arg_str);
  ObSandboxProcess(const ObSandboxProcess &other);
  ObSandboxProcess &operator=(const ObSandboxProcess &other);
  virtual ~ObSandboxProcess();


  // Start the sandbox process
  int start(bool redirect_stdin_stdout = true);

  // Destroy the sandbox process
  int destroy();

  // Configuration (must be called before start)
  int set_execute_path(const char *path);
  int set_execute_arg(const char *arg_str);
  int set_root_path(const char *root_path);
  int mount_path(const char *src, const char *dest, unsigned long mountflags = MOUNT_RDONLY);
  void set_pid_limit(int64_t limit) { pid_limit_ = limit; }
  void set_timeout(int64_t timeout_us) { running_timeout_ = timeout_us; }

  // Communication
  int send_msg(const char* buf, size_t size);
  int read_msg(char* buf, size_t size, size_t &ret_size);
  // Read with poll-based timeout protection (timeout_ms < 0 means infinite wait)
  int read_msg_with_timeout(char *buf, size_t size, size_t &ret_size, int timeout_ms);

  // Resource usage
  void update_resource_usage(); // check and update the resource usage of the sandbox process, called by ObSandboxManager::run1

  // Initialize placeholder memory context attached to the current SQL worker's
  // MemoryContext subtree, so that placeholder hold is visible to
  // tree_mem_hold() and can trigger query_memory_limit_percentage checks.
  // Must be called from the SQL execution thread that owns this sandbox.
  int init_placeholder_mem_context();

  // Getters
  pid_t get_pid() const { return pid_; }
  SandboxState get_state() const { return state_; }
  int64_t get_memory_usage() const { return memory_usage_; }
  int64_t get_cpu_time() const { return cpu_time_; }
  double get_cpu_usage() const { return cpu_usage_; }
  int64_t get_cpu_time_delta() const { return cpu_time_ - last_cpu_time_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  char get_process_state() const { return process_state_; }
  const char *get_process_name() const { return process_name_; }
  int64_t get_start_time() const { return start_time_; }
  const common::ObString &get_execute_path() const { return execute_path_; }
  const common::ObString &get_root_path() const { return root_path_; }

  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  bool is_memory_limited() const { return memory_limited_; }

private:
  // Called by ObSandboxManager
  void set_pid(pid_t pid) { pid_ = pid; }
  void set_pipes(int stdin_fd, int stdout_fd) { pipe_fd_stdin_ = stdin_fd; pipe_fd_stdout_ = stdout_fd; }
  void set_state(SandboxState state) { state_ = state; }
  int set_string_(const char *src, common::ObString &dst);
  int set_string_(const common::ObString &src, common::ObString &dst);
  void reset_string_storage_();
  void sync_memory_placeholder_();

private:
  common::ObString execute_path_; // absolute path of the executable file
  common::ObString execute_arg_;
  common::ObString root_path_;
  int64_t running_timeout_; // not used yet

  pid_t pid_;
  uint64_t tenant_id_;
  common::ObAddr addr_; // Reserved, the addr used to communicate with the sandbox process
  SandboxState state_;
  int pipe_fd_stdin_;
  int pipe_fd_stdout_;

  int64_t start_time_;
  int64_t cpu_time_; // Cumulative CPU time
  int64_t last_cpu_time_; // for calculating cpu usage
  int64_t last_check_time_; // Last check time for child process status
  double cpu_usage_; // CPU usage percentage between two checks
  char process_state_; // process state from /proc/<pid>/stat
  int64_t memory_usage_;
  char process_name_[16];

  // Configurations
  struct MountInfo {
    common::ObString src_;
    common::ObString dest_;
    unsigned long flags_;
    TO_STRING_KV(K_(src), K_(dest), K_(flags));
  };
  int deep_copy_mount_infos_(const common::ObIArray<MountInfo> &src_mount_infos);
  common::ObArray<MountInfo> mount_infos_;
  int64_t pid_limit_;
  bool memory_limited_;
  lib::MemoryContext placeholder_mem_ctx_;
  lib::ObMemoryPlaceholder memory_placeholder_;
  common::ObArenaAllocator string_allocator_;
public:
  TO_STRING_KV(K_(execute_path), K_(execute_arg), K_(root_path), K_(running_timeout),
               K_(pid), "state", static_cast<int32_t>(state_),
               K_(pipe_fd_stdin), K_(pipe_fd_stdout),
               K_(start_time), K_(cpu_time), K_(cpu_usage), "process_state", process_state_, K_(memory_usage),
               K_(pid_limit), K_(mount_infos));

  friend class ObSandboxManager;
  friend class ObSandboxClient;
};

// Lightweight snapshot for display only, does not own process or fd resources
struct ObSandboxProcessSnapshot {
  ObSandboxProcessSnapshot();
  int fill_from(const ObSandboxProcess &process);

  pid_t get_pid() const { return pid_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  SandboxState get_state() const { return state_; }
  int64_t get_memory_usage() const { return memory_usage_; }
  int64_t get_cpu_time() const { return cpu_time_; }
  double get_cpu_usage() const { return cpu_usage_; }
  char get_process_state() const { return process_state_; }
  const char *get_process_name() const { return process_name_; }
  int64_t get_start_time() const { return start_time_; }
  common::ObString get_execute_path() const
  {
    return common::ObString(execute_path_len_, execute_path_buf_);
  }
  common::ObString get_root_path() const
  {
    return common::ObString(root_path_len_, root_path_buf_);
  }

  TO_STRING_KV(K_(pid), K_(tenant_id), "state", static_cast<int32_t>(state_),
               K_(memory_usage), K_(cpu_time), K_(cpu_usage),
               "process_state", process_state_, K_(process_name),
               K_(start_time), "execute_path", get_execute_path(),
               "root_path", get_root_path());

private:
  pid_t pid_;
  uint64_t tenant_id_;
  SandboxState state_;
  int64_t memory_usage_;
  int64_t cpu_time_;
  double cpu_usage_;
  char process_state_;
  char process_name_[16];
  int64_t start_time_;
  char execute_path_buf_[common::MAX_PATH_SIZE];
  int32_t execute_path_len_;
  char root_path_buf_[common::MAX_PATH_SIZE];
  int32_t root_path_len_;
};

class ObSandboxManager {
public:
  static const int64_t MONITOR_INTERVAL_US = 1000 * 1000; // 1s
  static ObSandboxManager &get_instance();
  int init();
  void destroy();

  int create_sandbox_process(ObSandboxProcess& process, int child_stdin, int child_stdout);

  int destroy_sandbox_process(ObSandboxProcess &process);

  // Background check task
  void run1();

  // Register/Unregister process (internal use for tracking)
  int register_process(ObSandboxProcess* process);
  void unregister_process(ObSandboxProcess* process);

  int get_sandbox_process_snapshots(common::ObIArray<ObSandboxProcessSnapshot> &snapshots);

private:
  ObSandboxManager();
  ~ObSandboxManager();

  class MonitorTask : public common::ObTimerTask {
  public:
    MonitorTask(ObSandboxManager &mgr) : mgr_(mgr) {}
    virtual void runTimerTask() override { mgr_.run1(); }
  private:
    ObSandboxManager &mgr_;
  };

private:
  common::ObArray<ObSandboxProcess*> sandbox_process_list_;
  common::SpinRWLock rwlock_; // Protects sandbox_process_list_
  ObSandboxClient sandbox_client_;
  common::ObTimer timer_;
  MonitorTask monitor_task_;

  friend class ObSandboxProcess;
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_SANDBOX_MANAGER_H_
