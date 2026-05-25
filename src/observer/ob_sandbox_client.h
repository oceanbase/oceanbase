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

#ifndef OCEANBASE_OBSERVER_OB_SANDBOX_CLIENT_H_
#define OCEANBASE_OBSERVER_OB_SANDBOX_CLIENT_H_

#include "lib/net/ob_addr.h"
#include "lib/lock/ob_mutex.h"
#include <sys/types.h>

namespace oceanbase
{
namespace observer
{
class ObSandboxProcess;
class ObSandboxClient {
public:
  ObSandboxClient();
  ~ObSandboxClient();
  // Create a new sandbox client process (bin/ob_sandbox) if it's not running

  // OBServer主动退出时，先回收obsandbox子进程
  void destroy();

  // send request to server_addr_ to create sandbox child process, return the pid of the child process

  int create_ob_sandbox();

  int try_recycle_sandbox_process();

  int create_sandbox_process(const ObSandboxProcess& process,
                            const int64_t timeout_us,
                            pid_t &pid,
                            const int stdin_fd = -1,
                            const int stdout_fd = -1);
  int destroy_sandbox_process(pid_t pid);
  int check_process_status(pid_t pid);

private:
  int create_ob_sandbox_();

  int create_sandbox_process_(const ObSandboxProcess& process,
                              const int64_t timeout_us,
                              pid_t &pid,
                              int stdin_fd,
                              int stdout_fd);

private:
  pid_t server_pid_; // the pid of ob_sandbox process
  common::ObAddr server_addr_; // Not used in socketpair mode
  int server_fd_; // used to communicate with ob_sandbox process
  int fd_pass_sock_; // used to pass fd between observer and ob_sandbox
  lib::ObMutex mutex_; // protect server_pid_, server_fd_, fd_pass_sock_
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_SANDBOX_CLIENT_H_
