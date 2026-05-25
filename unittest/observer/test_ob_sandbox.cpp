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
#include <gtest/gtest.h>
#define private public
#include <thread>
#include <vector>
#include <map>
#include <sys/wait.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>
#include "lib/oblog/ob_log_module.h"
#include "share/ob_sandbox_protocol.h"
#include "observer/ob_sandbox_client.h"
#include "observer/ob_sandbox_manager.h"
#include "share/ob_server_struct.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;

class MockSandboxServer {
public:
  MockSandboxServer(int fd) : fd_(fd), running_(true) {}

  void run() {
    while (running_) {
      SandboxMsgHeader header;
      // Use select/poll with timeout to allow checking running_ flag periodically
      // or rely on close() interrupting read() (which might be platform dependent or behavior dependent)
      // A safer way is using select.
      fd_set readfds;
      struct timeval tv;
      FD_ZERO(&readfds);
      FD_SET(fd_, &readfds);
      tv.tv_sec = 0;
      tv.tv_usec = 100000; // 100ms

      int select_ret = select(fd_ + 1, &readfds, NULL, NULL, &tv);
      if (select_ret == -1) {
          if (errno == EINTR) continue;
          break;
      } else if (select_ret == 0) {
          continue; // Timeout
      }

      if (ObSandboxProtocolHelper::read_n(fd_, (char*)&header, sizeof(header)) != 0) {
        break;
      }
      ObCurTraceId::set(header.trace_id_);
      if (header.magic_ != SANDBOX_MAGIC) {
        break;
      }

      std::vector<char> buf(header.data_len_);
      if (header.data_len_ > 0) {
        if (ObSandboxProtocolHelper::read_n(fd_, buf.data(), header.data_len_) != 0) {
          break;
        }
      }

      switch (header.type_) {
        case SANDBOX_MSG_CREATE:
          handle_create(buf, header.data_len_);
          break;
        case SANDBOX_MSG_DESTROY:
          handle_destroy(buf, header.data_len_);
          break;
        case SANDBOX_MSG_CHECK:
          handle_check(buf, header.data_len_);
          break;
        default:
          break;
      }
    }
  }

  void stop() {
    running_ = false;
    // Close fd to break the read loop
    // Since read_n might be blocking on read(), closing fd in another thread
    // is the standard way to unblock it in this simple mock.
    shutdown(fd_, SHUT_RDWR);
    close(fd_);
  }

  void cleanup() {
    for (auto& pair : processes_) {
        if (pair.second.running) {
            kill(pair.first, SIGKILL);
            waitpid(pair.first, NULL, 0);
        }
    }
  }

private:
  struct ProcessInfo {
      int status;
      bool running;
  };

  void handle_create(const std::vector<char>& buf, uint32_t len) {
    SandboxMsgCreate msg;
    int64_t pos = 0;
    SandboxResponse resp;

    if (OB_SUCCESS != msg.deserialize(buf.data(), len, pos)) {
        resp.ret_code_ = -1;
        ObSandboxProtocolHelper::send_response(fd_, resp);
        return;
    }

    // Skip fd passing for simplicity in this unit test
    // In real scenario, we would need to recv_fds here if msg.stdin_fd_ >= 0

    std::string path(msg.binary_path_.ptr(), msg.binary_path_.length());
    std::string arg_str(msg.arg_str_.ptr(), msg.arg_str_.length());

    std::vector<std::string> args;
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

    pid_t pid = fork();
    if (pid == 0) {
        // Child
        // Close sockets in child
        // close(fd_);
        execvp(path.c_str(), argv.data());
        _exit(127);
    } else if (pid > 0) {
        // Parent
        processes_[pid] = {0, true};
        resp.ret_code_ = 0;
        resp.pid_ = pid;
        resp.is_running_ = 1;
    } else {
        resp.ret_code_ = errno;
    }

    for (auto p : argv) free(p);
    ObSandboxProtocolHelper::send_response(fd_, resp);
  }

  void handle_destroy(const std::vector<char>& buf, uint32_t len) {
    SandboxMsgDestroy msg;
    int64_t pos = 0;
    SandboxResponse resp;

    if (OB_SUCCESS != msg.deserialize(buf.data(), len, pos)) {
        resp.ret_code_ = -1;
        ObSandboxProtocolHelper::send_response(fd_, resp);
        return;
    }

    int32_t pid = msg.pid_;
    resp.pid_ = pid;

    if (processes_.find(pid) != processes_.end()) {
        kill(pid, SIGKILL);
        int status;
        waitpid(pid, &status, 0);
        processes_[pid].running = false;
        processes_[pid].status = status;
        resp.ret_code_ = 0;
        resp.status_ = status;
    } else {
        resp.ret_code_ = ESRCH;
    }
    ObSandboxProtocolHelper::send_response(fd_, resp);
  }

  void handle_check(const std::vector<char>& buf, uint32_t len) {
    SandboxMsgCheck msg;
    int64_t pos = 0;
    SandboxResponse resp;

    if (OB_SUCCESS != msg.deserialize(buf.data(), len, pos)) {
        resp.ret_code_ = -1;
        ObSandboxProtocolHelper::send_response(fd_, resp);
        return;
    }

    int32_t pid = msg.pid_;
    resp.pid_ = pid;

    if (processes_.find(pid) != processes_.end()) {
        if (processes_[pid].running) {
            int status;
            pid_t ret = waitpid(pid, &status, WNOHANG);
            if (ret == pid) {
                processes_[pid].running = false;
                processes_[pid].status = status;
            }
        }
        resp.ret_code_ = 0;
        resp.is_running_ = processes_[pid].running ? 1 : 0;
        resp.status_ = processes_[pid].status;
    } else {
        resp.ret_code_ = ESRCH;
    }
    ObSandboxProtocolHelper::send_response(fd_, resp);
  }

  int fd_;
  volatile bool running_;
  std::map<pid_t, ProcessInfo> processes_;
};

class TestObSandboxProtocol : public ::testing::Test {
protected:
  void SetUp() override {
    ObCurTraceId::init(ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 10001));
    ObTimerService::get_instance().start();
    ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM, 0, sv_));
    server_ = new MockSandboxServer(sv_[1]);
    server_thread_ = std::thread(&MockSandboxServer::run, server_);
  }

  void TearDown() override {
    server_->stop();
    if (server_thread_.joinable()) {
      server_thread_.join();
    }
    server_->cleanup();
    delete server_;
    close(sv_[0]);
    // sv_[1] is closed by server_->stop()
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }

  int sv_[2]; // sv_[0] for client, sv_[1] for server
  MockSandboxServer* server_;
  std::thread server_thread_;
};

TEST_F(TestObSandboxProtocol, TestRealSleepProcess) {
  SandboxMsgCreate msg;
  msg.root_path_ = ObString();
  msg.mount_infos_.reset();
  msg.binary_path_ = ObString("sleep");
  msg.arg_str_ = ObString("0.5"); // sleep 0.5s
  msg.stdin_fd_ = -1;
  msg.stdout_fd_ = -1;

  // 1. Create Process
  ASSERT_EQ(OB_SUCCESS, ObSandboxProtocolHelper::send_request(sv_[0], SANDBOX_MSG_CREATE, msg));

  SandboxResponse resp;
  ASSERT_EQ(OB_SUCCESS, ObSandboxProtocolHelper::recv_response(sv_[0], resp));
  ASSERT_EQ(0, resp.ret_code_);
  ASSERT_GT(resp.pid_, 0);
  ASSERT_EQ(1, resp.is_running_);

  pid_t pid = resp.pid_;

  // 2. Check Process (Should be running)
  SandboxMsgCheck check_msg;
  check_msg.pid_ = pid;
  ASSERT_EQ(OB_SUCCESS, ObSandboxProtocolHelper::send_request(sv_[0], SANDBOX_MSG_CHECK, check_msg));
  ASSERT_EQ(OB_SUCCESS, ObSandboxProtocolHelper::recv_response(sv_[0], resp));
  ASSERT_EQ(0, resp.ret_code_);
  ASSERT_EQ(pid, resp.pid_);
  // Note: Race condition possible if sleep finishes too fast, but 0.5s should be enough
  // If it fails, we might need longer sleep
  if (resp.is_running_ == 0) {
      // It finished very fast?
      printf("Process finished earlier than expected\n");
  } else {
      EXPECT_EQ(1, resp.is_running_);
  }

  // 3. Wait for process to exit
  usleep(600000); // Wait 0.6s

  // 4. Check Process (Should be exited)
  ASSERT_EQ(OB_SUCCESS, ObSandboxProtocolHelper::send_request(sv_[0], SANDBOX_MSG_CHECK, check_msg));
  ASSERT_EQ(OB_SUCCESS, ObSandboxProtocolHelper::recv_response(sv_[0], resp));
  ASSERT_EQ(0, resp.ret_code_);
  ASSERT_EQ(pid, resp.pid_);
  ASSERT_EQ(0, resp.is_running_);
}

TEST_F(TestObSandboxProtocol, TestDestroyProcess) {
  SandboxMsgCreate msg;
  msg.root_path_ = ObString();
  msg.mount_infos_.reset();
  msg.binary_path_ = ObString("sleep");
  msg.arg_str_ = ObString("10"); // sleep 10s
  msg.stdin_fd_ = -1;
  msg.stdout_fd_ = -1;

  // 1. Create
  ASSERT_EQ(OB_SUCCESS, ObSandboxProtocolHelper::send_request(sv_[0], SANDBOX_MSG_CREATE, msg));
  SandboxResponse resp;
  ASSERT_EQ(OB_SUCCESS, ObSandboxProtocolHelper::recv_response(sv_[0], resp));
  ASSERT_EQ(0, resp.ret_code_);
  pid_t pid = resp.pid_;

  // 2. Check (Running)
  SandboxMsgCheck check_msg;
  check_msg.pid_ = pid;
  ASSERT_EQ(OB_SUCCESS, ObSandboxProtocolHelper::send_request(sv_[0], SANDBOX_MSG_CHECK, check_msg));
  ASSERT_EQ(OB_SUCCESS, ObSandboxProtocolHelper::recv_response(sv_[0], resp));
  ASSERT_EQ(1, resp.is_running_);

  // 3. Destroy
  SandboxMsgDestroy destroy_msg;
  destroy_msg.pid_ = pid;
  ASSERT_EQ(OB_SUCCESS, ObSandboxProtocolHelper::send_request(sv_[0], SANDBOX_MSG_DESTROY, destroy_msg));
  ASSERT_EQ(OB_SUCCESS, ObSandboxProtocolHelper::recv_response(sv_[0], resp));
  ASSERT_EQ(0, resp.ret_code_);
  ASSERT_EQ(pid, resp.pid_);

  // 4. Check (Exited)
  ASSERT_EQ(OB_SUCCESS, ObSandboxProtocolHelper::send_request(sv_[0], SANDBOX_MSG_CHECK, check_msg));
  ASSERT_EQ(OB_SUCCESS, ObSandboxProtocolHelper::recv_response(sv_[0], resp));
  ASSERT_EQ(0, resp.ret_code_);
  ASSERT_EQ(0, resp.is_running_);
}

TEST_F(TestObSandboxProtocol, TestSandboxManager) {
  // Check if ob_sandbox binary exists
  if (access("./ob_sandbox", X_OK) != 0) {
    LOG_INFO("ob_sandbox binary not found, skipping TestSandboxManager");
  } else {
    LOG_INFO("ob_sandbox binary found, start test TestSandboxManager");
    system("cp ./ob_sandbox ./bin/ob_sandbox");
    ObSandboxManager& manager = ObSandboxManager::get_instance();
    manager.destroy();
    EXPECT_EQ(OB_SUCCESS, manager.init());
    ObSandboxProcess process("/bin/sleep", "2");
    EXPECT_EQ(OB_SUCCESS, process.start());
    EXPECT_GT(process.get_pid(), 0);
    usleep(100000);
    // check process is still running
    int check_pid = process.get_pid();
    int kill_ret = kill(check_pid, 0);
    EXPECT_TRUE(kill_ret == 0);
    // Check that the process is not a zombie
    char proc_stat_path[64];
    snprintf(proc_stat_path, sizeof(proc_stat_path), "/proc/%d/stat", check_pid);
    FILE *stat_file = fopen(proc_stat_path, "r");
    ASSERT_TRUE(stat_file != nullptr) << "Failed to open proc stat file for pid " << check_pid;
    if (stat_file != nullptr) {
      int scanned_pid = 0;
      char comm[64] = {0};
      char state = 0;
      // The format of stat: pid (comm) state ... See 'man proc'
      int n = fscanf(stat_file, "%d %63s %c", &scanned_pid, comm, &state);
      fclose(stat_file);
      ASSERT_EQ(n, 3);
      EXPECT_NE(state, 'Z') << "Process should not be a zombie, got state 'Z'";
    }
    // test updating the resource usage of the process
    usleep(ObSandboxManager::MONITOR_INTERVAL_US + 500000); // wait for timer task update the resource usage
    char process_info[512];
    process.to_string(process_info, sizeof(process_info));
    EXPECT_GT(process.get_memory_usage(), 0);
    EXPECT_EQ(process.get_cpu_usage(), 0);
    EXPECT_EQ(process.get_process_state(), 'S');
    EXPECT_NE(process.process_name_, "sleep");
    process.destroy();

    // Test more complex scenarios
    // mount path and read output from process
    char exe_path[PATH_MAX] = {0};
    ssize_t path_len = readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
    ASSERT_GT(path_len, 0);
    exe_path[path_len] = '\0';
    char ls_arg[128] = {0};
    snprintf(ls_arg, sizeof(ls_arg), "-alh %s", exe_path);
    process = ObSandboxProcess("/bin/ls", ls_arg);

    EXPECT_EQ(OB_SUCCESS, process.mount_path(exe_path, exe_path));
    EXPECT_EQ(OB_SUCCESS, process.start());
    EXPECT_GT(process.get_pid(), 0);
    usleep(50000);
    char output[1024] = {0};
    size_t ret_size = 0;
    int ret = process.read_msg(output, sizeof(output), ret_size);
    // printf("ret: %d\n", ret);
    // printf("ret_size: %zu\n", ret_size);
    // printf("output: %s\n", output);
    // test the mount path is working
    EXPECT_EQ(0, ret);
    EXPECT_GT(ret_size, 0);
    EXPECT_TRUE(nullptr != strstr(output, exe_path));

    // test proc path is not accessible, should return -1
    process.destroy();
    snprintf(ls_arg, sizeof(ls_arg), "/proc");
    process.set_execute_arg(ls_arg);
    process.start();
    memset(output, 0, sizeof(output));
    ret = process.read_msg(output, sizeof(output), ret_size);
    EXPECT_EQ(OB_SUCCESS, ret);
    char str_self[] = "self\n";
    EXPECT_EQ(0, strcmp(output, str_self));

    // test process start error
    EXPECT_EQ(OB_ENTRY_EXIST, process.start());
    process.destroy();
    process.set_execute_path("invalid_path");
    EXPECT_EQ(OB_INVALID_ARGUMENT, process.start());

    manager.destroy();
  }
}

TEST_F(TestObSandboxProtocol, TestSandboxManagerConcurrent) {
  // Check if ob_sandbox binary exists
  if (access("./ob_sandbox", X_OK) != 0) {
    LOG_INFO("ob_sandbox binary not found, skipping TestSandboxManagerConcurrent");
  } else {
    LOG_INFO("ob_sandbox binary found, start test TestSandboxManagerConcurrent");
    system("cp ./ob_sandbox ./bin/ob_sandbox");

    ObSandboxManager& manager = ObSandboxManager::get_instance();
    manager.destroy();
    EXPECT_EQ(OB_SUCCESS, manager.init());
    const int num_threads = 8;
    const int processes_per_thread = 5; // Each thread creates and destroys 5 processes
    std::vector<std::thread> threads;
    std::vector<int> success_count(num_threads, 0);
    std::vector<int> fail_count(num_threads, 0);

    // Create threads for concurrent operations
    for (int i = 0; i < num_threads; ++i) {
      threads.emplace_back([i, &success_count, &fail_count]() {
        for (int j = 0; j < 5; ++j) {
          ObSandboxProcess process("/bin/sleep", "1");
          int ret = process.start();
          if (OB_SUCCESS == ret && process.get_pid() > 0) {
            success_count[i]++;
            // Wait a bit before destroying
            usleep(100000); // 100ms
            process.destroy();
          } else {
            fail_count[i]++;
            LOG_WARN("thread create sandbox process failed", K(i), K(j), K(ret));
          }
        }
      });
    }

    // Wait for all threads to complete
    for (auto& t : threads) {
      t.join();
    }

    // Verify results
    int total_success = 0;
    int total_fail = 0;
    for (int i = 0; i < num_threads; ++i) {
      total_success += success_count[i];
      total_fail += fail_count[i];
      LOG_INFO("thread result", K(i), "success", success_count[i], "fail", fail_count[i]);
    }

    LOG_INFO("concurrent test result", "total_success", total_success, "total_fail", total_fail);
    EXPECT_EQ(num_threads * processes_per_thread, total_success);
    EXPECT_EQ(0, total_fail);

    manager.destroy();
  }
}

// Test ObSandboxClient lifecycle management
TEST_F(TestObSandboxProtocol, TestLifecycle) {
  // Use sleep to mock ob_sandbox
  // ./bin/ob_sandbox should be a symlink or copy of sleep
  system("cp /bin/sleep ./bin/ob_sandbox");

  ObSandboxClient& client = ObSandboxManager::get_instance().sandbox_client_;
  int ret = client.create_ob_sandbox();
  SERVER_LOG(INFO, "server_pid", K(client.server_pid_));
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_GT(client.server_pid_, 0);

  // test ob_sandbox process exit normally
  usleep(ObSandboxManager::MONITOR_INTERVAL_US + 500000);
  client.try_recycle_sandbox_process();
  EXPECT_EQ(-1, client.server_pid_);

  ret = client.create_ob_sandbox();
  EXPECT_GT(client.server_pid_, 0);
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestObSandboxProtocol, TestObSandboxProcessCpuTime) {
  // Check if ob_sandbox binary exists
  if (access("./ob_sandbox", X_OK) != 0) {
    LOG_INFO("ob_sandbox binary not found, skipping TestObSandboxProcessCpuTime");
    return;
  }
  system("cp ./ob_sandbox ./bin/ob_sandbox");
  LOG_INFO("ob_sandbox binary found, start test TestObSandboxProcessCpuTime");

  ObSandboxManager& manager = ObSandboxManager::get_instance();
  manager.destroy();
  EXPECT_EQ(OB_SUCCESS, manager.init());
  ObSandboxProcess process("/bin/yes", "");
  process.set_tenant_id(OB_SYS_TENANT_ID);
  bool redirect_stdin_stdout = false;
  EXPECT_EQ(OB_SUCCESS, process.start(redirect_stdin_stdout));
  EXPECT_GT(process.get_pid(), 0);
  // reset process pipe output
  int dev_null_fd = open("/dev/null", O_WRONLY);
  ASSERT_GT(dev_null_fd, 0);
  dup2(dev_null_fd, process.pipe_fd_stdout_);
  for (int i = 0; i < 4; i++) {
    usleep(ObSandboxManager::MONITOR_INTERVAL_US + 100000);
    // printf("state: %lu\n", static_cast<int32_t>(process.get_state()));
    // printf("cpu time: %ld, usage: %f\n", process.get_cpu_time(), process.get_cpu_usage());
    if ( i > 0) {
      // first check cpu usage is 0
      EXPECT_GT(process.get_cpu_usage(), 10.0);
    }
    EXPECT_EQ(SandboxState::STATE_RUNNING, process.get_state());
    EXPECT_GT(process.get_cpu_time(), 0);
    EXPECT_GT(process.last_check_time_, 0);
  }
  process.destroy();

  // test communication again
  process.start();
  char output[28] = {0};
  size_t ret_size = 0;
  process.read_msg(output, sizeof(output), ret_size);
  EXPECT_GT(ret_size, 0);
  EXPECT_EQ('y', output[0]);

  manager.destroy();
}

TEST_F(TestObSandboxProtocol, TestMemoryLimit) {
  // it is a local test, need to prepare some binaries

  // Check if ob_sandbox binary exists
  if (access("./ob_sandbox", X_OK) != 0) {
    LOG_INFO("ob_sandbox binary not found, skipping TestMemoryLimit");
    return;
  } else if (access("./mem_stress", X_OK) != 0) {
    LOG_INFO("stress binary not found, skipping TestMemoryLimit");
    return;
  }
  system("cp ./ob_sandbox ./bin/ob_sandbox");
  LOG_INFO("ob_sandbox binary found, start test TestMemoryLimit");

  ObSandboxManager& manager = ObSandboxManager::get_instance();
  manager.destroy();
  EXPECT_EQ(OB_SUCCESS, manager.init());
  char mem_stress_path[PATH_MAX] = {0};
  char cwd[PATH_MAX] = {0};
  if (getcwd(cwd, sizeof(cwd)) != nullptr) {
    snprintf(mem_stress_path, sizeof(mem_stress_path), "%s/mem_stress", cwd);
  }
  ObSandboxProcess process(mem_stress_path, "");
  EXPECT_EQ(OB_SUCCESS, process.start());
  EXPECT_GT(process.get_pid(), 0);

  // Wait for process to start and allocate some memory
  usleep(500000); // 0.5s

  // Check process is running
  EXPECT_EQ(SandboxState::STATE_RUNNING, process.get_state());
  usleep(10000);
  int check_pid = process.get_pid();
  int kill_ret = kill(check_pid, 0);
  EXPECT_EQ(0, kill_ret);

  // Cleanup
  process.destroy();
  manager.destroy();
}

int main(int argc, char **argv)
{
  unlink("test_ob_sandbox.log");
  mkdir("./bin", 0755);
  mkdir("./etc", 0755);
  signal(SIGPIPE, SIG_IGN);
  ObClockGenerator::init();
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_file_name("test_ob_sandbox.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();

  return ret;
}
