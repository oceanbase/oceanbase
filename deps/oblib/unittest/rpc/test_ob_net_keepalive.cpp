/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER
#define private public
#define protected public
#include "rpc/obrpc/ob_net_keepalive.h"
#undef private
#undef protected

#include <gmock/gmock.h>
#include "lib/net/ob_addr.h"
#include "lib/time/ob_time_utility.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <thread>
#include <atomic>
#include <fcntl.h>
#include "rpc/frame/ob_req_transport.h"
#include "rpc/frame/ob_net_easy.h"
#include "common/ob_clock_generator.h"

using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::rpc::frame;

class TestObNetKeepAliveProbe : public testing::Test
{
public:
  static void SetUpTestCase() {
  }
  ObNetEasy net_;
  void SetUp() override {
    net_.net_keepalive_register();
    ObNetKeepAlive &ka = ObNetKeepAlive::get_instance();
    ka.start();
  }

  void TearDown() override {
    ObNetKeepAlive &ka = ObNetKeepAlive::get_instance();
    ka.stop();
    ka.destroy();
  }

  static int find_free_port() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_port = 0;
    sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(sock, (struct sockaddr *)&sin, sizeof(sin)) < 0) {
      close(sock);
      return -1;
    }
    socklen_t len = sizeof(sin);
    if (getsockname(sock, (struct sockaddr *)&sin, &len) < 0) {
        close(sock);
        return -1;
    }
    close(sock);
    return ntohs(sin.sin_port);
  }

  static void start_server(int port, std::atomic<bool> &stop) {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_port = htons(port);
    sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    if (bind(listen_fd, (struct sockaddr *)&sin, sizeof(sin)) < 0) {
        return;
    }
    listen(listen_fd, 5);

    int flags = fcntl(listen_fd, F_GETFL, 0);
    fcntl(listen_fd, F_SETFL, flags | O_NONBLOCK);

    while (!stop) {
      struct sockaddr_in client_addr;
      socklen_t len = sizeof(client_addr);
      int client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &len);
      if (client_fd >= 0) {
        close(client_fd);
      }
      usleep(10000);
    }
    close(listen_fd);
  }
};
#define CONNECT_KEEPALIVE_INTERVAL 60L * 1000 * 1000 // 60s
TEST_F(TestObNetKeepAliveProbe, success_to_fail) {
  int port = find_free_port();
  ASSERT_GT(port, 0);

  std::atomic<bool> stop_server(false);
  std::thread server_thread(start_server, port, std::ref(stop_server));
  usleep(100000);

  ObAddr addr;
  addr.set_ip_addr("127.0.0.1", port);
  bool in_blacklist = false;

  ObNetKeepAlive &ka = ObNetKeepAlive::get_instance();

  // Reset state for this addr if it exists
  easy_addr_t ez_addr = to_ez_addr(addr);
  auto *rs = ka.regist_dest_if_need(ez_addr);
  ASSERT_NE(rs, nullptr);
  ASSERT_EQ(rs->probe_connect_fd_, -1);
  ASSERT_EQ(rs->last_probe_connect_ts_, 0);
  ASSERT_EQ(rs->is_probe_black_, false);

  // 1. Initial call
  int ret = ka.probe_connectivity(addr, in_blacklist);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_NE(rs->probe_connect_fd_, -1);
  ASSERT_GT(rs->last_probe_connect_ts_, 0);
  ASSERT_EQ(rs->is_probe_black_, false);

  // 2. Wait and check
  int64_t start_time = ObTimeUtility::current_time();
  int max_retries = 20;
  bool connected = false;
  while(max_retries--) {
      usleep(10000);
      ret = ka.probe_connectivity(addr, in_blacklist);
      rs = ka.regist_dest_if_need(ez_addr);
      if (rs && rs->probe_connect_fd_ == -1 && !rs->is_probe_black_) {
          // If we are back to IDLE state and not blacklisted, it means success
          // Check timestamp to be sure it ran
          if (rs->last_probe_connect_ts_ > 0) {
            connected = true;
            break;
          }
      }
  }
  int64_t cost_time = ObTimeUtility::current_time() - start_time;
  ASSERT_LT(cost_time, 100000);
  ASSERT_TRUE(connected);
  ASSERT_EQ(rs->probe_connect_fd_, -1);
  ASSERT_FALSE(in_blacklist);

  stop_server = true;
  server_thread.join();

  // when server is stopped, the probe_connectivity should detect in_blacklist = true
  LOG_INFO("when server is stopped, the probe_connectivity should detect in_blacklist = true");
  // reduce last_probe_connect_ts_ to trgger reconnect
  rs->last_probe_connect_ts_ = ObTimeUtility::current_time() - (CONNECT_KEEPALIVE_INTERVAL * 2);
  ret = ka.probe_connectivity(addr, in_blacklist);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_NE(rs->probe_connect_fd_, -1);
  ASSERT_GT(rs->last_probe_connect_ts_, 0);
  ASSERT_EQ(rs->is_probe_black_, false);
  usleep(10000);
  ret = ka.probe_connectivity(addr, in_blacklist);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(rs->probe_connect_fd_, -1);
  ASSERT_EQ(rs->is_probe_black_, true);
}

TEST_F(TestObNetKeepAliveProbe, failed_to_success) {
  int port = find_free_port();
  ASSERT_GT(port, 0);
  std::atomic<bool> stop_server(false);
  ObAddr addr;
  addr.set_ip_addr("127.0.0.1", port);
  bool in_blacklist = false;
  ObNetKeepAlive &ka = ObNetKeepAlive::get_instance();
  easy_addr_t ez_addr = to_ez_addr(addr);
  auto *rs = ka.regist_dest_if_need(ez_addr);
  ASSERT_NE(rs, nullptr);
  int ret = ka.probe_connectivity(addr, in_blacklist);
  ASSERT_EQ(ret, OB_SUCCESS);
  int max_retries = 50;
  while(max_retries--) {
    usleep(100000);
    if (!rs->is_probe_black_ && rs->probe_connect_fd_ == -1) {
      rs->last_probe_connect_ts_ = ObTimeUtility::current_time() - (CONNECT_KEEPALIVE_INTERVAL * 2);
    }
    ka.probe_connectivity(addr, in_blacklist);
    if (in_blacklist) break;
  }
  LOG_INFO("start server to make it connected");
  std::thread server_thread(start_server, port, std::ref(stop_server));
  usleep(100000);
  rs->last_probe_connect_ts_ = ObTimeUtility::current_time() - (CONNECT_KEEPALIVE_INTERVAL * 2);
  ret = ka.probe_connectivity(addr, in_blacklist);
  ASSERT_EQ(ret, OB_SUCCESS);
  bool connected = false;
  max_retries = 50;
  while(max_retries--) {
    usleep(100000);
    ka.probe_connectivity(addr, in_blacklist);
    if (!in_blacklist && !rs->is_probe_black_ && rs->probe_connect_fd_ == -1) {
      connected = true;
      break;
    }
  }
  ASSERT_TRUE(connected);
  ASSERT_FALSE(in_blacklist);
  stop_server = true;
  server_thread.join();
}

TEST_F(TestObNetKeepAliveProbe, probe_connect_hung) {
  ObAddr addr;
  addr.set_ip_addr("8.8.8.8", 22);
  bool in_blacklist = false;
  ObNetKeepAlive &ka = ObNetKeepAlive::get_instance();

  easy_addr_t ez_addr = to_ez_addr(addr);
  ASSERT_EQ(ka.probe_connectivity(addr, in_blacklist), OB_SUCCESS);

  int64_t start_time = ObClockGenerator::getClock();
  for(int i=0; i < 10; ++i) {
    usleep(500000);
    int ret = ka.probe_connectivity(addr, in_blacklist);
    ASSERT_EQ(ret, OB_SUCCESS);
    int64_t current_time = ObClockGenerator::getClock();
    LOG_INFO("8.8.8.8:22 probe", K(current_time), K(start_time), K(in_blacklist));
    if (current_time - start_time < 3000000) {
      ASSERT_EQ(in_blacklist, false);
    } else {
      ASSERT_EQ(in_blacklist, true);
    }
  }
}

TEST_F(TestObNetKeepAliveProbe, concurrent_probe_connectivity) {
  int port = find_free_port();
  ASSERT_GT(port, 0);
  std::atomic<bool> stop_server(false);
  // Start server to ensure connectivity
  std::thread server_thread(start_server, port, std::ref(stop_server));
  usleep(100000);

  ObAddr addr;
  addr.set_ip_addr("127.0.0.1", port);
  ObNetKeepAlive &ka = ObNetKeepAlive::get_instance();

  const int thread_count = 10;
  const int loop_count = 100;
  std::vector<std::thread> threads;
  std::atomic<int64_t> total_success(0);

  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back([&]() {
      bool in_blacklist = false;
      for (int j = 0; j < loop_count; ++j) {
        int ret = ka.probe_connectivity(addr, in_blacklist);
        if (OB_SUCCESS == ret && !in_blacklist) {
          total_success++;
        }
        usleep(1000);
      }
    });
  }
  for (auto &t : threads) {
    t.join();
  }
  stop_server = true;
  server_thread.join();
  LOG_INFO("concurrent probe connectivity finished", K(total_success.load()));
  ASSERT_LT(total_success.load(), thread_count * loop_count);
}


int main(int argc, char **argv)
{
  system("rm -f test_ob_net_keepalive.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_ob_net_keepalive.log", true);
  ObClockGenerator::init();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
