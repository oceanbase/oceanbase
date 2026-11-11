/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX CLOG

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>

#define private public
#define protected public
#include "rpc/obrpc/ob_net_keepalive.h"
#include "rpc/frame/ob_net_easy.h"
#include "logservice/common_util/ob_log_active_keep_alive.h"
#include "logservice/arbserver/palf_env_lite_mgr.h"
#undef private
#undef protected

#include "lib/net/ob_addr.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace obrpc;
using namespace rpc::frame;
using namespace logservice;
using namespace palflite;

class TestObLogActiveKeepAlive : public testing::Test
{
public:
  ObNetEasy net_;

  void SetUp() override
  {
    net_.net_keepalive_register();
    ObNetKeepAlive::get_instance().start();
  }

  void TearDown() override
  {
    ObNetKeepAlive &ka = ObNetKeepAlive::get_instance();
    ka.stop();
    ka.destroy();
  }

  static int find_free_port()
  {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = htonl(INADDR_ANY);
    const int start_port = 20000;
    int port = -1;
    for (int candidate = start_port; candidate <= 65535; ++candidate) {
      sin.sin_port = htons(candidate);
      if (bind(sock, (struct sockaddr *)&sin, sizeof(sin)) == 0) {
        port = candidate;
        break;
      }
    }
    close(sock);
    return port;
  }

  static void start_server(int port, std::atomic<bool> &stop)
  {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { return; }
    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_port = htons(port);
    sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    if (bind(listen_fd, (struct sockaddr *)&sin, sizeof(sin)) < 0) {
      close(listen_fd);
      return;
    }
    listen(listen_fd, 5);

    int flags = fcntl(listen_fd, F_GETFL, 0);
    fcntl(listen_fd, F_SETFL, flags | O_NONBLOCK);

    while (!stop.load()) {
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

#define CONNECT_KEEPALIVE_INTERVAL 60L * 1000 * 1000 // 60s, consistent with test_ob_net_keepalive.cpp

TEST_F(TestObLogActiveKeepAlive, isolation_to_blacklist_to_recover)
{
  const int port = find_free_port();
  if (port <= 0) {
    PALF_LOG(INFO, "skip test because no available local tcp port in current environment");
    return;
  }

  std::atomic<bool> stop_server(false);
  std::thread server_thread(start_server, port, std::ref(stop_server));
  usleep(100000);

  ObAddr dst;
  dst.set_ip_addr("127.0.0.1", port);
  ObAddr self_addr;
  self_addr.set_ip_addr("127.0.0.1", port + 1);
  easy_addr_t ez_dst = to_ez_addr(dst);

  ObLogActiveKeepAlive worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(self_addr));
  ASSERT_EQ(OB_SUCCESS, worker.addr_set_.set_refactored(dst, 1));

  // Run probe_once_ a few times, ensure keepalive state is created.
  ObNetKeepAlive &ka = ObNetKeepAlive::get_instance();
  ObNetKeepAlive::DestKeepAliveState *rs = nullptr;
  bool seen_state = false;
  for (int i = 0; i < 50; ++i) {
    worker.probe_once_();
    rs = ka.regist_dest_if_need(ez_dst);
    if (OB_NOT_NULL(rs) && rs->last_probe_connect_ts_ > 0) {
      seen_state = true;
      break;
    }
    usleep(10000);
  }
  ASSERT_TRUE(seen_state);
  ASSERT_NE(nullptr, rs);

  // Simulate network isolation: stop the loopback server.
  stop_server = true;
  server_thread.join();

  // Force reconnect by rewinding last_probe_connect_ts_.
  rs->last_probe_connect_ts_ = ObTimeUtility::current_time() - (CONNECT_KEEPALIVE_INTERVAL * 2);

  bool blacklisted = false;
  for (int i = 0; i < 50; ++i) {
    worker.probe_once_();
    rs = ka.regist_dest_if_need(ez_dst);
    if (OB_NOT_NULL(rs) && rs->is_probe_black_) {
      blacklisted = true;
      break;
    }
    usleep(10000);
  }
  ASSERT_TRUE(blacklisted);

  // Recovery: restart server and verify it can become non-black.
  stop_server = false;
  std::thread server_thread2(start_server, port, std::ref(stop_server));
  usleep(100000);

  // Force reconnect again, then wait until it is no longer probe-black.
  rs->last_probe_connect_ts_ = ObTimeUtility::current_time() - (CONNECT_KEEPALIVE_INTERVAL * 2);
  bool recovered = false;
  for (int i = 0; i < 100; ++i) {
    worker.probe_once_();
    bool in_blacklist = false;
    (void)ka.probe_connectivity(dst, in_blacklist);
    rs = ka.regist_dest_if_need(ez_dst);
    if (OB_NOT_NULL(rs) && !rs->is_probe_black_ && !in_blacklist && rs->probe_connect_fd_ == -1) {
      recovered = true;
      break;
    }
    usleep(10000);
  }
  EXPECT_TRUE(recovered);

  stop_server = true;
  server_thread2.join();

  worker.destroy();
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_ob_log_active_keep_alive.log*");
  OB_LOGGER.set_file_name("test_ob_log_active_keep_alive.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
