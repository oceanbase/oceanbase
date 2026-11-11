/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define private public
#include "logservice/common_util/ob_log_active_keep_alive.h"
#include "logservice/arbserver/palf_env_lite_mgr.h"
#undef private
#include "observer/ob_server.h"

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace logservice;

static ObAddr make_addr(const char *ip, const int port)
{
  ObAddr addr;
  addr.set_ip_addr(ip, port);
  return addr;
}

struct ScopedServerRuntimeEnv
{
  ScopedServerRuntimeEnv()
      : gctx_(observer::ObServer::get_instance().get_gctx()),
        saved_startup_mode_(gctx_.startup_mode_),
        saved_omt_(gctx_.omt_),
        saved_cluster_id_(GCONF.cluster_id.get_value())
  {}

  ~ScopedServerRuntimeEnv()
  {
    gctx_.startup_mode_ = saved_startup_mode_;
    gctx_.omt_ = saved_omt_;
    GCONF.cluster_id = saved_cluster_id_;
  }

  void set_startup_mode(const share::ObServerMode mode)
  {
    gctx_.startup_mode_ = mode;
  }

  void set_omt(omt::ObMultiTenant *omt)
  {
    gctx_.omt_ = omt;
  }

  void set_cluster_id(const int64_t cluster_id)
  {
    GCONF.cluster_id = cluster_id;
  }

  ObGlobalContext &gctx_;
  share::ObServerMode saved_startup_mode_;
  omt::ObMultiTenant *saved_omt_;
  int64_t saved_cluster_id_;
};

TEST(TestObLogActiveKeepAliveBasic, init_destroy_and_not_init_snapshot)
{
  ObLogActiveKeepAlive worker;
  const ObAddr self_addr = make_addr("127.0.0.1", 10000);

  EXPECT_EQ(OB_INVALID_ARGUMENT, worker.init(ObAddr()));
  EXPECT_EQ(OB_SUCCESS, worker.init(self_addr));
  EXPECT_TRUE(worker.is_inited());
  EXPECT_EQ(OB_INIT_TWICE, worker.init(self_addr));

  worker.destroy();
  EXPECT_FALSE(worker.is_inited());
  worker.destroy();
  EXPECT_FALSE(worker.is_inited());
}

TEST(TestObLogActiveKeepAliveBasic, snapshot_empty_with_initialized_empty_mgr_returns_eagain)
{
  ScopedServerRuntimeEnv runtime_env;
  runtime_env.set_startup_mode(share::ObServerMode::INVALID_MODE);
  runtime_env.set_cluster_id(1);
  runtime_env.set_omt(nullptr);

  ObLogActiveKeepAlive worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(make_addr("127.0.0.1", 10000)));

  ASSERT_EQ(OB_EAGAIN, worker.refresh_addr_set_());
  EXPECT_EQ(0, worker.addr_set_.size());
}

TEST(TestObLogActiveKeepAliveBasic, snapshot_when_is_arbitration_mode_false_returns_eagain)
{
  ScopedServerRuntimeEnv runtime_env;
  runtime_env.set_startup_mode(share::ObServerMode::NORMAL_MODE);
  runtime_env.set_cluster_id(1);
  runtime_env.set_omt(nullptr);

  ASSERT_FALSE(observer::ObServer::get_instance().is_arbitration_mode());

  ObLogActiveKeepAlive worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(make_addr("127.0.0.1", 10000)));

  ASSERT_EQ(OB_EAGAIN, worker.refresh_addr_set_());
  EXPECT_EQ(0, worker.addr_set_.size());
}

TEST(TestObLogActiveKeepAliveBasic, snapshot_when_is_arbitration_mode_true_returns_not_init)
{
  ScopedServerRuntimeEnv runtime_env;
  runtime_env.set_startup_mode(share::ObServerMode::ARBITRATION_MODE);
  runtime_env.set_omt(nullptr);

  ASSERT_TRUE(observer::ObServer::get_instance().is_arbitration_mode());
  palflite::PalfEnvLiteMgr::get_instance().destroy();

  ObLogActiveKeepAlive worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(make_addr("127.0.0.1", 10000)));

  ASSERT_EQ(OB_NOT_INIT, worker.refresh_addr_set_());
  EXPECT_EQ(0, worker.addr_set_.size());
}

TEST(TestObLogActiveKeepAliveBasic, collect_cluster_ids_from_handle_rejects_null_handle)
{
  ObLogActiveKeepAlive worker;
  ASSERT_EQ(OB_SUCCESS, worker.init(make_addr("127.0.0.1", 10000)));

  EXPECT_EQ(OB_INVALID_ARGUMENT, worker.collect_addr_from_handle_(1, nullptr));
  EXPECT_EQ(0, worker.addr_set_.size());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_ob_log_active_keep_alive_basic.log*");
  OB_LOGGER.set_file_name("test_ob_log_active_keep_alive_basic.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
