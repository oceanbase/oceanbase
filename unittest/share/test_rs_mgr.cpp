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

#define USING_LOG_PREFIX SHARE


#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#define protected public
#include "schema/db_initializer.h"
#include "share/ob_rs_mgr.h"
#include "share/ob_inner_config_root_addr.h"
#include "rpc/mock_ob_srv_rpc_proxy.h"
#include "share/ob_common_rpc_proxy.h"
#include "schema/db_initializer.h"
#include "partition_table/fake_part_property_getter.h"
#include "lib/container/ob_array_iterator.h"
//#include "rpc/mock_ob_common_rpc_proxy.h"

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace obrpc;
using namespace schema;
using namespace host;

using testing::_;
using ::testing::Return;
using ::testing::Invoke;

static ObArray<ObRootAddr> global_rs_list;
static ObArray<ObRootAddr> global_readonly_rs_list;

class FakeRootAddrAgent : public ObRootAddrAgent
{
public:
  virtual int store(const ObRootAddrList &addr_list, const ObRootAddrList &readonly_addr_list, const bool force)
  {
    int ret = OB_SUCCESS;
    UNUSED(force);
    global_rs_list.reuse();
    global_readonly_rs_list.reuse();
    if (OB_FAIL(global_rs_list.assign(addr_list))) {
      LOG_WARN("global_rs_list assign fail", K(ret));
    } else if (OB_FAIL(global_readonly_rs_list.assign(readonly_addr_list))) {
      LOG_WARN("global_readonly_rs_list assign fail", K(ret));
    }
    return ret;
  }

  virtual int fetch(ObRootAddrList &addr_list, ObRootAddrList &readonly_addr_list)
  {
    int ret = OB_SUCCESS;
    addr_list.reuse();
    readonly_addr_list.reuse();
    if (OB_FAIL(addr_list.assign(global_rs_list))) {
      LOG_WARN("addr_list assign fail", K(ret));
    } else if (OB_FAIL(readonly_addr_list.assign(global_readonly_rs_list))) {
      LOG_WARN("readonly_addr_list assign fail", K(ret));
    }
    return ret;
  }

};

class FakeSrvRpcProxy : public  ObCommonRpcProxy
{
public:
  FakeSrvRpcProxy() : ObCommonRpcProxy(this)
  {
  }

  virtual int get_rootserver_role(ObGetRootserverRoleResult &role, const ObRpcOpts &)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(role.init(FOLLOWER, role.get_status()))) {
      LOG_WARN("fail to init a ObGetRootserverRoleResult", KR(ret));
    }
    FOREACH(r, global_rs_list) {
      LOG_INFO("dst", K(dst_));
      if (r->server_ == dst_
          && r->role_ == LEADER
          && OB_FAIL(role.init(LEADER, role.get_status()))) {
        LOG_WARN("fail to init a ObGetRootserverRoleResult", KR(ret));
      }
    }
    ret = dst_.is_valid() ? ret : OB_BAD_ADDRESS;
    return ret;
  }
};

class TestRsMgr : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown() {}
private:
  ObServerConfig config_;
  DBInitializer db_initer_;

  FakeSrvRpcProxy rpc_proxy_;
  ObRsMgr rs_mgr_;
  FakeRootAddrAgent addr_agent_;
};

void TestRsMgr::SetUp()
{
  config_.rootservice_list.set_value("");
  config_.obconfig_url.set_value("");

  // ASSERT_EQ(OB_SUCCESS, db_initer_.init());

  ASSERT_EQ(OB_SUCCESS, rs_mgr_.init(&rpc_proxy_, &config_, &db_initer_.get_sql_proxy()));
  memcpy(&rs_mgr_.addr_agent_, &addr_agent_, 8);

  global_rs_list.reuse();
  global_readonly_rs_list.reuse();
}

TEST_F(TestRsMgr, common)
{
  ASSERT_TRUE(rs_mgr_.is_inited());
  ASSERT_EQ(OB_INIT_TWICE, rs_mgr_.init(&rpc_proxy_, &config_, &db_initer_.get_sql_proxy()));

  ObAddr rs;
  ASSERT_EQ(OB_SUCCESS, rs_mgr_.get_master_root_server(rs));
  ASSERT_TRUE(rs.is_valid() == false);
  ASSERT_NE(OB_SUCCESS, rs_mgr_.update_rs_list());
  ASSERT_NE(OB_SUCCESS, rs_mgr_.renew_master_rootserver());

  ObRootAddr addr;
  addr.server_ = A;
  addr.role_ = FOLLOWER;
  addr.sql_port_ = 3003;
  ASSERT_EQ(OB_SUCCESS, global_rs_list.push_back(addr));

  addr.server_ = B;
  addr.role_ = FOLLOWER;
  addr.sql_port_ = 3003;
  ASSERT_EQ(OB_SUCCESS, global_rs_list.push_back(addr));

  addr.server_ = C;
  addr.role_ = FOLLOWER;
  addr.sql_port_ = 3003;
  ASSERT_EQ(OB_SUCCESS, global_rs_list.push_back(addr));

  ASSERT_EQ(OB_SUCCESS, rs_mgr_.update_rs_list());
  ASSERT_EQ(OB_SUCCESS, rs_mgr_.get_master_root_server(rs));
  ASSERT_TRUE(rs.is_valid() == false);
  ASSERT_NE(OB_SUCCESS, rs_mgr_.renew_master_rootserver());

  global_rs_list.at(0).role_ = LEADER;
  ASSERT_EQ(OB_SUCCESS, rs_mgr_.renew_master_rootserver());
  ASSERT_EQ(OB_SUCCESS, rs_mgr_.get_master_root_server(rs));
  ASSERT_EQ(A, rs);

  global_rs_list.at(0).role_ = FOLLOWER;
  global_rs_list.at(1).role_ = LEADER;
  ASSERT_EQ(OB_SUCCESS, rs_mgr_.renew_master_rootserver());
  ASSERT_EQ(OB_SUCCESS, rs_mgr_.get_master_root_server(rs));
  ASSERT_EQ(B, rs);
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
