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

#include <gtest/gtest.h>
#define USING_LOG_PREFIX SHARE
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/net/ob_addr.h"
#include "share/config/ob_server_config.h"
#include "share/backup/ob_log_restore_struct.h"
#include "share/ob_query_standalone_info_rpc_struct.h"
#include "share/ob_log_restore_proxy.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "logservice/logrouteservice/ob_log_route_service.h"
#include "share/ob_define.h"  // is_server_down_error

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace share;
using namespace obrpc;
using namespace logservice;

// standalone IP 解耦 mittest。用例统一 OB_SYS_TENANT_ID（探测 RPC 不消费 tenant）。
// 用例列表：
//   基础：is_loopback | standalone_info 序列化 | query_standalone_info handler
//   探测/翻译：query_peer_is_standalone | standalone IP 翻译 | 不可达/空 addr/多 addr
//   SET 校验：不可达拒绝 | standalone↔standalone | SYNC_STANDBY_DEST | 决策逻辑
//   枚举：CENTRALIZED / DISTRIBUTED / INVALID | is_standalone | assign
//   SET 学地址：只学 rpc_port、公网 IP 不被覆盖 | set_sql_addr 覆盖语义 | 入参不一致
//   场景1：check_peer_form_for_set_command 全链路
//   场景3/4：LRS 使用 CDC external address 映射单 server loopback 路由
//   防御：query/get_leader 非法入参

class TestStandalonePublicAddress : public ObSimpleClusterTestBase
{
public:
  TestStandalonePublicAddress() : ObSimpleClusterTestBase("test_standalone_public_address_") {}
  static void build_self_service_attr(ObRestoreSourceServiceAttr &service_attr);
};

void TestStandalonePublicAddress::build_self_service_attr(ObRestoreSourceServiceAttr &service_attr)
{
  service_attr.reset();
  ObRestoreSourceServiceAddr svc_addr;
  ObAddr self_addr = GCTX.self_addr();
  ASSERT_EQ(OB_SUCCESS, svc_addr.set_sql_addr(self_addr));
  ASSERT_EQ(OB_SUCCESS, svc_addr.set_svr_port(self_addr.get_port()));
  ASSERT_EQ(OB_SUCCESS, service_attr.addr_.push_back(svc_addr));
  service_attr.user_.cluster_id_ = GCONF.cluster_id;
  service_attr.user_.tenant_id_ = OB_SYS_TENANT_ID;
  service_attr.user_.mode_ = ObCompatibilityMode::MYSQL_MODE;
  STRCPY(service_attr.user_.user_name_, "root");
  STRCPY(service_attr.user_.tenant_name_, "sys");
  STRCPY(service_attr.encrypt_passwd_, "dummy");
}

TEST_F(TestStandalonePublicAddress, test_addr_is_loopback)
{
  LOG_INFO("TEST: test_addr_is_loopback");

  {
    ObAddr addr;
    ASSERT_TRUE(addr.set_ip_addr("127.0.0.1", 2882));
    ASSERT_TRUE(addr.is_loopback());
  }
  {
    ObAddr addr;
    ASSERT_TRUE(addr.set_ip_addr("127.0.0.2", 2882));
    ASSERT_TRUE(addr.is_loopback());
  }
  {
    ObAddr addr;
    ASSERT_TRUE(addr.set_ip_addr("127.255.255.255", 2882));
    ASSERT_TRUE(addr.is_loopback());
  }
  {
    ObAddr addr;
    ASSERT_TRUE(addr.set_ip_addr("128.0.0.1", 2882));
    ASSERT_FALSE(addr.is_loopback());
  }
  {
    ObAddr addr;
    ASSERT_TRUE(addr.set_ip_addr("126.255.255.255", 2882));
    ASSERT_FALSE(addr.is_loopback());
  }
  {
    ObAddr addr;
    ASSERT_TRUE(addr.set_ip_addr("::1", 2882));
    ASSERT_TRUE(addr.is_loopback());
  }
  {
    ObAddr addr;
    ASSERT_TRUE(addr.set_ip_addr("10.0.0.1", 2882));
    ASSERT_FALSE(addr.is_loopback());
  }
  {
    ObAddr addr;
    ASSERT_TRUE(addr.set_ip_addr("2001:db8::1", 2882));
    ASSERT_FALSE(addr.is_loopback());
  }
  {
    ObAddr addr;
    ASSERT_TRUE(addr.set_ip_addr("::", 2882));
    ASSERT_FALSE(addr.is_loopback());
  }
  {
    // v4-mapped 回环 ::ffff:127.0.0.1 不算 loopback（IN6_IS_ADDR_LOOPBACK 语义）
    ObAddr addr;
    ASSERT_TRUE(addr.set_ip_addr("::ffff:127.0.0.1", 2882));
    ASSERT_FALSE(addr.is_loopback());
  }
  {
    ObAddr addr;
    ASSERT_FALSE(addr.is_loopback());
  }
}

TEST_F(TestStandalonePublicAddress, test_standalone_info_serialize)
{
  LOG_INFO("TEST: test_standalone_info_serialize");
  char buf[1024] = {0};

  {
    ObQueryDeployModeInfoArg arg;
    ASSERT_TRUE(arg.is_valid());
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, arg.serialize(buf, sizeof(buf), pos));
    const int64_t data_len = pos;

    ObQueryDeployModeInfoArg arg2;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, arg2.deserialize(buf, data_len, pos));
    ASSERT_TRUE(arg2.is_valid());
  }

  ObDeployMode all_status[] = {
    ObDeployMode::DISTRIBUTED,
    ObDeployMode::CENTRALIZED,
    ObDeployMode::INVALID_STATUS,
  };
  for (int i = 0; i < 3; ++i) {
    ObQueryDeployModeInfoResult res;
    res.set_status(all_status[i]);
    ASSERT_EQ(all_status[i], res.get_status());

    int64_t pos = 0;
    MEMSET(buf, 0, sizeof(buf));
    ASSERT_EQ(OB_SUCCESS, res.serialize(buf, sizeof(buf), pos));
    const int64_t data_len = pos;

    ObQueryDeployModeInfoResult res2;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, res2.deserialize(buf, data_len, pos));
    ASSERT_EQ(all_status[i], res2.get_status());
  }
}

TEST_F(TestStandalonePublicAddress, test_query_standalone_info_handler)
{
  LOG_INFO("TEST: test_query_standalone_info_handler");

  ObAddr self_addr = GCTX.self_addr();
  ObQueryDeployModeInfoArg arg;
  ObQueryDeployModeInfoResult result;
  ASSERT_EQ(OB_SUCCESS,
      GCTX.srv_rpc_proxy_->to(self_addr).timeout(5000000L)
          .query_standalone_info(arg, result));

#ifdef OB_ENABLE_STANDALONE_LAUNCH
  ASSERT_EQ(ObDeployMode::CENTRALIZED, result.get_status());
#else
  ASSERT_EQ(ObDeployMode::DISTRIBUTED, result.get_status());
#endif
}

#ifdef OB_ENABLE_STANDALONE_LAUNCH
TEST_F(TestStandalonePublicAddress, test_query_peer_is_standalone)
{
  LOG_INFO("TEST: test_query_peer_is_standalone");

  ObRestoreSourceServiceAttr service_attr;
  build_self_service_attr(service_attr);

  bool peer_is_standalone = false;
  ASSERT_EQ(OB_SUCCESS,
      ObLogRestoreProxyUtil::query_peer_is_standalone_by_rpc(
          service_attr, GCTX.srv_rpc_proxy_, peer_is_standalone));
  ASSERT_TRUE(peer_is_standalone);
}

TEST_F(TestStandalonePublicAddress, test_check_peer_form_unreachable)
{
  LOG_INFO("TEST: test_check_peer_form_unreachable");

  ObRestoreSourceServiceAttr service_attr;
  service_attr.reset();
  ObRestoreSourceServiceAddr svc_addr;
  ObAddr unreachable;
  ASSERT_TRUE(unreachable.set_ip_addr("192.0.2.1", 99));
  ASSERT_EQ(OB_SUCCESS, svc_addr.set_sql_addr(unreachable));
  ASSERT_EQ(OB_SUCCESS, svc_addr.set_svr_port(99));
  ASSERT_EQ(OB_SUCCESS, service_attr.addr_.push_back(svc_addr));
  service_attr.user_.cluster_id_ = GCONF.cluster_id;
  service_attr.user_.tenant_id_ = OB_SYS_TENANT_ID;
  service_attr.user_.mode_ = ObCompatibilityMode::MYSQL_MODE;
  STRCPY(service_attr.user_.user_name_, "root");
  STRCPY(service_attr.user_.tenant_name_, "sys");
  STRCPY(service_attr.encrypt_passwd_, "dummy");

  int ret = service_attr.check_peer_form_for_set_command(
      *GCTX.srv_rpc_proxy_, "LOG_RESTORE_SOURCE");
  ASSERT_EQ(OB_OP_NOT_ALLOW, ret);
}

TEST_F(TestStandalonePublicAddress, test_query_peer_multi_addr)
{
  LOG_INFO("TEST: test_query_peer_multi_addr");

  ObRestoreSourceServiceAttr service_attr;
  service_attr.reset();

  ObRestoreSourceServiceAddr bad_addr;
  ObAddr unreachable;
  ASSERT_TRUE(unreachable.set_ip_addr("192.0.2.1", 99));
  ASSERT_EQ(OB_SUCCESS, bad_addr.set_sql_addr(unreachable));
  ASSERT_EQ(OB_SUCCESS, bad_addr.set_svr_port(99));
  ASSERT_EQ(OB_SUCCESS, service_attr.addr_.push_back(bad_addr));

  ObRestoreSourceServiceAddr good_addr;
  ObAddr self_addr = GCTX.self_addr();
  ASSERT_EQ(OB_SUCCESS, good_addr.set_sql_addr(self_addr));
  ASSERT_EQ(OB_SUCCESS, good_addr.set_svr_port(self_addr.get_port()));
  ASSERT_EQ(OB_SUCCESS, service_attr.addr_.push_back(good_addr));

  service_attr.user_.cluster_id_ = GCONF.cluster_id;
  service_attr.user_.tenant_id_ = OB_SYS_TENANT_ID;
  service_attr.user_.mode_ = ObCompatibilityMode::MYSQL_MODE;
  STRCPY(service_attr.user_.user_name_, "root");
  STRCPY(service_attr.user_.tenant_name_, "sys");
  STRCPY(service_attr.encrypt_passwd_, "dummy");

  bool peer_is_standalone = false;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      ObLogRestoreProxyUtil::query_peer_is_standalone_by_rpc(
          service_attr, GCTX.srv_rpc_proxy_, peer_is_standalone));
}
#endif

TEST_F(TestStandalonePublicAddress, test_check_peer_form_standalone_to_standalone)
{
  LOG_INFO("TEST: test_check_peer_form_standalone_to_standalone");

  ObRestoreSourceServiceAttr service_attr;
  build_self_service_attr(service_attr);

#ifdef OB_ENABLE_STANDALONE_LAUNCH
  int ret = service_attr.check_peer_form_for_set_command(
      *GCTX.srv_rpc_proxy_, "LOG_RESTORE_SOURCE");
  ASSERT_EQ(OB_SUCCESS, ret);
#endif
}

TEST_F(TestStandalonePublicAddress, test_check_peer_form_sync_standby_dest)
{
  LOG_INFO("TEST: test_check_peer_form_sync_standby_dest");

  ObRestoreSourceServiceAttr service_attr;
  build_self_service_attr(service_attr);

#ifdef OB_ENABLE_STANDALONE_LAUNCH
  int ret = service_attr.check_peer_form_for_set_command(
      *GCTX.srv_rpc_proxy_, "SYNC_STANDBY_DEST");
  ASSERT_EQ(OB_SUCCESS, ret);
#endif
}

TEST_F(TestStandalonePublicAddress, test_check_peer_form_decision_logic)
{
  LOG_INFO("TEST: test_check_peer_form_decision_logic");

  auto decide = [](int probe_ret, bool peer_is_standalone) -> int {
    int ret = probe_ret;
    if (OB_TIMEOUT == ret || OB_CONNECT_ERROR == ret || is_server_down_error(ret)) {
      ret = OB_OP_NOT_ALLOW;
    } else if (OB_NOT_SUPPORTED == ret) {
      ret = OB_OP_NOT_ALLOW;
    } else if (OB_SUCCESS != ret) {
    } else if (!peer_is_standalone) {
      ret = OB_OP_NOT_ALLOW;
    } else {
      ret = OB_SUCCESS;
    }
    return ret;
  };

  ASSERT_EQ(OB_SUCCESS, decide(OB_SUCCESS, true));
  ASSERT_EQ(OB_OP_NOT_ALLOW, decide(OB_SUCCESS, false));
  ASSERT_EQ(OB_OP_NOT_ALLOW, decide(OB_NOT_SUPPORTED, false));
  ASSERT_EQ(OB_OP_NOT_ALLOW, decide(OB_NOT_SUPPORTED, true));
  ASSERT_EQ(OB_OP_NOT_ALLOW, decide(OB_TIMEOUT, false));
  ASSERT_EQ(OB_OP_NOT_ALLOW, decide(OB_CONNECT_ERROR, false));
  ASSERT_EQ(OB_ERR_UNEXPECTED, decide(OB_ERR_UNEXPECTED, false));
}

TEST_F(TestStandalonePublicAddress, test_status_enum_CENTRALIZED)
{
  LOG_INFO("TEST: test_status_enum_CENTRALIZED");
  ObQueryDeployModeInfoResult result;
  result.set_status(ObDeployMode::CENTRALIZED);
  ASSERT_TRUE(ObDeployMode::CENTRALIZED == result.get_status());
  ASSERT_TRUE(result.is_valid());
}

TEST_F(TestStandalonePublicAddress, test_status_enum_distributed)
{
  LOG_INFO("TEST: test_status_enum_distributed");
  ObQueryDeployModeInfoResult result;
  result.set_status(ObDeployMode::DISTRIBUTED);
  ASSERT_FALSE(ObDeployMode::CENTRALIZED == result.get_status());
  ASSERT_TRUE(result.is_valid());
}

TEST_F(TestStandalonePublicAddress, test_status_enum_invalid)
{
  LOG_INFO("TEST: test_status_enum_invalid");
  ObQueryDeployModeInfoResult result;
  ASSERT_EQ(ObDeployMode::INVALID_STATUS, result.get_status());
  ASSERT_FALSE(ObDeployMode::CENTRALIZED == result.get_status());
  ASSERT_FALSE(result.is_valid());
}

#ifdef OB_ENABLE_STANDALONE_LAUNCH
TEST_F(TestStandalonePublicAddress, test_learn_rpc_port_keeps_public_ip)
{
  LOG_INFO("TEST: test_learn_rpc_port_keeps_public_ip");

  ObRestoreSourceServiceAttr service_attr;
  service_attr.reset();
  ObRestoreSourceServiceAddr dba_addr;
  ObAddr public_addr;
  ASSERT_TRUE(public_addr.set_ip_addr("1.2.3.4", 2881));
  ASSERT_EQ(OB_SUCCESS, dba_addr.set_sql_addr(public_addr));
  ASSERT_EQ(OB_SUCCESS, service_attr.addr_.push_back(dba_addr));
  service_attr.user_.cluster_id_ = GCONF.cluster_id;
  service_attr.user_.tenant_id_ = OB_SYS_TENANT_ID;
  service_attr.user_.mode_ = ObCompatibilityMode::MYSQL_MODE;
  STRCPY(service_attr.user_.user_name_, "root");
  STRCPY(service_attr.user_.tenant_name_, "sys");
  STRCPY(service_attr.encrypt_passwd_, "dummy");

  ASSERT_FALSE(service_attr.has_svr_port());

  ObArray<ObAddr> learned_ip_list;
  ObArray<int32_t> learned_svr_port_list;
  ObAddr loopback_sql_addr;
  ASSERT_TRUE(loopback_sql_addr.set_ip_addr("127.0.0.1", 2881));
  ASSERT_EQ(OB_SUCCESS, learned_ip_list.push_back(loopback_sql_addr));
  ASSERT_EQ(OB_SUCCESS, learned_svr_port_list.push_back(2882));
  ASSERT_EQ(OB_SUCCESS,
      service_attr.set_sql_addr_and_svr_port_list(learned_ip_list, learned_svr_port_list));

  ASSERT_TRUE(service_attr.has_svr_port());
  {
    ObAddr cur;
    ASSERT_EQ(OB_SUCCESS, service_attr.addr_.at(0).get_sql_addr(cur));
    char ip[MAX_IP_ADDR_LENGTH] = {0};
    cur.ip_to_string(ip, sizeof(ip));
    ASSERT_STREQ("1.2.3.4", ip);
    ASSERT_EQ(2881, cur.get_port());
  }
  {
    ObAddr svr;
    ASSERT_EQ(OB_SUCCESS, service_attr.addr_.at(0).get_svr_addr(svr));
    char ip[MAX_IP_ADDR_LENGTH] = {0};
    svr.ip_to_string(ip, sizeof(ip));
    ASSERT_STREQ("1.2.3.4", ip);
    ASSERT_EQ(2882, svr.get_port());
    LOG_INFO("svr addr after learn rpc_port", K(svr));
  }
}
#endif

#ifndef OB_ENABLE_STANDALONE_LAUNCH
TEST_F(TestStandalonePublicAddress, test_overwrite_replaces_public_ip_is_expected)
{
  LOG_INFO("TEST: test_overwrite_replaces_public_ip_is_expected");

  ObRestoreSourceServiceAttr service_attr;
  service_attr.reset();
  ObRestoreSourceServiceAddr dba_addr;
  ObAddr public_addr;
  ASSERT_TRUE(public_addr.set_ip_addr("1.2.3.4", 2881));
  ASSERT_EQ(OB_SUCCESS, dba_addr.set_sql_addr(public_addr));
  ASSERT_EQ(OB_SUCCESS, service_attr.addr_.push_back(dba_addr));

  ObArray<ObAddr> learned_ip_list;
  ObArray<int32_t> learned_svr_port_list;
  ObAddr loopback;
  ASSERT_TRUE(loopback.set_ip_addr("127.0.0.1", 2881));
  ASSERT_EQ(OB_SUCCESS, learned_ip_list.push_back(loopback));
  ASSERT_EQ(OB_SUCCESS, learned_svr_port_list.push_back(2882));
  ASSERT_EQ(OB_SUCCESS,
      service_attr.set_sql_addr_and_svr_port_list(learned_ip_list, learned_svr_port_list));

  ObAddr cur;
  ASSERT_EQ(OB_SUCCESS, service_attr.addr_.at(0).get_sql_addr(cur));
  char ip[MAX_IP_ADDR_LENGTH] = {0};
  cur.ip_to_string(ip, sizeof(ip));
  ASSERT_STREQ("127.0.0.1", ip);
  ASSERT_TRUE(service_attr.has_svr_port());
}
#endif

#ifdef OB_ENABLE_STANDALONE_LAUNCH
TEST_F(TestStandalonePublicAddress, test_set_sql_addr_and_svr_port_list_only_learns_rpc_port)
{
  LOG_INFO("TEST: test_set_sql_addr_and_svr_port_list_only_learns_rpc_port");

  ObRestoreSourceServiceAttr attr;
  attr.reset();
  ObRestoreSourceServiceAddr dba_addr;
  ObAddr public_addr;
  ASSERT_TRUE(public_addr.set_ip_addr("1.2.3.4", 2881));
  ASSERT_EQ(OB_SUCCESS, dba_addr.set_sql_addr(public_addr));
  ASSERT_EQ(OB_SUCCESS, dba_addr.set_svr_port(2882));
  ASSERT_EQ(OB_SUCCESS, attr.addr_.push_back(dba_addr));
  attr.user_.cluster_id_ = GCONF.cluster_id;
  attr.user_.tenant_id_ = OB_SYS_TENANT_ID;
  attr.user_.mode_ = ObCompatibilityMode::MYSQL_MODE;
  STRCPY(attr.user_.user_name_, "root");
  STRCPY(attr.user_.tenant_name_, "sys");
  STRCPY(attr.encrypt_passwd_, "dummy");

  ObArray<ObAddr> learned_ip_list;
  ObArray<int32_t> learned_svr_port_list;
  ObAddr loopback_sql_addr;
  ASSERT_TRUE(loopback_sql_addr.set_ip_addr("127.0.0.1", 2881));
  ASSERT_EQ(OB_SUCCESS, learned_ip_list.push_back(loopback_sql_addr));
  ASSERT_EQ(OB_SUCCESS, learned_svr_port_list.push_back(2882));

  ASSERT_EQ(OB_SUCCESS,
      attr.set_sql_addr_and_svr_port_list(learned_ip_list, learned_svr_port_list));

  ObAddr cur;
  ASSERT_EQ(OB_SUCCESS, attr.addr_.at(0).get_sql_addr(cur));
  char ip[MAX_IP_ADDR_LENGTH] = {0};
  cur.ip_to_string(ip, sizeof(ip));
  ASSERT_STREQ("1.2.3.4", ip);
  ASSERT_TRUE(attr.has_svr_port());

  ObAddr svr;
  ASSERT_EQ(OB_SUCCESS, attr.addr_.at(0).get_svr_addr(svr));
  svr.ip_to_string(ip, sizeof(ip));
  ASSERT_STREQ("1.2.3.4", ip);
  ASSERT_EQ(2882, svr.get_port());
  LOG_INFO("learn rpc_port only, public ip kept");
}
#endif

#ifdef OB_ENABLE_STANDALONE_LAUNCH
TEST_F(TestStandalonePublicAddress, test_set_sql_addr_and_svr_port_list_reject_invalid_addr_)
{
  LOG_INFO("TEST: test_set_sql_addr_and_svr_port_list_reject_invalid_addr_");

  ObRestoreSourceServiceAttr service_attr;
  service_attr.reset();
  ObArray<ObAddr> learned_ip_list;
  ObArray<int32_t> learned_svr_port_list;
  ObAddr loopback;
  ASSERT_TRUE(loopback.set_ip_addr("127.0.0.1", 2881));
  ASSERT_EQ(OB_SUCCESS, learned_ip_list.push_back(loopback));
  ASSERT_EQ(OB_SUCCESS, learned_svr_port_list.push_back(2882));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      service_attr.set_sql_addr_and_svr_port_list(learned_ip_list, learned_svr_port_list));

  for (int i = 0; i < 2; ++i) {
    ObRestoreSourceServiceAddr a;
    ObAddr addr;
    ASSERT_TRUE(addr.set_ip_addr("127.0.0.1", 2881 + i));
    ASSERT_EQ(OB_SUCCESS, a.set_sql_addr(addr));
    ASSERT_EQ(OB_SUCCESS, service_attr.addr_.push_back(a));
  }
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      service_attr.set_sql_addr_and_svr_port_list(learned_ip_list, learned_svr_port_list));
}
#endif

#ifdef OB_ENABLE_STANDALONE_LAUNCH
TEST_F(TestStandalonePublicAddress, test_check_peer_form_for_set_command_after_learn_rpc_port)
{
  LOG_INFO("TEST: test_check_peer_form_for_set_command_after_learn_rpc_port");

  ObRestoreSourceServiceAttr service_attr;
  build_self_service_attr(service_attr);

  ObArray<ObAddr> learned_ip_list;
  ObArray<int32_t> learned_svr_port_list;
  ObAddr loopback;
  const int32_t rpc_port = GCTX.self_addr().get_port();
  ASSERT_TRUE(loopback.set_ip_addr("127.0.0.1", 2881));
  ASSERT_EQ(OB_SUCCESS, learned_ip_list.push_back(loopback));
  ASSERT_EQ(OB_SUCCESS, learned_svr_port_list.push_back(rpc_port));
  ASSERT_EQ(OB_SUCCESS,
      service_attr.set_sql_addr_and_svr_port_list(learned_ip_list, learned_svr_port_list));

  ASSERT_EQ(OB_SUCCESS,
      service_attr.check_peer_form_for_set_command(*GCTX.srv_rpc_proxy_, "LOG_RESTORE_SOURCE"));

  ObAddr cur;
  ASSERT_EQ(OB_SUCCESS, service_attr.addr_.at(0).get_sql_addr(cur));
  ObAddr self_addr = GCTX.self_addr();
  char ip[MAX_IP_ADDR_LENGTH] = {0};
  char self_ip[MAX_IP_ADDR_LENGTH] = {0};
  cur.ip_to_string(ip, sizeof(ip));
  ASSERT_TRUE(self_addr.ip_to_string(self_ip, sizeof(self_ip)));
  ASSERT_STREQ(self_ip, ip);
  ASSERT_TRUE(service_attr.has_svr_port());
  ObAddr svr;
  ASSERT_EQ(OB_SUCCESS, service_attr.addr_.at(0).get_svr_addr(svr));
  ASSERT_EQ(rpc_port, svr.get_port());
}
#endif

TEST_F(TestStandalonePublicAddress, test_route_addr_mapping_single_loopback_server)
{
  ObLogAllSvrCache all_svr_cache;
  ObAllServerInfo all_server_info;
  ASSERT_EQ(OB_SUCCESS, all_server_info.init(1));
  AllServerRecord record;
  record.svr_id_ = 1;
  ASSERT_TRUE(record.server_.set_ip_addr("127.0.0.1", 2882));
  record.status_ = ObServerStatus::OB_SERVER_ACTIVE;
  ASSERT_EQ(OB_SUCCESS, all_server_info.add(record));
  all_svr_cache.publish_cluster_topology_(all_server_info);

  ObLogExternalAddrConfig config;
  ObArray<ObAddr> addr_list;
  ObAddr external_addr;
  ASSERT_TRUE(external_addr.set_ip_addr("192.0.2.10", 2881));
  ASSERT_EQ(OB_SUCCESS, addr_list.push_back(external_addr));
  ASSERT_EQ(OB_SUCCESS, config.assign(
      ObLogExternalAddrSource::CDC_RS_LIST, addr_list, 7));

  ObAddr route_addr;
  ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", 4882));
  ObLogClusterTopology topology;
  ASSERT_EQ(OB_SUCCESS, all_svr_cache.get_cluster_topology(topology));
  ASSERT_EQ(OB_SUCCESS, topology.resolve_cluster_route_addr(config, route_addr));
  ObAddr expected;
  ASSERT_TRUE(expected.set_ip_addr("192.0.2.10", 4882));
  ASSERT_EQ(expected, route_addr);
  ASSERT_EQ(7, config.version_);
}

TEST_F(TestStandalonePublicAddress, test_route_addr_mapping_reject_multi_server)
{
  ObLogAllSvrCache all_svr_cache;
  ObAllServerInfo all_server_info;
  ASSERT_EQ(OB_SUCCESS, all_server_info.init(1));
  for (int64_t idx = 0; idx < 2; ++idx) {
    AllServerRecord record;
    record.svr_id_ = idx + 1;
    ASSERT_TRUE(record.server_.set_ip_addr("127.0.0.1", 2882 + idx));
    record.status_ = ObServerStatus::OB_SERVER_ACTIVE;
    ASSERT_EQ(OB_SUCCESS, all_server_info.add(record));
  }
  all_svr_cache.publish_cluster_topology_(all_server_info);

  ObLogExternalAddrConfig config;
  ObArray<ObAddr> addr_list;
  ObAddr external_addr;
  ASSERT_TRUE(external_addr.set_ip_addr("192.0.2.10", 2881));
  ASSERT_EQ(OB_SUCCESS, addr_list.push_back(external_addr));
  ASSERT_EQ(OB_SUCCESS, config.assign(
      ObLogExternalAddrSource::CDC_RS_LIST, addr_list));

  ObAddr route_addr;
  ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", 4882));
  ObLogClusterTopology topology;
  ASSERT_EQ(OB_SUCCESS, all_svr_cache.get_cluster_topology(topology));
  ASSERT_EQ(OB_STATE_NOT_MATCH, topology.resolve_cluster_route_addr(config, route_addr));
}

TEST_F(TestStandalonePublicAddress, test_external_addr_config_ambiguous)
{
  ObLogExternalAddrConfig config;
  ObArray<ObAddr> addr_list;
  ObAddr addr;
  ASSERT_TRUE(addr.set_ip_addr("192.0.2.10", 2881));
  ASSERT_EQ(OB_SUCCESS, addr_list.push_back(addr));
  ASSERT_TRUE(addr.set_ip_addr("192.0.2.11", 2881));
  ASSERT_EQ(OB_SUCCESS, addr_list.push_back(addr));
  ASSERT_EQ(OB_SUCCESS, config.assign(
      ObLogExternalAddrSource::CDC_TENANT_ENDPOINT, addr_list));
  ASSERT_EQ(ObLogExternalAddrState::AMBIGUOUS, config.state_);
  ASSERT_FALSE(config.external_addr_.is_valid());
}

TEST_F(TestStandalonePublicAddress, test_tenant_sync_loopback_mapping_not_supported)
{
  ObLogClusterTopology topology;
  ObLogExternalAddrConfig config;
  config.source_ = ObLogExternalAddrSource::CDC_TENANT_ENDPOINT;
  ObAddr route_addr;
  ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_EQ(OB_NOT_SUPPORTED, topology.resolve_cluster_route_addr(config, route_addr));
}

TEST_F(TestStandalonePublicAddress, test_restore_source_loopback_mapping_without_cluster_topology)
{
  ObLogRouteService route_service;
  route_service.is_tenant_mode_ = true;

  ObArray<ObAddr> addr_list;
  ObAddr external_addr;
  ASSERT_TRUE(external_addr.set_ip_addr("192.0.2.10", 2881));
  ASSERT_EQ(OB_SUCCESS, addr_list.push_back(external_addr));
  ASSERT_EQ(OB_SUCCESS, route_service.external_addr_config_.assign(
      ObLogExternalAddrSource::RESTORE_SOURCE, addr_list));

  ObAddr route_addr;
  ObAddr expected_addr;
  ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_TRUE(expected_addr.set_ip_addr("192.0.2.10", 4882));
  ASSERT_EQ(OB_SUCCESS, route_service.resolve_route_server_addr_(route_addr));
  ASSERT_EQ(expected_addr, route_addr);
}

TEST_F(TestStandalonePublicAddress, test_restore_source_mapping_rejects_non_tenant_mode)
{
  ObLogRouteService route_service;
  route_service.is_tenant_mode_ = false;

  ObArray<ObAddr> addr_list;
  ObAddr external_addr;
  ASSERT_TRUE(external_addr.set_ip_addr("192.0.2.10", 2881));
  ASSERT_EQ(OB_SUCCESS, addr_list.push_back(external_addr));
  ASSERT_EQ(OB_SUCCESS, route_service.external_addr_config_.assign(
      ObLogExternalAddrSource::RESTORE_SOURCE, addr_list));

  ObAddr route_addr;
  ObAddr original_addr;
  ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_TRUE(original_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_EQ(OB_STATE_NOT_MATCH, route_service.resolve_route_server_addr_(route_addr));
  ASSERT_EQ(original_addr, route_addr);
}

TEST_F(TestStandalonePublicAddress, test_get_primary_ls_leader_addr_by_rpc_self)
{
  LOG_INFO("TEST: test_get_primary_ls_leader_addr_by_rpc_self");

  ObRestoreSourceServiceAttr service_attr;
  build_self_service_attr(service_attr);

  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  ObAddr leader_addr;
  ObLogRestoreProxyUtil proxy_util;
  ASSERT_EQ(OB_SUCCESS,
      proxy_util.get_primary_ls_leader_addr_by_rpc(
          service_attr, GCTX.srv_rpc_proxy_, ls_id, leader_addr));
  ASSERT_TRUE(leader_addr.is_valid());

  ObAddr sql_addr;
  ASSERT_EQ(OB_SUCCESS, service_attr.addr_.at(0).get_sql_addr(sql_addr));
  char leader_ip[MAX_IP_ADDR_LENGTH] = {0};
  char sql_ip[MAX_IP_ADDR_LENGTH] = {0};
  ASSERT_TRUE(leader_addr.ip_to_string(leader_ip, sizeof(leader_ip)));
  ASSERT_TRUE(sql_addr.ip_to_string(sql_ip, sizeof(sql_ip)));
  ASSERT_STREQ(sql_ip, leader_ip);
  LOG_INFO("primary ls leader addr", K(leader_addr), K(sql_addr));
}

// ========== 补充用例：枚举语义 / assign ==========

TEST_F(TestStandalonePublicAddress, test_deploy_mode_result_is_standalone)
{
  LOG_INFO("TEST: test_deploy_mode_result_is_standalone");

  ObQueryDeployModeInfoResult result;
  result.set_status(ObDeployMode::CENTRALIZED);
  ASSERT_TRUE(result.is_standalone());

  result.set_status(ObDeployMode::DISTRIBUTED);
  ASSERT_FALSE(result.is_standalone());

  result.set_status(ObDeployMode::INVALID_STATUS);
  ASSERT_FALSE(result.is_standalone());
}

TEST_F(TestStandalonePublicAddress, test_deploy_mode_info_assign)
{
  LOG_INFO("TEST: test_deploy_mode_info_assign");

  {
    ObQueryDeployModeInfoArg src;
    ObQueryDeployModeInfoArg dst;
    ASSERT_TRUE(src.is_valid());
    ASSERT_TRUE(dst.is_valid());
    ASSERT_EQ(OB_SUCCESS, dst.assign(src));
    ASSERT_TRUE(dst.is_valid());
  }

  {
    ObQueryDeployModeInfoResult src;
    ObQueryDeployModeInfoResult dst;
    src.set_status(ObDeployMode::CENTRALIZED);
    ASSERT_EQ(OB_SUCCESS, dst.assign(src));
    ASSERT_EQ(ObDeployMode::CENTRALIZED, dst.get_status());
    ASSERT_TRUE(dst.is_standalone());

    src.set_status(ObDeployMode::DISTRIBUTED);
    ASSERT_EQ(OB_SUCCESS, dst.assign(src));
    ASSERT_EQ(ObDeployMode::DISTRIBUTED, dst.get_status());
    ASSERT_FALSE(dst.is_standalone());
  }
}

// ========== 补充用例：set_sql_addr_and_svr_port_list 入参不一致 ==========

TEST_F(TestStandalonePublicAddress, test_set_sql_addr_and_svr_port_list_reject_mismatched_lists)
{
  LOG_INFO("TEST: test_set_sql_addr_and_svr_port_list_reject_mismatched_lists");

  ObRestoreSourceServiceAttr service_attr;
  service_attr.reset();
  ObRestoreSourceServiceAddr a;
  ObAddr addr;
  ASSERT_TRUE(addr.set_ip_addr("1.2.3.4", 2881));
  ASSERT_EQ(OB_SUCCESS, a.set_sql_addr(addr));
  ASSERT_EQ(OB_SUCCESS, service_attr.addr_.push_back(a));

  // addr_list.count() != svr_port_list.count()
  ObArray<ObAddr> addr_list;
  ObArray<int32_t> svr_port_list;
  ObAddr a1;
  ASSERT_TRUE(a1.set_ip_addr("127.0.0.1", 2881));
  ASSERT_EQ(OB_SUCCESS, addr_list.push_back(a1));
  ObAddr a2;
  ASSERT_TRUE(a2.set_ip_addr("127.0.0.1", 2882));
  ASSERT_EQ(OB_SUCCESS, addr_list.push_back(a2));
  ASSERT_EQ(OB_SUCCESS, svr_port_list.push_back(2882));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      service_attr.set_sql_addr_and_svr_port_list(addr_list, svr_port_list));

  // both empty
  ObArray<ObAddr> empty_addr_list;
  ObArray<int32_t> empty_svr_port_list;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      service_attr.set_sql_addr_and_svr_port_list(empty_addr_list, empty_svr_port_list));
}

// ========== 补充用例：standalone 模式错误路径 ==========

#ifdef OB_ENABLE_STANDALONE_LAUNCH
TEST_F(TestStandalonePublicAddress, test_query_peer_is_standalone_empty_addr)
{
  LOG_INFO("TEST: test_query_peer_is_standalone_empty_addr");

  ObRestoreSourceServiceAttr service_attr;
  service_attr.reset();
  service_attr.user_.cluster_id_ = GCONF.cluster_id;
  service_attr.user_.tenant_id_ = OB_SYS_TENANT_ID;
  service_attr.user_.mode_ = ObCompatibilityMode::MYSQL_MODE;
  STRCPY(service_attr.user_.user_name_, "root");
  STRCPY(service_attr.user_.tenant_name_, "sys");
  STRCPY(service_attr.encrypt_passwd_, "dummy");

  ASSERT_FALSE(service_attr.is_valid());
  ASSERT_TRUE(service_attr.addr_.empty());

  bool peer_is_standalone = false;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      ObLogRestoreProxyUtil::query_peer_is_standalone_by_rpc(
          service_attr, GCTX.srv_rpc_proxy_, peer_is_standalone));
}
#endif

#ifdef OB_ENABLE_STANDALONE_LAUNCH
TEST_F(TestStandalonePublicAddress, test_get_primary_ls_leader_addr_by_rpc_multi_addr)
{
  LOG_INFO("TEST: test_get_primary_ls_leader_addr_by_rpc_multi_addr");

  ObRestoreSourceServiceAttr service_attr;
  service_attr.reset();
  for (int i = 0; i < 2; ++i) {
    ObRestoreSourceServiceAddr a;
    ObAddr addr;
    ASSERT_TRUE(addr.set_ip_addr("1.2.3.4", 2881 + i));
    ASSERT_EQ(OB_SUCCESS, a.set_sql_addr(addr));
    ASSERT_EQ(OB_SUCCESS, a.set_svr_port(2882 + i));
    ASSERT_EQ(OB_SUCCESS, service_attr.addr_.push_back(a));
  }
  service_attr.user_.cluster_id_ = GCONF.cluster_id;
  service_attr.user_.tenant_id_ = OB_SYS_TENANT_ID;
  service_attr.user_.mode_ = ObCompatibilityMode::MYSQL_MODE;
  STRCPY(service_attr.user_.user_name_, "root");
  STRCPY(service_attr.user_.tenant_name_, "sys");
  STRCPY(service_attr.encrypt_passwd_, "dummy");

  ASSERT_TRUE(service_attr.is_valid());
  ASSERT_TRUE(service_attr.has_svr_port());
  ASSERT_EQ(2, service_attr.addr_.count());

  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  ObAddr leader_addr;
  ObLogRestoreProxyUtil proxy_util;
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      proxy_util.get_primary_ls_leader_addr_by_rpc(
          service_attr, GCTX.srv_rpc_proxy_, ls_id, leader_addr));
}
#endif

// ========== 补充用例:set_sql_addr_and_svr_port_list set_svr_port 失败分支 ==========

TEST_F(TestStandalonePublicAddress, test_set_sql_addr_and_svr_port_list_reject_non_positive_port)
{
  LOG_INFO("TEST: test_set_sql_addr_and_svr_port_list_reject_non_positive_port");

  ObRestoreSourceServiceAttr service_attr;
  service_attr.reset();
  ObRestoreSourceServiceAddr a;
  ObAddr addr;
  ASSERT_TRUE(addr.set_ip_addr("1.2.3.4", 2881));
  ASSERT_EQ(OB_SUCCESS, a.set_sql_addr(addr));
  ASSERT_EQ(OB_SUCCESS, service_attr.addr_.push_back(a));

  ObArray<ObAddr> addr_list;
  ObArray<int32_t> svr_port_list;
  ObAddr loopback;
  ASSERT_TRUE(loopback.set_ip_addr("127.0.0.1", 2881));
  ASSERT_EQ(OB_SUCCESS, addr_list.push_back(loopback));
  ASSERT_EQ(OB_SUCCESS, svr_port_list.push_back(0));

  ASSERT_EQ(OB_INVALID_ARGUMENT,
      service_attr.set_sql_addr_and_svr_port_list(addr_list, svr_port_list));
}

// ========== 补充用例:get_primary_ls_leader_addr_by_rpc 前置参数校验 ==========

TEST_F(TestStandalonePublicAddress, test_get_primary_ls_leader_addr_by_rpc_null_proxy)
{
  LOG_INFO("TEST: test_get_primary_ls_leader_addr_by_rpc_null_proxy");

  ObRestoreSourceServiceAttr service_attr;
  build_self_service_attr(service_attr);

  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  ObAddr leader_addr;
  ObLogRestoreProxyUtil proxy_util;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      proxy_util.get_primary_ls_leader_addr_by_rpc(
          service_attr, NULL, ls_id, leader_addr));
}

TEST_F(TestStandalonePublicAddress, test_get_primary_ls_leader_addr_by_rpc_invalid_ls_id)
{
  LOG_INFO("TEST: test_get_primary_ls_leader_addr_by_rpc_invalid_ls_id");

  ObRestoreSourceServiceAttr service_attr;
  build_self_service_attr(service_attr);

  const ObLSID invalid_ls_id;  // default → INVALID_LS_ID
  ASSERT_FALSE(invalid_ls_id.is_valid());

  ObAddr leader_addr;
  ObLogRestoreProxyUtil proxy_util;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      proxy_util.get_primary_ls_leader_addr_by_rpc(
          service_attr, GCTX.srv_rpc_proxy_, invalid_ls_id, leader_addr));
}

TEST_F(TestStandalonePublicAddress, test_get_primary_ls_leader_addr_by_rpc_invalid_attr)
{
  LOG_INFO("TEST: test_get_primary_ls_leader_addr_by_rpc_invalid_attr");

  ObRestoreSourceServiceAttr service_attr;
  service_attr.reset();
  ASSERT_FALSE(service_attr.is_valid());

  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  ObAddr leader_addr;
  ObLogRestoreProxyUtil proxy_util;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      proxy_util.get_primary_ls_leader_addr_by_rpc(
          service_attr, GCTX.srv_rpc_proxy_, ls_id, leader_addr));
}

TEST_F(TestStandalonePublicAddress, test_get_primary_ls_leader_addr_by_rpc_no_svr_port)
{
  LOG_INFO("TEST: test_get_primary_ls_leader_addr_by_rpc_no_svr_port");

  ObRestoreSourceServiceAttr service_attr;
  service_attr.reset();
  ObRestoreSourceServiceAddr a;
  ObAddr addr;
  ASSERT_TRUE(addr.set_ip_addr("1.2.3.4", 2881));
  ASSERT_EQ(OB_SUCCESS, a.set_sql_addr(addr));
  ASSERT_EQ(OB_SUCCESS, service_attr.addr_.push_back(a));
  service_attr.user_.cluster_id_ = GCONF.cluster_id;
  service_attr.user_.tenant_id_ = OB_SYS_TENANT_ID;
  service_attr.user_.mode_ = ObCompatibilityMode::MYSQL_MODE;
  STRCPY(service_attr.user_.user_name_, "root");
  STRCPY(service_attr.user_.tenant_name_, "sys");
  STRCPY(service_attr.encrypt_passwd_, "dummy");

  ASSERT_TRUE(service_attr.is_valid());
  ASSERT_FALSE(service_attr.has_svr_port());

  const ObLSID ls_id(ObLSID::SYS_LS_ID);
  ObAddr leader_addr;
  ObLogRestoreProxyUtil proxy_util;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      proxy_util.get_primary_ls_leader_addr_by_rpc(
          service_attr, GCTX.srv_rpc_proxy_, ls_id, leader_addr));
}

// ========== 补充用例:query_peer_is_standalone_by_rpc 错误路径 ==========

#ifdef OB_ENABLE_STANDALONE_LAUNCH
TEST_F(TestStandalonePublicAddress, test_query_peer_is_standalone_null_proxy)
{
  LOG_INFO("TEST: test_query_peer_is_standalone_null_proxy");

  ObRestoreSourceServiceAttr service_attr;
  build_self_service_attr(service_attr);

  bool peer_is_standalone = false;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      ObLogRestoreProxyUtil::query_peer_is_standalone_by_rpc(
          service_attr, NULL, peer_is_standalone));
}
#endif

#ifdef OB_ENABLE_STANDALONE_LAUNCH
TEST_F(TestStandalonePublicAddress, test_query_peer_is_standalone_no_svr_port)
{
  LOG_INFO("TEST: test_query_peer_is_standalone_no_svr_port");

  ObRestoreSourceServiceAttr service_attr;
  service_attr.reset();
  ObRestoreSourceServiceAddr a;
  ObAddr addr;
  ASSERT_TRUE(addr.set_ip_addr("1.2.3.4", 2881));
  ASSERT_EQ(OB_SUCCESS, a.set_sql_addr(addr));
  ASSERT_EQ(OB_SUCCESS, service_attr.addr_.push_back(a));
  service_attr.user_.cluster_id_ = GCONF.cluster_id;
  service_attr.user_.tenant_id_ = OB_SYS_TENANT_ID;
  service_attr.user_.mode_ = ObCompatibilityMode::MYSQL_MODE;
  STRCPY(service_attr.user_.user_name_, "root");
  STRCPY(service_attr.user_.tenant_name_, "sys");
  STRCPY(service_attr.encrypt_passwd_, "dummy");

  ASSERT_TRUE(service_attr.is_valid());
  ASSERT_FALSE(service_attr.has_svr_port());
  ASSERT_EQ(1, service_attr.addr_.count());

  bool peer_is_standalone = false;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      ObLogRestoreProxyUtil::query_peer_is_standalone_by_rpc(
          service_attr, GCTX.srv_rpc_proxy_, peer_is_standalone));
}
#endif

#ifdef OB_ENABLE_STANDALONE_LAUNCH
TEST_F(TestStandalonePublicAddress, test_query_peer_is_standalone_unreachable)
{
  LOG_INFO("TEST: test_query_peer_is_standalone_unreachable");

  ObRestoreSourceServiceAttr service_attr;
  service_attr.reset();
  ObRestoreSourceServiceAddr a;
  ObAddr unreachable;
  ASSERT_TRUE(unreachable.set_ip_addr("192.0.2.1", 99));
  ASSERT_EQ(OB_SUCCESS, a.set_sql_addr(unreachable));
  ASSERT_EQ(OB_SUCCESS, a.set_svr_port(99));
  ASSERT_EQ(OB_SUCCESS, service_attr.addr_.push_back(a));
  service_attr.user_.cluster_id_ = GCONF.cluster_id;
  service_attr.user_.tenant_id_ = OB_SYS_TENANT_ID;
  service_attr.user_.mode_ = ObCompatibilityMode::MYSQL_MODE;
  STRCPY(service_attr.user_.user_name_, "root");
  STRCPY(service_attr.user_.tenant_name_, "sys");
  STRCPY(service_attr.encrypt_passwd_, "dummy");

  bool peer_is_standalone = false;
  int ret = ObLogRestoreProxyUtil::query_peer_is_standalone_by_rpc(
      service_attr, GCTX.srv_rpc_proxy_, peer_is_standalone);
  ASSERT_NE(OB_SUCCESS, ret);
  ASSERT_FALSE(peer_is_standalone);
}
#endif

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  int64_t c = 0;
  char *log_level = (char*)"INFO";
  while (EOF != (c = getopt(argc, argv, "l:"))) {
    switch (c) {
    case 'l':
      log_level = optarg;
      oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
      break;
    default:
      break;
    }
  }
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(log_level);
  LOG_INFO("main>>>");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
