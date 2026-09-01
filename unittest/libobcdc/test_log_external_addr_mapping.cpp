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

#include "lib/ob_errno.h"
#include "logservice/libobcdc/src/ob_cdc_tenant_sql_server_provider.h"
#include "logservice/libobcdc/src/ob_cdc_tenant_endpoint_provider.h"
#include "logservice/logrouteservice/ob_log_all_svr_cache.h"
#include "logservice/logrouteservice/ob_log_route_service.h"

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace libobcdc;
using namespace logservice;
using namespace share;

class TestLogExternalAddrMapping : public ::testing::Test
{
public:
  static void add_server(ObAllServerInfo &all_server_info,
      const int64_t server_id,
      const char *ip,
      const int32_t port,
      const ObServerStatus::DisplayStatus status)
  {
    AllServerRecord record;
    record.svr_id_ = server_id;
    ASSERT_TRUE(record.server_.set_ip_addr(ip, port));
    record.status_ = status;
    ASSERT_EQ(OB_SUCCESS, all_server_info.add(record));
  }

  static void build_unique_config(const ObLogExternalAddrSource source,
      const char *ip,
      const int32_t port,
      ObLogExternalAddrConfig &config,
      const int64_t version = 1)
  {
    ObArray<ObAddr> addr_list;
    ObAddr addr;
    ASSERT_TRUE(addr.set_ip_addr(ip, port));
    ASSERT_EQ(OB_SUCCESS, addr_list.push_back(addr));
    ASSERT_EQ(OB_SUCCESS,
        config.assign(source, addr_list, version));
  }

  static void build_unique_config(const char *ip,
      const int32_t port,
      ObLogExternalAddrConfig &config,
      const int64_t version = 1)
  {
    build_unique_config(
        ObLogExternalAddrSource::CDC_RS_LIST, ip, port, config, version);
  }
};

TEST_F(TestLogExternalAddrMapping, test_same_ip_with_different_ports_is_unique)
{
  ObLogExternalAddrConfig config;
  ObArray<ObAddr> addr_list;
  ObAddr first_addr;
  ObAddr second_addr;
  ASSERT_TRUE(first_addr.set_ip_addr("192.0.2.10", 2881));
  ASSERT_TRUE(second_addr.set_ip_addr("192.0.2.10", 3881));
  ASSERT_EQ(OB_SUCCESS, addr_list.push_back(first_addr));
  ASSERT_EQ(OB_SUCCESS, addr_list.push_back(second_addr));

  ASSERT_EQ(OB_SUCCESS,
      config.assign(ObLogExternalAddrSource::CDC_RS_LIST, addr_list, 9));
  ASSERT_EQ(ObLogExternalAddrSource::CDC_RS_LIST, config.source_);
  ASSERT_EQ(ObLogExternalAddrState::UNIQUE, config.state_);
  ASSERT_EQ(first_addr, config.external_addr_);
  ASSERT_EQ(9, config.version_);
  ASSERT_TRUE(config.is_unique());
  ASSERT_TRUE(config.is_provided());
}

TEST_F(TestLogExternalAddrMapping, test_empty_or_invalid_address_is_invalid)
{
  ObLogExternalAddrConfig config;
  ObArray<ObAddr> addr_list;
  ASSERT_EQ(OB_SUCCESS,
      config.assign(ObLogExternalAddrSource::CDC_RS_LIST, addr_list));
  ASSERT_EQ(ObLogExternalAddrState::INVALID, config.state_);
  ASSERT_FALSE(config.external_addr_.is_valid());
  ASSERT_TRUE(config.is_provided());
  ASSERT_FALSE(config.is_unique());

  ObAddr valid_addr;
  ObAddr invalid_addr;
  ASSERT_TRUE(valid_addr.set_ip_addr("192.0.2.10", 2881));
  ASSERT_EQ(OB_SUCCESS, addr_list.push_back(valid_addr));
  ASSERT_EQ(OB_SUCCESS, addr_list.push_back(invalid_addr));
  ASSERT_EQ(OB_SUCCESS,
      config.assign(ObLogExternalAddrSource::CDC_RS_LIST, addr_list));
  ASSERT_EQ(ObLogExternalAddrState::INVALID, config.state_);
  ASSERT_FALSE(config.external_addr_.is_valid());
}

TEST_F(TestLogExternalAddrMapping, test_topology_not_ready_or_empty_need_retry)
{
  ObLogExternalAddrConfig config;
  build_unique_config("192.0.2.10", 2881, config);
  ObAddr route_addr;
  ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", 4882));

  ObLogClusterTopology topology;
  ASSERT_EQ(OB_NEED_RETRY, topology.resolve_cluster_route_addr(config, route_addr));

  ObLogAllSvrCache all_svr_cache;
  ObAllServerInfo all_server_info;
  ASSERT_EQ(OB_SUCCESS, all_server_info.init(1));
  all_svr_cache.publish_cluster_topology_(all_server_info);
  ASSERT_EQ(OB_SUCCESS, all_svr_cache.get_cluster_topology(topology));
  ASSERT_TRUE(topology.is_ready_);
  ASSERT_EQ(0, topology.active_server_count_);
  ASSERT_EQ(OB_NEED_RETRY, topology.resolve_cluster_route_addr(config, route_addr));
}

TEST_F(TestLogExternalAddrMapping, test_non_loopback_only_server_rejects_loopback_route)
{
  ObLogAllSvrCache all_svr_cache;
  ObAllServerInfo all_server_info;
  ASSERT_EQ(OB_SUCCESS, all_server_info.init(1));
  add_server(all_server_info, 1, "10.0.0.1", 2882, ObServerStatus::OB_SERVER_ACTIVE);
  all_svr_cache.publish_cluster_topology_(all_server_info);

  ObLogClusterTopology topology;
  ObLogExternalAddrConfig config;
  ObAddr route_addr;
  build_unique_config("192.0.2.10", 2881, config);
  ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_EQ(OB_SUCCESS, all_svr_cache.get_cluster_topology(topology));
  ASSERT_EQ(1, topology.active_server_count_);
  ASSERT_FALSE(topology.only_server_.is_loopback());
  ASSERT_EQ(OB_STATE_NOT_MATCH, topology.resolve_cluster_route_addr(config, route_addr));
}

TEST_F(TestLogExternalAddrMapping, test_deleting_server_is_counted)
{
  ObLogAllSvrCache all_svr_cache;
  ObAllServerInfo all_server_info;
  ASSERT_EQ(OB_SUCCESS, all_server_info.init(1));
  add_server(all_server_info, 1, "127.0.0.1", 2882, ObServerStatus::OB_SERVER_DELETING);
  all_svr_cache.publish_cluster_topology_(all_server_info);

  ObLogClusterTopology topology;
  ObLogExternalAddrConfig config;
  ObAddr route_addr;
  ObAddr expected_addr;
  build_unique_config("192.0.2.10", 2881, config);
  ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_TRUE(expected_addr.set_ip_addr("192.0.2.10", 4882));
  ASSERT_EQ(OB_SUCCESS, all_svr_cache.get_cluster_topology(topology));
  ASSERT_EQ(1, topology.active_server_count_);
  ASSERT_EQ(OB_SUCCESS, topology.resolve_cluster_route_addr(config, route_addr));
  ASSERT_EQ(expected_addr, route_addr);
}

TEST_F(TestLogExternalAddrMapping, test_mixed_multi_server_topology_keeps_loopback_route)
{
  ObLogAllSvrCache all_svr_cache;
  ObAllServerInfo all_server_info;
  ASSERT_EQ(OB_SUCCESS, all_server_info.init(1));
  add_server(all_server_info, 1, "127.0.0.1", 2882, ObServerStatus::OB_SERVER_ACTIVE);
  add_server(all_server_info, 2, "10.0.0.2", 2882, ObServerStatus::OB_SERVER_ACTIVE);
  all_svr_cache.publish_cluster_topology_(all_server_info);

  ObLogClusterTopology topology;
  ObLogExternalAddrConfig config;
  ObAddr route_addr;
  ObAddr expected_addr;
  build_unique_config("192.0.2.10", 2881, config);
  ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_TRUE(expected_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_EQ(OB_SUCCESS, all_svr_cache.get_cluster_topology(topology));
  ASSERT_EQ(2, topology.active_server_count_);
  ASSERT_FALSE(topology.only_server_.is_valid());
  ASSERT_EQ(OB_SUCCESS, topology.resolve_cluster_route_addr(config, route_addr));
  ASSERT_EQ(expected_addr, route_addr);
}

TEST_F(TestLogExternalAddrMapping, test_multi_loopback_server_topology_keeps_observer_routes)
{
  ObLogAllSvrCache all_svr_cache;
  ObAllServerInfo all_server_info;
  ASSERT_EQ(OB_SUCCESS, all_server_info.init(1));
  add_server(all_server_info, 1, "127.0.0.1", 2882, ObServerStatus::OB_SERVER_ACTIVE);
  add_server(all_server_info, 2, "127.0.0.1", 3882, ObServerStatus::OB_SERVER_ACTIVE);
  add_server(all_server_info, 3, "127.0.0.1", 4882, ObServerStatus::OB_SERVER_ACTIVE);
  all_svr_cache.publish_cluster_topology_(all_server_info);

  ObLogClusterTopology topology;
  ObLogExternalAddrConfig config;
  ObAddr first_route_addr;
  ObAddr second_route_addr;
  ObAddr third_route_addr;
  ObAddr expected_first_addr;
  ObAddr expected_second_addr;
  ObAddr expected_third_addr;
  build_unique_config("192.0.2.10", 2881, config);
  ASSERT_TRUE(first_route_addr.set_ip_addr("127.0.0.1", 2882));
  ASSERT_TRUE(second_route_addr.set_ip_addr("127.0.0.1", 3882));
  ASSERT_TRUE(third_route_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_TRUE(expected_first_addr.set_ip_addr("127.0.0.1", 2882));
  ASSERT_TRUE(expected_second_addr.set_ip_addr("127.0.0.1", 3882));
  ASSERT_TRUE(expected_third_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_EQ(OB_SUCCESS, all_svr_cache.get_cluster_topology(topology));
  ASSERT_EQ(3, topology.active_server_count_);
  ASSERT_FALSE(topology.only_server_.is_valid());
  ASSERT_EQ(OB_SUCCESS, topology.resolve_cluster_route_addr(config, first_route_addr));
  ASSERT_EQ(OB_SUCCESS, topology.resolve_cluster_route_addr(config, second_route_addr));
  ASSERT_EQ(OB_SUCCESS, topology.resolve_cluster_route_addr(config, third_route_addr));
  ASSERT_EQ(expected_first_addr, first_route_addr);
  ASSERT_EQ(expected_second_addr, second_route_addr);
  ASSERT_EQ(expected_third_addr, third_route_addr);
}

TEST_F(TestLogExternalAddrMapping, test_route_service_multi_server_keeps_loopback_routes)
{
  ObLogRouteService route_service;
  ObAllServerInfo all_server_info;
  ASSERT_EQ(OB_SUCCESS, all_server_info.init(1));
  add_server(all_server_info, 1, "10.0.0.1", 2882, ObServerStatus::OB_SERVER_ACTIVE);
  add_server(all_server_info, 2, "10.0.0.1", 3882, ObServerStatus::OB_SERVER_ACTIVE);
  add_server(all_server_info, 3, "10.0.0.1", 4882, ObServerStatus::OB_SERVER_ACTIVE);
  route_service.all_svr_cache_.publish_cluster_topology_(all_server_info);
  build_unique_config("192.0.2.10", 2881, route_service.external_addr_config_);

  ObLogClusterTopology topology;
  ASSERT_EQ(OB_SUCCESS, route_service.all_svr_cache_.get_cluster_topology(topology));
  ASSERT_TRUE(topology.is_ready_);
  ASSERT_EQ(3, topology.active_server_count_);

  const int32_t rpc_ports[] = {2882, 3882, 4882};
  for (int64_t idx = 0; idx < ARRAYSIZEOF(rpc_ports); ++idx) {
    ObAddr route_addr;
    ObAddr expected_addr;
    ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", rpc_ports[idx]));
    ASSERT_TRUE(expected_addr.set_ip_addr("127.0.0.1", rpc_ports[idx]));
    ASSERT_EQ(OB_SUCCESS, route_service.resolve_route_server_addr_(route_addr));
    ASSERT_EQ(expected_addr, route_addr);
  }
}

TEST_F(TestLogExternalAddrMapping, test_route_service_non_loopback_route_is_kept)
{
  ObLogRouteService route_service;
  route_service.is_tenant_mode_ = false;
  build_unique_config(ObLogExternalAddrSource::RESTORE_SOURCE,
      "192.0.2.10", 2881, route_service.external_addr_config_);

  ObAddr route_addr;
  ObAddr expected_addr;
  ASSERT_TRUE(route_addr.set_ip_addr("10.0.0.2", 4882));
  ASSERT_TRUE(expected_addr.set_ip_addr("10.0.0.2", 4882));
  ASSERT_EQ(OB_SUCCESS, route_service.resolve_route_server_addr_(route_addr));
  ASSERT_EQ(expected_addr, route_addr);
}

#ifndef OB_ENABLE_STANDALONE_LAUNCH
TEST_F(TestLogExternalAddrMapping, test_route_service_without_external_config_keeps_loopback)
{
  ObLogRouteService route_service;
  ObAddr route_addr;
  ObAddr expected_addr;
  ASSERT_FALSE(route_service.external_addr_config_.is_provided());
  ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_TRUE(expected_addr.set_ip_addr("127.0.0.1", 4882));

  ASSERT_EQ(OB_SUCCESS, route_service.resolve_route_server_addr_(route_addr));
  ASSERT_EQ(expected_addr, route_addr);
}
#endif

TEST_F(TestLogExternalAddrMapping, test_route_service_single_loopback_server_is_mapped)
{
  ObLogRouteService route_service;
  ObAllServerInfo all_server_info;
  ASSERT_EQ(OB_SUCCESS, all_server_info.init(1));
  add_server(all_server_info, 1, "127.0.0.1", 2882, ObServerStatus::OB_SERVER_ACTIVE);
  route_service.all_svr_cache_.publish_cluster_topology_(all_server_info);
  build_unique_config("192.0.2.10", 2881, route_service.external_addr_config_, 7);

  ObAddr route_addr;
  ObAddr expected_addr;
  ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_TRUE(expected_addr.set_ip_addr("192.0.2.10", 4882));
  ASSERT_EQ(OB_SUCCESS, route_service.resolve_route_server_addr_(route_addr));
  ASSERT_EQ(expected_addr, route_addr);
  ASSERT_EQ(7, route_service.external_addr_config_.version_);
}

TEST_F(TestLogExternalAddrMapping, test_route_service_topology_not_ready_need_retry)
{
  ObLogRouteService route_service;
  build_unique_config("192.0.2.10", 2881, route_service.external_addr_config_);

  ObAddr route_addr;
  ObAddr expected_addr;
  ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_TRUE(expected_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_EQ(OB_NEED_RETRY, route_service.resolve_route_server_addr_(route_addr));
  ASSERT_EQ(expected_addr, route_addr);
}

TEST_F(TestLogExternalAddrMapping, test_route_service_tenant_endpoint_rejects_loopback)
{
  ObLogRouteService route_service;
  build_unique_config(ObLogExternalAddrSource::CDC_TENANT_ENDPOINT,
      "192.0.2.10", 2881, route_service.external_addr_config_);

  ObAddr route_addr;
  ObAddr expected_addr;
  ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_TRUE(expected_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_EQ(OB_NOT_SUPPORTED, route_service.resolve_route_server_addr_(route_addr));
  ASSERT_EQ(expected_addr, route_addr);
}

TEST_F(TestLogExternalAddrMapping, test_route_service_restore_source_maps_in_tenant_mode)
{
  ObLogRouteService route_service;
  route_service.is_tenant_mode_ = true;
  build_unique_config(ObLogExternalAddrSource::RESTORE_SOURCE,
      "192.0.2.10", 2881, route_service.external_addr_config_);

  ObAddr route_addr;
  ObAddr expected_addr;
  ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_TRUE(expected_addr.set_ip_addr("192.0.2.10", 4882));
  ASSERT_EQ(OB_SUCCESS, route_service.resolve_route_server_addr_(route_addr));
  ASSERT_EQ(expected_addr, route_addr);
}

TEST_F(TestLogExternalAddrMapping, test_route_service_restore_source_rejects_non_tenant_mode)
{
  ObLogRouteService route_service;
  route_service.is_tenant_mode_ = false;
  build_unique_config(ObLogExternalAddrSource::RESTORE_SOURCE,
      "192.0.2.10", 2881, route_service.external_addr_config_);

  ObAddr route_addr;
  ObAddr expected_addr;
  ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_TRUE(expected_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_EQ(OB_STATE_NOT_MATCH, route_service.resolve_route_server_addr_(route_addr));
  ASSERT_EQ(expected_addr, route_addr);
}

TEST_F(TestLogExternalAddrMapping, test_loopback_external_address_allows_noop)
{
  ObLogAllSvrCache all_svr_cache;
  ObAllServerInfo all_server_info;
  ASSERT_EQ(OB_SUCCESS, all_server_info.init(1));
  add_server(all_server_info, 1, "127.0.0.1", 2882, ObServerStatus::OB_SERVER_ACTIVE);
  all_svr_cache.publish_cluster_topology_(all_server_info);

  ObLogClusterTopology topology;
  ObLogExternalAddrConfig config;
  ObAddr route_addr;
  ObAddr expected_addr;
  build_unique_config("127.0.0.1", 2881, config);
  ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_TRUE(expected_addr.set_ip_addr("127.0.0.1", 4882));
  ASSERT_EQ(OB_SUCCESS, all_svr_cache.get_cluster_topology(topology));
  ASSERT_EQ(OB_SUCCESS, topology.resolve_cluster_route_addr(config, route_addr));
  ASSERT_EQ(expected_addr, route_addr);
}

TEST_F(TestLogExternalAddrMapping, test_unusable_config_states_reject_mapping)
{
  ObLogClusterTopology topology;
  topology.active_server_count_ = 1;
  ASSERT_TRUE(topology.only_server_.set_ip_addr("127.0.0.1", 2882));
  topology.is_ready_ = true;

  ObLogExternalAddrConfig config;
  ObAddr route_addr;
  ASSERT_TRUE(route_addr.set_ip_addr("127.0.0.1", 4882));

  ObArray<ObAddr> addr_list;
  ASSERT_EQ(OB_SUCCESS,
      config.assign(ObLogExternalAddrSource::CDC_RS_LIST, addr_list));
  ASSERT_EQ(ObLogExternalAddrState::INVALID, config.state_);
  ASSERT_EQ(OB_NOT_SUPPORTED, topology.resolve_cluster_route_addr(config, route_addr));

  ObAddr first_addr;
  ObAddr second_addr;
  ASSERT_TRUE(first_addr.set_ip_addr("192.0.2.10", 2881));
  ASSERT_TRUE(second_addr.set_ip_addr("192.0.2.11", 2881));
  ASSERT_EQ(OB_SUCCESS, addr_list.push_back(first_addr));
  ASSERT_EQ(OB_SUCCESS, addr_list.push_back(second_addr));
  ASSERT_EQ(OB_SUCCESS,
      config.assign(ObLogExternalAddrSource::CDC_RS_LIST, addr_list));
  ASSERT_EQ(ObLogExternalAddrState::AMBIGUOUS, config.state_);
  ASSERT_EQ(OB_NOT_SUPPORTED, topology.resolve_cluster_route_addr(config, route_addr));
}

TEST_F(TestLogExternalAddrMapping, test_tenant_endpoint_config_has_source_without_candidate)
{
  ObCDCEndpointProvider provider;
  ObLogExternalAddrConfig config;
  provider.is_inited_ = true;
  const int ret = provider.get_external_addr_config(config);
  provider.is_inited_ = false;

  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObLogExternalAddrSource::CDC_TENANT_ENDPOINT, config.source_);
  ASSERT_EQ(ObLogExternalAddrState::NOT_PROVIDED, config.state_);
  ASSERT_FALSE(config.external_addr_.is_valid());
  ASSERT_EQ(0, config.version_);
  ASSERT_FALSE(config.is_unique());
  ASSERT_FALSE(config.is_provided());
}

TEST_F(TestLogExternalAddrMapping, test_single_loopback_sql_server_is_mapped)
{
  ObCDCTenantSQLServerProvider provider;
  ObLogExternalAddrConfig config;
  ObAddr inner_sql_server;
  ObAddr expected_sql_server;
  build_unique_config("192.0.2.10", 2881, config);
  ASSERT_TRUE(inner_sql_server.set_ip_addr("127.0.0.1", 3881));
  ASSERT_TRUE(expected_sql_server.set_ip_addr("192.0.2.10", 3881));
  ASSERT_EQ(OB_SUCCESS, provider.server_list_.push_back(inner_sql_server));
  provider.external_sql_addr_ = config.external_addr_;
  provider.is_standalone_sql_topology_ = true;

  provider.map_standalone_sql_server_(provider.server_list_);

  ASSERT_EQ(1, provider.server_list_.count());
  ASSERT_EQ(expected_sql_server, provider.server_list_.at(0));
}

TEST_F(TestLogExternalAddrMapping, test_sql_server_mapping_requires_standalone_topology)
{
  ObCDCTenantSQLServerProvider provider;
  ObLogExternalAddrConfig config;
  ObAddr first_sql_server;
  ObAddr second_sql_server;
  build_unique_config("192.0.2.10", 2881, config);
  ASSERT_TRUE(first_sql_server.set_ip_addr("127.0.0.1", 3881));
  ASSERT_TRUE(second_sql_server.set_ip_addr("10.0.0.2", 3881));
  ASSERT_EQ(OB_SUCCESS, provider.server_list_.push_back(first_sql_server));
  ASSERT_EQ(OB_SUCCESS, provider.server_list_.push_back(second_sql_server));
  provider.external_sql_addr_ = config.external_addr_;
  provider.is_standalone_sql_topology_ = true;

  provider.map_standalone_sql_server_(provider.server_list_);

  ASSERT_EQ(first_sql_server, provider.server_list_.at(0));
  ASSERT_EQ(second_sql_server, provider.server_list_.at(1));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
