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

#define USING_LOG_PREFIX RS

#include "gtest/gtest.h"
#include <gmock/gmock.h>
#define private public
#define protected public
#include "../share/schema/db_initializer.h"
#include "lib/stat/ob_session_stat.h"
#include "share/cache/ob_kv_storecache.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_leader_coordinator.h"
#include "share/config/ob_server_config.h"
#include "share/ob_lease_struct.h"
#include "share/ob_rpc_struct.h"
#include "../share/partition_table/fake_part_property_getter.h"
#include "share/ob_tenant_mgr.h"
#undef private
#undef protected
#include "rpc/mock_ob_srv_rpc_proxy.h"
#include "mock_freeze_info_manager.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;
using namespace host;
namespace rootserver
{
class ObFakeServerChangeCB : public ObIServerChangeCallback
{
public:
  ObFakeServerChangeCB() {}
  virtual ~ObFakeServerChangeCB() {}
  virtual int on_server_change()
  {
    return OB_SUCCESS;
  }
};


class ObFakeCB : public ObIStatusChangeCallback
{
public:
  ObFakeCB() : return_success_(true) {}
  int wakeup_balancer() { return OB_SUCCESS; }
  int wakeup_daily_merger() { return OB_SUCCESS; }
  int on_server_status_change(const common::ObAddr &server);
  int on_start_server(const common::ObAddr &server) {UNUSED(server); return OB_SUCCESS;}
  int on_stop_server(const common::ObAddr &server) {UNUSED(server); return OB_SUCCESS;}
  void set_return_success(const bool return_success) { return_success_ = return_success; }
private:
  bool return_success_;
};

int ObFakeCB::on_server_status_change(const common::ObAddr &server)
{
  UNUSED(server);
  int ret = OB_SUCCESS;
  if (return_success_) {
    ret = OB_SUCCESS;
  } else {
    ret = OB_ERROR;
  }
  return ret;
}

class FakeZoneManager : public ObZoneManager
{
public:
  FakeZoneManager() {}
  virtual ~FakeZoneManager() {}
  virtual int check_zone_active(const common::ObZone &zone, bool &zone_active) const
  {
    zone_active = (zone == "1" || zone == "2");
    return OB_SUCCESS;
  }
};

class FakeUnitManager : public ObUnitManager
{
public:
  FakeUnitManager(ObServerManager &server_mgr, ObZoneManager &zone_mgr)
    : ObUnitManager(server_mgr, zone_mgr)
  {
  }
  virtual ~FakeUnitManager() {}
  virtual int migrate_out_units(const ObAddr &server, const ObIArray<ObAddr> &, bool &empty)
  {
    ObAddr inner_server;
    inner_server.set_ip_addr("127.0.0.1", 9999);
    if (server == inner_server) {
      empty = false;
    } else {
      empty = true;
    }
    return OB_SUCCESS;
  }

  virtual int cancel_migrate_out_units(const common::ObAddr &server)
  {
    UNUSED(server);
    return OB_SUCCESS;
  }

  virtual int check_server_empty(const common::ObAddr &server, bool &empty) const
  {
    ObAddr inner_server;
    inner_server.set_ip_addr("127.0.0.1", 9999);
    if (server == inner_server) {
      empty = true;
    } else {
      empty = false;
    }
    return OB_SUCCESS;
  }
};

class FakeLeaderCoordinator : public ObLeaderCoordinator
{
public:
  virtual void signal();
};

void FakeLeaderCoordinator::signal()
{
  //do nothing
}

ObServerConfig &config_ = ObServerConfig::get_instance();
class TestServerManager : public ::testing::Test
{
public:
  TestServerManager();
  virtual ~TestServerManager();
  virtual void SetUp();
  virtual void TearDown();
protected:
  ObFakeCB cb_;
  ObFakeServerChangeCB cb2_;
  MockObSrvRpcProxy rpc_proxy_;
  MockFreezeInfoManager freeze_info_manager_;
  ObServerManager server_manager_;
  FakeZoneManager zone_mgr_;
  FakeUnitManager unit_mgr_;
  FakeLeaderCoordinator leader_coordinator_;
  DBInitializer db_initer_;
  uint64_t server_id_;
  void build_status(ObServerStatus::DisplayStatus status, const int64_t now,
                    const ObZone &zone, const ObAddr &server,
                    ObServerStatus &server_status);
};

TestServerManager::TestServerManager()
  : cb_(), cb2_(), server_manager_(), zone_mgr_(), unit_mgr_(server_manager_, zone_mgr_),
    server_id_(OB_INIT_SERVER_ID)
{
}

TestServerManager::~TestServerManager()
{
}

template <typename T>
  ObArray<T> to_array(const T &t) { ObArray<T> res; res.push_back(t); return res; }

void TestServerManager::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
}

void TestServerManager::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, db_initer_.init());
  ObKVGlobalCache::get_instance().init();


  ASSERT_EQ(OB_SUCCESS, db_initer_.create_system_table(false));
  ASSERT_EQ(OB_SUCCESS, db_initer_.fill_sys_stat_table(OB_SYS_TENANT_ID));

  const int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
  ObAddr rs_addr = F;
  ASSERT_EQ(OB_SUCCESS, server_manager_.init(cb_, cb2_, db_initer_.get_sql_proxy(), unit_mgr_,
                                             zone_mgr_, leader_coordinator_, config_, rs_addr,
                                             rpc_proxy_));
  server_manager_.reset();
  ObServerManager::ObServerStatusArray server_statuses;
  ObServerStatus server_status;
  ObAddr server;
  // offline server status
  server_status.reset();
  server = A;
  build_status(ObServerStatus::OB_SERVER_INACTIVE, now, "1", server, server_status);
  ASSERT_EQ(OB_SUCCESS, server_statuses.push_back(server_status));
  // serving server status
  server_status.reset();
  server = B;
  build_status(ObServerStatus::OB_SERVER_ACTIVE, now, "1", server, server_status);
  ASSERT_EQ(OB_SUCCESS, server_statuses.push_back(server_status));
  // delete server status
  server_status.reset();
  server = C;
  build_status(ObServerStatus::OB_SERVER_DELETING, now, "1", server, server_status);
  ASSERT_EQ(OB_SUCCESS, server_statuses.push_back(server_status));
  ASSERT_EQ(OB_SUCCESS, server_manager_.load_server_statuses(server_statuses));
  ASSERT_EQ(true, server_manager_.has_build());

  // ObServerManager::load_server_status() will set heartbeat status to lease_expired if alive,
  // set B to alive manually.
  server = B;
  ObServerStatus ss;
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(server, ss));
  ss.hb_status_ = ObServerStatus::OB_HEARTBEAT_ALIVE;
  ASSERT_EQ(OB_SUCCESS, server_manager_.update_server_status(ss));
}

void TestServerManager::build_status(ObServerStatus::DisplayStatus status, const int64_t now,
                                     const ObZone &zone, const ObAddr &server,
                                     ObServerStatus &server_status)
{
  if (ObServerStatus::OB_SERVER_INACTIVE == status) {
    // lease outdate 4s
    server_status.last_hb_time_ = now - config_.lease_time - 4000000;
    server_status.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_NORMAL;
    server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED;
  } else if (ObServerStatus::OB_SERVER_ACTIVE == status) {
    // 1s
    server_status.last_hb_time_ = now - 1000000;
    server_status.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_NORMAL;
    server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_ALIVE;
  } else if (ObServerStatus::OB_SERVER_DELETING == status) {
    server_status.last_hb_time_ = 0;
    server_status.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_DELETING;
    server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED;
  }
  ASSERT_TRUE(strlen("dev1.0") == snprintf(server_status.build_version_, OB_SERVER_VERSION_LENGTH,
      "%s", "dev1.0"));
  server_status.zone_ = zone;
  server_status.server_ = server;
  server_status.id_ = server_id_;
  ++server_id_;
}

TEST_F(TestServerManager, basic)
{
  ObServerManager server_mgr;
  ObServerInfoList server_info_list;
  uint64_t server_id = OB_INVALID_ID;
  bool to_alive = false;
  bool update_delay_time_flag = false;
  ASSERT_EQ(OB_NOT_INIT, server_mgr.add_server_list(server_info_list, server_id));
  ObLeaseRequest lq;
  ASSERT_EQ(OB_NOT_INIT, server_mgr.receive_hb(lq, server_id, to_alive, update_delay_time_flag));
  ObAddr server;
  server = B;
  bool is_alive = false;
  ASSERT_EQ(OB_NOT_INIT, server_mgr.check_server_alive(server, is_alive));
  ObZone zone;
  int64_t count = 0;
  ObServerManager::ObServerArray servers;
  ASSERT_EQ(OB_NOT_INIT, server_mgr.get_alive_servers(zone, servers));
  ASSERT_EQ(OB_NOT_INIT, server_mgr.get_alive_server_count(zone, count));
  ASSERT_EQ(OB_NOT_INIT, server_mgr.get_servers_of_zone(zone, servers));
  ASSERT_EQ(OB_NOT_INIT, server_mgr.check_servers());
  ObServerStatus status;
  ASSERT_EQ(OB_NOT_INIT, server_mgr.get_server_status(server, status));
  ASSERT_EQ(OB_NOT_INIT, server_mgr.update_server_status(status));
  ObServerManager::ObServerStatusArray statuses;
  ASSERT_EQ(OB_NOT_INIT, server_mgr.load_server_statuses(statuses));
  ASSERT_EQ(OB_NOT_INIT, server_mgr.get_server_statuses(zone, statuses));
  ASSERT_EQ(OB_NOT_INIT, server_mgr.get_server_zone(server, zone));
  ASSERT_EQ(OB_NOT_INIT, server_mgr.add_server(server, zone));
  ASSERT_EQ(OB_NOT_INIT, server_mgr.delete_server(to_array(server), zone));
  ASSERT_EQ(OB_NOT_INIT, server_mgr.end_delete_server(server, zone, true));
  ASSERT_EQ(OB_NOT_INIT, server_mgr.start_server(server, zone));
  ASSERT_EQ(OB_NOT_INIT, server_mgr.stop_server(server, zone));
  server_mgr.reset();
  LOG_INFO("server manager", K(server_mgr));
}

#define GET_STR(fun, ...) ({ const char *__str = NULL; ret = fun(__VA_ARGS__, __str); __str;})
#define GET_STATUS(fun, ...) ({ ObServerStatus::DisplayStatus __s; ret = fun(__VA_ARGS__, __s); __s; })
TEST_F(TestServerManager, server_status_func)
{
  ObServerStatus server_status;
  server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED;
  server_status.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_NORMAL;

  int ret = OB_SUCCESS;
  ASSERT_STREQ("inactive", GET_STR(ObServerStatus::display_status_str, server_status.get_display_status()));
  ASSERT_EQ(server_status.get_display_status(), GET_STATUS(ObServerStatus::str2display_status, "inactive"));

  server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_ALIVE;
  ASSERT_STREQ("active", GET_STR(ObServerStatus::display_status_str, server_status.get_display_status()));
  ASSERT_EQ(server_status.get_display_status(), GET_STATUS(ObServerStatus::str2display_status, "active"));

  server_status.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_DELETING;
  ASSERT_STREQ("deleting", GET_STR(ObServerStatus::display_status_str, server_status.get_display_status()));
  ASSERT_EQ(server_status.get_display_status(), GET_STATUS(ObServerStatus::str2display_status, "deleting"));

  ASSERT_STREQ(NULL, GET_STR(ObServerStatus::display_status_str, ObServerStatus::OB_DISPLAY_MAX));
  ASSERT_NE(OB_SUCCESS, ret);

  ASSERT_EQ(ObServerStatus::OB_DISPLAY_MAX, GET_STATUS(ObServerStatus::str2display_status, "randomxxx"));
  ASSERT_NE(OB_SUCCESS, ret);

  ASSERT_FALSE(server_status.is_valid());
  server_status.server_ = B;
  server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_MAX;
  ASSERT_FALSE(server_status.is_valid());
  server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_ALIVE;
  ASSERT_FALSE(server_status.is_valid());
  server_status.id_ = OB_INIT_SERVER_ID;
  ASSERT_TRUE(server_status.is_valid());
}

TEST_F(TestServerManager, receive_hb)
{
  int64_t alive_count = 0;
  bool is_alive = false;
  bool to_alive = false;
  bool update_delay_time_flag = false;
  ObServerManager::ObServerArray server_list;
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_alive_servers("1", server_list));
  ASSERT_EQ(1, server_list.count());
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_alive_server_count("1", alive_count));
  ASSERT_EQ(1, alive_count);

  ObLeaseRequest lq;
  uint64_t server_id = OB_INVALID_ID;
  // receive hb of not exist server
  lq.server_ = A1;
  lq.zone_ = "1";
  lq.inner_port_ = 3306;
  lq.resource_info_.cpu_ = 1;
  lq.resource_info_.mem_in_use_ = 0;
  lq.resource_info_.mem_total_ = 1024;
  lq.resource_info_.disk_in_use_ = 0;
  lq.resource_info_.disk_total_ = 1024;
  ASSERT_TRUE(strlen("dev1.0") == snprintf(lq.build_version_, OB_SERVER_VERSION_LENGTH,
      "%s", "dev1.0"));
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_server_alive(lq.server_, is_alive));
  ASSERT_FALSE(is_alive);
  ASSERT_EQ(OB_SERVER_NOT_IN_WHITE_LIST, server_manager_.receive_hb(lq, server_id, to_alive, update_delay_time_flag));
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_server_alive(lq.server_, is_alive));
  ASSERT_FALSE(is_alive);

  // receive hb of non exist server
  lq.server_ = D;
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_server_alive(lq.server_, is_alive));
  ASSERT_FALSE(is_alive);
  ASSERT_EQ(OB_SERVER_NOT_IN_WHITE_LIST, server_manager_.receive_hb(lq, server_id, to_alive, update_delay_time_flag));
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_server_alive(lq.server_, is_alive));
  ASSERT_FALSE(is_alive);

  // receive hb of offline server
  lq.server_ = A;
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_server_alive(lq.server_, is_alive));
  ASSERT_FALSE(is_alive);
  ASSERT_EQ(OB_SUCCESS, server_manager_.receive_hb(lq, server_id, to_alive, update_delay_time_flag));
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_server_alive(lq.server_, is_alive));
  ASSERT_TRUE(is_alive);

  // receive hb of serving server
  lq.server_ = B;
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_server_alive(lq.server_, is_alive));
  ASSERT_TRUE(is_alive);
  ASSERT_EQ(OB_SUCCESS, server_manager_.receive_hb(lq, server_id, to_alive, update_delay_time_flag));
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_server_alive(lq.server_, is_alive));
  ASSERT_TRUE(is_alive);

  // receive hb of serving but svn version change server
  ASSERT_TRUE(strlen("dev1.111") == snprintf(lq.build_version_, OB_SERVER_VERSION_LENGTH,
      "%s", "dev1.111"));
  lq.server_ = B;
  ASSERT_EQ(OB_SUCCESS, server_manager_.receive_hb(lq, server_id, to_alive, update_delay_time_flag));
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_server_alive(lq.server_, is_alive));
  ASSERT_TRUE(is_alive);

  ASSERT_EQ(OB_SUCCESS, server_manager_.get_alive_servers("1", server_list));
  ASSERT_EQ(2, server_list.count());
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_alive_server_count("1", alive_count));
  ASSERT_EQ(2, alive_count);

  // rs change, so rs_addr change, server with root_server change from false to true
  MockObSrvRpcProxy rpc_proxy;
  MockFreezeInfoManager freeze_info_manager;
  ObServerManager temp_server_manager;
  ASSERT_EQ(OB_SUCCESS, temp_server_manager.init(cb_, cb2_, db_initer_.get_sql_proxy(), unit_mgr_,
                                                 zone_mgr_, leader_coordinator_, config_, lq.server_,
                                                 rpc_proxy));
  ObArray<ObServerStatus> statuses;
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_statuses(ObZone(""), statuses));
  ASSERT_EQ(OB_SUCCESS, temp_server_manager.load_server_statuses(statuses));
  ASSERT_EQ(OB_SUCCESS, temp_server_manager.receive_hb(lq, server_id, to_alive, update_delay_time_flag));
  ASSERT_EQ(OB_SUCCESS, temp_server_manager.check_server_alive(lq.server_, is_alive));
  ASSERT_TRUE(is_alive);
  ObServerStatus status;
  ASSERT_EQ(OB_SUCCESS, temp_server_manager.get_server_status(lq.server_, status));
  ASSERT_TRUE(status.with_rootserver_);
}

TEST_F(TestServerManager, get_set_server_status)
{
  ObServerStatus server_status;
  ObAddr server;
  server = B;
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(server, server_status));
  ASSERT_TRUE(server_status.is_active());
  server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED;
  ASSERT_EQ(OB_SUCCESS, server_manager_.update_server_status(server_status));
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(server, server_status));
  ASSERT_FALSE(server_status.is_active());

  // not exist server status
  server = E;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, server_manager_.get_server_status(server, server_status));
  server_status.server_ = server;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, server_manager_.update_server_status(server_status));
}

TEST_F(TestServerManager, check_servers)
{
  ObServerStatus server_status;
  const int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
  ObAddr server;
  server = A;
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(server, server_status));
  server_status.last_hb_time_ = now;
  ASSERT_EQ(OB_SUCCESS, server_manager_.update_server_status(server_status));

  server = B;
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(server, server_status));
  server_status.last_hb_time_ = now - config_.lease_time - 4000000L;
  ASSERT_EQ(OB_SUCCESS, server_manager_.update_server_status(server_status));

  ASSERT_EQ(OB_SUCCESS, server_manager_.check_servers());
  server = B;
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(server, server_status));
  ASSERT_FALSE(server_status.is_alive());

  // block migrate in time
  server = A;
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(server, server_status));
  server_status.block_migrate_in_time_ = now - config_.migration_disable_time- 1;
  ASSERT_EQ(OB_SUCCESS, server_manager_.update_server_status(server_status));
  bool migrate_in_blocked = false;
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_migrate_in_blocked(server, migrate_in_blocked));
  ASSERT_TRUE(migrate_in_blocked);
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_servers());
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(server, server_status));
  ASSERT_EQ(0, server_status.block_migrate_in_time_);
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_migrate_in_blocked(server, migrate_in_blocked));
  ASSERT_FALSE(migrate_in_blocked);
}

TEST_F(TestServerManager, load_server_statuses)
{
  ObServerManager::ObServerStatusArray server_statuses;

  // server already exist, will not add to server manager,
  // server manager has newer server status
  ObServerStatus server_status;
  ObAddr server;
  const int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
  server_status.reset();
  server = A;
  build_status(ObServerStatus::OB_SERVER_DELETING, now, "1", server, server_status);
  ASSERT_EQ(OB_SUCCESS, server_statuses.push_back(server_status));
  ASSERT_EQ(OB_SUCCESS, server_manager_.load_server_statuses(server_statuses));
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_statuses("", server_statuses));
  ASSERT_EQ(1, server_statuses.count());
  ASSERT_EQ(ObServerStatus::OB_SERVER_ADMIN_DELETING, server_statuses.at(0).admin_status_);
}

TEST_F(TestServerManager, get)
{
  ObServerManager::ObServerStatusArray server_statuses;
  // add a server of other zone
  ObLeaseRequest lq;
  uint64_t server_id = OB_INVALID_ID;
  bool to_alive = false;
  bool update_delay_time_flag = false;
  lq.server_ = A1;
  lq.zone_ = "2";
  ASSERT_TRUE(strlen("dev1.0") == snprintf(lq.build_version_, OB_SERVER_VERSION_LENGTH,
      "%s", "dev1.0"));
  obrpc::ObServerInfo server_info;
  server_info.server_ = lq.server_;
  server_info.zone_ = lq.zone_;
  lq.inner_port_ = 3306;
  lq.resource_info_.cpu_ = 1;
  lq.resource_info_.mem_in_use_ = 0;
  lq.resource_info_.mem_total_ = 1024;
  lq.resource_info_.disk_in_use_ = 0;
  lq.resource_info_.disk_total_ = 1024;

  ObSArray<obrpc::ObServerInfo> server_infos;
  ASSERT_EQ(OB_SUCCESS, server_infos.push_back(server_info));
  ASSERT_EQ(OB_SUCCESS, server_manager_.add_server_list(server_infos, server_id));
  ASSERT_EQ(OB_SUCCESS, server_manager_.receive_hb(lq, server_id, to_alive, update_delay_time_flag));

  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_statuses("1", server_statuses));
  ASSERT_EQ(3, server_statuses.count());
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_statuses("2", server_statuses));
  ASSERT_EQ(1, server_statuses.count());
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_statuses("", server_statuses));
  ASSERT_EQ(4, server_statuses.count());

  ObServerManager::ObServerArray servers;
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_servers_of_zone("1", servers));
  ASSERT_EQ(3, servers.count());
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_servers_of_zone("2", servers));
  ASSERT_EQ(1, servers.count());
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_servers_of_zone("", servers));
  ASSERT_EQ(4, servers.count());

  ObZone zone;
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_zone(lq.server_, zone));
  ASSERT_EQ(ObZone("2"), zone);
  LOG_INFO("Print", K(server_manager_));

  int64_t lease_time = 0;
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_lease_duration(lease_time));
  ASSERT_EQ(lease_time, db_initer_.get_config().lease_time);
}

TEST_F(TestServerManager, commit_error)
{
  bool is_alive = false;
  cb_.set_return_success(false);
  // if commit_task failed, server manager and __all_server will be not consistent
  ObLeaseRequest lq;
  uint64_t server_id = OB_INVALID_ID;
  bool to_alive = false;
  bool update_delay_time_flag = false;
  lq.server_ = A1;
  lq.zone_ = "1";
  lq.inner_port_ = 3306;
  lq.resource_info_.cpu_ = 1;
  lq.resource_info_.mem_in_use_ = 0;
  lq.resource_info_.mem_total_ = 1024;
  lq.resource_info_.disk_in_use_ = 0;
  lq.resource_info_.disk_total_ = 1024;
  ASSERT_TRUE(strlen("dev1.0") == snprintf(lq.build_version_, OB_SERVER_VERSION_LENGTH,
      "%s", "dev1.0"));
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_server_alive(lq.server_, is_alive));
  ASSERT_FALSE(is_alive);
  obrpc::ObServerInfo server_info;
  server_info.server_ = lq.server_;
  server_info.zone_ = lq.zone_;
  ObSArray<obrpc::ObServerInfo> server_infos;
  ASSERT_EQ(OB_SUCCESS, server_infos.push_back(server_info));
  ASSERT_EQ(OB_SUCCESS, server_manager_.add_server_list(server_infos, server_id));

  ASSERT_EQ(OB_ERROR, server_manager_.receive_hb(lq, server_id, to_alive, update_delay_time_flag));
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_server_alive(lq.server_, is_alive));
  ASSERT_TRUE(is_alive);

  const int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
  ObServerStatus server_status;
  ObAddr server;
  server = B;
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(server, server_status));
  server_status.last_hb_time_ = now - config_.lease_time - 4000000L;
  ASSERT_EQ(OB_SUCCESS, server_manager_.update_server_status(server_status));

  ASSERT_EQ(OB_ERROR, server_manager_.check_servers());
}

TEST_F(TestServerManager, heartbeatchecker)
{
  ObHeartbeatChecker checker;
  ASSERT_EQ(OB_SUCCESS, checker.init(server_manager_));
  ASSERT_EQ(OB_INIT_TWICE, checker.init(server_manager_));
  checker.start();
  usleep(200 * 100 * 1000L); // check_server will be invoked 200 times
  ObServerStatus server_status;
  ObAddr server;
  server = B;
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(server, server_status));
  ASSERT_FALSE(server_status.is_active());
  checker.stop();
  checker.wait();
}

//TEST_F(TestServerManager, maintain_server)
//{
//  ObLeaseRequest lq;
//  uint64_t server_id = OB_INVALID_ID;
//  bool to_alive = false;
//  lq.server_ = E;
//  lq.zone_ = "2";
//  lq.inner_port_ = 3306;
//  lq.resource_info_.cpu_ = 1;
//  lq.resource_info_.mem_in_use_ = 0;
//  lq.resource_info_.mem_total_ = 1024;
//  lq.resource_info_.disk_in_use_ = 0;
//  lq.resource_info_.disk_total_ = 1024;
//  ASSERT_TRUE(strlen("dev1.0") == snprintf(lq.build_version_, OB_SERVER_VERSION_LENGTH,
//      "%s", "dev1.0"));
//  ASSERT_EQ(OB_SERVER_NOT_IN_WHITE_LIST, server_manager_.receive_hb(lq, server_id, to_alive));
//  bool is_alive = false;
//  ASSERT_EQ(OB_SUCCESS, server_manager_.check_server_alive(lq.server_, is_alive));
//  ASSERT_FALSE(is_alive);
//
//  // add server
//  ObAddr invalid_server;
//  ASSERT_EQ(OB_INVALID_ARGUMENT, server_manager_.add_server(invalid_server, lq.zone_));
//  ASSERT_EQ(OB_SUCCESS, server_manager_.add_server(lq.server_, lq.zone_));
//  ASSERT_EQ(OB_ENTRY_EXIST, server_manager_.add_server(lq.server_, lq.zone_));
//  ASSERT_EQ(OB_INVALID_ARGUMENT, server_manager_.start_server(invalid_server, lq.zone_));
//  ASSERT_EQ(OB_SERVER_ZONE_NOT_MATCH, server_manager_.start_server(lq.server_, "5"));
//  ASSERT_EQ(OB_SUCCESS, server_manager_.start_server(lq.server_, lq.zone_));
//  ObServerStatus server_status;
//  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(lq.server_, server_status));
//  ASSERT_FALSE(server_status.is_active());
//  ASSERT_EQ(OB_SUCCESS, server_manager_.receive_hb(lq, server_id, to_alive));
//  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(lq.server_, server_status));
//  ASSERT_TRUE(server_status.is_active());
//  ObLeaseRequest not_match_lq = lq;
//  not_match_lq.zone_ = "1";
//  ASSERT_EQ(OB_SERVER_ZONE_NOT_MATCH, server_manager_.receive_hb(not_match_lq, server_id, to_alive));
//
//  ObAddr not_exist_server;
//  not_exist_server = A1;
//  ObZone not_match_zone = "1";
//  // delete server, invalid server and not exist server
//  ASSERT_EQ(OB_INVALID_ARGUMENT, server_manager_.delete_server(to_array(invalid_server), not_match_zone));
//  ASSERT_EQ(OB_ENTRY_NOT_EXIST, server_manager_.delete_server(to_array(not_exist_server), not_match_zone));
//  ASSERT_EQ(OB_INVALID_ARGUMENT, server_manager_.end_delete_server(invalid_server, not_match_zone));
//  ASSERT_EQ(OB_ENTRY_NOT_EXIST, server_manager_.end_delete_server(not_exist_server, not_match_zone));
//
//  //add or stop server, invalid server and not exist server
//  ASSERT_EQ(OB_INVALID_ARGUMENT, server_manager_.start_server(invalid_server, not_match_zone));
//  ASSERT_EQ(OB_ENTRY_NOT_EXIST, server_manager_.start_server(not_exist_server, not_match_zone));
//  ASSERT_EQ(OB_INVALID_ARGUMENT, server_manager_.stop_server(invalid_server, not_match_zone));
//  ASSERT_EQ(OB_ENTRY_NOT_EXIST, server_manager_.stop_server(not_exist_server, not_match_zone));
//
//  //stop server twice
//  ASSERT_EQ(OB_SUCCESS, server_manager_.stop_server(lq.server_, lq.zone_));
//  ASSERT_EQ(OB_SUCCESS, server_manager_.stop_server(lq.server_, lq.zone_));
//  //start server again
//  ASSERT_EQ(OB_SUCCESS, server_manager_.start_server(lq.server_, lq.zone_));
//  ASSERT_EQ(OB_SUCCESS, server_manager_.start_server(lq.server_, lq.zone_));
//  //stop servers in multiple zone
//  ObAddr server;
//  server = A;
//  ASSERT_EQ(OB_SUCCESS, server_manager_.stop_server(lq.server_, lq.zone_));
//  ASSERT_EQ(OB_STOP_SERVER_IN_MULTIPLE_ZONES, server_manager_.stop_server(server, "1"));
//
//  // delete server, then cancel delete
//  ASSERT_EQ(OB_SERVER_ZONE_NOT_MATCH, server_manager_.delete_server(to_array(lq.server_), not_match_zone));
//  ASSERT_EQ(OB_SUCCESS, server_manager_.delete_server(to_array(lq.server_), lq.zone_));
//  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(lq.server_, server_status));
//  ASSERT_EQ(ObServerStatus::OB_SERVER_DELETING, server_status.get_display_status());
//  ASSERT_EQ(OB_SERVER_ZONE_NOT_MATCH, server_manager_.delete_server(to_array(lq.server_), not_match_zone));
//  ASSERT_EQ(OB_SUCCESS, server_manager_.end_delete_server(lq.server_, lq.zone_, false));
//  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(lq.server_, server_status));
//  ASSERT_EQ(ObServerStatus::OB_SERVER_ACTIVE, server_status.get_display_status());
//  ASSERT_EQ(OB_SERVER_NOT_DELETING, server_manager_.end_delete_server(lq.server_, lq.zone_, false));
//
//  // delete server, then commit delete
//  ASSERT_EQ(OB_SUCCESS, server_manager_.delete_server(to_array(lq.server_), lq.zone_));
//  ASSERT_EQ(OB_SUCCESS, server_manager_.end_delete_server(lq.server_, lq.zone_, true));
//  ASSERT_EQ(OB_ENTRY_NOT_EXIST, server_manager_.get_server_status(lq.server_, server_status));
//  ASSERT_EQ(OB_ENTRY_NOT_EXIST, server_manager_.end_delete_server(lq.server_, lq.zone_, true));
//  ASSERT_EQ(OB_ENTRY_NOT_EXIST, server_manager_.delete_server(to_array(lq.server_), lq.zone_));
//
//  ObZone not_exist_zone = "not_exist";
//  ASSERT_EQ(OB_ZONE_NOT_ACTIVE, server_manager_.add_server(lq.server_, not_exist_zone));
//  ASSERT_EQ(OB_SUCCESS, server_manager_.add_server(lq.server_, lq.zone_));
//  ASSERT_EQ(OB_SERVER_ZONE_NOT_MATCH, server_manager_.delete_server(to_array(lq.server_), not_exist_zone));
//  ASSERT_EQ(OB_SERVER_ZONE_NOT_MATCH, server_manager_.end_delete_server(lq.server_, not_exist_zone, true));
//
//  // empty server, delete then add server again
//  ObAddr empty_server;
//  empty_server.set_ip_addr("127.0.0.1", 1111);
//  ASSERT_EQ(OB_SUCCESS, server_manager_.add_server(empty_server, "1"));
//  ASSERT_EQ(OB_SUCCESS, server_manager_.delete_server(to_array(empty_server), "1"));
//  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(empty_server, server_status));
//  ASSERT_EQ(OB_SUCCESS, server_manager_.end_delete_server(empty_server, server_status.zone_, true));
//  ASSERT_EQ(OB_ENTRY_NOT_EXIST, server_manager_.get_server_status(empty_server, server_status));
//  ASSERT_EQ(OB_SUCCESS, server_manager_.add_server(empty_server, "1"));
//}

TEST_F(TestServerManager, update_merged_version)
{
  ObAddr s1, s2, s3;
  s1 = A;
  s2 = B;
  s3 = C;

  ObLeaseRequest lq;
  lq.zone_ = "1";
  lq.server_ = s1;
  lq.inner_port_ = 3306;
  lq.resource_info_.cpu_ = 1;
  lq.resource_info_.mem_in_use_ = 0;
  lq.resource_info_.mem_total_ = 1024;
  lq.resource_info_.disk_in_use_ = 0;
  lq.resource_info_.disk_total_ = 1024;
  uint64_t server_id;
  bool to_alive = false;
  bool update_delay_time_flag = false;
  ASSERT_EQ(OB_SUCCESS, server_manager_.receive_hb(lq, server_id, to_alive, update_delay_time_flag));
  lq.server_ = s2;
  ASSERT_EQ(OB_SUCCESS, server_manager_.receive_hb(lq, server_id, to_alive, update_delay_time_flag));
  lq.server_ = s3;
  ASSERT_EQ(OB_SUCCESS, server_manager_.receive_hb(lq, server_id, to_alive, update_delay_time_flag));

  bool merged = false;
  ASSERT_EQ(OB_SUCCESS, server_manager_.update_merged_version(s1, 2, merged));
  ASSERT_FALSE(merged);
  ASSERT_EQ(OB_SUCCESS, server_manager_.update_merged_version(s2, 2, merged));
  ASSERT_FALSE(merged);
  ASSERT_EQ(OB_SUCCESS, server_manager_.update_merged_version(s3, 2, merged));
  ASSERT_TRUE(merged);
}

TEST_F(TestServerManager, block_migrate_in)
{
  ObAddr s1, not_exist_server;
  s1 = B;
  not_exist_server = A1;

  bool is_block = true;
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_migrate_in_blocked(s1, is_block));
  ASSERT_FALSE(is_block);
  ASSERT_EQ(OB_SUCCESS, server_manager_.block_migrate_in(s1));
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_migrate_in_blocked(s1, is_block));
  ASSERT_TRUE(is_block);
  ASSERT_EQ(OB_SUCCESS, server_manager_.unblock_migrate_in(s1));
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_migrate_in_blocked(s1, is_block));
  ASSERT_FALSE(is_block);

  is_block = true;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, server_manager_.check_migrate_in_blocked(not_exist_server, is_block));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, server_manager_.block_migrate_in(not_exist_server));
}

TEST_F(TestServerManager, in_service)
{
  ObAddr s1, not_exist_server;
  s1 = A;
  not_exist_server = A1;

  bool in_service = false;
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_in_service(s1, in_service));
  ASSERT_FALSE(in_service);
  ObLeaseRequest lq;
  lq.zone_ = "1";
  lq.server_ = s1;
  lq.start_service_time_ = ObTimeUtility::current_time();
  lq.inner_port_ = 3306;
  lq.resource_info_.cpu_ = 1;
  lq.resource_info_.mem_in_use_ = 0;
  lq.resource_info_.mem_total_ = 1024;
  lq.resource_info_.disk_in_use_ = 0;
  lq.resource_info_.disk_total_ = 1024;

  uint64_t server_id;
  bool to_alive = false;
  bool update_delay_time_flag = false;
  ASSERT_EQ(OB_SUCCESS, server_manager_.receive_hb(lq, server_id, to_alive, update_delay_time_flag));
  ASSERT_EQ(OB_SUCCESS, server_manager_.check_in_service(s1, in_service));
  ASSERT_TRUE(in_service);

  in_service = false;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, server_manager_.check_in_service(not_exist_server, in_service));
}

TEST_F(TestServerManager, with_partition)
{
  ASSERT_EQ(OB_SUCCESS, server_manager_.set_with_partition(B));
  ASSERT_NE(OB_SUCCESS, server_manager_.set_with_partition(A)); // non-alive can not set with partition flag

  ObServerStatus A_s;
  ObServerStatus B_s;
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(A, A_s));
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(B, B_s));

  ASSERT_TRUE(B_s.with_partition_);
  ASSERT_EQ(OB_SUCCESS, server_manager_.clear_with_partiton(A, A_s.last_hb_time_));

  // last hb time mismatch, return success, keep with partition flag unchanged.
  ASSERT_EQ(OB_SUCCESS, server_manager_.clear_with_partiton(B, B_s.last_hb_time_ + 1));
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(B, B_s));
  ASSERT_TRUE(B_s.with_partition_);

  ASSERT_EQ(OB_SUCCESS, server_manager_.clear_with_partiton(B, B_s.last_hb_time_));
  ASSERT_EQ(OB_SUCCESS, server_manager_.get_server_status(B, B_s));
  ASSERT_FALSE(B_s.with_partition_);
}

}//end namespace rootserver
}//end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_server_manager.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_server_manager.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);

  ::testing::InitGoogleTest(&argc,argv);

  oceanbase::common::ObTenantManager::get_instance().init(10);
  oceanbase::common::ObTenantManager::get_instance().add_tenant(OB_SYS_TENANT_ID);
  oceanbase::common::ObTenantManager::get_instance().set_tenant_mem_limit(OB_SYS_TENANT_ID, 1024L * 1024L * 1024L, 1024L * 1024L * 1024L);
  return RUN_ALL_TESTS();
}
