/**
 * Copyright (c) 2023 OceanBase
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
#include <gmock/gmock.h>

#define USING_LOG_PREFIX SHARE
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "share/location_cache/ob_location_service.h" // ObLocationService

namespace oceanbase
{
using namespace unittest;
namespace share
{
using namespace common;

static const int64_t TOTAL_NUM = 110;
static uint64_t g_tenant_id;
static ObSEArray<ObTabletLSPair, TOTAL_NUM> g_tablet_ls_pairs;

class TestLocationService : public unittest::ObSimpleClusterTestBase
{
public:
  TestLocationService() : unittest::ObSimpleClusterTestBase("test_location_service") {}
  int batch_create_table(ObMySQLProxy &sql_proxy, const int64_t TOTAL_NUM, const bool oracle_mode, ObIArray<ObTabletLSPair> &tablet_ls_pairs);
};

int TestLocationService::batch_create_table(
    ObMySQLProxy &sql_proxy,
    const int64_t TOTAL_NUM,
    const bool oracle_mode,
    ObIArray<ObTabletLSPair> &tablet_ls_pairs)
{
  int ret = OB_SUCCESS;
  tablet_ls_pairs.reset();
  ObSqlString sql;
  // batch create table
  int64_t affected_rows = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < TOTAL_NUM; ++i) {
    sql.reset();
    if (OB_FAIL(sql.assign_fmt("create table T%ld(c1 int)", i))) {
    } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
    }
  }
  // batch get table_id
  sql.reset();
  if (OB_FAIL(sql.assign_fmt("select TABLET_ID, LS_ID from %sDBA_OB_TABLE_LOCATIONS where table_name in (", oracle_mode ? "" : "oceanbase."))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < TOTAL_NUM; ++i) {
      if (OB_FAIL(sql.append_fmt("%s'T%ld'", 0 == i ? "" : ",", i))) {}
    }
    if (FAILEDx(sql.append_fmt(") order by TABLET_ID"))) {};
  }
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    if (OB_FAIL(tablet_ls_pairs.reserve(TOTAL_NUM))) {
    } else if (OB_FAIL(sql_proxy.read(result, sql.ptr()))) {
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null result", KR(ret), K(sql));
    } else {
      sqlclient::ObMySQLResult &res = *result.get_result();
      uint64_t tablet_id = ObTabletID::INVALID_TABLET_ID;
      int64_t ls_id = ObLSID::INVALID_LS_ID;
      while(OB_SUCC(ret) && OB_SUCC(res.next())) {
        EXTRACT_INT_FIELD_MYSQL(res, "TABLET_ID", tablet_id, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(res, "LS_ID", ls_id, int64_t);
        if (OB_FAIL(tablet_ls_pairs.push_back(ObTabletLSPair(tablet_id, ls_id)))) {}
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to generate data", K(sql));
      }
    }
  }
  return ret;
}

TEST_F(TestLocationService, prepare_data)
{
  g_tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, batch_create_table(sql_proxy, TOTAL_NUM, false, g_tablet_ls_pairs));
}

TEST_F(TestLocationService, test_ls_location_service)
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  ObLocationService *location_service = GCTX.location_service_;
  ASSERT_TRUE(OB_NOT_NULL(location_service));
  ObLSLocationService *ls_location_service = &(location_service->ls_location_service_);
  ASSERT_TRUE(OB_NOT_NULL(ls_location_service));

  ObSEArray<ObLSID, 2> ls_ids;
  ObSEArray<ObLSLocation, 2> ls_locations;
  ASSERT_EQ(OB_SUCCESS, ls_ids.push_back(ObLSID(1)));
  ASSERT_EQ(OB_SUCCESS, ls_ids.push_back(ObLSID(1001)));
  ASSERT_EQ(OB_SUCCESS, ls_location_service->batch_renew_ls_locations(GCONF.cluster_id, g_tenant_id, ls_ids, ls_locations));
  ASSERT_TRUE(ls_ids.count() == ls_locations.count());
  ARRAY_FOREACH(ls_locations, idx) {
    ASSERT_TRUE(ls_locations.at(idx).get_ls_id() == ls_ids.at(idx));
    ASSERT_TRUE(!ls_locations.at(idx).get_replica_locations().empty());
  }

  // nonexistent ls_id return fake empty location
  ls_ids.reset();
  ls_locations.reset();
  ASSERT_EQ(OB_SUCCESS, ls_ids.push_back(ObLSID(1)));
  ASSERT_EQ(OB_SUCCESS, ls_ids.push_back(ObLSID(1234))); // nonexistent ls_id
  ASSERT_EQ(OB_SUCCESS, ls_location_service->batch_renew_ls_locations(GCONF.cluster_id, g_tenant_id, ls_ids, ls_locations));
  ASSERT_TRUE(ls_ids.count() == ls_locations.count() + 1);
  ASSERT_TRUE(ls_locations.at(0).get_ls_id() == ls_ids.at(0));
  ASSERT_TRUE(!ls_locations.at(0).get_replica_locations().empty());

  // duplicated ls_id return error
  ls_ids.reset();
  ls_locations.reset();
  ASSERT_EQ(OB_SUCCESS, ls_ids.push_back(ObLSID(1001)));
  ASSERT_EQ(OB_SUCCESS, ls_ids.push_back(ObLSID(1001))); // duplicated ls_id
  ASSERT_EQ(OB_ERR_DUP_ARGUMENT, ls_location_service->batch_renew_ls_locations(GCONF.cluster_id, g_tenant_id, ls_ids, ls_locations));
}

TEST_F(TestLocationService, test_tablet_ls_service)
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(g_tablet_ls_pairs.count() == TOTAL_NUM);
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  ObLocationService *location_service = GCTX.location_service_;
  ASSERT_TRUE(OB_NOT_NULL(location_service));
  ObTabletLSService *tablet_ls_service = &(location_service->tablet_ls_service_);
  ASSERT_TRUE(OB_NOT_NULL(tablet_ls_service));

  ObArenaAllocator allocator;
  ObList<ObTabletID, ObIAllocator> tablet_list(allocator);
  ObSEArray<ObTabletLSCache, TOTAL_NUM> tablet_ls_caches;
  ARRAY_FOREACH(g_tablet_ls_pairs, idx) {
    const ObTabletLSPair &pair = g_tablet_ls_pairs.at(idx);
    ASSERT_EQ(OB_SUCCESS, tablet_list.push_back(pair.get_tablet_id()));
  }
  ASSERT_EQ(OB_SUCCESS, tablet_ls_service->batch_renew_tablet_ls_cache(g_tenant_id, tablet_list, tablet_ls_caches));
  ASSERT_TRUE(tablet_list.size() == tablet_ls_caches.count());
  ASSERT_TRUE(tablet_ls_caches.count() == g_tablet_ls_pairs.count());
  ARRAY_FOREACH(tablet_ls_caches, idx) {
    const ObTabletLSCache &cache = tablet_ls_caches.at(idx);
    bool find = false;
    FOREACH(tablet_id, tablet_list) {
      if (*tablet_id == cache.get_tablet_id()) {
        find = true;
      }
    }
    ASSERT_TRUE(find);
    ARRAY_FOREACH(g_tablet_ls_pairs, j) {
      const ObTabletLSPair &pair = g_tablet_ls_pairs.at(j);
      if (pair.get_tablet_id() == cache.get_tablet_id()) {
        ASSERT_TRUE(pair.get_ls_id() == cache.get_ls_id());
        break;
      }
    }
  }

  // nonexistent tablet_id return OB_SUCCESS
  tablet_list.reset();
  tablet_ls_caches.reset();
  ASSERT_EQ(OB_SUCCESS, tablet_list.push_back(g_tablet_ls_pairs.at(0).get_tablet_id()));
  ASSERT_EQ(OB_SUCCESS, tablet_list.push_back(ObTabletID(654321))); // nonexistent ls_id
  ASSERT_EQ(OB_SUCCESS, tablet_ls_service->batch_renew_tablet_ls_cache(g_tenant_id, tablet_list, tablet_ls_caches));
  ASSERT_TRUE(tablet_ls_caches.count() == 1);
  ASSERT_TRUE(tablet_ls_caches.at(0).get_tablet_id() == g_tablet_ls_pairs.at(0).get_tablet_id());
  ASSERT_TRUE(tablet_ls_caches.at(0).get_ls_id() == g_tablet_ls_pairs.at(0).get_ls_id());

  // duplicated tablet_id return OB_SUCCESS
  tablet_list.reset();
  tablet_ls_caches.reset();
  ASSERT_EQ(OB_SUCCESS, tablet_list.push_back(g_tablet_ls_pairs.at(0).get_tablet_id()));
  ASSERT_EQ(OB_SUCCESS, tablet_list.push_back(g_tablet_ls_pairs.at(0).get_tablet_id()));
  ASSERT_EQ(OB_SUCCESS, tablet_ls_service->batch_renew_tablet_ls_cache(g_tenant_id, tablet_list, tablet_ls_caches));
}

TEST_F(TestLocationService, test_location_service)
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(g_tablet_ls_pairs.count() == TOTAL_NUM);
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  ObLocationService *location_service = GCTX.location_service_;
  ASSERT_TRUE(OB_NOT_NULL(location_service));

  ObArenaAllocator allocator;
  ObList<ObTabletID, ObIAllocator> tablet_list(allocator);
  ObSEArray<ObTabletLSCache, TOTAL_NUM> tablet_ls_caches;
  ARRAY_FOREACH(g_tablet_ls_pairs, idx) {
    const ObTabletLSPair &pair = g_tablet_ls_pairs.at(idx);
    ASSERT_EQ(OB_SUCCESS, tablet_list.push_back(pair.get_tablet_id()));
  }

  ASSERT_EQ(OB_SUCCESS, location_service->batch_renew_tablet_locations(g_tenant_id, tablet_list, OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST, true));
  ASSERT_EQ(OB_SUCCESS, location_service->batch_renew_tablet_locations(g_tenant_id, tablet_list, OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST, false));
  ASSERT_EQ(OB_SUCCESS, location_service->batch_renew_tablet_locations(g_tenant_id, tablet_list, OB_NOT_MASTER, true));
  ASSERT_EQ(OB_SUCCESS, location_service->batch_renew_tablet_locations(g_tenant_id, tablet_list, OB_NOT_MASTER, false));
}

TEST_F(TestLocationService, test_check_ls_exist)
{
  // create tenant
  uint64_t user_tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, create_tenant("tt2"));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(user_tenant_id, "tt2"));
  uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);

  ObLSID user_ls_id(1001);
  ObLSID uncreated_ls_id(6666);
  ObLSID invalid_ls_id(123);
  ObLSID test_creating_ls_id(1111);
  ObLSID test_creat_abort_ls_id(1112);
  const uint64_t not_exist_tenant_id = 1234;
  ObLSExistState state;

  // user tenant
  ASSERT_EQ(OB_SUCCESS, ObLocationService::check_ls_exist(user_tenant_id, SYS_LS, state));
  ASSERT_TRUE(state.is_existing());
  state.reset();
  ASSERT_EQ(OB_SUCCESS, ObLocationService::check_ls_exist(user_tenant_id, user_ls_id, state));
  ASSERT_TRUE(state.is_existing());
  state.reset();
  ASSERT_EQ(OB_SUCCESS, ObLocationService::check_ls_exist(user_tenant_id, uncreated_ls_id, state));
  ASSERT_TRUE(state.is_uncreated());

  common::ObMySQLProxy &inner_proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("delete from oceanbase.__all_ls_status where tenant_id = %lu and ls_id = %ld", user_tenant_id, user_ls_id.id()));
  ASSERT_EQ(OB_SUCCESS, inner_proxy.write(get_private_table_exec_tenant_id(user_tenant_id), sql.ptr(), affected_rows));
  state.reset();
  ASSERT_EQ(OB_SUCCESS, ObLocationService::check_ls_exist(user_tenant_id, user_ls_id, state));
  ASSERT_TRUE(state.is_deleted());

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into oceanbase.__all_ls_status (tenant_id, ls_id, status, ls_group_id, unit_group_id) values (%lu, %ld, 'CREATING', 0, 0)", user_tenant_id, test_creating_ls_id.id()));
  ASSERT_EQ(OB_SUCCESS, inner_proxy.write(get_private_table_exec_tenant_id(user_tenant_id), sql.ptr(), affected_rows));
  state.reset();
  ASSERT_EQ(OB_SUCCESS, ObLocationService::check_ls_exist(user_tenant_id, test_creating_ls_id, state));
  ASSERT_TRUE(state.is_uncreated()); // treat CREATING ls as UNCREATED

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into oceanbase.__all_ls_status (tenant_id, ls_id, status, ls_group_id, unit_group_id) values (%lu, %ld, 'CREATE_ABORT', 0, 0)", user_tenant_id, test_creat_abort_ls_id.id()));
  ASSERT_EQ(OB_SUCCESS, inner_proxy.write(get_private_table_exec_tenant_id(user_tenant_id), sql.ptr(), affected_rows));
  state.reset();
  ASSERT_EQ(OB_SUCCESS, ObLocationService::check_ls_exist(user_tenant_id, test_creat_abort_ls_id, state));
  ASSERT_TRUE(state.is_deleted()); // treat CREATE_ABLORT ls as DELETED

  ASSERT_EQ(OB_INVALID_ARGUMENT, ObLocationService::check_ls_exist(user_tenant_id, invalid_ls_id, state));

  // sys tenant
  state.reset();
  ASSERT_EQ(OB_SUCCESS, ObLocationService::check_ls_exist(OB_SYS_TENANT_ID, SYS_LS, state));
  ASSERT_TRUE(state.is_existing());
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObLocationService::check_ls_exist(OB_SYS_TENANT_ID, user_ls_id, state));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObLocationService::check_ls_exist(OB_SYS_TENANT_ID, invalid_ls_id, state));

  // not exist tenant
  ASSERT_EQ(OB_TENANT_NOT_EXIST, ObLocationService::check_ls_exist(not_exist_tenant_id, SYS_LS, state));
  // virtual tenant
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObLocationService::check_ls_exist(OB_SERVER_TENANT_ID, SYS_LS, state));

  // meta tenant
  state.reset();
  ASSERT_EQ(OB_SUCCESS, ObLocationService::check_ls_exist(meta_tenant_id, SYS_LS, state));
  ASSERT_TRUE(state.is_existing());
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObLocationService::check_ls_exist(meta_tenant_id, user_ls_id, state));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObLocationService::check_ls_exist(meta_tenant_id, invalid_ls_id, state));

  // meta tenant not in normal status
  state.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_CHECK_LS_EXIST_WITH_TENANT_NOT_NORMAL, error_code = 4016, frequency = 1"));
  ASSERT_EQ(OB_SUCCESS, inner_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, ObLocationService::check_ls_exist(meta_tenant_id, SYS_LS, state));
  ASSERT_TRUE(state.is_existing());

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("update oceanbase.__all_ls_status set status = 'CREATING' where tenant_id = %lu and ls_id = %ld", meta_tenant_id, ObLSID::SYS_LS_ID));
  ASSERT_EQ(OB_SUCCESS, inner_proxy.write(get_private_table_exec_tenant_id(meta_tenant_id), sql.ptr(), affected_rows));
  state.reset();
  ASSERT_EQ(OB_SUCCESS, ObLocationService::check_ls_exist(meta_tenant_id, SYS_LS, state));
  ASSERT_TRUE(state.is_uncreated());

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("delete from oceanbase.__all_ls_status where tenant_id = %lu and ls_id = %ld", meta_tenant_id, ObLSID::SYS_LS_ID));
  ASSERT_EQ(OB_SUCCESS, inner_proxy.write(get_private_table_exec_tenant_id(meta_tenant_id), sql.ptr(), affected_rows));
  state.reset();
  ASSERT_EQ(OB_SUCCESS, ObLocationService::check_ls_exist(meta_tenant_id, SYS_LS, state));
  ASSERT_TRUE(state.is_uncreated());

  // reset
  ASSERT_EQ(OB_SUCCESS, delete_tenant("tt2"));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set_tp tp_name = EN_CHECK_LS_EXIST_WITH_TENANT_NOT_NORMAL, error_code = 0, frequency = 0"));
  ASSERT_EQ(OB_SUCCESS, inner_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  bool tenant_exist = true;
  int ret = OB_SUCCESS;
  while (true == tenant_exist && OB_SUCC(ret)) {
    if (OB_FAIL(check_tenant_exist(tenant_exist, "tt2"))) {
      SERVER_LOG(WARN, "check_tenant_exist failed", K(ret));
    } else {
      usleep(1_s);
    }
  }
}

TEST_F(TestLocationService, test_clear_tablet_ls_cache)
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(g_tablet_ls_pairs.count() == TOTAL_NUM);
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  ObLocationService *location_service = GCTX.location_service_;
  ASSERT_TRUE(OB_NOT_NULL(location_service));
  ObTabletLSService *tablet_ls_service = &(location_service->tablet_ls_service_);
  ASSERT_TRUE(OB_NOT_NULL(tablet_ls_service));

  // create tenant
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, create_tenant("oracle", "2G", "2G", true));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id, "oracle"));
  ASSERT_TRUE(is_valid_tenant_id(tenant_id));

  // create sql_proxy
  common::sqlclient::ObSingleMySQLConnectionPool sql_conn_pool;
  common::ObMySQLProxy oracle_sql_proxy;
  sql_conn_pool.set_db_param("sys@oracle", "", "SYS");
  ObConnPoolConfigParam param;
  param.sqlclient_wait_timeout_ = 1000;
  param.long_query_timeout_ = 300*1000*1000;
  param.connection_refresh_interval_ = 200*1000;
  param.connection_pool_warn_time_ = 10*1000*1000;
  param.sqlclient_per_observer_conn_limit_ = 1000;
  common::ObAddr db_addr;
  db_addr.set_ip_addr(get_curr_simple_server().get_local_ip().c_str(), get_curr_simple_server().get_mysql_port());
  ret = sql_conn_pool.init(db_addr, param);
  if (OB_SUCC(ret)) {
    sql_conn_pool.set_mode(common::sqlclient::ObMySQLConnection::DEBUG_MODE);
    ret = oracle_sql_proxy.init(&sql_conn_pool);
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  // create table
  const int64_t TABLET_COUNT = 10;
  ObSEArray<ObTabletLSPair, TABLET_COUNT> tablet_ls_pairs;
  ASSERT_EQ(OB_SUCCESS, batch_create_table(oracle_sql_proxy, TABLET_COUNT, true, tablet_ls_pairs));
  ASSERT_TRUE(TABLET_COUNT == tablet_ls_pairs.count());
  const int64_t cache_size_before_renew = tablet_ls_service->inner_cache_.size();
  ASSERT_TRUE(cache_size_before_renew > 0);
  ObArenaAllocator allocator;
  ObList<ObTabletID, ObIAllocator> tablet_list(allocator);
  ObSEArray<ObTabletLSCache, TABLET_COUNT> tablet_ls_caches;
  ARRAY_FOREACH(tablet_ls_pairs, idx) {
    const ObTabletLSPair &pair = tablet_ls_pairs.at(idx);
    ASSERT_EQ(OB_SUCCESS, tablet_list.push_back(pair.get_tablet_id()));
  }

  // renew cache
  ASSERT_EQ(OB_SUCCESS, tablet_ls_service->batch_renew_tablet_ls_cache(tenant_id, tablet_list, tablet_ls_caches));
  int64_t cache_size = tablet_ls_service->inner_cache_.size();
  ASSERT_TRUE(TABLET_COUNT == cache_size - cache_size_before_renew);

  // test clear dropped tenant cache
  ASSERT_EQ(OB_SUCCESS, delete_tenant("oracle"));
  ASSERT_EQ(OB_SUCCESS, tablet_ls_service->clear_expired_cache());
  cache_size = tablet_ls_service->inner_cache_.size();
  ASSERT_TRUE(cache_size_before_renew == cache_size);

  // test 1 million cache clear
  const bool update_only = false;
  for (int64_t i = 0; i < 1000000; ++i) {
    ObTabletLSCache cache;
    ASSERT_EQ(OB_SUCCESS, cache.init(tenant_id, ObTabletID(i+300000), ObLSID(1002), ObClockGenerator::getClock(), 1));
    ASSERT_EQ(OB_SUCCESS, tablet_ls_service->inner_cache_.update(cache, update_only));
  }
  cache_size = tablet_ls_service->inner_cache_.size();
  ASSERT_TRUE(1000000 == cache_size - cache_size_before_renew);
  const int64_t start_time = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, tablet_ls_service->clear_expired_cache());
  cache_size = tablet_ls_service->inner_cache_.size();
  ASSERT_TRUE(cache_size_before_renew == cache_size);
  LOG_INFO("TEST: clear 1 million cache", "cost_time", ObTimeUtility::current_time() - start_time); // cost_time = 1.67s
}

TEST_F(TestLocationService, test_clear_ls_location)
{
  int ret = OB_SUCCESS;
  uint64_t user_tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(user_tenant_id, "tt1"));
  ASSERT_TRUE(is_user_tenant(user_tenant_id));
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  ObLocationService *location_service = GCTX.location_service_;
  ASSERT_TRUE(OB_NOT_NULL(location_service));
  ObLSLocationService *ls_location_service = &(location_service->ls_location_service_);
  ASSERT_TRUE(OB_NOT_NULL(ls_location_service));
  const ObLSID &user_ls_id = ObLSID(1001);
  ObLSLocation location;
  // assert caches exist
  usleep(ls_location_service->RENEW_LS_LOCATION_INTERVAL_US);
  ASSERT_EQ(OB_SUCCESS, ls_location_service->get_from_cache_(GCONF.cluster_id, user_tenant_id, user_ls_id, location));
  ASSERT_TRUE(location.get_cache_key() == ObLSLocationCacheKey(GCONF.cluster_id, user_tenant_id, user_ls_id));
  location.reset();
  ASSERT_EQ(OB_SUCCESS, ls_location_service->get_from_cache_(GCONF.cluster_id, meta_tenant_id, SYS_LS, location));
  ASSERT_TRUE(location.get_cache_key() == ObLSLocationCacheKey(GCONF.cluster_id, meta_tenant_id, SYS_LS));

  // drop tenant force
  ASSERT_EQ(OB_SUCCESS, delete_tenant("tt1"));
  // meta tenant is dropped in schema and user tenant unit has been gc
  bool is_dropped = false;
  ASSERT_EQ(OB_SUCCESS, GSCHEMASERVICE.check_if_tenant_has_been_dropped(meta_tenant_id, is_dropped));
  ASSERT_TRUE(is_dropped);

  // auto clear caches successfully
  usleep(ls_location_service->CLEAR_CACHE_INTERVAL);
  usleep(ls_location_service->RENEW_LS_LOCATION_BY_RPC_INTERVAL_US + GCONF.rpc_timeout);
  ASSERT_EQ(OB_CACHE_NOT_HIT, ls_location_service->get_from_cache_(GCONF.cluster_id, user_tenant_id, user_ls_id, location));
  ASSERT_EQ(OB_CACHE_NOT_HIT, ls_location_service->get_from_cache_(GCONF.cluster_id, meta_tenant_id, SYS_LS, location));
}

} // namespace rootserver
} // namespace oceanbase
int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
