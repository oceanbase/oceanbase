// owner: handora.qc
// owner group: transaction

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
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public
#include "env/ob_multi_replica_test_base.h"
#include "env/ob_multi_replica_util.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/incremental/garbage_collector/ob_ss_garbage_collector_rpc.h"

#define CUR_TEST_CASE_NAME ObTestMultiSSGC

DEFINE_MULTI_ZONE_TEST_CASE_CLASS
MULTI_REPLICA_TEST_MAIN_FUNCTION(test_multi_ss_gc_);

namespace oceanbase
{
namespace unittest
{

using namespace storage;

#define EXE_SQL(sql_str)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define EXE_SQL_FMT(...)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__));               \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));


void get_tablet_info_with_table_name(common::ObMySQLProxy &sql_proxy,
                                     const char *name,
                                     int64_t &table_id,
                                     int64_t &object_id,
                                     int64_t &tablet_id,
                                     int64_t &ls_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("SELECT table_id, object_id, tablet_id, ls_id FROM oceanbase.DBA_OB_TABLE_LOCATIONS WHERE TABLE_NAME= '%s';", name));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("table_id", table_id));
    ASSERT_EQ(OB_SUCCESS, result->get_int("object_id", object_id));
    ASSERT_EQ(OB_SUCCESS, result->get_int("tablet_id", tablet_id));
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", ls_id));
  }
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), create_test_env)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t tenant_id = 0;

  // ============================== Phase1. create tenant ==============================
  SERVER_LOG(INFO, "create_tenant start");
  ASSERT_EQ(OB_SUCCESS, create_tenant(DEFAULT_TEST_TENANT_NAME,
                                      "2G",
                                      "2G",
                                      false,
                                      "zone1"));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  SERVER_LOG(INFO, "create_tenant end", K(tenant_id));

  SERVER_LOG(INFO, "[ObMultiSSLog1] create test tenant success", K(tenant_id));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection_qc = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection_qc));
  ASSERT_NE(nullptr, connection_qc);

  // ============================== Phase2. create table ==============================
  TRANS_LOG(INFO, "create table qcc1 start");
  EXE_SQL("create table qcc1 (a int)");
  TRANS_LOG(INFO, "create_table qcc1 end");
  usleep(3 * 1000 * 1000);

  TRANS_LOG(INFO, "create table qcc2 start");
  EXE_SQL("create table qcc2 (a int)");
  TRANS_LOG(INFO, "create_table qcc2 end");
  usleep(3 * 1000 * 1000);

  ObLSID loc1;
  ObLSID loc2;
  ASSERT_EQ(0, SSH::select_table_loc(tenant_id, "qcc1", loc1));
  ASSERT_EQ(0, SSH::select_table_loc(tenant_id, "qcc2", loc2));
  int64_t table1;
  int64_t object1;
  int64_t tablet1;
  int64_t ls1;
  int64_t table2;
  int64_t object2;
  int64_t tablet2;
  int64_t ls2;
  get_tablet_info_with_table_name(sql_proxy, "qcc1", table1, object1, tablet1, ls1);
  get_tablet_info_with_table_name(sql_proxy, "qcc2", table2, object2, tablet2, ls2);
  fprintf(stdout, "ZONE1: qcc1 is created successfully, loc1: %ld, table1: %ld, tablet1: %ld, ls1: %ld\n",
          loc1.id(), table1, tablet1, ls1);
  fprintf(stdout, "ZONE2: qcc2 is created successfully, loc2: %ld, table2: %ld, tablet2: %ld, ls2: %ld\n",
          loc2.id(), table2, tablet2, ls2);

  ObCStringHelper helper;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  ObArenaAllocator allocator(lib::ObMemAttr(MTL_ID(), "SSPreciseGC"));

  // Test1: test 1 tablets search with 3 machine
  storage::ObLSTabletsMap ls_tablet_map;
  ASSERT_EQ(OB_SUCCESS, ls_tablet_map.create(10, "qcc", "qcc", MTL_ID()));
  storage::ObSSPreciseGCTablets gc_tablets;
  ASSERT_EQ(OB_SUCCESS, gc_tablets.push_back(storage::ObSSPreciseGCTablet(
                                               loc1,
                                               ObTabletID(tablet1),
                                               share::SCN::min_scn())));
  storage::ObSSPreciseGCInfos gc_infos;
  ObAddrTabletsMap addr_tablets_map;
  ASSERT_EQ(OB_SUCCESS, ls_tablet_map.set_refactored(ObLSID(1001), gc_tablets));
  ASSERT_EQ(OB_SUCCESS, ObSSPreciseGCRPC::study_precise_ss_gc(ls_tablet_map, gc_infos));
  ASSERT_EQ(0, gc_infos.count());
  for (int64_t i = 0; i < gc_infos.count(); i++) {
    fprintf(stdout, "qcc debug1 %s\n", helper.convert(gc_infos[i]));
  }

  ASSERT_EQ(OB_SUCCESS, addr_tablets_map.create(10, "SSPreciseGC", "SSPreciseGC", MTL_ID()));
  ASSERT_EQ(OB_SUCCESS, ObSSPreciseGCRPC::collect_addr_tablets_map_(allocator, ls_tablet_map, addr_tablets_map));
  ASSERT_EQ(3, addr_tablets_map.size());
  for (ObAddrTabletsMap::iterator iter =
         addr_tablets_map.begin(); iter != addr_tablets_map.end(); ++iter) {
    ASSERT_EQ(1, iter->second->count());
    fprintf(stdout, "qcc debug2 %s %s\n", helper.convert(iter->first), helper.convert(*iter->second));
  }

  // Test2: test 2 tablets search with 3 machine
  common::ObAddr addrs[3];
  int64_t index = 0;

  addr_tablets_map.clear();
  ASSERT_EQ(OB_SUCCESS, gc_tablets.push_back(storage::ObSSPreciseGCTablet(
                                               loc2,
                                               ObTabletID(tablet2),
                                               share::SCN::min_scn())));
  ASSERT_EQ(OB_SUCCESS, ls_tablet_map.set_refactored(ObLSID(1001), gc_tablets, 1));
  ObSSPreciseGCRPC::study_precise_ss_gc(ls_tablet_map, gc_infos);
  ASSERT_EQ(0, gc_infos.count());
  for (int64_t i = 0; i < gc_infos.count(); i++) {
    fprintf(stdout, "qcc debug3 %s\n", helper.convert(gc_infos[i]));
  }

  ASSERT_EQ(OB_SUCCESS, ObSSPreciseGCRPC::collect_addr_tablets_map_(allocator, ls_tablet_map, addr_tablets_map));
  ASSERT_EQ(3, addr_tablets_map.size());
  for (ObAddrTabletsMap::iterator iter =
         addr_tablets_map.begin(); iter != addr_tablets_map.end(); ++iter) {
    addrs[index] = iter->first;
    index++;
    ASSERT_EQ(2, iter->second->count());
    fprintf(stdout, "qcc debug4 %s %s\n", helper.convert(iter->first), helper.convert(*iter->second));
  }

  // Test3: test 2 gc infos merge
  ObSSPreciseGCInfos infos1;
  for (int i = 0; i < index; i++) {
    share::SCN row_scn;
    row_scn.convert_for_tx(i);
    ASSERT_EQ(OB_SUCCESS, infos1.push_back(ObSSPreciseGCInfo(GCTX.self_addr(),
                                                             ObLSID(1),
                                                             ObTabletID(i),
                                                             share::SCN::min_scn(),
                                                             row_scn)));
  }

  ObSSPreciseGCInfos infos2;
  for (int i = 0; i < index - 1; i++) {
    share::SCN row_scn;
    row_scn.convert_for_tx(1);
    ASSERT_EQ(OB_SUCCESS, infos2.push_back(ObSSPreciseGCInfo(GCTX.self_addr(),
                                                             ObLSID(1),
                                                             ObTabletID(index - i - 2),
                                                             share::SCN::min_scn(),
                                                             row_scn)));
  }

  ObSSPreciseGCInfos infos;
  ASSERT_EQ(OB_SUCCESS, ObSSPreciseGCRPC::merge_server_gc_infos_(infos1, infos));
  ASSERT_EQ(OB_SUCCESS, ObSSPreciseGCRPC::merge_server_gc_infos_(infos2, infos));
  ASSERT_EQ(3, infos.count());
  for (int i = 0; i < index; i++) {
    if (i == 0) {
      ASSERT_EQ(0, infos[i].row_scn_.get_val_for_tx());
    } else if (i == 1) {
      ASSERT_EQ(1, infos[i].row_scn_.get_val_for_tx());
    } else {
      ASSERT_EQ(2, infos[i].row_scn_.get_val_for_tx());
    }
  }

  // Test OK
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), create_test_env2)
{
  // if you want to use the second node, use it for free @yangyifei
}

} // namespace unittest
} // namespace oceanbase
