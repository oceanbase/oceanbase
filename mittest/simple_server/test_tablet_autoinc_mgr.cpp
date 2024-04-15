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

#include "env/ob_simple_server_restart_helper.h"
#include "env/ob_simple_cluster_test_base.h"
#include "storage_ha/test_transfer_common_fun.h"
#include "lib/ob_errno.h"
#include "rootserver/ob_tenant_transfer_service.h" // ObTenantTransferService
#include "share/transfer/ob_transfer_task_operator.h" // ObTransferTaskOperator
#include "share/location_cache/ob_location_service.h" // ObLocationService
#include "share/ob_tablet_autoincrement_service.h"
#include "storage/high_availability/ob_transfer_handler.h"  //ObTransferHandler
#include "lib/utility/utility.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace unittest;
using rootserver::ObTenantTransferService;
namespace share
{
using namespace common;

static const int64_t TOTAL_NUM = 110;
static uint64_t g_tenant_id;
static ObTransferPartList g_part_list;
static ObSEArray<ObTabletLSPair, TOTAL_NUM> g_tablet_ls_pairs;

class TestTabletAutoincMgr : public unittest::ObSimpleClusterTestBase
{
public:
  TestTabletAutoincMgr() : unittest::ObSimpleClusterTestBase("test_tablet_autoinc_mgr") {}
  int prepare_tablet_ls_pairs(ObMySQLProxy &sql_proxy, const char *table_name, ObIArray<ObTabletLSPair> &tablet_ls_pairs);
  int prepare_part_list(ObMySQLProxy &sql_proxy, const char *table_name, ObTransferPartList &part_list);
};

int TestTabletAutoincMgr::prepare_tablet_ls_pairs(
    ObMySQLProxy &sql_proxy,
    const char *table_name,
    ObIArray<ObTabletLSPair> &tablet_ls_pairs)
{
  int ret = OB_SUCCESS;
  tablet_ls_pairs.reset();
  ObSqlString sql;
  int64_t affected_rows = 0;
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    if (OB_FAIL(sql.assign_fmt("select TABLET_ID, LS_ID from oceanbase.__all_tablet_to_ls where table_id in (select table_id from oceanbase.__all_table where table_id = (select table_id from oceanbase.__all_table where table_name = '%s') union select table_id from oceanbase.__all_table where data_table_id = (select table_id from oceanbase.__all_table where table_name = '%s')) order by TABLET_ID", table_name, table_name))) {
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

int TestTabletAutoincMgr::prepare_part_list(
    ObMySQLProxy &sql_proxy,
    const char *table_name,
    ObTransferPartList &part_list)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    if (OB_FAIL(sql.assign_fmt("select object_id from oceanbase.DBA_OBJECTS where OBJECT_NAME='%s'", table_name))) {
    } else if (OB_FAIL(sql_proxy.read(result, sql.ptr()))) {
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      sqlclient::ObMySQLResult &res = *result.get_result();
      uint64_t table_id = OB_INVALID_ID;
      uint64_t part_id = OB_INVALID_ID;
      int64_t part_count = 0;
      if (OB_SUCC(ret) && OB_SUCC(res.next())) {
        EXTRACT_INT_FIELD_MYSQL(res, "object_id", table_id, uint64_t);
      }
      while(OB_SUCC(ret)) {
        part_id = OB_INVALID_ID;
        ObTransferPartInfo part_info;
        if (OB_SUCC(res.next())) {
          ++part_count;
          EXTRACT_INT_FIELD_MYSQL(res, "object_id", part_id, uint64_t);
          if (OB_FAIL(part_info.init(table_id, part_id))) {
          } else if (OB_FAIL(part_list.push_back(part_info))) {
          }
        }
      }
      if (OB_ITER_END == ret) {
        if (0 == part_count && OB_INVALID_ID != table_id && OB_INVALID_ID == part_id) {
          ObTransferPartInfo part_info(table_id, 0);
          (void)part_list.push_back(part_info);
        }
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to generate data", K(sql));
      }
      LOG_INFO("finish read sql", K(sql), K(part_list), K(table_id), K(part_id));
    }
  }
  return ret;
}

TEST_F(TestTabletAutoincMgr, test_lob_tablet_autoinc_location_cache)
{
  g_tenant_id = OB_INVALID_TENANT_ID;

  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;

  // create table and prepare basic info
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create table t1(c1 int, c2 longtext)"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, prepare_tablet_ls_pairs(sql_proxy, "t1", g_tablet_ls_pairs));
  ASSERT_EQ(OB_SUCCESS, prepare_part_list(sql_proxy, "t1", g_part_list));
  ASSERT_EQ(1, g_part_list.count());
  ASSERT_EQ(3, g_tablet_ls_pairs.count());

  // refresh tablet ls cache
  ObLocationService *location_service = GCTX.location_service_;
  ASSERT_TRUE(OB_NOT_NULL(location_service));
  ObTabletLSService *tablet_ls_service = &(location_service->tablet_ls_service_);
  ObLSLocationService *ls_location_service = &(location_service->ls_location_service_);
  bool is_cache_hit = false;
  ObArray<ObTabletLSCache> old_tablet_ls_cache;
  for (int64_t i = 0; i < g_tablet_ls_pairs.count(); i++) {
    const ObTabletID &tablet_id = g_tablet_ls_pairs.at(i).tablet_id_;
    ObLSID ls_id;
    ObTabletLSCache tablet_ls_cache;
    ASSERT_EQ(OB_SUCCESS, tablet_ls_service->get(g_tenant_id, tablet_id, INT64_MAX, is_cache_hit, ls_id));
    ASSERT_EQ(OB_SUCCESS, tablet_ls_service->get_from_cache_(g_tenant_id, tablet_id, tablet_ls_cache));
    ASSERT_EQ(OB_SUCCESS, old_tablet_ls_cache.push_back(tablet_ls_cache));
  }

  // create other ls by cluster table
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create table dup_table(c1 int) duplicate_scope = 'CLUSTER' partition by hash(c1) partitions 4"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  ObTenantTransferService *tenant_transfer = MTL(ObTenantTransferService*);
  ASSERT_TRUE(OB_NOT_NULL(tenant_transfer));

  // transfer t1 to other ls
  ObTransferTaskID task_id;
  ObTransferTask transfer_task;
  ObMySQLTransaction trans;
  const ObLSID src_ls_id(1001);
  const ObLSID dst_ls_id(1002);
  ASSERT_EQ(OB_SUCCESS, trans.start(&inner_sql_proxy, g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, tenant_transfer->generate_transfer_task(trans, src_ls_id, dst_ls_id, g_part_list, ObBalanceTaskID(123), transfer_task));
  task_id = transfer_task.get_task_id();
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ObTransferStatus expected_status(ObTransferStatus::COMPLETED);
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, wait_transfer_task(g_tenant_id, task_id, expected_status, true/*is_from_his*/, inner_sql_proxy, task));
  ASSERT_EQ(OB_SUCCESS, task.result_);

  // restore old tablet ls cache
  const bool update_only = false;
  for (int64_t i = 0; i < old_tablet_ls_cache.count(); i++) {
    ASSERT_EQ(OB_SUCCESS, tablet_ls_service->update_cache(old_tablet_ls_cache.at(i), update_only));
  }

  // remove source ls and clear src ls cache
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(src_ls_id));
  ObLSLocationCacheKey cache_key(GCONF.cluster_id, g_tenant_id, src_ls_id);
  ObLSLocation tmp_loc;
  ASSERT_EQ(OB_SUCCESS, ls_location_service->inner_cache_.del(cache_key, 0/*safe_delete_time*/));
  ASSERT_EQ(OB_LS_LOCATION_NOT_EXIST, ls_location_service->nonblock_get(GCONF.cluster_id, g_tenant_id, src_ls_id, tmp_loc));

  // insert lob
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into t1 values (2, repeat('abcde0123456789', 1000));"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
}

} // namespace rootserver
} // namespace oceanbase
int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("WDIAG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
