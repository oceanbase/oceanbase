/**
 * Copyright (c) 2024 OceanBase
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
#define protected public
#define private public
#include "mittest/env/ob_simple_server_helper.h"
#include "env/ob_simple_cluster_test_base.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
using namespace storage;
using namespace share;
namespace unittest
{

class ObUpdateTabletDirectlyTest : public ObSimpleClusterTestBase
{
public:
  ObUpdateTabletDirectlyTest()
    : ObSimpleClusterTestBase("test_update_tablet_directly_when_mini"),
      tenant_id_(1)
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    ObSqlString sql;
    int64_t affected_rows = 0;
    sql_proxy.write("drop database test", affected_rows);
    sql_proxy.write("create database test", affected_rows);
    sql_proxy.write("use test", affected_rows);
  }
  int get_tablet(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle);
  uint64_t tenant_id_;
};

int ObUpdateTabletDirectlyTest::get_tablet(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "failed to get ls", K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret), KP(ls));
  } else if (OB_FAIL(ls->get_tablet_svr()->direct_get_tablet(tablet_id, tablet_handle))) {
    STORAGE_LOG(WARN, "failed to get tablet", K(ret), KP(ls));
  }

  return ret;
}

TEST_F(ObUpdateTabletDirectlyTest, update_tablet_directly)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));

  int64_t affected_rows = 0;
#define WRITE_SQL(str) \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(str, affected_rows));
  WRITE_SQL("create table t1(a int primary key)");
  WRITE_SQL("set ob_trx_timeout = 3000000000");
  WRITE_SQL("set ob_trx_idle_timeout = 3000000000");
  WRITE_SQL("set ob_query_timeout = 3000000000");
  WRITE_SQL("set autocommit=0");

  STORAGE_LOG(INFO, "insert data start");
  WRITE_SQL("begin;");
  WRITE_SQL("insert into t1 values(1);");
  WRITE_SQL("rollback;");
  STORAGE_LOG(INFO, "insert data finish");
  ObSqlString sql;
  sql.assign_fmt("select tablet_id as val from oceanbase.__all_virtual_table where table_name = 't1' limit 1");
  int64_t tablet_id = 0;
  int64_t ls_id = 0;
  if (OB_SUCC(SimpleServerHelper::select_int64(conn, sql.ptr(), tablet_id))) {
    sql.reuse();
    sql.assign_fmt("select ls_id as val from oceanbase.__all_virtual_tablet_to_ls where tablet_id = %ld limit 1", tablet_id);
    if (OB_SUCC(SimpleServerHelper::select_int64(conn, sql.ptr(), ls_id))) {
      sql.reuse();
      sql.assign_fmt("alter system minor freeze tablet_id=%ld", tablet_id);
      WRITE_SQL(sql.ptr());
    }
  }
  int64_t idx = 0;
  ObTabletHandle tablet_handle;
  do {
    ASSERT_EQ(OB_SUCCESS, get_tablet(ObLSID(ls_id), ObTabletID(tablet_id), tablet_handle));
    ObSEArray<ObTableHandleV2, BASIC_MEMSTORE_CNT> memtable_handles;
    if (OB_FAIL(tablet_handle.get_obj()->get_all_memtables_from_memtable_mgr(memtable_handles))) {
      STORAGE_LOG(WARN, "failed to get all memtable", K(ret), K(ls_id), K(tablet_id));
    } else if (memtable_handles.empty()) {
      break;
    } else {
      STORAGE_LOG(INFO, "wait memtable release", K(ret), K(ls_id), K(tablet_id));
      sleep(5);
    }
  } while (idx < 100 && OB_SUCC(ret));
}

} // unittest
} // oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}