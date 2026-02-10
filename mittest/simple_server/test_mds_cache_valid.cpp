/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define protected public
#define private public
#include "common/ob_smart_var.h"
#include "env/ob_simple_cluster_test_base.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "rootserver/compaction_ttl/ob_compaction_ttl_service.h"
#include "share/compaction_ttl/ob_compaction_ttl_util.h"
#include "share/ob_cluster_version.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/table/ob_ttl_util.h"
#include "simple_server/compaction_basic_func.h"
#include "storage/truncate_info/ob_truncate_info_array.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ob_storage_struct.h"
#include "share/ob_ls_id.h"
#include "unittest/storage/ob_truncate_info_helper.h"

namespace oceanbase
{
using namespace storage;
using namespace share;
using namespace common;
using namespace rootserver;
namespace unittest
{

class ObMDSCacheValidTest : public ObSimpleClusterTestBase
{
public:
  ObMDSCacheValidTest() : ObSimpleClusterTestBase("test_mds_cache_valid"), tenant_id_(0)
  {
    ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    ObSqlString sql;
    int64_t affected_rows = 0;
    sql_proxy.write("drop database test", affected_rows);
    sql_proxy.write("create database test", affected_rows);
    sql_proxy.write("use test", affected_rows);
  }
  virtual void SetUp() override
  {
    bool tenant_exist = false;
    if (OB_SUCCESS != check_tenant_exist(tenant_exist) || !tenant_exist) {
      ObSimpleClusterTestBase::SetUp();
      ASSERT_EQ(OB_SUCCESS, create_tenant());
      ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
    }
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_));
    oceanbase::palf::election::MAX_TST = 500 * 1000;
  }
  uint64_t tenant_id_;

private:
  void insert_rows(const char *table_name, const int64_t rowkey_start, const int64_t row_count);
};

#define EXEC_FAIL(sql) ASSERT_NE(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows))
#define EXEC_SUCC(sql) ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows))

void ObMDSCacheValidTest::insert_rows(const char *table_name,
                                      const int64_t rowkey_start,
                                      const int64_t row_count)
{
  ObSqlString sql;
  int64_t affected_rows = 0;
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  for (int64_t i = 0; i < row_count; ++i) {
    sql.assign_fmt("insert into %s values(%ld, 'val%ld')", table_name, rowkey_start + i, i);
    EXEC_SUCC(sql);
  }
}

TEST_F(ObMDSCacheValidTest, mds_cache_valid_after_ttl_and_dump_test)
{
  ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  sqlclient::ObISQLConnection *sys_conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sys_proxy.acquire(sys_conn));

  ObSqlString sql;
  int64_t affected_rows = 0;
  const char *table_name = "mds_cache_test_table";

  sql.assign_fmt("alter system set enable_ttl=true");
  EXEC_SUCC(sql);

  // 1. 建立 rowscn 的 ttl 表
  sql.assign_fmt("create table %s (c1 int primary key, c2 varchar(200)) "
                 "merge_engine=append_only TTL ora_rowscn + INTERVAL 1 second BY COMPACTION",
                 table_name);
  EXEC_SUCC(sql);
  COMMON_LOG(INFO, "create table with rowscn ttl", K(table_name));

  // 2. 写入数据
  const int64_t TEST_ROW_COUNT = 10;
  insert_rows(table_name, 0, TEST_ROW_COUNT);
  COMMON_LOG(INFO, "insert rows", K(TEST_ROW_COUNT));

  // 3. 触发 TTL
  sleep(2);

  int64_t table_id = 0;
  ASSERT_EQ(OB_SUCCESS,
            CompactionBasicFunc::get_table_id(*sys_conn, table_name, false /*is_index*/, table_id));
  COMMON_LOG(INFO, "get table id", K(table_id));
  sql.assign_fmt("alter system trigger ttl");
  EXEC_SUCC(sql);
  COMMON_LOG(INFO, "trigger ttl service");

  // 等待 TTL 任务完成
  sleep(2);

  // 做查询更新 mds_cache
  sql.assign_fmt("select count(*) as cnt from %s", table_name);
  while (true) {
    int ret = OB_SUCCESS;
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult *result = nullptr;
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      result = res.get_result();
      ASSERT_TRUE(result != nullptr);
      ASSERT_EQ(OB_SUCCESS, result->next());
      int64_t cnt = 0;
      ASSERT_EQ(OB_SUCCESS, result->get_int("cnt", cnt));
      COMMON_LOG(INFO, "query result", K(cnt));
      if (cnt == 0) {
        break;
      } else {
        sleep(1);
      }
    }
  }

  // 4. 获取 tablet 并验证 mds_cache 是有效的
  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS,
            CompactionBasicFunc::get_tablet_and_ls_id(*sys_conn, table_id, tablet_id, ls_id));
  COMMON_LOG(INFO, "get tablet and ls id", K(tablet_id), K(ls_id));

  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_tablet(ls_id, tablet_id, tablet_handle));
  ASSERT_TRUE(tablet_handle.is_valid());
  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  ASSERT_TRUE(tablet->ttl_filter_info_cache_.is_valid());

  // 5. 做一次转储
  sql.assign_fmt("alter system minor freeze");
  EXEC_SUCC(sql);
  COMMON_LOG(INFO, "trigger minor freeze");

  // 等待转储完成
  sleep(3);

  // 6. 再次验证 tablet 上 mds_cache 是有效的
  tablet_handle.reset();
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_tablet(ls_id, tablet_id, tablet_handle));
  ASSERT_TRUE(tablet_handle.is_valid());
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  ASSERT_TRUE(tablet->ttl_filter_info_cache_.is_valid());
}

TEST_F(ObMDSCacheValidTest, mds_cache_valid_after_truncate_and_dump_test)
{
  ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  sqlclient::ObISQLConnection *sys_conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sys_proxy.acquire(sys_conn));

  ObSqlString sql;
  int64_t affected_rows = 0;
  const char *table_name = "truncate_cache_test_table";

  // 1. 建立分区表
  sql.assign_fmt("create table %s (c1 int, c2 varchar(200), primary key(c1)) "
                 "partition by range(c1) "
                 "(partition p0 values less than (10), "
                 "partition p1 values less than (20), "
                 "partition p2 values less than (30))",
                 table_name);
  EXEC_SUCC(sql);
  sql.assign_fmt("create index idx1 on %s(c1) global partition by hash(c1) partitions 1;", table_name);
  EXEC_SUCC(sql);

  // 2. 写入数据
  const int64_t TEST_ROW_COUNT = 10;
  for (int64_t i = 0; i < TEST_ROW_COUNT; ++i) {
    sql.assign_fmt("insert into %s values(%ld, 'val%ld')", table_name, i, i);
    EXEC_SUCC(sql);
  }
  COMMON_LOG(INFO, "insert rows", K(TEST_ROW_COUNT));

  // 3. 触发 truncate
  sql.assign_fmt("alter system set _ob_enable_truncate_partition_preserve_global_index = true");
  EXEC_SUCC(sql);
  sql.assign_fmt("alter table %s truncate partition p1", table_name);
  EXEC_SUCC(sql);
  COMMON_LOG(INFO, "trigger truncate partition");

  // 4. 做一次查询更新 mds cache
  sql.assign_fmt("select /*+INDEX(%s idx1)*/* from %s order by c1 asc", table_name, table_name);
  EXEC_SUCC(sql);
  COMMON_LOG(INFO, "query table", K(table_name));

  // 5. 获取 tablet 并验证 mds_cache 是有效的
  int64_t table_id = 0;
  ASSERT_EQ(OB_SUCCESS,
            CompactionBasicFunc::get_table_id(*sys_conn, table_name, true /*is_index*/, table_id));
  COMMON_LOG(INFO, "get table id", K(table_id));

  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS,
            CompactionBasicFunc::get_tablet_and_ls_id(*sys_conn, table_id, tablet_id, ls_id));
  COMMON_LOG(INFO, "get tablet and ls id", K(tablet_id), K(ls_id));

  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_tablet(ls_id, tablet_id, tablet_handle));
  ASSERT_TRUE(tablet_handle.is_valid());
  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  ASSERT_TRUE(tablet->truncate_info_cache_.is_valid());

  // 5. 做一次转储
  sql.assign_fmt("alter system minor freeze");
  EXEC_SUCC(sql);
  COMMON_LOG(INFO, "trigger minor freeze");

  // 等待转储完成
  sleep(3);

  // 6. 再次验证 tablet 上 mds_cache 是有效的
  tablet_handle.reset();
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_tablet(ls_id, tablet_id, tablet_handle));
  ASSERT_TRUE(tablet_handle.is_valid());
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  ASSERT_TRUE(tablet->truncate_info_cache_.is_valid());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
