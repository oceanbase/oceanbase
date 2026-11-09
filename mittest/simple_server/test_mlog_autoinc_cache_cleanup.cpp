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
#include "lib/utility/ob_defer.h"

#define private public
#define protected public
#include "env/ob_simple_cluster_test_base.h"
#include "env/ob_simple_server_helper.h"
#include "share/ob_tablet_autoincrement_service.h"
#undef protected
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;

TEST(TestTabletAutoincCacheCleaner, collect_hidden_partition_tablets)
{
  using namespace schema;
  for (const ObTableType table_type : {USER_TABLE, AUX_LOB_META}) {
    SCOPED_TRACE(table_type);
    const uint64_t tenant_id = 1001;
    ObTableSchema table_schema;
    table_schema.set_tenant_id(tenant_id);
    table_schema.set_table_id(50001);
    table_schema.set_table_type(table_type);
    if (USER_TABLE == table_type) {
      table_schema.set_table_pk_exists_mode(TOM_TABLE_WITHOUT_PK);
    }
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
    table_schema.set_part_num(1);
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_RANGE);
    ObPartition normal_part;
    normal_part.set_part_id(1);
    normal_part.set_tablet_id(ObTabletID(200001));
    normal_part.set_partition_type(PARTITION_TYPE_NORMAL);
    ASSERT_EQ(OB_SUCCESS, table_schema.add_partition(normal_part));
    ObTabletAutoincCacheCleaner normal_cleaner(tenant_id);
    ASSERT_EQ(OB_SUCCESS, normal_cleaner.add_single_table(table_schema));
    ASSERT_EQ(1, normal_cleaner.tablet_ids_.count());
    ASSERT_EQ(normal_part.get_tablet_id(), normal_cleaner.tablet_ids_.at(0));

    // Split recovery drops both visible partitions and the hidden split source.
    ObPartition hidden_part;
    hidden_part.set_part_id(2);
    hidden_part.set_tablet_id(ObTabletID(200002));
    hidden_part.set_partition_type(PARTITION_TYPE_SPLIT_SOURCE);
    ASSERT_EQ(OB_SUCCESS, table_schema.add_partition(hidden_part));
    ASSERT_EQ(1, table_schema.get_hidden_partition_num());
    ObTabletAutoincCacheCleaner cleaner(tenant_id);
    ASSERT_EQ(OB_SUCCESS, cleaner.add_single_table(table_schema));
    ASSERT_EQ(2, cleaner.tablet_ids_.count());
    ASSERT_EQ(normal_part.get_tablet_id(), cleaner.tablet_ids_.at(0));
    ASSERT_EQ(hidden_part.get_tablet_id(), cleaner.tablet_ids_.at(1));
  }
}

TEST(TestTabletAutoincCacheCleaner, empty_commit_needs_no_services)
{
  ObClusterVersion &cluster_version = ObClusterVersion::get_instance();
  const uint64_t saved_version = cluster_version.get_cluster_version();
  DEFER(cluster_version.update_cluster_version(saved_version));
  cluster_version.update_cluster_version(CLUSTER_VERSION_4_3_0_1);
  ObTabletAutoincCacheCleaner cleaner(OB_INVALID_TENANT_ID);
  // An empty cleanup must not look up tenant servers or require RPC/SQL services.
  ASSERT_EQ(OB_SUCCESS, cleaner.commit());
}

class TestMlogAutoincCacheCleanup : public ObSimpleClusterTestBase
{
public:
  TestMlogAutoincCacheCleanup() : ObSimpleClusterTestBase("test_mlog_autoinc_cache_cleanup") {}
};

TEST_F(TestMlogAutoincCacheCleanup, test_mlog_cache_cleanup_after_complete_refresh)
{
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(
      "create table autoinc_mlog_base(id int primary key, v int)", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(
      "create materialized view log on autoinc_mlog_base "
      "with primary key, rowid, sequence (v) including new values", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(
      "create materialized view autoinc_mlog_mv refresh fast on demand "
      "as select id, v from autoinc_mlog_base", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(
      "create materialized view log on autoinc_mlog_mv "
      "with primary key, rowid, sequence (v) including new values", affected_rows));

  auto get_mlog_tablets = [tenant_id](const uint64_t table_id, ObIArray<ObTabletID> &tablet_ids) {
    int ret = OB_SUCCESS;
    schema::ObSchemaGetterGuard guard;
    const schema::ObTableSchema *table_schema = nullptr;
    const schema::ObTableSchema *mlog_schema = nullptr;
    tablet_ids.reuse();
    if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, guard))) {
    } else if (OB_FAIL(guard.get_table_schema(tenant_id, table_id, table_schema))) {
    } else if (OB_ISNULL(table_schema) || !table_schema->has_mlog_table()) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(guard.get_table_schema(tenant_id, table_schema->get_mlog_tid(), mlog_schema))) {
    } else if (OB_ISNULL(mlog_schema)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = mlog_schema->get_tablet_ids(tablet_ids);
    }
    return ret;
  };
  auto cache_lookup = [tenant_id](const ObTabletID &tablet_id) {
    ObTabletAutoincrementService &service = ObTabletAutoincrementService::get_instance();
    ObTabletAutoincKey key;
    key.tenant_id_ = tenant_id;
    key.tablet_id_ = tablet_id;
    ObTabletAutoincMgr *mgr = nullptr;
    const int ret = service.tablet_autoinc_mgr_map_.get(key, mgr);
    if (nullptr != mgr) {
      service.release_mgr(mgr);
    }
    return ret;
  };

  int64_t base_table_id = 0;
  ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(sql_proxy,
      "select table_id as val from oceanbase.__all_table where table_name = 'autoinc_mlog_base'", base_table_id));
  ObArray<ObTabletID> base_mlog_tablets;
  ASSERT_EQ(OB_SUCCESS, get_mlog_tablets(base_table_id, base_mlog_tablets));
  ASSERT_EQ(1, base_mlog_tablets.count());
  for (int64_t round = 0; round < 3; ++round) {
    int64_t container_id = 0;
    ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(sql_proxy,
        "select data_table_id as val from oceanbase.__all_table where table_name = 'autoinc_mlog_mv'", container_id));
    ObArray<ObTabletID> old_mlog_tablets;
    ASSERT_EQ(OB_SUCCESS, get_mlog_tablets(container_id, old_mlog_tablets));
    ASSERT_EQ(1, old_mlog_tablets.count());
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into autoinc_mlog_base values (%ld, %ld)", round, round));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write("call dbms_mview.refresh('autoinc_mlog_mv', 'f')", affected_rows));
    // FAST refresh must create the cache through real mlog writes before COMPLETE replaces it.
    ASSERT_EQ(OB_SUCCESS, cache_lookup(old_mlog_tablets.at(0)));
    ASSERT_EQ(OB_SUCCESS, cache_lookup(base_mlog_tablets.at(0)));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write("call dbms_mview.refresh('autoinc_mlog_mv', 'c')", affected_rows));
    const int64_t deadline = ObTimeUtility::current_time() + 60 * 1000 * 1000;
    int cache_ret = OB_SUCCESS;
    while (OB_SUCCESS == (cache_ret = cache_lookup(old_mlog_tablets.at(0)))
           && ObTimeUtility::current_time() < deadline) {
      ob_usleep(100 * 1000);
    }
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, cache_ret);
    // The base table's live mlog must retain its cache.
    ASSERT_EQ(OB_SUCCESS, cache_lookup(base_mlog_tablets.at(0)));
    int64_t new_container_id = 0;
    ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(sql_proxy,
        "select data_table_id as val from oceanbase.__all_table where table_name = 'autoinc_mlog_mv'", new_container_id));
    ASSERT_NE(container_id, new_container_id);
    ObArray<ObTabletID> new_mlog_tablets;
    ASSERT_EQ(OB_SUCCESS, get_mlog_tablets(new_container_id, new_mlog_tablets));
    ASSERT_EQ(1, new_mlog_tablets.count());
    ASSERT_NE(old_mlog_tablets.at(0), new_mlog_tablets.at(0));
    int64_t row_count = 0;
    ASSERT_EQ(OB_SUCCESS, SimpleServerHelper::select_int64(sql_proxy,
        "select count(*) as val from autoinc_mlog_mv", row_count));
    ASSERT_EQ(round + 1, row_count);
  }
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("drop materialized view autoinc_mlog_mv", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("drop table autoinc_mlog_base", affected_rows));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("WDIAG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
