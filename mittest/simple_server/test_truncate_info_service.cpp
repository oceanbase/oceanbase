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
#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define protected public
#define private public
#include "mittest/env/ob_simple_server_helper.h"
#include "env/ob_simple_cluster_test_base.h"
#include "unittest/storage/ob_truncate_info_helper.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/ob_ls_id.h"
#include "storage/truncate_info/ob_truncate_info_array.h"
#include "share/truncate_info/ob_truncate_info_util.h"
#include "storage/truncate_info/ob_mds_info_distinct_mgr.h"

namespace oceanbase
{
using namespace storage;
using namespace share;
namespace unittest
{

class ObTruncateInfoServiceTest : public ObSimpleClusterTestBase
{
public:
  ObTruncateInfoServiceTest()
    : ObSimpleClusterTestBase("test_truncate_info_service"),
      tenant_id_(1),
      allocator_()
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    ObSqlString sql;
    int64_t affected_rows = 0;
    sql_proxy.write("drop database test", affected_rows);
    sql_proxy.write("create database test", affected_rows);
    sql_proxy.write("use test", affected_rows);
  }
  int get_table_schema(
    schema::ObSchemaGetterGuard &schema_guard,
    const int64_t table_id,
    const ObTableSchema *&table_schema);
  int get_table_id(
    sqlclient::ObISQLConnection &conn,
    const char *table_name,
    const bool is_index,
    int64_t &table_id);
  int get_tablet_and_ls_id(
    sqlclient::ObISQLConnection &conn,
    const int64_t table_id,
    ObTabletID &tablet_id,
    ObLSID &ls_id);
  void check_tablet_cache(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const bool is_valid,
    const int64_t newest_commit_version = 0,
    const int64_t newest_schema_version = 0);
  void check_kv_cache(
    ObTablet &tablet,
    const int64_t info_cnt,
    const int64_t newest_commit_version,
    const int64_t newest_schema_version);
  int wait_mds_flush(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id);
  uint64_t tenant_id_;
  ObArenaAllocator allocator_;
};

int ObTruncateInfoServiceTest::get_table_schema(
    schema::ObSchemaGetterGuard &schema_guard,
    const int64_t table_id,
    const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService *schema_service = nullptr;
  table_schema = nullptr;
  int64_t schema_version = 0;
  if (OB_ISNULL(schema_service = MTL(ObTenantSchemaService *)->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema service from MTL", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_refreshed_schema_version(
                    tenant_id_, schema_version))) {
    LOG_WARN("fail to get tenant local schema version", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_service->retry_get_schema_guard(tenant_id_,
                                                          schema_version,
                                                          table_id,
                                                          schema_guard,
                                                          schema_version))) {
    LOG_WARN("failed to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
    LOG_WARN("Fail to get table schema", K(ret), K(table_id));
  }
  return ret;
}

int ObTruncateInfoServiceTest::get_table_id(
    sqlclient::ObISQLConnection &conn,
    const char *table_name,
    const bool is_index,
    int64_t &table_id)
{
  table_id = 0;
  ObSqlString sql;
  sql.assign_fmt("select table_id as val from oceanbase.__all_virtual_table where table_name like '%c%s%s%c' limit 1", '%', table_name, is_index ? "_idx" : "", '%');
  return SimpleServerHelper::select_int64(&conn, sql.ptr(), table_id);
}

int ObTruncateInfoServiceTest::get_tablet_and_ls_id(
    sqlclient::ObISQLConnection &conn,
    const int64_t table_id,
    ObTabletID &tablet_id,
    ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  int64_t tmp_tablet_id = 0;
  int64_t tmp_ls_id = 0;
  ObSqlString sql;
  sql.assign_fmt("select tablet_id as val from oceanbase.__all_virtual_tablet_to_ls where table_id = %ld limit 1", table_id);
  if (OB_SUCC(SimpleServerHelper::select_int64(&conn, sql.ptr(), tmp_tablet_id))) {
    sql.reuse();
    sql.assign_fmt("select ls_id as val from oceanbase.__all_virtual_tablet_to_ls where table_id = %ld and tablet_id = %ld limit 1", table_id, tmp_tablet_id);
    if (OB_SUCC(SimpleServerHelper::select_int64(&conn, sql.ptr(), tmp_ls_id))) {
      tablet_id = ObTabletID(tmp_tablet_id);
      ls_id = ObLSID(tmp_ls_id);
    }
  }
  return ret;
}

void ObTruncateInfoServiceTest::check_tablet_cache(
  const ObLSID &ls_id,
  const ObTabletID &tablet_id,
  const bool is_valid,
  const int64_t newest_commit_version,
  const int64_t newest_schema_version)
{
  LOG_INFO("read_truncate_info_array check_tablet_cache", K(ls_id), K(tablet_id), K(is_valid));
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_tablet(ls_id, tablet_id, tablet_handle));
  ObTruncateInfoCache &truncate_info_cache = tablet_handle.get_obj()->truncate_info_cache_;
  ASSERT_EQ(is_valid, truncate_info_cache.is_valid());
  int64_t read_commit_version = 0;
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_truncate_info_newest_version(read_commit_version, count));
  if (is_valid) {
    ASSERT_EQ(newest_commit_version, truncate_info_cache.newest_commit_version_);
    ASSERT_EQ(newest_schema_version, truncate_info_cache.newest_schema_version_);
    ASSERT_EQ(newest_commit_version, read_commit_version);
  }
}

void ObTruncateInfoServiceTest::check_kv_cache(
    ObTablet &tablet,
    const int64_t info_cnt,
    const int64_t newest_commit_version,
    const int64_t newest_schema_version)
{
  ObTruncateInfoCacheKey cache_key(tenant_id_, tablet.get_tablet_id(), newest_schema_version, tablet.get_last_major_snapshot_version());
  ObTruncateInfoArray array;
  int ret = ObTruncateInfoKVCacheUtil::get_truncate_info_array(
                            allocator_, cache_key, array);
  LOG_INFO("read_truncate_info_array check_kv_cache", K(tablet.get_tablet_id()), K(ret));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(info_cnt, array.count());
  ASSERT_EQ(newest_commit_version, array.at(array.count() - 1)->commit_version_);
  ASSERT_EQ(newest_schema_version, array.at(array.count() - 1)->schema_version_);
}

int ObTruncateInfoServiceTest::wait_mds_flush(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  SCN max_decided_scn;
  SCN rec_scn;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;

  if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    COMMON_LOG(WARN, "failed to get ls", K(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->direct_get_tablet(tablet_id, tablet_handle))) {
    COMMON_LOG(WARN, "failed to get tablet", K(ret), K(ls_handle));
  } else if (OB_FAIL(ls_handle.get_ls()->get_max_decided_scn(max_decided_scn))) {
    COMMON_LOG(WARN, "failed to get max decided scn", K(ret), K(ls_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->mds_table_flush(max_decided_scn))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      COMMON_LOG(WARN, "failed to get tablet", K(ret), K(ls_handle));
    }
  } else {
    rec_scn = SCN::max_scn();
    while (OB_SUCC(ret) && SCN::max_scn() != rec_scn) {
      sleep(5);
      ObTabletPointer* pointer = dynamic_cast<ObTabletPointer*>(tablet_handle.get_obj()->pointer_hdl_.get_resource_ptr());
      if (pointer->mds_table_handler_.mds_table_handle_.is_valid()) {
        ret = pointer->mds_table_handler_.mds_table_handle_.get_rec_scn(rec_scn);
        COMMON_LOG(INFO, "print rec_scn", K(tablet_id), K(rec_scn));
      }
    };
  }
  return ret;
}

TEST_F(ObTruncateInfoServiceTest, truncate_range_part)
{
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));

  ObSqlString sql;
  
  sql.assign_fmt("create table t_range1(c1 int, c2 int, c3 int, c4 int, primary key(c1, c2)) \
                  partition by range(c1) \
                  subpartition by range(c2) \
                  ( \
                    partition p0 values less than (10) \
                    ( \
                      subpartition p0sp0 values less than (100), \
                      subpartition p0sp1 values less than (200), \
                      subpartition p0sp2 values less than (300) \
                    ), \
                    partition p1 values less than (20) \
                    ( \
                      subpartition p1sp0 values less than (400), \
                      subpartition p1sp1 values less than (500), \
                      subpartition p1sp2 values less than (600) \
                    ), \
                    partition p2 values less than (30) \
                    ( \
                      subpartition p2sp0 values less than (700), \
                      subpartition p2sp1 values less than (800), \
                      subpartition p2sp2 values less than (MAXVALUE) \
                    ) \
                  )");
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set _ob_enable_truncate_partition_preserve_global_index = true", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create index t_range1_idx on t_range1(c2) global", affected_rows));

  int64_t data_table_id = 0, index_table_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_table_id(*conn, "t_range1", false/*is_index*/, data_table_id));
  ASSERT_EQ(OB_SUCCESS, get_table_id(*conn, "t_range1", true/*is_index*/, index_table_id));

  schema::ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard, data_table_id, table_schema));
  ObObj range_end_obj;
  ObRowkey range_end_rowkey(&range_end_obj, 1);
  const ObPartition *part = nullptr;
  const ObSubPartition *subpart = nullptr;
  const ObBasePartition *ret_part = nullptr;
  const ObBasePartition *ret_prev_part = nullptr;
#define CHECK_PART_HIGH_BOUND(num)                                             \
  range_end_obj.set_int(num);                                                  \
  ASSERT_EQ(range_end_rowkey, part->get_high_bound_val());
#define CHECK_SUBPART_HIGH_BOUND(num)                                          \
  range_end_obj.set_int(num);                                                  \
  ASSERT_EQ(range_end_rowkey, subpart->get_high_bound_val());
#define CHECK_PREV_PART_HIGH_BOUND(num)                                        \
  range_end_obj.set_int(num);                                                  \
  ASSERT_EQ(range_end_rowkey, ret_prev_part->get_high_bound_val());
  {
    ObString part_str("p0");
    ASSERT_EQ(OB_SUCCESS, table_schema->get_partition_by_name(part_str, part));
    ASSERT_EQ(0, part_str.compare(part->get_part_name()));
    CHECK_PART_HIGH_BOUND(10);

    ASSERT_EQ(OB_SUCCESS, table_schema->get_partition_and_prev_by_name(part_str, PARTITION_LEVEL_ONE, ret_part, ret_prev_part));
    ASSERT_EQ(0, part_str.compare(ret_part->get_part_name()));
    ASSERT_EQ(nullptr, ret_prev_part);
  }
  {
    ObString part_str("p2");
    ASSERT_EQ(OB_SUCCESS, table_schema->get_partition_by_name(part_str, part));
    ASSERT_EQ(0, part_str.compare(part->get_part_name()));
    CHECK_PART_HIGH_BOUND(30);

    ASSERT_EQ(OB_SUCCESS, table_schema->get_partition_and_prev_by_name(part_str, PARTITION_LEVEL_ONE, ret_part, ret_prev_part));
    ObString prev_part_str("p1");
    ASSERT_EQ(0, prev_part_str.compare(ret_prev_part->get_part_name()));
    CHECK_PREV_PART_HIGH_BOUND(20);
  }
  {
    ObString part_str("p8");
    ASSERT_EQ(OB_UNKNOWN_PARTITION, table_schema->get_partition_by_name(part_str, part));
    ASSERT_EQ(OB_UNKNOWN_PARTITION, table_schema->get_subpartition_by_name(part_str, part, subpart));
  }
  {
    ObString part_str("p1");
    ObString subpart_str("p1sp1");
    ASSERT_EQ(OB_SUCCESS, table_schema->get_subpartition_by_name(subpart_str, part, subpart));
    ASSERT_EQ(0, part_str.compare(part->get_part_name()));
    CHECK_PART_HIGH_BOUND(20);
    ASSERT_EQ(0, subpart_str.compare(subpart->get_part_name()));
    CHECK_SUBPART_HIGH_BOUND(500);

    ASSERT_EQ(OB_SUCCESS, table_schema->get_partition_and_prev_by_name(subpart_str, PARTITION_LEVEL_TWO, ret_part, ret_prev_part));
    ASSERT_EQ(0, subpart_str.compare(ret_part->get_part_name()));
    ObString prev_subpart_str("p1sp0");
    ASSERT_EQ(0, prev_subpart_str.compare(ret_prev_part->get_part_name()));
    CHECK_PREV_PART_HIGH_BOUND(400);
  }
  {
    ObString part_str("p2");
    ObString subpart_str("p2sp0");
    ASSERT_EQ(OB_SUCCESS, table_schema->get_subpartition_by_name(subpart_str, part, subpart));
    ASSERT_EQ(0, part_str.compare(part->get_part_name()));
    CHECK_PART_HIGH_BOUND(30);
    ASSERT_EQ(0, subpart_str.compare(subpart->get_part_name()));
    CHECK_SUBPART_HIGH_BOUND(700);

    ASSERT_EQ(OB_SUCCESS, table_schema->get_partition_and_prev_by_name(subpart_str, PARTITION_LEVEL_TWO, ret_part, ret_prev_part));
    ASSERT_EQ(0, subpart_str.compare(ret_part->get_part_name()));
    ASSERT_EQ(nullptr, ret_prev_part);
  }
  {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter table t_range1 truncate partition p1", affected_rows));
    ObTabletID tablet_id;
    ObLSID ls_id;
    ObTabletHandle tablet_handle;
    int64_t newest_commit_version = 0;
    int64_t newest_schema_version = 0;
    int64_t read_commit_version = 0;
    ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, index_table_id, tablet_id, ls_id));
    ASSERT_EQ(OB_SUCCESS, wait_mds_flush(ls_id, tablet_id));
    ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_tablet(ls_id, tablet_id, tablet_handle));
    // before first read_truncate_info_array, cache is invalid
    check_tablet_cache(ls_id, tablet_id, false/*is_valid*/);

    ObTruncateInfoArray truncate_info_array;
    ASSERT_EQ(OB_SUCCESS,
              TruncateInfoHelper::read_distinct_truncate_info_array(
                  allocator_, ls_id, tablet_id,
                  ObVersionRange(0, EXIST_READ_SNAPSHOT_VERSION), truncate_info_array));
    ASSERT_EQ(1, truncate_info_array.count());

    ObTruncatePartition truncate_part;
    ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, 10, 20, truncate_part));
    ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 1/*part_key_idx*/, truncate_part));
    bool equal = false;
    if (OB_NOT_NULL(truncate_info_array.at(0))) {
      ASSERT_FALSE(truncate_info_array.at(0)->is_sub_part_);
      LOG_INFO("print 111", KPC(truncate_info_array.at(0)), K(truncate_part));
      ASSERT_EQ(OB_SUCCESS, truncate_part.compare(truncate_info_array.at(0)->truncate_part_, equal));
      ASSERT_TRUE(equal);
      newest_commit_version = truncate_info_array.at(0)->commit_version_;
      newest_schema_version = truncate_info_array.at(0)->schema_version_;

      check_tablet_cache(ls_id, tablet_id, true/*is_valid*/, newest_commit_version, newest_schema_version);
      check_kv_cache(*tablet_handle.get_obj(), 1/*info_cnt*/, newest_commit_version, newest_schema_version);
      LOG_INFO("read_truncate_info_array success to check tablet cache 1", KR(ret), K(tablet_id), K(newest_commit_version), K(newest_schema_version));
    }

    ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter table t_range1 truncate subpartition p1sp2", affected_rows));
    ASSERT_EQ(OB_SUCCESS,
              TruncateInfoHelper::read_distinct_truncate_info_array(
                  allocator_, ls_id, tablet_id,
                  ObVersionRange(newest_commit_version, EXIST_READ_SNAPSHOT_VERSION), truncate_info_array));
    // this query should not update tablet cache
    check_tablet_cache(ls_id, tablet_id, false/*is_valid*/);
    ASSERT_EQ(1, truncate_info_array.count());
    if (OB_NOT_NULL(truncate_info_array.at(0))) {
      newest_commit_version = truncate_info_array.at(0)->commit_version_;
      ASSERT_TRUE(truncate_info_array.at(0)->is_sub_part_);
      LOG_INFO("print 222", KPC(truncate_info_array.at(0)), K(truncate_part));
      ASSERT_EQ(OB_SUCCESS, truncate_part.compare(truncate_info_array.at(0)->truncate_part_, equal));
      ASSERT_TRUE(equal);
      ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, 500, 600, truncate_part));
      ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 0/*part_key_idx*/, truncate_part));
      ASSERT_EQ(OB_SUCCESS, truncate_part.compare(truncate_info_array.at(0)->truncate_subpart_, equal));
      ASSERT_TRUE(equal);
      newest_commit_version = truncate_info_array.at(0)->commit_version_;
      newest_schema_version = truncate_info_array.at(0)->schema_version_;
    }
    ASSERT_EQ(OB_SUCCESS,
              TruncateInfoHelper::read_distinct_truncate_info_array(
                  allocator_, ls_id, tablet_id,
                  ObVersionRange(0, EXIST_READ_SNAPSHOT_VERSION), truncate_info_array));
    check_tablet_cache(ls_id, tablet_id, true/*is_valid*/, newest_commit_version, newest_schema_version);
    check_kv_cache(*tablet_handle.get_obj(), 2/*info_cnt*/, newest_commit_version, newest_schema_version);
    LOG_INFO("read_truncate_info_array success to check tablet cache 2", KR(ret), K(tablet_id), K(newest_commit_version), K(newest_schema_version));
  }
#undef CHECK_PREV_PART_HIGH_BOUND
#undef CHECK_SUBPART_HIGH_BOUND
#undef CHECK_PART_HIGH_BOUND
}

TEST_F(ObTruncateInfoServiceTest, truncate_range_columns_part)
{
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));

  ObSqlString sql;
  sql.assign_fmt("create table t_range2(c1 int, c2 int, c3 int, c4 int, primary key(c1, c2, c3, c4)) \
                  partition by range columns(c1, c3) \
                  subpartition by range columns(c2, c4) \
                  ( \
                    partition p0 values less than (10, 10) \
                    ( \
                      subpartition p0sp0 values less than (100, 100), \
                      subpartition p0sp1 values less than (200, 200), \
                      subpartition p0sp2 values less than (300, 300) \
                    ), \
                    partition p1 values less than (20, 20) \
                    ( \
                      subpartition p1sp0 values less than (400, 400), \
                      subpartition p1sp1 values less than (500, 500), \
                      subpartition p1sp2 values less than (600, 600) \
                    ), \
                    partition p2 values less than (30, 30) \
                    ( \
                      subpartition p2sp0 values less than (700, 700), \
                      subpartition p2sp1 values less than (800, 800), \
                      subpartition p2sp2 values less than (MAXVALUE, MAXVALUE) \
                    ) \
                  )");
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set _ob_enable_truncate_partition_preserve_global_index = true", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create index t_range2_idx on t_range2(c2) global", affected_rows));

  int64_t data_table_id = 0, index_table_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_table_id(*conn, "t_range2", false/*is_index*/, data_table_id));
  ASSERT_EQ(OB_SUCCESS, get_table_id(*conn, "t_range2", true/*is_index*/, index_table_id));

  schema::ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard, data_table_id, table_schema));
  ObObj range_start_obj[2], range_end_obj[2];
  ObRowkey range_start_rowkey(range_start_obj, 2);
  ObRowkey range_end_rowkey(range_end_obj, 2);
#define GENERATE_START_RANGE(num)                                                   \
  range_start_obj[0].set_int(num);                                                  \
  range_start_obj[1].set_int(num);
#define GENERATE_END_RANGE(num)                                                   \
  range_end_obj[0].set_int(num);                                                  \
  range_end_obj[1].set_int(num);
  {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter table t_range2 truncate partition p1", affected_rows));
    ObTabletID tablet_id;
    ObLSID ls_id;
    ObTabletHandle tablet_handle;
    int64_t newest_commit_version = 0;
    int64_t newest_schema_version = 0;
    ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, index_table_id, tablet_id, ls_id));
    ASSERT_EQ(OB_SUCCESS, wait_mds_flush(ls_id, tablet_id));
    ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_tablet(ls_id, tablet_id, tablet_handle));
    // before first read_truncate_info_array, cache is invalid
    check_tablet_cache(ls_id, tablet_id, false/*is_valid*/);

    ObTruncateInfoArray truncate_info_array;
    ASSERT_EQ(OB_SUCCESS,
              TruncateInfoHelper::read_distinct_truncate_info_array(
                  allocator_, ls_id, tablet_id,
                  ObVersionRange(0, EXIST_READ_SNAPSHOT_VERSION), truncate_info_array));
    ASSERT_EQ(1, truncate_info_array.count());

    ObTruncatePartition truncate_part;
    GENERATE_START_RANGE(10);
    GENERATE_END_RANGE(20);
    ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, range_start_rowkey, range_end_rowkey, truncate_part));
    const int64_t col_idxs1[] = {1, 2};
    ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 2/*cnt*/, col_idxs1/*part_key_idx*/, truncate_part));
    bool equal = false;
    if (OB_NOT_NULL(truncate_info_array.at(0))) {
      ASSERT_FALSE(truncate_info_array.at(0)->is_sub_part_);
      LOG_INFO("print111", KPC(truncate_info_array.at(0)), K(truncate_part));
      ASSERT_EQ(OB_SUCCESS, truncate_part.compare(truncate_info_array.at(0)->truncate_part_, equal));
      ASSERT_TRUE(equal);
      newest_commit_version = truncate_info_array.at(0)->commit_version_;
      newest_schema_version = truncate_info_array.at(0)->schema_version_;
      check_tablet_cache(ls_id, tablet_id, true/*is_valid*/, newest_commit_version, newest_schema_version);
      check_kv_cache(*tablet_handle.get_obj(), 1/*info_cnt*/, newest_commit_version, newest_schema_version);
    }

    ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter table t_range2 truncate subpartition p1sp2", affected_rows));
    ASSERT_EQ(OB_SUCCESS,
              TruncateInfoHelper::read_distinct_truncate_info_array(
                  allocator_, ls_id, tablet_id,
                  ObVersionRange(newest_commit_version, EXIST_READ_SNAPSHOT_VERSION), truncate_info_array));
    // this query should not update tablet cache
    check_tablet_cache(ls_id, tablet_id, false/*is_valid*/);
    ASSERT_EQ(1, truncate_info_array.count());
    if (OB_NOT_NULL(truncate_info_array.at(0))) {
      ObTruncateInfo *cmp_info = truncate_info_array.at(0);
      newest_commit_version = cmp_info->commit_version_;
      newest_schema_version = cmp_info->schema_version_;
      ASSERT_TRUE(cmp_info->is_sub_part_);
      LOG_INFO("print222", KPC(cmp_info), K(truncate_part));
      ASSERT_EQ(OB_SUCCESS, truncate_part.compare(cmp_info->truncate_part_, equal));
      ASSERT_TRUE(equal);
      GENERATE_START_RANGE(500);
      GENERATE_END_RANGE(600);
      ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, range_start_rowkey, range_end_rowkey, truncate_part));
      const int64_t col_idxs2[] = {0, 3};
      ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 2/*cnt*/, col_idxs2/*part_key_idx*/, truncate_part));
      ASSERT_EQ(OB_SUCCESS, truncate_part.compare(cmp_info->truncate_subpart_, equal));
      ASSERT_TRUE(equal);
    }
    ASSERT_EQ(OB_SUCCESS,
              TruncateInfoHelper::read_distinct_truncate_info_array(
                  allocator_, ls_id, tablet_id,
                  ObVersionRange(0, EXIST_READ_SNAPSHOT_VERSION), truncate_info_array));
    check_tablet_cache(ls_id, tablet_id, true/*is_valid*/, newest_commit_version, newest_schema_version);
    check_kv_cache(*tablet_handle.get_obj(), 2/*info_cnt*/, newest_commit_version, newest_schema_version);
  }
#undef GENERATE_START_RANGE
#undef GENERATE_END_RANGE
}

TEST_F(ObTruncateInfoServiceTest, truncate_list_part)
{
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));

  ObSqlString sql;
  sql.assign_fmt("create table t_list1(c1 int, c2 int, c3 int, c4 int, primary key(c1, c2)) \
                  partition by range(c1) \
                  subpartition by list(c2) \
                  ( \
                    partition p0 values less than (10) \
                    ( \
                      subpartition p0sp0 values in (100, 200, 300), \
                      subpartition p0sp1 values in (400), \
                      subpartition p0sp2 values in (DEFAULT) \
                    ), \
                    partition p1 values less than (20) \
                    ( \
                      subpartition p1sp0 values in (500), \
                      subpartition p1sp1 values in (600), \
                      subpartition p1sp2 values in (700) \
                    ), \
                    partition p2 values less than (MAXVALUE) \
                    ( \
                      subpartition p2sp0 values in (800), \
                      subpartition p2sp1 values in (900, 1000), \
                      subpartition p2sp2 values in (DEFAULT) \
                    ) \
                  )");
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set _ob_enable_truncate_partition_preserve_global_index = true", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create index t_list1_idx on t_list1(c2) global", affected_rows));

  int64_t data_table_id = 0, index_table_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_table_id(*conn, "t_list1", false/*is_index*/, data_table_id));
  ASSERT_EQ(OB_SUCCESS, get_table_id(*conn, "t_list1", true/*is_index*/, index_table_id));

  schema::ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard, data_table_id, table_schema));
  ObObj range_end_obj;
  ObRowkey range_end_rowkey(&range_end_obj, 1);
  const ObPartition *part = nullptr;
  const ObSubPartition *subpart = nullptr;
  const ObBasePartition *ret_part = nullptr;
  const ObBasePartition *ret_prev_part = nullptr;
#define CHECK_PART_HIGH_BOUND(num)                                             \
  range_end_obj.set_int(num);                                                  \
  ASSERT_EQ(range_end_rowkey, part->get_high_bound_val());
  {
    ObString part_str("p0");
    ASSERT_EQ(OB_SUCCESS, table_schema->get_partition_by_name(part_str, part));
    ASSERT_EQ(0, part_str.compare(part->get_part_name()));
    CHECK_PART_HIGH_BOUND(10);

    ASSERT_EQ(OB_SUCCESS, table_schema->get_partition_and_prev_by_name(part_str, PARTITION_LEVEL_ONE, ret_part, ret_prev_part));
    ASSERT_EQ(0, part_str.compare(ret_part->get_part_name()));
    ASSERT_EQ(nullptr, ret_prev_part);
  }
  {
    ObString part_str("p10");
    ASSERT_EQ(OB_UNKNOWN_PARTITION, table_schema->get_partition_by_name(part_str, part));
    ASSERT_EQ(OB_UNKNOWN_PARTITION, table_schema->get_subpartition_by_name(part_str, part, subpart));
  }
  {
    ObString part_str("p1");
    ObString subpart_str("p1sp1");
    ASSERT_EQ(OB_SUCCESS, table_schema->get_subpartition_by_name(subpart_str, part, subpart));
    ASSERT_EQ(0, part_str.compare(part->get_part_name()));
    CHECK_PART_HIGH_BOUND(20);
    ASSERT_EQ(0, subpart_str.compare(subpart->get_part_name()));
    ASSERT_EQ(1, subpart->get_list_row_values().count());

    ASSERT_EQ(OB_SUCCESS, table_schema->get_partition_and_prev_by_name(subpart_str, PARTITION_LEVEL_TWO, ret_part, ret_prev_part));
    ASSERT_EQ(0, subpart_str.compare(ret_part->get_part_name()));
    ObString prev_subpart_str("p1sp0");
    ASSERT_EQ(0, prev_subpart_str.compare(ret_prev_part->get_part_name()));
  }
  {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter table t_list1 truncate partition p1", affected_rows));
    ObTabletID tablet_id;
    ObLSID ls_id;
    ObTabletHandle tablet_handle;
    int64_t last_commit_version = 0;
    bool equal = false;
    ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, index_table_id, tablet_id, ls_id));
    ASSERT_EQ(OB_SUCCESS, wait_mds_flush(ls_id, tablet_id));
    ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_tablet(ls_id, tablet_id, tablet_handle));
    ObTruncateInfoArray truncate_info_array;
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->read_truncate_info_array(
      allocator_, ObVersionRange(0, EXIST_READ_SNAPSHOT_VERSION), false/*for_access*/, truncate_info_array));
    ASSERT_EQ(1, truncate_info_array.count());

    ObTruncatePartition truncate_part;
    ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, 10, 20, truncate_part));
    ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 1/*part_key_idx*/, truncate_part));
    if (OB_NOT_NULL(truncate_info_array.at(0))) {
      ASSERT_FALSE(truncate_info_array.at(0)->is_sub_part_);
      LOG_INFO("print", KPC(truncate_info_array.at(0)), K(truncate_part));
      ASSERT_EQ(OB_SUCCESS, truncate_part.compare(truncate_info_array.at(0)->truncate_part_, equal));
      ASSERT_TRUE(equal);
      last_commit_version = truncate_info_array.at(0)->commit_version_;
    }

    ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter table t_list1 truncate subpartition p2sp2", affected_rows));
    truncate_info_array.reset();
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->read_truncate_info_array(
      allocator_, ObVersionRange(last_commit_version, EXIST_READ_SNAPSHOT_VERSION), false/*for_access*/, truncate_info_array));
    ASSERT_EQ(1, truncate_info_array.count());
    if (OB_NOT_NULL(truncate_info_array.at(0))) {
      last_commit_version = truncate_info_array.at(0)->commit_version_;
      ASSERT_TRUE(truncate_info_array.at(0)->is_sub_part_);
      ObObj range_begin_obj, range_end_obj;
      ObRowkey range_begin_rowkey(&range_begin_obj, 1);
      ObRowkey range_end_rowkey(&range_end_obj, 1);
      range_begin_obj.set_int(20);
      range_end_obj.set_max_value();
      ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, range_begin_rowkey, range_end_rowkey, truncate_part));
      LOG_INFO("print", KPC(truncate_info_array.at(0)), K(truncate_part));
      ASSERT_EQ(OB_SUCCESS, truncate_part.compare(truncate_info_array.at(0)->truncate_part_, equal));
      ASSERT_TRUE(equal);
      const int64_t list_val_cnt = 3;
      int64_t list_vals[] = {800, 900, 1000};
      ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, list_vals, list_val_cnt, truncate_part));
      ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 0/*part_key_idx*/, truncate_part));
      truncate_part.part_op_ = ObTruncatePartition::EXCEPT;
      LOG_INFO("print", KPC(truncate_info_array.at(0)), K(truncate_part));
      ASSERT_EQ(OB_SUCCESS, truncate_part.compare(truncate_info_array.at(0)->truncate_subpart_, equal));
      ASSERT_TRUE(equal);
    }

    // create column store global index, truncate will rebuild index
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create index t_list1_idx2 on t_list1(c3) global with column group(all columns, each column)", affected_rows));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter table t_list1 truncate subpartition p2sp2", affected_rows));
    truncate_info_array.reset();
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->read_truncate_info_array(
      allocator_, ObVersionRange(last_commit_version, EXIST_READ_SNAPSHOT_VERSION), false/*for_access*/, truncate_info_array));
    ASSERT_EQ(1, truncate_info_array.count());
  }
}

TEST_F(ObTruncateInfoServiceTest, truncate_with_expr_part_key)
{
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));

  ObSqlString sql;
  sql.assign_fmt("create table t_expr1(c1 int, c2 int, c3 int, c4 int, primary key(c1, c2)) \
                  partition by range (c1 + c2) \
                  ( \
                    partition p0 values less than (10), \
                    partition p1 values less than (20), \
                    partition p2 values less than (MAXVALUE) \
                  )");
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set _ob_enable_truncate_partition_preserve_global_index = true", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create index t_expr1_idx on t_expr1(c2) global", affected_rows));

  int64_t index_table_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_table_id(*conn, "t_expr1", true/*is_index*/, index_table_id));
  schema::ObSchemaGetterGuard schema_guard;
  const ObTableSchema *index_table_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard, index_table_id, index_table_schema));
  int64_t index_table_id_after_truncate = 0;

  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter table t_expr1 truncate partition p0", affected_rows));
  ASSERT_EQ(OB_SUCCESS, get_table_id(*conn, "t_expr1", true/*is_index*/, index_table_id_after_truncate));
  ASSERT_TRUE(index_table_id != index_table_id_after_truncate);

/*
* create t_expr2
*/
  sql.reuse();
  sql.assign_fmt("create table t_expr2(c1 int, c2 int, c3 int, c4 int, primary key(c1, c2)) \
                  partition by range(c1 + c2) \
                  subpartition by list(c2) \
                  ( \
                    partition p0 values less than (10) \
                    ( \
                      subpartition p0sp0 values in (100, 200, 300), \
                      subpartition p0sp1 values in (400), \
                      subpartition p0sp2 values in (DEFAULT) \
                    ), \
                    partition p1 values less than (20) \
                    ( \
                      subpartition p1sp0 values in (500), \
                      subpartition p1sp1 values in (600), \
                      subpartition p1sp2 values in (700) \
                    ), \
                    partition p2 values less than (MAXVALUE) \
                    ( \
                      subpartition p2sp0 values in (800), \
                      subpartition p2sp1 values in (900, 1000), \
                      subpartition p2sp2 values in (DEFAULT) \
                    ) \
                  )");
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create index t_expr2_idx on t_expr2(c2) global", affected_rows));

  ASSERT_EQ(OB_SUCCESS, get_table_id(*conn, "t_expr2", true/*is_index*/, index_table_id));

  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter table t_expr2 truncate partition p0", affected_rows));
  ASSERT_EQ(OB_SUCCESS, get_table_id(*conn, "t_expr2", true/*is_index*/, index_table_id_after_truncate));
  ASSERT_TRUE(index_table_id != index_table_id_after_truncate);
  index_table_id = index_table_id_after_truncate;

  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter table t_expr2 truncate subpartition p2sp0", affected_rows));
  ASSERT_EQ(OB_SUCCESS, get_table_id(*conn, "t_expr2", true/*is_index*/, index_table_id_after_truncate));
  ASSERT_TRUE(index_table_id != index_table_id_after_truncate);
/*
* create t_expr3
*/
  sql.reuse();
  sql.assign_fmt("create table t_expr3(c1 int, c2 int, c3 int, c4 int, primary key(c1, c2, c3)) \
                  partition by range(c1) \
                  subpartition by list(c2 + c3) \
                  ( \
                    partition p0 values less than (10) \
                    ( \
                      subpartition p0sp0 values in (100, 200, 300), \
                      subpartition p0sp1 values in (400), \
                      subpartition p0sp2 values in (DEFAULT) \
                    ), \
                    partition p1 values less than (20) \
                    ( \
                      subpartition p1sp0 values in (500), \
                      subpartition p1sp1 values in (600), \
                      subpartition p1sp2 values in (700) \
                    ), \
                    partition p2 values less than (MAXVALUE) \
                    ( \
                      subpartition p2sp0 values in (800), \
                      subpartition p2sp1 values in (900, 1000), \
                      subpartition p2sp2 values in (DEFAULT) \
                    ) \
                  )");
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create index t_expr3_idx on t_expr3(c2) global", affected_rows));

  ASSERT_EQ(OB_SUCCESS, get_table_id(*conn, "t_expr3", true/*is_index*/, index_table_id));

  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter table t_expr3 truncate subpartition p2sp0", affected_rows));
  ASSERT_EQ(OB_SUCCESS, get_table_id(*conn, "t_expr3", true/*is_index*/, index_table_id_after_truncate));
  ASSERT_TRUE(index_table_id != index_table_id_after_truncate);
  index_table_id = index_table_id_after_truncate;

  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter table t_expr3 truncate partition p2", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter table t_expr3 truncate partition p2", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter table t_expr3 truncate partition p2", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter table t_expr3 truncate partition p2", affected_rows));
  ASSERT_EQ(OB_SUCCESS, get_table_id(*conn, "t_expr3", true/*is_index*/, index_table_id_after_truncate));
  ASSERT_EQ(index_table_id, index_table_id_after_truncate);
  ObTruncateInfoArray truncate_info_array;
  ObTabletID tablet_id;
  ObLSID ls_id;
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, index_table_id, tablet_id, ls_id));
  ASSERT_EQ(OB_SUCCESS, wait_mds_flush(ls_id, tablet_id));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_tablet(ls_id, tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS,
              TruncateInfoHelper::read_distinct_truncate_info_array(
                  allocator_, ls_id, tablet_id,
                  ObVersionRange(0, EXIST_READ_SNAPSHOT_VERSION), truncate_info_array));
  ASSERT_EQ(1, truncate_info_array.count());
}

TEST_F(ObTruncateInfoServiceTest, truncate_default_part)
{
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));

  ObSqlString sql;
  sql.assign_fmt("create table t_list_default(c1 int, c2 int, primary key(c1, c2)) partition by list(c2) ( \
                  partition p0 values in (DEFAULT))");
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set _ob_enable_truncate_partition_preserve_global_index = true", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create index t_list_default_idx on t_list_default(c2) global", affected_rows));

  int64_t index_table_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_table_id(*conn, "t_list_default", true/*is_index*/, index_table_id));
  schema::ObSchemaGetterGuard schema_guard;
  const ObTableSchema *index_table_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_table_schema(schema_guard, index_table_id, index_table_schema));
  int64_t index_table_id_after_truncate = 0;

  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter table t_list_default truncate partition p0", affected_rows));
  ASSERT_EQ(OB_SUCCESS, get_table_id(*conn, "t_list_default", true/*is_index*/, index_table_id_after_truncate));
  ASSERT_TRUE(index_table_id == index_table_id_after_truncate);

  ObTruncateInfoArray truncate_info_array;
  ObTabletID tablet_id;
  ObLSID ls_id;
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(*conn, index_table_id, tablet_id, ls_id));
  ASSERT_EQ(OB_SUCCESS, wait_mds_flush(ls_id, tablet_id));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_tablet(ls_id, tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->read_truncate_info_array(
      allocator_, ObVersionRange(0, EXIST_READ_SNAPSHOT_VERSION), false/*for_access*/, truncate_info_array));
  ASSERT_EQ(1, truncate_info_array.count());
  ObTruncateInfo *truncate_info = truncate_info_array.at(0);
  if (OB_NOT_NULL(truncate_info)) {
    ASSERT_TRUE(truncate_info->truncate_part_.is_valid());
    ASSERT_EQ(ObTruncatePartition::LIST_PART, truncate_info->truncate_part_.part_type_);
    ASSERT_EQ(ObTruncatePartition::ALL, truncate_info->truncate_part_.part_op_);
    ASSERT_EQ(0, truncate_info->truncate_part_.list_row_values_.count());
  }
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
