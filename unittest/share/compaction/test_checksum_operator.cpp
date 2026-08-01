/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/ob_dml_sql_splicer.h"
#include <gtest/gtest.h>
#include <initializer_list>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "share/compaction/ob_table_ckm_items.h"

namespace oceanbase
{
namespace unittest
{

static int add_ckm_item(
    compaction::ObTableCkmItems &table_ckm,
    const uint64_t tenant_id,
    const int64_t tablet_id,
    const int64_t ls_id,
    const share::SCN &compaction_scn,
    const int64_t row_count,
    const std::initializer_list<int64_t> &checksums)
{
  int ret = OB_SUCCESS;
  share::ObTabletReplicaChecksumItem item;
  common::ObSEArray<int64_t, 4> checksum_array;
  item.tenant_id_ = tenant_id;
  item.tablet_id_ = common::ObTabletID(tablet_id);
  item.ls_id_ = share::ObLSID(ls_id);
  item.compaction_scn_ = compaction_scn;
  item.row_count_ = row_count;
  for (const int64_t checksum : checksums) {
    if (OB_FAIL(checksum_array.push_back(checksum))) {
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(item.column_meta_.init(checksum_array))) {
    } else if (OB_FAIL(table_ckm.tablet_pairs_.push_back(
        share::ObTabletLSPair(item.tablet_id_, item.ls_id_)))) {
    } else if (OB_FAIL(table_ckm.ckm_items_.push_back(item))) {
    }
  }
  return ret;
}

static int add_int_column(
    share::schema::ObTableSchema &table_schema,
    const uint64_t table_id,
    const uint64_t column_id,
    const int64_t rowkey_position)
{
  share::schema::ObColumnSchemaV2 column;
  column.set_table_id(table_id);
  column.set_column_id(column_id);
  column.set_data_type(common::ObIntType);
  column.set_rowkey_position(rowkey_position);
  return table_schema.add_column(column);
}

TEST(ObPTSqlSplicer, batch) {
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer splicer;
  ObSqlString sql;
  ObString result("INSERT INTO tname (gmt_modified, gmt_create, tenant_id, tablet_id, svr_ip, svr_port, ls_id, compaction_scn, row_count, data_checksum, column_checksums, b_column_checksums) VALUES "
                  "(now(6), now(6), 1004, 200001, '127.0.0.1', 1001, 1001, 1234567890, 100, 1234567, 'column_checksums-01', 'b_column_checksums-01'),"
                  "(now(6), now(6), 1004, 200001, '127.0.0.2', 1002, 1001, 1234567890, 100, 1234567, 'column_checksums-02', 'b_column_checksums-02'),"
                  "(now(6), now(6), 1004, 200001, '127.0.0.3', 1003, 1001, 1234567890, 100, 1234567, 'column_checksums-03', 'b_column_checksums-03') "
                  "ON DUPLICATE KEY UPDATE "
                  "gmt_modified=VALUES(gmt_modified),"
                  "gmt_create=VALUES(gmt_create),"
                  "tenant_id=VALUES(tenant_id),"
                  "tablet_id=VALUES(tablet_id),"
                  "svr_ip=VALUES(svr_ip),"
                  "svr_port=VALUES(svr_port),"
                  "ls_id=VALUES(ls_id),"
                  "compaction_scn=VALUES(compaction_scn),"
                  "row_count=VALUES(row_count),"
                  "data_checksum=VALUES(data_checksum),"
                  "column_checksums=VALUES(column_checksums),"
                  "b_column_checksums=VALUES(b_column_checksums)");

  ObAddr ips[3];
  ObString column_checksums[3] = {"column_checksums-01", "column_checksums-02", "column_checksums-03"};
  ObString b_column_checksums[3] = {"b_column_checksums-01", "b_column_checksums-02", "b_column_checksums-03"};
  ASSERT_TRUE(ips[0].set_ip_addr("127.0.0.1", 1001));
  ASSERT_TRUE(ips[1].set_ip_addr("127.0.0.2", 1002));
  ASSERT_TRUE(ips[2].set_ip_addr("127.0.0.3", 1003));
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";

  for (int64_t i = 0; i < 3; i++) {
    ASSERT_TRUE(ips[i].ip_to_string(ip, sizeof(ip)));
    ASSERT_EQ(OB_SUCCESS, splicer.add_gmt_modified());
    ASSERT_EQ(OB_SUCCESS, splicer.add_gmt_create());
    ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("tenant_id", 1004));
    ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("tablet_id", 200001));
    ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("svr_ip", ip));
    ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("svr_port", ips[i].get_port()));
    ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("ls_id", 1001));
    ASSERT_EQ(OB_SUCCESS, splicer.add_column("compaction_scn", 1234567890));
    ASSERT_EQ(OB_SUCCESS, splicer.add_column("row_count", 100));
    ASSERT_EQ(OB_SUCCESS, splicer.add_column("data_checksum", 1234567));
    ASSERT_EQ(OB_SUCCESS, splicer.add_column("column_checksums", column_checksums[i]));
    ASSERT_EQ(OB_SUCCESS, splicer.add_column("b_column_checksums", b_column_checksums[i]));
    ASSERT_EQ(OB_SUCCESS, splicer.finish_row());
  }
  ASSERT_EQ(OB_SUCCESS, splicer.splice_batch_insert_update_sql("tname", sql));
  LOG_INFO("finish splice batch insert update sql", K(ret), K(sql), K(result));
  ASSERT_EQ(0, result.compare(sql.string()));
}

TEST(ObTableCkmItems, reset_skip_verify_ckm)
{
  compaction::ObTableCkmItems table_ckm(OB_SYS_TENANT_ID);
  table_ckm.is_inited_ = true;
  table_ckm.should_skip_verify_ckm_ = true;
  table_ckm.clear();
  ASSERT_FALSE(table_ckm.should_skip_verify_ckm());
}

TEST(ObTableCkmItems, aggregate_legal_tail_columns)
{
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  share::SCN compaction_scn;
  compaction::ObTableCkmItems table_ckm(tenant_id);
  ASSERT_EQ(OB_SUCCESS, compaction_scn.convert_for_inner_table_field(100));
  ASSERT_EQ(OB_SUCCESS, table_ckm.ckm_items_.init(tenant_id, 2));
  ASSERT_EQ(OB_SUCCESS, add_ckm_item(
      table_ckm, tenant_id, 200001, 1001, compaction_scn, 3, {10, 20}));
  ASSERT_EQ(OB_SUCCESS, add_ckm_item(
      table_ckm, tenant_id, 200002, 1001, compaction_scn, 7, {30, 40, 0}));

  int64_t row_count = 0;
  ASSERT_EQ(OB_SUCCESS, table_ckm.build_column_ckm_sum_array(
      true /*is_data_table*/, compaction_scn, row_count));
  ASSERT_EQ(10, row_count);
  ASSERT_EQ(2, table_ckm.ckm_sum_array_.count());
  ASSERT_EQ(40, table_ckm.ckm_sum_array_.at(0));
  ASSERT_EQ(60, table_ckm.ckm_sum_array_.at(1));
}

TEST(ObTableCkmItems, reject_tail_columns_with_mismatched_scn)
{
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  share::SCN compaction_scn;
  share::SCN newer_scn;
  compaction::ObTableCkmItems table_ckm(tenant_id);
  ASSERT_EQ(OB_SUCCESS, compaction_scn.convert_for_inner_table_field(100));
  ASSERT_EQ(OB_SUCCESS, newer_scn.convert_for_inner_table_field(101));
  ASSERT_EQ(OB_SUCCESS, table_ckm.ckm_items_.init(tenant_id, 2));
  ASSERT_EQ(OB_SUCCESS, add_ckm_item(
      table_ckm, tenant_id, 200001, 1001, compaction_scn, 3, {10, 20}));
  ASSERT_EQ(OB_SUCCESS, add_ckm_item(
      table_ckm, tenant_id, 200002, 1001, newer_scn, 7, {30, 40, 0}));

  int64_t row_count = 0;
  ASSERT_EQ(OB_ITEM_NOT_MATCH, table_ckm.build_column_ckm_sum_array(
      true /*is_data_table*/, compaction_scn, row_count));
  ASSERT_TRUE(table_ckm.ckm_sum_array_.empty());
}

TEST(ObTableCkmItems, aggregate_tail_columns_with_unordered_items)
{
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  share::SCN compaction_scn;
  compaction::ObTableCkmItems table_ckm(tenant_id);
  ASSERT_EQ(OB_SUCCESS, compaction_scn.convert_for_inner_table_field(100));
  ASSERT_EQ(OB_SUCCESS, table_ckm.ckm_items_.init(tenant_id, 2));
  ASSERT_EQ(OB_SUCCESS, add_ckm_item(
      table_ckm, tenant_id, 200002, 1001, compaction_scn, 7, {30, 40, 0}));
  ASSERT_EQ(OB_SUCCESS, add_ckm_item(
      table_ckm, tenant_id, 200001, 1001, compaction_scn, 3, {10, 20}));

  int64_t row_count = 0;
  ASSERT_EQ(OB_SUCCESS, table_ckm.build_column_ckm_sum_array(
      true /*is_data_table*/, compaction_scn, row_count));
  ASSERT_EQ(10, row_count);
  ASSERT_EQ(2, table_ckm.ckm_sum_array_.count());
  ASSERT_EQ(40, table_ckm.ckm_sum_array_.at(0));
  ASSERT_EQ(60, table_ckm.ckm_sum_array_.at(1));
}

TEST(ObTableCkmItems, reject_short_index_checksum_array)
{
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const uint64_t data_table_id = 200001;
  const uint64_t index_table_id = 200002;
  share::schema::ObTableSchema data_schema;
  share::schema::ObTableSchema index_schema;
  compaction::ObTableCkmItems data_ckm(tenant_id);
  compaction::ObTableCkmItems index_ckm(tenant_id);
  common::ObSEArray<int64_t, 1> data_checksums;
  common::ObSEArray<int64_t, 1> index_checksums;
  share::ObColumnChecksumErrorInfo error_info;

  data_schema.set_tenant_id(tenant_id);
  data_schema.set_database_id(OB_SYS_DATABASE_ID);
  data_schema.set_table_id(data_table_id);
  data_schema.set_schema_version(1);
  ASSERT_EQ(OB_SUCCESS, data_schema.set_table_name(
      common::ObString::make_string("data_table")));
  data_schema.set_rowkey_column_num(1);
  ASSERT_EQ(OB_SUCCESS, add_int_column(
      data_schema, data_table_id, OB_APP_MIN_COLUMN_ID, 1));
  index_schema.set_tenant_id(tenant_id);
  index_schema.set_database_id(OB_SYS_DATABASE_ID);
  index_schema.set_table_id(index_table_id);
  index_schema.set_schema_version(1);
  ASSERT_EQ(OB_SUCCESS, index_schema.set_table_name(
      common::ObString::make_string("index_table")));
  index_schema.set_data_table_id(data_table_id);
  index_schema.set_rowkey_column_num(1);
  ASSERT_EQ(OB_SUCCESS, add_int_column(
      index_schema, index_table_id, OB_APP_MIN_COLUMN_ID, 1));
  ASSERT_EQ(OB_SUCCESS, data_ckm.sort_col_id_array_.build(tenant_id, data_schema));
  ASSERT_EQ(OB_SUCCESS, data_checksums.push_back(10));

  ASSERT_EQ(OB_ERR_UNEXPECTED, compaction::ObTableCkmItems::compare_ckm_by_column_ids(
      data_ckm,
      index_ckm,
      data_schema,
      index_schema,
      data_checksums,
      index_checksums,
      error_info));
}

TEST(ObArrayWithMap, copy_preserves_tablet_count_without_map)
{
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  share::ObReplicaCkmArray source;
  share::ObReplicaCkmArray copied;
  share::ObTabletReplicaChecksumItem item;
  common::ObSEArray<int64_t, 1> checksums;

  ASSERT_EQ(OB_SUCCESS, source.init(tenant_id, 3));
  ASSERT_EQ(OB_SUCCESS, item.set_tenant_id(tenant_id));
  item.ls_id_ = share::ObLSID(1001);
  item.tablet_id_ = common::ObTabletID(200001);
  ASSERT_TRUE(item.server_.set_ip_addr("127.0.0.1", 2882));
  ASSERT_EQ(OB_SUCCESS, checksums.push_back(10));
  ASSERT_EQ(OB_SUCCESS, item.column_meta_.init(checksums));
  item.data_checksum_type_ = share::ObDataChecksumType::DATA_CHECKSUM_NORMAL;
  ASSERT_TRUE(item.is_valid());
  ASSERT_EQ(OB_SUCCESS, source.push_back(item));
  ASSERT_EQ(OB_SUCCESS, source.push_back(item));
  item.tablet_id_ = common::ObTabletID(200002);
  ASSERT_EQ(OB_SUCCESS, source.push_back(item));
  ASSERT_EQ(2, source.get_tablet_cnt());

  ASSERT_EQ(OB_SUCCESS, copied.init(tenant_id, source));
  ASSERT_EQ(source.count(), copied.count());
  ASSERT_EQ(source.get_tablet_cnt(), copied.get_tablet_cnt());
}

TEST(ObTabletReplicaChecksumOperator, batch_get_sql_orders_tablets)
{
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  share::SCN compaction_scn;
  common::ObSEArray<share::ObTabletLSPair, 2> pairs;
  common::ObSqlString sql;

  ASSERT_EQ(OB_SUCCESS, compaction_scn.convert_for_inner_table_field(100));
  ASSERT_EQ(OB_SUCCESS, pairs.push_back(share::ObTabletLSPair(200002, 1002)));
  ASSERT_EQ(OB_SUCCESS, pairs.push_back(share::ObTabletLSPair(200001, 1001)));
  ASSERT_EQ(OB_SUCCESS,
      share::ObTabletReplicaChecksumOperator::construct_batch_get_sql_str_(
          tenant_id,
          compaction_scn,
          pairs,
          0,
          pairs.count(),
          sql,
          false /*include_larger_than*/,
          true /*with_compaction_scn*/));
  ASSERT_NE(nullptr, strstr(sql.ptr(), " ORDER BY tablet_id, ls_id"));
}

} // end unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
