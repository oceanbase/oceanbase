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
#define protected public
#define private public

// #include "mittest/env/ob_simple_server_helper.h"
// #include "env/ob_simple_cluster_test_base.h"
#include "storage/truncate_info/ob_truncate_info_array.h"
#include "storage/truncate_info/ob_truncate_partition_filter.h"
#include "share/schema/ob_list_row_values.h"
#include "unittest/storage/ob_truncate_info_helper.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::storage;

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 1;
  int time_sec_ = 0;
};

TestRunCtx RunCtx;

// class ObTruncatePartitionFitlerTest : public ObSimpleClusterTestBase
class ObTruncatePartitionFitlerTest : public ::testing::Test
{
public:
  ObTruncatePartitionFitlerTest()
    : /*ObSimpleClusterTestBase("test_truncate_partition_filter"),*/
      schema_rowkey_cnt_(0),
      cols_desc_()
  {
    // common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    // ObSqlString sql;
    // int64_t affected_rows = 0;
    // sql_proxy.write("drop database test", affected_rows);
    // sql_proxy.write("create database test", affected_rows);
    // sql_proxy.write("use test", affected_rows);
  }
private:
  void build_truncate_info(
      const ObTruncatePartition::TruncatePartType part_type,
      const int64_t row_scn,
      const ObRowkey &range_begin_rowkey,
      const ObRowkey &range_end_rowkey,
      const int64_t part_col_cnt,
      ObTruncateInfo &range_truncate_info);
  void build_truncate_info(
      const ObTruncatePartition::TruncatePartType part_type,
      const int64_t row_scn,
      const ObListRowValues &list_row_values,
      const int64_t part_col_cnt,
      ObTruncateInfo &range_truncate_info,
      const ObTruncatePartition::TruncatePartOp part_op = ObTruncatePartition::INCLUDE);
  void build_range_list_truncate_info(
     const int64_t row_scn,
      const ObRowkey &range_begin_rowkey,
      const ObRowkey &range_end_rowkey,
      const ObListRowValues &list_row_values,
      const int64_t part_col_cnt,
      ObTruncateInfo &range_truncate_info,
      const ObTruncatePartition::TruncatePartOp part_op = ObTruncatePartition::INCLUDE,
      const ObTruncatePartition::TruncatePartOp subpart_op = ObTruncatePartition::INCLUDE);
  void build_filter(
    const ObTruncateInfo &truncate_info,
    ObTruncatePartitionFilter &filter);
  void rescan_filter(
    const ObTruncateInfo &truncate_info,
    ObTruncatePartitionFilter &filter);
  void build_filter(
    const ObTruncateInfoArray &truncate_info_array,
    ObTruncatePartitionFilter &filter);
  void get_index_table_cols_desc();
  void get_index_table_cols_param(const bool nullable);
  static const int64_t trans_id = 1;
  int64_t schema_rowkey_cnt_;
  ObSEArray<ObColDesc, 8> cols_desc_;
  ObSEArray<ObColumnParam*, 8> cols_param_;
  int64_t col_idxs_[8];
  int64_t sub_col_idxs_[8];
  ObArenaAllocator allocator_;
};

void ObTruncatePartitionFitlerTest::build_truncate_info(
    const ObTruncatePartition::TruncatePartType part_type,
    const int64_t row_scn,
    const ObRowkey &range_begin_rowkey,
    const ObRowkey &range_end_rowkey,
    const int64_t part_col_cnt,
    ObTruncateInfo &range_truncate_info)
{
  TruncateInfoHelper::mock_truncate_info(allocator_, trans_id, row_scn, row_scn, range_truncate_info);
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, range_begin_rowkey, range_end_rowkey, range_truncate_info.truncate_part_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, part_col_cnt, col_idxs_, range_truncate_info.truncate_part_));
}

void ObTruncatePartitionFitlerTest::build_truncate_info(
    const ObTruncatePartition::TruncatePartType part_type,
    const int64_t row_scn,
    const ObListRowValues &list_row_values,
    const int64_t part_col_cnt,
    ObTruncateInfo &list_truncate_info,
    const ObTruncatePartition::TruncatePartOp part_op)
{
  TruncateInfoHelper::mock_truncate_info(allocator_, trans_id, row_scn, row_scn, list_truncate_info);
  list_truncate_info.truncate_part_.part_type_ = part_type;
  list_truncate_info.truncate_part_.part_op_ = part_op;
  ASSERT_EQ(OB_SUCCESS, list_truncate_info.truncate_part_.list_row_values_.init(allocator_, list_row_values.get_values()));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, part_col_cnt, col_idxs_, list_truncate_info.truncate_part_));
}

void ObTruncatePartitionFitlerTest::build_range_list_truncate_info(
    const int64_t row_scn,
    const ObRowkey &range_begin_rowkey,
    const ObRowkey &range_end_rowkey,
    const ObListRowValues &list_row_values,
    const int64_t part_col_cnt,
    ObTruncateInfo &truncate_info,
    const ObTruncatePartition::TruncatePartOp part_op,
    const ObTruncatePartition::TruncatePartOp subpart_op)
{
  TruncateInfoHelper::mock_truncate_info(allocator_, trans_id, row_scn, row_scn, truncate_info);
  truncate_info.is_sub_part_ = true;
  ObTruncatePartition &part = truncate_info.truncate_part_;
  ObTruncatePartition &subpart = truncate_info.truncate_subpart_;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, range_begin_rowkey, range_end_rowkey, part));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, part_col_cnt, col_idxs_, part));

  subpart.part_type_ = ObTruncatePartition::LIST_PART;
  subpart.part_op_ = subpart_op;
  ASSERT_EQ(OB_SUCCESS, subpart.list_row_values_.init(allocator_, list_row_values.get_values()));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, part_col_cnt, sub_col_idxs_, subpart));
}

void ObTruncatePartitionFitlerTest::get_index_table_cols_desc()
{
  cols_desc_.reuse();
  schema_rowkey_cnt_ = 2;
  share::schema::ObColDesc tmp_col_desc;
  ObObjMeta meta_type;
  meta_type.set_int();
  tmp_col_desc.col_type_ = meta_type;
  tmp_col_desc.col_order_ = ObOrderType::ASC;
  ASSERT_EQ(OB_SUCCESS, cols_desc_.push_back(tmp_col_desc));
  ASSERT_EQ(OB_SUCCESS, cols_desc_.push_back(tmp_col_desc));
  ASSERT_EQ(OB_SUCCESS, cols_desc_.push_back(tmp_col_desc));
  ASSERT_EQ(OB_SUCCESS, cols_desc_.push_back(tmp_col_desc));
}

void ObTruncatePartitionFitlerTest::get_index_table_cols_param(const bool nullable)
{
  cols_param_.reuse();
  ObColumnParam *column = nullptr;
  for (int64_t i = 0; i < schema_rowkey_cnt_ + 2 + 2; ++i) {
    column = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObTableParam::alloc_column(allocator_, column));
    column->set_nullable_for_write(nullable);
    if (i < schema_rowkey_cnt_) {
      column->set_column_id(i + 16);
    } else if (i < schema_rowkey_cnt_ + 2) {
      column->set_column_id(OB_MIN_SHADOW_COLUMN_ID + i - schema_rowkey_cnt_);
    } else {
      column->set_column_id(OB_HIDDEN_TRANS_VERSION_COLUMN_ID);
    }
    ASSERT_EQ(OB_SUCCESS, cols_param_.push_back(column));
  }
}

void ObTruncatePartitionFitlerTest::build_filter(
    const ObTruncateInfo &truncate_info,
    ObTruncatePartitionFilter &filter)
{
  ObTruncateInfoArray truncate_info_array;
  truncate_info_array.init_for_first_creation(allocator_);
  ASSERT_EQ(OB_SUCCESS, truncate_info_array.append_with_deep_copy(truncate_info));
  ASSERT_EQ(OB_SUCCESS, filter.init_truncate_filter(schema_rowkey_cnt_, cols_desc_, &cols_param_, truncate_info_array));
  filter.filter_type_ = ObTruncateFilterType::NORMAL_FILTER;
}

void ObTruncatePartitionFitlerTest::rescan_filter(
    const ObTruncateInfo &truncate_info,
    ObTruncatePartitionFilter &filter)
{
  ObTruncateInfoArray truncate_info_array;
  truncate_info_array.init_for_first_creation(allocator_);
  ASSERT_EQ(OB_SUCCESS, truncate_info_array.append_with_deep_copy(truncate_info));
  ASSERT_EQ(OB_SUCCESS, filter.truncate_filter_executor_->switch_info(filter.filter_factory_, schema_rowkey_cnt_, cols_desc_, truncate_info_array));
}

void ObTruncatePartitionFitlerTest::build_filter(
    const ObTruncateInfoArray &truncate_info_array,
    ObTruncatePartitionFilter &filter)
{
  ASSERT_EQ(OB_SUCCESS, filter.init_truncate_filter(schema_rowkey_cnt_, cols_desc_, &cols_param_, truncate_info_array));
  filter.filter_type_ = ObTruncateFilterType::NORMAL_FILTER;
}

TEST_F(ObTruncatePartitionFitlerTest, part_filter)
{
  get_index_table_cols_desc();
  // share::ObTenantSwitchGuard tguard;
  // ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  // common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  // sqlclient::ObISQLConnection *conn = NULL;
  // ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));

  // ---------------------- range partition ----------------------
  // ObSqlString sql;
  // sql.assign_fmt("create table t_range1(c1 int, c2 int, c3 int, primary key(c1,c2)) \
  //     partition by range(c2) \
  //     (partition `p0` values less than (10), \
  //     partition `p1` values less than (100), \
  //     partition `p2` values less than (MAXVALUE))");
  // int64_t affected_rows = 0;
  // ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  // ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create index t_range1_idx on t_range1(c2) global", affected_rows));

  ObDatumRow index_part_row;
  ASSERT_EQ(OB_SUCCESS, index_part_row.init(allocator_, 4));
  ObStorageDatum &scn_datum = index_part_row.storage_datums_[2];
  ObStorageDatum &range_part_datum = index_part_row.storage_datums_[0];
  int64_t row_scn = 1000;
  ObObj range_begin_obj, range_end_obj;
  ObRowkey range_begin_rowkey(&range_begin_obj, 1);
  ObRowkey range_end_rowkey(&range_end_obj, 1);
  bool filtered = false;
  // ---- first partition (min : 10)
  range_begin_obj.set_min_value();
  range_end_obj.set_int(10);

  ObTruncateInfo range_truncate_info1;
  col_idxs_[0] = 0;
  build_truncate_info(ObTruncatePartition::RANGE_PART, row_scn, range_begin_rowkey, range_end_rowkey, 1, range_truncate_info1);

  ObTruncatePartitionFilter truncate_range_part_filter1;
  get_index_table_cols_param(false);
  build_filter(range_truncate_info1, truncate_range_part_filter1);

#define CHECK_RANGE_ROW(scn_val, part_val, truncate_filter, expected) \
  filtered = false; \
  scn_datum.set_int(scn_val); \
  range_part_datum.set_int(part_val); \
  ASSERT_EQ(OB_SUCCESS, truncate_filter.filter(index_part_row, filtered)); \
  ASSERT_##expected(filtered);

  CHECK_RANGE_ROW(800, 5, truncate_range_part_filter1, TRUE);
  CHECK_RANGE_ROW(1000, 5, truncate_range_part_filter1, TRUE);
  CHECK_RANGE_ROW(1100, 5, truncate_range_part_filter1, FALSE);
  CHECK_RANGE_ROW(900, 10, truncate_range_part_filter1, FALSE);
  CHECK_RANGE_ROW(900, 15, truncate_range_part_filter1, FALSE);

  // ---- middle partition [10 : 100)
  range_begin_obj.set_int(10);
  range_end_obj.set_int(100);

  ObTruncateInfo range_truncate_info2;
  build_truncate_info(ObTruncatePartition::RANGE_PART, row_scn, range_begin_rowkey, range_end_rowkey, 1, range_truncate_info2);

  ObTruncatePartitionFilter truncate_range_part_filter2;
  get_index_table_cols_param(true);
  build_filter(range_truncate_info2, truncate_range_part_filter2);

  CHECK_RANGE_ROW(800, 5, truncate_range_part_filter2, FALSE);
  CHECK_RANGE_ROW(800, 10, truncate_range_part_filter2, TRUE);
  CHECK_RANGE_ROW(800, 50, truncate_range_part_filter2, TRUE);
  CHECK_RANGE_ROW(800, 100, truncate_range_part_filter2, FALSE);

  // ---- last partition [100 : max)
  range_begin_obj.set_int(100);
  range_end_obj.set_max_value();

  ObTruncateInfo range_truncate_info3;
  build_truncate_info(ObTruncatePartition::RANGE_PART, row_scn, range_begin_rowkey, range_end_rowkey, 1, range_truncate_info3);
  ObTruncatePartitionFilter truncate_range_part_filter3;
  get_index_table_cols_param(false);
  build_filter(range_truncate_info3, truncate_range_part_filter3);

  CHECK_RANGE_ROW(800, 5, truncate_range_part_filter3, FALSE);
  CHECK_RANGE_ROW(800, 10, truncate_range_part_filter3, FALSE);
  CHECK_RANGE_ROW(800, 100, truncate_range_part_filter3, TRUE);
  CHECK_RANGE_ROW(800, 200, truncate_range_part_filter3, TRUE);
  CHECK_RANGE_ROW(1001, 200, truncate_range_part_filter3, FALSE);

  // ---- whole partition (min : max)
  range_begin_obj.set_min_value();
  range_end_obj.set_max_value();

  ObTruncateInfo range_truncate_info4;
  build_truncate_info(ObTruncatePartition::RANGE_PART, row_scn, range_begin_rowkey, range_end_rowkey, 1, range_truncate_info4);
  ObTruncatePartitionFilter truncate_range_part_filter4;
  get_index_table_cols_param(true);
  build_filter(range_truncate_info4, truncate_range_part_filter4);

  CHECK_RANGE_ROW(800, 0, truncate_range_part_filter4, TRUE);
  CHECK_RANGE_ROW(800, 10, truncate_range_part_filter4, TRUE);
  CHECK_RANGE_ROW(800, 100, truncate_range_part_filter4, TRUE);
  CHECK_RANGE_ROW(800, 200, truncate_range_part_filter4, TRUE);
  CHECK_RANGE_ROW(1001, 0, truncate_range_part_filter4, FALSE);
  CHECK_RANGE_ROW(1001, 200, truncate_range_part_filter4, FALSE);

  // truncate_range_part_filter1 rescan middle partition [10 : 100)
  ObTruncateInfo rescan_range_truncate_info1;
  row_scn = 2000;
  range_begin_obj.set_int(10);
  range_end_obj.set_int(100);
  build_truncate_info(ObTruncatePartition::RANGE_PART, row_scn, range_begin_rowkey, range_end_rowkey, 1, rescan_range_truncate_info1);
  rescan_filter(rescan_range_truncate_info1, truncate_range_part_filter1);
  CHECK_RANGE_ROW(1800, 5, truncate_range_part_filter1, FALSE);
  CHECK_RANGE_ROW(2000, 9, truncate_range_part_filter1, FALSE);
  CHECK_RANGE_ROW(2000, 10, truncate_range_part_filter1, TRUE);
  CHECK_RANGE_ROW(2000, 11, truncate_range_part_filter1, TRUE);
  CHECK_RANGE_ROW(1900, 99, truncate_range_part_filter1, TRUE);
  CHECK_RANGE_ROW(2000, 99, truncate_range_part_filter1, TRUE);
  CHECK_RANGE_ROW(2001, 99, truncate_range_part_filter1, FALSE);
  CHECK_RANGE_ROW(2001, 100, truncate_range_part_filter1, FALSE);
  CHECK_RANGE_ROW(1900, 100, truncate_range_part_filter1, FALSE);
  CHECK_RANGE_ROW(1900, 101, truncate_range_part_filter1, FALSE);

  // ---------------------- range column partition ----------------------
  // sql.assign_fmt("create table t_range_columns(c1 int, c2 int, c3 int, primary key(c1,c2)) \
  //     partition by range columns(c1,c2) \
  //     (partition p0 values less than (10,20), \
  //     partition p1 values less than (100,200), \
  //     partition p2 values less than (MAXVALUE,MAXVALUE))");
  // affected_rows = 0;
  // ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  // ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create index t_range_columns_idx on t_range_columns(c2) global", affected_rows));
  // get_index_table_cols_desc(conn, "t_range_columns");

  row_scn = 1000;
  // ---- first partition (min : 10,20)
  ObObj range_columns_begin_objs[2], range_columns_end_objs[2];
  range_columns_begin_objs[0].set_min_value();
  range_columns_begin_objs[1].set_min_value();
  range_columns_end_objs[0].set_int(10);
  range_columns_end_objs[1].set_int(20);

  ObRowkey range_columns_min_begin_rowkey(range_columns_begin_objs, 1);
  ObRowkey range_columns_begin_rowkey(range_columns_begin_objs, 2);
  ObRowkey range_columns_end_rowkey(range_columns_end_objs, 2);
  ObRowkey range_columns_max_end_rowkey(range_columns_end_objs, 1);

  ObTruncateInfo range_columns_truncate_info1;
  col_idxs_[0] = 1;
  col_idxs_[1] = 0;
  build_truncate_info(ObTruncatePartition::RANGE_COLUMNS_PART, row_scn, range_columns_min_begin_rowkey, range_columns_end_rowkey, 2, range_columns_truncate_info1);
  
  ObTruncatePartitionFilter truncate_range_columns_part_filter1;
  get_index_table_cols_param(true);
  build_filter(range_columns_truncate_info1, truncate_range_columns_part_filter1);

  index_part_row.reuse();
  ObStorageDatum &range_columns_part_datum1 = index_part_row.storage_datums_[1];
  ObStorageDatum &range_columns_part_datum2 = index_part_row.storage_datums_[0];

#define CHECK_RANGE_COLUMNS_ROW(scn_val, part_val1, part_val2, truncate_filter, expected) \
  filtered = false; \
  scn_datum.set_int(scn_val); \
  range_columns_part_datum1.set_int(part_val1); \
  range_columns_part_datum2.set_int(part_val2); \
  ASSERT_EQ(OB_SUCCESS, truncate_filter.filter(index_part_row, filtered)); \
  ASSERT_##expected(filtered);

  CHECK_RANGE_COLUMNS_ROW(800, 5, 5, truncate_range_columns_part_filter1, TRUE);
  CHECK_RANGE_COLUMNS_ROW(1000, 5, 5, truncate_range_columns_part_filter1, TRUE);
  CHECK_RANGE_COLUMNS_ROW(1100, 5, 5, truncate_range_columns_part_filter1, FALSE);
  CHECK_RANGE_COLUMNS_ROW(900, 10, 20, truncate_range_columns_part_filter1, FALSE);
  CHECK_RANGE_COLUMNS_ROW(900, 10, 5, truncate_range_columns_part_filter1, TRUE);
  CHECK_RANGE_COLUMNS_ROW(900, 5, 30, truncate_range_columns_part_filter1, TRUE);
  CHECK_RANGE_COLUMNS_ROW(900, 25, 5, truncate_range_columns_part_filter1, FALSE);
  CHECK_RANGE_COLUMNS_ROW(900, 15, 25, truncate_range_columns_part_filter1, FALSE);

  // ---- middle partition [10,20 : 100,200)
  range_columns_begin_objs[0].set_int(10);
  range_columns_begin_objs[1].set_int(20);
  range_columns_end_objs[0].set_int(100);
  range_columns_end_objs[1].set_int(200);

  ObTruncateInfo range_columns_truncate_info2;
  build_truncate_info(ObTruncatePartition::RANGE_COLUMNS_PART, row_scn, range_columns_begin_rowkey, range_columns_end_rowkey, 2, range_columns_truncate_info2);
  
  ObTruncatePartitionFilter truncate_range_columns_part_filter2;
  get_index_table_cols_param(false);
  build_filter(range_columns_truncate_info2, truncate_range_columns_part_filter2);

  CHECK_RANGE_COLUMNS_ROW(800, 5, 15, truncate_range_columns_part_filter2, FALSE);
  CHECK_RANGE_COLUMNS_ROW(800, 10, 10, truncate_range_columns_part_filter2, FALSE);
  CHECK_RANGE_COLUMNS_ROW(800, 10, 20, truncate_range_columns_part_filter2, TRUE);
  CHECK_RANGE_COLUMNS_ROW(800, 10, 30, truncate_range_columns_part_filter2, TRUE);
  CHECK_RANGE_COLUMNS_ROW(800, 50, 30, truncate_range_columns_part_filter2, TRUE);
  CHECK_RANGE_COLUMNS_ROW(800, 100, 199, truncate_range_columns_part_filter2, TRUE);
  CHECK_RANGE_COLUMNS_ROW(800, 99, 299, truncate_range_columns_part_filter2, TRUE);
  CHECK_RANGE_COLUMNS_ROW(800, 100, 200, truncate_range_columns_part_filter2, FALSE);

  // ---- last partition [100,200 : max)
  range_columns_begin_objs[0].set_int(100);
  range_columns_begin_objs[1].set_int(200);
  range_columns_end_objs[0].set_max_value();
  range_columns_end_objs[1].set_max_value();

  ObTruncateInfo range_columns_truncate_info3;
  build_truncate_info(ObTruncatePartition::RANGE_COLUMNS_PART, row_scn, range_columns_begin_rowkey, range_columns_max_end_rowkey, 2, range_columns_truncate_info3);
  
  ObTruncatePartitionFilter truncate_range_columns_part_filter3;
  get_index_table_cols_param(true);
  build_filter(range_columns_truncate_info3, truncate_range_columns_part_filter3);

  CHECK_RANGE_COLUMNS_ROW(800, 5, 5, truncate_range_columns_part_filter3, FALSE);
  CHECK_RANGE_COLUMNS_ROW(800, 100, 199, truncate_range_columns_part_filter3, FALSE);
  CHECK_RANGE_COLUMNS_ROW(800, 99, 200, truncate_range_columns_part_filter3, FALSE);
  CHECK_RANGE_COLUMNS_ROW(800, 100, 200, truncate_range_columns_part_filter3, TRUE);
  CHECK_RANGE_COLUMNS_ROW(800, 101, 201, truncate_range_columns_part_filter3, TRUE);
  CHECK_RANGE_COLUMNS_ROW(1001, 101, 201, truncate_range_columns_part_filter3, FALSE);

  // ---- whole partition (min : max)
  range_columns_begin_objs[0].set_min_value();
  range_columns_begin_objs[1].set_min_value();
  range_columns_end_objs[0].set_max_value();
  range_columns_end_objs[1].set_max_value();

  ObTruncateInfo range_columns_truncate_info4;
  build_truncate_info(ObTruncatePartition::RANGE_COLUMNS_PART, row_scn, range_columns_begin_rowkey, range_columns_max_end_rowkey, 2, range_columns_truncate_info4);
  
  ObTruncatePartitionFilter truncate_range_columns_part_filter4;
  build_filter(range_columns_truncate_info4, truncate_range_columns_part_filter4);

  CHECK_RANGE_COLUMNS_ROW(800, 0, 0, truncate_range_columns_part_filter4, TRUE);
  CHECK_RANGE_COLUMNS_ROW(800, 100, 199, truncate_range_columns_part_filter4, TRUE);
  CHECK_RANGE_COLUMNS_ROW(800, 99, 200, truncate_range_columns_part_filter4, TRUE);
  CHECK_RANGE_COLUMNS_ROW(800, 100, 200, truncate_range_columns_part_filter4, TRUE);
  CHECK_RANGE_COLUMNS_ROW(800, 101, 201, truncate_range_columns_part_filter4, TRUE);
  CHECK_RANGE_COLUMNS_ROW(1001, 0, 5, truncate_range_columns_part_filter4, FALSE);
  CHECK_RANGE_COLUMNS_ROW(1001, 101, 201, truncate_range_columns_part_filter4, FALSE);

  // truncate_range_columns_part_filter1 rescan middle partition [10,20 : 100,200)
  ObTruncateInfo rescan_range_columns_truncate_info1;
  row_scn = 2000;
  range_columns_begin_objs[0].set_int(10);
  range_columns_begin_objs[1].set_int(20);
  range_columns_end_objs[0].set_int(100);
  range_columns_end_objs[1].set_int(200);
  build_truncate_info(ObTruncatePartition::RANGE_COLUMNS_PART, row_scn, range_columns_begin_rowkey, range_columns_end_rowkey, 2, rescan_range_columns_truncate_info1);
  rescan_filter(rescan_range_columns_truncate_info1, truncate_range_columns_part_filter1);
  CHECK_RANGE_COLUMNS_ROW(800, 5, 15, truncate_range_columns_part_filter1, FALSE);
  CHECK_RANGE_COLUMNS_ROW(800, 10, 10, truncate_range_columns_part_filter1, FALSE);
  CHECK_RANGE_COLUMNS_ROW(800, 10, 20, truncate_range_columns_part_filter1, TRUE);
  CHECK_RANGE_COLUMNS_ROW(800, 10, 30, truncate_range_columns_part_filter1, TRUE);
  CHECK_RANGE_COLUMNS_ROW(800, 50, 30, truncate_range_columns_part_filter1, TRUE);
  CHECK_RANGE_COLUMNS_ROW(800, 100, 199, truncate_range_columns_part_filter1, TRUE);
  CHECK_RANGE_COLUMNS_ROW(800, 99, 299, truncate_range_columns_part_filter1, TRUE);
  CHECK_RANGE_COLUMNS_ROW(800, 100, 200, truncate_range_columns_part_filter1, FALSE);
  CHECK_RANGE_COLUMNS_ROW(2000, 10, 20, truncate_range_columns_part_filter1, TRUE);
  CHECK_RANGE_COLUMNS_ROW(2001, 10, 20, truncate_range_columns_part_filter1, FALSE);

  // ---------------------- list partition --------------------------
  // sql.assign_fmt("create table t_list(c1 int, c2 int, c3 int, primary key(c1,c2)) \
  //     partition by list(c2) \
  //     (partition `p0` values in (1), \
  //     partition `p1` values in (2,3), \
  //     partition `p2` values in (DEFAULT))");
  // affected_rows = 0;
  // ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  // ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create index t_list_idx on t_list(c2) global", affected_rows));
  // get_index_table_cols_desc(conn, "t_list");

  row_scn = 1000;
  
  ObObj list_obj1;
  ObNewRow list_row1;
  list_row1.assign(&list_obj1, 1);

  ObListRowValues list_row_values;
  ASSERT_EQ(OB_SUCCESS, list_row_values.push_back(list_row1));

  // ---- first partition ((1))
  list_obj1.set_int(1);

  ObTruncateInfo list_truncate_info1;
  col_idxs_[0] = 0;
  build_truncate_info(ObTruncatePartition::LIST_PART, row_scn, list_row_values, 1, list_truncate_info1);
  
  ObTruncatePartitionFilter truncate_list_part_filter1;
  get_index_table_cols_param(false);
  build_filter(list_truncate_info1, truncate_list_part_filter1);
  ObStorageDatum &list_part_datum = index_part_row.storage_datums_[0];
  index_part_row.reuse();

#define CHECK_LIST_ROW(scn_val, part_val, truncate_filter, expected) \
  filtered = false; \
  scn_datum.set_int(scn_val); \
  list_part_datum.set_int(part_val); \
  ASSERT_EQ(OB_SUCCESS, truncate_filter.filter(index_part_row, filtered)); \
  ASSERT_##expected(filtered);

  CHECK_LIST_ROW(800, 1, truncate_list_part_filter1, TRUE);
  CHECK_LIST_ROW(1000, 1, truncate_list_part_filter1, TRUE);
  CHECK_LIST_ROW(1100, 1, truncate_list_part_filter1, FALSE);
  CHECK_LIST_ROW(900, 0, truncate_list_part_filter1, FALSE);
  CHECK_LIST_ROW(900, 2, truncate_list_part_filter1, FALSE);

  // ---- middle partition ((2),(3))  
  ObObj list_obj2;
  ObNewRow list_row2;
  list_row2.assign(&list_obj2, 1);
  
  ObListRowValues list_row_values2;
  ASSERT_EQ(OB_SUCCESS, list_row_values2.push_back(list_row1));
  ASSERT_EQ(OB_SUCCESS, list_row_values2.push_back(list_row2));

  list_obj1.set_int(2);
  list_obj2.set_int(3);

  ObTruncateInfo list_truncate_info2;
  build_truncate_info(ObTruncatePartition::LIST_PART, row_scn, list_row_values2, 1, list_truncate_info2);
  
  ObTruncatePartitionFilter truncate_list_part_filter2;
  get_index_table_cols_param(true);
  build_filter(list_truncate_info2, truncate_list_part_filter2);

  CHECK_LIST_ROW(800, 1, truncate_list_part_filter2, FALSE);
  CHECK_LIST_ROW(800, 2, truncate_list_part_filter2, TRUE);
  CHECK_LIST_ROW(800, 3, truncate_list_part_filter2, TRUE);
  CHECK_LIST_ROW(800, 4, truncate_list_part_filter2, FALSE);

  // ---- last partition (default) --> ((1),(2),(3))
  ObObj list_obj3;
  ObNewRow list_row3;
  list_row3.assign(&list_obj3, 1);

  ObListRowValues list_row_values3;
  ASSERT_EQ(OB_SUCCESS, list_row_values3.push_back(list_row1));
  ASSERT_EQ(OB_SUCCESS, list_row_values3.push_back(list_row2));
  ASSERT_EQ(OB_SUCCESS, list_row_values3.push_back(list_row3));

  list_obj1.set_int(1);
  list_obj2.set_int(2);
  list_obj3.set_int(3);

  ObTruncateInfo list_truncate_info3;
  build_truncate_info(ObTruncatePartition::LIST_PART, row_scn, list_row_values3, 1, list_truncate_info3, ObTruncatePartition::EXCEPT);
  
  ObTruncatePartitionFilter truncate_list_part_filter3;
  get_index_table_cols_param(false);
  build_filter(list_truncate_info3, truncate_list_part_filter3);

  CHECK_LIST_ROW(800, 1, truncate_list_part_filter3, FALSE);
  CHECK_LIST_ROW(1000, 1, truncate_list_part_filter3, FALSE);
  CHECK_LIST_ROW(1001, 1, truncate_list_part_filter3, FALSE);
  CHECK_LIST_ROW(800, 2, truncate_list_part_filter3, FALSE);
  CHECK_LIST_ROW(1000, 2, truncate_list_part_filter3, FALSE);
  CHECK_LIST_ROW(1001, 2, truncate_list_part_filter3, FALSE);
  CHECK_LIST_ROW(800, 3, truncate_list_part_filter3, FALSE);
  CHECK_LIST_ROW(1000, 3, truncate_list_part_filter3, FALSE);
  CHECK_LIST_ROW(1001, 3, truncate_list_part_filter3, FALSE);
  CHECK_LIST_ROW(800, 4, truncate_list_part_filter3, TRUE);
  CHECK_LIST_ROW(700, 5, truncate_list_part_filter3, TRUE);
  CHECK_LIST_ROW(700, 9, truncate_list_part_filter3, TRUE);
  CHECK_LIST_ROW(1000, 9, truncate_list_part_filter3, TRUE);
  CHECK_LIST_ROW(1001, 9, truncate_list_part_filter3, FALSE);

  // truncate_list_part_filter1 rescan middle partition middle partition ((2),(3))
  ObTruncateInfo rescan_list_truncate_info1;
  row_scn = 2000;
  list_obj1.set_int(2);
  list_obj2.set_int(3);
  build_truncate_info(ObTruncatePartition::LIST_PART, row_scn, list_row_values2, 1, rescan_list_truncate_info1);
  rescan_filter(rescan_list_truncate_info1, truncate_list_part_filter1);
  CHECK_LIST_ROW(800, 1, truncate_list_part_filter1, FALSE);
  CHECK_LIST_ROW(800, 2, truncate_list_part_filter1, TRUE);
  CHECK_LIST_ROW(800, 3, truncate_list_part_filter1, TRUE);
  CHECK_LIST_ROW(800, 4, truncate_list_part_filter1, FALSE);
  CHECK_LIST_ROW(2000, 2, truncate_list_part_filter1, TRUE);
  CHECK_LIST_ROW(2000, 3, truncate_list_part_filter1, TRUE);
  CHECK_LIST_ROW(2001, 2, truncate_list_part_filter1, FALSE);
  CHECK_LIST_ROW(2001, 3, truncate_list_part_filter1, FALSE);

  // ---- whole single partition (default)
  row_scn = 1000;
  ObListRowValues list_row_values4;
  ObTruncateInfo list_truncate_info4;
  build_truncate_info(ObTruncatePartition::LIST_PART, row_scn, list_row_values4, 1, list_truncate_info4, ObTruncatePartition::ALL);
  
  ObTruncatePartitionFilter truncate_list_part_filter4;
  get_index_table_cols_param(true);
  build_filter(list_truncate_info4, truncate_list_part_filter4);

  CHECK_LIST_ROW(800, 0, truncate_list_part_filter4, TRUE);
  CHECK_LIST_ROW(800, 1, truncate_list_part_filter4, TRUE);
  CHECK_LIST_ROW(800, 2, truncate_list_part_filter4, TRUE);
  CHECK_LIST_ROW(800, 3, truncate_list_part_filter4, TRUE);
  CHECK_LIST_ROW(1001, 0, truncate_list_part_filter4, FALSE);
  CHECK_LIST_ROW(1001, 1, truncate_list_part_filter4, FALSE);
  CHECK_LIST_ROW(1001, 2, truncate_list_part_filter4, FALSE);
  CHECK_LIST_ROW(1001, 3, truncate_list_part_filter4, FALSE);

  // ---------------------- list columns partition --------------------------
  // sql.assign_fmt("create table t_list_columns(c1 int, c2 int, c3 int, primary key(c1,c2)) \
  //     partition by list columns(c1,c2) \
  //     (partition p0 values in ((1,1),(2,2)), \
  //     partition p1 values in ((3,3)), \
  //     partition `p2` values in (DEFAULT))");
  // affected_rows = 0;
  // ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  // ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create index t_list_columns_idx on t_list_columns(c2) global", affected_rows));
  // get_index_table_cols_desc(conn, "t_list_columns");

  row_scn = 1000;
  
  ObObj list_columns_objs1[2];
  ObObj list_columns_objs2[2];
  ObNewRow list_columns_row1;
  ObNewRow list_columns_row2;
  list_columns_row1.assign(list_columns_objs1, 2);
  list_columns_row2.assign(list_columns_objs2, 2);
  ObListRowValues list_columns_row_values1;

  // ---- first partition ((1,1),(2,2))
  ASSERT_EQ(OB_SUCCESS, list_columns_row_values1.push_back(list_columns_row1));
  ASSERT_EQ(OB_SUCCESS, list_columns_row_values1.push_back(list_columns_row2));
  list_columns_objs1[0].set_int(1); list_columns_objs1[1].set_int(1);
  list_columns_objs2[0].set_int(2); list_columns_objs2[1].set_int(2);

  ObTruncateInfo list_columns_truncate_info1;
  col_idxs_[0] = 1;
  col_idxs_[1] = 0;
  build_truncate_info(ObTruncatePartition::LIST_COLUMNS_PART, row_scn, list_columns_row_values1, 2, list_columns_truncate_info1);
  
  ObTruncatePartitionFilter truncate_list_columns_part_filter1;
  get_index_table_cols_param(true);
  build_filter(list_columns_truncate_info1, truncate_list_columns_part_filter1);

  ObStorageDatum &list_columns_part_datum1 = index_part_row.storage_datums_[1];
  ObStorageDatum &list_columns_part_datum2 = index_part_row.storage_datums_[0];
  index_part_row.reuse();

#define CHECK_LIST_COLUMNS_ROW(scn_val, part_val1, part_val2, truncate_filter, expected) \
  filtered = false; \
  scn_datum.set_int(scn_val); \
  list_columns_part_datum1.set_int(part_val1); \
  list_columns_part_datum2.set_int(part_val2); \
  ASSERT_EQ(OB_SUCCESS, truncate_filter.filter(index_part_row, filtered)); \
  ASSERT_##expected(filtered);

  CHECK_LIST_COLUMNS_ROW(800, 1, 1, truncate_list_columns_part_filter1, TRUE);
  CHECK_LIST_COLUMNS_ROW(1000, 1, 1, truncate_list_columns_part_filter1, TRUE);
  CHECK_LIST_COLUMNS_ROW(1100, 1, 1, truncate_list_columns_part_filter1, FALSE);
  CHECK_LIST_COLUMNS_ROW(900, 2, 2, truncate_list_columns_part_filter1, TRUE);
  CHECK_LIST_COLUMNS_ROW(900, 1, 2, truncate_list_columns_part_filter1, FALSE);
  CHECK_LIST_COLUMNS_ROW(900, 2, 1, truncate_list_columns_part_filter1, FALSE);
  CHECK_LIST_COLUMNS_ROW(900, 3, 3, truncate_list_columns_part_filter1, FALSE);

  // ---- middle partition ((3,3))
  ObListRowValues list_columns_row_values2;
  ASSERT_EQ(OB_SUCCESS, list_columns_row_values2.push_back(list_columns_row1));

  list_columns_objs1[0].set_int(3); list_columns_objs1[1].set_int(3);

  ObTruncateInfo list_columns_truncate_info2;
  build_truncate_info(ObTruncatePartition::LIST_COLUMNS_PART, row_scn, list_columns_row_values2, 2, list_columns_truncate_info2);
  
  ObTruncatePartitionFilter truncate_list_columns_part_filter2;
  get_index_table_cols_param(false);
  build_filter(list_columns_truncate_info2, truncate_list_columns_part_filter2);

  CHECK_LIST_COLUMNS_ROW(800, 1, 1, truncate_list_columns_part_filter2, FALSE);
  CHECK_LIST_COLUMNS_ROW(800, 2, 2, truncate_list_columns_part_filter2, FALSE);
  CHECK_LIST_COLUMNS_ROW(800, 3, 1, truncate_list_columns_part_filter2, FALSE);
  CHECK_LIST_COLUMNS_ROW(800, 1, 3, truncate_list_columns_part_filter2, FALSE);
  CHECK_LIST_COLUMNS_ROW(800, 3, 3, truncate_list_columns_part_filter2, TRUE);
  CHECK_LIST_COLUMNS_ROW(1001, 3, 3, truncate_list_columns_part_filter2, FALSE);

  // ---- last partition (default) --> ((1,1),(2,2),(3,3))
  ObObj list_columns_objs3[2];
  ObNewRow list_columns_row3;
  list_columns_row3.assign(list_columns_objs3, 2);

  ObListRowValues list_columns_row_values3;
  ASSERT_EQ(OB_SUCCESS, list_columns_row_values3.push_back(list_columns_row1));
  ASSERT_EQ(OB_SUCCESS, list_columns_row_values3.push_back(list_columns_row2));
  ASSERT_EQ(OB_SUCCESS, list_columns_row_values3.push_back(list_columns_row3));

  list_columns_objs1[0].set_int(1); list_columns_objs1[1].set_int(1);
  list_columns_objs2[0].set_int(2); list_columns_objs2[1].set_int(2);
  list_columns_objs3[0].set_int(3); list_columns_objs3[1].set_int(3);

  ObTruncateInfo list_columns_truncate_info3;
  build_truncate_info(ObTruncatePartition::LIST_COLUMNS_PART, row_scn, list_columns_row_values3, 2, list_columns_truncate_info3);
  
  ObTruncatePartitionFilter truncate_list_columns_part_filter3;
  get_index_table_cols_param(true);
  build_filter(list_columns_truncate_info3, truncate_list_columns_part_filter3);

  CHECK_LIST_COLUMNS_ROW(800, 1, 1, truncate_list_columns_part_filter3, TRUE);
  CHECK_LIST_COLUMNS_ROW(1000, 1, 1, truncate_list_columns_part_filter3, TRUE);
  CHECK_LIST_COLUMNS_ROW(1001, 1, 1, truncate_list_columns_part_filter3, FALSE);
  CHECK_LIST_COLUMNS_ROW(800, 2, 2, truncate_list_columns_part_filter3, TRUE);
  CHECK_LIST_COLUMNS_ROW(1001, 2, 2, truncate_list_columns_part_filter3, FALSE);
  CHECK_LIST_COLUMNS_ROW(800, 3, 3, truncate_list_columns_part_filter3, TRUE);
  CHECK_LIST_COLUMNS_ROW(1000, 3, 3, truncate_list_columns_part_filter3, TRUE);
  CHECK_LIST_COLUMNS_ROW(1001, 3, 3, truncate_list_columns_part_filter3, FALSE);
  CHECK_LIST_COLUMNS_ROW(800, 4, 4, truncate_list_columns_part_filter3, FALSE);

  // truncate_list_columns_part_filter1 rescan last partition (default) --> ((1,1),(2,2),(3,3))
  ObTruncateInfo rescan_list_columns_truncate_info1;
  row_scn = 2000;
  list_obj1.set_int(2);
  list_obj2.set_int(3);
  build_truncate_info(ObTruncatePartition::LIST_COLUMNS_PART, row_scn, list_columns_row_values3, 2, rescan_list_columns_truncate_info1);
  rescan_filter(rescan_list_columns_truncate_info1, truncate_list_columns_part_filter1);
  CHECK_LIST_COLUMNS_ROW(800, 1, 1, truncate_list_columns_part_filter1, TRUE);
  CHECK_LIST_COLUMNS_ROW(1000, 1, 1, truncate_list_columns_part_filter1, TRUE);
  CHECK_LIST_COLUMNS_ROW(1001, 1, 1, truncate_list_columns_part_filter1, TRUE);
  CHECK_LIST_COLUMNS_ROW(800, 2, 2, truncate_list_columns_part_filter1, TRUE);
  CHECK_LIST_COLUMNS_ROW(1001, 2, 2, truncate_list_columns_part_filter1, TRUE);
  CHECK_LIST_COLUMNS_ROW(800, 3, 3, truncate_list_columns_part_filter1, TRUE);
  CHECK_LIST_COLUMNS_ROW(1000, 3, 3, truncate_list_columns_part_filter1, TRUE);
  CHECK_LIST_COLUMNS_ROW(1001, 3, 3, truncate_list_columns_part_filter1, TRUE);
  CHECK_LIST_COLUMNS_ROW(800, 4, 4, truncate_list_columns_part_filter1, FALSE);
  CHECK_LIST_COLUMNS_ROW(2000, 1, 1, truncate_list_columns_part_filter1, TRUE);
  CHECK_LIST_COLUMNS_ROW(2000, 2, 2, truncate_list_columns_part_filter1, TRUE);
  CHECK_LIST_COLUMNS_ROW(2000, 3, 3, truncate_list_columns_part_filter1, TRUE);
  CHECK_LIST_COLUMNS_ROW(2001, 1, 1, truncate_list_columns_part_filter1, FALSE);
  CHECK_LIST_COLUMNS_ROW(2001, 2, 2, truncate_list_columns_part_filter1, FALSE);
  CHECK_LIST_COLUMNS_ROW(2001, 3, 3, truncate_list_columns_part_filter1, FALSE);
}

TEST_F(ObTruncatePartitionFitlerTest, subpart_filter)
{
  get_index_table_cols_desc();
  get_index_table_cols_param(true);
  ObDatumRow index_part_row;
  ASSERT_EQ(OB_SUCCESS, index_part_row.init(allocator_, 4));
  ObStorageDatum &scn_datum = index_part_row.storage_datums_[2];
  ObStorageDatum &part_datum = index_part_row.storage_datums_[0];
  ObStorageDatum &subpart_datum = index_part_row.storage_datums_[1];
  int64_t row_scn = 1000;
  ObObj range_begin_obj, range_end_obj;
  ObRowkey range_begin_rowkey(&range_begin_obj, 1);
  ObRowkey range_end_rowkey(&range_end_obj, 1);
  bool filtered = false;
  // ---- first partition (min : 10)
  range_begin_obj.set_min_value();
  range_end_obj.set_int(10);

  ObObj list_obj1;
  ObNewRow list_row1;
  list_row1.assign(&list_obj1, 1);

  // ---- first partition ((1))
  list_obj1.set_int(1);

  ObListRowValues list_row_values;
  ASSERT_EQ(OB_SUCCESS, list_row_values.push_back(list_row1));

  #define CHECK_SUB_PART_ROW(scn_val, part_val, subpart_val, truncate_filter, expected) \
  filtered = false; \
  scn_datum.set_int(scn_val); \
  part_datum.set_int(part_val); \
  subpart_datum.set_int(subpart_val); \
  ASSERT_EQ(OB_SUCCESS, truncate_filter.filter(index_part_row, filtered)); \
  ASSERT_##expected(filtered);

  col_idxs_[0] = 0;
  sub_col_idxs_[0] = 1;
  ObTruncateInfo range_list_truncate_info1;
  build_range_list_truncate_info(
      row_scn,
      range_begin_rowkey, range_end_rowkey,
      list_row_values,
      1,
      range_list_truncate_info1);
  
  ObTruncatePartitionFilter truncate_range_list_part_filter1;
  get_index_table_cols_param(false);
  build_filter(range_list_truncate_info1, truncate_range_list_part_filter1);

  CHECK_SUB_PART_ROW(800, 1,1, truncate_range_list_part_filter1, TRUE);
  CHECK_SUB_PART_ROW(1000, 1,1, truncate_range_list_part_filter1, TRUE);
  CHECK_SUB_PART_ROW(1100, 1,1, truncate_range_list_part_filter1, FALSE);
  CHECK_SUB_PART_ROW(900, 1,0, truncate_range_list_part_filter1, FALSE);
  CHECK_SUB_PART_ROW(900, 1,2, truncate_range_list_part_filter1, FALSE);
  CHECK_SUB_PART_ROW(900, 10,1, truncate_range_list_part_filter1, FALSE);
  CHECK_SUB_PART_ROW(900, 11,1, truncate_range_list_part_filter1, FALSE);


  ObTruncateInfo range_list_truncate_info2;
  build_range_list_truncate_info(
      row_scn,
      range_begin_rowkey, range_end_rowkey,
      list_row_values,
      1,
      range_list_truncate_info2,
      ObTruncatePartition::INCLUDE,
      ObTruncatePartition::EXCEPT);
  
  ObTruncatePartitionFilter truncate_range_list_part_filter2;
  get_index_table_cols_param(true);
  build_filter(range_list_truncate_info2, truncate_range_list_part_filter2);

  CHECK_SUB_PART_ROW(800, 5,0, truncate_range_list_part_filter2, TRUE);
  CHECK_SUB_PART_ROW(1000, 5,0, truncate_range_list_part_filter2, TRUE);
  CHECK_SUB_PART_ROW(1100, 5,0, truncate_range_list_part_filter2, FALSE);
  CHECK_SUB_PART_ROW(800, 5,1, truncate_range_list_part_filter2, FALSE);
  CHECK_SUB_PART_ROW(800, 9,1, truncate_range_list_part_filter2, FALSE);
  CHECK_SUB_PART_ROW(800, 9,0, truncate_range_list_part_filter2, TRUE);
  CHECK_SUB_PART_ROW(800, -1,0, truncate_range_list_part_filter2, TRUE);
  CHECK_SUB_PART_ROW(800, 10,0, truncate_range_list_part_filter2, FALSE);
  CHECK_SUB_PART_ROW(800, 10,1, truncate_range_list_part_filter2, FALSE);
  CHECK_SUB_PART_ROW(800, 10,2, truncate_range_list_part_filter2, FALSE);

  // ---- whole single sub partition (default)
  ObTruncateInfo range_list_truncate_info3;
  ObListRowValues list_row_values3;
  build_range_list_truncate_info(
      row_scn,
      range_begin_rowkey, range_end_rowkey,
      list_row_values3,
      1,
      range_list_truncate_info3,
      ObTruncatePartition::INCLUDE,
      ObTruncatePartition::ALL);
  ObTruncatePartitionFilter truncate_range_list_part_filter3;
  get_index_table_cols_param(false);
  build_filter(range_list_truncate_info3, truncate_range_list_part_filter3);
  CHECK_SUB_PART_ROW(800, 5,0, truncate_range_list_part_filter3, TRUE);
  CHECK_SUB_PART_ROW(800, 5,1, truncate_range_list_part_filter3, TRUE);
  CHECK_SUB_PART_ROW(800, 5,2, truncate_range_list_part_filter3, TRUE);
  CHECK_SUB_PART_ROW(800, 5,3, truncate_range_list_part_filter3, TRUE);
  CHECK_SUB_PART_ROW(800, 10,0, truncate_range_list_part_filter3, FALSE);
  CHECK_SUB_PART_ROW(800, 10,3, truncate_range_list_part_filter3, FALSE);
  CHECK_SUB_PART_ROW(1001, 5,0, truncate_range_list_part_filter3, FALSE);
  CHECK_SUB_PART_ROW(1001, 5,1, truncate_range_list_part_filter3, FALSE);
  
  // truncate_range_list_part_filter3 rescan partition level one [10 : 100)
  ObTruncateInfo rescan_range_list_truncate_info3;
  row_scn = 2000;
  range_begin_obj.set_int(10);
  range_end_obj.set_int(100);
  build_range_list_truncate_info(
      row_scn,
      range_begin_rowkey, range_end_rowkey,
      list_row_values3,
      1,
      rescan_range_list_truncate_info3,
      ObTruncatePartition::INCLUDE,
      ObTruncatePartition::ALL);
  rescan_filter(rescan_range_list_truncate_info3, truncate_range_list_part_filter3);
  CHECK_SUB_PART_ROW(800, 9,0, truncate_range_list_part_filter3, FALSE);
  CHECK_SUB_PART_ROW(800, 10,0, truncate_range_list_part_filter3, TRUE);
  CHECK_SUB_PART_ROW(1800, 20,1, truncate_range_list_part_filter3, TRUE);
  CHECK_SUB_PART_ROW(2800, 30,2, truncate_range_list_part_filter3, FALSE);
  CHECK_SUB_PART_ROW(800, 99,3, truncate_range_list_part_filter3, TRUE);
  CHECK_SUB_PART_ROW(800, 100,0, truncate_range_list_part_filter3, FALSE);
}

TEST_F(ObTruncatePartitionFitlerTest, multi_truncate_info)
{
  get_index_table_cols_desc();
  get_index_table_cols_param(false);
  // share::ObTenantSwitchGuard tguard;
  // ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  // common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  // sqlclient::ObISQLConnection *conn = NULL;
  // ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));

  // ---------------------- range partition ----------------------
  // ObSqlString sql;
  // sql.assign_fmt("create table t_range1(c1 int, c2 int, c3 int, primary key(c1,c2)) \
  //     partition by range(c2) \
  //     (partition `p0` values less than (100), \
  //     partition `p1` values less than (200), \
  //     partition `p2` values less than (300), \
  //     partition `p3` values less than (MAXVALUE))");
  // int64_t affected_rows = 0;
  // ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  // ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create index t_range1_idx on t_range1(c2) global", affected_rows));
  // get_index_table_cols_desc(conn, "t_range1");

  ObDatumRow index_part_row;
  ASSERT_EQ(OB_SUCCESS, index_part_row.init(allocator_, 4));
  ObStorageDatum &scn_datum = index_part_row.storage_datums_[2];
  ObStorageDatum &range_part_datum = index_part_row.storage_datums_[0];
  int64_t row_scn = 0;
  ObObj range_begin_obj, range_end_obj;
  ObRowkey range_begin_rowkey(&range_begin_obj, 1);
  ObRowkey range_end_rowkey(&range_end_obj, 1);
  bool filtered = false;

  ObTruncateInfoArray truncate_info_array;
  truncate_info_array.init_for_first_creation(allocator_);

  // ---- first partition (min : 100), scn = 1000
  range_begin_obj.set_min_value();
  range_end_obj.set_int(100);

  ObTruncateInfo range_truncate_info1;
  col_idxs_[0] = 0;
  build_truncate_info(ObTruncatePartition::RANGE_PART, 1000/*row_scn*/, range_begin_rowkey, range_end_rowkey, 1, range_truncate_info1);
  ASSERT_EQ(OB_SUCCESS, truncate_info_array.append_with_deep_copy(range_truncate_info1));
  // ---- second partition [100 : 200), scn = 1200
  range_begin_obj.set_int(100);
  range_end_obj.set_int(200);

  ObTruncateInfo range_truncate_info2;
  build_truncate_info(ObTruncatePartition::RANGE_PART, 1200/*row_scn*/, range_begin_rowkey, range_end_rowkey, 1, range_truncate_info2);
  ASSERT_EQ(OB_SUCCESS, truncate_info_array.append_with_deep_copy(range_truncate_info2));

#define CHECK_ROW(scn, range_column, flag)                                     \
  scn_datum.set_int(scn);                                                      \
  range_part_datum.set_int(range_column);                                      \
  ASSERT_EQ(OB_SUCCESS,                                                        \
            truncate_range_part_filter.filter(index_part_row, filtered));      \
  ASSERT_##flag(filtered);
  {
    ObTruncatePartitionFilter truncate_range_part_filter;
    get_index_table_cols_param(false);
    build_filter(truncate_info_array, truncate_range_part_filter);

    row_scn = 800;
    CHECK_ROW(row_scn, 5, TRUE);
    CHECK_ROW(row_scn, 100, TRUE);
    CHECK_ROW(row_scn, 199, TRUE);
    CHECK_ROW(row_scn, 200, FALSE);
    CHECK_ROW(row_scn, 201, FALSE);

    row_scn = 1100;
    CHECK_ROW(row_scn, 50, FALSE);
    CHECK_ROW(row_scn, 100, TRUE);
    CHECK_ROW(row_scn, 150, TRUE);
    CHECK_ROW(row_scn, 200, FALSE);

    row_scn = 1300;
    CHECK_ROW(row_scn, 10, FALSE);
    CHECK_ROW(row_scn, 100, FALSE);
    CHECK_ROW(row_scn, 200, FALSE);
    CHECK_ROW(row_scn, 201, FALSE);
  }

  {
    // ---- last partition [300 : max), scn = 900
    range_begin_obj.set_int(300);
    range_end_obj.set_max_value();

    ObTruncateInfo range_truncate_info3;
    build_truncate_info(ObTruncatePartition::RANGE_PART, 900/*row_scn*/, range_begin_rowkey, range_end_rowkey, 1, range_truncate_info3);
    ASSERT_EQ(OB_SUCCESS, truncate_info_array.append_with_deep_copy(range_truncate_info3));
    ObTruncatePartitionFilter truncate_range_part_filter;
    get_index_table_cols_param(true);
    build_filter(truncate_info_array, truncate_range_part_filter);

    row_scn = 800;
    CHECK_ROW(row_scn, 5, TRUE);
    CHECK_ROW(row_scn, 100, TRUE);
    CHECK_ROW(row_scn, 199, TRUE);
    CHECK_ROW(row_scn, 200, FALSE);
    CHECK_ROW(row_scn, 201, FALSE);
    CHECK_ROW(row_scn, 300, TRUE);
    CHECK_ROW(row_scn, 400, TRUE);
    CHECK_ROW(row_scn, 500, TRUE);

    row_scn = 1100;
    CHECK_ROW(row_scn, 50, FALSE);
    CHECK_ROW(row_scn, 100, TRUE);
    CHECK_ROW(row_scn, 150, TRUE);
    CHECK_ROW(row_scn, 200, FALSE);
    CHECK_ROW(row_scn, 300, FALSE);

    row_scn = 1300;
    CHECK_ROW(row_scn, 10, FALSE);
    CHECK_ROW(row_scn, 100, FALSE);
    CHECK_ROW(row_scn, 200, FALSE);
    CHECK_ROW(row_scn, 201, FALSE);
    CHECK_ROW(row_scn, 300, FALSE);
  }
#undef CHECK_ROW
  // ---------------------- list partition --------------------------
  // sql.assign_fmt("create table t_list(c1 int, c2 int, c3 int, primary key(c1,c2)) \
  //     partition by list(c2) \
  //     (partition `p0` values in (1), \
  //     partition `p1` values in (2,3), \
  //     partition `p2` values in (4,5), \
  //     partition `p3` values in (DEFAULT))");
  // affected_rows = 0;
  // ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  // ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create index t_list_idx on t_list(c2) global", affected_rows));
  // get_index_table_cols_desc(conn, "t_list");

  ObTruncateInfoArray truncate_info_array2;
  truncate_info_array2.init_for_first_creation(allocator_);

  ObObj list_obj1;
  ObNewRow list_row1;
  list_row1.assign(&list_obj1, 1);

  ObListRowValues list_row_values;

  ObStorageDatum &list_part_datum = index_part_row.storage_datums_[0];
  index_part_row.reuse();

#define CHECK_ROW(scn, list_column, flag)                                      \
  scn_datum.set_int(scn);                                                      \
  list_part_datum.set_int(list_column);                                        \
  ASSERT_EQ(OB_SUCCESS,                                                        \
            truncate_list_part_filter.filter(index_part_row, filtered));       \
  ASSERT_##flag(filtered);
  // ---- first partition ((1)), scn = 1000
  list_obj1.set_int(1);
  ObTruncateInfo list_truncate_info1;
  ASSERT_EQ(OB_SUCCESS, list_row_values.push_back_with_deep_copy(allocator_, list_row1));
  build_truncate_info(ObTruncatePartition::LIST_PART, 1000/*row_scn*/, list_row_values, 1, list_truncate_info1);
  ASSERT_EQ(OB_SUCCESS, truncate_info_array2.append_with_deep_copy(list_truncate_info1));
  // ---- second partition except((1, 2, 3, 4, 5)), scn = 1200
  list_obj1.set_int(2);
  ASSERT_EQ(OB_SUCCESS, list_row_values.push_back_with_deep_copy(allocator_, list_row1));
  list_obj1.set_int(3);
  ASSERT_EQ(OB_SUCCESS, list_row_values.push_back_with_deep_copy(allocator_, list_row1));
  list_obj1.set_int(4);
  ASSERT_EQ(OB_SUCCESS, list_row_values.push_back_with_deep_copy(allocator_, list_row1));
  list_obj1.set_int(5);
  ASSERT_EQ(OB_SUCCESS, list_row_values.push_back_with_deep_copy(allocator_, list_row1));

  ObTruncateInfo list_truncate_info2;
  build_truncate_info(ObTruncatePartition::LIST_PART, 1200/*row_scn*/, list_row_values, 1, list_truncate_info2, ObTruncatePartition::EXCEPT);
  ASSERT_EQ(OB_SUCCESS, truncate_info_array2.append_with_deep_copy(list_truncate_info2));
  {
    ObTruncatePartitionFilter truncate_list_part_filter;
    get_index_table_cols_param(true);
    build_filter(truncate_info_array2, truncate_list_part_filter);
    row_scn = 800;
    CHECK_ROW(row_scn, 1, TRUE);
    CHECK_ROW(row_scn, 2, FALSE);
    CHECK_ROW(row_scn, 5, FALSE);
    CHECK_ROW(row_scn, 10, TRUE);
    CHECK_ROW(row_scn, 100, TRUE);

    row_scn = 1100;
    CHECK_ROW(row_scn, 1, FALSE);
    CHECK_ROW(row_scn, 2, FALSE);
    CHECK_ROW(row_scn, 3, FALSE);
    CHECK_ROW(row_scn, 4, FALSE);
    CHECK_ROW(row_scn, 5, FALSE);
    CHECK_ROW(row_scn, 10, TRUE);
    CHECK_ROW(row_scn, 100, TRUE);

    row_scn = 1300;
    CHECK_ROW(row_scn, 1, FALSE);
    CHECK_ROW(row_scn, 2, FALSE);
    CHECK_ROW(row_scn, 3, FALSE);
    CHECK_ROW(row_scn, 4, FALSE);
    CHECK_ROW(row_scn, 5, FALSE);
    CHECK_ROW(row_scn, 10, FALSE);
    CHECK_ROW(row_scn, 100, FALSE);
  }
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_truncate_partition_filter.log*");
  OB_LOGGER.set_file_name("test_truncate_partition_filter.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}