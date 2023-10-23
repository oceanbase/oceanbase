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
#define private public  // 获取private成员
#define protected public  // 获取protect成员
#include "common/row/ob_row.h"
#include "observer/table/ob_table_aggregation.h"

using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::observer;

class TestTableAggregation: public ::testing::Test
{
public:
  TestTableAggregation() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestTableAggregation);
};

TEST_F(TestTableAggregation, agg_min_int)
{
  ObTableAggregation min;
  min.type_ = ObTableAggregationType::MIN;
  min.column_ = ObString("c1");

  ObTableQuery query;
  ASSERT_EQ(OB_SUCCESS, query.aggregations_.push_back(min));
  ASSERT_EQ(OB_SUCCESS, query.select_columns_.push_back("c1"));

  const uint64_t PROJECT_IDX = 0;
  ObSEArray<uint64_t, 4> projs;
  ASSERT_EQ(OB_SUCCESS, projs.push_back(PROJECT_IDX));

  ObTableAggCalculator agg(query);
  agg.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
  agg.set_projs(projs);
  ASSERT_EQ(OB_SUCCESS, agg.init());

  oceanbase::common::ObNewRow row;
  const int COL_NUM = 3;
  ObObj objs[COL_NUM];
  row.cells_ = objs;
  row.count_ = COL_NUM;
  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_int(1);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(1, agg.results_.at(PROJECT_IDX).get_int());

  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_int(0);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(0, agg.results_.at(PROJECT_IDX).get_int());
}

TEST_F(TestTableAggregation, agg_min_double)
{
  ObTableAggregation min;
  min.type_ = ObTableAggregationType::MIN;
  min.column_ = ObString("c1");

  ObTableQuery query;
  ASSERT_EQ(OB_SUCCESS, query.aggregations_.push_back(min));
  ASSERT_EQ(OB_SUCCESS, query.select_columns_.push_back("c1"));

  const uint64_t PROJECT_IDX = 0;
  ObSEArray<uint64_t, 4> projs;
  ASSERT_EQ(OB_SUCCESS, projs.push_back(PROJECT_IDX));

  ObTableAggCalculator agg(query);
  agg.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
  agg.set_projs(projs);
  ASSERT_EQ(OB_SUCCESS, agg.init());

  oceanbase::common::ObNewRow row;
  const int COL_NUM = 3;
  ObObj objs[COL_NUM];
  row.cells_ = objs;
  row.count_ = COL_NUM;
  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_double(1.11);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(1.11, agg.results_.at(PROJECT_IDX).get_double());

  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_double(1.10009);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(1.10009, agg.results_.at(PROJECT_IDX).get_double());
}

TEST_F(TestTableAggregation, agg_min_varchar)
{
  ObTableAggregation min;
  min.type_ = ObTableAggregationType::MIN;
  min.column_ = ObString("c1");

  ObTableQuery query;
  ASSERT_EQ(OB_SUCCESS, query.aggregations_.push_back(min));
  ASSERT_EQ(OB_SUCCESS, query.select_columns_.push_back("c1"));

  const uint64_t PROJECT_IDX = 0;
  ObSEArray<uint64_t, 4> projs;
  ASSERT_EQ(OB_SUCCESS, projs.push_back(PROJECT_IDX));

  ObTableAggCalculator agg(query);
  agg.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
  agg.set_projs(projs);
  ASSERT_EQ(OB_SUCCESS, agg.init());

  oceanbase::common::ObNewRow row;
  const int COL_NUM = 3;
  ObObj objs[COL_NUM];
  row.cells_ = objs;
  row.count_ = COL_NUM;
  char val1[] = "abcde";
  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    row.cells_[i].set_varchar(val1);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(0, memcmp(val1, agg.results_.at(PROJECT_IDX).get_varchar().ptr(), strlen(val1)));

  char val2[] = "abcd";
  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    row.cells_[i].set_varchar(val2);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(0, memcmp(val2, agg.results_.at(PROJECT_IDX).get_varchar().ptr(), strlen(val2)));

  char val3[] = "abcdefg";
  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    row.cells_[i].set_varchar(val3);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(0, memcmp(val2, agg.results_.at(PROJECT_IDX).get_varchar().ptr(), strlen(val2)));
}

TEST_F(TestTableAggregation, agg_max_int)
{
  ObTableAggregation min;
  min.type_ = ObTableAggregationType::MAX;
  min.column_ = ObString("c1");

  ObTableQuery query;
  ASSERT_EQ(OB_SUCCESS, query.aggregations_.push_back(min));
  ASSERT_EQ(OB_SUCCESS, query.select_columns_.push_back("c1"));

  const uint64_t PROJECT_IDX = 0;
  ObSEArray<uint64_t, 4> projs;
  ASSERT_EQ(OB_SUCCESS, projs.push_back(PROJECT_IDX));

  ObTableAggCalculator agg(query);
  agg.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
  agg.set_projs(projs);
  ASSERT_EQ(OB_SUCCESS, agg.init());

  oceanbase::common::ObNewRow row;
  const int COL_NUM = 3;
  ObObj objs[COL_NUM];
  row.cells_ = objs;
  row.count_ = COL_NUM;
  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_int(0);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(0, agg.results_.at(PROJECT_IDX).get_int());

  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_int(1);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(1, agg.results_.at(PROJECT_IDX).get_int());
}

TEST_F(TestTableAggregation, agg_max_double)
{
  ObTableAggregation min;
  min.type_ = ObTableAggregationType::MAX;
  min.column_ = ObString("c1");

  ObTableQuery query;
  ASSERT_EQ(OB_SUCCESS, query.aggregations_.push_back(min));
  ASSERT_EQ(OB_SUCCESS, query.select_columns_.push_back("c1"));

  const uint64_t PROJECT_IDX = 0;
  ObSEArray<uint64_t, 4> projs;
  ASSERT_EQ(OB_SUCCESS, projs.push_back(PROJECT_IDX));

  ObTableAggCalculator agg(query);
  agg.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
  agg.set_projs(projs);
  ASSERT_EQ(OB_SUCCESS, agg.init());

  oceanbase::common::ObNewRow row;
  const int COL_NUM = 3;
  ObObj objs[COL_NUM];
  row.cells_ = objs;
  row.count_ = COL_NUM;
  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_double(0.001);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(0.001, agg.results_.at(PROJECT_IDX).get_double());

  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_double(0.01);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(0.01, agg.results_.at(PROJECT_IDX).get_double());
}

TEST_F(TestTableAggregation, agg_max_varchar)
{
  ObTableAggregation min;
  min.type_ = ObTableAggregationType::MAX;
  min.column_ = ObString("c1");

  ObTableQuery query;
  ASSERT_EQ(OB_SUCCESS, query.aggregations_.push_back(min));
  ASSERT_EQ(OB_SUCCESS, query.select_columns_.push_back("c1"));

  const uint64_t PROJECT_IDX = 0;
  ObSEArray<uint64_t, 4> projs;
  ASSERT_EQ(OB_SUCCESS, projs.push_back(PROJECT_IDX));

  ObTableAggCalculator agg(query);
  agg.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
  agg.set_projs(projs);
  ASSERT_EQ(OB_SUCCESS, agg.init());

  oceanbase::common::ObNewRow row;
  const int COL_NUM = 3;
  ObObj objs[COL_NUM];
  row.cells_ = objs;
  row.count_ = COL_NUM;
  char val1[] = "abcde";
  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    row.cells_[i].set_varchar(val1);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(0, memcmp(val1, agg.results_.at(PROJECT_IDX).get_varchar().ptr(), strlen(val1)));

  char val2[] = "abcd";
  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    row.cells_[i].set_varchar(val2);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(0, memcmp(val1, agg.results_.at(PROJECT_IDX).get_varchar().ptr(), strlen(val1)));

  char val3[] = "abcdefg";
  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    row.cells_[i].set_varchar(val3);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(0, memcmp(val3, agg.results_.at(PROJECT_IDX).get_varchar().ptr(), strlen(val3)));
}

TEST_F(TestTableAggregation, agg_count_column)
{
  ObTableAggregation min;
  min.type_ = ObTableAggregationType::COUNT;
  min.column_ = ObString("c1");

  ObTableQuery query;
  ASSERT_EQ(OB_SUCCESS, query.aggregations_.push_back(min));
  ASSERT_EQ(OB_SUCCESS, query.select_columns_.push_back("c1"));

  const uint64_t PROJECT_IDX = 0;
  ObSEArray<uint64_t, 4> projs;
  ASSERT_EQ(OB_SUCCESS, projs.push_back(PROJECT_IDX));

  ObTableAggCalculator agg(query);
  agg.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
  agg.set_projs(projs);
  ASSERT_EQ(OB_SUCCESS, agg.init());

  oceanbase::common::ObNewRow row;
  const int COL_NUM = 3;
  ObObj objs[COL_NUM];
  row.cells_ = objs;
  row.count_ = COL_NUM;
  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_int(0);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(1, agg.results_.at(PROJECT_IDX).get_int());
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(2, agg.results_.at(PROJECT_IDX).get_int());
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(4, agg.results_.at(PROJECT_IDX).get_int());
}

TEST_F(TestTableAggregation, agg_count_all)
{
  ObTableAggregation min;
  min.type_ = ObTableAggregationType::COUNT;
  min.column_ = ObString("*");

  ObTableQuery query;
  ASSERT_EQ(OB_SUCCESS, query.aggregations_.push_back(min));
  ASSERT_EQ(OB_SUCCESS, query.select_columns_.push_back("c1"));

  const uint64_t PROJECT_IDX = 0;
  ObSEArray<uint64_t, 4> projs;
  ASSERT_EQ(OB_SUCCESS, projs.push_back(PROJECT_IDX));

  ObTableAggCalculator agg(query);
  agg.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
  agg.set_projs(projs);
  ASSERT_EQ(OB_SUCCESS, agg.init());

  oceanbase::common::ObNewRow row;
  const int COL_NUM = 3;
  ObObj objs[COL_NUM];
  row.cells_ = objs;
  row.count_ = COL_NUM;
  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_int(0);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(1, agg.results_.at(PROJECT_IDX).get_int());
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(2, agg.results_.at(PROJECT_IDX).get_int());
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(4, agg.results_.at(PROJECT_IDX).get_int());
}

TEST_F(TestTableAggregation, agg_sum_int)
{
  ObTableAggregation min;
  min.type_ = ObTableAggregationType::SUM;
  min.column_ = ObString("c1");

  ObTableQuery query;
  ASSERT_EQ(OB_SUCCESS, query.aggregations_.push_back(min));
  ASSERT_EQ(OB_SUCCESS, query.select_columns_.push_back("c1"));

  const uint64_t PROJECT_IDX = 0;
  ObSEArray<uint64_t, 4> projs;
  ASSERT_EQ(OB_SUCCESS, projs.push_back(PROJECT_IDX));

  ObTableAggCalculator agg(query);
  agg.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
  agg.set_projs(projs);
  ASSERT_EQ(OB_SUCCESS, agg.init());

  oceanbase::common::ObNewRow row;
  const int COL_NUM = 3;
  ObObj objs[COL_NUM];
  row.cells_ = objs;
  row.count_ = COL_NUM;
  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_int(1);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  agg.final_aggregate();
  ASSERT_EQ(1, agg.results_.at(PROJECT_IDX).get_int());
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  agg.final_aggregate();
  ASSERT_EQ(2, agg.results_.at(PROJECT_IDX).get_int());
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  agg.final_aggregate();
  ASSERT_EQ(4, agg.results_.at(PROJECT_IDX).get_int());
}

TEST_F(TestTableAggregation, agg_sum_double)
{
  ObTableAggregation min;
  min.type_ = ObTableAggregationType::SUM;
  min.column_ = ObString("c1");

  ObTableQuery query;
  ASSERT_EQ(OB_SUCCESS, query.aggregations_.push_back(min));
  ASSERT_EQ(OB_SUCCESS, query.select_columns_.push_back("c1"));

  const uint64_t PROJECT_IDX = 0;
  ObSEArray<uint64_t, 4> projs;
  ASSERT_EQ(OB_SUCCESS, projs.push_back(PROJECT_IDX));

  ObTableAggCalculator agg(query);
  agg.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
  agg.set_projs(projs);
  ASSERT_EQ(OB_SUCCESS, agg.init());

  oceanbase::common::ObNewRow row;
  const int COL_NUM = 3;
  ObObj objs[COL_NUM];
  row.cells_ = objs;
  row.count_ = COL_NUM;
  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_double(0.1);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  agg.final_aggregate();
  ASSERT_EQ(0.1, agg.results_.at(PROJECT_IDX).get_double());
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  agg.final_aggregate();
  ASSERT_EQ(0.2, agg.results_.at(PROJECT_IDX).get_double());
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  agg.final_aggregate();
  ASSERT_EQ(0.4, agg.results_.at(PROJECT_IDX).get_double());
}

TEST_F(TestTableAggregation, agg_avg_int)
{
  ObTableAggregation min;
  min.type_ = ObTableAggregationType::AVG;
  min.column_ = ObString("c1");

  ObTableQuery query;
  ASSERT_EQ(OB_SUCCESS, query.aggregations_.push_back(min));
  ASSERT_EQ(OB_SUCCESS, query.select_columns_.push_back("c1"));

  const uint64_t PROJECT_IDX = 0;
  ObSEArray<uint64_t, 4> projs;
  ASSERT_EQ(OB_SUCCESS, projs.push_back(PROJECT_IDX));

  ObTableAggCalculator agg(query);
  agg.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
  agg.set_projs(projs);
  ASSERT_EQ(OB_SUCCESS, agg.init());

  oceanbase::common::ObNewRow row;
  const int COL_NUM = 3;
  ObObj objs[COL_NUM];
  row.cells_ = objs;
  row.count_ = COL_NUM;
  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_int(1);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  agg.final_aggregate();
  ASSERT_EQ(1, agg.results_.at(PROJECT_IDX).get_double());
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  agg.final_aggregate();
  ASSERT_EQ(1, agg.results_.at(PROJECT_IDX).get_double());
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  agg.final_aggregate();
  ASSERT_EQ(1, agg.results_.at(PROJECT_IDX).get_double());
}

TEST_F(TestTableAggregation, agg_avg_double)
{
  ObTableAggregation min;
  min.type_ = ObTableAggregationType::AVG;
  min.column_ = ObString("c1");

  ObTableQuery query;
  ASSERT_EQ(OB_SUCCESS, query.aggregations_.push_back(min));
  ASSERT_EQ(OB_SUCCESS, query.select_columns_.push_back("c1"));

  const uint64_t PROJECT_IDX = 0;
  ObSEArray<uint64_t, 4> projs;
  ASSERT_EQ(OB_SUCCESS, projs.push_back(PROJECT_IDX));

  ObTableAggCalculator agg(query);
  agg.allocator_.set_tenant_id(OB_SERVER_TENANT_ID);
  agg.set_projs(projs);
  ASSERT_EQ(OB_SUCCESS, agg.init());

  oceanbase::common::ObNewRow row;
  const int COL_NUM = 3;
  ObObj objs[COL_NUM];
  row.cells_ = objs;
  row.count_ = COL_NUM;
  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_double(0.1);
  }

  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  agg.final_aggregate();
  ASSERT_EQ(0.1, agg.results_.at(PROJECT_IDX).get_double());
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  agg.final_aggregate();
  ASSERT_EQ(0.1, agg.results_.at(PROJECT_IDX).get_double());
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  for (int i = 0; i< COL_NUM; ++i) {
    row.cells_[i].set_double(0.4);
  }
  ASSERT_EQ(OB_SUCCESS, agg.aggregate(row));
  agg.final_aggregate();
  ASSERT_EQ(0.16, agg.results_.at(PROJECT_IDX).get_double());
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_table_aggregation.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
