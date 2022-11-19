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
#include "ob_row_generate.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
using namespace share::schema;

namespace blocksstable
{

class TestRowGenerate : public ::testing::Test
{
public:
  TestRowGenerate() {}
  virtual ~TestRowGenerate() {}
  virtual void SetUp();
  virtual void TearDown();
};

void TestRowGenerate::SetUp()
{

}

void TestRowGenerate::TearDown()
{

}

TEST_F(TestRowGenerate, generate_and_check_row)
{
  int64_t table_id = 3001;
  const int64_t rowkey_column_num = 2;
  static const int64_t column_num = common::ObUnknownType;

  ObTableSchema table_schema;
  ObColumnSchemaV2 column;

  table_schema.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema.set_table_name("test_row_generate"));
  table_schema.set_tenant_id(1);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);

  char name[OB_MAX_FILE_NAME_LENGTH];
  for(int64_t i = 0; i < column_num; ++i){
    ObObjType obj_type = static_cast<ObObjType>(i);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    if(obj_type == common::ObIntType || obj_type == common::ObNumberType){
      column.set_rowkey_position(1);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }

  table_schema.set_rowkey_column_num(rowkey_column_num);
  table_schema.set_max_used_column_id(column_num);
  oceanbase::blocksstable::ObRowGenerate row_gene;

  oceanbase::common::ObObj objs[table_schema.get_column_count()];
  oceanbase::storage::ObStoreRow row;
  row.row_val_.cells_ = objs;
  row.row_val_.count_ = table_schema.get_column_count();

  ASSERT_EQ(OB_SUCCESS, row_gene.init(table_schema));
  for(int64_t i = 0; i < 100; ++i){
    bool exist = false;
    ASSERT_EQ(OB_SUCCESS, row_gene.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, row_gene.check_one_row(row, exist));
    ASSERT_TRUE(exist);
  }
}

TEST_F(TestRowGenerate, generate_and_check_row_own_allocator)
{
  int64_t table_id = 3001;
  const int64_t rowkey_column_num = 2;
  static const int64_t column_num = common::ObUnknownType;

  ObTableSchema table_schema;
  ObColumnSchemaV2 column;

  table_schema.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema.set_table_name("test_row_generate"));
  table_schema.set_tenant_id(1);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);

  char name[OB_MAX_FILE_NAME_LENGTH];
  for(int64_t i = 0; i < column_num; ++i){
    ObObjType obj_type = static_cast<ObObjType>(i);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    if(obj_type == common::ObIntType || obj_type == common::ObNumberType){
      column.set_rowkey_position(1);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }

  table_schema.set_rowkey_column_num(rowkey_column_num);
  table_schema.set_max_used_column_id(column_num);
  oceanbase::blocksstable::ObRowGenerate row_gene;

  oceanbase::common::ObObj objs[table_schema.get_column_count()];
  oceanbase::storage::ObStoreRow row;
  row.row_val_.cells_ = objs;
  row.row_val_.count_ = table_schema.get_column_count();

  ObArenaAllocator allocator(ObModIds::TEST);
  ASSERT_EQ(OB_SUCCESS, row_gene.init(table_schema, &allocator));
  for(int64_t i = 0; i < 100; ++i){
    bool exist = false;
    ASSERT_EQ(OB_SUCCESS, row_gene.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, row_gene.check_one_row(row, exist));
    ASSERT_TRUE(exist);
  }
}
}//blocksstable
}//oceanbase

int main(int argc, char* argv[])
{
  OB_LOGGER.set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
