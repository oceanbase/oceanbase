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
#include "blocksstable/ob_row_generate.h"
#include "storage/ob_i_store.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace share::schema;
using namespace storage;
using namespace common;
class TestIStore : public ::testing::Test
{
public:
  TestIStore();
  ~TestIStore();
  virtual void SetUp();
protected:
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 8;
  static const int64_t TEST_COLUMN_CNT = ObExtendType - 1;
  void prepare_schema();
  ObTableSchema table_schema_;
  ObRowGenerate row_generate_;
  ObArenaAllocator allocator_;
};

TestIStore::TestIStore() :
    table_schema_()
{
}

TestIStore::~TestIStore()
{

}

void TestIStore::SetUp()
{
  prepare_schema();
  row_generate_.init(table_schema_, &allocator_);
}

void TestIStore::prepare_schema()
{
  int64_t table_id = 3001;
  ObColumnSchemaV2 column;
  //init table schema
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_i_store"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(4 * 1024);
  table_schema_.set_compress_func_name("none");
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  ObObjMeta meta_type;
  for (int64_t i = 0; i < TEST_COLUMN_CNT; ++i) {
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    column.set_data_length(1);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    meta_type.set_type(obj_type);
    column.set_meta_type(meta_type);
    if (ob_is_string_type(obj_type) && obj_type != ObHexStringType) {
      meta_type.set_collation_level(CS_LEVEL_IMPLICIT);
      meta_type.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      column.set_meta_type(meta_type);
    }
    if (obj_type == common::ObVarcharType) {
      column.set_rowkey_position(1);
    } else if (obj_type == common::ObCharType) {
      column.set_rowkey_position(2);
    } else if (obj_type == common::ObDoubleType) {
      column.set_rowkey_position(3);
    } else if (obj_type == common::ObNumberType) {
      column.set_rowkey_position(4);
    } else if (obj_type == common::ObUNumberType) {
      column.set_rowkey_position(5);
    } else if (obj_type == common::ObIntType) {
      column.set_rowkey_position(6);
    } else if (obj_type == common::ObHexStringType) {
      column.set_rowkey_position(7);
    } else if (obj_type == common::ObUInt64Type) {
      column.set_rowkey_position(8);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
}


//TEST_F(TestIStore, RowsInfo)
//{
  //int ret = common::OB_SUCCESS;
  //int rowkey_size = 2;
  //int row_count = 10;
  //ObRowsInfo rows_info;
  //void *ptr = allocator_.alloc(row_count * sizeof(ObStoreRow));
  //ObStoreRow *rows = new (ptr) ObStoreRow[row_count];
  //for (int64_t i = 0; i < row_count; i++) {
    //void *rptr = allocator_.alloc(TEST_COLUMN_CNT * sizeof(ObObj));
    //ObObj *cells = new (rptr) ObObj[TEST_COLUMN_CNT];
    //rows[i].row_val_.assign(cells, TEST_COLUMN_CNT);
    //row_generate_.get_next_row(rows[i]);
  //}
  //ret = rows_info.check_duplicate(rows, row_count, rowkey_size);
  //EXPECT_EQ(OB_SUCCESS, ret);

  //row_generate_.get_next_row(0, rows[1]);
  //ret = rows_info.check_duplicate(rows, row_count, rowkey_size);
  //EXPECT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE, ret);

  //row_count = 1;
  //ret = rows_info.check_duplicate(rows, row_count, rowkey_size);
  //EXPECT_EQ(OB_SUCCESS, ret);
//}

}
int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  //OB_LOGGER.set_log_level("ERROR");
  return RUN_ALL_TESTS();
}
