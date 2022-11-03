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
#include "../mockcontainer/mock_ob_iterator.h"
// #include "storage/ob_sstable_query_struct.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/blocksstable/ob_micro_block_scanner.h"
#include "storage/blocksstable/ob_micro_block_writer.h"
#include "ob_row_generate.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;

namespace unittest
{
static const int64_t rowkey_column_count = 2;
//every ObObjType from ObNullType to ObExtendType
//static const int64_t column_num = ObUnknownType;
static const int64_t column_num = ObHexStringType;
static const int64_t macro_block_size = 2L * 1024 * 1024;
static const int64_t table_id = 3001;
const int64_t test_row_num = 10;

class TestMicroBlockScanner : public ::testing::Test
{
public:
  TestMicroBlockScanner();
  virtual void SetUp();
  virtual void TearDown() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
  int check_row(ObMicroBlockScanner &scanner,
      const int64_t start_row, const int64_t end_row, const bool reverse);

protected:
  ObRowGenerate row_generate_;
  ObColumnMap column_map_;
  ObArenaAllocator allocator_;
  //oceanbase::common::ObObj objs_[column_num];
};

TestMicroBlockScanner::TestMicroBlockScanner()
  : allocator_(ObModIds::TEST)
{

}

void TestMicroBlockScanner::SetUp()
{
  const int64_t table_id = 3001;
  ObTableSchema table_schema;
  ObColumnSchemaV2 column;
  //init table schema
  table_schema.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema.set_table_name("test_row_writer"));
  table_schema.set_tenant_id(1);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(rowkey_column_count);
  table_schema.set_max_used_column_id(column_num);
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for(int64_t i = 0; i < column_num; ++i){
    ObObjType obj_type = static_cast<ObObjType>(i);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(1);
    if(obj_type == common::ObIntType){
      column.set_rowkey_position(1);
    } else if(obj_type == common::ObVarcharType) {
      column.set_rowkey_position(2);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }
  //init ObRowGenerate
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema, &allocator_));
}

int TestMicroBlockScanner::check_row(ObMicroBlockScanner &scanner,
    const int64_t start_row, const int64_t end_row, const bool reverse)
{
  int ret = OB_SUCCESS;
  const ObStoreRow *get_row;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow row;
  row.row_val_.cells_ = objs;
  row.row_val_.count_ = column_num;
  if(!reverse){
    for(int64_t i = start_row; i <= end_row; ++ i){
      EXPECT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, row));
      EXPECT_EQ(OB_SUCCESS, scanner.get_next_row(get_row));
      EXPECT_EQ(get_row->row_val_.count_, column_num);
      for(int64_t j = 0; j < column_num && ret == OB_SUCCESS; ++ j){
        if(get_row->row_val_.cells_[j] != objs[j]){
          ret = OB_ERROR;
          STORAGE_LOG(ERROR, "obj not equal", K(get_row->row_val_.cells_[j]), K(objs[j]));
        }
      }
      bool exist = false;
      EXPECT_EQ(OB_SUCCESS, row_generate_.check_one_row(*get_row, exist));
      EXPECT_TRUE(exist);
    }
  } else {
     for(int64_t i = end_row; i >= start_row; -- i){
      EXPECT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, row));
      EXPECT_EQ(OB_SUCCESS, scanner.get_next_row(get_row));
      EXPECT_EQ(get_row->row_val_.count_, column_num);
      for(int64_t j = 0; j < column_num && ret == OB_SUCCESS; ++ j){
        if(get_row->row_val_.cells_[j] != objs[j]){
          ret = OB_ERROR;
          STORAGE_LOG(ERROR, "obj not equal", K(get_row->row_val_.cells_[j]), K(objs[j]));
        }
      }
      bool exist = false;
      EXPECT_EQ(OB_SUCCESS, row_generate_.check_one_row(*get_row, exist));
      EXPECT_TRUE(exist);
    }
  }
  return ret;
}

TEST_F(TestMicroBlockScanner, test_scanner)
{
  int ret = OB_SUCCESS;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow row;
  row.row_val_.cells_ = objs;
  row.row_val_.count_ = column_num;
  //ObMicroBlockWriter init the column_map and block_data
  ObMicroBlockWriter writer;
  ret = writer.init(macro_block_size, rowkey_column_count, column_num);
  ASSERT_EQ(OB_SUCCESS, ret);
  for(int64_t i = 0; i < test_row_num; ++i){
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  char *buf = NULL;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, writer.build_block(buf, size));
  //init column_map
  ObArray<ObColDesc> cols;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(cols));
  ASSERT_EQ(OB_SUCCESS, column_map_.init(allocator_,
                                         row_generate_.get_schema().get_schema_version(),
                                         row_generate_.get_schema().get_rowkey_column_num(),
                                         column_num, cols));

  ObMicroBlockData block(buf, size);
  //normal success
  ObMicroBlockScanner scanner(allocator_);
  ObStoreRange range;
  range.set_table_id(table_id);
  ObStoreRow start_row;
  ObStoreRow end_row;
  ObObj range_objs[2][column_num];
  start_row.row_val_.cells_ = range_objs[0];
  start_row.row_val_.count_ = column_num;
  end_row.row_val_.cells_ = range_objs[1];
  end_row.row_val_.count_ = column_num;
  //set inclusive
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(0, start_row));
  range.get_start_key().assign(range_objs[0], rowkey_column_count);
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(test_row_num - 1, end_row));
  range.get_end_key().assign(range_objs[1], rowkey_column_count);
  range.get_border_flag().set_inclusive_start();
  range.get_border_flag().set_inclusive_end();
  ret = scanner.set_scan_param(column_map_, range, block, true, false, FLAT_ROW_STORE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, check_row(scanner, 0, test_row_num - 1, false));
  ret = scanner.set_scan_param(column_map_, range, block, true, true, FLAT_ROW_STORE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, check_row(scanner, 0, test_row_num - 1, true));
  //unset inclusive
  range.get_start_key().assign(range_objs[0], rowkey_column_count);
  range.get_end_key().assign(range_objs[1], rowkey_column_count);
  range.get_border_flag().unset_inclusive_start();
  range.get_border_flag().unset_inclusive_end();
  ret = scanner.set_scan_param(column_map_, range, block, true, false, FLAT_ROW_STORE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, check_row(scanner, 1, test_row_num - 2, false));
  ret = scanner.set_scan_param(column_map_, range, block, true, true, FLAT_ROW_STORE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, check_row(scanner, 1, test_row_num - 2, true));
  //not bound
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(1, start_row));
  range.get_start_key().assign(range_objs[0], rowkey_column_count);
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(test_row_num - 2, end_row));
  range.get_end_key().assign(range_objs[1], rowkey_column_count);
  range.get_border_flag().unset_inclusive_start();
  range.get_border_flag().unset_inclusive_start();
  ret = scanner.set_scan_param(column_map_, range, block, false, false, FLAT_ROW_STORE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, check_row(scanner, 0, test_row_num - 1, false));
  ret = scanner.set_scan_param(column_map_, range, block, false, true, FLAT_ROW_STORE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, check_row(scanner, 0, test_row_num - 1, true));

  //block error
  ObMicroBlockData block2(buf, 0);
  ASSERT_EQ(OB_INVALID_ARGUMENT, scanner.set_scan_param(column_map_, range, block2, true, false, FLAT_ROW_STORE));
  //start_key is beyond
  ObStoreRow row_beyond;
  ObObj obj_tmp[column_num];
  row_beyond.row_val_.cells_ = obj_tmp;
  row_beyond.row_val_.count_ = column_num;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row_beyond));
  range.get_start_key().assign(row_beyond.row_val_.cells_, rowkey_column_count);
  ASSERT_EQ(OB_INVALID_ARGUMENT, scanner.set_scan_param(column_map_, range, block, true, false, FLAT_ROW_STORE));
  const ObStoreRow *r_row = NULL;
  /*ASSERT_EQ(OB_BEYOND_THE_RANGE, scanner.get_next_row(r_row));
  ASSERT_TRUE(NULL == r_row);*/
  //start key is the last one and unset inclusive
  range.get_border_flag().unset_inclusive_start();
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(test_row_num - 1, start_row));
  range.get_start_key().assign(range_objs[0], rowkey_column_count);
  ASSERT_EQ(OB_INVALID_ARGUMENT, scanner.set_scan_param(column_map_,range, block, true, false, FLAT_ROW_STORE));
  //endkey key is last one
  range.get_border_flag().set_inclusive_start();
  range.get_border_flag().unset_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(0, start_row));
  range.get_start_key().assign(range_objs[0], rowkey_column_count);
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(test_row_num - 1, end_row));
  range.get_end_key().assign(range_objs[1], rowkey_column_count);
  ASSERT_EQ(OB_SUCCESS, scanner.set_scan_param(column_map_,range, block, true, false, FLAT_ROW_STORE));
  ASSERT_EQ(OB_SUCCESS, check_row(scanner, 0, test_row_num - 2, false));
  //endkey is the first one and unset inclusive
  range.get_border_flag().unset_inclusive_start();
  range.get_border_flag().unset_inclusive_end();
  range.get_start_key().set_min();
  range.get_end_key().assign(range_objs[0], rowkey_column_count);
  ASSERT_EQ(OB_SUCCESS, scanner.set_scan_param(column_map_, range, block, true, false, FLAT_ROW_STORE));
  ASSERT_EQ(OB_BEYOND_THE_RANGE, scanner.get_next_row(r_row));
  ASSERT_TRUE(NULL == r_row);
  //endkey is beyond
  range.get_end_key().assign(row_beyond.row_val_.cells_, rowkey_column_count);
  ASSERT_EQ(OB_SUCCESS, scanner.set_scan_param(column_map_, range, block, true, false, FLAT_ROW_STORE));
  ASSERT_EQ(OB_SUCCESS, check_row(scanner, 0, test_row_num - 1, false));

}

}//end namespace unittest
}//end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
