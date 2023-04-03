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

#include "storage/ob_multi_version_sstable_test.h"
#include <gtest/gtest.h>
#define private public
#include "storage/ob_partition_base_data_ob_reader.h"

namespace oceanbase
{
using namespace storage;
using namespace share::schema;
using namespace common;
namespace unittest
{

class ObLogicRowProducerWrapper : public ObIStoreRowIterator
{
public:
  ObLogicRowProducerWrapper(ObLogicRowProducer &producer) : producer_(producer) {}
  int get_next_row(const ObStoreRow *&row)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(producer_.get_next_row(row))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "failed to get next row", K(ret));
      }
    }
    return ret;
  }
private:
  ObLogicRowProducer& producer_;
};

class TestLogicRowProducer : public ObMultiVersionSSTableTest
{
public:
  TestLogicRowProducer() : ObMultiVersionSSTableTest("test_logic_row_producer") {}
  ~TestLogicRowProducer() {}
  void SetUp()
  {
    ObMultiVersionSSTableTest::SetUp();
  }
  void TearDown()
  {
    ObMultiVersionSSTableTest::TearDown();
  }
};

TEST_F(TestLogicRowProducer, bug16079910)
{
  ObTableSchema table_schema;
  ObColumnSchemaV2 column;
  table_schema.set_tenant_id(TENANT_ID);
  table_schema.set_table_id(combine_id(TENANT_ID, TABLE_ID));
  table_schema.set_rowkey_column_num(1);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_compress_func_name("none");
  table_schema.set_table_type(USER_TABLE);
  table_schema.set_schema_version(SCHEMA_VERSION);
  table_schema.set_table_name(ObString("test_table"));
  column.reset();
  column.set_table_id(combine_id(TENANT_ID, TABLE_ID));
  column.set_column_id(16);
  column.set_data_type(ObVarcharType);
  column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  column.set_rowkey_position(1);
  column.set_data_length(1);
  ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));

  const int64_t rowkey_cnt = 2;
  const char *micro_data[2];
  micro_data[0] =
      "var              bigint flag    multi_version_row_flag\n"
      "hesitati         -8     EXIST   CL\n"
      "zappedspa        -2     EXIST   CL\n"
      "zappedverifying  -7     EXIST   CL\n"
      "zapping          -8     EXIST   CL\n";

  micro_data[1] =
      "var              bigint flag    multi_version_row_flag\n"
      "zappingc         -8     EXIST   CL\n"
      "zappingd         -2     EXIST   CL\n"
      "zappingf         -7     EXIST   CL\n"
      "zappingi         -8     EXIST   CL\n";

  prepare_data(micro_data, 2, rowkey_cnt);

  const char *result =
      "var              bigint flag    multi_version_row_flag\n"
      "zappedspa        -2     EXIST   CL\n"
      "zappedverifying  -7     EXIST   CL\n"
      "zapping          -8     EXIST   CL\n"
      "zappingc         -8     EXIST   CL\n"
      "zappingd         -2     EXIST   CL\n"
      "zappingf         -7     EXIST   CL\n"
      "zappingi         -8     EXIST   CL\n";


  ObLogicRowProducer producer;
  ObTablesHandle tables_handle;
  ObVersionRange version_range;
  const ObStoreRow *row;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 0;
  version_range.snapshot_version_ = 10;
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable_));
  producer.arg_.table_key_.table_id_ = combine_id(TENANT_ID, TABLE_ID);
  producer.arg_.key_range_.set_whole_range();
  producer.is_inited_ = true;
  OK(producer.data_buffer_.ensure_space(2<<20));
  producer.table_schema_ = &table_schema;
  ASSERT_EQ(OB_SUCCESS, producer.ms_row_iterator_.init(tables_handle, table_schema, producer.arg_.key_range_, version_range));
  ASSERT_EQ(OB_SUCCESS, producer.get_next_row(row));
  ObStoreRange new_key_range;
  producer.ms_row_iterator_.reuse();
  ASSERT_EQ(OB_SUCCESS, producer.set_new_key_range(new_key_range));
  ASSERT_EQ(OB_SUCCESS, producer.ms_row_iterator_.init(tables_handle, table_schema, new_key_range, version_range));
  ObLogicRowProducerWrapper producer_wrapper(producer);
  ObMockIterator res_iter;
  OK(res_iter.from(result));
  ASSERT_TRUE(res_iter.equals(producer_wrapper, true));
}

}
}

int main(int argc, char **argv)
{
  STORAGE_LOG(INFO, "begin unittest: test_logic_row_producer");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
