// Copyright 2019-2021 Alibaba Inc. All Rights Reserved.
// Author:
//     
// This file defines test_medium_compaction_mgr.cpp
//

#include <gtest/gtest.h>
#define protected public
#define private public
#include <string.h>
#include "storage/compaction/ob_medium_compaction_mgr.h"
#include "share/schema/ob_column_schema.h"

namespace oceanbase
{
using namespace common;
using namespace compaction;
using namespace storage;

namespace unittest
{

class TestMediumCompactionMgr : public ::testing::Test
{
public:
  virtual void SetUp()
  {
    share::schema::ObTableSchema table_schema;
    prepare_schema(table_schema);

    medium_info_.compaction_type_ = ObMediumCompactionInfo::MEDIUM_COMPACTION;
    medium_info_.medium_snapshot_ = 100;
    medium_info_.data_version_ = 100;
    medium_info_.cluster_id_ = INIT_CLUSTER_ID;

    medium_info_.storage_schema_.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL);
    GCONF.cluster_id = 1;
  }
  virtual void TearDown()
  {
    medium_info_.reset();
    allocator_.reset();
  }
  int construct_list(
      const char *snapshot_list,
      ObMediumCompactionInfoList &list,
      const int64_t cluster_id = INIT_CLUSTER_ID);
  int construct_array(
      const char *snapshot_list,
      ObIArray<int64_t> &array);
  void prepare_schema(share::schema::ObTableSchema &table_schema);

  static const int64_t INIT_CLUSTER_ID = 1;
  static const int64_t OTHER_CLUSTER_ID = 2;
private:
  ObMediumCompactionInfo medium_info_;
  ObSEArray<int64_t, 10> array_;
  ObArenaAllocator allocator_;
};

int TestMediumCompactionMgr::construct_array(
    const char *snapshot_list,
    ObIArray<int64_t> &array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(snapshot_list)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(snapshot_list));
  } else {
    array.reset();
    std::string copy(snapshot_list);
    char *org = const_cast<char *>(copy.c_str());
    static const char *delim = " ";
    char *s = std::strtok(org, delim);
    if (NULL != s) {
      array.push_back(atoi(s));
      while (NULL != (s= strtok(NULL, delim))) {
        array.push_back(atoi(s));
      }
    }
  }
  return ret;
}

int TestMediumCompactionMgr::construct_list(
    const char *snapshot_list,
    ObMediumCompactionInfoList &list,
    const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (!list.is_inited_ && OB_FAIL(list.init(allocator_))) {
    COMMON_LOG(WARN, "failed to init list", K(ret));
  }
  construct_array(snapshot_list, array_);
  for (int i = 0; OB_SUCC(ret) && i < array_.count(); ++i) {
    medium_info_.cluster_id_ = cluster_id;
    medium_info_.medium_snapshot_ = array_.at(i);
    ret = list.add_medium_compaction_info(medium_info_);
  }
  return ret;
}

static const int64_t TENANT_ID = 1;
static const int64_t TABLE_ID = 7777;
static const int64_t TEST_ROWKEY_COLUMN_CNT = 3;
static const int64_t TEST_COLUMN_CNT = 6;

void TestMediumCompactionMgr::prepare_schema(share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  int64_t micro_block_size = 16 * 1024;
  const uint64_t tenant_id = TENANT_ID;
  const uint64_t table_id = TABLE_ID;
  share::schema::ObColumnSchemaV2 column;

  //generate data table schema
  table_schema.reset();
  ret = table_schema.set_table_name("test_merge_multi_version");
  ASSERT_EQ(OB_SUCCESS, ret);
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  const int64_t column_ids[] = {16,17,20,21,22,23,24,29};
  for(int64_t i = 0; i < TEST_COLUMN_CNT; ++i){
    ObObjType obj_type = ObIntType;
    const int64_t column_id = column_ids[i];

    if (i == 1) {
      obj_type = ObVarcharType;
    }
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(column_id);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(10);
    if (i < TEST_ROWKEY_COLUMN_CNT) {
      column.set_rowkey_position(i + 1);
    } else {
      column.set_rowkey_position(0);
    }
    COMMON_LOG(INFO, "add column", K(i), K(column));
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }
  COMMON_LOG(INFO, "dump stable schema", LITERAL_K(TEST_ROWKEY_COLUMN_CNT), K(table_schema));
}

TEST_F(TestMediumCompactionMgr, test_basic_init)
{
  ObMediumCompactionInfoList list_1;
  ASSERT_EQ(OB_SUCCESS, construct_list("300, 400, 500", list_1));

  ObMediumCompactionInfoList list_2;
  ASSERT_EQ(OB_SUCCESS, construct_list("100, 200", list_2));

  ObMediumCompactionInfoList out_list;
  ASSERT_EQ(OB_SUCCESS, out_list.init(allocator_, &list_2, &list_1, 0));

  ASSERT_EQ(5, out_list.size());
  ASSERT_EQ(100, out_list.get_min_medium_snapshot());
  ASSERT_EQ(500, out_list.get_max_medium_snapshot());

  out_list.reset();

  ASSERT_EQ(OB_SUCCESS, out_list.init(allocator_, &list_2, &list_1, 400));
  ASSERT_EQ(1, out_list.size());
  ASSERT_EQ(500, out_list.get_min_medium_snapshot());
  ASSERT_EQ(500, out_list.get_max_medium_snapshot());
}

TEST_F(TestMediumCompactionMgr, test_push_list_error)
{
  ObMediumCompactionInfoList test_list;
  ASSERT_EQ(OB_SUCCESS, construct_list("300, 500, 400", test_list));
  ASSERT_EQ(test_list.get_max_medium_snapshot(), 500);
  ASSERT_EQ(test_list.get_list().get_size(), 2);

  medium_info_.medium_snapshot_ = 900;
  ASSERT_EQ(OB_SUCCESS, test_list.add_medium_compaction_info(medium_info_));
  ASSERT_EQ(test_list.get_list().get_size(), 3);

  medium_info_.medium_snapshot_ = 700;
  ASSERT_EQ(OB_SUCCESS, test_list.add_medium_compaction_info(medium_info_));
  ASSERT_EQ(test_list.get_list().get_size(), 3);
  ASSERT_EQ(test_list.get_max_medium_snapshot(), 900);

  medium_info_.medium_snapshot_ = 1000;
  ASSERT_EQ(OB_SUCCESS, test_list.add_medium_compaction_info(medium_info_));
  ASSERT_EQ(test_list.get_list().get_size(), 4);

  medium_info_.medium_snapshot_ = 1200;
  ASSERT_EQ(OB_SUCCESS, test_list.add_medium_compaction_info(medium_info_));
  ASSERT_EQ(test_list.get_list().get_size(), 5);

  medium_info_.medium_snapshot_ = 100;
  ASSERT_EQ(OB_SUCCESS, test_list.add_medium_compaction_info(medium_info_));
  ASSERT_EQ(test_list.get_list().get_size(), 5);

  medium_info_.medium_snapshot_ = 100;
  ASSERT_EQ(test_list.size(), 5); // 300 500 900 1000 1200

  ObMediumCompactionInfoList test_list_2;
  ASSERT_EQ(OB_SUCCESS, test_list_2.init(allocator_, &test_list, nullptr, 900));
  ASSERT_EQ(1000, test_list_2.get_min_medium_snapshot());
  ASSERT_EQ(test_list_2.size(), 2);
  const ObMediumCompactionInfo *ret_info = nullptr;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, test_list_2.get_specified_scn_info(900, ret_info));

  test_list_2.reset();
  ASSERT_EQ(OB_SUCCESS, test_list_2.init(allocator_, &test_list, nullptr, 900));
}

}//end namespace unittest
}//end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_medium_compaction_mgr.log*");
  OB_LOGGER.set_file_name("test_medium_compaction_mgr.log");
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
