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

#define UNITTEST_DEBUG
#define USING_LOG_PREFIX STORAGE
#include <gmock/gmock.h>

#define private public
#define protected public

#include "storage/ddl/ob_tablet_split_task.h"

namespace oceanbase
{

using namespace common;
using namespace lib;
using namespace share;
using namespace storage;
using namespace blocksstable;
using namespace share::schema;
using namespace compaction;

static const int64_t TEST_TENANT_ID = 1;
static const int64_t TEST_TABLE_ID = 8000;
static const ObLSID TEST_LS_ID = ObLSID(9001);
static const ObTabletID TEST_TABLET_ID = ObTabletID(2323233);
static const int64_t TEST_LOB_COL_CNT = 3;
static const int64_t TEST_SPLIT_DST_CNT = 2;
static const int64_t TEST_CONCURRENT_CNT = 10;

namespace unittest
{

class TestDataSplit : public ::testing::Test
{
public:
  TestDataSplit() = default;
  ~TestDataSplit() = default;
public:
  virtual void SetUp() override;
  virtual void TearDown() override;
public:
  int prepare_mock_start_arg(
      const bool is_split,
      obrpc::ObDDLBuildSingleReplicaRequestArg &arg);
  int prepare_mock_finish_arg(
      obrpc::ObTabletSplitArg &arg);
};

void TestDataSplit::SetUp()
{
}

void TestDataSplit::TearDown()
{
}

int TestDataSplit::prepare_mock_start_arg(
    const bool is_split,
    obrpc::ObDDLBuildSingleReplicaRequestArg &arg)
{
  int ret = OB_SUCCESS;
  arg.tenant_id_           = TEST_TENANT_ID;
  arg.ls_id_               = TEST_LS_ID;
  arg.source_tablet_id_    = TEST_TABLET_ID;
  arg.dest_tablet_id_      = TEST_TABLET_ID;
  arg.source_table_id_     = TEST_TABLE_ID;
  arg.dest_schema_id_      = TEST_TABLE_ID;
  arg.schema_version_      = 1;
  arg.snapshot_version_    = 1;
  arg.ddl_type_            = 1006;
  arg.task_id_             = 1;
  arg.parallelism_         = 1;
  arg.execution_id_        = 1;
  arg.tablet_task_id_      = 1;
  arg.data_format_version_ = 1;
  arg.consumer_group_id_   = 1;
  arg.dest_tenant_id_      = TEST_TENANT_ID;
  arg.dest_ls_id_          = TEST_LS_ID;
  arg.dest_schema_version_ = 1;
  if (is_split) {
    arg.ddl_type_ = 100;
    arg.compaction_scn_ = 1;
    arg.can_reuse_macro_block_ = 1;
    arg.split_sstable_type_ = ObSplitSSTableType::SPLIT_BOTH;
    ObDatumRowkey tmp_datum_rowkey;
    ObStorageDatum datums[OB_INNER_MAX_ROWKEY_COLUMN_NUMBER];
    tmp_datum_rowkey.assign(datums, OB_INNER_MAX_ROWKEY_COLUMN_NUMBER);
    if (OB_FAIL(arg.parallel_datum_rowkey_list_.prepare_allocate(TEST_CONCURRENT_CNT + 1))) {
      LOG_WARN("prepare allocate failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < TEST_CONCURRENT_CNT + 1; i++) {
      if (OB_SUCC(ret) && i < TEST_LOB_COL_CNT) {
        if (OB_FAIL(arg.lob_col_idxs_.push_back(i))) {
          LOG_WARN("push back failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (i == 0) {
        ObDatumRowkey tmp_min_key;
        tmp_min_key.set_min_rowkey();
        if (OB_FAIL(tmp_min_key.deep_copy(arg.parallel_datum_rowkey_list_[i], arg.rowkey_allocator_))) {
          LOG_WARN("failed to push min rowkey", K(ret));
        }
      } else if (i == TEST_CONCURRENT_CNT) {
        ObDatumRowkey tmp_max_key;
        tmp_max_key.set_max_rowkey();
        if (OB_FAIL(tmp_max_key.deep_copy(arg.parallel_datum_rowkey_list_[i], arg.rowkey_allocator_))) {
          LOG_WARN("failed to push min rowkey", K(ret));
        }
      } else {
        tmp_datum_rowkey.datums_[0].set_string("aaaaa");
        tmp_datum_rowkey.datums_[1].set_int(i);
        tmp_datum_rowkey.datum_cnt_ = 2;
        if (OB_FAIL(tmp_datum_rowkey.deep_copy(arg.parallel_datum_rowkey_list_[i] /*dst*/, arg.rowkey_allocator_))) {
          LOG_WARN("failed to deep copy datum rowkey", K(ret), K(i), K(tmp_datum_rowkey));
        }
      }
    }
  }
  return ret;
}

int TestDataSplit::prepare_mock_finish_arg(obrpc::ObTabletSplitArg &arg)
{
  int ret = OB_SUCCESS;
  arg.ls_id_ = TEST_LS_ID;
  arg.table_id_ = TEST_TABLE_ID;
  arg.lob_table_id_ = TEST_TABLE_ID;
  arg.schema_version_ = 1;
  arg.task_id_ = 1;
  arg.source_tablet_id_ = TEST_TABLET_ID;
  // ASSERT_EQ(OB_SUCCESS, arg.dest_tablets_id_.push_back(TEST_TABLET_ID));
  // ASSERT_EQ(OB_SUCCESS, arg.dest_tablets_id_.push_back(TEST_TABLET_ID));
  arg.compaction_scn_ = 1;
  arg.data_format_version_ = 1;
  arg.consumer_group_id_ = 1;
  arg.can_reuse_macro_block_ = true;
  arg.split_sstable_type_ = ObSplitSSTableType::SPLIT_BOTH;
  // ASSERT_EQ(OB_SUCCESS, arg.lob_col_idxs_.push_back(0));
  // ASSERT_EQ(OB_SUCCESS, arg.lob_col_idxs_.push_back(1));
  // ASSERT_EQ(OB_SUCCESS, arg.lob_col_idxs_.push_back(2));
  ObDatumRowkey tmp_datum_rowkey;
  ObStorageDatum datums[OB_INNER_MAX_ROWKEY_COLUMN_NUMBER];
  tmp_datum_rowkey.assign(datums, OB_INNER_MAX_ROWKEY_COLUMN_NUMBER);
  if (OB_FAIL(arg.parallel_datum_rowkey_list_.prepare_allocate(TEST_CONCURRENT_CNT + 1))) {
    LOG_WARN("prepare allocate failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < TEST_CONCURRENT_CNT + 1; i++) {
    if (OB_SUCC(ret) && i < TEST_LOB_COL_CNT) {
      if (OB_FAIL(arg.lob_col_idxs_.push_back(i))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && i < TEST_SPLIT_DST_CNT) {
      if (OB_FAIL(arg.dest_tablets_id_.push_back(TEST_TABLET_ID))) {
        LOG_WARN("push back failed", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (i == 0) {
      ObDatumRowkey tmp_min_key;
      tmp_min_key.set_min_rowkey();
      if (OB_FAIL(tmp_min_key.deep_copy(arg.parallel_datum_rowkey_list_[i], arg.rowkey_allocator_))) {
        LOG_WARN("failed to push min rowkey", K(ret));
      }
    } else if (i == TEST_CONCURRENT_CNT) {
      ObDatumRowkey tmp_max_key;
      tmp_max_key.set_max_rowkey();
      if (OB_FAIL(tmp_max_key.deep_copy(arg.parallel_datum_rowkey_list_[i], arg.rowkey_allocator_))) {
        LOG_WARN("failed to push min rowkey", K(ret));
      }
    } else {
      tmp_datum_rowkey.datums_[0].set_string("aaaaa");
      tmp_datum_rowkey.datums_[1].set_int(i);
      tmp_datum_rowkey.datum_cnt_ = 2;
      if (OB_FAIL(tmp_datum_rowkey.deep_copy(arg.parallel_datum_rowkey_list_[i] /*dst*/, arg.rowkey_allocator_))) {
        LOG_WARN("failed to deep copy datum rowkey", K(ret), K(i), K(tmp_datum_rowkey));
      }
    }
  }
  return ret;
}

TEST_F(TestDataSplit, test_single_replica_request_arg_serialize)
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "TestDataSplit::ObDDLBuildSingleReplicaRequestArg");
  int64_t pos = 0;
  int64_t write_pos = 0;
  const int64_t buf_len = 1 * 1024 * 1024;
  char serialize_buf[buf_len];
  memset(serialize_buf, 0, sizeof(serialize_buf));
  // without split info, scenarios like drop column.
  obrpc::ObDDLBuildSingleReplicaRequestArg drop_column_arg;
  ret = prepare_mock_start_arg(false, drop_column_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = drop_column_arg.serialize(serialize_buf, buf_len, write_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(drop_column_arg.get_serialize_size(), write_pos);
  obrpc::ObDDLBuildSingleReplicaRequestArg deserialize_drop_column_arg;
  ret = deserialize_drop_column_arg.deserialize(serialize_buf, write_pos, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(pos, write_pos);

  // with split info, scenarios like tablet split.
  pos = write_pos = 0;
  memset(serialize_buf, 0, sizeof(serialize_buf));
  obrpc::ObDDLBuildSingleReplicaRequestArg split_arg;
  ret = prepare_mock_start_arg(true, split_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = split_arg.serialize(serialize_buf, buf_len, write_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(split_arg.get_serialize_size(), write_pos);
  obrpc::ObDDLBuildSingleReplicaRequestArg deserialize_split_arg;
  ret = deserialize_split_arg.deserialize(serialize_buf, write_pos, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(pos, write_pos);
  ASSERT_EQ(TEST_CONCURRENT_CNT + 1, deserialize_split_arg.parallel_datum_rowkey_list_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < TEST_LOB_COL_CNT; i++) {
    ASSERT_TRUE(deserialize_split_arg.lob_col_idxs_[i] == i);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < TEST_CONCURRENT_CNT + 1; i++) {
    if (i == 0) {
      ASSERT_TRUE(split_arg.parallel_datum_rowkey_list_[i].is_min_rowkey());
      ASSERT_TRUE(deserialize_split_arg.parallel_datum_rowkey_list_[i].is_min_rowkey());
    } else if (i == TEST_CONCURRENT_CNT) {
      ASSERT_TRUE(split_arg.parallel_datum_rowkey_list_[i].is_max_rowkey());
      ASSERT_TRUE(deserialize_split_arg.parallel_datum_rowkey_list_[i].is_max_rowkey());
    } else {
      ASSERT_TRUE(split_arg.parallel_datum_rowkey_list_[i].datums_[0] == deserialize_split_arg.parallel_datum_rowkey_list_[i].datums_[0]);
      ASSERT_TRUE(i == deserialize_split_arg.parallel_datum_rowkey_list_[i].datums_[1].get_int());
    }
  }
}

TEST_F(TestDataSplit, test_split_finish_arg_serialize)
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "TestDataSplit::test_split_finish_arg_serialize");
  int64_t pos = 0;
  int64_t write_pos = 0;
  const int64_t buf_len = 1 * 1024 * 1024;
  char serialize_buf[buf_len];
  memset(serialize_buf, 0, sizeof(serialize_buf));
  obrpc::ObTabletSplitArg split_arg;
  ret = prepare_mock_finish_arg(split_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = split_arg.serialize(serialize_buf, buf_len, write_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(split_arg.get_serialize_size(), write_pos);

  obrpc::ObTabletSplitArg deserialize_split_arg;
  ret = deserialize_split_arg.deserialize(serialize_buf, write_pos, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(pos, write_pos);
  ASSERT_EQ(TEST_CONCURRENT_CNT + 1, deserialize_split_arg.parallel_datum_rowkey_list_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < TEST_LOB_COL_CNT; i++) {
    ASSERT_TRUE(deserialize_split_arg.lob_col_idxs_[i] == i);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < TEST_CONCURRENT_CNT - 1; i++) {
    if (i == 0) {
      ASSERT_TRUE(split_arg.parallel_datum_rowkey_list_[i].is_min_rowkey());
      ASSERT_TRUE(deserialize_split_arg.parallel_datum_rowkey_list_[i].is_min_rowkey());
    } else if (i == TEST_CONCURRENT_CNT) {
      ASSERT_TRUE(split_arg.parallel_datum_rowkey_list_[i].is_max_rowkey());
      ASSERT_TRUE(deserialize_split_arg.parallel_datum_rowkey_list_[i].is_max_rowkey());
    } else {
      ASSERT_TRUE(split_arg.parallel_datum_rowkey_list_[i].datums_[0] == deserialize_split_arg.parallel_datum_rowkey_list_[i].datums_[0]);
      ASSERT_TRUE(i == deserialize_split_arg.parallel_datum_rowkey_list_[i].datums_[1].get_int());
    }
  }
}

TEST_F(TestDataSplit, test_convert_rowkey_to_range)
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "TestDataSplit::test_convert_rowkey_to_range");
  ObStorageDatum cmp_datum;
  cmp_datum.set_string("aaaaa");
  ObArray<ObDatumRange> datum_ranges_array;
  ObArenaAllocator tmp_arena("SplitCnvRange");
  obrpc::ObDDLBuildSingleReplicaRequestArg split_arg;
  ret = prepare_mock_start_arg(true, split_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTabletSplitUtil::convert_rowkey_to_range(tmp_arena, split_arg.parallel_datum_rowkey_list_, datum_ranges_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(TEST_CONCURRENT_CNT, datum_ranges_array.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < TEST_CONCURRENT_CNT; i++) {
    if (i == 0) {
      ASSERT_TRUE(datum_ranges_array.at(i).start_key_.is_min_rowkey());
    } else {
      const ObDatumRowkey &last_end_key = datum_ranges_array.at(i - 1).end_key_;
      const ObDatumRowkey &cur_start_key = datum_ranges_array.at(i).start_key_;
      const ObDatumRowkey &cur_end_key = datum_ranges_array.at(i).end_key_;
      ASSERT_TRUE(last_end_key == cur_start_key);
      ASSERT_TRUE(cmp_datum == cur_start_key.datums_[0]);
      ASSERT_TRUE(i == cur_start_key.datums_[1].get_int());
      if (TEST_CONCURRENT_CNT - 1 == i) { // the last datum range.
        ASSERT_TRUE(cur_end_key.is_max_rowkey());
      }
    }
  }
  STORAGE_LOG(INFO, "TestDataSplit::test_convert_rowkey_to_range", K(ret), "parallel_datum_rowkey_list", split_arg.parallel_datum_rowkey_list_,
      K(datum_ranges_array));
}

} //unittest
} //oceanbase


int main(int argc, char **argv)
{
  system("rm -rf test_tablet_data_split.log*");
  OB_LOGGER.set_file_name("test_tablet_data_split.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_tablet_data_split");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
