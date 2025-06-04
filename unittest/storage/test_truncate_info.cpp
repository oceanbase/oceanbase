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
#define USING_LOG_PREFIX COMMON
#include <gtest/gtest.h>
#define protected public
#define private public
#include "storage/truncate_info/ob_truncate_info.h"
#include "storage/ob_truncate_info_helper.h"
#include "storage/compaction/ob_mds_filter_info.h"
#include "lib/allocator/ob_fifo_allocator.h"
namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace compaction;
using namespace share::schema;
namespace unittest
{
class TestTruncateInfo : public ::testing::Test
{
public:
  TestTruncateInfo() {}
  virtual ~TestTruncateInfo() {}
  ObArenaAllocator allocator_;
};

TEST_F(TestTruncateInfo, serialize_truncate_range_part)
{
  ObTruncatePartition part;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, 100, 200, part));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 1, part));
  const int64_t BUF_LEN = 10 * 1024;
  ObArenaAllocator str_alloctor;
  char *buf = (char *)str_alloctor.alloc(sizeof(char) * BUF_LEN);
  ASSERT_TRUE(nullptr != buf);

  int64_t write_pos = 0;
  ASSERT_TRUE(part.is_valid());
  ASSERT_EQ(OB_SUCCESS, part.serialize(buf, BUF_LEN, write_pos));
  ASSERT_EQ(write_pos, part.get_serialize_size());

  ObTruncatePartition tmp_part;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_part.deserialize(allocator_, buf, write_pos, read_pos));
  ASSERT_EQ(read_pos, write_pos);
  ASSERT_TRUE(tmp_part.is_valid());

  MEMSET(buf, '\0', sizeof(char) * BUF_LEN);
  bool equal = false;
  ASSERT_EQ(OB_SUCCESS, part.compare(tmp_part, equal));
  ASSERT_TRUE(equal);

  ObTruncatePartition other;
  ASSERT_EQ(OB_SUCCESS, other.assign(str_alloctor, tmp_part));
  allocator_.reset();
  ASSERT_EQ(OB_SUCCESS, other.compare(tmp_part, equal));
  ASSERT_TRUE(equal);

  part.destroy(allocator_);
  tmp_part.destroy(allocator_);
  other.destroy(str_alloctor);
}

TEST_F(TestTruncateInfo, serialize_truncate_list_part)
{
  // mock list value part
  ObTruncatePartition part;
  const int64_t list_val_cnt = 3;
  int64_t list_vals[] = {200, 300, 100};
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, list_vals, list_val_cnt, part));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 1, part));

  const int64_t BUF_LEN = 10 * 1024;
  ObArenaAllocator str_alloctor;
  char *buf = (char *)str_alloctor.alloc(sizeof(char) * BUF_LEN);
  ASSERT_TRUE(nullptr != buf);

  int64_t write_pos = 0;
  ASSERT_TRUE(part.is_valid());
  ASSERT_EQ(OB_SUCCESS, part.serialize(buf, BUF_LEN, write_pos));
  ASSERT_EQ(write_pos, part.get_serialize_size());
  LOG_INFO("part after serialize", K(part));

  ObTruncatePartition tmp_part;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_part.deserialize(allocator_, buf, write_pos, read_pos));
  ASSERT_EQ(read_pos, write_pos);
  ASSERT_TRUE(tmp_part.is_valid());

  MEMSET(buf, '\0', sizeof(char) * BUF_LEN);
  bool equal = false;
  LOG_INFO("part after deserialize", K(tmp_part));
  ASSERT_EQ(OB_SUCCESS, part.compare(tmp_part, equal));
  ASSERT_TRUE(equal);

  ObTruncatePartition other;
  ASSERT_EQ(OB_SUCCESS, other.assign(str_alloctor, tmp_part));
  ASSERT_EQ(OB_SUCCESS, other.compare(tmp_part, equal));
  ASSERT_TRUE(equal);
  tmp_part.destroy(allocator_);
  part.destroy(allocator_);
  other.destroy(str_alloctor);
  allocator_.reuse();
}

TEST_F(TestTruncateInfo, part_and_subpart)
{
  ObTruncateInfo truncate_info;
  const int64_t list_val_cnt = 3;
  int64_t list_vals[] = {200, 300, 100};
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, list_vals, list_val_cnt, truncate_info.truncate_part_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 1, truncate_info.truncate_part_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, 100, 200, truncate_info.truncate_subpart_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 2, truncate_info.truncate_subpart_));

  TruncateInfoHelper::mock_truncate_info(allocator_, 1, 100, truncate_info);
  truncate_info.is_sub_part_ = true;
  ASSERT_TRUE(truncate_info.is_valid());

  const int64_t BUF_LEN = 10 * 1024;
  ObArenaAllocator str_alloctor;
  char *buf = (char *)str_alloctor.alloc(sizeof(char) * BUF_LEN);
  ASSERT_TRUE(nullptr != buf);

  int64_t write_pos = 0;
  ASSERT_EQ(OB_SUCCESS, truncate_info.serialize(buf, BUF_LEN, write_pos));
  ASSERT_EQ(write_pos, truncate_info.get_serialize_size());

  ObTruncateInfo tmp_truncate_info;
  int64_t read_pos = 0;
  tmp_truncate_info.deserialize(allocator_, buf, write_pos, read_pos);
  ASSERT_EQ(read_pos, write_pos);
  ASSERT_TRUE(tmp_truncate_info.is_valid());

  bool equal = false;
  ASSERT_EQ(OB_SUCCESS, tmp_truncate_info.compare(truncate_info, equal));
  ASSERT_TRUE(equal);

  ObTruncateInfo truncate_info2;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, list_vals, list_val_cnt, truncate_info2.truncate_part_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 1, truncate_info2.truncate_part_));
  TruncateInfoHelper::mock_truncate_info(allocator_, 2, 200, truncate_info2);

  ASSERT_EQ(OB_SUCCESS, truncate_info2.compare(truncate_info, equal));
  ASSERT_FALSE(equal);
  ASSERT_EQ(OB_SUCCESS, truncate_info2.compare_truncate_part_info(truncate_info, equal));
  ASSERT_FALSE(equal);

  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, 100, 200, truncate_info2.truncate_subpart_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 2, truncate_info2.truncate_part_));
  truncate_info2.is_sub_part_ = true;
  ASSERT_EQ(OB_SUCCESS, truncate_info2.compare(truncate_info, equal));
  ASSERT_FALSE(equal);
  ASSERT_EQ(OB_SUCCESS, truncate_info2.compare_truncate_part_info(truncate_info, equal));
  ASSERT_TRUE(equal);
  truncate_info.destroy();
  truncate_info2.destroy();
}

TEST_F(TestTruncateInfo, mock_invalid_truncate_info)
{
  ObTruncatePartition truncate_part;
  const int64_t list_val_cnt = 3;
  int64_t list_vals[] = {200, 300, 100};
  int64_t part_key_idxs[] = {1, 2};
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, list_vals, list_val_cnt, truncate_part));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 2, part_key_idxs, truncate_part));
  // part ket cnt != list_row_cell_cnt
  ASSERT_FALSE(truncate_part.is_valid());

  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, 100, 200, truncate_part));
  // part ket cnt != rowkey_cell_cnt
  ASSERT_FALSE(truncate_part.is_valid());

  ObObj range_begin_obj[2], range_end_obj[2];
  ObRowkey range_begin_rowkey(range_begin_obj, 2);
  ObRowkey range_end_rowkey(range_end_obj, 2);
  range_begin_obj[0].set_int(100);
  range_begin_obj[1].set_int(100);
  range_end_obj[0].set_int(200);
  range_end_obj[1].set_int(200);
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, range_begin_rowkey, range_end_rowkey, truncate_part));
  ASSERT_TRUE(truncate_part.is_valid());
  truncate_part.destroy(allocator_);
}

TEST_F(TestTruncateInfo, test_memleak)
{
  ObStorageListRowValues src_values;
  ObStorageListRowValues dst_values;
  lib::ObMemAttr attr(1, "TestListRowVal");
  ObFIFOAllocator tmp_allocator;
  ObFIFOAllocator tmp_allocator2;
  ASSERT_EQ(OB_SUCCESS, tmp_allocator.init(lib::ObMallocAllocator::get_instance(), OB_MALLOC_BIG_BLOCK_SIZE, attr));
  ASSERT_EQ(OB_SUCCESS, tmp_allocator2.init(lib::ObMallocAllocator::get_instance(), OB_MALLOC_BIG_BLOCK_SIZE, attr));
  ObArray<ObNewRow> row_values;
  const int64_t count = 2;
  ObObj objs[count];
  ObNewRow new_row(objs, count);
  ObString str("test_string");
  objs[0].set_varchar(str);
  objs[1].set_varchar(str);
  ASSERT_EQ(OB_SUCCESS, row_values.push_back(new_row));
  ASSERT_EQ(tmp_allocator.used(), 0);
  ASSERT_EQ(OB_SUCCESS, src_values.init(tmp_allocator, row_values));

  const int64_t BUF_LEN = 10 * 1024;
  ObArenaAllocator str_alloctor;
  char *buf = (char *)str_alloctor.alloc(sizeof(char) * BUF_LEN);
  ASSERT_TRUE(nullptr != buf);

  int64_t write_pos = 0;
  ASSERT_EQ(OB_SUCCESS, src_values.serialize(buf, BUF_LEN, write_pos));
  ASSERT_EQ(write_pos, src_values.get_serialize_size());

  int64_t read_pos = 0;
  dst_values.deserialize(allocator_, buf, write_pos, read_pos);
  ASSERT_EQ(read_pos, write_pos);

  src_values.destroy(tmp_allocator);
  ASSERT_EQ(tmp_allocator.used(), 0);
  ASSERT_TRUE(dst_values.is_valid());
  dst_values.destroy(allocator_);

  ObTruncatePartition truncate_part;
  ObTruncatePartition truncate_part2;
  ObObj range_begin_obj[2], range_end_obj[2];
  ObRowkey range_begin_rowkey(range_begin_obj, 2);
  ObRowkey range_end_rowkey(range_end_obj, 2);
  range_begin_obj[0].set_varchar(str);
  range_begin_obj[1].set_int(100);
  range_end_obj[0].set_varchar(str);
  range_end_obj[1].set_int(200);
  int64_t part_key_idxs[] = {1, 2};
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(tmp_allocator, range_begin_rowkey, range_end_rowkey, truncate_part));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(tmp_allocator, 2, part_key_idxs, truncate_part));
  ASSERT_TRUE(truncate_part.is_valid());
  ASSERT_EQ(OB_SUCCESS, truncate_part2.assign(tmp_allocator2, truncate_part));
  bool equal = false;
  ASSERT_EQ(OB_SUCCESS, truncate_part2.compare(truncate_part, equal));
  ASSERT_TRUE(equal);

  // after destroy, allocator should be empty
  truncate_part.destroy(tmp_allocator);
  ASSERT_EQ(tmp_allocator.used(), 0);
  // assigned truncate_part should be valid
  ASSERT_TRUE(truncate_part2.is_valid());

  truncate_part2.destroy(tmp_allocator2);
  ASSERT_EQ(tmp_allocator2.used(), 0);
}

TEST_F(TestTruncateInfo, test_null_list_val)
{
  int ret = OB_SUCCESS;
  bool equal = false;
  ObTruncatePartition truncate_part;
  ObTruncatePartition truncate_part2;
  ObListRowValues tmp_values(allocator_);
  truncate_part.part_type_ = ObTruncatePartition::LIST_PART;
  truncate_part.part_op_ = ObTruncatePartition::INCLUDE;
  ObObj tmp_obj;
  ObObj tmp_obj2;
  ObNewRow tmp_row(&tmp_obj, 1);
  tmp_obj.set_null();
  tmp_obj2.set_null();
  if (OB_FAIL(tmp_values.push_back(tmp_row))) {
    COMMON_LOG(WARN, "Fail to push row", K(ret), K(tmp_row));
  } else if (OB_FAIL(truncate_part.list_row_values_.init(allocator_, tmp_values.get_values()))) {
    COMMON_LOG(WARN, "failed to init list row values", KR(ret), K(tmp_values));
  }
  int64_t part_key_idxs[] = {0};
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 1, part_key_idxs, truncate_part));

  ASSERT_EQ(OB_SUCCESS, truncate_part2.assign(allocator_, truncate_part));
  ASSERT_EQ(OB_SUCCESS, truncate_part.compare(truncate_part2, equal));
  ASSERT_TRUE(equal);
  ASSERT_EQ(0, tmp_obj.compare(tmp_obj2));

  THIS_WORKER.set_compatibility_mode(lib::Worker::CompatMode::ORACLE);
  ASSERT_EQ(OB_SUCCESS, truncate_part.compare(truncate_part2, equal));
  ASSERT_TRUE(equal);
  ASSERT_EQ(0, tmp_obj.compare(tmp_obj2));
  truncate_part.destroy(allocator_);
  truncate_part2.destroy(allocator_);
}

}//end namespace unittest
}//end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_truncate_info.log*");
  OB_LOGGER.set_file_name("test_truncate_info.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}