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


#include "share/ob_ls_id.h"
#define UNITTEST_DEBUG
#include <gtest/gtest.h>
#define private public
#define protected public
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/multi_data_source/compile_utility/map_type_index_in_tuple.h"
#include "storage/multi_data_source/adapter_define/mds_dump_node.h"
#include <thread>
#include <iostream>
#include <vector>
#include <chrono>
#include "storage/multi_data_source/runtime_utility/mds_factory.h"
#include "common/ob_clock_generator.h"
#include "storage/multi_data_source/mds_node.h"
#include "storage/multi_data_source/mds_table_handle.h"
#include "storage/tablet/ob_tablet_meta.h"
namespace oceanbase {
namespace storage
{
namespace mds
{
void *MdsAllocator::alloc(const int64_t size)
{
  void *ptr = ob_malloc(size, "MDS");
  ATOMIC_INC(&alloc_times_);
  MDS_LOG(DEBUG, "alloc obj", KP(ptr), K(size), K(lbt()));
  return ptr;
}
void MdsAllocator::free(void *ptr) {
  ATOMIC_INC(&free_times_);
  MDS_LOG(DEBUG, "free obj", KP(ptr), K(lbt()));
  ob_free(ptr);
}
}
}
namespace unittest {

using namespace common;
using namespace std;
using namespace storage;
using namespace mds;

class TestMdsDumpKV: public ::testing::Test
{
public:
  TestMdsDumpKV() {};
  virtual ~TestMdsDumpKV() {};
  virtual void SetUp() {
  };
  virtual void TearDown() {
  };
  static void test_dump_node_convert_and_serialize_and_compare();
  static void test_dump_dummy_key();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestMdsDumpKV);
};

void TestMdsDumpKV::test_dump_node_convert_and_serialize_and_compare()
{
  ExampleUserData2 first_data;
  ASSERT_EQ(OB_SUCCESS, first_data.assign(MdsAllocator::get_instance(), "123"));
  UserMdsNode<DummyKey, ExampleUserData2> user_mds_node(nullptr, MdsNodeType::SET, WriterType::TRANSACTION, 1);
  ASSERT_EQ(OB_SUCCESS, meta::move_or_copy_or_assign(first_data, user_mds_node.user_data_, MdsAllocator::get_instance()));
  ASSERT_EQ(true, first_data.data_.ptr() != user_mds_node.user_data_.data_.ptr());
  ASSERT_EQ(0, memcmp(first_data.data_.ptr(), user_mds_node.user_data_.data_.ptr(), first_data.data_.length()));
  user_mds_node.try_on_redo(mock_scn(10));
  user_mds_node.try_before_prepare();
  user_mds_node.try_on_prepare(mock_scn(11));
  user_mds_node.try_on_commit(mock_scn(11), mock_scn(11));
  MdsDumpNode dump_node;
  ASSERT_EQ(OB_SUCCESS, dump_node.init(GET_MDS_TABLE_ID<NormalMdsTable>::value, GET_MDS_UNIT_ID<UnitTestMdsTable, DummyKey, ExampleUserData2>::value, user_mds_node, mds::MdsAllocator::get_instance()));
  constexpr int buf_len = 1024;
  char buffer[buf_len] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, dump_node.serialize(buffer, buf_len, pos));
  MdsDumpNode dump_node2;
  pos = 0;

  ObArenaAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, dump_node2.deserialize(allocator, buffer, buf_len, pos));
  MDS_ASSERT(dump_node2.crc_check_number_ == dump_node2.generate_hash());
  ASSERT_EQ(dump_node2.generate_hash(), dump_node2.crc_check_number_);
  ASSERT_EQ(dump_node2.crc_check_number_, dump_node.crc_check_number_);
  UserMdsNode<DummyKey, ExampleUserData2> user_mds_node2;
  ASSERT_EQ(OB_SUCCESS, dump_node2.convert_to_user_mds_node(user_mds_node2, share::ObLSID(1), ObTabletID(1)));
  ASSERT_EQ(0, memcmp(user_mds_node.user_data_.data_.ptr(), user_mds_node2.user_data_.data_.ptr(), user_mds_node.user_data_.data_.length()));
  ASSERT_EQ(user_mds_node.redo_scn_, user_mds_node2.redo_scn_);
  ASSERT_EQ(user_mds_node.end_scn_, user_mds_node2.end_scn_);
  ASSERT_EQ(user_mds_node.trans_version_, user_mds_node2.trans_version_);
  ASSERT_EQ(user_mds_node.status_.union_.value_, user_mds_node2.status_.union_.value_);
  ASSERT_EQ(user_mds_node.seq_no_, user_mds_node2.seq_no_);
}

void TestMdsDumpKV::test_dump_dummy_key()
{
  DummyKey key;
  MdsDumpKey dump_key1, dump_key2;
  dump_key1.init(0, 0, key, MdsAllocator::get_instance());
  constexpr int buf_len = 1024;
  char buffer[buf_len] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, dump_key1.serialize(buffer, buf_len, pos));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, dump_key2.deserialize(buffer, buf_len, pos));
  int compare_result = 0;
  ASSERT_EQ(OB_SUCCESS, dump_key1.compare(dump_key2, compare_result));
  ASSERT_EQ(0, compare_result);
}


TEST_F(TestMdsDumpKV, test_convert_and_serialize_and_compare) { TestMdsDumpKV::test_dump_node_convert_and_serialize_and_compare(); }
TEST_F(TestMdsDumpKV, test_dump_dummy_key) { TestMdsDumpKV::test_dump_dummy_key(); }

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_mds_dump_kv.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_mds_dump_kv.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  oceanbase::observer::ObMdsEventBuffer::init();
  int ret = RUN_ALL_TESTS();
  oceanbase::observer::ObMdsEventBuffer::destroy();
  int64_t alloc_times = oceanbase::storage::mds::MdsAllocator::get_alloc_times();
  int64_t free_times = oceanbase::storage::mds::MdsAllocator::get_free_times();
  if (alloc_times != free_times) {
    MDS_LOG(ERROR, "memory may leak", K(free_times), K(alloc_times));
    ret = -1;
  } else {
    MDS_LOG(INFO, "all memory released", K(free_times), K(alloc_times));
  }
  return ret;
}
