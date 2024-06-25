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
#include "storage/multi_data_source/mds_table_impl.h"
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
#include "storage/tablet/ob_mds_schema_helper.h"
namespace oceanbase {
namespace storage {
namespace mds {
void *DefaultAllocator::alloc(const int64_t size) {
  void *ptr = std::malloc(size);// ob_malloc(size, "MDS");
  ATOMIC_INC(&alloc_times_);
  MDS_LOG(DEBUG, "alloc obj", KP(ptr), K(size), K(lbt()));
  return ptr;
}
void DefaultAllocator::free(void *ptr) {
  ATOMIC_INC(&free_times_);
  MDS_LOG(DEBUG, "free obj", KP(ptr), K(lbt()));
  std::free(ptr);// ob_free(ptr);
}
void *MdsAllocator::alloc(const int64_t size) {
  void *ptr = std::malloc(size);// ob_malloc(size, "MDS");
  ATOMIC_INC(&alloc_times_);
  MDS_LOG(DEBUG, "alloc obj", KP(ptr), K(size), K(lbt()));
  return ptr;
}
void MdsAllocator::free(void *ptr) {
  ATOMIC_INC(&free_times_);
  MDS_LOG(DEBUG, "free obj", KP(ptr), K(lbt()));
  std::free(ptr);// ob_free(ptr);
}
}}}
namespace oceanbase {
namespace unittest {

using namespace common;
using namespace std;
using namespace storage;
using namespace mds;

struct OldMdsDumpNode
{
public:
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
public:
  static constexpr int64_t UNIS_VERSION = 1;

  uint8_t mds_table_id_;
  uint8_t mds_unit_id_;
  uint32_t crc_check_number_;
  MdsNodeStatus status_;
  ObIAllocator *allocator_;// to release serialized buffer
  int64_t writer_id_; // mostly is tx id
  int64_t seq_no_;// if one writer write multi nodes in one same row, seq_no is used to order those writes
  share::SCN redo_scn_; // log scn of redo
  share::SCN end_scn_; // log scn of commit/abort
  share::SCN trans_version_; // read as prepare version if phase is not COMMIT, or read as commit version
  common::ObString user_data_;// different user data type serialized result(may be very large, but should be less than 1.5MB)
};

int OldMdsDumpNode::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_ENCODE,
              UNIS_VERSION,
              mds_table_id_,
              mds_unit_id_,
              writer_id_,
              seq_no_,
              redo_scn_,
              end_scn_,
              trans_version_,
              status_,
              crc_check_number_,
              user_data_);

  return ret;
}

int OldMdsDumpNode::deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  ObString user_data;

  LST_DO_CODE(OB_UNIS_DECODE,
              version,
              mds_table_id_,
              mds_unit_id_,
              writer_id_,
              seq_no_,
              redo_scn_,
              end_scn_,
              trans_version_,
              status_,
              crc_check_number_,
              user_data);

  if (OB_SUCC(ret)) {
    allocator_ = &allocator;
    const int64_t len = user_data.length();
    char *buffer = nullptr;
    if (0 == len) {
    } else if (OB_ISNULL(buffer = static_cast<char*>(allocator_->alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      MDS_LOG(WARN, "failed to allocate memory", K(ret), K(len));
    } else {
      MEMCPY(buffer, user_data.ptr(), len);
      user_data_.assign(buffer, len);
    }
  }

  return ret;
}

int64_t OldMdsDumpNode::get_serialize_size() const
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              UNIS_VERSION,
              mds_table_id_,
              mds_unit_id_,
              writer_id_,
              seq_no_,
              redo_scn_,
              end_scn_,
              trans_version_,
              status_,
              crc_check_number_,
              user_data_);

  return len;
}

class TestMdsDumpKV: public ::testing::Test
{
public:
  TestMdsDumpKV() { ObMdsSchemaHelper::get_instance().init(); }
  virtual ~TestMdsDumpKV() {}
  virtual void SetUp() {
  }
  virtual void TearDown() {
  }
  static void test_dump_node_convert_and_serialize_and_compare();
  static void test_dump_dummy_key();
  static void test_dump_kv_wrapper();
  static void test_dump_node_deserialize();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestMdsDumpKV);
};

void TestMdsDumpKV::test_dump_node_convert_and_serialize_and_compare()
{
  ExampleUserData2 first_data;
  ASSERT_EQ(OB_SUCCESS, first_data.assign(MdsAllocator::get_instance(), "123"));
  UserMdsNode<DummyKey, ExampleUserData2> user_mds_node(nullptr, MdsNodeType::SET, WriterType::TRANSACTION, 1, transaction::ObTxSEQ::mk_v0(100));
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

void TestMdsDumpKV::test_dump_kv_wrapper()
{
  MdsDumpKV dump_kv;
  // construct dump key
  DummyKey key;
  dump_kv.k_.init(GET_MDS_TABLE_ID<UnitTestMdsTable>::value, GET_MDS_UNIT_ID<UnitTestMdsTable, DummyKey, ExampleUserData2>::value, key, MdsAllocator::get_instance());

  // construct dump node
  ExampleUserData2 first_data;
  ASSERT_EQ(OB_SUCCESS, first_data.assign(MdsAllocator::get_instance(), "123"));
  UserMdsNode<DummyKey, ExampleUserData2> user_mds_node(nullptr, MdsNodeType::SET, WriterType::TRANSACTION, 1, transaction::ObTxSEQ::mk_v0(1));
  ASSERT_EQ(OB_SUCCESS, meta::move_or_copy_or_assign(first_data, user_mds_node.user_data_, MdsAllocator::get_instance()));
  ASSERT_EQ(true, first_data.data_.ptr() != user_mds_node.user_data_.data_.ptr());
  ASSERT_EQ(0, memcmp(first_data.data_.ptr(), user_mds_node.user_data_.data_.ptr(), first_data.data_.length()));
  user_mds_node.try_on_redo(mock_scn(10));
  user_mds_node.try_before_prepare();
  user_mds_node.try_on_prepare(mock_scn(11));
  user_mds_node.try_on_commit(mock_scn(11), mock_scn(11));
  ASSERT_EQ(OB_SUCCESS, dump_kv.v_.init(GET_MDS_TABLE_ID<UnitTestMdsTable>::value, GET_MDS_UNIT_ID<UnitTestMdsTable, DummyKey, ExampleUserData2>::value, user_mds_node, mds::MdsAllocator::get_instance()));

  // convert dump kv to adapter
  MdsDumpKVStorageAdapter adapter(dump_kv);

  // convert from adapter
  MdsDumpKV dump_kv2;
  ASSERT_EQ(OB_SUCCESS, dump_kv2.convert_from_adapter(MdsAllocator::get_instance(), adapter));

  // convert adapter to row;
  ObArenaAllocator allocator;
  blocksstable::ObDatumRow mds_row;
  ASSERT_EQ(OB_SUCCESS, mds_row.init(ObMdsSchemaHelper::MDS_MULTI_VERSION_ROW_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, adapter.convert_to_mds_row(allocator, mds_row));

  // TODO(@luhaopeng.lhp): open this case later
  /*
  // convert adapter from row;
  MdsDumpKVStorageAdapter adapter2;
  // mds row should come from sstable, which does not offer multi version row(trans version+sql no)
  ASSERT_EQ(OB_SUCCESS, adapter2.convert_from_mds_row(mds_row));


  ASSERT_EQ(adapter.type_, adapter2.type_);
  ASSERT_EQ(0, adapter.key_.compare(adapter2.key_));
  ASSERT_EQ(adapter.meta_info_.trans_version_.val_, adapter2.meta_info_.trans_version_.val_);
  ASSERT_EQ(adapter.meta_info_.seq_no_, adapter2.meta_info_.seq_no_);
  ASSERT_EQ(adapter.meta_info_.mds_table_id_, adapter2.meta_info_.mds_table_id_);
  ASSERT_EQ(adapter.meta_info_.key_crc_check_number_, adapter2.meta_info_.key_crc_check_number_);
  ASSERT_EQ(adapter.meta_info_.data_crc_check_number_, adapter2.meta_info_.data_crc_check_number_);
  ASSERT_EQ(adapter.meta_info_.status_.union_.value_, adapter2.meta_info_.status_.union_.value_);
  ASSERT_EQ(adapter.meta_info_.writer_id_, adapter2.meta_info_.writer_id_);
  ASSERT_EQ(adapter.meta_info_.redo_scn_.val_, adapter2.meta_info_.redo_scn_.val_);
  ASSERT_EQ(adapter.meta_info_.end_scn_.val_, adapter2.meta_info_.end_scn_.val_);
  ASSERT_EQ(0, adapter.user_data_.compare(adapter2.user_data_));
  */
}

void TestMdsDumpKV::test_dump_node_deserialize()
{
  int ret = OB_SUCCESS;

  ObTabletCreateDeleteMdsUserData user_data;
  user_data.tablet_status_ = ObTabletStatus::NORMAL;
  user_data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;
  user_data.create_commit_scn_ = share::SCN::plus(share::SCN::min_scn(), 10);
  user_data.create_commit_version_ = 10;

  ObArenaAllocator allocator;
  int64_t size = user_data.get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);

  int64_t pos = 0;
  ret = user_data.serialize(buf, size, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  OldMdsDumpNode old_dump_node;
  old_dump_node.mds_table_id_ = 1;
  old_dump_node.mds_unit_id_ = 1;
  old_dump_node.crc_check_number_ = 0xabcd;
  old_dump_node.status_.union_.field_.node_type_ = MdsNodeType::SET;
  old_dump_node.status_.union_.field_.writer_type_ = WriterType::TRANSACTION;
  old_dump_node.status_.union_.field_.state_ = TwoPhaseCommitState::ON_COMMIT;
  old_dump_node.writer_id_ = 123;
  old_dump_node.seq_no_ = 0;
  old_dump_node.redo_scn_ = share::SCN::plus(share::SCN::min_scn(), 5);
  old_dump_node.end_scn_ = share::SCN::plus(share::SCN::min_scn(), 10);
  old_dump_node.trans_version_ = share::SCN::plus(share::SCN::min_scn(), 10);
  old_dump_node.user_data_.assign(buf, size);

  size = old_dump_node.get_serialize_size();
  pos = 0;
  buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);
  ret = old_dump_node.serialize(buf, size, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  // compat
  MdsDumpNode dump_node;
  pos = 0;
  ret = dump_node.deserialize(allocator, buf, size, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(transaction::ObTxSEQ::MIN_VAL(), dump_node.seq_no_);
}

TEST_F(TestMdsDumpKV, test_convert_and_serialize_and_compare) { TestMdsDumpKV::test_dump_node_convert_and_serialize_and_compare(); }
TEST_F(TestMdsDumpKV, test_dump_dummy_key) { TestMdsDumpKV::test_dump_dummy_key(); }
TEST_F(TestMdsDumpKV, test_dump_kv_wrapper) { TestMdsDumpKV::test_dump_kv_wrapper(); }
TEST_F(TestMdsDumpKV, test_dump_node_deserialize) { TestMdsDumpKV::test_dump_node_deserialize(); }

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
