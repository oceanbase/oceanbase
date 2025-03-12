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
static bool MDS_FLUSHER_ALLOW_ALLOC = true;
#include <gtest/gtest.h>
#define private public
#define protected public
#include "storage/ls/ob_ls.h"
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
namespace storage
{

share::SCN MOCK_MAX_CONSEQUENT_CALLBACKED_SCN;

namespace mds
{

int MdsTableBase::merge(const int64_t construct_sequence, const share::SCN &flushing_scn)
{
  return OB_SUCCESS;
}

}
}
namespace unittest {

using namespace common;
using namespace std;
using namespace storage;
using namespace mds;
using namespace transaction;

class TestMdsTableForcellyRemove: public ::testing::Test
{
public:
  TestMdsTableForcellyRemove() { ObMdsSchemaHelper::get_instance().init(); }
  virtual ~TestMdsTableForcellyRemove() {}
  virtual void SetUp() {
  }
  virtual void TearDown() {
  }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestMdsTableForcellyRemove);
};

TEST_F(TestMdsTableForcellyRemove, reset) {
  MdsTableHandle handle;
  ASSERT_EQ(OB_SUCCESS, handle.init<UnitTestMdsTable>(MdsAllocator::get_instance(), ObTabletID(1), share::ObLSID(1), share::SCN::min_scn(), (ObTabletPointer*)0x111));
  MdsCtx ctx1(MdsWriter(transaction::ObTransID(1)));
  handle.set<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), ExampleUserData1(1), ctx1);
  ctx1.on_redo(mock_scn(10));
  ctx1.on_commit(mock_scn(20), mock_scn(20));
  MdsCtx ctx2(MdsWriter(transaction::ObTransID(2)));
  handle.set<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), ExampleUserData1(1), ctx2);
  ctx2.on_redo(mock_scn(21));
  ctx2.on_commit(mock_scn(25), mock_scn(25));
  MdsCtx ctx3(MdsWriter(transaction::ObTransID(3)));
  handle.set<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), ExampleUserData1(1), ctx3);
  ctx3.on_redo(mock_scn(29));
  ctx3.on_commit(mock_scn(40), mock_scn(40));

  ASSERT_EQ(OB_SUCCESS, handle.forcely_remove_nodes("test", share::SCN::max_scn()));
  int64_t node_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, handle.get_node_cnt(node_cnt));
  ASSERT_EQ(0, node_cnt);
  share::SCN rec_scn_after_remove;
  ASSERT_EQ(OB_SUCCESS, handle.get_rec_scn(rec_scn_after_remove));
  ASSERT_EQ(share::SCN::max_scn(), rec_scn_after_remove);

  MdsCtx ctx4(MdsWriter(transaction::ObTransID(1)));
  int ret = handle.replay<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), ExampleUserData1(1), ctx4, mock_scn(10));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, handle.get_node_cnt(node_cnt));
  ASSERT_EQ(1, node_cnt);// success
}

TEST_F(TestMdsTableForcellyRemove, remove) {
  MdsTableHandle handle;
  ASSERT_EQ(OB_SUCCESS, handle.init<UnitTestMdsTable>(MdsAllocator::get_instance(), ObTabletID(1), share::ObLSID(1), share::SCN::min_scn(), (ObTabletPointer*)0x111));
  MdsCtx ctx1(MdsWriter(transaction::ObTransID(1)));
  handle.set<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), ExampleUserData1(1), ctx1);
  ctx1.on_redo(mock_scn(10));
  ctx1.on_commit(mock_scn(20), mock_scn(20));
  MdsCtx ctx2(MdsWriter(transaction::ObTransID(2)));
  handle.set<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), ExampleUserData1(1), ctx2);
  ctx2.on_redo(mock_scn(21));
  ctx2.on_commit(mock_scn(25), mock_scn(25));
  MdsCtx ctx3(MdsWriter(transaction::ObTransID(3)));
  handle.set<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), ExampleUserData1(1), ctx3);
  ctx3.on_redo(mock_scn(29));
  ctx3.on_commit(mock_scn(40), mock_scn(40));

  ASSERT_EQ(OB_SUCCESS, handle.forcely_remove_nodes("test", mock_scn(27)));
  int64_t node_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, handle.get_node_cnt(node_cnt));
  ASSERT_EQ(1, node_cnt);
  share::SCN rec_scn_after_remove;
  ASSERT_EQ(OB_SUCCESS, handle.get_rec_scn(rec_scn_after_remove));
  ASSERT_EQ(mock_scn(29), rec_scn_after_remove);

  MdsCtx ctx4(MdsWriter(transaction::ObTransID(1)));
  int ret = handle.replay<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), ExampleUserData1(1), ctx4, mock_scn(10));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, handle.get_node_cnt(node_cnt));
  ASSERT_EQ(1, node_cnt);// failed
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_mds_forcelly_remove.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_mds_forcelly_remove.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
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
