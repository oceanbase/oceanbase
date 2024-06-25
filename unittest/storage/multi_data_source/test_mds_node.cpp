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
#include "lib/utility/utility.h"
#include <atomic>
#include <gtest/gtest.h>
#define private public
#define protected public
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"

#include <thread>
#include <iostream>
#include <vector>
#include <chrono>
#include "storage/multi_data_source/runtime_utility/mds_factory.h"
#include "common/ob_clock_generator.h"
#include "storage/multi_data_source/mds_node.h"
#include "common/meta_programming/ob_type_traits.h"
#include "storage/multi_data_source/mds_row.h"
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

class TestMdsNode: public ::testing::Test
{
public:
  TestMdsNode() { ObMdsSchemaHelper::get_instance().init(); };
  virtual ~TestMdsNode() {};
  virtual void SetUp() {
  };
  virtual void TearDown() {
  };
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestMdsNode);
};

std::atomic_int call_on_set(0);
std::atomic_int call_try_on_redo(0);
std::atomic_int call_try_on_commit(0);
std::atomic_int call_try_on_abort(0);

struct UserDataWithCallBack
{
  UserDataWithCallBack() : val_(-1) {}
  UserDataWithCallBack(int val) : val_(val) {}
  void on_set() {
    static_assert(OB_TRAIT_HAS_ON_SET(UserDataWithCallBack), "compile check on_set will not be called be MDS");
    call_on_set++;
  }
  void on_redo(share::SCN redo_scn) {
    static_assert(OB_TRAIT_HAS_ON_REDO(UserDataWithCallBack), "compile check try_on_redo will not be called be MDS");
    call_try_on_redo++;
  }
  void on_commit(share::SCN commit_version, share::SCN commit_scn) {
    static_assert(OB_TRAIT_HAS_ON_COMMIT(UserDataWithCallBack), "compile check try_on_commit will not be called be MDS");
    ob_usleep(100_ms);
    call_try_on_commit++;
  }
  void on_abort(share::SCN abort_version) {
    static_assert(OB_TRAIT_HAS_ON_ABORT(UserDataWithCallBack), "compile check try_on_abort will not be called be MDS");
    call_try_on_abort++;
  }
  TO_STRING_KV("test", val_);
  int val_;
};

TEST_F(TestMdsNode, call_user_method) {
  MdsRow<DummyKey, UserDataWithCallBack> row;
  MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(100)), transaction::ObTxSEQ::mk_v0(1));// commit finally
  ASSERT_EQ(OB_SUCCESS, row.set(UserDataWithCallBack(1), ctx, {share::ObLSID(0), 0}));
  ctx.on_redo(mock_scn(1));
  ctx.before_prepare();
  ctx.on_prepare(mock_scn(2));
  ctx.on_commit(mock_scn(3), mock_scn(3));
  ASSERT_NE(call_on_set, 0);
  ASSERT_NE(call_try_on_redo, 0);
  ASSERT_NE(call_try_on_commit, 0);
  ASSERT_EQ(call_try_on_abort, 0);
}

TEST_F(TestMdsNode, release_node_while_node_in_ctx) {
  MdsRow<DummyKey, UserDataWithCallBack> row;
  MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(100)), transaction::ObTxSEQ::mk_v0(1));// commit finally
  ASSERT_EQ(OB_SUCCESS, row.set(UserDataWithCallBack(1), ctx, {share::ObLSID(0), 0}));
  ctx.on_redo(mock_scn(1));
  ctx.before_prepare();
  row.~MdsRow();
  ASSERT_EQ(true, ctx.write_list_.empty());
  ctx.on_prepare(mock_scn(2));
  ctx.on_commit(mock_scn(3), mock_scn(3));
}

TEST_F(TestMdsNode, release_node_while_node_in_ctx_concurrent) {
  call_on_set = 0;
  call_try_on_redo = 0;
  call_try_on_commit = 0;
  call_try_on_abort = 0;
  MdsRow<DummyKey, UserDataWithCallBack> row;
  MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(100)), transaction::ObTxSEQ::mk_v0(1));// commit finally
  // 提交这些node将会耗时50ms
  ASSERT_EQ(OB_SUCCESS, row.set(UserDataWithCallBack(1), ctx, {share::ObLSID(0), 0}));
  ASSERT_EQ(OB_SUCCESS, row.set(UserDataWithCallBack(2), ctx, {share::ObLSID(0), 0}));
  ASSERT_EQ(OB_SUCCESS, row.set(UserDataWithCallBack(3), ctx, {share::ObLSID(0), 0}));
  ASSERT_EQ(OB_SUCCESS, row.set(UserDataWithCallBack(4), ctx, {share::ObLSID(0), 0}));
  ASSERT_EQ(OB_SUCCESS, row.set(UserDataWithCallBack(5), ctx, {share::ObLSID(0), 0}));

  std::thread t1([&ctx]() {
    OCCAM_LOG(DEBUG, "t1 start");
    ctx.on_redo(mock_scn(1));
    ctx.before_prepare();
    ctx.on_prepare(mock_scn(2));
    ctx.on_commit(mock_scn(3), mock_scn(3));// will cost 500ms
  });
  std::thread t2([&row]() {
    OCCAM_LOG(DEBUG, "t2 start");
    ob_usleep(250_ms);
    row.~MdsRow();
  });
  t1.join();
  t2.join();
  ASSERT_EQ(5, call_on_set);
  ASSERT_EQ(5, call_try_on_redo);
  ASSERT_GE(call_try_on_commit, 0);
  ASSERT_LE(call_try_on_commit, 5);
}

// TEST_F(TestMdsNode, test_prepare_version_with_commit) {
//   UserMdsNode<DummyKey, UserDataWithCallBack> node(nullptr, MdsNodeType::SET, WriterType::TRANSACTION, 1);
//   ASSERT_EQ(node.get_prepare_version_(), share::SCN::max_scn());
//   node.try_on_redo(mock_scn(1));
//   ASSERT_EQ(node.get_prepare_version_(), share::SCN::max_scn());
//   node.try_before_prepare();
//   ASSERT_EQ(node.get_prepare_version_(), share::SCN::min_scn());
//   node.try_on_prepare(mock_scn(2));
//   ASSERT_EQ(node.get_prepare_version_(), mock_scn(2)); // prepare version没有传下来
//   ASSERT_EQ(node.is_aborted_(), false);
//   ASSERT_EQ(node.is_committed_(), false);
//   ASSERT_EQ(node.is_decided_(), false);
//   node.try_on_commit(mock_scn(3), mock_scn(3));
//   ASSERT_EQ(node.is_aborted_(), false);
//   ASSERT_EQ(node.is_committed_(), true);
//   ASSERT_EQ(node.is_decided_(), true);
//   ASSERT_EQ(node.get_prepare_version_(), mock_scn(3));
// }

// TEST_F(TestMdsNode, test_prepare_version_with_abort) {
//   UserMdsNode<DummyKey, UserDataWithCallBack> node(nullptr, MdsNodeType::SET, WriterType::TRANSACTION, 1);
//   ASSERT_EQ(node.is_aborted_(), false);
//   ASSERT_EQ(node.is_committed_(), false);
//   ASSERT_EQ(node.is_decided_(), false);
//   node.try_on_abort(share::SCN::max_scn());
//   ASSERT_EQ(node.is_aborted_(), true);
//   ASSERT_EQ(node.is_committed_(), false);
//   ASSERT_EQ(node.is_decided_(), true);
//   ASSERT_EQ(call_try_on_abort, true);
//   ASSERT_EQ(node.get_prepare_version_(), share::SCN::max_scn());
// }

// TEST_F(TestMdsNode, test_commit_version_with_commit) {
//   UserMdsNode<DummyKey, UserDataWithCallBack> node(nullptr, MdsNodeType::SET, WriterType::TRANSACTION, 1);
//   ASSERT_EQ(node.get_commit_version_(), share::SCN::max_scn());
//   node.try_on_redo(mock_scn(1));
//   ASSERT_EQ(node.get_commit_version_(), share::SCN::max_scn());
//   node.try_before_prepare();
//   ASSERT_EQ(node.get_commit_version_(), share::SCN::max_scn());
//   node.try_on_prepare(mock_scn(2));
//   ASSERT_EQ(node.get_commit_version_(), share::SCN::max_scn());
//   node.try_on_commit(mock_scn(3), mock_scn(3));
//   ASSERT_EQ(node.get_commit_version_(), mock_scn(3));// only valid after commit
// }

// TEST_F(TestMdsNode, test_commit_version_with_abort) {
//   UserMdsNode<DummyKey, UserDataWithCallBack> node(nullptr, MdsNodeType::SET, WriterType::TRANSACTION, 1);
//   node.try_on_abort(share::SCN::max_scn());
//   ASSERT_EQ(node.get_commit_version_(), share::SCN::max_scn());
// }

TEST_F(TestMdsNode, test_node_print) {
  UserMdsNode<DummyKey, UserDataWithCallBack> node0(nullptr, MdsNodeType::SET, WriterType::TRANSACTION, 1, transaction::ObTxSEQ::mk_v0(100));
  UserMdsNode<DummyKey, UserDataWithCallBack> node1(nullptr, MdsNodeType::SET, WriterType::TRANSACTION, 1, transaction::ObTxSEQ::mk_v0(100));
}


}
}

int main(int argc, char **argv)
{
  system("rm -rf test_mds_node.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_mds_node.log", false);
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