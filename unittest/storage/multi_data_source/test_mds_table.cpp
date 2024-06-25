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
#include "storage/multi_data_source/test/example_user_data_define.h"
#define UNITTEST_DEBUG
#include "lib/utility/utility.h"
#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/ob_ls_id.h"
#include "storage/multi_data_source/mds_writer.h"
#include <thread>
#include <iostream>
#include <vector>
#include <chrono>
#include <exception>
#include "lib/ob_errno.h"
#include "share/ob_errno.h"
#include "storage/multi_data_source/adapter_define/mds_dump_node.h"
#include "lib/allocator/ob_malloc.h"
#include "storage/multi_data_source/mds_node.h"
#include "common/ob_clock_generator.h"
#include "storage/multi_data_source/mds_row.h"
#include "storage/multi_data_source/mds_unit.h"
#include "storage/multi_data_source/mds_table_handle.h"
#include "storage/multi_data_source/mds_table_handler.h"
#include "storage/tx/ob_trans_define.h"
#include <algorithm>
#include <numeric>
#include "storage/multi_data_source/runtime_utility/mds_lock.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/multi_data_source/mds_table_iterator.h"
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
using namespace transaction;

class TestMdsTable: public ::testing::Test
{
public:
  TestMdsTable() {
    ObMdsSchemaHelper::get_instance().init();
  }
  virtual ~TestMdsTable() {}
  virtual void SetUp() {
  }
  virtual void TearDown() {
  }
  static void compare_binary_key();
  static void set();
  static void replay();
  static void get_latest();
  static void get_snapshot();
  static void get_snapshot_hung_1s();
  static void get_by_writer();
  static void insert_multi_row();
  static void get_multi_row();
  // static void for_each_scan();
  static void standard_iterator();
  static void OB_iterator();
  static void test_flush();
  static void test_is_locked_by_others();
  static void test_multi_key_remove();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestMdsTable);
};

ObMdsTableHandler mds_table_hanlder;
MdsTableHandle &mds_table_ = mds_table_hanlder.mds_table_handle_;

/***********************************************Single Row*************************************************************/
struct A { ObSpinLock lock_; };
struct B { MdsLock lock_; };

#define GET_REAL_MDS_TABLE(mds_table) (*((MdsTableImpl<UnitTestMdsTable>*)(static_cast<guard::LightDataBlock<MdsTableImpl<UnitTestMdsTable>>*>((mds_table.p_mds_table_base_.ctrl_ptr_->p_data_block_))->data_)))

static constexpr int64_t MAX_KEY_SERIALIZE_SIZE = 8;
template <typename UnitKey>
inline int test_compare_binary_key(const UnitKey &lhs, const UnitKey &rhs, int &compare_result) {
  int ret = OB_SUCCESS;
  uint8_t lhs_buffer[MAX_KEY_SERIALIZE_SIZE];
  uint8_t rhs_buffer[MAX_KEY_SERIALIZE_SIZE];
  memset(lhs_buffer, 0, MAX_KEY_SERIALIZE_SIZE);
  memset(rhs_buffer, 0, MAX_KEY_SERIALIZE_SIZE);
  int64_t pos1 = 0;
  int64_t pos2 = 0;
  if (OB_FAIL(lhs.mds_serialize((char *)lhs_buffer, MAX_KEY_SERIALIZE_SIZE, pos1))) {
    MDS_LOG(WARN, "fail to serialize lhs buffer", K(lhs), K(rhs));
  } else if (OB_FAIL(rhs.mds_serialize((char *)rhs_buffer, MAX_KEY_SERIALIZE_SIZE, pos2))) {
    MDS_LOG(WARN, "fail to serialize rhs buffer", K(lhs), K(rhs));
  } else {
    int64_t max_pos = std::max(pos1, pos2);
    int64_t idx = 0;
    for (; idx < max_pos && OB_SUCC(ret); ++idx) {
      if (lhs_buffer[idx] > rhs_buffer[idx]) {
        compare_result = 1;
        break;
      } else if (lhs_buffer[idx] < rhs_buffer[idx]) {
        compare_result = -1;
        break;
      } else {
        continue;
      }
    }
    if (idx == max_pos) {
      compare_result = 0;
    }
    MDS_LOG(DEBUG, "compare binary key", K(lhs), K(rhs), K(compare_result), K(lhs_buffer), K(rhs_buffer));
  }
  return ret;
}
void TestMdsTable::compare_binary_key() {
  ExampleUserKey key1(0x1000000000000001);
  ExampleUserKey key2(0x1000000000000002);
  int comare_result = 0;
  ASSERT_EQ(OB_SUCCESS, unittest::test_compare_binary_key(key1, key2, comare_result));
  ASSERT_EQ(true, comare_result < 0);
  ASSERT_EQ(OB_SUCCESS, unittest::test_compare_binary_key(key2, key1, comare_result));
  ASSERT_EQ(true, comare_result > 0);

  int64_t pos = 0;
  char buffer[8] = {0};
  key1.mds_serialize(buffer, 8, pos);
  pos = 0;
  key1.mds_deserialize(buffer, 8, pos);
  ASSERT_EQ(key1.value_, 0x1000000000000001);
}

void TestMdsTable::set() {
  ASSERT_EQ(OB_SUCCESS, mds_table_.init<UnitTestMdsTable>(MdsAllocator::get_instance(), ObTabletID(1), share::ObLSID(1), (ObTabletPointer*)0x111));
  MDS_LOG(INFO, "test sizeof", K(sizeof(MdsTableImpl<UnitTestMdsTable>)), K(sizeof(B)), K(mds_table_.p_mds_table_base_.ctrl_ptr_->ref_));
  ExampleUserData1 data1(1);
  ExampleUserData2 data2;
  ASSERT_EQ(OB_SUCCESS, data2.assign(MdsAllocator::get_instance(), "123"));
  MdsCtx ctx1(mds::MdsWriter(ObTransID(1)));// commit finally
  DummyKey key;
  ASSERT_EQ(OB_SUCCESS, mds_table_.set(data1, ctx1));
  ASSERT_EQ(OB_SUCCESS, mds_table_.set(data2, ctx1));
  ctx1.on_redo(mock_scn(10));
  ctx1.before_prepare();
  ctx1.on_prepare(mock_scn(10));
  ctx1.on_commit(mock_scn(10), mock_scn(10));
  ExampleUserData1 data3(3);
  MdsCtx ctx2(mds::MdsWriter(ObTransID(2)));// abort by RAII finally
  ASSERT_EQ(OB_SUCCESS, mds_table_.set(data1, ctx2));
  ctx2.on_abort(mock_scn(8));
}
// <DummyKey, ExampleUserData1>: (data:1, writer:1, ver:10)
// <DummyKey, ExampleUserData2>: (data:2, writer:1, ver:10)

void TestMdsTable::replay() {
  ExampleUserData1 data1(3);
  MdsCtx ctx1(mds::MdsWriter(ObTransID(1)));// commit finally
  ASSERT_EQ(OB_SUCCESS, mds_table_.replay(data1, ctx1, mock_scn(9)));
  // ctx1.on_redo(mock_scn(9));// insert to tail on unit ExampleUserData1
  ctx1.before_prepare();
  ctx1.on_prepare(mock_scn(9));
  ctx1.on_commit(mock_scn(9), mock_scn(9));
  auto &unit = GET_REAL_MDS_TABLE(mds_table_).unit_tuple_.element<MdsUnit<DummyKey, ExampleUserData1>>();
  MDS_LOG(INFO, "xuwang test", K(unit.single_row_));

  share::SCN recorde_scn = mock_scn(0);
  unit.single_row_.v_.sorted_list_.for_each_node_from_tail_to_head_until_true([&](const UserMdsNode<DummyKey, ExampleUserData1> &node) -> int {
    if (!node.is_aborted_()) {
      MDS_ASSERT(node.redo_scn_ > recorde_scn);
      recorde_scn = node.redo_scn_;
    }
    return OB_SUCCESS;
  });
}
// <DummyKey, ExampleUserData1>: (data:1, writer:1, ver:10) -> (data:3, writer:1, ver:9)
// <DummyKey, ExampleUserData2>: (data:2, writer:1, ver:10)

void TestMdsTable::get_latest()
{
  ExampleUserData1 data1(5);
  MdsCtx ctx1(mds::MdsWriter(ObTransID(1)));// abort finally
  ASSERT_EQ(OB_SUCCESS, mds_table_.set(data1, ctx1));
  int value = 0;
  bool unused_committed_flag = false;
  ASSERT_EQ(OB_SUCCESS, mds_table_.get_latest<ExampleUserData1>([&value](const ExampleUserData1 &data) {
    value = data.value_;
    return OB_SUCCESS;
  }, unused_committed_flag));
  ASSERT_EQ(5, value);// read uncommitted
  ctx1.on_abort(mock_scn(11));
}

void TestMdsTable::get_snapshot()
{
  int value = 0;
  ASSERT_EQ(OB_SUCCESS, mds_table_.get_snapshot<ExampleUserData1>([&value](const ExampleUserData1 &data) {
    value = data.value_;
    return OB_SUCCESS;
  }, mock_scn(9)));
  ASSERT_EQ(3, value);// read snapshot
}

void TestMdsTable::get_snapshot_hung_1s()
{
  std::thread th1([&]() {
    MdsCtx ctx(mds::MdsWriter(ObTransID(1)));// abort finally
    ExampleUserData1 data(1);
    ASSERT_EQ(OB_SUCCESS, mds_table_.set(data, ctx));
    ctx.on_redo(mock_scn(3));
    ctx.before_prepare();
    this_thread::sleep_for(chrono::milliseconds(1200));
    ctx.on_abort(mock_scn(12));
  });

  std::thread th2([&]() {
    this_thread::sleep_for(chrono::milliseconds(100));
    ExampleUserData1 data;
    int64_t start_ts = ObClockGenerator::getRealClock();
    ASSERT_EQ(OB_SUCCESS, mds_table_.get_snapshot<ExampleUserData1>([&data](const ExampleUserData1 &node_data) {
      data = node_data;
      return OB_SUCCESS;
    }, share::SCN::max_scn(), 2_s));
    ASSERT_LE(1_s, ObClockGenerator::getRealClock() - start_ts);
    ASSERT_EQ(1, data.value_);// read snapshot
  });

  th1.join();
  th2.join();
}

void TestMdsTable::get_by_writer()
{
  ExampleUserData1 data1(15);
  MdsCtx ctx1(mds::MdsWriter(ObTransID(15)));// abort finally
  ASSERT_EQ(OB_SUCCESS, mds_table_.set(data1, ctx1));
  ExampleUserData2 data2;
  ASSERT_EQ(OB_SUCCESS, data2.assign(MdsAllocator::get_instance(), "3456"));
  MdsCtx ctx2(mds::MdsWriter(ObTransID(20)));// commit finally with version 20
  ASSERT_EQ(OB_SUCCESS, mds_table_.set(data2, ctx2));
  ctx2.on_redo(mock_scn(20));
  ctx2.before_prepare();
  ctx2.on_prepare(mock_scn(20));
  ctx2.on_commit(mock_scn(20), mock_scn(20));
  int value = 0;
  ASSERT_EQ(OB_SUCCESS, mds_table_.get_by_writer<ExampleUserData1>([&value](const ExampleUserData1 &data) {
    value = data.value_;
    return OB_SUCCESS;
  }, mds::MdsWriter(ObTransID(15)), mock_scn(15)));// read self uncommitted change
  ASSERT_EQ(15, value);// read uncommitted
  ASSERT_EQ(OB_SUCCESS, mds_table_.get_by_writer<ExampleUserData2>([&value](const ExampleUserData2 &data) {
    value = data.data_.length();
    return OB_SUCCESS;
  }, mds::MdsWriter(ObTransID(15)), mock_scn(15)));// read others committed change
  ASSERT_EQ(3, value);// read last committed
  ctx1.on_abort(mock_scn(20));
}
// <DummyKey, ExampleUserData1>: (data:1, writer:1, ver:10) -> (data:3, writer:1, ver:9)
// <DummyKey, ExampleUserData2>: (data:20, writer:20, ver:20) -> (data:2, writer:1, ver:10)


/***********************************************Multi Row**************************************************************/

void TestMdsTable::insert_multi_row() {
  ExampleUserKey key(1);
  ExampleUserData1 data1(1);
  MdsCtx ctx(mds::MdsWriter(ObTransID(1)));// commit finally
  ASSERT_EQ(OB_SUCCESS, mds_table_.set(key, data1, ctx));
  ctx.on_redo(mock_scn(1));
  ctx.before_prepare();
  ctx.on_prepare(mock_scn(1));
  ctx.on_commit(mock_scn(1), mock_scn(1));

  ExampleUserKey key2(2);
  ExampleUserData1 data2(2);
  MdsCtx ctx2(mds::MdsWriter(ObTransID(2)));// commit finally
  ASSERT_EQ(OB_SUCCESS, mds_table_.set(key2, data2, ctx2));
  ASSERT_EQ(OB_SUCCESS, mds_table_.set(key, data2, ctx2));// 因为只存储单版本，所以data1读不到了
  ctx2.on_redo(mock_scn(2));
  ctx2.before_prepare();
  ctx2.on_prepare(mock_scn(2));
  ctx2.on_commit(mock_scn(2), mock_scn(2));
}
// <DummyKey, ExampleUserData1>: (data:1, writer:1, ver:10) -> (data:3, writer:1, ver:9)
// <DummyKey, ExampleUserData2>: (data:20, writer:20, ver:20) -> (data:2, writer:1, ver:10)
// <ExampleUserKey, ExampleUserData1> : <1> : (data:2, writer:2, ver:2) -> (data:1, writer:1, ver:1)
//                                      <2> : (data:2, writer:2, ver:2)

void TestMdsTable::get_multi_row() {
  ExampleUserData1 read_data;
  ExampleUserKey key(1);
  ExampleUserData1 data3(3);
  MdsCtx ctx3(mds::MdsWriter(ObTransID(3)));// abort finally
  ASSERT_EQ(OB_SUCCESS, mds_table_.set(key, data3, ctx3));
  auto read_op = [&read_data](const ExampleUserData1 &data) { read_data = data; return OB_SUCCESS; };
  int ret = mds_table_.get_snapshot<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), read_op, mock_scn(2));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, read_data.value_);
  ret = mds_table_.get_snapshot<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), read_op, mock_scn(1));// 没有转储，旧版本还是保留的
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, read_data.value_);

  bool unused_committed_flag = false;
  ret = mds_table_.get_latest<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), read_op, unused_committed_flag);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, read_data.value_);
  ctx3.on_abort(mock_scn(3));
}

struct ScanOp {
  ScanOp() : valid_count_(0), total_count_(0) {}
  int operator()(const MdsNode &node) {
    if (!node.is_aborted_())
      ++valid_count_;
    return OB_SUCCESS;
  }
  int valid_count_;
  int total_count_;
};

// void TestMdsTable::for_each_scan() {
//   ScanOp op;
//   ASSERT_EQ(OB_SUCCESS, mds_table_.for_each_scan_node(op));
//   ASSERT_EQ(op.valid_count_, 7);
// }

void TestMdsTable::standard_iterator() {
  // iter kv unit, range for
  MdsUnit<ExampleUserKey, ExampleUserData1> *p_mds_unit = nullptr;
  ASSERT_EQ(OB_SUCCESS, mds_table_.get_mds_unit(p_mds_unit));// need handle protect table life
  {
    MdsRLockGuard lg(p_mds_unit->lock_);// lock unit
    for (auto &kv_row : *p_mds_unit) {
      MdsRLockGuard lg(kv_row.v_.lock_);// lock row
      for (auto &mds_node : kv_row.v_) {
        MDS_LOG(INFO, "print iter mds node", K(kv_row.k_), K(mds_node));
      }
    }
  }
  // iter dummy key unit, reverse iter
  MdsUnit<DummyKey, ExampleUserData1> *p_mds_unit2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, mds_table_.get_mds_unit(p_mds_unit2));// need handle protect table life
  {
    int64_t cnt_committed = 0;
    // using KvRowIter = MdsUnit<DummyKey, ExampleUserData2>::iterator;
    // using NodeIter = KvRowIter::row_type::iterator;
    using KvRowIter = MdsUnit<DummyKey, ExampleUserData1>::reverse_iterator;
    using NodeIter = KvRowIter::row_type::reverse_iterator;
    MdsRLockGuard lg(p_mds_unit->lock_);// lock unit
    for (KvRowIter iter1 = p_mds_unit2->rbegin(); iter1 != p_mds_unit2->rend(); ++iter1) {// there is actually only one
      MdsRLockGuard lg(iter1->v_.lock_);// lock row
      for (NodeIter iter2 = iter1->v_.rbegin(); iter2 != iter1->v_.rend(); ++iter2) {
        if (iter2->is_committed_()) {
          cnt_committed += 1;
        }
      }
      int64_t cnt = std::count_if(iter1->v_.begin(), iter1->v_.end(), [](UserMdsNode<DummyKey, ExampleUserData1> &node){ return node.is_committed_(); });
      ASSERT_EQ(cnt_committed, cnt);
    }
  }
}

void TestMdsTable::OB_iterator() {
  ObMdsUnitRowNodeScanIterator<ExampleUserKey, ExampleUserData1> iter;
  ExampleUserKey key;
  UserMdsNode<ExampleUserKey, ExampleUserData1> *p_node = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter.init(mds_table_));
  ASSERT_EQ(OB_SUCCESS, iter.get_next(key, p_node));
  MDS_LOG(INFO, "print iter kv", K(key), K(*p_node));
  ASSERT_EQ(ExampleUserKey(1).value_ == key.value_, true);
  {
    ASSERT_EQ(ExampleUserData1(2), p_node->user_data_);
    ASSERT_EQ(true, p_node->is_committed_());
    ASSERT_EQ(mock_scn(2), p_node->get_commit_version_());
  }
  ASSERT_EQ(OB_SUCCESS, iter.get_next(key, p_node));
  MDS_LOG(INFO, "print iter kv", K(key), K(*p_node));
  ASSERT_EQ(ExampleUserKey(1).value_ == key.value_, true);
  {
    ASSERT_EQ(ExampleUserData1(1), p_node->user_data_);
    ASSERT_EQ(true, p_node->is_committed_());
    ASSERT_EQ(mock_scn(1), p_node->get_commit_version_());
  }
  ASSERT_EQ(OB_SUCCESS, iter.get_next(key, p_node));
  ASSERT_EQ(ExampleUserKey(2).value_ == key.value_, true);
  ASSERT_EQ(OB_ITER_END, iter.get_next(key, p_node));
}

// <DummyKey, ExampleUserData1>: (data:1, writer:1, ver:10) -> (data:3, writer:1, ver:9)
// <DummyKey, ExampleUserData2>: (data:20, writer:20, ver:20) -> (data:2, writer:1, ver:10)
// <ExampleUserKey, ExampleUserData1> : <1> : (data:100, writer:100, ver:19001) -> (data:2, writer:2, ver:2) -> (data:1, writer:1, ver:1)
//                                      <2> : (data:200, writer:200, ver:MAX)
void TestMdsTable::test_flush() {
  ExampleUserKey key(1);
  ExampleUserData1 data1(100);
  MdsCtx ctx(mds::MdsWriter(ObTransID(100)));// commit finally
  ASSERT_EQ(OB_SUCCESS, mds_table_.set(key, data1, ctx));
  ctx.on_redo(mock_scn(19001));
  ctx.before_prepare();
  ctx.on_prepare(mock_scn(19001));
  ctx.on_commit(mock_scn(19002), mock_scn(19002));

  ExampleUserKey key2(2);
  ExampleUserData1 data2(200);
  MdsCtx ctx2(mds::MdsWriter(ObTransID(200)));// abort finally
  ASSERT_EQ(OB_SUCCESS, mds_table_.set(key2, data2, ctx2));
  ctx2.on_redo(mock_scn(200));

  int idx = 0;
  ASSERT_EQ(OB_SUCCESS, mds_table_.flush(mock_scn(300), mock_scn(500)));// 1. 以300为版本号进行flush动作
  ASSERT_EQ(mock_scn(199), mds_table_.p_mds_table_base_->flushing_scn_);// 2. 实际上以199为版本号进行flush动作
  ASSERT_EQ(OB_SUCCESS, (mds_table_.scan_all_nodes_to_dump<ScanRowOrder::ASC, ScanNodeOrder::FROM_OLD_TO_NEW>(
    [&idx](const MdsDumpKV &kv) -> int {// 2. 转储时扫描mds table
      MDS_ASSERT(kv.v_.end_scn_ < mock_scn(199));// 扫描时看不到199版本以上的提交
      MDS_ASSERT(idx < 10);
      MDS_LOG(INFO, "print dump node kv", K(kv));
      return OB_SUCCESS;
    }, 0, true)
  ));
  mds_table_.on_flush(mock_scn(199), OB_SUCCESS);// 3. 推大rec_scn【至少】到200
  share::SCN rec_scn;
  ASSERT_EQ(OB_SUCCESS, mds_table_.get_rec_scn(rec_scn));
  MDS_LOG(INFO, "print rec scn", K(rec_scn));
  ASSERT_EQ(rec_scn, mock_scn(200));
  MdsCtx ctx3(mds::MdsWriter(ObTransID(101)));// abort finally
  ASSERT_EQ(OB_SUCCESS, mds_table_.replay(ExampleUserKey(111), ExampleUserData1(111), ctx3, mock_scn(100)));
  ASSERT_EQ(OB_SUCCESS, mds_table_.get_rec_scn(rec_scn));
  ASSERT_EQ(rec_scn, mock_scn(100));
  ctx3.on_abort(mock_scn(100));

  ScanOp op;
  // 未转储：一个已决node + 一个未决node
  MDS_LOG(INFO, "print free times", K(oceanbase::storage::mds::MdsAllocator::get_alloc_times()), K(oceanbase::storage::mds::MdsAllocator::get_free_times()));// 回收已转储的node
  ASSERT_EQ(OB_SUCCESS, mds_table_.try_recycle(mock_scn(200)));
  // ASSERT_EQ(OB_SUCCESS, mds_table_.for_each_scan_node(op));
  // ASSERT_EQ(op.valid_count_, 2);
  ctx2.on_abort(mock_scn(200));
  MDS_LOG(INFO, "print free times", K(oceanbase::storage::mds::MdsAllocator::get_free_times()));// 回收已转储的node
}
// <ExampleUserKey, ExampleUserData1> : <1> : (data:100, writer:100, ver:19001)

void TestMdsTable::test_is_locked_by_others() {
  int ret = OB_SUCCESS;
  bool is_locked = false;
  ret = mds_table_.is_locked_by_others<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), is_locked);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, is_locked);
  ExampleUserData1 data1(300);
  MdsCtx ctx(mds::MdsWriter(ObTransID(100)));// abort finally
  ASSERT_EQ(OB_SUCCESS, mds_table_.set(ExampleUserKey(1), data1, ctx));
  ret = mds_table_.is_locked_by_others<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), is_locked);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, is_locked);
  ret = mds_table_.is_locked_by_others<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), is_locked, mds::MdsWriter(ObTransID(100)));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, is_locked);
}

// <ExampleUserKey, ExampleUserData1> : <1> : (data:100, writer:100, ver:19001)
void TestMdsTable::test_multi_key_remove() {
  bool is_committed = false;
  int ret = mds_table_.get_latest<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), [](const ExampleUserData1 &data){
    return OB_SUCCESS;
  }, is_committed);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, mds_table_.flush(mock_scn(200), mock_scn(500)));
  ASSERT_EQ(OB_SUCCESS, mds_table_.try_recycle(mock_scn(200)));
  ret = mds_table_.get_latest<ExampleUserKey, ExampleUserData1>(ExampleUserKey(2), [](const ExampleUserData1 &data){
    return OB_SUCCESS;
  }, is_committed);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  MdsCtx ctx(mds::MdsWriter(ObTransID(1)));
  ret = mds_table_.remove<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mds_table_.get_latest<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), [](const ExampleUserData1 &data){
    return OB_SUCCESS;
  }, is_committed);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

TEST_F(TestMdsTable, compare_binary_key) { TestMdsTable::compare_binary_key(); }
TEST_F(TestMdsTable, set) { TestMdsTable::set(); }
TEST_F(TestMdsTable, replay) { TestMdsTable::replay(); }
TEST_F(TestMdsTable, get_latest) { TestMdsTable::get_latest(); }
TEST_F(TestMdsTable, get_snapshot) { TestMdsTable::get_snapshot(); }
TEST_F(TestMdsTable, get_snapshot_hung_1s) { TestMdsTable::get_snapshot_hung_1s(); }
TEST_F(TestMdsTable, get_by_writer) { TestMdsTable::get_by_writer(); }
TEST_F(TestMdsTable, insert_multi_row) { TestMdsTable::insert_multi_row(); }
TEST_F(TestMdsTable, get_multi_row) { TestMdsTable::get_multi_row(); }
TEST_F(TestMdsTable, test_standard_style_iterator) { TestMdsTable::standard_iterator(); }
TEST_F(TestMdsTable, test_OB_style_iterator) { TestMdsTable::OB_iterator(); }
TEST_F(TestMdsTable, test_flush) { TestMdsTable::test_flush(); }
TEST_F(TestMdsTable, test_is_locked_by_others) { TestMdsTable::test_is_locked_by_others(); }
TEST_F(TestMdsTable, test_multi_key_remove) { TestMdsTable::test_multi_key_remove(); }

TEST_F(TestMdsTable, basic_trans_example) {
  MdsTableHandle mth;
  // 1. 初始化为UnitTestMdsTable
  ASSERT_EQ(OB_SUCCESS, mth.init<UnitTestMdsTable>(mds::DefaultAllocator::get_instance(),
                                                   ObTabletID(1),
                                                   share::ObLSID(1),
                                                   nullptr));
  MdsTableHandle mth2 = mth;// 两个引用计数
  MdsCtx ctx(mds::MdsWriter(ObTransID(123)));// 创建一个写入句柄，接入多源事务，ctx由事务层创建
  // 2. 写入数据
  ASSERT_EQ(OB_SUCCESS, mth.set(ExampleUserData1(1), ctx));// 写入第一个数据单元，成功
  ASSERT_EQ(OB_OBJ_TYPE_ERROR, mth.set((int)54321, ctx));// 写入第二个数据单元，但UnitTestMdsTable并未注册该类型数据，Type ERROR
  // 3. 对写入数据写CLOG并进行两阶段提交，接入多源事务，则该流程由事务层代为执行， 用户无感知
  ctx.on_redo(mock_scn(100));
  ctx.before_prepare();
  ctx.on_prepare(mock_scn(100));
  ctx.on_commit(mock_scn(100), mock_scn(100));
  // 4. 读取最新已提交数据
  ASSERT_EQ(OB_SUCCESS, mth.get_snapshot<ExampleUserData1>([](const ExampleUserData1 &data) {
                                                             return data.value_ != 1 ? OB_ERR_UNEXPECTED : OB_SUCCESS;
                                                           }));
}// 5. 最后一个Handle析构的时候，MdsTable发生真正的析构行为

TEST_F(TestMdsTable, basic_non_trans_example) {
  MdsTableHandle mth;
  // 1. 初始化为UnitTestMdsTable
  ASSERT_EQ(OB_SUCCESS, mth.init<UnitTestMdsTable>(mds::DefaultAllocator::get_instance(),
                                                   ObTabletID(1),
                                                   share::ObLSID(1),
                                                   nullptr));
  MdsTableHandle mth2 = mth;// 两个引用计数
  MdsCtx ctx(MdsWriter(WriterType::AUTO_INC_SEQ, 1));// 创建一个写入句柄，不接入事务，自己写日志
  // 2. 写入数据
  ASSERT_EQ(OB_SUCCESS, mth.set(ExampleUserData1(1), ctx));// 写入第一个数据单元，成功
  ASSERT_EQ(OB_OBJ_TYPE_ERROR, mth.set((int)54321, ctx));// 写入第二个数据单元，但UnitTestMdsTable并未注册该类型数据，Type ERROR
  // 3. 对写入数据写CLOG并进行单条日志提交
  ctx.single_log_commit(mock_scn(100), mock_scn(100));
  // 4. 读取最新已提交数据
  ASSERT_EQ(OB_SUCCESS, mth.get_snapshot<ExampleUserData1>([](const ExampleUserData1 &data) {
                                                             return data.value_ != 1 ? OB_ERR_UNEXPECTED : OB_SUCCESS;
                                                           }));
}// 5. 最后一个Handle析构的时候，MdsTable发生真正的析构行为

TEST_F(TestMdsTable, test_recycle) {
  int64_t alloc_times = oceanbase::storage::mds::MdsAllocator::get_alloc_times();
  int64_t free_times = oceanbase::storage::mds::MdsAllocator::get_free_times();
  ASSERT_NE(alloc_times, free_times);
  ASSERT_EQ(OB_SUCCESS, mds_table_.try_recycle(mock_scn(20000)));
  int64_t valid_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, mds_table_.get_node_cnt(valid_cnt));
  ASSERT_EQ(1, valid_cnt);// 此时还有一个19001版本的已提交数据，因为rec_scn没有推上去
  ASSERT_EQ(OB_SUCCESS, mds_table_.flush(mock_scn(20000), mock_scn(40000)));
  mds_table_.scan_all_nodes_to_dump<ScanRowOrder::ASC, ScanNodeOrder::FROM_OLD_TO_NEW>([](const MdsDumpKV &){
    return OB_SUCCESS;
  }, 0, true);
  mds_table_.on_flush(mock_scn(40000), OB_SUCCESS);
  share::SCN rec_scn;
  ASSERT_EQ(OB_SUCCESS, mds_table_.get_rec_scn(rec_scn));
  MDS_LOG(INFO, "print rec scn", K(rec_scn));
  ASSERT_EQ(share::SCN::max_scn(), rec_scn);
  ASSERT_EQ(OB_SUCCESS, mds_table_.try_recycle(mock_scn(40000)));
  ASSERT_EQ(OB_SUCCESS, mds_table_.get_node_cnt(valid_cnt));
  ASSERT_EQ(0, valid_cnt);// 此时还有一个19001版本的已提交数据，因为rec_scn没有推上去
}

TEST_F(TestMdsTable, test_recalculate_flush_scn_op) {
  MdsTableHandle mds_table;
  ASSERT_EQ(OB_SUCCESS, mds_table.init<UnitTestMdsTable>(MdsAllocator::get_instance(), ObTabletID(1), share::ObLSID(1), (ObTabletPointer*)0x111));
  MdsCtx ctx1(mds::MdsWriter(ObTransID(1)));
  MdsCtx ctx2(mds::MdsWriter(ObTransID(2)));
  MdsCtx ctx3(mds::MdsWriter(ObTransID(3)));
  ASSERT_EQ(OB_SUCCESS, mds_table.set(ExampleUserData1(1), ctx1));
  ctx1.on_redo(mock_scn(1));
  ctx1.on_commit(mock_scn(3), mock_scn(3));
  ASSERT_EQ(OB_SUCCESS, mds_table.set(ExampleUserData1(2), ctx2));
  ctx2.on_redo(mock_scn(5));
  ctx2.on_commit(mock_scn(7), mock_scn(7));
  ASSERT_EQ(OB_SUCCESS, mds_table.set(ExampleUserData1(3), ctx3));
  ctx3.on_redo(mock_scn(9));
  ctx3.on_commit(mock_scn(11), mock_scn(11));
  ASSERT_EQ(OB_SUCCESS, mds_table.flush(mock_scn(4), mock_scn(4)));
  ASSERT_EQ(mock_scn(4), mds_table.p_mds_table_base_->flushing_scn_);
  mds_table.on_flush(mock_scn(4), OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, mds_table.flush(mock_scn(5), mock_scn(5)));// no need do flush, directly advance rec_scn
  ASSERT_EQ(false, mds_table.p_mds_table_base_->flushing_scn_.is_valid());
  ASSERT_EQ(OB_SUCCESS, mds_table.flush(mock_scn(6), mock_scn(6)));// no need do flush, directly advance rec_scn
  ASSERT_EQ(false, mds_table.p_mds_table_base_->flushing_scn_.is_valid());
  ASSERT_EQ(OB_SUCCESS, mds_table.flush(mock_scn(7), mock_scn(7)));
  ASSERT_EQ(mock_scn(7), mds_table.p_mds_table_base_->flushing_scn_);
  mds_table.on_flush(mock_scn(7), OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, mds_table.flush(mock_scn(8), mock_scn(8)));
  ASSERT_EQ(false, mds_table.p_mds_table_base_->flushing_scn_.is_valid());
  ASSERT_EQ(mock_scn(9), mds_table.p_mds_table_base_->rec_scn_);
}

// TEST_F(TestMdsTable, test_node_commit_in_row) {
//   MdsRow<DummyKey, ExampleUserData1> row;
//   MdsCtx ctx1(mds::MdsWriter(WriterType::AUTO_INC_SEQ, 1));
//   ASSERT_EQ(OB_SUCCESS, row.set(ExampleUserData1(1), ctx1, 0));
//   ctx1.single_log_commit(mock_scn(1), mock_scn(1));
//   MdsCtx ctx2(mds::MdsWriter(WriterType::AUTO_INC_SEQ, 2));
//   ASSERT_EQ(OB_SUCCESS, row.set(ExampleUserData1(2), ctx2, 0));
//   ctx1.single_log_commit(mock_scn(2), mock_scn(2));// won't release last committed node
//   int node_cnt = 0;
//   row.sorted_list_.for_each_node_from_head_to_tail_until_true([&node_cnt](const UserMdsNode<DummyKey, ExampleUserData1> &){ ++node_cnt; return false; });
//   ASSERT_EQ(2, node_cnt);
// }

TEST_F(TestMdsTable, test_rw_lock_rrlock) {
  MdsLock lock;
  std::thread t1([&lock]() {
    MdsRLockGuard lg(lock);
    ob_usleep(1_s);
  });
  std::thread t2([&lock]() {
    MdsRLockGuard lg(lock);
    ob_usleep(1_s);
  });
  t1.join();
  t2.join();
}

TEST_F(TestMdsTable, test_rw_lock_wrlock) {
  MdsLock lock;
  std::thread t1([&lock]() {
    MdsWLockGuard lg(lock);
    ob_usleep(1_s);
  });
  std::thread t2([&lock]() {
    ob_usleep(200_ms);
    MdsRLockGuard lg(lock);
    ob_usleep(1_s);
  });
  t1.join();
  t2.join();
}

TEST_F(TestMdsTable, test_rw_lock_rwlock) {
  MdsLock lock;
  std::thread t1([&lock]() {
    ob_usleep(200_ms);
    MdsWLockGuard lg(lock);
    ob_usleep(1_s);
  });
  std::thread t2([&lock]() {
    MdsRLockGuard lg(lock);
    ob_usleep(1_s);
  });
  t1.join();
  t2.join();
}

TEST_F(TestMdsTable, test_rw_lock_wwlock) {
  MdsLock lock;
  std::thread t1([&lock]() {
    MdsWLockGuard lg(lock);
    ob_usleep(1_s);
  });
  std::thread t2([&lock]() {
    MdsWLockGuard lg(lock);
    ob_usleep(1_s);
  });
  t1.join();
  t2.join();
}

TEST_F(TestMdsTable, test_rvalue_set) {
  ExampleUserData2 data;
  ASSERT_EQ(OB_SUCCESS, data.assign(MdsAllocator::get_instance(), "123"));
  MdsCtx ctx(mds::MdsWriter(ObTransID(100)));
  ObString str = data.data_;
  ASSERT_EQ(OB_SUCCESS, mds_table_.set(std::move(data), ctx, 0));
  bool is_committed = false;
  ASSERT_EQ(OB_SUCCESS, mds_table_.get_latest<ExampleUserData2>([&data, str](const ExampleUserData2 &read_data) -> int {
    MDS_ASSERT(data.data_ == nullptr);
    MDS_ASSERT(str.ptr() == read_data.data_.ptr());
    return OB_SUCCESS;
  }, is_committed));
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_mds_table.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_mds_table.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  oceanbase::unittest::mds_table_.~MdsTableHandle();
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
