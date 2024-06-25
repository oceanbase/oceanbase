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
#include "lib/future/ob_future.h"
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#define UNITTEST_DEBUG
#define private public
#define protected public
#include "storage/multi_data_source/runtime_utility/common_define.h"
#include "storage/multi_data_source/mds_table_base.h"
#include "storage/multi_data_source/mds_table_mgr.h"
#include "storage/ls/ob_ls.h"
#include "storage/multi_data_source/mds_table_handle.h"
#include "storage/multi_data_source/mds_table_order_flusher.h"
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
using namespace std;

oceanbase::common::ObPromise<void> PROMISE1, PROMISE2;
vector<oceanbase::storage::mds::FlushKey> V_ActualDoFlushKey;

static std::atomic<bool> NEED_ALLOC_FAIL(false);
static std::atomic<bool> NEED_ALLOC_FAIL_AFTER_RESERVE(false);

namespace oceanbase {
namespace logservice
{
int ObLogHandler::get_max_decided_scn(share::SCN &scn)
{
  scn = unittest::mock_scn(500);
  return OB_SUCCESS;
}
}
namespace storage
{
namespace mds
{
template <>
int MdsTableImpl<UnitTestMdsTable>::flush(share::SCN need_advanced_rec_scn_lower_limit, share::SCN max_decided_scn) {
  V_ActualDoFlushKey.push_back({tablet_id_, rec_scn_});
  return OB_SUCCESS;
}
void *MdsFlusherModulePageAllocator::alloc(const int64_t size, const ObMemAttr &attr) {
  void *ret = nullptr;
  if (!NEED_ALLOC_FAIL) {
    ret = (NULL == allocator_) ? share::mtl_malloc(size, attr) : allocator_->alloc(size, attr);
  } else {
    MDS_LOG(DEBUG, "mock no memory", K(lbt()));
  }
  return ret;
}

// template <>
// void MdsTableOrderFlusher<FLUSH_FOR_ALL_SIZE, true>::reserve_memory(int64_t mds_table_total_size_likely) {// it'ok if failed
//   int ret = OB_SUCCESS;
//   abort();
//   constexpr int64_t max_tablet_number = 100 * 10000;//sizeof(100w FlushKey) = 16MB
//   int64_t reserve_size = std::min(mds_table_total_size_likely * 2, max_tablet_number);
//   if (OB_FAIL(extra_mds_tables_.reserve(reserve_size))) {
//     MDS_LOG(WARN, "fail to reserve memory", KR(ret));
//     array_err_ = ret;
//   }
//   if (NEED_ALLOC_FAIL_AFTER_RESERVE) {
//     NEED_ALLOC_FAIL = true;
//     OCCAM_LOG(INFO, "set PEOMISE1 and wait PROMISE2");
//     PROMISE1.set();
//     ObFuture<void> future = PROMISE2.get_future();
//     future.wait();
//   }
// }
}
}
namespace unittest {

using namespace common;
using namespace std;
using namespace storage;
using namespace mds;
using namespace transaction;

static constexpr int64_t TEST_ALL_SIZE = FLUSH_FOR_ALL_SIZE * 10;

class TestMdsTableFlush: public ::testing::Test
{
public:
  TestMdsTableFlush() { ObMdsSchemaHelper::get_instance().init(); }
  virtual ~TestMdsTableFlush() {}
  virtual void SetUp() { V_ActualDoFlushKey.clear(); NEED_ALLOC_FAIL = false; NEED_ALLOC_FAIL_AFTER_RESERVE = false; }
  virtual void TearDown() { }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestMdsTableFlush);
};

TEST_F(TestMdsTableFlush, flusher_for_some_order) {
  FlusherForSome some;
  for (int i = FLUSH_FOR_SOME_SIZE; i > 0; --i) {
    some.record_mds_table({ObTabletID(i), mock_scn(i)});
  }
  for (int i = 0; i < FLUSH_FOR_SOME_SIZE; ++i) {
    ASSERT_EQ(some.high_priority_flusher_.high_priority_mds_tables_[i].rec_scn_, mock_scn(i + 1));
  }
  some.record_mds_table({ObTabletID(999), mock_scn(999)});// 没影响，被扔了
  for (int i = 0; i < FLUSH_FOR_SOME_SIZE; ++i) {
    ASSERT_EQ(some.high_priority_flusher_.high_priority_mds_tables_[i].rec_scn_, mock_scn(i + 1));
  }
  some.record_mds_table({ObTabletID(0), mock_scn(0)});// 变成第一个，其余的往后挤
  for (int i = 0; i < FLUSH_FOR_SOME_SIZE; ++i) {
    ASSERT_EQ(some.high_priority_flusher_.high_priority_mds_tables_[i].rec_scn_, mock_scn(i));
  }
  some.record_mds_table({ObTabletID(999), mock_scn(FLUSH_FOR_SOME_SIZE - 2)});// 最后一个有两份
  for (int i = 0; i < FLUSH_FOR_SOME_SIZE - 1; ++i) {
    ASSERT_EQ(some.high_priority_flusher_.high_priority_mds_tables_[i].rec_scn_, mock_scn(i));
  }
  ASSERT_EQ(some.high_priority_flusher_.high_priority_mds_tables_[FLUSH_FOR_SOME_SIZE - 1].rec_scn_, mock_scn(FLUSH_FOR_SOME_SIZE - 2));
}

TEST_F(TestMdsTableFlush, flusher_for_all_order_with_enough_memory) {
  std::vector<FlushKey> v_key;
  for (int i = 0; i < TEST_ALL_SIZE; ++i) {
    v_key.push_back({ObTabletID(100 + i), mock_scn(100 + i)});
  }
  std::random_shuffle(v_key.begin(), v_key.end());
  ObLS ls;
  ObMdsTableMgr mgr;
  vector<MdsTableHandle> v;
  ASSERT_EQ(mgr.init(&ls), OB_SUCCESS);
  // 加入mgr的顺序是乱序的
  for (int i = 0; i < v_key.size(); ++i) {
    MdsTableHandle mds_table;
    ASSERT_EQ(OB_SUCCESS, mds_table.init<UnitTestMdsTable>(MdsAllocator::get_instance(),
                                                            v_key[i].tablet_id_,
                                                            share::ObLSID(1),
                                                            (ObTabletPointer*)0x111,
                                                            &mgr));
    MdsTableBase *p_mds_table = mds_table.p_mds_table_base_.data_;
    p_mds_table->rec_scn_ = v_key[i].rec_scn_;
    v.push_back(mds_table);
  }
  ASSERT_EQ(OB_SUCCESS, mgr.flush(share::SCN::max_scn(), -1));
  ASSERT_EQ(TEST_ALL_SIZE, V_ActualDoFlushKey.size());
  for (int i = 0; i < V_ActualDoFlushKey.size(); ++i) {
    if (V_ActualDoFlushKey[i].rec_scn_ != mock_scn(100 + i)) {
      MDS_LOG(INFO, "DEBUG", K(V_ActualDoFlushKey[i].rec_scn_), K(i));
    }
    ASSERT_EQ(V_ActualDoFlushKey[i].rec_scn_, mock_scn(100 + i));
    ASSERT_EQ(V_ActualDoFlushKey[i].tablet_id_, ObTabletID(100 + i));
  }
}

TEST_F(TestMdsTableFlush, flusher_for_all_order_with_limitted_memory_reserve_fail) {
  NEED_ALLOC_FAIL = true;
  std::vector<FlushKey> v_key;
  for (int i = 0; i < TEST_ALL_SIZE; ++i) {
    v_key.push_back({ObTabletID(100 + i), mock_scn(100 + i)});
  }
  std::random_shuffle(v_key.begin(), v_key.end());
  ObLS ls;
  ObMdsTableMgr mgr;
  vector<MdsTableHandle> v;
  ASSERT_EQ(mgr.init(&ls), OB_SUCCESS);
  // 加入mgr的顺序是乱序的
  for (int i = 0; i < v_key.size(); ++i) {
    MdsTableHandle mds_table;
    ASSERT_EQ(OB_SUCCESS, mds_table.init<UnitTestMdsTable>(MdsAllocator::get_instance(),
                                                            v_key[i].tablet_id_,
                                                            share::ObLSID(1),
                                                            (ObTabletPointer*)0x111,
                                                            &mgr));
    MdsTableBase *p_mds_table = mds_table.p_mds_table_base_.data_;
    p_mds_table->rec_scn_ = v_key[i].rec_scn_;
    v.push_back(mds_table);
  }
  ASSERT_EQ(OB_SUCCESS, mgr.flush(share::SCN::max_scn(), -1));
  ASSERT_EQ(TEST_ALL_SIZE + FLUSH_FOR_ALL_SIZE, V_ActualDoFlushKey.size());// 只保证最前面的TEST_ALL_SIZE的tablet是有序的，并且rec scn最小
  for (int i = 0; i < FLUSH_FOR_ALL_SIZE; ++i) {
    if (V_ActualDoFlushKey[i].rec_scn_ != mock_scn(100 + i)) {
      MDS_LOG(INFO, "DEBUG", K(V_ActualDoFlushKey[i].rec_scn_), K(i));
    }
    ASSERT_EQ(V_ActualDoFlushKey[i].rec_scn_, mock_scn(100 + i));
    ASSERT_EQ(V_ActualDoFlushKey[i].tablet_id_, ObTabletID(100 + i));
  }
}

TEST_F(TestMdsTableFlush, flusher_for_one) {
  FlusherForOne one;
  for (int i = FLUSH_FOR_SOME_SIZE; i > 0; --i) {
    one.record_mds_table({ObTabletID(i), mock_scn(i)});
  }
  ASSERT_EQ(one.min_key().rec_scn_, mock_scn(1));
}

// // release版本的逻辑重写不生效
// TEST_F(TestMdsTableFlush, flusher_for_all_order_with_limitted_memory_reserve_success_but_push_back_fail) {
//   NEED_ALLOC_FAIL_AFTER_RESERVE = true;// 只支持reserve的时候分配一次内存
//   const int64_t BIG_TEST_SIZE = 5 * TEST_ALL_SIZE;

//   std::vector<FlushKey> v_key;
//   for (int i = 0; i < BIG_TEST_SIZE; ++i) {
//     v_key.push_back({ObTabletID(100 + i), mock_scn(100 + i)});
//   }
//   std::random_shuffle(v_key.begin(), v_key.end());
//   ObLS ls;
//   ObMdsTableMgr mgr;
//   vector<MdsTableHandle> v;
//   ASSERT_EQ(mgr.init(&ls), OB_SUCCESS);
//   // 首先加入第一部分
//   for (int i = 0; i < TEST_ALL_SIZE; ++i) {
//     MdsTableHandle mds_table;
//     ASSERT_EQ(OB_SUCCESS, mds_table.init<UnitTestMdsTable>(MdsAllocator::get_instance(),
//                                                            v_key[i].tablet_id_,
//                                                            share::ObLSID(1),
//                                                            (ObTabletPointer*)0x111,
//                                                            &mgr));
//     MdsTableBase *p_mds_table = mds_table.p_mds_table_base_.data_;
//     p_mds_table->rec_scn_ = v_key[i].rec_scn_;
//     v.push_back(mds_table);
//   }

//   ASSERT_EQ(OB_SUCCESS, PROMISE1.init());
//   ASSERT_EQ(OB_SUCCESS, PROMISE2.init());

//   // 开启一个新的线程做flush，模拟并发增加mds table的情况（以至于reverse的内存不够用了，过程中发生额外的内存分配，但失败的情况）
//   std::thread t1([&](){
//     ASSERT_EQ(OB_SUCCESS, mgr.flush(share::SCN::max_scn()));
//   });

//   {
//     OCCAM_LOG(INFO, "wait PEOMISE1");
//     ObFuture<void> future = PROMISE1.get_future();
//     future.wait();
//   }

//   {
//     for (int i = TEST_ALL_SIZE; i < v_key.size(); ++i) {// 继续再注册99 * TEST_ALL_SIZE的新mds table
//       MdsTableHandle mds_table;
//       ASSERT_EQ(OB_SUCCESS, mds_table.init<UnitTestMdsTable>(MdsAllocator::get_instance(),
//                                                              v_key[i].tablet_id_,
//                                                              share::ObLSID(1),
//                                                              (ObTabletPointer*)0x111,
//                                                              &mgr));
//       MdsTableBase *p_mds_table = mds_table.p_mds_table_base_.data_;
//       p_mds_table->rec_scn_ = v_key[i].rec_scn_;
//       v.push_back(mds_table);
//     }
//     PROMISE2.set();
//   }

//   t1.join();

//   ASSERT_EQ(BIG_TEST_SIZE + FLUSH_FOR_ALL_SIZE, V_ActualDoFlushKey.size());// 所有的mds table都经历了转储
//   for (int i = 0; i < FLUSH_FOR_ALL_SIZE; ++i) {// 但只保证栈上记录的tablet是有序的，并且rec scn最小
//     ASSERT_EQ(V_ActualDoFlushKey[i].rec_scn_, mock_scn(100 + i));
//     ASSERT_EQ(V_ActualDoFlushKey[i].tablet_id_, ObTabletID(100 + i));
//   }

// }

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_mds_table_flusher.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_mds_table_flusher.log", false);
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
