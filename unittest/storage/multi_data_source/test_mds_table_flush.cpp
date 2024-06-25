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
#include "lib/utility/utility.h"
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
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
namespace oceanbase {
namespace storage
{

share::SCN MOCK_MAX_CONSEQUENT_CALLBACKED_SCN;

namespace mds
{

int MdsTableBase::get_ls_max_consequent_callbacked_scn_(share::SCN &max_consequent_callbacked_scn) const
{
  max_consequent_callbacked_scn = MOCK_MAX_CONSEQUENT_CALLBACKED_SCN;
  return OB_SUCCESS;
}

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

class TestMdsTableFlush: public ::testing::Test
{
public:
  TestMdsTableFlush() { ObMdsSchemaHelper::get_instance().init(); }
  virtual ~TestMdsTableFlush() {}
  virtual void SetUp() {
  }
  virtual void TearDown() {
  }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestMdsTableFlush);
};


//                                                max_decided_scn:475
//                                                     │
//                                                     │
//                                                     │
//                                       ┌──────────┐  │                      ┌──────────┐                           ┌──────────┐
// MdsUnit<DummyKey, ExampleUserData1>   │(MAX, 500]│◄─┼──────────────────────┤[300, 250]│◄──────────────────────────┤(100,  50]│
//                                       └──────────┘  │                      └──────────┘                           └──────────┘
//                                                     │
//                                                     │
//                                                     │
//                                                     │
//                                                     │   ┌──────────┐                           ┌──────────┐
// MdsUnit<DummyKey, ExampleUserData2>                 │   │[400, 350]│◄──────────────────────────┤[225, 200]│
//                                                     │   └──────────┘                           └──────────┘
//                                                     │
//                                                     │
//                                                     │
//                                                     │
int construct_tested_mds_table(MdsTableHandle &handle) {
  int ret = OB_SUCCESS;
  handle.reset();
  vector<MdsCtx*> v_ctx;
  for (int i = 0; i < 7; ++i) {
    v_ctx.push_back(new MdsCtx(MdsWriter(transaction::ObTransID(i))));
  }
  if (OB_FAIL(handle.init<UnitTestMdsTable>(MdsAllocator::get_instance(), ObTabletID(1), share::ObLSID(1), (ObTabletPointer*)0x111))) {
  } else if (OB_FAIL(handle.set<ExampleUserData1>(1, *v_ctx[0]))) {
  } else if (FALSE_IT(v_ctx[0]->on_redo(mock_scn(50)))) {
  } else if (FALSE_IT(v_ctx[0]->on_commit(mock_scn(100), mock_scn(100)))) {
  } else if (OB_FAIL(handle.set<ExampleUserData1>(2, *v_ctx[1]))) {
  } else if (FALSE_IT(v_ctx[1]->on_redo(mock_scn(250)))) {
  } else if (FALSE_IT(v_ctx[1]->on_commit(mock_scn(300), mock_scn(300)))) {
  } else if (OB_FAIL(handle.set<ExampleUserData1>(3, *v_ctx[2]))) {
  } else if (FALSE_IT(v_ctx[2]->on_redo(mock_scn(500)))) {
  } else if (OB_FAIL(handle.set<ExampleUserData2>(ExampleUserData2(), *v_ctx[3]))) {
  } else if (FALSE_IT(v_ctx[3]->on_redo(mock_scn(200)))) {
  } else if (FALSE_IT(v_ctx[3]->on_commit(mock_scn(225), mock_scn(225)))) {
  } else if (OB_FAIL(handle.set<ExampleUserData2>(ExampleUserData2(), *v_ctx[4]))) {
  } else if (FALSE_IT(v_ctx[4]->on_redo(mock_scn(350)))) {
  } else if (FALSE_IT(v_ctx[4]->on_commit(mock_scn(400), mock_scn(400)))) {
  } else if (OB_SUCCESS != (ret = handle.set<ExampleUserKey, ExampleUserData1>(ExampleUserKey(1), ExampleUserData1(1), *v_ctx[5]))) {
  } else if (OB_SUCCESS != (ret = handle.set<ExampleUserKey, ExampleUserData1>(ExampleUserKey(2), ExampleUserData1(2), *v_ctx[6]))) {
  }
  v_ctx[6]->on_abort(mock_scn(10));
  return ret;
}

TEST_F(TestMdsTableFlush, normal_flush) {
  MdsTableHandle handle;
  ASSERT_EQ(OB_SUCCESS, construct_tested_mds_table(handle));
  share::SCN rec_scn;
  ASSERT_EQ(OB_SUCCESS, handle.get_rec_scn(rec_scn));
  ASSERT_EQ(mock_scn(50), rec_scn);// 没转储的时候是最小的node的redo scn值

  // 第一次转储
  ASSERT_EQ(OB_SUCCESS, handle.flush(mock_scn(1000), mock_scn(125)));// 因为max_decided_scn较小，所以会用125做flush
  bool is_flusing = false;
  ASSERT_EQ(OB_SUCCESS, handle.is_flushing(is_flusing));// 在flush流程中
  ASSERT_EQ(true, is_flusing);
  ASSERT_EQ(mock_scn(125), handle.p_mds_table_base_->flushing_scn_);
  int scan_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, (handle.scan_all_nodes_to_dump<ScanRowOrder::ASC, ScanNodeOrder::FROM_OLD_TO_NEW>([&scan_cnt](const MdsDumpKV &kv) -> int {
    scan_cnt++;
    return OB_SUCCESS;
  }, 0, true)));
  ASSERT_EQ(1, scan_cnt);
  handle.on_flush(mock_scn(125), OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, handle.get_rec_scn(rec_scn));
  OCCAM_LOG(INFO, "print rec scn", K(rec_scn));
  ASSERT_EQ(mock_scn(200), rec_scn);

  // 第二次转储
  ASSERT_EQ(OB_SUCCESS, handle.flush(mock_scn(1000), mock_scn(140)));
  ASSERT_EQ(OB_SUCCESS, handle.is_flushing(is_flusing));
  ASSERT_EQ(false, is_flusing);// 没转储
  ASSERT_EQ(OB_SUCCESS, handle.get_rec_scn(rec_scn));
  OCCAM_LOG(INFO, "print rec scn", K(rec_scn));
  ASSERT_EQ(mock_scn(200), rec_scn);// 没变化

  // 第三次转储
  ASSERT_EQ(OB_SUCCESS, handle.flush(mock_scn(1000), mock_scn(275)));
  ASSERT_EQ(OB_SUCCESS, handle.is_flushing(is_flusing));// 在flush流程中
  ASSERT_EQ(true, is_flusing);
  ASSERT_EQ(mock_scn(249), handle.p_mds_table_base_->flushing_scn_);
  scan_cnt = 0;
  auto scan_op = [&scan_cnt](const MdsDumpKV &kv) -> int {
    scan_cnt++;
    return OB_SUCCESS;
  };
  ASSERT_EQ(OB_SUCCESS, (handle.scan_all_nodes_to_dump<ScanRowOrder::ASC, ScanNodeOrder::FROM_OLD_TO_NEW>(scan_op, 0, true)));
  ASSERT_EQ(1, scan_cnt);
  handle.on_flush(mock_scn(249), OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, handle.get_rec_scn(rec_scn));
  OCCAM_LOG(INFO, "print rec scn", K(rec_scn));
  ASSERT_EQ(mock_scn(250), rec_scn);

  // 第四次转储
  ASSERT_EQ(OB_SUCCESS, handle.flush(mock_scn(1000), mock_scn(550)));
  ASSERT_EQ(OB_SUCCESS, handle.is_flushing(is_flusing));// 在flush流程中
  ASSERT_EQ(true, is_flusing);
  ASSERT_EQ(mock_scn(499), handle.p_mds_table_base_->flushing_scn_);
  scan_cnt = 0;

  ASSERT_EQ(OB_SUCCESS, (handle.scan_all_nodes_to_dump<ScanRowOrder::ASC, ScanNodeOrder::FROM_OLD_TO_NEW>([&scan_cnt](const MdsDumpKV &kv) -> int {
    scan_cnt++;
    return OB_SUCCESS;
  }, 0, true)));
  ASSERT_EQ(2, scan_cnt);
  handle.on_flush(mock_scn(499), OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, handle.get_rec_scn(rec_scn));
  OCCAM_LOG(INFO, "print rec scn", K(rec_scn));
  ASSERT_EQ(mock_scn(500), rec_scn);

  // 第五次转储
  ASSERT_EQ(OB_SUCCESS, handle.flush(mock_scn(1000), mock_scn(600)));
  ASSERT_EQ(OB_SUCCESS, handle.is_flushing(is_flusing));
  ASSERT_EQ(false, is_flusing);// 没转储
  ASSERT_EQ(OB_SUCCESS, handle.get_rec_scn(rec_scn));
  OCCAM_LOG(INFO, "print rec scn", K(rec_scn));
  ASSERT_EQ(mock_scn(500), rec_scn);// 没变化

  // 第六次转储
  ASSERT_EQ(OB_SUCCESS, handle.flush(mock_scn(1000), mock_scn(590)));
  ASSERT_EQ(OB_SUCCESS, handle.is_flushing(is_flusing));
  ASSERT_EQ(false, is_flusing);// 没转储
  ASSERT_EQ(OB_SUCCESS, handle.get_rec_scn(rec_scn));
  OCCAM_LOG(INFO, "print rec scn", K(rec_scn));
  ASSERT_EQ(mock_scn(500), rec_scn);// 没变化
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_mds_table_flush.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_mds_table_flush.log", false);
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
