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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include <gtest/gtest.h>
#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "lib/allocator/page_arena.h"
#include "ob_log_utils.h"
#define private public
#include "logservice/libobcdc/src/ob_log_part_svr_list.h"
#include "logservice/libobcdc/src/ob_log_instance.h"
#include "logservice/libobcdc/src/ob_log_fetcher.h"

#include "ob_log_start_log_id_locator.h"   // StartLogIdLocateReq

using namespace oceanbase;
using namespace common;
using namespace libobcdc;

namespace oceanbase
{
namespace unittest
{
class TestObLogPartSvrList: public ::testing::Test
{
public :
  virtual void SetUp();
  virtual void TearDown();
public:
  typedef PartSvrList::SvrItem SvrItem;
  typedef PartSvrList::LogIdRange LogIdRange;
  // 验证svr_item字段的正确性
 void is_svr_item_correct(PartSvrList &svr_list,
                         const int64_t svr_item_index,
                         common::ObAddrWithSeq &expect_svr,
                         const int64_t expect_range_num,
                         LogIdRange *expect_log_ranges);
private:
  static const int64_t SERVER_COUNT = 64;
  static const int64_t MAX_RANGE_NUMBER = 4;
private:
  common::ObAddrWithSeq servers[SERVER_COUNT];
  common::ObArenaAllocator allocator;
};

void TestObLogPartSvrList::SetUp()
{
  for (int64_t idx = 0; idx < SERVER_COUNT; idx++) {
    ObAddr addr(ObAddr::VER::IPV4, "127.0.0.1", static_cast<int32_t>(idx + 8000));
    servers[idx].set_addr_seq(addr, idx + 1);
  }
}

void TestObLogPartSvrList::TearDown()
{
}


void TestObLogPartSvrList::is_svr_item_correct(PartSvrList &svr_list,
                                               const int64_t svr_item_index,
                                               common::ObAddrWithSeq &expect_svr,
                                               const int64_t expect_range_num,
                                               LogIdRange *expect_log_ranges)
{
  SvrItem svr_item;
  EXPECT_EQ(OB_SUCCESS, svr_list.svr_items_.at(svr_item_index, svr_item));

  EXPECT_EQ(expect_svr, svr_item.svr_);
  EXPECT_EQ(expect_range_num, svr_item.range_num_);
  for (int64_t idx = 0; idx < expect_range_num; idx++) {
    EXPECT_EQ(expect_log_ranges[idx].start_log_id_, svr_item.log_ranges_[idx].start_log_id_);
    EXPECT_EQ(expect_log_ranges[idx].end_log_id_, svr_item.log_ranges_[idx].end_log_id_);
  }
}


//////////////////////Basic function tests//////////////////////////////////////////
// PartSvrList::add_server_or_update()
// The main test is the insert_range_ function, which calls find_pos_and_merge to find the position, but no log range merge has occurred
TEST_F(TestObLogPartSvrList, add_server_test1)
{
 // 声明
 const int64_t svr_idx = 0;
 common::ObAddrWithSeq expect_svr = servers[svr_idx];
 int64_t expect_range_num = 0;
 LogIdRange expect_log_ranges[MAX_RANGE_NUMBER];
 (void)memset(expect_log_ranges, 0, MAX_RANGE_NUMBER * sizeof(LogIdRange));
  const bool is_located_in_meta_table = false;
  const bool is_leader = false;

  PartSvrList svr_list;
  EXPECT_EQ(0, svr_list.next_svr_index_);
  EXPECT_EQ(0, svr_list.count());

  /// add log id range: (100, 200)
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(servers[0], 100, 200,
        is_located_in_meta_table, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, is_leader));
  expect_range_num++;
	// Verify the correctness of the svr_item field
  EXPECT_EQ(1, svr_list.count());
  expect_log_ranges[0].reset(100, 200);
  is_svr_item_correct(svr_list, svr_idx, expect_svr, expect_range_num, expect_log_ranges);

  /// add log id range: (300, 400)
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(servers[0], 300, 400,
        is_located_in_meta_table, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, is_leader));
  expect_range_num++;
	// Verify the correctness of the svr_item field
  expect_log_ranges[1].reset(300, 400);
  is_svr_item_correct(svr_list, svr_idx, expect_svr, expect_range_num, expect_log_ranges);

  /// add log id range: (500, 600)
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(servers[0], 500, 600,
        is_located_in_meta_table, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, is_leader));
  expect_range_num++;
	// Verify the correctness of the svr_item field
  expect_log_ranges[2].reset(500, 600);
  is_svr_item_correct(svr_list, svr_idx, expect_svr, expect_range_num, expect_log_ranges);

  /// add log id range: (60, 80)
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(servers[0], 60, 80,
        is_located_in_meta_table, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, is_leader));
  expect_range_num++;
  // Verify the correctness of the svr_item field
  expect_log_ranges[0].reset(60, 80);
  expect_log_ranges[1].reset(100, 200);
  expect_log_ranges[2].reset(300, 400);
  expect_log_ranges[3].reset(500, 600);
  is_svr_item_correct(svr_list, svr_idx, expect_svr, expect_range_num, expect_log_ranges);

  /// add log id range: (410, 450)
  //  current range:[60, 80], [100, 200], [300, 400], [700, 800]
  //  If no merge occurs and the array is full, find the insertion position pos, and perform a manual merge with the range at position pos
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(servers[0], 700, 800,
        is_located_in_meta_table, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, is_leader));
  // Verify the correctness of the svr_item field
  expect_log_ranges[3].reset(500, 800);
  is_svr_item_correct(svr_list, svr_idx, expect_svr, expect_range_num, expect_log_ranges);

  /// add log id range: (410, 450)
  //  current range:[60, 80], [100, 200], [300, 400], [700, 800]
  //  If no merge occurs and the array is full, find the insertion position pos, and perform a manual merge with the range at position pos
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(servers[0], 410, 450,
        is_located_in_meta_table, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, is_leader));
  // Verify the correctness of the svr_item field
  expect_log_ranges[3].reset(410, 800);
  is_svr_item_correct(svr_list, svr_idx, expect_svr, expect_range_num, expect_log_ranges);

  /// add log id range: (30, 40)
  //  current range:[60, 80], [100, 200], [300, 400], [410, 800]
  //  If no merge occurs and the array is full, find the insertion position pos, and perform a manual merge with the range at position pos
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(servers[0], 30, 40,
        is_located_in_meta_table, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, is_leader));
	// Verify the correctness of the svr_item field
  expect_log_ranges[0].reset(30, 80);
    is_svr_item_correct(svr_list, svr_idx, expect_svr, expect_range_num, expect_log_ranges);
}

// PartSvrList::add_server_or_update()
// The main test is the find_pos_and_merge_ function, where find_pos_and_merge_ is called to find the position and a merge occurs
TEST_F(TestObLogPartSvrList, add_server_test2)
{
  const int64_t svr_idx = 0;
  common::ObAddrWithSeq expect_svr = servers[svr_idx];
  int64_t expect_range_num = 0;
  LogIdRange expect_log_ranges[MAX_RANGE_NUMBER];
  (void)memset(expect_log_ranges, 0, MAX_RANGE_NUMBER * sizeof(LogIdRange));
  const bool is_located_in_meta_table = false;
  const bool is_leader = false;

  PartSvrList svr_list;
  EXPECT_EQ(0, svr_list.next_svr_index_);
  EXPECT_EQ(0, svr_list.count());
	// init range
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(expect_svr, 60, 80,
        is_located_in_meta_table, REGION_PRIORITY_LOW, REPLICA_PRIORITY_FULL, is_leader));
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(expect_svr, 100, 200,
        is_located_in_meta_table, REGION_PRIORITY_LOW, REPLICA_PRIORITY_FULL, is_leader));
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(expect_svr, 300, 400,
        is_located_in_meta_table, REGION_PRIORITY_LOW, REPLICA_PRIORITY_FULL, is_leader));
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(expect_svr, 500, 600,
        is_located_in_meta_table, REGION_PRIORITY_LOW, REPLICA_PRIORITY_FULL, is_leader));
  expect_range_num = 4;
  expect_log_ranges[0].reset(60, 80);
  expect_log_ranges[1].reset(100, 200);
  expect_log_ranges[2].reset(300, 400);
  expect_log_ranges[3].reset(500, 600);

  /// add log id range: (70, 90)
  //  current range:[60, 80], [100, 200], [300, 400], [500, 600]
  //  Merge with 1st range only
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(expect_svr, 70, 90,
        is_located_in_meta_table, REGION_PRIORITY_LOW, REPLICA_PRIORITY_FULL, is_leader));
  // 验证svr_item字段正确性
  expect_log_ranges[0].reset(60, 90);
  is_svr_item_correct(svr_list, svr_idx, expect_svr, expect_range_num, expect_log_ranges);

  /// add log id range: (50, 450)
  //  current range:[60, 90], [100, 200], [300, 400], [500, 600]
  //  and the 1st, 2nd and 3rd rang occur together
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(expect_svr, 50, 450,
        is_located_in_meta_table, REGION_PRIORITY_LOW, REPLICA_PRIORITY_FULL, is_leader));
	// Verify the correctness of the svr_item field
  expect_range_num = 2;
  expect_log_ranges[0].reset(50, 450);
  expect_log_ranges[1].reset(500, 600);
  is_svr_item_correct(svr_list, svr_idx, expect_svr, expect_range_num, expect_log_ranges);
}

TEST_F(TestObLogPartSvrList, next_server)
{
  ObLogInstance *instance = ObLogInstance::get_instance();
  IObLogFetcher *fetcher = new ObLogFetcher();
  static_cast<ObLogFetcher*>(fetcher)->inited_ = true;
  instance->fetcher_ = fetcher;
  LOG_INFO("found fetcher", K(TCTX.fetcher_));
  // request next log: log_id=250
  uint64_t next_log_id = 250;
  BlackList black_list;
  ObLogSvrIdentity svr;

  int64_t svr_idx = 0;
  common::ObAddrWithSeq expect_svr;
  int64_t expect_range_num = 0;
  LogIdRange expect_log_ranges[MAX_RANGE_NUMBER];
  (void)memset(expect_log_ranges, 0, MAX_RANGE_NUMBER * sizeof(LogIdRange));

  PartSvrList svr_list;
  EXPECT_EQ(0, svr_list.next_svr_index_);
  EXPECT_EQ(0, svr_list.count());
  const bool is_located_in_meta_table = false;
  const bool is_leader = false;

  /// case 1: for this partition, the current ServerList has 3 servers
  // server-1: log range: [300, 500], [600, 700]
  // server-2: log range: [100, 150], [160, 200]
  // server-3: log range: [50, 90], [100, 150], [200, 300], [400, 500]
  //
  // for server-1, log id is at lower limit of range, exit directly; server-1 does not serve 250 logs, but server1 is valid, move to next server
  //
  // for server-2, server-2 does not serve 250 logs, and server2 is invalid, because next_log_id is generally
  // monotonically increasing, then server-2 maintains a log range, all less than 250, ServerList needs to delete server2
  //
  // For server-3, server-3 serves 250 logs, and needs to delete the [50, 90], [100, 150] logs it maintains

  // server-1
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(servers[1], 300, 500,
        is_located_in_meta_table, REGION_PRIORITY_LOW, REPLICA_PRIORITY_FULL, is_leader));
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(servers[1], 600, 700,
        is_located_in_meta_table, REGION_PRIORITY_LOW, REPLICA_PRIORITY_FULL, is_leader));

  // server-2
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(servers[2], 100, 150,
        is_located_in_meta_table, REGION_PRIORITY_LOW, REPLICA_PRIORITY_FULL, is_leader));
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(servers[2], 160, 200,
        is_located_in_meta_table, REGION_PRIORITY_LOW, REPLICA_PRIORITY_FULL, is_leader));

  // server-3
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(servers[3], 50, 90,
        is_located_in_meta_table, REGION_PRIORITY_LOW, REPLICA_PRIORITY_FULL, is_leader));
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(servers[3], 100, 150,
        is_located_in_meta_table, REGION_PRIORITY_LOW, REPLICA_PRIORITY_FULL, is_leader));
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(servers[3], 200, 300,
        is_located_in_meta_table, REGION_PRIORITY_LOW, REPLICA_PRIORITY_FULL, is_leader));
  EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(servers[3], 400, 500,
        is_located_in_meta_table, REGION_PRIORITY_LOW, REPLICA_PRIORITY_FULL, is_leader));

  EXPECT_EQ(3, svr_list.count());
  EXPECT_EQ(OB_SUCCESS, svr_list.next_server(next_log_id, black_list, svr));

  /// Verify correctness
  /// Number of svr minus 1
  EXPECT_EQ(2, svr_list.count());

  // verify log rang of eserver－3
  svr_idx = 1;
  expect_svr = servers[3];
  expect_range_num = 2;
  expect_log_ranges[0].reset(200, 300);
  expect_log_ranges[1].reset(400, 500);
  is_svr_item_correct(svr_list, svr_idx, expect_svr, expect_range_num, expect_log_ranges);


  /// case 2: For this partition, the current ServerList has 2 servers
  //  server-1: log range: [300, 500], [600, 700]
  //  server-3: log range: [200, 300], [400, 500]
  //
  EXPECT_EQ(2, svr_list.svr_items_.count());
  svr.reset();
  EXPECT_EQ(OB_SUCCESS, svr_list.next_server(next_log_id, black_list, svr));

  // 请求650
  next_log_id = 650;
  EXPECT_EQ(OB_SUCCESS, svr_list.next_server(next_log_id, black_list, svr));
  svr_idx = 0;
  expect_svr = servers[1];
  expect_range_num = 1;
  expect_log_ranges[0].reset(600, 700);
  is_svr_item_correct(svr_list, svr_idx, expect_svr, expect_range_num, expect_log_ranges);
}

// PartSvrList: exist(), get_sever_array()
TEST_F(TestObLogPartSvrList, other_function)
{
 PartSvrList svr_list;

  for (int64_t idx = 0; idx < 32; idx++) {
    // Half of the clog_history table records and half of the meta table records
    if (idx < 16) {
      const bool is_located_in_meta_table1 = false;
      const bool is_leader1 = false;
     EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(servers[idx], 100, 200,
            is_located_in_meta_table1, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, is_leader1));
    } else {
      const bool is_located_in_meta_table2 = true;
      bool is_leader2 = false;
      if (31 == idx) {
        is_leader2 = true;
      }
     EXPECT_EQ(OB_SUCCESS, svr_list.add_server_or_update(servers[idx], 100, 200,
            is_located_in_meta_table2, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, is_leader2));
    }
 }

  int64_t svr_index = -1;
  for (int64_t idx = 0; idx < 32; idx++) {
    EXPECT_TRUE(svr_list.exist(servers[idx].get_addr(), svr_index));
  }
  StartLogIdLocateReq::SvrList svr_list_for_locate_start_log_id;
  EXPECT_EQ(OB_SUCCESS, svr_list.get_server_array_for_locate_start_log_id(svr_list_for_locate_start_log_id));
  EXPECT_EQ(32, svr_list_for_locate_start_log_id.count());

  // verify leader is the first
  EXPECT_EQ(svr_list_for_locate_start_log_id.at(0).svr_identity_.get_principal_svr(), servers[31]);
  for (int64_t idx = 1; idx < 32; idx++) {
    const ObAddrWithSeq &addr = svr_list_for_locate_start_log_id.at(idx).svr_identity_.get_principal_svr();
    int64_t start_idx = -1;
    int64_t end_idx = -1;

    // meta table
    if (idx < 16) {
      start_idx = 16;
      end_idx = 32;
    } else {
    // clog history table
      start_idx = 0;
      end_idx = 16;
    }

    bool find = false;
    for (int64_t svr_idx = start_idx; svr_idx < end_idx; ++svr_idx) {
      if (addr == servers[svr_idx]) {
        find = true;
        break;
      }
    }
    EXPECT_TRUE(find);
  }
  //
  // There are 6 servers in total, added in the following order.
  // server sequence: svr1, svr2, svr1, svr3, svr4, svr1, svr2, sv3, sv4, sv5, sv6
  //
  // server: svr1, sv2, sv3, sv4, sv5, sv6
  // is_meta_table 0 0 0 0 0 1 1
  // is_leader 0 0 0 0 0 0 1
  // Expected: leader comes first, followed by meta table, remaining
  // sv6, sv5 .....

  PartSvrList svr_list1;
  // svr1
  EXPECT_EQ(OB_SUCCESS, svr_list1.add_server_or_update(servers[1], 100, 200, false, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, false));

  // svr2
  EXPECT_EQ(OB_SUCCESS, svr_list1.add_server_or_update(servers[2], 100, 200, false, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, false));

  // svr1
  EXPECT_EQ(OB_SUCCESS, svr_list1.add_server_or_update(servers[1], 100, 200, false, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, false));

  // svr3
  EXPECT_EQ(OB_SUCCESS, svr_list1.add_server_or_update(servers[3], 100, 200, false, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, false));

  // svr4
  EXPECT_EQ(OB_SUCCESS, svr_list1.add_server_or_update(servers[4], 100, 200, false, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, false));

  // svr4
  EXPECT_EQ(OB_SUCCESS, svr_list1.add_server_or_update(servers[1], 100, 200, false, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, false));

  // svr4
  EXPECT_EQ(OB_SUCCESS, svr_list1.add_server_or_update(servers[2], 100, 200, false, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, false));

  // svr4
  EXPECT_EQ(OB_SUCCESS, svr_list1.add_server_or_update(servers[3], 100, 200, false, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, false));

  // svr5
  EXPECT_EQ(OB_SUCCESS, svr_list1.add_server_or_update(servers[5], 100, 200, true, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, false));

  // svr6
  EXPECT_EQ(OB_SUCCESS, svr_list1.add_server_or_update(servers[6], 100, 200, true, REGION_PRIORITY_HIGH, REPLICA_PRIORITY_FULL, true));

  StartLogIdLocateReq::SvrList svr_list_for_locate_start_log_id_1;
  EXPECT_EQ(OB_SUCCESS, svr_list1.get_server_array_for_locate_start_log_id(svr_list_for_locate_start_log_id_1));
  EXPECT_EQ(6, svr_list_for_locate_start_log_id_1.count());

  int expect_result_index[] = {6, 5};
  for (int64_t idx = 0; idx < 2; idx++) {
    EXPECT_EQ(svr_list_for_locate_start_log_id_1.at(idx).svr_identity_.get_principal_svr(), servers[expect_result_index[idx]]);
  }
}

}//end of unittest
}//end of oceanbase

int main(int argc, char **argv)
{
  // ObLogger::get_logger().set_mod_log_levels("ALL.*:DEBUG, TLOG.*:DEBUG");
  // testing::InitGoogleTest(&argc,argv);
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_log_part_svr_list.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
