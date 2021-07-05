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

#include "storage/transaction/ob_trans_define.h"
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_partition_key.h"
#include "common/ob_clock_generator.h"
#include "sql/ob_sql_define.h"
#include "sql/resolver/ob_stmt.h"

namespace oceanbase {
using namespace common;
using namespace transaction;
using namespace sql;
namespace unittest {
class TestObTransDefine : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

public:
  // valid partition parameters
  static const int64_t VALID_TABLE_ID = 1;
  static const int32_t VALID_PARTITION_ID = 1;
  static const int32_t VALID_PARTITION_COUNT = 100;
  // invalid partition parameters
  static const int64_t INVALID_TABLE_ID = -1;
  static const int32_t INVALID_PARTITION_ID = -1;
  static const int32_t INVALID_PARTITION_COUNT = -100;

  static const int32_t PORT = 8080;
  static const ObAddr::VER IP_TYPE = ObAddr::IPV4;
  static const char* LOCAL_IP;
};
const char* TestObTransDefine::LOCAL_IP = "127.0.0.1";

////////////////////////////////test ObTransID//////////////////////////////////////////
// test the init of ObTransID
TEST_F(TestObTransDefine, trans_id_constructor)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransID trans_id;
  EXPECT_FALSE(trans_id.is_valid());

  ObAddr observer(TestObTransDefine::IP_TYPE, TestObTransDefine::LOCAL_IP, TestObTransDefine::PORT);
  ObTransID trans_id1(observer);
  EXPECT_EQ(true, trans_id1.is_valid());

  ObTransID trans_id2(trans_id1);
  EXPECT_EQ(1, trans_id2.get_inc_num());
  EXPECT_EQ(true, trans_id2.is_valid());

  ObTransID trans_id3(trans_id1);
  trans_id2 = trans_id3;
  EXPECT_EQ(1, trans_id2.get_inc_num());
  EXPECT_EQ(true, trans_id2.is_valid());

  EXPECT_TRUE(trans_id1 == trans_id2);

  EXPECT_EQ(0, trans_id1.compare(trans_id2));
  EXPECT_EQ(0, trans_id2.compare(trans_id1));
}
// test the serialization and deserialization of ObTransID
TEST_F(TestObTransDefine, trans_id_enconde_deconde)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObAddr observer(TestObTransDefine::IP_TYPE, TestObTransDefine::LOCAL_IP, TestObTransDefine::PORT);
  ObTransID trans_id(observer);
  EXPECT_EQ(2, trans_id.get_inc_num());
  // serialization
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, trans_id.serialize(buffer, BUFFER_SIZE, pos));
  // deserialization
  ObTransID trans_id1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, trans_id1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(2, trans_id1.get_inc_num());

  EXPECT_EQ(trans_id, trans_id1);
}

// test the reset
TEST_F(TestObTransDefine, trans_id_reset)
{
  ObAddr observer(TestObTransDefine::IP_TYPE, TestObTransDefine::LOCAL_IP, TestObTransDefine::PORT);
  ObTransID tid_1(observer);
  ObTransID tid_2(tid_1);
  EXPECT_TRUE(tid_2 == tid_1);
  EXPECT_EQ(0, tid_2.compare(tid_1));
  EXPECT_TRUE(tid_2.is_valid());
  tid_2.reset();
  EXPECT_FALSE(tid_2 == tid_1);
  EXPECT_NE(0, tid_2.compare(tid_1));
  EXPECT_FALSE(tid_2.is_valid());
}

/*
TEST_F(TestObTransDefine, trans_id_compare)
{
  ObAddr addr_little(TestObTransDefine::IP_TYPE, TestObTransDefine::LOCAL_IP, 1000);
  ObAddr addr_bigger(TestObTransDefine::IP_TYPE, TestObTransDefine::LOCAL_IP, 2000);

  ASSERT_TRUE(addr_little < addr_bigger);

  ObTransID tid_1(addr_little);
  EXPECT_TRUE(0 == tid_1.compare(tid_1));

  ObTransID tid_1_copy(tid_1);
  EXPECT_TRUE(0 == tid_1.compare(tid_1_copy));
  EXPECT_TRUE(0 == tid_1_copy.compare(tid_1));

  ObTransID tid_2(addr_little);
  EXPECT_TRUE(tid_1.compare(tid_2) < 0);
  EXPECT_TRUE(tid_2.compare(tid_1) > 0);

  ObTransID tid_3(addr_bigger);
  EXPECT_TRUE(tid_1.compare(tid_3) < 0);
  EXPECT_TRUE(tid_3.compare(tid_1) > 0);
  EXPECT_TRUE(tid_2.compare(tid_3) < 0);
  EXPECT_TRUE(tid_3.compare(tid_2) > 0);

  ObTransID tid_4(addr_little);
  EXPECT_TRUE(tid_3.compare(tid_4) > 0);
  EXPECT_TRUE(tid_4.compare(tid_3) < 0);
}
*/

// test the hash of ObTransID
TEST_F(TestObTransDefine, trans_id_hash)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObAddr observer = ObAddr(TestObTransDefine::IP_TYPE, TestObTransDefine::LOCAL_IP, TestObTransDefine::PORT);
  ObTransID trans_id(observer);
  uint64_t hashcode = trans_id.hash();
  TRANS_LOG(INFO, "hashcode info", K(hashcode));
}

//////////////////////////////////////////test ObXATransID///////////////////////////////////
TEST_F(TestObTransDefine, xid_encode_decode)
{
  const char* gtrid_cstr = "gtrid01";
  const char* bqual_cstr = "bqual01";
  char gtrid_buf[128];
  char bqual_buf[128];
  common::ObString gtrid_str;
  common::ObString bqual_str;
  gtrid_str.reset();
  gtrid_str.assign_buffer(gtrid_buf, sizeof(gtrid_buf));
  gtrid_str.write(gtrid_cstr, 7);
  bqual_str.reset();
  bqual_str.assign_buffer(bqual_buf, sizeof(bqual_buf));
  bqual_str.write(bqual_cstr, 7);
  ObXATransID xid;
  xid.set(gtrid_str, bqual_str, 1);
  // serialization
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, xid.serialize(buffer, BUFFER_SIZE, pos));
  // deserialization
  ObXATransID xid2;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, xid2.deserialize(buffer, pos, start_index));
  EXPECT_EQ(xid, xid2);
  TRANS_LOG(INFO, "xid info", K(xid), K(xid2));
}

//////////////////////////////////////////test ObTransDesc///////////////////////////////////
// test the set_tranas_id and get_trans_id of ObTransDesc
TEST_F(TestObTransDefine, trans_desc_set_get_trans_id)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // invalid trans_id
  ObTransDesc trans_desc;
  ObTransID trans_id;
  EXPECT_EQ(OB_INVALID_ARGUMENT, trans_desc.set_trans_id(trans_id));
  TRANS_LOG(INFO, "trans_desc info after setted with invalid_trans_id", K(trans_desc));
  // valid trans_id
  ObAddr observer(TestObTransDefine::IP_TYPE, TestObTransDefine::LOCAL_IP, TestObTransDefine::PORT);
  ObTransID trans_id1(observer);
  EXPECT_EQ(OB_SUCCESS, trans_desc.set_trans_id(trans_id1));
  EXPECT_FALSE(trans_desc.is_valid());

  // test the get_trans_id
  const ObTransID& trans_id2 = trans_desc.get_trans_id();
  EXPECT_EQ(trans_id1, trans_id2);
  TRANS_LOG(INFO, "trans_id1 and the copied obj trans_id2 info", K(trans_id1), K(trans_id2));
}

// test the set_snapshot_version and get_snapshot_version of ObTransDesc
TEST_F(TestObTransDefine, trans_desc_set_get_snapshot_version)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // invalid snapshot_version
  int64_t snapshot_version = -1;
  ObTransDesc trans_desc;
  EXPECT_EQ(OB_INVALID_ARGUMENT, trans_desc.set_snapshot_version(snapshot_version));

  snapshot_version = 1;
  EXPECT_EQ(OB_SUCCESS, trans_desc.set_snapshot_version(snapshot_version));
  TRANS_LOG(INFO, "trans_desc info after setted valid snapshot_version", K(trans_desc));

  // test the get_snapshot
  EXPECT_EQ(snapshot_version, trans_desc.get_snapshot_version());
  TRANS_LOG(INFO, "snapshot_version", K(snapshot_version));
}
// test the set_trans_param and get_trans_param of ObTransDesc
TEST_F(TestObTransDefine, trans_desc_set_get_trans_param)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // invalid trans_param
  ObTransDesc trans_desc;
  ObStartTransParam parms;
  EXPECT_EQ(OB_INVALID_ARGUMENT, trans_desc.set_trans_param(parms));

  parms.set_access_mode(ObTransAccessMode::READ_ONLY);
  parms.set_type(ObTransType::TRANS_USER);
  parms.set_isolation(ObTransIsolation::READ_COMMITED);
  EXPECT_EQ(OB_SUCCESS, trans_desc.set_trans_param(parms));
  // test the get_trans_param
  const ObStartTransParam& parms1 = trans_desc.get_trans_param();
  EXPECT_EQ(parms.get_access_mode(), parms1.get_access_mode());
  EXPECT_EQ(parms.get_type(), parms1.get_type());
  EXPECT_EQ(parms.get_isolation(), parms1.get_isolation());
}
// test the sql_no, trans_expired_time, cur_stmt_expired_time and rollback of scheduler
TEST_F(TestObTransDefine, trans_desc_set_get_args)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransDesc trans_desc;
  ObAddr scheduler;
  EXPECT_EQ(OB_INVALID_ARGUMENT, trans_desc.set_scheduler(scheduler));

  scheduler = ObAddr(TestObTransDefine::IP_TYPE, TestObTransDefine::LOCAL_IP, TestObTransDefine::PORT);
  EXPECT_EQ(OB_SUCCESS, trans_desc.set_scheduler(scheduler));
  const ObAddr& scheduler1 = trans_desc.get_scheduler();
  EXPECT_EQ(scheduler, scheduler1);
  // sql_no
  trans_desc.inc_sql_no();
  EXPECT_EQ(1, trans_desc.get_sql_no() & 0xffffffffL);
  // trans_expired_time
  EXPECT_EQ(OB_INVALID_ARGUMENT, trans_desc.set_trans_expired_time(-1));
  EXPECT_EQ(OB_SUCCESS, trans_desc.set_trans_expired_time(1000));
  EXPECT_EQ(1000, trans_desc.get_trans_expired_time());
  // cur_stmt_expired_time
  EXPECT_EQ(OB_INVALID_ARGUMENT, trans_desc.set_cur_stmt_expired_time(-1));
  EXPECT_EQ(OB_SUCCESS, trans_desc.set_cur_stmt_expired_time(1000));
  EXPECT_EQ(1000, trans_desc.get_cur_stmt_expired_time());
  // rollback
  trans_desc.set_need_rollback();
  trans_desc.need_rollback();
  // other set/get mothod
  ObPartitionKey partition_key(TestObTransDefine::VALID_TABLE_ID,
      TestObTransDefine::VALID_PARTITION_ID,
      TestObTransDefine::VALID_PARTITION_COUNT);
  EXPECT_TRUE(false == trans_desc.has_create_ctx(partition_key));
  EXPECT_EQ(true, trans_desc.is_trans_timeout());
  EXPECT_EQ(true, trans_desc.is_stmt_timeout());
  EXPECT_TRUE(false == trans_desc.is_readonly());
}
// test the set_stmt_participants and get_stmt_participants
TEST_F(TestObTransDefine, trans_desc_set_get_participants)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransDesc trans_desc;
  ObPartitionArray participants;
  ObPartitionKey partition_key(TestObTransDefine::VALID_TABLE_ID,
      TestObTransDefine::VALID_PARTITION_ID,
      TestObTransDefine::VALID_PARTITION_COUNT);
  // EXPECT_EQ(OB_INVALID_ARGUMENT, trans_desc.set_stmt_participants(participants));
  participants.push_back(partition_key);
  ASSERT_EQ(OB_SUCCESS, trans_desc.set_stmt_participants(participants));

  const ObPartitionArray& array = trans_desc.get_stmt_participants();
  ASSERT_EQ(participants.count(), array.count());
  const ObPartitionKey& partition = array.at(0);
  ASSERT_EQ(partition_key, partition);
}

// test merge_participants
TEST_F(TestObTransDefine, trans_desc_merage_participants)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransDesc trans_desc;
  ObPartitionArray stmt_participants;
  ObPartitionKey partition_key(TestObTransDefine::VALID_TABLE_ID,
      TestObTransDefine::VALID_PARTITION_ID,
      TestObTransDefine::VALID_PARTITION_COUNT);
  stmt_participants.push_back(partition_key);
  EXPECT_EQ(OB_SUCCESS, trans_desc.set_stmt_participants(stmt_participants));
  EXPECT_EQ(OB_SUCCESS, trans_desc.merge_participants());
  trans_desc.clear_stmt_participants();
}
// test set/get_stmt_desc
TEST_F(TestObTransDefine, set_get_stmt_desc)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransDesc trans_desc;
  ObPartitionArray stmt_participants;
  ObPartitionKey partition_key(TestObTransDefine::VALID_TABLE_ID,
      TestObTransDefine::VALID_PARTITION_ID,
      TestObTransDefine::VALID_PARTITION_COUNT);
  stmt_participants.push_back(partition_key);
  EXPECT_EQ(OB_SUCCESS, trans_desc.set_stmt_participants(stmt_participants));
  EXPECT_EQ(OB_SUCCESS, trans_desc.merge_participants());

  // stmt_desc
  ObStmtDesc& stmt_desc = trans_desc.get_cur_stmt_desc();
  sql::ObPhyPlanType plan_type = OB_PHY_PLAN_LOCAL;
  stmt::StmtType stmt_type = stmt::T_SELECT;
  int64_t consistency_level = ObTransConsistencyLevel::STRONG;

  stmt_desc.phy_plan_type_ = plan_type;
  stmt_desc.consistency_level_ = consistency_level;
  stmt_desc.stmt_type_ = stmt_type;
  stmt_desc.is_sfu_ = false;
  EXPECT_EQ(consistency_level, stmt_desc.consistency_level_);

  const ObStmtDesc& stmt_desc1 = trans_desc.get_cur_stmt_desc();
  EXPECT_EQ(OB_PHY_PLAN_LOCAL, stmt_desc1.phy_plan_type_);
  EXPECT_EQ(stmt::T_SELECT, stmt_desc1.stmt_type_);
  EXPECT_EQ(consistency_level, stmt_desc1.consistency_level_);
}
// test the serialization and deserialization of ObTransDesc
TEST_F(TestObTransDefine, trans_desc_encode_decode)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransDesc trans_desc;
  ObAddr observer(TestObTransDefine::IP_TYPE, TestObTransDefine::LOCAL_IP, TestObTransDefine::PORT);
  ObTransID trans_id(observer);
  EXPECT_EQ(OB_SUCCESS, trans_desc.set_trans_id(trans_id));
  // serialization
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, trans_desc.serialize(buffer, BUFFER_SIZE, pos));
  // deserialization
  ObTransDesc trans_desc1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, trans_desc1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(trans_id, trans_desc1.get_trans_id());
  EXPECT_EQ(trans_desc.get_xid(), trans_desc1.get_xid());
}
// test the parse of ObTransID
TEST_F(TestObTransDefine, trans_id_parse)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObAddr observer(TestObTransDefine::IP_TYPE, TestObTransDefine::LOCAL_IP, TestObTransDefine::PORT);
  ObTransID trans_id(observer);
  char trans_id_buf[128];
  (void)trans_id.to_string(trans_id_buf, 128);
  ObTransID trans_id1;
  EXPECT_EQ(OB_SUCCESS, trans_id1.parse(trans_id_buf));
  EXPECT_EQ(trans_id, trans_id1);
  const char* trans_id_str1 = "invalid trans_id str";
  const char* trans_id_str2 = "{hash:13654146498437109679, inc:23227686, addr:\"127.0.0.1:8080\"}";
  ObTransID trans_id2;
  EXPECT_EQ(OB_INVALID_ARGUMENT, trans_id2.parse(trans_id_str1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, trans_id2.parse(trans_id_str2));
}
///////////////////////////////////////test ObStartTransParam///////////////////////////////////////////////
TEST_F(TestObTransDefine, ob_start_trans_param_is_valid)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  // create an object of ObStartTransParm
  ObStartTransParam parms;
  EXPECT_FALSE(parms.is_valid());

  parms.set_access_mode(ObTransAccessMode::READ_ONLY);
  parms.set_type(ObTransType::TRANS_USER);
  parms.set_isolation(ObTransIsolation::READ_COMMITED);

  EXPECT_TRUE(parms.is_valid());
  parms.reset();
  // check the parameter values after reset
  EXPECT_TRUE(ObTransAccessMode::UNKNOWN == parms.get_access_mode());
  EXPECT_TRUE(ObTransType::UNKNOWN == parms.get_type());
  EXPECT_TRUE(ObTransIsolation::UNKNOWN == parms.get_isolation());
}

// test the serialization and deserialization of ObTransSeqNum
TEST_F(TestObTransDefine, start_trans_param_encode_decode)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObStartTransParam parms;
  EXPECT_FALSE(parms.is_valid());

  parms.set_access_mode(ObTransAccessMode::READ_ONLY);
  parms.set_type(ObTransType::TRANS_USER);
  parms.set_isolation(ObTransIsolation::READ_COMMITED);

  // serialization
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, parms.serialize(buffer, BUFFER_SIZE, pos));
  // deserialization
  ObStartTransParam param1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, param1.deserialize(buffer, pos, start_index));

  // check the values
  EXPECT_TRUE(ObTransAccessMode::READ_ONLY == param1.get_access_mode());
  EXPECT_TRUE(ObTransType::TRANS_USER == param1.get_type());
  EXPECT_TRUE(ObTransIsolation::READ_COMMITED == param1.get_isolation());
}

// test the query operation for trans undo status
TEST_F(TestObTransDefine, test_ob_trans_undo_status)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransUndoStatus undo_status;
  EXPECT_FALSE(undo_status.is_contain(2));
  ASSERT_EQ(OB_SUCCESS, undo_status.undo(2, 3));
  EXPECT_FALSE(undo_status.is_contain(2));
  EXPECT_FALSE(undo_status.is_contain(4));
  EXPECT_TRUE(undo_status.is_contain(3));

  ASSERT_EQ(OB_SUCCESS, undo_status.undo(7, 9));
  EXPECT_FALSE(undo_status.is_contain(2));
  EXPECT_FALSE(undo_status.is_contain(4));
  EXPECT_TRUE(undo_status.is_contain(3));
  EXPECT_FALSE(undo_status.is_contain(7));
  EXPECT_FALSE(undo_status.is_contain(10));
  EXPECT_TRUE(undo_status.is_contain(9));

  ASSERT_EQ(OB_SUCCESS, undo_status.undo(11, 15));
  EXPECT_FALSE(undo_status.is_contain(2));
  EXPECT_FALSE(undo_status.is_contain(4));
  EXPECT_TRUE(undo_status.is_contain(3));
  EXPECT_FALSE(undo_status.is_contain(7));
  EXPECT_FALSE(undo_status.is_contain(10));
  EXPECT_TRUE(undo_status.is_contain(9));
  EXPECT_FALSE(undo_status.is_contain(11));
  EXPECT_FALSE(undo_status.is_contain(16));
  EXPECT_TRUE(undo_status.is_contain(12));
  EXPECT_TRUE(undo_status.is_contain(13));
  EXPECT_TRUE(undo_status.is_contain(14));
  EXPECT_TRUE(undo_status.is_contain(15));

  ASSERT_EQ(OB_SUCCESS, undo_status.undo(15, 20));
  EXPECT_FALSE(undo_status.is_contain(2));
  EXPECT_FALSE(undo_status.is_contain(4));
  EXPECT_TRUE(undo_status.is_contain(3));
  EXPECT_FALSE(undo_status.is_contain(7));
  EXPECT_FALSE(undo_status.is_contain(10));
  EXPECT_TRUE(undo_status.is_contain(9));
  EXPECT_FALSE(undo_status.is_contain(11));
  EXPECT_TRUE(undo_status.is_contain(16));
  EXPECT_TRUE(undo_status.is_contain(12));
  EXPECT_TRUE(undo_status.is_contain(13));
  EXPECT_TRUE(undo_status.is_contain(14));
  EXPECT_TRUE(undo_status.is_contain(15));
  EXPECT_TRUE(undo_status.is_contain(19));
  EXPECT_TRUE(undo_status.is_contain(20));

  ASSERT_EQ(OB_SUCCESS, undo_status.undo(21, 30));
  EXPECT_FALSE(undo_status.is_contain(2));
  EXPECT_FALSE(undo_status.is_contain(4));
  EXPECT_TRUE(undo_status.is_contain(3));
  EXPECT_FALSE(undo_status.is_contain(7));
  EXPECT_FALSE(undo_status.is_contain(10));
  EXPECT_TRUE(undo_status.is_contain(9));
  EXPECT_FALSE(undo_status.is_contain(11));
  EXPECT_TRUE(undo_status.is_contain(16));
  EXPECT_TRUE(undo_status.is_contain(12));
  EXPECT_TRUE(undo_status.is_contain(13));
  EXPECT_TRUE(undo_status.is_contain(14));
  EXPECT_TRUE(undo_status.is_contain(15));
  EXPECT_TRUE(undo_status.is_contain(19));
  EXPECT_TRUE(undo_status.is_contain(20));
  EXPECT_FALSE(undo_status.is_contain(21));
  EXPECT_FALSE(undo_status.is_contain(31));
  EXPECT_TRUE(undo_status.is_contain(22));
  EXPECT_TRUE(undo_status.is_contain(30));

  ASSERT_EQ(OB_SUCCESS, undo_status.undo(40, 50));
  ASSERT_EQ(OB_SUCCESS, undo_status.undo(60, 70));
  ASSERT_EQ(OB_SUCCESS, undo_status.undo(35, 75));
  EXPECT_TRUE(undo_status.is_contain(40));
  EXPECT_TRUE(undo_status.is_contain(50));
  EXPECT_TRUE(undo_status.is_contain(60));
  EXPECT_TRUE(undo_status.is_contain(70));
  EXPECT_TRUE(undo_status.is_contain(75));
  EXPECT_FALSE(undo_status.is_contain(35));

  ASSERT_EQ(OB_SUCCESS, undo_status.undo(100, 200));
  ASSERT_EQ(OB_SUCCESS, undo_status.undo(90, 190));
  EXPECT_FALSE(undo_status.is_contain(91));
  ASSERT_EQ(OB_SUCCESS, undo_status.undo(250, 300));
  ASSERT_EQ(OB_SUCCESS, undo_status.undo(100, 300));
  EXPECT_TRUE(undo_status.is_contain(300));
  EXPECT_FALSE(undo_status.is_contain(100));
}
}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  system("rm -rf test_ob_trans_define.log*");
  logger.set_file_name("test_ob_trans_define.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
