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

#define protected public
#include "storage/tx/ob_tx_msg.h"
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_clock_generator.h"

namespace oceanbase
{
using namespace common;
using namespace transaction;
namespace unittest
{

class TestObTxMsg : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}

  // define port
  static const int32_t PORT = 8080;
  // ObAddr using ipv4 for test
  static const ObAddr::VER IP_TYPE = ObAddr::IPV4;
  // ip address for test
  static const char *LOCAL_IP;

  static const share::ObLSID VALID_LS_ID;
  static const share::ObLSID INVALID_LS_ID;
};
const char *TestObTxMsg::LOCAL_IP = "127.0.0.1";
const share::ObLSID TestObTxMsg::VALID_LS_ID = share::ObLSID(1);
const share::ObLSID TestObTxMsg::INVALID_LS_ID = share::ObLSID(-1);

class MockObTxDesc
{
public:
  MockObTxDesc()
  {
    cluster_version_ = oceanbase::common::cal_version(4, 0, 0, 0);
    tenant_id_ = 1001;
    tx_id_ = 1;
    receiver_ = TestObTxMsg::VALID_LS_ID;
    epoch_ = 1;
    self_ = ObAddr(TestObTxMsg::IP_TYPE, TestObTxMsg::LOCAL_IP, TestObTxMsg::PORT);
    cluster_id_ = 1;
    op_sn_ = 1;
  }
  ~MockObTxDesc() {}
  int build_tx_commit_msg(ObTxCommitMsg &msg)
  {
    int ret = OB_SUCCESS;
    msg.cluster_version_ = cluster_version_;
    msg.tenant_id_ = tenant_id_;
    msg.tx_id_ = tx_id_;
    msg.receiver_ = receiver_;
    msg.sender_addr_ = self_;
    msg.sender_ = share::SCHEDULER_LS;
    msg.cluster_id_ = cluster_id_;
    msg.timestamp_ = op_sn_;
    msg.epoch_ = epoch_;
    msg.request_id_ = op_sn_;
    msg.expire_ts_ = INT_MAX64;
    if (OB_FAIL(msg.parts_.push_back(TestObTxMsg::VALID_LS_ID))) {
      TRANS_LOG(WARN, "push_back parts fail", K(ret));
    }
    return ret;
  }
  void build_tx_commit_resp_msg(ObTxCommitRespMsg &msg)
  {
    msg.cluster_version_ = cluster_version_;
    msg.tenant_id_ = tenant_id_;
    msg.tx_id_ = tx_id_;
    msg.receiver_ = receiver_;
    msg.sender_addr_ = self_;
    msg.sender_ = share::SCHEDULER_LS;
    msg.cluster_id_ = cluster_id_;
    msg.timestamp_ = op_sn_;
    msg.epoch_ = epoch_;
    msg.request_id_ = op_sn_;
    msg.commit_version_ = share::SCN::max_scn();
    msg.ret_ = OB_SUCCESS;
  }
  void build_tx_abort_msg(ObTxAbortMsg &msg)
  {
    msg.cluster_version_ = cluster_version_;
    msg.tenant_id_ = tenant_id_;
    msg.tx_id_ = tx_id_;
    msg.receiver_ = receiver_;
    msg.sender_addr_ = self_;
    msg.sender_ = share::SCHEDULER_LS;
    msg.cluster_id_ = cluster_id_;
    msg.timestamp_ = op_sn_;
    msg.epoch_ = epoch_;
    msg.request_id_ = op_sn_;
    msg.reason_ = 0;
  }
  void build_tx_rollback_sp_msg(ObTxRollbackSPMsg &msg, ObTxDesc *tx)
  {
    msg.cluster_version_ = cluster_version_;
    msg.tenant_id_ = tenant_id_;
    msg.tx_id_ = tx_id_;
    msg.receiver_ = receiver_;
    msg.sender_addr_ = self_;
    msg.sender_ = share::SCHEDULER_LS;
    msg.cluster_id_ = cluster_id_;
    msg.timestamp_ = op_sn_;
    msg.epoch_ = -1;
    msg.request_id_ = op_sn_;
    msg.savepoint_ = ObTxSEQ(1, 0);
    msg.op_sn_ = op_sn_;
    msg.tx_seq_base_ = 10000000001;
    msg.tx_ptr_ = tx;
  }
  void build_tx_keepalive_msg(ObTxKeepaliveMsg &msg)
  {
    msg.cluster_version_ = cluster_version_;
    msg.tenant_id_ = tenant_id_;
    msg.cluster_id_ = cluster_id_;
    msg.request_id_ = op_sn_;
    msg.tx_id_ = tx_id_;
    msg.sender_addr_ = self_;
    msg.sender_ = share::ObLSID(1000001);
    msg.receiver_ = share::SCHEDULER_LS;
    msg.epoch_ = 1000;
    msg.status_ = OB_SUCCESS;
  }
  int build_tx_2pc_prepare_msg(Ob2pcPrepareReqMsg &msg)
  {
    int ret = OB_SUCCESS;
    msg.cluster_version_ = cluster_version_;
    msg.tenant_id_ = tenant_id_;
    msg.tx_id_ = tx_id_;
    msg.receiver_ = receiver_;
    msg.sender_addr_ = self_;
    msg.sender_ = share::SCHEDULER_LS;
    msg.cluster_id_ = cluster_id_;
    msg.timestamp_ = op_sn_;
    msg.epoch_ = -1;
    msg.request_id_ = op_sn_;
    msg.upstream_ = receiver_;
    msg.app_trace_info_ = ObString("test");
    return ret;
  }
  int build_tx_2pc_prepare_resp_msg(Ob2pcPrepareRespMsg &msg)
  {
    int ret = OB_SUCCESS;
    msg.cluster_version_ = cluster_version_;
    msg.tenant_id_ = tenant_id_;
    msg.tx_id_ = tx_id_;
    msg.receiver_ = receiver_;
    msg.sender_addr_ = self_;
    msg.sender_ = share::SCHEDULER_LS;
    msg.cluster_id_ = cluster_id_;
    msg.timestamp_ = op_sn_;
    msg.epoch_ = -1;
    msg.request_id_ = op_sn_;
    msg.prepare_version_ = share::SCN::max_scn();
    if (OB_FAIL(msg.prepare_info_array_.push_back(ObLSLogInfo()))) {
      TRANS_LOG(WARN, "push_back info fail", K(ret));
    }
    return ret;
  }
  void build_tx_2pc_pre_commit_msg(Ob2pcPreCommitReqMsg &msg)
  {
    msg.cluster_version_ = cluster_version_;
    msg.tenant_id_ = tenant_id_;
    msg.tx_id_ = tx_id_;
    msg.receiver_ = receiver_;
    msg.sender_addr_ = self_;
    msg.sender_ = share::SCHEDULER_LS;
    msg.cluster_id_ = cluster_id_;
    msg.timestamp_ = op_sn_;
    msg.epoch_ = -1;
    msg.request_id_ = op_sn_;
    msg.commit_version_ = share::SCN::max_scn();
  }
  void build_tx_2pc_pre_commit_resp_msg(Ob2pcPreCommitRespMsg &msg)
  {
    msg.cluster_version_ = cluster_version_;
    msg.tenant_id_ = tenant_id_;
    msg.tx_id_ = tx_id_;
    msg.receiver_ = receiver_;
    msg.sender_addr_ = self_;
    msg.sender_ = share::SCHEDULER_LS;
    msg.cluster_id_ = cluster_id_;
    msg.timestamp_ = op_sn_;
    msg.epoch_ = -1;
    msg.request_id_ = op_sn_;
    msg.commit_version_ = share::SCN::max_scn();
  }
  int build_tx_2pc_commit_msg(Ob2pcCommitReqMsg &msg)
  {
    int ret = OB_SUCCESS;
    msg.cluster_version_ = cluster_version_;
    msg.tenant_id_ = tenant_id_;
    msg.tx_id_ = tx_id_;
    msg.receiver_ = receiver_;
    msg.sender_addr_ = self_;
    msg.sender_ = share::SCHEDULER_LS;
    msg.cluster_id_ = cluster_id_;
    msg.timestamp_ = op_sn_;
    msg.epoch_ = -1;
    msg.request_id_ = op_sn_;
    msg.commit_version_ = share::SCN::max_scn();
    if (OB_FAIL(msg.prepare_info_array_.push_back(ObLSLogInfo()))) {
      TRANS_LOG(WARN, "push_back info fail", K(ret));
    }
    return ret;
  }
  void build_tx_2pc_abort_msg(Ob2pcAbortReqMsg &msg)
  {
    msg.cluster_version_ = cluster_version_;
    msg.tenant_id_ = tenant_id_;
    msg.tx_id_ = tx_id_;
    msg.receiver_ = receiver_;
    msg.sender_addr_ = self_;
    msg.sender_ = share::SCHEDULER_LS;
    msg.cluster_id_ = cluster_id_;
    msg.timestamp_ = op_sn_;
    msg.epoch_ = -1;
    msg.request_id_ = op_sn_;
  }
  int build_tx_sub_prepare_msg(ObTxSubPrepareMsg &msg)
  {
    int ret = OB_SUCCESS;
    msg.cluster_version_ = cluster_version_;
    msg.tenant_id_ = tenant_id_;
    msg.tx_id_ = tx_id_;
    msg.receiver_ = receiver_;
    msg.sender_addr_ = self_;
    msg.sender_ = share::SCHEDULER_LS;
    msg.cluster_id_ = cluster_id_;
    msg.timestamp_ = op_sn_;
    msg.epoch_ = -1;
    msg.expire_ts_ = INT_MAX64;
    msg.request_id_ = op_sn_;
    msg.xid_ = ObXATransID();
    msg.xid_.set(ObString("aaa"), ObString("bbb"), 1);
    if (OB_FAIL(msg.parts_.push_back(TestObTxMsg::VALID_LS_ID))) {
      TRANS_LOG(WARN, "push_back parts fail", K(ret));
    }
    return ret;
  }
  void build_tx_sub_commit_msg(ObTxSubCommitMsg &msg)
  {
    msg.cluster_version_ = cluster_version_;
    msg.tenant_id_ = tenant_id_;
    msg.tx_id_ = tx_id_;
    msg.receiver_ = receiver_;
    msg.sender_addr_ = self_;
    msg.sender_ = share::SCHEDULER_LS;
    msg.cluster_id_ = cluster_id_;
    msg.timestamp_ = op_sn_;
    msg.epoch_ = -1;
    msg.request_id_ = op_sn_;
    msg.xid_ = ObXATransID();
    msg.xid_.set(ObString("aaa"), ObString("bbb"), 1);
  }
  void build_tx_sub_rollback_msg(ObTxSubRollbackMsg &msg)
  {
    msg.cluster_version_ = cluster_version_;
    msg.tenant_id_ = tenant_id_;
    msg.tx_id_ = tx_id_;
    msg.receiver_ = receiver_;
    msg.sender_addr_ = self_;
    msg.sender_ = share::SCHEDULER_LS;
    msg.cluster_id_ = cluster_id_;
    msg.timestamp_ = op_sn_;
    msg.epoch_ = -1;
    msg.request_id_ = op_sn_;
    msg.xid_ = ObXATransID();
    msg.xid_.set(ObString("aaa"), ObString("bbb"), 1);
  }
public:
  int64_t cluster_version_;
  uint64_t tenant_id_;
  ObTransID tx_id_;
  share::ObLSID receiver_;
  int64_t epoch_;
  int64_t cluster_id_;
  common::ObAddr self_;
  uint64_t op_sn_;
};

TEST_F(TestObTxMsg, trans_commit_msg)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockObTxDesc tx;
  ObTxCommitMsg msg;
  ASSERT_EQ(OB_SUCCESS, tx.build_tx_commit_msg(msg));
  ASSERT_TRUE(msg.is_valid());

  // test the serialization of ObTransMsg
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, msg.serialize(buffer, BUFFER_SIZE, pos));

  // test the deserialization of ObTransMsg
  ObTxCommitMsg msg1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, msg1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(msg.get_msg_type(), msg1.get_msg_type());
  EXPECT_EQ(msg.cluster_version_, msg1.cluster_version_);
  EXPECT_EQ(msg.get_tenant_id(), msg1.get_tenant_id());
  EXPECT_EQ(msg.get_trans_id(), msg1.get_trans_id());
  EXPECT_EQ(msg.get_receiver(), msg1.get_receiver());
  EXPECT_EQ(msg.get_epoch(), msg1.get_epoch());
  EXPECT_EQ(msg.get_sender_addr(), msg1.get_sender_addr());
  EXPECT_EQ(msg.get_sender(), msg1.get_sender());
  EXPECT_EQ(msg.get_request_id(), msg1.get_request_id());
  EXPECT_EQ(msg.get_timestamp(), msg1.get_timestamp());
  EXPECT_EQ(msg.cluster_id_, msg1.cluster_id_);
  EXPECT_EQ(msg.expire_ts_, msg1.expire_ts_);
  EXPECT_EQ(msg.parts_.count(), msg1.parts_.count());
}

TEST_F(TestObTxMsg, trans_commit_resp_msg)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockObTxDesc tx;
  ObTxCommitRespMsg msg;
  tx.build_tx_commit_resp_msg(msg);
  ASSERT_TRUE(msg.is_valid());

  // test the serialization of ObTransMsg
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, msg.serialize(buffer, BUFFER_SIZE, pos));

  // test the deserialization of ObTransMsg
  ObTxCommitRespMsg msg1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, msg1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(msg.get_msg_type(), msg1.get_msg_type());
  EXPECT_EQ(msg.cluster_version_, msg1.cluster_version_);
  EXPECT_EQ(msg.get_tenant_id(), msg1.get_tenant_id());
  EXPECT_EQ(msg.get_trans_id(), msg1.get_trans_id());
  EXPECT_EQ(msg.get_receiver(), msg1.get_receiver());
  EXPECT_EQ(msg.get_epoch(), msg1.get_epoch());
  EXPECT_EQ(msg.get_sender_addr(), msg1.get_sender_addr());
  EXPECT_EQ(msg.get_sender(), msg1.get_sender());
  EXPECT_EQ(msg.get_request_id(), msg1.get_request_id());
  EXPECT_EQ(msg.get_timestamp(), msg1.get_timestamp());
  EXPECT_EQ(msg.cluster_id_, msg1.cluster_id_);
  EXPECT_EQ(msg.ret_, msg1.ret_);
  EXPECT_EQ(msg.commit_version_, msg1.commit_version_);
}

TEST_F(TestObTxMsg, trans_abort_msg)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockObTxDesc tx;
  ObTxAbortMsg msg;
  tx.build_tx_abort_msg(msg);
  ASSERT_TRUE(msg.is_valid());

  // test the serialization of ObTransMsg
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, msg.serialize(buffer, BUFFER_SIZE, pos));

  // test the deserialization of ObTransMsg
  ObTxAbortMsg msg1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, msg1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(msg.get_msg_type(), msg1.get_msg_type());
  EXPECT_EQ(msg.cluster_version_, msg1.cluster_version_);
  EXPECT_EQ(msg.get_tenant_id(), msg1.get_tenant_id());
  EXPECT_EQ(msg.get_trans_id(), msg1.get_trans_id());
  EXPECT_EQ(msg.get_receiver(), msg1.get_receiver());
  EXPECT_EQ(msg.get_epoch(), msg1.get_epoch());
  EXPECT_EQ(msg.get_sender_addr(), msg1.get_sender_addr());
  EXPECT_EQ(msg.get_sender(), msg1.get_sender());
  EXPECT_EQ(msg.get_request_id(), msg1.get_request_id());
  EXPECT_EQ(msg.get_timestamp(), msg1.get_timestamp());
  EXPECT_EQ(msg.cluster_id_, msg1.cluster_id_);
  EXPECT_EQ(msg.reason_, msg1.reason_);
}

TEST_F(TestObTxMsg, trans_rollback_sp_msg)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockObTxDesc tx;
  ObTxRollbackSPMsg msg;
  ObTxDesc tx1;
  ObTxPart p;
  p.id_ = TestObTxMsg::VALID_LS_ID;
  tx1.parts_.push_back(p);
  tx.build_tx_rollback_sp_msg(msg, &tx1);
  ASSERT_TRUE(msg.is_valid());

  // test the serialization of ObTransMsg
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, msg.serialize(buffer, BUFFER_SIZE, pos));

  // test the deserialization of ObTransMsg
  ObTxRollbackSPMsg msg1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, msg1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(msg.get_msg_type(), msg1.get_msg_type());
  EXPECT_EQ(msg.cluster_version_, msg1.cluster_version_);
  EXPECT_EQ(msg.get_tenant_id(), msg1.get_tenant_id());
  EXPECT_EQ(msg.get_trans_id(), msg1.get_trans_id());
  EXPECT_EQ(msg.get_receiver(), msg1.get_receiver());
  EXPECT_EQ(msg.get_epoch(), msg1.get_epoch());
  EXPECT_EQ(msg.get_sender_addr(), msg1.get_sender_addr());
  EXPECT_EQ(msg.get_sender(), msg1.get_sender());
  EXPECT_EQ(msg.get_request_id(), msg1.get_request_id());
  EXPECT_EQ(msg.get_timestamp(), msg1.get_timestamp());
  EXPECT_EQ(msg.cluster_id_, msg1.cluster_id_);
  EXPECT_EQ(msg.savepoint_, msg1.savepoint_);
  EXPECT_EQ(msg.op_sn_, msg1.op_sn_);
  EXPECT_EQ(msg.tx_seq_base_, msg1.tx_seq_base_);
  EXPECT_EQ(msg.tx_ptr_->parts_[0].id_, msg1.tx_ptr_->parts_[0].id_);
  if (OB_NOT_NULL(msg.tx_ptr_)) {
    msg.tx_ptr_ = NULL;
  }
}

TEST_F(TestObTxMsg, trans_keepalive_msg)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockObTxDesc tx;
  ObTxKeepaliveMsg msg;
  tx.build_tx_keepalive_msg(msg);
  ASSERT_TRUE(msg.is_valid());

  // test the serialization of ObTransMsg
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, msg.serialize(buffer, BUFFER_SIZE, pos));

  // test the deserialization of ObTransMsg
  ObTxKeepaliveMsg msg1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, msg1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(msg.get_msg_type(), msg1.get_msg_type());
  EXPECT_EQ(msg.cluster_version_, msg1.cluster_version_);
  EXPECT_EQ(msg.get_tenant_id(), msg1.get_tenant_id());
  EXPECT_EQ(msg.get_trans_id(), msg1.get_trans_id());
  EXPECT_EQ(msg.get_receiver(), msg1.get_receiver());
  EXPECT_EQ(msg.get_epoch(), msg1.get_epoch());
  EXPECT_EQ(msg.get_sender_addr(), msg1.get_sender_addr());
  EXPECT_EQ(msg.get_sender(), msg1.get_sender());
  EXPECT_EQ(msg.get_request_id(), msg1.get_request_id());
  EXPECT_EQ(msg.get_timestamp(), msg1.get_timestamp());
  EXPECT_EQ(msg.cluster_id_, msg1.cluster_id_);
  EXPECT_EQ(msg.status_, msg1.status_);
}

TEST_F(TestObTxMsg, trans_2pc_prepare_msg)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockObTxDesc tx;
  Ob2pcPrepareReqMsg msg;
  ASSERT_EQ(OB_SUCCESS, tx.build_tx_2pc_prepare_msg(msg));
  ASSERT_TRUE(msg.is_valid());

  // test the serialization of ObTransMsg
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, msg.serialize(buffer, BUFFER_SIZE, pos));

  // test the deserialization of ObTransMsg
  Ob2pcPrepareReqMsg msg1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, msg1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(msg.get_msg_type(), msg1.get_msg_type());
  EXPECT_EQ(msg.cluster_version_, msg1.cluster_version_);
  EXPECT_EQ(msg.get_tenant_id(), msg1.get_tenant_id());
  EXPECT_EQ(msg.get_trans_id(), msg1.get_trans_id());
  EXPECT_EQ(msg.get_receiver(), msg1.get_receiver());
  EXPECT_EQ(msg.get_epoch(), msg1.get_epoch());
  EXPECT_EQ(msg.get_sender_addr(), msg1.get_sender_addr());
  EXPECT_EQ(msg.get_sender(), msg1.get_sender());
  EXPECT_EQ(msg.get_request_id(), msg1.get_request_id());
  EXPECT_EQ(msg.get_timestamp(), msg1.get_timestamp());
  EXPECT_EQ(msg.cluster_id_, msg1.cluster_id_);
  EXPECT_EQ(msg.upstream_, msg1.upstream_);
  EXPECT_EQ(msg.app_trace_info_, msg1.app_trace_info_);
}

TEST_F(TestObTxMsg, trans_2pc_prepare_resp_msg)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockObTxDesc tx;
  Ob2pcPrepareRespMsg msg;
  ASSERT_EQ(OB_SUCCESS, tx.build_tx_2pc_prepare_resp_msg(msg));
  ASSERT_TRUE(msg.is_valid());

  // test the serialization of ObTransMsg
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, msg.serialize(buffer, BUFFER_SIZE, pos));

  // test the deserialization of ObTransMsg
  Ob2pcPrepareRespMsg msg1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, msg1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(msg.get_msg_type(), msg1.get_msg_type());
  EXPECT_EQ(msg.cluster_version_, msg1.cluster_version_);
  EXPECT_EQ(msg.get_tenant_id(), msg1.get_tenant_id());
  EXPECT_EQ(msg.get_trans_id(), msg1.get_trans_id());
  EXPECT_EQ(msg.get_receiver(), msg1.get_receiver());
  EXPECT_EQ(msg.get_epoch(), msg1.get_epoch());
  EXPECT_EQ(msg.get_sender_addr(), msg1.get_sender_addr());
  EXPECT_EQ(msg.get_sender(), msg1.get_sender());
  EXPECT_EQ(msg.get_request_id(), msg1.get_request_id());
  EXPECT_EQ(msg.get_timestamp(), msg1.get_timestamp());
  EXPECT_EQ(msg.cluster_id_, msg1.cluster_id_);
  EXPECT_EQ(msg.prepare_version_, msg1.prepare_version_);
}

TEST_F(TestObTxMsg, trans_2pc_pre_commit_msg)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockObTxDesc tx;
  Ob2pcPreCommitReqMsg msg;
  tx.build_tx_2pc_pre_commit_msg(msg);
  ASSERT_TRUE(msg.is_valid());

  // test the serialization of ObTransMsg
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, msg.serialize(buffer, BUFFER_SIZE, pos));
  // test the deserialization of ObTransMsg
  Ob2pcPreCommitReqMsg msg1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, msg1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(msg.get_msg_type(), msg1.get_msg_type());
  EXPECT_EQ(msg.cluster_version_, msg1.cluster_version_);
  EXPECT_EQ(msg.get_tenant_id(), msg1.get_tenant_id());
  EXPECT_EQ(msg.get_trans_id(), msg1.get_trans_id());
  EXPECT_EQ(msg.get_receiver(), msg1.get_receiver());
  EXPECT_EQ(msg.get_epoch(), msg1.get_epoch());
  EXPECT_EQ(msg.get_sender_addr(), msg1.get_sender_addr());
  EXPECT_EQ(msg.get_sender(), msg1.get_sender());
  EXPECT_EQ(msg.get_request_id(), msg1.get_request_id());
  EXPECT_EQ(msg.get_timestamp(), msg1.get_timestamp());
  EXPECT_EQ(msg.cluster_id_, msg1.cluster_id_);
  EXPECT_EQ(msg.commit_version_, msg1.commit_version_);
}

TEST_F(TestObTxMsg, trans_2pc_pre_commit_resp_msg)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockObTxDesc tx;
  Ob2pcPreCommitRespMsg msg;
  tx.build_tx_2pc_pre_commit_resp_msg(msg);
  ASSERT_TRUE(msg.is_valid());

  // test the serialization of ObTransMsg
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, msg.serialize(buffer, BUFFER_SIZE, pos));

  // test the deserialization of ObTransMsg
  Ob2pcPreCommitRespMsg msg1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, msg1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(msg.get_msg_type(), msg1.get_msg_type());
  EXPECT_EQ(msg.cluster_version_, msg1.cluster_version_);
  EXPECT_EQ(msg.get_tenant_id(), msg1.get_tenant_id());
  EXPECT_EQ(msg.get_trans_id(), msg1.get_trans_id());
  EXPECT_EQ(msg.get_receiver(), msg1.get_receiver());
  EXPECT_EQ(msg.get_epoch(), msg1.get_epoch());
  EXPECT_EQ(msg.get_sender_addr(), msg1.get_sender_addr());
  EXPECT_EQ(msg.get_sender(), msg1.get_sender());
  EXPECT_EQ(msg.get_request_id(), msg1.get_request_id());
  EXPECT_EQ(msg.get_timestamp(), msg1.get_timestamp());
  EXPECT_EQ(msg.cluster_id_, msg1.cluster_id_);
  EXPECT_EQ(msg.commit_version_, msg1.commit_version_);
}

TEST_F(TestObTxMsg, trans_2pc_commit_msg)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockObTxDesc tx;
  Ob2pcCommitReqMsg msg;
  ASSERT_EQ(OB_SUCCESS, tx.build_tx_2pc_commit_msg(msg));
  ASSERT_TRUE(msg.is_valid());

  // test the serialization of ObTransMsg
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, msg.serialize(buffer, BUFFER_SIZE, pos));

  // test the deserialization of ObTransMsg
  Ob2pcCommitReqMsg msg1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, msg1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(msg.get_msg_type(), msg1.get_msg_type());
  EXPECT_EQ(msg.cluster_version_, msg1.cluster_version_);
  EXPECT_EQ(msg.get_tenant_id(), msg1.get_tenant_id());
  EXPECT_EQ(msg.get_trans_id(), msg1.get_trans_id());
  EXPECT_EQ(msg.get_receiver(), msg1.get_receiver());
  EXPECT_EQ(msg.get_epoch(), msg1.get_epoch());
  EXPECT_EQ(msg.get_sender_addr(), msg1.get_sender_addr());
  EXPECT_EQ(msg.get_sender(), msg1.get_sender());
  EXPECT_EQ(msg.get_request_id(), msg1.get_request_id());
  EXPECT_EQ(msg.get_timestamp(), msg1.get_timestamp());
  EXPECT_EQ(msg.cluster_id_, msg1.cluster_id_);
  EXPECT_EQ(msg.commit_version_, msg1.commit_version_);
}

TEST_F(TestObTxMsg, trans_2pc_abort_msg)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockObTxDesc tx;
  Ob2pcAbortReqMsg msg;
  tx.build_tx_2pc_abort_msg(msg);
  ASSERT_TRUE(msg.is_valid());

  // test the serialization of ObTransMsg
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, msg.serialize(buffer, BUFFER_SIZE, pos));

  // test the deserialization of ObTransMsg
  Ob2pcAbortReqMsg msg1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, msg1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(msg.get_msg_type(), msg1.get_msg_type());
  EXPECT_EQ(msg.cluster_version_, msg1.cluster_version_);
  EXPECT_EQ(msg.get_tenant_id(), msg1.get_tenant_id());
  EXPECT_EQ(msg.get_trans_id(), msg1.get_trans_id());
  EXPECT_EQ(msg.get_receiver(), msg1.get_receiver());
  EXPECT_EQ(msg.get_epoch(), msg1.get_epoch());
  EXPECT_EQ(msg.get_sender_addr(), msg1.get_sender_addr());
  EXPECT_EQ(msg.get_sender(), msg1.get_sender());
  EXPECT_EQ(msg.get_request_id(), msg1.get_request_id());
  EXPECT_EQ(msg.get_timestamp(), msg1.get_timestamp());
  EXPECT_EQ(msg.cluster_id_, msg1.cluster_id_);
}

TEST_F(TestObTxMsg, trans_sub_prepare_msg)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockObTxDesc tx;
  ObTxSubPrepareMsg msg;
  ASSERT_EQ(OB_SUCCESS, tx.build_tx_sub_prepare_msg(msg));
  ASSERT_TRUE(msg.is_valid());

  // test the serialization of ObTransMsg
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, msg.serialize(buffer, BUFFER_SIZE, pos));

  // test the deserialization of ObTransMsg
  ObTxSubPrepareMsg msg1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, msg1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(msg.get_msg_type(), msg1.get_msg_type());
  EXPECT_EQ(msg.cluster_version_, msg1.cluster_version_);
  EXPECT_EQ(msg.get_tenant_id(), msg1.get_tenant_id());
  EXPECT_EQ(msg.get_trans_id(), msg1.get_trans_id());
  EXPECT_EQ(msg.get_receiver(), msg1.get_receiver());
  EXPECT_EQ(msg.get_epoch(), msg1.get_epoch());
  EXPECT_EQ(msg.get_sender_addr(), msg1.get_sender_addr());
  EXPECT_EQ(msg.get_sender(), msg1.get_sender());
  EXPECT_EQ(msg.get_request_id(), msg1.get_request_id());
  EXPECT_EQ(msg.get_timestamp(), msg1.get_timestamp());
  EXPECT_EQ(msg.cluster_id_, msg1.cluster_id_);
  EXPECT_EQ(msg.xid_, msg1.xid_);
  EXPECT_EQ(msg.parts_.count(), msg1.parts_.count());
}

TEST_F(TestObTxMsg, trans_sub_commit_msg)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockObTxDesc tx;
  ObTxSubCommitMsg msg;
  tx.build_tx_sub_commit_msg(msg);
  ASSERT_TRUE(msg.is_valid());

  // test the serialization of ObTransMsg
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, msg.serialize(buffer, BUFFER_SIZE, pos));

  // test the deserialization of ObTransMsg
  ObTxSubCommitMsg msg1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, msg1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(msg.get_msg_type(), msg1.get_msg_type());
  EXPECT_EQ(msg.cluster_version_, msg1.cluster_version_);
  EXPECT_EQ(msg.get_tenant_id(), msg1.get_tenant_id());
  EXPECT_EQ(msg.get_trans_id(), msg1.get_trans_id());
  EXPECT_EQ(msg.get_receiver(), msg1.get_receiver());
  EXPECT_EQ(msg.get_epoch(), msg1.get_epoch());
  EXPECT_EQ(msg.get_sender_addr(), msg1.get_sender_addr());
  EXPECT_EQ(msg.get_sender(), msg1.get_sender());
  EXPECT_EQ(msg.get_request_id(), msg1.get_request_id());
  EXPECT_EQ(msg.get_timestamp(), msg1.get_timestamp());
  EXPECT_EQ(msg.cluster_id_, msg1.cluster_id_);
  EXPECT_EQ(msg.xid_, msg1.xid_);
}

TEST_F(TestObTxMsg, trans_sub_rollback_msg)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  MockObTxDesc tx;
  ObTxSubRollbackMsg msg;
  tx.build_tx_sub_rollback_msg(msg);
  ASSERT_TRUE(msg.is_valid());

  // test the serialization of ObTransMsg
  int64_t pos = 0;
  const int64_t BUFFER_SIZE = 10240;
  char buffer[BUFFER_SIZE];
  ASSERT_EQ(OB_SUCCESS, msg.serialize(buffer, BUFFER_SIZE, pos));

  // test the deserialization of ObTransMsg
  ObTxSubRollbackMsg msg1;
  int64_t start_index = 0;
  ASSERT_EQ(OB_SUCCESS, msg1.deserialize(buffer, pos, start_index));
  EXPECT_EQ(msg.get_msg_type(), msg1.get_msg_type());
  EXPECT_EQ(msg.cluster_version_, msg1.cluster_version_);
  EXPECT_EQ(msg.get_tenant_id(), msg1.get_tenant_id());
  EXPECT_EQ(msg.get_trans_id(), msg1.get_trans_id());
  EXPECT_EQ(msg.get_receiver(), msg1.get_receiver());
  EXPECT_EQ(msg.get_epoch(), msg1.get_epoch());
  EXPECT_EQ(msg.get_sender_addr(), msg1.get_sender_addr());
  EXPECT_EQ(msg.get_sender(), msg1.get_sender());
  EXPECT_EQ(msg.get_request_id(), msg1.get_request_id());
  EXPECT_EQ(msg.get_timestamp(), msg1.get_timestamp());
  EXPECT_EQ(msg.cluster_id_, msg1.cluster_id_);
  EXPECT_EQ(msg.xid_, msg1.xid_);
}

}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_tx_msg.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
