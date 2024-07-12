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

#define OB_TX_LOG_TEST
#include <gtest/gtest.h>
#define private public
#include "storage/tx/ob_tx_log.h"
#include "logservice/ob_log_base_header.h"
#include "lib/container/ob_array_helper.h"
void ob_abort (void) __THROW {}
namespace oceanbase
{
using namespace common;
using namespace transaction;
using namespace storage;
using namespace share;

namespace unittest
{
class TestObTxLog : public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}
public:
};

//const TEST
TxID TEST_TX_ID = 1024;
int64_t TEST_CLUSTER_VERSION = DATA_VERSION_4_3_0_0;
ObAddr TEST_ADDR(ObAddr::VER::IPV4,"1.0.0.1",606);
int TEST_TRANS_TYPE = 1;
int TEST_SESSION_ID = 56831;
common::ObString TEST_TRACE_ID_STR("trace_id_test");
bool TEST_CAN_ELR =  false;
int64_t TEST_QUERY_TIME = 90000;
bool TEST_IS_DUP = true;
bool TEST_IS_SUB2PC = false;
int64_t TEST_EPOCH = 1315;
int64_t TEST_LAST_OP_SN = 1315;
auto TEST_FIRST_SCN = ObTxSEQ(1315, 0);
auto TEST_LAST_SCN = ObTxSEQ(1315, 0);
uint64_t TEST_ORG_CLUSTER_ID = 1208;
common::ObString TEST_TRCE_INFO("trace_info_test");
LogOffSet TEST_LOG_OFFSET(10);
int64_t TEST_COMMIT_VERSION = 190878;
int64_t TEST_CHECKSUM = 29890209;
ObArray<uint8_t> TEST_CHECKSUM_SIGNATURE_ARRAY;
int64_t TEST_SCHEMA_VERSION = 372837;
int64_t TEST_TX_EXPIRED_TIME = 12099087;
int64_t TEST_LOG_ENTRY_NO = 1233;
ObTxSEQ TEST_MAX_SUBMITTED_SEQ_NO = ObTxSEQ(12345, 0);
ObTxSEQ TEST_SERIAL_FINAL_SEQ_NO = ObTxSEQ(12346, 0);
LSKey TEST_LS_KEY;
ObXATransID TEST_XID;
ObTxPrevLogType TEST_PREV_LOG_TYPE(ObTxPrevLogType::TypeEnum::TRANSFER_IN);


struct OldTestLog
{
  ObTxSerCompatByte compat_bytes_;
  int64_t tx_id_1;
  int64_t tx_id_2;

  OldTestLog()
  {
    tx_id_1 = 0;
    tx_id_2 = 0;
    int ret = OB_SUCCESS;
    if (OB_FAIL(compat_bytes_.init(2))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }
  OB_UNIS_VERSION(1);
};

OB_TX_SERIALIZE_MEMBER(OldTestLog, compat_bytes_, tx_id_1, tx_id_2);

struct NewTestLog
{
  ObTxSerCompatByte compat_bytes_;
  int64_t tx_id_1;
  int64_t tx_id_2;
  int64_t tx_id_3;

  NewTestLog()
  {
    tx_id_1 = 0;
    tx_id_2 = 0;
    tx_id_3 = 0;
    int ret = OB_SUCCESS;
    if (OB_FAIL(compat_bytes_.init(2))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }
  OB_UNIS_VERSION(1);
};

OB_TX_SERIALIZE_MEMBER(NewTestLog, compat_bytes_, tx_id_1, tx_id_2, tx_id_3);

// test ObTxLogBlockHeader
TEST_F(TestObTxLog, tx_log_block_header)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int64_t pos = 0;
  ObTxLogBlock fill_block, replay_block;

  ObTxLogBlockHeader &fill_block_header =  fill_block.get_header();
  fill_block_header.init(TEST_ORG_CLUSTER_ID, TEST_CLUSTER_VERSION, TEST_LOG_ENTRY_NO, ObTransID(TEST_TX_ID), TEST_ADDR);
  fill_block_header.set_serial_final();
  ASSERT_TRUE(fill_block_header.is_serial_final());
  ASSERT_EQ(OB_SUCCESS, fill_block.init_for_fill());
  fill_block.seal(TEST_TX_ID);
  // check log_block_header
  char *buf = fill_block.get_buf();
  logservice::ObLogBaseHeader base_header_1;


  pos = 0;
  base_header_1.deserialize(buf, base_header_1.get_serialize_size(),  pos);
  EXPECT_EQ(base_header_1.get_log_type() , ObTxLogBlock::DEFAULT_LOG_BLOCK_TYPE);
  EXPECT_EQ(base_header_1.get_replay_hint(), TEST_TX_ID);

  ObTxLogBlockHeader &replay_block_header = replay_block.get_header();
  ASSERT_EQ(OB_SUCCESS, replay_block.init_for_replay(buf, fill_block.get_size()));
  uint64_t tmp_cluster_id = replay_block_header.get_org_cluster_id();
  EXPECT_EQ(TEST_ORG_CLUSTER_ID, tmp_cluster_id);
  EXPECT_EQ(replay_block.get_log_base_header().get_replay_hint(), TEST_TX_ID);
  EXPECT_EQ(TEST_CLUSTER_VERSION, replay_block_header.get_cluster_version());
  EXPECT_EQ(TEST_LOG_ENTRY_NO, replay_block_header.get_log_entry_no());
  EXPECT_EQ(fill_block_header.flags(), replay_block_header.flags());
  EXPECT_TRUE(replay_block_header.is_serial_final());

  // reuse
  fill_block.get_header().init(TEST_ORG_CLUSTER_ID + 1, TEST_CLUSTER_VERSION + 1, TEST_LOG_ENTRY_NO + 1, ObTransID(TEST_TX_ID + 1), TEST_ADDR);
  fill_block.reuse_for_fill();
  fill_block.seal(TEST_TX_ID + 1);
  buf = fill_block.get_buf();
  pos = 0;

  logservice::ObLogBaseHeader base_header_2;
  base_header_2.deserialize(buf, base_header_2.get_serialize_size(),  pos);
  EXPECT_EQ(base_header_2.get_log_type() , ObTxLogBlock::DEFAULT_LOG_BLOCK_TYPE);
  EXPECT_EQ(base_header_2.get_replay_hint(), TEST_TX_ID + 1);
  ObTxLogBlock replay_block2;
  ObTxLogBlockHeader &replay_block_header2 = replay_block2.get_header();
  ASSERT_EQ(OB_SUCCESS, replay_block2.init_for_replay(buf, fill_block.get_size(), pos));
  EXPECT_EQ(TEST_ORG_CLUSTER_ID + 1, replay_block_header2.get_org_cluster_id());
  EXPECT_EQ(TEST_CLUSTER_VERSION + 1, replay_block_header2.get_cluster_version());
  EXPECT_EQ(TEST_LOG_ENTRY_NO + 1, replay_block_header2.get_log_entry_no());
  EXPECT_EQ(fill_block_header.flags(), replay_block_header2.flags());
  EXPECT_FALSE(replay_block_header2.is_serial_final());
}

TEST_F(TestObTxLog, tx_log_body_except_redo)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxLogBlock fill_block;
  ObTxLogBlock replay_block;

  ObLSArray TEST_LS_ARRAY;
  TEST_LS_ARRAY.push_back(LSKey());
  ObTxCommitParts TEST_COMMIT_PARTS;
  TEST_COMMIT_PARTS.push_back(ObTxExecPart(TEST_LS_KEY, 0, 0));
  ObRedoLSNArray TEST_LOG_OFFSET_ARRY;
  TEST_LOG_OFFSET_ARRY.push_back(TEST_LOG_OFFSET);
  ObLSLogInfoArray TEST_INFO_ARRAY;
  TEST_INFO_ARRAY.push_back(ObLSLogInfo());
  ObTxBufferNodeArray TEST_TX_BUFFER_NODE_ARRAY;
  ObString str("TEST CASE");
  ObTxBufferNode node;
  node.init(ObTxDataSourceType::LS_TABLE, str, share::SCN(), transaction::ObTxSEQ::mk_v0(100), nullptr);
  TEST_TX_BUFFER_NODE_ARRAY.push_back(node);

  ObTxCommitInfoLog fill_commit_state(TEST_ADDR,
                                       TEST_LS_ARRAY,
                                       TEST_LS_KEY,
                                       TEST_IS_SUB2PC,
                                       TEST_IS_DUP,
                                       TEST_CAN_ELR,
                                       TEST_TRACE_ID_STR,
                                       TEST_TRCE_INFO,
                                       TEST_LOG_OFFSET,
                                       TEST_LOG_OFFSET_ARRY,
                                       TEST_LS_ARRAY,
                                       TEST_CLUSTER_VERSION,
                                       TEST_XID,
                                       TEST_COMMIT_PARTS,
                                       TEST_EPOCH);
  // ASSERT_EQ(OB_SUCCESS, fill_commit_state.before_serialize());
  ObTxActiveInfoLog fill_active_state(TEST_ADDR,
                                       TEST_TRANS_TYPE,
                                       TEST_SESSION_ID,
                                       0,
                                       TEST_TRACE_ID_STR,
                                       TEST_SCHEMA_VERSION,
                                       TEST_CAN_ELR,
                                       TEST_ADDR,
                                       TEST_QUERY_TIME,
                                       TEST_IS_SUB2PC,
                                       TEST_IS_DUP,
                                       TEST_TX_EXPIRED_TIME,
                                       TEST_EPOCH,
                                       TEST_LAST_OP_SN,
                                       TEST_FIRST_SCN,
                                       TEST_LAST_SCN,
                                       TEST_MAX_SUBMITTED_SEQ_NO,
                                       TEST_CLUSTER_VERSION,
                                       TEST_XID,
                                       TEST_SERIAL_FINAL_SEQ_NO);
  ObTxPrepareLog filll_prepare(TEST_LS_ARRAY, TEST_LOG_OFFSET, TEST_PREV_LOG_TYPE);
  ObTxCommitLog fill_commit(share::SCN::base_scn(),
                            TEST_CHECKSUM,
                            TEST_CHECKSUM_SIGNATURE_ARRAY,
                            TEST_LS_ARRAY,
                            TEST_TX_BUFFER_NODE_ARRAY,
                            TEST_TRANS_TYPE,
                            TEST_LOG_OFFSET,
                            TEST_INFO_ARRAY,
                            TEST_PREV_LOG_TYPE);
  ObTxClearLog fill_clear(TEST_LS_ARRAY);
  ObTxAbortLog fill_abort(TEST_TX_BUFFER_NODE_ARRAY);
  ObTxRecordLog fill_record(TEST_LOG_OFFSET, TEST_LOG_OFFSET_ARRY);

  ObTxLogBlockHeader &header = fill_block.get_header();
  header.init(TEST_ORG_CLUSTER_ID, TEST_CLUSTER_VERSION, TEST_LOG_ENTRY_NO, ObTransID(TEST_TX_ID), TEST_ADDR);
  ASSERT_EQ(OB_SUCCESS, fill_block.init_for_fill());
  ASSERT_EQ(OB_SUCCESS, fill_block.add_new_log(fill_active_state));
  ASSERT_EQ(OB_SUCCESS, fill_block.add_new_log(fill_commit_state));
  ASSERT_EQ(OB_SUCCESS, fill_block.add_new_log(filll_prepare));
  ASSERT_EQ(OB_SUCCESS, fill_block.add_new_log(fill_commit));
  ASSERT_EQ(OB_SUCCESS, fill_block.add_new_log(fill_clear));
  ASSERT_EQ(OB_SUCCESS, fill_block.add_new_log(fill_abort));
  ASSERT_EQ(OB_SUCCESS, fill_block.add_new_log(fill_record));
  fill_block.seal(TEST_TX_ID);
  ObTxLogHeader tx_log_header;
  ASSERT_EQ(OB_SUCCESS, replay_block.init_for_replay(fill_block.get_buf(), fill_block.get_size()));

  uint64_t tmp_cluster_id = replay_block.get_header().get_org_cluster_id();
  EXPECT_EQ(TEST_ORG_CLUSTER_ID, tmp_cluster_id);

  ObTxActiveInfoLogTempRef active_temp_ref;
  ObTxActiveInfoLog replay_active_state(active_temp_ref);
  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(tx_log_header));
  EXPECT_EQ(ObTxLogType::TX_ACTIVE_INFO_LOG, tx_log_header.get_tx_log_type());
  ASSERT_EQ(OB_SUCCESS, replay_block.deserialize_log_body(replay_active_state));
  EXPECT_EQ(TEST_ADDR, replay_active_state.get_scheduler());
  EXPECT_EQ(TEST_TRANS_TYPE, replay_active_state.get_trans_type());
  EXPECT_EQ(TEST_SESSION_ID, replay_active_state.get_session_id());
  EXPECT_EQ(TEST_TRACE_ID_STR, replay_active_state.get_app_trace_id());
  EXPECT_EQ(TEST_SCHEMA_VERSION, replay_active_state.get_schema_version());
  EXPECT_EQ(TEST_CAN_ELR, replay_active_state.is_elr());
  EXPECT_EQ(TEST_ADDR, replay_active_state.get_proposal_leader());
  EXPECT_EQ(TEST_QUERY_TIME, replay_active_state.get_cur_query_start_time());
  EXPECT_EQ(TEST_IS_SUB2PC, replay_active_state.is_sub2pc());
  EXPECT_EQ(TEST_IS_DUP, replay_active_state.is_dup_tx());
  EXPECT_EQ(TEST_TX_EXPIRED_TIME, replay_active_state.get_tx_expired_time());
  EXPECT_EQ(TEST_EPOCH, replay_active_state.get_epoch());
  EXPECT_EQ(TEST_LAST_OP_SN, replay_active_state.get_last_op_sn());
  EXPECT_EQ(TEST_FIRST_SCN, replay_active_state.get_first_seq_no());
  EXPECT_EQ(TEST_LAST_SCN, replay_active_state.get_last_seq_no());
  EXPECT_EQ(0, replay_active_state.get_cluster_version());
  EXPECT_EQ(TEST_XID, replay_active_state.get_xid());
  EXPECT_EQ(TEST_SERIAL_FINAL_SEQ_NO, replay_active_state.get_serial_final_seq_no());

  ObTxCommitInfoLogTempRef commit_state_temp_ref;
  ObTxCommitInfoLog replay_commit_state(commit_state_temp_ref);
  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(tx_log_header));
  EXPECT_EQ(ObTxLogType::TX_COMMIT_INFO_LOG, tx_log_header.get_tx_log_type());
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            replay_block.deserialize_log_body(replay_active_state)); // error log type
  ASSERT_EQ(OB_SUCCESS, replay_block.deserialize_log_body(replay_commit_state));

  ObTxPrepareLogTempRef prepare_temp_ref;
  ObTxPrepareLog replay_prepare(prepare_temp_ref);
  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(tx_log_header));
  EXPECT_EQ(ObTxLogType::TX_PREPARE_LOG, tx_log_header.get_tx_log_type());
  ASSERT_EQ(OB_SUCCESS, replay_block.deserialize_log_body(replay_prepare));
  EXPECT_EQ(TEST_PREV_LOG_TYPE.prev_log_type_, replay_prepare.get_prev_log_type().prev_log_type_);

  ObTxCommitLogTempRef commit_temp_ref;
  ObTxCommitLog replay_commit(commit_temp_ref);
  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(tx_log_header));
  EXPECT_EQ(ObTxLogType::TX_COMMIT_LOG, tx_log_header.get_tx_log_type());
  ASSERT_EQ(OB_SUCCESS, replay_block.deserialize_log_body(replay_commit));
  EXPECT_EQ(TEST_PREV_LOG_TYPE.prev_log_type_, replay_commit.get_prev_log_type().prev_log_type_);

  ObTxClearLogTempRef clear_temp_ref;
  ObTxClearLog replay_clear(clear_temp_ref);
  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(tx_log_header));
  EXPECT_EQ(ObTxLogType::TX_CLEAR_LOG, tx_log_header.get_tx_log_type());
  ASSERT_EQ(OB_SUCCESS, replay_block.deserialize_log_body(replay_clear));

  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(tx_log_header));
  EXPECT_EQ(ObTxLogType::TX_ABORT_LOG, tx_log_header.get_tx_log_type());

  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(tx_log_header));
  EXPECT_EQ(ObTxLogType::TX_RECORD_LOG, tx_log_header.get_tx_log_type());

  ASSERT_EQ(OB_ITER_END, replay_block.get_next_log(tx_log_header)); // ITER_END
}

TEST_F(TestObTxLog, tx_log_body_redo)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTxLogBlock fill_block;
  ObTxLogBlock replay_block;
  ObTxLogBlock replay_block_2;

  ObLSArray TEST_LS_ARRAY;
  TEST_LS_ARRAY.push_back(LSKey());
  ObTxCommitParts TEST_COMMIT_PARTS;
  TEST_COMMIT_PARTS.push_back(ObTxExecPart(TEST_LS_KEY, 0, 0));
  ObRedoLSNArray TEST_LOG_OFFSET_ARRY;
  TEST_LOG_OFFSET_ARRY.push_back(TEST_LOG_OFFSET);
  ObLSLogInfoArray TEST_INFO_ARRAY;
  TEST_INFO_ARRAY.push_back(ObLSLogInfo());
  ObTxBufferNodeArray TEST_TX_BUFFER_NODE_ARRAY;
  ObString str("TEST CASE");
  ObTxBufferNode node;
  node.init(ObTxDataSourceType::LS_TABLE, str, share::SCN(), transaction::ObTxSEQ::mk_v0(100), nullptr);
  TEST_TX_BUFFER_NODE_ARRAY.push_back(node);

  ObTxCommitInfoLog fill_commit_state(TEST_ADDR,
                                       TEST_LS_ARRAY,
                                       TEST_LS_KEY,
                                       TEST_IS_SUB2PC,
                                       TEST_IS_DUP,
                                       TEST_CAN_ELR,
                                       TEST_TRACE_ID_STR,
                                       TEST_TRCE_INFO,
                                       TEST_LOG_OFFSET,
                                       TEST_LOG_OFFSET_ARRY,
                                       TEST_LS_ARRAY,
                                       TEST_CLUSTER_VERSION,
                                       TEST_XID,
                                       TEST_COMMIT_PARTS,
                                       TEST_EPOCH);
  ObTxCommitLog fill_commit(share::SCN::base_scn(),
                            TEST_CHECKSUM,
                            TEST_CHECKSUM_SIGNATURE_ARRAY,
                            TEST_LS_ARRAY,
                            TEST_TX_BUFFER_NODE_ARRAY,
                            TEST_TRANS_TYPE,
                            TEST_LOG_OFFSET,
                            TEST_INFO_ARRAY,
                            TEST_PREV_LOG_TYPE);

  ObTxLogBlockHeader &fill_block_header = fill_block.get_header();
  fill_block_header.init(TEST_ORG_CLUSTER_ID, TEST_CLUSTER_VERSION, TEST_LOG_ENTRY_NO, ObTransID(TEST_TX_ID), TEST_ADDR);
  ASSERT_EQ(OB_SUCCESS, fill_block.init_for_fill());

  ObString TEST_MUTATOR_BUF("FFF");
  int64_t mutator_pos = 0;
  ObTxRedoLog fill_redo(TEST_CLUSTER_VERSION);
  ASSERT_EQ(OB_SUCCESS, fill_block.prepare_mutator_buf(fill_redo));
  ASSERT_EQ(OB_SUCCESS, serialization::encode(fill_redo.get_mutator_buf(),
                                              fill_redo.get_mutator_size(),
                                              mutator_pos,
                                              TEST_MUTATOR_BUF));
  ASSERT_EQ(OB_SUCCESS, fill_block.finish_mutator_buf(fill_redo, mutator_pos));
  ASSERT_EQ(OB_SUCCESS, fill_block.add_new_log(fill_commit_state));
  ASSERT_EQ(OB_SUCCESS, fill_block.add_new_log(fill_commit));
  fill_block.seal(TEST_TX_ID);
  mutator_pos = 0;
  TxID id = 0;
  ObTxLogHeader log_header;
  ObString replay_mutator_buf;
  ObTxRedoLogTempRef redo_temp_ref;
  ObTxRedoLog replay_redo(redo_temp_ref);

  ObTxLogBlockHeader &replay_block_header = replay_block.get_header();
  ASSERT_EQ(OB_SUCCESS, replay_block.init_for_replay(fill_block.get_buf(), fill_block.get_size()));

  uint64_t tmp_cluster_id = replay_block_header.get_org_cluster_id();
  EXPECT_EQ(TEST_ORG_CLUSTER_ID, tmp_cluster_id);
  EXPECT_EQ(TEST_CLUSTER_VERSION, replay_block_header.get_cluster_version());

  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(log_header));
  EXPECT_EQ(ObTxLogType::TX_REDO_LOG, log_header.get_tx_log_type());
  ASSERT_EQ(OB_SUCCESS, replay_block.deserialize_log_body(replay_redo));
  EXPECT_EQ(fill_redo.get_mutator_size(), replay_redo.get_mutator_size());
  TRANS_LOG(INFO,
            "Mutator Info",
            K(fill_redo.get_mutator_buf()),
            K(replay_redo.get_replay_mutator_buf()),
            K(replay_redo.get_mutator_size()));
  ASSERT_EQ(OB_SUCCESS, serialization::decode(replay_redo.get_replay_mutator_buf(),
                                              replay_redo.get_mutator_size(),
                                              mutator_pos,
                                              replay_mutator_buf));
  EXPECT_EQ(TEST_MUTATOR_BUF, replay_mutator_buf);
  // EXPECT_EQ(TEST_CLOG_ENCRYPT_INFO,replay_redo.get_clog_encrypt_info());
  EXPECT_EQ(replay_redo.get_cluster_version(), 0);
  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(log_header));
  EXPECT_EQ(ObTxLogType::TX_COMMIT_INFO_LOG, log_header.get_tx_log_type());
  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(log_header));
  EXPECT_EQ(ObTxLogType::TX_COMMIT_LOG, log_header.get_tx_log_type());


  //ignore replay log, only need commit log
  ObTxLogBlockHeader &replay_block_header_2 = replay_block_2.get_header();
  ASSERT_EQ(OB_SUCCESS, replay_block_2.init_for_replay(fill_block.get_buf(), fill_block.get_size()));

  tmp_cluster_id = replay_block_header_2.get_org_cluster_id();
  EXPECT_EQ(TEST_ORG_CLUSTER_ID, tmp_cluster_id);

  ASSERT_EQ(OB_SUCCESS, replay_block_2.get_next_log(log_header));
  EXPECT_EQ(ObTxLogType::TX_REDO_LOG, log_header.get_tx_log_type());
  // ASSERT_EQ(OB_SUCCESS, replay_block.deserialize_log_body(replay_redo));
  // ASSERT_EQ(OB_SUCCESS,
  //           serialization::decode(replay_redo.get_replay_mutator_buf(),
  //                                 replay_redo.get_mutator_size(),
  //                                 mutator_pos,
  //                                 replay_mutator_buf));
  // EXPECT_EQ(TEST_MUTATOR_BUF, replay_mutator_buf);
  // EXPECT_EQ(TEST_CLOG_ENCRYPT_INFO,replay_redo.get_clog_encrypt_info());
  // EXPECT_EQ(TEST_CLUSTER_VERSION,replay_redo.get_cluster_version());
  ASSERT_EQ(OB_SUCCESS, replay_block_2.get_next_log(log_header));
  EXPECT_EQ(ObTxLogType::TX_COMMIT_INFO_LOG, log_header.get_tx_log_type());
  ASSERT_EQ(OB_SUCCESS, replay_block_2.get_next_log(log_header));
  EXPECT_EQ(ObTxLogType::TX_COMMIT_LOG, log_header.get_tx_log_type());
  ObTxCommitLogTempRef commit_temp_ref;
  ObTxCommitLog replay_commit(commit_temp_ref);
  ASSERT_EQ(OB_SUCCESS, replay_block_2.deserialize_log_body(replay_commit));
  EXPECT_EQ(share::SCN::base_scn(), replay_commit.get_commit_version());

}

TEST_F(TestObTxLog, test_compat_bytes)
{
  ObLSArray TEST_LS_ARRAY;
  TEST_LS_ARRAY.push_back(LSKey());
  ObTxCommitParts TEST_COMMIT_PARTS;
  TEST_COMMIT_PARTS.push_back(ObTxExecPart(TEST_LS_KEY, 0, 0));
  ObRedoLSNArray TEST_LOG_OFFSET_ARRY;
  TEST_LOG_OFFSET_ARRY.push_back(TEST_LOG_OFFSET);
  ObLSLogInfoArray TEST_INFO_ARRAY;
  TEST_INFO_ARRAY.push_back(ObLSLogInfo());
  ObTxBufferNodeArray TEST_TX_BUFFER_NODE_ARRAY;
  ObString str("TEST CASE");
  ObTxBufferNode node;
  node.init(ObTxDataSourceType::LS_TABLE, str, share::SCN(), transaction::ObTxSEQ::mk_v0(100), nullptr);
  TEST_TX_BUFFER_NODE_ARRAY.push_back(node);

  ObTxCommitInfoLog fill_commit_info(TEST_ADDR,
                                     TEST_LS_ARRAY,
                                     TEST_LS_KEY,
                                     TEST_IS_SUB2PC,
                                     TEST_IS_DUP,
                                     TEST_CAN_ELR,
                                     TEST_TRACE_ID_STR,
                                     TEST_TRCE_INFO,
                                     TEST_LOG_OFFSET,
                                     TEST_LOG_OFFSET_ARRY,
                                     TEST_LS_ARRAY,
                                     TEST_CLUSTER_VERSION,
                                     TEST_XID,
                                     TEST_COMMIT_PARTS,
                                     TEST_EPOCH);
  ObTxCommitInfoLogTempRef commit_info_temp_ref;
  ObTxCommitInfoLog replay_commit_info(commit_info_temp_ref);

  // ASSERT_EQ(OB_SUCCESS, fill_commit_info.before_serialize());
  TRANS_LOG(INFO,
            "the size of commit info with all compat_bytes",
            K(fill_commit_info.get_serialize_size()));
  ASSERT_EQ(true, fill_commit_info.is_dup_tx());
  ASSERT_EQ(false, fill_commit_info.get_participants().empty());
  void *tmp_buf = ob_malloc(1 * 1024 * 1024, ObNewModIds::TEST);
  int64_t pos = 0;
  fill_commit_info.compat_bytes_.set_object_flag(2, false);
  fill_commit_info.compat_bytes_.set_object_flag(5, false);
  ASSERT_EQ(OB_SUCCESS, fill_commit_info.serialize((char *)tmp_buf, 1 * 1024 * 1024, pos));
  TRANS_LOG(INFO,
            "the size of commit info with compat_bytes",
            K(fill_commit_info.get_serialize_size()),
            K(pos));

  pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            replay_commit_info.deserialize((const char *)tmp_buf, 1 * 1024 * 1024, pos));
  EXPECT_EQ(replay_commit_info.is_dup_tx(), false);
  EXPECT_EQ(replay_commit_info.get_participants().empty(), true);

}

TEST_F(TestObTxLog, test_default_log_deserialize)
{
  ObTxLogBlock fill_block;
  ObTxLogBlock replay_block;

  ObTxCommitInfoLogTempRef fill_commit_state_ref;
  ObTxActiveInfoLogTempRef fill_active_state_ref;
  ObTxPrepareLogTempRef fill_prepare_ref;
  ObTxCommitLogTempRef fill_commit_ref;
  ObTxClearLogTempRef fill_clear_ref;
  ObTxAbortLogTempRef fill_abort_ref;
  ObTxRecordLogTempRef fill_record_ref;

  ObTxCommitInfoLog fill_commit_state(fill_commit_state_ref);
  ObTxActiveInfoLog fill_active_state(fill_active_state_ref);
  ObTxPrepareLog fill_prepare(fill_prepare_ref);
  ObTxCommitLog fill_commit(fill_commit_ref);
  ObTxClearLog fill_clear(fill_clear_ref);
  ObTxAbortLog fill_abort(fill_abort_ref);
  ObTxRecordLog fill_record(fill_record_ref);

  // ObTxLogBlockHeader fill_block_header(TEST_ORG_CLUSTER_ID, TEST_LOG_ENTRY_NO,
  // ObTransID(TEST_TX_ID));
  ObTxLogBlockHeader &fill_block_header = fill_block.get_header();
  fill_block_header.init(1, TEST_CLUSTER_VERSION, 2, ObTransID(3), TEST_ADDR);
  ASSERT_EQ(OB_SUCCESS, fill_block.init_for_fill());
  ASSERT_EQ(OB_SUCCESS, fill_block.add_new_log(fill_active_state));
  ASSERT_EQ(OB_SUCCESS, fill_block.add_new_log(fill_commit_state));
  ASSERT_EQ(OB_SUCCESS, fill_block.add_new_log(fill_prepare));
  ASSERT_EQ(OB_SUCCESS, fill_block.add_new_log(fill_commit));
  ASSERT_EQ(OB_SUCCESS, fill_block.add_new_log(fill_clear));
  ASSERT_EQ(OB_SUCCESS, fill_block.add_new_log(fill_abort));
  ASSERT_EQ(OB_SUCCESS, fill_block.add_new_log(fill_record));
  fill_block.seal(TEST_TX_ID);

  TxID id = 0;
  int64_t fill_member_cnt = 0;
  int64_t replay_member_cnt = 0;
  ObTxLogHeader tx_log_header;
  ObTxLogBlockHeader &replay_block_header = replay_block.get_header();
  ASSERT_EQ(OB_SUCCESS, replay_block.init_for_replay(fill_block.get_buf(), fill_block.get_size()));
  fill_member_cnt = fill_block_header.compat_bytes_.total_obj_cnt_;
  EXPECT_EQ(fill_block_header.get_org_cluster_id(), replay_block_header.get_org_cluster_id());
  replay_member_cnt++;
  EXPECT_EQ(fill_block_header.get_log_entry_no(), replay_block_header.get_log_entry_no());
  replay_member_cnt++;
  EXPECT_EQ(fill_block_header.get_tx_id().get_id(), replay_block_header.get_tx_id().get_id());
  replay_member_cnt++;
  EXPECT_EQ(fill_block_header.get_scheduler(), replay_block_header.get_scheduler());
  replay_member_cnt++;
  EXPECT_EQ(fill_block_header.get_cluster_version(), replay_block_header.get_cluster_version());
  replay_member_cnt++;
  EXPECT_EQ(fill_block_header.flags(), replay_block_header.flags());
  replay_member_cnt++;
  EXPECT_EQ(replay_member_cnt, fill_member_cnt - 1/*1 skipped*/);

  ObTxActiveInfoLogTempRef active_temp_ref;
  ObTxActiveInfoLog replay_active_state(active_temp_ref);
  fill_member_cnt = replay_member_cnt = 0;
  fill_member_cnt = fill_active_state.compat_bytes_.total_obj_cnt_;
  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(tx_log_header));
  EXPECT_EQ(ObTxLogType::TX_ACTIVE_INFO_LOG, tx_log_header.get_tx_log_type());
  ASSERT_EQ(OB_SUCCESS, replay_block.deserialize_log_body(replay_active_state));
  EXPECT_EQ(fill_active_state.get_scheduler(), replay_active_state.get_scheduler());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.get_trans_type(), replay_active_state.get_trans_type());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.get_session_id(), replay_active_state.get_session_id());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.get_app_trace_id(), replay_active_state.get_app_trace_id());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.get_schema_version(), replay_active_state.get_schema_version());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.is_elr(), replay_active_state.is_elr());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.get_proposal_leader(), replay_active_state.get_proposal_leader());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.get_cur_query_start_time(),
            replay_active_state.get_cur_query_start_time());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.is_sub2pc(), replay_active_state.is_sub2pc());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.is_dup_tx(), replay_active_state.is_dup_tx());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.get_tx_expired_time(), replay_active_state.get_tx_expired_time());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.get_epoch(), replay_active_state.get_epoch());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.get_last_op_sn(), replay_active_state.get_last_op_sn());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.get_first_seq_no(), replay_active_state.get_first_seq_no());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.get_last_seq_no(), replay_active_state.get_last_seq_no());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.get_cluster_version(), replay_active_state.get_cluster_version());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.get_max_submitted_seq_no(), replay_active_state.get_max_submitted_seq_no());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.get_xid(), replay_active_state.get_xid());
  replay_member_cnt++;
  EXPECT_EQ(fill_active_state.get_serial_final_seq_no(), replay_active_state.get_serial_final_seq_no());
  replay_member_cnt++;
  EXPECT_EQ(replay_member_cnt, fill_member_cnt);

  ObTxCommitInfoLogTempRef commit_state_temp_ref;
  ObTxCommitInfoLog replay_commit_state(commit_state_temp_ref);
  fill_member_cnt = replay_member_cnt = 0;
  fill_member_cnt = fill_commit_state.compat_bytes_.total_obj_cnt_;
  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(tx_log_header));
  EXPECT_EQ(ObTxLogType::TX_COMMIT_INFO_LOG, tx_log_header.get_tx_log_type());
  ASSERT_EQ(OB_SUCCESS, replay_block.deserialize_log_body(replay_commit_state));
  EXPECT_EQ(fill_commit_state.get_scheduler(), replay_commit_state.get_scheduler());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_participants().count(),
            replay_commit_state.get_participants().count());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_upstream(), replay_commit_state.get_upstream());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.is_sub2pc(), replay_commit_state.is_sub2pc());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.is_dup_tx(), replay_commit_state.is_dup_tx());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.is_elr(), replay_commit_state.is_elr());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_incremental_participants().count(),
            replay_commit_state.get_incremental_participants().count());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_cluster_version(), replay_commit_state.get_cluster_version());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_app_trace_id(), replay_commit_state.get_app_trace_id());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_app_trace_info(), replay_commit_state.get_app_trace_info());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_prev_record_lsn(), replay_commit_state.get_prev_record_lsn());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_redo_lsns().count(), replay_commit_state.get_redo_lsns().count());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_xid(), replay_commit_state.get_xid());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_commit_parts().count(), replay_commit_state.get_commit_parts().count());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_epoch(), replay_commit_state.get_epoch());
  replay_member_cnt++;
  EXPECT_EQ(replay_member_cnt, fill_member_cnt);

  ObTxPrepareLogTempRef prepare_temp_ref;
  ObTxPrepareLog replay_prepare(prepare_temp_ref);
  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(tx_log_header));
  EXPECT_EQ(ObTxLogType::TX_PREPARE_LOG, tx_log_header.get_tx_log_type());
  ASSERT_EQ(OB_SUCCESS, replay_block.deserialize_log_body(replay_prepare));
  fill_member_cnt = replay_member_cnt = 0;
  fill_member_cnt = fill_prepare.compat_bytes_.total_obj_cnt_;
  EXPECT_EQ(fill_prepare.get_incremental_participants().count(),
            replay_prepare.get_incremental_participants().count());
  replay_member_cnt++;
  EXPECT_EQ(fill_prepare.get_prev_lsn(), replay_prepare.get_prev_lsn());
  replay_member_cnt++;
  EXPECT_EQ(fill_prepare.get_prev_log_type().prev_log_type_, replay_prepare.get_prev_log_type().prev_log_type_);
  replay_member_cnt++;
  EXPECT_EQ(replay_member_cnt, fill_member_cnt);

  ObTxCommitLogTempRef commit_temp_ref;
  ObTxCommitLog replay_commit(commit_temp_ref);
  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(tx_log_header));
  EXPECT_EQ(ObTxLogType::TX_COMMIT_LOG, tx_log_header.get_tx_log_type());
  ASSERT_EQ(OB_SUCCESS, replay_block.deserialize_log_body(replay_commit));

  ObTxClearLogTempRef clear_temp_ref;
  ObTxClearLog replay_clear(clear_temp_ref);
  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(tx_log_header));
  EXPECT_EQ(ObTxLogType::TX_CLEAR_LOG, tx_log_header.get_tx_log_type());
  ASSERT_EQ(OB_SUCCESS, replay_block.deserialize_log_body(replay_clear));

  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(tx_log_header));
  EXPECT_EQ(ObTxLogType::TX_ABORT_LOG, tx_log_header.get_tx_log_type());

  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(tx_log_header));
  EXPECT_EQ(ObTxLogType::TX_RECORD_LOG, tx_log_header.get_tx_log_type());

  ASSERT_EQ(OB_ITER_END, replay_block.get_next_log(tx_log_header)); // ITER_END
}

TEST_F(TestObTxLog, TestComapt)
{
  OldTestLog old_fill;
  NewTestLog new_fill;
  old_fill.tx_id_1 = TEST_TX_ID;
  old_fill.tx_id_2 = TEST_TX_ID;
  new_fill.tx_id_1 = TEST_TX_ID;
  new_fill.tx_id_2 = TEST_TX_ID;
  new_fill.tx_id_3 = TEST_TX_ID;

  char old_fill_buf[1024];
  char new_fill_buf[1024];
  memset(old_fill_buf, 0, sizeof(char) * 1024);
  memset(new_fill_buf, 0, sizeof(char) * 1024);

  int64_t old_pos,new_pos;
  int64_t old_size,new_size;

  old_pos = new_pos = 0;
  EXPECT_EQ(OB_SUCCESS, old_fill.serialize(old_fill_buf, 1024, old_pos));
  EXPECT_EQ(OB_SUCCESS, new_fill.serialize(new_fill_buf, 1024, new_pos));
  old_size = old_pos;
  new_size = new_pos;
  TRANS_LOG(INFO, " serialize test fill log", K(old_size), K(new_size));

  OldTestLog old_replay;
  NewTestLog new_replay;
  old_pos = new_pos = 0;
  EXPECT_EQ(OB_SUCCESS, new_replay.deserialize(old_fill_buf, old_size, old_pos));
  EXPECT_EQ(OB_SUCCESS, old_replay.deserialize(new_fill_buf, new_size, new_pos));

  EXPECT_EQ(TEST_TX_ID, old_replay.tx_id_1);
  EXPECT_EQ(TEST_TX_ID, old_replay.tx_id_2);

  EXPECT_EQ(TEST_TX_ID, new_replay.tx_id_1);
  EXPECT_EQ(TEST_TX_ID, new_replay.tx_id_2);
  EXPECT_EQ(0, new_replay.tx_id_3);

}

void test_big_commit_info_log(int64_t log_size)
{

  ObTxLogBlock fill_block;
  ObTxLogBlock replay_block;
  ObTxBigSegmentBuf fill_big_segment;
  ObTxBigSegmentBuf replay_big_segment;

  ObLSArray TEST_LS_ARRAY;
  TEST_LS_ARRAY.push_back(LSKey());
  ObTxCommitParts TEST_COMMIT_PARTS;
  TEST_COMMIT_PARTS.push_back(ObTxExecPart(TEST_LS_KEY, 0, 0));
  ObRedoLSNArray TEST_BIG_REDO_LSN_ARRAY;
  for (int i = 0; i < log_size / sizeof(palf::LSN); i++) {
    TEST_BIG_REDO_LSN_ARRAY.push_back(palf::LSN(i));
  }

  EXPECT_EQ(TEST_BIG_REDO_LSN_ARRAY.count(), log_size / sizeof(palf::LSN));
  ObTxCommitInfoLog fill_commit_state(TEST_ADDR, TEST_LS_ARRAY, TEST_LS_KEY, TEST_IS_SUB2PC,
                                      TEST_IS_DUP, TEST_CAN_ELR, TEST_TRACE_ID_STR, TEST_TRCE_INFO,
                                      TEST_LOG_OFFSET, TEST_BIG_REDO_LSN_ARRAY, TEST_LS_ARRAY,
                                      TEST_CLUSTER_VERSION, TEST_XID,
                                      TEST_COMMIT_PARTS, TEST_EPOCH);
  ObTxLogBlockHeader &fill_block_header = fill_block.get_header();
  fill_block_header.init(TEST_ORG_CLUSTER_ID, TEST_CLUSTER_VERSION, TEST_LOG_ENTRY_NO, ObTransID(TEST_TX_ID), TEST_ADDR);
  ASSERT_EQ(OB_SUCCESS, fill_block.init_for_fill());
  ASSERT_EQ(OB_LOG_TOO_LARGE, fill_block.add_new_log(fill_commit_state, &fill_big_segment));

  const char *submit_buf = nullptr;
  int64_t submit_buf_len = 0;
  uint64_t part_count = 0;
  ObTxLogHeader log_type_header;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)
         && OB_EAGAIN
                == (ret = (fill_block.acquire_segment_log_buf(ObTxLogType::TX_COMMIT_INFO_LOG)))) {
    share::SCN tmp_scn;
    EXPECT_EQ(OB_SUCCESS, tmp_scn.convert_for_inner_table_field(part_count));
    if (OB_FAIL(fill_block.set_prev_big_segment_scn(tmp_scn))) {
      TRANS_LOG(WARN, "set prev big segment scn failed", K(ret), K(part_count));
    } else if (OB_FAIL(fill_block.seal(TEST_TX_ID))) {
      TRANS_LOG(WARN, "seal block fail", K(ret));
    } else {
      replay_block.reset();
      ObTxLogBlockHeader &replay_block_header = replay_block.get_header();
      ASSERT_EQ(OB_SUCCESS, replay_block.init_for_replay(fill_block.get_buf(), fill_block.get_size()));
      if (OB_FAIL(replay_block.get_next_log(log_type_header, &replay_big_segment))) {
        TRANS_LOG(WARN, "get next log failed", K(ret), K(part_count));
        EXPECT_EQ(OB_LOG_TOO_LARGE, ret);
        EXPECT_EQ(ObTxLogType::TX_BIG_SEGMENT_LOG, log_type_header.get_tx_log_type());
        ret = OB_SUCCESS;
      }
      TRANS_LOG(INFO, "collect one part from fill_big_segment", K(ret), K(part_count),
                K(log_type_header), K(replay_big_segment));
    }
    part_count++;
  }
  // EXPECT_EQ(ObTxLogType::TX_COMMIT_INFO_LOG, log_type_header.get_tx_log_type());
  if (OB_ITER_END == ret) {
    fill_block.seal(TEST_TX_ID);
    replay_block.reset();
    ObTxLogBlockHeader &replay_block_header = replay_block.get_header();
    ASSERT_EQ(OB_SUCCESS, replay_block.init_for_replay(fill_block.get_buf(), fill_block.get_size()));
    if (OB_FAIL(replay_block.get_next_log(log_type_header, &replay_big_segment))) {
      TRANS_LOG(WARN, "get next log failed", K(ret), K(part_count));
    }
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(ObTxLogType::TX_COMMIT_INFO_LOG, log_type_header.get_tx_log_type());

    TRANS_LOG(INFO, "collect one part from fill_big_segment", K(ret), K(part_count),
              K(log_type_header), K(replay_big_segment));
    part_count++;
  }

  int64_t TOTAL_PART_COUNT =
      fill_commit_state.get_serialize_size() / ObTxLogBlock::BIG_SEGMENT_SPILT_SIZE;
  if (fill_commit_state.get_serialize_size() % ObTxLogBlock::BIG_SEGMENT_SPILT_SIZE > 0) {
    TOTAL_PART_COUNT++;
  }
  TRANS_LOG(INFO, "TOTAL PART COUNT", K(TOTAL_PART_COUNT), K(part_count));
  EXPECT_EQ(TOTAL_PART_COUNT, part_count);

  ObTxCommitInfoLogTempRef commit_state_temp_ref;
  ObTxCommitInfoLog replay_commit_state(commit_state_temp_ref);
  EXPECT_EQ(OB_SUCCESS, replay_block.deserialize_log_body(replay_commit_state));

  int64_t fill_member_cnt = 0;
  int64_t replay_member_cnt = 0;
  fill_member_cnt = replay_member_cnt = 0;
  fill_member_cnt = fill_commit_state.compat_bytes_.total_obj_cnt_;
  EXPECT_EQ(fill_commit_state.get_scheduler(), replay_commit_state.get_scheduler());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_participants().count(),
            replay_commit_state.get_participants().count());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_upstream(), replay_commit_state.get_upstream());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.is_sub2pc(), replay_commit_state.is_sub2pc());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.is_dup_tx(), replay_commit_state.is_dup_tx());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.is_elr(), replay_commit_state.is_elr());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_incremental_participants().count(),
            replay_commit_state.get_incremental_participants().count());
  replay_member_cnt++;
  EXPECT_EQ(0, replay_commit_state.get_cluster_version());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_app_trace_id().length(),
            replay_commit_state.get_app_trace_id().length());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_app_trace_info().length(),
            replay_commit_state.get_app_trace_info().length());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_prev_record_lsn(), replay_commit_state.get_prev_record_lsn());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_redo_lsns().count(), replay_commit_state.get_redo_lsns().count());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_xid(), replay_commit_state.get_xid());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_commit_parts().count(), replay_commit_state.get_commit_parts().count());
  replay_member_cnt++;
  EXPECT_EQ(fill_commit_state.get_epoch(), replay_commit_state.get_epoch());
  replay_member_cnt++;
  EXPECT_EQ(replay_member_cnt, fill_member_cnt);
}

TEST_F(TestObTxLog, test_big_segment_log)
{
  test_big_commit_info_log(2*1024*1024);

  test_big_commit_info_log(10*1024*1024);
}

TEST_F(TestObTxLog, test_commit_log_with_checksum_signature)
{
  uint64_t checksum = 0;
  uint8_t sig[64];
  ObArrayHelper<uint8_t> checksum_signatures(64, sig);
  for(int i = 0; i< 64; i++) {
    uint64_t checksum_i = ObRandom::rand(1, 99999);
    checksum = ob_crc64(checksum, &checksum_i, sizeof(checksum_i));
    checksum_signatures.push_back((uint8_t)(checksum_i & 0xFF));
  }
  ObLSArray ls_array;
  ls_array.push_back(ObLSID(1001));
  ObTxBufferNodeArray tx_buffer_node_array;
  ObTxBufferNode node;
  ObString str("hello,world");
  node.init(ObTxDataSourceType::LS_TABLE, str, share::SCN(), transaction::ObTxSEQ::mk_v0(100), nullptr);
  tx_buffer_node_array.push_back(node);
  ObLSLogInfoArray ls_info_array;
  ls_info_array.push_back(ObLSLogInfo());
  share::SCN scn;
  scn.convert_for_tx(101010101010101);
  ObTxCommitLog log0(scn,
                     checksum,
                     checksum_signatures,
                     ls_array,
                     tx_buffer_node_array,
                     1,
                     LogOffSet(100),
                     ls_info_array,
                     TEST_PREV_LOG_TYPE);
  int64_t size = log0.get_serialize_size();
  char *buf = new char[size];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, log0.serialize(buf, size, pos));
  ObTxCommitLogTempRef ref;
  ObTxCommitLog log1(ref);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, log1.deserialize(buf, size, pos));
  ASSERT_EQ(log1.checksum_, log0.checksum_);
  ASSERT_EQ(log1.checksum_, checksum);
  ASSERT_EQ(log1.checksum_sig_.count(), 64);
  for(int i = 0; i < log1.checksum_sig_.count(); i++) {
    ASSERT_EQ(log1.checksum_sig_.at(i), sig[i]);
  }
}

TEST_F(TestObTxLog, test_start_working_log)
{
  ObTransID fake_tx_id(0);
  ObTxLogBlockHeader header(1, 1, 1, fake_tx_id, ObAddr());
  EXPECT_EQ(0, header.get_serialize_size_());
  EXPECT_EQ(OB_SUCCESS, header.before_serialize());
  int64_t ser_size_ = header.get_serialize_size_();
  int64_t ser_size = header.get_serialize_size();
  EXPECT_NE(0, ser_size_);
  char buf[256];
  MEMSET(buf, 0, 256);
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, header.serialize(buf, 256, pos));
  EXPECT_EQ(pos, ser_size);
  ObTxLogBlockHeader header2;
  int64_t pos0 = 0;
  EXPECT_EQ(OB_SUCCESS, header2.deserialize(buf, 256, pos0));
  EXPECT_LE(pos0, pos);
  EXPECT_EQ(header2.tx_id_, fake_tx_id);
  EXPECT_EQ(header2.cluster_version_, header.cluster_version_);
  EXPECT_EQ(header2.log_entry_no_, header.log_entry_no_);
}

TEST_F(TestObTxLog, test_tx_block_header_serialize)
{
  // 1. user must call before_serialize, before get_serialize_size(), serialize()
  ObTransID tx_id(1024);
  ObAddr addr(ObAddr::VER::IPV4, "127.2.3.4", 2048);
  ObTxLogBlockHeader header(101, 102, 103, tx_id, addr);
  EXPECT_EQ(0, header.serialize_size_);
  EXPECT_EQ(OB_SUCCESS, header.before_serialize());
  int64_t ser_size_ = header.get_serialize_size_();
  EXPECT_EQ(ser_size_, header.get_serialize_size_());
  int64_t ser_size = header.get_serialize_size();
  EXPECT_GT(ser_size_, 0);
  char buf[256];
  MEMSET(buf, 0, 256);
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, header.serialize(buf, 256, pos));
  EXPECT_EQ(pos, ser_size);

  // test deserialize ok
  ObTxLogBlockHeader header2;
  int64_t pos0 = 0;
  EXPECT_EQ(OB_SUCCESS, header2.deserialize(buf, 256, pos0));
  EXPECT_LE(pos0, pos);
  EXPECT_EQ(header2.tx_id_, tx_id);
  EXPECT_EQ(header2.org_cluster_id_, 101);
  EXPECT_EQ(header2.cluster_version_, 102);
  EXPECT_EQ(header2.log_entry_no_, 103);
  EXPECT_EQ(header2.scheduler_, addr);

  // the serilize size is always equals to header.serialize_size_
  header.serialize_size_ = 240;
  int64_t ser_size2 = header.get_serialize_size();
  EXPECT_GT(ser_size2, 240);
  EXPECT_EQ(240, header.get_serialize_size_());
  MEMSET(buf, 0, 256);
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, header.serialize(buf, 256, pos));
  EXPECT_EQ(pos, ser_size2);
}

} // namespace unittest
} // namespace oceanbase

using namespace oceanbase;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_tx_log.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
