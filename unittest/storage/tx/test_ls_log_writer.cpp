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

#include <gtest/gtest.h>
#include <unistd.h>
#include <cstdio>
#define private public
#define protected public
#include "ob_mock_tx_log_adapter.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "storage/tx/ob_tx_ls_log_writer.h"
#include "logservice/ob_log_base_header.h"

namespace oceanbase
{

using namespace transaction;
using namespace storage;
using namespace share;

namespace unittest
{
MockTxLogAdapter tx_log_adapter;
MockTxLogParam param;
ObTxLSLogWriter ls_log_writer;

class TestLSLogWriter : public ::testing::Test
{
public:
  virtual void SetUp()
  {
    tx_log_adapter.init(&param);
    tx_log_adapter.start();
  }
  virtual void TearDown()
  {
    tx_log_adapter.stop();
    tx_log_adapter.wait();
    tx_log_adapter.destroy();
  }

public:
};

const ObLSID TEST_LS_ID(735);

TEST_F(TestLSLogWriter, submit_start_working_log)
{
  int64_t tmp_tenant_id = 1;
  ObLSTxCtxMgr tmp_mgr;
  common::ObConcurrentFIFOAllocator tmp_allocator;

  ObTxLogBlock replay_block;
  int64_t replay_hint = 0;
  share::SCN log_ts;
  std::string log_string;
  ObTxLogHeader log_header;
  ObTxStartWorkingLogTempRef tmp_ref;
  ObTxStartWorkingLog sw_log(tmp_ref);
  int64_t test_leader_epoch = 1308;

  ASSERT_EQ(OB_SUCCESS, ls_log_writer.init(tmp_tenant_id, TEST_LS_ID, &tx_log_adapter,
                                           (ObLSTxCtxMgr *)&tmp_mgr));
  ASSERT_EQ(OB_SUCCESS, ls_log_writer.submit_start_working_log(test_leader_epoch, log_ts));

  ASSERT_EQ(true, tx_log_adapter.get_log(log_ts.get_val_for_gts(), log_string));
  ASSERT_EQ(OB_SUCCESS, replay_block.init_for_replay(log_string.c_str(), log_string.size()));
  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(log_header));
  EXPECT_EQ(ObTxLogType::TX_START_WORKING_LOG, log_header.get_tx_log_type());
  ASSERT_EQ(OB_SUCCESS, replay_block.deserialize_log_body(sw_log));
  EXPECT_EQ(test_leader_epoch, sw_log.get_leader_epoch());
}

TEST_F(TestLSLogWriter, inline_buf_covers_start_working_log)
{
  logservice::ObLogBaseHeader base_header(logservice::ObLogBaseType::TRANS_SERVICE_LOG_BASE_TYPE,
                                          logservice::ObReplayBarrierType::STRICT_BARRIER, 1);
  ObTransID fake_tx_id(0);
  ObTxLogBlockHeader block_header(1, 1, 1, fake_tx_id, ObAddr());
  ObTxLogHeader tx_header(ObTxLogType::TX_START_WORKING_LOG);
  ObTxStartWorkingLog sw_log(1308);
  ASSERT_EQ(OB_SUCCESS, block_header.before_serialize());
  const int64_t need = base_header.get_serialize_size()
                     + block_header.get_serialize_size()
                     + tx_header.get_serialize_size()
                     + sw_log.get_serialize_size();
  EXPECT_LE(sw_log.get_serialize_size(), ObTxStartWorkingLog::MAX_SERIALIZE_SIZE);
  EXPECT_LE(need, ObTxLSCtrlLogBufSize::INLINE_SIZE);
}

TEST_F(TestLSLogWriter, serialize_fail_returns_cb)
{
  int64_t tmp_tenant_id = 1;
  ObLSTxCtxMgr tmp_mgr;
  ObTxLSLogWriter writer;
  share::SCN log_ts;

  ASSERT_EQ(OB_SUCCESS, writer.init(tmp_tenant_id, TEST_LS_ID, &tx_log_adapter,
                                    (ObLSTxCtxMgr *)&tmp_mgr));
  ASSERT_EQ(1, writer.free_cbs_.get_size());
  ASSERT_EQ(0, writer.start_working_cbs_.get_size());

  ObTxLSLogCb *cb = writer.free_cbs_.get_first();
  ASSERT_TRUE(NULL != cb);
  cb->log_buf_ = NULL;
  cb->log_buf_len_ = 1 << 20; // skip heap fallback, force serialize INVALID_ARGUMENT

  EXPECT_NE(OB_SUCCESS, writer.submit_start_working_log(1308, log_ts));
  EXPECT_EQ(1, writer.free_cbs_.get_size());
  EXPECT_EQ(0, writer.start_working_cbs_.get_size());

  cb = writer.free_cbs_.get_first();
  ASSERT_TRUE(NULL != cb);
  cb->log_buf_ = cb->inline_log_buf_;
  cb->log_buf_len_ = ObTxLSLogCb::INLINE_LOG_BUF_SIZE;

  ObTxLSLogCb *got = NULL;
  ASSERT_EQ(OB_SUCCESS, writer.get_log_cb_(ObTxLogType::TX_START_WORKING_LOG, got));
  ASSERT_TRUE(NULL != got);
  EXPECT_EQ(0, writer.free_cbs_.get_size());
  EXPECT_EQ(1, writer.start_working_cbs_.get_size());
  ASSERT_EQ(OB_SUCCESS, writer.return_log_cb_(got));
  EXPECT_EQ(1, writer.free_cbs_.get_size());
  EXPECT_EQ(0, writer.start_working_cbs_.get_size());
  writer.reset();
}

TEST_F(TestLSLogWriter, ensure_log_buf_fallback_to_heap)
{
  int64_t tmp_tenant_id = 1;
  ObLSTxCtxMgr tmp_mgr;
  ObTxLSLogWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.init(tmp_tenant_id, TEST_LS_ID, &tx_log_adapter,
                                    (ObLSTxCtxMgr *)&tmp_mgr));
  ObTxLSLogCb *cb = writer.free_cbs_.get_first();
  ASSERT_TRUE(NULL != cb);
  ASSERT_EQ(cb->inline_log_buf_, cb->log_buf_);
  const int64_t need = ObTxLSLogCb::INLINE_LOG_BUF_SIZE + 64;
  ASSERT_EQ(OB_SUCCESS, cb->ensure_log_buf_(need));
  EXPECT_NE(cb->inline_log_buf_, cb->log_buf_);
  EXPECT_EQ(need, cb->log_buf_len_);
  writer.reset();
}

} // namespace unittest
} // namespace oceanbase

using namespace oceanbase;

int main(int argc, char **argv)
{
  int ret = 1;
  char log_path[256];
  snprintf(log_path, sizeof(log_path), "test_ls_log_writer_%d.log", static_cast<int>(getpid()));
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name(log_path, true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
