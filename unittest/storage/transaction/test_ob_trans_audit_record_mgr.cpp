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

#include "storage/transaction/ob_trans_audit_record_mgr.h"
#include "share/ob_errno.h"
#include <gtest/gtest.h>

namespace oceanbase {
using namespace transaction;
using namespace common;
namespace unittest {
class TestObTransAuditRecordMgr : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

public:
  static const int32_t MAX_MEMORY = 100 * (1 << 20);  // 100MB
  static const uint64_t TENANT_ID = 1000;
};

TEST_F(TestObTransAuditRecordMgr, get_revert)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObTransAuditRecordMgr record_manager;

  ASSERT_EQ(
      OB_SUCCESS, record_manager.init(TestObTransAuditRecordMgr::MAX_MEMORY, TestObTransAuditRecordMgr::TENANT_ID));
  int32_t record_count = TestObTransAuditRecordMgr::MAX_MEMORY / sizeof(ObTransAuditRecord);

  ObTransAuditRecord* rec;

  for (int64_t i = 0; i < record_count * 2; i++) {
    if (i < record_count) {
      ASSERT_EQ(OB_SUCCESS, record_manager.get_empty_record(rec));
    } else {
      ASSERT_NE(OB_SUCCESS, record_manager.get_empty_record(rec));
    }
  }

  for (int64_t i = 0; i < record_count; i++) {
    ASSERT_EQ(OB_SUCCESS, record_manager.revert_record(rec));
  }
}

TEST_F(TestObTransAuditRecordMgr, set_and_iterate)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  ObTransAuditRecordMgr record_manager;

  ASSERT_EQ(
      OB_SUCCESS, record_manager.init(TestObTransAuditRecordMgr::MAX_MEMORY, TestObTransAuditRecordMgr::TENANT_ID));
  int32_t record_count = TestObTransAuditRecordMgr::MAX_MEMORY / sizeof(ObTransAuditRecord);

  ObTransAuditRecord* rec;

  for (int32_t i = 0; i < record_count; i++) {
    ASSERT_EQ(OB_SUCCESS, record_manager.get_empty_record(rec));
    int64_t tenant_id = 1;
    common::ObAddr addr;
    ObTransID trans_id;
    common::ObPartitionKey pkey;
    uint64_t session_id = i;
    uint64_t proxy_session_id = 456;
    int32_t trans_type = 0;
    int32_t ctx_refer = 1;
    uint64_t ctx_create_time = 1234567890;
    uint64_t expired_time = 98765;
    ObStartTransParam trans_param;
    int64_t trans_ctx_type = 1;
    int status = common::OB_SUCCESS;
    bool for_replay = false;
    ObTransCtx* dummy_ctx = (ObTransCtx*)0x1234;
    rec->init(dummy_ctx);
    ASSERT_EQ(OB_SUCCESS,
        rec->set_trans_audit_data(tenant_id,
            addr,
            trans_id,
            pkey,
            session_id,
            proxy_session_id,
            trans_type,
            ctx_refer,
            ctx_create_time,
            expired_time,
            trans_param,
            trans_ctx_type,
            status,
            for_replay));

    for (int32_t j = 1; j <= ObTransAuditRecord::STMT_INFO_COUNT; j++) {
      int64_t sql_no = j;
      common::ObTraceIdAdaptor trace_id;
      int64_t proxy_receive_us = 100 + i + j;
      int64_t server_receive_us = 200 + j;
      int64_t trans_receive_us = 300 + j;
      ASSERT_EQ(OB_SUCCESS,
          rec->set_start_stmt_info(sql_no,
              sql::ObPhyPlanType::OB_PHY_PLAN_LOCAL,
              trace_id,
              proxy_receive_us,
              server_receive_us,
              trans_receive_us));
      int64_t trans_execute_us = 400 + j;
      int64_t lock_for_read_retry_count = 500 + j;
      ASSERT_EQ(OB_SUCCESS, rec->set_end_stmt_info(sql_no, trans_execute_us, lock_for_read_retry_count));
    }
  }

  // Iterate trans audit
  ObTransAuditDataIterator iter;
  ObTransAuditCommonInfo common_info;
  ObTransAuditInfo trans_info;
  char trace_log_buffer[2048];

  ASSERT_EQ(OB_SUCCESS, iter.init(&record_manager));
  for (int32_t i = 0; i < record_count; i++) {
    ASSERT_EQ(OB_SUCCESS, iter.get_next(common_info, trans_info, trace_log_buffer, 2048));
    ASSERT_EQ(trans_info.session_id_, i);
  }
  ASSERT_EQ(OB_ITER_END, iter.get_next(common_info, trans_info, trace_log_buffer, 2048));
  ASSERT_FALSE(iter.is_valid());

  // Iterate trans sql audit
  ObTransSQLAuditDataIterator sql_iter;
  ObTransAuditStmtInfo stmt_info;

  ASSERT_EQ(OB_SUCCESS, sql_iter.init(&record_manager));
  for (int32_t i = 0; i < record_count; i++) {
    for (int32_t j = 1; j <= ObTransAuditRecord::STMT_INFO_COUNT; j++) {
      ASSERT_EQ(OB_SUCCESS, sql_iter.get_next(common_info, stmt_info));
      ASSERT_EQ(stmt_info.proxy_receive_us_, 100 + i + j);
    }
  }
  ASSERT_EQ(OB_ITER_END, sql_iter.get_next(common_info, stmt_info));
  ASSERT_FALSE(iter.is_valid());
}

}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_trans_audit_record_mgr.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;

  return 0;
}
