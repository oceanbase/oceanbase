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
#define private public
#include "ob_dml_check.h"
#include "storage/ob_partition_service.h"
#include "storage/transaction/ob_trans_service.h"
#include "mock_ob_iterator.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace transaction;
using namespace share;
using namespace share::schema;
using namespace sql;
namespace unittest {

void check_result_iter(ObNewRowIterator& result, ObStoreRowIterator& expected)
{
  int ret1 = OB_SUCCESS;
  int ret2 = OB_SUCCESS;
  int i = 0;
  ObNewRow* row1 = NULL;
  const ObStoreRow* row2 = NULL;
  ret1 = result.get_next_row(row1);
  ret2 = expected.get_next_row(row2);
  while (OB_SUCCESS == ret1 && OB_SUCCESS == ret2) {
    ASSERT_EQ(row1->count_, row2->row_val_.count_);
    for (int j = 0; j < row1->count_; ++j) {
      ASSERT_EQ(row1->cells_[j], row2->row_val_.cells_[j]) << "row:" << i << " col:" << j;
    }
    ++i;
    ret1 = result.get_next_row(row1);
    ret2 = expected.get_next_row(row2);
  }
  ASSERT_EQ(ret1, ret2);
  ASSERT_EQ(OB_ITER_END, ret2);
}

void do_scan_check(
    const common::ObPartitionKey& pkey, const char* scan_str, ObTableScanParam& scan_param, MockObServer& server)
{
  int ret = OB_SUCCESS;
  ObTransDesc trans_desc;
  uint64_t tenant_id = OB_SYS_TENANT_ID;
  const int64_t thread_id = 100;
  bool rollback = false;
  int64_t timeout = 1000000000;
  int64_t idle_timeout = 500000000;
  int64_t trans_expired_time = ObTimeUtility::current_time() + timeout;
  int64_t stmt_expired_time = ObTimeUtility::current_time() + idle_timeout;
  ObPartitionService* partition_service = server.get_partition_service();
  EXPECT_TRUE(NULL != partition_service);
  ObStartTransParam trans_param;
  trans_param.set_access_mode(ObTransAccessMode::READ_WRITE);
  trans_param.set_type(ObTransType::TRANS_USER);
  trans_param.set_isolation(ObTransIsolation::READ_COMMITED);
  trans_param.set_cluster_version(GET_MIN_CLUSTER_VERSION());
  ObPartitionArray participants;
  ObPartitionArray discard_participants;
  EXPECT_EQ(OB_SUCCESS, participants.push_back(pkey));
  ObStmtDesc& stmt_desc = trans_desc.get_cur_stmt_desc();
  stmt_desc.phy_plan_type_ = OB_PHY_PLAN_DISTRIBUTED;
  stmt_desc.stmt_type_ = stmt::T_SELECT;
  stmt_desc.is_sfu_ = false;
  stmt_desc.consistency_level_ = ObTransConsistencyLevel::STRONG;

  ObPartitionLeaderArray arr;
  ObStmtParam stmt_param;
  EXPECT_EQ(OB_SUCCESS, stmt_param.init(tenant_id, stmt_expired_time, false));

  sleep(1);
  // begine transaction
  ret = partition_service->start_trans(tenant_id, thread_id, trans_param, trans_expired_time, 0, 0, trans_desc);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_SUCCESS, arr.push(pkey, trans_desc.get_scheduler()));
  TRANS_LOG(INFO, "start statment", K(tenant_id), K(trans_desc), K(participants));
  ret = partition_service->start_stmt(stmt_param, trans_desc, arr, participants);
  TRANS_LOG(INFO, "start statment ok");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_SUCCESS, participants.push_back(pkey));
  ObPartitionEpochArray partition_epoch_arr;
  ret = partition_service->start_participant(trans_desc, participants, partition_epoch_arr);
  EXPECT_EQ(OB_SUCCESS, ret);
  TRANS_LOG(INFO, "start participant ok");

  // scan rows
  scan_param.trans_desc_ = &trans_desc;
  scan_param.pkey_ = pkey;
  scan_param.timeout_ = ObTimeUtility::current_time() + 1000000000;
  scan_param.scan_flag_.flag_ = 0;  // no flag
  scan_param.reserved_cell_count_ = 6;
  scan_param.schema_version_ = ObRestoreSchema::RESTORE_SCHEMA_VERSION;
  ObNewRowIterator* scan_iter = NULL;
  ret = partition_service->table_scan(scan_param, scan_iter);
  EXPECT_EQ(OB_SUCCESS, ret);
  ObMockIterator scan_expect;
  ASSERT_EQ(OB_SUCCESS, scan_expect.from(scan_str));
  check_result_iter(*scan_iter, scan_expect);
  ret = partition_service->revert_scan_iter(scan_iter);
  EXPECT_EQ(OB_SUCCESS, ret);

  EXPECT_EQ(OB_SUCCESS, participants.push_back(pkey));
  ret = partition_service->end_participant(rollback, trans_desc, participants);
  EXPECT_EQ(OB_SUCCESS, ret);
  // end scan statement
  transaction::ObPartitionEpochArray epoch_arr;
  bool incomplete = false;
  ret =
      partition_service->end_stmt(rollback, incomplete, participants, epoch_arr, discard_participants, arr, trans_desc);
  EXPECT_EQ(OB_SUCCESS, ret);

  // end transaction
  static MockObEndTransCallback callback;
  ret = partition_service->end_trans(rollback, trans_desc, callback, ObTimeUtility::current_time() + 100000000);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// void do_create_check(const common::ObPartitionKey &pkey,
// const ObVersion &version,
// const ObMemberList &member_list,
// const int64_t replica_num,
// MockObServer &server)
//{
// int ret = OB_SUCCESS;
// ObArray<ObTableSchema> schemas;
// ObTableSchema table_schema;
// ObPartitionService *partition_service = server.get_partition_service();
// EXPECT_TRUE(NULL != partition_service);
// int64_t now = ObTimeUtility::current_time();
// int64_t schema_version = 0;
// ret = ObInnerTableSchema::all_core_table_schema(table_schema);
// EXPECT_EQ(OB_SUCCESS, ret);
// table_schema.set_table_id(pkey.get_table_id());
// ret = schemas.push_back(table_schema);
// EXPECT_EQ(OB_SUCCESS, ret);
// ret = partition_service->create_partition(pkey,
// schema_version,
// version,
// replica_num,
// member_list,
// server.get_self(),
// now,
// now,
// schemas);
// EXPECT_EQ(OB_SUCCESS, ret);
// sleep(1);
//}

void do_insert_check(const common::ObPartitionKey& pkey, const char* ins_str,
    const common::ObIArray<uint64_t>& column_ids, MockObServer& server)
{
  int ret = OB_SUCCESS;
  ObTransDesc trans_desc;
  uint64_t tenant_id = OB_SYS_TENANT_ID;
  const int64_t thread_id = 100;
  bool rollback = false;
  int64_t timeout = 1000000000;
  int64_t idle_timeout = 500000000;
  int64_t trans_expired_time = ObTimeUtility::current_time() + timeout;
  int64_t stmt_expired_time = ObTimeUtility::current_time() + idle_timeout;
  int64_t affected_rows = 0;
  ObPartitionService* partition_service = server.get_partition_service();
  EXPECT_TRUE(NULL != partition_service);
  ObStartTransParam trans_param;
  trans_param.set_access_mode(ObTransAccessMode::READ_WRITE);
  trans_param.set_type(ObTransType::TRANS_USER);
  trans_param.set_isolation(ObTransIsolation::READ_COMMITED);
  trans_param.set_cluster_version(GET_MIN_CLUSTER_VERSION());
  ObPartitionArray participants;
  ObPartitionArray discard_participants;
  EXPECT_EQ(OB_SUCCESS, participants.push_back(pkey));
  ObStmtDesc& stmt_desc = trans_desc.get_cur_stmt_desc();
  stmt_desc.phy_plan_type_ = OB_PHY_PLAN_DISTRIBUTED;
  stmt_desc.stmt_type_ = stmt::T_INSERT;
  stmt_desc.is_sfu_ = false;
  stmt_desc.consistency_level_ = ObTransConsistencyLevel::STRONG;

  ObPartitionLeaderArray arr;
  ObStmtParam stmt_param;
  EXPECT_EQ(OB_SUCCESS, stmt_param.init(tenant_id, stmt_expired_time, false));

  sleep(1);
  // begine transaction
  ret = partition_service->start_trans(tenant_id, thread_id, trans_param, trans_expired_time, 0, 0, trans_desc);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_SUCCESS, arr.push(pkey, trans_desc.get_scheduler()));
  TRANS_LOG(INFO, "start statment", K(tenant_id), K(trans_desc), K(participants));
  ret = partition_service->start_stmt(stmt_param, trans_desc, arr, participants);
  EXPECT_EQ(OB_SUCCESS, ret);
  TRANS_LOG(INFO, "start statment ok");
  EXPECT_EQ(OB_SUCCESS, participants.push_back(pkey));
  ObPartitionEpochArray partition_epoch_arr;
  ret = partition_service->start_participant(trans_desc, participants, partition_epoch_arr);
  EXPECT_EQ(OB_SUCCESS, ret);
  TRANS_LOG(INFO, "start participant ok");

  // insert values
  ObMockNewRowIterator ins_iter;
  ASSERT_EQ(OB_SUCCESS, ins_iter.from(ins_str));
  ObDMLBaseParam dml_param;
  dml_param.timeout_ = ObTimeUtility::current_time() + timeout;
  dml_param.schema_version_ = ObRestoreSchema::RESTORE_SCHEMA_VERSION;
  ret = partition_service->insert_rows(trans_desc, dml_param, pkey, column_ids, &ins_iter, affected_rows);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_SUCCESS, participants.push_back(pkey));
  ret = partition_service->end_participant(rollback, trans_desc, participants);
  EXPECT_EQ(OB_SUCCESS, ret);
  // end insert statement
  transaction::ObPartitionEpochArray epoch_arr;
  bool incomplete = false;
  ret =
      partition_service->end_stmt(rollback, incomplete, participants, epoch_arr, discard_participants, arr, trans_desc);
  EXPECT_EQ(OB_SUCCESS, ret);

  // end transaction
  static MockObEndTransCallback callback;
  ret = partition_service->end_trans(rollback, trans_desc, callback, ObTimeUtility::current_time() + 100000000);
  EXPECT_EQ(OB_SUCCESS, ret);
}

void do_insert_duplicate_check(const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
    const common::ObIArray<uint64_t>& duplicated_column_ids, const common::ObNewRow& row, const ObInsertFlag flag,
    const char* duplicate_str,  // NULL if no confliction
    MockObServer& server)
{
  int ret = OB_SUCCESS;
  ObTransDesc trans_desc;
  uint64_t tenant_id = OB_SYS_TENANT_ID;
  const int64_t thread_id = 100;
  bool rollback = false;
  int64_t timeout = 1000000000;
  int64_t idle_timeout = 500000000;
  int64_t trans_expired_time = ObTimeUtility::current_time() + timeout;
  int64_t stmt_expired_time = ObTimeUtility::current_time() + idle_timeout;
  int64_t affected_rows = 0;
  ObPartitionService* partition_service = server.get_partition_service();
  EXPECT_TRUE(NULL != partition_service);
  ObStartTransParam trans_param;
  trans_param.set_access_mode(ObTransAccessMode::READ_WRITE);
  trans_param.set_type(ObTransType::TRANS_USER);
  trans_param.set_isolation(ObTransIsolation::READ_COMMITED);
  trans_param.set_cluster_version(GET_MIN_CLUSTER_VERSION());
  ObPartitionArray participants;
  ObPartitionArray discard_participants;
  EXPECT_EQ(OB_SUCCESS, participants.push_back(pkey));
  ObStmtDesc& stmt_desc = trans_desc.get_cur_stmt_desc();
  stmt_desc.phy_plan_type_ = OB_PHY_PLAN_DISTRIBUTED;
  stmt_desc.stmt_type_ = stmt::T_INSERT;
  stmt_desc.is_sfu_ = false;
  stmt_desc.consistency_level_ = ObTransConsistencyLevel::STRONG;

  ObPartitionLeaderArray arr;
  ObStmtParam stmt_param;
  EXPECT_EQ(OB_SUCCESS, stmt_param.init(tenant_id, stmt_expired_time, false));

  // begine transaction
  ret = partition_service->start_trans(tenant_id, thread_id, trans_param, trans_expired_time, 0, 0, trans_desc);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_SUCCESS, arr.push(pkey, trans_desc.get_scheduler()));
  TRANS_LOG(INFO, "start statment", K(tenant_id), K(trans_desc), K(participants));
  ret = partition_service->start_stmt(stmt_param, trans_desc, arr, participants);
  EXPECT_EQ(OB_SUCCESS, ret);
  TRANS_LOG(INFO, "start statment ok");
  EXPECT_EQ(OB_SUCCESS, participants.push_back(pkey));
  ObPartitionEpochArray partition_epoch_arr;
  ret = partition_service->start_participant(trans_desc, participants, partition_epoch_arr);
  EXPECT_EQ(OB_SUCCESS, ret);
  TRANS_LOG(INFO, "start participant ok");

  // insert values
  ObNewRowIterator* duplicated_rows = NULL;
  ObDMLBaseParam dml_param;
  dml_param.timeout_ = ObTimeUtility::current_time() + timeout;
  dml_param.schema_version_ = ObRestoreSchema::RESTORE_SCHEMA_VERSION;
  ret = partition_service->insert_row(
      trans_desc, dml_param, pkey, column_ids, duplicated_column_ids, row, flag, affected_rows, duplicated_rows);
  if (NULL != duplicated_rows) {
    EXPECT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE, ret);
  } else {
    EXPECT_EQ(OB_SUCCESS, ret);
  }

  if (NULL != duplicate_str) {
    EXPECT_TRUE(NULL != duplicated_rows);
    ObMockIterator dup_iter;
    ASSERT_EQ(OB_SUCCESS, dup_iter.from(duplicate_str));
    check_result_iter(*duplicated_rows, dup_iter);
    ASSERT_EQ(OB_SUCCESS, partition_service->revert_insert_iter(pkey, duplicated_rows));
  }

  EXPECT_EQ(OB_SUCCESS, participants.push_back(pkey));
  ret = partition_service->end_participant(rollback, trans_desc, participants);
  EXPECT_EQ(OB_SUCCESS, ret);
  // end insert statement
  transaction::ObPartitionEpochArray epoch_arr;
  bool incomplete = false;
  ret =
      partition_service->end_stmt(rollback, incomplete, participants, epoch_arr, discard_participants, arr, trans_desc);
  EXPECT_EQ(OB_SUCCESS, ret);
  // end transaction
  static MockObEndTransCallback callback;
  ret = partition_service->end_trans(rollback, trans_desc, callback, ObTimeUtility::current_time() + 100000000);
  EXPECT_EQ(OB_SUCCESS, ret);
}

}  // namespace unittest
}  // namespace oceanbase
