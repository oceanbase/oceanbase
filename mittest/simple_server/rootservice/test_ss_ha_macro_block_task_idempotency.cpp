// owner: yangyi.yyy
// owner group: backup_data

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

#define USING_LOG_PREFIX SHARE

#include "env/ob_simple_cluster_test_base.h"
#include "share/backup/ob_ss_ha_macro_block_struct.h"
#include "close_modules/shared_storage/share/backup/ob_ss_ha_macro_block_table_operator.h"

namespace oceanbase
{
using namespace unittest;
namespace share
{
using namespace common;

static const ObSSHAMacroTaskType TASK_TYPE(ObSSHAMacroTaskType::Type::BACKUP);

class TestSSHAMacroBlockTaskOperator : public ObSimpleClusterTestBase
{
public:
  TestSSHAMacroBlockTaskOperator() : ObSimpleClusterTestBase("test_ss_ha_macro_block_task_idempotency") {}

  static void SetUpTestCase()
  {
    ObSimpleClusterTestBase::SetUpTestCase();
  }

  void setup_task_attr(ObSSHAMacroBlockTaskAttr &attr,
                       const uint64_t tenant_id,
                       const int64_t parent_task_id,
                       const int64_t ls_id,
                       const int64_t task_id,
                       const int64_t macro_block_cnt);

  void insert_tasks(ObMySQLProxy &sql_proxy,
                    const uint64_t tenant_id,
                    const int64_t parent_task_id,
                    const int64_t task_count);
};

void TestSSHAMacroBlockTaskOperator::setup_task_attr(
    ObSSHAMacroBlockTaskAttr &attr,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const int64_t ls_id,
    const int64_t task_id,
    const int64_t macro_block_cnt)
{
  attr.reset();
  attr.tenant_id_ = tenant_id;
  attr.parent_task_id_ = parent_task_id;
  attr.ls_id_ = ObLSID(ls_id);
  attr.task_id_ = task_id;
  attr.task_type_ = TASK_TYPE;
  attr.macro_block_cnt_ = macro_block_cnt;
  attr.status_.set_status(ObBackupTaskStatus::Status::INIT);
  attr.start_time_ = ObTimeUtility::current_time();
  attr.end_time_ = 0;
  attr.result_ = OB_SUCCESS;
  attr.retry_cnt_ = 0;
  attr.total_bytes_ = macro_block_cnt * 2 * 1024 * 1024L; // 2MB per macro block
  attr.finish_bytes_ = macro_block_cnt * 2 * 1024 * 1024L;
}

void TestSSHAMacroBlockTaskOperator::insert_tasks(
    ObMySQLProxy &sql_proxy,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const int64_t task_count)
{
  ObArray<ObSSHAMacroBlockTaskAttr> task_attrs;
  for (int64_t i = 1; i <= task_count; ++i) {
    ObSSHAMacroBlockTaskAttr attr;
    setup_task_attr(attr, tenant_id, parent_task_id, /*ls_id=*/1001, /*task_id=*/i, /*macro_block_cnt=*/10);
    ASSERT_EQ(OB_SUCCESS, task_attrs.push_back(attr));
  }
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::batch_insert_macro_block_tasks(
      sql_proxy, tenant_id, task_attrs));
}

// Test check_all_tasks_finished: verifies DB-based completion check
TEST_F(TestSSHAMacroBlockTaskOperator, test_check_all_tasks_finished)
{
  int ret = OB_SUCCESS;
  ObSimpleClusterTestBase::SetUp();
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  LOG_INFO("test_check_all_tasks_finished: tenant created", K(tenant_id));

  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();

  const int64_t PARENT_TASK_ID = 1;
  const int64_t TASK_COUNT = 3;

  // Step 1: Insert progress row (mgr state) with total_task_count = 3
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::insert_mgr_state_init(
      sql_proxy, tenant_id, PARENT_TASK_ID, TASK_TYPE, TASK_COUNT));

  // Step 2: Insert 3 task rows in INIT status
  insert_tasks(sql_proxy, tenant_id, PARENT_TASK_ID, TASK_COUNT);

  // Step 3: Verify check_all_tasks_finished returns false (all tasks still INIT)
  bool is_finished = true;
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::check_all_tasks_finished(
      sql_proxy, tenant_id, PARENT_TASK_ID, TASK_TYPE, is_finished));
  ASSERT_FALSE(is_finished);

  // Step 4: Complete all 3 tasks via update_task_completion_with_progress
  const int64_t end_time = ObTimeUtility::current_time();
  for (int64_t i = 1; i <= TASK_COUNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::update_task_completion_with_progress(
        sql_proxy, tenant_id, PARENT_TASK_ID, TASK_TYPE,
        /*ls_id=*/1001, /*task_id=*/i, end_time, OB_SUCCESS, ObString("test complete")));
  }

  // Step 5: Verify check_all_tasks_finished returns true
  is_finished = false;
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::check_all_tasks_finished(
      sql_proxy, tenant_id, PARENT_TASK_ID, TASK_TYPE, is_finished));
  ASSERT_TRUE(is_finished);

  // Step 6: Verify get_task_statistics is consistent
  int64_t total_tasks = 0, finished_tasks = 0, total_macro_blocks = 0, finished_macro_blocks = 0;
  int64_t total_bytes = 0, finish_bytes = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::get_task_statistics(
      sql_proxy, tenant_id, PARENT_TASK_ID, TASK_TYPE,
      total_tasks, finished_tasks, total_macro_blocks, finished_macro_blocks,
      total_bytes, finish_bytes));
  ASSERT_EQ(TASK_COUNT, total_tasks);
  ASSERT_EQ(TASK_COUNT, finished_tasks);

  // Cleanup
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::delete_tasks(
      sql_proxy, tenant_id, PARENT_TASK_ID, TASK_TYPE, nullptr/*backup_service*/));
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::delete_mgr_state(
      sql_proxy, tenant_id, PARENT_TASK_ID, TASK_TYPE));

  LOG_INFO("test_check_all_tasks_finished: PASSED");
}

// Test update_task_completion_with_progress idempotency:
// calling it twice for the same task should NOT double-count in progress table
// Uses a different parent_task_id to isolate data from test_check_all_tasks_finished
TEST_F(TestSSHAMacroBlockTaskOperator, test_update_completion_idempotency)
{
  int ret = OB_SUCCESS;
  ObSimpleClusterTestBase::SetUp();
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  LOG_INFO("test_update_completion_idempotency: reuse tenant", K(tenant_id));

  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();

  // Use a different parent_task_id to isolate from the previous test case
  const int64_t PARENT_TASK_ID = 2;
  const int64_t TASK_COUNT = 2;

  // Step 1: Insert progress row with total_task_count = 2
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::insert_mgr_state_init(
      sql_proxy, tenant_id, PARENT_TASK_ID, TASK_TYPE, TASK_COUNT));

  // Step 2: Insert 2 task rows
  insert_tasks(sql_proxy, tenant_id, PARENT_TASK_ID, TASK_COUNT);

  const int64_t end_time = ObTimeUtility::current_time();

  // Step 3: Complete task 1
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::update_task_completion_with_progress(
      sql_proxy, tenant_id, PARENT_TASK_ID, TASK_TYPE,
      /*ls_id=*/1001, /*task_id=*/1, end_time, OB_SUCCESS, ObString("first call")));

  // Step 4: Duplicate complete for task 1 — should be idempotent (no error)
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::update_task_completion_with_progress(
      sql_proxy, tenant_id, PARENT_TASK_ID, TASK_TYPE,
      /*ls_id=*/1001, /*task_id=*/1, end_time, OB_SUCCESS, ObString("duplicate call")));

  // Step 5: Verify finished_tasks == 1 (NOT 2, proving no double-count)
  int64_t total_tasks = 0, finished_tasks = 0, total_macro_blocks = 0, finished_macro_blocks = 0;
  int64_t total_bytes = 0, finish_bytes = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::get_task_statistics(
      sql_proxy, tenant_id, PARENT_TASK_ID, TASK_TYPE,
      total_tasks, finished_tasks, total_macro_blocks, finished_macro_blocks,
      total_bytes, finish_bytes));
  ASSERT_EQ(TASK_COUNT, total_tasks);
  ASSERT_EQ(1, finished_tasks);
  LOG_INFO("after duplicate call, finished_tasks should be 1", K(finished_tasks));

  // Step 6: check_all_tasks_finished should still be false (task 2 still INIT)
  bool is_finished = true;
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::check_all_tasks_finished(
      sql_proxy, tenant_id, PARENT_TASK_ID, TASK_TYPE, is_finished));
  ASSERT_FALSE(is_finished);

  // Step 7: Complete task 2
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::update_task_completion_with_progress(
      sql_proxy, tenant_id, PARENT_TASK_ID, TASK_TYPE,
      /*ls_id=*/1001, /*task_id=*/2, end_time, OB_SUCCESS, ObString("task 2 complete")));

  // Step 8: Now both tasks are done
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::get_task_statistics(
      sql_proxy, tenant_id, PARENT_TASK_ID, TASK_TYPE,
      total_tasks, finished_tasks, total_macro_blocks, finished_macro_blocks,
      total_bytes, finish_bytes));
  ASSERT_EQ(TASK_COUNT, finished_tasks);

  is_finished = false;
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::check_all_tasks_finished(
      sql_proxy, tenant_id, PARENT_TASK_ID, TASK_TYPE, is_finished));
  ASSERT_TRUE(is_finished);

  // Cleanup
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::delete_tasks(
      sql_proxy, tenant_id, PARENT_TASK_ID, TASK_TYPE, nullptr/*backup_service*/));
  ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::delete_mgr_state(
      sql_proxy, tenant_id, PARENT_TASK_ID, TASK_TYPE));

  LOG_INFO("test_update_completion_idempotency: PASSED");
}

// Test check_all_tasks_finished with invalid arguments
TEST_F(TestSSHAMacroBlockTaskOperator, test_check_all_tasks_finished_invalid_args)
{
  int ret = OB_SUCCESS;
  ObSimpleClusterTestBase::SetUp();
  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();
  bool is_finished = false;

  // Invalid tenant_id
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSHAMacroBlockTaskOperator::check_all_tasks_finished(
      sql_proxy, OB_INVALID_TENANT_ID, 1, TASK_TYPE, is_finished));

  // Invalid parent_task_id (<= 0)
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSHAMacroBlockTaskOperator::check_all_tasks_finished(
      sql_proxy, 1002, 0, TASK_TYPE, is_finished));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSHAMacroBlockTaskOperator::check_all_tasks_finished(
      sql_proxy, 1002, -1, TASK_TYPE, is_finished));

  // Invalid task_type
  ObSSHAMacroTaskType invalid_type;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSHAMacroBlockTaskOperator::check_all_tasks_finished(
      sql_proxy, 1002, 1, invalid_type, is_finished));

  LOG_INFO("test_check_all_tasks_finished_invalid_args: PASSED");
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
