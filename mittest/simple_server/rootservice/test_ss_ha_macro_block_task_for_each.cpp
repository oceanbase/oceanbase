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

#define private public
#include "close_modules/shared_storage/share/backup/ob_ss_ha_macro_block_table_operator.h"
#include "share/backup/ob_ss_ha_macro_block_struct.h"

namespace oceanbase
{
using namespace unittest;
namespace share
{
using namespace common;
using namespace blocksstable;

// Helper: build a valid ObSSHAMacroBlockTaskAttr that can survive process_task_row_ validation.
// Each task contains one macro block; macro_block_cnt_ == batch.count() == 1.
static int build_valid_task_attr(
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type,
    const int64_t ls_id,
    const int64_t task_id,
    ObSSHAMacroBlockTaskAttr &attr)
{
  int ret = OB_SUCCESS;
  attr.reset();
  attr.tenant_id_ = tenant_id;
  attr.parent_task_id_ = parent_task_id;
  attr.task_type_ = task_type;
  attr.ls_id_ = ObLSID(ls_id);
  attr.task_id_ = task_id;
  attr.status_ = ObBackupTaskStatus(ObBackupTaskStatus::INIT);

  // Build a valid ObSSHAMacroBlockBatch with one macro block
  ObSSHAMacroBlockBatch batch;
  ObSSHAMacroBlockInfo info;
  info.ls_id_ = ObLSID(ls_id);
  info.macro_block_id_ = MacroBlockId(0 /*write_seq*/, task_id /*block_index*/, 0 /*third_id*/);
  info.occupy_size_ = 1024;
  if (OB_FAIL(batch.add_one(info))) {
    LOG_WARN("failed to add macro block info", KR(ret));
  } else if (OB_FAIL(attr.macro_id_list_.set_macro_block_batch(batch))) {
    LOG_WARN("failed to set macro block batch", KR(ret));
  } else {
    attr.macro_block_cnt_ = batch.count();
  }
  return ret;
}

// Helper: delete all tasks for a given (parent_task_id, task_type)
static int cleanup_tasks(ObISQLClient &sql_proxy, const uint64_t tenant_id,
    const int64_t parent_task_id, const ObSSHAMacroTaskType &task_type)
{
  return ObSSHAMacroBlockTaskOperator::delete_tasks(
      sql_proxy, tenant_id, parent_task_id, task_type, nullptr);
}

class TestSSHAMacroBlockTaskForEach : public ObSimpleClusterTestBase
{
public:
  TestSSHAMacroBlockTaskForEach()
    : ObSimpleClusterTestBase("test_ss_ha_macro_block_task_for_each") {}
};

// Test for_each_macro_block_task_for_ls with small batch_size to exercise multi-batch pagination.
// This verifies the fix for the bug where task_id > last_task_id pagination skipped rows
// when multiple (parent_task_id, task_type) combinations shared the same task_id values.
TEST_F(TestSSHAMacroBlockTaskForEach, test_for_each_pagination)
{
  int ret = OB_SUCCESS;
  ObSimpleClusterTestBase::SetUp();
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  LOG_INFO("new tenant_id", K(tenant_id));
  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();

  const int64_t parent_task_id = 100;
  const ObSSHAMacroTaskType task_type(ObSSHAMacroTaskType::Type::BACKUP);
  const int64_t ls_id = 1001;
  const int64_t total_tasks = 10;

  // Cleanup any leftover data
  ASSERT_EQ(OB_SUCCESS, cleanup_tasks(sql_proxy, tenant_id, parent_task_id, task_type));

  // 1. Insert tasks: task_id 1..10 for (parent_task_id=100, task_type=BACKUP, ls_id=1001)
  {
    ObArray<ObSSHAMacroBlockTaskAttr> attrs;
    ObSSHAMacroBlockTaskAttr attr;
    for (int64_t t = 1; t <= total_tasks; ++t) {
      ASSERT_EQ(OB_SUCCESS, build_valid_task_attr(tenant_id, parent_task_id, task_type, ls_id, t, attr));
      ASSERT_EQ(OB_SUCCESS, attrs.push_back(attr));
    }
    ASSERT_EQ(OB_SUCCESS,
        ObSSHAMacroBlockTaskOperator::batch_insert_macro_block_tasks(sql_proxy, tenant_id, attrs));
  }

  // 2. for_each with batch_size=3 — forces 4 batches (3+3+3+1), verifies pagination across batches
  {
    int64_t processed_count = 0;
    ObSSHAMacroBlockTaskOperator::TaskProcessor counter_processor;
    auto count_fn = [&processed_count](const ObSSHAMacroBlockBatch &) -> int {
      ++processed_count;
      return OB_SUCCESS;
    };
    ASSERT_EQ(OB_SUCCESS, counter_processor.assign(count_fn));

    ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
        sql_proxy, tenant_id, parent_task_id, task_type, ObLSID(ls_id),
        3 /*batch_size*/, counter_processor));
    ASSERT_EQ(total_tasks, processed_count);
    LOG_INFO("test_for_each_pagination: batch_size=3 passed", K(processed_count));
  }

  // 3. for_each with batch_size=1 — extreme case, one row per batch
  {
    int64_t processed_count = 0;
    ObSSHAMacroBlockTaskOperator::TaskProcessor counter_processor;
    auto count_fn = [&processed_count](const ObSSHAMacroBlockBatch &) -> int {
      ++processed_count;
      return OB_SUCCESS;
    };
    ASSERT_EQ(OB_SUCCESS, counter_processor.assign(count_fn));

    ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
        sql_proxy, tenant_id, parent_task_id, task_type, ObLSID(ls_id),
        1 /*batch_size*/, counter_processor));
    ASSERT_EQ(total_tasks, processed_count);
    LOG_INFO("test_for_each_pagination: batch_size=1 passed", K(processed_count));
  }

  // 4. for_each with batch_size larger than total — single batch
  {
    int64_t processed_count = 0;
    ObSSHAMacroBlockTaskOperator::TaskProcessor counter_processor;
    auto count_fn = [&processed_count](const ObSSHAMacroBlockBatch &) -> int {
      ++processed_count;
      return OB_SUCCESS;
    };
    ASSERT_EQ(OB_SUCCESS, counter_processor.assign(count_fn));

    ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
        sql_proxy, tenant_id, parent_task_id, task_type, ObLSID(ls_id),
        2048 /*batch_size*/, counter_processor));
    ASSERT_EQ(total_tasks, processed_count);
    LOG_INFO("test_for_each_pagination: batch_size=2048 passed", K(processed_count));
  }

  // 5. for_each on empty ls — should process 0 rows
  {
    int64_t processed_count = 0;
    ObSSHAMacroBlockTaskOperator::TaskProcessor counter_processor;
    auto count_fn = [&processed_count](const ObSSHAMacroBlockBatch &) -> int {
      ++processed_count;
      return OB_SUCCESS;
    };
    ASSERT_EQ(OB_SUCCESS, counter_processor.assign(count_fn));

    ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
        sql_proxy, tenant_id, parent_task_id, task_type, ObLSID(9999),
        3 /*batch_size*/, counter_processor));
    ASSERT_EQ(0, processed_count);
    LOG_INFO("test_for_each_pagination: empty ls passed", K(processed_count));
  }

  // Cleanup
  ASSERT_EQ(OB_SUCCESS, cleanup_tasks(sql_proxy, tenant_id, parent_task_id, task_type));
}

// Test that different (parent_task_id, task_type) scopes are properly isolated.
// This is the core scenario that the original bug affected: without parent_task_id/task_type
// in the WHERE clause, rows from different scopes with the same task_id would collide.
TEST_F(TestSSHAMacroBlockTaskForEach, test_scope_isolation)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();

  const int64_t parent_task_id_a = 200;
  const int64_t parent_task_id_b = 201;
  const ObSSHAMacroTaskType backup_type(ObSSHAMacroTaskType::Type::BACKUP);
  const ObSSHAMacroTaskType restore_type(ObSSHAMacroTaskType::Type::RESTORE);
  const int64_t ls_id = 1001;

  // Cleanup
  ASSERT_EQ(OB_SUCCESS, cleanup_tasks(sql_proxy, tenant_id, parent_task_id_a, backup_type));
  ASSERT_EQ(OB_SUCCESS, cleanup_tasks(sql_proxy, tenant_id, parent_task_id_b, backup_type));
  ASSERT_EQ(OB_SUCCESS, cleanup_tasks(sql_proxy, tenant_id, parent_task_id_a, restore_type));

  // Insert tasks in three different scopes, all sharing the same ls_id and overlapping task_ids:
  //   Scope A: (parent_task_id=200, BACKUP,  ls_id=1001) -> task_id 1..5
  //   Scope B: (parent_task_id=201, BACKUP,  ls_id=1001) -> task_id 1..3
  //   Scope C: (parent_task_id=200, RESTORE, ls_id=1001) -> task_id 1..4
  {
    ObArray<ObSSHAMacroBlockTaskAttr> attrs;
    ObSSHAMacroBlockTaskAttr attr;

    // Scope A
    for (int64_t t = 1; t <= 5; ++t) {
      ASSERT_EQ(OB_SUCCESS, build_valid_task_attr(tenant_id, parent_task_id_a, backup_type, ls_id, t, attr));
      ASSERT_EQ(OB_SUCCESS, attrs.push_back(attr));
    }
    // Scope B
    for (int64_t t = 1; t <= 3; ++t) {
      ASSERT_EQ(OB_SUCCESS, build_valid_task_attr(tenant_id, parent_task_id_b, backup_type, ls_id, t, attr));
      ASSERT_EQ(OB_SUCCESS, attrs.push_back(attr));
    }
    // Scope C
    for (int64_t t = 1; t <= 4; ++t) {
      ASSERT_EQ(OB_SUCCESS, build_valid_task_attr(tenant_id, parent_task_id_a, restore_type, ls_id, t, attr));
      ASSERT_EQ(OB_SUCCESS, attrs.push_back(attr));
    }
    ASSERT_EQ(OB_SUCCESS,
        ObSSHAMacroBlockTaskOperator::batch_insert_macro_block_tasks(sql_proxy, tenant_id, attrs));
  }

  // Query Scope A with batch_size=2 — should get exactly 5 rows
  {
    int64_t count = 0;
    ObSSHAMacroBlockTaskOperator::TaskProcessor processor;
    auto fn = [&count](const ObSSHAMacroBlockBatch &) -> int { ++count; return OB_SUCCESS; };
    ASSERT_EQ(OB_SUCCESS, processor.assign(fn));

    ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
        sql_proxy, tenant_id, parent_task_id_a, backup_type, ObLSID(ls_id),
        2 /*batch_size*/, processor));
    ASSERT_EQ(5, count);
    LOG_INFO("scope A (BACKUP, ptid=200): OK", K(count));
  }

  // Query Scope B with batch_size=2 — should get exactly 3 rows
  {
    int64_t count = 0;
    ObSSHAMacroBlockTaskOperator::TaskProcessor processor;
    auto fn = [&count](const ObSSHAMacroBlockBatch &) -> int { ++count; return OB_SUCCESS; };
    ASSERT_EQ(OB_SUCCESS, processor.assign(fn));

    ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
        sql_proxy, tenant_id, parent_task_id_b, backup_type, ObLSID(ls_id),
        2 /*batch_size*/, processor));
    ASSERT_EQ(3, count);
    LOG_INFO("scope B (BACKUP, ptid=201): OK", K(count));
  }

  // Query Scope C with batch_size=2 — should get exactly 4 rows
  {
    int64_t count = 0;
    ObSSHAMacroBlockTaskOperator::TaskProcessor processor;
    auto fn = [&count](const ObSSHAMacroBlockBatch &) -> int { ++count; return OB_SUCCESS; };
    ASSERT_EQ(OB_SUCCESS, processor.assign(fn));

    ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
        sql_proxy, tenant_id, parent_task_id_a, restore_type, ObLSID(ls_id),
        2 /*batch_size*/, processor));
    ASSERT_EQ(4, count);
    LOG_INFO("scope C (RESTORE, ptid=200): OK", K(count));
  }

  // Cleanup
  ASSERT_EQ(OB_SUCCESS, cleanup_tasks(sql_proxy, tenant_id, parent_task_id_a, backup_type));
  ASSERT_EQ(OB_SUCCESS, cleanup_tasks(sql_proxy, tenant_id, parent_task_id_b, backup_type));
  ASSERT_EQ(OB_SUCCESS, cleanup_tasks(sql_proxy, tenant_id, parent_task_id_a, restore_type));
}

// Test invalid arguments
TEST_F(TestSSHAMacroBlockTaskForEach, test_invalid_args)
{
  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();
  const ObSSHAMacroTaskType backup_type(ObSSHAMacroTaskType::Type::BACKUP);
  ObSSHAMacroBlockTaskOperator::TaskProcessor processor;
  auto noop = [](const ObSSHAMacroBlockBatch &) -> int { return OB_SUCCESS; };
  ASSERT_EQ(OB_SUCCESS, processor.assign(noop));

  // invalid tenant_id
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
      sql_proxy, OB_INVALID_TENANT_ID, 1, backup_type, ObLSID(1001), 10, processor));
  // invalid parent_task_id
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
      sql_proxy, 1002, 0, backup_type, ObLSID(1001), 10, processor));
  // invalid task_type
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
      sql_proxy, 1002, 1, ObSSHAMacroTaskType(), ObLSID(1001), 10, processor));
  // invalid batch_size
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
      sql_proxy, 1002, 1, backup_type, ObLSID(1001), 0, processor));
  // invalid ls_id
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSHAMacroBlockTaskOperator::for_each_macro_block_task_for_ls(
      sql_proxy, 1002, 1, backup_type, ObLSID(), 10, processor));
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
