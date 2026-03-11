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

static int get_task_count(ObISQLClient &sql_proxy, const uint64_t tenant_id,
    const int64_t parent_task_id, const ObSSHAMacroTaskType &task_type, int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt(
        "SELECT COUNT(*) AS cnt FROM %s"
        " WHERE tenant_id = %lu AND parent_task_id = %ld AND task_type = '%s'",
        OB_ALL_TENANT_MACRO_BLOCK_HA_TASK_TNAME,
        tenant_id, parent_task_id, task_type.get_str()))) {
      LOG_WARN("failed to assign sql", KR(ret));
    } else if (OB_FAIL(sql_proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("failed to get next", KR(ret));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "cnt", count, int64_t);
    }
  }
  return ret;
}

static int batch_delete_tasks(ObISQLClient &sql_proxy, const uint64_t tenant_id,
    const int64_t parent_task_id, const ObSSHAMacroTaskType &task_type)
{
  return ObSSHAMacroBlockTaskOperator::delete_tasks(
      sql_proxy, tenant_id, parent_task_id, task_type, nullptr/*backup_service*/);
}

static int build_task_attr(const uint64_t tenant_id, const int64_t parent_task_id,
    const ObSSHAMacroTaskType &task_type, const int64_t ls_id, const int64_t task_id,
    ObSSHAMacroBlockTaskAttr &attr)
{
  int ret = OB_SUCCESS;
  attr.reset();
  attr.tenant_id_ = tenant_id;
  attr.parent_task_id_ = parent_task_id;
  attr.task_type_ = task_type;
  attr.ls_id_ = ObLSID(ls_id);
  attr.task_id_ = task_id;
  attr.macro_block_cnt_ = 1;
  attr.status_ = ObBackupTaskStatus(ObBackupTaskStatus::INIT);
  ObSSHAMacroBlockBatch batch;
  ObSSHAMacroBlockInfo info;
  info.ls_id_ = ObLSID(ls_id);
  info.macro_block_id_ = blocksstable::MacroBlockId(0, task_id, 0);
  info.occupy_size_ = 1;
  if (OB_FAIL(batch.add_one(info))) {
    LOG_WARN("failed to add macro block info", KR(ret));
  } else if (OB_FAIL(attr.macro_id_list_.set_macro_block_batch(batch))) {
    LOG_WARN("failed to set macro_id_list", KR(ret));
  }
  return ret;
}

class TestSSHAMacroBlockTaskOperator : public ObSimpleClusterTestBase
{
public:
  TestSSHAMacroBlockTaskOperator() : ObSimpleClusterTestBase("test_ss_ha_macro_block_task_operator") {}
};

TEST_F(TestSSHAMacroBlockTaskOperator, test_delete_tasks)
{
  int ret = OB_SUCCESS;
  ObSimpleClusterTestBase::SetUp();
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  LOG_INFO("new tenant_id", K(tenant_id));
  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();

  const int64_t parent_task_id = 1;
  const ObSSHAMacroTaskType task_type(ObSSHAMacroTaskType::Type::BACKUP);
  int64_t count = 0;

  // 1. delete_tasks on empty table — should succeed with 0 batches
  ASSERT_EQ(OB_SUCCESS, batch_delete_tasks(
      sql_proxy, tenant_id, parent_task_id, task_type));

  // 2. insert test data: 250 tasks across 3 ls_ids
  //    This ensures multiple batches (BATCH_SIZE = 100)
  {
    ObArray<ObSSHAMacroBlockTaskAttr> attrs;
    ObSSHAMacroBlockTaskAttr attr;
    for (int64_t ls = 1001; ls <= 1003; ++ls) {
      int64_t task_cnt = (ls == 1001) ? 100 : (ls == 1002 ? 100 : 50);
      for (int64_t t = 1; t <= task_cnt; ++t) {
        ASSERT_EQ(OB_SUCCESS, build_task_attr(tenant_id, parent_task_id, task_type, ls, t, attr));
        ASSERT_EQ(OB_SUCCESS, attrs.push_back(attr));
      }
    }
    ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::batch_insert_macro_block_tasks(
        sql_proxy, tenant_id, attrs));
  }

  // verify 250 rows inserted
  ASSERT_EQ(OB_SUCCESS, get_task_count(sql_proxy, tenant_id, parent_task_id, task_type, count));
  ASSERT_EQ(250, count);

  // 3. delete_tasks — should delete all 250 rows in multiple batches
  ASSERT_EQ(OB_SUCCESS, batch_delete_tasks(
      sql_proxy, tenant_id, parent_task_id, task_type));

  // verify all deleted
  ASSERT_EQ(OB_SUCCESS, get_task_count(sql_proxy, tenant_id, parent_task_id, task_type, count));
  ASSERT_EQ(0, count);

  // 4. delete_tasks again on empty table — should succeed (idempotent)
  ASSERT_EQ(OB_SUCCESS, batch_delete_tasks(
      sql_proxy, tenant_id, parent_task_id, task_type));

  // 5. test isolation: insert tasks for two different parent_task_ids,
  //    delete one, verify the other is untouched
  {
    const int64_t parent_task_id_a = 10;
    const int64_t parent_task_id_b = 20;
    ObArray<ObSSHAMacroBlockTaskAttr> attrs;
    ObSSHAMacroBlockTaskAttr attr;
    for (int64_t t = 1; t <= 5; ++t) {
      ASSERT_EQ(OB_SUCCESS, build_task_attr(tenant_id, parent_task_id_a, task_type, 1001, t, attr));
      ASSERT_EQ(OB_SUCCESS, attrs.push_back(attr));
    }
    for (int64_t t = 1; t <= 3; ++t) {
      ASSERT_EQ(OB_SUCCESS, build_task_attr(tenant_id, parent_task_id_b, task_type, 1001, t, attr));
      ASSERT_EQ(OB_SUCCESS, attrs.push_back(attr));
    }
    ASSERT_EQ(OB_SUCCESS, ObSSHAMacroBlockTaskOperator::batch_insert_macro_block_tasks(
        sql_proxy, tenant_id, attrs));

    // delete only parent_task_id_a
    ASSERT_EQ(OB_SUCCESS, batch_delete_tasks(
        sql_proxy, tenant_id, parent_task_id_a, task_type));

    // parent_task_id_a should be empty
    ASSERT_EQ(OB_SUCCESS, get_task_count(sql_proxy, tenant_id, parent_task_id_a, task_type, count));
    ASSERT_EQ(0, count);

    // parent_task_id_b should be untouched
    ASSERT_EQ(OB_SUCCESS, get_task_count(sql_proxy, tenant_id, parent_task_id_b, task_type, count));
    ASSERT_EQ(3, count);

    // cleanup
    ASSERT_EQ(OB_SUCCESS, batch_delete_tasks(
        sql_proxy, tenant_id, parent_task_id_b, task_type));
  }

  // 6. test invalid arguments
  ASSERT_EQ(OB_INVALID_ARGUMENT, batch_delete_tasks(
      sql_proxy, OB_INVALID_TENANT_ID, parent_task_id, task_type));
  ASSERT_EQ(OB_INVALID_ARGUMENT, batch_delete_tasks(
      sql_proxy, tenant_id, 0, task_type));
  ASSERT_EQ(OB_INVALID_ARGUMENT, batch_delete_tasks(
      sql_proxy, tenant_id, parent_task_id, ObSSHAMacroTaskType()));
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
