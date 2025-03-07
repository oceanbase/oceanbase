/**
 * Copyright (c) 2023 OceanBase
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
#include <gmock/gmock.h>

#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "rootserver/ob_partition_balance.h"
#include "share/ls/ob_ls_status_operator.h"

namespace oceanbase
{
using namespace unittest;
using namespace share;
using namespace share::schema;
using namespace common;
namespace rootserver
{
#define TEST_INFO(fmt, args...) FLOG_INFO("[TEST] " fmt, ##args)

#define EXE_SQL(...)                                                \
  ASSERT_EQ(OB_SUCCESS, sql.assign(__VA_ARGS__));                   \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

class TestDupTablePartitionBalance : public ObSimpleClusterTestBase
{
public:
  TestDupTablePartitionBalance() : ObSimpleClusterTestBase("test_dup_table_partition_balance") {}
};

TEST_F(TestDupTablePartitionBalance, create_tenant)
{
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  TEST_INFO("create tenant finshed", K(tenant_id));
}

// prepare duplicate_scope changed table: 9 normal->dup, 9 dup->normal
TEST_F(TestDupTablePartitionBalance, prepare_dup_change_table)
{
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ObSqlString sql;
  ObSqlString create_sql;
  int64_t affected_rows = 0;
  EXE_SQL("create table normal_1(c1 int, c2 int, c3 longtext, index(c1) local, index(c2) global)");
  EXE_SQL("create table normal_2(c1 int, c2 int, c3 longtext, index(c1) local, index(c2) global) partition by hash(c1) partitions 2");
  EXE_SQL("create table normal_3(c1 int, c2 int, c3 longtext, index(c1) local, index(c2) global) partition by range(c1) subpartition by hash(c1) subpartitions 2 (partition p1 values less than (100), partition p2 values less than MAXVALUE)");
  EXE_SQL("alter table normal_1 duplicate_scope='cluster'"); // need transfer part: 1 normal_part (the global index of nonpart table is actually local index)
  EXE_SQL("alter table normal_2 duplicate_scope='cluster'"); // need transfer part: 2 normal_part + 1 global_index
  EXE_SQL("alter table normal_3 duplicate_scope='cluster'"); // need transfer part: 4 normal_part + 1 global_index
  EXE_SQL("create table dup_1(c1 int, c2 int, c3 longtext, index(c1) local, index(c2) global) duplicate_scope = 'cluster'");
  EXE_SQL("create table dup_2(c1 int, c2 int, c3 longtext, index(c1) local, index(c2) global) duplicate_scope = 'cluster' partition by hash(c1) partitions 2");
  EXE_SQL("create table dup_3(c1 int, c2 int, c3 longtext, index(c1) local, index(c2) global) duplicate_scope = 'cluster' partition by range(c1) subpartition by hash(c1) subpartitions 2 (partition p1 values less than (100), partition p2 values less than MAXVALUE)");
  EXE_SQL("alter table dup_1 duplicate_scope='none'"); // need transfer part: 1 normal_part (the global index of nonpart table is actually local_index)
  EXE_SQL("alter table dup_2 duplicate_scope='none'"); // need transfer part: 2 normal_part + 1 global_index
  EXE_SQL("create tablegroup tg1 sharding = 'NONE'");
  EXE_SQL("alter table dup_3 duplicate_scope='none' tablegroup='tg1'"); // need transfer part: 4 normal_part + 1 global_index
  TEST_INFO("prepare dup change table finished");
}

TEST_F(TestDupTablePartitionBalance, verify_need_transfer_part_map)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  ObMySQLProxy &inner_proxy = get_curr_observer().get_mysql_proxy();

  // get dup ls id
  ObLSID dup_ls_id;
  ObLSStatusOperator status_op;
  ObLSStatusInfo dup_ls_status_info;
  ASSERT_EQ(OB_SUCCESS, status_op.get_duplicate_ls_status_info(tenant_id, inner_proxy, dup_ls_status_info));
  dup_ls_id = dup_ls_status_info.get_ls_id();

  // do check
  ObTenantSwitchGuard guard;
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id));
  ObPartitionBalance part_balance;
  ASSERT_EQ(OB_SUCCESS, part_balance.init(tenant_id, GCTX.schema_service_, &inner_proxy, 1, 1, ObPartitionBalance::GEN_TRANSFER_TASK));
  ASSERT_EQ(OB_SUCCESS, part_balance.process());

  // verify dup change part_map: 9 normal->dup, 9 dup->normal
  ASSERT_TRUE(1 == part_balance.job_generator_.normal_to_dup_part_map_.size() && 1 == part_balance.job_generator_.dup_to_normal_part_map_.size());
  for (auto iter = part_balance.job_generator_.normal_to_dup_part_map_.begin(); iter != part_balance.job_generator_.normal_to_dup_part_map_.end(); iter++) {
    ASSERT_TRUE(dup_ls_id != iter->first.get_src_ls_id() && dup_ls_id == iter->first.get_dest_ls_id());
    ASSERT_TRUE(9 == iter->second.count());
    TEST_INFO("normal_to_dup_part:", "task_key", iter->first, "part_cnt", iter->second.count(), "part_list", iter->second);
  }
  for (auto iter = part_balance.job_generator_.dup_to_normal_part_map_.begin(); iter != part_balance.job_generator_.dup_to_normal_part_map_.end(); iter++) {
    ASSERT_TRUE(dup_ls_id == iter->first.get_src_ls_id() && dup_ls_id != iter->first.get_dest_ls_id());
    ASSERT_TRUE(9 == iter->second.count());
    TEST_INFO("dup_to_normal_part:", "task_key", iter->first, "part_cnt", iter->second.count(), "part_list", iter->second);
  }
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
