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
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "rootserver/ob_partition_balance.h"

namespace oceanbase
{

namespace rootserver
{
#define TEST_INFO(fmt, args...) FLOG_INFO("[TEST] " fmt, ##args)

int g_ls_cnt = 1;
int ObPartitionBalance::prepare_ls_()
{
  ls_desc_map_.create(10, lib::Label(""));
  int ls_cnt = g_ls_cnt;
  for (int i = 1; i <= ls_cnt; i++) {
    auto ls_desc = new ObLSDesc(ObLSID(i), 0, ObZone("z1"));
    ls_desc_array_.push_back(ls_desc);
    ls_desc_map_.set_refactored(ls_desc->ls_id_, ls_desc);
    job_generator_.ls_group_id_map_.set_refactored(ObLSID(i), 1001);
  }
  return OB_SUCCESS;
}

int ObPartitionBalance::process(const ObBalanceJobID &job_id, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  ObBalanceJobType job_type(ObBalanceJobType::BALANCE_JOB_PARTITION);
  ObBalanceStrategy balance_strategy(ObBalanceStrategy::PB_INTER_GROUP);
  if (OB_FAIL(prepare_balance_group_())) {
    LOG_WARN("prepare_balance_group fail", KR(ret));
  } else if (OB_FAIL(save_balance_group_stat_())) {
    LOG_WARN("save_balance_group_stat fail", KR(ret));
  } else if (OB_FAIL(process_weight_balance_intragroup_())) {
    LOG_WARN("process_weight_balance_intragroup failed", KR(ret));
  } else if (OB_FAIL(process_balance_partition_inner_())) {
    LOG_WARN("process_balance_partition_inner fail", KR(ret));
  } else if (OB_FAIL(process_balance_partition_extend_())) {
    LOG_WARN("process_balance_partition_extend fail", KR(ret));
  } else if (OB_FAIL(process_balance_partition_disk_())) {
    LOG_WARN("process_balance_partition_disk fail", KR(ret));
  } else if (!job_generator_.need_gen_job()) {
    LOG_INFO("no need gen job");
  } else if (job_generator_.need_gen_job()
            && OB_FAIL(job_generator_.gen_balance_job_and_tasks(job_type, balance_strategy))) {
    LOG_WARN("gen_balance_job_and_tasks fail", KR(ret));
  }
  return ret;
}
}


namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;


class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int time_sec_ = 0;
};

TestRunCtx RunCtx;

void print_part_map(const ObTransferPartMap &part_map, const char* label) {
  for (auto iter = part_map.begin(); iter != part_map.end(); iter++) {
    TEST_INFO("part map", K(label), "task_key", iter->first, "part_cnt", iter->second.count(), "part_list", iter->second);
  }
}

void print_part_group(const ObPartitionBalance &part_balance)
{
  FOREACH(iter, part_balance.bg_map_) {
    const ObBalanceGroup &bg = iter->first;
    const ObArray<ObBalanceGroupInfo*> &ls_part_groups = iter->second;
    for (int ls_idx = 0; ls_idx < ls_part_groups.count(); ls_idx++) {
      const ObBalanceGroupInfo *part_groups = ls_part_groups.at(ls_idx);
      ObArray<ObPartGroupInfo *> part_groups_arr;
      ObSqlString part_groups_str;
      const ObPartGroupContainer *pg_ctn = dynamic_cast<ObPartGroupContainer *>(part_groups->pg_container_);
      for (auto unit_it = pg_ctn->bg_units_.begin(); unit_it != pg_ctn->bg_units_.end(); unit_it++) {
        const ObBalanceGroupUnit *unit = *unit_it;
        for (int i = 0; i < unit->pg_buckets_.count(); i++) {
          append(part_groups_arr, unit->pg_buckets_.at(i));
        }
      }
      std::sort(part_groups_arr.begin(), part_groups_arr.end(), [] (const ObPartGroupInfo *left, const ObPartGroupInfo *right) {
        const ObTransferPartInfo &part_left = left->part_list_.at(0);
        const ObTransferPartInfo &part_right = right->part_list_.at(0);
        if (part_left.table_id_ == part_right.table_id_) {
          return part_left.part_object_id_ < part_right.part_object_id_;
        }
        return part_left.table_id_ < part_right.table_id_;
      });
      for (int i = 0; i < part_groups_arr.count(); i++) {
        ObPartGroupInfo *part_group = part_groups_arr.at(i);
        if (i > 0) {
          part_groups_str.append(" ");
        }
        part_groups_str.append("[");
        for (int j = 0; j < part_group->count(); j++) {
          ObTransferPartInfo &part = part_group->part_list_.at(j);
          if (j > 0) {
            part_groups_str.append_fmt(" ");
          }
          part_groups_str.append_fmt("%ld:%ld", part.table_id(), part.part_object_id());
        }
        part_groups_str.append_fmt("]");
      }
      LOG_INFO("balance_part_job bg_map", "balance group", bg.id_, "ls_id", part_groups->ls_id_,
              "part_group_count", part_groups->get_part_groups_count(),
              "part_groups", part_groups_str);
    }
  }
}

class ObBalancePartitionTest : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObBalancePartitionTest() : ObSimpleClusterTestBase("test_ob_balance_partition_") {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    ObSqlString sql;
    int64_t affected_rows = 0;
    sql_proxy.write("drop database test", affected_rows);
    sql_proxy.write("create database test", affected_rows);
    sql_proxy.write("use test", affected_rows);
    sql_proxy.write("drop tablegroup if exists my_tablegroup", affected_rows);
  }

  int run(int ls_cnt) {
    int ret = OB_SUCCESS;
    g_ls_cnt = ls_cnt;
    ObPartitionBalance balance_part_job;
    ObCurTraceId::get_trace_id()->set(std::string(std::to_string(ObTimeUtility::current_time()%10000)).c_str());
    ObTenantSwitchGuard guard;
    ObLSStatusOperator status_op;
    TEST_INFO("balance_part_job start>>>>>>>>>>>>>>>>>", K(ls_cnt));

    if (OB_FAIL(guard.switch_to(OB_SYS_TENANT_ID))) {
      LOG_WARN("switch tenant", KR(ret));
    } else if (OB_FAIL(balance_part_job.init(OB_SYS_TENANT_ID, GCTX.schema_service_,
        GCTX.sql_proxy_, ObPartitionBalance::GEN_TRANSFER_TASK))) {
      LOG_WARN("balance_part_job init fail", KR(ret));
    } else if (OB_FAIL(balance_part_job.process())) {
      LOG_WARN("balance_part_job process fail", KR(ret));
    } else {
      std::sort(balance_part_job.ls_desc_array_.begin(), balance_part_job.ls_desc_array_.end(), [] (const ObLSDesc* left, const ObLSDesc* right) {
        return left->get_partgroup_cnt() < right->get_partgroup_cnt();
      });
      TEST_INFO("balance_part_job bg_map size", K(balance_part_job.bg_map_.size()));
      for (auto iter = balance_part_job.bg_map_.begin(); iter != balance_part_job.bg_map_.end(); iter++) {
        for (int i = 0; i < iter->second.count(); i++) {
          TEST_INFO("balance_part_job bg_map", K(iter->first), K(iter->second.at(i)->ls_id_), K(iter->second.at(i)->get_part_groups_count()));
        }
      }
      print_part_map(balance_part_job.job_generator_.dup_to_normal_part_map_, "dup_to_normal");
      print_part_map(balance_part_job.job_generator_.normal_to_dup_part_map_, "normal_to_dup");
      print_part_map(balance_part_job.job_generator_.dup_to_dup_part_map_, "dup_to_dup");
      print_part_map(balance_part_job.job_generator_.normal_to_normal_part_map_, "normal_to_normal");
      print_part_group(balance_part_job);
      if (balance_part_job.ls_desc_array_.at(balance_part_job.ls_desc_array_.count() - 1)->get_partgroup_cnt() - balance_part_job.ls_desc_array_.at(0)->get_partgroup_cnt() > 1) {
        ret = -1;
        LOG_WARN("partition not balance", KR(ret), K(ls_cnt), KPC(balance_part_job.ls_desc_array_.at(balance_part_job.ls_desc_array_.count()-1)), KPC(balance_part_job.ls_desc_array_.at(0)));
      }
    }
    return ret;
  }
};

TEST_F(ObBalancePartitionTest, observer_start)
{
  TEST_INFO("observer_start succ");
}

TEST_F(ObBalancePartitionTest, empty)
{
  TEST_INFO("-----------empty-------------");
  for (int i = 1; i <= 2; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i));
  }
}

// 非分区表整体是一个均衡组，表做平铺
TEST_F(ObBalancePartitionTest, simple_table)
{
  TEST_INFO("-----------simple_table-------------");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // 创建表
  for (int i = 1; i <= 60; i++) {
    sql.assign_fmt("create table basic_%d(col1 int)", i);
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }
  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 20; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i));
  }
}

// 每个一级分区的表是一个均衡组，一级分区做平铺
TEST_F(ObBalancePartitionTest, partition)
{
  TEST_INFO("-----------partition-------------");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  // 创建表
  for (int i = 1; i <= 5; i++) {
    ObSqlString sql;
    sql.assign_fmt("create table partition_%d(col1 int) partition by hash(col1) partitions 30", i);
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }
  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 10; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i));
  }
}

TEST_F(ObBalancePartitionTest, subpart)
{
  TEST_INFO("-----------subpart-------------");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
   // 创建表
   for (int i = 1; i <= 5; i++) {
    ObSqlString sql;
    sql.assign_fmt("create table subpart_%d(col1 int) partition by range(col1) subpartition by hash(col1) subpartitions 20 (partition p1 values less than (100), partition p2 values less than MAXVALUE)", i);
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }
  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 10; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i));
  }
}

TEST_F(ObBalancePartitionTest, tablegroup_sharding_none)
{
  TEST_INFO("-----------tablegroup_sharding_none-------------");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  sql.assign_fmt("create tablegroup my_tablegroup sharding='NONE'");
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  for (int i = 0; i < 10; i++) {
    sql.assign_fmt("create table stu_%d(col1 int) tablegroup='my_tablegroup'", i);
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

    sql.assign_fmt("create table part_%d(col1 int) tablegroup=my_tablegroup partition by hash(col1) partitions 10", i);
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

    sql.assign_fmt("create table subpart_%d(col1 int) tablegroup=my_tablegroup partition by range(col1) subpartition by hash(col1) subpartitions 10 (partition p1 values less than (100), partition p2 values less than MAXVALUE)", i);
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }

  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i));
  }
}

TEST_F(ObBalancePartitionTest, tablegroup_sharding_partition1)
{
  TEST_INFO("-----------tablegroup_sharding_partition1-------------");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  sql.assign_fmt("create tablegroup my_tablegroup sharding='PARTITION'");
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  for (int i = 0; i < 10; i++) {
    sql.assign_fmt("create table stu_%d(col1 int) tablegroup=my_tablegroup", i);
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }

  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i));
  }
}

TEST_F(ObBalancePartitionTest, tablegroup_sharding_partition2)
{
  TEST_INFO("-----------tablegroup_sharding_partition2-------------");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  sql.assign_fmt("create tablegroup my_tablegroup sharding='PARTITION'");
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  for (int i = 0; i < 10; i++) {
    sql.assign_fmt("create table stu_%d(col1 int) tablegroup=my_tablegroup partition by hash(col1) partitions 10", i);
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }

  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i));
  }
}

TEST_F(ObBalancePartitionTest, tablegroup_sharding_partition3)
{
  TEST_INFO("-----------tablegroup_sharding_partition3-------------");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  sql.assign_fmt("create tablegroup my_tablegroup sharding='PARTITION'");
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  for (int i = 0; i < 10; i++) {
    sql.assign_fmt("create table part_%d(col1 int) tablegroup=my_tablegroup partition by range(col1) (partition p1 values less than(100), partition p2 values less than MAXVALUE)", i);
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

    sql.assign_fmt("create table subpart_%d(col1 int) tablegroup=my_tablegroup partition by range(col1) subpartition by hash(col1) subpartitions 10 (partition p1 values less than (100), partition p2 values less than MAXVALUE)", i);
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }

  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i));
  }
}

TEST_F(ObBalancePartitionTest, tablegroup_sharding_adaptive1)
{
  TEST_INFO("-----------tablegroup_sharding_adaptive1-------------");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  sql.assign_fmt("create tablegroup my_tablegroup sharding='ADAPTIVE'");
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  for (int i = 0; i < 10; i++) {
    sql.assign_fmt("create table stu_%d(col1 int) tablegroup=my_tablegroup", i);
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }

  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i));
  }
}

TEST_F(ObBalancePartitionTest, tablegroup_sharding_adaptive2)
{
  TEST_INFO("-----------tablegroup_sharding_adaptive2-------------");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  sql.assign_fmt("create tablegroup my_tablegroup sharding='ADAPTIVE'");
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  for (int i = 0; i < 10; i++) {
    sql.assign_fmt("create table stu_%d(col1 int) tablegroup=my_tablegroup partition by hash(col1) partitions 10", i);
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }

  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i));
  }
}

TEST_F(ObBalancePartitionTest, tablegroup_sharding_adaptive3)
{
  TEST_INFO("-----------tablegroup_sharding_adaptive3-------------");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  sql.assign_fmt("create tablegroup my_tablegroup sharding='ADAPTIVE'");
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  for (int i = 0; i < 10; i++) {
    sql.assign_fmt("create table subpart_%d(col1 int) tablegroup=my_tablegroup partition by range(col1) subpartition by hash(col1) subpartitions 10 (partition p1 values less than (100), partition p2 values less than MAXVALUE)", i);
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }

  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i));
  }
}

TEST_F(ObBalancePartitionTest, end)
{
  if (RunCtx.time_sec_ > 0) {
    ::sleep(RunCtx.time_sec_);
  }
}

void mock_zone_scope_bg(
    ObPartitionBalance &balance_part_job,
    const uint64_t tablegroup_id,
    const char *bg_name,
    const ObZone &zone,
    const int64_t bg_data_size,
    const int64_t bg_weight,
    const ObIArray<ObLSID> &all_ls_id_array,
    const ObLSID &src_ls_id)
{
  ObBalanceGroup bg;
  bg.id_ = ObBalanceGroupID(tablegroup_id, 0 /*id_low*/);
  bg.scope_ = ObBalanceGroup::BG_SCOPE_ZONE;
  ASSERT_EQ(OB_SUCCESS, bg.name_.assign(bg_name));

  const ObScopeZoneBGStatInfo stat(bg_data_size, bg_weight, zone);
  ASSERT_EQ(OB_SUCCESS, balance_part_job.scope_zone_bg_stat_map_.set_refactored(bg, stat));

  // build bg_map_ with one ObBalanceGroupInfo for every LS.
  // Only src_ls_id has part groups; others are empty, but must exist so that
  // get_bg_info_by_ls_id_() can always find dest_ls_id during zone tg cnt balance.
  ASSERT_TRUE(all_ls_id_array.count() > 0);
  ObArray<ObBalanceGroupInfo *> arr;
  for (int64_t i = 0; i < all_ls_id_array.count(); ++i) {
    const ObLSID &ls_id = all_ls_id_array.at(i);
    void *buf = balance_part_job.allocator_.alloc(sizeof(ObBalanceGroupInfo));
    ObBalanceGroupInfo *bg_info = new(buf) ObBalanceGroupInfo(balance_part_job.allocator_);
    ASSERT_EQ(OB_SUCCESS, bg_info->init(bg.id(), ls_id, balance_part_job.ls_desc_array_.count()));

    if (ls_id == src_ls_id) {
      // append one part group with one part (table_id is tablegroup_id for easy identification)
      void *pg_buf = balance_part_job.allocator_.alloc(sizeof(ObPartGroupInfo));
      ObPartGroupInfo *pg = new(pg_buf) ObPartGroupInfo(balance_part_job.allocator_);
      ObTransferPartInfo part(tablegroup_id, tablegroup_id);
      ASSERT_EQ(OB_SUCCESS, pg->init(tablegroup_id /*part_group_uid*/, tablegroup_id /*bg_unit_id*/));
      ASSERT_EQ(OB_SUCCESS, pg->add_part(part, 10 /*data_size*/, 0 /*balance_weight*/));
      ASSERT_EQ(OB_SUCCESS, bg_info->pg_container_->append_part_group(pg));
    }

    ASSERT_EQ(OB_SUCCESS, arr.push_back(bg_info));
  }
  ASSERT_EQ(OB_SUCCESS, balance_part_job.bg_map_.set_refactored(bg, arr));
}

// Helper: mock a BG with configurable per-LS data sizes for disk balance testing.
// Each LS in all_ls_id_array gets an ObBalanceGroupInfo entry; only LSes with
// ls_pg_data_sizes[i] > 0 will have a part group (so try_swap_part_group_in_bg_
// can still find the LS entry even when the LS has no data in this BG).
void mock_disk_balance_bg(
    ObPartitionBalance &balance_part_job,
    const uint64_t bg_id_high,
    const char *bg_name,
    const ObBalanceGroup::Scope scope,
    const ObZone &zone,
    const int64_t bg_stat_data_size,
    const int64_t bg_stat_weight,
    const ObIArray<ObLSID> &all_ls_id_array,
    const ObIArray<int64_t> &ls_pg_data_sizes)
{
  ASSERT_EQ(all_ls_id_array.count(), ls_pg_data_sizes.count());

  ObBalanceGroup bg;
  bg.id_ = ObBalanceGroupID(bg_id_high, 0);
  bg.scope_ = scope;
  ASSERT_EQ(OB_SUCCESS, bg.name_.assign(bg_name));

  if (scope == ObBalanceGroup::BG_SCOPE_ZONE) {
    const ObScopeZoneBGStatInfo stat(bg_stat_data_size, bg_stat_weight, zone);
    ASSERT_EQ(OB_SUCCESS, balance_part_job.scope_zone_bg_stat_map_.set_refactored(bg, stat));
  }

  ObArray<ObBalanceGroupInfo *> arr;
  for (int64_t i = 0; i < all_ls_id_array.count(); ++i) {
    const ObLSID &ls_id = all_ls_id_array.at(i);
    void *buf = balance_part_job.allocator_.alloc(sizeof(ObBalanceGroupInfo));
    ObBalanceGroupInfo *bg_info = new(buf) ObBalanceGroupInfo(balance_part_job.allocator_);
    ASSERT_EQ(OB_SUCCESS, bg_info->init(bg.id(), ls_id, balance_part_job.ls_desc_array_.count()));

    const int64_t data_size = ls_pg_data_sizes.at(i);
    if (data_size > 0) {
      void *pg_buf = balance_part_job.allocator_.alloc(sizeof(ObPartGroupInfo));
      ObPartGroupInfo *pg = new(pg_buf) ObPartGroupInfo(balance_part_job.allocator_);
      const uint64_t part_uid = bg_id_high * 10 + i;
      ObTransferPartInfo part(part_uid, part_uid);
      ASSERT_EQ(OB_SUCCESS, pg->init(part_uid, bg_id_high));
      ASSERT_EQ(OB_SUCCESS, pg->add_part(part, data_size, 0 /*balance_weight*/));
      ASSERT_EQ(OB_SUCCESS, bg_info->pg_container_->append_part_group(pg));
    }

    ASSERT_EQ(OB_SUCCESS, arr.push_back(bg_info));
  }
  ASSERT_EQ(OB_SUCCESS, balance_part_job.bg_map_.set_refactored(bg, arr));
}

// mock 6 LS: 3 in z1, 3 in z2
//         z1     z2
// lsg1    LS1    LS4
// lsg2    LS2    LS5
// lsg3    LS3    LS6
int mock_ls_desc_6_ls_3z1_3z2(
    ObPartitionBalance &balance_part_job,
    ObArray<ObLSID> &all_ls_id_array)
{
  int ret = OB_SUCCESS;
  balance_part_job.ls_desc_array_.reset();
  balance_part_job.ls_desc_map_.reuse();
  balance_part_job.job_generator_.ls_group_id_map_.reuse();
  all_ls_id_array.reset();
  for (int i = 1; OB_SUCC(ret) && i <= 6; i++) {
    const ObLSID ls_id(i);
    const ObZone zone(i <= 3 ? "z1" : "z2");
    ObLSDesc *ls_desc = nullptr;
    if (OB_FAIL(all_ls_id_array.push_back(ls_id))) {
      LOG_WARN("push back ls id failed", KR(ret), K(ls_id));
    } else if (OB_ISNULL(ls_desc = reinterpret_cast<ObLSDesc*>(
        balance_part_job.allocator_.alloc(sizeof(ObLSDesc))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mem fail", KR(ret));
    } else if (FALSE_IT(new(ls_desc) ObLSDesc(ls_id, i % 3, zone))) {
    } else if (OB_FAIL(balance_part_job.ls_desc_array_.push_back(ls_desc))) {
      LOG_WARN("push back failed", KR(ret), KPC(ls_desc));
    } else if (OB_FAIL(balance_part_job.ls_desc_map_.set_refactored(ls_desc->get_ls_id(), ls_desc))) {
      LOG_WARN("set_refactored failed", KR(ret), KPC(ls_desc));
    } else if (OB_FAIL(balance_part_job.job_generator_.ls_group_id_map_.set_refactored(
        ls_desc->get_ls_id(), ls_desc->get_ls_group_id()))) {
      LOG_WARN("set_refactored failed", KR(ret), KPC(ls_desc));
    }
  }
  return ret;
}

TEST_F(ObBalancePartitionTest, zone_scope_tg_cnt_balance_weight0_only)
{
  TEST_INFO("-----------zone_scope_tg_cnt_balance_weight0_only-------------");
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard guard;
  ObPartitionBalance balance_part_job;

  if (OB_FAIL(guard.switch_to(OB_SYS_TENANT_ID))) {
    LOG_WARN("switch tenant failed", KR(ret));
  } else if (OB_FAIL(balance_part_job.init(OB_SYS_TENANT_ID, GCTX.schema_service_,
      GCTX.sql_proxy_, ObPartitionBalance::GEN_TRANSFER_TASK))) {
    LOG_WARN("balance_part_job init fail", KR(ret));
  } else {
    ObArray<ObLSID> all_ls_id_array;
    if (OB_FAIL(mock_ls_desc_6_ls_3z1_3z2(balance_part_job, all_ls_id_array))) {
      LOG_WARN("mock ls desc failed", KR(ret));
    }

    // mock 4 ZONE scope balance groups, but only 3 with bg_weight==0 are counted
    balance_part_job.bg_map_.reuse();
    balance_part_job.scope_zone_bg_stat_map_.reuse();
    balance_part_job.job_generator_.normal_to_normal_part_map_.reuse();

    // 3 bg_weight==0 bgs in z1, z2 has 0 -> should move exactly one to z2
    mock_zone_scope_bg(balance_part_job, 10001, "ZONE_BG_1", ObZone("z1"), 1 /*small*/, 0 /*weight==0*/, all_ls_id_array, ObLSID(1));
    mock_zone_scope_bg(balance_part_job, 10002, "ZONE_BG_2", ObZone("z1"), 100, 0, all_ls_id_array, ObLSID(2));
    mock_zone_scope_bg(balance_part_job, 10003, "ZONE_BG_3", ObZone("z1"), 200, 0, all_ls_id_array, ObLSID(3));
    // bg_weight!=0 should be ignored by zone tg cnt balance
    mock_zone_scope_bg(balance_part_job, 20001, "ZONE_BG_WEIGHTED", ObZone("z1"), 0, 10 /*weight!=0*/, all_ls_id_array, ObLSID(1));

    if (OB_SUCC(ret)) {
      ASSERT_EQ(OB_SUCCESS, balance_part_job.process_balance_zone_tablegroup_count_());
      ASSERT_TRUE(balance_part_job.job_generator_.need_gen_job());
      ASSERT_EQ(1, balance_part_job.job_generator_.normal_to_normal_part_map_.size());
      auto iter = balance_part_job.job_generator_.normal_to_normal_part_map_.begin();
      ASSERT_TRUE(iter != balance_part_job.job_generator_.normal_to_normal_part_map_.end());
      ASSERT_EQ(iter->first.get_src_ls_id().id() + 3, iter->first.get_dest_ls_id().id());
      ASSERT_EQ(1, iter->second.count());
      const uint64_t table_id = iter->second.at(0).table_id();
      ASSERT_TRUE(10001 == table_id || 10002 == table_id || 10003 == table_id);
      ASSERT_EQ(static_cast<int64_t>(table_id) - 10000, iter->first.get_src_ls_id().id());
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObBalancePartitionTest, zone_scope_tg_weight_balance_basic)
{
  TEST_INFO("-----------zone_scope_tg_weight_balance_basic-------------");
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard guard;
  ObPartitionBalance balance_part_job;

  if (OB_FAIL(guard.switch_to(OB_SYS_TENANT_ID))) {
    LOG_WARN("switch tenant failed", KR(ret));
  } else if (OB_FAIL(balance_part_job.init(OB_SYS_TENANT_ID, GCTX.schema_service_,
      GCTX.sql_proxy_, ObPartitionBalance::GEN_TRANSFER_TASK))) {
    LOG_WARN("balance_part_job init fail", KR(ret));
  } else {
    ObArray<ObLSID> all_ls_id_array;
    if (OB_FAIL(mock_ls_desc_6_ls_3z1_3z2(balance_part_job, all_ls_id_array))) {
      LOG_WARN("mock ls desc failed", KR(ret));
    }

    // mock 5 ZONE scope balance groups with bg_weight>0, 3 in z1 and 2 in z2
    balance_part_job.bg_map_.reuse();
    balance_part_job.scope_zone_bg_stat_map_.reuse();
    balance_part_job.job_generator_.normal_to_normal_part_map_.reuse();

    // z1 total weight=30, z2 total weight=2, avg=16 -> should move exactly one bg(weight=10) from z1 to z2
    mock_zone_scope_bg(balance_part_job, 10001, "ZONE_BG_W1", ObZone("z1"), 1 /*disk*/, 10 /*weight*/, all_ls_id_array, ObLSID(1));
    mock_zone_scope_bg(balance_part_job, 10002, "ZONE_BG_W2", ObZone("z1"), 1 /*disk*/, 10 /*weight*/, all_ls_id_array, ObLSID(2));
    mock_zone_scope_bg(balance_part_job, 10003, "ZONE_BG_W3", ObZone("z1"), 1 /*disk*/, 10 /*weight*/, all_ls_id_array, ObLSID(3));
    mock_zone_scope_bg(balance_part_job, 20001, "ZONE_BG_W4", ObZone("z2"), 1 /*disk*/, 1 /*weight*/, all_ls_id_array, ObLSID(4));
    mock_zone_scope_bg(balance_part_job, 20002, "ZONE_BG_W5", ObZone("z2"), 1 /*disk*/, 1 /*weight*/, all_ls_id_array, ObLSID(5));

    if (OB_SUCC(ret)) {
      ASSERT_EQ(OB_SUCCESS, balance_part_job.process_balance_zone_tablegroup_weight_());
      ASSERT_TRUE(balance_part_job.job_generator_.need_gen_job());
      ASSERT_EQ(1, balance_part_job.job_generator_.normal_to_normal_part_map_.size());
      auto iter = balance_part_job.job_generator_.normal_to_normal_part_map_.begin();
      ASSERT_TRUE(iter != balance_part_job.job_generator_.normal_to_normal_part_map_.end());
      // z1 -> z2 mapping: LSx -> LSx+3
      ASSERT_EQ(iter->first.get_src_ls_id().id() + 3, iter->first.get_dest_ls_id().id());
      ASSERT_EQ(1, iter->second.count());
      const uint64_t table_id = iter->second.at(0).table_id();
      ASSERT_TRUE(10001 == table_id || 10002 == table_id || 10003 == table_id);
    }

    // z1 total weight=30, z2 total weight=12, avg=21 -> should swap exactly one bg(weight=10) with bg(weight=5) between z1 and z2
    mock_zone_scope_bg(balance_part_job, 20003, "ZONE_BG_W6", ObZone("z2"), 1 /*disk*/, 5 /*weight*/, all_ls_id_array, ObLSID(4));
    mock_zone_scope_bg(balance_part_job, 20004, "ZONE_BG_W7", ObZone("z2"), 1 /*disk*/, 5 /*weight*/, all_ls_id_array, ObLSID(5));
    mock_zone_scope_bg(balance_part_job, 20005, "ZONE_BG_W8", ObZone("z2"), 1 /*disk*/, 5 /*weight*/, all_ls_id_array, ObLSID(6));

    if (OB_SUCC(ret)) {
      ASSERT_EQ(OB_SUCCESS, balance_part_job.process_balance_zone_tablegroup_weight_());
      ASSERT_TRUE(balance_part_job.job_generator_.need_gen_job());
      ASSERT_EQ(2, balance_part_job.job_generator_.normal_to_normal_part_map_.size());

      bool found_z1_to_z2_weight_10 = false;
      bool found_z2_to_z1_weight_5 = false;
      for (auto iter = balance_part_job.job_generator_.normal_to_normal_part_map_.begin();
           iter != balance_part_job.job_generator_.normal_to_normal_part_map_.end();
           ++iter) {
        ASSERT_EQ(1, iter->second.count());
        const int64_t src_ls_id = iter->first.get_src_ls_id().id();
        const int64_t dest_ls_id = iter->first.get_dest_ls_id().id();
        const uint64_t table_id = iter->second.at(0).table_id();

        if (src_ls_id >= 1 && src_ls_id <= 3) {
          found_z1_to_z2_weight_10 = true;
          ASSERT_EQ(src_ls_id + 3, dest_ls_id);
          ASSERT_TRUE(10001 == table_id || 10002 == table_id || 10003 == table_id);
        } else {
          found_z2_to_z1_weight_5 = true;
          ASSERT_EQ(src_ls_id - 3, dest_ls_id);
          ASSERT_TRUE(20003 == table_id || 20004 == table_id || 20005 == table_id);
        }
      }
      ASSERT_TRUE(found_z1_to_z2_weight_10);
      ASSERT_TRUE(found_z2_to_z1_weight_5);
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObBalancePartitionTest, zone_scope_tg_disk_balance_basic)
{
  TEST_INFO("-----------zone_scope_tg_disk_balance_basic-------------");
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard guard;
  ObPartitionBalance balance_part_job;

  if (OB_FAIL(guard.switch_to(OB_SYS_TENANT_ID))) {
    LOG_WARN("switch tenant failed", KR(ret));
  } else if (OB_FAIL(balance_part_job.init(OB_SYS_TENANT_ID, GCTX.schema_service_,
      GCTX.sql_proxy_, ObPartitionBalance::GEN_TRANSFER_TASK))) {
    LOG_WARN("balance_part_job init fail", KR(ret));
  } else {
    static const int64_t GB = 1024L * 1024L * 1024L;
    ObArray<ObLSID> all_ls_id_array;
    if (OB_FAIL(mock_ls_desc_6_ls_3z1_3z2(balance_part_job, all_ls_id_array))) {
      LOG_WARN("mock ls desc failed", KR(ret));
    }

    // mock 4 ZONE scope balance groups, all bg_weight==0
    // z1 disk: 101G + 100G + 99G = 300G (max)
    // z2 disk: 50G + 60G + 70G = 180G (min)
    // diff=120G, should be swapped (default tolerance=10%, default threshold=50G, ls_cnt_in_max_zone=3 -> 150G)
    balance_part_job.bg_map_.reuse();
    balance_part_job.scope_zone_bg_stat_map_.reuse();
    balance_part_job.job_generator_.normal_to_normal_part_map_.reuse();

    mock_zone_scope_bg(balance_part_job, 30001, "ZONE_BG_D1", ObZone("z1"), 101 * GB, 50 /*weight*/, all_ls_id_array, ObLSID(1));
    mock_zone_scope_bg(balance_part_job, 30002, "ZONE_BG_D2", ObZone("z1"), 100 * GB, 50 /*weight*/, all_ls_id_array, ObLSID(2));
    mock_zone_scope_bg(balance_part_job, 30003, "ZONE_BG_D3", ObZone("z1"), 99 * GB, 1 /*weight*/, all_ls_id_array, ObLSID(3));
    mock_zone_scope_bg(balance_part_job, 40001, "ZONE_BG_D4", ObZone("z2"), 59 * GB, 100 /*weight*/, all_ls_id_array, ObLSID(4));
    mock_zone_scope_bg(balance_part_job, 40002, "ZONE_BG_D5", ObZone("z2"), 60 * GB, 1 /*weight*/, all_ls_id_array, ObLSID(5));
    mock_zone_scope_bg(balance_part_job, 40003, "ZONE_BG_D6", ObZone("z2"), 61 * GB, 1 /*weight*/, all_ls_id_array, ObLSID(6));

    if (OB_SUCC(ret)) {
      ASSERT_EQ(OB_SUCCESS, balance_part_job.process_balance_zone_tablegroup_disk_());
      ASSERT_TRUE(balance_part_job.job_generator_.need_gen_job());
      // swap should generate two directions
      // only swap when weight is the same, so 30003 and 40002 will be swapped
      ASSERT_EQ(2, balance_part_job.job_generator_.normal_to_normal_part_map_.size());
      bool found_30003 = false;
      bool found_40002 = false;
      for (auto it = balance_part_job.job_generator_.normal_to_normal_part_map_.begin();
           it != balance_part_job.job_generator_.normal_to_normal_part_map_.end();
           ++it) {
        ASSERT_EQ(1, it->second.count());
        const uint64_t table_id = it->second.at(0).table_id();
        if (30003 == table_id) {
          found_30003 = true;
        } else if (40002 == table_id) {
          found_40002 = true;
        }
      }
      ASSERT_TRUE(found_30003);
      ASSERT_TRUE(found_40002);
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}



TEST_F(ObBalancePartitionTest, zone_scope_tg_extend_balance_scatter_within_zone)
{
  TEST_INFO("-----------zone_scope_tg_extend_balance_scatter_within_zone-------------");
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard guard;
  ObPartitionBalance balance_part_job;

  if (OB_FAIL(guard.switch_to(OB_SYS_TENANT_ID))) {
    LOG_WARN("switch tenant failed", KR(ret));
  } else if (OB_FAIL(balance_part_job.init(OB_SYS_TENANT_ID, GCTX.schema_service_,
      GCTX.sql_proxy_, ObPartitionBalance::GEN_TRANSFER_TASK))) {
    LOG_WARN("balance_part_job init fail", KR(ret));
  } else {
    ObArray<ObLSID> all_ls_id_array;
    if (OB_FAIL(mock_ls_desc_6_ls_3z1_3z2(balance_part_job, all_ls_id_array))) {
      LOG_WARN("mock ls desc failed", KR(ret));
    }

    // mock 9 ZONE scope balance groups, all bg_weight==0:
    // - 3 bgs are in z1 but all located on LS1
    // - 6 bgs are in z2 but all located on LS4
    // After process_balance_partition_extend_(), they should be scattered within zone.
    balance_part_job.bg_map_.reuse();
    balance_part_job.weighted_bg_map_.reuse();
    balance_part_job.scope_zone_bg_stat_map_.reuse();
    balance_part_job.job_generator_.normal_to_normal_part_map_.reuse();

    mock_zone_scope_bg(balance_part_job, 50001, "ZONE_EXT_BG_1", ObZone("z1"), 1 /*disk*/, 0 /*weight*/, all_ls_id_array, ObLSID(1));
    mock_zone_scope_bg(balance_part_job, 50002, "ZONE_EXT_BG_2", ObZone("z1"), 1 /*disk*/, 0 /*weight*/, all_ls_id_array, ObLSID(1));
    mock_zone_scope_bg(balance_part_job, 50003, "ZONE_EXT_BG_3", ObZone("z1"), 1 /*disk*/, 0 /*weight*/, all_ls_id_array, ObLSID(1));
    mock_zone_scope_bg(balance_part_job, 60001, "ZONE_EXT_BG_4", ObZone("z2"), 1 /*disk*/, 0 /*weight*/, all_ls_id_array, ObLSID(4));
    mock_zone_scope_bg(balance_part_job, 60002, "ZONE_EXT_BG_5", ObZone("z2"), 1 /*disk*/, 0 /*weight*/, all_ls_id_array, ObLSID(4));
    mock_zone_scope_bg(balance_part_job, 60003, "ZONE_EXT_BG_6", ObZone("z2"), 1 /*disk*/, 0 /*weight*/, all_ls_id_array, ObLSID(4));
    mock_zone_scope_bg(balance_part_job, 60004, "ZONE_EXT_BG_7", ObZone("z2"), 1 /*disk*/, 0 /*weight*/, all_ls_id_array, ObLSID(4));
    mock_zone_scope_bg(balance_part_job, 60005, "ZONE_EXT_BG_8", ObZone("z2"), 1 /*disk*/, 0 /*weight*/, all_ls_id_array, ObLSID(4));
    mock_zone_scope_bg(balance_part_job, 60006, "ZONE_EXT_BG_9", ObZone("z2"), 1 /*disk*/, 0 /*weight*/, all_ls_id_array, ObLSID(4));

    if (OB_SUCC(ret)) {
      ASSERT_EQ(OB_SUCCESS, balance_part_job.process_balance_partition_extend_());
      ASSERT_TRUE(balance_part_job.job_generator_.need_gen_job());
      // z1: 3 bgs on LS1 -> need 2 transfers to LS2/LS3
      // z2: 6 bgs on LS4 -> need 4 transfers to LS5/LS6
      ASSERT_EQ(4, balance_part_job.job_generator_.normal_to_normal_part_map_.size());

      int64_t z1_transfer_cnt = 0;
      int64_t z2_transfer_cnt = 0;
      int64_t total_transfer_cnt = 0;
      int64_t dest_2_cnt = 0;
      int64_t dest_3_cnt = 0;
      int64_t dest_5_cnt = 0;
      int64_t dest_6_cnt = 0;
      for (auto it = balance_part_job.job_generator_.normal_to_normal_part_map_.begin();
           it != balance_part_job.job_generator_.normal_to_normal_part_map_.end();
           ++it) {
        const int64_t src_ls_id = it->first.get_src_ls_id().id();
        const int64_t dest_ls_id = it->first.get_dest_ls_id().id();
        const int64_t part_cnt = it->second.count();
        ASSERT_GT(part_cnt, 0);
        total_transfer_cnt += part_cnt;

        // Assert: only balance within each zone (no cross-zone transfers)
        if (src_ls_id >= 1 && src_ls_id <= 3) {
          ASSERT_TRUE(dest_ls_id >= 1 && dest_ls_id <= 3);
          z1_transfer_cnt += part_cnt;
          if (2 == dest_ls_id) {
            dest_2_cnt += part_cnt;
          } else if (3 == dest_ls_id) {
            dest_3_cnt += part_cnt;
          } else {
            FAIL() << "unexpected z1 dest_ls_id=" << dest_ls_id;
          }
        } else if (src_ls_id >= 4 && src_ls_id <= 6) {
          ASSERT_TRUE(dest_ls_id >= 4 && dest_ls_id <= 6);
          z2_transfer_cnt += part_cnt;
          if (5 == dest_ls_id) {
            dest_5_cnt += part_cnt;
          } else if (6 == dest_ls_id) {
            dest_6_cnt += part_cnt;
          } else {
            FAIL() << "unexpected z2 dest_ls_id=" << dest_ls_id;
          }
        } else {
          FAIL() << "unexpected src_ls_id=" << src_ls_id;
        }

        // Validate: moved parts belong to correct zone bgs
        for (int64_t i = 0; i < it->second.count(); ++i) {
          const uint64_t table_id = it->second.at(i).table_id();
          if (src_ls_id >= 1 && src_ls_id <= 3) {
            ASSERT_TRUE(50001 == table_id || 50002 == table_id || 50003 == table_id);
          } else {
            ASSERT_TRUE(60001 == table_id || 60002 == table_id || 60003 == table_id
                || 60004 == table_id || 60005 == table_id || 60006 == table_id);
          }
        }
      }
      ASSERT_EQ(2, z1_transfer_cnt);
      ASSERT_EQ(4, z2_transfer_cnt);
      ASSERT_EQ(6, total_transfer_cnt);
      ASSERT_EQ(1, dest_2_cnt);
      ASSERT_EQ(1, dest_3_cnt);
      ASSERT_EQ(2, dest_5_cnt);
      ASSERT_EQ(2, dest_6_cnt);

      // Validate LS stats:
      // - z1 has 3 bgs and 3 ls -> 1 pg per ls
      // - z2 has 6 bgs and 3 ls -> 2 pg per ls
      // (process_balance_partition_extend_() will call prepare_ls_desc_(BG_SCOPE_ZONE) and then update stats by transfers.)
      for (int64_t i = 0; i < balance_part_job.ls_desc_array_.count(); ++i) {
        const ObLSDesc *ls_desc = balance_part_job.ls_desc_array_.at(i);
        ASSERT_NE(nullptr, ls_desc);
        const int64_t ls_id = ls_desc->get_ls_id().id();
        if (ls_id >= 1 && ls_id <= 3) {
          ASSERT_EQ(1, ls_desc->get_unweighted_partgroup_cnt());
        } else if (ls_id >= 4 && ls_id <= 6) {
          ASSERT_EQ(2, ls_desc->get_unweighted_partgroup_cnt());
        } else {
          FAIL() << "unexpected ls_id=" << ls_id;
        }
      }
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

// Test: process_balance_partition_disk_ correctly handles both CLUSTER and ZONE scope BGs
//       using the "ZONE first, then CLUSTER with total data" strategy.
//
// Setup (6 LSes: LS1-LS3 in z1, LS4-LS6 in z2):
//
//   CLUSTER scope BGs (C_BG1, C_BG2):
//     Each BG has LS1=40G, LS2-LS6=5G each.
//
//   ZONE scope BGs (z1: Z1_BG1, Z1_BG2;  z2: Z2_BG1, Z2_BG2):
//     z1 BGs: LS1=40G, LS2=5G, LS3=5G; LS4-LS6=0 (no pg).
//     z2 BGs: LS4=40G, LS5=5G, LS6=5G; LS1-LS3=0 (no pg).
//
// Expected behavior:
//   Step 1 (ZONE within zones): 1 swap in z1 (LS1<->LS2), 1 swap in z2 (LS4<->LS5).
//     After: z1 zone data balanced (LS1=45G, LS2=45G), z2 likewise.
//   Step 2 (CLUSTER with total data): total shows LS1=125G(max), LS3=20G(min).
//     CLUSTER BGs are swapped from LS1 to LS3 and LS6, achieving global balance.
//     After: ALL 6 LSes = 55G each.
//   ZONE BGs are NEVER touched in Step 2 — no oscillation risk.
//
TEST_F(ObBalancePartitionTest, disk_balance_cluster_and_zone_scope)
{
  TEST_INFO("-----------disk_balance_cluster_and_zone_scope-------------");
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard guard;
  ObPartitionBalance balance_part_job;

  if (OB_FAIL(guard.switch_to(OB_SYS_TENANT_ID))) {
    LOG_WARN("switch tenant failed", KR(ret));
  } else if (OB_FAIL(balance_part_job.init(OB_SYS_TENANT_ID, GCTX.schema_service_,
      GCTX.sql_proxy_, ObPartitionBalance::GEN_TRANSFER_TASK))) {
    LOG_WARN("balance_part_job init fail", KR(ret));
  } else {
    static const int64_t GB = 1024L * 1024L * 1024L;
    ObArray<ObLSID> all_ls_id_array;
    if (OB_FAIL(mock_ls_desc_6_ls_3z1_3z2(balance_part_job, all_ls_id_array))) {
      LOG_WARN("mock ls desc failed", KR(ret));
    }

    balance_part_job.bg_map_.reuse();
    balance_part_job.weighted_bg_map_.reuse();
    balance_part_job.scope_zone_bg_stat_map_.reuse();
    balance_part_job.job_generator_.normal_to_normal_part_map_.reuse();

    // -------- CLUSTER scope BGs --------
    // LS1 is heavy (40G per BG), LS2-LS6 are light (5G per BG).
    // After 2 BGs: LS1=80G, LS2-LS6=10G.
    ObArray<int64_t> c_bg_sizes;
    for (int64_t i = 0; i < 6; ++i) {
      ASSERT_EQ(OB_SUCCESS, c_bg_sizes.push_back(i == 0 ? 40 * GB : 5 * GB));
    }
    mock_disk_balance_bg(balance_part_job, 70001, "C_DISK_BG1",
        ObBalanceGroup::BG_SCOPE_CLUSTER, ObZone(""), 0, 0, all_ls_id_array, c_bg_sizes);
    mock_disk_balance_bg(balance_part_job, 70002, "C_DISK_BG2",
        ObBalanceGroup::BG_SCOPE_CLUSTER, ObZone(""), 0, 0, all_ls_id_array, c_bg_sizes);

    // -------- ZONE scope BGs --------
    // z1 BGs: LS1=40G, LS2=5G, LS3=5G; LS4-LS6 have no pg.
    ObArray<int64_t> z1_bg_sizes;
    for (int64_t i = 0; i < 6; ++i) {
      ASSERT_EQ(OB_SUCCESS, z1_bg_sizes.push_back(i == 0 ? 40 * GB : (i <= 2 ? 5 * GB : 0)));
    }
    mock_disk_balance_bg(balance_part_job, 80001, "Z_DISK_Z1_BG1",
        ObBalanceGroup::BG_SCOPE_ZONE, ObZone("z1"), 80 * GB, 0, all_ls_id_array, z1_bg_sizes);
    mock_disk_balance_bg(balance_part_job, 80002, "Z_DISK_Z1_BG2",
        ObBalanceGroup::BG_SCOPE_ZONE, ObZone("z1"), 80 * GB, 0, all_ls_id_array, z1_bg_sizes);

    // z2 BGs: LS4=40G, LS5=5G, LS6=5G; LS1-LS3 have no pg.
    ObArray<int64_t> z2_bg_sizes;
    for (int64_t i = 0; i < 6; ++i) {
      ASSERT_EQ(OB_SUCCESS, z2_bg_sizes.push_back(i == 3 ? 40 * GB : (i >= 3 ? 5 * GB : 0)));
    }
    mock_disk_balance_bg(balance_part_job, 90001, "Z_DISK_Z2_BG1",
        ObBalanceGroup::BG_SCOPE_ZONE, ObZone("z2"), 80 * GB, 0, all_ls_id_array, z2_bg_sizes);
    mock_disk_balance_bg(balance_part_job, 90002, "Z_DISK_Z2_BG2",
        ObBalanceGroup::BG_SCOPE_ZONE, ObZone("z2"), 80 * GB, 0, all_ls_id_array, z2_bg_sizes);

    if (OB_SUCC(ret)) {
      ASSERT_EQ(OB_SUCCESS, balance_part_job.process_balance_partition_disk_());
      ASSERT_TRUE(balance_part_job.job_generator_.need_gen_job());

      // Verify transfers by scope.
      // Each disk swap generates 2 transfer parts (largest<->smallest).
      // - CLUSTER swap: parts identified by table_id/10 in {70001, 70002}
      // - ZONE z1 swap: parts identified by table_id/10 in {80001, 80002}
      // - ZONE z2 swap: parts identified by table_id/10 in {90001, 90002}
      int64_t cluster_transfer_cnt = 0;
      int64_t zone_z1_transfer_cnt = 0;
      int64_t zone_z2_transfer_cnt = 0;

      for (auto it = balance_part_job.job_generator_.normal_to_normal_part_map_.begin();
           it != balance_part_job.job_generator_.normal_to_normal_part_map_.end();
           ++it) {
        const int64_t src_ls_id = it->first.get_src_ls_id().id();
        const int64_t dest_ls_id = it->first.get_dest_ls_id().id();

        for (int64_t i = 0; i < it->second.count(); ++i) {
          const uint64_t table_id = it->second.at(i).table_id();
          const uint64_t bg_id = table_id / 10;

          if (bg_id == 70001 || bg_id == 70002) {
            cluster_transfer_cnt++;
          } else if (bg_id == 80001 || bg_id == 80002) {
            zone_z1_transfer_cnt++;
            // ZONE z1 transfers must stay within z1 LSes (LS1-LS3)
            ASSERT_TRUE(src_ls_id >= 1 && src_ls_id <= 3)
                << "z1 src_ls_id=" << src_ls_id << " out of z1 range";
            ASSERT_TRUE(dest_ls_id >= 1 && dest_ls_id <= 3)
                << "z1 dest_ls_id=" << dest_ls_id << " out of z1 range";
          } else if (bg_id == 90001 || bg_id == 90002) {
            zone_z2_transfer_cnt++;
            // ZONE z2 transfers must stay within z2 LSes (LS4-LS6)
            ASSERT_TRUE(src_ls_id >= 4 && src_ls_id <= 6)
                << "z2 src_ls_id=" << src_ls_id << " out of z2 range";
            ASSERT_TRUE(dest_ls_id >= 4 && dest_ls_id <= 6)
                << "z2 dest_ls_id=" << dest_ls_id << " out of z2 range";
          } else {
            FAIL() << "unexpected bg_id=" << bg_id << " (table_id=" << table_id << ")";
          }
        }
      }

      TEST_INFO("disk_balance_cluster_and_zone_scope result",
          K(cluster_transfer_cnt), K(zone_z1_transfer_cnt), K(zone_z2_transfer_cnt));

      // ZONE: each zone triggers 1 swap = 2 parts each
      ASSERT_EQ(2, zone_z1_transfer_cnt);
      ASSERT_EQ(2, zone_z2_transfer_cnt);
      // CLUSTER: Step 2 uses total data (ALL BGs) to find max/min, so CLUSTER BGs
      // are swapped more aggressively to compensate ZONE-induced imbalance.
      // In this case: 2 CLUSTER swaps (C_BG1 LS1->LS3, C_BG2 LS1->LS6) = 4 parts.
      ASSERT_EQ(4, cluster_transfer_cnt);

      // After balance, all LSes should have equal total data_size (55G each).
      // Step 3 calls prepare_ls_desc_() to recompute from all BGs post-swap.
      for (int64_t i = 0; i < balance_part_job.ls_desc_array_.count(); ++i) {
        const ObLSDesc *ls_desc = balance_part_job.ls_desc_array_.at(i);
        ASSERT_NE(nullptr, ls_desc);
        ASSERT_EQ(55 * GB, ls_desc->get_data_size())
            << "ls_id=" << ls_desc->get_ls_id().id()
            << " data_size=" << ls_desc->get_data_size();
      }
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

void mock_pg(
    const int64_t pg_cnt,
    const int64_t bg_unit_id,
    const int64_t balance_weight,
    ObPartGroupContainer &pg_container)
{
  for (int64_t i = 0; i < pg_cnt; ++i) {
    int64_t part_group_uid = bg_unit_id * pg_cnt + i;
    ObTransferPartInfo part(part_group_uid, part_group_uid);
    int64_t data_size = i + bg_unit_id + pg_cnt;
    void *buf = pg_container.alloc_.alloc(sizeof(ObPartGroupInfo));
    ObPartGroupInfo *part_group = new(buf) ObPartGroupInfo(pg_container.alloc_);
    ASSERT_EQ(OB_SUCCESS, part_group->init(part_group_uid, bg_unit_id));
    ASSERT_EQ(OB_SUCCESS, part_group->add_part(part, data_size, balance_weight));
    ASSERT_EQ(OB_SUCCESS, pg_container.append_part_group(part_group));
  }
  TEST_INFO("mock_pg finished", K(pg_container));
}

TEST_F(ObBalancePartitionTest, test_part_group_container)
{
  TEST_INFO("-----------test part group container-------------");
  ObArenaAllocator allocer;
  const int64_t LS_NUM = 3;
  const int64_t WEIGHTED_PG_NUM = 10;
  ObPartGroupContainer src_pg_container(allocer);
  ObPartGroupContainer dest_pg_container(allocer);
  ObPartGroupContainer weighted_pg_container(allocer);
  ObBalanceGroupID bg_id(111,0);
  ASSERT_EQ(OB_SUCCESS, src_pg_container.init(bg_id, LS_NUM));
  ASSERT_EQ(OB_SUCCESS, dest_pg_container.init(bg_id, LS_NUM));
  ASSERT_EQ(OB_SUCCESS, weighted_pg_container.init(bg_id, LS_NUM));
  mock_pg(100, 1, 0, src_pg_container);
  mock_pg(100, 2, 0, src_pg_container);
  mock_pg(WEIGHTED_PG_NUM, 1, 1, src_pg_container);
  TEST_INFO("src_pg_container", K(src_pg_container));

  ObArray<int64_t> weight_arr;
  ASSERT_EQ(OB_SUCCESS, src_pg_container.get_balance_weight_array(weight_arr));
  ASSERT_EQ(WEIGHTED_PG_NUM, weight_arr.count());
  TEST_INFO("get_balance_weight_array", K(weight_arr), K(src_pg_container));

  ObPartGroupInfo *largest_pg = nullptr;
  ObPartGroupInfo *smallest_pg = nullptr;
  ASSERT_EQ(OB_SUCCESS, src_pg_container.get_largest_part_group(largest_pg));
  ASSERT_EQ(OB_SUCCESS, src_pg_container.get_smallest_part_group(smallest_pg));
  ASSERT_TRUE(largest_pg != nullptr);
  ASSERT_TRUE(smallest_pg != nullptr);
  TEST_INFO("get largest and smallest part_group", KPC(largest_pg), KPC(smallest_pg), K(src_pg_container));

  ASSERT_EQ(OB_SUCCESS, src_pg_container.remove_part_group(largest_pg));
  ASSERT_EQ(OB_SUCCESS, src_pg_container.remove_part_group(smallest_pg));
  ObPartGroupInfo *tmp_pg = nullptr;
  ASSERT_EQ(OB_SUCCESS, src_pg_container.get_largest_part_group(tmp_pg));
  ASSERT_TRUE(largest_pg != tmp_pg && tmp_pg->get_data_size() < largest_pg->get_data_size());
  tmp_pg = nullptr;
  ASSERT_EQ(OB_SUCCESS, src_pg_container.get_smallest_part_group(tmp_pg));
  ASSERT_TRUE(smallest_pg != tmp_pg && tmp_pg->get_data_size() > smallest_pg->get_data_size());

  int64_t SELECT_NUM = 5;
  for(int64_t i = 0; i < SELECT_NUM; ++i) {
    ObPartGroupInfo *pg = nullptr;
    ASSERT_EQ(OB_SUCCESS, src_pg_container.select(1, dest_pg_container, pg));
    ASSERT_EQ(OB_SUCCESS, src_pg_container.remove_part_group(pg));
    ASSERT_EQ(OB_SUCCESS, dest_pg_container.append_part_group(pg));
    TEST_INFO("select with weight", K(i), KPC(pg), K(src_pg_container), K(dest_pg_container));
  }
  SELECT_NUM = 20;
  for(int64_t i = 0; i < SELECT_NUM; ++i) {
    ObPartGroupInfo *pg = nullptr;
    ASSERT_EQ(OB_SUCCESS, src_pg_container.select(0, dest_pg_container, pg));
    ASSERT_EQ(OB_SUCCESS, src_pg_container.remove_part_group(pg));
    ASSERT_EQ(OB_SUCCESS, dest_pg_container.append_part_group(pg));
    TEST_INFO("select without weight", K(i), KPC(pg), K(src_pg_container), K(dest_pg_container));
  }

  ASSERT_EQ(OB_SUCCESS, src_pg_container.split_out_weighted_part_groups(weighted_pg_container));
  ASSERT_TRUE(src_pg_container.get_balance_weight() == 0);
  TEST_INFO("split_out_weighted_part_groups", K(weighted_pg_container), K(src_pg_container));

}

} // end unittest
} // end oceanbase


int main(int argc, char **argv)
{
  int c = 0;
  int time_sec = 0;
  char *log_level = (char*)"INFO";
  while(EOF != (c = getopt(argc,argv,"t:l:"))) {
    switch(c) {
    case 't':
      time_sec = atoi(optarg);
      break;
    case 'l':
     log_level = optarg;
     oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
     break;
    default:
      break;
    }
  }
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(log_level);

  TEST_INFO("main>>>");
  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
