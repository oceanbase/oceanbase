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
#include "storage/tx_storage/ob_ls_service.h"
#include "share/ls/ob_ls_operator.h"

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
    auto ls_desc = new ObLSDesc(ObLSID(i), 0);
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
  } else if (OB_FAIL(process_balance_partition_inner_())) {
    LOG_WARN("process_balance_partition_inner fail", KR(ret));
  } else if (OB_FAIL(process_balance_partition_extend_())) {
    LOG_WARN("process_balance_partition_extend fail", KR(ret));
  } else if (OB_FAIL(process_balance_partition_disk_())) {
    LOG_WARN("process_balance_partition_disk fail", KR(ret));
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
      ObArray<ObTransferPartGroup *> part_groups_arr;
      ObSqlString part_groups_str;
      if (ObPartDistributionMode::ROUND_ROBIN == part_balance.part_distribution_mode_) {
        const ObRRPartGroupContainer *pg_ctn = dynamic_cast<ObRRPartGroupContainer *>(part_groups->pg_container_);
        for (auto unit_it = pg_ctn->bg_units_.begin(); unit_it != pg_ctn->bg_units_.end(); unit_it++) {
          const ObBalanceGroupUnit *unit = unit_it->second;
          for (int i = 0; i < unit->part_group_buckets_.count(); i++) {
            append(part_groups_arr, unit->part_group_buckets_.at(i));
          }
        }
      } else if (ObPartDistributionMode::CONTINUOUS == part_balance.part_distribution_mode_) {
        const ObContinuousPartGroupContainer *pg_ctn =
          dynamic_cast<ObContinuousPartGroupContainer *>(part_groups->pg_container_);
        append(part_groups_arr, pg_ctn->part_groups_);
      }
      std::sort(part_groups_arr.begin(), part_groups_arr.end(), [] (const ObTransferPartGroup *left, const ObTransferPartGroup *right) {
        const ObTransferPartInfo &part_left = left->part_list_.at(0);
        const ObTransferPartInfo &part_right = right->part_list_.at(0);
        if (part_left.table_id_ == part_right.table_id_) {
          return part_left.part_object_id_ < part_right.part_object_id_;
        }
        return part_left.table_id_ < part_right.table_id_;
      });
      for (int i = 0; i < part_groups_arr.count(); i++) {
        ObTransferPartGroup *part_group = part_groups_arr.at(i);
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
              "part_group_count", part_groups->get_part_group_count(),
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

  int run(int ls_cnt, ObPartDistributionMode part_distribution_mode) {
    int ret = OB_SUCCESS;
    g_ls_cnt = ls_cnt;
    ObPartitionBalance balance_part_job;
    ObCurTraceId::get_trace_id()->set(std::string(std::to_string(ObTimeUtility::current_time()%10000)).c_str());
    ObTenantSwitchGuard guard;
    ObLSStatusOperator status_op;
    TEST_INFO("balance_part_job start>>>>>>>>>>>>>>>>>");

    if (OB_FAIL(guard.switch_to(OB_SYS_TENANT_ID))) {
      LOG_WARN("switch tenant", KR(ret));
    } else if (OB_FAIL(balance_part_job.init(OB_SYS_TENANT_ID, GCTX.schema_service_,
        GCTX.sql_proxy_, 1, 1, ObPartitionBalance::GEN_TRANSFER_TASK, part_distribution_mode))) {
      LOG_WARN("balance_part_job init fail", KR(ret));
    } else if (OB_FAIL(balance_part_job.process())) {
      LOG_WARN("balance_part_job process fail", KR(ret));
    } else {
      std::sort(balance_part_job.ls_desc_array_.begin(), balance_part_job.ls_desc_array_.end(), [] (const ObLSDesc* left, const ObLSDesc* right) {
        return left->partgroup_cnt_ < right->partgroup_cnt_;
      });
      TEST_INFO("balance_part_job bg_map size", K(balance_part_job.bg_map_.size()));
      for (auto iter = balance_part_job.bg_map_.begin(); iter != balance_part_job.bg_map_.end(); iter++) {
        for (int i = 0; i < iter->second.count(); i++) {
          TEST_INFO("balance_part_job bg_map", K(iter->first), K(iter->second.at(i)->ls_id_), K(iter->second.at(i)->get_part_group_count()));
        }
      }
      print_part_map(balance_part_job.job_generator_.dup_to_normal_part_map_, "dup_to_normal");
      print_part_map(balance_part_job.job_generator_.normal_to_dup_part_map_, "normal_to_dup");
      print_part_map(balance_part_job.job_generator_.dup_to_dup_part_map_, "dup_to_dup");
      print_part_map(balance_part_job.job_generator_.normal_to_normal_part_map_, "normal_to_normal");
      print_part_group(balance_part_job);
      if (balance_part_job.ls_desc_array_.at(balance_part_job.ls_desc_array_.count() - 1)->partgroup_cnt_ - balance_part_job.ls_desc_array_.at(0)->partgroup_cnt_ > 1) {
        ret = -1;
        LOG_WARN("partition not balance", KR(ret), K(balance_part_job.ls_desc_array_.at(balance_part_job.ls_desc_array_.count()-1)), K(balance_part_job.ls_desc_array_.at(0)));
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
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::CONTINUOUS));
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::ROUND_ROBIN));
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
  LOG_INFO("-----------continuous-------------");
  for (int i = 1; i <= 20; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::CONTINUOUS));
  }
  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 20; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::ROUND_ROBIN));
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
  LOG_INFO("-----------continuous-------------");
  for (int i = 1; i <= 10; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::CONTINUOUS));
  }
  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 10; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::ROUND_ROBIN));
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
  LOG_INFO("-----------continuous-------------");
  for (int i = 1; i <= 10; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::CONTINUOUS));
  }
  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 10; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::ROUND_ROBIN));
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

  LOG_INFO("-----------continuous-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::CONTINUOUS));
  }
  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::ROUND_ROBIN));
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

  LOG_INFO("-----------continuous-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::CONTINUOUS));
  }
  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::ROUND_ROBIN));
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

  LOG_INFO("-----------continuous-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::CONTINUOUS));
  }
  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::ROUND_ROBIN));
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

  LOG_INFO("-----------continuous-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::CONTINUOUS));
  }
  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::ROUND_ROBIN));
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

  LOG_INFO("-----------continuous-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::CONTINUOUS));
  }
  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::ROUND_ROBIN));
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

  LOG_INFO("-----------continuous-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::CONTINUOUS));
  }
  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::ROUND_ROBIN));
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

  LOG_INFO("-----------continuous-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::CONTINUOUS));
  }
  LOG_INFO("-----------round robin-------------");
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(OB_SUCCESS, run(i, ObPartDistributionMode::ROUND_ROBIN));
  }
}

TEST_F(ObBalancePartitionTest, end)
{
  if (RunCtx.time_sec_ > 0) {
    ::sleep(RunCtx.time_sec_);
  }
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
