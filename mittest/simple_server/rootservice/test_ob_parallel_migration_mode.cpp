// owner: jinqian.zzy 
// owner group: rs

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

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "rootserver/ob_disaster_recovery_task_mgr.h"
#include "rootserver/ob_disaster_recovery_task.h"

namespace oceanbase
{
using namespace unittest;
namespace share
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace schema;
using namespace common;

class TestParallelMigrationMode : public unittest::ObSimpleClusterTestBase
{
public:
  TestParallelMigrationMode() : unittest::ObSimpleClusterTestBase("test_ob_parallel_migration_mode") {}
};

TEST_F(TestParallelMigrationMode, test_parse_from_string)
{
  int ret = OB_SUCCESS;
  
  rootserver::ObParallelMigrationMode mode;
  ObString auto_mode("auto");
  ObString on_mode("on");
  ObString off_mode("off");
  ObString not_valid_mode("onoff");
  ret = mode.parse_from_string(auto_mode);
  ASSERT_EQ(true, mode.is_auto_mode());
  ASSERT_EQ(OB_SUCCESS, ret);

  mode.reset();
  ret = mode.parse_from_string(on_mode);
  ASSERT_EQ(true, mode.is_on_mode());
  ASSERT_EQ(OB_SUCCESS, ret);

  mode.reset();
  ret = mode.parse_from_string(off_mode);
  ASSERT_EQ(true, mode.is_off_mode());
  ASSERT_EQ(OB_SUCCESS, ret);
  
  mode.reset();
  ret = mode.parse_from_string(not_valid_mode);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

TEST_F(TestParallelMigrationMode, test_get_mode_str)
{
  int ret = OB_SUCCESS;
  rootserver::ObParallelMigrationMode auto_mode(rootserver::ObParallelMigrationMode::ParallelMigrationMode::AUTO);
  rootserver::ObParallelMigrationMode on_mode(rootserver::ObParallelMigrationMode::ParallelMigrationMode::ON);
  rootserver::ObParallelMigrationMode off_mode(rootserver::ObParallelMigrationMode::ParallelMigrationMode::OFF);
  rootserver::ObParallelMigrationMode max_mode(rootserver::ObParallelMigrationMode::ParallelMigrationMode::MAX);
  ObString auto_str(auto_mode.get_mode_str());
  ASSERT_EQ("AUTO", std::string(auto_str.ptr(), auto_str.length()));

  ObString on_str(on_mode.get_mode_str());
  ASSERT_EQ("ON", std::string(on_str.ptr(), on_str.length()));

  ObString off_str(off_mode.get_mode_str());
  ASSERT_EQ("OFF", std::string(off_str.ptr(), off_str.length()));

  ObString max_str(max_mode.get_mode_str());
  ASSERT_EQ("", std::string(max_str.ptr(), max_str.length()));
}

TEST_F(TestParallelMigrationMode, test_to_string)
{
  int ret = OB_SUCCESS;
  rootserver::ObParallelMigrationMode auto_mode(rootserver::ObParallelMigrationMode::ParallelMigrationMode::AUTO);
  rootserver::ObParallelMigrationMode on_mode(rootserver::ObParallelMigrationMode::ParallelMigrationMode::ON);
  rootserver::ObParallelMigrationMode off_mode(rootserver::ObParallelMigrationMode::ParallelMigrationMode::OFF);
  rootserver::ObParallelMigrationMode max_mode(rootserver::ObParallelMigrationMode::ParallelMigrationMode::MAX);

  char auto_buf[100];
  int auto_len = sizeof(auto_buf);
  int pos1 = auto_mode.to_string(auto_buf, auto_len);
  ASSERT_EQ("{mode:0, mode:\"AUTO\"}", std::string(auto_buf, pos1));

  char on_buf[100];
  int on_len = sizeof(on_buf);
  int pos2 = on_mode.to_string(on_buf, on_len);
  ASSERT_EQ("{mode:1, mode:\"ON\"}", std::string(on_buf, pos2));

  char off_buf[100];
  int off_len = sizeof(off_buf);
  int pos3 = off_mode.to_string(off_buf, off_len);
  ASSERT_EQ("{mode:2, mode:\"OFF\"}", std::string(off_buf, pos3));

  char max_buf[100];
  int max_len = sizeof(max_buf);
  int pos4 = max_mode.to_string(max_buf, max_len);
  ASSERT_EQ("{mode:3, mode:null}", std::string(max_buf, pos4));
}

TEST_F(TestParallelMigrationMode, test_task_type)
{
  int ret = OB_SUCCESS;
  ObString migrate("MIGRATE REPLICA");
  ObString add("ADD REPLICA");
  ObString build("BUILD ONLY IN MEMBER LIST");
  ObString transform("TYPE TRANSFORM");
  ObString remove_paxos("REMOVE PAXOS REPLICA");
  ObString remove_non_paxos("REMOVE NON PAXOS REPLICA");
  ObString modify("MODIFY PAXOS REPLICA NUMBER");
  ObString max("MAX_TYPE");
  ObString no_valid("NOT_VALID_TYPE");
  rootserver::ObDRTaskType task_type = rootserver::ObDRTaskType::MAX_TYPE;
  ret = parse_disaster_recovery_task_type_from_string(migrate, task_type);
  ASSERT_EQ(rootserver::ObDRTaskType::LS_MIGRATE_REPLICA, task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  task_type = rootserver::ObDRTaskType::MAX_TYPE;

  ret = parse_disaster_recovery_task_type_from_string(add, task_type);
  ASSERT_EQ(rootserver::ObDRTaskType::LS_ADD_REPLICA, task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  task_type = rootserver::ObDRTaskType::MAX_TYPE;

  ret = parse_disaster_recovery_task_type_from_string(build, task_type);
  ASSERT_EQ(rootserver::ObDRTaskType::LS_BUILD_ONLY_IN_MEMBER_LIST, task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  task_type = rootserver::ObDRTaskType::MAX_TYPE;

  ret = parse_disaster_recovery_task_type_from_string(transform, task_type);
  ASSERT_EQ(rootserver::ObDRTaskType::LS_TYPE_TRANSFORM, task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  task_type = rootserver::ObDRTaskType::MAX_TYPE;

  ret = parse_disaster_recovery_task_type_from_string(remove_paxos, task_type);
  ASSERT_EQ(rootserver::ObDRTaskType::LS_REMOVE_PAXOS_REPLICA, task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  task_type = rootserver::ObDRTaskType::MAX_TYPE;

  ret = parse_disaster_recovery_task_type_from_string(remove_non_paxos, task_type);
  ASSERT_EQ(rootserver::ObDRTaskType::LS_REMOVE_NON_PAXOS_REPLICA, task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  task_type = rootserver::ObDRTaskType::MAX_TYPE;

  ret = parse_disaster_recovery_task_type_from_string(modify, task_type);
  ASSERT_EQ(rootserver::ObDRTaskType::LS_MODIFY_PAXOS_REPLICA_NUMBER, task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  task_type = rootserver::ObDRTaskType::MAX_TYPE;

  ret = parse_disaster_recovery_task_type_from_string(max, task_type);
  ASSERT_EQ(rootserver::ObDRTaskType::MAX_TYPE, task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  task_type = rootserver::ObDRTaskType::MAX_TYPE;

  ret = parse_disaster_recovery_task_type_from_string(no_valid, task_type);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
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