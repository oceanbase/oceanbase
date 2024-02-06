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
#include "lib/mysqlclient/ob_mysql_result.h"
#include "rootserver/ddl_task/ob_ddl_task.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::rootserver;


class ObDDLTaskTest : public ObSimpleClusterTestBase
{
public:
  ObDDLTaskTest() : ObSimpleClusterTestBase("test_ddl_task_") {}
};

TEST_F(ObDDLTaskTest, create_tenant)
{
  uint64_t tenant_id;
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  ASSERT_NE(0, tenant_id);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

class MockDDLTask : public ObDDLTask
{
public:
  MockDDLTask() : ObDDLTask(DDL_INVALID) {  }
  virtual int process() { return OB_SUCCESS; }
  virtual int cleanup_impl() { return OB_SUCCESS; }
  virtual void flt_set_task_span_tag() const {  }
  virtual void flt_set_status_span_tag() const {  }
};

TEST_F(ObDDLTaskTest, switch_status)
{
  common::ObMySQLProxy &tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ObArenaAllocator arena;
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  MockDDLTask task;
  ObDDLTaskRecord task_record;
  task.ddl_stmt_str_  = "create index xxx";
  task.gmt_create_ = ObTimeUtility::current_time();
  task.tenant_id_ = tenant_id;
  task.object_id_ = 1;
  task.target_object_id_ = 1;
  task.schema_version_ = 1;
  task.task_type_ = DDL_CREATE_INDEX;
  task.task_status_ = ObDDLTaskStatus::PREPARE;
  task.task_id_ = 1;
  task.parent_task_id_ = 0;
  task.task_version_ = 1;
  task.execution_id_ = 1;
  task.ret_code_ = OB_SUCCESS;
  task.trace_id_.id_.seq_ = 1;
  ASSERT_EQ(OB_SUCCESS, task.convert_to_record(task_record, arena));
  ASSERT_EQ(OB_SUCCESS, ObDDLTaskRecordOperator::insert_record(*GCTX.sql_proxy_, task_record));
  ASSERT_EQ(OB_SUCCESS, task.switch_status(ObDDLTaskStatus::DROP_SCHEMA, false/*enable_flt*/, OB_SUCCESS));
}

TEST_F(ObDDLTaskTest, end)
{
  // used for debug
//  ::sleep(3600);
}

}
}

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
