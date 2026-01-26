// owner: ouyanghongrong.oyh
// owner group: storage

/**
 * Copyright (c) 2025 OceanBase
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
 #define USING_LOG_PREFIX STORAGE
 #define protected public
 #define private public

 #include "env/ob_simple_cluster_test_base.h"
 #include "share/compaction/ob_compaction_resource_manager.h"

 namespace oceanbase
 {
 namespace compaction
 {
 namespace unittest
 {

 static uint64_t tenant_id = OB_INVALID_TENANT_ID;

 class ObCompactionResourceManagerTest : public ::oceanbase::unittest::ObSimpleClusterTestBase
 {
 public:
   ObCompactionResourceManagerTest()
     : ObSimpleClusterTestBase("test_compaction_resource_manager")
   {}
   void execute_stmt(int &ret, const char* label, const char* stmt);
 };

 void ObCompactionResourceManagerTest::execute_stmt(int &ret, const char* label, const char* stmt)
 {
    ret = OB_SUCCESS;
    int64_t affected_rows = 0;
    LOG_INFO("start execute", K(label), K(stmt));
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(stmt, affected_rows));
    LOG_INFO("finish execute", K(label), K(stmt));
 }

 TEST_F(ObCompactionResourceManagerTest, add_tenant)
 {
   ASSERT_EQ(OB_SUCCESS, create_tenant());
   ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
   ASSERT_NE(0, tenant_id);
   ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
 }

 TEST_F(ObCompactionResourceManagerTest, test_copy_plan)
 {
   int ret = OB_SUCCESS;
   MTL_SWITCH(tenant_id) {
     uint64_t mtl_tenant_id = MTL_ID();
     ASSERT_EQ(mtl_tenant_id, tenant_id);
     ObResourceManagerProxy proxy;
     bool exists = false;
     ASSERT_EQ(OB_SUCCESS, proxy.check_if_plan_exist(tenant_id, "SOURCE_PLAN", exists));
     ASSERT_EQ(false, exists);

     ObObj comment;
     comment.set_varchar("source plan for testing");
     ASSERT_EQ(OB_ERR_RES_PLAN_NOT_EXIST, proxy.copy_plan(tenant_id, "SOURCE_PLAN", "TARGET_PLAN", comment));
     execute_stmt(ret, "create_source_plan", "call DBMS_RESOURCE_MANAGER.CREATE_PLAN(PLAN => 'SOURCE_PLAN', COMMENT => 'source plan for testing')");
     execute_stmt(ret, "create_existing_plan", "call DBMS_RESOURCE_MANAGER.CREATE_PLAN(PLAN => 'EXISTING_PLAN', COMMENT => 'existing plan for testing')");
     ASSERT_EQ(OB_ERR_RES_OBJ_ALREADY_EXIST, proxy.copy_plan(tenant_id, "SOURCE_PLAN", "EXISTING_PLAN", comment));
     ASSERT_EQ(OB_SUCCESS, proxy.copy_plan(tenant_id, "SOURCE_PLAN", "TARGET_PLAN", comment));
   }
 }


TEST_F(ObCompactionResourceManagerTest, test_default_resource_manager_plan)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    uint64_t mtl_tenant_id = MTL_ID();
    LOG_INFO("after swith tenant, currrent tenant", K(tenant_id), K(mtl_tenant_id));

    ObArenaAllocator allocator;
    ObDailyWindowJobConfig job_cfg(allocator);
    ObSqlString job_config_out_str;
    ObString uninited_plan;
    ObString default_plan("");
    // when resource manager plan is default value '', need use ObSqlString to set \n at the end of the string, otherwise it will be threated as null
    {
      ObString current_plan;
      char plan_name_data[OB_MAX_RESOURCE_PLAN_NAME_LENGTH];
      ObDataBuffer plan_allocator(plan_name_data, OB_MAX_RESOURCE_PLAN_NAME_LENGTH);
      ASSERT_EQ(OB_SUCCESS, schema::ObSchemaUtils::get_tenant_varchar_variable(tenant_id, SYS_VAR_RESOURCE_MANAGER_PLAN, plan_allocator, current_plan));
      const bool equal_to_uninited_plan = current_plan.case_compare(uninited_plan) == 0;
      const bool equal_to_default_plan = current_plan.case_compare(default_plan) == 0;
      LOG_INFO("current resource manager plan is", K(current_plan), K(equal_to_uninited_plan), K(equal_to_default_plan));
      ASSERT_EQ(OB_SUCCESS, job_cfg.set_prev_plan(current_plan));
      ASSERT_EQ(OB_SUCCESS, job_cfg.encode_to_string(job_config_out_str));
      LOG_INFO("job config is", K(tenant_id), K(job_cfg), K(job_config_out_str));
    }
    {
      ObSqlString current_plan;
      ObCompactionResourceManager compaction_resource_manager;
      compaction_resource_manager.get_current_resource_manager_plan_(current_plan);
      const bool equal_to_uninited_plan = current_plan.string().case_compare(uninited_plan) == 0;
      const bool equal_to_default_plan = current_plan.string().case_compare(default_plan) == 0;
      LOG_INFO("current resource manager plan is", K(current_plan), K(equal_to_uninited_plan), K(equal_to_default_plan));
      ASSERT_EQ(OB_SUCCESS, job_cfg.set_prev_plan(current_plan.string()));
      ASSERT_EQ(OB_SUCCESS, job_cfg.encode_to_string(job_config_out_str));
      LOG_INFO("job config is", K(tenant_id), K(job_cfg), K(job_config_out_str));
    }
  }
}

TEST_F(ObCompactionResourceManagerTest, test_alter_sys_variable)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    uint64_t mtl_tenant_id = MTL_ID();
    ASSERT_EQ(mtl_tenant_id, tenant_id);

    ObCompactionResourceManager compaction_resource_manager;
    ObSqlString original_plan;
    ASSERT_EQ(OB_SUCCESS, compaction_resource_manager.get_current_resource_manager_plan_(original_plan));
    ASSERT_TRUE(original_plan.string().empty());

    ObMySQLTransaction trans;
    ObResourceManagerProxy::TransGuard trans_guard(trans, tenant_id, ret);
    int64_t affected_rows = 0;
    if (trans_guard.ready()) {
      ASSERT_EQ(OB_ERR_RES_MGR_PLAN_NOT_EXIST, trans.write(tenant_id, "set global resource_manager_plan = 'non_exist_plan'", affected_rows));
    }
  }
}

TEST_F(ObCompactionResourceManagerTest, test_switch_to_window_plan)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    uint64_t mtl_tenant_id = MTL_ID();
    ASSERT_EQ(mtl_tenant_id, tenant_id);
    ObCompactionResourceManager compaction_resource_manager;
    int64_t current_epoch = 1;
    rootserver::ObWindowResourceCache resource_cache;
    bool is_switched = false;
    // 1. start with default resource manager plan and no mapping rule
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, compaction_resource_manager.check_window_plan_and_consumer_group_exist_());
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, compaction_resource_manager.check_and_switch(true /*to_window*/, current_epoch, resource_cache, is_switched));
    ASSERT_FALSE(is_switched);
    ASSERT_EQ(OB_SUCCESS, compaction_resource_manager.check_and_switch(false /*to_window*/, current_epoch, resource_cache, is_switched));
    ASSERT_FALSE(is_switched);
    // 2. create window plan and mapping rule
    execute_stmt(ret, "create window plan",
                      "call DBMS_RESOURCE_MANAGER.CREATE_PLAN('INNER_DAILY_WINDOW_PLAN')");
    execute_stmt(ret, "create consumer group",
                      "call DBMS_RESOURCE_MANAGER.CREATE_CONSUMER_GROUP('INNER_DAILY_WINDOW_COMPACTION_LOW_GROUP')");
    execute_stmt(ret, "create mapping rule",
                      "call DBMS_RESOURCE_MANAGER.CREATE_PLAN_DIRECTIVE(PLAN => 'INNER_DAILY_WINDOW_PLAN', GROUP_OR_SUBPLAN => 'INNER_DAILY_WINDOW_COMPACTION_LOW_GROUP', MAX_IOPS => 100, MIN_IOPS => 30, WEIGHT_IOPS => 50,UTILIZATION_LIMIT => 100, MGMT_P1 => 70)");
    ASSERT_EQ(OB_SUCCESS, compaction_resource_manager.check_window_plan_and_consumer_group_exist_());
    ASSERT_EQ(OB_SUCCESS, compaction_resource_manager.check_and_switch(true /*to_window*/, current_epoch, resource_cache, is_switched));
    ASSERT_TRUE(is_switched);
    ASSERT_EQ(OB_SUCCESS, compaction_resource_manager.check_and_switch(true /*to_window*/, current_epoch, resource_cache, is_switched));
    ASSERT_FALSE(is_switched);
    ASSERT_EQ(OB_SUCCESS, compaction_resource_manager.check_and_switch(false /*to_window*/, current_epoch, resource_cache, is_switched));
    ASSERT_TRUE(is_switched);
    ASSERT_EQ(OB_SUCCESS, compaction_resource_manager.check_and_switch(false /*to_window*/, current_epoch, resource_cache, is_switched));
    ASSERT_FALSE(is_switched);
    // 3. create global resource manager plan and mapping rule
    execute_stmt(ret, "create normal resource manager plan",
                      "call DBMS_RESOURCE_MANAGER.CREATE_PLAN('GLOBAL_PLAN')");
    execute_stmt(ret, "create consumer group",
                      "call DBMS_RESOURCE_MANAGER.CREATE_CONSUMER_GROUP('BACKGROUND_GROUP')");
    execute_stmt(ret, "create consumer group mapping rule",
                      "call DBMS_RESOURCE_MANAGER.SET_CONSUMER_GROUP_MAPPING('FUNCTION', 'COMPACTION_LOW', 'BACKGROUND_GROUP')");
    ASSERT_EQ(OB_SUCCESS, compaction_resource_manager.check_window_plan_and_consumer_group_exist_());
    ASSERT_EQ(OB_SUCCESS, compaction_resource_manager.check_and_switch(true /*to_window*/, current_epoch, resource_cache, is_switched));
    ASSERT_TRUE(is_switched);
    ASSERT_EQ(OB_SUCCESS, compaction_resource_manager.check_and_switch(true /*to_window*/, current_epoch, resource_cache, is_switched));
    ASSERT_FALSE(is_switched);
    ASSERT_EQ(OB_SUCCESS, compaction_resource_manager.check_and_switch(false /*to_window*/, current_epoch, resource_cache, is_switched));
    ASSERT_TRUE(is_switched);
    ASSERT_EQ(OB_SUCCESS, compaction_resource_manager.check_and_switch(false /*to_window*/, current_epoch, resource_cache, is_switched));
    ASSERT_FALSE(is_switched);
  }
}

 } // namespace unittest
 } // namespace compaction
 } // namespace oceanbase

 int main(int argc, char **argv)
 {
   oceanbase::unittest::init_log_and_gtest(argc, argv);
   OB_LOGGER.set_log_level("INFO");
   ::testing::InitGoogleTest(&argc, argv);
   return RUN_ALL_TESTS();
 }
