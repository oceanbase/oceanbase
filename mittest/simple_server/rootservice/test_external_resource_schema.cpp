// owner: heyongyi.hyy
// owner group: pl

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

#include <gmock/gmock.h>

#define USING_LOG_PREFIX SHARE
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "share/schema/ob_schema_service_sql_impl.h"

namespace oceanbase
{
using namespace unittest;
namespace share
{
using namespace share::schema;
using namespace common;

static uint64_t g_tenant_id;

class TestExternalResource : public unittest::ObSimpleClusterTestBase
{
public:
TestExternalResource() : unittest::ObSimpleClusterTestBase("test_schema_service_sql_impl") {}
};

TEST_F(TestExternalResource, prepare_data)
{
g_tenant_id = OB_INVALID_TENANT_ID;

ASSERT_EQ(OB_SUCCESS, create_tenant());
ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

TEST_F(TestExternalResource, test_get_increment_external_resource_keys_reversely)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObSchemaMgr *schema_mgr = nullptr;
  int64_t curr_version = OB_INVALID_VERSION;
  ObRefreshSchemaStatus schema_status;

  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ASSERT_TRUE(OB_NOT_NULL(GCTX.schema_service_));
  ObSchemaService *schema_service = GCTX.schema_service_->get_schema_service();
  ASSERT_TRUE(OB_NOT_NULL(schema_service));
  ASSERT_EQ(OB_SUCCESS, get_curr_observer().get_schema_service().get_tenant_schema_guard(g_tenant_id, schema_guard));
  ASSERT_EQ(OB_SUCCESS, schema_guard.get_schema_version(g_tenant_id, curr_version));
  ASSERT_NE(OB_INVALID_VERSION, curr_version);
  ASSERT_EQ(OB_SUCCESS, schema_guard.get_schema_status(g_tenant_id, schema_status));
  ASSERT_TRUE(OB_NOT_NULL(schema_guard.schema_service_));
  ASSERT_EQ(OB_SUCCESS, schema_guard.get_schema_mgr(g_tenant_id, schema_mgr));
  ASSERT_TRUE(OB_NOT_NULL(schema_mgr));

  ObExternalResourceMgr &mgr = const_cast<ObExternalResourceMgr&>(schema_mgr->external_resource_mgr_);

  // 1. add external resource
  uint64_t id = OB_INVALID_ID;
  int64_t version = OB_INVALID_VERSION;
  ASSERT_EQ(OB_SUCCESS, schema_service->fetch_new_external_resource_id(g_tenant_id, id));
  ASSERT_NE(OB_INVALID_ID, id);
  ASSERT_EQ(OB_SUCCESS, schema_service->gen_new_schema_version(g_tenant_id, curr_version, version));
  ASSERT_NE(OB_INVALID_VERSION, version);
  ObSimpleExternalResourceSchema schema;
  schema.set_tenant_id(g_tenant_id);
  schema.set_resource_id(id);
  schema.set_schema_version(version);
  schema.set_database_id(OB_SYS_DATABASE_ID);
  schema.set_name("my_res");
  schema.set_type(ObSimpleExternalResourceSchema::JAVA_JAR_TYPE);
  ASSERT_EQ(OB_SUCCESS, mgr.add_external_resource(schema));

  // 2. try get from schema_guard
  {
    const ObSimpleExternalResourceSchema *schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_external_resource_schema(g_tenant_id, id, schema));
    ASSERT_TRUE(OB_NOT_NULL(schema));
  }

  // 3. test ObServerSchemaService::get_increment_external_resource_keys_reversely
  {
    ObSchemaOperation op;
    op.op_type_ = OB_DDL_CREATE_EXTERNAL_RESOURCE;
    op.tenant_id_ = g_tenant_id;
    op.external_resource_id_ = id;
    op.database_id_ = OB_SYS_DATABASE_ID;

    ObServerSchemaService::AllSchemaKeys ids;
    ASSERT_EQ(OB_SUCCESS, ids.create(8));
    ASSERT_EQ(OB_SUCCESS, schema_guard.schema_service_->get_increment_external_resource_keys_reversely(*schema_mgr, op, ids));
    ASSERT_EQ(0, ids.new_external_resource_keys_.size());
    ASSERT_EQ(1, ids.del_external_resource_keys_.size());
    ASSERT_EQ(OB_SUCCESS, schema_guard.schema_service_->update_schema_mgr(inner_sql_proxy, schema_status, const_cast<ObSchemaMgr&>(*schema_mgr), curr_version, ids));
  }

  // 4. try get from schema_guard again
  {
    const ObSimpleExternalResourceSchema *schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_external_resource_schema(g_tenant_id, id, schema));
    ASSERT_TRUE(OB_ISNULL(schema));
  }
}

} // namespace rootserver
} // namespace oceanbase
int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
