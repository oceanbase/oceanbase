// owner: shenyunlong.syl
// owner group: shenzhen

/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX SERVER_OMT
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "observer/omt/ob_tenant_ai_service.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"

using namespace oceanbase::observer;
using namespace oceanbase::share;
using namespace oceanbase::omt;
using namespace oceanbase::common;

namespace oceanbase
{
namespace unittest
{
class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int time_sec_ = 0;
};

TestRunCtx RunCtx;

class TestAiService: public ObSimpleClusterTestBase
{
public:
  TestAiService() : ObSimpleClusterTestBase("test_ai_service") {}
  virtual ~TestAiService() {}
  void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
  }
private:
  DISALLOW_COPY_AND_ASSIGN(TestAiService);
};

void check_ai_model_endpoint(const ObAiModelEndpointInfo &endpoint_info,
                             ObArenaAllocator &allocator,
                             ObString &endpoint_name,
                             ObString &ai_model_name,
                             ObString &url,
                             ObString &access_key,
                             ObString &provider,
                             ObString &request_model_name,
                             ObString &parameters,
                             ObString &request_transform_fn,
                             ObString &response_transform_fn)
{
  ASSERT_EQ(endpoint_name, endpoint_info.get_name());
  ASSERT_EQ(ai_model_name, endpoint_info.get_ai_model_name());
  ASSERT_EQ(url, endpoint_info.get_url());
  ASSERT_NE(access_key, endpoint_info.get_encrypted_access_key());
  ObString unencrypted_access_key;
  ASSERT_EQ(OB_SUCCESS, endpoint_info.get_unencrypted_access_key(allocator, unencrypted_access_key));
  ASSERT_EQ(access_key, unencrypted_access_key);
  ASSERT_EQ(provider, endpoint_info.get_provider());
  ASSERT_EQ(request_model_name, endpoint_info.get_request_model_name());
  ASSERT_EQ(parameters, endpoint_info.get_parameters());
  ASSERT_EQ(request_transform_fn, endpoint_info.get_request_transform_fn());
  ASSERT_EQ(response_transform_fn, endpoint_info.get_response_transform_fn());
}

TEST_F(TestAiService, test_ai_model_endpoint)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObTenantAiService *ai_service = MTL(ObTenantAiService*);
  ObAiServiceGuard ai_service_guard;
  const ObAiModelEndpointInfo *endpoint_info = nullptr;

  ObString endpoint_name = "test_endpoint";
  ObString ai_model_name = "test_ai_model";
  ObString url = "http://license.coscl.org.cn/MulanPubL-2.0";
  ObString access_key = "sk-1234455";
  ObString provider = "aliyun-openai";
  ObString request_model_name = "text-embedding-v2-custom-model";
  ObString parameters = "";
  ObString request_transform_fn = "";
  ObString response_transform_fn = "";
  common::ObArenaAllocator allocator;
  ObSqlString sql;

  // 1. create ai model endpoint
  std::string json_str = R"({"url": ")";
  json_str += url.ptr();
  json_str += R"(", "access_key": ")";
  json_str += access_key.ptr();
  json_str += R"(", "ai_model_name": ")";
  json_str += ai_model_name.ptr();
  json_str += R"(", "provider": ")";
  json_str += provider.ptr();
  json_str += R"(", "request_model_name": ")";
  json_str += request_model_name.ptr();
  json_str += R"(", "parameters": ")";
  json_str += parameters.ptr();
  json_str += R"(", "request_transform_fn": ")";
  json_str += request_transform_fn.ptr();
  json_str += R"(", "response_transform_fn": ")";
  json_str += response_transform_fn.ptr();
  json_str += R"("})";
  sql.assign_fmt("call DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT ('%s', '%s')", endpoint_name.ptr(), json_str.c_str());
  int64_t affected_rows = 0;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  // 2. get ai model endpoint by endpoint name
  ASSERT_EQ(OB_SUCCESS, ai_service->get_ai_service_guard(ai_service_guard));
  ASSERT_EQ(OB_SUCCESS, ai_service_guard.get_ai_endpoint(endpoint_name, endpoint_info));
  ASSERT_TRUE(endpoint_info != nullptr);
  check_ai_model_endpoint(*endpoint_info, allocator, endpoint_name, ai_model_name, url, access_key,
                          provider, request_model_name, parameters, request_transform_fn, response_transform_fn);

  // 3. get ai model endpoint by ai model name
  endpoint_info = nullptr;
  ASSERT_EQ(OB_SUCCESS, ai_service_guard.get_ai_endpoint_by_ai_model_name(ai_model_name, endpoint_info));
  ASSERT_TRUE(endpoint_info != nullptr);
  check_ai_model_endpoint(*endpoint_info, allocator, endpoint_name, ai_model_name, url, access_key,
                          provider, request_model_name, parameters, request_transform_fn, response_transform_fn);

  // 3. alter ai model endpoint
  access_key = "my_new_access_key_1234567890";
  provider = "openai";

  json_str = R"({"access_key": ")";
  json_str += access_key.ptr();
  json_str += R"(", "provider": ")";
  json_str += provider.ptr();
  json_str += R"(", "request_transform_fn": ")";
  json_str += request_transform_fn.ptr();
  json_str += R"(", "response_transform_fn": ")";
  json_str += response_transform_fn.ptr();
  json_str += R"("})";

  sql.assign_fmt("call DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT ('test_endpoint', '%s')", json_str.c_str());
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  // 4. get ai model endpoint by endpoint name after alter ai model endpoint
  endpoint_info = nullptr;
  ASSERT_EQ(OB_SUCCESS, ai_service_guard.get_ai_endpoint(endpoint_name, endpoint_info));
  ASSERT_TRUE(endpoint_info != nullptr);
  check_ai_model_endpoint(*endpoint_info, allocator, endpoint_name, ai_model_name, url, access_key,
                          provider, request_model_name, parameters, request_transform_fn, response_transform_fn);

  // 5. get ai model endpoint by ai model name after alter ai model endpoint
  endpoint_info = nullptr;
  ASSERT_EQ(OB_SUCCESS, ai_service_guard.get_ai_endpoint_by_ai_model_name(ai_model_name, endpoint_info));
  ASSERT_TRUE(endpoint_info != nullptr);
  check_ai_model_endpoint(*endpoint_info, allocator, endpoint_name, ai_model_name, url, access_key,
                          provider, request_model_name, parameters, request_transform_fn, response_transform_fn);

  // 6. get ai model endpoint by non-exist ai model point
  ASSERT_EQ(OB_AI_FUNC_ENDPOINT_NOT_FOUND, ai_service_guard.get_ai_endpoint("test_endpoint2", endpoint_info));

  // 7. drop ai model endpoint
  sql.assign("call DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT ('test_endpoint')");
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_AI_FUNC_ENDPOINT_NOT_FOUND, ai_service_guard.get_ai_endpoint("test_endpoint", endpoint_info));
}

TEST_F(TestAiService, test_get_increment_ai_model_keys_reversely)
{
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  ObSchemaGetterGuard schema_guard;
  const ObSchemaMgr *schema_mgr = nullptr;
  int64_t curr_version = OB_INVALID_VERSION;
  ObRefreshSchemaStatus schema_status;

  ObMySQLProxy &inner_sql_proxy = get_curr_simple_server().get_sql_proxy();
  ASSERT_TRUE(OB_NOT_NULL(GCTX.schema_service_));
  ObSchemaService *schema_service = GCTX.schema_service_->get_schema_service();
  ASSERT_TRUE(OB_NOT_NULL(schema_service));
  ASSERT_EQ(OB_SUCCESS, get_curr_observer().get_schema_service().get_tenant_schema_guard(tenant_id, schema_guard));
  ASSERT_EQ(OB_SUCCESS, schema_guard.get_schema_version(tenant_id, curr_version));
  ASSERT_NE(OB_INVALID_VERSION, curr_version);
  ASSERT_EQ(OB_SUCCESS, schema_guard.get_schema_status(tenant_id, schema_status));
  ASSERT_TRUE(OB_NOT_NULL(schema_guard.schema_service_));
  ASSERT_EQ(OB_SUCCESS, schema_guard.get_schema_mgr(tenant_id, schema_mgr));
  ASSERT_TRUE(OB_NOT_NULL(schema_mgr));

  ObAiModelMgr &mgr = const_cast<ObAiModelMgr&>(schema_mgr->ai_model_mgr_);

  // 1. add ai model
  uint64_t id = OB_INVALID_ID;
  int64_t version = OB_INVALID_VERSION;
  ASSERT_EQ(OB_SUCCESS, schema_service->fetch_new_ai_model_id(tenant_id, id));
  ASSERT_NE(OB_INVALID_ID, id);
  ASSERT_EQ(OB_SUCCESS, schema_service->gen_new_schema_version(tenant_id, curr_version, version));
  ASSERT_NE(OB_INVALID_VERSION, version);
  ObAiModelSchema schema;
  schema.set_tenant_id(tenant_id);
  schema.set_model_id(id);
  schema.set_schema_version(version);
  schema.set_name("my_ai_model");
  schema.set_type(EndpointType::RERANK);
  schema.set_model_name("my_model_name");
  ASSERT_EQ(OB_SUCCESS, mgr.add_ai_model(schema, ObNameCaseMode::OB_LOWERCASE_AND_INSENSITIVE));

  // 2. try get from schema_guard
  {
    const ObAiModelSchema *schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_ai_model_schema(tenant_id, id, schema));
    ASSERT_TRUE(OB_NOT_NULL(schema));
    ASSERT_EQ(schema->get_ai_model_id(), id);
    ASSERT_EQ(schema->get_tenant_id(), tenant_id);
    ASSERT_EQ(schema->get_schema_version(), version);
    ASSERT_EQ(schema->get_name(), "my_ai_model");
    ASSERT_EQ(schema->get_type(), EndpointType::RERANK);
    ASSERT_EQ(schema->get_model_name(), "my_model_name");
  }

  // 3. test ObServerSchemaService::get_increment_external_resource_keys_reversely
  {
    ObSchemaOperation op;
    op.op_type_ = OB_DDL_CREATE_AI_MODEL;
    op.tenant_id_ = tenant_id;
    op.ai_model_id_ = id;

    ObServerSchemaService::AllSchemaKeys ids;
    ASSERT_EQ(OB_SUCCESS, ids.create(8));
    ASSERT_EQ(OB_SUCCESS, schema_guard.schema_service_->get_increment_ai_model_keys_reversely(*schema_mgr, op, ids));
    ASSERT_EQ(0, ids.new_ai_model_keys_.size());
    ASSERT_EQ(1, ids.del_ai_model_keys_.size());
    ASSERT_EQ(OB_SUCCESS, schema_guard.schema_service_->update_schema_mgr(inner_sql_proxy, schema_status, const_cast<ObSchemaMgr&>(*schema_mgr), curr_version, ids));
  }

  // 4. try get from schema_guard again
  {
    const ObAiModelSchema *schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_ai_model_schema(tenant_id, id, schema));
    ASSERT_TRUE(OB_ISNULL(schema));
  }
}

TEST_F(TestAiService, test_get_model_config_info)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  common::ObArenaAllocator allocator;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const int64_t MB = 1024L * 1024L;

  ObString model_key = "test_model_cfg";
  ObString endpoint_name = "test_model_cfg_endpoint";
  ObString model_name = "qwen3-vl-embedding";
  ObString request_model_name = "qwen3-vl-embedding";
  ObString url = "https://example.com/v1/embeddings";
  ObString access_key = "sk-test-0001";
  ObString provider = "ALIYUN-DASHSCOPE";

  // cleanup leftovers from previous failed runs (best effort)
  sql.assign_fmt("call DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT ('%s')", endpoint_name.ptr());
  (void)sql_proxy.write(sql.ptr(), affected_rows);
  sql.assign_fmt("call DBMS_AI_SERVICE.DROP_AI_MODEL ('%s')", model_key.ptr());
  (void)sql_proxy.write(sql.ptr(), affected_rows);

  // 1. create ai model
  std::string model_json = R"({"model_name": ")";
  model_json += model_name.ptr();
  model_json += R"(", "type": "DENSE_EMBEDDING"})";
  sql.assign_fmt("call DBMS_AI_SERVICE.CREATE_AI_MODEL ('%s', '%s')", model_key.ptr(), model_json.c_str());
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  // 2. create ai endpoint with explicit batch_size/max_image_size
  std::string endpoint_json = R"({"url": ")";
  endpoint_json += url.ptr();
  endpoint_json += R"(", "access_key": ")";
  endpoint_json += access_key.ptr();
  endpoint_json += R"(", "ai_model_name": ")";
  endpoint_json += model_key.ptr();
  endpoint_json += R"(", "provider": ")";
  endpoint_json += provider.ptr();
  endpoint_json += R"(", "request_model_name": ")";
  endpoint_json += request_model_name.ptr();
  endpoint_json += R"(", "parameters": "{\\\"batch_size\\\":4,\\\"max_image_size\\\":2097152}"})";
  sql.assign_fmt("call DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT ('%s', '%s')", endpoint_name.ptr(), endpoint_json.c_str());
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  // 3. get model config and verify endpoint overrides
  {
    ObAIModelConfigInfo config;
    ASSERT_EQ(OB_SUCCESS, common::ObAIFuncUtils::get_model_config_info(allocator, model_key, config));
    ASSERT_EQ(0, config.get_model_key().compare(model_key));
    ASSERT_EQ(0, config.get_model_name().compare(model_name));
    ASSERT_EQ(0, config.get_provider().case_compare("ALIYUN-DASHSCOPE"));
    ASSERT_EQ(0, config.get_request_model_name().compare(request_model_name));
    ASSERT_EQ(4, config.get_batch_size());
    ASSERT_EQ(2 * MB, config.get_max_image_size());
  }

  // 4. alter endpoint, keep only batch_size then fallback max_image_size to default model config
  std::string alter_json = R"({"parameters": "{\\\"batch_size\\\":3}"})";
  sql.assign_fmt("call DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT ('%s', '%s')", endpoint_name.ptr(), alter_json.c_str());
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  {
    ObAIModelConfigInfo config;
    ASSERT_EQ(OB_SUCCESS, common::ObAIFuncUtils::get_model_config_info(allocator, model_key, config));
    ASSERT_EQ(3, config.get_batch_size());
    ASSERT_EQ(5 * MB, config.get_max_image_size());
  }

  // 5. alter endpoint, clear parameters then both values fallback to default
  alter_json = R"({"parameters": ""})";
  sql.assign_fmt("call DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT ('%s', '%s')", endpoint_name.ptr(), alter_json.c_str());
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  {
    ObAIModelConfigInfo config;
    ASSERT_EQ(OB_SUCCESS, common::ObAIFuncUtils::get_model_config_info(allocator, model_key, config));
    ASSERT_EQ(5, config.get_batch_size());
    ASSERT_EQ(5 * MB, config.get_max_image_size());
  }

  // 6. alter endpoint with mixed-case provider/model and verify case-insensitive default config lookup
  alter_json = R"({"provider":"aliyun-dashscope","request_model_name":"QWEN3-VL-EMBEDDING","parameters":""})";
  sql.assign_fmt("call DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT ('%s', '%s')", endpoint_name.ptr(), alter_json.c_str());
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  {
    ObAIModelConfigInfo config;
    ASSERT_EQ(OB_SUCCESS, common::ObAIFuncUtils::get_model_config_info(allocator, model_key, config));
    ASSERT_EQ(0, config.get_provider().case_compare("aliyun-dashscope"));
    ASSERT_EQ(0, config.get_request_model_name().case_compare("QWEN3-VL-EMBEDDING"));
    ASSERT_EQ(5, config.get_batch_size());
    ASSERT_EQ(5 * MB, config.get_max_image_size());
  }

  // 7. cleanup
  sql.assign_fmt("call DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT ('%s')", endpoint_name.ptr());
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  sql.assign_fmt("call DBMS_AI_SERVICE.DROP_AI_MODEL ('%s')", model_key.ptr());
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
}

TEST_F(TestAiService, test_get_model_config_info_negative)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  common::ObArenaAllocator allocator;
  ObSqlString sql;
  int64_t affected_rows = 0;

  // 1. empty model key
  {
    ObAIModelConfigInfo config;
    ASSERT_EQ(OB_INVALID_ARGUMENT, common::ObAIFuncUtils::get_model_config_info(allocator, ObString(), config));
  }

  // 2. model key not exists
  {
    ObAIModelConfigInfo config;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              common::ObAIFuncUtils::get_model_config_info(allocator,
                                                           ObString::make_string("model_key_not_exists"),
                                                           config));
  }

  // 3. model exists but endpoint missing
  ObString model_key = "test_model_cfg_no_endpoint";
  ObString model_name = "qwen3-vl-embedding";
  std::string model_json = R"({"model_name": ")";
  model_json += model_name.ptr();
  model_json += R"(", "type": "DENSE_EMBEDDING"})";
  sql.assign_fmt("call DBMS_AI_SERVICE.CREATE_AI_MODEL ('%s', '%s')", model_key.ptr(), model_json.c_str());
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  {
    ObAIModelConfigInfo config;
    ASSERT_EQ(OB_AI_FUNC_ENDPOINT_NOT_FOUND, common::ObAIFuncUtils::get_model_config_info(allocator, model_key, config));
  }

  sql.assign_fmt("call DBMS_AI_SERVICE.DROP_AI_MODEL ('%s')", model_key.ptr());
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
}

TEST_F(TestAiService, end)
{
  RunCtx.time_sec_ = 0;
  if (RunCtx.time_sec_ > 0) {
    ::sleep(RunCtx.time_sec_);
  }
}
} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  int64_t c = 0;
  int64_t time_sec = 0;
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

  LOG_INFO("main>>>");
  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}