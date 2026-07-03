/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#include "share/ai_service/ob_ai_service_struct.h"
#undef private
#include "lib/json/ob_json.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace unittest
{

class TestAiServiceStruct : public ::testing::Test
{
public:
  void init_valid_endpoint(ObAiModelEndpointInfo &endpoint, const char *parameters = "")
  {
    endpoint.reset();
    endpoint.name_ = ObString::make_string("ep1");
    endpoint.scope_ = ObAiModelEndpointInfo::DEFAULT_SCOPE;
    endpoint.ai_model_name_ = ObString::make_string("model1");
    endpoint.url_ = ObString::make_string("https://example.com/v1/chat/completions");
    endpoint.access_key_ = ObString::make_string("mock_access_key");
    endpoint.provider_ = ObString::make_string("OPENAI");
    endpoint.parameters_ = ObString::make_string(parameters);
    endpoint.request_transform_fn_.reset();
    endpoint.response_transform_fn_.reset();
  }

  int build_json_base(ObArenaAllocator &allocator, const char *json_text, ObIJsonBase *&jbase)
  {
    ObString json_str = ObString::make_string(json_text);
    return ObJsonBaseFactory::get_json_base(&allocator,
                                            json_str,
                                            ObJsonInType::JSON_TREE,
                                            ObJsonInType::JSON_TREE,
                                            jbase);
  }

  int alter_endpoint_parameters(const char *delta_json, ObAiModelEndpointInfo &endpoint)
  {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator("TestAIEP");
    ObIJsonBase *delta_jbase = nullptr;
    if (OB_FAIL(build_json_base(allocator, delta_json, delta_jbase))) {
      return ret;
    }
    if (nullptr == delta_jbase) {
      return OB_ERR_UNEXPECTED;
    }
    ret = endpoint.merge_delta_endpoint(allocator, *delta_jbase);
    return ret;
  }
};

// check_valid(): create path
TEST_F(TestAiServiceStruct, check_valid_positive_cases)
{
  ObAiModelEndpointInfo endpoint;
  init_valid_endpoint(endpoint, "{\"batch_size\":8,\"max_image_size\":2048}");
  ASSERT_EQ(OB_SUCCESS, endpoint.check_valid());
  init_valid_endpoint(endpoint, "{\"batch_size\":1}");
  ASSERT_EQ(OB_SUCCESS, endpoint.check_valid());
  init_valid_endpoint(endpoint, "{\"max_image_size\":4096}");
  ASSERT_EQ(OB_SUCCESS, endpoint.check_valid());
  init_valid_endpoint(endpoint, "");
  ASSERT_EQ(OB_SUCCESS, endpoint.check_valid());
  // temperature: current design does not validate, should pass
  init_valid_endpoint(endpoint, "{\"temperature\":1}");
  ASSERT_EQ(OB_SUCCESS, endpoint.check_valid());
}

TEST_F(TestAiServiceStruct, check_valid_reject_type_or_format_errors)
{
  ObAiModelEndpointInfo endpoint;
  // value must be integer, string is rejected
  init_valid_endpoint(endpoint, "{\"batch_size\":\"8\"}");
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID, endpoint.check_valid());
  // value must be integer, float is rejected
  init_valid_endpoint(endpoint, "{\"max_image_size\":3.14}");
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID, endpoint.check_valid());
  // value must be integer, bool is rejected
  init_valid_endpoint(endpoint, "{\"batch_size\":true}");
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID, endpoint.check_valid());
  // parameters must be a JSON object
  init_valid_endpoint(endpoint, "[1,2,3]");
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID, endpoint.check_valid());
  // malformed JSON text in parameters string
  init_valid_endpoint(endpoint, "{\"batch_size\":16");
  ASSERT_EQ(OB_ERR_INVALID_JSON_TEXT, endpoint.check_valid());
}

TEST_F(TestAiServiceStruct, check_valid_reject_value_or_key_errors)
{
  ObAiModelEndpointInfo endpoint;
  // batch_size must be non-negative
  init_valid_endpoint(endpoint, "{\"batch_size\":-1}");
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID, endpoint.check_valid());
  // max_image_size must be non-negative
  init_valid_endpoint(endpoint, "{\"max_image_size\":-1024}");
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID, endpoint.check_valid());
}

// merge_delta_endpoint(): alter path
TEST_F(TestAiServiceStruct, merge_delta_endpoint_positive_cases)
{
  ObAiModelEndpointInfo endpoint;
  init_valid_endpoint(endpoint, "");
  ASSERT_EQ(OB_SUCCESS, alter_endpoint_parameters("{\"parameters\":\"{\\\"batch_size\\\":16}\"}", endpoint));
  ASSERT_EQ(0, endpoint.get_parameters().compare("{\"batch_size\":16}"));
  ASSERT_EQ(OB_SUCCESS,
            alter_endpoint_parameters("{\"parameters\":\"{\\\"batch_size\\\":8,\\\"max_image_size\\\":1024}\"}", endpoint));
  ASSERT_EQ(0, endpoint.get_parameters().compare("{\"batch_size\":8,\"max_image_size\":1024}"));
}

TEST_F(TestAiServiceStruct, merge_delta_endpoint_reject_type_or_format_errors)
{
  ObAiModelEndpointInfo endpoint;
  init_valid_endpoint(endpoint, "");
  // inner parameters JSON has string type for integer field
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
            alter_endpoint_parameters(R"({"parameters":"{\"batch_size\":\"16\"}"})", endpoint));
  // inner parameters must be object, not array
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
            alter_endpoint_parameters(R"({"parameters":"[1,2]"})", endpoint));
  // inner parameters JSON text is malformed
  ASSERT_EQ(OB_ERR_INVALID_JSON_TEXT,
            alter_endpoint_parameters("{\"parameters\":\"{\\\"batch_size\\\":16\"}", endpoint));
  // outer parameters field must be string in alter payload
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
            alter_endpoint_parameters("{\"parameters\":{\"batch_size\":16}}", endpoint));
}

TEST_F(TestAiServiceStruct, merge_delta_endpoint_reject_value_or_key_errors)
{
  ObAiModelEndpointInfo endpoint;
  init_valid_endpoint(endpoint, "");
  // temperature: current design does not validate, should pass
  ASSERT_EQ(OB_SUCCESS,
            alter_endpoint_parameters(R"({"parameters":"{\"temperature\":1}"})", endpoint));
  // negative batch_size is invalid
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
            alter_endpoint_parameters(R"({"parameters":"{\"batch_size\":-1}"})", endpoint));
  // negative max_image_size is invalid
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
            alter_endpoint_parameters(R"({"parameters":"{\"max_image_size\":-1024}"})", endpoint));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ai_service_struct.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
