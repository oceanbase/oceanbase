/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "ob_expr_test_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObAIPromptTest: public ::testing::Test
{
public:
  ObAIPromptTest();
  virtual ~ObAIPromptTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObAIPromptTest(const ObAIPromptTest &other);
  ObAIPromptTest& operator=(const ObAIPromptTest &other);
protected:
  // data members
};

ObAIPromptTest::ObAIPromptTest()
{
}

ObAIPromptTest::~ObAIPromptTest()
{
}

void ObAIPromptTest::SetUp()
{
}

void ObAIPromptTest::TearDown()
{
}

TEST_F(ObAIPromptTest, test_ob_is_vaild_prompt_object)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString a("a");
  ObJsonString a_str(a);
  ObString raw_str("{\"template\": \"{0}+{1}={2} 吗？请回答true或false\", \"args\": [\"1\", \"2\", \"3\"]}");
  ObJsonObject *prompt_object = NULL;
  ObString prompt_str;
  ObAIFuncJsonUtils::get_json_object_form_str(allocator, raw_str, prompt_object);
  ASSERT_TRUE(ObAIFuncPromptObjectUtils::is_valid_prompt_object(prompt_object));
  prompt_object->add("a",&a_str);
  ASSERT_FALSE(ObAIFuncPromptObjectUtils::is_valid_prompt_object(prompt_object));
}

TEST_F(ObAIPromptTest, test_replace_all_str_args_in_template)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString a("a");
  ObJsonString a_str(a);
  ObString raw_str("{\"template\": \"{0}+{1}={2} 吗？请回答true或false\", \"args\": [\"1\", \"2\", \"3\"]}");
  ObJsonObject *prompt_object = NULL;
  ObString prompt_str;
  ObString result_str;
  ObAIFuncJsonUtils::get_json_object_form_str(allocator, raw_str, prompt_object);
  ASSERT_TRUE(ObAIFuncPromptObjectUtils::is_valid_prompt_object(prompt_object));
  ASSERT_EQ(OB_SUCCESS, ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(allocator, prompt_object, prompt_str));
  ob_write_string(allocator, prompt_str, result_str,true);
  ASSERT_EQ(result_str, "1+2=3 吗？请回答true或false");
  // std::cout << "prompt_str: " << prompt_str.ptr() << std::endl;
  // std::cout << "result_str: " << result_str.ptr() << std::endl;
}

TEST_F(ObAIPromptTest, test_parse_template_arg_types_basic_and_ignore_invalid)
{
  ObString template_str("prefix {0} and {img_1} ignore {img_} {img_a} {abc} {img_2");
  ObSEArray<ObAIFuncPromptObjectUtils::PromptArgType, 8> arg_types;
  ASSERT_EQ(OB_SUCCESS, arg_types.push_back(ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN));
  ASSERT_EQ(OB_SUCCESS, arg_types.push_back(ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN));
  ASSERT_EQ(OB_SUCCESS, arg_types.push_back(ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN));
  ASSERT_EQ(OB_SUCCESS, ObAIFuncPromptObjectUtils::parse_template_arg_types(template_str, 3, arg_types));
  ASSERT_EQ(ObAIFuncPromptObjectUtils::PromptArgType::TEXT, arg_types.at(0));
  ASSERT_EQ(ObAIFuncPromptObjectUtils::PromptArgType::IMAGE, arg_types.at(1));
  ASSERT_EQ(ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN, arg_types.at(2));
}

TEST_F(ObAIPromptTest, test_parse_template_arg_types_error_cases)
{
  {
    ObString template_str("{0} + {img_0}");
    ObSEArray<ObAIFuncPromptObjectUtils::PromptArgType, 8> arg_types;
    ASSERT_EQ(OB_SUCCESS, arg_types.push_back(ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN));
    ASSERT_EQ(OB_INVALID_ARGUMENT, ObAIFuncPromptObjectUtils::parse_template_arg_types(template_str, 1, arg_types));
  }
  {
    ObString template_str("{1}");
    ObSEArray<ObAIFuncPromptObjectUtils::PromptArgType, 8> arg_types;
    ASSERT_EQ(OB_SUCCESS, arg_types.push_back(ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN));
    ASSERT_EQ(OB_INVALID_ARGUMENT, ObAIFuncPromptObjectUtils::parse_template_arg_types(template_str, 1, arg_types));
  }
  {
    ObString template_str("{999999999999999999999}");
    ObSEArray<ObAIFuncPromptObjectUtils::PromptArgType, 8> arg_types;
    ASSERT_EQ(OB_SUCCESS, arg_types.push_back(ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN));
    ASSERT_EQ(OB_INVALID_ARGUMENT, ObAIFuncPromptObjectUtils::parse_template_arg_types(template_str, 1, arg_types));
  }
  {
    ObString template_str("{0} {0}");
    ObSEArray<ObAIFuncPromptObjectUtils::PromptArgType, 8> arg_types;
    ASSERT_EQ(OB_SUCCESS, arg_types.push_back(ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN));
    ASSERT_EQ(OB_INVALID_ARGUMENT, ObAIFuncPromptObjectUtils::parse_template_arg_types(template_str, 1, arg_types));
  }
  {
    ObString template_str("{img_1}");
    ObSEArray<ObAIFuncPromptObjectUtils::PromptArgType, 8> arg_types;
    ASSERT_EQ(OB_SUCCESS, arg_types.push_back(ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN));
    ASSERT_EQ(OB_INVALID_ARGUMENT, ObAIFuncPromptObjectUtils::parse_template_arg_types(template_str, 1, arg_types));
  }
}

TEST_F(ObAIPromptTest, test_parse_template_arg_types_exact_positive_cases)
{
  {
    ObString template_str("a {0} b {1}");
    ObSEArray<ObAIFuncPromptObjectUtils::PromptArgType, 8> arg_types;
    ASSERT_EQ(OB_SUCCESS, arg_types.push_back(ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN));
    ASSERT_EQ(OB_SUCCESS, arg_types.push_back(ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN));
    ASSERT_EQ(OB_SUCCESS, ObAIFuncPromptObjectUtils::parse_template_arg_types(template_str, 2, arg_types));
    ASSERT_EQ(ObAIFuncPromptObjectUtils::PromptArgType::TEXT, arg_types.at(0));
    ASSERT_EQ(ObAIFuncPromptObjectUtils::PromptArgType::TEXT, arg_types.at(1));
  }
  {
    ObString template_str("a {img_0} b");
    ObSEArray<ObAIFuncPromptObjectUtils::PromptArgType, 8> arg_types;
    ASSERT_EQ(OB_SUCCESS, arg_types.push_back(ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN));
    ASSERT_EQ(OB_SUCCESS, ObAIFuncPromptObjectUtils::parse_template_arg_types(template_str, 1, arg_types));
    ASSERT_EQ(ObAIFuncPromptObjectUtils::PromptArgType::IMAGE, arg_types.at(0));
  }
  {
    ObString template_str("{img_0} {1}");
    ObSEArray<ObAIFuncPromptObjectUtils::PromptArgType, 8> arg_types;
    ASSERT_EQ(OB_SUCCESS, arg_types.push_back(ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN));
    ASSERT_EQ(OB_SUCCESS, arg_types.push_back(ObAIFuncPromptObjectUtils::PromptArgType::UNKNOWN));
    ASSERT_EQ(OB_SUCCESS, ObAIFuncPromptObjectUtils::parse_template_arg_types(template_str, 2, arg_types));
    ASSERT_EQ(ObAIFuncPromptObjectUtils::PromptArgType::IMAGE, arg_types.at(0));
    ASSERT_EQ(ObAIFuncPromptObjectUtils::PromptArgType::TEXT, arg_types.at(1));
  }
}

TEST_F(ObAIPromptTest, test_build_image_arg_object_url_and_binary)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonObject *image_obj = NULL;
  ObJsonNode *node = NULL;

  ASSERT_EQ(OB_SUCCESS,
            ObAIFuncPromptObjectUtils::build_image_arg_object(
                allocator, ObString("https://www.oceanbase.com/a.png"),
                ObAIFuncPromptObjectUtils::ImageArgType::URL, image_obj));
  ASSERT_TRUE(NULL != image_obj);
  node = image_obj->get_value("type");
  ASSERT_TRUE(NULL != node);
  ASSERT_EQ(ObJsonNodeType::J_STRING, node->json_type());
  ASSERT_EQ(ObString("image"), static_cast<ObJsonString *>(node)->get_str());
  node = image_obj->get_value("url");
  ASSERT_TRUE(NULL != node);
  ASSERT_EQ(ObJsonNodeType::J_STRING, node->json_type());
  ASSERT_EQ(ObString("https://www.oceanbase.com/a.png"), static_cast<ObJsonString *>(node)->get_str());

  image_obj = NULL;
  ASSERT_EQ(OB_SUCCESS,
            ObAIFuncPromptObjectUtils::build_image_arg_object(
                allocator, ObString("https://www.oceanbase.com/b.png"),
                ObAIFuncPromptObjectUtils::ImageArgType::URL, image_obj));
  ASSERT_TRUE(NULL != image_obj);
  node = image_obj->get_value("url");
  ASSERT_TRUE(NULL != node);
  ASSERT_EQ(ObJsonNodeType::J_STRING, node->json_type());
  ASSERT_EQ(ObString("https://www.oceanbase.com/b.png"), static_cast<ObJsonString *>(node)->get_str());

  image_obj = NULL;
  ASSERT_EQ(OB_SUCCESS,
            ObAIFuncPromptObjectUtils::build_image_arg_object(
                allocator, ObString("https://www.oceanbase.com/c.png"),
                ObAIFuncPromptObjectUtils::ImageArgType::URL, image_obj));
  ASSERT_TRUE(NULL != image_obj);
  node = image_obj->get_value("url");
  ASSERT_TRUE(NULL != node);
  ASSERT_EQ(ObJsonNodeType::J_STRING, node->json_type());
  ASSERT_EQ(ObString("https://www.oceanbase.com/c.png"), static_cast<ObJsonString *>(node)->get_str());

  // BINARY 类型需要传入带正确魔数的图片二进制，实现通过 get_type_from_binary 识别类型
  const uint8_t png_magic[] = {0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A};
  ObString png_binary(8, reinterpret_cast<const char *>(png_magic));
  image_obj = NULL;
  ASSERT_EQ(OB_SUCCESS,
            ObAIFuncPromptObjectUtils::build_image_arg_object(
                allocator, png_binary,
                ObAIFuncPromptObjectUtils::ImageArgType::BINARY, image_obj));
  ASSERT_TRUE(NULL != image_obj);
  node = image_obj->get_value("format");
  ASSERT_TRUE(NULL != node);
  ASSERT_EQ(ObJsonNodeType::J_STRING, node->json_type());
  ASSERT_EQ(ObString("png"), static_cast<ObJsonString *>(node)->get_str());
  node = image_obj->get_value("data");
  ASSERT_TRUE(NULL != node);
  ASSERT_EQ(ObJsonNodeType::J_STRING, node->json_type());
  ASSERT_EQ(ObString("iVBORw0KGgo="), static_cast<ObJsonString *>(node)->get_str());

  const uint8_t jpeg_magic[] = {0xFF, 0xD8, 0xFF};
  ObString jpeg_binary(3, reinterpret_cast<const char *>(jpeg_magic));
  image_obj = NULL;
  ASSERT_EQ(OB_SUCCESS,
            ObAIFuncPromptObjectUtils::build_image_arg_object(
                allocator, jpeg_binary,
                ObAIFuncPromptObjectUtils::ImageArgType::BINARY, image_obj));
  ASSERT_TRUE(NULL != image_obj);
  node = image_obj->get_value("format");
  ASSERT_TRUE(NULL != node);
  ASSERT_EQ(ObJsonNodeType::J_STRING, node->json_type());
  ASSERT_EQ(ObString("jpeg"), static_cast<ObJsonString *>(node)->get_str());
  node = image_obj->get_value("data");
  ASSERT_TRUE(NULL != node);
  ASSERT_EQ(ObJsonNodeType::J_STRING, node->json_type());
  ASSERT_EQ(ObString("/9j/"), static_cast<ObJsonString *>(node)->get_str());
}

TEST_F(ObAIPromptTest, test_build_image_arg_object_error_cases)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonObject *image_obj = NULL;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObAIFuncPromptObjectUtils::build_image_arg_object(
                allocator, ObString("ftp://invalid"),
                ObAIFuncPromptObjectUtils::ImageArgType::URL, image_obj));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObAIFuncPromptObjectUtils::build_image_arg_object(
                allocator, ObString("abc"),
                ObAIFuncPromptObjectUtils::ImageArgType::URL, image_obj));

  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObAIFuncPromptObjectUtils::build_image_arg_object(
                allocator, ObString("abc"),
                static_cast<ObAIFuncPromptObjectUtils::ImageArgType>(99), image_obj));

  // BINARY 类型要求输入为带正确魔数的图片二进制，非图片数据返回 OB_INVALID_DATA
  ASSERT_EQ(OB_INVALID_DATA,
            ObAIFuncPromptObjectUtils::build_image_arg_object(
                allocator, ObString("abc"),
                ObAIFuncPromptObjectUtils::ImageArgType::BINARY, image_obj));
  ASSERT_EQ(OB_INVALID_DATA,
            ObAIFuncPromptObjectUtils::build_image_arg_object(
                allocator, ObString("Hello"),
                ObAIFuncPromptObjectUtils::ImageArgType::BINARY, image_obj));
}


int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
