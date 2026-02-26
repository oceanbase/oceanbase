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


int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
