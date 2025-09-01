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

class ObOpenAIUtilsTest: public ::testing::Test
{
public:
    ObOpenAIUtilsTest();
    virtual ~ObOpenAIUtilsTest();
    virtual void SetUp();
    virtual void TearDown();
private:
    // disallow copy
    ObOpenAIUtilsTest(const ObOpenAIUtilsTest &other);
    ObOpenAIUtilsTest& operator=(const ObOpenAIUtilsTest &other);
protected:
    // data members
};

ObOpenAIUtilsTest::ObOpenAIUtilsTest()
{
}

ObOpenAIUtilsTest::~ObOpenAIUtilsTest()
{
}

void ObOpenAIUtilsTest::SetUp()
{
}

void ObOpenAIUtilsTest::TearDown()
{
}

TEST_F(ObOpenAIUtilsTest, test_complete_get_header)
{
    ObArray<ObString> headers;
    ObString api_key("sk-1234567890");
    ObString authorization("Authorization: Bearer sk-1234567890");
    ObString content_type("Content-Type: application/json");
    ObArenaAllocator allocator(ObModIds::TEST);
    ObOpenAIUtils::ObOpenAIComplete completion;
    ASSERT_EQ(OB_SUCCESS, completion.get_header(allocator, api_key, headers));
    ASSERT_EQ(2, headers.count());
    ASSERT_EQ(authorization, headers[0]);
    ASSERT_EQ(content_type, headers[1]);
}

TEST_F(ObOpenAIUtilsTest, test_complete_get_body)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString model("gpt-4o-mini");
    ObString prompt("You are a helpful assistant.");
    ObString content("oceanbase is a distributed database");
    ObString messages("[{\"role\": \"system\", \"content\": \"You are a helpful assistant.\"}, {\"role\": \"user\", \"content\": \"what is oceanbase?\"}]");
    ObOpenAIUtils::ObOpenAIComplete completion;
    ObJsonObject *body = nullptr;
    ObJsonObject *config = nullptr;
    ASSERT_EQ(OB_SUCCESS, completion.get_body(allocator, model, prompt, content, config, body));
    ObJsonNode *model_node = body->get_value("model");
    ObStringBuffer model_buf(&allocator);
    model_node->print(model_buf, 0);
    ASSERT_EQ(model, model_buf.string());

    ObJsonNode *messages_node = body->get_value("messages");
    ObJsonArray *messages_array = static_cast<ObJsonArray *>(messages_node);
    ASSERT_EQ(2, messages_array->element_count());

    ObJsonNode *message_node1 = messages_array->get_value(0);
    ObJsonObject *message_obj1 = static_cast<ObJsonObject *>(message_node1);
    ObJsonNode *prompt_node = message_obj1->get_value("content");
    ObStringBuffer prompt_buf(&allocator);
    prompt_node->print(prompt_buf, 0);
    ASSERT_EQ(prompt, prompt_buf.string());

    ObJsonNode *message_node2 = messages_array->get_value(1);
    ObJsonObject *message_obj2 = static_cast<ObJsonObject *>(message_node2);
    ObJsonNode *content_node = message_obj2->get_value("content");
    ObStringBuffer content_buf(&allocator);
    content_node->print(content_buf, 0);
    ASSERT_EQ(content, content_buf.string());
}

TEST_F(ObOpenAIUtilsTest, test_complete_parse_output)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString content("天气真好。");
    ObString response(
        "{"
        "\"choices\": [ "
            "{"
                "\"message\": {"
                    "\"role\": \"assistant\","
                    "\"content\": \"天气真好。\""
                "},"
                "\"finish_reason\": \"stop\","
                "\"index\": 0,"
                "\"logprobs\": null"
            "}"
        "],"
        "\"object\": \"chat.completion\","
        "\"usage\": {"
            "\"prompt_tokens\": 3019,"
            "\"completion_tokens\": 104,"
            "\"total_tokens\": 3123,"
            "\"prompt_tokens_details\": {"
                "\"cached_tokens\": 2048"
            "}"
        "},"
        "\"created\": 1735120033,"
        "\"system_fingerprint\": null,"
        "\"model\": \"qwen-plus\","
        "\"id\": \"chatcmpl-6ada9ed2-7f33-9de2-8bb0-78bd4035025a\""
    "}"
    );
    ObOpenAIUtils::ObOpenAIComplete completion;
    ObIJsonBase *j_base = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, response, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base));
    ObJsonObject *http_response = static_cast<ObJsonObject *>(j_base);
    ObIJsonBase *result = nullptr;
    ASSERT_EQ(OB_SUCCESS, completion.parse_output(allocator, http_response, result));
    ObStringBuffer content_buf(&allocator);
    result->print(content_buf, 0);
    ASSERT_EQ(content, content_buf.string());
}

TEST_F(ObOpenAIUtilsTest, test_complete_parse_output_empty)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString content("天气真好。");
    ObString response(
        "{"
        "\"choices\": [ "
            "{"
                "\"message\": {"
                    "\"role\": \"assistant\""

                "},"
                "\"finish_reason\": \"stop\","
                "\"index\": 0,"
                "\"logprobs\": null"
            "}"
        "],"
        "\"object\": \"chat.completion\","
        "\"usage\": {"
            "\"prompt_tokens\": 3019,"
            "\"completion_tokens\": 104,"
            "\"total_tokens\": 3123,"
            "\"prompt_tokens_details\": {"
                "\"cached_tokens\": 2048"
            "}"
        "},"
        "\"created\": 1735120033,"
        "\"system_fingerprint\": null,"
        "\"model\": \"qwen-plus\","
        "\"id\": \"chatcmpl-6ada9ed2-7f33-9de2-8bb0-78bd4035025a\""
    "}"
    );
    ObOpenAIUtils::ObOpenAIComplete completion;
    ObIJsonBase *j_base = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, response, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base));
    ObJsonObject *http_response = static_cast<ObJsonObject *>(j_base);
    ObIJsonBase *result = nullptr;
    ASSERT_EQ(OB_ERR_UNEXPECTED, completion.parse_output(allocator, http_response, result));
}

TEST_F(ObOpenAIUtilsTest, test_embedding_get_header)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString api_key("sk-1234567890");
    ObString authorization("Authorization: Bearer sk-1234567890");
    ObString content_type("Content-Type: application/json");
    ObOpenAIUtils::ObOpenAIEmbed embedding;
    ObArray<ObString> headers;
    ASSERT_EQ(OB_SUCCESS, embedding.get_header(allocator, api_key, headers));
    ASSERT_EQ(2, headers.count());
    ASSERT_EQ(authorization, headers[0]);
    ASSERT_EQ(content_type, headers[1]);
}

TEST_F(ObOpenAIUtilsTest, test_embedding_get_body)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString model("text-embedding-3-small");
    ObString input("oceanbase is a distributed database");
    ObArray<ObString> input_array;
    input_array.push_back(input);
    ObOpenAIUtils::ObOpenAIEmbed embedding;
    ObJsonObject *body = nullptr;
    ObJsonObject *config = nullptr;
    ASSERT_EQ(OB_SUCCESS, embedding.get_body(allocator, model, input_array, config, body));

    ObJsonNode *model_node = body->get_value("model");
    ObStringBuffer model_buf(&allocator);
    model_node->print(model_buf, 0);
    ASSERT_EQ(model, model_buf.string());

    ObJsonNode *input_node = body->get_value("input");
    ObJsonArray *input_array_node = static_cast<ObJsonArray *>(input_node);
    ASSERT_EQ(1, input_array_node->element_count());
    ObJsonNode *input_node2 = input_array_node->get_value(0);
    ObStringBuffer input_buf(&allocator);
    input_node2->print(input_buf, 0);
    ASSERT_EQ(input, input_buf.string());
}

TEST_F(ObOpenAIUtilsTest, test_embedding_parse_output)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString content("[0.0023064255, -0.009327292, -0.0028842222]");
    ObString response(
        "{"
            "\"data\": ["
                "{"
                    "\"embedding\": [0.0023064255, -0.009327292, -0.0028842222],"
                    "\"index\": 0,"
                    "\"object\": \"embedding\""
                "}"
            "],"
            "\"model\":\"text-embedding-v4\","
            "\"object\":\"list\","
            "\"usage\":{\"prompt_tokens\":23,\"total_tokens\":23},"
            "\"id\":\"f62c2ae7-0906-9758-ab34-47c5764f07e2\""
        "}"
    );
    ObOpenAIUtils::ObOpenAIEmbed embedding;
    ObIJsonBase *j_base = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, response, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base));
    ObJsonObject *http_response = static_cast<ObJsonObject *>(j_base);
    ObIJsonBase *result = nullptr;
    ASSERT_EQ(OB_SUCCESS, embedding.parse_output(allocator, http_response, result));

    ObJsonArray *embeddings_array = static_cast<ObJsonArray *>(result);
    ASSERT_EQ(1, embeddings_array->element_count());

    ObJsonNode *embedding_node = embeddings_array->get_value(0);
    ObJsonArray *embedding_array = static_cast<ObJsonArray *>(embedding_node);
    ObStringBuffer embedding_buf(&allocator);
    embedding_array->print(embedding_buf, 0);
    ASSERT_EQ(content, embedding_buf.string());
}

TEST_F(ObOpenAIUtilsTest, test_embedding_parse_output_empty)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString content("[0.0023064255, -0.009327292, -0.0028842222]");
    ObString response(
        "{"
            "\"data\": ["
                "{"
                    "\"index\": 0,"
                    "\"object\": \"embedding\""
                "}"
            "],"
            "\"model\":\"text-embedding-v4\","
            "\"object\":\"list\","
            "\"usage\":{\"prompt_tokens\":23,\"total_tokens\":23},"
            "\"id\":\"f62c2ae7-0906-9758-ab34-47c5764f07e2\""
        "}"
    );
    ObOpenAIUtils::ObOpenAIEmbed embedding;
    ObIJsonBase *j_base = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, response, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base));
    ObJsonObject *http_response = static_cast<ObJsonObject *>(j_base);
    ObIJsonBase *result = nullptr;
    ASSERT_EQ(OB_ERR_UNEXPECTED, embedding.parse_output(allocator, http_response, result));
}


int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
