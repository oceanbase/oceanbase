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

class ObDashscopeUtilsTest: public ::testing::Test
{
public:
    ObDashscopeUtilsTest();
    virtual ~ObDashscopeUtilsTest();
    virtual void SetUp();
    virtual void TearDown();
private:
    // disallow copy
    ObDashscopeUtilsTest(const ObDashscopeUtilsTest &other);
    ObDashscopeUtilsTest& operator=(const ObDashscopeUtilsTest &other);
protected:
    // data members
};

ObDashscopeUtilsTest::ObDashscopeUtilsTest()
{
}

ObDashscopeUtilsTest::~ObDashscopeUtilsTest()
{
}

void ObDashscopeUtilsTest::SetUp()
{
}

void ObDashscopeUtilsTest::TearDown()
{
}

TEST_F(ObDashscopeUtilsTest, test_rerank_get_header)
{
    ObArray<ObString> headers;
    ObString api_key("sk-1234567890");
    ObString authorization("Authorization: Bearer sk-1234567890");
    ObString content_type("Content-Type: application/json");
    ObArenaAllocator allocator(ObModIds::TEST);
    ObDashscopeUtils::ObDashscopeRerank rerank;
    ASSERT_EQ(OB_SUCCESS, rerank.get_header(allocator, api_key, headers));
    ASSERT_EQ(2, headers.count());
    ASSERT_EQ(authorization, headers[0]);
    ASSERT_EQ(content_type, headers[1]);
}

TEST_F(ObDashscopeUtilsTest, test_rerank_get_body)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString model("gte-rerank-v2");
    ObString query("what is oceanbase?");
    ObString documents("[\"oceanbase is a distributed database\", \"nice to meet you\", \"happy birthday\"]");
    ObJsonArray *document_array = nullptr;
    ObIJsonBase *j_base = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, documents, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base));
    document_array = static_cast<ObJsonArray *>(j_base);
    ObJsonObject config(&allocator);
    ObJsonObject *body = nullptr;
    ObDashscopeUtils::ObDashscopeRerank rerank;
    ASSERT_EQ(OB_SUCCESS, rerank.get_body(allocator, model, query, document_array, &config, body));
    ObJsonNode *model_node = body->get_value("model");
    ObStringBuffer model_buf(&allocator);
    model_node->print(model_buf, 0);
    ASSERT_EQ(model, model_buf.string());

    ObJsonNode *input_node = body->get_value("input");
    ObJsonObject *input_obj = static_cast<ObJsonObject *>(input_node);

    ObJsonNode *query_node = input_obj->get_value("query");
    ObStringBuffer query_buf(&allocator);
    query_node->print(query_buf, 0);
    ASSERT_EQ(query, query_buf.string());

    // cout << "query: " << query_buf.string() << endl;
    ObJsonNode *documents_node = input_obj->get_value("documents");
    ObStringBuffer documents_buf(&allocator);
    documents_node->print(documents_buf, 0);
    // cout << "documents: " << documents_buf.string().ptr() << endl;
    ASSERT_EQ(documents, documents_buf.string());
}

TEST_F(ObDashscopeUtilsTest, test_rerank_parse_output)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString response(
        "{"
        "\"status_code\": 200,"
        "\"request_id\": \"9676afe6-fa1a-9895-bf00-b8376333062a\","
        "\"code\": \"\","
        "\"message\": \"\","
        "\"output\": {"
        "     \"results\": ["
        "        {"
        "            \"index\": 0,"
        "            \"relevance_score\": 0.7314485774089865,"
        "            \"document\": {"
        "                \"text\": \"文本排序模型广泛用于搜索引擎和推荐系统中，它们根据文本相关性对候选文本进行排序\""
        "            }"
        "        },"
        "        {"
        "            \"index\": 2,"
        "            \"relevance_score\": 0.5831720487049298,"
        "            \"document\": {"
        "                \"text\": \"预训练语言模型的发展给文本排序模型带来了新的进展\""
        "            }"
        "        },"
        "        {"
        "            \"index\": 1,"
        "            \"relevance_score\": 0.04973238644524712,"
        "            \"document\": {"
        "                \"text\": \"量子计算是计算科学的一个前沿领域\""
        "            }"
        "        }"
        "    ]"
        "},"
        "\"usage\": {"
        "    \"total_tokens\": 79 "
        "}"
        "}"
    );
    ObString idx("0");
    ObString relevance_score("0.7314485774089865");
    ObString document("{\"text\": \"文本排序模型广泛用于搜索引擎和推荐系统中，它们根据文本相关性对候选文本进行排序\"}");
    ObDashscopeUtils::ObDashscopeRerank rerank;
    ObIJsonBase *j_base = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, response, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base));
    ObJsonObject *http_response = static_cast<ObJsonObject *>(j_base);
    ObIJsonBase *result = nullptr;
    ASSERT_EQ(OB_SUCCESS, rerank.parse_output(allocator, http_response, result));
    ObJsonArray *results_array = static_cast<ObJsonArray *>(result);
    ASSERT_EQ(3, results_array->element_count());
    ObJsonNode* result_node = results_array->get_value(0);
    ObJsonObject *result_obj = static_cast<ObJsonObject *>(result_node);
    ObJsonNode *index_node = result_obj->get_value("index");
    ObStringBuffer index_buf(&allocator);
    ASSERT_EQ(OB_SUCCESS, index_node->print(index_buf, 0));
    ASSERT_EQ(idx, index_buf.string());
    ObJsonNode *relevance_score_node = result_obj->get_value("relevance_score");
    ObStringBuffer relevance_score_buf(&allocator);
    ASSERT_EQ(OB_SUCCESS, relevance_score_node->print(relevance_score_buf, 0));
    ASSERT_EQ(relevance_score, relevance_score_buf.string());
    ObJsonNode *document_node = result_obj->get_value("document");
    ObStringBuffer document_buf(&allocator);
    ASSERT_EQ(OB_SUCCESS, document_node->print(document_buf, 0));
    ASSERT_EQ(document, document_buf.string());
}

TEST_F(ObDashscopeUtilsTest, test_complete_get_header)
{
    ObArray<ObString> headers;
    ObString api_key("sk-1234567890");
    ObString authorization("Authorization: Bearer sk-1234567890");
    ObString content_type("Content-Type: application/json");
    ObDashscopeUtils::ObDashscopeComplete complete;
    ObArenaAllocator allocator(ObModIds::TEST);
    ASSERT_EQ(OB_SUCCESS, complete.get_header(allocator, api_key, headers));
    ASSERT_EQ(2, headers.count());
    ASSERT_EQ(authorization, headers[0]);
    ASSERT_EQ(content_type, headers[1]);
}

TEST_F(ObDashscopeUtilsTest, test_complete_get_body)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString model("qwen2.5-coder-32b");
    ObString prompt;
    ObString content("what is oceanbase?");
    ObJsonObject config(&allocator);
    ObJsonObject *body = nullptr;
    ObDashscopeUtils::ObDashscopeComplete complete;
    ASSERT_EQ(OB_SUCCESS, complete.get_body(allocator, model, prompt, content, &config, body));
    ObJsonNode *model_node = body->get_value("model");
    ObStringBuffer model_buf(&allocator);
    model_node->print(model_buf, 0);
    ASSERT_EQ(model, model_buf.string());

    ObJsonNode *input_node = body->get_value("input");
    ObJsonObject *input_obj = static_cast<ObJsonObject *>(input_node);

    ObJsonNode *messages_node = input_obj->get_value("messages");
    ObJsonArray *messages_array = static_cast<ObJsonArray *>(messages_node);
    ASSERT_EQ(1, messages_array->element_count());
    ObJsonNode *message_node = messages_array->get_value(0);
    ObJsonObject *message_obj = static_cast<ObJsonObject *>(message_node);
    ObJsonNode *content_node = message_obj->get_value("content");
    ObStringBuffer content_buf(&allocator);
    ASSERT_EQ(OB_SUCCESS, content_node->print(content_buf, 0));
    ASSERT_EQ(content, content_buf.string());
}

TEST_F(ObDashscopeUtilsTest, test_complete_parse_output)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString response(
        "{"
        "\"status_code\": 200,"
        "\"request_id\": \"902fee3b-f7f0-9a8c-96a1-6b4ea25af114\","
        "\"code\": \"\","
        "\"message\": \"\","
        "\"output\": {"
        "    \"text\": null,"
        "    \"finish_reason\": null,"
        "    \"choices\": ["
        "        {"
        "            \"finish_reason\": \"stop\","
        "            \"message\": {"
        "                \"role\": \"assistant\","
        "                \"content\": \"我是阿里云开发的一款超大规模语言模型，我叫通义千问。\""
        "            }"
        "        }"
        "    ]"
        "},"
        "\"usage\": {"
        "    \"input_tokens\": 22,"
        "    \"output_tokens\": 17,"
        "    \"total_tokens\": 39"
        "}"
        "}"
    );
    ObString text("我是阿里云开发的一款超大规模语言模型，我叫通义千问。");
    ObDashscopeUtils::ObDashscopeComplete complete;
    ObIJsonBase *j_base = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, response, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base));
    ObJsonObject *http_response = static_cast<ObJsonObject *>(j_base);
    ObIJsonBase *result = nullptr;
    ASSERT_EQ(OB_SUCCESS, complete.parse_output(allocator, http_response, result));
    ObJsonBuffer j_buf(&allocator);
    ASSERT_EQ(OB_SUCCESS, result->print(j_buf, 0));
    ASSERT_EQ(text, j_buf.string());
}

TEST_F(ObDashscopeUtilsTest, test_embed_get_header)
{
    ObArray<ObString> headers;
    ObString api_key("sk-1234567890");
    ObString authorization("Authorization: Bearer sk-1234567890");
    ObString content_type("Content-Type: application/json");
    ObDashscopeUtils::ObDashscopeEmbed embed;
    ObArenaAllocator allocator(ObModIds::TEST);
    ASSERT_EQ(OB_SUCCESS, embed.get_header(allocator, api_key, headers));
    ASSERT_EQ(2, headers.count());
    ASSERT_EQ(authorization, headers[0]);
    ASSERT_EQ(content_type, headers[1]);
}

TEST_F(ObDashscopeUtilsTest, test_embed_get_body)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString model("bge-m3");
    ObString content("what is oceanbase?");
    ObArray<ObString> contents;
    contents.push_back(content);
    ObJsonObject config(&allocator);
    ObJsonObject *body = nullptr;
    ObDashscopeUtils::ObDashscopeEmbed embed;
    ASSERT_EQ(OB_SUCCESS, embed.get_body(allocator, model, contents, &config, body));
    ObJsonNode *model_node = body->get_value("model");
    ObStringBuffer model_buf(&allocator);
    model_node->print(model_buf, 0);
    ASSERT_EQ(model, model_buf.string());

    ObJsonNode *input_node = body->get_value("input");
    ObJsonObject *input_obj = static_cast<ObJsonObject *>(input_node);
    ObJsonNode *texts_node = input_obj->get_value("texts");
    ObJsonArray *texts_array = static_cast<ObJsonArray *>(texts_node);
    ASSERT_EQ(1, texts_array->element_count());
    ObJsonNode *text_node = texts_array->get_value(0);
    ObStringBuffer text_buf(&allocator);
    ASSERT_EQ(OB_SUCCESS, text_node->print(text_buf, 0));
    ASSERT_EQ(content, text_buf.string());
}

TEST_F(ObDashscopeUtilsTest, test_embed_parse_output)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString response(
        "{"
        "\"status_code\": 200,"
        "\"request_id\": \"902fee3b-f7f0-9a8c-96a1-6b4ea25af114\","
        "\"code\": 123,"
        "\"message\": \"null\","
        "\"output\": {"
        "    \"embeddings\": ["
        "        {"
        "            \"embedding\": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]"
        "        }"
        "    ]"
        " }"
        "}"
    );
    ObString embedding("[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]");
    ObDashscopeUtils::ObDashscopeEmbed embed;
    ObIJsonBase *j_base = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, response, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base));
    ObJsonObject *http_response = static_cast<ObJsonObject *>(j_base);
    ObIJsonBase *result = nullptr;
    ASSERT_EQ(OB_SUCCESS, embed.parse_output(allocator, http_response, result));
    ObJsonArray *embeddings_array = static_cast<ObJsonArray *>(result);
    ASSERT_EQ(1, embeddings_array->element_count());
    ObJsonNode *embedding_node = embeddings_array->get_value(0);
    ObJsonArray *embedding_array = static_cast<ObJsonArray *>(embedding_node);
    ObJsonBuffer embedding_buf(&allocator);
    ASSERT_EQ(OB_SUCCESS, embedding_array->print(embedding_buf, 0));
    ASSERT_EQ(embedding, embedding_buf.string());
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
