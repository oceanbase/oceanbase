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

class ObSiliconflowUtilsTest: public ::testing::Test
{
public:
    ObSiliconflowUtilsTest();
    virtual ~ObSiliconflowUtilsTest();
    virtual void SetUp();
    virtual void TearDown();
private:
    // disallow copy
    ObSiliconflowUtilsTest(const ObSiliconflowUtilsTest &other);
    ObSiliconflowUtilsTest& operator=(const ObSiliconflowUtilsTest &other);
protected:
    // data members
};

ObSiliconflowUtilsTest::ObSiliconflowUtilsTest()
{
}

ObSiliconflowUtilsTest::~ObSiliconflowUtilsTest()
{
}

void ObSiliconflowUtilsTest::SetUp()
{
}

void ObSiliconflowUtilsTest::TearDown()
{
}

TEST_F(ObSiliconflowUtilsTest, test_get_header)
{
    ObArray<ObString> headers;
    ObString api_key("sk-1234567890");
    ObString authorization("Authorization: Bearer sk-1234567890");
    ObString content_type("Content-Type: application/json");
    ObArenaAllocator allocator(ObModIds::TEST);
    ASSERT_EQ(OB_SUCCESS, ObSiliconflowUtils::get_header(allocator, api_key, headers));
    ASSERT_EQ(2, headers.count());
    ASSERT_EQ(authorization, headers[0]);
    ASSERT_EQ(content_type, headers[1]);
}


TEST_F(ObSiliconflowUtilsTest, test_get_body)
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
    ObSiliconflowUtils::ObSiliconflowRerank rerank;
    ASSERT_EQ(OB_SUCCESS, rerank.get_body(allocator, model, query, document_array, &config, body));
    ObJsonNode *model_node = body->get_value("model");
    ObStringBuffer model_buf(&allocator);
    model_node->print(model_buf, 0);
    ASSERT_EQ(model, model_buf.string());

    ObJsonNode *query_node = body->get_value("query");
    ObStringBuffer query_buf(&allocator);
    query_node->print(query_buf, 0);
    ASSERT_EQ(query, query_buf.string());

    ObJsonNode *documents_node = body->get_value("documents");
    ObStringBuffer documents_buf(&allocator);
    documents_node->print(documents_buf, 0);
    // cout << "documents: " << documents_buf.string().ptr() << endl;
    ASSERT_EQ(documents, documents_buf.string());
}

TEST_F(ObSiliconflowUtilsTest, test_parse_output)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    /*{
  "id": "<string>",
  "results": [
    {
      "document": {
        "text": "<string>"
      },
      "index": 123,
      "relevance_score": 123
    }
  ],
  "tokens": {
    "input_tokens": 123,
    "output_tokens": 123
  }
}*/
    //将上面的json字符串以合理的形式组织近response的参数里。
    ObString response(
        "{"
        "\"id\": 0,"
        "\"results\": ["
            "{"
                "\"document\": {\"text\": \"文本排序模型广泛用于搜索引擎和推荐系统中，它们根据文本相关性对候选文本进行排序\"},"
                "\"index\": 0,"
                "\"relevance_score\": 0.7314485774089865"
           "}"
        "],"
        "\"tokens\": {"
            "\"input_tokens\": 123,"
            "\"output_tokens\": 123"
        "}"
    "}"
    );
    ObString idx("0");
    ObString relevance_score("0.7314485774089865");
    ObString document("{\"text\": \"文本排序模型广泛用于搜索引擎和推荐系统中，它们根据文本相关性对候选文本进行排序\"}");
    ObSiliconflowUtils::ObSiliconflowRerank rerank;
    ObIJsonBase *j_base = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, response, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base));
    ObJsonObject *http_response = static_cast<ObJsonObject *>(j_base);
    ObIJsonBase *result = nullptr;

    ASSERT_EQ(OB_SUCCESS, rerank.parse_output(allocator, http_response, result));
    ObJsonArray *results_array = static_cast<ObJsonArray *>(result);
    ASSERT_EQ(1, results_array->element_count());
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

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
