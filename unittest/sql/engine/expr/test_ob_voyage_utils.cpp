/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "ob_expr_test_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_voyage_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObVoyageUtilsTest : public ::testing::Test
{
public:
  ObVoyageUtilsTest() : allocator_(ObModIds::TEST) {}
  virtual ~ObVoyageUtilsTest() {}

  ObJsonObject *parse_object(const char *text)
  {
    ObIJsonBase *j_base = nullptr;
    EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator_, ObString(text), ObJsonInType::JSON_TREE,
                                                           ObJsonInType::JSON_TREE, j_base));
    return static_cast<ObJsonObject *>(j_base);
  }
  ObJsonObject *dimension_config(int64_t dimension)
  {
    ObJsonObject *config = nullptr;
    ObJsonInt *dim = nullptr;
    EXPECT_EQ(OB_SUCCESS, ObAIFuncJsonUtils::get_json_object(allocator_, config));
    EXPECT_EQ(OB_SUCCESS, ObAIFuncJsonUtils::get_json_int(allocator_, dimension, dim));
    EXPECT_EQ(OB_SUCCESS, config->add("dimensions", dim));
    return config;
  }
  ObString print(ObJsonNode *node)
  {
    ObStringBuffer buf(&allocator_);
    EXPECT_EQ(OB_SUCCESS, node->print(buf, 1));
    return buf.string();
  }
  int embed_body(const char *model, const char *content, const char *type, ObJsonObject *config, ObJsonObject *&body)
  {
    ObVoyageUtils::ObVoyageEmbed embed;
    ObString model_str(model);
    ObArray<ObString> contents;
    EXPECT_EQ(OB_SUCCESS, contents.push_back(ObString(content)));
    return embed.get_body(allocator_, model_str, contents, config, ObString(type), body);
  }

protected:
  ObArenaAllocator allocator_;
};

TEST_F(ObVoyageUtilsTest, test_get_header)
{
  ObArray<ObString> headers;
  ObString api_key("pa-1234567890");
  ObVoyageUtils::ObVoyageEmbed embed;
  ASSERT_EQ(OB_SUCCESS, embed.get_header(allocator_, api_key, headers));
  ASSERT_EQ(2, headers.count());
  ASSERT_EQ(ObString("Authorization: Bearer pa-1234567890"), headers[0]);
  ASSERT_EQ(ObString("Content-Type: application/json"), headers[1]);
}

// There is no default model: an empty, unknown, other-provider or wrong-kind model is an error.
TEST_F(ObVoyageUtilsTest, test_model_is_required_and_explicit)
{
  ObJsonObject *body = nullptr;
  ASSERT_EQ(OB_INVALID_ARGUMENT, embed_body("", "hello", "text", nullptr, body));
  ASSERT_EQ(OB_NOT_SUPPORTED, embed_body("voyage-9000", "hello", "text", nullptr, body));
  ASSERT_EQ(OB_NOT_SUPPORTED, embed_body("text-embedding-3-small", "hello", "text", nullptr, body));
  // a rerank model is not an embedding model
  ASSERT_EQ(OB_NOT_SUPPORTED, embed_body("rerank-3", "hello", "text", nullptr, body));
  // a model Voyage serves but that is not in the supported list is rejected as well
  ASSERT_EQ(OB_NOT_SUPPORTED, embed_body("voyage-3", "hello", "text", nullptr, body));
  ASSERT_EQ(nullptr, body);

  ObVoyageUtils::ObVoyageRerank rerank;
  ObString query("q");
  ObJsonObject *rerank_body = nullptr;
  ObJsonObject *docs_obj = parse_object("{\"d\": [\"a\"]}");
  ObJsonArray *docs = static_cast<ObJsonArray *>(docs_obj->get_value("d"));
  ObString empty_model;
  ObString embed_model("voyage-4");
  ObString other_provider_model("gte-rerank-v2");
  ASSERT_EQ(OB_INVALID_ARGUMENT, rerank.get_body(allocator_, empty_model, query, docs, nullptr, rerank_body));
  ASSERT_EQ(OB_NOT_SUPPORTED, rerank.get_body(allocator_, embed_model, query, docs, nullptr, rerank_body));
  ASSERT_EQ(OB_NOT_SUPPORTED, rerank.get_body(allocator_, other_provider_model, query, docs, nullptr, rerank_body));
}

TEST_F(ObVoyageUtilsTest, test_text_body)
{
  ObJsonObject *body = nullptr;
  ASSERT_EQ(OB_SUCCESS, embed_body("voyage-4-lite", "hello", "text", nullptr, body));
  ASSERT_EQ(ObString("\"voyage-4-lite\""), print(body->get_value("model")));
  ASSERT_EQ(ObString("[\"hello\"]"), print(body->get_value("input")));
  // nothing is sent that was not asked for
  ASSERT_EQ(nullptr, body->get_value("output_dimension"));
  ASSERT_EQ(nullptr, body->get_value("input_type"));
  ASSERT_EQ(nullptr, body->get_value("enable_auto_chunking"));

  ASSERT_EQ(OB_SUCCESS, embed_body("voyage-4-lite", "hello", "text", dimension_config(512), body));
  ASSERT_EQ(ObString("512"), print(body->get_value("output_dimension")));
}

TEST_F(ObVoyageUtilsTest, test_output_dimension_values)
{
  ObJsonObject *body = nullptr;
  // Matryoshka models: 256, 512, 1024, 2048
  const int64_t valid[] = {256, 512, 1024, 2048};
  for (int64_t i = 0; i < 4; ++i) {
    ASSERT_EQ(OB_SUCCESS, embed_body("voyage-3.5", "hello", "text", dimension_config(valid[i]), body));
  }
  ASSERT_EQ(OB_INVALID_ARGUMENT, embed_body("voyage-3.5", "hello", "text", dimension_config(300), body));
  ASSERT_EQ(OB_INVALID_ARGUMENT, embed_body("voyage-3.5", "hello", "text", dimension_config(4096), body));
  // fixed dimension models
  ASSERT_EQ(OB_SUCCESS, embed_body("voyage-law-2", "hello", "text", dimension_config(1024), body));
  ASSERT_EQ(OB_INVALID_ARGUMENT, embed_body("voyage-law-2", "hello", "text", dimension_config(256), body));
  ASSERT_EQ(OB_INVALID_ARGUMENT, embed_body("voyage-finance-2", "hello", "text", dimension_config(512), body));
  ASSERT_EQ(OB_INVALID_ARGUMENT, embed_body("voyage-multimodal-3", "hello", "text", dimension_config(256), body));
  ASSERT_EQ(OB_SUCCESS, embed_body("voyage-multimodal-3.5", "hello", "text", dimension_config(256), body));
  // dimension must be an integer
  ObJsonObject *config = parse_object("{\"dimensions\": \"256\"}");
  ASSERT_EQ(OB_INVALID_ARGUMENT, embed_body("voyage-4", "hello", "text", config, body));
}

TEST_F(ObVoyageUtilsTest, test_text_model_rejects_image)
{
  ObJsonObject *body = nullptr;
  ASSERT_EQ(OB_NOT_SUPPORTED, embed_body("voyage-4", "https://example.com/a.png", "image", nullptr, body));
}

TEST_F(ObVoyageUtilsTest, test_input_limit)
{
  ObVoyageUtils::ObVoyageEmbed embed;
  ObString model("voyage-4-lite");
  ObArray<ObString> contents;
  for (int64_t i = 0; i < 1001; ++i) {
    ASSERT_EQ(OB_SUCCESS, contents.push_back(ObString("x")));
  }
  ObJsonObject *body = nullptr;
  ASSERT_EQ(OB_INVALID_ARGUMENT, embed.get_body(allocator_, model, contents, nullptr, ObString("text"), body));
  contents.reset();
  for (int64_t i = 0; i < 1000; ++i) {
    ASSERT_EQ(OB_SUCCESS, contents.push_back(ObString("x")));
  }
  ASSERT_EQ(OB_SUCCESS, embed.get_body(allocator_, model, contents, nullptr, ObString("text"), body));
}

// Auto chunking needs input_type "document" and one string per document (a flat list).
TEST_F(ObVoyageUtilsTest, test_contextualized_body)
{
  const char *models[] = {"voyage-context-4", "voyage-context-3"};
  for (int64_t i = 0; i < 2; ++i) {
    ObJsonObject *body = nullptr;
    ASSERT_EQ(OB_SUCCESS, embed_body(models[i], "a whole document", "text", dimension_config(256), body));
    ASSERT_EQ(ObString("[\"a whole document\"]"), print(body->get_value("inputs")));
    ASSERT_EQ(nullptr, body->get_value("input"));
    ASSERT_EQ(ObString("\"document\""), print(body->get_value("input_type")));
    ASSERT_EQ(ObString("true"), print(body->get_value("enable_auto_chunking")));
    ASSERT_EQ(ObString("32000"), print(body->get_value("chunk_size")));
    ASSERT_EQ(ObString("256"), print(body->get_value("output_dimension")));
  }
  ObJsonObject *body = nullptr;
  ASSERT_EQ(OB_NOT_SUPPORTED, embed_body("voyage-context-3", "https://example.com/a.png", "image", nullptr, body));
}

TEST_F(ObVoyageUtilsTest, test_multimodal_body)
{
  ObJsonObject *body = nullptr;
  ASSERT_EQ(OB_SUCCESS, embed_body("voyage-multimodal-3.5", "a cat", "text", nullptr, body));
  // the json tree keeps object keys sorted by length and then by value
  ASSERT_EQ(ObString("[{\"content\": [{\"text\": \"a cat\", \"type\": \"text\"}]}]"), print(body->get_value("inputs")));

  ASSERT_EQ(OB_SUCCESS, embed_body("voyage-multimodal-3", "https://example.com/a.png", "image", nullptr, body));
  ASSERT_EQ(ObString("[{\"content\": [{\"type\": \"image_url\", \"image_url\": \"https://example.com/a.png\"}]}]"),
            print(body->get_value("inputs")));

  ASSERT_EQ(OB_SUCCESS, embed_body("voyage-multimodal-3", "data:image/png;base64,AAAA", "image", nullptr, body));
  ASSERT_EQ(ObString("[{\"content\": [{\"type\": \"image_base64\", \"image_base64\": \"data:image/png;base64,AAAA\"}]}]"),
            print(body->get_value("inputs")));
}

TEST_F(ObVoyageUtilsTest, test_multimodal_mixed_input_types)
{
  ObVoyageUtils::ObVoyageEmbed embed;
  ObString model("voyage-multimodal-3.5");
  ObArray<ObString> contents;
  ObArray<ObString> types;
  ASSERT_EQ(OB_SUCCESS, contents.push_back(ObString("a cat")));
  ASSERT_EQ(OB_SUCCESS, types.push_back(ObString("text")));
  ASSERT_EQ(OB_SUCCESS, contents.push_back(ObString("https://example.com/a.png")));
  ASSERT_EQ(OB_SUCCESS, types.push_back(ObString("image")));
  ObJsonObject *body = nullptr;
  ASSERT_EQ(OB_SUCCESS, embed.get_body(allocator_, model, contents, nullptr, types, body));
  ASSERT_EQ(2, static_cast<ObJsonArray *>(body->get_value("inputs"))->element_count());
  // input types have to match the contents
  types.pop_back();
  ASSERT_EQ(OB_INVALID_ARGUMENT, embed.get_body(allocator_, model, contents, nullptr, types, body));
}

TEST_F(ObVoyageUtilsTest, test_parse_text_output_keeps_input_order)
{
  ObVoyageUtils::ObVoyageEmbed embed;
  ObJsonObject *response = parse_object(
      "{\"object\": \"list\", \"data\": ["
      "{\"object\": \"embedding\", \"embedding\": [0.3, 0.4], \"index\": 1},"
      "{\"object\": \"embedding\", \"embedding\": [0.1, 0.2], \"index\": 0}],"
      "\"model\": \"voyage-4\", \"usage\": {\"total_tokens\": 4}}");
  ObIJsonBase *result = nullptr;
  ASSERT_EQ(OB_SUCCESS, embed.parse_output(allocator_, response, result));
  ASSERT_EQ(2, result->element_count());
  ASSERT_EQ(ObString("[[0.1, 0.2], [0.3, 0.4]]"), print(static_cast<ObJsonNode *>(result)));
}

TEST_F(ObVoyageUtilsTest, test_parse_output_errors)
{
  ObVoyageUtils::ObVoyageEmbed embed;
  ObIJsonBase *result = nullptr;
  // error body of Voyage: {"detail": "..."}
  ASSERT_EQ(OB_INVALID_DATA, embed.parse_output(allocator_, parse_object("{\"detail\": \"bad request\"}"), result));
  ASSERT_EQ(OB_INVALID_ARGUMENT, embed.parse_output(allocator_, nullptr, result));
  // item without embedding
  ASSERT_EQ(OB_INVALID_DATA, embed.parse_output(allocator_, parse_object("{\"data\": [{\"index\": 0}]}"), result));
  // duplicated and out of range index
  ASSERT_EQ(OB_INVALID_DATA, embed.parse_output(allocator_,
      parse_object("{\"data\": [{\"embedding\": [1.0], \"index\": 0}, {\"embedding\": [2.0], \"index\": 0}]}"), result));
  ASSERT_EQ(OB_INVALID_DATA, embed.parse_output(allocator_,
      parse_object("{\"data\": [{\"embedding\": [1.0], \"index\": 5}]}"), result));
}

TEST_F(ObVoyageUtilsTest, test_parse_contextualized_output)
{
  ObVoyageUtils::ObVoyageEmbed embed;
  ObIJsonBase *result = nullptr;
  // one chunk per document: one input is one vector
  ObJsonObject *one_chunk = parse_object(
      "{\"object\": \"list\", \"data\": ["
      "{\"object\": \"list\", \"data\": [{\"object\": \"embedding\", \"embedding\": [0.1, 0.2], \"index\": 0, \"text\": \"doc a\"}], \"index\": 0},"
      "{\"object\": \"list\", \"data\": [{\"object\": \"embedding\", \"embedding\": [0.3, 0.4], \"index\": 0, \"text\": \"doc b\"}], \"index\": 1}],"
      "\"model\": \"voyage-context-3\", \"usage\": {\"total_tokens\": 8}, \"chunker_version\": \"1.0.0\"}");
  ASSERT_EQ(OB_SUCCESS, embed.parse_output(allocator_, one_chunk, result));
  ASSERT_EQ(ObString("[[0.1, 0.2], [0.3, 0.4]]"), print(static_cast<ObJsonNode *>(result)));

  // more than one chunk for a document is an error, not a silent drop
  ObJsonObject *two_chunks = parse_object(
      "{\"data\": [{\"object\": \"list\", \"index\": 0, \"data\": ["
      "{\"embedding\": [0.1], \"index\": 0, \"text\": \"first\"}, {\"embedding\": [0.2], \"index\": 1, \"text\": \"second\"}]}]}");
  ASSERT_EQ(OB_INVALID_ARGUMENT, embed.parse_output(allocator_, two_chunks, result));
  // no chunk at all
  ASSERT_EQ(OB_INVALID_DATA, embed.parse_output(allocator_,
      parse_object("{\"data\": [{\"object\": \"list\", \"index\": 0, \"data\": []}]}"), result));
}

TEST_F(ObVoyageUtilsTest, test_rerank_body)
{
  ObVoyageUtils::ObVoyageRerank rerank;
  ObJsonObject *docs_obj = parse_object("{\"d\": [\"oceanbase is a distributed database\", \"happy birthday\"]}");
  ObJsonArray *docs = static_cast<ObJsonArray *>(docs_obj->get_value("d"));
  ObString query("what is oceanbase?");
  const char *models[] = {"rerank-3", "rerank-3-lite", "rerank-2.5", "rerank-2.5-lite"};
  for (int64_t i = 0; i < 4; ++i) {
    ObString model(models[i]);
    ObJsonObject *body = nullptr;
    ASSERT_EQ(OB_SUCCESS, rerank.get_body(allocator_, model, query, docs, nullptr, body));
    ASSERT_EQ(print(docs_obj->get_value("d")), print(body->get_value("documents")));
    ASSERT_EQ(ObString("\"what is oceanbase?\""), print(body->get_value("query")));
    ASSERT_EQ(nullptr, body->get_value("top_k"));
  }
  ObString model("rerank-3");
  ObJsonObject *body = nullptr;
  ObJsonObject *config = parse_object("{\"top_k\": 1}");
  ASSERT_EQ(OB_SUCCESS, rerank.get_body(allocator_, model, query, docs, config, body));
  ASSERT_EQ(ObString("1"), print(body->get_value("top_k")));
  ASSERT_EQ(OB_INVALID_ARGUMENT, rerank.get_body(allocator_, model, query, docs, parse_object("{\"top_k\": 0}"), body));
  ASSERT_EQ(OB_INVALID_ARGUMENT, rerank.get_body(allocator_, model, query, docs, parse_object("{\"top_k\": \"1\"}"), body));
}

TEST_F(ObVoyageUtilsTest, test_rerank_parse_output)
{
  ObVoyageUtils::ObVoyageRerank rerank;
  ObIJsonBase *result = nullptr;
  ObJsonObject *response = parse_object(
      "{\"object\": \"list\", \"data\": [{\"relevance_score\": 0.45, \"index\": 0}],"
      "\"model\": \"rerank-3\", \"usage\": {\"total_tokens\": 9}}");
  ASSERT_EQ(OB_SUCCESS, rerank.parse_output(allocator_, response, result));
  ASSERT_EQ(1, result->element_count());
  ASSERT_EQ(OB_INVALID_DATA, rerank.parse_output(allocator_, parse_object("{\"detail\": \"bad request\"}"), result));
  // items the rerank expression cannot read
  const char *bad_items[] = {
    "{\"data\": [1]}",
    "{\"data\": [{\"relevance_score\": 0.45}]}",
    "{\"data\": [{\"index\": 0}]}",
    "{\"data\": [{\"index\": \"0\", \"relevance_score\": 0.45}]}",
    "{\"data\": [{\"index\": -1, \"relevance_score\": 0.45}]}",
    "{\"data\": [{\"index\": 0, \"relevance_score\": \"high\"}]}",
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(bad_items); ++i) {
    ASSERT_EQ(OB_INVALID_DATA, rerank.parse_output(allocator_, parse_object(bad_items[i]), result)) << bad_items[i];
  }
}

// A batch of rows must stay below the documented request token limit when every row is as long as
// the context window of the model: 1M tokens for the *-lite models, 320K for voyage-4 and
// voyage-3.5, 120K for the other models (the voyage-code-4 limit is not documented: one row).
TEST_F(ObVoyageUtilsTest, test_batch_size_stays_below_request_limit)
{
  struct Limit { const char *model; int64_t context; int64_t request_tokens; };
  const Limit limits[] = {
    {"voyage-4-large", 32000, 120000}, {"voyage-4", 32000, 320000}, {"voyage-4-lite", 32000, 1000000},
    {"voyage-3-large", 32000, 120000}, {"voyage-3.5", 32000, 320000}, {"voyage-3.5-lite", 32000, 1000000},
    {"voyage-code-3", 32000, 120000}, {"voyage-finance-2", 32000, 120000}, {"voyage-law-2", 16000, 120000},
    {"voyage-context-4", 32000, 120000}, {"voyage-context-3", 32000, 120000},
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(limits); ++i) {
    const ObVoyageUtils::ModelSpec *spec = ObVoyageUtils::find_model(ObString(limits[i].model));
    ASSERT_NE(nullptr, spec) << limits[i].model;
    ASSERT_GT(spec->batch_size_, 0) << limits[i].model;
    ASSERT_LE(spec->batch_size_, 1000) << limits[i].model;
    ASSERT_LE(spec->batch_size_ * limits[i].context, limits[i].request_tokens) << limits[i].model;
  }
  ASSERT_EQ(0, ObVoyageUtils::find_model(ObString("voyage-code-4"))->batch_size_);
  // multimodal rows are sent one per request: URL and base64 images cannot share a request
  ASSERT_EQ(1, ObVoyageUtils::find_model(ObString("voyage-multimodal-3"))->batch_size_);
  ASSERT_EQ(1, ObVoyageUtils::find_model(ObString("voyage-multimodal-3.5"))->batch_size_);
}

TEST_F(ObVoyageUtilsTest, test_provider_dispatch)
{
  ObAIFuncIEmbed *embed = nullptr;
  ObAIFuncIRerank *rerank = nullptr;
  ObString provider("voyageai");
  ASSERT_EQ(OB_SUCCESS, ObAIFuncUtils::get_embed_provider(allocator_, provider, embed));
  ASSERT_NE(nullptr, embed);
  ASSERT_EQ(OB_SUCCESS, ObAIFuncUtils::get_rerank_provider(allocator_, provider, rerank));
  ASSERT_NE(nullptr, rerank);
  // the multimodal flag of the other providers does not change the Voyage provider
  embed = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObAIFuncUtils::get_embed_provider(allocator_, provider, embed, true));
  ASSERT_NE(nullptr, embed);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
