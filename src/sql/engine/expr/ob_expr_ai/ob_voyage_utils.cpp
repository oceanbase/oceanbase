/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_voyage_utils.h"
#include "ob_ai_func_utils.h"

namespace oceanbase
{
namespace common
{

namespace
{
const int64_t VOYAGE_MAX_INPUTS_PER_REQUEST = 1000;

// Names are matched case-insensitively. batch_size_ is explained in ob_voyage_utils.h.
const ObVoyageUtils::ModelSpec VOYAGE_MODELS[] = {
  // text embedding, 32K context
  {"voyage-4-large",  ObVoyageUtils::KIND_TEXT, true, 3},
  {"voyage-4",        ObVoyageUtils::KIND_TEXT, true, 10},
  {"voyage-4-lite",   ObVoyageUtils::KIND_TEXT, true, 31},
  // no documented request token limit: one row per request
  {"voyage-code-4",   ObVoyageUtils::KIND_TEXT, true, 0},
  {"voyage-3-large",  ObVoyageUtils::KIND_TEXT, true, 3},
  {"voyage-3.5",      ObVoyageUtils::KIND_TEXT, true, 10},
  {"voyage-3.5-lite", ObVoyageUtils::KIND_TEXT, true, 31},
  {"voyage-code-3",   ObVoyageUtils::KIND_TEXT, true, 3},
  {"voyage-finance-2", ObVoyageUtils::KIND_TEXT, false, 3},
  // 16K context: 120K / 16K
  {"voyage-law-2",    ObVoyageUtils::KIND_TEXT, false, 7},
  // contextualized embedding, 32K context per document, 120K tokens per request
  {"voyage-context-4", ObVoyageUtils::KIND_CONTEXT, true, 3},
  {"voyage-context-3", ObVoyageUtils::KIND_CONTEXT, true, 3},
  // multimodal embedding. One row per request: Voyage rejects URL and base64 images in one request.
  {"voyage-multimodal-3.5", ObVoyageUtils::KIND_MULTIMODAL, true, 1},
  {"voyage-multimodal-3",   ObVoyageUtils::KIND_MULTIMODAL, false, 1},
  // rerank
  {"rerank-3",        ObVoyageUtils::KIND_RERANK, false, 0},
  {"rerank-3-lite",   ObVoyageUtils::KIND_RERANK, false, 0},
  {"rerank-2.5",      ObVoyageUtils::KIND_RERANK, false, 0},
  {"rerank-2.5-lite", ObVoyageUtils::KIND_RERANK, false, 0},
};

int add_string(ObIAllocator &allocator, ObJsonObject *obj, const char *key, ObString value)
{
  int ret = OB_SUCCESS;
  ObJsonString *str_node = nullptr;
  if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, value, str_node))) {
    LOG_WARN("Failed to get json string", K(ret));
  } else if (OB_FAIL(obj->add(key, str_node))) {
    LOG_WARN("Failed to add string", K(ret), K(key));
  }
  return ret;
}

int add_bool(ObIAllocator &allocator, ObJsonObject *obj, const char *key, bool value)
{
  int ret = OB_SUCCESS;
  ObJsonBoolean *bool_node = nullptr;
  if (OB_FAIL(ObAIFuncJsonUtils::get_json_boolean(allocator, value, bool_node))) {
    LOG_WARN("Failed to get json boolean", K(ret));
  } else if (OB_FAIL(obj->add(key, bool_node))) {
    LOG_WARN("Failed to add boolean", K(ret), K(key));
  }
  return ret;
}

int add_int(ObIAllocator &allocator, ObJsonObject *obj, const char *key, int64_t value)
{
  int ret = OB_SUCCESS;
  ObJsonInt *int_node = nullptr;
  if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, value, int_node))) {
    LOG_WARN("Failed to get json int", K(ret));
  } else if (OB_FAIL(obj->add(key, int_node))) {
    LOG_WARN("Failed to add int", K(ret), K(key));
  }
  return ret;
}

bool is_url(const ObString &content)
{
  return content.prefix_match_ci("http://") || content.prefix_match_ci("https://");
}
} // namespace

const ObVoyageUtils::ModelSpec *ObVoyageUtils::find_model(const ObString &model)
{
  const ModelSpec *found = nullptr;
  for (int64_t i = 0; OB_ISNULL(found) && i < ARRAYSIZEOF(VOYAGE_MODELS); ++i) {
    if (model.case_compare(VOYAGE_MODELS[i].name_) == 0) {
      found = &VOYAGE_MODELS[i];
    }
  }
  return found;
}

int64_t ObVoyageUtils::get_model_count()
{
  return ARRAYSIZEOF(VOYAGE_MODELS);
}

const ObVoyageUtils::ModelSpec &ObVoyageUtils::get_model(const int64_t idx)
{
  return VOYAGE_MODELS[idx];
}

int ObVoyageUtils::check_output_dimension(const ModelSpec &spec, int64_t dimension)
{
  int ret = OB_SUCCESS;
  bool valid = false;
  if (spec.supports_output_dimension_) {
    valid = (dimension == 256 || dimension == 512 || dimension == 1024 || dimension == 2048);
  } else {
    valid = (dimension == DEFAULT_DIMENSION);
  }
  if (!valid) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dimension is not supported by the voyage model", K(ret), K(dimension), K(spec.name_));
    if (spec.supports_output_dimension_) {
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "voyage model dimension, supported values are 256, 512, 1024 and 2048");
    } else {
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "voyage model dimension, this model only supports 1024");
    }
  }
  return ret;
}

int ObVoyageUtils::get_header(common::ObIAllocator &allocator,
                              common::ObString &api_key,
                              common::ObArray<ObString> &headers)
{
  // Authorization: Bearer <key> and Content-Type: application/json, as for the OpenAI style APIs.
  return ObOpenAIUtils::get_header(allocator, api_key, headers);
}

int ObVoyageUtils::ObVoyageEmbed::get_header(common::ObIAllocator &allocator,
                                             common::ObString &api_key,
                                             common::ObArray<ObString> &headers)
{
  return ObVoyageUtils::get_header(allocator, api_key, headers);
}

int ObVoyageUtils::ObVoyageEmbed::get_dimension(const ModelSpec &spec, common::ObJsonObject *config, int64_t &dimension)
{
  int ret = OB_SUCCESS;
  dimension = 0;
  ObJsonNode *dimension_node = OB_NOT_NULL(config) ? config->get_value("dimensions") : nullptr;
  if (OB_ISNULL(dimension_node)) {
    // not requested: output_dimension is not sent
  } else if (dimension_node->json_type() != ObJsonNodeType::J_INT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dimension is not int", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "voyage model dimension, dimension must be an integer");
  } else {
    dimension = static_cast<ObJsonInt *>(dimension_node)->value();
    if (OB_FAIL(check_output_dimension(spec, dimension))) {
      LOG_WARN("invalid dimension", K(ret), K(dimension));
    }
  }
  return ret;
}

int ObVoyageUtils::ObVoyageEmbed::add_output_dimension(common::ObIAllocator &allocator,
                                                       int64_t dimension,
                                                       common::ObJsonObject *body)
{
  int ret = OB_SUCCESS;
  if (dimension > 0 && OB_FAIL(add_int(allocator, body, "output_dimension", dimension))) {
    LOG_WARN("Failed to add output_dimension", K(ret));
  }
  return ret;
}

int ObVoyageUtils::ObVoyageEmbed::get_body(common::ObIAllocator &allocator,
                                           common::ObString &model,
                                           common::ObArray<ObString> &contents,
                                           common::ObJsonObject *config,
                                           common::ObString input_type,
                                           common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> input_type_array;
  for (int64_t i = 0; OB_SUCC(ret) && i < contents.count(); ++i) {
    if (OB_FAIL(input_type_array.push_back(input_type))) {
      LOG_WARN("Failed to push back input type", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(get_body(allocator, model, contents, config, input_type_array, body))) {
    LOG_WARN("Failed to get body", K(ret));
  }
  return ret;
}

int ObVoyageUtils::ObVoyageEmbed::get_body(common::ObIAllocator &allocator,
                                           common::ObString &model,
                                           common::ObArray<ObString> &contents,
                                           common::ObJsonObject *config,
                                           common::ObArray<ObString> &input_type_array,
                                           common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  const ModelSpec *spec = find_model(model);
  int64_t dimension = 0;
  if (model.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("voyage model is not specified", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "voyage embedding, model is required");
  } else if (contents.empty() || input_type_array.count() != contents.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("contents is empty or does not match input types", K(ret), K(contents.count()),
             K(input_type_array.count()));
  } else if (contents.count() > VOYAGE_MAX_INPUTS_PER_REQUEST) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("too many inputs for one voyage request", K(ret), K(contents.count()));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "voyage embedding, at most 1000 inputs per request");
  } else if (OB_ISNULL(spec)
             || (spec->kind_ != KIND_TEXT && spec->kind_ != KIND_CONTEXT && spec->kind_ != KIND_MULTIMODAL)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not a voyage embedding model", K(ret), K(model));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "voyage embedding, unknown model");
  } else if (OB_FAIL(get_dimension(*spec, config, dimension))) {
    LOG_WARN("Failed to get dimension", K(ret));
  } else {
    ObJsonObject *body_obj = nullptr;
    ObJsonArray *input_array = nullptr;
    const bool is_multimodal = (spec->kind_ == KIND_MULTIMODAL);
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(add_string(allocator, body_obj, "model", model))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, input_array))) {
      LOG_WARN("Failed to get json array", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < contents.count(); ++i) {
      const bool is_image = (input_type_array.at(i).case_compare("image") == 0);
      ObString content = contents.at(i);
      if (is_image && !is_multimodal) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("voyage text embedding model does not support image input", K(ret), K(model));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "voyage model image embedding, use a voyage-multimodal model");
      } else if (!is_multimodal) {
        ObJsonString *str_node = nullptr;
        if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, content, str_node))) {
          LOG_WARN("Failed to get json string", K(ret));
        } else if (OB_FAIL(input_array->append(str_node))) {
          LOG_WARN("Failed to append input", K(ret));
        }
      } else {
        // {"content": [{"type": "text", "text": ".."}]} or
        // {"content": [{"type": "image_url", "image_url": ".."}]} or
        // {"content": [{"type": "image_base64", "image_base64": "data:image/png;base64,.."}]}
        ObJsonObject *item_obj = nullptr;
        ObJsonObject *element_obj = nullptr;
        ObJsonArray *content_array = nullptr;
        const char *type_name = "text";
        if (is_image) {
          type_name = is_url(content) ? "image_url" : "image_base64";
        }
        if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, item_obj))
            || OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, element_obj))
            || OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, content_array))) {
          LOG_WARN("Failed to allocate json", K(ret));
        } else if (OB_FAIL(add_string(allocator, element_obj, "type", ObString(type_name)))) {
          LOG_WARN("Failed to add type", K(ret));
        } else if (OB_FAIL(add_string(allocator, element_obj, type_name, content))) {
          LOG_WARN("Failed to add content", K(ret));
        } else if (OB_FAIL(content_array->append(element_obj))) {
          LOG_WARN("Failed to append content element", K(ret));
        } else if (OB_FAIL(item_obj->add("content", content_array))) {
          LOG_WARN("Failed to add content", K(ret));
        } else if (OB_FAIL(input_array->append(item_obj))) {
          LOG_WARN("Failed to append input", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(body_obj->add(spec->kind_ == KIND_TEXT ? "input" : "inputs", input_array))) {
      LOG_WARN("Failed to add input", K(ret));
    } else if (spec->kind_ == KIND_CONTEXT
               // Auto chunking works with input_type "document" and one string per document only.
               && (OB_FAIL(add_string(allocator, body_obj, "input_type", ObString("document")))
                   || OB_FAIL(add_bool(allocator, body_obj, "enable_auto_chunking", true))
                   || OB_FAIL(add_int(allocator, body_obj, "chunk_size", CONTEXT_CHUNK_SIZE)))) {
      LOG_WARN("Failed to add auto chunking parameters", K(ret));
    } else if (OB_FAIL(add_output_dimension(allocator, dimension, body_obj))) {
      LOG_WARN("Failed to add output dimension", K(ret));
    } else {
      body = body_obj;
    }
  }
  return ret;
}

int ObVoyageUtils::ObVoyageEmbed::parse_output(common::ObIAllocator &allocator,
                                               common::ObJsonObject *http_response,
                                               common::ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  ObJsonArray *data_array = nullptr;
  ObJsonArray *result_array = nullptr;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else if (OB_ISNULL(http_response->get_value("data"))
             || http_response->get_value("data")->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_INVALID_DATA;
    LOG_WARN("voyage response has no data array", K(ret));
  } else if (OB_FALSE_IT(data_array = static_cast<ObJsonArray *>(http_response->get_value("data")))) {
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, result_array))) {
    LOG_WARN("Failed to get json array", K(ret));
  }
  const int64_t count = OB_SUCC(ret) ? data_array->element_count() : 0;
  // Vectors are returned in input order: slot i holds the item with "index" i.
  ObArray<ObJsonNode *> slots;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_FAIL(slots.push_back(nullptr))) {
      LOG_WARN("Failed to push back slot", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObJsonNode *item_node = data_array->get_value(i);
    ObJsonObject *item = nullptr;
    ObJsonNode *embedding = nullptr;
    int64_t index = i;
    if (OB_ISNULL(item_node) || item_node->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_DATA;
      LOG_WARN("voyage embedding item is not an object", K(ret), K(i));
    } else if (OB_FALSE_IT(item = static_cast<ObJsonObject *>(item_node))) {
    } else if (OB_NOT_NULL(item->get_value("index"))) {
      if (item->get_value("index")->json_type() != ObJsonNodeType::J_INT) {
        ret = OB_INVALID_DATA;
        LOG_WARN("voyage embedding index is not an integer", K(ret), K(i));
      } else {
        index = static_cast<ObJsonInt *>(item->get_value("index"))->value();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(item->get_value("embedding"))) {
      embedding = item->get_value("embedding");
    } else if (OB_NOT_NULL(item->get_value("data"))
               && item->get_value("data")->json_type() == ObJsonNodeType::J_ARRAY) {
      // contextualized embeddings: one list of chunks per document
      ObJsonArray *chunks = static_cast<ObJsonArray *>(item->get_value("data"));
      ObJsonNode *chunk_node = nullptr;
      if (chunks->element_count() > 1) {
        // OB_INVALID_ARGUMENT keeps the message below, OB_INVALID_DATA is replaced by a dump of the response
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("voyage split a document into more than one chunk", K(ret), K(i), K(chunks->element_count()));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "voyage contextualized embedding, the input is too long for one vector");
      } else if (chunks->element_count() == 0) {
        ret = OB_INVALID_DATA;
        LOG_WARN("voyage returned no chunk for a document", K(ret), K(i));
      } else if (OB_ISNULL(chunk_node = chunks->get_value(0))
                 || chunk_node->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_DATA;
        LOG_WARN("voyage chunk is not an object", K(ret), K(i));
      } else {
        embedding = static_cast<ObJsonObject *>(chunk_node)->get_value("embedding");
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(embedding) || embedding->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_INVALID_DATA;
      LOG_WARN("voyage response item has no embedding", K(ret), K(i));
    } else if (index < 0 || index >= count || OB_NOT_NULL(slots.at(index))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("voyage embedding index is out of range or duplicated", K(ret), K(index), K(count));
    } else {
      slots.at(index) = embedding;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_FAIL(result_array->append(slots.at(i)))) {
      LOG_WARN("Failed to append embedding", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    result = result_array;
  }
  return ret;
}

int ObVoyageUtils::ObVoyageRerank::get_header(common::ObIAllocator &allocator,
                                              common::ObString &api_key,
                                              common::ObArray<ObString> &headers)
{
  return ObVoyageUtils::get_header(allocator, api_key, headers);
}

int ObVoyageUtils::ObVoyageRerank::get_body(common::ObIAllocator &allocator,
                                            common::ObString &model,
                                            common::ObString &query,
                                            common::ObJsonArray *document_array,
                                            common::ObJsonObject *config,
                                            common::ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  const ModelSpec *spec = find_model(model);
  if (model.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("voyage rerank model is not specified", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "voyage rerank, model is required");
  } else if (OB_ISNULL(spec) || spec->kind_ != KIND_RERANK) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not a voyage rerank model", K(ret), K(model));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "voyage rerank, unknown model");
  } else if (query.empty() || OB_ISNULL(document_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("query or document_array is empty", K(ret));
  } else if (document_array->element_count() > VOYAGE_MAX_INPUTS_PER_REQUEST) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("too many documents for one voyage rerank request", K(ret), K(document_array->element_count()));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "voyage rerank, at most 1000 documents per request");
  } else {
    ObJsonObject *body_obj = nullptr;
    ObJsonNode *top_k = OB_NOT_NULL(config) ? config->get_value("top_k") : nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, body_obj))) {
      LOG_WARN("Failed to get json object", K(ret));
    } else if (OB_FAIL(add_string(allocator, body_obj, "model", model))) {
      LOG_WARN("Failed to add model", K(ret));
    } else if (OB_FAIL(add_string(allocator, body_obj, "query", query))) {
      LOG_WARN("Failed to add query", K(ret));
    } else if (OB_FAIL(body_obj->add("documents", document_array))) {
      LOG_WARN("Failed to add documents", K(ret));
    } else if (OB_NOT_NULL(top_k)) {
      if (top_k->json_type() != ObJsonNodeType::J_INT || static_cast<ObJsonInt *>(top_k)->value() <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("top_k must be a positive integer", K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "voyage rerank, top_k must be a positive integer");
      } else if (OB_FAIL(add_int(allocator, body_obj, "top_k", static_cast<ObJsonInt *>(top_k)->value()))) {
        LOG_WARN("Failed to add top_k", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      body = body_obj;
    }
  }
  return ret;
}

int ObVoyageUtils::ObVoyageRerank::parse_output(common::ObIAllocator &allocator,
                                                common::ObJsonObject *http_response,
                                                common::ObIJsonBase *&result)
{
  int ret = OB_SUCCESS;
  ObJsonNode *data_node = nullptr;
  if (OB_ISNULL(http_response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("http_response is null", K(ret));
  } else if (OB_ISNULL(data_node = http_response->get_value("data"))
             || data_node->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_INVALID_DATA;
    LOG_WARN("voyage rerank response has no data array", K(ret));
  } else {
    // the rerank expression reads index and relevance_score without checking their types
    ObJsonArray *items = static_cast<ObJsonArray *>(data_node);
    for (int64_t i = 0; OB_SUCC(ret) && i < items->element_count(); ++i) {
      ObJsonNode *item = items->get_value(i);
      ObJsonNode *index = nullptr;
      ObJsonNode *score = nullptr;
      if (OB_ISNULL(item) || item->json_type() != ObJsonNodeType::J_OBJECT
          || OB_ISNULL(index = static_cast<ObJsonObject *>(item)->get_value("index"))
          || index->json_type() != ObJsonNodeType::J_INT || static_cast<ObJsonInt *>(index)->value() < 0
          || OB_ISNULL(score = static_cast<ObJsonObject *>(item)->get_value("relevance_score"))
          || score->json_type() != ObJsonNodeType::J_DOUBLE) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid voyage rerank result item", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      result = items;
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
