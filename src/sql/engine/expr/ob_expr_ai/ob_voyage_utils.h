/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_VOYAGE_UTILS_H_
#define OB_VOYAGE_UTILS_H_

#include "ob_ai_func.h"

namespace oceanbase
{
namespace common
{

// Voyage AI (https://docs.voyageai.com) request builders and response parsers.
//
// Models are listed explicitly. A model that is not in the table is rejected, there is no
// default model and no silent fallback. The kind of the model decides the request shape:
//   KIND_TEXT        POST /v1/embeddings                  {"model", "input": [str]}
//   KIND_CONTEXT     POST /v1/contextualizedembeddings    {"model", "inputs": [str], auto chunking}
//   KIND_MULTIMODAL  POST /v1/multimodalembeddings        {"model", "inputs": [{"content": [..]}]}
//   KIND_RERANK      POST /v1/rerank                      {"model", "query", "documents"}
// The endpoint URL of an AI model endpoint has to be the Voyage URL that matches the model kind.
class ObVoyageUtils
{
public:
  enum ModelKind
  {
    KIND_TEXT = 0,
    KIND_CONTEXT,
    KIND_MULTIMODAL,
    KIND_RERANK
  };

  struct ModelSpec
  {
    const char *name_;
    ModelKind kind_;
    // True when output_dimension may be 256, 512, 1024 or 2048 (Matryoshka). Otherwise the
    // only accepted dimension is DEFAULT_DIMENSION.
    bool supports_output_dimension_;
    // Rows of one ai_embed request batch. Voyage limits the tokens of one request (1M for the
    // *-lite models, 320K for voyage-4 and voyage-3.5, 120K for the others) and every input is
    // cut to the context length of the model, so batch size = request limit / context length
    // keeps a request below the limit without counting tokens. 0 = no documented request
    // limit, the model is not batched.
    int64_t batch_size_;
  };

  static constexpr int64_t DEFAULT_DIMENSION = 1024;
  static constexpr int64_t CONTEXT_CHUNK_SIZE = 32000;
  // Voyage accepts images up to 20 MB.
  static constexpr int64_t MAX_IMAGE_SIZE = 20 * 1024 * 1024;

  // Case-insensitive lookup in the model table. Returns nullptr for an unknown model.
  static const ModelSpec *find_model(const ObString &model);
  static int64_t get_model_count();
  static const ModelSpec &get_model(const int64_t idx);
  static int check_output_dimension(const ModelSpec &spec, int64_t dimension);
  static int get_header(common::ObIAllocator &allocator,
                        common::ObString &api_key,
                        common::ObArray<ObString> &headers);

  class ObVoyageEmbed : public ObAIFuncIEmbed
  {
  public:
    ObVoyageEmbed() {}
    virtual ~ObVoyageEmbed() {}
    virtual int get_header(common::ObIAllocator &allocator,
                           common::ObString &api_key,
                           common::ObArray<ObString> &headers) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObArray<ObString> &contents,
                         common::ObJsonObject *config,
                         common::ObString input_type,
                         common::ObJsonObject *&body) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObArray<ObString> &contents,
                         common::ObJsonObject *config,
                         common::ObArray<ObString> &input_type_array,
                         common::ObJsonObject *&body) override;
    // Result is an array with exactly one embedding per input. A contextualized response that
    // holds more than one chunk for a document is an error: one input is one vector.
    virtual int parse_output(common::ObIAllocator &allocator,
                             common::ObJsonObject *http_response,
                             common::ObIJsonBase *&result) override;
  private:
    int get_dimension(const ObVoyageUtils::ModelSpec &spec, common::ObJsonObject *config, int64_t &dimension);
    int add_output_dimension(common::ObIAllocator &allocator, int64_t dimension, common::ObJsonObject *body);
    DISALLOW_COPY_AND_ASSIGN(ObVoyageEmbed);
  };

  class ObVoyageRerank : public ObAIFuncIRerank
  {
  public:
    ObVoyageRerank() {}
    virtual ~ObVoyageRerank() {}
    virtual int get_header(common::ObIAllocator &allocator,
                           common::ObString &api_key,
                           common::ObArray<ObString> &headers) override;
    virtual int get_body(common::ObIAllocator &allocator,
                         common::ObString &model,
                         common::ObString &query,
                         common::ObJsonArray *document_array,
                         common::ObJsonObject *config,
                         common::ObJsonObject *&body) override;
    // Result is the "data" array: [{"index", "relevance_score"}, ...].
    virtual int parse_output(common::ObIAllocator &allocator,
                             common::ObJsonObject *http_response,
                             common::ObIJsonBase *&result) override;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObVoyageRerank);
  };

private:
  DISALLOW_COPY_AND_ASSIGN(ObVoyageUtils);
};

} // namespace common
} // namespace oceanbase

#endif // OB_VOYAGE_UTILS_H_
