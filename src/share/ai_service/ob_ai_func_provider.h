/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_AI_FUNC_PROVIDER_H_
#define OB_AI_FUNC_PROVIDER_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/ob_allocator.h"
#include "share/ai_service/ob_ai_exec_struct.h"

namespace oceanbase
{
namespace common
{
class ObJsonObject;
class ObJsonArray;
class ObIJsonBase;

struct ObAiBatchFileConstraints
{
  static constexpr int64_t MAX_LINES_PER_FILE = 50000;
  static constexpr int64_t MAX_FILE_SIZE_BYTES = 100 * 1024 * 1024;
  static constexpr int64_t MAX_LINE_SIZE_BYTES = 6 * 1024 * 1024;
  static constexpr int64_t MAX_CUSTOM_ID_LENGTH = 256;
};

struct ObAiBatchFileLine
{
public:
  ObAiBatchFileLine() { reset(); }
  ~ObAiBatchFileLine() = default;

  void reset()
  {
    custom_id_.reset();
    method_.reset();
    url_.reset();
    body_.reset();
    line_size_ = 0;
  }

  bool is_valid() const
  {
    return !custom_id_.empty() && !method_.empty() && !url_.empty();
  }

  int to_json(common::ObIAllocator &allocator, common::ObString &json_str) const;

  TO_STRING_KV(K_(custom_id), K_(method), K_(url), K_(line_size));

public:
  common::ObString custom_id_;
  common::ObString method_;
  common::ObString url_;
  common::ObString body_;
  int64_t line_size_;
};

class ObAIFuncBase
{
public:
  ObAIFuncBase() {}
  virtual ~ObAIFuncBase() {}
  virtual int get_header(common::ObIAllocator &allocator,
                         common::ObString &api_key,
                         ObArray<ObString> &headers) = 0;
  virtual int parse_output(common::ObIAllocator &allocator,
                           common::ObJsonObject *http_response,
                           common::ObIJsonBase *&result) = 0;
  virtual share::ObAiCommandType get_command_type() const = 0;
  virtual int get_batch_submit_spec(common::ObString &endpoint,
                                    common::ObString &completion_window) const
  { return OB_NOT_SUPPORTED; }

  // Codec encode: constructs one complete JSONL batch line from (model, index, input_text).
  // Default returns OB_NOT_SUPPORTED; embed providers override with their format.
  virtual int encode_batch_line(common::ObIAllocator &allocator,
                                const common::ObString &model,
                                int64_t index,
                                const common::ObString &input_text,
                                ObAiBatchFileLine &line)
  { return OB_NOT_SUPPORTED; }

  // Codec decode: parses raw response_body and fills row completely.
  // Pure virtual — all concrete providers must implement (non-embed ones return OB_NOT_SUPPORTED).
  virtual int decode_result(common::ObIAllocator &allocator,
                            const common::ObString &response_body,
                            share::ObAiResultRow &row) = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncBase);
};

class ObAIFuncIComplete : public ObAIFuncBase
{
public:
  ObAIFuncIComplete() {}
  virtual ~ObAIFuncIComplete() {}
  virtual int get_body(common::ObIAllocator &allocator,
                       common::ObString &model,
                       common::ObString &prompt,
                       common::ObString &content,
                       common::ObJsonObject *config,
                       common::ObJsonObject *&body) = 0;
  virtual int set_config_json_format(common::ObIAllocator &allocator, common::ObJsonObject *config) = 0;
  virtual share::ObAiCommandType get_command_type() const override { return share::OB_AI_COMMAND_COMPLETE; }
  virtual int decode_result(common::ObIAllocator &allocator,
                            const common::ObString &response_body,
                            share::ObAiResultRow &row) override
  { return OB_NOT_SUPPORTED; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncIComplete);
};

class ObAIFuncIVLComplete : public ObAIFuncBase
{
public:
  ObAIFuncIVLComplete() {}
  virtual ~ObAIFuncIVLComplete() {}
  virtual int get_body(common::ObIAllocator &allocator,
                       common::ObString &model,
                       common::ObString &prompt,
                       common::ObJsonObject *prompt_object,
                       common::ObJsonObject *config,
                       common::ObJsonObject *&body) = 0;
  virtual share::ObAiCommandType get_command_type() const override { return share::OB_AI_COMMAND_COMPLETE; }
  virtual int decode_result(common::ObIAllocator &allocator,
                            const common::ObString &response_body,
                            share::ObAiResultRow &row) override
  { return OB_NOT_SUPPORTED; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncIVLComplete);
};

class ObAIFuncIEmbed : public ObAIFuncBase
{
public:
  ObAIFuncIEmbed() {}
  virtual ~ObAIFuncIEmbed() {}
  virtual int get_body(common::ObIAllocator &allocator,
                       common::ObString &model,
                       common::ObArray<ObString> &contents,
                       common::ObJsonObject *config,
                       common::ObString input_type,  // "text" or "image"
                       common::ObJsonObject *&body) = 0;
  virtual int get_body(common::ObIAllocator &allocator,
                       common::ObString &model,
                       common::ObArray<ObString> &contents,
                       common::ObJsonObject *config,
                       common::ObArray<ObString> &input_type_array,  // input type per content, e.g. text/image
                       common::ObJsonObject *&body) = 0;
  virtual share::ObAiCommandType get_command_type() const override { return share::OB_AI_COMMAND_EMBED; }
  virtual int decode_result(common::ObIAllocator &allocator,
                            const common::ObString &response_body,
                            share::ObAiResultRow &row) override
  { return OB_NOT_SUPPORTED; }
  // Path appended to the registered base URL for real-time embedding requests.
  // Providers that require the user to register the full endpoint URL should return "".
  virtual const char *get_embed_path() const { return "/embeddings"; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncIEmbed);
};

class ObAIFuncIRerank : public ObAIFuncBase
{
public:
  ObAIFuncIRerank() {}
  virtual ~ObAIFuncIRerank() {}
  virtual int get_body(common::ObIAllocator &allocator,
                       common::ObString &model,
                       common::ObString &query,
                       common::ObJsonArray *document_array,
                       common::ObJsonObject *config,
                       common::ObJsonObject *&body) = 0;
  virtual share::ObAiCommandType get_command_type() const override { return share::OB_AI_COMMAND_RERANK; }
  virtual int decode_result(common::ObIAllocator &allocator,
                            const common::ObString &response_body,
                            share::ObAiResultRow &row) override
  { return OB_NOT_SUPPORTED; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncIRerank);
};

class ObAIFuncHandle
{
public:
  ObAIFuncHandle() {}
  virtual ~ObAIFuncHandle() {}
  virtual int send_post(common::ObIAllocator &allocator,
                        const ObString &url,
                        ObArray<ObString> &headers,
                        ObJsonObject *data,
                        ObJsonObject *&response) = 0;
  virtual int send_post_batch(common::ObIAllocator &allocator,
                              const ObString &url,
                              ObArray<ObString> &headers,
                              ObArray<ObJsonObject *> &data_array,
                              ObArray<ObJsonObject *> &responses) = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncHandle);
};

} // namespace common
} // namespace oceanbase

#endif /* OB_AI_FUNC_PROVIDER_H_ */
