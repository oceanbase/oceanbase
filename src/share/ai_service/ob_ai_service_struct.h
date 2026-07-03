/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_AI_SERVICE_STRUCT_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_AI_SERVICE_STRUCT_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "share/ob_service_name_proxy.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_tree.h"
// Note: Do NOT include ob_schema_struct.h here - only forward declaration is needed
// to avoid circular dependency through ob_object.h -> ObExecContext forward declaration

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObAiModelSchema;
class ObAIProviderSchema;
}

struct EndpointType final
{
  enum TYPE : uint8_t
  {
    INVALID_TYPE = 0,
    DENSE_EMBEDDING = 1,
    SPARSE_EMBEDDING = 2,
    COMPLETION = 3,
    RERANK = 4,
    // add new endpoint type before this line
    // also remember to add ENDPOINT_TYPE_STR
    MAX_TYPE
  };
  static EndpointType::TYPE str_to_endpoint_type(const ObString &type_str);
  static EndpointType::TYPE convert_type_from_int(const int64_t type)
  {

    EndpointType::TYPE endpoint_type = EndpointType::INVALID_TYPE;
    if (type >= static_cast<int64_t>(EndpointType::INVALID_TYPE) && type <= static_cast<int64_t>(EndpointType::MAX_TYPE)) {
      endpoint_type = static_cast<EndpointType::TYPE>(type);
    }
    return endpoint_type;
  }
private:
  static const char *ENDPOINT_TYPE_STR[];
};

class ObAiServiceModelInfo
{
  OB_UNIS_VERSION(1);
public:
  ObAiServiceModelInfo() { reset(); }
  ~ObAiServiceModelInfo() = default;

  void reset()
  {
    name_.reset();
    type_ = EndpointType::MAX_TYPE;
    model_name_.reset();
  }

  int parse_from_json_base(const ObString &name, const common::ObIJsonBase &params_jbase);
  int check_valid() const;

  const ObString &get_name() const { return name_; }
  EndpointType::TYPE get_type() const { return type_; }
  const ObString &get_model_name() const { return model_name_; }

  TO_STRING_KV(K_(name),
               K_(type),
               K_(model_name));
private:
  ObString name_;
  EndpointType::TYPE type_;
  ObString model_name_;
};


// ai service endpoint info from user side
class ObAiModelEndpointInfo
{
  friend class ObAiServiceProxy;
public:
  ObAiModelEndpointInfo() { reset(); }
  ~ObAiModelEndpointInfo() = default;

  void reset()
  {
    name_.reset();
    scope_ = DEFAULT_SCOPE;
    ai_model_name_.reset();
    url_.reset();
    access_key_.reset();
    provider_.reset();
    request_model_name_.reset();
    parameters_.reset();
    request_transform_fn_.reset();
    response_transform_fn_.reset();
    endpoint_id_ = OB_INVALID_ID;
  }

  int parse_from_json_base(common::ObArenaAllocator &allocator,const ObString &name, const common::ObIJsonBase &params_jbase);
  int merge_delta_endpoint(common::ObArenaAllocator &allocator, const ObIJsonBase &delta_endpoint);
  int check_valid() const;

  // Deep copy endpoint info to target
  int deep_copy(common::ObIAllocator &allocator, const ObAiModelEndpointInfo &other);

  const ObString &get_name() const { return name_; }
  const ObString &get_scope() const { return scope_; }
  const ObString &get_url() const { return url_; }
  // Derive base URL for BatchFile API by stripping known endpoint suffixes (e.g. "/embeddings").
  // Supports both full URL (original convention) and base URL conventions.
  ObString get_batch_file_url() const;
  const ObString &get_encrypted_access_key() const { return access_key_; }
  int get_unencrypted_access_key(common::ObIAllocator &allocator, ObString &unencrypted_access_key) const;
  // For decrypting access_key stored in __all_ai_model_provider (same format as endpoint table).
  int assign_storage_access_key_only(common::ObIAllocator &allocator, const ObString &encrypted_access_key_from_table);
  const ObString &get_ai_model_name() const { return ai_model_name_; }
  const ObString &get_request_model_name() const { return request_model_name_; }
  const ObString &get_provider() const { return provider_; }
  const ObString &get_parameters() const { return parameters_; }
  const ObString &get_request_transform_fn() const { return request_transform_fn_; }
  const ObString &get_response_transform_fn() const { return response_transform_fn_; }
  uint64_t get_endpoint_id() const { return endpoint_id_; }
  void set_endpoint_id(uint64_t endpoint_id) { endpoint_id_ = endpoint_id; }
  static int encrypt_access_key_to_storage_format(common::ObIAllocator &allocator,
                                                  const common::ObString &plain_access_key,
                                                  common::ObString &storage_access_key);

  TO_STRING_KV(K_(name),
               K_(scope),
               K_(endpoint_id),
               K_(url),
               K_(access_key),
               K_(ai_model_name),
               K_(provider),
               K_(request_model_name),
               K_(parameters),
               K_(request_transform_fn),
               K_(response_transform_fn));
private:
  static const ObString DEFAULT_SCOPE;
  static bool is_valid_provider(const ObString &provider);
  int encrypt_access_key_(common::ObIAllocator &allocator, const ObString &access_key, ObString &encrypted_access_key);
  int decrypt_access_key_(common::ObIAllocator &allocator, const ObString &encrypted_access_key, ObString &unencrypted_access_key) const;
  int encrypt_access_key_no_tde_(common::ObIAllocator &allocator, const ObString &access_key, ObString &encrypted_access_key);
  int decrypt_access_key_no_tde_(common::ObIAllocator &allocator, const ObString &encrypted_access_key, ObString &unencrypted_access_key) const;
private:
  uint64_t endpoint_id_;
  ObString name_;
  ObString scope_;
  ObString ai_model_name_;
  ObString url_;
  ObString access_key_;
  ObString provider_;
  ObString request_model_name_;
  ObString parameters_;
  ObString request_transform_fn_;
  ObString response_transform_fn_;
};

struct ObAIModelConfigItem
{
public:
  static const int64_t DEFAULT_MIN_CONCURRENCY = 10;
  static const int64_t DEFAULT_MAX_CONCURRENCY = 100;

  ObAIModelConfigItem()
      : batch_size_(0),
        max_image_size_(0),
        min_concurrency_(DEFAULT_MIN_CONCURRENCY),
        max_concurrency_(DEFAULT_MAX_CONCURRENCY) {}
  ~ObAIModelConfigItem() = default;
public:
  int64_t batch_size_;
  int64_t max_image_size_;
  int64_t min_concurrency_;
  int64_t max_concurrency_;
  TO_STRING_KV(K_(batch_size), K_(max_image_size), K_(min_concurrency), K_(max_concurrency));
};


struct ObAIModelConfigInfo
{
public:
  ObAIModelConfigInfo() { reset(); }
  ~ObAIModelConfigInfo() = default;

  void reset()
  {
    model_key_.reset();
    model_name_.reset();
    model_type_ = EndpointType::MAX_TYPE;
    provider_.reset();
    url_.reset();
    api_key_.reset();
    request_model_name_.reset();
    provider_base_url_.reset();
    message_parameters_ = nullptr;
    batch_size_ = 0;
    max_image_size_ = 0;
    min_concurrency_ = ObAIModelConfigItem::DEFAULT_MIN_CONCURRENCY;
    max_concurrency_ = ObAIModelConfigItem::DEFAULT_MAX_CONCURRENCY;
  }

  int init(ObIAllocator &allocator, const schema::ObAiModelSchema &ai_model_schema, const ObAiModelEndpointInfo &endpoint_info);
  // New syntax: provider/model from __all_ai_model_provider + optional __all_ai_model_profile (caller supplies full HTTP URL).
  int init_from_inline_provider_model(common::ObIAllocator &allocator,
                                      const common::ObString &inline_model_key,
                                      const schema::ObAIProviderSchema &provider_schema,
                                      const common::ObString &request_model_name,
                                      const EndpointType::TYPE model_type,
                                      const common::ObString &dispatch_provider_tag,
                                      const common::ObString &full_service_url);
  int merge_default_config(ObIAllocator &allocator, const ObAIModelConfigItem &default_config);
  int apply_profile_params(ObIAllocator &allocator,
                           const ObString &model_config,
                           const ObString &run_config);
  const ObString &get_model_key() const { return model_key_; }
  const ObString &get_model_name() const { return model_name_; }
  EndpointType::TYPE get_model_type() const { return model_type_; }
  const ObString &get_provider() const { return provider_; }
  const ObString &get_url() const { return url_; }
  const ObString &get_api_key() const { return api_key_; }
  const ObString &get_provider_base_url() const { return provider_base_url_; }
  const ObString &get_request_model_name() const { return request_model_name_; }
  common::ObJsonObject* get_message_parameters() const { return message_parameters_; }
  int64_t get_batch_size() const { return batch_size_; }
  int64_t get_max_image_size() const { return max_image_size_; }
  int64_t get_min_concurrency() const { return min_concurrency_; }
  int64_t get_max_concurrency() const { return max_concurrency_; }

  TO_STRING_KV(K_(model_key),
               K_(model_name),
               K_(model_type),
               K_(provider),
               K_(url),
               K_(provider_base_url),
               K_(request_model_name),
               K_(message_parameters),
               K_(batch_size),
               K_(max_image_size),
               K_(min_concurrency),
               K_(max_concurrency));

private:
  ObString model_key_;
  // ai service model info
  ObString model_name_;
  EndpointType::TYPE model_type_;
  // endpoint info
  ObString provider_;
  ObString url_;
  ObString api_key_;
  ObString request_model_name_;
  ObString provider_base_url_; // only set for inline provider/model; empty for DDL-created models
  common::ObJsonObject* message_parameters_;
  int64_t batch_size_;
  int64_t max_image_size_;
  int64_t min_concurrency_;
  int64_t max_concurrency_;
};

inline bool is_supported_ai_provider_protocol(const common::ObString &protocol)
{
  return 0 == protocol.case_compare("openai")
      || 0 == protocol.case_compare("dashscope")
      || 0 == protocol.case_compare("cohere");
}

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_AI_SERVICE_STRUCT_H_