/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_AI_SERVICE_OB_AI_SERVICE_STRUCT_H_
#define OCEANBASE_SHARE_AI_SERVICE_OB_AI_SERVICE_STRUCT_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_service_name_proxy.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_tree.h"
// Do NOT include ob_schema_struct.h: forward declaration only to avoid circular dependency.

namespace oceanbase { namespace share { struct ObAiGatewayCircuitState; } }

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

  int deep_copy(common::ObIAllocator &allocator, const ObAiModelEndpointInfo &other);

  const ObString &get_name() const { return name_; }
  const ObString &get_scope() const { return scope_; }
  const ObString &get_url() const { return url_; }
  ObString get_batch_file_url() const;
  const ObString &get_encrypted_access_key() const { return access_key_; }
  int get_unencrypted_access_key(common::ObIAllocator &allocator, ObString &unencrypted_access_key) const;
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
  ObAIModelConfigInfo() : gw_state_(nullptr) { reset(); }
  ~ObAIModelConfigInfo() { release_gw_state_(); }

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
    is_gateway_route_ = false;
    release_gw_state_();
    op_type_ = EndpointType::MAX_TYPE;
  }

  int init(ObIAllocator &allocator, const schema::ObAiModelSchema &ai_model_schema, const ObAiModelEndpointInfo &endpoint_info);
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
  void init_gateway_route(const ObString &model_key,
                          ObAiGatewayCircuitState *gw_state,
                          EndpointType::TYPE op_type)
  {
    is_gateway_route_ = true;
    gw_state_ = gw_state;
    op_type_ = op_type;
    model_type_ = op_type;
    model_key_ = model_key;
  }
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
  bool is_gateway_route() const { return is_gateway_route_; }
  ObAiGatewayCircuitState *get_gw_state() const { return gw_state_; }
  EndpointType::TYPE get_op_type() const { return op_type_; }

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
               K_(max_concurrency),
               K_(is_gateway_route),
               K_(op_type),
               KP_(gw_state));

private:
  ObString model_key_;
  ObString model_name_;
  EndpointType::TYPE model_type_;
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
  bool is_gateway_route_;
  ObAiGatewayCircuitState *gw_state_; // owned: released in dtor/reset
  EndpointType::TYPE op_type_;

  // Releases gw_state_ ref (out-of-line: dec_ref_and_release needs the full type).
  void release_gw_state_();
  DISALLOW_COPY_AND_ASSIGN(ObAIModelConfigInfo);
};

inline bool is_supported_ai_provider_protocol(const common::ObString &protocol)
{
  return 0 == protocol.case_compare("openai")
      || 0 == protocol.case_compare("dashscope")
      || 0 == protocol.case_compare("cohere");
}

const int64_t OB_MAX_AI_GATEWAY_NAME_LENGTH = 256;
const int64_t OB_MAX_AI_GATEWAY_ENDPOINT_NAME_LENGTH = 256;

struct ObAiGatewayEndpoint
{
  ObString endpoint_name_;
  ObString provider_;          // parsed from model (part before '/')
  ObString model_name_;        // parsed from model (part after '/')
  ObString model_;             // original "provider/model_name" string
  int64_t weight_;             // routing weight (default 0)

  ObAiGatewayEndpoint() : weight_(0) {}
  void reset()
  {
    endpoint_name_.reset();
    provider_.reset();
    model_name_.reset();
    model_.reset();
    weight_ = 0;
  }
  TO_STRING_KV(K_(endpoint_name), K_(provider), K_(model_name), K_(model), K_(weight));
};

struct ObAiCircuitBreakerParams
{
  static constexpr int64_t MAX_ENDPOINTS_PER_GATEWAY = 5;
  static constexpr int64_t DEFAULT_FAILURE_RATE_THRESHOLD = 50;
  static constexpr int64_t DEFAULT_WINDOW_SIZE_SECONDS = 60;
  static constexpr int64_t DEFAULT_MINIMUM_REQUESTS = 10;
  static constexpr int64_t DEFAULT_BREAK_DURATION_SECONDS = 60;
  static constexpr int64_t DEFAULT_PROBE_REQUESTS = 3;
  static constexpr int64_t MAX_WINDOW_SIZE_SECONDS = 300;
  static constexpr int64_t MIN_FAILURE_RATE_THRESHOLD = 1;
  static constexpr int64_t MAX_FAILURE_RATE_THRESHOLD = 100;
  static constexpr int64_t MIN_WINDOW_SIZE_SECONDS = 1;
  static constexpr int64_t MIN_MINIMUM_REQUESTS = 1;
  static constexpr int64_t MAX_MINIMUM_REQUESTS = 1000000;
  static constexpr int64_t MIN_BREAK_DURATION_SECONDS = 1;
  // Cap break_duration so break_duration_seconds_ * 1000000L (us) cannot overflow int64.
  static constexpr int64_t MAX_BREAK_DURATION_SECONDS = 30L * 24 * 3600;
  static constexpr int64_t MIN_PROBE_REQUESTS = 1;
  static constexpr int64_t MAX_PROBE_REQUESTS = 10000;
  static constexpr int64_t US_PER_SECOND = 1000000L;
  static constexpr int64_t FAILURE_RATE_PERCENT_BASE = 100;

  int64_t failure_rate_threshold_;
  int64_t window_size_seconds_;
  int64_t minimum_requests_;
  int64_t break_duration_seconds_;
  int64_t probe_requests_;

  ObAiCircuitBreakerParams() { set_defaults(); }
  void set_defaults()
  {
    failure_rate_threshold_ = DEFAULT_FAILURE_RATE_THRESHOLD;
    window_size_seconds_ = DEFAULT_WINDOW_SIZE_SECONDS;
    minimum_requests_ = DEFAULT_MINIMUM_REQUESTS;
    break_duration_seconds_ = DEFAULT_BREAK_DURATION_SECONDS;
    probe_requests_ = DEFAULT_PROBE_REQUESTS;
  }
  static int validate_ranges(int64_t failure_rate_threshold,
                             int64_t window_size_seconds,
                             int64_t minimum_requests,
                             int64_t break_duration_seconds,
                             int64_t probe_requests);

  TO_STRING_KV(K_(failure_rate_threshold), K_(window_size_seconds), K_(minimum_requests),
               K_(break_duration_seconds), K_(probe_requests));
};

static inline bool is_ai_gateway_endpoint_error(int64_t http_code)
{
  return http_code == 429 || http_code == 500 || http_code == 502
      || http_code == 503 || http_code == 504;
}

int parse_gateway_endpoints_json(common::ObIAllocator &allocator,
                                 const common::ObString &json_str,
                                 common::ObIArray<ObAiGatewayEndpoint> &endpoints);

int parse_gateway_circuit_breaker_json(common::ObIAllocator &allocator,
                                       const common::ObString &json_str,
                                       ObAiCircuitBreakerParams &params);

// Field-level merge for ALTER: old_json as base, new_json overlays specified
// fields; unspecified fields keep their old values. Validates the merged result.
int merge_gateway_circuit_breaker_json(common::ObIAllocator &allocator,
                                       const common::ObString &old_json,
                                       const common::ObString &new_json,
                                       common::ObString &merged_json);

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AI_SERVICE_OB_AI_SERVICE_STRUCT_H_