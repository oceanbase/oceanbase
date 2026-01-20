/**
* Copyright (c) 2023 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*/

#include <openssl/hmac.h>
#include <openssl/sha.h>
#include "share/catalog/rest/auth/ob_rest_auth_mgr.h"
#include "lib/json_type/ob_json_parse.h"
#include "observer/ob_server.h"
#include "share/catalog/rest/client/ob_curl_rest_client.h"

#define USING_LOG_PREFIX SHARE

namespace oceanbase
{
namespace share
{

ObRestOAuth2Key::ObRestOAuth2Key()
  : tenant_id_(OB_INVALID_TENANT_ID), catalog_id_(OB_INVALID_ID),
    accessid_(ObRestCatalogProperties::OB_MAX_ACCESSID_LENGTH, 0, accessid_ptr_),
    accesskey_(ObRestCatalogProperties::OB_MAX_ACCESSKEY_LENGTH, 0, accesskey_ptr_),
    scope_(ObRestCatalogProperties::OB_MAX_SCOPE_LENGTH, 0, scope_ptr_),
    oauth2_svr_uri_(OB_MAX_URI_LENGTH, 0, oauth2_svr_uri_ptr_)
{
}

ObRestOAuth2Key::ObRestOAuth2Key(const uint64_t tenant_id, const uint64_t catalog_id,
                                 const ObString &access_id, const ObString &access_key,
                                 const ObString &scope, const ObString &oauth2_svr_uri)
                : tenant_id_(tenant_id), catalog_id_(catalog_id),
                accessid_(ObRestCatalogProperties::OB_MAX_ACCESSID_LENGTH, 0, accessid_ptr_),
                accesskey_(ObRestCatalogProperties::OB_MAX_ACCESSKEY_LENGTH, 0, accesskey_ptr_),
                scope_(ObRestCatalogProperties::OB_MAX_SCOPE_LENGTH, 0, scope_ptr_),
                oauth2_svr_uri_(OB_MAX_URI_LENGTH, 0, oauth2_svr_uri_ptr_)
{
  MEMCPY(accessid_ptr_, access_id.ptr(), access_id.length());
  accessid_.set_length(access_id.length());
  MEMCPY(accesskey_ptr_, access_key.ptr(), access_key.length());
  accesskey_.set_length(access_key.length());
  MEMCPY(scope_ptr_, scope.ptr(), scope.length());
  scope_.set_length(scope.length());
  MEMCPY(oauth2_svr_uri_ptr_, oauth2_svr_uri.ptr(), oauth2_svr_uri.length());
  oauth2_svr_uri_.set_length(oauth2_svr_uri.length());
}

ObRestOAuth2Key::ObRestOAuth2Key(const ObRestOAuth2Key &other)
              : tenant_id_(other.tenant_id_), catalog_id_(other.catalog_id_),
                accessid_(ObRestCatalogProperties::OB_MAX_ACCESSID_LENGTH, 0, accessid_ptr_),
                accesskey_(ObRestCatalogProperties::OB_MAX_ACCESSKEY_LENGTH, 0, accesskey_ptr_),
                scope_(ObRestCatalogProperties::OB_MAX_SCOPE_LENGTH, 0, scope_ptr_),
                oauth2_svr_uri_(OB_MAX_URI_LENGTH, 0, oauth2_svr_uri_ptr_)
{
  MEMCPY(accessid_ptr_, other.accessid_ptr_, other.accessid_.length());
  accessid_.set_length(other.accessid_.length());
  MEMCPY(accesskey_ptr_, other.accesskey_ptr_, other.accesskey_.length());
  accesskey_.set_length(other.accesskey_.length());
  MEMCPY(scope_ptr_, other.scope_ptr_, other.scope_.length());
  scope_.set_length(other.scope_.length());
  MEMCPY(oauth2_svr_uri_ptr_, other.oauth2_svr_uri_ptr_, other.oauth2_svr_uri_.length());
  oauth2_svr_uri_.set_length(other.oauth2_svr_uri_.length());
}

ObRestOAuth2Key &ObRestOAuth2Key::operator=(const ObRestOAuth2Key &other)
{
  assign(other);
  return *this;
}

int ObRestOAuth2Key::assign(const ObRestOAuth2Key &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    catalog_id_ = other.catalog_id_;
    if (other.accessid_.length() > ObRestCatalogProperties::OB_MAX_ACCESSID_LENGTH
        || other.accesskey_.length() > ObRestCatalogProperties::OB_MAX_ACCESSKEY_LENGTH
        || other.scope_.length() > ObRestCatalogProperties::OB_MAX_SCOPE_LENGTH
        || other.oauth2_svr_uri_.length() > OB_MAX_URI_LENGTH) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid oauth2 key", K(ret), K(other.accessid_.length()), K(other.accesskey_.length()), K(other.scope_.length()), K(other.oauth2_svr_uri_.length()));
    } else {
      MEMCPY(accessid_ptr_, other.accessid_ptr_, other.accessid_.length());
      accessid_.set_length(other.accessid_.length());
      MEMCPY(accesskey_ptr_, other.accesskey_ptr_, other.accesskey_.length());
      accesskey_.set_length(other.accesskey_.length());
      MEMCPY(scope_ptr_, other.scope_ptr_, other.scope_.length());
      scope_.set_length(other.scope_.length());
      MEMCPY(oauth2_svr_uri_ptr_, other.oauth2_svr_uri_ptr_, other.oauth2_svr_uri_.length());
      oauth2_svr_uri_.set_length(other.oauth2_svr_uri_.length());
    }
  }
  return ret;
}

uint64_t ObRestOAuth2Key::hash() const
{
  uint64_t hash_value = 0;
  hash_value = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_value);
  hash_value = murmurhash(&catalog_id_, sizeof(catalog_id_), hash_value);
  hash_value = murmurhash(accessid_.ptr(), static_cast<int32_t>(accessid_.length()), hash_value);
  hash_value = murmurhash(accesskey_.ptr(), static_cast<int32_t>(accesskey_.length()), hash_value);
  hash_value = murmurhash(scope_.ptr(), static_cast<int32_t>(scope_.length()), hash_value);
  hash_value = murmurhash(oauth2_svr_uri_.ptr(), static_cast<int32_t>(oauth2_svr_uri_.length()), hash_value);
  return hash_value;
}

int ObRestOAuth2Key::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = hash();
  return ret;
}

bool ObRestOAuth2Key::operator==(const ObRestOAuth2Key &other) const
{
  return tenant_id_ == other.tenant_id_
         && catalog_id_ == other.catalog_id_
         && accessid_ == other.accessid_
         && accesskey_ == other.accesskey_
         && scope_ == other.scope_
         && oauth2_svr_uri_ == other.oauth2_svr_uri_;
}

bool ObRestOAuth2Key::is_valid() const
{
  return tenant_id_ != OB_INVALID_TENANT_ID
         && catalog_id_ != OB_INVALID_ID
         && !accessid_.empty()
         && !accesskey_.empty()  // scope can be empty
         && !oauth2_svr_uri_.empty();
}

ObRestAuthMgr::~ObRestAuthMgr()
{
  if (oauth2_credential_map_.created()) {
    oauth2_credential_map_.destroy();
  }
}

int ObRestAuthMgr::init()
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_MAP_BUCKET_CNT = 100;
  lib::ObMemAttr mem_attr(OB_SERVER_TENANT_ID, "oauth2_cre");
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRestAuthMgr already inited", K(ret));
  } else if (OB_FAIL(oauth2_credential_allocator_.init(nullptr,
                                                       OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                       mem_attr))) {
    LOG_WARN("fail to init oauth2_credential_allocator", K(ret));
  } else if (OB_FAIL(oauth2_credential_map_.create(DEFAULT_MAP_BUCKET_CNT,
                                            "OAuth2CredMap",
                                            "OAuth2CredMap"))) {
    LOG_WARN("fail to create oauth2_credential_map", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::ReqMemEvict, tg_id_))) {
    LOG_WARN("failed to create tg", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("failed to start tg", K(ret));
  } else {
    refresh_task_.auth_mgr_ = this;
    int64_t refresh_interval = GCONF.rest_oauth2_credential_refresh_interval;
    if (OB_FAIL(TG_SCHEDULE(tg_id_, refresh_task_, refresh_interval, true))) {
      LOG_WARN("failed to schedule refresh task", K(ret), K(refresh_interval));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

void ObRestAuthMgr::stop()
{
  if (is_inited_) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    TG_DESTROY(tg_id_);
    oauth2_credential_allocator_.reset();
    is_inited_ = false;
  }
}

int ObRestAuthMgr::authenticate(const uint64_t tenant_id,
                                const uint64_t catalog_id,
                                ObRestHttpRequest &request,
                                const ObRestCatalogProperties &rest_properties,
                                ObIAllocator &allocator,
                                bool force_refresh)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRestAuthMgr not inited", K(ret));
  } else {
    switch (rest_properties.auth_type_) {
      case ObRestAuthType::NONE_TYPE: {
        // do nothing
        break;
      }
      case ObRestAuthType::OAUTH2_TYPE: {
        // if token is not empty and not force refresh, use token directly
        if (!rest_properties.token_.empty() && !force_refresh) {
          if (OB_FAIL(request.add_header(AuthHeader, rest_properties.token_))) {
            LOG_WARN("fail to add authorization header", K(ret));
          }
        } else {
          ObRestOAuth2Key oauth2_key(tenant_id, catalog_id,
                                    rest_properties.accessid_, rest_properties.accesskey_,
                                    rest_properties.scope_, rest_properties.oauth2_svr_uri_);
          ObString auth_info;
          if (force_refresh) {
            // force refresh, curl directly
            if (OB_FAIL(curl_oauth2_credential(oauth2_key, true /*update_access_time*/))) {
              LOG_WARN("fail to curl oauth2 credential", K(ret), K(oauth2_key));
            } else if (OB_FAIL(get_oauth2_credential_from_map(oauth2_key, auth_info, allocator))) {
              LOG_WARN("fail to get oauth2 credential from map", K(ret), K(oauth2_key));
            }
          } else if (OB_FAIL(get_oauth2_credential_from_map(oauth2_key, auth_info, allocator))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;  // ignore ret
              if (OB_FAIL(curl_oauth2_credential(oauth2_key, true /*update_access_time*/))) {
                LOG_WARN("fail to curl oauth2 credential", K(ret), K(oauth2_key));
              } else if (OB_FAIL(get_oauth2_credential_from_map(oauth2_key, auth_info, allocator))) {
                LOG_WARN("fail to get oauth2 credential from map", K(ret), K(oauth2_key));
              }
            } else {
              LOG_WARN("fail to get oauth2 credential from map", K(ret), K(oauth2_key));
            }
          }

          if (OB_SUCC(ret) && OB_FAIL(request.add_header(AuthHeader, auth_info))) {
            LOG_WARN("fail to add authorization header", K(ret));
          }
        }
        break;
      }
      case ObRestAuthType::SIGV4_TYPE: {
        ObSqlString full_url;
        if (OB_FAIL(full_url.append_fmt("%.*s%.*s",
                                        rest_properties.uri_.length(),
                                        rest_properties.uri_.ptr(),
                                        request.get_url().length(),
                                        request.get_url().ptr()))) {
          LOG_WARN("fail to append full url", K(ret));
        } else if (OB_FAIL(get_sigv4_signature(request,
                                               full_url.string(),
                                               rest_properties,
                                               allocator))) {
          LOG_WARN("fail to generate sigv4 signature", K(ret));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid type", K(ret), K(rest_properties.auth_type_));
        break;
      }
    }
  }
  return ret;
}

// sigv4需要对arn进行二次编码
static int double_encode_uri_partially(const ObString &uri, ObIAllocator &allocator, ObString &result)
{
  int ret = OB_SUCCESS;
  result.reset();

  if (uri.empty()) {
    result = uri;
  } else {
    const char *arn_start = nullptr;
    const char *arn_end = nullptr;
    const char *uri_ptr = uri.ptr();
    int64_t uri_len = uri.length();

    for (int64_t i = 0; i < uri_len - 2; ++i) {
      if ((uri_ptr[i] == 'a' || uri_ptr[i] == 'A') &&
          (uri_ptr[i+1] == 'r' || uri_ptr[i+1] == 'R') &&
          (uri_ptr[i+2] == 'n' || uri_ptr[i+2] == 'N')) {
        if ((i + 3 < uri_len && uri_ptr[i+3] == ':') ||
            (i + 6 < uri_len && uri_ptr[i+3] == '%' &&
             uri_ptr[i+4] == '3' && (uri_ptr[i+5] == 'A' || uri_ptr[i+5] == 'a'))) {
          arn_start = uri_ptr + i;
          break;
        }
      }
    }

    if (arn_start != nullptr) {
      const char *current = arn_start;
      const char *next_slash = nullptr;
      // 查找下一个路径分隔符 '/'
      for (const char *p = current; p < uri_ptr + uri_len; ++p) {
        if (*p == '/') {
          next_slash = p;
          break;
        }
      }

      if (next_slash != nullptr) {
        arn_end = next_slash;
      } else {
        arn_end = uri_ptr + uri_len;
      }

      ObString prefix = ObString(arn_start - uri_ptr, uri_ptr);
      ObString arn_part = ObString(arn_end - arn_start, arn_start);
      ObString suffix = ObString((uri_ptr + uri_len) - arn_end, arn_end);

      char *double_encoded_arn = curl_easy_escape(NULL, arn_part.ptr(), arn_part.length());
      if (OB_ISNULL(double_encoded_arn)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to double encode ARN part", K(ret));
      } else {
        ObSqlString result_str;
        if (OB_FAIL(result_str.append_fmt("%.*s%s%.*s",
                                          static_cast<int>(prefix.length()), prefix.ptr(),
                                          double_encoded_arn,
                                          static_cast<int>(suffix.length()), suffix.ptr()))) {
          LOG_WARN("fail to build result URI", K(ret));
        } else if (OB_FAIL(ob_write_string(allocator, result_str.string(), result, true))) {
          LOG_WARN("fail to write result URI", K(ret));
        }
        curl_free(double_encoded_arn);
      }
    } else {
      char *double_encode = curl_easy_escape(NULL, uri.ptr(), uri.length());
      if (OB_ISNULL(double_encode)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to double encode URI", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, double_encode, result, true))) {
        LOG_WARN("fail to write double encoded URI", K(ret));
      } else {
        curl_free(double_encode);
      }
    }
  }

  return ret;
}

int ObRestAuthMgr::extract_host_and_canonical_uri(ObString &full_url,
                   ObString &host, ObString &canonical_uri, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  host.reset();
  canonical_uri.reset();
  int prefix_len = 0;
  if (full_url.prefix_match(ObRestCatalogProperties::HTTPS_PREFIX)) {
    prefix_len = strlen(ObRestCatalogProperties::HTTPS_PREFIX);
  } else if (full_url.prefix_match(ObRestCatalogProperties::HTTP_PREFIX)) {
    prefix_len = strlen(ObRestCatalogProperties::HTTP_PREFIX);
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid url", K(ret), K(full_url));
  }
  if (OB_SUCC(ret)) {
    full_url += prefix_len;
    const char *split_pos = full_url.find('/');
    if (OB_ISNULL(split_pos)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid url", K(ret), K(full_url));
    } else {
      ObString host_str = ObString(split_pos - full_url.ptr(), full_url.ptr());
      ObString canonical_uri_str = ObString(full_url.length() - (split_pos - full_url.ptr()), split_pos);
      if (OB_FAIL(ob_write_string(allocator, host_str, host, true))) {
        LOG_WARN("fail to write host", K(ret));
      } else if (OB_FAIL(double_encode_uri_partially(canonical_uri_str, allocator, canonical_uri))) {
        LOG_WARN("fail to double encode canonical uri", K(ret));
      }
    }
  }
  LOG_INFO("extract host and canonical uri", K(host), K(canonical_uri));
  return ret;
}

int ObRestAuthMgr::hmac_sha256(const unsigned char *key, int key_len,
                       const unsigned char *data, int data_len,
                       unsigned char *output)
{
  int ret = OB_SUCCESS;
  unsigned int len = 32;
  if (HMAC(EVP_sha256(), key, key_len, data, data_len, output, &len) == NULL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to compute hmac-sha256", K(ret));
  }
  return ret;
}

int ObRestAuthMgr::sha256(const unsigned char *data, int data_len, unsigned char *output)
{
  int ret = OB_SUCCESS;
  SHA256_CTX ctx;
  if (SHA256_Init(&ctx) == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to init sha256", K(ret));
  } else if (SHA256_Update(&ctx, data, data_len) == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to update sha256", K(ret));
  } else if (SHA256_Final(output, &ctx) == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to final sha256", K(ret));
  }
  return ret;
}

static int hex_encode(const unsigned char *data, int len, ObIAllocator &allocator, ObString &output)
{
  int ret = OB_SUCCESS;
  char *hex_buf = static_cast<char *>(allocator.alloc(len * 2 + 1));
  if (OB_ISNULL(hex_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else {
    for (int i = 0; i < len; ++i) {
      snprintf(hex_buf + i * 2, 3, "%02x", data[i]);
    }
    hex_buf[len * 2] = '\0';
    output.assign_ptr(hex_buf, len * 2);
  }
  return ret;
}

int ObRestAuthMgr::generate_date_string(const ObTime &obtime, const ObString &format,
                                        ObString &date_str, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  char buf[OB_MAX_TIMESTAMP_LENGTH] = {0};
  int64_t pos = 0;
  ObString locale;
  bool res_null = false;
  if (format.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(format));
  } else if (OB_FAIL(ObTimeConverter::ob_time_to_str_format(obtime, format, buf, sizeof(buf), pos, res_null, locale))) {
    LOG_WARN("fail to convert ob time to string", K(ret));
  } else if (res_null) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to convert ob time to string", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, buf, date_str, true))) {
    LOG_WARN("fail to write date string", K(ret));
  }
  return ret;
}

int ObRestAuthMgr::get_sigv4_signature(ObRestHttpRequest &request,
                                       const ObString &url,
                                       const ObRestCatalogProperties &rest_properties,
                                       ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObRestHttpMethod method = request.get_method();
  if (method < HTTP_METHOD_INVALID || method > HTTP_METHOD_DELETE ||
      url.empty() || rest_properties.accessid_.empty() ||
      rest_properties.accesskey_.empty() || rest_properties.sign_name_.empty() ||
      rest_properties.sign_region_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(url), K(rest_properties.accessid_),
             K(rest_properties.sign_name_), K(rest_properties.sign_region_));
  }

  ObString host;
  ObString canonical_uri;
  ObString copy_url;
  if (OB_FAIL(ret)) {

  } else if (OB_FAIL(ob_write_string(allocator, url, copy_url, true))) {
    LOG_WARN("fail to copy url", K(ret));
  } else if (OB_FAIL(extract_host_and_canonical_uri(copy_url, host, canonical_uri, allocator))) {
    LOG_WARN("fail to extract host and canonical uri", K(ret));
  }

  ObString amz_date;
  ObString date_stamp;
  ObTime time;
  int64_t cur_ts = ObTimeUtil::current_time();
  if (OB_FAIL(ret)) {

  } else if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(cur_ts, NULL, time))) {
    LOG_WARN("fail to convert time", K(ret));
  } else if (OB_FAIL(generate_date_string(time, "%Y%m%dT%H%i%SZ", amz_date, allocator))) {
    LOG_WARN("fail to generate date string", K(ret));
  } else if (OB_FAIL(generate_date_string(time, "%Y%m%d", date_stamp, allocator))) {
    LOG_WARN("fail to generate date string", K(ret));
  }

  const char *http_method = REST_HTTP_METHOD_NAMES[method];
  const char *canonical_query_string = "";

  ObSqlString canonical_headers;
  if (OB_FAIL(ret)) {

  } else if (OB_FAIL(canonical_headers.append_fmt("%s:%.*s\n", HOST_HEADER,
                                                  static_cast<int>(host.length()), host.ptr()))) {
    LOG_WARN("fail to append host header", K(ret));
  } else if (OB_FAIL(canonical_headers.append_fmt("%s:%.*s\n", X_AMZ_DATE_HEADER,
                                                   static_cast<int>(amz_date.length()), amz_date.ptr()))) {
    LOG_WARN("fail to append x-amz-date header", K(ret));
  }

  ObString body_hash;
  if (OB_FAIL(ret)) {

  } else {
    if (request.get_body().empty()) {
      body_hash.assign_ptr(EMPTY_HASHED_PAYLOAD, strlen(EMPTY_HASHED_PAYLOAD));
    } else {
      unsigned char body_hash_buf[32];
      if (OB_FAIL(sha256(reinterpret_cast<const unsigned char *>(request.get_body().ptr()),
                         static_cast<int>(request.get_body().length()),
                         body_hash_buf))) {
        LOG_WARN("fail to hash body", K(ret));
      } else if (OB_FAIL(hex_encode(body_hash_buf, 32, allocator, body_hash))) {
        LOG_WARN("fail to hex encode body hash", K(ret));
      }
    }
  }

  ObSqlString canonical_request;
  if (OB_FAIL(ret)) {

  } else if (OB_FAIL(canonical_request.append_fmt("%s\n%.*s\n%s\n%.*s\n%s\n%.*s",
                                            http_method,
                                            static_cast<int>(canonical_uri.length()),
                                            canonical_uri.ptr(),
                                            canonical_query_string,
                                            static_cast<int>(canonical_headers.length()),
                                            canonical_headers.ptr(),
                                            SIGNED_HEADERS,
                                            static_cast<int>(body_hash.length()),
                                            body_hash.ptr()))) {
    LOG_WARN("fail to build canonical request", K(ret));
  }

  unsigned char canonical_request_hash[32];
  ObString canonical_request_str = canonical_request.string();
  ObString canonical_request_hash_hex;

  if (OB_FAIL(ret)) {

  } else if (OB_FAIL(sha256(reinterpret_cast<const unsigned char *>(canonical_request_str.ptr()),
                     static_cast<int>(canonical_request_str.length()),
                     canonical_request_hash))) {
    LOG_WARN("fail to hash canonical request", K(ret));
  } else if (OB_FAIL(hex_encode(canonical_request_hash, 32, allocator, canonical_request_hash_hex))) {
    LOG_WARN("fail to hex encode canonical request hash", K(ret));
  }

  ObSqlString string_to_sign;
  ObString credential_scope;
  ObSqlString credential_scope_str;
  if (OB_FAIL(ret)) {

  } else if (OB_FAIL(credential_scope_str.append_fmt("%.*s/%.*s/%.*s/%s",
                                               static_cast<int>(date_stamp.length()), date_stamp.ptr(),
                                               static_cast<int>(rest_properties.sign_region_.length()),
                                               rest_properties.sign_region_.ptr(),
                                               static_cast<int>(rest_properties.sign_name_.length()),
                                               rest_properties.sign_name_.ptr(),
                                               AWS4_REQUEST))) {
    LOG_WARN("fail to build credential scope", K(ret));
  }

  credential_scope = credential_scope_str.string();
  if (OB_FAIL(ret)) {

  } else if (OB_FAIL(string_to_sign.append_fmt("AWS4-HMAC-SHA256\n%.*s\n%.*s\n%.*s",
                                         static_cast<int>(amz_date.length()), amz_date.ptr(),
                                         static_cast<int>(credential_scope.length()), credential_scope.ptr(),
                                         static_cast<int>(canonical_request_hash_hex.length()),
                                         canonical_request_hash_hex.ptr()))) {
    LOG_WARN("fail to build string to sign", K(ret));
  }


  unsigned char k_date[32] = {0};
  unsigned char k_region[32] = {0};
  unsigned char k_service[32] = {0};
  unsigned char k_signing[32] = {0};
  unsigned char final_signature[32] = {0};
  ObString signature_hex;
  ObString string_to_sign_str = string_to_sign.string();
  ObSqlString k_secret;
  if (OB_FAIL(ret)) {

  } else if (OB_FAIL(k_secret.append_fmt("AWS4%.*s",
                                   static_cast<int>(rest_properties.accesskey_.length()),
                                   rest_properties.accesskey_.ptr()))) {
    LOG_WARN("fail to build k secret", K(ret));
  } else if (OB_FAIL(hmac_sha256(reinterpret_cast<const unsigned char *>(k_secret.ptr()),
                           static_cast<int>(k_secret.length()),
                           reinterpret_cast<const unsigned char *>(date_stamp.ptr()),
                           static_cast<int>(date_stamp.length()),
                           k_date))) {
    LOG_WARN("fail to compute k_date", K(ret));
  } else if (OB_FAIL(hmac_sha256(k_date, 32,
                           reinterpret_cast<const unsigned char *>(rest_properties.sign_region_.ptr()),
                           static_cast<int>(rest_properties.sign_region_.length()),
                           k_region))) {
    LOG_WARN("fail to compute k_region", K(ret));
  } else if (OB_FAIL(hmac_sha256(k_region, 32,
                           reinterpret_cast<const unsigned char *>(rest_properties.sign_name_.ptr()),
                           static_cast<int>(rest_properties.sign_name_.length()),
                           k_service))) {
    LOG_WARN("fail to compute k_service", K(ret));
  } else if (OB_FAIL(hmac_sha256(k_service, 32,
                           reinterpret_cast<const unsigned char *>(AWS4_REQUEST),
                           strlen(AWS4_REQUEST),
                           k_signing))) {
    LOG_WARN("fail to compute k_signing", K(ret));
  } else if (OB_FAIL(hmac_sha256(k_signing, 32,
                           reinterpret_cast<const unsigned char *>(string_to_sign_str.ptr()),
                           static_cast<int>(string_to_sign_str.length()),
                           final_signature))) {
    LOG_WARN("fail to compute final signature", K(ret));
  } else if (OB_FAIL(hex_encode(final_signature, 32, allocator, signature_hex))) {
    LOG_WARN("fail to hex encode signature", K(ret));
  }

  ObSqlString auth_header;
  ObString signature;
  if (OB_FAIL(ret)) {

  } else if (OB_FAIL(auth_header.append_fmt("AWS4-HMAC-SHA256 Credential=%.*s/%.*s, SignedHeaders=%s, Signature=%.*s",
                                      static_cast<int>(rest_properties.accessid_.length()),
                                      rest_properties.accessid_.ptr(),
                                      static_cast<int>(credential_scope.length()),
                                      credential_scope.ptr(),
                                      SIGNED_HEADERS,
                                      static_cast<int>(signature_hex.length()),
                                      signature_hex.ptr()))) {
    LOG_WARN("fail to build auth header", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, auth_header.string(), signature, true))) {
    LOG_WARN("fail to write signature", K(ret));
  } else if (OB_FAIL(request.add_header(AuthHeader, signature))) {
    LOG_WARN("fail to add authorization header", K(ret));
  } else if (OB_FAIL(request.add_header(HOST_HEADER, host))) {
    LOG_WARN("fail to add host header", K(ret));
  } else if (OB_FAIL(request.add_header(X_AMZ_DATE_HEADER, amz_date))) {
    LOG_WARN("fail to add x-amz-date header", K(ret));
  }

  return ret;
}

int ObRestAuthMgr::curl_oauth2_credential(const ObRestOAuth2Key &oauth2_key,
                                          const bool update_access_time)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("oauth2_cre_tmp");
  ObCurlRestClient client(tmp_allocator);
  ObRestCatalogProperties properties;
  properties.uri_.assign_ptr(oauth2_key.oauth2_svr_uri_.ptr(), oauth2_key.oauth2_svr_uri_.length());
  if (OB_FAIL(client.init(properties))) {
    LOG_WARN("fail to init client", K(ret));
  } else {
    ObRestHttpRequest request(tmp_allocator);  // 没有子路径拼接
    ObRestHttpResponse response(tmp_allocator);
    ObString access_token;
    ObString token_type;
    ObSqlString concat_token;
    ObRestOauth2Value oauth2_value;
    if (OB_FALSE_IT(request.set_method(ObRestHttpMethod::HTTP_METHOD_POST))) {
      // do nothing
    } else if (OB_FAIL(request.add_query_param("grant_type", "client_credentials"))) {
      LOG_WARN("fail to add grant type", K(ret));
    } else if (OB_FAIL(request.add_query_param("client_id", oauth2_key.accessid_))) {
      LOG_WARN("fail to add client id", K(ret));
    } else if (OB_FAIL(request.add_query_param("client_secret", oauth2_key.accesskey_))) {
      LOG_WARN("fail to add client secret", K(ret));
    } else if (OB_FAIL(request.add_query_param("scope", oauth2_key.scope_))) {
      LOG_WARN("fail to add scope", K(ret));
    } else if (OB_FAIL(client.execute(request, response))) {
      LOG_WARN("fail to execute oauth2 request", K(ret));
    } else if (OB_FAIL(parse_oauth2_response(response.get_body(), access_token, token_type, tmp_allocator))) {
      LOG_WARN("fail to parse oauth2 response", K(ret));
    } else if (OB_FAIL(concat_token.append_fmt("%.*s %.*s",
                                              token_type.length(), token_type.ptr(),
                                              access_token.length(), access_token.ptr()))) {
      LOG_WARN("fail to append full token", K(ret));
    } else if (OB_FAIL(ob_write_string(oauth2_credential_allocator_,
                                       concat_token.string(), oauth2_value.access_token_, true))) {
      LOG_WARN("fail to write full token", K(ret));
    } else {
      common::SpinWLockGuard guard(oauth2_credential_lock_);
      // Handle access time: update or preserve
      ObRestOauth2ValueAccessTimeCallBack read_callback(false /*update_access_time*/);
      if (update_access_time) {
        oauth2_value.last_access_time_us_ = ObTimeUtility::current_time();
      } else {
        // Preserve old access time for background refresh
        if (OB_FAIL(oauth2_credential_map_.read_atomic(oauth2_key, read_callback))) {
          if (ret == OB_HASH_NOT_EXIST) {
            ret = OB_SUCCESS;
            oauth2_value.last_access_time_us_ = ObTimeUtility::current_time();
          } else {
            LOG_WARN("fail to get credential access time", K(ret), K(oauth2_key));
          }
        } else {
          oauth2_value.last_access_time_us_ = read_callback.original_access_time_us_;
        }
      }

      if (OB_SUCC(ret)) {
        ObRestOauth2Value old_token;
        if (OB_FAIL(oauth2_credential_map_.erase_refactored(oauth2_key, &old_token))) {
          if (ret == OB_HASH_NOT_EXIST) {
            ret = OB_SUCCESS;
            LOG_INFO("oauth2 credential not found", K(oauth2_key), K(oauth2_key.hash()));
          } else {
            LOG_WARN("fail to erase oauth2 credential", K(ret));
          }
        } else if (OB_NOT_NULL(old_token.access_token_.ptr())) {
          oauth2_credential_allocator_.free(old_token.access_token_.ptr());  // 释放旧的token
        }

        if (OB_SUCC(ret) && OB_FAIL(oauth2_credential_map_.set_refactored(oauth2_key, oauth2_value, 1))) {
          LOG_WARN("fail to cache oauth2 credential", K(ret));
        }
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(oauth2_value.access_token_.ptr())) {
        oauth2_credential_allocator_.free(oauth2_value.access_token_.ptr());
      }
    }
  }
  return ret;
}

int ObRestAuthMgr::parse_oauth2_response(const ObString &response_body,
                                         ObString &access_token,
                                         ObString &token_type,
                                         ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObJsonNode *json_node = nullptr;
  access_token.reset();
  token_type.assign_ptr(DEFAULT_TOKEN_TYPE, strlen(DEFAULT_TOKEN_TYPE));
  if (response_body.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty response body", K(ret));
  } else if (OB_FAIL(ObJsonParser::get_tree(&allocator, response_body, json_node))) {
    LOG_WARN("failed to parse oauth2 response json", K(ret), K(response_body));
  } else if (OB_ISNULL(json_node) || ObJsonNodeType::J_OBJECT != json_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json format, expected object", K(ret));
  } else {
    ObJsonObject *json_object = static_cast<ObJsonObject *>(json_node);
    ObJsonNode *access_token_node = json_object->get_value(ACCESS_TOKEN_KEY);
    if (OB_ISNULL(access_token_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("access_token field not found in oauth2 response", K(ret));
    } else if (ObJsonNodeType::J_STRING != access_token_node->json_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("access_token is not a string", K(ret));
    } else {
      ObJsonString *token_string = static_cast<ObJsonString *>(access_token_node);
      ObString token_value = token_string->value();
      if (OB_FAIL(ob_write_string(allocator, token_value, access_token, true))) {
        LOG_WARN("fail to write access_token", K(ret));
      }
    }

    ObJsonNode *token_type_node = json_object->get_value(TOKEN_TYPE_KEY);
    if (OB_FAIL(ret) || OB_ISNULL(token_type_node)) {
      // do nothing
    } else if (ObJsonNodeType::J_STRING != token_type_node->json_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("token_type is not a string", K(ret));
    } else {
      ObJsonString *token_type_string = static_cast<ObJsonString *>(token_type_node);
      ObString type_value = token_type_string->value();
      if (OB_FAIL(ob_write_string(allocator, type_value, token_type, true))) {
        LOG_WARN("fail to write token_type", K(ret));
      }
    }
  }
  return ret;
}

int ObRestAuthMgr::get_oauth2_credential_from_map(const ObRestOAuth2Key &oauth2_key,
                                                  ObString &access_token,
                                                  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRestAuthMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(!oauth2_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("oauth2_key is invalid", K(ret), K(oauth2_key));
  } else {
    common::SpinRLockGuard guard(oauth2_credential_lock_);
    ObRestOauth2Value oauth2_value;
    ObRestOauth2ValueAccessTimeCallBack callback(true /*update_access_time*/);
    if (OB_FAIL(oauth2_credential_map_.get_refactored(oauth2_key, oauth2_value))) {
      LOG_WARN("fail to get_refactored", K(ret), K(oauth2_key));
    } else if (OB_FAIL(oauth2_credential_map_.atomic_refactored(oauth2_key, callback))) {
      LOG_WARN("fail to update access time", K(ret), K(oauth2_key));
    } else if (OB_FAIL(ob_write_string(allocator, oauth2_value.access_token_, access_token, true))) {
      LOG_WARN("fail to write access_token", K(ret));
    }
  }
  return ret;
}

int ObRestAuthMgr::refresh()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRestAuthMgr not init", K(ret));
  } else {
    ObArray<ObRestOAuth2Key> oauth2_key_to_refresh;
    ObArray<ObRestOAuth2Key> oauth2_key_to_delete;
    {
      common::SpinRLockGuard guard(oauth2_credential_lock_);
      common::hash::ObHashMap<ObRestOAuth2Key, ObRestOauth2Value>::iterator iter = oauth2_credential_map_.begin();
      int64_t cur_ts = ObTimeUtility::current_time();
      int64_t credential_evict_time = GCONF.rest_oauth2_credential_evict_time;
      while (OB_SUCC(ret) && iter != oauth2_credential_map_.end()) {
        ObRestOAuth2Key &oauth2_key = iter->first;
        ObRestOauth2Value &oauth2_value = iter->second;
        if (cur_ts - oauth2_value.last_access_time_us_ >= credential_evict_time) {
          if (OB_FAIL(oauth2_key_to_delete.push_back(oauth2_key))) {
            LOG_WARN("failed to push back oauth2 key to delete", K(ret), K(oauth2_key));
          }
        } else {
          if (OB_FAIL(oauth2_key_to_refresh.push_back(oauth2_key))) {
            LOG_WARN("failed to push back oauth2 key to refresh", K(ret), K(oauth2_key));
          }
        }
        ++iter;
      }
    }

    {
      common::SpinWLockGuard guard(oauth2_credential_lock_);
      for (int64_t i = 0; OB_SUCC(ret) && i < oauth2_key_to_delete.count(); i++) {
        ObRestOauth2Value old_token;
        if (OB_FAIL(oauth2_credential_map_.erase_refactored(oauth2_key_to_delete[i], &old_token))) {
          if (ret == OB_HASH_NOT_EXIST) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to delete oauth2 credential", K(ret), K(i), K(oauth2_key_to_delete[i]));
          }
        } else {
          if (OB_NOT_NULL(old_token.access_token_.ptr())) {
            oauth2_credential_allocator_.free(old_token.access_token_.ptr());
          }
        }
      }
    }

    // Refresh active credentials (preserve access time)
    for (int64_t i = 0; OB_SUCC(ret) && i < oauth2_key_to_refresh.count(); i++) {
      if (OB_FAIL(curl_oauth2_credential(oauth2_key_to_refresh[i], false /*update_access_time*/))) {
        LOG_WARN("failed to refresh oauth2 credential", K(ret), K(i), K(oauth2_key_to_refresh[i]));
      }
    }
  }
  return ret;
}

void ObRestOauth2Value::reset()
{
  access_token_.reset();
  last_access_time_us_ = 0;
}

int ObRestOauth2Value::assign(const ObRestOauth2Value &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    access_token_.assign_ptr(other.access_token_.ptr(), other.access_token_.length());
    last_access_time_us_ = other.last_access_time_us_;
  }
  return ret;
}

ObRestAuthMgr& ObRestAuthMgr::get_instance()
{
  static ObRestAuthMgr instance_;
  return instance_;
}

void ObRestAuthMgrRefreshTask::runTimerTask(void)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(auth_mgr_)) {
    LOG_INFO("run timer task to refresh auth mgr", K(ret));
    if (OB_FAIL(auth_mgr_->refresh())) {
      LOG_WARN("failed to refresh auth mgr", K(ret));
    }
  }
}

}
}
