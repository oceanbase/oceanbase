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

#define USING_LOG_PREFIX SHARE

#include "share/catalog/rest/client/ob_curl_rest_client.h"
#include "lib/string/ob_string.h"
#include "share/catalog/ob_catalog_properties.h"
#include <openssl/ssl.h>
#include <openssl/x509_vfy.h>
#include <openssl/err.h>

namespace oceanbase
{
namespace share
{

static CURLcode ssl_ctx_callback(CURL *curl, void *ssl_ctx, void *userptr)
{
  UNUSED(curl);
  CURLcode ret = CURLE_OK;
  SSL_CTX *ctx = static_cast<SSL_CTX *>(ssl_ctx);
  const char *ca_file = static_cast<const char *>(userptr);

  if (OB_ISNULL(ctx)) {
    ret = CURLE_SSL_CERTPROBLEM;
    LOG_WARN("ssl context is null");
  } else if (OB_NOT_NULL(ca_file) && strlen(ca_file) > 0) {
    X509_STORE *store = SSL_CTX_get_cert_store(ctx);
    if (OB_ISNULL(store)) {
      ret = CURLE_SSL_CERTPROBLEM;
      LOG_WARN("failed to get cert store from ssl context");
    } else {
      if (1 != X509_STORE_load_locations(store, ca_file, NULL)) {
        ret = CURLE_SSL_CACERT_BADFILE;
        LOG_WARN("failed to load ca certificates from file", K(ca_file));
      }
    }
  }
  return ret;
}

int ObCurlRestClient::do_init(const ObRestCatalogProperties &properties)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("curl client already initialized");
  } else {
    curl_ = curl_easy_init();
    if (nullptr == curl_) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to init curl easy handle");
    } else {
      curl_easy_setopt(curl_, CURLOPT_NOSIGNAL, 1L);
      curl_easy_setopt(curl_, CURLOPT_TIMEOUT, properties.http_timeout_/1000/1000);
      curl_easy_setopt(curl_, CURLOPT_FOLLOWLOCATION, 1L);
      curl_easy_setopt(curl_, CURLOPT_MAXREDIRS, 3L);

      // TCP keepalive
      curl_easy_setopt(curl_, CURLOPT_TCP_KEEPALIVE, 1L);
      curl_easy_setopt(curl_, CURLOPT_TCP_KEEPIDLE, properties.http_keep_alive_time_/1000/1000);

      // SSL证书验证
      if (uri_.prefix_match(ObRestCatalogProperties::HTTPS_PREFIX)) {
        curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, 1L);
        curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, 2L);
        curl_easy_setopt(curl_, CURLOPT_SSL_CTX_FUNCTION, ssl_ctx_callback);
        curl_easy_setopt(curl_, CURLOPT_SSL_CTX_DATA, OB_SSL_REST_CA_FILE);
      }
    }
  }
  return ret;
}

int ObCurlRestClient::do_destroy()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    cleanup_curl();
  } else {
    ret = OB_NOT_INIT;
    LOG_WARN("curl client not initialized", K(ret));
  }
  return ret;
}

int ObCurlRestClient::do_reuse()
{
  int ret = OB_SUCCESS;
  // do nothing
  return ret;
}

int ObCurlRestClient::execute(const ObRestHttpRequest &request, ObRestHttpResponse &response)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("curl client not initialized", K(ret));
  } else if (OB_FAIL(set_curl_options(request, response))) {
    LOG_WARN("failed to set curl options", K(ret));
  } else {
    CURLcode cc = CURLE_OK;
    long http_code = 0;
    char *content_type = nullptr;
    if (CURLE_OK != (cc = curl_easy_perform(curl_))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("curl perform failed", K(ret), K(cc),K(request.get_url()));
    } else if (CURLE_OK != (cc = curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &http_code))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("curl get info failed", K(ret), K(cc), K(request.get_url()));
    } else if (FALSE_IT(response.set_status_code(http_code))) {
      // do nothing
    } else if (http_code / 100 != 2 && http_code != 304) {  // 内部先校验状态码, 外部有需要可以重置状态码
      if (http_code == 401 || http_code == 403) {
        ret = OB_HTTP_UNAUTHORIZED;
        LOG_WARN("http unauthorized", K(ret), K(http_code), K(request.get_url()));
      } else if (http_code == 404) {
        ret = OB_HTTP_NOT_FOUND;
        LOG_WARN("http not found", K(ret), K(http_code), K(request.get_url()));
        LOG_USER_ERROR(OB_HTTP_NOT_FOUND, response.get_body().ptr());
      } else if (http_code / 100 == 4) {
        ret = OB_HTTP_BAD_REQUEST;
        LOG_WARN("http bad request", K(ret), K(http_code), K(request.get_url()));
        LOG_USER_ERROR(OB_HTTP_BAD_REQUEST, response.get_body().ptr());
      } else if (http_code / 100 == 5) {
        ret = OB_HTTP_SERVER_ERROR;
        LOG_WARN("http server error", K(ret), K(http_code), K(request.get_url()));
        LOG_USER_ERROR(OB_HTTP_SERVER_ERROR, response.get_body().ptr());
      } else {
        ret = OB_CURL_ERROR;
        LOG_WARN("http error", K(ret), K(http_code), K(request.get_url()));
      }
    } else if (CURLE_OK != (cc = curl_easy_getinfo(curl_, CURLINFO_CONTENT_TYPE, &content_type))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("curl get content type failed", K(ret), K(cc), K(request.get_url()));
    } else if (FALSE_IT(response.set_content_type(ObString(content_type)))) {
      // do nothing
    } else if (content_type != nullptr) {
      response.set_content_type(ObString(content_type));
    }
  }
  return ret;
}

int ObCurlRestClient::init_curl()
{
  int ret = OB_SUCCESS;

  return ret;
}

void ObCurlRestClient::cleanup_curl()
{
  if (nullptr != curl_) {
    curl_easy_cleanup(curl_);
    curl_ = nullptr;
  }
  if (nullptr != header_list_) {
    curl_slist_free_all(header_list_);
    header_list_ = nullptr;
  }
}

int ObCurlRestClient::set_curl_options(const ObRestHttpRequest &request, ObRestHttpResponse &response)
{
  int ret = OB_SUCCESS;
  if (nullptr == curl_) {
    ret = OB_NOT_INIT;
    LOG_WARN("curl handle not initialized", K(ret));
  } else {
    CURLcode cc = CURLE_OK;
    ObString req_url;
    ObSqlString concat_url;

    // url
    if (OB_FAIL(request.get_concat_url(req_url))) {
      LOG_WARN("failed to get concat url", K(ret));
    } else if (OB_FAIL(concat_url.append(uri_))) {
      LOG_WARN("failed to append uri", K(ret));
    } else if (OB_FAIL(concat_url.append(req_url))) {
      LOG_WARN("failed to append req url", K(ret));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_URL, concat_url.ptr()))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to set URL", K(ret), K(cc), K(concat_url));
    }

    // http method
    if (OB_SUCC(ret)) {
      ObRestHttpMethod method = request.get_method();
      switch (method) {
        case HTTP_METHOD_HEAD: {
          if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_NOBODY, 1L))) {
            ret = OB_CURL_ERROR;
            LOG_WARN("failed to set NOBODY", K(ret), K(cc), K(concat_url));
          }
          break;
        }
        case HTTP_METHOD_POST: {
          if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_POST, 1L))) {
            ret = OB_CURL_ERROR;
            LOG_WARN("failed to set POST", K(ret), K(cc), K(concat_url));
          }
          break;
        }
        case HTTP_METHOD_PUT: {
          if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, "PUT"))) {
            ret = OB_CURL_ERROR;
            LOG_WARN("failed to set CUSTOMREQUEST", K(ret), K(cc), K(concat_url));
          }
          break;
        }
        case HTTP_METHOD_DELETE: {
          if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, "DELETE"))) {
            ret = OB_CURL_ERROR;
            LOG_WARN("failed to set CUSTOMREQUEST", K(ret), K(cc), K(concat_url));
          }
          break;
        }
        case HTTP_METHOD_GET: {
          // do nothing
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument for http method", K(ret), K(method));
          break;
        }
      }
    }

    // body
    ObString body = request.get_body();
    if (OB_SUCC(ret) && !body.empty()) {
      if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, body.ptr()))) {
        ret = OB_CURL_ERROR;
        LOG_WARN("failed to set POSTFIELDS", K(ret), K(cc), K(concat_url));
      } else if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_POSTFIELDSIZE, body.length()))) {
        ret = OB_CURL_ERROR;
        LOG_WARN("failed to set POSTFIELDSIZE", K(ret), K(cc), K(concat_url));
      }
    }

    // headers
    common::ObArray<common::ObString> header_names;
    common::ObArray<common::ObString> header_values;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(request.get_all_headers(header_names, header_values))) {
      LOG_WARN("failed to get all headers", K(ret));
    } else if (header_names.count() != header_values.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("header names and values count mismatch", K(ret), K(header_names.count()), K(header_values.count()));
    } else {
      // Clean up previous header list if exists
      if (nullptr != header_list_) {
        curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, nullptr);
        curl_slist_free_all(header_list_);
        header_list_ = nullptr;
      }
      // Build header list for curl
      for (int64_t i = 0; i < header_names.count() && OB_SUCC(ret); ++i) {
        ObSqlString header_str;
        if (OB_FAIL(header_str.append_fmt("%.*s: %.*s",
                                          header_names[i].length(), header_names[i].ptr(),
                                          header_values[i].length(), header_values[i].ptr()))) {
          LOG_WARN("failed to format header", K(ret), K(header_names[i]), K(header_values[i]));
        } else {
          struct curl_slist *new_header = curl_slist_append(header_list_, header_str.ptr());
          if (nullptr == new_header) {
            ret = OB_CURL_ERROR;
            LOG_WARN("failed to append header to curl list", K(ret), K(header_str));
          } else {
            header_list_ = new_header;
          }
        }
      }
      // Set the header list to curl
      if (OB_SUCC(ret) && header_list_ != nullptr) {
        if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, header_list_))) {
          ret = OB_CURL_ERROR;
          LOG_WARN("failed to set HTTP headers", K(ret), K(cc));
        }
      }
    }

    // write callback
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, ObCurlRestClient::write_callback))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to set WRITEFUNCTION", K(ret), K(cc), K(concat_url));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response.body_))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("failed to set WRITEDATA", K(ret), K(cc), K(concat_url));
    }
  }
  return ret;
}

size_t ObCurlRestClient::write_callback(void *contents, size_t size, size_t nmemb, void *buffer)
{
  int ret = OB_SUCCESS;
  size_t total_size = size * nmemb;
  if (OB_ISNULL(contents) || OB_ISNULL(buffer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for curl_write_callback", K(ret), K(contents), K(size), K(nmemb), K(buffer));
  } else if (total_size > 0) {
    ObSqlString &result = *static_cast<ObSqlString*>(buffer);
    if (OB_FAIL(result.append(static_cast<const char*>(contents), total_size))) {
      LOG_WARN("failed to append to result", K(ret), K(result), K(contents), K(total_size));
    }
  }
  return OB_SUCC(ret) ? total_size : 0;
}

}
}