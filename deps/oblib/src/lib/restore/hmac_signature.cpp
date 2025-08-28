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

#include "hmac_signature.h"
#include "lib/encode/ob_base64_encode.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array_iterator.h"

namespace oceanbase
{
namespace common
{

// Function to generate a signature nonce
int generate_signature_nonce(char *nonce, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  // Get current timestamp in milliseconds
  const int64_t curr_time_us = ObTimeUtility::current_time();
  if (OB_ISNULL(nonce) || OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid args to generate signature nonce", K(ret), KP(nonce), K(buf_size));
  } else {
    int64_t random_num = ObRandom::rand(0, OB_MAX_STS_SIGNATURE_RAND_NUM);
    if (OB_FAIL(databuff_printf(nonce, buf_size, "%ld%ld", curr_time_us, random_num))) {
      OB_LOG(WARN, "failed to gen signature nonce", K(ret), K(nonce), K(buf_size), K(curr_time_us), K(random_num));
    }
  }
  return ret;
}

// request_id is used as the identifier for connecting to ocp.
// observer needs to ensure that request_id is unique and there are no other requirements.
int generate_request_id(char *request_id, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  char signature_nonce[OB_MAX_STS_SIGNATURE_NONCE_LENTH] = {0};
  // Get current time
  if (OB_FAIL(generate_signature_nonce(signature_nonce, sizeof(signature_nonce)))) {
    OB_LOG(WARN, "generage signature nonce failed", K(ret), K(signature_nonce), K(sizeof(signature_nonce)));
  } else if (OB_FAIL(databuff_printf(request_id, buf_size, "observer-%s", signature_nonce))) {
    OB_LOG(WARN, "failed to generate request id", K(ret), K(request_id), K(buf_size), K(signature_nonce));
  }
  return ret;
}

int base64_encoded(const char *input, const int64_t input_len, char *encoded_result, const int64_t encoded_result_buf_len)
{
  int ret = OB_SUCCESS;
  int64_t encoded_buf_len = ObBase64Encoder::needed_encoded_length(input_len);
  int64_t encoded_pos = 0;

  if (OB_ISNULL(input) || OB_UNLIKELY(input_len <= 0) || OB_ISNULL(encoded_result) || OB_UNLIKELY(encoded_result_buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid args to encode base64", K(ret), KP(input), K(input_len), KP(encoded_result), K(encoded_result_buf_len));
  } else if (OB_UNLIKELY(encoded_buf_len >= encoded_result_buf_len)) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "size overflow", K(ret), K(encoded_buf_len), K(encoded_result_buf_len));
  } else if (OB_FAIL(ObBase64Encoder::encode((const uint8_t *)input, input_len, encoded_result, encoded_result_buf_len, encoded_pos))) {
    OB_LOG(WARN, "encode base64 fails", K(ret), KP(input), K(input_len), KP(encoded_result), K(encoded_result_buf_len),
        K(encoded_buf_len), K(encoded_pos));
  } else if (OB_UNLIKELY(encoded_pos >= encoded_result_buf_len)) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "size overflow", K(ret), K(encoded_pos), K(encoded_result_buf_len));
  } else {
    encoded_result[encoded_pos] = '\0';
  }
  return ret;
}

int percent_encode(const char *content, char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(content) || OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(content), KP(buf), K(buf_len));
  } else {
    char *p = buf;
    const int64_t content_len = strlen(content);
    int64_t num = 0;
    const char *hex = "0123456789ABCDEF";
    int64_t triple_num = 0;
    if (OB_UNLIKELY(content_len <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid content", K(ret), K(content_len));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < content_len; ++i) {
      char c = content[i];
      if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
        num++;
      }
    }
    triple_num = content_len - num;
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(content_len + 2 * triple_num >= buf_len)) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG(WARN, "content is too long", K(ret), KP(content), K(content_len), K(triple_num), K(buf_len));
    } else {
      for (uint64_t i = 0; OB_SUCC(ret) && i < content_len; ++i) {
        char c = content[i];
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
          *p++ = c;
        } else if (c == ' ') {
          *p++ = '%';
          *p++ = '2';
          *p++ = '0';
        } else {
          *p++ = '%';
          *p++ = hex[(c >> 4) & 0xF];
          *p++ = hex[c & 0xF];
        }
      }
      *p = '\0';
    }
  }
  return ret;
}

// The signature generated by hmac_sha1 may contain \0,
// result_len is used to record the actual length of the signature after hmac_sha1
int hmac_sha1(const char *key, const char *data, char *encoded_result, const int64_t encode_buf_len,
    uint32_t &result_len)
{
  int ret = OB_SUCCESS;
  unsigned char *result = nullptr;
  ObArenaAllocator allocator("hmac_sha1");
  // The buffer should have at least EVP_MAX_MD_SIZE bytes of space to ensure that it can store the
  // output of any hashing algorithm.
  if (OB_ISNULL(key) || OB_ISNULL(data) || OB_ISNULL(encoded_result)
      || OB_UNLIKELY(encode_buf_len < EVP_MAX_MD_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(key), KP(data), KP(encoded_result),
        K(encode_buf_len), K(result_len));
  }
  // The max length of hmac_sha1 is 20
  else if (OB_ISNULL(result = static_cast<unsigned char *>(allocator.alloc(sizeof(char) * 20)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to malloc", K(ret));
  } else {
    HMAC_CTX *hmac = HMAC_CTX_new();
    if (OB_ISNULL(hmac)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "failed to allocate memory", K(ret));
    } else if (OB_UNLIKELY(1 != HMAC_Init_ex(hmac, key, strlen(key), EVP_sha1(), nullptr))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "failed to init hmac", K(ret), KP(key), K(strlen(key)));
    } else {
      if (OB_UNLIKELY(1 != HMAC_Update(hmac, reinterpret_cast<const unsigned char *>(data), strlen(data)))) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "failed to update hmac", K(ret), KP(data), K(strlen(data)));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(1 != HMAC_Final(hmac, result, &result_len))) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "failed to final hmac", K(ret), KP(result), K(result_len));
      } else if (OB_UNLIKELY(result_len >= encode_buf_len)) {
        ret = OB_SIZE_OVERFLOW;
        OB_LOG(WARN, "encoded result is too long", K(ret), K(result_len), K(encode_buf_len));
      } else {
        MEMCPY(encoded_result, result, result_len);
        encoded_result[result_len] = '\0';
      }
    }
    if (OB_NOT_NULL(hmac)) {
      HMAC_CTX_free(hmac);
    }
  }
  return ret;
}

struct Compare
{
  // Sort lexicographically by key
  bool operator()(const std::pair<const char *, const char *> &a, const std::pair<const char *, const char *> &b) const
  {
    // Check if either key is NULL
    bool flag = false;
    if (OB_ISNULL(a.first) && OB_ISNULL(b.first)) {
      flag = false;  // Both are NULL, considered equal
    } else if (OB_ISNULL(a.first)) {
      flag = true;  // NULL is considered less than any non-NULL string
    } else if (OB_ISNULL(b.first)) {
      flag = false;  // Any non-NULL string is considered greater than NULL
    } else {
      int key_cmp = strcmp(a.first, b.first);
      if (key_cmp < 0) {
        flag =  true;
      } else if (key_cmp > 0) {
        flag = false;
      } else {
        // If keys are equal, compare values
        // Check if either value is NULL
        if (OB_ISNULL(a.second) && OB_ISNULL(b.second)) {
          flag = false;  // Both are NULL, considered equal
        } else if (OB_ISNULL(a.second)) {
          flag = true;  // NULL is considered less than any non-NULL string
        } else if (OB_ISNULL(b.second)) {
          flag = false;  // Any non-NULL string is considered greater than NULL
        } else {
          flag = strcmp(a.second, b.second) < 0;
        }
      }
    }
    return flag;
  }
};

int sign_request(
    ObArray<std::pair<const char *, const char *>> params, 
    const char *method, char *signature, 
    const int64_t signature_buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(params.count() <= 0) || OB_ISNULL(method) || OB_ISNULL(signature)
      || OB_UNLIKELY(signature_buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(params.count()), KP(method), KP(signature),
        K(signature_buf_len));
  } else {
    // Sort the parameters by key
    lib::ob_sort(params.begin(), params.end(), Compare());
    // Make the string to sign
    const char *sk = nullptr;
    // Currently, the longest key used for signature will not exceed 64
    const int64_t OB_MAX_HMAC_ENCODED_KEY = 64;
    constexpr int64_t OB_MAX_HMAC_ENCODED_VALUE = MAX(OB_MAX_STS_SIGNATURE_NONCE_LENTH,
        MAX(OB_MAX_STS_REQUEST_ID_LENTH, MAX(OB_MAX_ROLE_ARN_LENGTH, MAX(OB_MAX_STS_AK_LENGTH, OB_MAX_STS_SK_LENGTH))));
    char encoded_key[OB_MAX_HMAC_ENCODED_KEY] = {0};
    char encoded_value[OB_MAX_HMAC_ENCODED_VALUE] = {0};
    char delimiter[4] = {0};
    char params_to_sign[OB_MAX_STS_CONCAT_LENGTH] = {0};
    char concat_params[OB_MAX_STS_CONCAT_LENGTH] = {0};
    char sign_result[OB_MAX_STS_CONCAT_LENGTH] = {0};
    char encoded_result[OB_MAX_STS_SIGNATURE_LENGTH] = {0};
    uint32_t sign_result_len = 0;
    ObArenaAllocator allocator("hmac_signature");
    ObStringBuffer encoded_params(&allocator);

    const char *first = nullptr;
    const char *second = nullptr;
    // e.g.Action=GetResourceSTSCredential&CloudProvider=aliyun&RequestId=obrequest&RequestSource=OBSERVER
    // &ResourceAccount=oceanbase&ResourceType=OSS&SignatureNonce=xxxxx
    for (uint64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
      first = params.at(i).first;
      second = params.at(i).second;
      if (OB_ISNULL(first) || OB_ISNULL(second)) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "params is not allowed to be nullptr", K(ret), K(first), KP(second), K(i));
      } else if (MEMCMP(first, "ObSignature", strlen(first)) == 0 
                 || MEMCMP(first, "ObSignatureKey", strlen(first)) == 0
                 || MEMCMP(first, "ObSignatureSecret", strlen(first)) == 0 
                 || MEMCMP(first, "UID", strlen(first)) == 0
                 || MEMCMP(first, "OpSource", strlen(first)) == 0) {
        if (MEMCMP(first, "ObSignatureSecret", strlen(first)) == 0) {
          sk = second;
        }
        continue;
      } else if (OB_FAIL(percent_encode(first, encoded_key, sizeof(encoded_key)))) {
        OB_LOG(WARN, "failed to percent encode", K(ret), K(i), K(first));
      } else if (OB_FAIL(percent_encode(second, encoded_value, sizeof(encoded_value)))) {
        OB_LOG(WARN, "failed to percent encode", K(ret), K(i), KP(second));
      } else if (i != 0 && OB_FAIL(encoded_params.append("&"))) {
        OB_LOG(WARN, "failed to append", K(ret), K(i));
      } else if (OB_FAIL(encoded_params.append(encoded_key))) {
        OB_LOG(WARN, "failed to append", K(ret), KP(encoded_key));
      } else if (OB_FAIL(encoded_params.append("="))) {
        OB_LOG(WARN, "failed to append", K(ret));
      } else if (OB_FAIL(encoded_params.append(encoded_value))) {
        OB_LOG(WARN, "failed to append", K(ret), KP(encoded_value));
      }
    }
    // e.g. params_to_sign="POST&%2F&Action%3DGetResourceSTSCredential%26CloudProvider%3Daliyun%26
    // RequestId%3Dobrequest%26RequestSource%3DOBSERVER%26ResourceAccount%3Doceanbase%26ResourceType%3DOSS%26SignatureNonce%3Dxxxxxx"
    if (FAILEDx(percent_encode("/", delimiter, sizeof(delimiter)))) {
      OB_LOG(WARN, "failed to percent encode", K(ret), K(delimiter), K(sizeof(delimiter)));
    } else if (OB_FAIL(percent_encode(encoded_params.ptr(), concat_params, sizeof(concat_params)))) {
      OB_LOG(WARN, "failed to percent encode", K(ret), KP(encoded_params.ptr()), K(encoded_params.length()), KP(concat_params));
    } else if (OB_FAIL(databuff_printf(
                   params_to_sign, sizeof(params_to_sign), "%s&%s&%s", method, delimiter, concat_params))) {
      OB_LOG(WARN, "failed to percent encode", K(ret), K(method), K(delimiter), KP(concat_params), KP(params_to_sign));
    }
    // signature with HMAC-SHA1
    else if (OB_FAIL(hmac_sha1(sk, params_to_sign, sign_result, sizeof(sign_result), sign_result_len))) {
      OB_LOG(WARN, "failed to hmac sha1", K(ret), KP(params_to_sign), K(sign_result), K(sign_result_len));
    } else if (OB_FAIL(base64_encoded(sign_result, sign_result_len, encoded_result, sizeof(encoded_result)))) {
      OB_LOG(WARN, "failed to base64 encode", K(ret), K(sign_result));
    }
    const int64_t len = strlen(encoded_result);
    if (OB_FAIL(ret)) {
    } else if (len >= signature_buf_len) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG(WARN, "signature is too long", K(ret), K(signature_buf_len), K(len));
    } else {
      MEMCPY(signature, encoded_result, len);
      signature[len] = '\0';
    }
  }
  return ret;
}
}  // namespace common
}  // namespace oceanbase