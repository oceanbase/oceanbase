/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX COMMON

#include "ob_sha256_crypt.h"
#include "ob_crypt_common.h"
#include "ob_encrypted_helper.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_print_utils.h"
#include <openssl/evp.h>
#include <cstring>
#include <cstdio>

using namespace oceanbase::common;

namespace oceanbase {
namespace common {

// Intermediate multi-hash magic (unix-crypt style). Final stored auth string uses '$A$'.
static const char SHA256_CRYPT_ALG_MAGIC[] = "$5$";

int ObSha256Crypt::generate_sha256_multi_hash(
    const char *plaintext,
    const int64_t plaintext_len,
    const char *salt,
    const int64_t salt_len,
    const int64_t rounds,
    ObIAllocator &allocator,
    ObString &output)
{
  return ObCryptCommon::generate_auth_multi_hash(
      EVP_sha256(), OB_SHA256_DIGEST_LENGTH, OB_SHA256_DIGEST_LENGTH,
      SHA256_CRYPT_ALG_MAGIC,
      plaintext, plaintext_len, salt, salt_len, rounds, allocator, output);
}

int ObSha256Crypt::generate_user_salt(char *buffer, const int64_t buffer_len)
{
  return ObCryptCommon::generate_user_salt(buffer, buffer_len);
}

int ObSha256Crypt::extract_user_salt(const char *crypt_str,
                                     const int64_t crypt_str_len,
                                     const char **salt_begin,
                                     const char **salt_end)
{
  return ObCryptCommon::extract_user_salt(crypt_str, crypt_str_len, salt_begin, salt_end);
}

int ObSha256Crypt::serialize_auth_string(
    const char *salt,
    const int64_t salt_len,
    const char *digest,
    const int64_t digest_len,
    const int64_t iterations,
    ObIAllocator &allocator,
    ObString &output)
{
  return ObCryptCommon::serialize_auth_string(OB_AUTH_STRING_DIGEST_TYPE,
                                               salt, salt_len, digest, digest_len,
                                               iterations, allocator, output);
}

int ObSha256Crypt::deserialize_auth_string(
    const ObString &auth_string,
    ObString &salt,
    ObString &digest,
    int64_t &iterations)
{
  return ObCryptCommon::deserialize_auth_string(OB_AUTH_STRING_DIGEST_TYPE,
                                                 auth_string, salt, digest, iterations);
}

int ObSha256Crypt::check_sha256_password(
    const ObString &plaintext_password,
    const ObString &scramble,
    const ObString &stored_auth_string,
    bool &is_match)
{
  return ObCryptCommon::check_password(EVP_sha256(),
                                       OB_SHA256_DIGEST_LENGTH,
                                       SHA256_CRYPT_ALG_MAGIC,
                                       OB_AUTH_STRING_DIGEST_TYPE,
                                       plaintext_password,
                                       scramble,
                                       stored_auth_string,
                                       is_match);
}

int ObSha256Crypt::generate_sha2_digest_for_cache(
    const char *plaintext_password,
    const int64_t plaintext_len,
    unsigned char *digest_output,
    const int64_t digest_len)
{
  return ObCryptCommon::generate_digest_for_cache(
      EVP_sha256(), OB_SHA256_DIGEST_LENGTH, plaintext_password, plaintext_len, digest_output, digest_len);
}

int ObSha256Crypt::verify_fast_auth_scramble(
    const ObString &client_scramble_response,
    const ObString &scramble,
    const ObString &cached_digest,
    bool &is_match)
{
  return ObCryptCommon::verify_fast_auth_scramble(
      EVP_sha256(), OB_SHA256_DIGEST_LENGTH, client_scramble_response, scramble, cached_digest, is_match);
}

int ObSha256Crypt::encrypt_passwd_to_caching_sha2(const ObString &password,
                                                    ObString &encrypted_pass,
                                                    char *enc_buf,
                                                    const int64_t buf_len,
                                                    const int64_t digest_rounds)
{
  return ObCryptCommon::encrypt_passwd(EVP_sha256(),
                                       OB_SHA256_DIGEST_LENGTH,
                                       SHA256_CRYPT_ALG_MAGIC,
                                       OB_AUTH_STRING_DIGEST_TYPE,
                                       password,
                                       encrypted_pass,
                                       enc_buf,
                                       buf_len,
                                       digest_rounds);
}

} // namespace common
} // namespace oceanbase
