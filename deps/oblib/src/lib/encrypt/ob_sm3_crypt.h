/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LIB_ENCRYPT_OB_SM3_CRYPT_
#define OCEANBASE_LIB_ENCRYPT_OB_SM3_CRYPT_

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "lib/encrypt/ob_crypt_common.h"

namespace oceanbase
{
namespace common
{

// SM3 digest length (byte semantics)
#define OB_SM3_DIGEST_LENGTH OB_CRYPT_RAW_DIGEST_LEN32

// SM3 magic character for the final stored auth string ('$S$...')
#define OB_SM3_AUTH_STRING_DIGEST_TYPE 'S'
// Auth string buffer length, including terminating '\0'
#define OB_SM3_PASSWD_BUF_LEN (OB_CRYPT_AUTH_STRING_LEN + 1)

/**
 * SM3 password crypt implementation for ob_sm3_password plugin.
 *
 * Uses SM3 hash (Chinese national standard GB/T 32905-2016) to implement
 * password hashing, verification, fast authentication scramble, and
 * cache digest generation.
 *
 * Auth string format: $S$[iterations]$[salt][digest]
 *   - '$S$' is the SM3 magic prefix (distinct from SHA256's '$A$')
 *   - iterations: 3-digit hexadecimal, actual rounds = iterations * 1000
 *   - salt: 20 bytes of random data
 *   - digest: 32 bytes SM3 hash, base64-encoded to 43 characters
 */
class ObSm3Crypt
{
public:
  /**
   * Generate SM3-crypt multi-round hash in unix-crypt intermediate form:
   * $S$[3-digit-hex]$salt$base64_digest
   * Final stored auth string ($S$... without '$' between salt and digest)
   * is produced by serialize_auth_string.
   *
   * @param plaintext      Plaintext password
   * @param plaintext_len  Plaintext password length
   * @param salt           Salt value
   * @param salt_len       Salt length
   * @param rounds         Hash rounds
   * @param allocator      Memory allocator
   * @param output         Output hash string
   * @return Error code
   */
  static int generate_sm3_multi_hash(
      const char *plaintext,
      const int64_t plaintext_len,
      const char *salt,
      const int64_t salt_len,
      const int64_t rounds,
      ObIAllocator &allocator,
      ObString &output);

  /**
   * Generate random salt for SM3 auth string.
   *
   * @param buffer      Output buffer
   * @param buffer_len  Buffer length
   * @return Error code
   */
  static int generate_user_salt(char *buffer, const int64_t buffer_len);

  /**
   * Extract user salt from unix-crypt intermediate multi-hash string.
   * Format: $S$XXX$salt$digest (salt is between the 3rd and 4th $).
   *
   * @param crypt_str      Encrypted string
   * @param crypt_str_len  Encrypted string length
   * @param salt_begin     Salt start position (output)
   * @param salt_end       Salt end position (output)
   * @return Salt length on success
   */
  static int extract_user_salt(const char *crypt_str,
                               const int64_t crypt_str_len,
                               const char **salt_begin,
                               const char **salt_end);

  /**
   * Serialize SM3 authentication string.
   * Format: $S$[iterations]$[salt][digest]
   *
   * @param salt        Salt value
   * @param salt_len    Salt length
   * @param digest      Digest value (base64-encoded)
   * @param digest_len  Digest length
   * @param iterations  Iteration count
   * @param allocator   Memory allocator
   * @param output      Output serialized string
   * @return Error code
   */
  static int serialize_auth_string(
      const char *salt,
      const int64_t salt_len,
      const char *digest,
      const int64_t digest_len,
      const int64_t iterations,
      ObIAllocator &allocator,
      ObString &output);

  /**
   * Deserialize SM3 authentication string.
   * Format: $S$[iterations]$[salt][digest]
   *
   * @param auth_string  Authentication string
   * @param salt         Output salt value
   * @param digest       Output digest value (base64-encoded)
   * @param iterations   Output iteration count
   * @return Error code
   */
  static int deserialize_auth_string(
      const ObString &auth_string,
      ObString &salt,
      ObString &digest,
      int64_t &iterations);

  /**
   * Verify SM3 password (full authentication mode).
   * Uses plaintext password for full verification against stored $S$ string.
   *
   * @param plaintext_password  Plaintext password
   * @param scramble            Server random number (unused, reserved for interface compatibility)
   * @param stored_auth_string  Stored authentication string ($S$ format)
   * @param is_match            Output: whether password matches
   * @return Error code
   */
  static int check_sm3_password(
      const ObString &plaintext_password,
      const ObString &scramble,
      const ObString &stored_auth_string,
      bool &is_match);

  /**
   * Generate double SM3 digest for cache.
   * digest = SM3(SM3(plaintext_password))
   *
   * Cache stores SM3(SM3(password)) for fast authentication,
   * consistent with caching_sha2_password cache semantics.
   *
   * @param plaintext_password  Plaintext password
   * @param plaintext_len       Password length
   * @param digest_output       Output 32-byte digest buffer
   * @param digest_len          Digest buffer size (must be >= 32)
   * @return Error code
   */
  static int generate_sm3_digest_for_cache(
      const char *plaintext_password,
      const int64_t plaintext_len,
      unsigned char *digest_output,
      const int64_t digest_len);

  /**
   * Verify fast authentication scramble response.
   *
   * SM3 fast authentication algorithm:
   *
   * Client calculation (based on 20-byte server scramble):
   *   stage1 = SM3(password)                     // 32 bytes
   *   stage2 = SM3(stage1)                       // 32 bytes = SM3(SM3(password))
   *   stage3 = SM3(stage2 || server_scramble)    // 32 bytes
   *   client_response = XOR(stage1, stage3)      // 32 bytes
   *
   * Server verification:
   *   cached_stage2 = SM3(SM3(password))         // from cache, 32 bytes
   *   stage3 = SM3(cached_stage2 || server_scramble)
   *   expected_stage1 = XOR(client_response, stage3)
   *   expected_stage2 = SM3(expected_stage1)
   *   verify: expected_stage2 == cached_stage2
   *
   * @param client_scramble_response  Client-sent 32-byte scramble response
   * @param scramble                  Server-generated 20-byte random number
   * @param cached_digest             Cached SM3(SM3(password)) (32 bytes)
   * @param is_match                  Output: whether matches
   * @return Error code
   */
  static int verify_fast_auth_scramble(
      const ObString &client_scramble_response,
      const ObString &scramble,
      const ObString &cached_digest,
      bool &is_match);

  /**
   * Encrypt password for ob_sm3_password plugin.
   * Generated format: $S$[iterations]$[salt][base64_digest]
   *
   * @param password        Plaintext password
   * @param encrypted_pass  Output encrypted password string
   * @param enc_buf         Output buffer
   * @param buf_len         Buffer length
   * @param digest_rounds   Number of SM3 iterations (default: OB_CRYPT_AUTH_ROUNDS_DEFAULT)
   * @return Error code
   */
  static int encrypt_passwd_to_ob_sm3(
      const ObString &password,
      ObString &encrypted_pass,
      char *enc_buf,
      const int64_t buf_len,
      const int64_t digest_rounds = OB_CRYPT_AUTH_ROUNDS_DEFAULT);

};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_ENCRYPT_OB_SM3_CRYPT_
