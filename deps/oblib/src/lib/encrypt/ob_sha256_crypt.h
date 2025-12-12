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

#ifndef OB_SHA256_CRYPT_H_
#define OB_SHA256_CRYPT_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"

namespace oceanbase {
namespace common {

// Server-generated scramble length, consistent with ObSMConnection::SCRAMBLE_BUF_SIZE
#define SCRAMBLE_LENGTH 20

// SHA256-crypt related constant definitions
#define OB_SHA256_DIGEST_LENGTH 32  // SHA256 digest length (byte semantics)
#define OB_SHA256_MIXCHARS 32       // Number of mix characters
#define OB_SHA256_SALT_LENGTH 20    // Salt length
#define OB_SHA256_ROUNDS_DEFAULT 5000 // Default rounds
#define OB_SHA256_ROUNDS_MIN 1000     // Minimum rounds
#define OB_SHA256_ROUNDS_MAX 999999999 // Maximum rounds
#define OB_SHA256_MAX_PASSWORD_SIZE 256 // Maximum password length

// Auth string serialization related constants
#define OB_AUTH_STRING_DELIMITER '$'
#define OB_AUTH_STRING_DIGEST_TYPE 'A'  // SHA256
#define OB_AUTH_STRING_ITERATION_LENGTH 3 // Iteration count length (hexadecimal)
#define OB_AUTH_STRING_SALT_LENGTH 20     // Salt length
#define OB_AUTH_STRING_DIGEST_LENGTH 43   // Digest length (after Base64 encoding, byte semantics)
#define OB_AUTH_STRING_ITERATION_MULTIPLIER 1000 // Iteration count multiplier
#define CACHING_SHA2_PASSWD_BUF_LEN (1 + 1 + 1 + OB_AUTH_STRING_ITERATION_LENGTH + 1 + OB_AUTH_STRING_SALT_LENGTH + OB_AUTH_STRING_DIGEST_LENGTH + 1) // Authentication string buffer length (including '\0', byte semantics)

// SHA256-crypt algorithm implementation
class ObSha256Crypt {
public:
  /**
   * Generate SHA256-crypt hash value
   * @param plaintext Plaintext password
   * @param plaintext_len Plaintext password length
   * @param salt Salt value
   * @param salt_len Salt length
   * @param rounds Hash rounds (optional, default uses OB_SHA256_ROUNDS_DEFAULT)
   * @param allocator Memory allocator
   * @param output Output hash string
   * @return Error code
   */
  static int generate_sha256_multi_hash(
      const char *plaintext,
      const int64_t plaintext_len,
      const char *salt,
      const int64_t salt_len,
      const int64_t rounds,
      ObIAllocator &allocator,
      ObString &output);

  /**
   * Generate random salt
   * @param buffer Output buffer
   * @param buffer_len Buffer length
   * @return Error code
   */
  static int generate_user_salt(char *buffer, const int64_t buffer_len);

  /**
   * Extract user salt from encrypted string
   * @param crypt_str Encrypted string
   * @param crypt_str_len Encrypted string length
   * @param salt_begin Salt start position (output)
   * @param salt_end Salt end position (output)
   * @return Salt length
   */
  static int extract_user_salt(const char *crypt_str,
                               const int64_t crypt_str_len,
                               const char **salt_begin,
                               const char **salt_end);

  /**
   * Serialize authentication string
   * Format: $A$[iterations]$[salt][digest]
   * @param digest_type Digest type (currently only SHA256 supported)
   * @param salt Salt value
   * @param salt_len Salt length
   * @param digest Digest
   * @param digest_len Digest length
   * @param iterations Iteration count
   * @param allocator Memory allocator
   * @param output Output serialized string
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
   * Deserialize authentication string
   * Format: $A$[iterations]$[salt][digest]
   * @param auth_string Authentication string
   * @param salt Output salt value
   * @param digest Output digest
   * @param iterations Output iteration count
   * @return Error code
   */
  static int deserialize_auth_string(
      const ObString &auth_string,
      ObString &salt,
      ObString &digest,
      int64_t &iterations);

  /**
   * Verify caching_sha2_password password (full authentication mode)
   * Uses plaintext password for full verification
   *
   * @param plaintext_password Plaintext password
   * @param scramble Server random number (not used in this function, reserved for interface compatibility)
   * @param stored_auth_string Stored authentication string (format: $A$XXX$salt+digest)
   * @param is_match Output: whether password matches
   * @return Error code
   */
  static int check_sha256_password(
      const ObString &plaintext_password,
      const ObString &scramble,
      const ObString &stored_auth_string,
      bool &is_match);

  /**
   * Generate double SHA256 digest for cache
   * digest = SHA256(SHA256(plaintext_password))
   *
   * MySQL caching_sha2_password cache mechanism:
   * - Do not cache plaintext password
   * - Do not cache SHA256(password) (this would be exposed to client)
   * - Cache SHA256(SHA256(password)) for fast authentication
   *
   * @param plaintext_password Plaintext password
   * @param plaintext_len Password length
   * @param digest_output Output 32-byte digest buffer (must be pre-allocated)
   * @return Error code
   */
  static int generate_sha2_digest_for_cache(
      const char *plaintext_password,
      const int64_t plaintext_len,
      unsigned char *digest_output,
      const int64_t digest_len);  // Must be at least 32 bytes

  /**
   * Verify fast authentication scramble response
   *
   * MySQL caching_sha2_password fast authentication algorithm:
   *
   * 1. Client calculation (based on 20-byte server scramble):
   *    stage1 = SHA256(password)                    // 32 bytes
   *    stage2 = SHA256(stage1)                      // 32 bytes, i.e., SHA256(SHA256(password))
   *    stage3 = SHA256(stage2 + server_scramble)    // 32 bytes, note scramble is 20 bytes
   *    client_response = XOR(stage1, stage3)        // 32 bytes XOR 32 bytes = 32 bytes
   *
   * 2. Server verification:
   *    cached_stage2 = SHA256(SHA256(password))     // Get from cache, 32 bytes
   *    stage3 = SHA256(cached_stage2 + server_scramble)  // Use 20-byte scramble
   *    expected_stage1 = XOR(client_response, stage3)
   *    expected_stage2 = SHA256(expected_stage1)
   *    Verify: expected_stage2 == cached_stage2
   *
   * Note:
   * - server_scramble is always 20 bytes (SCRAMBLE_LENGTH = 20)
   * - client_response is 32 bytes (OB_SHA256_DIGEST_LENGTH = 32)
   * - cached_digest is 32 bytes (SHA256(SHA256(password)))
   *
   * @param client_scramble_response Client-sent 32-byte scramble response
   * @param scramble Server-generated 20-byte random number (SCRAMBLE_LENGTH)
   * @param cached_digest Cached SHA256(SHA256(password)) (32 bytes)
   * @param is_match Output: whether matches
   * @return Error code
   */
  static int verify_fast_auth_scramble(
      const ObString &client_scramble_response,  // 32 bytes
      const ObString &scramble,                   // 20 bytes
      const ObString &cached_digest,              // 32 bytes
      bool &is_match);

  /**
   * Encrypt password for caching_sha2_password plugin
   * Generated format: $A$[iterations]$[salt][digest]
   *
   * @param password Plaintext password
   * @param encrypted_pass Output encrypted password string
   * @param enc_buf Output buffer
   * @param buf_len Buffer length
   * @param digest_rounds Number of SHA256 iterations (default: OB_SHA256_ROUNDS_DEFAULT if 0)
   * @return Error code
   */
  static int encrypt_passwd_to_caching_sha2(const ObString &password,
                                              ObString &encrypted_pass,
                                              char *enc_buf,
                                              const int64_t buf_len,
                                              const int64_t digest_rounds = OB_SHA256_ROUNDS_DEFAULT);

private:
  // Internal helper functions
  static int base64_encode_24bit(const unsigned char *data,
                                 char *output,
                                 int64_t &output_len,
                                 int64_t max_output_len);
  static void clear_sensitive_data(unsigned char *data, int64_t len);
};

} // namespace common
} // namespace oceanbase

#endif // OB_SHA256_CRYPT_H_
