/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LIB_ENCRYPT_OB_CRYPT_COMMON_
#define OCEANBASE_LIB_ENCRYPT_OB_CRYPT_COMMON_

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include <openssl/evp.h>

namespace oceanbase
{
namespace common
{

// Common auth-string constants shared by SHA256-crypt and SM3-crypt
static const int64_t OB_CRYPT_AUTH_SALT_LEN = 20;

// Server-generated scramble length, consistent with ObSMConnection::SCRAMBLE_BUF_SIZE
#define SCRAMBLE_LENGTH 20
// Raw digest length in bytes, shared by SHA256 and SM3 (both produce 32-byte digests)
static const int64_t OB_CRYPT_RAW_DIGEST_LEN32 = 32;
// Base64-encoded digest length in characters
static const int64_t OB_CRYPT_AUTH_DIGEST_LEN = 43;
static const int64_t OB_CRYPT_AUTH_ITERATION_LEN = 3;
static const int64_t OB_CRYPT_AUTH_ITERATION_MULTIPLIER = 1000;
static const int64_t OB_CRYPT_AUTH_ROUNDS_DEFAULT = 5000;
static const int64_t OB_CRYPT_AUTH_ROUNDS_MIN = 1000;
static const int64_t OB_CRYPT_AUTH_ROUNDS_MAX = 999999999;
static const int64_t OB_CRYPT_MAX_PASSWORD_SIZE = 256;
static const char OB_CRYPT_AUTH_DELIMITER = '$';
// $<type>$XXX$salt(20)+digest(43)
static const int64_t OB_CRYPT_AUTH_STRING_LEN =
    1 + 1 + 1 + OB_CRYPT_AUTH_ITERATION_LEN + 1 + OB_CRYPT_AUTH_SALT_LEN + OB_CRYPT_AUTH_DIGEST_LEN;

/**
 * Algorithm-agnostic single-shot hash and XOR helper, parameterized on the
 * EVP digest algorithm so SHA256-crypt and SM3-crypt can share one implementation.
 */
class ObCryptHash
{
public:
  /**
   * Single-shot hash using the given EVP digest algorithm.
   *
   * @param md          EVP digest algorithm (e.g. EVP_sha256(), EVP_sm3())
   * @param input       Input data
   * @param input_len   Input data length
   * @param digest      Output buffer (must be at least EVP_MD_size(md) bytes)
   * @param digest_len  Output buffer capacity
   * @param ctx         Optional caller-owned context to reuse across hashes. When null, the
   *                    function creates and destroys a context for this call.
   * @return OB_SUCCESS on success
   */
  static int hash(const EVP_MD *md,
                  const unsigned char *input,
                  const int64_t input_len,
                  unsigned char *digest,
                  const int64_t digest_len,
                  EVP_MD_CTX *ctx = nullptr);

  /**
   * XOR two equal-length byte arrays.
   *
   * @param a    First array
   * @param b    Second array
   * @param out  Output array
   * @param len  Length of each array
   * @return OB_SUCCESS on success
   */
  static int xor_digests(const unsigned char *a,
                         const unsigned char *b,
                         unsigned char *out,
                         const int64_t len);
};

/**
 * Common crypt utilities shared by SHA256-crypt and SM3-crypt.
 *
 * Provides base64 encoding, salt generation, sensitive-data clearing,
 * multi-round hash core, auth-string multi-hash formatting, salt extraction
 * and auth-string serialization/deserialization that is identical across
 * both crypt variants except for the digest-type magic character and EVP digest.
 */
class ObCryptCommon
{
public:
  /**
   * Base64-encode a 32-byte digest into the crypt(3) 43-character form.
   *
   * @param data          32-byte raw digest
   * @param output        Output buffer
   * @param output_len    Output: number of characters written
   * @param max_output_len Capacity of output buffer
   * @return OB_SUCCESS on success
   */
  static int base64_encode_24bit(const unsigned char *data,
                                 char *output,
                                 int64_t &output_len,
                                 int64_t max_output_len);

  /**
   * Generate random salt bytes, sanitised to ASCII and free of '\0' / '$'.
   *
   * @param buffer      Output buffer
   * @param buffer_len  Number of random bytes to generate
   * @return OB_SUCCESS on success
   */
  static int generate_user_salt(char *buffer, const int64_t buffer_len);

  /**
   * Zero-fill a sensitive buffer.
   *
   * @param data  Buffer to clear
   * @param len   Length in bytes
   */
  static void clear_sensitive_data(unsigned char *data, int64_t len);

  /**
   * Compute the glibc-style crypt multi-round mixing algorithm (A/B/C/DP/DS
   * contexts), parameterized on the EVP digest algorithm so that SHA256-crypt
   * and SM3-crypt can share one core routine.
   *
   * Only computes the raw digest bytes; callers own password-length limits,
   * rounds clamping and output string formatting.
   *
   * @param md EVP digest algorithm (e.g. EVP_sha256(), EVP_sm3())
   * @param digest_len Digest length in bytes produced by md
   * @param mixchars Number of mix characters used by the algorithm
   * @param plaintext Plaintext password
   * @param plaintext_len Plaintext password length
   * @param salt Salt value
   * @param salt_len Salt length
   * @param rounds Hash rounds
   * @param allocator Memory allocator
   * @param digest_out Output buffer for the raw digest bytes
   * @param digest_out_capacity Capacity of digest_out, must be >= digest_len
   * @return Error code
   */
  static int generate_crypt_multi_hash(const EVP_MD *md,
                                       const int64_t digest_len,
                                       const int64_t mixchars,
                                       const char *plaintext,
                                       const int64_t plaintext_len,
                                       const char *salt,
                                       const int64_t salt_len,
                                       const int64_t rounds,
                                       ObIAllocator &allocator,
                                       unsigned char *digest_out,
                                       const int64_t digest_out_capacity);

  /**
   * Generate multi-round crypt hash and format in unix-crypt intermediate form:
   * <alg_magic><3-digit-hex-iterations>$<salt>$<base64-digest>
   *
   * This matches the historical ObSha256Crypt intermediate format
   * (e.g. "$5$00A$salt$digest"). Final stored auth strings ($A$ / $S$)
   * are produced separately by serialize_auth_string after extracting
   * salt/digest from this intermediate output.
   *
   * @param md             EVP digest algorithm
   * @param digest_len     Raw digest length in bytes
   * @param mixchars       Mix character count
   * @param alg_magic      Algorithm magic prefix including leading/trailing '$'
   *                       (e.g. "$5$" for SHA256-crypt, "$S$" for SM3-crypt)
   * @param plaintext      Plaintext password
   * @param plaintext_len  Password length
   * @param salt           Salt bytes
   * @param salt_len       Salt length
   * @param rounds         Hash rounds (<=0 uses default)
   * @param allocator      Memory allocator
   * @param output         Output formatted string
   * @return Error code
   */
  static int generate_auth_multi_hash(const EVP_MD *md,
                                      const int64_t digest_len,
                                      const int64_t mixchars,
                                      const char *alg_magic,
                                      const char *plaintext,
                                      const int64_t plaintext_len,
                                      const char *salt,
                                      const int64_t salt_len,
                                      const int64_t rounds,
                                      ObIAllocator &allocator,
                                      ObString &output);

  /**
   * Extract user salt from unix-crypt intermediate multi-hash string.
   * Format: $<type>$XXX$salt$hash
   * Salt is between the 3rd and 4th '$' delimiters.
   *
   * @param crypt_str      Encrypted string
   * @param crypt_str_len  Encrypted string length
   * @param salt_begin     Salt start position (output)
   * @param salt_end       Salt end position (output)
   * @return Salt length on success, 0 on failure
   */
  static int extract_user_salt(const char *crypt_str,
                               const int64_t crypt_str_len,
                               const char **salt_begin,
                               const char **salt_end);

  /**
   * Serialize an auth string: $<digest_type>$<iterations_hex>$<salt><digest>
   *
   * @param digest_type  Magic character ('A' for SHA256, 'S' for SM3)
   * @param salt         Salt bytes
   * @param salt_len     Salt length (must be OB_CRYPT_AUTH_SALT_LEN)
   * @param digest       Base64-encoded digest bytes
   * @param digest_len   Digest length (must be OB_CRYPT_AUTH_DIGEST_LEN)
   * @param iterations   Raw iteration count
   * @param allocator    Memory allocator
   * @param output       Result string
   * @return OB_SUCCESS on success
   */
  static int serialize_auth_string(char digest_type,
                                   const char *salt,
                                   const int64_t salt_len,
                                   const char *digest,
                                   const int64_t digest_len,
                                   const int64_t iterations,
                                   ObIAllocator &allocator,
                                   ObString &output);

  /**
   * Deserialize an auth string: $<digest_type>$<iterations_hex>$<salt><digest>
   *
   * @param expected_digest_type  Expected magic character
   * @param auth_string           Input auth string
   * @param salt                  Output salt
   * @param digest                Output base64-encoded digest
   * @param iterations            Output raw iteration count
   * @return OB_SUCCESS on success
   */
  static int deserialize_auth_string(char expected_digest_type,
                                     const ObString &auth_string,
                                     ObString &salt,
                                     ObString &digest,
                                     int64_t &iterations);

  /**
   * Verify a password (full authentication mode) against a stored auth string,
   * parameterized on the EVP digest algorithm so SHA256-crypt and SM3-crypt
   * can share one implementation.
   *
   * Empty stored_auth_string only matches an empty plaintext_password.
   *
   * @param md                   EVP digest algorithm
   * @param digest_len           Raw digest length in bytes
   * @param intermediate_magic   Intermediate multi-hash magic prefix (e.g. "$5$", "$S$")
   * @param digest_type          Magic character ('A' for SHA256, 'S' for SM3)
   * @param plaintext_password   Plaintext password
   * @param scramble             Server random number (unused, reserved for interface compatibility)
   * @param stored_auth_string   Stored authentication string
   * @param is_match             Output: whether password matches
   * @return OB_SUCCESS on success
   */
  static int check_password(const EVP_MD *md,
                            const int64_t digest_len,
                            const char *intermediate_magic,
                            char digest_type,
                            const ObString &plaintext_password,
                            const ObString &scramble,
                            const ObString &stored_auth_string,
                            bool &is_match);

  /**
   * Generate double digest for cache: digest = MD(MD(plaintext_password)).
   *
   * @param md                 EVP digest algorithm
   * @param digest_len         Raw digest length in bytes
   * @param plaintext_password Plaintext password
   * @param plaintext_len      Password length
   * @param digest_output      Output digest buffer (must be >= digest_len)
   * @param digest_output_len  Output buffer capacity
   * @return OB_SUCCESS on success
   */
  static int generate_digest_for_cache(const EVP_MD *md,
                                       const int64_t digest_len,
                                       const char *plaintext_password,
                                       const int64_t plaintext_len,
                                       unsigned char *digest_output,
                                       const int64_t digest_output_len);

  /**
   * Verify a fast-auth (cache-accelerated) scramble response.
   *
   * stage3 = MD(cached_digest || scramble)
   * expected_stage1 = XOR(client_response, stage3)
   * expected_stage2 = MD(expected_stage1)
   * verify: expected_stage2 == cached_digest
   *
   * @param md                        EVP digest algorithm
   * @param digest_len                Raw digest length in bytes
   * @param client_scramble_response  Client-sent scramble response (digest_len bytes)
   * @param scramble                  Server-generated random number (SCRAMBLE_LENGTH bytes)
   * @param cached_digest             Cached MD(MD(password)) (digest_len bytes)
   * @param is_match                  Output: whether matches
   * @return OB_SUCCESS on success
   */
  static int verify_fast_auth_scramble(const EVP_MD *md,
                                       const int64_t digest_len,
                                       const ObString &client_scramble_response,
                                       const ObString &scramble,
                                       const ObString &cached_digest,
                                       bool &is_match);

  /**
   * Encrypt a password into a stored auth string: $<digest_type>$[iterations]$[salt][digest]
   *
   * Empty password produces an empty encrypted_pass and returns OB_SUCCESS.
   * digest_rounds must be > OB_CRYPT_AUTH_ROUNDS_DEFAULT and a multiple of
   * OB_CRYPT_AUTH_ITERATION_MULTIPLIER, otherwise OB_CRYPT_AUTH_ROUNDS_DEFAULT is used.
   *
   * @param md                 EVP digest algorithm
   * @param digest_len         Raw digest length in bytes
   * @param intermediate_magic Intermediate multi-hash magic prefix (e.g. "$5$", "$S$")
   * @param digest_type        Magic character for the final stored auth string
   * @param password           Plaintext password
   * @param encrypted_pass     Output encrypted password string
   * @param enc_buf            Output buffer
   * @param buf_len            Buffer length
   * @param digest_rounds      Number of hash iterations
   * @return OB_SUCCESS on success
   */
  static int encrypt_passwd(const EVP_MD *md,
                            const int64_t digest_len,
                            const char *intermediate_magic,
                            char digest_type,
                            const ObString &password,
                            ObString &encrypted_pass,
                            char *enc_buf,
                            const int64_t buf_len,
                            const int64_t digest_rounds);
};

}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_LIB_ENCRYPT_OB_CRYPT_COMMON_
