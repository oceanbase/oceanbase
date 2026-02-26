/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX COMMON

#include "ob_sha256_crypt.h"
#include "ob_encrypted_helper.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_print_utils.h"
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <cstring>
#include <cstdio>

using namespace oceanbase::common;

namespace oceanbase {
namespace common {

// Base64 encoding table
static const char b64_chars[] = "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

// Algorithm identifier
static const char crypt_alg_magic[] = "$5$";
static const int crypt_alg_magic_len = sizeof(crypt_alg_magic) - 1;

int ObSha256Crypt::generate_sha256_multi_hash(
    const char *plaintext,
    const int64_t plaintext_len,
    const char *salt,
    const int64_t salt_len,
    const int64_t rounds,
    ObIAllocator &allocator,
    ObString &output)
{
  int ret = OB_SUCCESS;

  // Parameter check
  if (OB_ISNULL(plaintext) || plaintext_len <= 0 ||
      OB_ISNULL(salt) || salt_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(plaintext), K(plaintext_len),
             KP(salt), K(salt_len));
  } else if (plaintext_len > OB_SHA256_MAX_PASSWORD_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("password too long", K(ret), K(plaintext_len));
  } else {
    // Calculate actual rounds to use
    int64_t actual_rounds = rounds;
    if (actual_rounds <= 0) {
      actual_rounds = OB_SHA256_ROUNDS_DEFAULT;
    } else if (actual_rounds < OB_SHA256_ROUNDS_MIN) {
      actual_rounds = OB_SHA256_ROUNDS_MIN;
    } else if (actual_rounds > OB_SHA256_ROUNDS_MAX) {
      actual_rounds = OB_SHA256_ROUNDS_MAX;
    }

    // Calculate output buffer size
    int64_t output_len = crypt_alg_magic_len + 1 + // $5$
                         20 + // rounds=XXXXX$
                         20 + // salt
                         1 +  // $
                         43;  // hash
    char *output_buf = nullptr;

    if (OB_ISNULL(output_buf = static_cast<char*>(allocator.alloc(output_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for output buffer", K(ret), K(output_len));
    } else {
      // Initialize SHA256 context
      EVP_MD_CTX *ctx_a = nullptr;
      EVP_MD_CTX *ctx_b = nullptr;
      EVP_MD_CTX *ctx_c = nullptr;
      EVP_MD_CTX *ctx_dp = nullptr;
      EVP_MD_CTX *ctx_ds = nullptr;

      if (OB_ISNULL(ctx_a = EVP_MD_CTX_create()) ||
          OB_ISNULL(ctx_b = EVP_MD_CTX_create()) ||
          OB_ISNULL(ctx_c = EVP_MD_CTX_create()) ||
          OB_ISNULL(ctx_dp = EVP_MD_CTX_create()) ||
          OB_ISNULL(ctx_ds = EVP_MD_CTX_create())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create EVP context", K(ret));
      } else {
        // Prepare salt
        const char *actual_salt = salt;
        int64_t actual_salt_len = salt_len;

        // Skip algorithm identifier
        if (strncmp(salt, crypt_alg_magic, crypt_alg_magic_len) == 0) {
          actual_salt += crypt_alg_magic_len + 1;
          actual_salt_len -= crypt_alg_magic_len + 1;
        }

        // Step 1: Initialize context A
        if (OB_SUCC(ret) && !EVP_DigestInit_ex(ctx_a, EVP_sha256(), nullptr)) {
          ret = OB_ERR_AES_ENCRYPT;
          LOG_WARN("fail to init digest context A", K(ret));
        }

        // Step 2-3: Update password and salt
        if (OB_SUCC(ret) &&
            (!EVP_DigestUpdate(ctx_a, plaintext, plaintext_len) ||
             !EVP_DigestUpdate(ctx_a, actual_salt, actual_salt_len))) {
          ret = OB_ERR_AES_ENCRYPT;
          LOG_WARN("fail to update digest context A", K(ret));
        }

        // Step 4-8: Calculate B
        unsigned char B[OB_SHA256_DIGEST_LENGTH];
        if (OB_SUCC(ret)) {
          if (!EVP_DigestInit_ex(ctx_b, EVP_sha256(), nullptr) ||
              !EVP_DigestUpdate(ctx_b, plaintext, plaintext_len) ||
              !EVP_DigestUpdate(ctx_b, actual_salt, actual_salt_len) ||
              !EVP_DigestUpdate(ctx_b, plaintext, plaintext_len) ||
              !EVP_DigestFinal_ex(ctx_b, B, nullptr)) {
            ret = OB_ERR_AES_ENCRYPT;
            LOG_WARN("fail to calculate B", K(ret));
          }
        }

        // Step 9-11: Update context A
        if (OB_SUCC(ret)) {
          int64_t i = plaintext_len;
          while (OB_SUCC(ret) && i > OB_SHA256_MIXCHARS) {
            if (!EVP_DigestUpdate(ctx_a, B, OB_SHA256_MIXCHARS)) {
              ret = OB_ERR_AES_ENCRYPT;
              LOG_WARN("fail to update digest context A with B", K(ret));
            }
            i -= OB_SHA256_MIXCHARS;
          }
          if (OB_SUCC(ret) && i > 0) {
            if (!EVP_DigestUpdate(ctx_a, B, i)) {
              ret = OB_ERR_AES_ENCRYPT;
              LOG_WARN("fail to update digest context A with remaining B", K(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          int64_t i = plaintext_len;
          while (OB_SUCC(ret) && i > 0) {
            if ((i & 1) != 0) {
              if (!EVP_DigestUpdate(ctx_a, B, OB_SHA256_MIXCHARS)) {
                ret = OB_ERR_AES_ENCRYPT;
                LOG_WARN("fail to update digest context A with B in loop", K(ret));
              }
            } else {
              if (!EVP_DigestUpdate(ctx_a, plaintext, plaintext_len)) {
                ret = OB_ERR_AES_ENCRYPT;
                LOG_WARN("fail to update digest context A with plaintext in loop", K(ret));
              }
            }
            i >>= 1;
          }
        }

        // Step 12: Get A
        unsigned char A[OB_SHA256_DIGEST_LENGTH];
        if (OB_SUCC(ret) && !EVP_DigestFinal_ex(ctx_a, A, nullptr)) {
          ret = OB_ERR_AES_ENCRYPT;
          LOG_WARN("fail to finalize digest context A", K(ret));
        }

        // Step 13-15: Calculate DP
        unsigned char DP[OB_SHA256_DIGEST_LENGTH];
        if (OB_SUCC(ret)) {
          if (!EVP_DigestInit_ex(ctx_dp, EVP_sha256(), nullptr)) {
            ret = OB_ERR_AES_ENCRYPT;
            LOG_WARN("fail to init digest context DP", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < plaintext_len; i++) {
              if (!EVP_DigestUpdate(ctx_dp, plaintext, plaintext_len)) {
                ret = OB_ERR_AES_ENCRYPT;
                LOG_WARN("fail to update digest context DP", K(ret));
              }
            }
            if (OB_SUCC(ret) && !EVP_DigestFinal_ex(ctx_dp, DP, nullptr)) {
              ret = OB_ERR_AES_ENCRYPT;
              LOG_WARN("fail to finalize digest context DP", K(ret));
            }
          }
        }

        // Step 16: Prepare P
        char *P = nullptr;
        if (OB_SUCC(ret)) {
          int64_t p_len = plaintext_len;
          if (OB_ISNULL(P = static_cast<char*>(allocator.alloc(p_len)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory for P", K(ret), K(p_len));
          } else {
            char *Pp = P;
            int64_t i = plaintext_len;
            while (i >= OB_SHA256_MIXCHARS) {
              memcpy(Pp, DP, OB_SHA256_MIXCHARS);
              Pp += OB_SHA256_MIXCHARS;
              i -= OB_SHA256_MIXCHARS;
            }
            if (i > 0) {
              memcpy(Pp, DP, i);
            }
          }
        }

        // Step 17-19: Calculate DS
        unsigned char DS[OB_SHA256_DIGEST_LENGTH];
        if (OB_SUCC(ret)) {
          if (!EVP_DigestInit_ex(ctx_ds, EVP_sha256(), nullptr)) {
            ret = OB_ERR_AES_ENCRYPT;
            LOG_WARN("fail to init digest context DS", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < 16U + A[0]; i++) {
              if (!EVP_DigestUpdate(ctx_ds, actual_salt, actual_salt_len)) {
                ret = OB_ERR_AES_ENCRYPT;
                LOG_WARN("fail to update digest context DS", K(ret));
              }
            }
            if (OB_SUCC(ret) && !EVP_DigestFinal_ex(ctx_ds, DS, nullptr)) {
              ret = OB_ERR_AES_ENCRYPT;
              LOG_WARN("fail to finalize digest context DS", K(ret));
            }
          }
        }

        // Step 20: Prepare S
        char *S = nullptr;
        if (OB_SUCC(ret)) {
          int64_t s_len = actual_salt_len;
          if (OB_ISNULL(S = static_cast<char*>(allocator.alloc(s_len)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory for S", K(ret), K(s_len));
          } else {
            char *Sp = S;
            int64_t i = actual_salt_len;
            while (i >= OB_SHA256_MIXCHARS) {
              memcpy(Sp, DS, OB_SHA256_MIXCHARS);
              Sp += OB_SHA256_MIXCHARS;
              i -= OB_SHA256_MIXCHARS;
            }
            if (i > 0) {
              memcpy(Sp, DS, i);
            }
          }
        }

        // Step 21: Main loop
        if (OB_SUCC(ret)) {
          for (int64_t i = 0; OB_SUCC(ret) && i < actual_rounds; i++) {
            if (!EVP_DigestInit_ex(ctx_c, EVP_sha256(), nullptr)) {
              ret = OB_ERR_AES_ENCRYPT;
              LOG_WARN("fail to init digest context C", K(ret));
            }

            if ((i & 1) != 0) {
              if (!EVP_DigestUpdate(ctx_c, P, plaintext_len)) {
                ret = OB_ERR_AES_ENCRYPT;
                LOG_WARN("fail to update digest context C with P", K(ret));
              }
            } else {
              if (i == 0) {
                if (!EVP_DigestUpdate(ctx_c, A, OB_SHA256_MIXCHARS)) {
                  ret = OB_ERR_AES_ENCRYPT;
                  LOG_WARN("fail to update digest context C with A", K(ret));
                }
              } else {
                if (!EVP_DigestUpdate(ctx_c, DP, OB_SHA256_MIXCHARS)) {
                  ret = OB_ERR_AES_ENCRYPT;
                  LOG_WARN("fail to update digest context C with DP", K(ret));
                }
              }
            }

            if (OB_SUCC(ret) && i % 3 != 0) {
              if (!EVP_DigestUpdate(ctx_c, S, actual_salt_len)) {
                ret = OB_ERR_AES_ENCRYPT;
                LOG_WARN("fail to update digest context C with S", K(ret));
              }
            }

            if (OB_SUCC(ret) && i % 7 != 0) {
              if (!EVP_DigestUpdate(ctx_c, P, plaintext_len)) {
                ret = OB_ERR_AES_ENCRYPT;
                LOG_WARN("fail to update digest context C with P in mod 7", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              if ((i & 1) != 0) {
                if (i == 0) {
                  if (!EVP_DigestUpdate(ctx_c, A, OB_SHA256_MIXCHARS)) {
                    ret = OB_ERR_AES_ENCRYPT;
                    LOG_WARN("fail to update digest context C with A in odd", K(ret));
                  }
                } else {
                  if (!EVP_DigestUpdate(ctx_c, DP, OB_SHA256_MIXCHARS)) {
                    ret = OB_ERR_AES_ENCRYPT;
                    LOG_WARN("fail to update digest context C with DP in odd", K(ret));
                  }
                }
              } else {
                if (!EVP_DigestUpdate(ctx_c, P, plaintext_len)) {
                  ret = OB_ERR_AES_ENCRYPT;
                  LOG_WARN("fail to update digest context C with P in even", K(ret));
                }
              }
            }

            if (OB_SUCC(ret) && !EVP_DigestFinal_ex(ctx_c, DP, nullptr)) {
              ret = OB_ERR_AES_ENCRYPT;
              LOG_WARN("fail to finalize digest context C", K(ret));
            }
          }
        }

        // Step 22: Generate output string
        if (OB_SUCC(ret)) {
          char *p = output_buf;
          int64_t remaining_len = output_len;

          // Write algorithm identifier and rounds (3-digit hexadecimal)
          // Format: $5$[3-digit-hex]$salt$hash
          // The 3-digit hex represents rounds/1000 (e.g., 10000 rounds = 0x0A = "00A")
          int64_t rounds_encoded = actual_rounds / 1000;
          if (rounds_encoded < 0 || rounds_encoded > 0xFFF) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("rounds out of range for 3-digit hex encoding", K(ret), K(actual_rounds), K(rounds_encoded));
          } else {
            int written = snprintf(p, remaining_len, "%s%03lX$", crypt_alg_magic,
                                 static_cast<unsigned long>(rounds_encoded));
            if (written < 0 || written >= remaining_len) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to write rounds info", K(ret));
            } else {
              p += written;
              remaining_len -= written;
            }
          }

          // Write salt
          if (OB_SUCC(ret)) {
            if (remaining_len < actual_salt_len + 1) {  // +1 for '$'
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("insufficient buffer space for salt", K(ret));
            } else {
              memcpy(p, actual_salt, actual_salt_len);
              p += actual_salt_len;
              *p++ = '$';
              remaining_len -= (actual_salt_len + 1);
            }
          }

          // Write hash value (Base64 encoded)
          if (OB_SUCC(ret)) {
            int64_t hash_output_len = 0;
            if (OB_FAIL(base64_encode_24bit(DP, p, hash_output_len, remaining_len))) {
              LOG_WARN("fail to encode hash", K(ret));
            } else {
              p += hash_output_len;
              remaining_len -= hash_output_len;
            }
          }

          if (OB_SUCC(ret)) {
            output.assign_ptr(output_buf, static_cast<int32_t>(p - output_buf));
          }
        }

        // Clear sensitive data
        if (OB_NOT_NULL(P)) {
          clear_sensitive_data(reinterpret_cast<unsigned char*>(P), plaintext_len);
          allocator.free(P);
        }
        if (OB_NOT_NULL(S)) {
          clear_sensitive_data(reinterpret_cast<unsigned char*>(S), actual_salt_len);
          allocator.free(S);
        }
        clear_sensitive_data(A, sizeof(A));
        clear_sensitive_data(B, sizeof(B));
        clear_sensitive_data(DP, sizeof(DP));
        clear_sensitive_data(DS, sizeof(DS));

        // Clean up context
        if (OB_NOT_NULL(ctx_a)) EVP_MD_CTX_destroy(ctx_a);
        if (OB_NOT_NULL(ctx_b)) EVP_MD_CTX_destroy(ctx_b);
        if (OB_NOT_NULL(ctx_c)) EVP_MD_CTX_destroy(ctx_c);
        if (OB_NOT_NULL(ctx_dp)) EVP_MD_CTX_destroy(ctx_dp);
        if (OB_NOT_NULL(ctx_ds)) EVP_MD_CTX_destroy(ctx_ds);
      }

      if (OB_SUCCESS != ret && OB_NOT_NULL(output_buf)) {
        allocator.free(output_buf);
      }
    }
  }

  return ret;
}

int ObSha256Crypt::generate_user_salt(char *buffer, const int64_t buffer_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buffer) || buffer_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buffer), K(buffer_len));
  } else {
    // Generate random bytes
    if (RAND_bytes(reinterpret_cast<unsigned char*>(buffer), buffer_len) != 1) {
      ret = OB_ERR_AES_ENCRYPT;
      LOG_WARN("fail to generate random bytes", K(ret));
    } else {
      // Ensure valid UTF-8 string, avoid delimiter
      // Process all bytes in the buffer without reserving space for '\0'
      for (int64_t i = 0; i < buffer_len; i++) {
        buffer[i] &= 0x7f;  // Ensure ASCII range
        if (buffer[i] == '\0' || buffer[i] == '$') {
          buffer[i] = buffer[i] + 1;
        }
      }
    }
  }

  return ret;
}

int ObSha256Crypt::extract_user_salt(const char *crypt_str,
                                     const int64_t crypt_str_len,
                                     const char **salt_begin,
                                     const char **salt_end)
{
  int ret = OB_SUCCESS;
  int salt_len = 0;

  if (OB_ISNULL(crypt_str) || crypt_str_len <= 0 ||
      OB_ISNULL(salt_begin) || OB_ISNULL(salt_end)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(crypt_str), K(crypt_str_len),
             KP(salt_begin), KP(salt_end));
  } else {
    const char *it = crypt_str;
    const char *end = crypt_str + crypt_str_len;
    int delimiter_count = 0;

    // Format: $A$[3-digit-hex]$salt$hash
    // Find the 3rd $ delimiter (after 3-digit hex), salt is between 3rd and 4th $
    while (it < end) {
      if (*it == '$') {
        ++delimiter_count;
        if (delimiter_count == 3) {
          *salt_begin = it + 1;
        }
        if (delimiter_count == 4) {
          *salt_end = it;
          break;
        }
      }
      ++it;
    }

    if (OB_NOT_NULL(*salt_begin) && OB_NOT_NULL(*salt_end)) {
      salt_len = *salt_end - *salt_begin;
      if (salt_len != OB_SHA256_SALT_LENGTH) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid salt length", K(ret), K(salt_len), K(OB_SHA256_SALT_LENGTH));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to extract salt from crypt string", K(ret));
    }
  }

  return salt_len;
}

// Helper function: Base64 encode 24-bit data
// Parameters:
//   w - 24-bit data to encode
//   n - Number of 6-bit groups to encode
//   p - Reference to output pointer
//   remaining - Reference to remaining space
// Return value: Number of characters actually written
static int encode_24bit_helper(uint32_t w, int n, char *&p, int64_t &remaining)
{
  int written = 0;
  while (--n >= 0 && remaining > 0) {
    *p++ = b64_chars[w & 0x3f];
    w >>= 6;
    remaining--;
    written++;
  }
  return written;
}

int ObSha256Crypt::base64_encode_24bit(const unsigned char *data,
                                       char *output,
                                       int64_t &output_len,
                                       int64_t max_output_len)
{
  int ret = OB_SUCCESS;
  output_len = 0;

  if (OB_ISNULL(data) || OB_ISNULL(output) || max_output_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(data), KP(output), K(max_output_len));
  } else {
    char *p = output;
    int64_t remaining = max_output_len;

    // Base64 encode according to SHA256-crypt specification
    // Encode specific positions of DP array
    output_len += encode_24bit_helper((data[0] << 16) | (data[10] << 8) | data[20], 4, p, remaining);
    output_len += encode_24bit_helper((data[21] << 16) | (data[1] << 8) | data[11], 4, p, remaining);
    output_len += encode_24bit_helper((data[12] << 16) | (data[22] << 8) | data[2], 4, p, remaining);
    output_len += encode_24bit_helper((data[3] << 16) | (data[13] << 8) | data[23], 4, p, remaining);
    output_len += encode_24bit_helper((data[24] << 16) | (data[4] << 8) | data[14], 4, p, remaining);
    output_len += encode_24bit_helper((data[15] << 16) | (data[25] << 8) | data[5], 4, p, remaining);
    output_len += encode_24bit_helper((data[6] << 16) | (data[16] << 8) | data[26], 4, p, remaining);
    output_len += encode_24bit_helper((data[27] << 16) | (data[7] << 8) | data[17], 4, p, remaining);
    output_len += encode_24bit_helper((data[18] << 16) | (data[28] << 8) | data[8], 4, p, remaining);
    output_len += encode_24bit_helper((data[9] << 16) | (data[19] << 8) | data[29], 4, p, remaining);
    output_len += encode_24bit_helper((0 << 16) | (data[31] << 8) | data[30], 3, p, remaining);
  }

  return ret;
}

void ObSha256Crypt::clear_sensitive_data(unsigned char *data, int64_t len)
{
  if (OB_NOT_NULL(data) && len > 0) {
    memset(data, 0, len);
  }
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
  int ret = OB_SUCCESS;

  // Parameter check
  if (OB_ISNULL(salt) || salt_len <= 0 ||
      OB_ISNULL(digest) || digest_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(salt), K(salt_len),
             KP(digest), K(digest_len));
  } else if (salt_len != OB_AUTH_STRING_SALT_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid salt length", K(ret), K(salt_len), K(OB_AUTH_STRING_SALT_LENGTH));
  } else if (digest_len != OB_AUTH_STRING_DIGEST_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid digest length", K(ret), K(digest_len), K(OB_AUTH_STRING_DIGEST_LENGTH));
  } else if (iterations < OB_SHA256_ROUNDS_MIN || iterations > OB_SHA256_ROUNDS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iterations", K(ret), K(iterations), K(OB_SHA256_ROUNDS_MIN), K(OB_SHA256_ROUNDS_MAX));
  } else {
    // Calculate output buffer size
    // Format: $A$[iterations]$[salt][digest]
    int64_t output_len = 1 + // $
                           1 + // A
                           1 + // $
                           3 + // iterations (3-digit hexadecimal)
                           1 + // $
                           salt_len + // salt
                           digest_len; // digest

    char *output_buf = nullptr;
    if (OB_ISNULL(output_buf = static_cast<char*>(allocator.alloc(output_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for output buffer", K(ret), K(output_len));
    } else {
      char *p = output_buf;
      int64_t remaining_len = output_len;

      // Write delimiter and digest type
      if (remaining_len < 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insufficient buffer space", K(ret));
      } else {
        *p++ = OB_AUTH_STRING_DELIMITER;
        *p++ = OB_AUTH_STRING_DIGEST_TYPE;
        remaining_len -= 2;
      }

      // Write iteration count (3-digit hexadecimal)
      if (OB_SUCC(ret)) {
        if (remaining_len < 4) { // 1 + 3
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("insufficient buffer space for iterations", K(ret));
        } else {
          *p++ = OB_AUTH_STRING_DELIMITER;
          int written = snprintf(p, remaining_len - 1, "%03lX",
                               static_cast<unsigned long>(iterations / OB_AUTH_STRING_ITERATION_MULTIPLIER));
          if (written < 0 || written >= remaining_len - 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to write iterations", K(ret));
          } else {
            p += written;
            remaining_len -= written + 1;
          }
        }
      }

      // Write delimiter
      if (OB_SUCC(ret)) {
        if (remaining_len < 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("insufficient buffer space for delimiter", K(ret));
        } else {
          *p++ = OB_AUTH_STRING_DELIMITER;
          remaining_len -= 1;
        }
      }

      // Write salt
      if (OB_SUCC(ret)) {
        if (remaining_len < salt_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("insufficient buffer space for salt", K(ret));
        } else {
          memcpy(p, salt, salt_len);
          p += salt_len;
          remaining_len -= salt_len;
        }
      }

      // Write digest
      if (OB_SUCC(ret)) {
        if (remaining_len < digest_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("insufficient buffer space for digest", K(ret));
        } else {
          memcpy(p, digest, digest_len);
          p += digest_len;
          remaining_len -= digest_len;
        }
      }

      if (OB_SUCC(ret)) {
        output.assign_ptr(output_buf, static_cast<int32_t>(p - output_buf));
      }

      if (OB_SUCCESS != ret && OB_NOT_NULL(output_buf)) {
        allocator.free(output_buf);
      }
    }
  }

  return ret;
}

int ObSha256Crypt::deserialize_auth_string(
    const ObString &auth_string,
    ObString &salt,
    ObString &digest,
    int64_t &iterations)
{
  int ret = OB_SUCCESS;

  // Parameter check
  if (auth_string.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty auth string", K(ret));
  } else {
    const char *str = auth_string.ptr();
    int64_t len = auth_string.length();
    int64_t pos = 0;

    // Check format: $A$[iterations]$[salt][digest]
    if (len < 6) { // Minimum length: $A$000$
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("auth string too short", K(ret), K(len));
    } else if (str[pos++] != OB_AUTH_STRING_DELIMITER) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid auth string format: missing first delimiter", K(ret));
    } else if (str[pos++] != OB_AUTH_STRING_DIGEST_TYPE) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid auth string format: invalid digest type", K(ret), K(str[pos-1]));
    } else if (str[pos++] != OB_AUTH_STRING_DELIMITER) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid auth string format: missing second delimiter", K(ret));
    } else {
      // Parse iteration count
      if (pos + OB_AUTH_STRING_ITERATION_LENGTH > len) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid auth string format: incomplete iterations", K(ret));
      } else {
        char iteration_str[OB_AUTH_STRING_ITERATION_LENGTH + 1] = {0};
        memcpy(iteration_str, str + pos, OB_AUTH_STRING_ITERATION_LENGTH);
        pos += OB_AUTH_STRING_ITERATION_LENGTH;

        char *end_ptr = nullptr;
        errno = 0;
        unsigned long iteration_count = strtoul(iteration_str, &end_ptr, 16);

        if (errno != 0 || *end_ptr != '\0') {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid auth string format: invalid iteration count", K(ret), K(iteration_str));
        } else {
          iterations = static_cast<int64_t>(iteration_count * OB_AUTH_STRING_ITERATION_MULTIPLIER);
        }
      }

      // Check third delimiter
      if (OB_SUCC(ret)) {
        if (pos >= len || str[pos++] != OB_AUTH_STRING_DELIMITER) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid auth string format: missing third delimiter", K(ret));
        }
      }

      // Parse salt
      if (OB_SUCC(ret)) {
        if (pos + OB_AUTH_STRING_SALT_LENGTH > len) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid auth string format: incomplete salt", K(ret));
        } else {
          salt.assign_ptr(str + pos, static_cast<int32_t>(OB_AUTH_STRING_SALT_LENGTH));
          pos += OB_AUTH_STRING_SALT_LENGTH;
        }
      }

      // Parse digest
      if (OB_SUCC(ret)) {
        int64_t remaining_len = len - pos;
        // Strictly verify digest length must be 43 characters (after base64 encoding)
        if (remaining_len != OB_AUTH_STRING_DIGEST_LENGTH) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid auth string format: invalid digest length",
                  K(ret), K(remaining_len), "expected", OB_AUTH_STRING_DIGEST_LENGTH);
        } else {
          // Verify base64 format: each character must be in the base64 character set
          bool is_valid_base64 = true;
          const char *digest_start = str + pos;
          for (int64_t i = 0; i < OB_AUTH_STRING_DIGEST_LENGTH && is_valid_base64; ++i) {
            char c = digest_start[i];
            // Check if character is in base64 character set: ./0-9A-Za-z
            if (!((c >= '.' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'))) {
              is_valid_base64 = false;
            }
          }
          if (!is_valid_base64) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid auth string format: invalid base64 digest format",
                    K(ret), "digest", ObString(static_cast<int32_t>(OB_AUTH_STRING_DIGEST_LENGTH), digest_start));
          } else {
            digest.assign_ptr(str + pos, static_cast<int32_t>(OB_AUTH_STRING_DIGEST_LENGTH));
          }
        }
      }
    }
  }

  return ret;
}

/**
 * 验证 caching_sha2_password 的密码（完整认证模式）
 * 使用明文密码进行完整验证
 *
 * @param plaintext_password 明文密码
 * @param scramble 服务器随机数（此函数中不使用，保留用于接口兼容）
 * @param stored_auth_string 存储的认证字符串（格式：$A$XXX$salt+digest）
 * @param is_match 输出：密码是否匹配
 * @return 错误码
 */
int ObSha256Crypt::check_sha256_password(
    const ObString &plaintext_password,
    const ObString &scramble,
    const ObString &stored_auth_string,
    bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  ObArenaAllocator allocator("SHA256Check");  // Use local temporary allocator

  UNUSED(scramble);  // Full authentication does not use scramble

  // Verify input parameters
  if (stored_auth_string.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stored_auth_string is empty", K(ret));
  } else if (plaintext_password.empty()) {
    // Empty password case: client sends empty password, stored should also be empty password
    if (stored_auth_string.empty()) {
      is_match = true;
    }
  } else {
    // Step 1: Deserialize stored authentication string, get salt, digest, iterations
    ObString stored_salt;
    ObString stored_digest_base64;
    int64_t iterations = 0;

    if (OB_FAIL(deserialize_auth_string(stored_auth_string, stored_salt, stored_digest_base64, iterations))) {
      LOG_WARN("failed to deserialize auth string", K(ret), K(stored_auth_string));
    } else {
      // ========== Full Authentication: Verify using plaintext password ==========
      LOG_DEBUG("caching_sha2_password: Full Authentication mode",
                K(plaintext_password.length()), K(stored_salt.length()));

      // Step 2: Generate SHA256 hash using plaintext password and salt (Unix crypt format)
      ObString unix_format_hash;
      if (OB_FAIL(generate_sha256_multi_hash(
              plaintext_password.ptr(),
              plaintext_password.length(),
              stored_salt.ptr(),
              stored_salt.length(),
              iterations,
              allocator,
              unix_format_hash))) {
        LOG_WARN("failed to generate sha256 hash", K(ret));
      } else {
        // Step 3: Extract digest from Unix format hash
        // unix_format_hash format: $A$[3-digit-hex]$salt$digest
        const char *salt_begin = nullptr;
        const char *salt_end = nullptr;
        int salt_len = extract_user_salt(unix_format_hash.ptr(), unix_format_hash.length(), &salt_begin, &salt_end);

        if (salt_len <= 0 || salt_begin == nullptr || salt_end == nullptr) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to extract salt from computed hash", K(ret));
        } else {
          // Find start position of digest (after last $)
          const char *digest_begin = salt_end + 1;
          int64_t digest_len = unix_format_hash.length() - (digest_begin - unix_format_hash.ptr());

          // Step 4: Re-serialize using original stored_salt and calculated digest
          // This ensures salt part is exactly consistent with stored
          ObString computed_auth_string;
          if (OB_FAIL(serialize_auth_string(
                  stored_salt.ptr(), stored_salt.length(),  // Use original stored_salt
                  digest_begin, digest_len,
                  iterations,
                  allocator,
                  computed_auth_string))) {
            LOG_WARN("failed to serialize computed auth string", K(ret));
          } else {
            // Step 5: Compare two authentication strings
            if (0 == stored_auth_string.compare(computed_auth_string)) {
              is_match = true;
              LOG_DEBUG("caching_sha2_password: Full Authentication succeeded");
            } else {
              LOG_DEBUG("caching_sha2_password: Full Authentication failed - auth string mismatch",
                       K(computed_auth_string), K(stored_auth_string));
            }
          }
        }
      }
    }
  }

  return ret;
}

/**
 * 生成用于缓存的双重 SHA256 摘要
 * digest = SHA256(SHA256(plaintext_password))
 */
int ObSha256Crypt::generate_sha2_digest_for_cache(
    const char *plaintext_password,
    const int64_t plaintext_len,
    unsigned char *digest_output,
    const int64_t digest_len)
{
  int ret = OB_SUCCESS;
  unsigned char stage1[OB_SHA256_DIGEST_LENGTH];

  if (OB_ISNULL(plaintext_password) || plaintext_len <= 0 ||
      OB_ISNULL(digest_output) || digest_len != OB_SHA256_DIGEST_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(plaintext_password),
             K(plaintext_len), KP(digest_output));
  } else {
    // Stage 1: SHA256(plaintext_password)
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, plaintext_password, plaintext_len);
    SHA256_Final(stage1, &ctx);

    // Stage 2: SHA256(SHA256(plaintext_password))
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, stage1, OB_SHA256_DIGEST_LENGTH);
    SHA256_Final(digest_output, &ctx);

    // Clear sensitive data
    MEMSET(stage1, 0, sizeof(stage1));

    LOG_DEBUG("generated sha2 digest for cache", K(plaintext_len));
  }

  return ret;
}

/**
 * 验证快速认证的 scramble 响应
 */
int ObSha256Crypt::verify_fast_auth_scramble(
    const ObString &client_scramble_response,
    const ObString &scramble,
    const ObString &cached_digest,
    bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;

  // Parameter validation
  // client_scramble_response: 32 bytes (SHA256 response)
  // scramble: 20 bytes (server random number)
  // cached_digest: 32 bytes (SHA256(SHA256(password)))
  if (client_scramble_response.length() != OB_SHA256_DIGEST_LENGTH ||
      scramble.length() != SCRAMBLE_LENGTH ||  // Must be 20 bytes
      cached_digest.length() != OB_SHA256_DIGEST_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments for fast auth verification",
             K(ret),
             K(client_scramble_response.length()),
             K(scramble.length()),
             K(cached_digest.length()),
             "expected_client_response_len", OB_SHA256_DIGEST_LENGTH,
             "expected_scramble_len", SCRAMBLE_LENGTH,
             "expected_cached_digest_len", OB_SHA256_DIGEST_LENGTH);
  } else {
    // Step 1: Calculate stage3 = SHA256(cached_digest + scramble)
    unsigned char stage3[SHA256_DIGEST_LENGTH];
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, cached_digest.ptr(), cached_digest.length());
    SHA256_Update(&ctx, scramble.ptr(), scramble.length());
    SHA256_Final(stage3, &ctx);

    // Step 2: Calculate expected_stage1 = XOR(client_response, stage3)
    unsigned char expected_stage1[SHA256_DIGEST_LENGTH];
    const unsigned char *client_response =
        reinterpret_cast<const unsigned char*>(client_scramble_response.ptr());

    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
      expected_stage1[i] = client_response[i] ^ stage3[i];
    }

    // Step 3: Calculate expected_stage2 = SHA256(expected_stage1)
    unsigned char expected_stage2[SHA256_DIGEST_LENGTH];
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, expected_stage1, SHA256_DIGEST_LENGTH);
    SHA256_Final(expected_stage2, &ctx);

    // Step 4: Verify expected_stage2 == cached_digest
    if (0 == MEMCMP(expected_stage2, cached_digest.ptr(), SHA256_DIGEST_LENGTH)) {
      is_match = true;
      LOG_DEBUG("fast auth verification succeeded");
    } else {
      LOG_DEBUG("fast auth verification failed - digest mismatch");
    }

    // Clear sensitive data
    MEMSET(stage3, 0, sizeof(stage3));
    MEMSET(expected_stage1, 0, sizeof(expected_stage1));
    MEMSET(expected_stage2, 0, sizeof(expected_stage2));
  }

  return ret;
}

/**
 * 加密密码用于 caching_sha2_password 插件
 *
 * 参数:
 *   password: 明文密码
 *   encrypted_pass: 输出的加密密码字符串
 *   enc_buf: 输出缓冲区
 *   buf_len: 缓冲区长度
 *
 * 返回值:
 *   OB_SUCCESS: 成功
 *   其他: 失败错误码
 */
int ObSha256Crypt::encrypt_passwd_to_caching_sha2(const ObString &password,
                                                    ObString &encrypted_pass,
                                                    char *enc_buf,
                                                    const int64_t buf_len,
                                                    const int64_t digest_rounds)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(enc_buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid enc_buf", K(ret), KP(enc_buf), K(buf_len));
  } else {
    // Use provided digest_rounds, or default if 0
    int64_t actual_rounds = (digest_rounds > 5000 && digest_rounds % 1000 == 0) ? digest_rounds : OB_SHA256_ROUNDS_DEFAULT;

    // Generate random salt
    char salt_buf[OB_AUTH_STRING_SALT_LENGTH] = {0};
    if (OB_FAIL(generate_user_salt(salt_buf, OB_AUTH_STRING_SALT_LENGTH))) {
      LOG_WARN("failed to generate user salt", K(ret));
    } else {
      // Generate password hash using SHA256 multi-round hash
      ModulePageAllocator allocator("CachingSha2");
      ObString hash_result;
      if (OB_FAIL(generate_sha256_multi_hash(
                      password.ptr(), password.length(),
                      salt_buf, OB_AUTH_STRING_SALT_LENGTH,
                      actual_rounds,
                      allocator,
                      hash_result))) {
        LOG_WARN("failed to generate sha256 hash", K(ret));
      } else {
        // Extract salt and digest from generated hash
        const char *salt_begin = nullptr;
        const char *salt_end = nullptr;
        int salt_len = extract_user_salt(hash_result.ptr(), hash_result.length(), &salt_begin, &salt_end);

        if (salt_len <= 0 || salt_begin == nullptr || salt_end == nullptr) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to extract salt from hash", K(ret));
        } else {
          // Find start position of hash value (after last $)
          const char *digest_begin = salt_end + 1;
          int64_t digest_len = hash_result.length() - (digest_begin - hash_result.ptr());

          // Serialize authentication string
          ObString serialized_auth;
          if (OB_FAIL(serialize_auth_string(
                          salt_begin, salt_len,
                          digest_begin, digest_len,
                          actual_rounds,
                          allocator,
                          serialized_auth))) {
            LOG_WARN("failed to serialize auth string", K(ret), K(digest_len),
                     "expected", OB_AUTH_STRING_DIGEST_LENGTH);
          } else {
            // Copy result to output buffer
            if (serialized_auth.length() >= buf_len) {
              ret = OB_BUF_NOT_ENOUGH;
              LOG_WARN("buffer not enough for serialized auth string", K(ret),
                       K(serialized_auth.length()), K(buf_len));
            } else {
              MEMCPY(enc_buf, serialized_auth.ptr(), serialized_auth.length());
              encrypted_pass.assign_ptr(enc_buf, serialized_auth.length());
              LOG_DEBUG("final encrypted_pass debug",
                       "encrypted_pass_len", encrypted_pass.length(),
                       "encrypted_pass", encrypted_pass,
                       "digest_rounds", actual_rounds);
            }
          }
        }
      }
    }
  }

  return ret;
}

} // namespace common
} // namespace oceanbase
