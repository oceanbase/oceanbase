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

#define USING_LOG_PREFIX COMMON

#include "ob_crypt_common.h"

#include <openssl/evp.h>
#include <openssl/rand.h>

#include <cstdio>
#include <cstring>

#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace common
{

int ObCryptHash::hash(const EVP_MD *md,
                      const unsigned char *input,
                      const int64_t input_len,
                      unsigned char *digest,
                      const int64_t digest_len,
                      EVP_MD_CTX *ctx)
{
  int ret = OB_SUCCESS;
  const int md_size = OB_ISNULL(md) ? 0 : EVP_MD_size(md);
  unsigned int out_len = 0;
  const bool need_ctx = OB_ISNULL(ctx);

  if (OB_UNLIKELY(OB_ISNULL(md) || OB_ISNULL(input) || input_len <= 0 || OB_ISNULL(digest) || md_size <= 0
                  || md_size > EVP_MAX_MD_SIZE || digest_len < md_size || digest_len > EVP_MAX_MD_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for hash",
             K(ret),
             KP(md),
             KP(input),
             K(input_len),
             KP(digest),
             K(digest_len),
             K(md_size));
  } else if (need_ctx && OB_ISNULL(ctx = EVP_MD_CTX_create())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create EVP_MD_CTX", K(ret));
  } else if (0 == EVP_DigestInit_ex(ctx, md, nullptr)) {
    ret = OB_ERR_AES_ENCRYPT;
    LOG_WARN("fail to initialize digest", K(ret));
  } else if (0 == EVP_DigestUpdate(ctx, input, input_len)) {
    ret = OB_ERR_AES_ENCRYPT;
    LOG_WARN("fail to update digest", K(ret));
  } else if (0 == EVP_DigestFinal_ex(ctx, digest, &out_len)) {
    ret = OB_ERR_AES_ENCRYPT;
    LOG_WARN("fail to finalize digest", K(ret));
  } else if (out_len != static_cast<unsigned int>(md_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected digest length", K(ret), K(out_len), K(md_size));
  }

  if (need_ctx && OB_NOT_NULL(ctx)) {
    EVP_MD_CTX_destroy(ctx);
  }

  return ret;
}

int ObCryptHash::xor_digests(const unsigned char *a, const unsigned char *b, unsigned char *out, const int64_t len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(a) || OB_ISNULL(b) || OB_ISNULL(out) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for xor", K(ret), KP(a), KP(b), KP(out), K(len));
  } else {
    for (int64_t i = 0; i < len; i++) {
      out[i] = a[i] ^ b[i];
    }
  }

  return ret;
}

// Base64 encoding table used by crypt(3)-style output
static const char b64_chars[] = "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

// Write n 6-bit groups from w into the output buffer
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

int ObCryptCommon::base64_encode_24bit(const unsigned char *data,
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

int ObCryptCommon::generate_user_salt(char *buffer, const int64_t buffer_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buffer) || buffer_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buffer), K(buffer_len));
  } else {
    if (RAND_bytes(reinterpret_cast<unsigned char *>(buffer), buffer_len) != 1) {
      ret = OB_ERR_AES_ENCRYPT;
      LOG_WARN("fail to generate random bytes", K(ret));
    } else {
      for (int64_t i = 0; i < buffer_len; i++) {
        buffer[i] &= 0x7f;
        if (buffer[i] == '\0' || buffer[i] == '$') {
          buffer[i] = buffer[i] + 1;
        }
      }
    }
  }

  return ret;
}

void ObCryptCommon::clear_sensitive_data(unsigned char *data, int64_t len)
{
  if (OB_NOT_NULL(data) && len > 0) {
    MEMSET(data, 0, len);
  }
}

int ObCryptCommon::generate_crypt_multi_hash(const EVP_MD *md,
                                             const int64_t digest_len,
                                             const int64_t mixchars,
                                             const char *plaintext,
                                             const int64_t plaintext_len,
                                             const char *salt,
                                             const int64_t salt_len,
                                             const int64_t rounds,
                                             ObIAllocator &allocator,
                                             unsigned char *digest_out,
                                             const int64_t digest_out_capacity)
{
  int ret = OB_SUCCESS;
  const int md_size = OB_ISNULL(md) ? 0 : EVP_MD_size(md);

  if (OB_ISNULL(md) || md_size <= 0 || md_size > EVP_MAX_MD_SIZE || digest_len <= 0
      || digest_len > EVP_MAX_MD_SIZE || digest_len != md_size || mixchars <= 0 || mixchars > digest_len
      || OB_ISNULL(plaintext) || plaintext_len <= 0 || OB_ISNULL(salt) || salt_len <= 0 || rounds <= 0
      || OB_ISNULL(digest_out) || digest_out_capacity < digest_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(ret),
             KP(md),
             K(md_size),
             K(digest_len),
             K(mixchars),
             KP(plaintext),
             K(plaintext_len),
             KP(salt),
             K(salt_len),
             K(rounds),
             KP(digest_out),
             K(digest_out_capacity));
  } else {
    // Initialize digest contexts
    EVP_MD_CTX *ctx_a = nullptr;
    EVP_MD_CTX *ctx_b = nullptr;
    EVP_MD_CTX *ctx_c = nullptr;
    EVP_MD_CTX *ctx_dp = nullptr;
    EVP_MD_CTX *ctx_ds = nullptr;

    if (OB_ISNULL(ctx_a = EVP_MD_CTX_create())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create digest context A", K(ret));
    } else if (OB_ISNULL(ctx_b = EVP_MD_CTX_create())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create digest context B", K(ret));
    } else if (OB_ISNULL(ctx_c = EVP_MD_CTX_create())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create digest context C", K(ret));
    } else if (OB_ISNULL(ctx_dp = EVP_MD_CTX_create())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create digest context DP", K(ret));
    } else if (OB_ISNULL(ctx_ds = EVP_MD_CTX_create())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create digest context DS", K(ret));
    } else {
      // Step 1: Initialize context A
      if (OB_SUCC(ret) && !EVP_DigestInit_ex(ctx_a, md, nullptr)) {
        ret = OB_ERR_AES_ENCRYPT;
        LOG_WARN("fail to init digest context A", K(ret));
      }

      // Step 2-3: Update password and salt
      if (OB_SUCC(ret)
          && (!EVP_DigestUpdate(ctx_a, plaintext, plaintext_len) || !EVP_DigestUpdate(ctx_a, salt, salt_len))) {
        ret = OB_ERR_AES_ENCRYPT;
        LOG_WARN("fail to update digest context A", K(ret));
      }

      // Step 4-8: Calculate B
      unsigned char B[EVP_MAX_MD_SIZE];
      if (OB_SUCC(ret)) {
        if (!EVP_DigestInit_ex(ctx_b, md, nullptr) || !EVP_DigestUpdate(ctx_b, plaintext, plaintext_len)
            || !EVP_DigestUpdate(ctx_b, salt, salt_len) || !EVP_DigestUpdate(ctx_b, plaintext, plaintext_len)
            || !EVP_DigestFinal_ex(ctx_b, B, nullptr)) {
          ret = OB_ERR_AES_ENCRYPT;
          LOG_WARN("fail to calculate B", K(ret));
        }
      }

      // Step 9-11: Update context A
      if (OB_SUCC(ret)) {
        int64_t i = plaintext_len;
        while (OB_SUCC(ret) && i > mixchars) {
          if (!EVP_DigestUpdate(ctx_a, B, mixchars)) {
            ret = OB_ERR_AES_ENCRYPT;
            LOG_WARN("fail to update digest context A with B", K(ret));
          }
          i -= mixchars;
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
            if (!EVP_DigestUpdate(ctx_a, B, mixchars)) {
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
      unsigned char A[EVP_MAX_MD_SIZE];
      if (OB_SUCC(ret) && !EVP_DigestFinal_ex(ctx_a, A, nullptr)) {
        ret = OB_ERR_AES_ENCRYPT;
        LOG_WARN("fail to finalize digest context A", K(ret));
      }

      // Step 13-15: Calculate DP
      unsigned char DP[EVP_MAX_MD_SIZE];
      if (OB_SUCC(ret)) {
        if (!EVP_DigestInit_ex(ctx_dp, md, nullptr)) {
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
        if (OB_ISNULL(P = static_cast<char *>(allocator.alloc(p_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for P", K(ret), K(p_len));
        } else {
          char *Pp = P;
          int64_t i = plaintext_len;
          while (i >= mixchars) {
            memcpy(Pp, DP, mixchars);
            Pp += mixchars;
            i -= mixchars;
          }
          if (i > 0) {
            memcpy(Pp, DP, i);
          }
        }
      }

      // Step 17-19: Calculate DS
      unsigned char DS[EVP_MAX_MD_SIZE];
      if (OB_SUCC(ret)) {
        if (!EVP_DigestInit_ex(ctx_ds, md, nullptr)) {
          ret = OB_ERR_AES_ENCRYPT;
          LOG_WARN("fail to init digest context DS", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < 16U + A[0]; i++) {
            if (!EVP_DigestUpdate(ctx_ds, salt, salt_len)) {
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
        int64_t s_len = salt_len;
        if (OB_ISNULL(S = static_cast<char *>(allocator.alloc(s_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for S", K(ret), K(s_len));
        } else {
          char *Sp = S;
          int64_t i = salt_len;
          while (i >= mixchars) {
            memcpy(Sp, DS, mixchars);
            Sp += mixchars;
            i -= mixchars;
          }
          if (i > 0) {
            memcpy(Sp, DS, i);
          }
        }
      }

      // Step 21: Main loop
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < rounds; i++) {
          if (!EVP_DigestInit_ex(ctx_c, md, nullptr)) {
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
              if (!EVP_DigestUpdate(ctx_c, A, mixchars)) {
                ret = OB_ERR_AES_ENCRYPT;
                LOG_WARN("fail to update digest context C with A", K(ret));
              }
            } else {
              if (!EVP_DigestUpdate(ctx_c, DP, mixchars)) {
                ret = OB_ERR_AES_ENCRYPT;
                LOG_WARN("fail to update digest context C with DP", K(ret));
              }
            }
          }

          if (OB_SUCC(ret) && i % 3 != 0) {
            if (!EVP_DigestUpdate(ctx_c, S, salt_len)) {
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
                if (!EVP_DigestUpdate(ctx_c, A, mixchars)) {
                  ret = OB_ERR_AES_ENCRYPT;
                  LOG_WARN("fail to update digest context C with A in odd", K(ret));
                }
              } else {
                if (!EVP_DigestUpdate(ctx_c, DP, mixchars)) {
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

      if (OB_SUCC(ret)) {
        MEMCPY(digest_out, DP, digest_len);
      }

      // Clear sensitive data
      if (OB_NOT_NULL(P)) {
        clear_sensitive_data(reinterpret_cast<unsigned char *>(P), plaintext_len);
        allocator.free(P);
      }
      if (OB_NOT_NULL(S)) {
        clear_sensitive_data(reinterpret_cast<unsigned char *>(S), salt_len);
        allocator.free(S);
      }
      clear_sensitive_data(A, sizeof(A));
      clear_sensitive_data(B, sizeof(B));
      clear_sensitive_data(DP, sizeof(DP));
      clear_sensitive_data(DS, sizeof(DS));

    }

    // Clean up every context, including contexts created before a later
    // context allocation failed.
    if (OB_NOT_NULL(ctx_a)) {
      EVP_MD_CTX_destroy(ctx_a);
    }
    if (OB_NOT_NULL(ctx_b)) {
      EVP_MD_CTX_destroy(ctx_b);
    }
    if (OB_NOT_NULL(ctx_c)) {
      EVP_MD_CTX_destroy(ctx_c);
    }
    if (OB_NOT_NULL(ctx_dp)) {
      EVP_MD_CTX_destroy(ctx_dp);
    }
    if (OB_NOT_NULL(ctx_ds)) {
      EVP_MD_CTX_destroy(ctx_ds);
    }
  }

  return ret;
}

int ObCryptCommon::generate_auth_multi_hash(const EVP_MD *md,
                                            const int64_t digest_len,
                                            const int64_t mixchars,
                                            const char *alg_magic,
                                            const char *plaintext,
                                            const int64_t plaintext_len,
                                            const char *salt,
                                            const int64_t salt_len,
                                            const int64_t rounds,
                                            ObIAllocator &allocator,
                                            ObString &output)
{
  int ret = OB_SUCCESS;
  const int md_size = OB_ISNULL(md) ? 0 : EVP_MD_size(md);

  if (OB_ISNULL(md) || md_size <= 0 || md_size > EVP_MAX_MD_SIZE || digest_len <= 0
      || digest_len > EVP_MAX_MD_SIZE || digest_len != md_size || digest_len != OB_CRYPT_RAW_DIGEST_LEN32
      || mixchars <= 0 || mixchars > digest_len || OB_ISNULL(alg_magic) || OB_ISNULL(plaintext)
      || plaintext_len <= 0 || OB_ISNULL(salt) || salt_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
             K(ret),
             KP(md),
             K(md_size),
             K(digest_len),
             K(mixchars),
             KP(alg_magic),
             KP(plaintext),
             K(plaintext_len),
             KP(salt),
             K(salt_len));
  } else if (plaintext_len > OB_CRYPT_MAX_PASSWORD_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("password too long", K(ret), K(plaintext_len));
  } else {
    // Clamp rounds
    int64_t actual_rounds = rounds;
    if (actual_rounds <= 0) {
      actual_rounds = OB_CRYPT_AUTH_ROUNDS_DEFAULT;
    } else if (actual_rounds < OB_CRYPT_AUTH_ROUNDS_MIN) {
      actual_rounds = OB_CRYPT_AUTH_ROUNDS_MIN;
    } else if (actual_rounds > OB_CRYPT_AUTH_ROUNDS_MAX) {
      actual_rounds = OB_CRYPT_AUTH_ROUNDS_MAX;
    }

    // Format: <alg_magic>XXX$salt$digest  (e.g. "$5$00A$salt$digest")
    // alg_magic already includes leading and trailing '$' (e.g. "$5$")
    const int64_t alg_magic_len = static_cast<int64_t>(STRLEN(alg_magic));
    int64_t output_len = alg_magic_len + OB_CRYPT_AUTH_ITERATION_LEN + 1 + salt_len + 1 + OB_CRYPT_AUTH_DIGEST_LEN + 1;
    char *output_buf = nullptr;

    if (OB_ISNULL(output_buf = static_cast<char *>(allocator.alloc(output_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for output buffer", K(ret), K(output_len));
    } else {
      unsigned char digest_buf[EVP_MAX_MD_SIZE];
      if (OB_FAIL(generate_crypt_multi_hash(md,
                                            digest_len,
                                            mixchars,
                                            plaintext,
                                            plaintext_len,
                                            salt,
                                            salt_len,
                                            actual_rounds,
                                            allocator,
                                            digest_buf,
                                            sizeof(digest_buf)))) {
        LOG_WARN("fail to generate crypt multi hash", K(ret));
      } else {
        char *p = output_buf;
        int64_t remaining_len = output_len;

        // Write algorithm identifier and rounds (3-digit hexadecimal)
        // Format: <alg_magic><3-digit-hex>$salt$digest
        int64_t rounds_encoded = actual_rounds / OB_CRYPT_AUTH_ITERATION_MULTIPLIER;
        if (rounds_encoded < 0 || rounds_encoded > 0xFFF) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("rounds out of range for 3-digit hex encoding", K(ret), K(actual_rounds), K(rounds_encoded));
        } else {
          int written = snprintf(p,
                                 remaining_len,
                                 "%s%03lX%c",
                                 alg_magic,
                                 static_cast<unsigned long>(rounds_encoded),
                                 OB_CRYPT_AUTH_DELIMITER);
          if (written < 0 || written >= remaining_len) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to write rounds info", K(ret));
          } else {
            p += written;
            remaining_len -= written;
          }
        }

        // Write salt followed by '$'
        if (OB_SUCC(ret)) {
          if (remaining_len < salt_len + 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("insufficient buffer space for salt", K(ret));
          } else {
            MEMCPY(p, salt, salt_len);
            p += salt_len;
            *p++ = OB_CRYPT_AUTH_DELIMITER;
            remaining_len -= (salt_len + 1);
          }
        }

        // Write hash value (Base64 encoded)
        if (OB_SUCC(ret)) {
          int64_t hash_output_len = 0;
          if (OB_FAIL(base64_encode_24bit(digest_buf, p, hash_output_len, remaining_len))) {
            LOG_WARN("fail to encode hash", K(ret));
          } else {
            p += hash_output_len;
          }
        }

        if (OB_SUCC(ret)) {
          output.assign_ptr(output_buf, static_cast<int32_t>(p - output_buf));
        }
      }
      clear_sensitive_data(digest_buf, sizeof(digest_buf));

      if (OB_SUCCESS != ret && OB_NOT_NULL(output_buf)) {
        allocator.free(output_buf);
      }
    }
  }

  return ret;
}

int ObCryptCommon::extract_user_salt(const char *crypt_str,
                                     const int64_t crypt_str_len,
                                     const char **salt_begin,
                                     const char **salt_end)
{
  int salt_len = 0;

  if (OB_ISNULL(crypt_str) || crypt_str_len <= 0 || OB_ISNULL(salt_begin) || OB_ISNULL(salt_end)) {
    // return 0 salt_len to indicate error
  } else {
    const char *it = crypt_str;
    const char *end = crypt_str + crypt_str_len;
    int delimiter_count = 0;

    // Format: $<type>$[3-digit-hex]$salt$hash
    // Salt is between the 3rd and 4th '$' delimiters
    *salt_begin = nullptr;
    *salt_end = nullptr;
    while (it < end) {
      if (*it == OB_CRYPT_AUTH_DELIMITER) {
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
      salt_len = static_cast<int>(*salt_end - *salt_begin);
      if (salt_len != OB_CRYPT_AUTH_SALT_LEN) {
        salt_len = 0;
        *salt_begin = nullptr;
        *salt_end = nullptr;
      }
    } else {
      *salt_begin = nullptr;
      *salt_end = nullptr;
      salt_len = 0;
    }
  }

  return salt_len;
}

int ObCryptCommon::serialize_auth_string(char digest_type,
                                         const char *salt,
                                         const int64_t salt_len,
                                         const char *digest,
                                         const int64_t digest_len,
                                         const int64_t iterations,
                                         ObIAllocator &allocator,
                                         ObString &output)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(salt) || salt_len <= 0 || OB_ISNULL(digest) || digest_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(salt), K(salt_len), KP(digest), K(digest_len));
  } else if (salt_len != OB_CRYPT_AUTH_SALT_LEN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid salt length", K(ret), K(salt_len));
  } else if (digest_len != OB_CRYPT_AUTH_DIGEST_LEN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid digest length", K(ret), K(digest_len));
  } else if (iterations < OB_CRYPT_AUTH_ROUNDS_MIN || iterations > OB_CRYPT_AUTH_ROUNDS_MAX
             || iterations % OB_CRYPT_AUTH_ITERATION_MULTIPLIER != 0
             || iterations / OB_CRYPT_AUTH_ITERATION_MULTIPLIER > 0xFFF) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iterations for auth string", K(ret), K(iterations));
  } else {
    int64_t output_len = OB_CRYPT_AUTH_STRING_LEN;
    char *output_buf = nullptr;

    if (OB_ISNULL(output_buf = static_cast<char *>(allocator.alloc(output_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for output buffer", K(ret), K(output_len));
    } else {
      char *p = output_buf;
      int64_t remaining_len = output_len;

      *p++ = OB_CRYPT_AUTH_DELIMITER;
      *p++ = digest_type;
      *p++ = OB_CRYPT_AUTH_DELIMITER;
      remaining_len -= 3;

      int written = snprintf(p,
                             remaining_len,
                             "%03lX",
                             static_cast<unsigned long>(iterations / OB_CRYPT_AUTH_ITERATION_MULTIPLIER));
      if (written < 0 || written >= remaining_len) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to write iterations", K(ret));
      } else {
        p += written;
        remaining_len -= written;
      }

      if (OB_SUCC(ret)) {
        if (remaining_len < 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("insufficient buffer space for iteration delimiter", K(ret));
        } else {
          *p++ = OB_CRYPT_AUTH_DELIMITER;
          remaining_len -= 1;
        }
      }

      if (OB_SUCC(ret)) {
        if (remaining_len < salt_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("insufficient buffer space for salt", K(ret), K(salt_len), K(remaining_len));
        } else {
          MEMCPY(p, salt, salt_len);
          p += salt_len;
          remaining_len -= salt_len;
        }
      }

      if (OB_SUCC(ret)) {
        if (remaining_len < digest_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("insufficient buffer space for digest", K(ret), K(digest_len), K(remaining_len));
        } else {
          MEMCPY(p, digest, digest_len);
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

int ObCryptCommon::deserialize_auth_string(char expected_digest_type,
                                           const ObString &auth_string,
                                           ObString &salt,
                                           ObString &digest,
                                           int64_t &iterations)
{
  int ret = OB_SUCCESS;

  if (auth_string.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty auth string", K(ret));
  } else {
    const char *str = auth_string.ptr();
    int64_t len = auth_string.length();
    int64_t pos = 0;

    if (len < 6) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("auth string too short", K(ret), K(len));
    } else if (str[pos++] != OB_CRYPT_AUTH_DELIMITER) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid auth string format: missing first delimiter", K(ret));
    } else if (str[pos++] != expected_digest_type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid auth string format: invalid digest type, expected", K(ret), K(expected_digest_type));
    } else if (str[pos++] != OB_CRYPT_AUTH_DELIMITER) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid auth string format: missing second delimiter", K(ret));
    } else {
      if (pos + OB_CRYPT_AUTH_ITERATION_LEN > len) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid auth string format: incomplete iterations", K(ret));
      } else {
        char iteration_str[OB_CRYPT_AUTH_ITERATION_LEN + 1];
        MEMSET(iteration_str, 0, sizeof(iteration_str));
        MEMCPY(iteration_str, str + pos, OB_CRYPT_AUTH_ITERATION_LEN);
        pos += OB_CRYPT_AUTH_ITERATION_LEN;

        char *end_ptr = nullptr;
        errno = 0;
        unsigned long iteration_count = strtoul(iteration_str, &end_ptr, 16);

        if (errno != 0 || OB_ISNULL(end_ptr) || *end_ptr != '\0') {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid auth string format: invalid iteration count", K(ret), K(iteration_str));
        } else {
          iterations = static_cast<int64_t>(iteration_count * OB_CRYPT_AUTH_ITERATION_MULTIPLIER);
        }
      }

      if (OB_SUCC(ret)) {
        if (pos >= len || str[pos++] != OB_CRYPT_AUTH_DELIMITER) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid auth string format: missing third delimiter", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (pos + OB_CRYPT_AUTH_SALT_LEN > len) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid auth string format: incomplete salt", K(ret));
        } else {
          salt.assign_ptr(str + pos, static_cast<int32_t>(OB_CRYPT_AUTH_SALT_LEN));
          pos += OB_CRYPT_AUTH_SALT_LEN;
        }
      }

      if (OB_SUCC(ret)) {
        int64_t remaining_len = len - pos;
        if (remaining_len != OB_CRYPT_AUTH_DIGEST_LEN) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid auth string format: invalid digest length",
                   K(ret),
                   K(remaining_len),
                   "expected",
                   OB_CRYPT_AUTH_DIGEST_LEN);
        } else {
          bool is_valid_base64 = true;
          const char *digest_start = str + pos;
          for (int64_t i = 0; i < OB_CRYPT_AUTH_DIGEST_LEN && is_valid_base64; ++i) {
            char c = digest_start[i];
            if (!((c >= '.' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'))) {
              is_valid_base64 = false;
            }
          }
          if (!is_valid_base64) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid auth string format: invalid base64 digest format", K(ret));
          } else {
            digest.assign_ptr(str + pos, static_cast<int32_t>(OB_CRYPT_AUTH_DIGEST_LEN));
          }
        }
      }
    }
  }

  return ret;
}

int ObCryptCommon::check_password(const EVP_MD *md,
                                  const int64_t digest_len,
                                  const char *intermediate_magic,
                                  char digest_type,
                                  const ObString &plaintext_password,
                                  const ObString &scramble,
                                  const ObString &stored_auth_string,
                                  bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  ObArenaAllocator allocator("CryptCheck");
  UNUSED(scramble);

  if (stored_auth_string.empty()) {
    // Empty stored auth string: only empty password matches
    if (plaintext_password.empty()) {
      is_match = true;
    }
  } else if (plaintext_password.empty()) {
    // Non-empty stored but empty input: no match
    is_match = false;
  } else {
    // Step 1: Deserialize stored authentication string
    ObString stored_salt;
    ObString stored_digest_base64;
    int64_t iterations = 0;

    if (OB_FAIL(deserialize_auth_string(digest_type,
                                        stored_auth_string,
                                        stored_salt,
                                        stored_digest_base64,
                                        iterations))) {
      LOG_WARN("failed to deserialize auth string", K(ret), K(stored_auth_string));
    } else {
      // Step 2: Generate hash using plaintext password and salt
      ObString computed_hash;
      if (OB_FAIL(generate_auth_multi_hash(md,
                                           digest_len,
                                           digest_len,
                                           intermediate_magic,
                                           plaintext_password.ptr(),
                                           plaintext_password.length(),
                                           stored_salt.ptr(),
                                           stored_salt.length(),
                                           iterations,
                                           allocator,
                                           computed_hash))) {
        LOG_WARN("failed to generate hash", K(ret));
      } else {
        // Step 3: Extract digest from unix-format multi-hash
        // computed_hash format: <alg_magic>[3-digit-hex]$salt$digest
        const char *salt_begin = nullptr;
        const char *salt_end = nullptr;
        int salt_len = extract_user_salt(computed_hash.ptr(), computed_hash.length(), &salt_begin, &salt_end);

        if (salt_len <= 0 || OB_ISNULL(salt_begin) || OB_ISNULL(salt_end)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to extract salt from computed hash", K(ret));
        } else {
          // Digest starts after the '$' following salt
          const char *digest_begin = salt_end + 1;
          int64_t digest_out_len = computed_hash.length() - (digest_begin - computed_hash.ptr());

          // Step 4: Re-serialize using original stored_salt and computed digest
          ObString computed_auth_string;
          if (OB_FAIL(serialize_auth_string(digest_type,
                                            stored_salt.ptr(),
                                            stored_salt.length(),
                                            digest_begin,
                                            digest_out_len,
                                            iterations,
                                            allocator,
                                            computed_auth_string))) {
            LOG_WARN("failed to serialize computed auth string", K(ret));
          } else {
            // Step 5: Compare
            if (0 == stored_auth_string.compare(computed_auth_string)) {
              is_match = true;
              LOG_DEBUG("password: Full Authentication succeeded", K(digest_type));
            } else {
              LOG_DEBUG("password: Full Authentication failed - auth string mismatch", K(digest_type));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObCryptCommon::generate_digest_for_cache(const EVP_MD *md,
                                             const int64_t digest_len,
                                             const char *plaintext_password,
                                             const int64_t plaintext_len,
                                             unsigned char *digest_output,
                                             const int64_t digest_output_len)
{
  int ret = OB_SUCCESS;
  const int md_size = OB_ISNULL(md) ? 0 : EVP_MD_size(md);
  unsigned char stage1[EVP_MAX_MD_SIZE];
  EVP_MD_CTX *ctx = nullptr;

  if (OB_ISNULL(md) || md_size <= 0 || md_size > EVP_MAX_MD_SIZE || digest_len <= 0 || digest_len > EVP_MAX_MD_SIZE
      || digest_len != md_size || OB_ISNULL(plaintext_password) || plaintext_len <= 0 || OB_ISNULL(digest_output)
      || digest_output_len < digest_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments",
             K(ret),
             KP(md),
             K(md_size),
             K(digest_len),
             KP(plaintext_password),
             K(plaintext_len),
             KP(digest_output),
             K(digest_output_len));
  } else if (OB_ISNULL(ctx = EVP_MD_CTX_create())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create EVP_MD_CTX", K(ret));
  } else if (OB_FAIL(ObCryptHash::hash(md,
                                       reinterpret_cast<const unsigned char *>(plaintext_password),
                                       plaintext_len,
                                       stage1,
                                       digest_len,
                                       ctx))) {
    LOG_WARN("fail to compute stage1", K(ret));
  } else if (OB_FAIL(ObCryptHash::hash(md, stage1, digest_len, digest_output, digest_len, ctx))) {
    LOG_WARN("fail to compute stage2", K(ret));
  }

  clear_sensitive_data(stage1, sizeof(stage1));
  if (OB_NOT_NULL(ctx)) {
    EVP_MD_CTX_destroy(ctx);
  }

  return ret;
}

int ObCryptCommon::verify_fast_auth_scramble(const EVP_MD *md,
                                             const int64_t digest_len,
                                             const ObString &client_scramble_response,
                                             const ObString &scramble,
                                             const ObString &cached_digest,
                                             bool &is_match)
{
  int ret = OB_SUCCESS;
  const int md_size = OB_ISNULL(md) ? 0 : EVP_MD_size(md);
  EVP_MD_CTX *ctx = nullptr;
  unsigned char combined[EVP_MAX_MD_SIZE + SCRAMBLE_LENGTH];
  unsigned char stage3[EVP_MAX_MD_SIZE];
  unsigned char expected_stage1[EVP_MAX_MD_SIZE];
  unsigned char expected_stage2[EVP_MAX_MD_SIZE];
  is_match = false;

  if (OB_ISNULL(md) || md_size <= 0 || md_size > EVP_MAX_MD_SIZE || digest_len <= 0 || digest_len > EVP_MAX_MD_SIZE
      || digest_len != md_size || OB_ISNULL(client_scramble_response.ptr())
      || client_scramble_response.length() != digest_len || OB_ISNULL(scramble.ptr())
      || scramble.length() != SCRAMBLE_LENGTH || OB_ISNULL(cached_digest.ptr())
      || cached_digest.length() != digest_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments for fast auth verification",
             K(ret),
             KP(md),
             K(md_size),
             K(digest_len),
             K(client_scramble_response.length()),
             K(scramble.length()),
             K(cached_digest.length()),
             "expected_client_response_len",
             digest_len,
             "expected_scramble_len",
             SCRAMBLE_LENGTH,
             "expected_cached_digest_len",
             digest_len);
  } else if (OB_ISNULL(ctx = EVP_MD_CTX_create())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create EVP_MD_CTX", K(ret));
  } else {
    // Step 1: Calculate stage3 = MD(cached_digest || scramble)
    MEMCPY(combined, cached_digest.ptr(), digest_len);
    MEMCPY(combined + digest_len, scramble.ptr(), SCRAMBLE_LENGTH);
    if (OB_FAIL(ObCryptHash::hash(md, combined, digest_len + SCRAMBLE_LENGTH, stage3, digest_len, ctx))) {
      LOG_WARN("fail to compute stage3", K(ret));
    } else if (OB_FAIL(ObCryptHash::xor_digests(reinterpret_cast<const unsigned char *>(client_scramble_response.ptr()),
                                                stage3,
                                                expected_stage1,
                                                digest_len))) {
      LOG_WARN("fail to xor digests", K(ret));
    } else if (OB_FAIL(ObCryptHash::hash(md, expected_stage1, digest_len, expected_stage2, digest_len, ctx))) {
      LOG_WARN("fail to compute expected_stage2", K(ret));
    } else if (0 == MEMCMP(expected_stage2, cached_digest.ptr(), digest_len)) {
      is_match = true;
      LOG_DEBUG("fast auth verification succeeded");
    } else {
      LOG_DEBUG("fast auth verification failed - digest mismatch");
    }
  }

  clear_sensitive_data(combined, sizeof(combined));
  clear_sensitive_data(stage3, sizeof(stage3));
  clear_sensitive_data(expected_stage1, sizeof(expected_stage1));
  clear_sensitive_data(expected_stage2, sizeof(expected_stage2));
  if (OB_NOT_NULL(ctx)) {
    EVP_MD_CTX_destroy(ctx);
  }

  return ret;
}

int ObCryptCommon::encrypt_passwd(const EVP_MD *md,
                                  const int64_t digest_len,
                                  const char *intermediate_magic,
                                  char digest_type,
                                  const ObString &password,
                                  ObString &encrypted_pass,
                                  char *enc_buf,
                                  const int64_t buf_len,
                                  const int64_t digest_rounds)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(enc_buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid enc_buf", K(ret), KP(enc_buf), K(buf_len));
  } else if (password.empty()) {
    // Empty password → empty auth string
    encrypted_pass.reset();
  } else {
    int64_t actual_rounds = (digest_rounds > OB_CRYPT_AUTH_ROUNDS_DEFAULT
                             && digest_rounds % OB_CRYPT_AUTH_ITERATION_MULTIPLIER == 0)
                            ? digest_rounds
                            : OB_CRYPT_AUTH_ROUNDS_DEFAULT;

    // Generate random salt
    char salt_buf[OB_CRYPT_AUTH_SALT_LEN];
    MEMSET(salt_buf, 0, sizeof(salt_buf));
    if (OB_FAIL(generate_user_salt(salt_buf, OB_CRYPT_AUTH_SALT_LEN))) {
      LOG_WARN("failed to generate user salt", K(ret));
    } else {
      // Generate password hash using multi-round hash
      ModulePageAllocator allocator("CryptEncrypt");
      ObString hash_result;
      if (OB_FAIL(generate_auth_multi_hash(md,
                                           digest_len,
                                           digest_len,
                                           intermediate_magic,
                                           password.ptr(),
                                           password.length(),
                                           salt_buf,
                                           OB_CRYPT_AUTH_SALT_LEN,
                                           actual_rounds,
                                           allocator,
                                           hash_result))) {
        LOG_WARN("failed to generate hash", K(ret));
      } else {
        // Extract salt and digest from generated hash
        // hash_result format: <intermediate_magic>[3-digit-hex]$salt$digest
        const char *salt_begin = nullptr;
        const char *salt_end = nullptr;
        int salt_len = extract_user_salt(hash_result.ptr(), hash_result.length(), &salt_begin, &salt_end);

        if (salt_len <= 0 || OB_ISNULL(salt_begin) || OB_ISNULL(salt_end)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to extract salt from hash", K(ret));
        } else {
          // Digest starts after the '$' following salt
          const char *digest_begin = salt_end + 1;
          int64_t digest_out_len = hash_result.length() - (digest_begin - hash_result.ptr());

          // Serialize authentication string
          ObString serialized_auth;
          if (OB_FAIL(serialize_auth_string(digest_type,
                                            salt_begin,
                                            salt_len,
                                            digest_begin,
                                            digest_out_len,
                                            actual_rounds,
                                            allocator,
                                            serialized_auth))) {
            LOG_WARN("failed to serialize auth string", K(ret));
          } else if (serialized_auth.length() >= buf_len) {
            ret = OB_BUF_NOT_ENOUGH;
            LOG_WARN("buffer not enough for serialized auth string", K(ret), K(serialized_auth.length()), K(buf_len));
          } else {
            MEMCPY(enc_buf, serialized_auth.ptr(), serialized_auth.length());
            encrypted_pass.assign_ptr(enc_buf, serialized_auth.length());
            LOG_DEBUG("encrypted_pass debug",
                      "encrypted_pass_len",
                      encrypted_pass.length(),
                      "digest_rounds",
                      actual_rounds);
          }
        }
      }
    }
  }

  return ret;
}

}  // namespace common
}  // namespace oceanbase
