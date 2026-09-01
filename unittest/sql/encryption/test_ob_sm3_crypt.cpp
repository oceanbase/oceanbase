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

#define USING_LOG_PREFIX COMMON
#include <gtest/gtest.h>
#include "lib/encrypt/ob_sm3_crypt.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_log.h"
#include <cstring>

using namespace oceanbase::common;

// ========== SM3 Hash Fixed Vector Tests ==========

TEST(ObCryptHash, sm3_abc_standard_vector)
{
  // GB/T 32905-2016 standard test vector: SM3("abc")
  // Expected: 66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0
  const unsigned char input[] = "abc";
  const int64_t input_len = 3;
  unsigned char digest[32];
  const unsigned char expected[] = {
    0x66, 0xc7, 0xf0, 0xf4, 0x62, 0xee, 0xed, 0xd9,
    0xd1, 0xf2, 0xd4, 0x6b, 0xdc, 0x10, 0xe4, 0xe2,
    0x41, 0x67, 0xc4, 0x87, 0x5c, 0xf2, 0xf7, 0xa2,
    0x29, 0x7d, 0xa0, 0x2b, 0x8f, 0x4b, 0xa8, 0xe0
  };

  int ret = ObCryptHash::hash(EVP_sm3(), input, input_len, digest, sizeof(digest));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, MEMCMP(digest, expected, 32));
}

TEST(ObCryptHash, sm3_64byte_standard_vector)
{
  // GB/T 32905-2016 second test vector: SM3("abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd")
  // 64 bytes of "abcd" repeated 16 times
  // Expected: debe9ff92275b8a138604889c18e5a4d6fdb70e5387e5765293dcba39c0c5732
  const unsigned char input[] = "abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd";
  const int64_t input_len = 64;
  unsigned char digest[32];
  const unsigned char expected[] = {
    0xde, 0xbe, 0x9f, 0xf9, 0x22, 0x75, 0xb8, 0xa1,
    0x38, 0x60, 0x48, 0x89, 0xc1, 0x8e, 0x5a, 0x4d,
    0x6f, 0xdb, 0x70, 0xe5, 0x38, 0x7e, 0x57, 0x65,
    0x29, 0x3d, 0xcb, 0xa3, 0x9c, 0x0c, 0x57, 0x32
  };

  int ret = ObCryptHash::hash(EVP_sm3(), input, input_len, digest, sizeof(digest));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, MEMCMP(digest, expected, 32));
}

TEST(ObCryptHash, sm3_empty_input)
{
  // SM3 of empty input should not crash
  const unsigned char input[] = "";
  const int64_t input_len = 0;
  unsigned char digest[32];

  int ret = ObCryptHash::hash(EVP_sm3(), input, input_len, digest, sizeof(digest));
  // Empty input is invalid because input_len must be > 0
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST(ObCryptHash, sm3_null_input)
{
  unsigned char digest[32];
  int ret = ObCryptHash::hash(EVP_sm3(), nullptr, 10, digest, sizeof(digest));
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST(ObCryptHash, sm3_small_digest_buffer)
{
  const unsigned char input[] = "test";
  unsigned char digest[16];  // Too small, need at least 32

  int ret = ObCryptHash::hash(EVP_sm3(), input, 4, digest, sizeof(digest));
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST(ObCryptHash, reusable_context)
{
  const unsigned char input1[] = "first input";
  const unsigned char input2[] = "second input";
  unsigned char digest1[OB_SM3_DIGEST_LENGTH];
  unsigned char digest2[OB_SM3_DIGEST_LENGTH];
  EVP_MD_CTX *ctx = EVP_MD_CTX_create();
  ASSERT_NE(nullptr, ctx);

  int ret = ObCryptHash::hash(EVP_sm3(), input1, sizeof(input1) - 1,
                              digest1, sizeof(digest1), ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObCryptHash::hash(EVP_sm3(), input2, sizeof(input2) - 1,
                          digest2, sizeof(digest2), ctx);
  ASSERT_EQ(OB_SUCCESS, ret);

  unsigned char expected1[OB_SM3_DIGEST_LENGTH];
  unsigned char expected2[OB_SM3_DIGEST_LENGTH];
  ret = ObCryptHash::hash(EVP_sm3(), input1, sizeof(input1) - 1,
                          expected1, sizeof(expected1));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObCryptHash::hash(EVP_sm3(), input2, sizeof(input2) - 1,
                          expected2, sizeof(expected2));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, MEMCMP(digest1, expected1, sizeof(digest1)));
  ASSERT_EQ(0, MEMCMP(digest2, expected2, sizeof(digest2)));

  EVP_MD_CTX_destroy(ctx);
}

TEST(ObCryptHash, digest_capacity_out_of_range)
{
  const unsigned char input[] = "test";
  unsigned char digest[EVP_MAX_MD_SIZE];

  int ret = ObCryptHash::hash(EVP_sm3(), input, sizeof(input) - 1,
                              digest, EVP_MAX_MD_SIZE + 1);
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST(ObCryptHash, digest_length_mismatch)
{
  const char *password = "password";
  unsigned char digest[OB_SM3_DIGEST_LENGTH];

  int ret = ObCryptCommon::generate_digest_for_cache(
      EVP_sm3(), OB_SM3_DIGEST_LENGTH + 1, password, strlen(password),
      digest, sizeof(digest));
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST(ObCryptHash, mixchars_out_of_range)
{
  ObArenaAllocator allocator("HashValid");
  const char salt[OB_CRYPT_AUTH_SALT_LEN + 1] = "0123456789abcdef0123";
  unsigned char digest[OB_SM3_DIGEST_LENGTH];

  int ret = ObCryptCommon::generate_crypt_multi_hash(
      EVP_sm3(), OB_SM3_DIGEST_LENGTH, OB_SM3_DIGEST_LENGTH + 1,
      "password", 8, salt, OB_CRYPT_AUTH_SALT_LEN, 5000,
      allocator, digest, sizeof(digest));
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST(ObCryptHash, xor_digests_basic)
{
  unsigned char a[] = {0x01, 0x02, 0x03, 0x04};
  unsigned char b[] = {0x01, 0x04, 0x01, 0x04};
  unsigned char out[4];
  unsigned char expected[] = {0x00, 0x06, 0x02, 0x00};

  int ret = ObCryptHash::xor_digests(a, b, out, 4);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, MEMCMP(out, expected, 4));
}

TEST(ObCryptHash, xor_digests_null)
{
  unsigned char a[] = {0x01, 0x02};
  unsigned char out[2];
  int ret = ObCryptHash::xor_digests(a, nullptr, out, 2);
  ASSERT_NE(OB_SUCCESS, ret);
}

// ========== Double SM3 Tests ==========

TEST(ObSm3Crypt, generate_sm3_digest_for_cache)
{
  // SM3(SM3("password")) should produce 32 bytes
  const char *password = "password";
  unsigned char digest[OB_SM3_DIGEST_LENGTH];

  int ret = ObSm3Crypt::generate_sm3_digest_for_cache(
      password, strlen(password), digest, sizeof(digest));
  ASSERT_EQ(OB_SUCCESS, ret);

  // Verify not all zeros
  unsigned char zero_buf[OB_SM3_DIGEST_LENGTH];
  MEMSET(zero_buf, 0, sizeof(zero_buf));
  ASSERT_NE(0, MEMCMP(digest, zero_buf, OB_SM3_DIGEST_LENGTH));

  // Verify deterministic: same input → same output
  unsigned char digest2[OB_SM3_DIGEST_LENGTH];
  ret = ObSm3Crypt::generate_sm3_digest_for_cache(
      password, strlen(password), digest2, sizeof(digest2));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, MEMCMP(digest, digest2, OB_SM3_DIGEST_LENGTH));

  // Verify different passwords produce different digests
  unsigned char digest3[OB_SM3_DIGEST_LENGTH];
  ret = ObSm3Crypt::generate_sm3_digest_for_cache(
      "different", 9, digest3, sizeof(digest3));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(0, MEMCMP(digest, digest3, OB_SM3_DIGEST_LENGTH));
}

TEST(ObSm3Crypt, generate_sm3_digest_for_cache_null)
{
  unsigned char digest[OB_SM3_DIGEST_LENGTH];
  int ret = ObSm3Crypt::generate_sm3_digest_for_cache(
      nullptr, 0, digest, sizeof(digest));
  ASSERT_NE(OB_SUCCESS, ret);
}

// ========== $S$ Serialization/Deserialization Tests ==========

TEST(ObSm3Crypt, serialize_deserialize_roundtrip)
{
  ObArenaAllocator allocator("Sm3Test");
  // Salt: exactly 20 bytes (no null terminator needed, passed by pointer+length)
  const char salt[OB_CRYPT_AUTH_SALT_LEN + 1] = "0123456789abcdef0123";
  // Digest: exactly 43 bytes of base64-encoded data
  const char digest[OB_CRYPT_AUTH_DIGEST_LEN + 1] =
      "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcde";
  int64_t iterations = 5000;

  // Serialize
  ObString serialized;
  int ret = ObSm3Crypt::serialize_auth_string(
      salt, OB_CRYPT_AUTH_SALT_LEN,
      digest, OB_CRYPT_AUTH_DIGEST_LEN,
      iterations, allocator, serialized);
  ASSERT_EQ(OB_SUCCESS, ret);
  // $S$ + iterations(3) + $ + salt(20) + digest(43) = 1+1+1+3+1+20+43 = 70
  ASSERT_EQ(70, serialized.length());

  // Verify format: $S$005$...
  ASSERT_EQ('$', serialized.ptr()[0]);
  ASSERT_EQ('S', serialized.ptr()[1]);
  ASSERT_EQ('$', serialized.ptr()[2]);
  // iterations = 5000, 5000/1000 = 5, hex = "005"
  ASSERT_EQ('0', serialized.ptr()[3]);
  ASSERT_EQ('0', serialized.ptr()[4]);
  ASSERT_EQ('5', serialized.ptr()[5]);
  ASSERT_EQ('$', serialized.ptr()[6]);

  // Deserialize
  ObString deser_salt;
  ObString deser_digest;
  int64_t deser_iterations = 0;
  ret = ObSm3Crypt::deserialize_auth_string(
      serialized, deser_salt, deser_digest, deser_iterations);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(5000, deser_iterations);
  ASSERT_EQ(OB_CRYPT_AUTH_SALT_LEN, deser_salt.length());
  ASSERT_EQ(OB_CRYPT_AUTH_DIGEST_LEN, deser_digest.length());
  ASSERT_EQ(0, MEMCMP(salt, deser_salt.ptr(), OB_CRYPT_AUTH_SALT_LEN));
  ASSERT_EQ(0, MEMCMP(digest, deser_digest.ptr(), OB_CRYPT_AUTH_DIGEST_LEN));
}

TEST(ObSm3Crypt, serialize_rejects_iteration_overflow)
{
  ObArenaAllocator allocator("Sm3SerVal");
  const char salt[OB_CRYPT_AUTH_SALT_LEN + 1] = "0123456789abcdef0123";
  const char digest[OB_CRYPT_AUTH_DIGEST_LEN + 1] =
      "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcde";
  ObString serialized;

  int ret = ObSm3Crypt::serialize_auth_string(
      salt, OB_CRYPT_AUTH_SALT_LEN,
      digest, OB_CRYPT_AUTH_DIGEST_LEN,
      4095000, allocator, serialized);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(70, serialized.length());

  serialized.reset();
  ret = ObSm3Crypt::serialize_auth_string(
      salt, OB_CRYPT_AUTH_SALT_LEN,
      digest, OB_CRYPT_AUTH_DIGEST_LEN,
      4096000, allocator, serialized);
  ASSERT_NE(OB_SUCCESS, ret);
  ASSERT_TRUE(serialized.empty());
}

TEST(ObSm3Crypt, deserialize_wrong_magic)
{
  // Use $A$ (SHA256 magic) instead of $S$
  const char *bad_auth = "$A$005$0123456789abcdef0123./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcde";
  ObString auth_str(strlen(bad_auth), bad_auth);
  ObString salt, digest;
  int64_t iterations = 0;

  int ret = ObSm3Crypt::deserialize_auth_string(auth_str, salt, digest, iterations);
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST(ObSm3Crypt, deserialize_wrong_length)
{
  // Too short
  const char *bad_auth = "$S$";
  ObString auth_str(strlen(bad_auth), bad_auth);
  ObString salt, digest;
  int64_t iterations = 0;

  int ret = ObSm3Crypt::deserialize_auth_string(auth_str, salt, digest, iterations);
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST(ObSm3Crypt, deserialize_invalid_iterations)
{
  // Non-hex iterations
  const char *bad_auth = "$S$GGG$0123456789abcdef0123./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcde";
  ObString auth_str(strlen(bad_auth), bad_auth);
  ObString salt, digest;
  int64_t iterations = 0;

  int ret = ObSm3Crypt::deserialize_auth_string(auth_str, salt, digest, iterations);
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST(ObSm3Crypt, deserialize_empty)
{
  ObString auth_str;
  ObString salt, digest;
  int64_t iterations = 0;

  int ret = ObSm3Crypt::deserialize_auth_string(auth_str, salt, digest, iterations);
  ASSERT_NE(OB_SUCCESS, ret);
}

// ========== encrypt_passwd_to_ob_sm3 Tests ==========

TEST(ObSm3Crypt, encrypt_passwd_basic)
{
  ObString password("test_password");
  char enc_buf[OB_SM3_PASSWD_BUF_LEN];
  MEMSET(enc_buf, 0, sizeof(enc_buf));
  ObString encrypted_pass;

  int ret = ObSm3Crypt::encrypt_passwd_to_ob_sm3(
      password, encrypted_pass, enc_buf, sizeof(enc_buf), 5000);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GT(encrypted_pass.length(), 0);

  // Verify format: should start with $S$
  ASSERT_EQ('$', encrypted_pass.ptr()[0]);
  ASSERT_EQ('S', encrypted_pass.ptr()[1]);
  ASSERT_EQ('$', encrypted_pass.ptr()[2]);

  // Verify can be deserialized
  ObString salt, digest;
  int64_t iterations = 0;
  ret = ObSm3Crypt::deserialize_auth_string(
      encrypted_pass, salt, digest, iterations);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(5000, iterations);
}

TEST(ObSm3Crypt, encrypt_passwd_empty)
{
  ObString password;
  char enc_buf[OB_SM3_PASSWD_BUF_LEN];
  MEMSET(enc_buf, 0, sizeof(enc_buf));
  ObString encrypted_pass;

  int ret = ObSm3Crypt::encrypt_passwd_to_ob_sm3(
      password, encrypted_pass, enc_buf, sizeof(enc_buf), 5000);
  ASSERT_EQ(OB_SUCCESS, ret);
  // Empty password → empty auth string
  ASSERT_EQ(0, encrypted_pass.length());
}

TEST(ObSm3Crypt, encrypt_passwd_different_rounds)
{
  ObString password("test_password");
  char enc_buf1[OB_SM3_PASSWD_BUF_LEN];
  char enc_buf2[OB_SM3_PASSWD_BUF_LEN];
  MEMSET(enc_buf1, 0, sizeof(enc_buf1));
  MEMSET(enc_buf2, 0, sizeof(enc_buf2));
  ObString ep1, ep2;

  int ret = ObSm3Crypt::encrypt_passwd_to_ob_sm3(
      password, ep1, enc_buf1, sizeof(enc_buf1), 5000);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ObSm3Crypt::encrypt_passwd_to_ob_sm3(
      password, ep2, enc_buf2, sizeof(enc_buf2), 10000);
  ASSERT_EQ(OB_SUCCESS, ret);

  // Different rounds should produce different auth strings
  ASSERT_NE(0, MEMCMP(ep1.ptr(), ep2.ptr(),
             ep1.length() < ep2.length() ? ep1.length() : ep2.length()));
}

// ========== Full Auth (check_sm3_password) Tests ==========

TEST(ObSm3Crypt, check_password_correct)
{
  ObString password("my_secret_password");
  char enc_buf[OB_SM3_PASSWD_BUF_LEN];
  MEMSET(enc_buf, 0, sizeof(enc_buf));
  ObString encrypted_pass;
  ObString empty_scramble;

  int ret = ObSm3Crypt::encrypt_passwd_to_ob_sm3(
      password, encrypted_pass, enc_buf, sizeof(enc_buf), 5000);
  ASSERT_EQ(OB_SUCCESS, ret);

  bool is_match = false;
  ret = ObSm3Crypt::check_sm3_password(
      password, empty_scramble, encrypted_pass, is_match);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_match);
}

TEST(ObSm3Crypt, check_password_wrong)
{
  ObString password("correct_password");
  char enc_buf[OB_SM3_PASSWD_BUF_LEN];
  MEMSET(enc_buf, 0, sizeof(enc_buf));
  ObString encrypted_pass;
  ObString empty_scramble;

  int ret = ObSm3Crypt::encrypt_passwd_to_ob_sm3(
      password, encrypted_pass, enc_buf, sizeof(enc_buf), 5000);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObString wrong_password("wrong_password");
  bool is_match = true;
  ret = ObSm3Crypt::check_sm3_password(
      wrong_password, empty_scramble, encrypted_pass, is_match);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(is_match);
}

TEST(ObSm3Crypt, check_password_empty_both)
{
  ObString empty_password;
  ObString empty_auth_string;
  ObString empty_scramble;
  bool is_match = false;

  int ret = ObSm3Crypt::check_sm3_password(
      empty_password, empty_scramble, empty_auth_string, is_match);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_match);
}

TEST(ObSm3Crypt, check_password_empty_input_vs_stored)
{
  ObString empty_input;
  ObString empty_scramble;
  // Create a stored auth string for a non-empty password
  ObString password("some_password");
  char enc_buf[OB_SM3_PASSWD_BUF_LEN];
  MEMSET(enc_buf, 0, sizeof(enc_buf));
  ObString encrypted_pass;

  int ret = ObSm3Crypt::encrypt_passwd_to_ob_sm3(
      password, encrypted_pass, enc_buf, sizeof(enc_buf), 5000);
  ASSERT_EQ(OB_SUCCESS, ret);

  bool is_match = true;
  ret = ObSm3Crypt::check_sm3_password(
      empty_input, empty_scramble, encrypted_pass, is_match);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(is_match);
}

// ========== Fast Auth Scramble Tests ==========

TEST(ObSm3Crypt, fast_auth_scramble_correct)
{
  const char *password = "fast_auth_test";
  const int64_t pw_len = strlen(password);

  // Step 1: Compute stage1 = SM3(password)
  unsigned char stage1[OB_SM3_DIGEST_LENGTH];
  int ret = ObCryptHash::hash(EVP_sm3(),
      reinterpret_cast<const unsigned char*>(password), pw_len,
      stage1, sizeof(stage1));
  ASSERT_EQ(OB_SUCCESS, ret);

  // Step 2: Compute stage2 = SM3(stage1) → this is what cache stores
  unsigned char stage2[OB_SM3_DIGEST_LENGTH];
  ret = ObCryptHash::hash(EVP_sm3(), stage1, sizeof(stage1), stage2, sizeof(stage2));
  ASSERT_EQ(OB_SUCCESS, ret);

  // Step 3: Generate a random scramble (20 bytes)
  unsigned char scramble[OB_CRYPT_AUTH_SALT_LEN];
  MEMSET(scramble, 0xAB, sizeof(scramble));  // Use known value

  // Step 4: Compute stage3 = SM3(stage2 || scramble)
  unsigned char combined[OB_SM3_DIGEST_LENGTH + OB_CRYPT_AUTH_SALT_LEN];
  MEMCPY(combined, stage2, OB_SM3_DIGEST_LENGTH);
  MEMCPY(combined + OB_SM3_DIGEST_LENGTH, scramble, OB_CRYPT_AUTH_SALT_LEN);
  unsigned char stage3[OB_SM3_DIGEST_LENGTH];
  ret = ObCryptHash::hash(EVP_sm3(), combined, sizeof(combined), stage3, sizeof(stage3));
  ASSERT_EQ(OB_SUCCESS, ret);

  // Step 5: Compute client_response = XOR(stage1, stage3)
  unsigned char client_response[OB_SM3_DIGEST_LENGTH];
  ret = ObCryptHash::xor_digests(stage1, stage3, client_response, sizeof(client_response));
  ASSERT_EQ(OB_SUCCESS, ret);

  // Step 6: Verify on server side
  ObString client_resp_str(sizeof(client_response),
      reinterpret_cast<const char*>(client_response));
  ObString scramble_str(sizeof(scramble),
      reinterpret_cast<const char*>(scramble));
  ObString cached_digest_str(sizeof(stage2),
      reinterpret_cast<const char*>(stage2));
  bool is_match = false;
  ret = ObSm3Crypt::verify_fast_auth_scramble(
      client_resp_str, scramble_str, cached_digest_str, is_match);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_match);

  // Clear sensitive data
  MEMSET(stage1, 0, sizeof(stage1));
  MEMSET(stage2, 0, sizeof(stage2));
  MEMSET(stage3, 0, sizeof(stage3));
  MEMSET(client_response, 0, sizeof(client_response));
}

TEST(ObSm3Crypt, fast_auth_scramble_wrong_password)
{
  const char *password = "real_password";
  const int64_t pw_len = strlen(password);

  // Server has cached SM3(SM3(password))
  unsigned char stage1[OB_SM3_DIGEST_LENGTH];
  ObCryptHash::hash(EVP_sm3(), reinterpret_cast<const unsigned char*>(password), pw_len,
                  stage1, sizeof(stage1));
  unsigned char stage2[OB_SM3_DIGEST_LENGTH];
  ObCryptHash::hash(EVP_sm3(), stage1, sizeof(stage1), stage2, sizeof(stage2));

  // But client uses wrong password
  const char *wrong_pw = "wrong_password";
  unsigned char wrong_stage1[OB_SM3_DIGEST_LENGTH];
  ObCryptHash::hash(EVP_sm3(), reinterpret_cast<const unsigned char*>(wrong_pw),
                  strlen(wrong_pw), wrong_stage1, sizeof(wrong_stage1));
  unsigned char wrong_stage2[OB_SM3_DIGEST_LENGTH];
  ObCryptHash::hash(EVP_sm3(), wrong_stage1, sizeof(wrong_stage1),
                  wrong_stage2, sizeof(wrong_stage2));

  unsigned char scramble[OB_CRYPT_AUTH_SALT_LEN];
  MEMSET(scramble, 0xCD, sizeof(scramble));

  unsigned char combined[OB_SM3_DIGEST_LENGTH + OB_CRYPT_AUTH_SALT_LEN];
  MEMCPY(combined, wrong_stage2, OB_SM3_DIGEST_LENGTH);
  MEMCPY(combined + OB_SM3_DIGEST_LENGTH, scramble, OB_CRYPT_AUTH_SALT_LEN);
  unsigned char wrong_stage3[OB_SM3_DIGEST_LENGTH];
  ObCryptHash::hash(EVP_sm3(), combined, sizeof(combined), wrong_stage3, sizeof(wrong_stage3));

  unsigned char wrong_response[OB_SM3_DIGEST_LENGTH];
  ObCryptHash::xor_digests(wrong_stage1, wrong_stage3, wrong_response, sizeof(wrong_response));

  ObString client_resp_str(sizeof(wrong_response),
      reinterpret_cast<const char*>(wrong_response));
  ObString scramble_str(sizeof(scramble),
      reinterpret_cast<const char*>(scramble));
  ObString cached_digest_str(sizeof(stage2),
      reinterpret_cast<const char*>(stage2));
  bool is_match = true;
  int ret = ObSm3Crypt::verify_fast_auth_scramble(
      client_resp_str, scramble_str, cached_digest_str, is_match);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(is_match);

  MEMSET(stage1, 0, sizeof(stage1));
  MEMSET(stage2, 0, sizeof(stage2));
}

TEST(ObSm3Crypt, fast_auth_scramble_wrong_lengths)
{
  // Wrong client_response length (31 instead of 32)
  unsigned char response[31];
  MEMSET(response, 0xAA, sizeof(response));
  unsigned char scramble[OB_CRYPT_AUTH_SALT_LEN];
  MEMSET(scramble, 0xBB, sizeof(scramble));
  unsigned char cached[OB_SM3_DIGEST_LENGTH];
  MEMSET(cached, 0xCC, sizeof(cached));

  ObString resp_str(sizeof(response), reinterpret_cast<const char*>(response));
  ObString scramble_str(sizeof(scramble), reinterpret_cast<const char*>(scramble));
  ObString cached_str(sizeof(cached), reinterpret_cast<const char*>(cached));
  bool is_match = true;
  int ret = ObSm3Crypt::verify_fast_auth_scramble(
      resp_str, scramble_str, cached_str, is_match);
  ASSERT_NE(OB_SUCCESS, ret);
}

// ========== generate_user_salt Tests ==========

TEST(ObSm3Crypt, generate_user_salt_valid)
{
  char salt[OB_CRYPT_AUTH_SALT_LEN];
  MEMSET(salt, 0, sizeof(salt));

  int ret = ObSm3Crypt::generate_user_salt(salt, sizeof(salt));
  ASSERT_EQ(OB_SUCCESS, ret);

  // Verify no zero bytes (extremely unlikely for random 20 bytes)
  // And no '$' character
  for (int i = 0; i < OB_CRYPT_AUTH_SALT_LEN; i++) {
    ASSERT_NE('$', salt[i]);
  }
}

TEST(ObSm3Crypt, generate_user_salt_null)
{
  int ret = ObSm3Crypt::generate_user_salt(nullptr, 20);
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST(ObSm3Crypt, generate_user_salt_zero_length)
{
  char salt[OB_CRYPT_AUTH_SALT_LEN];
  int ret = ObSm3Crypt::generate_user_salt(salt, 0);
  ASSERT_NE(OB_SUCCESS, ret);
}

// ========== Iteration Rounds Tests ==========

TEST(ObSm3Crypt, rounds_default)
{
  ObString password("test_password");
  char enc_buf[OB_SM3_PASSWD_BUF_LEN];
  MEMSET(enc_buf, 0, sizeof(enc_buf));
  ObString encrypted_pass;

  int ret = ObSm3Crypt::encrypt_passwd_to_ob_sm3(
      password, encrypted_pass, enc_buf, sizeof(enc_buf));
  ASSERT_EQ(OB_SUCCESS, ret);

  // Verify default rounds (5000 → hex "005")
  ASSERT_EQ('0', encrypted_pass.ptr()[3]);
  ASSERT_EQ('0', encrypted_pass.ptr()[4]);
  ASSERT_EQ('5', encrypted_pass.ptr()[5]);
}

TEST(ObSm3Crypt, rounds_below_default_falls_back_to_default)
{
  // rounds must be > OB_CRYPT_AUTH_ROUNDS_DEFAULT (5000) and a multiple of 1000
  // to take effect; 1000 does not qualify, so it falls back to the default.
  ObString password("test_password");
  char enc_buf[OB_SM3_PASSWD_BUF_LEN];
  MEMSET(enc_buf, 0, sizeof(enc_buf));
  ObString encrypted_pass;

  int ret = ObSm3Crypt::encrypt_passwd_to_ob_sm3(
      password, encrypted_pass, enc_buf, sizeof(enc_buf), 1000);
  ASSERT_EQ(OB_SUCCESS, ret);

  // Falls back to default rounds 5000 → hex "005"
  ASSERT_EQ('0', encrypted_pass.ptr()[3]);
  ASSERT_EQ('0', encrypted_pass.ptr()[4]);
  ASSERT_EQ('5', encrypted_pass.ptr()[5]);
}

// ========== Buffer Too Small Test ==========

TEST(ObSm3Crypt, encrypt_passwd_buffer_too_small)
{
  ObString password("test_password");
  char small_buf[10];
  MEMSET(small_buf, 0, sizeof(small_buf));
  ObString encrypted_pass;

  int ret = ObSm3Crypt::encrypt_passwd_to_ob_sm3(
      password, encrypted_pass, small_buf, sizeof(small_buf), 5000);
  ASSERT_NE(OB_SUCCESS, ret);
}

// ========== Determinism Test ==========

TEST(ObSm3Crypt, same_password_same_salt_same_output)
{
  // Verify that with the same salt, we get the same output
  const char *password = "test_password";
  const int64_t pw_len = strlen(password);
  const char fixed_salt[OB_CRYPT_AUTH_SALT_LEN] = "fixed_salt_01234567";

  ObArenaAllocator allocator("Sm3Test");
  ObString output1;
  int ret = ObSm3Crypt::generate_sm3_multi_hash(
      password, pw_len, fixed_salt, OB_CRYPT_AUTH_SALT_LEN,
      5000, allocator, output1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArenaAllocator allocator2("Sm3Test");
  ObString output2;
  ret = ObSm3Crypt::generate_sm3_multi_hash(
      password, pw_len, fixed_salt, OB_CRYPT_AUTH_SALT_LEN,
      5000, allocator2, output2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(output1.length(), output2.length());
  ASSERT_EQ(0, MEMCMP(output1.ptr(), output2.ptr(), output1.length()));
}

// ========== extract_user_salt Tests ==========

TEST(ObSm3Crypt, extract_user_salt_normal)
{
  // extract_user_salt operates on unix-crypt intermediate multi-hash:
  // $S$XXX$salt$digest (salt is between the 3rd and 4th $)
  const char *password = "extract_salt_test";
  const char fixed_salt[OB_CRYPT_AUTH_SALT_LEN] = "fixed_salt_01234567";
  ObArenaAllocator allocator("Sm3Test");
  ObString multi_hash;

  int ret = ObSm3Crypt::generate_sm3_multi_hash(
      password, strlen(password),
      fixed_salt, OB_CRYPT_AUTH_SALT_LEN,
      5000, allocator, multi_hash);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(multi_hash.empty());
  ASSERT_EQ(0, MEMCMP(multi_hash.ptr(), "$S$", 3));

  const char *salt_begin = nullptr;
  const char *salt_end = nullptr;
  ret = ObSm3Crypt::extract_user_salt(
      multi_hash.ptr(), multi_hash.length(), &salt_begin, &salt_end);
  // extract_user_salt returns salt length on success
  ASSERT_GT(ret, 0);
  ASSERT_TRUE(salt_begin != nullptr);
  ASSERT_TRUE(salt_end != nullptr);
  ASSERT_EQ(OB_CRYPT_AUTH_SALT_LEN, salt_end - salt_begin);
  ASSERT_EQ(0, MEMCMP(salt_begin, fixed_salt, OB_CRYPT_AUTH_SALT_LEN));
}

TEST(ObSm3Crypt, extract_user_salt_too_short)
{
  // Auth string too short to contain valid salt
  const char *short_str = "$S$";
  const char *salt_begin = nullptr;
  const char *salt_end = nullptr;
  int ret = ObSm3Crypt::extract_user_salt(short_str, 3, &salt_begin, &salt_end);
  // Should fail or return 0/negative indicating error
  ASSERT_GE(0, ret);
}

// ========== generate_sm3_multi_hash Error Tests ==========

TEST(ObSm3Crypt, generate_sm3_multi_hash_null_password)
{
  ObArenaAllocator allocator("Sm3Test");
  ObString output;
  const char fixed_salt[OB_CRYPT_AUTH_SALT_LEN] = "fixed_salt_01234567";

  int ret = ObSm3Crypt::generate_sm3_multi_hash(
      nullptr, 10, fixed_salt, OB_CRYPT_AUTH_SALT_LEN,
      5000, allocator, output);
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST(ObSm3Crypt, generate_sm3_multi_hash_password_too_long)
{
  ObArenaAllocator allocator("Sm3Test");
  ObString output;
  const char fixed_salt[OB_CRYPT_AUTH_SALT_LEN] = "fixed_salt_01234567";
  // Password exceeding OB_CRYPT_MAX_PASSWORD_SIZE (256)
  char long_password[OB_CRYPT_MAX_PASSWORD_SIZE + 1];
  MEMSET(long_password, 'A', sizeof(long_password));

  int ret = ObSm3Crypt::generate_sm3_multi_hash(
      long_password, sizeof(long_password), fixed_salt, OB_CRYPT_AUTH_SALT_LEN,
      5000, allocator, output);
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST(ObSm3Crypt, generate_sm3_multi_hash_rounds_at_min)
{
  ObArenaAllocator allocator("Sm3Test");
  ObString output;
  const char *password = "test";
  const char fixed_salt[OB_CRYPT_AUTH_SALT_LEN] = "fixed_salt_01234567";

  int ret = ObSm3Crypt::generate_sm3_multi_hash(
      password, strlen(password), fixed_salt, OB_CRYPT_AUTH_SALT_LEN,
      OB_CRYPT_AUTH_ROUNDS_MIN, allocator, output);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GT(output.length(), 0);
}

TEST(ObSm3Crypt, generate_sm3_multi_hash_rounds_large)
{
  // Large-round test: verify the function succeeds with a moderately large
  // but executable round count (100000 iterations). OB_CRYPT_AUTH_ROUNDS_MAX
  // (999999999) would take too long for a unit test.
  ObArenaAllocator allocator("Sm3Test");
  ObString output;
  const char *password = "test_large_rounds";
  const char fixed_salt[OB_CRYPT_AUTH_SALT_LEN] = "fixed_salt_01234567";

  int ret = ObSm3Crypt::generate_sm3_multi_hash(
      password, strlen(password), fixed_salt, OB_CRYPT_AUTH_SALT_LEN,
      100000, allocator, output);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GT(output.length(), 0);
}

TEST(ObSm3Crypt, generate_sm3_multi_hash_zero_length_salt)
{
  ObArenaAllocator allocator("Sm3Test");
  ObString output;
  const char *password = "test";
  const char *salt = "x";  // non-null but zero-length

  int ret = ObSm3Crypt::generate_sm3_multi_hash(
      password, strlen(password), salt, 0,
      5000, allocator, output);
  ASSERT_NE(OB_SUCCESS, ret);
}

// ========== Fast Auth Scramble Wrong Length Tests (scramble & cached_digest) ==========

TEST(ObSm3Crypt, fast_auth_scramble_wrong_scramble_length)
{
  // Valid client response (32 bytes) but wrong scramble length (not 20)
  unsigned char response[OB_SM3_DIGEST_LENGTH];
  MEMSET(response, 0xAA, sizeof(response));
  unsigned char scramble[10];  // Too short, should be OB_CRYPT_AUTH_SALT_LEN (20)
  MEMSET(scramble, 0xBB, sizeof(scramble));
  unsigned char cached[OB_SM3_DIGEST_LENGTH];
  MEMSET(cached, 0xCC, sizeof(cached));

  ObString resp_str(sizeof(response), reinterpret_cast<const char*>(response));
  ObString scramble_str(sizeof(scramble), reinterpret_cast<const char*>(scramble));
  ObString cached_str(sizeof(cached), reinterpret_cast<const char*>(cached));
  bool is_match = true;
  int ret = ObSm3Crypt::verify_fast_auth_scramble(
      resp_str, scramble_str, cached_str, is_match);
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST(ObSm3Crypt, fast_auth_scramble_wrong_cached_digest_length)
{
  // Valid client response (32 bytes), valid scramble (20 bytes), but wrong cached digest (not 32)
  unsigned char response[OB_SM3_DIGEST_LENGTH];
  MEMSET(response, 0xAA, sizeof(response));
  unsigned char scramble[OB_CRYPT_AUTH_SALT_LEN];
  MEMSET(scramble, 0xBB, sizeof(scramble));
  unsigned char cached[16];  // Too short, should be OB_SM3_DIGEST_LENGTH (32)
  MEMSET(cached, 0xCC, sizeof(cached));

  ObString resp_str(sizeof(response), reinterpret_cast<const char*>(response));
  ObString scramble_str(sizeof(scramble), reinterpret_cast<const char*>(scramble));
  ObString cached_str(sizeof(cached), reinterpret_cast<const char*>(cached));
  bool is_match = true;
  int ret = ObSm3Crypt::verify_fast_auth_scramble(
      resp_str, scramble_str, cached_str, is_match);
  ASSERT_NE(OB_SUCCESS, ret);
}

// ========== Rounds Clamping/Error Tests ==========

TEST(ObSm3Crypt, rounds_below_minimum)
{
  // rounds=500 does not satisfy (>OB_CRYPT_AUTH_ROUNDS_DEFAULT && multiple of 1000),
  // so it falls back to OB_CRYPT_AUTH_ROUNDS_DEFAULT (5000).
  ObString password("test_password");
  char enc_buf[OB_SM3_PASSWD_BUF_LEN];
  MEMSET(enc_buf, 0, sizeof(enc_buf));
  ObString encrypted_pass;

  int ret = ObSm3Crypt::encrypt_passwd_to_ob_sm3(
      password, encrypted_pass, enc_buf, sizeof(enc_buf), 500);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObString salt, digest;
  int64_t iterations = 0;
  ASSERT_EQ(OB_SUCCESS, ObSm3Crypt::deserialize_auth_string(
      encrypted_pass, salt, digest, iterations));
  ASSERT_EQ(OB_CRYPT_AUTH_ROUNDS_DEFAULT, iterations);
}

TEST(ObSm3Crypt, rounds_large_value)
{
  // Test with a large but executable round count (100000).
  // OB_CRYPT_AUTH_ROUNDS_MAX (999999999) would take too long for a unit test;
  // clamping at max must be verified by the production code itself.
  ObString password("test_password");
  char enc_buf[OB_SM3_PASSWD_BUF_LEN];
  MEMSET(enc_buf, 0, sizeof(enc_buf));
  ObString encrypted_pass;

  int ret = ObSm3Crypt::encrypt_passwd_to_ob_sm3(
      password, encrypted_pass, enc_buf, sizeof(enc_buf), 100000);
  ASSERT_EQ(OB_SUCCESS, ret);

  // Verify the result can be deserialized
  ObString salt, digest;
  int64_t iterations = 0;
  int deser_ret = ObSm3Crypt::deserialize_auth_string(
      encrypted_pass, salt, digest, iterations);
  ASSERT_EQ(OB_SUCCESS, deser_ret);
  ASSERT_EQ(100000, iterations);
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
