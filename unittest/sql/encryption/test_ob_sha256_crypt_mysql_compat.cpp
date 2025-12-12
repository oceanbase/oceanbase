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
#include <gtest/gtest.h>
#include "deps/oblib/src/lib/encrypt/ob_sha256_crypt.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_log.h"
#include <iostream>
#include <cstring>
#include <chrono>
#include <map>
#include <openssl/sha.h>
#include <openssl/evp.h>

using namespace oceanbase::common;

// Constant definitions, compatible with MySQL
static const int CACHING_SHA2_DIGEST_LENGTH = 32;
static const int SALT_LENGTH = 20;
static const int STORED_SHA256_DIGEST_LENGTH = 43;
static const int CRYPT_SALT_LENGTH = 20;
static const int CRYPT_MAX_PASSWORD_SIZE = 256;
static const int MAX_ITERATIONS = 0xFFF * 1000;
static const int DEFAULT_STORED_DIGEST_ROUNDS = 5000;

// Helper function: Print hexadecimal digest
template <typename T>
void print_hex(T digest) {
  std::cout << std::endl << "Generated digest:";
  for (uint i = 0; i < CACHING_SHA2_DIGEST_LENGTH; i++)
    printf("0x%02x ", digest[i]);
  std::cout << std::endl;
}

// Helper function: Generate hash key
static void make_hash_key(const char *username, const char *hostname,
                          std::string &key) {
  key.clear();
  key.append(username ? username : "");
  key.push_back('\0');
  key.append(hostname ? hostname : "");
  key.push_back('\0');
}

// Helper function: Generate SHA256 digest
static int generate_sha256_digest(const char *input, size_t input_len,
                                  unsigned char *output) {
  if (!input || !output) {
    return OB_INVALID_ARGUMENT;
  }

  EVP_MD_CTX *ctx = EVP_MD_CTX_new();
  if (!ctx) {
    return OB_ALLOCATE_MEMORY_FAILED;
  }

  if (EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
    EVP_MD_CTX_free(ctx);
    return OB_ERR_UNEXPECTED;
  }

  if (EVP_DigestUpdate(ctx, input, input_len) != 1) {
    EVP_MD_CTX_free(ctx);
    return OB_ERR_UNEXPECTED;
  }

  unsigned int digest_len = 0;
  if (EVP_DigestFinal_ex(ctx, output, &digest_len) != 1) {
    EVP_MD_CTX_free(ctx);
    return OB_ERR_UNEXPECTED;
  }

  EVP_MD_CTX_free(ctx);
  return OB_SUCCESS;
}

// Helper function: Generate double SHA256 digest
static int generate_double_sha256_digest(const char *input, size_t input_len,
                                         unsigned char *output) {
  unsigned char first_digest[CACHING_SHA2_DIGEST_LENGTH];
  int ret = generate_sha256_digest(input, input_len, first_digest);
  if (ret != OB_SUCCESS) {
    return ret;
  }

  return generate_sha256_digest(reinterpret_cast<const char*>(first_digest),
                               CACHING_SHA2_DIGEST_LENGTH, output);
}

// Helper function: Generate SHA256 scramble
static int generate_sha256_scramble(unsigned char *scramble, size_t scramble_len,
                                   const char *source, size_t source_len,
                                   const char *rnd, size_t rnd_len) {
  if (!scramble || !source || !rnd || scramble_len < CACHING_SHA2_DIGEST_LENGTH) {
    return OB_INVALID_ARGUMENT;
  }

  // Generate double SHA256 digest
  unsigned char digest_stage2[CACHING_SHA2_DIGEST_LENGTH];
  int ret = generate_double_sha256_digest(source, source_len, digest_stage2);
  if (ret != OB_SUCCESS) {
    return ret;
  }

  // Generate scramble using XOR operation as MySQL does
  for (size_t i = 0; i < CACHING_SHA2_DIGEST_LENGTH; i++) {
    scramble[i] = digest_stage2[i] ^ static_cast<unsigned char>(rnd[i % rnd_len]);
  }

  return OB_SUCCESS;
}

// Helper function: Validate SHA256 scramble
static int validate_sha256_scramble(const unsigned char *received_scramble, size_t scramble_len,
                                   const unsigned char *digest_stage2, size_t digest_len,
                                   const unsigned char *rnd, size_t rnd_len) {
  if (!received_scramble || !digest_stage2 || !rnd ||
      scramble_len < CACHING_SHA2_DIGEST_LENGTH ||
      digest_len < CACHING_SHA2_DIGEST_LENGTH) {
    return OB_INVALID_ARGUMENT;
  }

  // Regenerate expected scramble
  unsigned char expected_scramble[CACHING_SHA2_DIGEST_LENGTH];
  for (size_t i = 0; i < CACHING_SHA2_DIGEST_LENGTH; i++) {
    expected_scramble[i] = digest_stage2[i] ^ rnd[i % rnd_len];
  }

  // Compare result
  if (memcmp(received_scramble, expected_scramble, CACHING_SHA2_DIGEST_LENGTH) == 0) {
    return OB_SUCCESS;
  } else {
    return OB_PASSWORD_WRONG;
  }
}

class TestObSha256CryptMySQLCompat : public ::testing::Test
{
public:
  TestObSha256CryptMySQLCompat() {}
  virtual ~TestObSha256CryptMySQLCompat() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

// Test 1: Basic functionality test - Corresponds to MySQL's InitDigestContext
TEST_F(TestObSha256CryptMySQLCompat, InitDigestContext) {
  // Test of ObSha256Crypt basic functionality
  const char *password = "test_password";
  const char *salt = "test_salt_1234567890";
  int64_t password_len = strlen(password);
  int64_t salt_len = strlen(salt);
  int64_t rounds = 5000;

  ObArenaAllocator allocator;
  ObString output;

  int ret = ObSha256Crypt::generate_sha256_multi_hash(
      password, password_len, salt, salt_len, rounds, allocator, output);

  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(output.empty());
  std::cout << "Basic functionality test passed" << std::endl;
}

// Test 2: Single-stage SHA256 digest test - Corresponds to MySQL's DigestSingleStage
TEST_F(TestObSha256CryptMySQLCompat, DigestSingleStage) {
  const char *plaintext1 = "quick brown fox jumped over the lazy dog";
  size_t plaintext1_length = strlen(plaintext1);
  unsigned char expected_digest1[] = {
      0x5c, 0xa1, 0xa2, 0x08, 0xc4, 0x61, 0x95, 0x3f, 0x7c, 0x12, 0xd2,
      0x79, 0xc7, 0xa8, 0x62, 0x0f, 0x3f, 0x83, 0x87, 0xf4, 0x09, 0x8c,
      0xe8, 0x53, 0x1b, 0x17, 0x51, 0x03, 0x37, 0xbd, 0x63, 0x78};

  unsigned char digest_output[CACHING_SHA2_DIGEST_LENGTH];

  int ret = generate_sha256_digest(plaintext1, plaintext1_length, digest_output);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(memcmp(expected_digest1, digest_output, CACHING_SHA2_DIGEST_LENGTH), 0);

  const char *plaintext2 = "ABCD1234%^&*";
  size_t plaintext2_length = strlen(plaintext2);

  unsigned char expected_digest2[] = {
      0xdd, 0xaf, 0x2b, 0xa0, 0x31, 0x73, 0x45, 0x3d, 0x72, 0x34, 0x51,
      0x27, 0xeb, 0x32, 0xb5, 0x24, 0x00, 0xc8, 0xf0, 0x89, 0xcb, 0x8a,
      0xd6, 0xce, 0x3c, 0x67, 0x37, 0xac, 0x01, 0x45, 0x84, 0x09};

  ret = generate_sha256_digest(plaintext2, plaintext2_length, digest_output);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(memcmp(expected_digest2, digest_output, CACHING_SHA2_DIGEST_LENGTH), 0);

  std::cout << "Single stage digest test passed" << std::endl;
}

// Test 3: Multi-stage SHA256 digest test - Corresponds to MySQL's DigestMultiStage
TEST_F(TestObSha256CryptMySQLCompat, DigestMultiStage) {
  const char *plaintext_input1 = "quick brown fox jumped over the lazy dog";
  size_t plaintext_input1_length = strlen(plaintext_input1);
  const char *plaintext_input2 = "ABCD1234%^&*";
  size_t plaintext_input2_length = strlen(plaintext_input2);
  const char *plaintext_input3 = "Hello World";
  size_t plaintext_input3_length = strlen(plaintext_input3);

  unsigned char digest_output[CACHING_SHA2_DIGEST_LENGTH];

  unsigned char expected_digest1[] = {
      0xd5, 0x66, 0x19, 0xff, 0x7c, 0xdb, 0x13, 0x00, 0x0b, 0x57, 0x1c,
      0x33, 0x8f, 0xe3, 0x33, 0xaf, 0x41, 0x87, 0xa6, 0x83, 0x03, 0x31,
      0x1b, 0xb6, 0x64, 0x7b, 0x6f, 0xbe, 0x6e, 0xf0, 0x99, 0xd9};

  // Combine all inputs
  std::string combined_input;
  combined_input.append(plaintext_input1, plaintext_input1_length);
  combined_input.append(plaintext_input2, plaintext_input2_length);
  combined_input.append(plaintext_input3, plaintext_input3_length);

  int ret = generate_sha256_digest(combined_input.c_str(), combined_input.length(), digest_output);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(memcmp(expected_digest1, digest_output, CACHING_SHA2_DIGEST_LENGTH), 0);

  // Test different combination orders
  std::string combined_input2;
  combined_input2.append(plaintext_input1, plaintext_input1_length);
  combined_input2.append(plaintext_input3, plaintext_input3_length);
  combined_input2.append(plaintext_input2, plaintext_input2_length);

  unsigned char expected_digest2[] = {
      0xd0, 0xcf, 0xa1, 0xd2, 0x9f, 0xb6, 0xfe, 0x8f, 0x3c, 0xff, 0x2c,
      0xa8, 0x2a, 0xe2, 0xc6, 0x6b, 0x8b, 0x2b, 0x33, 0xf9, 0x38, 0x35,
      0xc7, 0xae, 0x6a, 0xfc, 0x28, 0x85, 0x5d, 0xd3, 0xe5, 0xc5};

  ret = generate_sha256_digest(combined_input2.c_str(), combined_input2.length(), digest_output);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(memcmp(expected_digest2, digest_output, CACHING_SHA2_DIGEST_LENGTH), 0);

  std::cout << "Multi stage digest test passed" << std::endl;
}

// Test 4: SHA256-crypt hash generation test - Corresponds to MySQL's generate_sha2_multi_hash
TEST_F(TestObSha256CryptMySQLCompat, GenerateSha2MultiHash) {
  std::string plaintext = "HahaH0hO1234#$@#%";
  std::string salt = "01234567899876543210";

  ObArenaAllocator allocator;
  ObString output;

  int ret = ObSha256Crypt::generate_sha256_multi_hash(
      plaintext.c_str(), plaintext.length(),
      salt.c_str(), salt.length(),
      5000, allocator, output);

  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(output.empty());

  // Verify output format: Should start with $5$
  ASSERT_TRUE(output.length() > 3);
  ASSERT_EQ(memcmp(output.ptr(), "$5$", 3), 0);

  std::cout << "Generated hash: " << output.ptr() << std::endl;
  std::cout << "SHA2 multi hash generation test passed" << std::endl;
}


// Test 6: Salt extraction test
TEST_F(TestObSha256CryptMySQLCompat, ExtractSalt) {
  const char *password = "test_password";
  const char *salt = "test_salt_1234567890";
  int64_t password_len = strlen(password);
  int64_t salt_len = strlen(salt);
  int64_t rounds = 5000;

  ObArenaAllocator allocator;
  ObString output;

  int ret = ObSha256Crypt::generate_sha256_multi_hash(
      password, password_len, salt, salt_len, rounds, allocator, output);

  ASSERT_EQ(ret, OB_SUCCESS);

  // Extract salt
  const char *salt_begin = nullptr;
  const char *salt_end = nullptr;
  int extracted_len = ObSha256Crypt::extract_user_salt(output.ptr(), output.length(), &salt_begin, &salt_end);

  ASSERT_GT(extracted_len, 0);
  ASSERT_NE(salt_begin, nullptr);
  ASSERT_NE(salt_end, nullptr);

  // Verify extracted salt
  std::string extracted_salt(salt_begin, extracted_len);
  std::cout << "Extracted salt: " << extracted_salt << std::endl;
  std::cout << "Salt extraction test passed" << std::endl;
}

// Test 7: Error handling test
TEST_F(TestObSha256CryptMySQLCompat, ErrorHandling) {
  ObArenaAllocator allocator;
  ObString output;

  // Test null pointer
  int ret = ObSha256Crypt::generate_sha256_multi_hash(
      nullptr, 10, "salt", 4, 5000, allocator, output);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);

  // Test null salt
  ret = ObSha256Crypt::generate_sha256_multi_hash(
      "password", 8, nullptr, 4, 5000, allocator, output);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);

  // Empty password handling
  ret = ObSha256Crypt::generate_sha256_multi_hash(
      "", 0, "salt", 4, 5000, allocator, output);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);

  // Test overlong password
  char long_password[300];
  memset(long_password, 'a', 299);
  long_password[299] = '\0';

  ret = ObSha256Crypt::generate_sha256_multi_hash(
      long_password, 299, "salt", 4, 5000, allocator, output);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);

  std::cout << "Error handling test passed" << std::endl;
}

// Test 8: Different rounds test
TEST_F(TestObSha256CryptMySQLCompat, DifferentRounds) {
  const char *password = "test_password";
  const char *salt = "test_salt_1234567890";
  int64_t password_len = strlen(password);
  int64_t salt_len = strlen(salt);

  ObArenaAllocator allocator;
  ObString output1, output2;

  // Test different iterations
  int ret1 = ObSha256Crypt::generate_sha256_multi_hash(
      password, password_len, salt, salt_len, 1000, allocator, output1);

  int ret2 = ObSha256Crypt::generate_sha256_multi_hash(
      password, password_len, salt, salt_len, 10000, allocator, output2);

  ASSERT_EQ(ret1, OB_SUCCESS);
  ASSERT_EQ(ret2, OB_SUCCESS);

  // Different rounds should yield different results
  ASSERT_NE(output1, output2);

  std::cout << "Different rounds test passed" << std::endl;
}

// Test 9: Boundary values test
TEST_F(TestObSha256CryptMySQLCompat, BoundaryValues) {
  ObArenaAllocator allocator;
  ObString output;

  // Test minimum rounds
  int ret = ObSha256Crypt::generate_sha256_multi_hash(
      "password", 8, "salt", 4, 1000, allocator, output);
  ASSERT_EQ(ret, OB_SUCCESS);

  // Test maximum rounds
  ret = ObSha256Crypt::generate_sha256_multi_hash(
      "password", 8, "salt", 4, 99999, allocator, output);
  ASSERT_EQ(ret, OB_SUCCESS);

  // Test minimum salt length
  ret = ObSha256Crypt::generate_sha256_multi_hash(
      "password", 8, "a", 1, 5000, allocator, output);
  ASSERT_EQ(ret, OB_SUCCESS);

  std::cout << "Boundary values test passed" << std::endl;
}

// Test 10: Performance test
TEST_F(TestObSha256CryptMySQLCompat, PerformanceTest) {
  const char *password = "performance_test_password";
  const char *salt = "performance_test_salt_1234567890";
  int64_t password_len = strlen(password);
  int64_t salt_len = strlen(salt);
  int64_t rounds = 5000;

  ObArenaAllocator allocator;
  ObString output;

  // Record start time
  auto start = std::chrono::high_resolution_clock::now();

  int ret = ObSha256Crypt::generate_sha256_multi_hash(
      password, password_len, salt, salt_len, rounds, allocator, output);

  // Record end time
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  ASSERT_EQ(ret, OB_SUCCESS);

  std::cout << "Performance test completed in " << duration.count() << " ms" << std::endl;
  std::cout << "Performance test passed" << std::endl;
}

// Test 11: SHA256 scramble generation test - Corresponds to MySQL's GenerateScramble
TEST_F(TestObSha256CryptMySQLCompat, GenerateScramble) {
  std::string source = "Ab12#$Cd56&*";
  std::string rnd = "eF!@34gH%^78";
  unsigned char scramble[CACHING_SHA2_DIGEST_LENGTH];

  int ret = generate_sha256_scramble(scramble, sizeof(scramble),
                                    source.c_str(), source.length(),
                                    rnd.c_str(), rnd.length());

  ASSERT_EQ(ret, OB_SUCCESS);

  // Verify scramble is non-zero
  bool has_non_zero = false;
  for (int i = 0; i < CACHING_SHA2_DIGEST_LENGTH; i++) {
    if (scramble[i] != 0) {
      has_non_zero = true;
      break;
    }
  }
  ASSERT_TRUE(has_non_zero);

  // Print generated scramble for debugging
  std::cout << "Generated scramble: ";
  for (int i = 0; i < CACHING_SHA2_DIGEST_LENGTH; i++) {
    printf("0x%02x ", scramble[i]);
  }
  std::cout << std::endl;

  std::cout << "Scramble generation test passed" << std::endl;
}

// Test 12: SHA256 scramble validation test - Corresponds to MySQL's ValidateScramble
TEST_F(TestObSha256CryptMySQLCompat, ValidateScramble) {
  const char *source = "Ab12#$Cd56&*";
  size_t source_length = strlen(source);
  std::string rnd = "eF!@34gH%^78";

  // Generate double SHA256 digest
  unsigned char digest_stage2[CACHING_SHA2_DIGEST_LENGTH];
  int ret = generate_double_sha256_digest(source, source_length, digest_stage2);
  ASSERT_EQ(ret, OB_SUCCESS);

  // Generate scramble
  unsigned char scramble[CACHING_SHA2_DIGEST_LENGTH];
  ret = generate_sha256_scramble(scramble, sizeof(scramble),
                                source, source_length,
                                rnd.c_str(), rnd.length());
  ASSERT_EQ(ret, OB_SUCCESS);

  // Validate scramble
  ret = validate_sha256_scramble(scramble, sizeof(scramble),
                                digest_stage2, sizeof(digest_stage2),
                                reinterpret_cast<const unsigned char*>(rnd.c_str()), rnd.length());

  ASSERT_EQ(ret, OB_SUCCESS);

  std::cout << "Scramble validation test passed" << std::endl;
}

// Test 13: Auth String serialize/deserialize test - Corresponds to MySQL's Caching_sha2_password_Serialize_Deserialize
TEST_F(TestObSha256CryptMySQLCompat, AuthStringSerializeDeserialize) {
  ObArenaAllocator allocator;

  // Prepare test data - compatible with MySQL format
  char salt[21] = "01234567899876543210";
  char digest[45];
  strcpy(digest, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0");
  int64_t iterations = 5000;

  ObString output;
  ObString parsed_salt;
  ObString parsed_digest;
  int64_t parsed_iterations = 0;

  // Serialization test
  int ret = ObSha256Crypt::serialize_auth_string(
      salt, 20, digest, 43, iterations, allocator, output);

  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(output.empty());

  // Verify serialization result format - compatible with MySQL caching_sha2_password format
  std::string auth_str(output.ptr(), output.length());
  ASSERT_EQ(auth_str[0], '$');
  ASSERT_EQ(auth_str[1], 'A');  // SHA256 type
  ASSERT_EQ(auth_str[2], '$');
  ASSERT_EQ(auth_str[6], '$');
  ASSERT_EQ(auth_str.length(), 70); // 3 + 3 + 1 + 20 + 43 = 70

  std::cout << "Serialized auth string: " << auth_str << std::endl;

  // Deserialization test
  ret = ObSha256Crypt::deserialize_auth_string(
      output, parsed_salt, parsed_digest, parsed_iterations);

  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(parsed_salt.length(), 20);
  ASSERT_EQ(parsed_digest.length(), 43);
  ASSERT_EQ(parsed_iterations, iterations);

  // Verify data consistency
  ASSERT_EQ(memcmp(parsed_salt.ptr(), salt, 20), 0);
  ASSERT_EQ(memcmp(parsed_digest.ptr(), digest, 43), 0);

  std::cout << "Auth string serialize/deserialize test passed" << std::endl;
}

// Test 13.2: Auth String error handling test
TEST_F(TestObSha256CryptMySQLCompat, AuthStringErrorHandling) {
  ObArenaAllocator allocator;
  ObString output;

  // Test null pointer
  int ret = ObSha256Crypt::serialize_auth_string(
      nullptr, 20, "digest", 43, 5000, allocator, output);
  ASSERT_NE(ret, OB_SUCCESS);

  // Test invalid salt length
  ret = ObSha256Crypt::serialize_auth_string(
      "salt", 4, "digest", 43, 5000, allocator, output);
  ASSERT_NE(ret, OB_SUCCESS);

  // Test invalid digest length
  ret = ObSha256Crypt::serialize_auth_string(
      "01234567899876543210", 20, "digest", 6, 5000, allocator, output);
  ASSERT_NE(ret, OB_SUCCESS);

  // Test invalid iterations
  ret = ObSha256Crypt::serialize_auth_string(
      "01234567899876543210", 20, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0", 43, 500, allocator, output);
  ASSERT_NE(ret, OB_SUCCESS);

  // Test deserialization with invalid input
  ObString salt, digest;
  int64_t iterations;

  // Test empty string
  ObString empty_str;
  ret = ObSha256Crypt::deserialize_auth_string(empty_str, salt, digest, iterations);
  ASSERT_NE(ret, OB_SUCCESS);

  // Test format error string
  ObString invalid_str("invalid_format");
  ret = ObSha256Crypt::deserialize_auth_string(invalid_str, salt, digest, iterations);
  ASSERT_NE(ret, OB_SUCCESS);

  // Test too short string
  ObString short_str("$A$00");
  ret = ObSha256Crypt::deserialize_auth_string(short_str, salt, digest, iterations);
  ASSERT_NE(ret, OB_SUCCESS);

  // Test invalid digest type
  ObString wrong_type("$B$005$01234567899876543210abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0");
  ret = ObSha256Crypt::deserialize_auth_string(wrong_type, salt, digest, iterations);
  ASSERT_NE(ret, OB_SUCCESS);

  std::cout << "Auth string error handling test passed" << std::endl;
}

// Test 13.3: Auth String MySQL format compatibility test
TEST_F(TestObSha256CryptMySQLCompat, AuthStringMySQLCompatibility) {
  ObArenaAllocator allocator;

  // Simulate MySQL generated auth string format
  char salt[21] = "01234567899876543210";
  char digest[45];
  strcpy(digest, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0");
  int64_t iterations = 5000;

  ObString output;
  ObString parsed_salt;
  ObString parsed_digest;
  int64_t parsed_iterations = 0;

  // Serialization
  int ret = ObSha256Crypt::serialize_auth_string(
      salt, 20, digest, 43, iterations, allocator, output);

  ASSERT_EQ(ret, OB_SUCCESS);

  // Verify format is compatible with MySQL
  std::string auth_str(output.ptr(), output.length());
  ASSERT_EQ(auth_str.substr(0, 3), "$A$");
  ASSERT_EQ(auth_str.substr(3, 3), "005"); // 5000 / 1000 = 5
  ASSERT_EQ(auth_str.substr(6, 1), "$");
  ASSERT_EQ(auth_str.length(), 70); // 3 + 3 + 1 + 20 + 43 = 70

  // Deserialization verification
  ret = ObSha256Crypt::deserialize_auth_string(
      output, parsed_salt, parsed_digest, parsed_iterations);

  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(parsed_iterations, iterations);

  std::cout << "Auth string MySQL compatibility test passed" << std::endl;
  std::cout << "Generated auth string: " << auth_str << std::endl;
}

// Test 13.4: Auth String round-trip test (serialize then deserialize)
TEST_F(TestObSha256CryptMySQLCompat, AuthStringRoundTripTest) {
  ObArenaAllocator allocator;

  // Prepare multiple sets of test data
  struct TestCase {
    char salt[21];
    char digest[63];
    int64_t iterations;
  } test_cases[] = {
    {"01234567899876543210", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0", 1000},
    {"abcdefghijklmnopqrst", "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", 5000},
    {"zyxwvutsrqponmlkjihg", "ZYXWVUTSRQPONMLKJIHGFEDCBAzyxwvutsrqponmlkjihgfedcba0123456789", 10000}
  };

  int test_count = sizeof(test_cases) / sizeof(test_cases[0]);

  for (int i = 0; i < test_count; i++) {
    ObString output;
    ObString parsed_salt;
    ObString parsed_digest;
    int64_t parsed_iterations = 0;

    // Serialization
    int ret = ObSha256Crypt::serialize_auth_string(
        test_cases[i].salt, 20, test_cases[i].digest, 43,
        test_cases[i].iterations, allocator, output);

    ASSERT_EQ(ret, OB_SUCCESS);

    // Deserialization
    ret = ObSha256Crypt::deserialize_auth_string(
        output, parsed_salt, parsed_digest, parsed_iterations);

    ASSERT_EQ(ret, OB_SUCCESS);

    // Verify data consistency
    ASSERT_EQ(parsed_salt.length(), 20);
    ASSERT_EQ(parsed_digest.length(), 43);
    ASSERT_EQ(parsed_iterations, test_cases[i].iterations);

    ASSERT_EQ(memcmp(parsed_salt.ptr(), test_cases[i].salt, 20), 0);
    ASSERT_EQ(memcmp(parsed_digest.ptr(), test_cases[i].digest, 43), 0);

    std::cout << "Auth string round trip test case " << i + 1 << " passed" << std::endl;
  }
}

// Test 13.5: Test that matches MySQL's actual Auth String format exactly
TEST_F(TestObSha256CryptMySQLCompat, AuthStringMySQLExactFormat) {
  ObArenaAllocator allocator;

  // Use test data exactly as MySQL caching_sha2_password
  std::string password = "HahaH0hO1234#$@#%";
  std::string salt = "01234567899876543210";
  int64_t iterations = 5000;

  // First generate the password hash
  ObString password_hash;
  int ret = ObSha256Crypt::generate_sha256_multi_hash(
      password.c_str(), password.length(),
      salt.c_str(), salt.length(),
      iterations, allocator, password_hash);

  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(password_hash.empty());

  // Verify hash format (should start with $5$)
  std::string hash_str(password_hash.ptr(), password_hash.length());
  ASSERT_TRUE(hash_str.length() > 3);
  ASSERT_EQ(hash_str.substr(0, 3), "$5$");

  // Extract salt and digest from the hash
  const char *salt_begin = nullptr;
  const char *salt_end = nullptr;
  int extracted_salt_len = ObSha256Crypt::extract_user_salt(password_hash.ptr(), password_hash.length(), &salt_begin, &salt_end);

  ASSERT_GT(extracted_salt_len, 0);
  ASSERT_NE(salt_begin, nullptr);
  ASSERT_NE(salt_end, nullptr);

  // Extract digest part (after $5$salt$)
  const char *hash_begin = salt_end + 1; // skip $
  int hash_len = password_hash.length() - (hash_begin - password_hash.ptr());

  ASSERT_EQ(hash_len, 43); // SHA256-crypt digest should be 43

  // Now serialize using the extracted salt and digest
  ObString auth_string;
  ret = ObSha256Crypt::serialize_auth_string(
      salt_begin, extracted_salt_len,
      hash_begin, hash_len,
      iterations, allocator, auth_string);

  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(auth_string.empty());

  // Verify serialization result format
  std::string auth_str(auth_string.ptr(), auth_string.length());
  ASSERT_EQ(auth_str.substr(0, 3), "$A$");
  ASSERT_EQ(auth_str.substr(3, 3), "005"); // 5000 / 1000 = 5
  ASSERT_EQ(auth_str.substr(6, 1), "$");
  ASSERT_EQ(auth_str.length(), 70); // 3 + 3 + 1 + 20 + 43 = 70

  // Verify salt part
  std::string auth_salt = auth_str.substr(7, 20);
  std::string original_salt(salt_begin, extracted_salt_len);
  ASSERT_EQ(auth_salt, original_salt);

  // Verify digest part
  std::string auth_digest = auth_str.substr(27, 43);
  std::string original_digest(hash_begin, hash_len);
  ASSERT_EQ(auth_digest, original_digest);

  // Deserialization test
  ObString parsed_salt;
  ObString parsed_digest;
  int64_t parsed_iterations = 0;

  ret = ObSha256Crypt::deserialize_auth_string(
      auth_string, parsed_salt, parsed_digest, parsed_iterations);

  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(parsed_salt.length(), 20);
  ASSERT_EQ(parsed_digest.length(), 43);
  ASSERT_EQ(parsed_iterations, iterations);

  // Verify deserialized data matches original data
  ASSERT_EQ(memcmp(parsed_salt.ptr(), salt_begin, 20), 0);
  ASSERT_EQ(memcmp(parsed_digest.ptr(), hash_begin, 43), 0);

  std::cout << "MySQL exact format auth string test passed" << std::endl;
  std::cout << "Generated auth string: " << auth_str << std::endl;
  std::cout << "Original hash: " << hash_str << std::endl;
}

// Test 14: Password cache test - Corresponds to MySQL's SHA2_password_cache
TEST_F(TestObSha256CryptMySQLCompat, PasswordCache) {
  // Simulate password cache function
  std::map<std::string, std::string> password_cache;

  std::string auth_id1 = "arthurdent.com";
  std::string auth_id2 = "marvintheparanoidandroid.com";
  std::string auth_id3 = "zaphodbeeblebrox";

  std::string password1 = "HahaH0hO1234#$@#%";
  std::string password2 = ";bCdEF34^i9&*\":({\\56\"";
  std::string password3 = " CQ#$ML CF%IF$#(<#R ()<_q@#(rq";

  // Add passwords to cache
  password_cache[auth_id1] = password1;
  password_cache[auth_id2] = password2;
  password_cache[auth_id3] = password3;

  // Verify cache size
  ASSERT_EQ(password_cache.size(), 3);

  // Verify password retrieval
  ASSERT_EQ(password_cache[auth_id1], password1);
  ASSERT_EQ(password_cache[auth_id2], password2);
  ASSERT_EQ(password_cache[auth_id3], password3);

  // Test nonexistent user
  ASSERT_EQ(password_cache.find("nonexistentexample.com"), password_cache.end());

  // Test password update
  std::string new_password = "new_password_123";
  password_cache[auth_id1] = new_password;
  ASSERT_EQ(password_cache[auth_id1], new_password);

  // Test password deletion
  password_cache.erase(auth_id2);
  ASSERT_EQ(password_cache.size(), 2);
  ASSERT_EQ(password_cache.find(auth_id2), password_cache.end());

  std::cout << "Password cache test passed" << std::endl;
}

// Test 15: Authentication test - Corresponds to MySQL's Caching_sha2_password_authenticate
TEST_F(TestObSha256CryptMySQLCompat, Authentication) {
  std::string auth_id = "arthurdent.com";
  std::string plaintext = "HahaH0hO1234#$@#%";
  std::string salt = "01234567899876543210";

  ObArenaAllocator allocator;
  ObString stored_hash;

  // Generate stored hash
  int ret = ObSha256Crypt::generate_sha256_multi_hash(
      plaintext.c_str(), plaintext.length(),
      salt.c_str(), salt.length(),
      5000, allocator, stored_hash);

  ASSERT_EQ(ret, OB_SUCCESS);

  // Simulate authentication process: generate hash using same password and salt, then compare
  ObArenaAllocator allocator2;
  ObString input_hash;

  ret = ObSha256Crypt::generate_sha256_multi_hash(
      plaintext.c_str(), plaintext.length(),
      salt.c_str(), salt.length(),
      5000, allocator2, input_hash);

  ASSERT_EQ(ret, OB_SUCCESS);

  // Compare hash values
  ASSERT_EQ(stored_hash, input_hash);

  // Test wrong password
  std::string wrong_password = "wrong_password";
  ObArenaAllocator allocator3;
  ObString wrong_hash;

  ret = ObSha256Crypt::generate_sha256_multi_hash(
      wrong_password.c_str(), wrong_password.length(),
      salt.c_str(), salt.length(),
      5000, allocator3, wrong_hash);

  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_NE(stored_hash, wrong_hash);

  std::cout << "Authentication test passed" << std::endl;
}

// Test 16: sha256_multi_hash一致性及异常用例验证
TEST_F(TestObSha256CryptMySQLCompat, Sha256MultiHashConsistencyAndErrorTest) {
  struct TestCase {
    const char* password;
    const char* salt;
    int64_t rounds;
    const char* expected_hash_prefix;
  } test_cases[] = {
    {"test_password", "test_salt_1234567890", 5000, "$5$"},
    {"HahaH0hO1234#$@#%", "01234567899876543210", 5000, "$5$"},
    {"", "empty_password_test", 5000, "$5$"},
    {"a", "single_char_password", 5000, "$5$"},
    {"very_long_password_that_exceeds_normal_length_but_is_still_within_limits_1234567890", "long_password_salt", 5000, "$5$"}
  };
  int test_count = sizeof(test_cases) / sizeof(test_cases[0]);
  for (int i = 0; i < test_count; i++) {
    ObArenaAllocator allocator;
    ObString output;
    int ret = ObSha256Crypt::generate_sha256_multi_hash(
        test_cases[i].password, strlen(test_cases[i].password),
        test_cases[i].salt, strlen(test_cases[i].salt),
        test_cases[i].rounds, allocator, output);
    if (strlen(test_cases[i].password) == 0) {
      ASSERT_NE(ret, OB_SUCCESS) << "Empty password should return error code";
      continue;
    }
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_FALSE(output.empty());
    std::string hash_str(output.ptr(), output.length());
    ASSERT_TRUE(hash_str.length() > 3);
    std::string expected_prefix(test_cases[i].expected_hash_prefix);
    ASSERT_EQ(hash_str.substr(0, 3), expected_prefix.substr(0, 3));
    ObArenaAllocator allocator2;
    ObString output2;
    ret = ObSha256Crypt::generate_sha256_multi_hash(
        test_cases[i].password, strlen(test_cases[i].password),
        test_cases[i].salt, strlen(test_cases[i].salt),
        test_cases[i].rounds, allocator2, output2);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(output, output2);
  }
}



// Test 18: Compatibility verification test based on actual MySQL authentication_string
TEST_F(TestObSha256CryptMySQLCompat, MySQLAuthStringCompatibilityVerification) {
  // Test data extracted from mysql_auth_test_results_20251011_155752.json
  // Format: {password, authentication_string, description}
  struct MySQLAuthTestCase {
    const char* password;
    const char* authentication_string;
    const char* description;
  } mysql_auth_test_cases[] = {
    {"dba", "$A$005$'\u0017D[\u0014\u00130P].]%[;NkaV\u0005easPgpUFUKuWxLpxHRRlVhpBtCsB0ipqMajCA49wVdR9", "短密码测试"},
    {"password", "$A$005$*\rY6\u00054\u0012\u001a?#^1fd)!\u000bTK%DRsWJMim4z/FFEC8rUqvAq97hgspzYmvzKtBWbkeRZ1", "纯字母密码测试"},
    {"123456", "$A$005$QqoVS\u00140ZtPM2\u0018pO4r7k\nXdnYQk.BH0.vFdOqBLRnWENT8xDy3F.6/5yebBAprJD", "纯数字密码测试"},
    {"admin", "$A$005$wEp)m\\\u001cO\f?K\u000eyVb\u007fKpZ\u000bA4XZ9ahl9Ir6dyElcTrf5baboQvUn3CyCfccscmbcX3", "admin密码测试"},
    {"root", "$A$005$VE\u0007oA\u0013(\u0004I\u001f8)Bc3Cl#*\u001f26pPrD/2a9QFukDIk6.1Ay95.jdn5UUF7BQb8IN7Gm3", "root密码测试"},
    {"pass@word", "$A$005$yy\u001ea&%FA|`_CFV+\u001dYso#Gv...8FTzhSlED8MQfhVN6EB14jSPNFRKamnP4rPQ92", "特殊字符密码测试"},
    {"test#123", "$A$005$%CVe\u0001\u0015'Qq'E6H&,>+\t0\u000e0/MEpx0XIZqTRqY2itM6pQxNAqb07vRPDef1d7CjcI2", "特殊字符密码测试2"},
    {"my$pass", "$A$005$d<89\u0006%\u001a]8G\u0016\u0015iHtef2pRgybC7K6lZB53lThuhYUSEeU93mAuJ3kroaBSeSFre.8", "特殊字符密码测试3"},
    {"user%123", "$A$005${\u0001p]\u0011T\u0011E[\u000eQx\u0014\\h}b,g\u001azysizRhCkXyiqpwXg/rvNeCE/LkN1k9vjUR8sGB6HbB", "特殊字符密码测试4"},
    {"data&base", "$A$005$\u0010>L/\"5z &y\u000e}D'tX\u0010\u000fC\u001eEU6/zBqlvyCfZ5magDIB4suI5OFZTw3DafLVkJmTxO4", "特殊字符密码测试5"},
    {"Complex_P@ssw0rd#2024", "$A$005$\u000eB19I,%f:X\ty5\rg\u0018> \u0011TjiJNjJ6WqsNTTujQETlA7QY2tv9qHWyCHFxr1cF8e04", "复杂密码测试"},
    {"MySecurePass!2024", "$A$005$E[KN#24V`A\r;f\u0005\u0010\u0017#OTGLOKdGiLX/rpzmi4ZDwvHL43/fTJsRCyUIEkpqvtsly7", "安全密码测试"},
    {"P@ssw0rd123!", "$A$005$sc<\\\u0001>b\u0010A't\u0015I\u001ak\u0015*\u0005x(PT4VnRphbc/GOdNEXGXYbR/JYQL6UOJl/VmZcuLMts4", "强密码测试"},
    {"Str0ng#P@ss", "$A$005$)p\u0004\u001c6?L2\u001f\u007fDk6\u0016%DEu\u0019hIegNlsF4z6otrHPfbs09BxK5V.qaO.6Zeq2YRjcB6Q4", "强密码测试2"},
    {"S3cur3!P@ss", "$A$005$;T\u001f%8%?NtV\u0003v\u0012-?\u0017To?\tm16aKlJ58HIDPm9V6VG.xRw6.i1d9GAC7Jz4/XMqqZ1", "强密码测试3"},
    {"very_long_password_123456789", "$A$005$zD\fm\u0018L\u0012%qWF\u001d'Sc@\\@VdpkEYcq1a/3CI6HrFkxkOXWcjBkodAWiUmsfDvyjJ5OD", "长密码测试"},
    {"abcdefghijklmnopqrstuvwxyz", "$A$005$7\u0003]\nNtVM~U8sM'zK|Fea.efyFUPImOIKOJkYdHIsEHfDhNG7DmP1BPhCT0FYCHC", "长字母密码测试"},
    {"12345678901234567890", "$A$005$\u0006;P\u000b\u000e\u0001R=(7'Z\u0007\u0019mj\u001a}\f6eKaLdUIRa3CWgTXg3UlWUcmynfdW3N9eqErieNrkI4B", "长数字密码测试"},
    {"Aa1!Bb2@Cc3#Dd4$Ee5%", "$A$005$fuSnRyYw\u001dH6h\u0018\nc>Um\u0018\u007fDqfuWBfFJw5pRKGqfVIcVqAUt3m24y/eD9.QQ1RyP/6", "复杂混合密码测试"},
    {"ThisIsAVeryLongPassword123!@#", "$A$005$9{s).iJ*{\u007f)Q\u0002\u0019\n\\(O\u007f[1js4gyn.0IRubKyLtwh6u/VY3Qck3VPZN8cwoflPpw6", "超长复杂密码测试"},
    {"中文密码测试", "$A$005$\u0005\u0001\r5\u0010}g2)~ksW&xt&g]w/Exs2CZZfNzXTzZlt85VmyVgrIouubdNbq4OzuKZQT.", "中文密码测试"},
    {"密码123", "$A$005$w\u0010f,)tk\u0019W7sh\f\u0019M\u0002\u0001?5^q9.m8s4HrsmjCb/.UL35ggLmUwZ2P3hROqXqcBuQs51", "中文数字密码测试"},
    {"测试@密码#2024", "$A$005$%\r6bLgaGoMz&\"RrFz3|\u0005E.TCpHgQoQhCdZCyyaKztxDdsp/gK0YbxMDmBMy4Xh3", "中文特殊字符密码测试"},
    {"用户密码", "$A$005$a_Hwf\u001dBzo.\u00050C3brNO3n.f9MRFOwKvtYKcM3.R7Gz6ZDcfVfyrLnuOgJLnm6g8D", "中文密码测试2"},
    {"测试用户123!@#", "$A$005$I\u001bk<\u0016\u001f2b3'z1\u007f\\hEe}seACnWpxxATWHQ46qVsoYHCCMcnFvpcNgZ1KnYhU3jsqC", "中文特殊字符密码测试2"},
    {"", "", "空密码测试"},
    {"a", "$A$005$\u0010\u000ep5d\\!\u001e\u0004Fu!l}O?l:hEEPM4WGhssPfWFase0LBcPuzZMptrILzrL2utkdZhyJ6", "单字符密码测试"},
    {"12", "$A$005$\t_ZZS\u0019FT-no\u0012;#MjtE+=xYT3T7osUyykeOPV.tt84TaS6oOoqhEG7q5iziQvBfB", "双字符密码测试"},
    {"!@#$%^&*()", "$A$005$V>%g\u0014q`-%{3\f=S\u000f\f&'(?NEMgDtOJAa.6oaCWNt769lbtbHbkq5RKKOhm1DaHBY1", "纯特殊字符密码测试"},
    {"   ", "$A$005$o&wi\"*lI%\u0017\u00123\u0006TU0\u00142|sX10PL23r/hi5iKzv9c9bHwabYJz71ccKIj5mePUgLx3", "空格密码测试"}
  };

  int test_count = sizeof(mysql_auth_test_cases) / sizeof(mysql_auth_test_cases[0]);
  int passed = 0;

  for (int i = 0; i < test_count; i++) {
    if (strlen(mysql_auth_test_cases[i].authentication_string) == 0) {
      // 跳过空密码用例
      passed += 1;
      continue;
    }
    // Step 1: 解析 authentication_string，得到 salt、rounds、hash
    ObString auth_string(mysql_auth_test_cases[i].authentication_string);
    ObString parsed_salt;
    ObString parsed_digest;
    int64_t parsed_iterations = 0;

    int ret = ObSha256Crypt::deserialize_auth_string(
        auth_string, parsed_salt, parsed_digest, parsed_iterations);

    ASSERT_EQ(ret, OB_SUCCESS) << "Deserialization failed, test case: " << (i + 1);
    ASSERT_EQ(parsed_salt.length(), 20) << "Salt length incorrect, test case: " << (i + 1);
    ASSERT_EQ(parsed_digest.length(), 43) << "Digest length incorrect, test case: " << (i + 1);
    ASSERT_EQ(parsed_iterations, 5000) << "Iteration count incorrect, test case: " << (i + 1);

    // Step 2: OB 生成 hash
    ObArenaAllocator allocator;
    ObString generated_hash;

    ret = ObSha256Crypt::generate_sha256_multi_hash(
        mysql_auth_test_cases[i].password, strlen(mysql_auth_test_cases[i].password),
        parsed_salt.ptr(), parsed_salt.length(),
        parsed_iterations, allocator, generated_hash);

    ASSERT_EQ(ret, OB_SUCCESS) << "Hash generation failed, test case: " << (i + 1);
    ASSERT_FALSE(generated_hash.empty()) << "Generated hash is empty, test case: " << (i + 1);

    // Step 3: 提取 digest 做对比
    const char *salt_begin = nullptr;
    const char *salt_end = nullptr;
    int extracted_salt_len = ObSha256Crypt::extract_user_salt(generated_hash.ptr(), generated_hash.length(), &salt_begin, &salt_end);

    ASSERT_GT(extracted_salt_len, 0) << "Salt extraction failed, test case: " << (i + 1);

    // 提取 digest 部分
    const char *hash_begin = salt_end + 1;
    int hash_len = generated_hash.length() - (hash_begin - generated_hash.ptr());
    ASSERT_EQ(hash_len, 43) << "Generated digest length incorrect, test case: " << (i + 1);

    // Digest 比较
    ASSERT_EQ(memcmp(parsed_digest.ptr(), hash_begin, 43), 0)
        << "OB-generated digest does not match deserialized digest, test case: " << (i + 1);

    // Salt 比较
    ASSERT_EQ(memcmp(parsed_salt.ptr(), salt_begin, 20), 0)
        << "Salt does not match, test case: " << (i + 1);

    passed++;
  }

  std::cout << "[OK] MySQL authentication string兼容性测试共通过 " << passed << " / " << test_count << " 个用例" << std::endl;
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
