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

#include <gtest/gtest.h>
#include <isa-l/erasure_code.h>
#include "lib/container/ob_array.h"
#include "lib/ec/ob_erasure_code_table_cache.h"
#include "lib/ec/ob_erasure_code_isa.h"

namespace oceanbase {
namespace common {

class ObECTableCacheTest : public testing::Test {
public:
  static void SetUpTestCase()
  {
    ASSERT_EQ(ObECCacheManager::get_ec_cache_mgr().init(1024 /*bucket num*/), OB_SUCCESS);
  }
  static void TearDownTestCase()
  {}

  bool erasure_contains(const ObArray<int64_t>& erasures, int64_t index)
  {
    int64_t count = erasures.count();
    for (int i = 0; i < count; i++) {
      if (erasures[i] == index)
        return true;
    }
    return false;
  }
};

TEST_F(ObECTableCacheTest, TestBase)
{
  INIT_SUCC(ret);
  ObECTableCache ec_cache;
  unsigned char* encoding_table = nullptr;
  unsigned char* encoding_matrix = nullptr;
  ObArenaAllocator allocator;
  int data_count = 4;
  int parity_count = 2;
  int64_t all_count = data_count + parity_count;

  ret = ec_cache.init(1024 /* bucket num */);

  // test set encoding matrix table into cache
  if (OB_SUCC(ret)) {
    int64_t matrix_size = data_count * all_count;
    if (nullptr == (encoding_matrix = (unsigned char*)allocator.alloc(matrix_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (nullptr == (encoding_table = (unsigned char*)allocator.alloc(matrix_size * 32))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    }
    ASSERT_EQ(ret, OB_SUCCESS);

    gf_gen_cauchy1_matrix(encoding_matrix, data_count + parity_count, data_count);
    ret = ec_cache.set_encoding_matrix(data_count, parity_count, encoding_matrix);
    ASSERT_EQ(ret, OB_SUCCESS);

    ec_init_tables(data_count, parity_count, &encoding_matrix[data_count * data_count], encoding_table);
    ret = ec_cache.set_encoding_table(data_count, parity_count, encoding_table);
    ASSERT_EQ(ret, OB_SUCCESS);
  }

  if (OB_SUCC(ret)) {
    bool compare_ret = false;
    ObArenaAllocator* allocator = new ObArenaAllocator(ObModIds::TEST);
    const static int64_t BUFFER_TEST_LEN = 2 * 1024 * 1024;

    ObArray<int64_t> erase_indexes;
    unsigned char* input_blocks[all_count];
    unsigned char* input_recovery_blocks[data_count];
    unsigned char* output_recover_blocks[parity_count];
    unsigned char* buf = nullptr;

    MEMSET(input_blocks, 0, sizeof(input_blocks));
    MEMSET(input_recovery_blocks, 0, sizeof(input_recovery_blocks));
    MEMSET(output_recover_blocks, 0, sizeof(output_recover_blocks));

    for (int i = 0; OB_SUCC(ret) && i < all_count; i++) {
      buf = (unsigned char*)allocator->alloc(BUFFER_TEST_LEN);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        input_blocks[i] = buf;
      }
    }

    for (int i = 0; OB_SUCC(ret) && i < parity_count; i++) {
      buf = (unsigned char*)allocator->alloc(BUFFER_TEST_LEN);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMSET(buf, 0, BUFFER_TEST_LEN);
        output_recover_blocks[i] = buf;
      }
    }

    if (OB_SUCC(ret)) {
      for (int i = 0; i < data_count; i++)
        for (int j = 0; j < BUFFER_TEST_LEN; j++)
          input_blocks[i][j] = (unsigned char)j;

      ec_encode_data(
          BUFFER_TEST_LEN, data_count, parity_count, encoding_table, input_blocks, &input_blocks[data_count]);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(erase_indexes.push_back(1))) {
      COMMON_LOG(WARN, "failed to push one element to erase indexes,", K(ret));
    } else if (OB_FAIL(erase_indexes.push_back(3))) {
      COMMON_LOG(WARN, "failed to push one element to erase indexes,", K(ret));
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    COMMON_LOG(INFO, "erasure indexes", K(to_cstring(erase_indexes)));

    for (int64_t i = 0, n = 0; OB_SUCC(ret) && i < all_count && n < data_count; i++) {
      if (!erasure_contains(erase_indexes, i)) {
        input_recovery_blocks[n] = input_blocks[i];
        n++;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObErasureCodeIsa::decode(
                   4, 2, BUFFER_TEST_LEN, erase_indexes, input_recovery_blocks, output_recover_blocks))) {
      COMMON_LOG(WARN, "failed to decode ec,", K(erase_indexes), K(ret));
    }
    int64_t nerrs = erase_indexes.count();
    for (int i = 0; i < nerrs; i++) {
      if (0 != memcmp(output_recover_blocks[i], input_blocks[erase_indexes[i]], BUFFER_TEST_LEN)) {
        compare_ret = true;
      }
    }
    ASSERT_FALSE(compare_ret);
  }

  ASSERT_EQ(ret, OB_SUCCESS);
}

TEST_F(ObECTableCacheTest, TestCacheExist)
{
  INIT_SUCC(ret);
  ObECTableCache ec_cache;
  unsigned char* encoding_table = nullptr;
  unsigned char* encoding_matrix = nullptr;
  ObArenaAllocator allocator;
  bool table_exist_flag = false;
  bool matrix_exist_flag = false;
  int data_count = 4;
  int parity_count = 2;

  if (OB_FAIL(ec_cache.init(1024 /* bucket num */))) {
    COMMON_LOG(WARN, "failed to init ec cache", K(ret));
  }
  ASSERT_EQ(ret, OB_SUCCESS);

  // test not exist
  if (OB_SUCC(ret)) {
    ret = ec_cache.get_encoding_table(data_count, parity_count, table_exist_flag, encoding_table);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_FALSE(table_exist_flag);

    ret = ec_cache.get_encoding_matrix(data_count, parity_count, matrix_exist_flag, encoding_matrix);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_FALSE(matrix_exist_flag);
  }

  // test set encoding matrix table into cache
  if (OB_SUCC(ret)) {
    int64_t all_count = data_count + parity_count;
    int64_t matrix_size = data_count * all_count;
    if (nullptr == (encoding_matrix = (unsigned char*)allocator.alloc(matrix_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to alloc matrix memory", K(matrix_size), K(ret));
    } else if (nullptr == (encoding_table = (unsigned char*)allocator.alloc(matrix_size * 32))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to alloc table memory", K(ret));
    }
    ASSERT_EQ(ret, OB_SUCCESS);

    gf_gen_cauchy1_matrix(encoding_matrix, data_count + parity_count, data_count);
    ret = ec_cache.set_encoding_matrix(data_count, parity_count, encoding_matrix);
    ASSERT_EQ(ret, OB_SUCCESS);

    ec_init_tables(data_count, parity_count, &encoding_matrix[data_count * data_count], encoding_table);
    ret = ec_cache.set_encoding_table(data_count, parity_count, encoding_table);
    ASSERT_EQ(ret, OB_SUCCESS);
  }

  // test encoding matrix table is added to cache
  if (OB_SUCC(ret)) {
    ret = ec_cache.get_encoding_table(data_count, parity_count, table_exist_flag, encoding_table);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_TRUE(table_exist_flag);

    ret = ec_cache.get_encoding_matrix(data_count, parity_count, matrix_exist_flag, encoding_matrix);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_TRUE(matrix_exist_flag);
  }
}

}  // namespace common
}  // namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_ec_table_cache.log*");
  OB_LOGGER.set_file_name("test_ec_table_cache.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
