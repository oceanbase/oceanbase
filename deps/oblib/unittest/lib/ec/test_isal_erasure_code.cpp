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
#include "lib/container/ob_array.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/ec/ob_erasure_code_isa.h"
#include "lib/ec/ob_erasure_code_table_cache.h"

#define DATA_BLOCK_COUNT 4
#define CODE_BLOCK_COUNT 2
#define DATA_BLOCK_SIZE 64

namespace oceanbase {
namespace common {

class ObErasureCodeTest : public testing::Test {
public:
  const static int64_t BUFFER_TEST_LEN = DATA_BLOCK_SIZE;
  const static int64_t SMALL_BUFFER_TEST_LEN = BUFFER_TEST_LEN / 8;

  static void SetUpTestCase()
  {
    INIT_SUCC(ret);
    ret = ObECCacheManager::get_ec_cache_mgr().init(1024 /*bucket num*/);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  static void TearDownTestCase()
  {}
  static int gen_err_list(int64_t all_count, int64_t data_count, ObArray<int64_t>& src_err_list);
  static int gen_parity_err_list(int64_t data_count, int64_t parity_count, ObArray<int64_t>& src_err_list);
  static bool erasure_contains(const ObArray<int64_t>& erasures, int64_t index);
  static void test_normal();
  static void test_normal2();
  static void test_parity_block_fail();
  static void test_append();

  static ObArenaAllocator* allocator;
};

ObArenaAllocator* ObErasureCodeTest::allocator = new ObArenaAllocator(ObModIds::TEST);

int ObErasureCodeTest::gen_err_list(int64_t all_count, int64_t data_count, ObArray<int64_t>& src_err_list)
{
  INIT_SUCC(ret);
  int64_t parity_count = all_count - data_count;

  for (int i = 0; OB_SUCC(ret) && i < all_count && src_err_list.count() < parity_count; i++) {
    if (rand() % 2 == 0) {
      ret = src_err_list.push_back(i);
    }
  }
  if (OB_SUCC(ret)) {
    if (src_err_list.count() == 0) {  // should have at least one error
      ret = src_err_list.push_back(rand() % all_count);
    } else {
    }
  }
  return ret;
}

int ObErasureCodeTest::gen_parity_err_list(int64_t data_count, int64_t parity_count, ObArray<int64_t>& src_err_list)
{
  INIT_SUCC(ret);

  for (int64_t i = data_count; OB_SUCC(ret) && i < data_count + parity_count; i++) {
    if ((rand() % 1000) > 300) {
      ret = src_err_list.push_back(i);
    }
  }
  if (OB_SUCC(ret)) {
    if (src_err_list.count() == 0) {  // should have at least one error
      ret = src_err_list.push_back(data_count);
    }
  }
  return ret;
}

bool ObErasureCodeTest::erasure_contains(const ObArray<int64_t>& erasures, int64_t index)
{
  bool contain_flag = false;

  int64_t count = erasures.count();
  for (int i = 0; i < count; i++) {
    if (erasures[i] == index) {
      contain_flag = true;
      break;
    }
  }

  return contain_flag;
}

void ObErasureCodeTest::test_append()
{
  INIT_SUCC(ret);
  bool compare_ret = false;
  int64_t data_count = DATA_BLOCK_COUNT;
  int64_t parity_count = CODE_BLOCK_COUNT;
  int64_t all_count = CODE_BLOCK_COUNT + DATA_BLOCK_COUNT;
  ObArray<int64_t> erase_indexes;
  unsigned char *input_blocks[all_count], *update_input_blocks[all_count];
  unsigned char* buf = NULL;

  memset(input_blocks, 0, sizeof(input_blocks));
  memset(update_input_blocks, 0, sizeof(update_input_blocks));

  for (int i = 0; OB_SUCC(ret) && i < all_count; i++) {
    buf = (unsigned char*)allocator->alloc(BUFFER_TEST_LEN);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      input_blocks[i] = buf;
    }
  }

  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = 0; OB_SUCC(ret) && i < all_count; i++) {
    buf = (unsigned char*)allocator->alloc(BUFFER_TEST_LEN);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      update_input_blocks[i] = buf;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = 0; i < data_count; i++)
    for (int j = 0; j < BUFFER_TEST_LEN; j++) {
      input_blocks[i][j] = (unsigned char)rand();
      update_input_blocks[i][j] = input_blocks[i][j];
    }

  // test block update
  ret = ObErasureCodeIsa::encode(data_count, parity_count, BUFFER_TEST_LEN, input_blocks, &input_blocks[data_count]);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int i = 0; i < data_count; i++) {
    ret = ObErasureCodeIsa::append_encode(
        data_count, parity_count, BUFFER_TEST_LEN, i, update_input_blocks[i], &update_input_blocks[data_count]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  compare_ret = true;
  for (int i = 0; i < parity_count; i++) {
    if (0 != memcmp(update_input_blocks[i + data_count], input_blocks[i + data_count], BUFFER_TEST_LEN)) {
      compare_ret = false;
    }
  }
  ASSERT_EQ(true, compare_ret);

  // test small block  append update
  unsigned char update_tmp_buffer[SMALL_BUFFER_TEST_LEN];
  unsigned char* update_parity_buffer[parity_count];

  for (int i = 0; i < parity_count; i++) {
    memset(update_input_blocks[i + data_count], 0, BUFFER_TEST_LEN);
  }
  for (int i = 0; i < data_count; i++) {
    int64_t offset = 0;
    while (offset < BUFFER_TEST_LEN) {
      memcpy(update_tmp_buffer, input_blocks[i] + offset, (size_t)SMALL_BUFFER_TEST_LEN);
      for (int j = 0; j < parity_count; j++) {
        update_parity_buffer[j] = update_input_blocks[j + data_count] + offset;
      }

      ret = ObErasureCodeIsa::append_encode(
          data_count, parity_count, SMALL_BUFFER_TEST_LEN, i, update_tmp_buffer, update_parity_buffer);
      ASSERT_EQ(OB_SUCCESS, ret);
      offset += SMALL_BUFFER_TEST_LEN;
    }
  }
  compare_ret = true;
  for (int i = 0; i < parity_count; i++) {
    if (0 != memcmp(update_input_blocks[i + data_count], input_blocks[i + data_count], BUFFER_TEST_LEN)) {
      compare_ret = false;
    }
  }
  ASSERT_EQ(true, compare_ret);
}

void ObErasureCodeTest::test_normal()
{
  INIT_SUCC(ret);
  bool compare_ret = false;
  int64_t data_count = DATA_BLOCK_COUNT;
  int64_t parity_count = CODE_BLOCK_COUNT;
  int64_t all_count = CODE_BLOCK_COUNT + DATA_BLOCK_COUNT;
  ObArray<int64_t> erase_indexes;
  unsigned char* input_blocks[all_count];
  unsigned char* input_recovery_blocks[data_count];
  unsigned char* output_recover_blocks[parity_count];
  unsigned char* buf = NULL;

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
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = 0; OB_SUCC(ret) && i < parity_count; i++) {
    buf = (unsigned char*)allocator->alloc(BUFFER_TEST_LEN);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMSET(buf, 0, BUFFER_TEST_LEN);
      output_recover_blocks[i] = buf;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int i = 0; i < data_count; i++)
    for (int j = 0; j < BUFFER_TEST_LEN; j++)
      input_blocks[i][j] = (unsigned char)j;

  ret = ObErasureCodeIsa::encode(data_count, parity_count, BUFFER_TEST_LEN, input_blocks, &input_blocks[data_count]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = gen_err_list(all_count, data_count, erase_indexes);
  COMMON_LOG(INFO, "erasure indexes", K(to_cstring(erase_indexes)));
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0, n = 0; i < all_count && n < data_count; i++) {
    if (!erasure_contains(erase_indexes, i)) {
      input_recovery_blocks[n] = input_blocks[i];
      n++;
    }
  }

  ret = ObErasureCodeIsa::decode(
      data_count, parity_count, BUFFER_TEST_LEN, erase_indexes, input_recovery_blocks, output_recover_blocks);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t nerrs = erase_indexes.count();
  for (int i = 0; i < nerrs; i++) {
    if (0 != memcmp(output_recover_blocks[i], input_blocks[erase_indexes[i]], BUFFER_TEST_LEN)) {
      compare_ret = true;
    }
  }
  ASSERT_FALSE(compare_ret);
}

void ObErasureCodeTest::test_normal2()
{
  INIT_SUCC(ret);
  bool compare_ret = false;
  int64_t data_count = DATA_BLOCK_COUNT;
  int64_t parity_count = CODE_BLOCK_COUNT;
  int64_t all_count = CODE_BLOCK_COUNT + DATA_BLOCK_COUNT;
  common::ObArray<int64_t> erase_indexes;
  common::ObArray<int64_t> input_recovery_indexes;
  unsigned char* input_blocks[all_count];
  unsigned char* input_recovery_blocks[data_count];
  unsigned char* output_recover_blocks[parity_count];
  unsigned char* buf = NULL;

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
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = 0; OB_SUCC(ret) && i < parity_count; i++) {
    buf = (unsigned char*)allocator->alloc(BUFFER_TEST_LEN);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMSET(buf, 0, BUFFER_TEST_LEN);
      output_recover_blocks[i] = buf;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int i = 0; i < data_count; i++)
    for (int j = 0; j < BUFFER_TEST_LEN; j++)
      input_blocks[i][j] = (unsigned char)j;

  ret = ObErasureCodeIsa::encode(data_count, parity_count, BUFFER_TEST_LEN, input_blocks, &input_blocks[data_count]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = gen_err_list(all_count, data_count, erase_indexes);
  COMMON_LOG(INFO, "erasure indexes", K(to_cstring(erase_indexes)));
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0, n = 0; i < all_count && n < data_count; i++) {
    if (!erasure_contains(erase_indexes, i)) {
      input_recovery_indexes.push_back(i);
      input_recovery_blocks[n] = input_blocks[i];
      n++;
    }
  }

  ret = ObErasureCodeIsa::decode(data_count,
      parity_count,
      BUFFER_TEST_LEN,
      input_recovery_indexes,
      erase_indexes,
      input_recovery_blocks,
      output_recover_blocks);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t nerrs = erase_indexes.count();
  for (int i = 0; i < nerrs; i++) {
    if (0 != memcmp(output_recover_blocks[i], input_blocks[erase_indexes[i]], BUFFER_TEST_LEN)) {
      compare_ret = true;
    }
  }
  ASSERT_FALSE(compare_ret);
}

void ObErasureCodeTest::test_parity_block_fail()
{
  INIT_SUCC(ret);
  bool compare_ret = false;
  int64_t data_count = DATA_BLOCK_COUNT;
  int64_t parity_count = CODE_BLOCK_COUNT;
  int64_t all_count = CODE_BLOCK_COUNT + DATA_BLOCK_COUNT;
  ObArray<int64_t> erase_indexes;
  unsigned char* input_blocks[all_count];
  unsigned char* input_recovery_blocks[data_count];
  unsigned char* output_recover_blocks[parity_count];
  unsigned char* buf = NULL;

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
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = 0; OB_SUCC(ret) && i < parity_count; i++) {
    buf = (unsigned char*)allocator->alloc(BUFFER_TEST_LEN);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMSET(buf, 0, BUFFER_TEST_LEN);
      output_recover_blocks[i] = buf;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int i = 0; i < data_count; i++)
    for (int j = 0; j < BUFFER_TEST_LEN; j++)
      input_blocks[i][j] = (unsigned char)j;

  ret = ObErasureCodeIsa::encode(data_count, parity_count, BUFFER_TEST_LEN, input_blocks, &input_blocks[data_count]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = gen_parity_err_list(data_count, parity_count, erase_indexes);
  COMMON_LOG(WARN, "erasure indexes", K(to_cstring(erase_indexes)));
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0, n = 0; i < all_count && n < data_count; i++) {
    if (!erasure_contains(erase_indexes, i)) {
      input_recovery_blocks[n] = input_blocks[i];
      n++;
    }
  }

  ret = ObErasureCodeIsa::decode(
      data_count, parity_count, BUFFER_TEST_LEN, erase_indexes, input_recovery_blocks, output_recover_blocks);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t nerrs = erase_indexes.count();
  for (int i = 0; i < nerrs; i++) {
    if (0 != memcmp(output_recover_blocks[i], input_blocks[erase_indexes[i]], BUFFER_TEST_LEN)) {
      compare_ret = true;
    }
  }
  ASSERT_FALSE(compare_ret);
}

TEST_F(ObErasureCodeTest, testnormal)
{
  test_normal();
  test_normal2();
}

TEST_F(ObErasureCodeTest, test_parity_block_fail)
{
  test_parity_block_fail();
}

TEST_F(ObErasureCodeTest, testappend)
{
  test_append();
}

}  // namespace common
}  // namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_erasure_code.log*");
  OB_LOGGER.set_file_name("test_erasure_code.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
