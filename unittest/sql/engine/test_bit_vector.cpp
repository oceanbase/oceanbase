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

#include "lib/random/ob_random.h"
#include "src/sql/engine/ob_bit_vector.h"
#include "src/sql/ob_eval_bound.h"

#define private public
#define WordType uint64_t

using namespace std;
namespace oceanbase
{
namespace sql
{
class ObTestBitVector : public ::testing::Test
{
public:
  ObTestBitVector()
  {}
  ~ObTestBitVector()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObTestBitVector);
};

void expect_range(ObBitVector *dest_bit_vector, int64_t start, int64_t middle, int64_t end)
{
  for (int64_t i = 0; i < start; i++) {
    EXPECT_EQ(0, dest_bit_vector->at(i));
  }
  for (int64_t i = start; i < middle; i++) {
    EXPECT_EQ(1, dest_bit_vector->at(i));
    dest_bit_vector->unset(i);
  }
  EXPECT_EQ(0, dest_bit_vector->at(middle));
  for (int64_t i = middle + 1; i < end; i++) {
    EXPECT_EQ(1, dest_bit_vector->at(i));
    dest_bit_vector->unset(i);
  }
  for (int64_t i = end; i < end + 100; i++) {
    EXPECT_EQ(0, dest_bit_vector->at(i));
  }
}

void test_range(ObBitVector *dest_bit_vector, ObBitVector *src_bit_vector, int64_t start,
                int64_t end)
{
  for (int i = 0; i < 2000; i++) {
    src_bit_vector->set(i);
  }

  int64_t middle = (start + end) / 2;
  dest_bit_vector->set_all(start, end);
  dest_bit_vector->unset(middle);
  expect_range(dest_bit_vector, start, middle, end);

  dest_bit_vector->set_all(start, end);
  dest_bit_vector->unset_all(start, end);
  for (int64_t i = 0; i < end + 100; i++) {
    EXPECT_EQ(0, dest_bit_vector->at(i));
  }

  src_bit_vector->unset(middle);
  dest_bit_vector->deep_copy(*src_bit_vector, start, end);
  expect_range(dest_bit_vector, start, middle, end);

  dest_bit_vector->bit_or(*src_bit_vector, start, end);
  expect_range(dest_bit_vector, start, middle, end);
  src_bit_vector->set(middle);

  for (int64_t i = start; i < end; i++) {
    dest_bit_vector->set(i);
  }
  EXPECT_EQ(1, dest_bit_vector->is_all_true(start, end));
  if (start > 0) {
    EXPECT_EQ(0, dest_bit_vector->is_all_true(start - 1, end));
  }
  EXPECT_EQ(0, dest_bit_vector->is_all_true(start, end + 1));
  for (int64_t i = start; i < end; i++) {
    dest_bit_vector->unset(i);
  }
}

TEST(ObTestBitVector, bit_or_range)
{
  char src_buf[1024];
  char dest_buf[1024];
  MEMSET(src_buf, 0, 1024);
  MEMSET(dest_buf, 0, 1024);
  ObBitVector *src_bit_vector = new (src_buf) ObBitVector;
  ObBitVector *dest_bit_vector = new (dest_buf) ObBitVector;

  test_range(dest_bit_vector, src_bit_vector, 13, 40);
  test_range(dest_bit_vector, src_bit_vector, 13, 63);
  test_range(dest_bit_vector, src_bit_vector, 13, 64);
  test_range(dest_bit_vector, src_bit_vector, 13, 127);
  test_range(dest_bit_vector, src_bit_vector, 13, 128);
  test_range(dest_bit_vector, src_bit_vector, 13, 258);
  test_range(dest_bit_vector, src_bit_vector, 0, 50);
  test_range(dest_bit_vector, src_bit_vector, 0, 100);
  test_range(dest_bit_vector, src_bit_vector, 0, 63);
  test_range(dest_bit_vector, src_bit_vector, 0, 64);
  test_range(dest_bit_vector, src_bit_vector, 0, 0);
  test_range(dest_bit_vector, src_bit_vector, 64, 64);
  test_range(dest_bit_vector, src_bit_vector, 64, 127);

}

// copy from the previos version ObBitVectorImpl, for check result
template <bool IS_FLIP, typename OP>
void copied_inner_foreach(const ObBitVectorImpl<WordType> &skip, int64_t size, OP op)
{
  int ret = OB_SUCCESS;
  int64_t tmp_step = 0;
  typedef uint16_t StepType;
  const int64_t step_size = sizeof(StepType) * CHAR_BIT;
  int64_t word_cnt = ObBitVectorImpl<WordType>::word_count(size);
  int64_t step = 0;
  const int64_t remain = size % ObBitVectorImpl<WordType>::WORD_BITS;
  for (int64_t i = 0; i < word_cnt && OB_SUCC(ret); ++i) {
    WordType s_word = (IS_FLIP ? ~skip.data_[i] : skip.data_[i]);
    // bool all_bits = (false ? skip.data_[i] == 0 : (~skip.data_[i]) == 0);
    if (i >= word_cnt - 1 && remain > 0) {
      // all_bits = ((false ? skip.data_[i] : ~skip.data_[i]) & ((1LU << remain) - 1)) == 0;
      s_word = s_word & ((1LU << remain) - 1);
    }
    if (s_word > 0) {
      WordType tmp_s_word = s_word;
      tmp_step = step;
      do {
        uint16_t step_val = tmp_s_word & 0xFFFF;
        if (0xFFFF == step_val) {
          // no skip
          // last batch ?
          int64_t mini_cnt = step_size;
          if (tmp_step + step_size > size) {
            mini_cnt = size - tmp_step;
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < mini_cnt; j++) {
            int64_t k = j + tmp_step;
            ret = op(k);
          }
        } else if (step_val > 0) {
          do {
            int64_t start_bit_idx = __builtin_ctz(step_val);
            int64_t k = start_bit_idx + tmp_step;
            ret = op(k);
            step_val &= (step_val - 1);
          } while (step_val > 0 && OB_SUCC(ret)); // end for, for one step size
        }
        tmp_step += step_size;
        tmp_s_word >>= step_size;
      } while (tmp_s_word > 0 && OB_SUCC(ret)); // one word-uint64_t
    }
    step += ObBitVectorImpl<WordType>::WORD_BITS;
  } // end for
}

// 这部分代码不要删除，用于调试新接口，因为ob的单测编译要编译一大堆无效文件，而ob_bit_vector.h这个头文件又被很多地方引用，
// 导致编译速度巨慢，尽量不要直接在ob_bit_vector.h改代码调试，而是在这里先把接口改正确了，然后再放到ob_bit_vector.h里面
// 进行调试
template <bool IS_FLIP, typename OP>
void my_foreach_bound(const ObBitVectorImpl<WordType> &skip, int64_t start_idx, int64_t end_idx, OP op)
{
  int ret = OB_SUCCESS;
  int64_t tmp_step = 0;
  typedef uint16_t StepType;
  const int64_t step_size = sizeof(StepType) * CHAR_BIT;

  int64_t start_cnt = start_idx / ObBitVectorImpl<WordType>::WORD_BITS; // start_idx is included
  const int64_t begin_remain = start_idx % ObBitVectorImpl<WordType>::WORD_BITS;
  const int64_t begin_mask = (-1LU << begin_remain);

  int64_t end_cnt = ObBitVectorImpl<WordType>::word_count(end_idx);     // end_idx is not included
  const int64_t end_remain = end_idx % ObBitVectorImpl<WordType>::WORD_BITS;
  const int64_t end_mask = (1LU << end_remain) - 1;

  int64_t step = ObBitVectorImpl<WordType>::WORD_BITS * start_cnt;
  for (int64_t i = start_cnt; i < end_cnt && OB_SUCC(ret); ++i) {
    WordType s_word = (IS_FLIP ? ~skip.data_[i] : skip.data_[i]);
    if (start_cnt == end_cnt - 1) {
      // if only one word, both begin_mask and end_mask should be used
      if (begin_remain > 0) {
        s_word = s_word & begin_mask;
      }
      if (end_remain > 0) {
        s_word = s_word & end_mask;
      }
    } else if (i == start_cnt && begin_remain > 0) {
      // add begin_mask for first word, remove the bit less than start_idx
      s_word = s_word & begin_mask;
    } else if (i == end_cnt - 1 && end_remain > 0) {
      // add end_mask for last word, remove the bit greater equal than end_idx
      s_word = s_word & end_mask;
    }
    if (s_word > 0) {
      WordType tmp_s_word = s_word;
      tmp_step = step;
      do {
        uint16_t step_val = tmp_s_word & 0xFFFF;
        if (0xFFFF == step_val) {
          for (int64_t j = 0; OB_SUCC(ret) && j < step_size; j++) {
            int64_t k = j + tmp_step;
            ret = op(k);
          }
        } else if (step_val > 0) {
          do {
            int64_t start_bit_idx = __builtin_ctz(step_val);
            int64_t k = start_bit_idx + tmp_step;
            ret = op(k);
            step_val &= (step_val - 1);
          } while (step_val > 0 && OB_SUCC(ret)); // end for, for one step size
        }
        tmp_step += step_size;
        tmp_s_word >>= step_size;
      } while (tmp_s_word > 0 && OB_SUCC(ret)); // one word-uint64_t
    }
    step += ObBitVectorImpl<WordType>::ObBitVectorImpl<WordType>::WORD_BITS;
  } // end for
}

void test_foreach_result_random(int64_t batch_size, int64_t start_idx, int64_t end_idx)
{
  void *buf = malloc(batch_size);
  ObBitVector *bit_vector = to_bit_vector(buf);
  bit_vector->init(batch_size);

  int64_t true_start_idx = common::ObRandom::rand(0, batch_size);
  int64_t true_end_idx = common::ObRandom::rand(0, batch_size);
  if (true_start_idx > true_end_idx) {
    swap(true_start_idx, true_end_idx);
  }

  bit_vector->set_all(true_start_idx, true_end_idx);
  EvalBound bound(batch_size, start_idx, end_idx, false);

  // cout << "start_idx: " << start_idx << "\nend_idx: " << end_idx
  //      << "\ntrue_start_idx: " << true_start_idx << "\ntrue_end_idx: " << true_end_idx << endl;

  // test foreach
  std::vector<int> result_foreach_ori(batch_size, 0);
  std::vector<int> result_foreach_batch(batch_size, 0);
  std::vector<int> result_foreach_bound(batch_size, 0);
  copied_inner_foreach<false>(*bit_vector, end_idx, [&](int64_t idx) __attribute__((always_inline)) {
    result_foreach_ori[idx] = 1;
    return OB_SUCCESS;
  });
  ObBitVector::foreach (*bit_vector, end_idx, [&](int64_t idx) __attribute__((always_inline)) {
    result_foreach_batch[idx] = 1;
    return OB_SUCCESS;
  });
  ObBitVector::foreach (*bit_vector, bound, [&](int64_t idx) __attribute__((always_inline)) {
    result_foreach_bound[idx] = 1;
    return OB_SUCCESS;
  });

  // test flip_foreach
  std::vector<int> result_flip_foreach_ori(batch_size, 0);
  std::vector<int> result_flip_foreach_batch(batch_size, 0);
  std::vector<int> result_flip_foreach_bound(batch_size, 0);
  copied_inner_foreach<true>(*bit_vector, end_idx, [&](int64_t idx) __attribute__((always_inline)) {
    result_flip_foreach_ori[idx] = 1;
    return OB_SUCCESS;
  });
  ObBitVector::flip_foreach(*bit_vector, end_idx, [&](int64_t idx) __attribute__((always_inline)) {
    result_flip_foreach_batch[idx] = 1;
    return OB_SUCCESS;
  });

  ObBitVector::flip_foreach(*bit_vector, bound, [&](int64_t idx) __attribute__((always_inline)) {
    result_flip_foreach_bound[idx] = 1;
    return OB_SUCCESS;
  });

  // result结果，0表示未处理，1表示处理
  for (int64_t i = 0; i < batch_size; ++i) {
    // 固定check新的batch接口是否和老的batch接口结果是否相同
    EXPECT_EQ(result_foreach_ori[i], result_foreach_batch[i]);
    EXPECT_EQ(result_flip_foreach_ori[i], result_flip_foreach_batch[i]);

    // 1. 对于 i < start_idx 部分, bound接口不会处理，只有batch接口和copied接口会处理
    // 2. 对于 start_idx <= i < end_idx 部分, 所有接口都会处理
    // 3. 对于 i >= end_idx 部分, 所有接口都不会处理
    if (i < start_idx) {
      if (i < true_start_idx) {
        // 此部分 bit vector 为 0，因此 foreach 结果为 0， flip foreach 结果为 1
        EXPECT_EQ(0, result_foreach_batch[i]);
        EXPECT_EQ(1, result_flip_foreach_batch[i]);
      } else if (i >= true_start_idx && i < true_end_idx) {
        // 此部分 bit vector 为 1，因此 foreach 结果为 1， flip foreach 结果为 0
        EXPECT_EQ(1, result_foreach_batch[i]);
        EXPECT_EQ(0, result_flip_foreach_batch[i]);
      } else if (i >= true_end_idx) {
        // 此部分 bit vector 为 0，因此 foreach 结果为 0， flip foreach 结果为 1
        EXPECT_EQ(0, result_foreach_batch[i]);
        EXPECT_EQ(1, result_flip_foreach_batch[i]);
      }
      // bound接口不会处理这部分数据，因此全部结果为 0
      EXPECT_EQ(0, result_foreach_bound[i]);
      EXPECT_EQ(0, result_flip_foreach_bound[i]);
    } else if (i >= start_idx && i < end_idx) {
      if (i < true_start_idx) {
        // 此部分 bit vector 为 0，因此 foreach 结果为 0， flip foreach 结果为 1
        EXPECT_EQ(0, result_foreach_batch[i]);
        EXPECT_EQ(1, result_flip_foreach_batch[i]);
        EXPECT_EQ(0, result_foreach_bound[i]);
        EXPECT_EQ(1, result_flip_foreach_bound[i]);
      } else if (i >= true_start_idx && i < true_end_idx) {
        // 此部分 bit vector 为 1，因此 foreach 结果为 1， flip foreach 结果为 0
        EXPECT_EQ(1, result_foreach_batch[i]);
        EXPECT_EQ(0, result_flip_foreach_batch[i]);
        EXPECT_EQ(1, result_foreach_bound[i]);
        EXPECT_EQ(0, result_flip_foreach_bound[i]);
      } else if (i >= true_end_idx) {
        // 此部分 bit vector 为 0，因此 foreach 结果为 0， flip foreach 结果为 1
        EXPECT_EQ(0, result_foreach_batch[i]);
        EXPECT_EQ(1, result_flip_foreach_batch[i]);
        EXPECT_EQ(0, result_foreach_bound[i]);
        EXPECT_EQ(1, result_flip_foreach_bound[i]);
      }
    } else if (i >= end_idx) {
      // 所有接口不会处理这部分数据，因此全部结果为 0
      EXPECT_EQ(0, result_foreach_batch[i]);
      EXPECT_EQ(0, result_flip_foreach_batch[i]);
      EXPECT_EQ(0, result_foreach_bound[i]);
      EXPECT_EQ(0, result_flip_foreach_bound[i]);
    }
  }
}

TEST(ObTestBitVector, test_foreach)
{
  int64_t batch_size = common::ObRandom::rand(0, 1024);
  int64_t round = 100;
  for (int64_t i = 0; i < round; ++i) {
    int64_t start_idx = common::ObRandom::rand(0, batch_size);
    int64_t end_idx = common::ObRandom::rand(0, batch_size);
    if (start_idx > end_idx) {
      swap(start_idx, end_idx);
    }
    test_foreach_result_random(batch_size, start_idx, end_idx);
  }
}

}
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}
