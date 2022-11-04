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
#include <vector>
#include "storage/blocksstable/encoding/ob_bit_stream.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;

uint64_t get_bit_mask(int64_t bit)
{
  static uint64_t g_mask[64];
  if (0 == bit) {
    return 0;
  } else {
    if (0 == g_mask[bit]) {
      for (int64_t i = 0; i < bit; ++i) {
        g_mask[bit] |= 1L << i;
      }
    }
    return g_mask[bit];
  }
}

TEST(ObBitStream, set_get)
{
  std::vector<int64_t> data;
  std::vector<int64_t> bit_cnt;

  int64_t N = 100;
  srand48(ObTimeUtility::current_time());
  data.reserve(N);
  bit_cnt.reserve(N);
  for (int64_t i = 0; i < N; i++) {
    data.push_back((lrand48() << 32) | lrand48());
  }
  data[0] = 0;
  srandom((int)ObTimeUtility::current_time());
  for (int64_t i = 0; i < N; i++) {
    bit_cnt.push_back(random() % 63 + 1);
  }

  int64_t offset = 0;
  FOREACH(c, bit_cnt) {
    offset += *c;
  }

  ObBitStream bs;
  int64_t length = offset / 8 + (offset % 8 == 0 ? 0 : 1);
  unsigned char *buf = new unsigned char[length + 8];
  memset(buf, 0, length);

  ASSERT_EQ(OB_SUCCESS, bs.init(buf, length));
  offset = 0;
  for (int64_t i = 0; i < N; i++) {
    if (i % 2 == 0) {
      ASSERT_EQ(OB_SUCCESS, bs.set(offset, bit_cnt.at(i), data.at(i)))
          << "offset: " << offset
          << ", bit: " << bit_cnt.at(i)
          << std::endl;
    }
    offset += bit_cnt.at(i);
  }
  offset = 0;
  for (int64_t i = 0; i < N; i++) {
    if (i % 2 != 0) {
      ASSERT_EQ(OB_SUCCESS, bs.set(offset, bit_cnt.at(i), data.at(i)));
    }
    offset += bit_cnt.at(i);
  }
  offset = 0;
  for (int64_t i = 0; i < N; i++) {
    int64_t stored = 0;
    int64_t bit = bit_cnt.at(i);
    ASSERT_EQ(OB_SUCCESS, bs.get(offset, bit_cnt.at(i), stored));
    ASSERT_EQ((uint64_t)stored & get_bit_mask(bit), (uint64_t)data.at(i) & get_bit_mask(bit))
        << "offset: " << offset
        << ", i: " << i
        << ", stored: " << stored
        << ", data: " << data.at(i)
        << ", bit: " << bit << std::endl;
    offset += bit_cnt.at(i);
  }

  // test memory_safe_set
  memset(buf, 0, length);
  offset = 0;
  for (int64_t i = 0; i < N; ++i) {
    int64_t bit = bit_cnt.at(i);
    uint64_t v = data.at(i) & ObBitStream::get_mask(bit);
    // printf("v %lu, bit %ld, mask %lu \n", v, bit, ObBitStream::get_mask(bit));
    ObBitStream::memory_safe_set(buf + offset / 8,
        offset % 8,
        bit + offset > 64,
        v);
    offset += bit;
  }

  offset = 0;
  for (int64_t i = 0; i < N; i++) {
    int64_t stored = 0;
    int64_t bit = bit_cnt.at(i);
    ASSERT_EQ(OB_SUCCESS, bs.get(offset, bit_cnt.at(i), stored));
    ASSERT_EQ((uint64_t)stored & get_bit_mask(bit), (uint64_t)data.at(i) & get_bit_mask(bit))
        << "offset: " << offset
        << ", i: " << i
        << ", stored: " << stored
        << ", data: " << data.at(i)
        << ", bit: " << bit << std::endl;
    offset += bit_cnt.at(i);
  }

  // for negative integer
  memset(buf, 0, length);
  FOREACH(d, data) {
    *d = 0 - *d;
  }
  offset = 0;
  for (int64_t i = 0; i < N; i++) {
    ASSERT_EQ(OB_SUCCESS, bs.set(offset, bit_cnt.at(i), data.at(i)));
    offset += bit_cnt.at(i);
  }
  offset = 0;
  for (int64_t i = 0; i < N; i++) {
    int64_t stored = 0;
    int64_t bit = bit_cnt.at(i);
    ASSERT_EQ(OB_SUCCESS, bs.get(offset, bit_cnt.at(i), stored));
    ASSERT_EQ((uint64_t)stored & get_bit_mask(bit), (uint64_t)data.at(i) & get_bit_mask(bit))
        << "offset: " << offset
        << ", i: " << i
        << ", stored: " << stored
        << ", data: " << data.at(i)
        << ", bit: " << bit << std::endl;
    offset += bit_cnt.at(i);
  }
}

TEST(ObBitStream, get_26)
{
  int16_t data[16];
  unsigned char buf[32];
  MEMSET(buf, 0, 64);
  for (int16_t i = 0; i < 16; ++i) {
    data[i] = i;
  }
  const int64_t cnt = 13;
  int64_t offset = 0;
  for (int16_t i = 0; i < 16; ++i) {
    ObBitStream::memory_safe_set(buf, offset, cnt, static_cast<uint64_t>(data[i]));
    offset += cnt;
  }

  int64_t get_16_value = 0;
  offset = 0;
  for (int16_t i = 0; i < 16; ++i) {
    get_16_value = 0;
    ObBitStream::get<ObBitStream::PACKED_LEN_LESS_THAN_26>(
        buf, offset, cnt, 32 * sizeof(unsigned char), get_16_value);
    ASSERT_EQ(data[i], get_16_value);
    offset += cnt;
  }
}

TEST(ObBitStream, perf)
{
  const int16_t NUM_CNT = 25;
  unsigned char buf[NUM_CNT * 4];
  MEMSET(buf, 0, NUM_CNT * 4);

  const int64_t cnt = 18;
  int64_t offset = 0;
  for (int16_t i = 0; i < NUM_CNT; ++i) {
    ObBitStream::memory_safe_set(buf, offset, cnt, static_cast<uint64_t>(i));
    offset += cnt;
  }

  int64_t get_value = 0;
  int64_t get_10_value = 0;
  offset = 0;
  for (int16_t i = 0; i < NUM_CNT; ++i) {
    get_value = 0;
    get_10_value = 0;
    ObBitStream::get(buf, offset, cnt, get_value);
    ObBitStream::get<ObBitStream::PACKED_LEN_LESS_THAN_26>(
        buf, offset, cnt, NUM_CNT * 8 * 4, get_10_value);
    ASSERT_EQ(get_value, get_10_value);
    offset += cnt;
  }

  int64_t start_time = common::ObTimeUtility::current_time();
  for (int64_t i = 0; i < 10000; ++i) {
    offset = 0;
    for (int64_t j = 0; j < NUM_CNT; ++j) {
      ObBitStream::get(buf, offset, cnt, get_value);
      offset += cnt;
    }
  }
  int64_t end_time = common::ObTimeUtility::current_time();
  std::cout << "first run: " << end_time - start_time << std::endl;

  start_time = common::ObTimeUtility::current_time();
  for (int64_t i = 0; i < 10000; ++i) {
    offset = 0;
    int64_t bs_len = NUM_CNT * 8 * 4;
    for (int64_t j = 0; j < NUM_CNT; ++j) {
      ObBitStream::get<ObBitStream::PACKED_LEN_LESS_THAN_26>(
        buf, offset, cnt, bs_len, get_10_value);
      offset += cnt;
    }
  }
  end_time = common::ObTimeUtility::current_time();
  std::cout << "second run: " << end_time - start_time << std::endl;
}

} // end namespace blocksstable
} // end namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
