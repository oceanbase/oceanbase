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

#include <vector>
#include <memory>
#include <limits>
#include <random>
#include <cmath>
#include <string>

#include "gtest/gtest.h"

#include "lib/codec/ob_composite_codec.h"
#include "lib/codec/ob_simd_fixed_pfor.h"
#include "lib/codec/ob_double_delta_zigzag_rle.h"
#include "lib/codec/ob_delta_zigzag_rle.h"
#include "lib/codec/ob_delta_zigzag_pfor.h"
#include "lib/codec/ob_double_delta_zigzag_pfor.h"
#include "lib/codec/ob_universal_compression.h"
#include "lib/codec/ob_xor_fixed_pfor.h"
#include "lib/codec/ob_tiered_codec.h"
#include "lib/codec/ob_generated_scalar_bp_func.h"
#include "lib/codec/ob_generated_unalign_simd_bp_func.h"

namespace oceanbase
{
namespace common
{

using ::testing::TestWithParam;
using ::testing::Values;

class ObBitPackingPerformanceTest : public ::testing::Test {
public:
  virtual void SetUp() {}

protected:
  std::unique_ptr<ObCodec> codec;
  std::vector<uint8_t> in8;
  std::vector<uint16_t> in16;
  std::vector<uint32_t> in32;
  std::vector<uint64_t> in64;
  std::vector<uint8_t> out8;
  std::vector<uint16_t> out16;
  std::vector<uint32_t> out32;
  std::vector<uint64_t> out64;

  void reset()
  {
    in8.clear();
    in16.clear();
    in32.clear();
    in64.clear();
    out8.clear();
    out16.clear();
    out32.clear();
    out64.clear();
  }

  template<class UIntT>
  void _genDataRandom(std::vector<UIntT>& v, uint32_t values, uint32_t repeat_cnt, bool is_inc) {
    v.clear();
    std::mt19937_64 e2(123456);
    std::uniform_int_distribution<UIntT> dist(
                            std::numeric_limits<UIntT>::min(),
                            std::numeric_limits<UIntT>::max());

    uint64_t x = values/repeat_cnt;
    uint64_t remain = values%repeat_cnt;

    if (is_inc) {
      UIntT tmp_v = dist(e2);
      for (uint64_t i = 0; i < values; i++) {
        v.push_back(tmp_v + i);
      }
    } else {
      for (int i = 0; i < x; ++i) {
        UIntT tmp_v = dist(e2);
        for (uint64_t j = 0; j < repeat_cnt; j++) {
          v.push_back(tmp_v);
        }
      }
      UIntT tmp_v2 = dist(e2);
      for (int i = 0; i < remain; ++i) {
        v.push_back(tmp_v2);
      }
    }
  }

  template<class UIntT>
  void _genDataWithFixBits(std::vector<UIntT>& v,
                           uint32_t bits,
                           uint32_t values) {
    v.clear();
    std::mt19937_64 e2(123456);
    std::uniform_int_distribution<UIntT> dist(
                            0,
                            (bits == (sizeof(UIntT) * 8)) ? (UIntT)(~0ULL) : (UIntT)((1ULL << bits) - 1));
    for (size_t i = 0; i < values; ++i) {
      v.push_back(static_cast<UIntT>(dist(e2) | (UIntT)(1ULL << (bits - 1))));
    }
  }
};

template<class UIntT>
void do_bp_encode_t(int64_t l_count, const UIntT *in, UIntT *out)
{
  const UIntT *orig_in = in;
  UIntT *orig_out = out;
  for (uint32_t m = 0; m <= sizeof(UIntT) * CHAR_BIT; m++) {
    int64_t start_us = ObTimeUtility::current_time();
    for (int64_t n = 0; n < 1000; n++) {
      in = orig_in;
      out = orig_out;
      for (int64_t i = 0; i < l_count; i+=32) {
        fastpack(in, (uint32_t *)out, m);
        in += 32;
        out += (32 * m)/CHAR_BIT/sizeof(UIntT);
      }
    }

    int64_t start2_us = ObTimeUtility::current_time();
    for (int64_t n = 0; n < 1000; n++) {
      in = orig_in;
      out = orig_out;
      for (int64_t i = 0; i < l_count; i+=32) {
        scalar_fastpackwithoutmask(in, (uint32_t *)out, m);
        in += 32;
        out += (32 * m)/CHAR_BIT/sizeof(UIntT);
      }
    }

    int64_t start3_us = ObTimeUtility::current_time();
    for (int64_t n = 0; n < 1000; n++) {
      in = orig_in;
      out = orig_out;
      for (int64_t i = 0; i < l_count; i+=128) {
        if (sizeof(UIntT) == 2) {
          uSIMD_fastpackwithoutmask_128_16((uint16_t *)in, (__m128i *)out, m);
          in += 128;
          out += ((128 * m) / CHAR_BIT / sizeof(UIntT));
        } else if (sizeof(UIntT) == 4) {
          uSIMD_fastpackwithoutmask_128_32((uint32_t *)in, (__m128i *)out, m);
          in += 128;
          out += ((128 * m) / CHAR_BIT / sizeof(UIntT));
        } else {
          // not support

        }
      }
    }
    int64_t end_us = ObTimeUtility::current_time();
    int64_t base_cost_us = start2_us - start_us;
    int64_t raw_cost_us = start3_us - start2_us;
    int64_t simd_cost_us = end_us - start3_us;
    double raw_ratio = (base_cost_us  - raw_cost_us) / (base_cost_us  * 1.0);
    double simd_ratio = (base_cost_us  - simd_cost_us) / (base_cost_us  * 1.0);
    printf("encode uint%ld_recursion----%d bit, cost_us:%ld, raw:%.2f, simd:%.2f\n", sizeof(UIntT) * CHAR_BIT, m, base_cost_us, raw_ratio, simd_ratio);
  }
}


template <class UIntT>
void do_bp_decode_t(int64_t l_count, const UIntT *in, UIntT *out)
{
  const UIntT *orig_in = in;
  UIntT *orig_out = out;
  for (uint32_t m = 0; m <= sizeof(UIntT) * CHAR_BIT; m++) {
    int64_t start_us = ObTimeUtility::current_time();
    for (int64_t n = 0; n < 1000; n++) {
      in = orig_in;
      out = orig_out;
      for (int64_t i = 0; i < l_count; i+=32) {
        fastunpack((const uint32_t *)in, out, m);
        in += (m * 32)/CHAR_BIT/sizeof(UIntT);
        out += 32;
      }
    }

    int64_t start2_us = ObTimeUtility::current_time();
    for (int64_t n = 0; n < 1000; n++) {
      in = orig_in;
      out = orig_out;
      for (int64_t i = 0; i < l_count; i+=32) {
        scalar_fastunpack((const uint32_t *)in, out, m);
        in += (m * 32)/CHAR_BIT/sizeof(UIntT);
        out += 32;
      }
    }

    int64_t start3_us = ObTimeUtility::current_time();
    for (int64_t n = 0; n < 1000; n++) {
      in = orig_in;
      out = orig_out;
      for (int64_t i = 0; i < l_count; i+=128) {
        if (sizeof(UIntT) == 2) {
          uSIMD_fastunpack_128_16((__m128i *)in, (uint16_t *)out, m);
          in += (128 * m)/sizeof(UIntT)/CHAR_BIT;
          out += 128;
        } else if (sizeof(UIntT) == 4) {
          uSIMD_fastunpack_128_32((__m128i *)in, (uint32_t *)out, m);
          in += (128 * m)/sizeof(UIntT)/CHAR_BIT;
          out += 128;
        } else {
          // not support
        }
      }
    }
    int64_t end_us = ObTimeUtility::current_time();
    int64_t base_cost_us = start2_us - start_us;
    int64_t raw_cost_us = start3_us - start2_us;
    int64_t simd_cost_us = end_us - start3_us;
    double raw_ratio = (base_cost_us  - raw_cost_us) / (base_cost_us  * 1.0);
    double simd_ratio = (base_cost_us  - simd_cost_us) / (base_cost_us  * 1.0);
    printf("decode uint%ld_recursion----%d bit, cost_us:%ld, raw:%.2f, simd:%.2f\n", sizeof(UIntT) * CHAR_BIT, m, base_cost_us, raw_ratio, simd_ratio);
  }
}


TEST_F(ObBitPackingPerformanceTest, test) {
  int64_t l_count = 128 * 1024;
  int64_t buf_len = l_count * sizeof(uint32_t);
  char *out = new char[buf_len];
  memset(out, 0, buf_len);

  _genDataRandom<uint32_t>(in32, l_count, 10, false);
  do_bp_encode_t(l_count, (const uint32_t *)in32.data(), (uint32_t *)out);
  do_bp_decode_t(l_count, (const uint32_t *)in32.data(), (uint32_t *)out);

  _genDataRandom<uint16_t>(in16, l_count, 10, false);
  do_bp_encode_t(l_count, (const uint16_t *)in32.data(), (uint16_t *)out);
  do_bp_decode_t(l_count, (const uint16_t *)in32.data(), (uint16_t *)out);

  _genDataRandom<uint8_t>(in8, l_count, 10, false);
  do_bp_encode_t(l_count, (const uint8_t *)in32.data(), (uint8_t *)out);
  do_bp_decode_t(l_count, (const uint8_t *)in32.data(), (uint8_t *)out);
}

} // namespace common
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  system("rm -f test_bitpacking_performance.log*");
  OB_LOGGER.set_file_name("test_bitpacking_performance.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");

  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
