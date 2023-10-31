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

class ObBitPackingTest : public ::testing::TestWithParam<std::string> {
public:
  virtual void SetUp() {
    std::string name = GetParam();
    if (name == "ObSimpleBitPacking") {
      codec.reset(new ObSimpleBitPacking());
    } else if (name == "ObSIMDFixedPFor") {
      codec.reset(new ObSIMDFixedPFor());
    } else if (name == "ObJustCopy") {
      codec.reset(new ObJustCopy());
    } else if (name == "ObDeltaZigzagRle") {
      codec.reset(new ObDeltaZigzagRle());
    } else if (name == "ObDeltaZigzagPFor") {
      codec.reset(new ObDeltaZigzagPFor());
    } else if (name == "ObDoubleDeltaZigzagRle") {
      codec.reset(new ObDoubleDeltaZigzagRle());
    } else if (name == "ObDoubleDeltaZigzagPFor") {
      codec.reset(new ObDoubleDeltaZigzagPFor());
    } else if (name == "ObTiredCodec-bp-uni") {
      auto tired_codec = new ObTiredCodec<ObSimpleBitPacking, ObUniversalCompression>();
      codec.reset(tired_codec);
      min_block_size_ = ObTiredCodec<ObSimpleBitPacking, ObUniversalCompression>::BlockSize;
      codec->set_allocator(alloc_);
      tired_codec->codec2_.set_compressor_type(ZSTD_1_3_8_COMPRESSOR);
    } else if (name == "ObXorFixedPfor") {
      codec.reset(new ObXorFixedPfor());
      min_block_size_ = ObXorFixedPfor::BlockSize;
    } else if (name == "ZSTD_1_3_8") {
      ObUniversalCompression *uc = new ObUniversalCompression();
      uc->set_compressor_type(ZSTD_1_3_8_COMPRESSOR);
      codec.reset(uc);
    } else if (name == "LZ4_191") {
      ObUniversalCompression *uc = new ObUniversalCompression();
      uc->set_compressor_type(LZ4_191_COMPRESSOR);
      codec.reset(uc);
    } else if (name == "SNAPPY") {
      ObUniversalCompression *uc = new ObUniversalCompression();
      uc->set_compressor_type(SNAPPY_COMPRESSOR);
      uc->set_allocator(alloc_);
      codec.reset(uc);
    } else if (name == "ZLIB") {
      ObUniversalCompression *uc = new ObUniversalCompression();
      uc->set_allocator(alloc_);
      uc->set_compressor_type(LZ4_COMPRESSOR);
      codec.reset(uc);
    }
    else {
      throw new std::logic_error("Unknown codec " + name);
    }
    min_block_size_ = codec->get_block_size();
  }

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
  bool support_int64_{true};
  bool support_int8_16_{true};
  uint32_t min_block_size_{0};
  ObArenaAllocator alloc_;
  uint32_t repeat_cnt_{50};

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
    alloc_.reuse();
  }

  template<class T>
  void verify(T *in, uint64_t in_cnt)
  {
    if (!divisibleby(in_cnt, min_block_size_)) {
      // if can not divide, just return
      return;
    }
    if ((sizeof(T) == 8) && !support_int64_) {
      return;
    }
    if (((sizeof(T) == 1) || (sizeof(T) == 2)) && !support_int8_16_) {
      return;
    }
    LIB_LOG(INFO, "verify", KP(in), K(in_cnt), K(sizeof(T)));
    codec->set_uint_bytes(sizeof(T));
    uint64_t inital_size = codec->get_max_encoding_size((char *)in, in_cnt * sizeof(T));
    uint64_t encoded_size = inital_size;
    char *encoded = new char[encoded_size];
    memset(encoded, 0xff, inital_size);

    uint64_t out_pos = 0;
    int64_t encode_start_us = ObTimeUtility::current_time();

    int ret = OB_SUCCESS;
    for (int64_t i = 0; i < repeat_cnt_; i++) {
      out_pos = 0;
      ret = codec->encode((char *)in,
                          in_cnt * sizeof(T),
                          encoded,
                          encoded_size,
                          out_pos);
    }
    int64_t encode_cost_us = (ObTimeUtility::current_time() - encode_start_us) / repeat_cnt_;
    double speed = ((in_cnt * sizeof(T)) / (encode_cost_us/(1000LL * 1000.0))) / (1024LL * 1024LL * 1024.0);
    double comp_ratio = (in_cnt * sizeof(T))/(out_pos * 1.0);

    ASSERT_TRUE(out_pos > 0 && out_pos < encoded_size);
    ASSERT_EQ(OB_SUCCESS, ret);

    std::vector<T> t_out;
    t_out.resize(in_cnt);

    uint64_t pos = 0;
    uint64_t out_pos2 = 0;
    int64_t decode_start_us = ObTimeUtility::current_time();
    for (int64_t i = 0; i < repeat_cnt_; i++) {
      pos = 0;
      out_pos2 = 0;
      ret = codec->decode(encoded,
                          out_pos,
                          pos,
                          in_cnt,
                          reinterpret_cast<char *>(t_out.data()),
                          in_cnt * sizeof(T),
                          out_pos2);
    }
    int64_t decode_cost_us = (ObTimeUtility::current_time() - decode_start_us) / repeat_cnt_;
    double decode_speed = (in_cnt * sizeof(T)) / (decode_cost_us/(1000LL * 1000.0)) / (1024LL * 1024 * 1024.0);

    std::string name = GetParam();
    printf("uint%ld_t----%-30s, encode GB/s:%.2f, ratio:%.2f, decode GB/s:%.2f \n", sizeof(T)*8, name.c_str(), speed, comp_ratio, decode_speed);

    ASSERT_EQ(OB_SUCCESS, ret);
    if (pos != out_pos) {
      ::abort();
    }
    ASSERT_EQ(pos, out_pos);
    ASSERT_EQ(out_pos2, in_cnt * sizeof(T));

    bool passed = true;
    for (size_t i = 0; i < in_cnt; ++i) {
      if (in[i] != t_out[i]) {
        passed = false;
      }
      EXPECT_EQ(in[i], t_out[i]);
    }
    if (!passed) {
      std::cout << "Test failed with input: ";
      for (size_t i = 0; i < in_cnt; ++i) {
        std::cout << in[i] << " ";
      }
      std::cout << std::endl;
      std::cout << "Test failed with output: ";
      for (size_t i = 0; i < in_cnt; ++i) {
        std::cout << t_out[i] << " ";
      }
      std::cout << std::endl;
    }
    delete []encoded;
    encoded = nullptr;
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

INSTANTIATE_TEST_CASE_P(
    FastPForLib,
    ObBitPackingTest,
    Values("ObSimpleBitPacking",
           "ObSIMDFixedPFor",
           "ObJustCopy",
           "ObDeltaZigzagRle",
           "ObDeltaZigzagPFor",
           "ObDoubleDeltaZigzagRle",
           "ObDoubleDeltaZigzagPFor",
           "ObTiredCodec-bp-uni",
           "ObXorFixedPfor",
           "ZSTD_1_3_8"
           //"LZ4_191",
           //"SNAPPY",
           //"ZLIB"
           ));
} // namespace common
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  system("rm -f test_codec_perfomance.log*");
  OB_LOGGER.set_file_name("test_codec_perfomance.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");

  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
