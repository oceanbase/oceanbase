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
    LIB_LOG(INFO, "current run", "name", name.c_str());
    if (name == "ObSimpleBitPacking") {
      codec.reset(new ObSimpleBitPacking());
      min_block_size_ = ObSimpleBitPacking::BlockSize;
    } else if (name == "ObSIMDFixedPFor") {
      codec.reset(new ObSIMDFixedPFor());
      min_block_size_ = ObSIMDFixedPFor::BlockSize;
    } else if (name == "ObJustCopy") {
      codec.reset(new ObJustCopy());
      min_block_size_ = ObJustCopy::BlockSize;
    } else if (name == "ObDeltaZigzagRle") {
      codec.reset(new ObDeltaZigzagRle());
      min_block_size_ = ObDeltaZigzagRle::BlockSize;
    } else if (name == "ObDeltaZigzagPFor") {
      codec.reset(new ObDeltaZigzagPFor());
      min_block_size_ = ObDeltaZigzagPFor::BlockSize;
    } else if (name == "ObDoubleDeltaZigzagRle") {
      codec.reset(new ObDoubleDeltaZigzagRle());
      min_block_size_ = ObDoubleDeltaZigzagRle::BlockSize;
    } else if (name == "ObDoubleDeltaZigzagPFor") {
      codec.reset(new ObDoubleDeltaZigzagPFor());
      min_block_size_ = ObDoubleDeltaZigzagPFor::BlockSize;
    } else if (name == "ObXorFixedPfor") {
      codec.reset(new ObXorFixedPfor());
      min_block_size_ = ObXorFixedPfor::BlockSize;
    } else if (name == "ObTiredCodec-bp-uni") {
      auto tired_codec = new ObTiredCodec<ObSimpleBitPacking, ObUniversalCompression>();
      codec.reset(tired_codec);
      min_block_size_ = ObTiredCodec<ObSimpleBitPacking, ObUniversalCompression>::BlockSize;
      codec->set_allocator(alloc_);
      tired_codec->codec2_.set_compressor_type(ZSTD_1_3_8_COMPRESSOR);
    }
    else {
      throw new std::logic_error("Unknown codec " + name);
    }
  }

protected:
  std::unique_ptr<ObCodec> codec;
  std::vector<uint32_t> in32;
  std::vector<uint64_t> in64;
  std::vector<uint8_t> in8;
  std::vector<uint16_t> in16;
  uint32_t min_block_size_{0};
  ObArenaAllocator alloc_;

  void reset()
  {
    in32.clear();
    in64.clear();
    in8.clear();
    in16.clear();
    alloc_.reuse();
  }

  template<class T>
  void verify(T *in, uint64_t in_cnt)
  {
    if (!divisibleby(in_cnt, min_block_size_)) {
      // if can not divide, just return
      return;
    }
    LIB_LOG(INFO, "verify", KP(in), K(in_cnt), K(sizeof(T)));
    codec->set_uint_bytes(sizeof(T));
    uint64_t inital_size = codec->get_max_encoding_size((char *)in, in_cnt * sizeof(T));
    uint64_t encoded_size = inital_size;
    char *encoded = new char[encoded_size];
    memset(encoded, 0xFF, inital_size);

    uint64_t out_pos = 0;
    int ret = codec->encode((char *)in,
                            in_cnt * sizeof(T),
                            encoded,
                            encoded_size,
                            out_pos);
    ASSERT_TRUE(out_pos > 0 && out_pos < encoded_size);
    ASSERT_EQ(OB_SUCCESS, ret);

    std::vector<T> t_out;
    t_out.resize(in_cnt);
    uint64_t pos = 0;
    uint64_t out_pos2 = 0;
    ret = codec->decode(encoded,
                        out_pos,
                        pos,
                        in_cnt,
                        reinterpret_cast<char *>(t_out.data()),
                        in_cnt * sizeof(T),
                        out_pos2);
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

  void _copy64() {
    in64.clear();
    for (size_t i = 0; i < in32.size(); ++i) {
      in64.push_back(in32[i]);
    }
  }

  template<class T>
  void _genDataRandom(std::vector<T>& v, uint32_t cnt) {
     _genDataRandom<T>(v, cnt, std::numeric_limits<T>::min(), std::numeric_limits<T>::max());
  }

  template<class T>
  void _genDataRandom(std::vector<T>& v, uint32_t cnt, T min, T max) {
    v.clear();
    std::mt19937_64 e2(123456);
    std::uniform_int_distribution<T> dist(min, max);
    for (int i = 0; i < cnt; ++i) {
      v.push_back(dist(e2));
    }
  }

  void _genDataRandom64(uint32_t cnt) {
    _genDataRandom<uint64_t>(in64, cnt);
  }

  void _genDataRandom32(uint32_t cnt) {
    _genDataRandom<uint32_t>(in32, cnt);
  }

  template<class T>
  void _genDataWithFixBits(
                           std::vector<T>& v,
                           uint32_t bits,
                           uint32_t values) {
    v.clear();
    std::mt19937_64 e2(123456);
    std::uniform_int_distribution<T> dist(
                            0,
                            (bits == sizeof(T) * 8) ? ((T)(~0ULL)) : ((T)((1ULL << bits) - 1)));
    for (size_t i = 0; i < values; ++i) {
      v.push_back(static_cast<T>(dist(e2) | (1ULL << (bits - 1))));
    }
  }

  void _genDataWithFixBits32(uint32_t bits, uint32_t cnt) {
    _genDataWithFixBits<uint32_t>(in32, bits, cnt);
  }

  void _genDataWithFixBits64(uint32_t bits, uint32_t cnt) {
    _genDataWithFixBits<uint64_t>(in64, bits, cnt);
  }

  void _verify32()
  {
    verify<uint32_t>((uint32_t *)in32.data(), in32.size());
  }
  void _verify64()
  {
    verify<uint64_t>((uint64_t *)in64.data(), in64.size());
  }

  template<typename T>
  void _copy_and_verfiy_all(std::vector<T> &orig)
  {
    int32_t cnt = orig.size();
    if ((void *)&in8 != (void *)&orig) {
      in8.clear();
      for (size_t i = 0; i < cnt; ++i) {
        in8.push_back(orig[i]);
      }
      verify<uint8_t>((uint8_t *)in8.data(), in8.size());
    }
    if ((void *)&in16 != (void *)&orig) {
      in16.clear();
      for (size_t i = 0; i < cnt; ++i) {
        in16.push_back(orig[i]);
      }
      verify<uint16_t>((uint16_t *)in16.data(), in16.size());
    }
    if ((void *)&in32 != (void *)&orig) {
      in32.clear();
      for (size_t i = 0; i < cnt; ++i) {
        in32.push_back(orig[i]);
      }
      verify<uint32_t>((uint32_t *)in32.data(), in32.size());
    }
    if ((void *)&in64 != (void *)&orig) {
      in64.clear();
      for (size_t i = 0; i < cnt; ++i) {
        in64.push_back(orig[i]);
      }
      verify<uint64_t>((uint64_t *)in64.data(), in64.size());
    }
  }
};

TEST_P(ObBitPackingTest, increasingSequence) {
  in32.resize(0);
  for (int i = 0; i < 128; ++i) {
    in32.push_back(1);
  }
  in32[0] = 4;
  in32[127] = 4;
  in32[8] = 4;
  _verify32();
  _copy_and_verfiy_all(in32);
}

TEST_P(ObBitPackingTest, increasingSequence2) {
  in32.resize(0);
  for (int i = -100; i < 156; ++i) {
    in32.push_back(i);
  }
  _verify32();
  _copy_and_verfiy_all(in32);
}

TEST_P(ObBitPackingTest, randomNumbers) {
  _genDataRandom(in32, 65536);
  _verify32();
  _copy_and_verfiy_all(in32);
}

TEST_P(ObBitPackingTest, randomNumbers64) {
  _genDataRandom64(65536);
  _verify64();
  _copy_and_verfiy_all(in64);
}

TEST_P(ObBitPackingTest, adHocNumbers64) {
  int64_t data[] = {
              -3673975021604308289,
              277811506958363848,
              -7625128575524920515,
              -3321922176697690625,
              -8484521102416600502,
              4879706116117661039,
              3108316356327171753,
              -5023690236249800232};

  in64.clear();
  for (int i = 0; i < 64; ++i) {
    for (int j = 0; j < 4; ++j) {
      in64.push_back(data[j]);
    }
  }
  _verify64();
  _copy_and_verfiy_all(in64);
}

TEST_P(ObBitPackingTest, fastpack_zeors) {
  in32.clear();
  for (int i = 0; i < 256; ++i) {
    in32.push_back(0);
  }
  _verify32();
  _copy_and_verfiy_all(in32);
}

TEST_P(ObBitPackingTest, fastpack_zeros_with_exceptions) {
  in32.clear();
  in32.push_back(1024);
  for (int i = 0; i < 254; ++i) {
    in32.push_back(0);
  }
  in32.push_back(1033);

  _verify32();
  _copy_and_verfiy_all(in32);
}

TEST_P(ObBitPackingTest, fastpack_min_max) {
  _genDataRandom(in32, 256);
  in32[0] = std::numeric_limits<int32_t>::min();
  in32[127] = std::numeric_limits<int32_t>::max();
  in32[128] = std::numeric_limits<int32_t>::min();
  in32[255] = std::numeric_limits<int32_t>::max();

  _verify32();
  _copy_and_verfiy_all(in32);
}

TEST_P(ObBitPackingTest, fastpack_min_max64) {
  _genDataRandom64(256);
  in64[0] = std::numeric_limits<int64_t>::min();
  in64[127] = std::numeric_limits<int64_t>::max();
  in64[128] = std::numeric_limits<int64_t>::min();
  in64[255] = std::numeric_limits<int64_t>::max();

  _verify64();
  _copy_and_verfiy_all(in64);
}

TEST_P(ObBitPackingTest, fastpack_1_noexcept) {
  _genDataWithFixBits(in32, 1, 1024);
  _verify32();
  _copy_and_verfiy_all(in32);
}

TEST_P(ObBitPackingTest, fastpack_2_noexcept) {
  _genDataWithFixBits(in32, 2, 1024);
  _verify32();
  _copy_and_verfiy_all(in32);
}

TEST_P(ObBitPackingTest, fastpack_4_noexcept) {
  _genDataWithFixBits(in32, 4, 1024);
  _verify32();
  _copy_and_verfiy_all(in32);
}

TEST_P(ObBitPackingTest, fastpack_5_except) {
  _genDataWithFixBits(in32, 5, 256);
  in32[10] = 10002124;
  in32[77] = 20002124;
  in32[177] = 50002124;
  _verify32();
  _copy_and_verfiy_all(in32);
}

TEST_P(ObBitPackingTest, fastpack_8_noexcept) {
  _genDataWithFixBits(in32, 8, 512);
  _verify32();
  _copy_and_verfiy_all(in32);
}

TEST_P(ObBitPackingTest, fastpack_16_noexcept) {
  _genDataWithFixBits(in32, 16, 256);
  _verify32();
  _copy_and_verfiy_all(in32);
}

TEST_P(ObBitPackingTest, fastpack_22_noexcept) {
  _genDataWithFixBits(in32, 22, 256);
  _verify32();
  _copy_and_verfiy_all(in32);
}

TEST_P(ObBitPackingTest, fastpack_32_noexcept) {
  _genDataWithFixBits(in32, 32, 768);
  _verify32();
  _copy_and_verfiy_all(in32);
}

TEST_P(ObBitPackingTest, fastpack_40_noexcept) {
  _genDataWithFixBits64(40, 512);
  _verify64();
  _copy_and_verfiy_all(in64);
}

TEST_P(ObBitPackingTest, fastpack_41_noexcept) {
  _genDataWithFixBits64(41, 512);
  _verify64();
  _copy_and_verfiy_all(in64);
}

TEST_P(ObBitPackingTest, fastpack_51_noexcept) {
  _genDataWithFixBits64(51, 512);
  _verify64();
  _copy_and_verfiy_all(in64);
}

TEST_P(ObBitPackingTest, fastpack_56_noexcept) {
  _genDataWithFixBits64(56, 512);
  _verify64();
  _copy_and_verfiy_all(in64);
}

TEST_P(ObBitPackingTest, fastpack_63_noexcept) {
  _genDataWithFixBits64(63, 512);
  _verify64();
  _copy_and_verfiy_all(in64);
}

TEST_P(ObBitPackingTest, fastpack_64_noexcept) {
  _genDataWithFixBits64(64, 768);
  _verify64();
  _copy_and_verfiy_all(in64);
}

TEST_P(ObBitPackingTest, fastpack_7_except_63) {
  _genDataWithFixBits64(7, 256);
  std::vector<uint64_t> excepts;
  _genDataWithFixBits<uint64_t>(excepts, 63, 6);
  in64[0] = excepts[0];
  in64[10] = excepts[1];
  in64[100] = excepts[2];
  in64[133] = excepts[3];
  in64[177] = excepts[4];
  in64[213] = excepts[5];
  _verify64();
  _copy_and_verfiy_all(in64);
}

TEST_P(ObBitPackingTest, fastpack_0_except_32) {
  _genDataWithFixBits64(32, 256);
  for (int i = 20; i < 40; ++i) {
    in64[i] = 0;
  }
  for (int i = 155; i < 195; i += 2) {
    in64[i] = 0;
  }
  _verify64();
  _copy_and_verfiy_all(in64);
}

TEST_P(ObBitPackingTest, fastpack_3_except_64) {
  _genDataWithFixBits64(3, 256);
  for (int i = 20; i < 40; ++i) {
    in64[i] = ~0UL - i;
  }
  for (int i = 155; i < 195; i += 2) {
    in64[i] = ~0UL - i ;
  }
  _verify64();
  _copy_and_verfiy_all(in64);
}

TEST_P(ObBitPackingTest, fastpack_13_with_small_numbers) {
  _genDataWithFixBits(in32, 13, 256);
  in32[20] = 3U << 5;
  in32[60] = 2U << 4;
  in32[150] = 7U << 3;
  _verify32();
  _copy_and_verfiy_all(in32);
}

TEST_P(ObBitPackingTest, fastpack_35_with_excepts_and_small_numbers) {
  _genDataWithFixBits64(35, 256);
  std::vector<uint64_t> excepts;
  _genDataWithFixBits<uint64_t>(excepts, 48, 6);
  in64[10] = 5U << 5;
  in64[133] = 7U << 4;
  in64[115] = 6U << 3;

  in64[5] = excepts[0];
  in64[21] = excepts[1];
  in64[22] = excepts[2];
  in64[137] = excepts[3];
  in64[155] = excepts[4];
  in64[221] = excepts[5];
  _verify64();
  _copy_and_verfiy_all(in64);
}

TEST_P(ObBitPackingTest, min_max_one_by_one) {
  for (int64_t i = 1; i < 1000; i++) {
    in8.push_back(0);
    in8.push_back(UINT8_MAX/2);
    in16.push_back(0);
    in16.push_back(UINT16_MAX/2);
    in32.push_back(0);
    in32.push_back(UINT32_MAX/2);
    in64.push_back(0);
    in64.push_back(UINT64_MAX/2);
  }
  verify<uint8_t>((uint8_t *)in8.data(), in8.size());
  verify<uint16_t>((uint16_t *)in16.data(), in16.size());
  verify<uint32_t>((uint32_t *)in32.data(), in32.size());
  verify<uint64_t>((uint64_t *)in64.data(), in64.size());
}

TEST_P(ObBitPackingTest, min_to_half_max_one_by_one) {
  for (int64_t i = 1; i < 1000; i++) {
    in8.push_back(0);
    in8.push_back(UINT8_MAX);
    in16.push_back(0);
    in16.push_back(UINT16_MAX);
    in32.push_back(0);
    in32.push_back(UINT32_MAX);
    in64.push_back(0);
    in64.push_back(UINT64_MAX);
  }
  verify<uint8_t>((uint8_t *)in8.data(), in8.size());
  verify<uint16_t>((uint16_t *)in16.data(), in16.size());
  verify<uint32_t>((uint32_t *)in32.data(), in32.size());
  verify<uint64_t>((uint64_t *)in64.data(), in64.size());
}

TEST_P(ObBitPackingTest, loop_8) {
  for (int64_t i = 1; i < 1000; i++) {
    for (int64_t j = 0; j <= 8; j++) {
      _genDataWithFixBits<uint8_t>(in8, j, i);
      verify<uint8_t>((uint8_t *)in8.data(), in8.size());
      _copy_and_verfiy_all(in8);
      reset();
    }
  }
}

TEST_P(ObBitPackingTest, loop_8_random) {
  for (int64_t i = 1; i < 1000; i++) {
    for (int64_t j = 0; j <= 8; j++) {
      _genDataRandom<uint8_t>(in8, i, 0, (1UL << j));
      for (int64_t m = 0; m < i; m++) {
        if (m % 21 == 2) {
          in8[m] = (1UL << (j + 4)); // execption value
        }
      }
      verify<uint8_t>((uint8_t *)in8.data(), in8.size());
      _copy_and_verfiy_all(in8);
      reset();
    }
  }
}

TEST_P(ObBitPackingTest, loop_16) {
  for (int64_t i = 1; i < 1000; i++) {
    for (int64_t j = 0; j <= 16; j++) {
      _genDataWithFixBits<uint16_t>(in16, j, i);
      verify<uint16_t>((uint16_t *)in16.data(), in16.size());
      _copy_and_verfiy_all(in16);
      reset();
    }
  }
}

TEST_P(ObBitPackingTest, loop_16_random) {
  for (int64_t i = 1; i < 1000; i++) {
    for (int64_t j = 0; j <= 16; j++) {
      _genDataRandom<uint16_t>(in16, i, 0, (1UL << j));
      for (int64_t m = 0; m < i; m++) {
        if (m % 11 == 2) {
          in16[m] = (1UL << (j + 5)); // execption value
        }
      }
      verify<uint16_t>((uint16_t *)in16.data(), in16.size());
      _copy_and_verfiy_all(in16);
      reset();
    }
  }
}

TEST_P(ObBitPackingTest, loop_32) {
  for (int64_t i = 1; i < 1000; i++) {
    for (int64_t j = 0; j <= 32; j++) {
      _genDataWithFixBits(in32, j, i);
      _verify32();
      _copy_and_verfiy_all(in32);
    }
  }
}

TEST_P(ObBitPackingTest, loop_32_random) {
  for (int64_t i = 1; i < 1000; i++) {
    for (int64_t j = 0; j <= 32; j++) {
      _genDataRandom<uint32_t>(in32, i, 0, (1UL << j));
      for (int64_t m = 0; m < i; m++) {
        if (m % 21 == 2) {
          in32[m] = (1UL << (j + 6)); // execption value
        }
      }
      _verify32();
      _copy_and_verfiy_all(in32);
    }
  }
}

TEST_P(ObBitPackingTest, loop_64) {
  for (int64_t i = 1; i < 1000; i++) {
    for (int64_t j = 0; j <= 64; j++) {
      _genDataWithFixBits64(j, i);
      _verify64();
      _copy_and_verfiy_all(in64);
      reset();
    }
  }
}

TEST_P(ObBitPackingTest, loop_64_random) {
  for (int64_t i = 1; i <= 1000; i++) {
    for (int64_t j = 0; j <= 64; j++) {
      _genDataRandom<uint64_t>(in64, i, 0, (1ULL << j));

      for (int64_t m = 0; m < i; m++) {
        if (m % 11 == 2) {
          in64[m] = (1ULL << (j + 6)); // execption value
        }
      }

      _verify64();
      _copy_and_verfiy_all(in64);
      reset();
    }
  }
}

INSTANTIATE_TEST_CASE_P(
    FastPForLib,
    ObBitPackingTest,
    Values("ObSimpleBitPacking",
           "ObSIMDFixedPFor",
           "ObDeltaZigzagRle",
           "ObDeltaZigzagPFor",
           "ObDoubleDeltaZigzagRle",
           "ObDoubleDeltaZigzagPFor",
           "ObXorFixedPfor",
           "ObTiredCodec-bp-uni"));
} // namespace common
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_bitpacking.log*");
  OB_LOGGER.set_file_name("test_bitpacking.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);

  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
