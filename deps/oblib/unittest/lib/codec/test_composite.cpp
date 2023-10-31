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

#include "lib/codec/ob_codecs.h"
#include "lib/codec/ob_composite_codec.h"
#include "lib/codec/ob_simd_fixed_pfor.h"

#include "gtest/gtest.h"

namespace oceanbase
{
namespace common
{

class CompositeCodecTest : public ::testing::Test {
  public:
    virtual void SetUp();

    protected:
      std::unique_ptr<ObCodec> codec;
      std::vector<int32_t> in32, out32;
      std::vector<uint32_t> encoded;
      std::vector<int64_t> in64, out64;

      void _verify() {
        codec->set_uint_bytes(sizeof(uint32_t));
        size_t inSize = in32.size();
        uint64_t inital_size = codec->get_max_encoding_size((char *)in32.data(), inSize * sizeof(uint32_t));
        uint64_t encoded_size = inital_size;
        char *encoded = new char[encoded_size];
        uint64_t out_pos = 0;

        int ret = codec->encode(reinterpret_cast<char *>(in32.data()),
                                inSize * sizeof(uint32_t),
                                encoded,
                                encoded_size,
                                out_pos);
        ASSERT_EQ(OB_SUCCESS, ret);

        uint64_t pos = 0;
        uint64_t out_pos2 = 0;
        out32.resize(inSize);
        ret = codec->decode(encoded,
                            out_pos,
                            pos,
                            inSize,
                            reinterpret_cast<char *>(out32.data()),
                            inSize * sizeof(uint32_t),
                            out_pos2);
        ASSERT_EQ(OB_SUCCESS, ret);

        bool passed = true;
        for (size_t i = 0; i < inSize; ++i) {
          if (in32[i] != out32[i]) {
            passed = false;
          }
          EXPECT_EQ(in32[i], out32[i]);
        }
        if (!passed) {
          std::cout << "Test failed with int32 input: ";
          for (size_t i = 0; i < inSize; ++i) {
            std::cout << in32[i] << " ";
          }
          std::cout << std::endl;
        }
        delete []encoded;
        encoded = nullptr;
      }

      void _verify64() {
        size_t inSize = in64.size();
        codec->set_uint_bytes(sizeof(uint64_t));
        uint64_t inital_size = codec-> get_max_encoding_size(reinterpret_cast<char *>(in64.data()), inSize * sizeof(uint64_t));
        uint64_t encoded_size = inital_size;
        char *encoded = new char[inital_size];

        uint64_t out_pos = 0;
        int ret = codec->encode(reinterpret_cast<char *>(in64.data()),
                                inSize * sizeof(uint64_t),
                                encoded,
                                encoded_size,
                                out_pos);
        ASSERT_EQ(OB_SUCCESS, ret);

        out64.resize(inSize);
        uint64_t pos = 0;
        uint64_t out_pos2 = 0;
        ret = codec->decode(encoded,
                            out_pos,
                            pos,
                            inSize,
                            reinterpret_cast<char *>(out64.data()),
                            inSize * sizeof(uint64_t),
                            out_pos2);
        ASSERT_EQ(OB_SUCCESS, ret);

        bool passed = true;
        for (size_t i = 0; i < inSize; ++i) {
          if (in64[i] != out64[i]) {
            passed = false;
          }
          EXPECT_EQ(in64[i], out64[i]);
        }
        if (!passed) {
          std::cout << "Test failed with int64 input: ";
          for (size_t i = 0; i < inSize; ++i) {
            std::cout << in64[i] << " ";
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
  };

void CompositeCodecTest::SetUp()
{
  codec.reset(new ObCompositeCodec<ObSIMDFixedPFor, ObJustCopy>());
}

//TEST_F(CompositeCodecTest, emptyArray) {
//  in32.resize(0);
//  out32.resize(0);
//  _verify();
//  _copy64();
//  _verify64();
//}

TEST_F(CompositeCodecTest, lessThanOneBlock) {
  in32.resize(0);
  for (int32_t i = 0; i < 255; i += 2) {
    in32.push_back(i);
  }
  _verify();
  _copy64();
  _verify64();
}

TEST_F(CompositeCodecTest, exactOneBlock) {
  in32.resize(0);
  for (int32_t i = 0; i < 256; i += 2) {
    in32.push_back(i);
  }
  _verify();
  _copy64();
  _verify64();
}

TEST_F(CompositeCodecTest, moreThanThreeBlock) {
  in32.resize(0);
  for (int i = 0; i < 1000; i = i + 3) {
    in32.push_back(i);
  }
  _verify();
  _copy64();
  _verify64();
}

TEST_F(CompositeCodecTest, randomeNumberMoreThanOnePage) {
  in32.resize(0);
  std::mt19937_64 e2(123456);
  std::uniform_int_distribution<int32_t> dist(
                          std::numeric_limits<int32_t>::min(),
                          std::numeric_limits<int32_t>::max());
  for (int i = 0; i < 70000; ++i) {
    in32.push_back(dist(e2));
  }
  _verify();
  _copy64();
  _verify64();
}

TEST_F(CompositeCodecTest, randomeNumberMoreThanOnePage64) {
  in64.resize(0);
  std::mt19937_64 e2(123456);
  std::uniform_int_distribution<int64_t> dist(
                          std::numeric_limits<int64_t>::min(),
                          std::numeric_limits<int64_t>::max());
  for (int i = 0; i < 70000; ++i) {
    in64.push_back(dist(e2));
  }
  _verify64();
}

} // namespace common
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_composite.log*");
  OB_LOGGER.set_file_name("test_composite.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");

  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
