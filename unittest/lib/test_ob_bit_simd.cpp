/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define OB_UNITTEST_MULTITARGET_CODE_SCOPED_TRACE(msg) SCOPED_TRACE(msg)

#include <gtest/gtest.h>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <vector>
#include "lib/container/ob_bit_simd.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase::common;

class TestObBitSimd : public ::testing::Test {};

// Sizes chosen to hit SIMD alignment boundaries and scalar tails:
//   pack/unpack:     SIMD chunks at 16(SSE)/32(AVX2)/64(AVX512)/128(NEON),  8-byte mid loop, <8 tail
//   bits_bit_or_bytes64: 64-element main loop, 8-element mid loop, <8 tail
//   popcnt_bits:     aligned at 512(SSE/NEON)/1024(AVX2)/2048(AVX512), 64-bit mid loop, <64 tail
//   popcnt_bytes:    64(SSE/NEON)/128(AVX2)/256(AVX512) byte chunks, scalar tail
static const std::vector<int64_t> kTestSizes = {
  0,   1,   2,   3,   4,   5,   6,   7,    8,    9,    10,   13,   15,   16,   17,   24,
  31,  32,  33,  48,  63,  64,  65,  72,   80,   100,  127,  128,  129,  192,  255,  256,
  257, 384, 511, 512, 513, 640, 768, 1023, 1024, 1025, 1536, 2047, 2048, 2049, 3072, 4096};

// ====================== reference helpers ======================

static int64_t ref_popcnt_bits(const uint64_t *data, int64_t nbits)
{
  int64_t cnt = 0;
  for (int64_t i = 0; i < nbits; ++i) {
    cnt += (data[i >> 6] >> (i & 63)) & 1;
  }
  return cnt;
}

static int64_t ref_popcnt_bits_u8(const uint8_t *data, int64_t nbits)
{
  int64_t cnt = 0;
  for (int64_t i = 0; i < nbits; ++i) {
    cnt += (data[i >> 3] >> (i & 7)) & 1;
  }
  return cnt;
}

// Fill every `step`-th bit in a byte-packed array.
static void fill_bits_pattern(uint8_t *data, int64_t nbits, int step)
{
  for (int64_t i = 0; i < nbits; i += step) {
    data[i >> 3] |= 1u << (i & 7);
  }
}

// ====================== popcnt_bytes ======================

TEST_F(TestObBitSimd, popcnt_bytes)
{
  for (int64_t sz : kTestSizes) {
    // all-zero
    std::vector<uint8_t> data(sz, 0);
    OB_UNITTEST_MULTITARGET_CODE(
      EXPECT_EQ(0, NS::popcnt_bytes(data.data(), sz)) << "size=" << sz;
    )

    // all-one
    std::fill(data.begin(), data.end(), 1);
    OB_UNITTEST_MULTITARGET_CODE(
      EXPECT_EQ(sz, NS::popcnt_bytes(data.data(), sz)) << "size=" << sz;
    )

    // only last byte is 1 — tests scalar tail produces correct count
    std::fill(data.begin(), data.end(), 0);
    if (sz > 0) {
      data[sz - 1] = 1;
      OB_UNITTEST_MULTITARGET_CODE(
        EXPECT_EQ(1, NS::popcnt_bytes(data.data(), sz)) << "size=" << sz;
      )
    }

    // alternating 0/1 — half set
    for (int64_t i = 0; i < sz; ++i) {
      data[i] = static_cast<uint8_t>(i & 1);
    }
    OB_UNITTEST_MULTITARGET_CODE(
      EXPECT_EQ(sz / 2, NS::popcnt_bytes(data.data(), sz)) << "size=" << sz;
    )
  }
}

// ====================== popcnt_bits (uint64_t*) ======================

TEST_F(TestObBitSimd, popcnt_bits_u64)
{
  for (int64_t sz : kTestSizes) {
    int64_t nwords = (sz + 63) / 64 + 1;

    // all-zero
    std::vector<uint64_t> data(nwords, 0);
    OB_UNITTEST_MULTITARGET_CODE(
      EXPECT_EQ(0, NS::popcnt_bits(data.data(), sz)) << "size=" << sz;
    )

    // all-one
    std::fill(data.begin(), data.end(), ~0ULL);
    OB_UNITTEST_MULTITARGET_CODE(
      EXPECT_EQ(sz, NS::popcnt_bits(data.data(), sz)) << "size=" << sz;
    )

    if (sz == 0) continue;

    // only last bit set — tests tail masking
    std::fill(data.begin(), data.end(), 0ULL);
    data[(sz - 1) >> 6] |= 1ULL << ((sz - 1) & 63);
    OB_UNITTEST_MULTITARGET_CODE(
      EXPECT_EQ(1, NS::popcnt_bits(data.data(), sz)) << "size=" << sz;
    )

    // every 3rd bit set
    std::fill(data.begin(), data.end(), 0ULL);
    for (int64_t i = 0; i < sz; i += 3) {
      data[i >> 6] |= 1ULL << (i & 63);
    }
    const int64_t ref = ref_popcnt_bits(data.data(), sz);
    OB_UNITTEST_MULTITARGET_CODE(
      EXPECT_EQ(ref, NS::popcnt_bits(data.data(), sz)) << "size=" << sz;
    )
  }
}

// ====================== popcnt_bits (uint8_t*) ======================

TEST_F(TestObBitSimd, popcnt_bits_u8)
{
  for (int64_t sz : kTestSizes) {
    int64_t nbytes = (sz + 7) / 8 + 8;

    // all-zero
    std::vector<uint8_t> data(nbytes, 0);
    OB_UNITTEST_MULTITARGET_CODE(
      EXPECT_EQ(0, NS::popcnt_bits(data.data(), sz)) << "size=" << sz;
    )

    // all-one
    std::fill(data.begin(), data.end(), static_cast<uint8_t>(0xFF));
    OB_UNITTEST_MULTITARGET_CODE(
      EXPECT_EQ(sz, NS::popcnt_bits(data.data(), sz)) << "size=" << sz;
    )

    if (sz == 0) continue;

    // only last bit set
    std::fill(data.begin(), data.end(), static_cast<uint8_t>(0));
    data[(sz - 1) >> 3] |= 1u << ((sz - 1) & 7);
    OB_UNITTEST_MULTITARGET_CODE(
      EXPECT_EQ(1, NS::popcnt_bits(data.data(), sz)) << "size=" << sz;
    )

    // every 2nd bit set
    std::fill(data.begin(), data.end(), static_cast<uint8_t>(0));
    fill_bits_pattern(data.data(), sz, 2);
    const int64_t ref = ref_popcnt_bits_u8(data.data(), sz);
    OB_UNITTEST_MULTITARGET_CODE(
      EXPECT_EQ(ref, NS::popcnt_bits(data.data(), sz)) << "size=" << sz;
    )
  }
}

// ====================== unpack_bits_to_bytes (uint8_t*) ======================

TEST_F(TestObBitSimd, unpack_bits_to_bytes)
{
  for (int64_t sz : kTestSizes) {
    if (sz == 0) continue;
    int64_t nbytes = (sz + 7) / 8 + 8;

    // pattern: every 3rd bit set
    std::vector<uint8_t> bits(nbytes, 0);
    fill_bits_pattern(bits.data(), sz, 3);

    for (int flip = 0; flip <= 1; ++flip) {
      OB_UNITTEST_MULTITARGET_CODE(
        std::vector<uint8_t> out(sz, 0xFF);
        if (flip) {
          NS::unpack_bits_to_bytes<true>(bits.data(), out.data(), sz);
        } else {
          NS::unpack_bits_to_bytes<false>(bits.data(), out.data(), sz);
        }
        for (int64_t i = 0; i < sz; ++i) {
          uint8_t raw = static_cast<uint8_t>((bits[i >> 3] >> (i & 7)) & 1);
          uint8_t expected = flip ? static_cast<uint8_t>(raw ^ 1) : raw;
          EXPECT_EQ(expected, out[i]) << "size=" << sz << " flip=" << flip << " i=" << i;
        }
      )
    }

    // all bits set — every output byte should be 1 (no flip) or 0 (flip)
    std::fill(bits.begin(), bits.end(), static_cast<uint8_t>(0xFF));
    OB_UNITTEST_MULTITARGET_CODE(
      std::vector<uint8_t> out(sz, 0xFF);
      NS::unpack_bits_to_bytes<false>(bits.data(), out.data(), sz);
      for (int64_t i = 0; i < sz; ++i) {
        EXPECT_EQ(1, out[i]) << "all_set size=" << sz << " i=" << i;
      }
    )
    OB_UNITTEST_MULTITARGET_CODE(
      std::vector<uint8_t> out(sz, 0xFF);
      NS::unpack_bits_to_bytes<true>(bits.data(), out.data(), sz);
      for (int64_t i = 0; i < sz; ++i) {
        EXPECT_EQ(0, out[i]) << "all_set_flip size=" << sz << " i=" << i;
      }
    )

    // only last bit set — verifies scalar tail handles the very last element
    std::fill(bits.begin(), bits.end(), static_cast<uint8_t>(0));
    bits[(sz - 1) >> 3] |= 1u << ((sz - 1) & 7);
    OB_UNITTEST_MULTITARGET_CODE(
      std::vector<uint8_t> out(sz, 0xFF);
      NS::unpack_bits_to_bytes<false>(bits.data(), out.data(), sz);
      for (int64_t i = 0; i < sz - 1; ++i) {
        EXPECT_EQ(0, out[i]) << "last_bit size=" << sz << " i=" << i;
      }
      EXPECT_EQ(1, out[sz - 1]) << "last_bit size=" << sz;
    )
  }
}

// ====================== pack_bytes_to_bits (uint8_t*) ======================

TEST_F(TestObBitSimd, pack_bytes_to_bits)
{
  for (int64_t sz : kTestSizes) {
    if (sz == 0) continue;

    // pattern: every 3rd byte set
    std::vector<uint8_t> bytes(sz, 0);
    for (int64_t i = 0; i < sz; i += 3) {
      bytes[i] = 1;
    }

    for (int flip = 0; flip <= 1; ++flip) {
      OB_UNITTEST_MULTITARGET_CODE(
        int64_t nbytes = (sz + 7) / 8 + 8;
        std::vector<uint8_t> bits(nbytes, 0);
        if (flip) {
          NS::pack_bytes_to_bits<true>(bytes.data(), bits.data(), sz);
        } else {
          NS::pack_bytes_to_bits<false>(bytes.data(), bits.data(), sz);
        }
        for (int64_t i = 0; i < sz; ++i) {
          uint8_t actual = static_cast<uint8_t>((bits[i >> 3] >> (i & 7)) & 1);
          uint8_t expected = flip ? static_cast<uint8_t>(bytes[i] ^ 1) : bytes[i];
          EXPECT_EQ(expected, actual) << "size=" << sz << " flip=" << flip << " i=" << i;
        }
      )
    }

    // only last byte set — verifies <8 tail path
    std::fill(bytes.begin(), bytes.end(), static_cast<uint8_t>(0));
    bytes[sz - 1] = 1;
    OB_UNITTEST_MULTITARGET_CODE(
      int64_t nbytes = (sz + 7) / 8 + 8;
      std::vector<uint8_t> bits(nbytes, 0);
      NS::pack_bytes_to_bits<false>(bytes.data(), bits.data(), sz);
      for (int64_t i = 0; i < sz; ++i) {
        uint8_t actual = static_cast<uint8_t>((bits[i >> 3] >> (i & 7)) & 1);
        uint8_t expected = (i == sz - 1) ? 1 : 0;
        EXPECT_EQ(expected, actual) << "last_byte size=" << sz << " i=" << i;
      }
    )
  }
}

TEST_F(TestObBitSimd, pack_bytes_to_bits_preserves_tail)
{
  // Tail bits beyond `size` must not be overwritten.
  // Test at each possible tail length 1..7.
  for (int64_t tail = 1; tail <= 7; ++tail) {
    const int64_t sz = 64 + tail;
    std::vector<uint8_t> bytes(sz, 0);
    bytes[sz - 1] = 1;
    OB_UNITTEST_MULTITARGET_CODE(
      int64_t nbytes = (sz + 7) / 8 + 8;
      std::vector<uint8_t> bits(nbytes, 0xFF);
      NS::pack_bytes_to_bits<false>(bytes.data(), bits.data(), sz);
      EXPECT_EQ(1, (bits[(sz - 1) >> 3] >> ((sz - 1) & 7)) & 1) << "tail=" << tail;
      int64_t next_byte = ((sz + 7) / 8) * 8;
      for (int64_t i = sz; i < next_byte; ++i) {
        EXPECT_EQ(1, (bits[i >> 3] >> (i & 7)) & 1) << "tail=" << tail << " preserved_bit=" << i;
      }
    )
  }
}

// ====================== roundtrip: pack then unpack ======================

TEST_F(TestObBitSimd, roundtrip_pack_unpack)
{
  for (int64_t sz : kTestSizes) {
    if (sz == 0) continue;
    std::vector<uint8_t> bytes(sz, 0);
    for (int64_t i = 0; i < sz; i += 2) {
      bytes[i] = 1;
    }
    OB_UNITTEST_MULTITARGET_CODE(
      int64_t nbytes = (sz + 7) / 8 + 8;
      std::vector<uint8_t> bits(nbytes, 0);
      NS::pack_bytes_to_bits<false>(bytes.data(), bits.data(), sz);

      std::vector<uint8_t> out(sz, 0xFF);
      NS::unpack_bits_to_bytes<false>(bits.data(), out.data(), sz);
      for (int64_t i = 0; i < sz; ++i) {
        EXPECT_EQ(bytes[i], out[i]) << "size=" << sz << " i=" << i;
      }

      std::fill(out.begin(), out.end(), 0xFF);
      NS::unpack_bits_to_bytes<true>(bits.data(), out.data(), sz);
      for (int64_t i = 0; i < sz; ++i) {
        EXPECT_EQ(static_cast<uint8_t>(bytes[i] ^ 1), out[i]) << "flip size=" << sz << " i=" << i;
      }
    )
  }
}

// ====================== bits_bit_or_bytes64 (uint8_t*) ======================

TEST_F(TestObBitSimd, bits_bit_or_bytes64)
{
  for (int64_t sz : kTestSizes) {
    if (sz == 0) continue;

    // pattern: every 3rd element nonzero
    std::vector<uint64_t> bytes(sz, 0);
    for (int64_t i = 0; i < sz; i += 3) {
      bytes[i] = static_cast<uint64_t>(i + 1);
    }

    for (int flip = 0; flip <= 1; ++flip) {
      int64_t nbytes = (sz + 7) / 8 + 8;
      std::vector<uint8_t> ref_bits(nbytes, 0);
      for (int64_t i = 0; i < sz; ++i) {
        bool nonzero = (bytes[i] != 0);
        bool set_bit = flip ? !nonzero : nonzero;
        if (set_bit) {
          ref_bits[i >> 3] |= 1u << (i & 7);
        }
      }
      OB_UNITTEST_MULTITARGET_CODE(
        std::vector<uint8_t> bits(nbytes, 0);
        if (flip) {
          NS::bits_bit_or_bytes64<true>(bits.data(), bytes.data(), sz);
        } else {
          NS::bits_bit_or_bytes64<false>(bits.data(), bytes.data(), sz);
        }
        for (int64_t i = 0; i < nbytes; ++i) {
          EXPECT_EQ(ref_bits[i], bits[i]) << "size=" << sz << " flip=" << flip << " byte=" << i;
        }
      )
    }

    // only last element nonzero — tests scalar tail
    std::fill(bytes.begin(), bytes.end(), 0ULL);
    bytes[sz - 1] = 42;
    OB_UNITTEST_MULTITARGET_CODE(
      int64_t nbytes = (sz + 7) / 8 + 8;
      std::vector<uint8_t> bits(nbytes, 0);
      NS::bits_bit_or_bytes64<false>(bits.data(), bytes.data(), sz);
      for (int64_t i = 0; i < sz; ++i) {
        uint8_t actual = static_cast<uint8_t>((bits[i >> 3] >> (i & 7)) & 1);
        uint8_t expected = (i == sz - 1) ? 1 : 0;
        EXPECT_EQ(expected, actual) << "last_elem size=" << sz << " i=" << i;
      }
    )
  }
}

TEST_F(TestObBitSimd, bits_bit_or_bytes64_or_semantics)
{
  const int64_t sz = 128;
  int64_t nbytes = (sz + 7) / 8 + 8;
  std::vector<uint64_t> bytes1(sz, 0);
  std::vector<uint64_t> bytes2(sz, 0);
  for (int64_t i = 0; i < sz; i += 2) {
    bytes1[i] = 1;
  }
  for (int64_t i = 0; i < sz; i += 3) {
    bytes2[i] = 1;
  }
  OB_UNITTEST_MULTITARGET_CODE(
    std::vector<uint8_t> bits(nbytes, 0);
    NS::bits_bit_or_bytes64<false>(bits.data(), bytes1.data(), sz);
    NS::bits_bit_or_bytes64<false>(bits.data(), bytes2.data(), sz);
    for (int64_t i = 0; i < sz; ++i) {
      uint8_t actual = static_cast<uint8_t>((bits[i >> 3] >> (i & 7)) & 1);
      uint8_t expected = (bytes1[i] != 0 || bytes2[i] != 0) ? 1 : 0;
      EXPECT_EQ(expected, actual) << "i=" << i;
    }
  )
}

int main(int argc, char **argv)
{
  system("rm -f test_ob_bit_simd.log*");
  OB_LOGGER.set_file_name("test_ob_bit_simd.log", true, false);
  ObLogger::get_logger().set_log_level("DEBUG");
  // Suppress grep output from CpuFlagSet::init_from_os() when detecting CPU flags.
  fflush(stdout);
  int saved_stdout = dup(STDOUT_FILENO);
  if (saved_stdout >= 0) {
    freopen("/dev/null", "w", stdout);
    init_arches();
    fflush(stdout);
    dup2(saved_stdout, STDOUT_FILENO);
    close(saved_stdout);
  } else {
    init_arches();
  }
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
