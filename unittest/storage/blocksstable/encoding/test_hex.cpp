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

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#include "storage/blocksstable/encoding/ob_hex_string_encoder.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

TEST(ObHexStringMap, store_order)
{
  ObHexStringMap map;
  const char *input_str = "0123456789";
  for (int i = 0; i < 10; i++) {
    map.mark(input_str[i]);
  }
  ASSERT_TRUE(map.can_packing());
  unsigned char index[10];
  map.build_index(index);

  unsigned char data[5];
  MEMSET(data, 0, sizeof(data));
  ObHexStringPacker packer(map, data);
  packer.pack(reinterpret_cast<const unsigned char *>(input_str), 10);

  char str[11];
  MEMSET(str, 0, 11);
  for (int i = 0; i < 10; i += 2) {
    snprintf(str + i, 3,  "%02x", data[i/2]);
  }

  ASSERT_EQ(0, memcmp(str, input_str, 10));
}

void pack(ObHexStringPacker &packer, const unsigned char *str, const int64_t len)
{
  int64_t done = 0;
  while (done < len) {
    const int64_t step =  std::max(std::min(len - done, random() % (std::max(len / 2, 1L))), 1L);
    if (random() & 0x1) {
      packer.pack(str + done, step);
    } else {
      for (int64_t i = 0; i < step; i++) {
        packer.pack(str[done + i]);
      }
    }
    done += step;
  }
}

void unpack(ObHexStringUnpacker &unpacker, unsigned char *str, const int64_t len)
{
  int64_t done = 0;
  while (done < len) {
    const int64_t step =  std::max(std::min(len - done, random() % (std::max(len / 2, 1L))), 1L);
    if (random() & 0x1) {
      unpacker.unpack(str + done, step);
    } else {
      for (int64_t i = 0; i < step; i++) {
        unpacker.unpack(str[done + i]);
      }
    }
    done += step;
  }
}

TEST(ObHexString, pack_unpack)
{
  for (int64_t i = 0; i < 100; i++) {
    ObHexStringMap map;
    while (map.size_ < 16) {
      map.mark(static_cast<unsigned char>(random() % 256));
    }
    ASSERT_TRUE(map.can_packing());
    unsigned char index[16];
    map.build_index(index);
    const int64_t len = random() % 1000 + 32;

    unsigned char input[len];
    unsigned char hex[len];
    unsigned char output[len];

    for (int64_t j = 0; j < len; j++) {
      input[j] = index[random() % 16];
    }
    MEMSET(hex, 0, len);
    ObHexStringPacker packer(map, hex);
    pack(packer, input, len);
    ObHexStringUnpacker unpacker(index, hex);
    unpack(unpacker, output, len);

    ASSERT_EQ(0, memcmp(input, output, len));
  }
}

} // end namespace blocksstable
} // end namespace oceanbase

int main(int argc, char **argv)
{

  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
