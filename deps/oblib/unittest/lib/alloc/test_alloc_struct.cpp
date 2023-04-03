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
#include "lib/alloc/alloc_struct.h"
#include "lib/alloc/abit_set.h"

using namespace oceanbase::lib;

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(TestABitSet, Basic)
{
  char bm_buf[ABitSet::buf_len(80)];
  ABitSet bm(80, bm_buf);

  EXPECT_EQ(-1, bm.find_first_significant(0));
  EXPECT_EQ(-1, bm.find_first_significant(79));
  EXPECT_EQ(-1, bm.find_first_significant(80));
  EXPECT_EQ(-1, bm.find_first_significant(81));
  EXPECT_EQ(-1, bm.find_first_significant(1000));

  bm.set(10);
  EXPECT_EQ(10, bm.find_first_significant(0));
  EXPECT_EQ(-1, bm.find_first_significant(79));
  EXPECT_EQ(-1, bm.find_first_significant(80));
  EXPECT_EQ(-1, bm.find_first_significant(81));
  EXPECT_EQ(-1, bm.find_first_significant(1000));

  bm.set(79);
  EXPECT_EQ(10, bm.find_first_significant(0));
  EXPECT_EQ(79, bm.find_first_significant(79));
  EXPECT_EQ(-1, bm.find_first_significant(80));
  EXPECT_EQ(-1, bm.find_first_significant(81));
  EXPECT_EQ(-1, bm.find_first_significant(1000));

  bm.unset(10);
  EXPECT_EQ(79, bm.find_first_significant(0));
  EXPECT_EQ(79, bm.find_first_significant(79));
  EXPECT_EQ(-1, bm.find_first_significant(80));
  EXPECT_EQ(-1, bm.find_first_significant(81));
  EXPECT_EQ(-1, bm.find_first_significant(1000));

  bm.set(80);
  EXPECT_EQ(79, bm.find_first_significant(0));
  EXPECT_EQ(79, bm.find_first_significant(79));
  EXPECT_EQ(-1, bm.find_first_significant(80));
  EXPECT_EQ(-1, bm.find_first_significant(81));
  EXPECT_EQ(-1, bm.find_first_significant(1000));

  bm.set(81);
  EXPECT_EQ(79, bm.find_first_significant(0));
  EXPECT_EQ(-1, bm.find_first_significant(80));
  EXPECT_EQ(-1, bm.find_first_significant(81));
  EXPECT_EQ(-1, bm.find_first_significant(1000));

  bm.set(0);
  EXPECT_EQ(0, bm.find_first_significant(0));

  EXPECT_TRUE(bm.isset(79));
  EXPECT_FALSE(bm.isset(80));
  EXPECT_FALSE(bm.isset(81));

  bm.clear();
  for (int i = 0; i < 80; i++) {
    EXPECT_FALSE(bm.isset(i));
  }

  char bm2_buf[ABitSet::buf_len(129)];
  ABitSet bm2(129, bm_buf);
  bm2.set(65);
  bm2.set(128);
  EXPECT_EQ(65, bm2.find_first_significant(0));
  EXPECT_EQ(128, bm2.find_first_significant(66));

  {
    char bm_buf[ABitSet::buf_len(8201)];
    ABitSet bm(8201, bm_buf);
    bm.set(6136);
    EXPECT_EQ(6136,bm.find_first_significant(6136));
    EXPECT_EQ(6136,bm.find_first_significant(5814));
    bm.unset(6136);
    EXPECT_EQ(-1,bm.find_first_significant(5814));
  }

  {
    char bm_buf[ABitSet::buf_len(8201)];
    ABitSet bm(8201, bm_buf);
    bm.set(88);
    bm.set(414);
    bm.set(960);
    bm.set(972);
    bm.set(976);
    EXPECT_EQ(-1, bm.find_first_significant(4044));
  }
}

TEST(TESTABitSet, Basic2)
{
  char bm_buf[ABitSet::buf_len(1026)];
  ABitSet bm(1026, bm_buf);
  bm.set(1024);
  EXPECT_EQ(1024, bm.find_first_significant(1023));
}

TEST(TESTABitSet, Big)
{
  char bm_buf[ABitSet::buf_len(4096)];
  ABitSet bm(4096, bm_buf);

  EXPECT_EQ(-1, bm.find_first_significant(0));

  bm.set(1234);
  EXPECT_TRUE(bm.isset(1234));
  EXPECT_EQ(1234, bm.find_first_significant(0));
  EXPECT_EQ(1234, bm.find_first_significant(1234));
  EXPECT_EQ(-1, bm.find_first_significant(1235));

  bm.set(3210);
  bm.isset(3210);
  EXPECT_EQ(1234, bm.find_first_significant(0));
  EXPECT_EQ(1234, bm.find_first_significant(1234));
  EXPECT_EQ(3210, bm.find_first_significant(1235));
  EXPECT_EQ(3210, bm.find_first_significant(3210));
  EXPECT_EQ(-1, bm.find_first_significant(3211));

  // test first most
  EXPECT_EQ(3210, bm.find_first_most_significant(4000));
  EXPECT_EQ(3210, bm.find_first_most_significant(3210));
  EXPECT_EQ(1234, bm.find_first_most_significant(3209));
  EXPECT_EQ(1234, bm.find_first_most_significant(1234));
  EXPECT_EQ(-1, bm.find_first_most_significant(1233));
  EXPECT_EQ(3210, bm.find_first_most_significant(4095));
  bm.set(4094);
  EXPECT_EQ(4094, bm.find_first_most_significant(4095));
}
