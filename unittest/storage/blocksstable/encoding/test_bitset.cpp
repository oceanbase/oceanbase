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
#include "storage/blocksstable/encoding/ob_encoding_bitset.h"
#include "lib/time/ob_time_utility.h"
#include <vector>

namespace oceanbase
{
namespace blocksstable
{

using namespace common;

TEST(BitSet, set_get)
{
  const int64_t sset_cnt = 50;
  const int64_t lset_cnt = 400;

  uint64_t s[1];
  uint64_t l[8];

  std::vector<int64_t> s_pos;
  std::vector<int64_t> l_pos;

  BitSet sset;
  ASSERT_EQ(OB_SUCCESS, sset.init(s, sset_cnt));
  BitSet lset;
  ASSERT_EQ(OB_SUCCESS, lset.init(l, lset_cnt));

  for (int64_t tn = 0; tn < 1000; ++tn) {
    sset.reset();
    lset.reset();
    s_pos.clear();
    l_pos.clear();

    int64_t i = 0;
    while(i < sset_cnt) {
      sset.set(i);
      s_pos.push_back(i);
      i += random() % 5 + 1;
    }

    for(i = 0; i < sset_cnt; ++i) {
      std::vector<int64_t>::iterator iter = std::find(s_pos.begin(), s_pos.end(), i);
      if (s_pos.end() != iter) {
        ASSERT_TRUE(sset.get(i))
          << "i: " << i << std::endl;
        ASSERT_EQ(iter - s_pos.begin(), sset.get_ref(i))
          << "i: " << i << std::endl;
      } else {
        ASSERT_FALSE(sset.get(i))
          << "i: " << i << std::endl;
      }
    }

    i = 0;
    while(i < lset_cnt) {
      lset.set(i);
      l_pos.push_back(i);
      i += random() % 5 + 1;
    }

    for(i = 0; i < lset_cnt; ++i) {
      std::vector<int64_t>::iterator iter = std::find(l_pos.begin(), l_pos.end(), i);
      if (l_pos.end() != iter) {
        ASSERT_TRUE(lset.get(i))
          << "i: " << i << std::endl;
        ASSERT_EQ(iter - l_pos.begin(), lset.get_ref(i))
          << "i: " << i << std::endl;
      } else {
        ASSERT_FALSE(lset.get(i))
          << "i: " << i << std::endl;
      }
    }
  }
}

TEST(BitSet, perf)
{
  const int64_t lset_cnt = 400;
  const int64_t pct = 10;
  const int64_t loops_cnt = 10000;

  uint64_t l[8];
  std::vector<int64_t> l_pos;
  BitSet lset;
  ASSERT_EQ(OB_SUCCESS, lset.init(l, lset_cnt));
  uint64_t bitset_time_used = 0;
  uint64_t bins_time_used = 0;
  uint64_t begin_time = 0;
  uint64_t end_time = 0;

  for (int64_t j = 0; j < loops_cnt; ++j) {
    lset.reset();
    l_pos.clear();
    int64_t i = 0;
    while(i < lset_cnt) {
      lset.set(i);
      l_pos.push_back(i);
      i += random() % pct + 1;
    }
    // get ref
    begin_time = ::oceanbase::common::ObTimeUtility::current_time();
    for (i = 0; i < l_pos.size(); ++i) {
      lset.get_ref(l_pos.at(i));
    }
    end_time = ::oceanbase::common::ObTimeUtility::current_time();
    bitset_time_used += end_time - begin_time;

    // binary search
    begin_time = ::oceanbase::common::ObTimeUtility::current_time();
    for (i = 0; i < l_pos.size(); ++i) {
      std::lower_bound(l_pos.begin(), l_pos.end(), l_pos.at(i));
    }
    end_time = ::oceanbase::common::ObTimeUtility::current_time();
    bins_time_used += end_time - begin_time;
  }
  printf("bitset time: %ld us\n", bitset_time_used);
  printf("binary search time: %ld us\n", bins_time_used);

}

} // end namespace blocksstable
} // end namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
