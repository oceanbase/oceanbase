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
#define private public

#include "lib/resource/ob_resource_limiter.h"

using namespace oceanbase::lib;

TEST(TestResourceLimiter, basic)
{
  int64_t max = 1LL<<30;
  ObResourceLimiter root(max, 0);
  ObResourceLimiter l1(max, 0);
  ObResourceLimiter l2(max, 0);
  ASSERT_EQ(NULL, root.child_);
  ASSERT_EQ(NULL, l1.parent_);
  ASSERT_EQ(NULL, l2.parent_);
  root.add_child(&l1);
  ASSERT_EQ(&l1, root.child_);
  ASSERT_EQ(&root, l1.parent_);
  root.add_child(&l2);
  ASSERT_EQ(&l2, root.child_);
  ASSERT_EQ(&root, l2.parent_);
  root.del_child(&l2);
  root.del_child(&l1);
  ASSERT_EQ(NULL, root.child_);
  ASSERT_EQ(NULL, l1.parent_);
  ASSERT_EQ(NULL, l2.parent_);

  int64_t size = 1<<10;
  root.add_child(&l1);
  ASSERT_EQ(0, root.hold_);
  l1.acquire(size);
  ASSERT_EQ(size, root.hold_);
  root.del_child(&l1);
  ASSERT_EQ(0, root.hold_);
  root.add_child(&l1);
  ASSERT_EQ(size, root.hold_);
}
TEST(TestResourceLimiter, max)
{
  // create resource limiter tree.
  int64_t max = 1LL<<30;
  int64_t max_1 = 256<<20;
  int64_t max_2 = INT64_MAX;
  ObResourceLimiter root(max, 0);
  ObResourceLimiter l1(max_1, 0);
  ObResourceLimiter l2(max_2, 0);
  root.add_child(&l1);
  root.add_child(&l2);
  ASSERT_EQ(l1.hold_, 0);
  ASSERT_EQ(l2.hold_, 0);
  ASSERT_EQ(root.hold_, 0);
  // acquire succeed when limiter.hold_ <= limiter.max_.
  ASSERT_EQ(false, l1.acquire(max_1 + 1));
  ASSERT_EQ(true, l1.acquire(max_1));
  ASSERT_EQ(l1.hold_, max_1);
  ASSERT_EQ(root.hold_, max_1);
  ASSERT_EQ(false, l2.acquire(max - max_1 + 1));
  ASSERT_EQ(true, l2.acquire(max - max_1));

  ASSERT_EQ(l1.hold_, max_1);
  ASSERT_EQ(l2.hold_, max - max_1);
  ASSERT_EQ(root.hold_, max);
}

TEST(TestResourceLimiter, min)
{
  // create resource limiter tree.
  int64_t max = 1LL<<30;
  int64_t min_1 = 256<<20;
  int64_t min_2 = max - min_1 + 1;
  ObResourceLimiter root(max, 0);
  ObResourceLimiter l1(INT64_MAX, min_1);
  ObResourceLimiter l2(INT64_MAX, min_2);
  root.add_child(&l1);
  root.add_child(&l2);
  // l2 can keep reservered resource when min_2 <= max - min_1.
  ASSERT_EQ(l1.hold_, min_1);
  ASSERT_EQ(l2.hold_, 0);
  min_2 = max - min_1;
  l2.set_min(min_2);
  ASSERT_EQ(l2.hold_, min_2);
  // the reservered resource cannot be hold by other limiter
  ASSERT_EQ(true, l1.acquire(max-min_2));
  ASSERT_EQ(false, l1.acquire(1));
}

TEST(TestResourceLimiter, acquire)
{
  // create resource limiter tree.
  int64_t max = 1LL<<30;
  int64_t min_1 = 256<<20;
  int64_t min_2 = 128<<20;
  ObResourceLimiter root(max, 0);
  ObResourceLimiter l1(INT64_MAX, min_1);
  ObResourceLimiter l2(INT64_MAX, min_2);
  root.add_child(&l1);
  root.add_child(&l2);

  // prioritize applying for resources from cache
  int64_t hold = min_1;
  int64_t cache = min_1;
  ASSERT_EQ(hold, l1.hold_);
  ASSERT_EQ(cache, l1.cache_);
  ASSERT_EQ(true, l1.acquire(1024));
  cache -= 1024;
  ASSERT_EQ(cache, l1.cache_);
  // applying for resources from parent when cache is not enough
  ASSERT_EQ(true, l1.acquire(cache + 1));
  hold += cache + 1;
  ASSERT_EQ(hold, l1.hold_);
  ASSERT_EQ(cache, l1.cache_);

  // prioritize releasing resources to parent
  int64_t p_hold = root.hold_;
  ASSERT_EQ(true, l1.acquire(-1024));
  hold -= 1024;
  p_hold -= 1024;
  ASSERT_EQ(hold, l1.hold_);
  ASSERT_EQ(cache, l1.cache_);
  ASSERT_EQ(p_hold, root.hold_);
  // releasing resources to cache when hold will less than the reserver
  ASSERT_EQ(true, l1.acquire(-(hold - min_1 + 1)));
  cache += (hold - min_1 + 1);
  ASSERT_EQ(hold, l1.hold_);
  ASSERT_EQ(cache, l1.cache_);
  ASSERT_EQ(p_hold, root.hold_);
}

int main(int argc, char *argv[])
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}