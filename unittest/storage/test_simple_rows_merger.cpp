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

#include "storage/access/ob_simple_rows_merger.h"
#include "lib/container/ob_se_array.h"
#include <gtest/gtest.h>
using namespace oceanbase::storage;

class ObSimpleRowsMergerTest: public ::testing::Test
{
  public:
    ObSimpleRowsMergerTest() {};
    virtual ~ObSimpleRowsMergerTest() {};
    virtual void SetUp() {};
    virtual void TearDown() {};
  private:
    // disallow copy
    ObSimpleRowsMergerTest(const ObSimpleRowsMergerTest &other);
    ObSimpleRowsMergerTest& operator=(const ObSimpleRowsMergerTest &other);
  private:
    // data members
};

struct TestItem
{
  TestItem() = default;
  TestItem(int64_t v) : v_(v), iter_idx_(0), equal_with_next_(false) {}
  TestItem(int64_t v, int64_t idx) : v_(v), iter_idx_(idx), equal_with_next_(false) {}
  TO_STRING_KV(K(v_), K(iter_idx_), K(equal_with_next_));
  int64_t v_;
  int64_t iter_idx_;
  bool equal_with_next_;
};

class TestCompator
{
public:
  int cmp(const TestItem &a, const TestItem &b, int64_t &cmp_ret)
  {
    cmp_ret = 0;
    if (a.v_ < b.v_) {
      cmp_ret = -1;
    } else if (a.v_ > b.v_) {
      cmp_ret = 1;
    }
    return OB_SUCCESS;
  }
};

TEST_F(ObSimpleRowsMergerTest, single)
{
  int ret = 0;
  TestCompator tc;
  ObArenaAllocator allocator;
  ObSimpleRowsMerger<TestItem, TestCompator> merger(tc);
  TestItem data(2021);

  // not init
  ret = merger.push(data);
  ASSERT_EQ(ret, OB_NOT_INIT);
  ret = merger.pop();
  ASSERT_EQ(ret, OB_NOT_INIT);
  ASSERT_EQ(0, merger.count());
  ASSERT_TRUE(merger.is_unique_champion());

  // 0 player
  ret = merger.init(0, allocator);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);

  // init twice
  ret = merger.init(1, allocator);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(merger.is_unique_champion());
  ret = merger.init(1, allocator);
  ASSERT_EQ(ret, OB_INIT_TWICE);
  ASSERT_EQ(0, merger.count());

  // push twice
  ret = merger.push(data);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(1, merger.count());
  ASSERT_TRUE(merger.is_unique_champion());
  ret = merger.push(data);
  ASSERT_EQ(ret, OB_SIZE_OVERFLOW);
  ASSERT_EQ(1, merger.count());

  // top twice
  const TestItem *top = nullptr;
  ASSERT_TRUE(merger.is_unique_champion());
  ret = merger.top(top);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(data.v_, top->v_);
  ASSERT_TRUE(merger.is_unique_champion());
  ret = merger.top(top);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(data.v_, top->v_);
  ASSERT_EQ(1, merger.count());
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(merger.is_unique_champion());
  ret = merger.pop();
  ASSERT_EQ(ret, OB_EMPTY_RESULT);
  ASSERT_EQ(0, merger.count());

  ret = merger.top(top);
  ASSERT_EQ(ret, OB_EMPTY_RESULT);
  ASSERT_EQ(0, merger.count());
}

TEST_F(ObSimpleRowsMergerTest, one_iter)
{
  int ret = 0;
  TestCompator tc;
  ObArenaAllocator allocator;
  ObSimpleRowsMerger<TestItem, TestCompator> merger(tc);
  const int64_t DATA_CNT = 7;
  TestItem data[DATA_CNT] = {4, 3, 2, 1, 5, 7, 6};
  ret = merger.init(DATA_CNT, allocator);
  ASSERT_EQ(ret, OB_SUCCESS);

  for (int64_t i = 0; i < DATA_CNT; ++i) {
    ret = merger.push(data[i]);
    ASSERT_EQ(ret, OB_SUCCESS);
  }

  // {4, 3, 2, 1, 5, 7, 6}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {4, 3, 2, 5, 7, 6}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {4, 3, 5, 7, 6}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {4, 5, 7, 6}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {5, 7, 6}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.push(-1);
  ASSERT_EQ(ret, OB_SUCCESS);
  // {5, 7, 6, -1}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.push(10);
  ASSERT_EQ(ret, OB_SUCCESS);
  // {5, 7, 6, -1, 10}
  ASSERT_TRUE(merger.is_unique_champion());

  // {{10,0}}
  const TestItem *top = nullptr;
  ret = merger.top(top);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(-1, top->v_);
  ASSERT_EQ(5, merger.count());
  ASSERT_TRUE(merger.is_unique_champion());
}

TEST_F(ObSimpleRowsMergerTest, two_iters)
{
  int ret = 0;
  TestCompator tc;
  ObArenaAllocator allocator;
  ObSimpleRowsMerger<TestItem, TestCompator> merger(tc);
  const int64_t DATA_CNT = 8;
  TestItem data[DATA_CNT] = {{1,0}, {1,1}, {2,1}, {2,0}, {3,1}, {3,0}, {5,0}, {4,1}};
  ret = merger.init(DATA_CNT, allocator);
  ASSERT_EQ(ret, OB_SUCCESS);

  for (int64_t i = 0; i < DATA_CNT; ++i) {
    ret = merger.push(data[i]);
    ASSERT_EQ(ret, OB_SUCCESS);
  }

  // {{1,0}, {1,1}, {2,1}, {2,0}, {3,1}, {3,0}, {5,0}, {4,1}}
  ASSERT_FALSE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{1,1}, {2,1}, {2,0}, {3,1}, {3,0}, {5,0}, {4,1}}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{2,1}, {2,0}, {3,1}, {3,0}, {5,0}, {4,1}}
  ASSERT_FALSE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{2,1}, {3,1}, {3,0}, {5,0}, {4,1}}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{3,1}, {3,0}, {5,0}, {4,1}}
  ASSERT_FALSE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{3,1}, {5,0}, {4,1}}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.push({-1,0});
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{-1,0}, {3,1}, {5,0}, {4,1}}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{3,1}, {5,0}, {4,1}}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{5,0}, {4,1}}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{5,0}}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.push({10,0});
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{5,0}, {10,0}}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
   // {{10,0}}
  ASSERT_TRUE(merger.is_unique_champion());

  // {{10,0}}
  const TestItem *top = nullptr;
  ret = merger.top(top);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(10, top->v_);
  ASSERT_EQ(1, merger.count());
  ASSERT_TRUE(merger.is_unique_champion());

  // {}
  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(merger.empty());
}

TEST_F(ObSimpleRowsMergerTest, three_iters)
{
  int ret = 0;
  TestCompator tc;
  ObArenaAllocator allocator;
  ObSimpleRowsMerger<TestItem, TestCompator> merger(tc);
  const int64_t DATA_CNT = 8;
  TestItem data[DATA_CNT] = {{1,0}, {1,1}, {1,2}, {2,2}, {2,0}, {3,1}, {4,0}, {4,2}};
  ret = merger.init(DATA_CNT, allocator);
  ASSERT_EQ(ret, OB_SUCCESS);

  for (int64_t i = 0; i < DATA_CNT; ++i) {
    ret = merger.push(data[i]);
    ASSERT_EQ(ret, OB_SUCCESS);
  }

  // {{1,0}, {1,1}, {1,2}, {2,2}, {2,0}, {3,1}, {4,0}, {4,2}}
  ASSERT_FALSE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{1,1}, {1,2}, {2,2}, {2,0}, {3,1}, {4,0}, {4,2}}
  ASSERT_FALSE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{1,2}, {2,2}, {2,0}, {3,1}, {4,0}, {4,2}}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{2,2}, {2,0}, {3,1}, {4,0}, {4,2}}
  ASSERT_FALSE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{2,2}, {3,1}, {4,0}, {4,2}}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{3,1}, {4,0}, {4,2}}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.push({3,0});
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{3,1}, {4,0}, {4,2}, {3,0}}
  ASSERT_FALSE(merger.is_unique_champion());

  ret = merger.push({3,2});
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{3,1}, {4,0}, {4,2}, {3,0}, {3,2}}
  ASSERT_FALSE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{3,1}, {4,0}, {4,2}, {3,2}}
  ASSERT_FALSE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{4,0}, {4,2}, {3,2}}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{4,0}, {4,2}}
  ASSERT_FALSE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{4,2}}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.push({10,2});
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{4,2}, {10,2}}
  ASSERT_TRUE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
   // {{10,2}}
  ASSERT_TRUE(merger.is_unique_champion());

  // {{10,2}}
  const TestItem *top = nullptr;
  ret = merger.top(top);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(10, top->v_);
  ASSERT_EQ(1, merger.count());
  ASSERT_TRUE(merger.is_unique_champion());

  // {}
  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(merger.empty());
}

TEST_F(ObSimpleRowsMergerTest, reset_range)
{
  int ret = 0;
  TestCompator tc;
  ObArenaAllocator allocator;
  ObSimpleRowsMerger<TestItem, TestCompator> merger(tc);
  const int64_t DATA_CNT = 5;
  TestItem data[DATA_CNT] = {{1,0}, {1,1}, {1,2}, {2,2}, {2,0}};
  ret = merger.init(DATA_CNT, allocator);
  ASSERT_EQ(ret, OB_SUCCESS);

  for (int64_t i = 0; i < DATA_CNT; ++i) {
    ret = merger.push(data[i]);
    ASSERT_EQ(ret, OB_SUCCESS);
  }

  // {{1,0}, {1,1}, {1,2}, {2,0}, {2,2}}
  ASSERT_FALSE(merger.is_unique_champion());

  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  // {{1,1}, {1,2}, {2,2}, {2,0}}
  ASSERT_FALSE(merger.is_unique_champion());

  int64_t gap_idx = 1;
  TestItem gap_item(3,1);
  const TestItem *top_item;
  TestItem items[10];
  int64_t remain_item = 0;
  while(OB_SUCC(ret) && !merger.empty()) {
    if (OB_FAIL(merger.top(top_item))) {
      STORAGE_LOG(WARN, "get loser tree top item fail", K(ret), K(merger));
    } else if (nullptr == top_item) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "item or row is null", K(ret), KP(top_item));
    } else {
      int64_t tree_ret = 0;
      ASSERT_EQ(tc.cmp(gap_item, *top_item, tree_ret), OB_SUCCESS);
      if (top_item->iter_idx_ < gap_idx || tree_ret < 0) {
        items[remain_item++] = *top_item;
        // must set to false if using simple row merger
        items[remain_item - 1].equal_with_next_ = false;
      }
      if (OB_FAIL(merger.pop())) {
        STORAGE_LOG(WARN, "pop loser tree fail", K(ret), K(merger));
      }
    }
  }
  ASSERT_EQ(ret, OB_SUCCESS);
  for (int64_t i = remain_item - 1; i >= 0 ; --i) {
    ret = merger.push(items[i]);
    ASSERT_EQ(ret, OB_SUCCESS);
  }
  // {{2,0}}
  ASSERT_EQ(1, merger.count());
  ASSERT_TRUE(merger.is_unique_champion());

  const TestItem *top = nullptr;
  ret = merger.top(top);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(2, top->v_);
  ASSERT_EQ(0, top->iter_idx_);
  ASSERT_TRUE(merger.is_unique_champion());

  // {}
  ret = merger.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(merger.empty());
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
