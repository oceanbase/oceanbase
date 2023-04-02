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

#include "lib/container/ob_loser_tree.h"
#include "lib/container/ob_se_array.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;

class ObLoserTreeTest: public ::testing::Test
{
  public:
    ObLoserTreeTest() {};
    virtual ~ObLoserTreeTest() {};
    virtual void SetUp() {};
    virtual void TearDown() {};
  private:
    // disallow copy
    ObLoserTreeTest(const ObLoserTreeTest &other);
    ObLoserTreeTest& operator=(const ObLoserTreeTest &other);
  private:
    // data members
};

class TestMaxComp
{
public:
  int cmp(const int64_t &a, const int64_t &b, int64_t &cmp_ret)
  {
    int ret = OB_SUCCESS;
    cmp_ret = 0;
    if (a < b) {
      cmp_ret = 1;
    } else if (a > b) {
      cmp_ret = -1;
    }
    return ret;
  }
};

TEST_F(ObLoserTreeTest, single)
{
  int ret = 0;
  TestMaxComp tc;
  ObArenaAllocator allocator;
  ObLoserTree<int64_t, TestMaxComp, 8> tree(tc);
  int64_t data = 2019;

  // not init
  ret = tree.push(data);
  ASSERT_EQ(ret, OB_NOT_INIT);
  ret = tree.pop();
  ASSERT_EQ(ret, OB_NOT_INIT);
  ASSERT_EQ(0, tree.count());
  ASSERT_TRUE(tree.is_unique_champion());

  // 0 player
  ret = tree.init(0, allocator);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);

  // init twice
  ret = tree.init(1, allocator);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(tree.is_unique_champion());
  ret = tree.init(1, allocator);
  ASSERT_EQ(ret, OB_INIT_TWICE);
  ASSERT_EQ(0, tree.count());

  // push twice
  ret = tree.push(data);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(1, tree.count());
  ASSERT_TRUE(tree.is_unique_champion());
  ret = tree.push(data);
  ASSERT_EQ(ret, OB_SIZE_OVERFLOW);
  ASSERT_EQ(1, tree.count());

  // not rebuild
  const int64_t *top = nullptr;
  ret = tree.top(top);
  ASSERT_EQ(ret, OB_ERR_UNEXPECTED);
  ret = tree.pop();
  ASSERT_EQ(ret, OB_ERR_UNEXPECTED);
  ASSERT_EQ(1, tree.count());

  // call rebuild and top twice
  ret = tree.rebuild();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(tree.is_unique_champion());
  ret = tree.rebuild();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(tree.is_unique_champion());
  ret = tree.top(top);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(data, *top);
  ASSERT_TRUE(tree.is_unique_champion());
  ret = tree.top(top);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(data, *top);
  ASSERT_EQ(1, tree.count());
  ASSERT_TRUE(tree.is_unique_champion());

  ret = tree.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(tree.is_unique_champion());
  ret = tree.pop();
  ASSERT_EQ(ret, OB_EMPTY_RESULT);
  ASSERT_EQ(0, tree.count());

  ret = tree.top(top);
  ASSERT_EQ(ret, OB_EMPTY_RESULT);
  ASSERT_EQ(0, tree.count());
}

TEST_F(ObLoserTreeTest, multiple_players)
{
  int ret = 0;
  TestMaxComp tc;
  ObArenaAllocator allocator;
  ObLoserTree<int64_t, TestMaxComp, 8> tree(tc);
  const int64_t DATA_CNT = 8;
  int64_t data[DATA_CNT] = {1, 2, 3, 4, 5, 6, 7, 8};

  // different player
  const int64_t *top;
  for (int64_t count = 1; count <= DATA_CNT; ++count) {
    tree.reset();
    ret = tree.init(count, allocator);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_TRUE(tree.empty());
    for (int64_t i = 0; i < count; ++i) {
      ret = tree.push(data[i]);
      ASSERT_EQ(ret, OB_SUCCESS);
      ASSERT_EQ(i + 1, tree.count());
    }
    ret = tree.rebuild();
    ASSERT_EQ(ret, OB_SUCCESS);
    ret = tree.top(top);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(count, *top);
    ASSERT_EQ(count, tree.count());
    ASSERT_FALSE(tree.empty());
  }
}

TEST_F(ObLoserTreeTest, basic)
{
  int ret = 0;
  TestMaxComp tc;
  ObArenaAllocator allocator;
  ObLoserTree<int64_t, TestMaxComp, 8> tree(tc);
  const int64_t DATA_CNT = 7;
  int64_t data[DATA_CNT] = {0, 8, 12, 3, 14, 7, 5};
  ret = tree.init(DATA_CNT, allocator);
  ASSERT_EQ(ret, OB_SUCCESS);

  for (int64_t i = 0; i < DATA_CNT; ++i) {
    ret = tree.push(data[i]);
    ASSERT_EQ(ret, OB_SUCCESS);
  }

  ret = tree.rebuild();
  ASSERT_EQ(ret, OB_SUCCESS);

  const int64_t *top = nullptr;
  ret = tree.top(top);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(*top, 14);
  ASSERT_EQ(DATA_CNT, tree.count());

  // {0, 8, 12, 3, null, 7, 5}
  ret = tree.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = tree.top(top);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(*top, 12);
  ASSERT_EQ(DATA_CNT - 1, tree.count());

  // {0, 8, null, 3, null, 7, 5}
  ret = tree.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = tree.top(top);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(*top, 8);
  ASSERT_EQ(DATA_CNT - 2, tree.count());

  // {0, null, null, 3, null, 7, 5}
  ret = tree.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = tree.top(top);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(*top, 7);
  ASSERT_EQ(DATA_CNT - 3, tree.count());

  // {0, 5, null, 3, null, 7, 5}
  ret = tree.push(-1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(DATA_CNT - 2, tree.count());
  ret = tree.rebuild();
  ASSERT_EQ(ret, OB_SUCCESS);

  ret = tree.top(top);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(*top, 7);
  ASSERT_EQ(DATA_CNT - 2, tree.count());

  // {0, 5, null, 3, 18, 7, 5}
  ret = tree.push(18);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(DATA_CNT - 1, tree.count());
  ret = tree.rebuild();
  ASSERT_EQ(ret, OB_SUCCESS);

  ret = tree.top(top);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(*top, 18);
  ASSERT_EQ(DATA_CNT - 1, tree.count());
}

TEST_F(ObLoserTreeTest, only_one_element)
{
  int ret = 0;
  TestMaxComp tc;
  ObArenaAllocator allocator;
  ObLoserTree<int64_t, TestMaxComp, 8> tree(tc);
  const int64_t DATA_CNT = 7;
  int64_t data[DATA_CNT] = {5, 12, 4, 3, 12, 0, 5};
  ret = tree.init(DATA_CNT, allocator);
  ASSERT_EQ(ret, OB_SUCCESS);

  for (int64_t i = 0; i < DATA_CNT; ++i) {
    ret = tree.push(data[i]);
    ASSERT_EQ(ret, OB_SUCCESS);
  }
  ret = tree.rebuild();
  ASSERT_EQ(ret, OB_SUCCESS);

  // only 0 is left
  for (int64_t i = 0; i < DATA_CNT - 1; ++i) {
    ret = tree.pop();
    ASSERT_EQ(ret, OB_SUCCESS);
  }

  const int64_t *top = nullptr;
  ret = tree.top(top);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(0, *top);

  ret = tree.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = tree.top(top);
  ASSERT_EQ(ret, OB_EMPTY_RESULT);

  // only 3 is left
  ret = tree.push(3);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = tree.rebuild();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(1, tree.count());
  ret = tree.pop();
  ASSERT_EQ(ret, OB_SUCCESS);

  // only 99 is left
  ret = tree.push(99);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = tree.rebuild();
  ret = tree.top(top);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(99, *top);
}

TEST_F(ObLoserTreeTest, unique_champion)
{
  int ret = 0;
  TestMaxComp tc;
  ObArenaAllocator allocator;
  ObLoserTree<int64_t, TestMaxComp, 8> tree(tc);
  const int64_t DATA_CNT = 7;
  int64_t data[DATA_CNT] = {4, 4, 7, 3, 12, 5, 12};
  ret = tree.init(DATA_CNT, allocator);
  ASSERT_EQ(ret, OB_SUCCESS);

  for (int64_t i = 0; i < DATA_CNT; ++i) {
    ret = tree.push(data[i]);
    ASSERT_EQ(ret, OB_SUCCESS);
  }
  ret = tree.rebuild();
  ASSERT_EQ(ret, OB_SUCCESS);

  // {4, 4, 7, 3, 12, 5, 12}
  ASSERT_FALSE(tree.is_unique_champion());

  // {4, 4, 7, 3, null, 5, 12}
  ret = tree.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(tree.is_unique_champion());

  // {4, 4, 7, 3, null, 5, null}
  ret = tree.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(tree.is_unique_champion());

  // {4, 4, null, 3, null, 5, null}
  ret = tree.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(tree.is_unique_champion());

  // {4, 4, null, 3, null, null, null}
  ret = tree.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(tree.is_unique_champion());

  // {4, 4, null, 3, -1, null, null}
  ret = tree.push(-1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = tree.rebuild();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(tree.is_unique_champion());

  // {4, 4, 10, 3, -1, null, null}
  ret = tree.push(10);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = tree.rebuild();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(tree.is_unique_champion());

  // {4, 4, 10, 3, -1, 10, null}
  ret = tree.push(10);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = tree.rebuild();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(tree.is_unique_champion());
}
int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
