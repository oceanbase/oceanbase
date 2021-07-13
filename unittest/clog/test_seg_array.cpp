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
#include "lib/container/ob_seg_array.h"
#include "clog/ob_file_id_cache.h"
using namespace oceanbase::common;
using namespace oceanbase::clog;

ObSmallAllocator small_allocator;

void init_env()
{
  const int64_t seg_size = ObSegArray<Log2File, ObFileIdList::SEG_STEP, ObFileIdList::SEG_COUNT>::get_seg_size();
  small_allocator.init(seg_size,
      ObModIds::OB_CSR_FILE_ID_CACHE_FILE_ITEM,
      OB_SERVER_TENANT_ID,
      OB_MALLOC_BIG_BLOCK_SIZE,
      128,
      4L * 1024L * 1024L * 1024L);
}

class TestSegArray : public ::testing::Test {
public:
  TestSegArray()
  {}
  virtual ~TestSegArray()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(TestSegArray);
};

struct Item {
  int x;
  int y;

  Item() : x(0), y(0)
  {}
  bool operator<=(const Item& that) const
  {
    return x <= that.x;
  }
  TO_STRING_KV(K(x), K(y));
};

struct ItemLEFunctor {
  bool operator()(const Item& a, const Item& b) const
  {
    return a <= b;
  }
};

struct ReverseForEachFunctor {
  int64_t cnt_;
  int operator()(const Item& item)
  {
    int ret = OB_SUCCESS;
    LIB_LOG(INFO, "print", K(item), K(cnt_));
    cnt_++;
    return ret;
  }
};

TEST(TestSegArray, test_fix_rq)
{
  ObFixedRingDeque<Item, 20> q;
  for (int i = 0; i < 10; i++) {
    Item item;
    item.x = i * 10;
    item.y = i * 10;
    ASSERT_EQ(OB_SUCCESS, q.push_back(item));
  }

  q.debug_print();

  Item target;
  target.x = -5;
  target.y = -5;
  Item prev;
  Item next;
  ItemLEFunctor le_functor;
  ASSERT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, q.search_boundary(target, prev, next, le_functor));
  EXPECT_EQ(0, next.x);
  EXPECT_EQ(0, next.y);

  for (int i = 0; i < 10; i++) {
    Item target;
    target.x = i * 10 + 5;
    target.y = i * 10 + 5;
    Item prev;
    Item next;
    int expect_err = i < 9 ? OB_SUCCESS : OB_ERR_OUT_OF_UPPER_BOUND;
    int search_err = q.search_boundary(target, prev, next, le_functor);
    ASSERT_EQ(expect_err, search_err);
    ASSERT_TRUE(prev.x == target.x - 5);
    if (i < 9) {
      ASSERT_TRUE(next.x == target.x + 5);
    }
    LIB_LOG(INFO, "search", K(target), K(prev), K(next), K(search_err));
  }

  for (int i = 0; i < 10; i++) {
    Item item;
    ASSERT_EQ(OB_SUCCESS, q.top_front(item));
    ASSERT_EQ(item.x, i * 10);
    LIB_LOG(INFO, "top_front", K(item));
    ASSERT_EQ(OB_SUCCESS, q.pop_front(item));
    ASSERT_EQ(item.x, i * 10);
    LIB_LOG(INFO, "pop_front", K(item));
  }

  do {
    Item item;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, q.top_front(item));
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, q.pop_front(item));
  } while (0);

  for (int i = 0; i < 10; i++) {
    Item item;
    item.x = (9 - i) * 10;
    item.y = (9 - i) * 10;
    ASSERT_EQ(OB_SUCCESS, q.push_front(item));
  }

  for (int i = 0; i < 10; i++) {
    Item target;
    target.x = i * 10 + 5;
    target.y = i * 10 + 5;
    Item prev;
    Item next;
    int expect_err = i < 9 ? OB_SUCCESS : OB_ERR_OUT_OF_UPPER_BOUND;
    int search_err = q.search_boundary(target, prev, next, le_functor);
    ASSERT_EQ(expect_err, search_err);
    ASSERT_TRUE(prev.x == target.x - 5);
    if (i < 9) {
      ASSERT_TRUE(next.x == target.x + 5);
    }
    LIB_LOG(INFO, "search", K(target), K(prev), K(next), K(search_err));
  }

  for (int i = 0; i < 10; i++) {
    Item item;
    ASSERT_EQ(OB_SUCCESS, q.top_back(item));
    ASSERT_EQ(item.x, (9 - i) * 10);
    LIB_LOG(INFO, "top_back", K(item));
    ASSERT_EQ(OB_SUCCESS, q.pop_back(item));
    ASSERT_EQ(item.x, (9 - i) * 10);
    LIB_LOG(INFO, "pop_back", K(item));
  }

  do {
    Item item;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, q.top_front(item));
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, q.pop_front(item));
  } while (0);
}

TEST(TestSegArray, test_fix_rq_wrapped)
{
  int ret = OB_SUCCESS;
  ObFixedRingDeque<Item, 21> q;
  for (int i = 0; i < 15; i++) {
    Item item;
    item.x = i * 2;
    item.y = i * 2;
    ASSERT_EQ(OB_SUCCESS, q.push_back(item));
  }
  q.debug_print();

  for (int i = 0; i < 10; i++) {
    Item item;
    item.x = -1;
    item.y = -1;
    ASSERT_EQ(OB_SUCCESS, q.pop_front(item));
    LIB_LOG(INFO, "poped item", K(item));
  }
  q.debug_print();

  for (int i = 15; i < 20; i++) {
    Item item;
    item.x = i * 2;
    item.y = i * 2;
    ASSERT_EQ(OB_SUCCESS, q.push_back(item));
  }
  q.debug_print();

  for (int i = 20; i < 25; i++) {
    Item item;
    item.x = i * 2;
    item.y = i * 2;
    ASSERT_EQ(OB_SUCCESS, q.push_back(item));
  }
  q.debug_print();

  Item target;
  target.x = 10;
  target.y = 10;
  Item prev;
  Item next;
  ItemLEFunctor le_functor;
  ASSERT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, q.search_boundary(target, prev, next, le_functor));
  EXPECT_EQ(20, next.x);
  EXPECT_EQ(20, next.y);

  for (int i = 20; i < 50; i++) {
    Item target;
    target.x = i;
    target.y = i;
    Item prev;
    Item next;
    int expect_err = i < 48 ? OB_SUCCESS : OB_ERR_OUT_OF_UPPER_BOUND;
    ret = q.search_boundary(target, prev, next, le_functor);
    ASSERT_EQ(expect_err, ret);
    EXPECT_EQ((i / 2) * 2, prev.x);
    if (i < 48) {
      EXPECT_EQ((i / 2 + 1) * 2, next.x);
    }

    LIB_LOG(INFO, "search", K(ret), K(i), K(target), K(prev), K(next));
  }
}

TEST(TestSegArray, test_seg_arr)
{
  int ret = OB_SUCCESS;
  ObSegArray<Item, 10, 8> seg_array;
  ASSERT_EQ(OB_SUCCESS, seg_array.init(&small_allocator));

  const int count = 65;
  for (int i = 0; i < count; i++) {
    Item item;
    item.x = i * 10;
    item.y = i * 10;
    ASSERT_EQ(OB_SUCCESS, seg_array.push_back(item));
    LIB_LOG(INFO, "push_back success", K(item));
  }
  LIB_LOG(INFO, "print seg_array");
  seg_array.debug_print();

  for (int i = 5; i < 650; i = i + 10) {
    Item target;
    target.x = i;
    target.y = i;
    Item prev;
    Item next;
    int expect_err = i < 640 ? OB_SUCCESS : OB_ERR_OUT_OF_UPPER_BOUND;
    ret = seg_array.search_boundary(target, prev, next);
    LIB_LOG(INFO, "seg_array search_boundary: ", K(ret), K(target), K(prev), K(next));
    EXPECT_EQ(expect_err, ret);
    EXPECT_EQ((i / 10) * 10, prev.x);
    if (i < 640) {
      EXPECT_EQ((i / 10 + 1) * 10, next.x);
    }
    ASSERT_TRUE(prev.x + 5 == target.x);
  }

  Item target;
  target.x = -100;
  target.y = -100;
  Item prev;
  Item next;
  ret = seg_array.search_boundary(target, prev, next);
  LIB_LOG(INFO, "search_boundary: ", K(ret), K(target), K(prev), K(next));
  ASSERT_TRUE(OB_ERR_OUT_OF_LOWER_BOUND == ret);
  EXPECT_EQ(0, next.x);

  LIB_LOG(INFO, "seg_array test pop_front and top_front");
  for (int i = 0; i < count; i++) {
    Item item;
    ASSERT_EQ(OB_SUCCESS, seg_array.top_front(item));
    ASSERT_EQ(item.x, i * 10);
    LIB_LOG(INFO, "seg_array top_front", K(item), K(i));
    ASSERT_EQ(OB_SUCCESS, seg_array.pop_front(item));
    ASSERT_EQ(item.x, i * 10);
    LIB_LOG(INFO, "seg_array pop_front", K(item), K(i));
  }

  seg_array.debug_print();

  LIB_LOG(INFO, "test push_front");
  for (int i = 0; i < count; i++) {
    Item item;
    item.x = (count - 1 - i) * 10;
    item.y = (count - 1 - i) * 10;
    ASSERT_EQ(OB_SUCCESS, seg_array.push_front(item));
    LIB_LOG(INFO, "push_front", K(item));
  }

  seg_array.debug_print();

  for (int i = 5; i < 650; i = i + 10) {
    Item target;
    target.x = i;
    target.y = i;
    Item prev;
    Item next;
    int expect_err = i < 640 ? OB_SUCCESS : OB_ERR_OUT_OF_UPPER_BOUND;
    ret = seg_array.search_boundary(target, prev, next);
    LIB_LOG(INFO, "seg_array search_boundary: ", K(ret), K(target), K(prev), K(next));
    EXPECT_EQ(expect_err, ret);
    EXPECT_EQ((i / 10) * 10, prev.x);
    if (i < 640) {
      EXPECT_EQ((i / 10 + 1) * 10, next.x);
    }
    ASSERT_TRUE(prev.x + 5 == target.x);
  }

  seg_array.debug_print();

  for (int i = 0; i < count; i++) {
    Item item;
    ASSERT_EQ(OB_SUCCESS, seg_array.top_back(item));
    LIB_LOG(INFO, "top_back", K(item), K(i));
    ASSERT_EQ(item.x, (count - 1 - i) * 10);
    ASSERT_EQ(OB_SUCCESS, seg_array.pop_back(item));
    LIB_LOG(INFO, "pop_back", K(item), K(i));
    ASSERT_EQ(item.x, (count - 1 - i) * 10);
  }
}

TEST(TestSegArray, test_seg_arr_pop)
{
  ObSegArray<Item, 5, 10> seg_array;
  ASSERT_EQ(OB_SUCCESS, seg_array.init(&small_allocator));
  for (int i = 0; i < 45; i++) {
    Item item;
    item.x = i * 2;
    item.y = i * 2;
    ASSERT_EQ(OB_SUCCESS, seg_array.push_back(item));
  }
  seg_array.debug_print();

  for (int i = 0; i < 45; i++) {
    Item poped;
    ASSERT_EQ(OB_SUCCESS, seg_array.pop_front(poped));
  }
  seg_array.debug_print();

  for (int i = 0; i < 45; i++) {
    Item item;
    item.x = i * 2;
    item.y = i * 2;
    ASSERT_EQ(OB_SUCCESS, seg_array.push_back(item));
  }
  seg_array.debug_print();

  for (int i = 0; i < 45; i++) {
    Item poped;
    ASSERT_EQ(OB_SUCCESS, seg_array.pop_front(poped));
  }
  seg_array.debug_print();
}

TEST(TestSegArray, test_seg_arr_tail_size)
{
  ObSegArray<Item, 10, 8> seg_array;
  ASSERT_EQ(OB_SUCCESS, seg_array.init(&small_allocator));

  int count = 5;
  for (int i = 0; i < count; i++) {
    Item item;
    item.x = i * 2;
    item.y = i * 2;
    ASSERT_EQ(OB_SUCCESS, seg_array.push_back(item));
  }
  LIB_LOG(INFO, "print seg_array");
  seg_array.debug_print();
}

TEST(TestSegArray, test_seg_arr_destroy)
{
  ObSegArray<Item, 10, 8> seg_array;
  ASSERT_EQ(OB_SUCCESS, seg_array.init(&small_allocator));

  int count = 5;
  for (int i = 0; i < count; i++) {
    Item item;
    item.x = i * 2;
    item.y = i * 2;
    ASSERT_EQ(OB_SUCCESS, seg_array.push_back(item));
  }
  LIB_LOG(INFO, "print seg_array");
  seg_array.debug_print();
  seg_array.destroy();
}

TEST(TestSegArray, test_reverse_for_each)
{
  ObSegArray<Item, 10, 8> seg_array;
  ASSERT_EQ(OB_SUCCESS, seg_array.init(&small_allocator));

  int count = 50;
  for (int i = 0; i < count; i++) {
    Item item;
    item.x = i;
    item.y = i;
    ASSERT_EQ(OB_SUCCESS, seg_array.push_back(item));
  }
  ReverseForEachFunctor fn;
  fn.cnt_ = 0;
  ASSERT_EQ(OB_SUCCESS, seg_array.reverse_for_each(fn));
  ASSERT_EQ(50, fn.cnt_);

  count = 15;
  fn.cnt_ = 0;
  for (int i = 0; i < count; i++) {
    Item item;
    ASSERT_EQ(OB_SUCCESS, seg_array.pop_front(item));
  }
  ASSERT_EQ(OB_SUCCESS, seg_array.reverse_for_each(fn));
  ASSERT_EQ(35, fn.cnt_);
}

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_seg_array.log", true, true);
  init_env();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
