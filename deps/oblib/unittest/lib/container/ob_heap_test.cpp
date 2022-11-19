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

#include "lib/container/ob_heap.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;

class TestComp
{
public:
  int get_error_code() { return OB_SUCCESS; }
  bool operator()(int64_t a, int64_t b)
  {
    return a < b ? true : false;
  }
};

class ObHeapTest: public ::testing::Test
{
  public:
  ObHeapTest();
    virtual ~ObHeapTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObHeapTest(const ObHeapTest &other);
    ObHeapTest& operator=(const ObHeapTest &other);
  private:
    // data members
};

ObHeapTest::ObHeapTest()
{
}

ObHeapTest::~ObHeapTest()
{
}

void ObHeapTest::SetUp()
{
}

void ObHeapTest::TearDown()
{
}


TEST_F(ObHeapTest, basic_test)
{
  TestComp tc;
  ObBinaryHeap<int64_t, TestComp, 30> heap(tc, NULL);
  heap.push(1);
  heap.push(9);
  heap.push(8);
  heap.push(7);
  heap.push(11);
  heap.push(6);
  heap.push(4);
  ASSERT_EQ(11, heap.top());
  int ret = heap.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(9, heap.top());
  ret = heap.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(8, heap.top());
  ret = heap.replace_top(50);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(50, heap.top());
  ret = heap.replace_top(23);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(23, heap.top());
  ASSERT_EQ(5, heap.count());
}

TEST_F(ObHeapTest, bound_test)
{
  TestComp tc;
  int ret = 0;
  ObArenaAllocator buf;
  ObBinaryHeap<int64_t, TestComp, 5> heap(tc, &buf);
  ret = heap.push(1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = heap.push(9);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = heap.push(8);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = heap.push(7);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = heap.push(11);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = heap.push(6);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = heap.push(4);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(11, heap.top());
  ret = heap.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(9, heap.top());
  ret = heap.pop();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(8, heap.top());
  ret = heap.replace_top(50);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(50, heap.top());
  ret = heap.replace_top(23);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(23, heap.top());
  ASSERT_EQ(5, heap.count());
}

class Element
{
public:
  explicit Element(int64_t val) : val_(val), pos_(-1) {}
  TO_STRING_KV(K(val_), K(pos_));
public:
  int64_t val_;
  int64_t pos_;
};

class CompElement
{
public:
  int get_error_code() { return OB_SUCCESS; }
  bool operator()(Element *a, Element *b)
  {
    return a->val_ < b->val_ ? true : false;
  }
};


TEST_F(ObHeapTest, remove_element)
{
  int ret = 0;
  CompElement tc;
  ObArenaAllocator buf;
  ObRemovableHeap<Element *, CompElement, &Element::pos_, 5> heap(tc, &buf);
  Element e1(1);
  Element e9(9);
  Element e8(8);
  Element e7(7);
  Element e11(11);
  Element e6(6);
  Element e4(4);
  ASSERT_EQ(OB_SUCCESS, heap.push(&e1));
  ASSERT_EQ(OB_SUCCESS, heap.push(&e9));
  ASSERT_EQ(1, e1.pos_);
  ASSERT_EQ(OB_SUCCESS, heap.push(&e8));
  ASSERT_EQ(OB_SUCCESS, heap.push(&e7));
  ASSERT_EQ(OB_SUCCESS, heap.push(&e11));
  ASSERT_EQ(OB_SUCCESS, heap.push(&e6));
  ASSERT_EQ(OB_SUCCESS, heap.push(&e4));
  LIB_LOG(INFO, "check heap", K(heap));
  ASSERT_EQ(11, heap.top()->val_);
  ASSERT_EQ(0, heap.top()->pos_);
  ASSERT_EQ(OB_SUCCESS, heap.pop());
  ASSERT_EQ(9, heap.top()->val_);
  ASSERT_EQ(0, heap.top()->pos_);
  ASSERT_EQ(OB_SUCCESS, heap.remove_by_index(0));
  ASSERT_EQ(8, heap.top()->val_);
  ASSERT_EQ(OB_SUCCESS, heap.remove(&e6));
  LIB_LOG(INFO, "check heap", K(heap));
}

TEST_F(ObHeapTest, remove_random)
{
  int ret = 0;
  CompElement compare_functor;
  ObArenaAllocator arena;
  ObRemovableHeap<Element *, CompElement, &Element::pos_, 5> heap(compare_functor, &arena);
  const int64_t test_count = 10000L * 10L;
  char *buf = (char *)arena.alloc(sizeof(Element) * test_count);
  ASSERT_TRUE(nullptr != buf);
  for (int64_t i = 0; OB_SUCC(ret) && i < test_count; ++i) {
    Element *item = new (buf + sizeof(Element) * i) Element(ObRandom::rand(0, test_count));
    ASSERT_EQ(OB_SUCCESS, heap.push(item));
  }
  int64_t last_val = INT64_MAX;
  for (int64_t i = 0; OB_SUCC(ret) && i < test_count; ++i) {
    ASSERT_GE(last_val, heap.top()->val_);
    last_val = heap.top()->val_;
    if (ObRandom::rand(0, test_count) % 3 == 0) {
      const int64_t index = ObRandom::rand(0, heap.count() - 1);
      ASSERT_EQ(heap.at(index)->pos_, index);
      ASSERT_GE(last_val, heap.at(index)->val_);
      ASSERT_EQ(OB_SUCCESS, heap.remove_by_index(index));
    } else {
      ASSERT_EQ(OB_SUCCESS, heap.pop());
    }
  }
  ASSERT_EQ(0, heap.count());
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("heap_test.log", true);
  return RUN_ALL_TESTS();
}
