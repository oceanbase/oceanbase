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

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include "lib/hash/ob_pre_alloc_link_hashmap.h"
#include "lib/allocator/ob_malloc.h"

#include "gtest/gtest.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;

struct Item {
  int64_t key_;
  int64_t value_;
  int64_t ref_;

  Item() : key_(0), value_(0), ref_(0)
  {}
  Item(const int64_t key, const int64_t value, const int64_t ref) : key_(key), value_(value), ref_(ref)
  {}
  TO_STRING_KV(K_(key), K_(value));
};

struct Node : public ObPreAllocLinkHashNode<int64_t, Item> {
  explicit Node(Item& item) : ObPreAllocLinkHashNode(item), next_(NULL)
  {}
  virtual ~Node()
  {}
  static uint64_t hash(int64_t key)
  {
    return key;
  }
  virtual uint64_t hash() const
  {
    return hash(item_.key_);
  }
  virtual const int64_t& get_key() const
  {
    return item_.key_;
  }
  Node* next_;
};

class ItemProtector {
public:
  static void hold(Item& item)
  {
    ++item.ref_;
  }
  static void release(Item& item)
  {
    --item.ref_;
  }
};

typedef common::hash::ObPreAllocLinkHashMap<int64_t, Item, Node, ItemProtector> TestMap;
struct TestForeachFinder : public TestMap::ForeachFunctor {
  TestForeachFinder() : count_(0)
  {}
  virtual ~TestForeachFinder()
  {}
  virtual int operator()(Item& item, bool& is_full)
  {
    int ret = OB_SUCCESS;
    if (count_ >= MAX_COUNT) {
      is_full = true;
    } else if (item.value_ == 0) {
      items_[count_++] = &item;
    } else if (item.value_ == 2) {
      ret = OB_ERR_SYS;
    }
    return ret;
  }

  static const int64_t MAX_COUNT = 3;
  Item* items_[MAX_COUNT];
  int64_t count_;
};

struct TestForeachCheckRef : public TestMap::ForeachFunctor {
  TestForeachCheckRef() : count_(0)
  {}
  virtual ~TestForeachCheckRef()
  {}
  virtual int operator()(Item& item, bool& is_full)
  {
    int ret = OB_SUCCESS;
    is_full = false;
    ++count_;
    if (item.ref_ != 0) {
      COMMON_LOG(ERROR, "ref not zero", K(item.ref_), K(item.key_));
      ret = OB_ERR_SYS;
    }
    return ret;
  }

  int64_t count_;
};

class TestEraseChecker : public TestMap::EraseChecker

{
public:
  virtual ~TestEraseChecker()
  {}
  virtual int operator()(Item& item)
  {
    int ret = OB_SUCCESS;
    if (item.value_ != 0) {
      ret = OB_EAGAIN;
    }
    return ret;
  }
};

class TestGetFunctor : public TestMap::GetFunctor {
public:
  TestGetFunctor() : item_(NULL)
  {}
  virtual ~TestGetFunctor()
  {}
  virtual int operator()(Item& item) override
  {
    int ret = OB_SUCCESS;
    item_ = NULL;
    if (item.value_ == 3) {
      ret = OB_ERR_SYS;
    } else {
      item_ = &item;
    }
    return ret;
  }
  Item* item_;

private:
  DISALLOW_COPY_AND_ASSIGN(TestGetFunctor);
};

TEST(TestObMemLessLinkHashMap, basic)
{
  TestMap map;
  int64_t buckets_count = 0;
  uint32_t latch_id = 1;
  const lib::ObLabel& label = "1";
  Item item1 = {1, 0, 0};
  Node* node1 = map.alloc_node(item1);
  COMMON_LOG(INFO, "dump", K(item1), K(node1->item_), KP(&item1), KP(&node1->item_));
  TestForeachFinder finder;

  ASSERT_EQ(OB_NOT_INIT, map.put(*node1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, map.init(buckets_count, latch_id, label));
  ASSERT_EQ(OB_INVALID_ARGUMENT, map.init(buckets_count, latch_id, label));
  buckets_count = 100;
  ASSERT_EQ(OB_SUCCESS, map.init(buckets_count, latch_id, label));
  ASSERT_EQ(OB_INIT_TWICE, map.init(buckets_count, latch_id, label));
  ASSERT_EQ(0, map.get_count());

  ASSERT_EQ(OB_SUCCESS, map.put(*node1));
  ASSERT_EQ(OB_HASH_EXIST, map.put(*node1));
  ASSERT_EQ(1, map.get_count());

  Item item2 = {1, 0, 0};
  Node node2(item2);
  ASSERT_EQ(OB_HASH_EXIST, map.put(node2));
  ASSERT_EQ(1, map.get_count());

  Item* get_item = NULL;
  ASSERT_EQ(OB_SUCCESS, map.get(1, get_item));
  ASSERT_EQ(&item1, get_item);
  ASSERT_EQ(OB_SUCCESS, map.foreach (finder));
  ASSERT_EQ(1, finder.count_);
  COMMON_LOG(INFO, "dump", K(node1), K(finder.items_[0]));
  ASSERT_EQ(&item1, finder.items_[0]);

  Item* del_item = NULL;
  ASSERT_EQ(OB_SUCCESS, map.erase(1, del_item));
  ASSERT_EQ(&item1, del_item);
  ASSERT_EQ(OB_HASH_NOT_EXIST, map.erase(1, del_item));
  ASSERT_EQ(OB_HASH_NOT_EXIST, map.get(1, del_item));
}

TEST(TestObMemLessLinkHashMap, same_hash)
{
  TestMap map;
  int64_t buckets_count = 1;
  uint32_t latch_id = 1;
  const lib::ObLabel& label = "1";
  Item item1 = {1, 0, 0};

  ASSERT_EQ(OB_SUCCESS, map.init(buckets_count, latch_id, label));
  buckets_count = map.get_buckets_count();
  Node* node1 = map.alloc_node(item1);
  Item item2 = {1 + buckets_count, 0, 0};
  Node* node2 = map.alloc_node(item2);

  ASSERT_EQ(OB_SUCCESS, map.put(*node1));
  ASSERT_EQ(OB_SUCCESS, map.put(*node2));
  ASSERT_EQ(2, map.get_count());

  Item* got_item = NULL;
  ASSERT_EQ(OB_SUCCESS, map.get(1, got_item));
  ASSERT_EQ(&item1, got_item);
  ASSERT_EQ(OB_SUCCESS, map.get(item2.key_, got_item));
  ASSERT_EQ(&item2, got_item);

  Item* del_item = NULL;
  ASSERT_EQ(OB_SUCCESS, map.erase(item2.key_, del_item));
  ASSERT_EQ(&item2, del_item);
  ASSERT_EQ(OB_SUCCESS, map.erase(item1.key_, del_item));
  ASSERT_EQ(&item1, del_item);
}

TEST(TestObMemLessLinkHashMap, erase)
{
  TestMap map;
  int64_t buckets_count = 100;
  uint32_t latch_id = 1;
  const lib::ObLabel& label = "1";
  TestEraseChecker checker;
  Item* del_item = NULL;

  ASSERT_EQ(OB_SUCCESS, map.init(buckets_count, latch_id, label));
  Item item1 = {1, 0, 0};
  Node* node1 = map.alloc_node(item1);
  Item item2 = {2, 1, 0};
  Node* node2 = map.alloc_node(item2);
  ASSERT_EQ(OB_SUCCESS, map.put(*node1));
  ASSERT_EQ(OB_SUCCESS, map.put(*node2));
  ASSERT_EQ(OB_SUCCESS, map.erase(1, del_item, &checker));
  ASSERT_EQ(del_item, &item1);
  ASSERT_EQ(OB_EAGAIN, map.erase(2, del_item, &checker));
}

TEST(TestObMemLessLinkHashMap, get_functor)
{
  TestMap map;
  int64_t buckets_count = 100;
  uint32_t latch_id = 1;
  const lib::ObLabel& label = "1";

  ASSERT_EQ(OB_SUCCESS, map.init(buckets_count, latch_id, label));

  Item item1 = {1, 0, 0};
  Node* node1 = map.alloc_node(item1);
  Item item2 = {2, 3, 0};
  Node* node2 = map.alloc_node(item2);
  TestGetFunctor get_functor;
  Item* got_item = NULL;

  ASSERT_EQ(OB_SUCCESS, map.put(*node1));
  ASSERT_EQ(OB_SUCCESS, map.put(*node2));
  ASSERT_EQ(OB_SUCCESS, map.get(1, got_item, &get_functor));
  ASSERT_EQ(got_item, &item1);
  ASSERT_EQ(OB_SUCCESS, map.get(1, get_functor));
  ASSERT_EQ(get_functor.item_, &item1);
  ASSERT_EQ(OB_ERR_SYS, map.get(2, got_item, &get_functor));
  ASSERT_TRUE(got_item == NULL);
  ASSERT_TRUE(get_functor.item_ == NULL);
  ASSERT_EQ(OB_HASH_EXIST, map.exist(1));
  ASSERT_EQ(OB_HASH_EXIST, map.exist(2));
  ASSERT_EQ(OB_HASH_NOT_EXIST, map.exist(3));
}

TEST(TestObMemLessLinkHashMap, foreach)
{
  TestMap map;
  int64_t buckets_count = 100;
  uint32_t latch_id = 1;
  const lib::ObLabel& label = "1";
  const int64_t count = 10000;
  Item* items[count];
  TestForeachFinder finder;

  ASSERT_EQ(OB_SUCCESS, map.init(buckets_count, latch_id, label));

  for (int64_t i = 0; i < count; ++i) {
    Item* item = new Item();
    item->key_ = i;
    item->value_ = i % 2;
    Node* node = map.alloc_node(*item);
    items[i] = item;
    ASSERT_EQ(OB_SUCCESS, map.put(*node));
  }

  for (int64_t i = 0; i < count; ++i) {
    Item* got_item = NULL;
    ASSERT_EQ(OB_SUCCESS, map.get(i, got_item));
    ASSERT_EQ(items[i], got_item);
  }

  ASSERT_EQ(OB_SUCCESS, map.foreach (finder));
  ASSERT_EQ(3, finder.count_);
  for (int64_t i = 0; i < 3; ++i) {
    COMMON_LOG(INFO, "dump", K(items[2 * i]), K(*finder.items_[i]));
  }

  for (int64_t i = 0; i < count; ++i) {
    delete items[i];
  }
}

TEST(TestObMemLessLinkHashMap, iterator)
{
  TestMap map;
  TestMap::Iterator iter(map);
  int64_t buckets_count = 100000;
  uint32_t latch_id = 1;
  const lib::ObLabel& label = "1";
  ASSERT_EQ(OB_SUCCESS, map.init(buckets_count, latch_id, label));
  Item item1 = {1, 0, 0};
  Node* node1 = map.alloc_node(item1);
  Item item2 = {2, 3, 0};
  Node* node2 = map.alloc_node(item2);
  TestGetFunctor get_functor;
  Item* got_item = NULL;

  ASSERT_EQ(OB_SUCCESS, map.put(*node1));
  ASSERT_EQ(OB_SUCCESS, map.put(*node2));
  ASSERT_EQ(OB_SUCCESS, iter.get_next(got_item));
  ASSERT_EQ(&item1, got_item);
  ASSERT_EQ(OB_SUCCESS, iter.get_next(got_item));
  ASSERT_EQ(&item2, got_item);
  ASSERT_EQ(OB_ITER_END, iter.get_next(got_item));
}

TEST(TestObMemLessLinkHashMap, iterator2)
{
  int ret = OB_SUCCESS;
  TestMap map;
  int64_t buckets_count = 100;
  uint32_t latch_id = 1;
  const lib::ObLabel& label = "1";
  ASSERT_EQ(OB_SUCCESS, map.init(buckets_count, latch_id, label));

  const int64_t count = 8061;
  for (int64_t i = 0; i < count; ++i) {
    Item* item = new Item();
    item->key_ = i;
    item->value_ = i;
    item->ref_ = 0;
    Node* node = map.alloc_node(*item);
    ASSERT_EQ(OB_SUCCESS, map.put(*node));
  }

  {
    TestMap::Iterator iter(map);
    Item* got_item = NULL;
    int64_t num = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next(got_item))) {
        COMMON_LOG(WARN, "rend", K(ret), K(num));
      } else {
        ++num;
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
  }

  TestForeachCheckRef checker;
  ASSERT_EQ(OB_SUCCESS, map.foreach (checker));
  ASSERT_EQ(count, checker.count_);
}
int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
