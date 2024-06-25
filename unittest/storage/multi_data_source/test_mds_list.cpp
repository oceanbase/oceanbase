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
#define UNITTEST_DEBUG
#include "lib/utility/utility.h"
#include <atomic>
#include <cstdlib>
#include <gtest/gtest.h>
#define private public
#define protected public
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"
#include <thread>
#include <iostream>
#include <vector>
#include <chrono>
#include "storage/multi_data_source/runtime_utility/mds_factory.h"
#include "common/ob_clock_generator.h"
#include "storage/multi_data_source/mds_node.h"
#include "common/meta_programming/ob_type_traits.h"
#include "storage/multi_data_source/mds_row.h"
#include "storage/tablet/ob_mds_schema_helper.h"
namespace oceanbase {
namespace storage {
namespace mds {
void *DefaultAllocator::alloc(const int64_t size) {
  void *ptr = std::malloc(size);// ob_malloc(size, "MDS");
  ATOMIC_INC(&alloc_times_);
  MDS_LOG(DEBUG, "alloc obj", KP(ptr), K(size), K(lbt()));
  return ptr;
}
void DefaultAllocator::free(void *ptr) {
  ATOMIC_INC(&free_times_);
  MDS_LOG(DEBUG, "free obj", KP(ptr), K(lbt()));
  std::free(ptr);// ob_free(ptr);
}
void *MdsAllocator::alloc(const int64_t size) {
  void *ptr = std::malloc(size);// ob_malloc(size, "MDS");
  ATOMIC_INC(&alloc_times_);
  MDS_LOG(DEBUG, "alloc obj", KP(ptr), K(size), K(lbt()));
  return ptr;
}
void MdsAllocator::free(void *ptr) {
  ATOMIC_INC(&free_times_);
  MDS_LOG(DEBUG, "free obj", KP(ptr), K(lbt()));
  std::free(ptr);// ob_free(ptr);
}
}}}
namespace oceanbase {
namespace unittest {

using namespace common;
using namespace std;
using namespace storage;
using namespace mds;

class TestMdsList: public ::testing::Test
{
public:
  TestMdsList() { ObMdsSchemaHelper::get_instance().init(); };
  virtual ~TestMdsList() {};
  virtual void SetUp() {
  };
  virtual void TearDown() {
  };
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestMdsList);
};

TEST_F(TestMdsList, del_from_tail) {
  SortedList<UserMdsNode<DummyKey, ExampleUserData1>, SORT_TYPE::DESC> list;
  for (int i = 0; i < 10; ++i) {
    ListNode<UserMdsNode<DummyKey, ExampleUserData1>> *new_node = new UserMdsNode<DummyKey, ExampleUserData1>();
    list.append(new_node);
  }
  list.for_each_node_from_tail_to_head_until_true([&list](const UserMdsNode<DummyKey, ExampleUserData1> &node) {
    list.del((ListNode<UserMdsNode<DummyKey, ExampleUserData1>> *)(&node));
    delete &node;
    return false;
  });
  ASSERT_EQ(list.empty(), true);
}

TEST_F(TestMdsList, del_from_head) {
  SortedList<UserMdsNode<DummyKey, ExampleUserData1>, SORT_TYPE::DESC> list;
  for (int i = 0; i < 10; ++i) {
    ListNode<UserMdsNode<DummyKey, ExampleUserData1>> *new_node = new UserMdsNode<DummyKey, ExampleUserData1>();
    list.append(new_node);
  }
  list.for_each_node_from_head_to_tail_until_true([&list](const UserMdsNode<DummyKey, ExampleUserData1> &node) {
    list.del((ListNode<UserMdsNode<DummyKey, ExampleUserData1>> *)(&node));
    delete &node;
    return false;
  });
  ASSERT_EQ(list.empty(), true);
}

TEST_F(TestMdsList, random_del) {
  for (int i = 0; i < 100; ++i) {
    SortedList<UserMdsNode<DummyKey, ExampleUserData1>, SORT_TYPE::DESC> list;
    vector<ListNode<UserMdsNode<DummyKey, ExampleUserData1>> *> v;
    for (int j = 0; j < 10; ++j) {
      ListNode<UserMdsNode<DummyKey, ExampleUserData1>> *new_node = new UserMdsNode<DummyKey, ExampleUserData1>();
      list.append(new_node);
      v.push_back(new_node);
    }
    int idx = rand() % 10;
    for (int k = 0; k < 10; ++k) {
      list.del(v[(idx + k) % 10]);
    }
    ASSERT_EQ(list.empty(), true);
  }
}

TEST_F(TestMdsList, insert_to_tail) {
  SortedList<UserMdsNode<DummyKey, ExampleUserData1>, SORT_TYPE::DESC> list;
  for (int j = 0; j < 10; ++j) {
    UserMdsNode<DummyKey, ExampleUserData1> *new_node = new UserMdsNode<DummyKey, ExampleUserData1>();
    new_node->redo_scn_ = mock_scn(j);
    list.insert(new_node);
  }
  list.for_each_node_from_head_to_tail_until_true([&list](const UserMdsNode<DummyKey, ExampleUserData1> &node) {
    list.del((ListNode<UserMdsNode<DummyKey, ExampleUserData1>> *)(&node));
    delete &node;
    return false;
  });
}

TEST_F(TestMdsList, insert_to_head) {
  SortedList<UserMdsNode<DummyKey, ExampleUserData1>, SORT_TYPE::DESC> list;
  for (int j = 0; j < 10; ++j) {
    UserMdsNode<DummyKey, ExampleUserData1> *new_node = new UserMdsNode<DummyKey, ExampleUserData1>();
    new_node->redo_scn_ = mock_scn(10 - j);
    list.insert(new_node);
  }
  list.for_each_node_from_head_to_tail_until_true([&list](const UserMdsNode<DummyKey, ExampleUserData1> &node) {
    list.del((ListNode<UserMdsNode<DummyKey, ExampleUserData1>> *)(&node));
    delete &node;
    return false;
  });
}

TEST_F(TestMdsList, random_insert_del) {
  for (int i = 0; i < 100; ++i) {
    SortedList<UserMdsNode<DummyKey, ExampleUserData1>, SORT_TYPE::DESC> list;
    vector<ListNode<UserMdsNode<DummyKey, ExampleUserData1>> *> v;
    for (int j = 0; j < 10; ++j) {
      UserMdsNode<DummyKey, ExampleUserData1> *new_node = new UserMdsNode<DummyKey, ExampleUserData1>();
      new_node->redo_scn_ = mock_scn(rand() % 10);
      list.insert(new_node);
      v.push_back(new_node);
    }
    int idx = rand() % 10;
    for (int k = 0; k < 10; ++k) {
      list.del(v[(idx + k) % 10]);
    }
    ASSERT_EQ(list.empty(), true);
  }
}

TEST_F(TestMdsList, fetch_and_insert) {
  SortedList<UserMdsNode<DummyKey, ExampleUserData1>, SORT_TYPE::DESC> list;
  UserMdsNode<DummyKey, ExampleUserData1> *new_node = new UserMdsNode<DummyKey, ExampleUserData1>();
  UserMdsNode<DummyKey, ExampleUserData1> *new_node2 = new UserMdsNode<DummyKey, ExampleUserData1>();
  list.insert_into_head(new_node);
  ListNode<UserMdsNode<DummyKey, ExampleUserData1>> *node = list.fetch_from_head();
  ASSERT_NE(nullptr, node);
  ASSERT_EQ(true, list.empty());
  list.insert_into_head(new_node);
  node = list.fetch_from_head();
  ASSERT_NE(nullptr, node);
  ASSERT_EQ(true, list.empty());
  list.insert_into_head(new_node);
  list.insert_into_head(new_node2);
  node = list.fetch_from_head();
  ASSERT_EQ(new_node2, node);
  ASSERT_EQ(false, list.empty());
  list.insert_into_head(new_node2);
  node = list.fetch_from_head();
  ASSERT_EQ(new_node2, node);
  delete new_node;
  delete new_node2;
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_mds_list.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_mds_list.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  int64_t alloc_times = oceanbase::storage::mds::MdsAllocator::get_alloc_times();
  int64_t free_times = oceanbase::storage::mds::MdsAllocator::get_free_times();
  if (alloc_times != free_times) {
    MDS_LOG(ERROR, "memory may leak", K(free_times), K(alloc_times));
    ret = -1;
  } else {
    MDS_LOG(INFO, "all memory released", K(free_times), K(alloc_times));
  }
  return ret;
}