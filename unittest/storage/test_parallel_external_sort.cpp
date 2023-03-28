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
#include "storage/ob_parallel_external_sort.h"
#undef private
#include <algorithm>
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_vector.h"
#include "lib/lock/ob_mutex.h"
#include "lib/random/ob_random.h"
#include "lib/string/ob_string.h"
#include "share/ob_tenant_mgr.h"
#include "share/ob_srv_rpc_proxy.h"
#include "./blocksstable/ob_data_file_prepare.h"
#include "share/ob_simple_mem_limit_getter.h"

namespace oceanbase
{
using namespace storage;
using namespace blocksstable;
using namespace common;
using namespace share::schema;
static ObSimpleMemLimitGetter getter;

namespace unittest
{
class TestItem;
class TestItemCompare;

class TestItem
{
public:
  TestItem();
  virtual ~TestItem();
  void set(const int64_t key, const char *buf, const int32_t length);
  int64_t get_key() const { return key_; }
  const ObString &get_value() const { return value_; }
  int64_t get_deep_copy_size() const;
  int deep_copy(const TestItem &src, char *buf, int64_t len, int64_t &pos);
  bool is_valid() const { return value_.length() > 0; }
  void reset() { key_ = 0; value_.reset(); }
  TO_STRING_KV(K(key_), K(value_));
  NEED_SERIALIZE_AND_DESERIALIZE;

private:
  int64_t key_;
  ObString value_;
};

class TestItemCompare
{
public:
  TestItemCompare(int &sort_ret): result_code_(sort_ret) {}
  bool operator() (const TestItem *left, const TestItem *right)
  {
    return left->get_key() < right->get_key();
  }
  int &result_code_;
};

TestItem::TestItem()
  : key_(0),
    value_()
{
}

TestItem::~TestItem()
{
}

void TestItem::set(const int64_t key, const char *buf, const int32_t length)
{
  key_ = key;
  value_.assign_ptr(buf, length);
}

int64_t TestItem::get_deep_copy_size() const
{
  return value_.length();
}

int TestItem::deep_copy(const TestItem &src, char *buf, int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (src.value_.length() > len - pos) {
    ret = OB_BUF_NOT_ENOUGH;
    COMMON_LOG(ERROR, "buf not enough", K(src), K(len), K(pos));
  } else {
    key_ = src.key_;
    memcpy(buf + pos, src.value_.ptr(), src.value_.length());
    value_.assign_ptr(buf + pos, src.value_.length());
    pos += src.value_.length();
  }
  return ret;
}

DEFINE_SERIALIZE(TestItem)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, key_))) {
    COMMON_LOG(WARN, "Fail to encode key", K(ret));
  } else if (OB_FAIL(value_.serialize(buf, buf_len, pos))) {
    COMMON_LOG(WARN, "failed to encode value", K(ret));
  }

  if (!is_valid()){
    COMMON_LOG(INFO, "encode invalid item", K(*this));
  }
  return ret;
}

DEFINE_DESERIALIZE(TestItem)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &key_))) {
    COMMON_LOG(WARN, "fail to decode key_", K(ret));
  } else if (OB_FAIL(value_.deserialize(buf, data_len, pos))) {
    COMMON_LOG(WARN, "failed to decode value", K(ret));
  } else {
    if (!is_valid() ){
      // COMMON_LOG(INFO, "decode invalid item", K(*this));
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(TestItem)
{
  int64_t size = 0;

  size += serialization::encoded_length_vi64(key_);
  size += value_.get_serialize_size();

  return size;
}

class TestParallelExternalSort : public blocksstable::TestDataFilePrepare
{
public:
  TestParallelExternalSort();
  virtual ~TestParallelExternalSort() {}
  int init_tenant_mgr();
  void destroy_tenant_mgr();
  int generate_random_str(char *&buf, int32_t &buf_len);
  int generate_items(const int64_t item_nums, const bool is_sorted, ObVector<TestItem *> &items);
  int generate_items_dup(const int64_t item_nums, const bool is_sorted, ObVector<TestItem *> &items);
  int shuffle_items(const int64_t task_id, const int64_t task_cnt, ObVector<TestItem *> &total_items,
      ObVector<TestItem *> &task_items);
  int build_reader(const ObVector<TestItem *> &items, const int64_t buf_cap,
      ObFragmentReaderV2<TestItem> &reader);
  void test_merge(const int64_t buf_cap, const int64_t items_count, const int64_t task_cnt);
  void test_merge_dup(const int64_t buf_cap, const int64_t items_count, const int64_t task_cnt);
  void test_sort_round(const int64_t buf_cap, const int64_t items_count, const int64_t merge_count);
  void test_multi_sort_round(const int64_t buf_cap, const int64_t items_count, const int64_t task_cnt, const int64_t merge_count);
  void test_memory_sort_round(const int64_t buf_mem_limit, const int64_t items_count);
  void test_sort(const int64_t buf_mem_limit, const int64_t file_buf_size, const int64_t items_cnt);
  void test_multi_task_sort(const int64_t buf_mem_limit, const int64_t file_buf_size, const int64_t items_cnt, const int64_t task_cnt);
  virtual void SetUp();
  virtual void TearDown();
public:
  static const int64_t MACRO_BLOCK_SIZE = 2 * 1024 * 1024;
  static const int64_t MACRO_BLOCK_COUNT = 15* 1024;
private:
  common::ObArenaAllocator allocator_;
};

TestParallelExternalSort::TestParallelExternalSort()
  : TestDataFilePrepare(&getter, "TestParallelExternalSort", MACRO_BLOCK_SIZE, MACRO_BLOCK_COUNT),
    allocator_(ObModIds::TEST)
{
}

void TestParallelExternalSort::SetUp()
{
  TestDataFilePrepare::SetUp();
  ASSERT_EQ(OB_SUCCESS, init_tenant_mgr());
  ASSERT_EQ(OB_SUCCESS, ObTmpFileManager::get_instance().init());
  static ObTenantBase tenant_ctx(OB_SYS_TENANT_ID);
  ObTenantEnv::set_tenant(&tenant_ctx);
  ObTenantIOManager *io_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
}

void TestParallelExternalSort::TearDown()
{
  allocator_.reuse();
  ObTmpFileManager::get_instance().destroy();
  TestDataFilePrepare::TearDown();
  destroy_tenant_mgr();
}

int TestParallelExternalSort::init_tenant_mgr()
{
  int ret = OB_SUCCESS;
  ObAddr self;
  obrpc::ObSrvRpcProxy rpc_proxy;
  obrpc::ObCommonRpcProxy rs_rpc_proxy;
  share::ObRsMgr rs_mgr;
  self.set_ip_addr("127.0.0.1", 8086);
  rpc::frame::ObReqTransport req_transport(NULL, NULL);
  const int64_t ulmt = 128LL << 30;
  const int64_t llmt = 128LL << 30;
  ret = getter.add_tenant(OB_SYS_TENANT_ID, ulmt, llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = getter.add_tenant(OB_SERVER_TENANT_ID, ulmt, llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  lib::set_memory_limit(128LL << 32);
  return ret;
}

void TestParallelExternalSort::destroy_tenant_mgr()
{
}

int TestParallelExternalSort::generate_random_str(char *&buf, int32_t &buf_len)
{
  int ret = OB_SUCCESS;
  const int32_t len = static_cast<int32_t>(ObRandom::rand(10, 127));
  if (NULL == (buf = static_cast<char *>(allocator_.alloc(len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "fail to allocate memory", K(ret), K(len));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
    buf[i] = static_cast<char> ('a' + ObRandom::rand(0, 25));
  }
  if (OB_SUCC(ret)) {
    buf_len = len;
  }
  return ret;
}

int TestParallelExternalSort::generate_items(const int64_t item_nums, const bool is_sorted,
    ObVector<TestItem *> &items)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int32_t buf_len = 0;
  void *buf_tmp = NULL;
  TestItem *item = NULL;
  items.reset();
  if (item_nums < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(item_nums));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < item_nums; ++i) {
      if (OB_FAIL(generate_random_str(buf, buf_len))) {
        COMMON_LOG(WARN, "fail to generate random str");
      } else if (NULL == (buf_tmp = allocator_.alloc(sizeof(TestItem)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "fail to allocate memory", K(ret));
      } else if (NULL == (item = new (buf_tmp) TestItem())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "fail to placement new TestItem", K(ret));
      } else {
        item->set(i, buf, buf_len);
        if (OB_FAIL(items.push_back(item))) {
          COMMON_LOG(WARN, "fail to push back item", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && !is_sorted) {
      std::random_shuffle(items.begin(), items.end());
    }
  }
  return ret;
}

int TestParallelExternalSort::generate_items_dup(const int64_t item_nums, const bool is_sorted,
    ObVector<TestItem *> &items)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int32_t buf_len = 0;
  void *buf_tmp = NULL;
  TestItem *item = NULL;
  items.reset();
  if (item_nums < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(item_nums));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < item_nums; ++i) {
      if (OB_FAIL(generate_random_str(buf, buf_len))) {
        COMMON_LOG(WARN, "fail to generate random str");
      } else if (NULL == (buf_tmp = allocator_.alloc(sizeof(TestItem)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "fail to allocate memory", K(ret));
      } else if (NULL == (item = new (buf_tmp) TestItem())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "fail to placement new TestItem", K(ret));
      } else {
        const int64_t k = ObRandom::rand(0, item_nums);
        item->set(k, buf, buf_len);
        if (OB_FAIL(items.push_back(item))) {
          COMMON_LOG(WARN, "fail to push back item", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && !is_sorted) {
      std::random_shuffle(items.begin(), items.end());
    }
  }
  return ret;
}

int TestParallelExternalSort::shuffle_items(const int64_t task_id, const int64_t task_cnt,
    ObVector<TestItem *> &total_items, ObVector<TestItem *> &task_items)
{
  int ret = OB_SUCCESS;
  const int64_t total_item_cnt = total_items.size();
  int64_t start_idx = std::max(0L, total_item_cnt * task_id / task_cnt);
  int64_t end_idx = std::min(total_item_cnt - 1, total_item_cnt * (task_id + 1) / task_cnt - 1);
  task_items.reset();
  for (int64_t i = start_idx; i <= end_idx; ++i) {
    if (OB_FAIL(task_items.push_back(total_items[i]))) {
      COMMON_LOG(WARN, "fail to push back task item", K(ret));
    }
  }
  return ret;
}

int TestParallelExternalSort::build_reader(const ObVector<TestItem *> &items, const int64_t buf_cap,
    ObFragmentReaderV2<TestItem> &reader)
{
  int ret = OB_SUCCESS;
  reader.reset();
  if (0 == items.size()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KP(items.size()));
  } else {
    const int64_t expire_timestamp = 0;
    TestItemCompare compare(ret);
    ObFragmentWriterV2<TestItem> writer;
    int64_t dir_id = -1;
    std::sort(items.begin(), items.end(), compare);
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.alloc_dir(dir_id))) {
      COMMON_LOG(WARN, "fail to allocate file directory", K(ret));
    } else if (OB_FAIL(writer.open(buf_cap, expire_timestamp, OB_SYS_TENANT_ID, dir_id))) {
      COMMON_LOG(WARN, "fail to open writer", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < items.size(); ++i) {
      if (OB_FAIL(writer.write_item(*items.at(i)))) {
        COMMON_LOG(WARN, "fail to write item");
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(writer.sync())) {
        COMMON_LOG(WARN, "fail to flush data", K(ret));
      } else if (OB_FAIL(reader.init(writer.get_fd(), writer.get_dir_id(), expire_timestamp, OB_SYS_TENANT_ID, writer.get_sample_item(),
          buf_cap))) {
        COMMON_LOG(WARN, "fail to open reader", K(ret));
      }
    }
  }
  return ret;
}

void TestParallelExternalSort::test_merge(const int64_t buf_cap, const int64_t items_cnt, const int64_t task_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t max_reader_count = 128;
  ObVector<TestItem *> total_items;
  ObVector<TestItem *> task_items;
  ObFragmentReaderV2<TestItem> readers[max_reader_count];
  ObArray<ObFragmentIterator<TestItem> *> readers_array;
  ObFragmentMerge<TestItem, TestItemCompare> merge;
  TestItemCompare compare(ret);
  const TestItem *item = NULL;
  ret = generate_items(items_cnt, false, total_items);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; OB_SUCC(ret) && i < task_cnt; ++i) {
    ret = shuffle_items(i, task_cnt, total_items, task_items);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = build_reader(task_items, buf_cap, readers[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < task_cnt; ++i) {
    if (OB_FAIL(readers_array.push_back(&readers[i]))) {
      COMMON_LOG(WARN, "fail to push back reader", K(ret), K(i));
    }
  }
  ASSERT_EQ(OB_SUCCESS, merge.init(readers_array, &compare));
  ASSERT_EQ(OB_SUCCESS, merge.open());
  std::sort(total_items.begin(), total_items.end(), compare);
  for (int64_t i = 0; OB_SUCC(ret) && i < total_items.size(); ++i) {
    ret = merge.get_next_item(item);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(item->get_key(), total_items.at(i)->get_key());
  }
  ret = merge.get_next_item(item);
  ASSERT_EQ(OB_ITER_END, ret);
}

void TestParallelExternalSort::test_merge_dup(const int64_t buf_cap, const int64_t items_cnt, const int64_t task_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t max_reader_count = 128;
  ObVector<TestItem *> total_items;
  ObVector<TestItem *> task_items;
  ObFragmentReaderV2<TestItem> readers[max_reader_count];
  ObArray<ObFragmentIterator<TestItem> *> readers_array;
  ObFragmentMerge<TestItem, TestItemCompare> merge;
  TestItemCompare compare(ret);
  const TestItem *item = NULL;
  ret = generate_items_dup(items_cnt, false, total_items);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; OB_SUCC(ret) && i < task_cnt; ++i) {
    ret = shuffle_items(i, task_cnt, total_items, task_items);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = build_reader(task_items, buf_cap, readers[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < task_cnt; ++i) {
    if (OB_FAIL(readers_array.push_back(&readers[i]))) {
      COMMON_LOG(WARN, "fail to push back reader", K(ret), K(i));
    }
  }
  ASSERT_EQ(OB_SUCCESS, merge.init(readers_array, &compare));
  ASSERT_EQ(OB_SUCCESS, merge.open());
  std::sort(total_items.begin(), total_items.end(), compare);
  for (int64_t i = 0; OB_SUCC(ret) && i < total_items.size(); ++i) {
    ret = merge.get_next_item(item);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(item->get_key(), total_items.at(i)->get_key());
  }
  ret = merge.get_next_item(item);
  ASSERT_EQ(OB_ITER_END, ret);
}

void TestParallelExternalSort::test_sort_round(const int64_t buf_cap, const int64_t items_cnt, const int64_t task_cnt)
{
  int ret = OB_SUCCESS;
  ObVector<TestItem *> total_items;
  ObVector<TestItem *> task_items;
  typedef ObExternalSortRound<TestItem, TestItemCompare> SortRound;
  SortRound sort_round;
  SortRound next_round;
  TestItemCompare compare(ret);
  const TestItem *item = NULL;
  const int64_t expire_timestamp = 0;
  ret = generate_items(items_cnt, false, total_items);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sort_round.init(task_cnt, buf_cap, expire_timestamp, OB_SYS_TENANT_ID, &compare);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = next_round.init(task_cnt, buf_cap, expire_timestamp, OB_SYS_TENANT_ID, &compare);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < task_cnt; ++i) {
    ret = shuffle_items(i, task_cnt, total_items, task_items);
    ASSERT_EQ(OB_SUCCESS, ret);
    std::sort(task_items.begin(), task_items.end(), compare);
    for (int64_t j = 0; OB_SUCC(ret) && j < task_items.size(); ++j) {
      ret = sort_round.add_item(*task_items.at(j));
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ret = sort_round.build_fragment();
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = sort_round.finish_write();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sort_round.do_merge(next_round);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = next_round.build_merger();
  ASSERT_EQ(OB_SUCCESS, ret);
  std::sort(total_items.begin(), total_items.end(), compare);
  for (int64_t i = 0; OB_SUCC(ret) && i < total_items.size(); ++i) {
    ret = next_round.get_next_item(item);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(total_items.at(i)->get_key(), item->get_key());
  }
  ret = next_round.get_next_item(item);
  ASSERT_EQ(OB_ITER_END, ret);
}

void TestParallelExternalSort::test_multi_sort_round(const int64_t buf_cap, const int64_t items_cnt, const int64_t task_cnt, const int64_t merge_count)
{
  int ret = OB_SUCCESS;
  ObVector<TestItem *> total_items;
  ObVector<TestItem *> task_items;
  typedef ObExternalSortRound<TestItem, TestItemCompare> SortRound;
  SortRound sort_rounds[2];
  SortRound *curr_round = &sort_rounds[0];
  SortRound *next_round = &sort_rounds[1];
  TestItemCompare compare(ret);
  const TestItem *item = NULL;
  const int64_t expire_timestamp = 0;
  ret = generate_items(items_cnt, false, total_items);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = curr_round->init(merge_count, buf_cap, expire_timestamp, OB_SYS_TENANT_ID, &compare);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < task_cnt; ++i) {
    ret = shuffle_items(i, task_cnt, total_items, task_items);
    ASSERT_EQ(OB_SUCCESS, ret);
    std::sort(task_items.begin(), task_items.end(), compare);
    for (int64_t j = 0; OB_SUCC(ret) && j < task_items.size(); ++j) {
      ret = curr_round->add_item(*task_items.at(j));
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ret = curr_round->build_fragment();
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = curr_round->finish_write();
  ASSERT_EQ(OB_SUCCESS, ret);
  while (curr_round->get_fragment_count() >= merge_count)
  {
    ret = next_round->init(merge_count, buf_cap, expire_timestamp, OB_SYS_TENANT_ID, &compare);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = curr_round->do_merge(*next_round);
    ASSERT_EQ(OB_SUCCESS, ret);
    std::swap(curr_round, next_round);
    ret = next_round->clean_up();
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = next_round->init(merge_count, buf_cap, expire_timestamp, OB_SYS_TENANT_ID, &compare);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = curr_round->do_merge(*next_round);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = next_round->build_merger();
  ASSERT_EQ(OB_SUCCESS, ret);
  std::sort(total_items.begin(), total_items.end(), compare);
  for (int64_t i = 0; OB_SUCC(ret) && i < total_items.size(); ++i) {
    ret = next_round->get_next_item(item);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(total_items.at(i)->get_key(), item->get_key());
  }
  ret = next_round->get_next_item(item);
  ASSERT_EQ(OB_ITER_END, ret);
}

void TestParallelExternalSort::test_memory_sort_round(const int64_t buf_mem_limit, const int64_t items_cnt)
{
  int ret = OB_SUCCESS;
  typedef ObExternalSortRound<TestItem, TestItemCompare> SortRound;
  typedef ObMemorySortRound<TestItem, TestItemCompare> MemorySortRound;
  SortRound sort_round;
  MemorySortRound memory_sort_round;
  ObVector<TestItem *> total_items;
  const int64_t merge_count = 2;
  const int64_t buf_cap = MACRO_BLOCK_SIZE;
  const int64_t expire_timestamp = 0;
  TestItemCompare compare(ret);
  const TestItem *item = NULL;
  ret = generate_items(items_cnt, false, total_items);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sort_round.init(merge_count, buf_cap, expire_timestamp, OB_SYS_TENANT_ID, &compare);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = memory_sort_round.init(buf_mem_limit, expire_timestamp, &compare, &sort_round);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < total_items.size(); ++i) {
    ret = memory_sort_round.add_item(*total_items.at(i));
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = memory_sort_round.finish();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(memory_sort_round.is_in_memory());
  std::sort(total_items.begin(), total_items.end(), compare);
  for (int64_t i = 0; OB_SUCC(ret) && i < total_items.size(); ++i) {
    ret = memory_sort_round.get_next_item(item);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(total_items.at(i)->get_key(), item->get_key());
  }
}

void TestParallelExternalSort::test_sort(const int64_t buf_mem_limit, const int64_t file_buf_size, const int64_t items_cnt)
{
  int ret = OB_SUCCESS;
  typedef ObExternalSort<TestItem, TestItemCompare> ExternalSort;
  ExternalSort external_sort;
  ObVector<TestItem *>total_items;
  TestItemCompare compare(ret);
  const int64_t expire_timestamp = 0;
  ret = external_sort.init(buf_mem_limit, file_buf_size, expire_timestamp, OB_SYS_TENANT_ID, &compare);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = generate_items(items_cnt, false, total_items);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < total_items.size(); ++i) {
    ret = external_sort.add_item(*total_items.at(i));
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = external_sort.do_sort(false);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestParallelExternalSort::test_multi_task_sort(const int64_t buf_mem_limit, const int64_t file_buf_size, const int64_t items_cnt, const int64_t task_cnt)
{
  int ret = OB_SUCCESS;
  typedef ObExternalSort<TestItem, TestItemCompare> ExternalSort;
  ExternalSort external_sort[task_cnt];
  ExternalSort combine_sort;
  ObVector<TestItem *> total_items;
  ObVector<TestItem *> task_items;
  TestItemCompare compare(ret);
  const TestItem *item = NULL;
  const int64_t expire_timestamp = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < task_cnt; ++i) {
    ret = external_sort[i].init(buf_mem_limit / task_cnt, file_buf_size, expire_timestamp, OB_SYS_TENANT_ID, &compare);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = generate_items(items_cnt, false, total_items);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < task_cnt; ++i) {
    shuffle_items(i, task_cnt, total_items, task_items);
    for (int64_t j = 0; OB_SUCC(ret) && j < task_items.size(); ++j) {
      ret = external_sort[i].add_item(*task_items.at(j));
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ret = external_sort[i].do_sort(false);
    ASSERT_EQ(OB_SUCCESS, ret);
    std::sort(task_items.begin(), task_items.end());
    STORAGE_LOG(INFO, "task i items", K(i), K(task_items));
  }

  ret = combine_sort.init(buf_mem_limit / task_cnt, file_buf_size, expire_timestamp, OB_SYS_TENANT_ID, &compare);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < task_cnt; ++i) {
    ret = external_sort[i].transfer_final_sorted_fragment_iter(combine_sort);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  STORAGE_LOG(INFO, "combine sort begin");
  ret = combine_sort.do_sort(true);
  ASSERT_EQ(OB_SUCCESS, ret);
  std::sort(total_items.begin(), total_items.end(), compare);
  STORAGE_LOG(INFO, "combine get_next_item begin");
  for (int64_t i = 0; OB_SUCC(ret) && i < total_items.size(); ++i) {
    ret = combine_sort.get_next_item(item);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(total_items.at(i)->get_key(), item->get_key());
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < task_cnt; ++i) {
    external_sort[i].clean_up();
  }
  combine_sort.clean_up();
}

TEST_F(TestParallelExternalSort, test_writer)
{
  ObFragmentWriterV2<TestItem> writer;
  ObVector<TestItem *> items;
  const int64_t expire_timestamp = 0;
  const int64_t buf_cap = MACRO_BLOCK_SIZE;
  int ret = OB_SUCCESS;
  int64_t dir_id = -1;

  ret = FILE_MANAGER_INSTANCE_V2.alloc_dir(dir_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  // single macro buffer, total write bytes is less than single macro buffer length
  ret = writer.open(buf_cap, expire_timestamp, OB_SYS_TENANT_ID, dir_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = generate_items(10, true, items);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < items.size(); ++i) {
    ret = writer.write_item(*items.at(i));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // single macro buffer, total write bytes is more than single macro buffer length
  writer.reset();
  ret = writer.open(buf_cap, expire_timestamp, OB_SYS_TENANT_ID, dir_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = generate_items(10000, true, items);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < items.size(); ++i) {
    ret = writer.write_item(*items.at(i));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // multiple macro buffers, total write bytes is less than total capacity of buffers
  writer.reset();
  ret = writer.open(2 * buf_cap, expire_timestamp, OB_SYS_TENANT_ID, dir_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = generate_items(100, true, items);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < items.size(); ++i) {
    ret = writer.write_item(*items.at(i));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // multiple macro buffers, total write bytes is more than total capacity of buffers
  writer.reset();
  ret = writer.open(2 * buf_cap, expire_timestamp, OB_SYS_TENANT_ID, dir_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = generate_items(10000, true, items);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < items.size(); ++i) {
    ret = writer.write_item(*items.at(i));
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

TEST_F(TestParallelExternalSort, test_reader)
{
  ObFragmentWriterV2<TestItem> writer;
  ObFragmentReaderV2<TestItem> reader;
  ObVector<TestItem *> items;
  const TestItem *item = NULL;
  const int64_t expire_timestamp = 0;
  const int64_t buf_cap = MACRO_BLOCK_SIZE;
  int ret = OB_SUCCESS;
  int64_t dir_id = -1;

  ret = FILE_MANAGER_INSTANCE_V2.alloc_dir(dir_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  // single macro buffer, total write bytes is less than single macro buffer length
  ret = writer.open(buf_cap, expire_timestamp, OB_SYS_TENANT_ID, dir_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = generate_items(100, true, items);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < items.size(); ++i) {
    ret = writer.write_item(*items.at(i));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ret = writer.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = reader.init(writer.get_fd(), writer.get_dir_id(), expire_timestamp, OB_SYS_TENANT_ID, writer.get_sample_item(), buf_cap);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = reader.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < items.size(); ++i) {
    ret = reader.get_next_item(item);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(i, item->get_key());
  }
  ret = reader.get_next_item(item);
  ASSERT_EQ(OB_ITER_END, ret);

  // single macro buffer, total write bytes is more than single macro buffer length
  writer.reset();
  ret = writer.open(buf_cap, expire_timestamp, OB_SYS_TENANT_ID, dir_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = generate_items(100, true, items);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < items.size(); ++i) {
    ret = writer.write_item(*items.at(i));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  reader.reset();
  ret = writer.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = reader.init(writer.get_fd(), writer.get_dir_id(), expire_timestamp, OB_SYS_TENANT_ID, writer.get_sample_item(), buf_cap);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = reader.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < items.size(); ++i) {
    ret = reader.get_next_item(item);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(i, item->get_key());
  }
  ret = reader.get_next_item(item);
  ASSERT_EQ(OB_ITER_END, ret);

  // multiple macro buffers, total write bytes is less than capacity of buffers
  writer.reset();
  ret = writer.open(3 * buf_cap, expire_timestamp, OB_SYS_TENANT_ID, dir_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = generate_items(1300, true, items);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < items.size(); ++i) {
    ret = writer.write_item(*items.at(i));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  reader.reset();
  ret = writer.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = reader.init(writer.get_fd(), writer.get_dir_id(), expire_timestamp, OB_SYS_TENANT_ID, writer.get_sample_item(), 3 * buf_cap);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = reader.prefetch();
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < items.size(); ++i) {
    ret = reader.get_next_item(item);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(i, item->get_key());
  }
  ret = reader.get_next_item(item);
  ASSERT_EQ(OB_ITER_END, ret);

  // multiple macro buffers, total write bytes is more than capacity of buffers
  writer.reset();
  ret = writer.open(3 * buf_cap, expire_timestamp, OB_SYS_TENANT_ID, dir_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = generate_items(10000, true, items);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < items.size(); ++i) {
    ret = writer.write_item(*items.at(i));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  reader.reset();
  ret = writer.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = reader.init(writer.get_fd(), writer.get_dir_id(), expire_timestamp, OB_SYS_TENANT_ID, writer.get_sample_item(), 3 * buf_cap);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = reader.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < items.size(); ++i) {
    ret = reader.get_next_item(item);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(i, item->get_key());
  }
  ret = reader.get_next_item(item);
  ASSERT_EQ(OB_ITER_END, ret);
}

TEST_F(TestParallelExternalSort, test_merge)
{
  int64_t merge_count = 2;
  const int64_t buf_cap = MACRO_BLOCK_SIZE;

  // multiple way merge
  // 1. different items cnt
  // 2. different buf_cap

  // 1. different item count
  for (int64_t m = 0; m < 3; ++m) {
    merge_count = ObRandom::rand(2, 33);
    for (int64_t i = 100; i < 1000; i *= 10) {
      test_merge(buf_cap, i, merge_count);
    }

    //2. different buf_cap
    for (int64_t i = buf_cap; i < 3 * buf_cap; i += buf_cap) {
      test_merge(i, 100, merge_count);
    }
  }
}

TEST_F(TestParallelExternalSort, test_merge_dup)
{
  int64_t merge_count = 2;
  const int64_t buf_cap = MACRO_BLOCK_SIZE;
  test_merge_dup(buf_cap, 100, merge_count);
}

TEST_F(TestParallelExternalSort, test_sort_round)
{
  int64_t merge_count = 2;
  const int64_t buf_cap = MACRO_BLOCK_SIZE;

  // multiple way merge
  // 1. different items cnt
  // 2. different buf_cap

  // 1. different item count
  for (int64_t m = 0; m < 1; ++m) {
    merge_count = ObRandom::rand(2, 33);
    for (int64_t i = 100; i < 1000; i *= 10) {
      test_sort_round(buf_cap, i, merge_count);
    }

    //2. different buf_cap
    for (int64_t i = buf_cap; i < 2 * buf_cap; i += buf_cap) {
      test_sort_round(i, 1000, merge_count);
    }
  }
}

TEST_F(TestParallelExternalSort, test_multi_sort_round)
{
  int64_t merge_count = 2;
  const int64_t buf_cap = MACRO_BLOCK_SIZE;

  // multiple way merge
  // 1. different items cnt
  // 2. different buf_cap
  // 3. different task_cnt

  // 1. different item count
  for (int64_t m = 0; m < 3; ++m) {
    merge_count = ObRandom::rand(2, 33);
    for (int64_t i = 100; i < 1000; i *= 10) {
      test_multi_sort_round(buf_cap, i, 16, merge_count);
    }

    // 2. different buf_cap
    for (int64_t i = buf_cap; i < 3 * buf_cap; i += buf_cap) {
      test_multi_sort_round(i, 100, 16, merge_count);
    }

    // 3. different task cnt
    for (int64_t i = 2; i < 16; i += 7) {
      test_multi_sort_round(buf_cap, 100, i, merge_count);
    }
  }
}

TEST_F(TestParallelExternalSort, test_memory_sort_round)
{
  const int64_t buf_mem_limit = 8 * 1024 * 1024;
  // 1. different buf memory limit
  // 2. different items cnt


  for (int64_t i = buf_mem_limit; i < buf_mem_limit + 10 * MACRO_BLOCK_SIZE;
      i += MACRO_BLOCK_SIZE) {
    test_memory_sort_round(i, 100);
  }

  for (int64_t i = 100; i < 1000; i *= 10) {
    test_memory_sort_round(buf_mem_limit, i);
  }
}

TEST_F(TestParallelExternalSort, test_sort)
{
  const int64_t file_buf_size = MACRO_BLOCK_SIZE;
  const int64_t buf_mem_limit = 8 * 1024 * 1024;
  int64_t count = 0;

  //1. different buf_mem_limit
  for (int64_t i = buf_mem_limit; i < 2 * buf_mem_limit; i += 1024 * 1024) {
    test_sort(i, file_buf_size, 100);
    ++count;
  }

  //2 .different file buf size
  for (int64_t i = file_buf_size; i < buf_mem_limit / 4; i += file_buf_size) {
    test_sort(buf_mem_limit, i, 100);
    ++count;
  }

  //3. different item size
  for (int64_t i = 1000; i < 10000; i *= 10) {
    test_sort(buf_mem_limit, file_buf_size, i);
    ++count;
  }
}

TEST_F(TestParallelExternalSort, test_multi_task_sort)
{
  const int64_t file_buf_size = MACRO_BLOCK_SIZE;
  const int64_t task_cnt = 4;
  const int64_t buf_mem_limit = 8 * 1024 * 1024 * task_cnt;
  test_multi_task_sort(buf_mem_limit, file_buf_size, 10000, task_cnt);
}

TEST_F(TestParallelExternalSort, test_get_before_sort)
{
  int ret = OB_SUCCESS;
  const int64_t file_buf_size = MACRO_BLOCK_SIZE;
  const int64_t buf_mem_limit = 8 * 1024 * 1024L;
  typedef ObExternalSort<TestItem, TestItemCompare> ExternalSort;
  ExternalSort external_sort;
  ObVector<TestItem *>total_items;
  TestItemCompare compare(ret);
  const int64_t expire_timestamp = 0;
  ret = external_sort.init(buf_mem_limit, file_buf_size, expire_timestamp, OB_SYS_TENANT_ID, &compare);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = generate_items(10, false, total_items);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < total_items.size(); ++i) {
    ret = external_sort.add_item(*total_items.at(i));
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  const TestItem *item = NULL;
  ret = external_sort.get_next_item(item);
  ASSERT_EQ(OB_ITER_END, ret);
}

}  // end namespace common
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_parallel_external_sort.log*");
  OB_LOGGER.set_file_name("test_parallel_external_sort.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
