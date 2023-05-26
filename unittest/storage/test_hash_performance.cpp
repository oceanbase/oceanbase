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

# include <gtest/gtest.h>
# include "lib/container/ob_array.h"
# include "lib/container/ob_array_iterator.h"
# include "lib/hash/ob_hashmap.h"
# include "lib/hash/ob_array_hash_map.h"
# include "lib/hash/ob_cuckoo_hashmap.h"
# include "lib/alloc/ob_malloc_allocator.h"
# include "storage/blocksstable/ob_block_sstable_struct.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace unittest
{

class SortIndices
{
public:
  SortIndices(const common::ObIArray<blocksstable::MacroBlockId> &block_ids)
    : block_ids_(block_ids)
  {}
  bool operator()(int64_t i, int64_t j) const
  { return block_ids_.at(i) < block_ids_.at(j); }
private:
  const common::ObIArray<blocksstable::MacroBlockId> &block_ids_;
};

class TestHashMapPerformance : public ::testing::Test
{
public:
  TestHashMapPerformance();
  virtual ~TestHashMapPerformance();
  int prepare_hash_map(const int64_t count, const double load_factor);
  int prepare_array_hash_map(const int64_t count, const double load_factor);
  int prepare_cuckoo_hash_map(const int64_t count, const double load_factor);
  int prepare_array(const int64_t count);
  int load_hash_data(const common::ObIArray<blocksstable::MacroBlockId> &block_ids,
      const common::ObIArray<int64_t> &sstable_block_idx);
  int load_array_hash_data(const common::ObIArray<blocksstable::MacroBlockId> &block_ids,
      const common::ObIArray<int64_t> &sstable_block_idx);
  int load_array_data(const common::ObIArray<blocksstable::MacroBlockId> &block_ids,
      const common::ObIArray<int64_t> &sstable_block_idx);
  int load_cuckoo_hash_data(const common::ObIArray<blocksstable::MacroBlockId> &block_ids,
      const common::ObIArray<int64_t> &sstable_block_idx);
  int generate_data(const int64_t count, common::ObArray<blocksstable::MacroBlockId> &block_ids,
      common::ObArray<int64_t> &sstable_block_idx);
  int generate_query_data(const int64_t count, const int64_t max_block_id, common::ObArray<blocksstable::MacroBlockId> &block_ids);
  int test_hash_performance(const common::ObIArray<blocksstable::MacroBlockId> &block_ids);
  int test_array_hash_performance(const common::ObIArray<blocksstable::MacroBlockId> &block_ids);
  int test_array_performance(const common::ObIArray<blocksstable::MacroBlockId> &block_ids);
  int test_cuckoo_hash_performance(const common::ObIArray<blocksstable::MacroBlockId> &block_ids);
private:
  common::hash::ObHashMap<blocksstable::MacroBlockId, int64_t, common::hash::NoPthreadDefendMode> hash_map_;
  common::ObArrayHashMap<blocksstable::MacroBlockId, int64_t> array_hash_map_;
  common::hash::ObCuckooHashMap<blocksstable::MacroBlockId, int64_t> cuckoo_hash_map_;
  blocksstable::MacroBlockId *block_ids_;
  int64_t *sstable_block_idx_;
  int64_t block_count_;
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator cuckoo_allocator_;
};

TestHashMapPerformance::TestHashMapPerformance()
  : hash_map_(), block_ids_(NULL), sstable_block_idx_(NULL), block_count_(0),
    allocator_(ObModIds::OB_SSTABLE), cuckoo_allocator_(ObModIds::OB_SSTABLE_BLOCK_FILE)
{
}

TestHashMapPerformance::~TestHashMapPerformance()
{
  hash_map_.destroy();
  block_ids_ = NULL;
  sstable_block_idx_ = NULL;
  allocator_.reset();
}

int TestHashMapPerformance::prepare_hash_map(const int64_t count, const double load_factor)
{
  int ret = OB_SUCCESS;
  const double bucket_num_tmp = load_factor * static_cast<double>(count);
  const int64_t bucket_num = static_cast<int64_t>(bucket_num_tmp);
  if (hash_map_.created()) {
    hash_map_.destroy();
  }
  if (OB_FAIL(hash_map_.create(bucket_num, "TestService", "TestService"))) {
    STORAGE_LOG(WARN, "fail to create hash map", K(ret));
  }
  return ret;
}

int TestHashMapPerformance::prepare_array_hash_map(const int64_t count, const double load_factor)
{
  int ret = OB_SUCCESS;
  const double bucket_num_tmp = load_factor * static_cast<double>(count);
  const int64_t bucket_num = static_cast<int64_t>(bucket_num_tmp);
  STORAGE_LOG(INFO, "array hash map bucket num", K(bucket_num));
  if (OB_FAIL(array_hash_map_.init("TicketQueue", bucket_num))) {
    STORAGE_LOG(WARN, "fail to create hash map", K(ret));
  }
  return ret;
}

int TestHashMapPerformance::prepare_array(const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(block_ids_ = static_cast<blocksstable::MacroBlockId *>(allocator_.alloc(sizeof(blocksstable::MacroBlockId) * count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory for block ids", K(ret));
  } else if (OB_ISNULL(sstable_block_idx_ = static_cast<int64_t *>(allocator_.alloc(count * sizeof(int64_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory for block ids", K(ret));
  } else {
    block_count_ = count;
  }
  return ret;
}

int TestHashMapPerformance::prepare_cuckoo_hash_map(const int64_t count, const double load_factor)
{
  int ret = OB_SUCCESS;
  const double bucket_num_tmp = load_factor * static_cast<double>(count);
  const int64_t bucket_num = static_cast<int64_t>(bucket_num_tmp);
  cuckoo_hash_map_.destroy();
  if (OB_FAIL(cuckoo_hash_map_.create(bucket_num, &cuckoo_allocator_))) {
    STORAGE_LOG(WARN, "fail to create cuckoo hashmap", K(ret));
  }
  return ret;
}

int TestHashMapPerformance::generate_data(const int64_t count,
    common::ObArray<blocksstable::MacroBlockId> &block_ids,
    common::ObArray<int64_t> &sstable_block_idx)
{
  int ret = OB_SUCCESS;
  block_ids.reset();
  sstable_block_idx.reset();
  for (int64_t i = 1; OB_SUCC(ret) && i <= count; ++i) {
    blocksstable::MacroBlockId block_id(0, i, 0);
    if (OB_FAIL(block_ids.push_back(block_id))) {
      STORAGE_LOG(WARN, "fail to push back block id", K(ret));
    } else if (OB_FAIL(sstable_block_idx.push_back(i))) {
      STORAGE_LOG(WARN, "fail to push back sstable block idx", K(ret));
    }
  }
  std::random_shuffle(block_ids.begin(), block_ids.end());
  std::random_shuffle(sstable_block_idx.begin(), sstable_block_idx.end());
  return ret;
}

int TestHashMapPerformance::load_hash_data(const common::ObIArray<blocksstable::MacroBlockId> &block_ids,
    const common::ObIArray<int64_t> &sstable_block_idx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < block_ids.count(); ++i) {
    if (OB_FAIL(hash_map_.set_refactored(block_ids.at(i), sstable_block_idx.at(i)))) {
      STORAGE_LOG(WARN, "fail to set hashmap", K(ret));
    }
  }
  return ret;
}

int TestHashMapPerformance::load_array_hash_data(const common::ObIArray<blocksstable::MacroBlockId> &block_ids,
    const common::ObIArray<int64_t> &sstable_block_idx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < block_ids.count(); ++i) {
    if (OB_FAIL(array_hash_map_.insert(block_ids.at(i), sstable_block_idx.at(i)))) {
      STORAGE_LOG(WARN, "fail to set hashmap", K(ret));
    }
  }
  return ret;
}

int TestHashMapPerformance::load_array_data(const common::ObIArray<blocksstable::MacroBlockId> &block_ids,
    const common::ObIArray<int64_t> &sstable_block_idx)
{
  int ret = OB_SUCCESS;
  common::ObArray<int64_t> indices;
  for (int64_t i = 0; OB_SUCC(ret) && i < block_ids.count(); ++i) {
    if (OB_FAIL(indices.push_back(i))) {
      STORAGE_LOG(WARN, "fail to push back indice", K(ret));
    }
  }
  std::sort(indices.begin(), indices.end(), SortIndices(block_ids));
  for (int64_t i = 0; OB_SUCC(ret) && i < block_ids.count(); ++i) {
    block_ids_[i] = block_ids.at(indices[i]);
    sstable_block_idx_[i] = sstable_block_idx.at(indices[i]);
    if (i > 0) {
      ret = block_ids_[i-1] < block_ids_[i] ? OB_SUCCESS : OB_ERROR;
    }
  }
  return ret;
}

int TestHashMapPerformance::load_cuckoo_hash_data(const common::ObIArray<blocksstable::MacroBlockId> &block_ids,
    const common::ObIArray<int64_t> &sstable_block_idx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < block_ids.count(); ++i) {
    if (OB_FAIL(cuckoo_hash_map_.set(block_ids.at(i), sstable_block_idx.at(i)))) {
      STORAGE_LOG(WARN, "fail to set hashmap", K(ret));
    }
  }
  return ret;
}

int TestHashMapPerformance::generate_query_data(const int64_t count,
    const int64_t max_block_id,
    common::ObArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  ObRandom random;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    const int64_t block_idx = random.get(1, max_block_id);
    if (OB_FAIL(block_ids.push_back(blocksstable::MacroBlockId(0, block_idx, 0)))) {
      STORAGE_LOG(WARN, "fail to push back block idx", K(ret));
    }
  }
  return ret;
}

int TestHashMapPerformance::test_hash_performance(const common::ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  const int64_t count = block_ids.count();
  int64_t sstable_idx;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_FAIL(hash_map_.get_refactored(block_ids.at(i), sstable_idx))) {
      STORAGE_LOG(WARN, "fail to get from hashmap", K(ret));
    }
  }
  const int64_t end_time = ObTimeUtility::current_time();
  STORAGE_LOG(INFO, "hash query total time", "cost_time", end_time - start_time);
  return ret;
}

int TestHashMapPerformance::test_array_hash_performance(const common::ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  const int64_t count = block_ids.count();
  int64_t sstable_idx;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_FAIL(array_hash_map_.get(block_ids.at(i), sstable_idx))) {
      STORAGE_LOG(WARN, "fail to get from hashmap", K(ret));
    }
  }
  const int64_t end_time = ObTimeUtility::current_time();
  STORAGE_LOG(INFO, "array hash query total time", "cost_time", end_time - start_time);
  return ret;
}

int TestHashMapPerformance::test_array_performance(const common::ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  const int64_t count = block_ids.count();
  int64_t sstable_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    auto iter = std::lower_bound(block_ids_, block_ids_ + count, block_ids.at(i));
    sstable_idx = iter - block_ids_;
  }
  const int64_t end_time = ObTimeUtility::current_time();
  STORAGE_LOG(INFO, "array query total time", "cost_time", end_time - start_time, K(sstable_idx));
  return ret;
}

int TestHashMapPerformance::test_cuckoo_hash_performance(const common::ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  const int64_t count = block_ids.count();
  int64_t sstable_idx;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_FAIL(cuckoo_hash_map_.get(block_ids.at(i), sstable_idx))) {
      STORAGE_LOG(WARN, "fail to get from hashmap", K(ret));
    }
  }
  const int64_t end_time = ObTimeUtility::current_time();
  STORAGE_LOG(INFO, "cuckoo hashmap query total time", "cost_time", end_time - start_time);
  return ret;
}

TEST_F(TestHashMapPerformance, test_performance)
{

  // test count = 100000, load_factor = 1
  int64_t count = 1000000;
  ObArray<blocksstable::MacroBlockId> block_ids;
  ObArray<int64_t> sstable_block_idx;
  ObArray<blocksstable::MacroBlockId> query_block_ids;
  ASSERT_EQ(OB_SUCCESS, generate_data(count, block_ids, sstable_block_idx));
  ASSERT_EQ(OB_SUCCESS, prepare_hash_map(count, 1.0));
  ASSERT_EQ(OB_SUCCESS, prepare_array_hash_map(count, 1.0));
  ASSERT_EQ(OB_SUCCESS, prepare_cuckoo_hash_map(count, 1.2));
  ASSERT_EQ(OB_SUCCESS, prepare_array(count));
  ASSERT_EQ(OB_SUCCESS, load_hash_data(block_ids, sstable_block_idx));
  ASSERT_EQ(OB_SUCCESS, load_array_hash_data(block_ids, sstable_block_idx));
  ASSERT_EQ(OB_SUCCESS, load_array_data(block_ids, sstable_block_idx));
  ASSERT_EQ(OB_SUCCESS, load_cuckoo_hash_data(block_ids, sstable_block_idx));
  ASSERT_EQ(OB_SUCCESS, generate_query_data(count, count, query_block_ids));
  test_hash_performance(query_block_ids);
  test_array_hash_performance(query_block_ids);
  test_array_performance(query_block_ids);
  test_cuckoo_hash_performance(query_block_ids);
  lib::ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  lib::ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(500);
}
}
}

int main(int argc, char **argv)
{
  system("rm -f test_hash_performance.log*");
  OB_LOGGER.set_file_name("test_hash_performance.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test_hash_performance");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
