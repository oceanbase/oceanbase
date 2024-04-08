/**
 * Copyright (c) 2023 OceanBase
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
#define protected public
#include "lib/oblog/ob_log_module.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/access/ob_global_iterator_pool.h"
namespace oceanbase
{

using namespace share;
using namespace common;
using namespace storage;

class ObTestQueryRowIterator : public ObQueryRowIterator
{
public:
  virtual int get_next_row(blocksstable::ObDatumRow *&row)
  {
    return OB_SUCCESS;
  }
  virtual void reset()
  {
  }
  virtual void reclaim()
  {
  }
};

class ObGlobalIteratorPoolTest: public ::testing::Test
{
public:
  ObGlobalIteratorPoolTest();
  virtual ~ObGlobalIteratorPoolTest() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
private:
  uint64_t tenant_id_;
  ObTenantBase tenant_base_;
};

ObGlobalIteratorPoolTest::ObGlobalIteratorPoolTest()
  : tenant_id_(1),
    tenant_base_(tenant_id_)
{
}

void ObGlobalIteratorPoolTest::SetUpTestCase()
{
  ASSERT_EQ(MockTenantModuleEnv::get_instance().init(), OB_SUCCESS);
}

void ObGlobalIteratorPoolTest::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void ObGlobalIteratorPoolTest::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
}

void ObGlobalIteratorPoolTest::TearDown()
{
}

TEST_F(ObGlobalIteratorPoolTest, init)
{
  int ret = 0;
  ObGlobalIteratorPool iter_pool;
  iter_pool.tenant_mem_user_limit_ = ObGlobalIteratorPool::ITER_POOL_TENANT_MIN_MEM_THRESHOLD + 1;
  ret = iter_pool.init();
  ASSERT_EQ(ret, OB_SUCCESS);
  const int64_t mem_limit = iter_pool.tenant_mem_user_limit_ * ObGlobalIteratorPool::ITER_POOL_MAX_MEM_PERCENT;
  const int64_t bucket_cnt = mem_limit / (ObGlobalIteratorPool::ITER_POOL_ITER_MEM_LIMIT * (1 + ObGlobalIteratorPool::ITER_POOL_MAX_CACHED_ITER_TYPE));
  ASSERT_EQ(1, iter_pool.tenant_id_);
  ASSERT_EQ(bucket_cnt, iter_pool.bucket_cnt_);
  for (int64_t i = 0; i <= ObGlobalIteratorPool::ITER_POOL_MAX_CACHED_ITER_TYPE; ++i) {
    CachedIteratorNode *nodes = iter_pool.cached_node_array_[i];
    ASSERT_TRUE(nullptr != nodes);
    for (int64_t j = 0; j < iter_pool.bucket_cnt_; ++j) {
      CachedIteratorNode &node = nodes[j];
      ASSERT_FALSE(node.is_occupied_);
      ASSERT_TRUE(nullptr == node.iter_);
      ASSERT_TRUE(nullptr == node.stmt_iter_pool_);
    }
  }
}

TEST_F(ObGlobalIteratorPoolTest, get)
{
  int ret = 0;
  ObGlobalIteratorPool iter_pool;
  iter_pool.tenant_mem_user_limit_ = ObGlobalIteratorPool::ITER_POOL_TENANT_MIN_MEM_THRESHOLD + 1;
  ret = iter_pool.init();
  ASSERT_EQ(ret, OB_SUCCESS);
  ObQRIterType type = T_INVALID_ITER_TYPE;
  CachedIteratorNode *cached_node = nullptr;
  ret = iter_pool.get(type, cached_node);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ASSERT_TRUE(nullptr == cached_node);

  type = T_MAX_ITER_TYPE;
  ret = iter_pool.get(type, cached_node);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ASSERT_TRUE(nullptr == cached_node);

  type = T_SINGLE_GET;
  ret = iter_pool.get(type, cached_node);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != cached_node);
  ASSERT_TRUE(cached_node->is_occupied_);
  ASSERT_TRUE(nullptr == cached_node->iter_);
  ASSERT_TRUE(nullptr != cached_node->stmt_iter_pool_);

  ObArenaAllocator *iter_alloc = cached_node->get_iter_allocator();
  void *buf =iter_alloc->alloc(sizeof(ObTestQueryRowIterator));
  ASSERT_TRUE(nullptr != buf);
  ObTestQueryRowIterator *merge = new (buf) ObTestQueryRowIterator();
  cached_node->set_iter(merge);

  type = T_SINGLE_GET;
  CachedIteratorNode *cached_node1 = nullptr;
  ret = iter_pool.get(type, cached_node1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr == cached_node1);

  iter_pool.release(cached_node);
  ret = iter_pool.get(type, cached_node1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != cached_node1);
  ASSERT_TRUE(cached_node1->is_occupied_);
  ASSERT_TRUE(nullptr != cached_node1->iter_);
  ASSERT_TRUE(nullptr != cached_node1->stmt_iter_pool_);
  ASSERT_TRUE(merge == cached_node1->iter_);
  iter_pool.release(cached_node);

  type = T_MULTI_GET;
  CachedIteratorNode *mg_cached_node = nullptr;
  ret = iter_pool.get(type, mg_cached_node);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != mg_cached_node);
  ASSERT_TRUE(mg_cached_node->is_occupied_);
  ASSERT_TRUE(nullptr == mg_cached_node->iter_);
  ASSERT_TRUE(nullptr != mg_cached_node->stmt_iter_pool_);
  iter_pool.release(mg_cached_node);

  type = T_SINGLE_SCAN;
  CachedIteratorNode *ss_cached_node = nullptr;
  ret = iter_pool.get(type, ss_cached_node);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != ss_cached_node);
  ASSERT_TRUE(ss_cached_node->is_occupied_);
  ASSERT_TRUE(nullptr == ss_cached_node->iter_);
  ASSERT_TRUE(nullptr != ss_cached_node->stmt_iter_pool_);
  iter_pool.release(ss_cached_node);

  type = T_MULTI_SCAN;
  CachedIteratorNode *ms_cached_node = nullptr;
  ret = iter_pool.get(type, ms_cached_node);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ASSERT_TRUE(nullptr == ms_cached_node);
}

TEST_F(ObGlobalIteratorPoolTest, release)
{
  int ret = 0;
  ObGlobalIteratorPool iter_pool;
  iter_pool.tenant_mem_user_limit_ = ObGlobalIteratorPool::ITER_POOL_TENANT_MIN_MEM_THRESHOLD + 1;
  ret = iter_pool.init();
  ASSERT_EQ(ret, OB_SUCCESS);
  ObQRIterType type = T_MULTI_GET;
  CachedIteratorNode *cached_node = nullptr;
  ret = iter_pool.get(type, cached_node);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != cached_node);
  ASSERT_TRUE(cached_node->is_occupied_);
  ASSERT_TRUE(nullptr == cached_node->iter_);
  ASSERT_TRUE(nullptr != cached_node->stmt_iter_pool_);

  ObArenaAllocator *iter_alloc = cached_node->get_iter_allocator();
  void *buf = iter_alloc->alloc(sizeof(ObTestQueryRowIterator));
  ASSERT_TRUE(nullptr != buf);
  ObTestQueryRowIterator *merge = new (buf) ObTestQueryRowIterator();
  cached_node->set_iter(merge);
  iter_pool.release(cached_node);

  CachedIteratorNode *cached_node1 = nullptr;
  ret = iter_pool.get(type, cached_node1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != cached_node1);
  ASSERT_TRUE(cached_node1->is_occupied_);
  ASSERT_TRUE(nullptr != cached_node1->iter_);
  ASSERT_TRUE(nullptr != cached_node1->stmt_iter_pool_);
  ASSERT_TRUE(merge == cached_node1->iter_);

  ObArenaAllocator *iter_alloc1 = cached_node1->get_iter_allocator();
  ASSERT_TRUE(iter_alloc == iter_alloc1);
  ASSERT_TRUE(iter_alloc1->total() > 0);
  buf = iter_alloc1->alloc(ObGlobalIteratorPool::ITER_POOL_ITER_MEM_LIMIT + 1);
  ASSERT_TRUE(nullptr != buf);
  ASSERT_TRUE(iter_alloc1->total() > ObGlobalIteratorPool::ITER_POOL_ITER_MEM_LIMIT);
  iter_pool.release(cached_node1);

  CachedIteratorNode *cached_node2 = nullptr;
  ret = iter_pool.get(type, cached_node2);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != cached_node2);
  ASSERT_TRUE(cached_node2->is_occupied_);
  ASSERT_TRUE(nullptr == cached_node2->iter_);
  ASSERT_TRUE(nullptr != cached_node2->stmt_iter_pool_);
  ObArenaAllocator *iter_alloc2 = cached_node2->get_iter_allocator();
  ASSERT_TRUE(iter_alloc == iter_alloc2);
  ASSERT_TRUE(iter_alloc2->total() < ObGlobalIteratorPool::ITER_POOL_ITER_MEM_LIMIT);

  buf = iter_alloc->alloc(sizeof(ObTestQueryRowIterator));
  ASSERT_TRUE(nullptr != buf);
  merge = new (buf) ObTestQueryRowIterator();
  cached_node2->set_iter(merge);
  cached_node2->set_exception_occur(true);
  iter_pool.release(cached_node2);

  CachedIteratorNode *cached_node3 = nullptr;
  ret = iter_pool.get(type, cached_node3);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != cached_node3);
  ASSERT_TRUE(cached_node3->is_occupied_);
  ASSERT_TRUE(nullptr == cached_node3->iter_);
  ASSERT_TRUE(nullptr != cached_node3->stmt_iter_pool_);
  ObArenaAllocator *iter_alloc3 = cached_node3->get_iter_allocator();
  ASSERT_TRUE(iter_alloc == iter_alloc3);
  ASSERT_TRUE(iter_alloc3->total() < ObGlobalIteratorPool::ITER_POOL_ITER_MEM_LIMIT);
}

TEST_F(ObGlobalIteratorPoolTest, destroy)
{
  int ret = 0;
  ObGlobalIteratorPool iter_pool;
  iter_pool.tenant_mem_user_limit_ = ObGlobalIteratorPool::ITER_POOL_TENANT_MIN_MEM_THRESHOLD + 1;
  ret = iter_pool.init();
  ASSERT_EQ(ret, OB_SUCCESS);

  ObQRIterType type = T_SINGLE_SCAN;
  CachedIteratorNode *cached_node = nullptr;
  ret = iter_pool.get(type, cached_node);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != cached_node);
  ASSERT_TRUE(cached_node->is_occupied_);
  ASSERT_TRUE(nullptr == cached_node->iter_);
  ASSERT_TRUE(nullptr != cached_node->stmt_iter_pool_);

  ObArenaAllocator *iter_alloc = cached_node->get_iter_allocator();
  void *buf = iter_alloc->alloc(sizeof(ObTestQueryRowIterator));
  ASSERT_TRUE(nullptr != buf);
  ObTestQueryRowIterator *merge = new (buf) ObTestQueryRowIterator();
  cached_node->set_iter(merge);
  iter_pool.release(cached_node);

  iter_pool.destroy();
  ASSERT_EQ(OB_INVALID_TENANT_ID, iter_pool.tenant_id_);
  ASSERT_EQ(0, iter_pool.bucket_cnt_);
  for (int64_t i = 0; i <= ObGlobalIteratorPool::ITER_POOL_MAX_CACHED_ITER_TYPE; ++i) {
    CachedIteratorNode *nodes = iter_pool.cached_node_array_[i];
    ASSERT_TRUE(nullptr == nodes);
  }
}

TEST_F(ObGlobalIteratorPoolTest, wash)
{
  int ret = 0;
  const int64_t tenant_mem_limit = lib::get_tenant_memory_limit(tenant_id_);
  const int64_t tenant_mem_hold = lib::get_tenant_memory_hold(tenant_id_);
  ObGlobalIteratorPool iter_pool;
  iter_pool.tenant_mem_user_limit_ = tenant_mem_limit;
  ret = iter_pool.init();
  ASSERT_EQ(ret, OB_SUCCESS);
  ObQRIterType type = T_SINGLE_SCAN;
  CachedIteratorNode *cached_node = nullptr;
  ret = iter_pool.get(type, cached_node);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != cached_node);
  ASSERT_TRUE(cached_node->is_occupied_);
  ASSERT_TRUE(nullptr == cached_node->iter_);

  ObArenaAllocator *iter_alloc = cached_node->get_iter_allocator();
  void *buf = iter_alloc->alloc(sizeof(ObTestQueryRowIterator));
  ASSERT_TRUE(nullptr != buf);
  ObTestQueryRowIterator *merge = new (buf) ObTestQueryRowIterator();
  cached_node->set_iter(merge);
  buf = iter_alloc->alloc(256 * 1024);
  ASSERT_TRUE(nullptr != buf);

  ASSERT_EQ(tenant_mem_limit, iter_pool.tenant_mem_user_limit_);

  ASSERT_TRUE(tenant_mem_limit > tenant_mem_hold) << "tenant_mem_hold=" << tenant_mem_hold
                                                  << "tenant_mem_hold=" << tenant_mem_hold;
  const int64_t test_tenant_mem_limit_low = tenant_mem_hold * 100 / ObGlobalIteratorPool::ITER_POOL_WASH_HIGH_THRESHOLD;

  set_tenant_memory_limit(tenant_id_, test_tenant_mem_limit_low - 10);
  iter_pool.wash();
  STORAGE_LOG(INFO, "after wash", K(iter_pool), K(test_tenant_mem_limit_low));
  ASSERT_FALSE(iter_pool.is_washing_);
  ASSERT_TRUE(iter_pool.is_disabled_);

  iter_pool.release(cached_node);
  ret = iter_pool.get(type, cached_node);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr == cached_node);
  STORAGE_LOG(INFO, "after release", K(iter_pool));

  set_tenant_memory_limit(tenant_id_, tenant_mem_limit);
  iter_pool.wash();
  STORAGE_LOG(INFO, "after wash", K(iter_pool), K(test_tenant_mem_limit_low));
  ASSERT_FALSE(iter_pool.is_washing_);
  ASSERT_FALSE(iter_pool.is_disabled_);

  set_tenant_memory_limit(tenant_id_, tenant_mem_hold + 10);
  iter_pool.wash();
  STORAGE_LOG(INFO, "after wash", K(iter_pool), K(test_tenant_mem_limit_low));
  ASSERT_FALSE(iter_pool.is_washing_);
  ASSERT_TRUE(iter_pool.is_disabled_);
  ASSERT_EQ(tenant_mem_hold + 10, iter_pool.tenant_mem_user_limit_);

  set_tenant_memory_limit(tenant_id_, tenant_mem_limit);
  iter_pool.wash();
  STORAGE_LOG(INFO, "after wash", K(iter_pool), K(test_tenant_mem_limit_low));
  ASSERT_FALSE(iter_pool.is_washing_);
  ASSERT_FALSE(iter_pool.is_disabled_);
  ASSERT_EQ(tenant_mem_limit, iter_pool.tenant_mem_user_limit_);
}

}

int main(int argc, char **argv)
{
  system("rm -f test_global_iterator_pool.log*");
  OB_LOGGER.set_file_name("test_global_iterator_pool.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
