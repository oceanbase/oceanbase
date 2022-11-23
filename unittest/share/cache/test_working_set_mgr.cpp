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

#define USING_LOG_PREFEX COMMON

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#define private public
#include "share/cache/ob_working_set_mgr.h"
#include "share/cache/ob_kv_storecache.h"
#include "ob_cache_test_utils.h"

using ::testing::_;

namespace oceanbase
{
namespace common
{
TEST(TestWorkingSet, common)
{
  ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().init());
  ObWorkingSetMgr ws_mgr;
  ASSERT_EQ(OB_SUCCESS, ws_mgr.init(ObKVGlobalCache::get_instance().store_));
  ObFixedQueue<WorkingSetMB> &ws_mb_pool = ws_mgr.ws_mb_pool_;
  ObWorkingSet working_set;

  // not init
  const int64_t SIZE = 1024;
  WSListKey ws_list_key;
  ws_list_key.tenant_id_ = 1;
  ws_list_key.cache_id_ = 0;
  ObKVCacheInstHandle inst_handle;
  ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().insts_.get_cache_inst(ws_list_key, inst_handle));
  ObKVCacheInst &inst = *inst_handle.get_inst();
  TestKVCacheKey<SIZE> key;
  TestKVCacheValue<SIZE> value;
  ObKVCachePair *kvpair = NULL;
  ObKVMemBlockHandle *mb_handle = NULL;
  WorkingSetMB *mb_wrapper = NULL;

  const int64_t limit = 10 * 1024 * 1024; // 10MB
  ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().store_.alloc_mbhandle(ws_list_key, mb_handle));
  ASSERT_EQ(OB_SUCCESS, working_set.init(ws_list_key, limit, mb_handle,
      ws_mb_pool, ObKVGlobalCache::get_instance().store_));
  ASSERT_TRUE(working_set.is_valid());

  const int64_t kv_cnt = 2 * limit / SIZE;
  for (int64_t i = 0; i < kv_cnt; ++i) {
    key.v_ = i;
    ASSERT_EQ(OB_SUCCESS, working_set.store(inst, key, value, kvpair, mb_wrapper));
    ObKVGlobalCache::get_instance().revert(mb_wrapper->mb_handle_);
  }
  COMMON_LOG(INFO, "xxx", "used", working_set.used_, K(limit));
  ASSERT_TRUE(working_set.used_ < limit + working_set.cur_->block_size_);
  ASSERT_TRUE(NULL != working_set.get_curr_mb());
  working_set.reset();

  // do again
  ASSERT_EQ(OB_SUCCESS, working_set.init(ws_list_key, limit, mb_handle,
      ws_mb_pool, ObKVGlobalCache::get_instance().store_));
  ASSERT_TRUE(working_set.is_valid());
  for (int64_t i = 0; i < kv_cnt; ++i) {
    key.v_ = i;
    ASSERT_EQ(OB_SUCCESS, working_set.store(inst, key, value, kvpair, mb_wrapper));
    ObKVGlobalCache::get_instance().revert(mb_wrapper->mb_handle_);
  }
  ASSERT_TRUE(working_set.used_ < limit + working_set.cur_->block_size_);
  COMMON_LOG(INFO, "xxx", "used", working_set.used_, K(limit));
  inst_handle.reset();
  ObKVGlobalCache::get_instance().destroy();
}

TEST(TestWorkingSetList, common)
{
  ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().init());
  ObWorkingSetMgr::WorkingSetList ws_list;
  ObWorkingSetMgr ws_mgr;
  ASSERT_EQ(OB_SUCCESS, ws_mgr.init(ObKVGlobalCache::get_instance().store_));
  ObKVCacheStore &store = ObKVGlobalCache::get_instance().store_;
  // not init
  ObWorkingSet ws;
  ObKVMemBlockHandle *mb_handle = NULL;
  ASSERT_EQ(OB_NOT_INIT, ws_list.add_ws(&ws));
  ASSERT_EQ(OB_NOT_INIT, ws_list.del_ws(&ws));
  ASSERT_EQ(OB_NOT_INIT, ws_list.pop_mb_handle(mb_handle));
  ASSERT_EQ(OB_NOT_INIT, ws_list.push_mb_handle(mb_handle));

  // init
  WSListKey list_key;
  list_key.tenant_id_ = OB_INVALID_ID;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ws_list.init(list_key, store));
  list_key.tenant_id_ = OB_SYS_TENANT_ID;
  list_key.cache_id_ = 1;
  ASSERT_EQ(OB_SUCCESS, ws_list.init(list_key, store));
  ASSERT_EQ(OB_INIT_TWICE, ws_list.init(list_key, store));

  ObArenaAllocator allocator;
  ObArray<ObWorkingSet *> working_sets;
  const int64_t limit = 1024;
  const int64_t count = 100;
  int64_t sum_limit = 0;
  ObKVMemBlockHandle new_mb_handle;
  new_mb_handle.handle_ref_.inc_ref_cnt();
  ObFixedQueue<WorkingSetMB> &ws_mb_pool = ws_mgr.ws_mb_pool_;

  for (int64_t i = 0; i < count; ++i) {
    void *buf = allocator.alloc(sizeof(ObWorkingSet));
    ASSERT_TRUE(NULL != buf);
    ObWorkingSet *ws = new (buf) ObWorkingSet();
    ObKVMemBlockHandle *mb_handle = NULL;
    ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().store_.alloc_mbhandle(list_key, mb_handle));
    ASSERT_EQ(OB_SUCCESS, ws->init(list_key, limit, mb_handle,
        ws_mb_pool, ObKVGlobalCache::get_instance().store_));
    ASSERT_EQ(OB_SUCCESS, working_sets.push_back(ws));
    sum_limit += limit;
  }

  for (int64_t i = 0; i < count; ++i) {
    ASSERT_EQ(OB_SUCCESS, ws_list.add_ws(working_sets.at(i)));
  }
  ASSERT_EQ(count, ws_list.list_.get_size());
  ASSERT_EQ(sum_limit, ws_list.limit_sum_);

  for (int64_t i = 0; i < count; ++i) {
    ASSERT_EQ(OB_SUCCESS, ws_list.del_ws(working_sets.at(i)));
  }
  ASSERT_EQ(0, ws_list.list_.get_size());
  ASSERT_EQ(0, ws_list.limit_sum_);

  for (int64_t i = 0; i < ObWorkingSetMgr::WorkingSetList::FREE_ARRAY_SIZE; ++i) {
    ASSERT_EQ(OB_SUCCESS, ws_list.push_mb_handle(&new_mb_handle));
  }
  ASSERT_EQ(OB_SIZE_OVERFLOW, ws_list.push_mb_handle(&new_mb_handle));
  ObKVMemBlockHandle *rt_handle = NULL;
  for (int64_t i = 0; i < ObWorkingSetMgr::WorkingSetList::FREE_ARRAY_SIZE; ++i) {
    ASSERT_EQ(OB_SUCCESS, ws_list.pop_mb_handle(rt_handle));
    ASSERT_EQ(rt_handle, &new_mb_handle);
    new_mb_handle.status_ = FULL;
  }
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ws_list.pop_mb_handle(rt_handle));

  for (int64_t i = 0; i < ObWorkingSetMgr::WorkingSetList::FREE_ARRAY_SIZE; ++i) {
    ASSERT_EQ(OB_SUCCESS, ws_list.push_mb_handle(&new_mb_handle));
  }
  ObKVGlobalCache::get_instance().destroy();
}

TEST(TestWorkingSetMgr, common)
{
  ObWorkingSetMgr mgr;
  ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().init());
  uint64_t tenant_id = 1;
  uint64_t cache_id = 1;
  ObArray<ObWorkingSet *> ws_array;
  ObWorkingSet *ws = NULL;
  const int64_t limit = 1024;
  WSListKey key;
  ASSERT_EQ(OB_NOT_INIT, mgr.create_working_set(key, limit, ws));
  ASSERT_EQ(OB_NOT_INIT, mgr.delete_working_set(ws));
  ASSERT_EQ(OB_SUCCESS, mgr.init(ObKVGlobalCache::get_instance().store_));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.create_working_set(key, limit, ws));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.delete_working_set(NULL));

  for (int64_t i = 0; i < 20; ++i) {
    tenant_id = i + 1;
    key.tenant_id_ = tenant_id;
    key.cache_id_ = cache_id;
    ASSERT_EQ(OB_SUCCESS, mgr.create_working_set(key, limit, ws));
    ASSERT_EQ(OB_SUCCESS, ws_array.push_back(ws));
  }

  for (int64_t i = 0; i < ws_array.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, mgr.delete_working_set(ws_array.at(i)));
  }

  for (int64_t i = 0; i < 20; ++i) {
    tenant_id = i % 20 + 1;
    key.tenant_id_ = tenant_id;
    key.cache_id_ = cache_id;
    ASSERT_EQ(OB_SUCCESS, mgr.create_working_set(key, limit, ws));
    ASSERT_EQ(OB_SUCCESS, ws_array.push_back(ws));
  }
  ObKVGlobalCache::get_instance().destroy();
}

}//end namespace common
}//end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
