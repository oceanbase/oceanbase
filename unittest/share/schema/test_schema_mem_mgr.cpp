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

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/random/ob_random.h"
#define private public
#include "share/schema/ob_schema_mgr.h"
#include "share/schema/ob_schema_mem_mgr.h"

namespace oceanbase
{
using namespace share::schema;

namespace common
{

class TestSchemaMemMgr: public ::testing::Test
{
};

TEST_F(TestSchemaMemMgr, basic_test)
{
  int ret = OB_SUCCESS;
  ObSchemaMgr *mgr = NULL;
  int pos = -1;

  void *ptr = NULL;
  ObIAllocator *allocator = NULL;
  ObSchemaMemMgr mem_mgr;
  // not init when alloc
  ret = mem_mgr.alloc(sizeof(ObSchemaMgr), ptr, &allocator);
  ASSERT_EQ(OB_INNER_STAT_ERROR, ret);
  ret = mem_mgr.init(ObModIds::OB_SCHEMA_MGR);
  ASSERT_EQ(OB_SUCCESS, ret);
  // invalid arg when alloc
  ret = mem_mgr.alloc(-1, ptr, &allocator);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = mem_mgr.alloc(0, ptr, &allocator);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = mem_mgr.alloc(sizeof(ObSchemaMgr), ptr, &allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != ptr && NULL != allocator);
  pos = mem_mgr.pos_;
  ASSERT_EQ(1, mem_mgr.ptrs_[pos].count());
  ASSERT_EQ(0, mem_mgr.ptrs_[1 - pos].count());
  ASSERT_EQ(ptr, mem_mgr.ptrs_[pos].at(0));
  ASSERT_EQ(allocator, &mem_mgr.allocer_[pos]);
  mgr = new (ptr) ObSchemaMgr(*allocator);
  mgr->dump();

  // not init when free
  mem_mgr.is_inited_ = false;
  ret = mem_mgr.free(static_cast<void *>(mgr));
  ASSERT_EQ(OB_INNER_STAT_ERROR, ret);
  mem_mgr.is_inited_ = true;
  // invalid arg when free
  ret = mem_mgr.free(NULL);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = mem_mgr.free(static_cast<void *>(mgr));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, mem_mgr.ptrs_[pos].count());
  int64_t alloc_cnt = 0;
  ret = mem_mgr.get_cur_alloc_cnt(alloc_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, alloc_cnt);
  ret = mem_mgr.free(static_cast<void *>(mgr));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, mem_mgr.ptrs_[pos].count());
  ret = mem_mgr.get_cur_alloc_cnt(alloc_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, alloc_cnt);
  ASSERT_TRUE(0 != mem_mgr.allocer_[pos].used());
  bool can_switch = false;
  ret = mem_mgr.check_can_switch_allocer(can_switch);
  ASSERT_TRUE(can_switch);
  ret = mem_mgr.switch_allocer();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(0 != mem_mgr.allocer_[pos].used());
  ret = mem_mgr.check_can_switch_allocer(can_switch);
  ASSERT_TRUE(can_switch);
  ret = mem_mgr.switch_allocer();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(0 == mem_mgr.allocer_[pos].used());
  mem_mgr.dump();
}

TEST_F(TestSchemaMemMgr, NULL_ptr)
{
  int ret = OB_SUCCESS;

  ObSchemaMemMgr mem_mgr;
  ret = mem_mgr.init(ObModIds::OB_SCHEMA_MGR);
  ASSERT_EQ(OB_SUCCESS, ret);

  // free when ptrs array contains NULL ptr.
  mem_mgr.ptrs_[0].push_back(NULL);
  ret = mem_mgr.free(static_cast<void *>(this));
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  mem_mgr.ptrs_[0].reset();

  // free when two ptrs array have the same ptr.
  mem_mgr.ptrs_[0].push_back(static_cast<void *>(this));
  mem_mgr.ptrs_[1].push_back(static_cast<void *>(this));
  ret = mem_mgr.free(static_cast<void *>(this));
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
}

TEST_F(TestSchemaMemMgr, check_can_switch_allocer)
{
  int ret = OB_SUCCESS;

  ObSchemaMemMgr mem_mgr;
  bool can_switch = false;
  // not init
  ret = mem_mgr.check_can_switch_allocer(can_switch);
  ASSERT_EQ(OB_INNER_STAT_ERROR, ret);
  ret = mem_mgr.init(ObModIds::OB_SCHEMA_MGR);
  ASSERT_EQ(OB_SUCCESS, ret);
  // can switch
  ret = mem_mgr.check_can_switch_allocer(can_switch);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(can_switch);
  // cannot switch
  mem_mgr.ptrs_[1].push_back(static_cast<void *>(this));
  ret = mem_mgr.check_can_switch_allocer(can_switch);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(can_switch);

}

TEST_F(TestSchemaMemMgr, switch_allocer)
{
  int ret = OB_SUCCESS;

  ObSchemaMemMgr mem_mgr;
  ret = mem_mgr.switch_allocer();
  ASSERT_EQ(OB_INNER_STAT_ERROR, ret);
  // not init
  ret = mem_mgr.init(ObModIds::OB_SCHEMA_MGR);
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_mgr.ptrs_[1].push_back(static_cast<void *>(this));
  mem_mgr.all_ptrs_[1].push_back(static_cast<void *>(this));
  ret = mem_mgr.switch_allocer();
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  mem_mgr.ptrs_[1].reset();
  ret = mem_mgr.switch_allocer();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, mem_mgr.all_ptrs_[1].count());

  // free when two ptrs array have the same ptr.
  mem_mgr.ptrs_[0].push_back(static_cast<void *>(this));
  mem_mgr.ptrs_[1].push_back(static_cast<void *>(this));
  ret = mem_mgr.free(static_cast<void *>(this));
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
}

// It's not matched with the real elimination strategy of increment schema refresh.
// Eliminate a random ObSchemaMgr at a time.
TEST_F(TestSchemaMemMgr, simulate_increment_refresh_schema)
{
  int ret = OB_SUCCESS;

  static const int min_switch_alloc_cnt = 32;
  static const int cache_size = 8;
  ObSchemaMgr *schema_mgr_for_cache = NULL;
  ObSchemaMgr *mgr_cache[cache_size];
  memset(mgr_cache, 0, sizeof(ObSchemaMgr*) * cache_size);
  ObSchemaMemMgr mem_mgr;
  ret = mem_mgr.init(ObModIds::OB_SCHEMA_MGR);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int alloc_size = sizeof(ObSchemaMgr);
  ObSchemaMgr *mgr = NULL;
  // init schema_mgr_for_cache
  void *tmp_ptr = NULL;
  ObIAllocator *allocator = NULL;
  ret = mem_mgr.alloc(alloc_size, tmp_ptr, &allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  schema_mgr_for_cache = new (tmp_ptr) ObSchemaMgr(*allocator);

  static const int refresh_times = 1024;
  int64_t switch_cnt = 0;
  for (int64_t i = 0; i < refresh_times; ++i) {
    schema_mgr_for_cache->set_schema_version(1);
    // eliminate randomly
    int64_t eli_pos = ObRandom::rand(0, cache_size - 1);
    void *eli_ptr = static_cast<void *>(mgr_cache[eli_pos]);
    ret = mem_mgr.alloc(alloc_size, tmp_ptr);
    ASSERT_EQ(OB_SUCCESS, ret);
    mgr = new (tmp_ptr) ObSchemaMgr;
    mgr->assign(*schema_mgr_for_cache);
    mgr_cache[eli_pos] = mgr;
    if (NULL != eli_ptr) {
      bool can_switch = false;
      int64_t alloc_cnt = 0;
      ret = mem_mgr.free(eli_ptr);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = mem_mgr.check_can_switch_allocer(can_switch);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = mem_mgr.get_cur_alloc_cnt(alloc_cnt);
      ASSERT_EQ(OB_SUCCESS, ret);
      LOG_INFO("debug", K(tmp_ptr), K(eli_pos), K(eli_ptr), K(alloc_cnt),
               K(mem_mgr.ptrs_[0].count()), K(mem_mgr.ptrs_[1].count()));
      if (can_switch && alloc_cnt > min_switch_alloc_cnt) {
        LOG_INFO("switch allocator");
        ++switch_cnt;
        // overwrite schema_mgr_for_cache
        void *tmp_ptr = NULL;
        ObIAllocator *allocator = NULL;
        ObSchemaMgr *old_mgr = schema_mgr_for_cache;
        ObSchemaMgr *new_mgr = NULL;
        ret = mem_mgr.switch_allocer();
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = mem_mgr.alloc(alloc_size, tmp_ptr, &allocator);
        ASSERT_EQ(OB_SUCCESS, ret);
        new_mgr = new (tmp_ptr) ObSchemaMgr(*allocator);
        new_mgr->deep_copy(*old_mgr);
        ret = mem_mgr.free(static_cast<void *> (old_mgr));
        ASSERT_EQ(OB_SUCCESS, ret);
        schema_mgr_for_cache = new_mgr;
        // expect no core
        schema_mgr_for_cache->dump();
      }
    }
  }
  // check
  for (int64_t i = 0; i < cache_size; ++i) {
    ObSchemaMgr *mgr = mgr_cache[i];
    bool exist = false;
    for (int64_t j = 0; j < mem_mgr.ptrs_[0].count() && !exist; ++j) {
      exist = mgr == mem_mgr.ptrs_[0].at(j);
    }
    for (int64_t j = 0; j < mem_mgr.ptrs_[1].count() && !exist; ++j) {
      exist = mgr == mem_mgr.ptrs_[1].at(j);
    }
    ASSERT_TRUE(exist);
  }
  bool exist = false;
  for (int64_t j = 0; j < mem_mgr.ptrs_[0].count() && !exist; ++j) {
    exist = schema_mgr_for_cache == mem_mgr.ptrs_[0].at(j);
  }
  for (int64_t j = 0; j < mem_mgr.ptrs_[1].count() && !exist; ++j) {
    exist = schema_mgr_for_cache == mem_mgr.ptrs_[1].at(j);
  }
  ASSERT_TRUE(exist);
  ASSERT_TRUE(switch_cnt > 10);
}

} // common
} // oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_schema_mem_mgr.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
