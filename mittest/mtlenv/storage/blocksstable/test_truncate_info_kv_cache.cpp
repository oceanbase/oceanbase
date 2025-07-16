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
#define protected public
#define private public
#include "storage/truncate_info/ob_truncate_info.h"
#include "storage/truncate_info/ob_truncate_info_array.h"
#include "unittest/storage/ob_truncate_info_helper.h"
#include "storage/truncate_info/ob_truncate_info_kv_cache.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace compaction;
static ObSimpleMemLimitGetter getter;
namespace unittest
{
class TestTruncateInfoKVCache : public ::testing::Test
{
public:
  TestTruncateInfoKVCache() {}
  virtual ~TestTruncateInfoKVCache() {}
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  ObArenaAllocator allocator_;
};

void TestTruncateInfoKVCache::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestTruncateInfoKVCache::SetUp()
{
  int ret = OB_SUCCESS;

  const int64_t bucket_num = 1024L;
  const int64_t max_cache_size = 1024L * 1024L * 512;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;

  ASSERT_EQ(true, MockTenantModuleEnv::get_instance().is_inited());
  if (!ObKVGlobalCache::get_instance().inited_) {
    ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().init(&getter,
        bucket_num,
        max_cache_size,
        block_size));
  }
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  ASSERT_EQ(true, tenant_config.is_valid());
}

void TestTruncateInfoKVCache::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestTruncateInfoKVCache::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
}

TEST_F(TestTruncateInfoKVCache, truncate_info_cache_deep_copy)
{
  ObTruncateInfo truncate_info;
  ObTruncateInfoValueHandle cache_handle;
  const int64_t list_val_cnt = 3;
  int64_t list_vals[] = {200, 300, 100};
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, list_vals, list_val_cnt, truncate_info.truncate_part_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 1, truncate_info.truncate_part_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, 100, 200, truncate_info.truncate_subpart_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 2, truncate_info.truncate_subpart_));

  TruncateInfoHelper::mock_truncate_info(allocator_, 1/*trans_id*/, 100/*schema_version*/, 1000/*commit_version*/, truncate_info);
  truncate_info.is_sub_part_ = true;
  ASSERT_TRUE(truncate_info.is_valid());

  const int64_t BUF_LEN = 10 * 1024;
  ObArenaAllocator str_alloctor;
  char *buf = (char *)str_alloctor.alloc(sizeof(char) * BUF_LEN);
  ASSERT_TRUE(nullptr != buf);
  const int64_t last_major_snapshot = 1;

  // test deep copy
  int64_t pos = 0;
  ObTruncateInfo dst_info;
  ASSERT_EQ(OB_SUCCESS, truncate_info.deep_copy(buf, BUF_LEN, pos, dst_info));
  ASSERT_EQ(pos, truncate_info.get_deep_copy_size());

  bool equal = false;
  ASSERT_EQ(OB_SUCCESS, dst_info.compare(truncate_info, equal));
  ASSERT_TRUE(equal);

  // write cache with one truncate info
  ObTruncateInfoKVCache &truncate_info_cache = ObStorageCacheSuite::get_instance().get_truncate_info_cache();
  ObTruncateInfoArray truncate_info_array;
  ASSERT_EQ(OB_SUCCESS, truncate_info_array.init_for_first_creation(allocator_));
  ASSERT_EQ(OB_SUCCESS, truncate_info_array.append_with_deep_copy(truncate_info));

  ObTabletID tablet_id(10001);
  ObTruncateInfoCacheValue cache_value;
  ObTruncateInfoCacheValue cache_value2;
  {
    ObTruncateInfoCacheKey cache_key(MTL_ID(), tablet_id, truncate_info.schema_version_, last_major_snapshot);
    ASSERT_EQ(OB_SUCCESS, cache_value.init(1/*count*/, &truncate_info));
    ASSERT_EQ(cache_value.size(), sizeof(ObTruncateInfoCacheValue) + sizeof(ObTruncateInfo) + truncate_info.get_deep_copy_size());
    ASSERT_EQ(OB_SUCCESS, truncate_info_cache.put_truncate_info_array(cache_key, cache_value));

    equal = false;
    ASSERT_EQ(OB_SUCCESS, truncate_info_cache.get_truncate_info_array(cache_key, cache_handle));
    if (cache_handle.is_valid() && cache_handle.value_->is_valid()) {
      ASSERT_EQ(1, cache_handle.value_->count_);
      ASSERT_EQ(OB_SUCCESS, cache_handle.value_->truncate_info_array_[0].compare(truncate_info, equal));
    }
    ASSERT_TRUE(equal);
  }

  // write cache with several truncate info
  truncate_info.destroy();
  int64_t list_vals2[] = {700, 900, 300};
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, list_vals2, list_val_cnt, truncate_info.truncate_part_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 1, truncate_info.truncate_part_));
  TruncateInfoHelper::mock_truncate_info(allocator_, 2/*trans_id*/, 200/*schema_version*/, 2000/*commit_version*/, truncate_info);
  ASSERT_TRUE(truncate_info.is_valid());
  ASSERT_EQ(OB_SUCCESS, truncate_info_array.append_with_deep_copy(truncate_info));
  ASSERT_EQ(2, truncate_info_array.count());
  {
    ObTruncateInfoCacheKey cache_key(MTL_ID(), tablet_id, truncate_info.schema_version_, last_major_snapshot);
    ASSERT_EQ(OB_SUCCESS, ObTruncateInfoKVCacheUtil::put_truncate_info_array(cache_key, truncate_info_array.truncate_info_array_));

    cache_handle.reset();
    equal = false;
    ASSERT_EQ(OB_SUCCESS, truncate_info_cache.get_truncate_info_array(cache_key, cache_handle));
    if (cache_handle.is_valid() && cache_handle.value_->is_valid()) {
      ASSERT_EQ(2, cache_handle.value_->count_);
      ASSERT_EQ(OB_SUCCESS, cache_handle.value_->truncate_info_array_[0].compare(*truncate_info_array.at(0), equal));
      ASSERT_EQ(OB_SUCCESS, cache_handle.value_->truncate_info_array_[1].compare(*truncate_info_array.at(1), equal));
    }
    ASSERT_TRUE(equal);
  }

  dst_info.destroy();
  truncate_info.destroy();
  truncate_info_cache.destroy();
}


}//end namespace unittest
}//end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_truncate_info_kv_cache.log*");
  OB_LOGGER.set_file_name("test_truncate_info_kv_cache.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}