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

#define USING_LOG_PREFIX STORAGE
#include "gtest/gtest.h"
#include <thread>
#define private public
#define protected public

#include "storage/shared_storage/ob_disk_space_meta.h"

namespace oceanbase 
{
namespace storage 
{
using namespace oceanbase::common;

class TestSSDiskSpaceMeta : public ::testing::Test 
{
public:
  TestSSDiskSpaceMeta();
  virtual ~TestSSDiskSpaceMeta();
  virtual void SetUp();
  virtual void TearDown();
};

TestSSDiskSpaceMeta::TestSSDiskSpaceMeta()
{}

TestSSDiskSpaceMeta::~TestSSDiskSpaceMeta()
{}

void TestSSDiskSpaceMeta::SetUp()
{}

void TestSSDiskSpaceMeta::TearDown()
{}

TEST_F(TestSSDiskSpaceMeta, tenant_cache_disk_info)
{
  ObTenantDiskCacheRatioInfo disk_cache_ratio_info;
  ASSERT_EQ(true, disk_cache_ratio_info.is_valid());
  disk_cache_ratio_info.micro_cache_size_pct_ += 1;
  ASSERT_EQ(false, disk_cache_ratio_info.is_valid());
  disk_cache_ratio_info.reset();
  disk_cache_ratio_info.micro_cache_size_pct_ += 1;
  disk_cache_ratio_info.private_macro_size_pct_ -= 1;
  const int64_t buf_len = 1024;
  char buf[buf_len];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, disk_cache_ratio_info.serialize(buf, buf_len, pos));
  ASSERT_EQ(pos, disk_cache_ratio_info.get_serialize_size());
  ObTenantDiskCacheRatioInfo tmp_disk_cache_ratio_info;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_disk_cache_ratio_info.deserialize(buf, buf_len, pos));
  ASSERT_EQ(pos, disk_cache_ratio_info.get_serialize_size());
  ASSERT_EQ(disk_cache_ratio_info.micro_cache_size_pct_, tmp_disk_cache_ratio_info.micro_cache_size_pct_);
  ASSERT_EQ(disk_cache_ratio_info.private_macro_size_pct_, tmp_disk_cache_ratio_info.private_macro_size_pct_);
}

TEST_F(TestSSDiskSpaceMeta, tenant_disk_space_meta_body)
{
  ObTenantDiskSpaceMetaBody space_meta_body;
  space_meta_body.tenant_id_ = 1;
  ASSERT_EQ(true, space_meta_body.is_valid());
  space_meta_body.disk_cache_ratio_.micro_cache_size_pct_ += 2;
  space_meta_body.disk_cache_ratio_.private_macro_size_pct_ -= 2;
  space_meta_body.meta_file_alloc_size_ = 1001;

  const int64_t buf_len = 1024;
  char buf[buf_len];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, space_meta_body.serialize(buf, buf_len, pos));
  ASSERT_EQ(pos, space_meta_body.get_serialize_size());
  ObTenantDiskSpaceMetaBody tmp_space_meta_body;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_space_meta_body.deserialize(buf, buf_len, pos));
  ASSERT_EQ(pos, tmp_space_meta_body.get_serialize_size());
  ASSERT_EQ(space_meta_body.meta_file_alloc_size_, tmp_space_meta_body.meta_file_alloc_size_);
  ASSERT_EQ(space_meta_body.disk_cache_ratio_.micro_cache_size_pct_, tmp_space_meta_body.disk_cache_ratio_.micro_cache_size_pct_);
  ASSERT_EQ(space_meta_body.disk_cache_ratio_.private_macro_size_pct_, tmp_space_meta_body.disk_cache_ratio_.private_macro_size_pct_);
}

TEST_F(TestSSDiskSpaceMeta, tenant_disk_space_meta_body_compat)
{
  ObTenantDiskSpaceMetaBody space_meta_body;
  space_meta_body.tenant_id_ = 1;
  ASSERT_EQ(true, space_meta_body.is_valid());
  space_meta_body.version_ = 1;
  space_meta_body.meta_file_alloc_size_ = 1001;
  space_meta_body.private_macro_alloc_size_ = 20001;
  space_meta_body.tmp_file_write_cache_alloc_size_ = 30001;
  space_meta_body.disk_cache_ratio_.micro_cache_size_pct_ += 10;
  space_meta_body.disk_cache_ratio_.private_macro_size_pct_ -= 10;

  const int64_t buf_len = 1024;
  char buf1[buf_len];
  int64_t pos1 = 0;
  ASSERT_EQ(OB_SUCCESS, space_meta_body.serialize(buf1, buf_len, pos1));
  ASSERT_EQ(pos1, space_meta_body.get_serialize_size());

  space_meta_body.version_ = 2;
  char buf2[buf_len];
  int64_t pos2 = 0;
  ASSERT_EQ(OB_SUCCESS, space_meta_body.serialize(buf2, buf_len, pos2));
  ASSERT_EQ(pos2, space_meta_body.get_serialize_size());

  ASSERT_LT(pos1, pos2);

  ObTenantDiskSpaceMetaBody tmp_space_meta_body;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_space_meta_body.deserialize(buf1, buf_len, pos));
  ASSERT_EQ(pos, pos1);
  ASSERT_EQ(space_meta_body.meta_file_alloc_size_, tmp_space_meta_body.meta_file_alloc_size_);
  ASSERT_EQ(space_meta_body.private_macro_alloc_size_, tmp_space_meta_body.private_macro_alloc_size_);
  ASSERT_NE(space_meta_body.disk_cache_ratio_.micro_cache_size_pct_, tmp_space_meta_body.disk_cache_ratio_.micro_cache_size_pct_);
  ASSERT_NE(space_meta_body.disk_cache_ratio_.private_macro_size_pct_, tmp_space_meta_body.disk_cache_ratio_.private_macro_size_pct_);

  tmp_space_meta_body.reset();
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_space_meta_body.deserialize(buf2, buf_len, pos));
  ASSERT_EQ(pos, pos2);
  ASSERT_EQ(space_meta_body.meta_file_alloc_size_, tmp_space_meta_body.meta_file_alloc_size_);
  ASSERT_EQ(space_meta_body.private_macro_alloc_size_, tmp_space_meta_body.private_macro_alloc_size_);
  ASSERT_EQ(space_meta_body.disk_cache_ratio_.micro_cache_size_pct_, tmp_space_meta_body.disk_cache_ratio_.micro_cache_size_pct_);
  ASSERT_EQ(space_meta_body.disk_cache_ratio_.private_macro_size_pct_, tmp_space_meta_body.disk_cache_ratio_.private_macro_size_pct_);
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_disk_space_meta.log*");
  OB_LOGGER.set_file_name("test_ss_disk_space_meta.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}