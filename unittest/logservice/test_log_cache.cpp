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

#include <string.h>
#include <gtest/gtest.h>
#define private public
#include "logservice/palf/log_cache.h"
#undef private
#include "logservice/common_util/ob_log_time_utils.h"
#include "share/ob_tenant_mem_limit_getter.h"
#include "share/rc/ob_tenant_base.h"
#include "logservice/palf/log_reader_utils.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace palf;
using namespace logservice;
using namespace share;

static const int64_t KV_CACHE_WASH_TIMER_INTERVAL_US = 60 * _SEC_;
static const int64_t DEFAULT_BUCKET_NUM = 10000000L;
static const int64_t DEFAULT_MAX_CACHE_SIZE = 1024L * 1024L * 1024L * 1024L;

class TestLogCache : public ::testing::Test
{
public:
  TestLogCache();
  ~TestLogCache();
  virtual void SetUp();
  virtual void TearDown();
};

TestLogCache::TestLogCache()
{
}

TestLogCache::~TestLogCache()
{
}

void TestLogCache::SetUp()
{
  // init cache
  ObKVGlobalCache::get_instance().init(&ObTenantMemLimitGetter::get_instance(),
                                       DEFAULT_BUCKET_NUM,
                                       DEFAULT_MAX_CACHE_SIZE,
                                       lib::ACHUNK_SIZE,
                                       KV_CACHE_WASH_TIMER_INTERVAL_US);
  OB_LOG_KV_CACHE.init(OB_LOG_KV_CACHE_NAME, 1);

  // init MTL
  ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
  ObTenantBase tbase(1001);
  ObTenantEnv::set_tenant(&tbase);
}

void TestLogCache::TearDown()
{
  PALF_LOG(INFO, "recycle_tenant_allocator start");
  OB_LOG_KV_CACHE.destroy();
  ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
}

LogStorage log_storage;
IPalfEnvImpl *palf_env_impl;

TEST_F(TestLogCache, test_basic_func)
{
  log_storage.is_inited_ = true;
  log_storage.logical_block_size_ = PALF_BLOCK_SIZE;
  const int64_t flashback_version = 0;
  int64_t palf_id = 1;
  LogColdCache cold_cache;
  cold_cache.init(palf_id, palf_env_impl, &log_storage);
  LSN lsn(0);
  int64_t in_read_size = MAX_LOG_BODY_SIZE;
  char *buf = reinterpret_cast<char *>(ob_malloc(MAX_LOG_BUFFER_SIZE, "LOG_KV_CACHE"));
  LogIteratorInfo iterator_info;
  {

    int64_t out_read_size = 0;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, cold_cache.get_cache_lines_(lsn, flashback_version, in_read_size, buf, out_read_size, &iterator_info));
    ReadBuf read_buf(buf, MAX_LOG_BUFFER_SIZE);
    EXPECT_EQ(OB_SUCCESS, cold_cache.fill_cache_lines_(flashback_version, lsn, MAX_LOG_BODY_SIZE, read_buf.buf_));
  }

  {
    iterator_info.reset();
    int64_t out_read_size = 0;
    in_read_size = 64 * 1024;
    EXPECT_EQ(OB_SUCCESS, cold_cache.get_cache_lines_(lsn, flashback_version, in_read_size, buf, out_read_size, &iterator_info));
    EXPECT_EQ(in_read_size, out_read_size);
  }

  {
    iterator_info.reset();
    int64_t out_read_size = 0;
    lsn.val_ = 1 * 1024 * 1024;
    EXPECT_EQ(OB_SUCCESS, cold_cache.get_cache_lines_(lsn, flashback_version, in_read_size, buf, out_read_size, &iterator_info));
    EXPECT_EQ(in_read_size, out_read_size);
  }

  {
    iterator_info.reset();
    int64_t out_read_size = 0;
    in_read_size = MAX_LOG_BODY_SIZE;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, cold_cache.get_cache_lines_(lsn, flashback_version, in_read_size, buf, out_read_size, &iterator_info));
  }

  ob_free(buf);
  buf = NULL;
}

TEST_F(TestLogCache, test_miss)
{
  log_storage.is_inited_ = true;
  log_storage.logical_block_size_ = PALF_BLOCK_SIZE;
  const int64_t flashback_version = 0;
  int64_t palf_id = 1;
  LogColdCache cold_cache;
  cold_cache.init(palf_id, palf_env_impl, &log_storage);
  LogIteratorInfo iterator_info;
  // test miss when has_read_size != 0
  {
    LSN lsn(1024);
    LSN old_lsn(lsn);
    int64_t in_read_size = MAX_LOG_BODY_SIZE;
    int64_t out_read_size = 0;
    int64_t has_read_size = 5000;
    int64_t real_read_size = lower_align(has_read_size, LOG_DIO_ALIGN_SIZE);
    EXPECT_EQ(OB_SUCCESS, cold_cache.deal_with_miss_(true, has_read_size,  in_read_size + CACHE_LINE_SIZE, lsn, in_read_size, out_read_size, &iterator_info));
    EXPECT_EQ(old_lsn.val_ + real_read_size, lsn.val_);
    EXPECT_EQ(MAX_LOG_BODY_SIZE - real_read_size, in_read_size);
    EXPECT_EQ(real_read_size, out_read_size);
  }

  // test miss for first read
  {
    iterator_info.reset();
    LSN lsn(67108864);
    LSN old_lsn(lsn);
    int64_t in_read_size = MAX_LOG_BODY_SIZE;
    int64_t has_read_size = 0;
    int64_t out_read_size = 0;
    EXPECT_EQ(OB_SUCCESS, cold_cache.deal_with_miss_(true, has_read_size, in_read_size + CACHE_LINE_SIZE, lsn, in_read_size, out_read_size, &iterator_info));
    EXPECT_EQ(PALF_BLOCK_SIZE, lsn.val_);
    EXPECT_EQ(MAX_LOG_BODY_SIZE + (old_lsn.val_ - lsn.val_), in_read_size);
  }

  // test miss for small buf
  {
    iterator_info.reset();
    LSN lsn(PALF_INITIAL_LSN_VAL);
    int64_t in_read_size = MAX_LOG_BODY_SIZE;
    int64_t has_read_size = 0;
    int64_t out_read_size = 0;
    int64_t buf_len = in_read_size;
    EXPECT_EQ(OB_SUCCESS, cold_cache.deal_with_miss_(true, has_read_size, buf_len, lsn, in_read_size, out_read_size, &iterator_info));
    // shouldn't read more log because of small buf size
    EXPECT_EQ(PALF_INITIAL_LSN_VAL, lsn.val_);
    EXPECT_EQ(MAX_LOG_BODY_SIZE, in_read_size);
  }

  // test miss for last cache line in log block
  {
    iterator_info.reset();
    // read lsn is in the last cache line of the second log block
    LSN lsn(PALF_BLOCK_SIZE * 2 - 32 * 1024);
    int64_t in_read_size = 32 * 1024;
    int64_t has_read_size = 0;
    int64_t out_read_size = 0;
    int64_t buf_len = in_read_size + CACHE_LINE_SIZE;
    EXPECT_EQ(OB_SUCCESS, cold_cache.deal_with_miss_(true, has_read_size, buf_len, lsn, in_read_size, out_read_size, &iterator_info));
    EXPECT_EQ(PALF_BLOCK_SIZE * 2 - 60 * 1024, lsn);
    EXPECT_EQ(LAST_CACHE_LINE_SIZE, in_read_size);
  }
}

TEST_F(TestLogCache, test_flashback)
{
  PALF_LOG(INFO, "begin flashback");
  log_storage.is_inited_ = true;
  log_storage.logical_block_size_ = PALF_BLOCK_SIZE;
  int64_t flashback_version = 0;
  int64_t palf_id = 1;
  LogColdCache cold_cache;
  cold_cache.init(palf_id, palf_env_impl, &log_storage);
  LSN lsn(0);
  int64_t in_read_size = MAX_LOG_BODY_SIZE;
  char *buf = reinterpret_cast<char *>(ob_malloc(MAX_LOG_BUFFER_SIZE, "LOG_KV_CACHE"));
  ReadBuf read_buf(buf, MAX_LOG_BUFFER_SIZE);
  LogIteratorInfo iterator_info;
  {
    int64_t out_read_size = 0;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, cold_cache.get_cache_lines_(lsn, flashback_version, in_read_size, buf, out_read_size, &iterator_info));
    EXPECT_EQ(OB_SUCCESS, cold_cache.fill_cache_lines_(flashback_version, lsn, MAX_LOG_BODY_SIZE, read_buf.buf_));
  }

  {
    iterator_info.reset();
    int64_t out_read_size = 0;
    flashback_version++;
    int64_t hit_cnt = cold_cache.log_cache_stat_.hit_cnt_;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, cold_cache.get_cache_lines_(lsn, flashback_version, in_read_size, buf, out_read_size, &iterator_info));
    EXPECT_EQ(hit_cnt, cold_cache.log_cache_stat_.hit_cnt_);
  }

  {
    iterator_info.reset();
    int64_t out_read_size = 0;
    EXPECT_EQ(OB_SUCCESS, cold_cache.fill_cache_lines_(flashback_version, lsn, MAX_LOG_BODY_SIZE, read_buf.buf_));
    in_read_size = 64 * 1024;
    int hit_cnt = cold_cache.log_cache_stat_.hit_cnt_;
    EXPECT_EQ(OB_SUCCESS, cold_cache.get_cache_lines_(lsn, flashback_version, in_read_size, buf, out_read_size, &iterator_info));
    EXPECT_LT(hit_cnt, cold_cache.log_cache_stat_.hit_cnt_);
  }

  ob_free(buf);
  buf = NULL;
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f ./test_log_cache.log");
  OB_LOGGER.set_file_name("test_log_cache.log", true, true, "test_log_cache.rs.log");
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin unittest::test_log_cache");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
