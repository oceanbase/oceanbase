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
#include <gtest/gtest.h>
#define private public
#define protected public

#include "storage/backup/ob_backup_tmp_file.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_index_cache.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "lib/random/ob_random.h"
#include "test_backup.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::backup;

namespace oceanbase {
namespace backup {

static ObSimpleMemLimitGetter getter;

class TestBackupIndexCache : public ::testing::Test {
public:
  TestBackupIndexCache();
  virtual ~TestBackupIndexCache();
  virtual void SetUp();
  virtual void TearDown();

protected:
  int64_t lower_mem_limit_;
  int64_t upper_mem_limit_;

private:
  DISALLOW_COPY_AND_ASSIGN(TestBackupIndexCache);
};

TestBackupIndexCache::TestBackupIndexCache() : lower_mem_limit_(8 * 1024 * 1024), upper_mem_limit_(16 * 1024 * 1024)
{}

TestBackupIndexCache::~TestBackupIndexCache()
{}

void TestBackupIndexCache::SetUp()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 1024;
  const int64_t block_size = lib::ACHUNK_SIZE;
  const uint64_t tenant_id = 1;
  ret = getter.add_tenant(tenant_id, lower_mem_limit_, upper_mem_limit_);
  ret = ObKVGlobalCache::get_instance().init(&getter, bucket_num, max_cache_size, block_size);
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  CHUNK_MGR.set_limit(5L * 1024L * 1024L * 1024L);
  ret = OB_BACKUP_INDEX_CACHE.init();
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestBackupIndexCache::TearDown()
{
  OB_BACKUP_INDEX_CACHE.destroy();
  ObKVGlobalCache::get_instance().destroy();
  getter.reset();
}

static bool cmp_backup_index_cache_value(const ObBackupIndexCacheValue *lhs, const ObBackupIndexCacheValue *rhs)
{
  bool bret = true;
  if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "lhs or rhs is null", KP(lhs), KP(rhs));
  } else if (lhs->len() != rhs->len()) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "lhs and rhs not equal", K(lhs), K(rhs));
  } else {
    const int64_t len = lhs->len();
    const char *lhs_buf = lhs->buf();
    const char *rhs_buf = rhs->buf();
    for (int64_t i = 0; i < len; ++i) {
      if (lhs_buf[i] != rhs_buf[i]) {
        bret = false;
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "buf content is not same", K(lhs_buf[i]), K(rhs_buf[i]), K(i));
        break;
      }
    }
  }
  return bret;
}

TEST_F(TestBackupIndexCache, put_and_read_cache)
{
  int ret = OB_SUCCESS;
  ObBackupIndexCacheKey cache_key;
  make_random_cache_key(cache_key);
  ObKVCacheHandle cache_handle;
  const ObBackupIndexCacheValue *new_cache_value = NULL;
  ret = OB_BACKUP_INDEX_CACHE.get(cache_key, new_cache_value, cache_handle);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, ret);
  const int64_t buf_len = 16 * 1024;
  char *buf = new char[buf_len];
  for (int64_t i = 0; i < buf_len; ++i) {
    buf[i] = static_cast<char>(i % 256);
  }
  ObBackupIndexCacheValue cache_value(buf, buf_len);
  ret = OB_BACKUP_INDEX_CACHE.put(cache_key, cache_value);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = OB_BACKUP_INDEX_CACHE.get(cache_key, new_cache_value, cache_handle);
  EXPECT_EQ(OB_SUCCESS, ret);
  cmp_backup_index_cache_value(&cache_value, new_cache_value);
}

}  // namespace backup
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_backup_index_cache.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_backup_index_cache.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
