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
#include <gmock/gmock.h>
#define  private public
#include "rootserver/ob_root_service.h"
#include "share/ob_max_id_cache.h"
#include "share/ob_max_id_fetcher.h"
#include "deps/oblib/src/lib/mysqlclient/ob_mysql_proxy.h"

#define ASSERT_SUCCESS(x) ASSERT_EQ((x), OB_SUCCESS)

// mock 掉对内部表的查询，验证一下缓存的正确性

namespace oceanbase
{
namespace share
{
uint64_t current_max_id = OB_INVALID_ID;
uint64_t target_size = OB_INVALID_SIZE;
bool hit = false;
int ObMaxIdFetcher::batch_fetch_new_max_id_from_inner_table(
    const uint64_t tenant_id,
    ObMaxIdType id_type,
    uint64_t &max_id,
    const uint64_t size)
{
  int ret = OB_SUCCESS;
  hit = true;
  if (size == target_size) {
    max_id = current_max_id + size;
  } else {
    max_id = OB_INVALID_ID;
  }
  LOG_INFO("hit inner table", K(hit), K(id_type), K(max_id), K(size), K(current_max_id), K(target_size));
  return ret;
}
void set_id_size(const uint64_t id, const uint64_t size)
{
  current_max_id = id;
  target_size = size;
  hit = false;
}
void reset_id_size()
{
  current_max_id = OB_INVALID_ID;
  target_size = OB_INVALID_SIZE;
  hit = false;
}
TEST(MaxIdCache, basic)
{
  rootserver::ObRootService rs;
  GCTX.root_service_ = &rs;
  rs.rs_status_.rs_status_ = share::status::ObRootServiceStatus::FULL_SERVICE;
  ObMaxIdCacheMgr mgr;
  ObMySQLProxy proxy;
  uint64_t id = OB_INVALID_ID;
  ASSERT_SUCCESS(mgr.init(&proxy));

  // 第一次查询，会缓存ObMaxIdCacheItem::CACHE_SIZE个ID
  // 查询前内部表值为1，查询后变为ObMaxIdCacheItem::CACHE_SIZE + 1
  set_id_size(1, ObMaxIdCacheItem::CACHE_SIZE);
  ASSERT_SUCCESS(mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, 1));
  ASSERT_EQ(id, 2);
  ASSERT_TRUE(hit);
  reset_id_size();

  // 查询ObMaxIdCacheItem::CACHE_SIZE-1次，不会触发内部表读取
  for (int64_t i = 1; i < ObMaxIdCacheItem::CACHE_SIZE; i++) {
    ASSERT_SUCCESS(mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, 1));
    ASSERT_EQ(id, i + 2);
    ASSERT_FALSE(hit);
  }

  // cache用完，触发内部表读取
  // 查询前内部表值为ObMaxIdCacheItem::CACHE_SIZE + 1，查询后变为ObMaxIdCacheItem::CACHE_SIZE * 2 + 1
  set_id_size(ObMaxIdCacheItem::CACHE_SIZE + 1, ObMaxIdCacheItem::CACHE_SIZE);
  ASSERT_SUCCESS(mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, 1));
  ASSERT_EQ(id, ObMaxIdCacheItem::CACHE_SIZE + 2);
  ASSERT_TRUE(hit);
  reset_id_size();

  // 读取ObMaxIdCacheItem::CACHE_SIZE - 1个值，不触发内部表读取
  ASSERT_SUCCESS(mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, ObMaxIdCacheItem::CACHE_SIZE - 1));
  ASSERT_EQ(id, ObMaxIdCacheItem::CACHE_SIZE + 3);
  ASSERT_FALSE(hit);

  // cache用完，触发内部表读取
  // 查询前内部表值为ObMaxIdCacheItem::CACHE_SIZE * 2 + 1，查询后变为ObMaxIdCacheItem::CACHE_SIZE * 3 + 1
  set_id_size(ObMaxIdCacheItem::CACHE_SIZE * 2 + 1, ObMaxIdCacheItem::CACHE_SIZE);
  ASSERT_SUCCESS(mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, 1));
  ASSERT_EQ(id, ObMaxIdCacheItem::CACHE_SIZE * 2 + 2);
  ASSERT_TRUE(hit);
  reset_id_size();

  // 查询前内部表值为ObMaxIdCacheItem::CACHE_SIZE * 3 + 1，查询后变为ObMaxIdCacheItem::CACHE_SIZE * 4 + 2
  set_id_size(ObMaxIdCacheItem::CACHE_SIZE * 3 + 1, ObMaxIdCacheItem::CACHE_SIZE + 1);
  ASSERT_SUCCESS(mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, ObMaxIdCacheItem::CACHE_SIZE + 1));
  // 可以复用前一段的缓存
  ASSERT_EQ(id, ObMaxIdCacheItem::CACHE_SIZE * 2 + 3);
  ASSERT_TRUE(hit);
  reset_id_size();

  // 当前缓存的值为[ObMaxIdCacheItem::CACHE_SIZE * 3 + 4, ObMaxIdCacheItem::CACHE_SIZE * 4 + 2]
  // 返回目前内部表使用的最大为ObMaxIdCacheItem::CACHE_SIZE * 4 + 3，表明有其他地方获取了id，无法连续，缓存的值需要丢弃
  set_id_size(ObMaxIdCacheItem::CACHE_SIZE * 4 + 3, ObMaxIdCacheItem::CACHE_SIZE);
  ASSERT_SUCCESS(mgr.fetch_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_OBJECT_ID_TYPE, id, ObMaxIdCacheItem::CACHE_SIZE));
  ASSERT_EQ(id, ObMaxIdCacheItem::CACHE_SIZE * 4 + 4);
  ASSERT_TRUE(hit);
  reset_id_size();
}
}
}
int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
