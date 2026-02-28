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
#include "share/ob_max_id_cache.h"
#include "rootserver/ob_root_service.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "deps/oblib/src/common/ob_tablet_id.h"
#include "share/ob_max_id_fetcher.h"

#define ASSERT_SUCCESS(x) ASSERT_EQ((x), OB_SUCCESS)

namespace oceanbase
{
namespace pl
{
int ObPLPackageManager::load_all_sys_package(common::ObMySQLProxy &sql_proxy)
{
  return OB_SUCCESS;
}
}
using namespace common;
class TestMaxIdCache : public unittest::ObSimpleClusterTestBase
{
public:
  TestMaxIdCache() : unittest::ObSimpleClusterTestBase("test_max_id_cache") {}
};
// 测试max_id_cache的各种接口
TEST_F(TestMaxIdCache, basic)
{
  ASSERT_NE(GCTX.root_service_, nullptr);
  ASSERT_NE(GCTX.sql_proxy_, nullptr);
  while (!GCTX.root_service_->is_full_service()) {
    ob_usleep(1_s);
  }
  rootserver::ObSysStat sys_stat;
  ASSERT_SUCCESS(sys_stat.set_initial_values(OB_SYS_TENANT_ID));
  ObMaxIdFetcher id_fetcher(*GCTX.sql_proxy_);
  uint64_t id = OB_INVALID_ID;
  int64_t extended_tablet_init_id = sys_stat.ob_max_used_extended_rowid_table_tablet_id_.value_.get_int();
  uint64_t normal_tablet_init_id = sys_stat.ob_max_used_normal_rowid_table_tablet_id_.value_.get_int();
  uint64_t table_init_id = sys_stat.ob_max_used_object_id_.value_.get_int();

  // 这个接口返回的是 [id, id+size)
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_EXTENDED_ROWID_TABLE_TABLET_ID_TYPE, id, 1));
  ASSERT_EQ(extended_tablet_init_id + 1, id);
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_EXTENDED_ROWID_TABLE_TABLET_ID_TYPE, id, 2));
  ASSERT_EQ(extended_tablet_init_id + 2, id);
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_EXTENDED_ROWID_TABLE_TABLET_ID_TYPE, id, 3));
  ASSERT_EQ(extended_tablet_init_id + 4, id);

  // 这个接口返回的是 [id, id+size)
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_NORMAL_ROWID_TABLE_TABLET_ID_TYPE, id, 1));
  ASSERT_EQ(normal_tablet_init_id + 1, id);
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_NORMAL_ROWID_TABLE_TABLET_ID_TYPE, id, 2));
  ASSERT_EQ(normal_tablet_init_id + 2, id);
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_NORMAL_ROWID_TABLE_TABLET_ID_TYPE, id, 3));
  ASSERT_EQ(normal_tablet_init_id + 4, id);

  // 这个接口返回的是 (id - size, id]
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_TABLE_ID_TYPE, id, UINT64_MAX, 1));
  ASSERT_EQ(table_init_id + 1, id); // 1
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_TABLE_ID_TYPE, id, UINT64_MAX, 2));
  ASSERT_EQ(table_init_id + 3, id); // 2 3
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_TABLE_ID_TYPE, id, UINT64_MAX, 3));
  ASSERT_EQ(table_init_id + 6, id); // 4 5 6
}

// rs切主后缓存被浪费
TEST_F(TestMaxIdCache, rs_change)
{
  ASSERT_NE(GCTX.root_service_, nullptr);
  ASSERT_SUCCESS(GCTX.root_service_->revoke_rs());
  while (!GCTX.root_service_->is_full_service()) {
    ob_usleep(1_s);
  }
  rootserver::ObSysStat sys_stat;
  ASSERT_SUCCESS(sys_stat.set_initial_values(OB_SYS_TENANT_ID));
  ObMaxIdFetcher id_fetcher(*GCTX.sql_proxy_);
  uint64_t id = OB_INVALID_ID;
  int64_t extended_tablet_init_id = sys_stat.ob_max_used_extended_rowid_table_tablet_id_.value_.get_int() + ObMaxIdCacheItem::CACHE_SIZE;
  uint64_t normal_tablet_init_id = sys_stat.ob_max_used_normal_rowid_table_tablet_id_.value_.get_int() + ObMaxIdCacheItem::CACHE_SIZE;
  uint64_t table_init_id = sys_stat.ob_max_used_object_id_.value_.get_int() + ObMaxIdCacheItem::CACHE_SIZE;

  // 这个接口返回的是 [id, id+size)
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_EXTENDED_ROWID_TABLE_TABLET_ID_TYPE, id, 1));
  ASSERT_EQ(extended_tablet_init_id + 1, id);
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_EXTENDED_ROWID_TABLE_TABLET_ID_TYPE, id, 2));
  ASSERT_EQ(extended_tablet_init_id + 2, id);
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_EXTENDED_ROWID_TABLE_TABLET_ID_TYPE, id, 3));
  ASSERT_EQ(extended_tablet_init_id + 4, id);

  // 这个接口返回的是 [id, id+size)
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_NORMAL_ROWID_TABLE_TABLET_ID_TYPE, id, 1));
  ASSERT_EQ(normal_tablet_init_id + 1, id);
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_NORMAL_ROWID_TABLE_TABLET_ID_TYPE, id, 2));
  ASSERT_EQ(normal_tablet_init_id + 2, id);
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_NORMAL_ROWID_TABLE_TABLET_ID_TYPE, id, 3));
  ASSERT_EQ(normal_tablet_init_id + 4, id);

  // 这个接口返回的是 (id - size, id]
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_TABLE_ID_TYPE, id, UINT64_MAX, 1));
  ASSERT_EQ(table_init_id + 1, id); // 1
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_TABLE_ID_TYPE, id, UINT64_MAX, 2));
  ASSERT_EQ(table_init_id + 3, id); // 2 3
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_TABLE_ID_TYPE, id, UINT64_MAX, 3));
  ASSERT_EQ(table_init_id + 6, id); // 4 5 6
}

TEST_F(TestMaxIdCache, max_id)
{
  ObMaxIdFetcher id_fetcher(*GCTX.sql_proxy_);
  uint64_t id = OB_INVALID_ID;
  ASSERT_NE(GCTX.root_service_, nullptr);
  ASSERT_NE(GCTX.sql_proxy_, nullptr);
  uint64_t current_id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_EXTENDED_ROWID_TABLE_TABLET_ID_TYPE, current_id, 1));
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_EXTENDED_ROWID_TABLE_TABLET_ID_TYPE,
        id, ObTabletID::MAX_USER_EXTENDED_ROWID_TABLE_TABLET_ID - 1 - current_id));
  id = OB_INVALID_ID;
  ASSERT_NE(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_EXTENDED_ROWID_TABLE_TABLET_ID_TYPE, current_id, 1),
      OB_SUCCESS) << current_id;

  current_id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_NORMAL_ROWID_TABLE_TABLET_ID_TYPE, current_id, 1));
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_NORMAL_ROWID_TABLE_TABLET_ID_TYPE,
        id, ObTabletID::MAX_USER_NORMAL_ROWID_TABLE_TABLET_ID - 1 - current_id));
  id = OB_INVALID_ID;
  ASSERT_NE(id_fetcher.fetch_new_max_ids(OB_SYS_TENANT_ID, OB_MAX_USED_NORMAL_ROWID_TABLE_TABLET_ID_TYPE, current_id, 1),
      OB_SUCCESS) << current_id;

  current_id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_TABLE_ID_TYPE, current_id, UINT64_MAX, 1));
  id = OB_INVALID_ID;
  ASSERT_SUCCESS(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_TABLE_ID_TYPE, id, UINT64_MAX, INT64_MAX - current_id));
  id = OB_INVALID_ID;
  ASSERT_NE(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID, OB_MAX_USED_TABLE_ID_TYPE, current_id, UINT64_MAX, 1),
      OB_SUCCESS) << current_id;
}
}

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
