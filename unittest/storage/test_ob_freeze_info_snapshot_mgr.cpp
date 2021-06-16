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
#include <cstdlib>
#include <thread>
#include <unistd.h>
#include <map>

#define private public
#define protected public
#include "storage/ob_freeze_info_snapshot_mgr.h"

namespace oceanbase {

using namespace storage;
using namespace common;

namespace unittest {
const int TEST_ALL_TENANT_NUM = 10;
class MockSchemaCache : public ObFreezeInfoSnapshotMgr::SchemaCache {
public:
  MockSchemaCache() : SchemaCache(schema_query_set_), schema_query_set_(*this)
  {}
  virtual int fetch_freeze_schema(const uint64_t, const int64_t, int64_t& schema_version) override
  {
    int ret = OB_SUCCESS;
    if (rand() % 4 == 0) {
      schema_version = -1;
    } else {
      schema_version = ObTimeUtility::current_time();
    }

    return ret;
  }

  virtual int fetch_freeze_schema(const uint64_t tenant_id, const int64_t, int64_t& schema_version,
      common::ObIArray<ObFreezeInfoSnapshotMgr::SchemaPair>& freeze_schema) override
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; i < TEST_ALL_TENANT_NUM; i++) {
      int64_t version = 0;
      if (rand() % 4 == 0) {
        version = -1;
      } else {
        version = ObTimeUtility::current_time();
        freeze_schema.push_back(ObFreezeInfoSnapshotMgr::SchemaPair(i, version));
      }
      if ((int64_t)tenant_id == i) {
        schema_version = version;
      }
    }

    return ret;
  }

private:
  ObFreezeInfoSnapshotMgr::SchemaQuerySet schema_query_set_;
};

class TestFreezeInfoSnapshotMgr : public ::testing::Test {
public:
  ObFreezeInfoSnapshotMgr mgr;
  const int TEST_TENANT_NUM = 10;
  const int TEST_NUM = 10;

  void insert()
  {
    ObArray<ObFreezeInfoSnapshotMgr::SchemaPair> schemas;
    ObArray<ObFreezeInfoSnapshotMgr::FreezeInfoLite> infos;
    ObArray<share::ObSnapshotInfo> snapshots;
    const int64_t backup_snapshot_version = 0;
    const int64_t delay_delete_snapshot_version = 0;

    for (int i = 0; i < TEST_NUM; i++) {
      schemas.reset();
      infos.reset();

      int victim = rand() % TEST_TENANT_NUM;
      int ok = rand();
      for (int j = 0; j < TEST_TENANT_NUM; j++) {
        if (ok % 2 == 0 && j == victim && i != 0) {
          schemas.push_back(ObFreezeInfoSnapshotMgr::SchemaPair(j + 1001, -1));
        } else {
          schemas.push_back(ObFreezeInfoSnapshotMgr::SchemaPair(j + 1001, i * TEST_TENANT_NUM + j));
        }
      }

      if (i == 0) {
        infos.push_back(ObFreezeInfoSnapshotMgr::FreezeInfoLite(1, 1, 0));
      }

      bool changed;
      mgr.update_info((i + 1) * 10,
          schemas,
          infos,
          snapshots,
          backup_snapshot_version,
          INT64_MAX,
          delay_delete_snapshot_version,
          changed);

      usleep(100000);
    }
  }
};

TEST_F(TestFreezeInfoSnapshotMgr, test_rs_return_minus_one_for_gc_snapshot_info)
{
  mgr.init_for_test();
  auto functor = [this]() -> void { this->insert(); };
  std::thread t(functor);

  usleep(50000);
  ObFreezeInfoSnapshotMgr::NeighbourFreezeInfo info[TEST_NUM][TEST_TENANT_NUM];

  for (int i = 0; i < TEST_NUM; i++) {
    for (int j = 0; j < TEST_TENANT_NUM; j++) {
      ObFreezeInfoSnapshotMgr::NeighbourFreezeInfo& info_item = info[i][j];
      EXPECT_EQ(
          OB_SUCCESS, mgr.get_neighbour_major_freeze(((((int64_t)j + 1001) << 40) + 50001), i * 10 + 5, info[i][j]));
      STORAGE_LOG(WARN, "get neighbour major freeze", K(j + 1001), K(i * 10 + 5), K(info_item));
      if (i != 0) {
        EXPECT_TRUE(info[i][j].next.schema_version >= info[i - 1][j].next.schema_version);
      }
    }

    usleep(10000);
  }

  t.join();
}

TEST_F(TestFreezeInfoSnapshotMgr, test_rs_return_minus_one_for_get_freeze_schema_evrsion)
{
  MockSchemaCache cache;
  MockSchemaCache::schema_node* p = NULL;

  p = (MockSchemaCache::schema_node*)cache.allocator_.alloc();
  cache.head_ = cache.tail_ = p;
  cache.inited_ = true;

  for (int64_t i = 0; i < 10000; i++) {
    for (int64_t j = 0; j < TEST_ALL_TENANT_NUM; j++) {
      int count = 0;
      int64_t schema_version;
      while (true) {
        count++;
        int ret = cache.get_freeze_schema_version(j, i * TEST_ALL_TENANT_NUM + j, false, schema_version);
        if (ret == OB_EAGAIN) {
          STORAGE_LOG(WARN,
              "test get invalid version, we retry",
              K(schema_version),
              "tenant_id",
              j,
              "freeze_version",
              i * TEST_ALL_TENANT_NUM + j,
              K(count));
        } else if (schema_version == -1) {
          EXPECT_TRUE(false);
        } else {
          break;
        }
      }
      STORAGE_LOG(INFO,
          "we finally succed with result",
          K(schema_version),
          "tenant_id",
          j,
          "freeze_version",
          i * TEST_ALL_TENANT_NUM + j,
          K(count));
    }
  }

  MockSchemaCache::schema_node* curr = cache.head_->next;
  STORAGE_LOG(INFO, "start finding -1 in list");
  while (curr) {
    EXPECT_TRUE(curr->schema_version != -1);
    curr = curr->next;
  }
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_ob_freeze_info_snapshot_mgr.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
