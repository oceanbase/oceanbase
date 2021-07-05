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
#include <gmock/gmock.h>
#define private public
#include "storage/ob_i_table.h"
#include "storage/ob_sstable.h"
#include "storage/ob_multi_version_table_store.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace memtable;

namespace unittest {

class TestMultiVersionTableStore : public ::testing::Test {
public:
  void fake_sstable(const int64_t base_version, const int64_t snapshot_version, ObSSTable& table);
};

void TestMultiVersionTableStore::fake_sstable(
    const int64_t base_version, const int64_t snapshot_version, ObSSTable& table)
{
  ObITable::TableKey key;
  key.table_type_ = ObITable::MINOR_SSTABLE;
  key.pkey_ = ObPartitionKey(combine_id(1, 3001), 0, 0);
  key.table_id_ = combine_id(1, 3001);
  key.trans_version_range_.base_version_ = base_version;
  key.trans_version_range_.multi_version_start_ = snapshot_version;
  key.trans_version_range_.snapshot_version_ = snapshot_version;
  ASSERT_EQ(OB_SUCCESS, table.init(key));
}

TEST_F(TestMultiVersionTableStore, add_gc_minor_sstable)
{
  ObMultiVersionTableStore table_store;
  const int64_t max_deferred_gc_count = 3;
  table_store.is_inited_ = true;
  const int64_t sstable_count = 10;
  ObSSTable t[sstable_count];
  for (int64_t i = 0; i < sstable_count; ++i) {
    fake_sstable(i, i + 1, t[i]);
    ASSERT_EQ(OB_SUCCESS, table_store.add_gc_minor_sstable(i, &t[i], max_deferred_gc_count));
  }
  ASSERT_EQ(max_deferred_gc_count, table_store.gc_sstable_count_);
  ASSERT_EQ(&t[9], table_store.gc_sstable_infos_[0].sstable_);
  ASSERT_EQ(9, table_store.gc_sstable_infos_[0].retired_ts_);
  ASSERT_EQ(&t[8], table_store.gc_sstable_infos_[1].sstable_);
  ASSERT_EQ(8, table_store.gc_sstable_infos_[1].retired_ts_);
  ASSERT_EQ(&t[7], table_store.gc_sstable_infos_[2].sstable_);
  ASSERT_EQ(7, table_store.gc_sstable_infos_[2].retired_ts_);

  ASSERT_EQ(OB_SUCCESS, table_store.add_gc_minor_sstable(100, &t[7], max_deferred_gc_count));
  ASSERT_EQ(max_deferred_gc_count, table_store.gc_sstable_count_);
  ASSERT_EQ(&t[7], table_store.gc_sstable_infos_[0].sstable_);
  ASSERT_EQ(100, table_store.gc_sstable_infos_[0].retired_ts_);
  ASSERT_EQ(&t[9], table_store.gc_sstable_infos_[1].sstable_);
  ASSERT_EQ(9, table_store.gc_sstable_infos_[1].retired_ts_);
  ASSERT_EQ(&t[8], table_store.gc_sstable_infos_[2].sstable_);
  ASSERT_EQ(8, table_store.gc_sstable_infos_[2].retired_ts_);
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_row_fuse");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
