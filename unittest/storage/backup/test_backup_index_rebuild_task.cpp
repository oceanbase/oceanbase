/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include "test_backup.h"
#define private public
#define protected public
#include "storage/backup/ob_backup_index_rebuild_task.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::backup;

namespace oceanbase {
namespace backup {

static ObBackupMetaIndex make_meta_index(
    const int64_t tablet_id, const ObBackupMetaType meta_type,
    const int64_t turn_id, const int64_t retry_id,
    const int64_t file_id = 1, const int64_t offset = 0, const int64_t length = 4096)
{
  ObBackupMetaIndex idx;
  idx.meta_key_.tablet_id_ = ObTabletID(tablet_id);
  idx.meta_key_.meta_type_ = meta_type;
  idx.backup_set_id_ = 1;
  idx.ls_id_ = ObLSID(1001);
  idx.turn_id_ = turn_id;
  idx.retry_id_ = retry_id;
  idx.file_id_ = file_id;
  idx.offset_ = offset;
  idx.length_ = length;
  return idx;
}

// Test subclass that overrides get_next_meta_index_ to read from a pre-sorted
// array instead of external sort, avoiding heavy runtime dependencies.
class TestableMetaIndexMergeFinishTask : public ObBackupMetaIndexMergeFinishTask {
public:
  TestableMetaIndexMergeFinishTask() : sorted_idx_(0), sorted_items_() {}

  void set_sorted_items(const ObIArray<ObBackupMetaIndex> &items)
  {
    sorted_items_.reset();
    sorted_idx_ = 0;
    for (int64_t i = 0; i < items.count(); ++i) {
      sorted_items_.push_back(items.at(i));
    }
  }

  // override to bypass external sort
  int get_next_meta_index_(ObBackupMetaIndex &meta_index) override
  {
    meta_index.reset();
    if (sorted_idx_ >= sorted_items_.count()) {
      return OB_ITER_END;
    }
    meta_index = sorted_items_.at(sorted_idx_);
    ++sorted_idx_;
    return OB_SUCCESS;
  }

private:
  int64_t sorted_idx_;
  ObArray<ObBackupMetaIndex> sorted_items_;
};

class TestFusedMetaIndex : public ::testing::Test {
public:
  void SetUp() override {}
  void TearDown() override {}

  // Sort items by meta_key (same order as BackupMetaIndexComparator) then feed
  void feed_sorted(const ObIArray<ObBackupMetaIndex> &items)
  {
    ObArray<ObBackupMetaIndex> sorted;
    for (int64_t i = 0; i < items.count(); ++i) {
      sorted.push_back(items.at(i));
    }
    // simple insertion sort by meta_key
    for (int64_t i = 1; i < sorted.count(); ++i) {
      for (int64_t j = i; j > 0 && sorted.at(j).meta_key_ < sorted.at(j - 1).meta_key_; --j) {
        ObBackupMetaIndex tmp = sorted.at(j);
        sorted.at(j) = sorted.at(j - 1);
        sorted.at(j - 1) = tmp;
      }
    }
    task_.set_sorted_items(sorted);
  }

  int collect_fused(ObIArray<ObBackupMetaIndex> &result)
  {
    int ret = OB_SUCCESS;
    result.reset();
    ObBackupMetaIndex idx;
    while (OB_SUCC(ret)) {
      idx.reset();
      ret = task_.get_next_fused_meta_index_(idx);
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else if (OB_SUCC(ret)) {
        if (OB_FAIL(result.push_back(idx))) {
          LOG_WARN("push back failed", K(ret));
        }
      }
    }
    return ret;
  }

  TestableMetaIndexMergeFinishTask task_;
};

TEST_F(TestFusedMetaIndex, empty_input)
{
  ObArray<ObBackupMetaIndex> empty;
  task_.set_sorted_items(empty);
  ObBackupMetaIndex idx;
  ASSERT_EQ(OB_ITER_END, task_.get_next_fused_meta_index_(idx));
  // call again after iter end
  ASSERT_EQ(OB_ITER_END, task_.get_next_fused_meta_index_(idx));
}

TEST_F(TestFusedMetaIndex, single_record)
{
  ObArray<ObBackupMetaIndex> items;
  ASSERT_EQ(OB_SUCCESS, items.push_back(make_meta_index(100, BACKUP_SSTABLE_META, 1, 0)));
  feed_sorted(items);

  ObArray<ObBackupMetaIndex> result;
  ASSERT_EQ(OB_SUCCESS, collect_fused(result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(ObTabletID(100), result.at(0).meta_key_.tablet_id_);
  ASSERT_EQ(1, result.at(0).turn_id_);
  ASSERT_EQ(0, result.at(0).retry_id_);
}

TEST_F(TestFusedMetaIndex, same_meta_key_pick_largest_turn_retry)
{
  ObArray<ObBackupMetaIndex> items;
  // same tablet_id=200, same meta_type, different (turn_id, retry_id)
  ASSERT_EQ(OB_SUCCESS, items.push_back(make_meta_index(200, BACKUP_SSTABLE_META, 1, 0)));
  ASSERT_EQ(OB_SUCCESS, items.push_back(make_meta_index(200, BACKUP_SSTABLE_META, 1, 2)));
  ASSERT_EQ(OB_SUCCESS, items.push_back(make_meta_index(200, BACKUP_SSTABLE_META, 2, 0)));
  ASSERT_EQ(OB_SUCCESS, items.push_back(make_meta_index(200, BACKUP_SSTABLE_META, 1, 5)));
  feed_sorted(items);

  ObArray<ObBackupMetaIndex> result;
  ASSERT_EQ(OB_SUCCESS, collect_fused(result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(2, result.at(0).turn_id_);
  ASSERT_EQ(0, result.at(0).retry_id_);
}

TEST_F(TestFusedMetaIndex, same_turn_pick_largest_retry)
{
  ObArray<ObBackupMetaIndex> items;
  ASSERT_EQ(OB_SUCCESS, items.push_back(make_meta_index(300, BACKUP_TABLET_META, 1, 0)));
  ASSERT_EQ(OB_SUCCESS, items.push_back(make_meta_index(300, BACKUP_TABLET_META, 1, 3)));
  ASSERT_EQ(OB_SUCCESS, items.push_back(make_meta_index(300, BACKUP_TABLET_META, 1, 1)));
  feed_sorted(items);

  ObArray<ObBackupMetaIndex> result;
  ASSERT_EQ(OB_SUCCESS, collect_fused(result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(1, result.at(0).turn_id_);
  ASSERT_EQ(3, result.at(0).retry_id_);
}

TEST_F(TestFusedMetaIndex, multiple_groups)
{
  ObArray<ObBackupMetaIndex> items;
  // group 1: tablet_id=100, BACKUP_SSTABLE_META
  ASSERT_EQ(OB_SUCCESS, items.push_back(make_meta_index(100, BACKUP_SSTABLE_META, 1, 0)));
  ASSERT_EQ(OB_SUCCESS, items.push_back(make_meta_index(100, BACKUP_SSTABLE_META, 1, 1)));
  // group 2: tablet_id=100, BACKUP_TABLET_META (different meta_type = different key)
  ASSERT_EQ(OB_SUCCESS, items.push_back(make_meta_index(100, BACKUP_TABLET_META, 2, 0)));
  // group 3: tablet_id=200, BACKUP_SSTABLE_META
  ASSERT_EQ(OB_SUCCESS, items.push_back(make_meta_index(200, BACKUP_SSTABLE_META, 1, 0)));
  ASSERT_EQ(OB_SUCCESS, items.push_back(make_meta_index(200, BACKUP_SSTABLE_META, 3, 0)));
  ASSERT_EQ(OB_SUCCESS, items.push_back(make_meta_index(200, BACKUP_SSTABLE_META, 2, 5)));
  feed_sorted(items);

  ObArray<ObBackupMetaIndex> result;
  ASSERT_EQ(OB_SUCCESS, collect_fused(result));
  ASSERT_EQ(3, result.count());

  // group 1 result
  ASSERT_EQ(ObTabletID(100), result.at(0).meta_key_.tablet_id_);
  ASSERT_EQ(BACKUP_SSTABLE_META, result.at(0).meta_key_.meta_type_);
  ASSERT_EQ(1, result.at(0).turn_id_);
  ASSERT_EQ(1, result.at(0).retry_id_);

  // group 2 result
  ASSERT_EQ(ObTabletID(100), result.at(1).meta_key_.tablet_id_);
  ASSERT_EQ(BACKUP_TABLET_META, result.at(1).meta_key_.meta_type_);
  ASSERT_EQ(2, result.at(1).turn_id_);
  ASSERT_EQ(0, result.at(1).retry_id_);

  // group 3 result
  ASSERT_EQ(ObTabletID(200), result.at(2).meta_key_.tablet_id_);
  ASSERT_EQ(BACKUP_SSTABLE_META, result.at(2).meta_key_.meta_type_);
  ASSERT_EQ(3, result.at(2).turn_id_);
  ASSERT_EQ(0, result.at(2).retry_id_);
}

TEST_F(TestFusedMetaIndex, repeated_iter_end)
{
  ObArray<ObBackupMetaIndex> items;
  ASSERT_EQ(OB_SUCCESS, items.push_back(make_meta_index(100, BACKUP_SSTABLE_META, 1, 0)));
  feed_sorted(items);

  ObBackupMetaIndex idx;
  ASSERT_EQ(OB_SUCCESS, task_.get_next_fused_meta_index_(idx));
  ASSERT_EQ(ObTabletID(100), idx.meta_key_.tablet_id_);
  ASSERT_EQ(OB_ITER_END, task_.get_next_fused_meta_index_(idx));
  ASSERT_EQ(OB_ITER_END, task_.get_next_fused_meta_index_(idx));
  ASSERT_EQ(OB_ITER_END, task_.get_next_fused_meta_index_(idx));
}

}  // namespace backup
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_backup_index_rebuild_task.log*");
  OB_LOGGER.set_file_name("test_backup_index_rebuild_task.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
