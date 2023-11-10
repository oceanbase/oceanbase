/**
 * Copyright (c) 2023 OceanBase
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
#include <gmock/gmock.h>
#include <thread>
#define USING_LOG_PREFIX STORAGE

#define USING_LOG_PREFIX STORAGE
#define private public
#define protected public

#include "mtlenv/mock_tenant_module_env.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/ob_storage_struct.h"
#include "storage/blocksstable/ob_sstable.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace compaction;
using namespace omt;
using namespace share;

class TestParallelMinorDag : public ::testing::Test
{
public:
  TestParallelMinorDag()
    : tenant_id_(1), allocator_(ObModIds::TEST), tenant_base_(tenant_id_) {}
  virtual ~TestParallelMinorDag() {}
  int prepare_merge_result(const int64_t sstable_cnt, ObGetMergeTablesResult &result);

  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();

  share::SCN get_start_log_ts(const int64_t idx);
  share::SCN get_end_log_ts(const int64_t idx);
  void check_result(
      const int64_t minor_compact_trigger,
      const int64_t sstable_cnt,
      const int64_t result_cnt);

  static const int64_t TENANT_ID = 1;
  static const int64_t TABLE_ID = 7777;
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 3;
  static const int64_t TEST_COLUMN_CNT = 6;
  static const int64_t MAX_SSTABLE_CNT = 64;

  const uint64_t tenant_id_;
  common::ObArenaAllocator allocator_;
  ObTenantBase tenant_base_;
  ObSSTable *fake_sstables_[MAX_SSTABLE_CNT];
};

void TestParallelMinorDag::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}
void TestParallelMinorDag::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}
void TestParallelMinorDag::SetUp()
{
  ObTenantMetaMemMgr *t3m = OB_NEW(ObTenantMetaMemMgr, ObModIds::TEST, tenant_id_);
  ASSERT_EQ(OB_SUCCESS, t3m->init());

  tenant_base_.set(t3m);
  ObTenantEnv::set_tenant(&tenant_base_);
  ASSERT_EQ(OB_SUCCESS, tenant_base_.init());

  MEMSET(fake_sstables_, 0, sizeof(ObSSTable*) * MAX_SSTABLE_CNT );
}
void TestParallelMinorDag::TearDown()
{
  for (int i = 0; i < MAX_SSTABLE_CNT; ++i) {
    if (nullptr != fake_sstables_[i]) {
      fake_sstables_[i]->~ObSSTable();
      allocator_.free(fake_sstables_[i]);
      fake_sstables_[i] = nullptr;
    }
  }
  allocator_.reset();

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  t3m->destroy();
  ObTenantEnv::set_tenant(nullptr);
}

int TestParallelMinorDag::prepare_merge_result(
    const int64_t sstable_cnt,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  result.reset();

  result.version_range_.base_version_ = 50;
  result.version_range_.snapshot_version_ = 100;
  result.version_range_.multi_version_start_ = 100;
  result.merge_version_ = 0;

  int64_t log_ts = 1;
  for (int i = 0; OB_SUCC(ret) && i < sstable_cnt; ++i) {
    char *buf = static_cast<char *>(allocator_.alloc(sizeof(ObSSTable)));
    ObSSTable *tmp_sstable = new (buf) ObSSTable();
    fake_sstables_[i] = tmp_sstable;
    fake_sstables_[i]->key_.scn_range_.start_scn_.convert_for_tx(log_ts++);
    fake_sstables_[i]->key_.scn_range_.end_scn_.convert_for_tx(log_ts);
    fake_sstables_[i]->key_.table_type_ = ObITable::TableType::MINI_SSTABLE;
    ObTableHandleV2 table_handle;
    if (OB_FAIL(table_handle.set_sstable(fake_sstables_[i], &allocator_))) {
      COMMON_LOG(WARN, "failed to set stable", K(ret));
    } else if (OB_FAIL(result.handle_.add_table(table_handle))) {
      COMMON_LOG(WARN, "failed to push table", K(ret), K(i), KPC(fake_sstables_[i]));
    }
  }
  result.scn_range_.start_scn_ = fake_sstables_[0]->key_.scn_range_.start_scn_;
  result.scn_range_.end_scn_ = fake_sstables_[sstable_cnt - 1]->key_.scn_range_.end_scn_;
  return ret;
}

share::SCN TestParallelMinorDag::get_start_log_ts(const int64_t idx)
{
  return fake_sstables_[idx]->key_.scn_range_.start_scn_;
}

share::SCN TestParallelMinorDag::get_end_log_ts(const int64_t idx)
{
  return fake_sstables_[idx]->key_.scn_range_.end_scn_;
}

void check_result_valid(const ObGetMergeTablesResult &result)
{
  ASSERT_EQ(result.handle_.get_table(0)->get_start_scn(), result.scn_range_.start_scn_);
  ASSERT_EQ(result.handle_.get_table(result.handle_.get_count() - 1)->get_end_scn(), result.scn_range_.end_scn_);
}

void TestParallelMinorDag::check_result(
    const int64_t minor_compact_trigger,
    const int64_t sstable_cnt,
    const int64_t result_cnt)
{
  ObGetMergeTablesResult result;
  ObArray<ObGetMergeTablesResult> result_array;
  ObMinorExecuteRangeMgr minor_range_mgr;

  ASSERT_EQ(OB_SUCCESS, prepare_merge_result(sstable_cnt, result));
  int ret = (minor_compact_trigger <= sstable_cnt) ? OB_SUCCESS : OB_NO_NEED_MERGE;
  ASSERT_EQ(ret, ObPartitionMergePolicy::generate_parallel_minor_interval(MINOR_MERGE, minor_compact_trigger, result, minor_range_mgr, result_array));

  for (int64_t i = 0; i < result_array.count(); ++i) {
    const ObGetMergeTablesResult &cur_result = result_array.at(i);
    int64_t table_cnt = cur_result.handle_.get_count();
    COMMON_LOG(INFO, "generate_parallel_minor_interval: ", K(minor_compact_trigger), K(sstable_cnt), K(result_cnt), K(table_cnt), K(cur_result));
  }
  ASSERT_EQ(result_array.count(), result_cnt);

  if (OB_FAIL(ret)) {
  } else if (sstable_cnt < ObPartitionMergePolicy::OB_MINOR_PARALLEL_SSTABLE_CNT_TRIGGER) {
    ASSERT_EQ(result_array.count(), 1);
    ASSERT_EQ(result_array.at(0).handle_.get_count(), sstable_cnt);
  } else {
    const int64_t table_cnt_in_dag = sstable_cnt / result_cnt;
    const int64_t table_cnt_in_last_dag = (sstable_cnt - table_cnt_in_dag * result_cnt) + table_cnt_in_dag;
    int idx = 0;
    for (int i = 0; i < result_array.count(); ++i) {
      const ObGetMergeTablesResult &cur_result = result_array.at(i);
      check_result_valid(cur_result);
      ASSERT_EQ(cur_result.scn_range_.start_scn_, get_start_log_ts(idx));
      if (i != result_array.count() - 1) {
        ASSERT_EQ(table_cnt_in_dag, cur_result.handle_.get_count());
        idx += table_cnt_in_dag;
      } else {
        ASSERT_EQ(table_cnt_in_last_dag, cur_result.handle_.get_count());
        idx += table_cnt_in_last_dag;
      }
      ASSERT_EQ(cur_result.scn_range_.end_scn_, get_end_log_ts(idx - 1));
    }
  }
}

namespace unittest
{
TEST_F(TestParallelMinorDag, test_parallel_interval)
{
  for (int64_t minor_compact_trigger = 2; minor_compact_trigger <= 16; ++minor_compact_trigger) {
    for (int64_t table_cnt = 2; table_cnt <= 64; ++table_cnt) {
      int64_t res_cnt = (minor_compact_trigger > table_cnt) ? 0 :  MAX(1, table_cnt / 10);
      COMMON_LOG(INFO, "check_result: ", K(minor_compact_trigger), K(table_cnt), K(res_cnt));
      check_result(minor_compact_trigger, table_cnt, res_cnt);
    }
  }
}

#define CHECK_IN_RANGE(start_log_ts, end_log_ts, flag) \
    fake_sstables_[0]->key_.scn_range_.start_scn_.convert_for_tx(start_log_ts); \
    fake_sstables_[0]->key_.scn_range_.end_scn_.convert_for_tx(end_log_ts); \
    ASSERT_EQ(flag, range_mgr.in_execute_range(fake_sstables_[0]));

ObScnRange construct_scn_range(const int64_t start_scn, const int64_t end_scn)
{
  ObScnRange ret_range;
  ret_range.start_scn_.convert_for_tx(start_scn);
  ret_range.end_scn_.convert_for_tx(end_scn);
  return ret_range;
}

TEST_F(TestParallelMinorDag, test_range_mgr)
{
  ObMinorExecuteRangeMgr range_mgr;
  int64_t sstable_cnt = 40;
  ObGetMergeTablesResult result;
  ASSERT_EQ(OB_SUCCESS, prepare_merge_result(sstable_cnt, result));

  range_mgr.exe_range_array_.push_back(construct_scn_range(60, 80));
  range_mgr.exe_range_array_.push_back(construct_scn_range(50, 70));
  ASSERT_EQ(OB_ERR_UNEXPECTED, range_mgr.sort_ranges());

  range_mgr.reset();
  range_mgr.exe_range_array_.push_back(construct_scn_range(60, 80));
  range_mgr.exe_range_array_.push_back(construct_scn_range(10, 20));
  range_mgr.exe_range_array_.push_back(construct_scn_range(30, 50));
  ASSERT_EQ(OB_SUCCESS, range_mgr.sort_ranges());
  COMMON_LOG(INFO, "success to sort ranges", K(range_mgr.exe_range_array_));

  CHECK_IN_RANGE(18, 19, true);
  CHECK_IN_RANGE(60, 70, true);
  CHECK_IN_RANGE(22, 30, false);
  CHECK_IN_RANGE(30, 50, true);

  range_mgr.reset();
  range_mgr.exe_range_array_.push_back(construct_scn_range(10, 20));
  range_mgr.exe_range_array_.push_back(construct_scn_range(40, 80));
  range_mgr.exe_range_array_.push_back(construct_scn_range(20, 40));
  ASSERT_EQ(OB_SUCCESS, range_mgr.sort_ranges());
  COMMON_LOG(INFO, "success to sort ranges", K(range_mgr.exe_range_array_));

  CHECK_IN_RANGE(18, 19, true);
  CHECK_IN_RANGE(60, 70, true);
  CHECK_IN_RANGE(22, 30, true);
  CHECK_IN_RANGE(30, 50, true);
  CHECK_IN_RANGE(80, 85, false);
  CHECK_IN_RANGE(30, 65, true);

  range_mgr.reset();
  range_mgr.exe_range_array_.push_back(construct_scn_range(0, 200));
  range_mgr.exe_range_array_.push_back(construct_scn_range(10, 20));
  range_mgr.exe_range_array_.push_back(construct_scn_range(40, 80));
  range_mgr.exe_range_array_.push_back(construct_scn_range(20, 40));
  ASSERT_EQ(OB_SUCCESS, range_mgr.sort_ranges());

  CHECK_IN_RANGE(100, 165, true);
}

TEST_F(TestParallelMinorDag, test_parallel_with_range_mgr)
{
  int64_t sstable_cnt = 40;
  ObGetMergeTablesResult result;
  ObArray<ObGetMergeTablesResult> result_array;
  ObMinorExecuteRangeMgr minor_range_mgr;

  minor_range_mgr.exe_range_array_.push_back(construct_scn_range(16, 21));
  minor_range_mgr.exe_range_array_.push_back(construct_scn_range(37, 41));

  ASSERT_EQ(OB_SUCCESS, prepare_merge_result(sstable_cnt, result));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::generate_parallel_minor_interval(MINOR_MERGE, 2, result, minor_range_mgr, result_array));
  ASSERT_EQ(result_array.count(), 2);

  ASSERT_EQ(result_array.at(0).scn_range_.start_scn_.get_val_for_tx(), 1);
  ASSERT_EQ(result_array.at(0).scn_range_.end_scn_.get_val_for_tx(), 16);

  ASSERT_EQ(result_array.at(1).scn_range_.start_scn_.get_val_for_tx(), 21);
  ASSERT_EQ(result_array.at(1).scn_range_.end_scn_.get_val_for_tx(), 37);


  result_array.reset();
  minor_range_mgr.reset();
  minor_range_mgr.exe_range_array_.push_back(construct_scn_range(13, 19));
  minor_range_mgr.exe_range_array_.push_back(construct_scn_range(37, 39));

  ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::generate_parallel_minor_interval(MINOR_MERGE, 2, result, minor_range_mgr, result_array));
  COMMON_LOG(INFO, "generate_parallel_minor_interval", K(result_array));
  ASSERT_EQ(result_array.count(), 1);

  ASSERT_EQ(result_array.at(0).scn_range_.start_scn_.get_val_for_tx(), 19);
  ASSERT_EQ(result_array.at(0).scn_range_.end_scn_.get_val_for_tx(), 37);


  result_array.reset();
  minor_range_mgr.reset();
  minor_range_mgr.exe_range_array_.push_back(construct_scn_range(1, 17));
  minor_range_mgr.exe_range_array_.push_back(construct_scn_range(18, 34));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::generate_parallel_minor_interval(MINOR_MERGE, 2, result, minor_range_mgr, result_array));
  ASSERT_EQ(result_array.count(), 0);


  result_array.reset();
  minor_range_mgr.reset();
  minor_range_mgr.exe_range_array_.push_back(construct_scn_range(1, 5));
  minor_range_mgr.exe_range_array_.push_back(construct_scn_range(26, 37));
  minor_range_mgr.exe_range_array_.push_back(construct_scn_range(39, 40));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::generate_parallel_minor_interval(MINOR_MERGE, 2, result, minor_range_mgr, result_array));
  ASSERT_EQ(result_array.count(), 2);

  ASSERT_EQ(result_array.at(0).scn_range_.start_scn_.get_val_for_tx(), 5);
  ASSERT_EQ(result_array.at(0).scn_range_.end_scn_.get_val_for_tx(), 15);

  ASSERT_EQ(result_array.at(1).scn_range_.start_scn_.get_val_for_tx(), 15);
  ASSERT_EQ(result_array.at(1).scn_range_.end_scn_.get_val_for_tx(), 26);



  result_array.reset();
  minor_range_mgr.reset();
  minor_range_mgr.exe_range_array_.push_back(construct_scn_range(1, 34));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::generate_parallel_minor_interval(MINOR_MERGE, 2, result, minor_range_mgr, result_array));
  COMMON_LOG(INFO, "generate_parallel_minor_interval", K(result_array));
  ASSERT_EQ(result_array.count(), 0);

  result_array.reset();
  minor_range_mgr.reset();
  minor_range_mgr.exe_range_array_.push_back(construct_scn_range(1, 30));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::generate_parallel_minor_interval(MINOR_MERGE, 2, result, minor_range_mgr, result_array));
  COMMON_LOG(INFO, "generate_parallel_minor_interval", K(result_array));
  ASSERT_EQ(result_array.count(), 1);

  ASSERT_EQ(result_array.at(0).scn_range_.start_scn_.get_val_for_tx(), 30);
  ASSERT_EQ(result_array.at(0).scn_range_.end_scn_.get_val_for_tx(), 41);


  result_array.reset();
  minor_range_mgr.reset();
  minor_range_mgr.exe_range_array_.push_back(construct_scn_range(5, 25));
  minor_range_mgr.exe_range_array_.push_back(construct_scn_range(35, 41));
  ASSERT_EQ(OB_SUCCESS, ObPartitionMergePolicy::generate_parallel_minor_interval(MINOR_MERGE, 4, result, minor_range_mgr, result_array));
  ASSERT_EQ(result_array.count(), 0);


  result_array.reset();
  minor_range_mgr.reset();
  minor_range_mgr.exe_range_array_.push_back(construct_scn_range(5, 25));
  minor_range_mgr.exe_range_array_.push_back(construct_scn_range(31, 41));
  ASSERT_EQ(OB_NO_NEED_MERGE, ObPartitionMergePolicy::generate_parallel_minor_interval(MINOR_MERGE, 16, result, minor_range_mgr, result_array));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_parallel_minor_dag.log*");
  OB_LOGGER.set_file_name("test_parallel_minor_dag.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
