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
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace omt;
using namespace share;
namespace unittest
{

class TestMockDataSplitUtil final
{
public:
  static void generate_minor_or_mds_sstable(
    const ObITable::TableType &table_type,
    const share::SCN &start_scn,
    const share::SCN &end_scn,
    ObSSTable &sstable,
    blocksstable::ObSSTableMeta &meta);
  static void push_back_sstables_scn_range(
    const int64_t start_log_ts,
    const int64_t end_log_ts,
    ObIArray<ObScnRange> &minors_range);
  static void put_incremental_minors_or_mds(
    const ObITable::TableType &table_type,
    ObTabletTableStore &tablet_table_store,
    const ObIArray<ObScnRange> &sstables_scn_range);

  static void put_split_minors(
    const bool is_shared_storage_mode,
    const ObTabletTableStore &old_table_store,
    const ObIArray<ObScnRange> &split_minors,
    const share::SCN &split_start_scn,
    const int EXPECTED_ERROR_CODE);

  static void put_split_mds(
    const bool is_shared_storage_mode,
    const ObTabletTableStore &old_table_store,
    const ObIArray<ObScnRange> &split_minors,
    const share::SCN &split_start_scn,
    const int EXPECTED_ERROR_CODE);
};

void TestMockDataSplitUtil::generate_minor_or_mds_sstable(
    const ObITable::TableType &table_type,
    const share::SCN &start_scn,
    const share::SCN &end_scn,
    ObSSTable &sstable,
    blocksstable::ObSSTableMeta &meta)
{
  int ret = OB_SUCCESS;
  meta.basic_meta_.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  meta.basic_meta_.latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  meta.basic_meta_.status_ = SSTABLE_READY_FOR_READ;
  meta.data_root_info_.addr_.set_none_addr();
  meta.macro_info_.macro_meta_info_.addr_.set_none_addr();
  meta.basic_meta_.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  meta.basic_meta_.set_upper_trans_version(2/*upper_trans_version*/);
  sstable.meta_cache_.set_upper_trans_version(2/*upper_trans_version*/);
  meta.macro_info_.entry_id_ = ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK;
  meta.is_inited_ = true;

  sstable.key_.table_type_ = table_type;
  sstable.key_.tablet_id_ = 1;
  sstable.key_.scn_range_.start_scn_ = start_scn;
  sstable.key_.scn_range_.end_scn_ = end_scn;

  sstable.meta_ = &meta;
  sstable.valid_for_reading_ = true;
}

void TestMockDataSplitUtil::push_back_sstables_scn_range(
    const int64_t start_log_ts,
    const int64_t end_log_ts,
    ObIArray<ObScnRange> &minors_range)
{
  int ret = OB_SUCCESS;
  ObScnRange scn_range;
  ret = scn_range.start_scn_.convert_for_gts(start_log_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = scn_range.end_scn_.convert_for_gts(end_log_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minors_range.push_back(scn_range);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestMockDataSplitUtil::put_incremental_minors_or_mds(
    const ObITable::TableType &table_type,
    ObTabletTableStore &tablet_table_store,
    const ObIArray<ObScnRange> &sstables_scn_range)
{
  int ret = OB_SUCCESS;
  const bool is_mds = storage::ObITable::is_mds_sstable(table_type);
  ObSSTableArray &old_sstables = is_mds ? tablet_table_store.mds_sstables_ : tablet_table_store.minor_tables_;
  old_sstables.reset();
  ObArenaAllocator tmp_arena;
  ObArray<ObITable *> tables_array;
  for (int64_t i = 0; i < sstables_scn_range.count(); i++) {
    void *buf = nullptr;
    ObSSTable *sstable = nullptr;
    ObSSTableMeta *sstable_meta = nullptr;
    ASSERT_NE(nullptr, (buf = tmp_arena.alloc(sizeof(ObSSTable))));
    sstable = new (buf) ObSSTable();
    ASSERT_NE(nullptr, (buf = tmp_arena.alloc(sizeof(ObSSTableMeta))));
    sstable_meta = new (buf) ObSSTableMeta();

    const share::SCN &start_scn = sstables_scn_range.at(i).start_scn_;
    const share::SCN &end_scn = sstables_scn_range.at(i).end_scn_;
    generate_minor_or_mds_sstable(table_type, start_scn, end_scn, *sstable, *sstable_meta);
    ret = tables_array.push_back(sstable);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = old_sstables.init(tmp_arena, tables_array, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int tables_cnt = old_sstables.count();
  ASSERT_EQ(tables_cnt, sstables_scn_range.count());
  for (int64_t i = 0; i < tables_cnt; i++) {
    ObITable *table = old_sstables.at(i);
    ASSERT_EQ(table->get_start_scn(), sstables_scn_range.at(i).start_scn_);
    ASSERT_EQ(table->get_end_scn(), sstables_scn_range.at(i).end_scn_);
  }
}

void TestMockDataSplitUtil::put_split_minors(
    const bool is_shared_storage_mode,
    const ObTabletTableStore &old_table_store,
    const ObIArray<ObScnRange> &split_minors,
    const share::SCN &split_start_scn,
    const int EXPECTED_ERROR_CODE)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_arena;
  ObTabletTableStore new_table_store;
  ObSSTableArray copied_old_store_minors;
  ret = copied_old_store_minors.init(tmp_arena, old_table_store.minor_tables_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHAStatus ha_status;
  ret = ha_status.init_status();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArray<ObITable *> tables_array;
  for (int64_t i = 0; i < split_minors.count(); i++) {
    void *buf = nullptr;
    ObSSTable *sstable = nullptr;
    ObSSTableMeta *sstable_meta = nullptr;
    ASSERT_NE(nullptr, (buf = tmp_arena.alloc(sizeof(ObSSTable))));
    sstable = new (buf) ObSSTable();
    ASSERT_NE(nullptr, (buf = tmp_arena.alloc(sizeof(ObSSTableMeta))));
    sstable_meta = new (buf) ObSSTableMeta();

    const share::SCN &start_scn = split_minors.at(i).start_scn_;
    const share::SCN &end_scn = split_minors.at(i).end_scn_;
    generate_minor_or_mds_sstable(ObITable::TableType::MINOR_SSTABLE, start_scn, end_scn, *sstable, *sstable_meta);
    ret = tables_array.push_back(sstable);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = new_table_store.build_split_minor_tables_(is_shared_storage_mode, tmp_arena, old_table_store, tables_array,
        1/*inc_base_snapshot*/, ha_status, split_start_scn);
  ASSERT_EQ(EXPECTED_ERROR_CODE, ret);
  STORAGE_LOG(INFO, "update minor tables finished", K(ret), K(ObPrintTableStore(new_table_store)));
  if (OB_SUCCESS == ret) { // update split_minors successfully.
    const int old_store_tables_cnt = copied_old_store_minors.count();
    const int new_store_tables_cnt = new_table_store.minor_tables_.count();
    const SCN expected_max_end_scn = old_store_tables_cnt == 0 ? split_start_scn : copied_old_store_minors[old_store_tables_cnt - 1]->get_end_scn();
    ASSERT_GT(new_store_tables_cnt, 0);
    share::SCN previous_end_scn = new_table_store.minor_tables_[0]->get_start_scn();
    if (is_shared_storage_mode) {
      ASSERT_EQ(previous_end_scn, split_minors.at(0).start_scn_);
    } else {
      ASSERT_LE(previous_end_scn, split_minors.at(0).start_scn_);
    }
    for (int64_t i = 0; i < new_store_tables_cnt; i++) { // check continuous.
      ObITable *table = new_table_store.minor_tables_.at(i);
      ASSERT_GE(table->get_end_scn(), table->get_start_scn());
      ASSERT_EQ(table->get_start_scn(), previous_end_scn);
      previous_end_scn = table->get_end_scn();
      if (i == new_store_tables_cnt - 1) {
        ASSERT_EQ(table->get_end_scn(), expected_max_end_scn);
      }
    }
  } else {
    ASSERT_EQ(copied_old_store_minors.count(), old_table_store.minor_tables_.count());
    for (int64_t i = 0; i < copied_old_store_minors.count(); i++) {
      ObITable *old_table = copied_old_store_minors.at(i);
      ObITable *new_table = old_table_store.minor_tables_.at(i);
      ASSERT_EQ(old_table->get_start_scn(), new_table->get_start_scn());
      ASSERT_EQ(old_table->get_end_scn(), new_table->get_end_scn());
    }
  }
}

void TestMockDataSplitUtil::put_split_mds(
    const bool is_shared_storage_mode,
    const ObTabletTableStore &old_table_store,
    const ObIArray<ObScnRange> &split_minors,
    const share::SCN &split_start_scn,
    const int EXPECTED_ERROR_CODE)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(1, split_minors.count());
  ObArenaAllocator tmp_arena;
  ObTabletTableStore new_table_store;
  UpdateUpperTransParam unused_param;
  ObSSTableArray copied_old_store_mds;
  ret = copied_old_store_mds.init(tmp_arena, old_table_store.mds_sstables_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHAStatus ha_status;
  ret = ha_status.init_status();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArray<ObITable *> tables_array;
  for (int64_t i = 0; i < split_minors.count(); i++) {
    void *buf = nullptr;
    ObSSTable *sstable = nullptr;
    ObSSTableMeta *sstable_meta = nullptr;
    ASSERT_NE(nullptr, (buf = tmp_arena.alloc(sizeof(ObSSTable))));
    sstable = new (buf) ObSSTable();
    ASSERT_NE(nullptr, (buf = tmp_arena.alloc(sizeof(ObSSTableMeta))));
    sstable_meta = new (buf) ObSSTableMeta();

    const share::SCN &start_scn = split_minors.at(i).start_scn_;
    const share::SCN &end_scn = split_minors.at(i).end_scn_;
    generate_minor_or_mds_sstable(ObITable::TableType::MDS_MINI_SSTABLE, start_scn, end_scn, *sstable, *sstable_meta);
    ret = tables_array.push_back(sstable);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = new_table_store.build_minor_tables(
        tmp_arena,
        static_cast<ObSSTable *>(tables_array.at(0)),
        old_table_store,
        false/*need_check_sstable*/,
        -1/*inc_base_snapshot_version*/,
        ha_status,
        unused_param,
        true/*is_mds*/,
        is_shared_storage_mode/*allow_adjust_next_start_scn*/);
  new_table_store.is_inited_ = true;
  if (OB_SUCCESS == ret) {
    ret = new_table_store.check_continuous();
    if (OB_SUCCESS != ret) {
      STORAGE_LOG(INFO, "revise ret_code", K(ret), K(ObPrintTableStore(new_table_store)));
      new_table_store.reset();
    }
  }
  ASSERT_EQ(EXPECTED_ERROR_CODE, ret);
  STORAGE_LOG(INFO, "update mds tables finished", K(ret), K(ObPrintTableStore(new_table_store)));
  if (OB_SUCCESS == ret) { // update split_minors successfully.
    const int old_store_tables_cnt = copied_old_store_mds.count();
    const int new_store_tables_cnt = new_table_store.mds_sstables_.count();
    ASSERT_GT(new_store_tables_cnt, 0);
    const SCN expected_max_end_scn = old_store_tables_cnt == 0 ? split_start_scn : copied_old_store_mds[old_store_tables_cnt - 1]->get_end_scn();
    share::SCN previous_end_scn = new_table_store.mds_sstables_[0]->get_start_scn();
    if (is_shared_storage_mode) {
      ASSERT_EQ(previous_end_scn, split_minors.at(0).start_scn_);
    } else {
      ASSERT_LE(previous_end_scn, split_minors.at(0).start_scn_);
    }
    for (int64_t i = 0; i < new_store_tables_cnt; i++) { // check continuous.
      ObITable *table = new_table_store.mds_sstables_.at(i);
      ASSERT_GE(table->get_end_scn(), table->get_start_scn());
      ASSERT_EQ(table->get_start_scn(), previous_end_scn);
      previous_end_scn = table->get_end_scn();
      if (i == new_store_tables_cnt - 1) {
        ASSERT_EQ(table->get_end_scn(), expected_max_end_scn);
      }
    }
  } else {
    ASSERT_EQ(EXPECTED_ERROR_CODE, ret);
    ASSERT_EQ(copied_old_store_mds.count(), old_table_store.mds_sstables_.count());
    for (int64_t i = 0; i < copied_old_store_mds.count(); i++) {
      ObITable *old_table = copied_old_store_mds.at(i);
      ObITable *new_table = old_table_store.mds_sstables_.at(i);
      ASSERT_EQ(old_table->get_start_scn(), new_table->get_start_scn());
      ASSERT_EQ(old_table->get_end_scn(), new_table->get_end_scn());
    }
  }
}


class TestSplitUpdateTableStore : public ::testing::Test
{
public:
  TestSplitUpdateTableStore()
    : arena_allocator_(),
      split_start_scn_()
  { }
  ~TestSplitUpdateTableStore() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
private:
  ObArenaAllocator arena_allocator_;
  share::SCN split_start_scn_;
  DISALLOW_COPY_AND_ASSIGN(TestSplitUpdateTableStore);
};

void TestSplitUpdateTableStore::SetUpTestCase()
{
  STORAGE_LOG(INFO, "TestSplitUpdateTableStore::SetUpTestCase");
}

void TestSplitUpdateTableStore::TearDownTestCase()
{
}

void TestSplitUpdateTableStore::SetUp()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "TestSplitUpdateTableStore::SetUp");
  ret = split_start_scn_.convert_for_gts(500);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestSplitUpdateTableStore::TearDown()
{
  arena_allocator_.reset();
}

TEST_F(TestSplitUpdateTableStore, sn_put_split_minor_firstly)
{
  int ret = OB_SUCCESS;
  ObTabletTableStore table_store;
  const bool is_shared_storage_mode = false;
  ObArray<ObScnRange> incremental_minors;
  ObArray<ObScnRange> split_minors;
  { // without incremental data.
    {
      STORAGE_LOG(INFO, "split-minors' end_scn < split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 400, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      STORAGE_LOG(INFO, "split-minors' end_scn = split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_SUCCESS);
    }
    {
      STORAGE_LOG(INFO, "split-minors' end_scn > split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 600, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
  }

  { // with incremental data.
    {
      STORAGE_LOG(INFO, "split_minors'end_scn < split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(500, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 400, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      STORAGE_LOG(INFO, "split_minors'end_scn > split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(500, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 600, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      STORAGE_LOG(INFO, "split_minors'end_scn == split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(500, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_SUCCESS);
    }
    {
      STORAGE_LOG(INFO, "incremental_minors'start_scn < split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(400, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      STORAGE_LOG(INFO, "incremental_minors'start_scn > split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(600, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
  }
}

TEST_F(TestSplitUpdateTableStore, sn_put_split_minor_repeatedly)
{
  int ret = OB_SUCCESS;
  const bool is_shared_storage_mode = false;
  ObArray<ObScnRange> incremental_minors;
  ObArray<ObScnRange> split_minors;
  ObTabletTableStore table_store;
  { // without incremental data.
    {
      STORAGE_LOG(INFO, "split-minors' end_scn < split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 400, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      STORAGE_LOG(INFO, "split-minors' end_scn = split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_SUCCESS);
    }
    {
      STORAGE_LOG(INFO, "split-minors' end_scn > split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 600, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      STORAGE_LOG(INFO, "split-minors' start_scn < old_store's start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(1, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      STORAGE_LOG(INFO, "split-minors' start_scn > old_store's start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_SUCCESS);
    }
  }

  { // with incremental data.
    {
      STORAGE_LOG(INFO, "split_minors'end_scn < split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 400, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      STORAGE_LOG(INFO, "split_minors'end_scn > split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 600, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      STORAGE_LOG(INFO, "split_minors'end_scn == split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_SUCCESS);
    }
    {
      STORAGE_LOG(INFO, "split_minors'start_scn < old_store's start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(1, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      STORAGE_LOG(INFO, "split_minors'start_scn > old_store's start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_SUCCESS);
    }
  }
}

TEST_F(TestSplitUpdateTableStore, ss_put_split_minor_firstly)
{
  int ret = OB_SUCCESS;
  ObTabletTableStore table_store;
  const bool is_shared_storage_mode = true;
  ObArray<ObScnRange> incremental_minors;
  ObArray<ObScnRange> split_minors;
  { // without incremental data.
    {
      STORAGE_LOG(INFO, "split-minors' end_scn < split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 400, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      STORAGE_LOG(INFO, "split-minors' end_scn = split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_SUCCESS);
    }
    {
      STORAGE_LOG(INFO, "split-minors' end_scn > split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 600, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
  }

  { // with incremental data.
    {
      // split_minors'end_scn < split_start_scn
      STORAGE_LOG(INFO, "split_minors'end_scn < split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(500, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 400, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      // split_minors'end_scn > split_start_scn
      STORAGE_LOG(INFO, "split_minors'end_scn > split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(500, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 600, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_SUCCESS);
    }
    {
      // split_minors'end_scn == split_start_scn
      STORAGE_LOG(INFO, "split_minors'end_scn == split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(500, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_SUCCESS);
    }
    {
      STORAGE_LOG(INFO, "incremental_minors'start_scn > split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(600, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      STORAGE_LOG(INFO, "incremental_minors'start_scn < split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
  }
}

TEST_F(TestSplitUpdateTableStore, ss_put_split_minor_repeatedly)
{
  int ret = OB_SUCCESS;
  const bool is_shared_storage_mode = true;
  ObArray<ObScnRange> incremental_minors;
  ObArray<ObScnRange> split_minors;
  ObTabletTableStore table_store;
  { // without incremental data.
    {
      STORAGE_LOG(INFO, "split-minors' end_scn < split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 400, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      STORAGE_LOG(INFO, "split-minors' end_scn = split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_SUCCESS);
    }
    {
      STORAGE_LOG(INFO, "split-minors' end_scn > split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 600, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      STORAGE_LOG(INFO, "split-minors' start_scn < old_store's start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(1, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      STORAGE_LOG(INFO, "split-minors' start_scn > old_store's start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
  }

  { // with incremental data.
    {
      STORAGE_LOG(INFO, "split_minors'end_scn < split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 400, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      STORAGE_LOG(INFO, "split_minors'end_scn > split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 600, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_SUCCESS);
    }
    {
      STORAGE_LOG(INFO, "split_minors'end_scn = split_start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_SUCCESS);
    }
    {
      STORAGE_LOG(INFO, "split_minors' end_scn > old_store's end_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 1200, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      // split_minors'start_scn < old_store's start_scn
      STORAGE_LOG(INFO, "split_minors' start_scn < old_store's start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(1, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
    {
      // split_minors'start_scn > old_store's start_scn
      STORAGE_LOG(INFO, "split_minors' start_scn > old_store's start_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 500, split_minors);
      TestMockDataSplitUtil::put_split_minors(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_UNEXPECTED);
    }
  }
}

TEST_F(TestSplitUpdateTableStore, put_split_mds_firstly)
{
  int ret = OB_SUCCESS;
  ObTabletTableStore table_store;
  ObArray<ObScnRange> incremental_minors;
  ObArray<ObScnRange> split_minors;
  for (int i = 0; i < 2; i++)
  {
    bool is_shared_storage_mode = (i % 2 == 0) ? false : true;
    { // without incremental data.
      {
        STORAGE_LOG(INFO, "split-minors' end_scn = split_start_scn", K(is_shared_storage_mode));
        table_store.reset();
        incremental_minors.reset();
        split_minors.reset();
        TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, split_minors);
        TestMockDataSplitUtil::put_split_mds(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_SUCCESS);
      }
    }

    { // with incremental data.
      {
        STORAGE_LOG(INFO, "split_minors'end_scn < split_start_scn", K(is_shared_storage_mode));
        table_store.reset();
        incremental_minors.reset();
        split_minors.reset();
        TestMockDataSplitUtil::push_back_sstables_scn_range(500, 1000, incremental_minors);
        TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MDS_MINI_SSTABLE, table_store, incremental_minors);
        TestMockDataSplitUtil::push_back_sstables_scn_range(100, 400, split_minors);
        TestMockDataSplitUtil::put_split_mds(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_ERR_SYS);
      }
      {
        STORAGE_LOG(INFO, "split_minors'end_scn > split_start_scn", K(is_shared_storage_mode));
        table_store.reset();
        incremental_minors.reset();
        split_minors.reset();
        TestMockDataSplitUtil::push_back_sstables_scn_range(500, 1000, incremental_minors);
        TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MDS_MINI_SSTABLE, table_store, incremental_minors);
        TestMockDataSplitUtil::push_back_sstables_scn_range(100, 600, split_minors);
        TestMockDataSplitUtil::put_split_mds(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_MINOR_SSTABLE_RANGE_CROSS);
      }
      {
        STORAGE_LOG(INFO, "split_minors'end_scn = split_start_scn", K(is_shared_storage_mode));
        table_store.reset();
        incremental_minors.reset();
        split_minors.reset();
        TestMockDataSplitUtil::push_back_sstables_scn_range(500, 800, incremental_minors);
        TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
        TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MDS_MINI_SSTABLE, table_store, incremental_minors);
        TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, split_minors);
        TestMockDataSplitUtil::put_split_mds(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_SUCCESS);
      }
    }
  }
}

TEST_F(TestSplitUpdateTableStore, put_split_mds_repeatedly)
{
  int ret = OB_SUCCESS;
  ObTabletTableStore table_store;
  ObArray<ObScnRange> incremental_minors;
  ObArray<ObScnRange> split_minors;
  for (int i = 0; i < 2; i++)
  {
    bool is_shared_storage_mode = (i % 2 == 0) ? false : true;
    { // without incremental data.
      {
        STORAGE_LOG(INFO, "split-minors' end_scn = split_start_scn", K(is_shared_storage_mode));
        table_store.reset();
        incremental_minors.reset();
        split_minors.reset();
        TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
        TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MDS_MINI_SSTABLE, table_store, incremental_minors);
        TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, split_minors);
        TestMockDataSplitUtil::put_split_mds(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_SUCCESS);
      }
    }

    { // with incremental data.
      {
        STORAGE_LOG(INFO, "split_minors'end_scn = split_start_scn", K(is_shared_storage_mode));
        table_store.reset();
        incremental_minors.reset();
        split_minors.reset();
        TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
        TestMockDataSplitUtil::push_back_sstables_scn_range(500, 1000, incremental_minors);
        TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MDS_MINI_SSTABLE, table_store, incremental_minors);
        TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, split_minors);
        TestMockDataSplitUtil::put_split_mds(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_SUCCESS);
      }
      {
        STORAGE_LOG(INFO, "split_minors'end_scn = split_start_scn", K(is_shared_storage_mode));
        table_store.reset();
        incremental_minors.reset();
        split_minors.reset();
        TestMockDataSplitUtil::push_back_sstables_scn_range(100, 1000, incremental_minors);
        TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MDS_MINI_SSTABLE, table_store, incremental_minors);
        TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, split_minors);
        TestMockDataSplitUtil::put_split_mds(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_MINOR_SSTABLE_RANGE_CROSS);
      }
      {
        STORAGE_LOG(INFO, "split_minors'end_scn = split_start_scn", K(is_shared_storage_mode));
        table_store.reset();
        incremental_minors.reset();
        split_minors.reset();
        TestMockDataSplitUtil::push_back_sstables_scn_range(100, 800, incremental_minors);
        TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
        TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MDS_MINI_SSTABLE, table_store, incremental_minors);
        TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, split_minors);
        TestMockDataSplitUtil::put_split_mds(is_shared_storage_mode, table_store, split_minors, split_start_scn_, OB_MINOR_SSTABLE_RANGE_CROSS);
      }
    }
  }
}


}//blocksstable
}//oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_data_split_update_table_store.log*");
  OB_LOGGER.set_file_name("test_data_split_update_table_store.log");
  OB_LOGGER.set_log_level("TRACE");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40L << 30);
  return RUN_ALL_TESTS();
}
