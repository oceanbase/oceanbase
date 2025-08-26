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
#include "storage/ddl/ob_tablet_split_util.h"
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
  static void check_update_rules(
    ObTabletTableStore &tablet_table_store,
    const ObIArray<ObScnRange> &split_minors,
    const int expected_error_code,
    const bool expected_update_firstly);
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

void TestMockDataSplitUtil::check_update_rules(
    ObTabletTableStore &tablet_table_store,
    const ObIArray<ObScnRange> &split_minors,
    const int expected_error_code,
    const bool expected_update_firstly)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_arena;
  bool is_update_firstly = true;
  ObArray<ObITable *> new_tables_array;
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
    ret = new_tables_array.push_back(sstable);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = ObTabletSplitUtil::check_split_minors_can_be_accepted(
    tablet_table_store.minor_tables_,
    new_tables_array,
    is_update_firstly);
  ASSERT_EQ(expected_error_code, ret);
  if (OB_SUCCESS == ret) {
    ASSERT_EQ(expected_update_firstly, is_update_firstly);
  }
}

class TestSplitUpdateTableStore : public ::testing::Test
{
public:
  TestSplitUpdateTableStore()
    : arena_allocator_()
  { }
  ~TestSplitUpdateTableStore() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
private:
  ObArenaAllocator arena_allocator_;
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
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestSplitUpdateTableStore::TearDown()
{
  arena_allocator_.reset();
}

TEST_F(TestSplitUpdateTableStore, put_split_minor_firstly)
{
  int ret = OB_SUCCESS;
  ObTabletTableStore table_store;
  ObArray<ObScnRange> incremental_minors;
  ObArray<ObScnRange> split_minors;
  { // without incremental data.
    {
      STORAGE_LOG(INFO, "1. put split minors without incremental data");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 400, split_minors);
      TestMockDataSplitUtil::check_update_rules(table_store, split_minors, OB_SUCCESS/*expected_error_code*/, true/*expected_update_firstly*/);
    }
  }

  { // with incremental data.
    {
      STORAGE_LOG(INFO, "2. not continuous with smaller split-minors' end_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(500, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 400, split_minors);
      TestMockDataSplitUtil::check_update_rules(table_store, split_minors, OB_ERR_UNEXPECTED, true);
    }
    {
      STORAGE_LOG(INFO, "3. not continuous with larger split-minors' end_scn");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(500, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 600, split_minors);
      TestMockDataSplitUtil::check_update_rules(table_store, split_minors, OB_ERR_UNEXPECTED, true);
    }
    {
      STORAGE_LOG(INFO, "4. continuous");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(500, 800, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(800, 1000, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 200, split_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 500, split_minors);
      TestMockDataSplitUtil::check_update_rules(table_store, split_minors, OB_SUCCESS, true);
    }
  }
}

TEST_F(TestSplitUpdateTableStore, sn_put_split_minor_repeatedly)
{
  int ret = OB_SUCCESS;
  ObArray<ObScnRange> incremental_minors;
  ObArray<ObScnRange> split_minors;
  ObTabletTableStore table_store;
  {
    {
      STORAGE_LOG(INFO, "1. update repeatedly with a smaller scope");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 400, split_minors);
      TestMockDataSplitUtil::check_update_rules(table_store, split_minors, OB_SUCCESS, false);
    }
    {
      STORAGE_LOG(INFO, "2.update repeatedly with a same scope");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, split_minors);
      TestMockDataSplitUtil::check_update_rules(table_store, split_minors, OB_SUCCESS, false);
    }
    {
      STORAGE_LOG(INFO, "3. update repeatedly with a smaller scope");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 500, split_minors);
      TestMockDataSplitUtil::check_update_rules(table_store, split_minors, OB_SUCCESS, false);
    }
    {
      STORAGE_LOG(INFO, "4. illegal repeated update.");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 600, split_minors);
      TestMockDataSplitUtil::check_update_rules(table_store, split_minors, OB_ERR_UNEXPECTED, false);
    }
    {
      STORAGE_LOG(INFO, "5. illegal repeated update.");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(200, 600, split_minors);
      TestMockDataSplitUtil::check_update_rules(table_store, split_minors, OB_ERR_UNEXPECTED, false);
    }
    {
      STORAGE_LOG(INFO, "5. illegal repeated update.");
      table_store.reset();
      incremental_minors.reset();
      split_minors.reset();
      TestMockDataSplitUtil::push_back_sstables_scn_range(100, 500, incremental_minors);
      TestMockDataSplitUtil::put_incremental_minors_or_mds(ObITable::TableType::MINOR_SSTABLE, table_store, incremental_minors);
      TestMockDataSplitUtil::push_back_sstables_scn_range(1, 500, split_minors);
      TestMockDataSplitUtil::check_update_rules(table_store, split_minors, OB_ERR_UNEXPECTED, false);
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
