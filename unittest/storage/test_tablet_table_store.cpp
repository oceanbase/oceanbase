/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
**/


#define UNITTEST_DEBUG
#define USING_LOG_PREFIX STORAGE
#include <gmock/gmock.h>

#define private public
#define protected public

#include "storage/mockcontainer/mock_ob_iterator.h"
#include "storage/ob_storage_struct.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/tablet/ob_table_store_util.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace memtable;
using namespace share::schema;
using namespace share;
using namespace compaction;
using namespace transaction;

namespace storage
{
  bool ObUpdateTableStoreParam::is_valid() const { return true; }
}

namespace unittest
{

ObITable::TableType TYPE_MAP[] = {
  ObITable::TableType::DATA_MEMTABLE,
  ObITable::TableType::TX_DATA_MEMTABLE,
  ObITable::TableType::TX_CTX_MEMTABLE,
  ObITable::TableType::LOCK_MEMTABLE,
  ObITable::TableType::DIRECT_LOAD_MEMTABLE,
  ObITable::TableType::MAX_MEMTABLE_TYPE,
  ObITable::TableType::MAX_MEMTABLE_TYPE,
  ObITable::TableType::MAX_MEMTABLE_TYPE,
  ObITable::TableType::MAX_MEMTABLE_TYPE,
  ObITable::TableType::MAX_MEMTABLE_TYPE,
  ObITable::TableType::MAJOR_SSTABLE,
  ObITable::TableType::MINOR_SSTABLE,
  ObITable::TableType::MINI_SSTABLE,
  ObITable::TableType::META_MAJOR_SSTABLE,
  ObITable::TableType::DDL_DUMP_SSTABLE,
  ObITable::TableType::REMOTE_LOGICAL_MINOR_SSTABLE,
  ObITable::TableType::DDL_MEM_SSTABLE,
  ObITable::TableType::COLUMN_ORIENTED_SSTABLE,
  ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE,
  ObITable::TableType::ROWKEY_COLUMN_GROUP_SSTABLE,
  ObITable::TableType::COLUMN_ORIENTED_META_SSTABLE,
  ObITable::TableType::DDL_MERGE_CO_SSTABLE,
  ObITable::TableType::DDL_MERGE_CG_SSTABLE,
  ObITable::TableType::DDL_MEM_CO_SSTABLE,
  ObITable::TableType::DDL_MEM_CG_SSTABLE,
  ObITable::TableType::DDL_MEM_MINI_SSTABLE,
  ObITable::TableType::MDS_MINI_SSTABLE,
  ObITable::TableType::MDS_MINOR_SSTABLE,
  ObITable::TableType::MICRO_MINI_SSTABLE,
  ObITable::TableType::INC_MAJOR_SSTABLE,
  ObITable::TableType::INC_COLUMN_ORIENTED_SSTABLE,
  ObITable::TableType::INC_NORMAL_COLUMN_GROUP_SSTABLE,
  ObITable::TableType::INC_ROWKEY_COLUMN_GROUP_SSTABLE,
  ObITable::TableType::MAX_TABLE_TYPE
};

class TestTableStore : public ::testing::Test
{
public:
  TestTableStore();
  virtual ~TestTableStore();
  virtual void SetUp();
  virtual void TearDown();
  int mock_sstable(const ObITable::TableKey &key, ObSSTable *&sstable);
  int mock_memtable(const ObITable::TableKey &key, ObMemtable *&memtable);
  int batch_mock_tables();
  int mock_tablet(ObTablet *&tablet);
  int mock_old_table_store(ObTabletTableStore *table_store);
private:
  ObArenaAllocator allocator_;
  ObMockIterator data_iter_;
  ObTabletID tablet_id_;
  const char *key_data_;
  ObArray<ObITable *> sstables_;
  ObArray<ObMemtable *> memtables_;
};

TestTableStore::TestTableStore()
  : allocator_(),
    data_iter_(),
    tablet_id_(200001),
    key_data_(nullptr),
    sstables_(),
    memtables_()
{
}

TestTableStore::~TestTableStore()
{
  allocator_.reset();
  data_iter_.reset();
  sstables_.reset();
  memtables_.reset();
  key_data_ = nullptr;
}

void TestTableStore::SetUp()
{
}

void TestTableStore::TearDown()
{
}

int TestTableStore::mock_sstable(const ObITable::TableKey &key, ObSSTable *&sstable)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  sstable = nullptr;

  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSTable)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for sstable", K(ret));
  } else if (OB_ISNULL(sstable = new (buf) ObSSTable())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for sstable", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSTableMeta)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for sstable meta", K(ret));
  } else if (OB_ISNULL(sstable->meta_ = new (buf) ObSSTableMeta())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for sstable meta", K(ret));
  } else {
    sstable->meta_->is_inited_ = true;
    sstable->key_ = key;
    sstable->valid_for_reading_ = true;

    if (sstable->is_meta_major_sstable()) {
      sstable->meta_cache_.max_merged_trans_version_ = key.scn_range_.end_scn_.get_val_for_tx();
      sstable->meta_->basic_meta_.max_merged_trans_version_ = key.scn_range_.end_scn_.get_val_for_tx();
    } else if (sstable->is_inc_major_type_sstable()) {
      compaction::ObUncommitTxDesc tx_desc(100, 200);
      ObMemUncommitTxInfo tx_info;
      tx_info.set_info_status_only_tx_id();
      EXPECT_EQ(OB_SUCCESS, tx_info.push_back(tx_desc));
      EXPECT_EQ(OB_SUCCESS, sstable->meta_->uncommit_tx_info_.init(allocator_, tx_info));

      sstable->meta_->basic_meta_.data_macro_block_count_ = 1;
      sstable->meta_cache_.data_macro_block_count_ = 1;
    } else if (sstable->is_inc_major_ddl_sstable()) {
      compaction::ObUncommitTxDesc tx_desc(100, 200);
      ObMemUncommitTxInfo tx_info;
      tx_info.set_info_status_only_tx_id();
      EXPECT_EQ(OB_SUCCESS, tx_info.push_back(tx_desc));
      EXPECT_EQ(OB_SUCCESS, sstable->meta_->uncommit_tx_info_.init(allocator_, tx_info));

      sstable->meta_->basic_meta_.data_macro_block_count_ = 1;
      sstable->meta_cache_.data_macro_block_count_ = 1;
    }

  }
  return ret;
}

int TestTableStore::mock_memtable(const ObITable::TableKey &key, ObMemtable *&memtable)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  memtable = nullptr;

  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMemtable)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for memtable", K(ret));
  } else if (OB_ISNULL(memtable = new (buf) ObMemtable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to allocate memory for memtable", K(ret));
  } else {
    memtable->key_ = key;
    memtable->is_inited_ = true;
  }
  return ret;
}

int TestTableStore::batch_mock_tables()
{
  int ret = OB_SUCCESS;
  sstables_.reset();
  memtables_.reset();

  if (nullptr != key_data_) {
    data_iter_.from(key_data_);
    for (int64_t idx = 0; idx < data_iter_.count(); ++idx) {
      const ObStoreRow *row = nullptr;
      data_iter_.get_row(idx, row);
      const ObObj *cells = row->row_val_.cells_;

      ObITable::TableKey table_key;
      table_key.tablet_id_ = tablet_id_;
      table_key.table_type_ = TYPE_MAP[cells[0].get_int()];
      table_key.scn_range_.start_scn_.convert_for_tx(cells[1].get_int());
      table_key.scn_range_.end_scn_.convert_for_tx(cells[2].get_int());

      if (ObITable::is_sstable(table_key.table_type_)) {
        ObSSTable *sstable = nullptr;
        if (OB_FAIL(mock_sstable(table_key, sstable))) {
          LOG_WARN("failed to mock sstable", K(ret), K(table_key));
        } else if (FALSE_IT(sstable->meta_->basic_meta_.max_merged_trans_version_ = cells[3].get_int())) {
        } else if (FALSE_IT(sstable->meta_->basic_meta_.upper_trans_version_ = 99999 == cells[4].get_int()
                                                                            ? INT64_MAX
                                                                            : cells[4].get_int())) {
        } else if (FALSE_IT(sstable->meta_cache_.max_merged_trans_version_ = cells[3].get_int())) {
        } else if (FALSE_IT(sstable->meta_cache_.upper_trans_version_ = sstable->meta_->basic_meta_.upper_trans_version_)) {
        } else if (OB_FAIL(sstables_.push_back(sstable))) {
          LOG_WARN("failed to add sstable", K(ret), KPC(sstable));
        }
      } else {
        ObMemtable *memtable = nullptr;
        if (OB_FAIL(mock_memtable(table_key, memtable))) {
          LOG_WARN("failed to mock sstable", K(ret), K(table_key));
        } else if (FALSE_IT(memtable->snapshot_version_.convert_for_tx(cells[3].get_int()))) {
        } else if (OB_FAIL(memtables_.push_back(memtable))) {
          LOG_WARN("failed to add memtable", K(ret), KPC(memtable));
        }
      }
    }
  }
  return ret;
}

int TestTableStore::mock_old_table_store(ObTabletTableStore *table_store)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(batch_mock_tables())) {
    LOG_WARN("failed to mock tables", K(ret));
  } else if (!sstables_.empty()) {
    ObArray<ObMetaDiskAddr> addrs;
    ObMetaDiskAddr addr;
    addr.set_mem_addr(1, 1);

    ObMajorChecksumInfo ckm_info;
    for (int64_t i = 0; i < sstables_.count(); ++i) {
      if (OB_FAIL(addrs.push_back(addr))) {
        LOG_WARN("failed to add addr", K(ret));
      }
    }

    if (FAILEDx(table_store->init(allocator_, sstables_, addrs, ckm_info))) {
      LOG_WARN("failed to init table store", K(ret));
    }
  }
  return ret;
}

int TestTableStore::mock_tablet(ObTablet *&tablet)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  tablet = nullptr;

  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTablet)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for tablet", K(ret));
  } else if (OB_ISNULL(tablet = new (buf) ObTablet())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for tablet", K(ret));
  } else {
    tablet->is_inited_ = true;
  }
  return ret;
}


TEST_F(TestTableStore, test_basic)
{
  key_data_ = nullptr;
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            1          1          1        \n"
              "11            1            150        150        150      \n"
              "11            150          200        200        200      \n"
              "0             200          250        250        250      \n"
              "0             250          300        300        300      \n"
              "0             300          350        350        350      \n";
  ObTabletTableStore table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&table_store));
  EXPECT_EQ(1, table_store.major_tables_.count());
  EXPECT_EQ(2, table_store.minor_tables_.count());
}


TEST_F(TestTableStore, test_first_init_table_store)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));

  ObITable::TableKey major_key;
  major_key.tablet_id_ = tablet_id_;
  major_key.scn_range_.start_scn_.set_min();
  major_key.scn_range_.end_scn_.set_base();
  major_key.table_type_ = ObITable::TableType::META_MAJOR_SSTABLE;

  ObSSTable *new_major = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(major_key, new_major));

  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_INVALID_ARGUMENT, new_table_store.init(allocator_, *tablet, new_major));

  new_major->key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, new_major));
}


TEST_F(TestTableStore, test_basic_add_new_major)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            1          1          1        \n"
              "11            1            150        150        150      \n"
              "11            150          200        200        200      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.minor_tables_.count());


  ObSSTable *new_major = nullptr;
  ObITable::TableKey major_key;
  major_key.tablet_id_ = tablet_id_;
  major_key.scn_range_.start_scn_.set_min();
  major_key.scn_range_.end_scn_.convert_for_tx(200);
  major_key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(major_key, new_major));


  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 150;
  param.sstable_ = new_major;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::MAJOR_MERGE;
  param.compaction_info_.need_report_ = true;


  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(2, new_table_store.major_tables_.count());
  EXPECT_EQ(2, new_table_store.minor_tables_.count());


  new_table_store.reset();
  param.multi_version_start_ = 200;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(0, new_table_store.minor_tables_.count());
}


TEST_F(TestTableStore, test_basic_add_new_minor)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(300);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            1          1          1        \n"
              "11            100          150        150        150      \n"
              "11            150          200        200        200      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.minor_tables_.count());


  ObSSTable *new_minor = nullptr;
  ObITable::TableKey minor_key;
  minor_key.tablet_id_ = tablet_id_;
  minor_key.scn_range_.start_scn_.convert_for_tx(200);
  minor_key.scn_range_.end_scn_.convert_for_tx(300);
  minor_key.table_type_ = ObITable::TableType::MINI_SSTABLE;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(minor_key, new_minor));


  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 50;
  param.sstable_ = new_minor;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::MINI_MERGE;
  param.compaction_info_.need_report_ = false;


  // test add new mini sstable after mini merge
  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(3, new_table_store.minor_tables_.count());


  // test add a mini sstable which end scn not equals to the clog checkpoint scn
  new_table_store.reset();
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  EXPECT_EQ(OB_ERR_UNEXPECTED, new_table_store.init(allocator_, *tablet, param, old_table_store));


  // test add a new minor which cover all mini sstables in old store
  new_table_store.reset();
  new_minor->key_.scn_range_.start_scn_.convert_for_tx(100);
  new_minor->key_.scn_range_.end_scn_.convert_for_tx(200);
  new_minor->meta_cache_.upper_trans_version_ = 200;
  new_minor->meta_->basic_meta_.upper_trans_version_ = 200;
  param.compaction_info_.merge_type_ = ObMergeType::MINOR_MERGE;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(1, new_table_store.minor_tables_.count());


  // test add a range crossed sstable
  new_table_store.reset();
  new_minor->key_.scn_range_.start_scn_.convert_for_tx(50);
  new_minor->key_.scn_range_.end_scn_.convert_for_tx(180);
  EXPECT_EQ(OB_MINOR_SSTABLE_RANGE_CROSS, new_table_store.init(allocator_, *tablet, param, old_table_store));


  // test add a new table which not cover any mini sstable in minor merge
  new_table_store.reset();
  new_minor->key_.scn_range_.start_scn_.convert_for_tx(50);
  new_minor->key_.scn_range_.end_scn_.convert_for_tx(60);
  EXPECT_EQ(OB_NO_NEED_MERGE, new_table_store.init(allocator_, *tablet, param, old_table_store));

  new_table_store.reset();
  new_minor->key_.scn_range_.start_scn_.convert_for_tx(250);
  new_minor->key_.scn_range_.end_scn_.convert_for_tx(300);
  EXPECT_EQ(OB_NO_NEED_MERGE, new_table_store.init(allocator_, *tablet, param, old_table_store));
}


TEST_F(TestTableStore, test_basic_add_new_inc_major)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            50         50         50       \n"
              "11            50           150        150        150      \n"
              "11            150          200        200        200      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.minor_tables_.count());


  ObSSTable *new_inc_major = nullptr;
  ObITable::TableKey major_key;
  major_key.tablet_id_ = tablet_id_;
  major_key.scn_range_.start_scn_.set_min();
  major_key.scn_range_.end_scn_.convert_for_tx(250);
  major_key.table_type_ = ObITable::TableType::INC_MAJOR_SSTABLE;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(major_key, new_inc_major));
  new_inc_major->meta_->basic_meta_.upper_trans_version_ = 250;
  new_inc_major->meta_cache_.upper_trans_version_ = 250;


  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 150;
  param.sstable_ = new_inc_major;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::MAJOR_MERGE;
  param.compaction_info_.need_report_ = true;


  // test add a new committed inc major
  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(2, new_table_store.minor_tables_.count());
  EXPECT_EQ(1, new_table_store.inc_major_tables_.count());


  // test add a aborted inc major
  new_table_store.reset();
  new_inc_major->meta_->basic_meta_.upper_trans_version_ = -1;
  new_inc_major->meta_cache_.upper_trans_version_ = -1;
  EXPECT_EQ(OB_NO_NEED_MERGE, new_table_store.init(allocator_, *tablet, param, old_table_store));
}


TEST_F(TestTableStore, test_replace_table_store)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            50         50         50       \n"
              "11            50           150        150        150      \n"
              "11            150          200        200        200      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.minor_tables_.count());

  // When replace sstable array is null, should assign old store to new store
  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, old_table_store, nullptr));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(2, new_table_store.minor_tables_.count());

  // When replace sstable array is null, should assign old store to new store
  ObArray<ObITable *> replace_tables;
  new_table_store.reset();
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, old_table_store, &replace_tables));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(2, new_table_store.minor_tables_.count());


  // init table store with valid replaced sstable
  ObITable::TableKey table_key;
  table_key.tablet_id_ = tablet_id_;
  table_key.table_type_ = TYPE_MAP[11];
  table_key.scn_range_.start_scn_.convert_for_tx(50);
  table_key.scn_range_.end_scn_.convert_for_tx(150);
  ObSSTable *sstable = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(table_key, sstable));
  sstable->meta_cache_.nested_offset_ = 2048;
  replace_tables.push_back(sstable);

  new_table_store.reset();
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, old_table_store, &replace_tables));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(2, new_table_store.minor_tables_.count());
  EXPECT_EQ(2048, new_table_store.minor_tables_[0]->meta_cache_.nested_offset_);


  // init table store with wrong replaced sstable, should retrun err
  replace_tables.reset();
  sstable->key_.scn_range_.end_scn_.convert_for_tx(180);
  replace_tables.push_back(sstable);

  new_table_store.reset();
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, new_table_store.init(allocator_, *tablet, old_table_store, &replace_tables));
}


TEST_F(TestTableStore, test_add_meta_table)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            100        100        100      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());

  ObITable::TableKey table_key;
  table_key.tablet_id_ = tablet_id_;
  table_key.table_type_ = TYPE_MAP[13];
  table_key.scn_range_.start_scn_.convert_for_tx(0);
  table_key.scn_range_.end_scn_.convert_for_tx(150);
  ObSSTable *sstable = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(table_key, sstable));
  sstable->meta_cache_.max_merged_trans_version_ = 150;

  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 50;
  param.sstable_ = sstable;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::META_MAJOR_MERGE;
  param.compaction_info_.need_report_ = false;


  // add new meta major
  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(1, new_table_store.meta_major_tables_.count());


  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "13            0            150        150        150      \n"
              "10            0            100        100        100      \n";
  old_table_store.reset();
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(1, old_table_store.meta_major_tables_.count());


  // update with new meta major
  sstable->key_.scn_range_.end_scn_.convert_for_tx(200);
  new_table_store.reset();
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(1, new_table_store.meta_major_tables_.count());
  EXPECT_EQ(200, new_table_store.meta_major_tables_[0]->key_.scn_range_.end_scn_.get_val_for_tx());


  // update with wrong meta major, expect fail
  sstable->key_.scn_range_.end_scn_.convert_for_tx(50);
  new_table_store.reset();
  EXPECT_EQ(OB_MINOR_SSTABLE_RANGE_CROSS, new_table_store.init(allocator_, *tablet, param, old_table_store));


  // update with a old major, should not recycle meta major
  sstable->key_.table_type_ = ObITable::MAJOR_SSTABLE;
  sstable->key_.scn_range_.end_scn_.convert_for_tx(120);
  param.compaction_info_.merge_type_ = ObMergeType::MAJOR_MERGE;
  new_table_store.reset();
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.meta_major_tables_.count());
}


TEST_F(TestTableStore, test_ha_build_inc_major_tables)
{
  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            100        100        100      \n"
              "29            0            80         80         -1       \n"
              "29            0            130        130        -1       \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.inc_major_tables_.count());


  ObBatchUpdateTableStoreParam param;
  ObITable::TableKey table_key;
  table_key.tablet_id_ = tablet_id_;
  table_key.table_type_ = ObITable::TableType::INC_MAJOR_SSTABLE;
  table_key.scn_range_.start_scn_.convert_for_tx(0);
  table_key.scn_range_.end_scn_.convert_for_tx(80);

  ObSSTable *sstable_1 = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(table_key, sstable_1));
  sstable_1->meta_cache_.upper_trans_version_ = 90;
  sstable_1->meta_->basic_meta_.upper_trans_version_ = 90;
  ObTableHandleV2 table_hdl_1;
  table_hdl_1.set_sstable(sstable_1, &allocator_);
  param.tables_handle_.add_table(table_hdl_1);

  table_key.scn_range_.end_scn_.convert_for_tx(130);
  ObSSTable *sstable_2 = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(table_key, sstable_2));
  sstable_2->meta_cache_.upper_trans_version_ = 150;
  sstable_2->meta_->basic_meta_.upper_trans_version_ = 150;
  ObTableHandleV2 table_hdl_2;
  table_hdl_2.set_sstable(sstable_2, &allocator_);
  param.tables_handle_.add_table(table_hdl_2);


  // replace two inc majors with commited version ones
  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_SUCCESS, new_table_store.build_ha_inc_major_tables_(allocator_, param, old_table_store, 100));
  EXPECT_EQ(1, new_table_store.inc_major_tables_.count());
  EXPECT_EQ(150, new_table_store.inc_major_tables_[0]->get_upper_trans_version());
}


TEST_F(TestTableStore, test_multi_inc_major_table)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            100        100        100      \n"
              "29            0            110        110        120      \n"
              "29            0            130        130        140      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.inc_major_tables_.count());

  ObSSTable *new_inc_major = nullptr;
  ObITable::TableKey major_key;
  major_key.tablet_id_ = tablet_id_;
  major_key.scn_range_.start_scn_.set_min();
  major_key.scn_range_.end_scn_.convert_for_tx(150);
  major_key.table_type_ = ObITable::TableType::INC_MAJOR_SSTABLE;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(major_key, new_inc_major));
  new_inc_major->meta_->basic_meta_.upper_trans_version_ = 170;
  new_inc_major->meta_cache_.upper_trans_version_ = 170;

  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 110;
  param.sstable_ = new_inc_major;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::MINOR_MERGE;
  param.compaction_info_.need_report_ = false;

  // test add a new committed inc major
  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(3, new_table_store.inc_major_tables_.count());
}

TEST_F(TestTableStore, test_get_inc_major_read_tables)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            100        100        100      \n"
              "29            0            90         90         100      \n"
              "29            0            110        110        120      \n"
              "29            0            130        130        140      \n"
              "29            0            150        150        170      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(4, old_table_store.inc_major_tables_.count());

  const int64_t snapshot_version = 140;
  ObTableStoreIterator iter;
  old_table_store.is_ready_for_read_ = true;
  EXPECT_EQ(OB_SUCCESS, old_table_store.get_read_tables(snapshot_version, *tablet, iter));
  ObITable *table = nullptr;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::INC_MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(120, table->get_upper_trans_version());
  EXPECT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::INC_MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(140, table->get_upper_trans_version());
  EXPECT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::INC_MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(170, table->get_upper_trans_version());
  EXPECT_EQ(OB_ITER_END, iter.get_next(table));
}

TEST_F(TestTableStore, test_get_inc_major_read_tables_without_major)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "29            0            90         90         100      \n"
              "29            0            110        110        120      \n"
              "29            0            130        130        140      \n"
              "29            0            150        150        170      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(4, old_table_store.inc_major_tables_.count());

  const int64_t snapshot_version = 140;
  ObTableStoreIterator iter;
  old_table_store.is_ready_for_read_ = true;
  EXPECT_EQ(OB_SUCCESS, old_table_store.get_read_tables(snapshot_version, *tablet, iter, ObGetReadTablesMode::SKIP_MAJOR));
  ObITable *table = nullptr;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::INC_MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(100, table->get_upper_trans_version());
  EXPECT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::INC_MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(120, table->get_upper_trans_version());
  EXPECT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::INC_MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(140, table->get_upper_trans_version());
  EXPECT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::INC_MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(170, table->get_upper_trans_version());
  EXPECT_EQ(OB_ITER_END, iter.get_next(table));
}

TEST_F(TestTableStore, test_basic_add_new_inc_major_ddl_sstable)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            50         50         50       \n"
              "11            50           150        150        150      \n"
              "11            150          200        200        200      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.minor_tables_.count());


  // add a new inc major ddl sstable
  ObSSTable *new_inc_major_ddl_sstable = nullptr;
  ObITable::TableKey inc_major_ddl_key;
  inc_major_ddl_key.tablet_id_ = tablet_id_;
  inc_major_ddl_key.scn_range_.start_scn_.set_min();
  inc_major_ddl_key.scn_range_.end_scn_.convert_for_tx(250);
  inc_major_ddl_key.table_type_ = ObITable::TableType::INC_MAJOR_DDL_DUMP_SSTABLE;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(inc_major_ddl_key, new_inc_major_ddl_sstable));
  new_inc_major_ddl_sstable->meta_->basic_meta_.upper_trans_version_ = INT64_MAX;
  new_inc_major_ddl_sstable->meta_cache_.upper_trans_version_ = INT64_MAX;
  new_inc_major_ddl_sstable->meta_->basic_meta_.ddl_scn_ = tablet->get_tablet_meta().ddl_start_scn_;


  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 150;
  param.sstable_ = new_inc_major_ddl_sstable;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::MINI_MERGE;
  param.compaction_info_.need_report_ = true;


  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(2, new_table_store.minor_tables_.count());
  EXPECT_EQ(0, new_table_store.inc_major_tables_.count());
  EXPECT_EQ(1, new_table_store.inc_major_ddl_sstables_.count());

  // add a new inc major sstable
  ObSSTable *new_inc_major_sstable = nullptr;
  ObITable::TableKey inc_major_key;
  inc_major_key.tablet_id_ = tablet_id_;
  inc_major_key.scn_range_.start_scn_.set_min();
  inc_major_key.scn_range_.end_scn_.convert_for_tx(250);
  inc_major_key.table_type_ = ObITable::TableType::INC_MAJOR_SSTABLE;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(inc_major_key, new_inc_major_sstable));
  new_inc_major_sstable->meta_->basic_meta_.upper_trans_version_ = 250;
  new_inc_major_sstable->meta_cache_.upper_trans_version_ = 250;


  ObUpdateTableStoreParam param2;
  param2.multi_version_start_ = 150;
  param2.sstable_ = new_inc_major_sstable;
  param2.allow_duplicate_sstable_ = false;
  param2.compaction_info_.merge_type_ = ObMergeType::MAJOR_MERGE;
  param2.compaction_info_.need_report_ = true;


  ObTabletTableStore new_table_store2;
  EXPECT_EQ(OB_SUCCESS, new_table_store2.init(allocator_, *tablet, param2, new_table_store));
  EXPECT_EQ(1, new_table_store2.major_tables_.count());
  EXPECT_EQ(2, new_table_store2.minor_tables_.count());
  EXPECT_EQ(1, new_table_store2.inc_major_tables_.count());
  EXPECT_EQ(0, new_table_store2.inc_major_ddl_sstables_.count());

  // add a new inc major ddl sstable
  ObSSTable *new_inc_major_ddl_sstable2 = nullptr;
  ObITable::TableKey inc_major_ddl_key2;
  inc_major_ddl_key2.tablet_id_ = tablet_id_;
  inc_major_ddl_key2.scn_range_.start_scn_.set_min();
  inc_major_ddl_key2.scn_range_.end_scn_.convert_for_tx(250);
  inc_major_ddl_key2.table_type_ = ObITable::TableType::INC_MAJOR_DDL_DUMP_SSTABLE;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(inc_major_ddl_key2, new_inc_major_ddl_sstable2));
  new_inc_major_ddl_sstable2->meta_->basic_meta_.upper_trans_version_ = INT64_MAX;
  new_inc_major_ddl_sstable2->meta_cache_.upper_trans_version_ = INT64_MAX;
  new_inc_major_ddl_sstable2->meta_->basic_meta_.ddl_scn_ = tablet->get_tablet_meta().ddl_start_scn_;

  ObUpdateTableStoreParam param3;
  param3.multi_version_start_ = 150;
  param3.sstable_ = new_inc_major_ddl_sstable2;
  param3.allow_duplicate_sstable_ = false;
  param3.compaction_info_.merge_type_ = ObMergeType::MINI_MERGE;
  param3.compaction_info_.need_report_ = true;


  ObTabletTableStore new_table_store3;
  EXPECT_EQ(OB_SUCCESS, new_table_store3.init(allocator_, *tablet, param3, new_table_store2));
  EXPECT_EQ(1, new_table_store3.major_tables_.count());
  EXPECT_EQ(2, new_table_store3.minor_tables_.count());
  EXPECT_EQ(1, new_table_store3.inc_major_tables_.count());
  EXPECT_EQ(0, new_table_store3.inc_major_ddl_sstables_.count());
}

} // unittest
} // oceanbase


int main(int argc, char **argv)
{
  system("rm -rf test_tablet_table_store.log*");
  OB_LOGGER.set_file_name("test_tablet_table_store.log");
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_tablet_table_store");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}