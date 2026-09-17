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
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"

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
// Isolate table-store construction from schema and compaction parameter validation.
bool ObUpdateTableStoreParam::is_valid() const
{
  const bool valid = true;
  return valid;
}
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
  ObArray<ObSSTable *> allocated_sstables_;
  ObArray<ObMemtable *> allocated_memtables_;
  ObTablet tablet_;
};

TestTableStore::TestTableStore()
  : allocator_(),
    data_iter_(),
    tablet_id_(200001),
    key_data_(nullptr),
    sstables_(),
    memtables_(),
    allocated_sstables_(),
    allocated_memtables_(),
    tablet_()
{
}

TestTableStore::~TestTableStore()
{
  for (int64_t i = 0; i < allocated_sstables_.count(); ++i) {
    ObSSTable *sstable = allocated_sstables_.at(i);
    if (nullptr != sstable) {
      sstable->~ObSSTable();
    }
  }
  for (int64_t i = 0; i < allocated_memtables_.count(); ++i) {
    ObMemtable *memtable = allocated_memtables_.at(i);
    if (nullptr != memtable) {
      memtable->~ObMemtable();
    }
  }
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

int TestTableStore::mock_sstable(
    const ObITable::TableKey &key,
    ObSSTable *&sstable)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  sstable = nullptr;

  ObTabletCreateSSTableParam param;
  param.table_key_ = key;
  param.max_merged_trans_version_ = key.scn_range_.end_scn_.get_val_for_tx();
  param.schema_version_ = 100;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = 0;
  param.progressive_merge_step_ = 0;
  param.index_type_ = ObIndexType::INDEX_TYPE_IS_NOT;
  param.table_mode_.mode_flag_ = ObTableModeFlag::TABLE_MODE_NORMAL;
  param.table_mode_.pk_mode_ = ObTablePKMode::TPKM_OLD_NO_PK;
  param.table_mode_.state_flag_ = ObTableStateFlag::TABLE_STATE_NORMAL;
  param.rowkey_column_cnt_ = 3;
  param.root_block_addr_.set_none_addr();
  param.data_block_macro_meta_addr_.set_none_addr();
  param.root_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  param.latest_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  param.data_index_tree_height_ = 0;
  param.index_blocks_cnt_ = 0;
  param.data_blocks_cnt_ = 0;
  param.micro_block_cnt_ = 0;
  param.use_old_macro_block_count_ = 0;
  param.data_checksum_ = 0;
  param.occupy_size_ = 0;
  param.ddl_scn_.set_min();
  param.filled_tx_scn_ = key.scn_range_.end_scn_;
  param.tx_data_recycle_scn_.set_min();
  param.rec_scn_.set_min();
  param.original_size_ = 0;
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.table_backup_flag_.reset();
  param.table_shared_flag_.reset();
  param.sstable_logic_seq_ = 0;
  param.row_count_ = 0;
  param.recycle_version_ = 0;
  param.root_macro_seq_ = 0;
  param.nested_size_ = 0;
  param.nested_offset_ = 0;
  param.column_group_cnt_ = 1;
  param.co_base_type_ = ObCOSSTableBaseType::INVALID_TYPE;
  param.full_column_cnt_ = 0;
  param.is_co_table_without_cgs_ = false;
  param.co_base_snapshot_version_ = 0;
  param.column_cnt_ = param.rowkey_column_cnt_;

  if (key.is_major_sstable()) {
    param.rec_scn_.set_min();
  } else {
    param.rec_scn_ = SCN::plus(key.get_start_scn(), 1);
  }

  if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_))) {
    LOG_WARN("failed to fill column checksums", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSTable)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for sstable", K(ret));
  } else if (OB_ISNULL(sstable = new (buf) ObSSTable())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for sstable", K(ret));
  } else if (OB_FAIL(allocated_sstables_.push_back(sstable))) {
    sstable->~ObSSTable();
    sstable = nullptr;
  } else if (OB_FAIL(sstable->init(param, &allocator_))) {
    LOG_WARN("failed to init sstable", K(ret));
  } else if (OB_ISNULL(sstable->meta_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (sstable->is_meta_major_sstable()) {
    const int64_t version = key.scn_range_.end_scn_.get_val_for_tx();
    sstable->meta_cache_.max_merged_trans_version_ = version;
    sstable->meta_->basic_meta_.max_merged_trans_version_ = version;
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
  } else if (OB_FAIL(allocated_memtables_.push_back(memtable))) {
    memtable->~ObMemtable();
    memtable = nullptr;
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

  if (nullptr == key_data_) {
  } else if (OB_FAIL(data_iter_.from(key_data_))) {
    LOG_WARN("failed to parse table data", K(ret));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < data_iter_.count(); ++idx) {
      const ObStoreRow *row = nullptr;
      if (OB_FAIL(data_iter_.get_row(idx, row))) {
        LOG_WARN("failed to get table data", K(ret), K(idx));
      } else if (OB_ISNULL(row) || OB_ISNULL(row->row_val_.cells_) || row->row_val_.count_ < 5) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const ObObj *cells = row->row_val_.cells_;
        const int64_t type = cells[0].get_int();
        ObITable::TableKey table_key;
        table_key.tablet_id_ = tablet_id_;
        if (type < 0 || type >= ARRAYSIZEOF(TYPE_MAP)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          table_key.table_type_ = TYPE_MAP[type];
          table_key.scn_range_.start_scn_.convert_for_tx(cells[1].get_int());
          table_key.scn_range_.end_scn_.convert_for_tx(cells[2].get_int());
          if (ObITable::is_sstable(table_key.table_type_)) {
            ObSSTable *sstable = nullptr;
            if (OB_FAIL(mock_sstable(table_key, sstable))) {
              LOG_WARN("failed to mock sstable", K(ret), K(table_key));
            } else {
              ObSSTableBasicMeta &meta = sstable->meta_->basic_meta_;
              ObSSTableMetaCache &cache = sstable->meta_cache_;
              meta.max_merged_trans_version_ = cells[3].get_int();
              meta.upper_trans_version_ = 99999 == cells[4].get_int() ? INT64_MAX : cells[4].get_int();
              cache.max_merged_trans_version_ = meta.max_merged_trans_version_;
              cache.upper_trans_version_ = meta.upper_trans_version_;
              if (OB_FAIL(sstables_.push_back(sstable))) {
                LOG_WARN("failed to add sstable", K(ret));
              }
            }
          } else {
            ObMemtable *memtable = nullptr;
            if (OB_FAIL(mock_memtable(table_key, memtable))) {
              LOG_WARN("failed to mock memtable", K(ret));
            } else {
              memtable->snapshot_version_.convert_for_tx(cells[3].get_int());
              if (OB_FAIL(memtables_.push_back(memtable))) {
                LOG_WARN("failed to add memtable", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int TestTableStore::mock_old_table_store(ObTabletTableStore *table_store)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table_store)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(batch_mock_tables())) {
    LOG_WARN("failed to mock tables", K(ret));
  } else if (!sstables_.empty()) {
    ObArray<ObMetaDiskAddr> addrs;
    ObMetaDiskAddr addr;
    addr.set_mem_addr(1, 1);

    ObMajorChecksumInfo ckm_info;
    for (int64_t i = 0; OB_SUCC(ret) && i < sstables_.count(); ++i) {
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
  const int ret = OB_SUCCESS;
  tablet_.is_inited_ = true;
  tablet_.tablet_meta_.tablet_id_ = tablet_id_;
  tablet = &tablet_;
  return ret;
}

TEST_F(TestTableStore, test_basic)
{
  key_data_ = nullptr;
  ObTablet *tablet = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(tablet));

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            1          1          1        \n"
              "11            1            150        150        150      \n"
              "11            150          200        200        200      \n"
              "0             200          250        250        250      \n"
              "0             250          300        300        300      \n"
              "0             300          350        350        350      \n";
  ObTabletTableStore table_store;
  ASSERT_EQ(OB_SUCCESS, mock_old_table_store(&table_store));
  EXPECT_EQ(1, table_store.major_tables_.count());
  EXPECT_EQ(2, table_store.minor_tables_.count());
}

TEST_F(TestTableStore, test_first_init_table_store)
{
  ObTablet *tablet = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(tablet));

  ObITable::TableKey major_key;
  major_key.tablet_id_ = tablet_id_;
  major_key.scn_range_.start_scn_.set_min();
  major_key.scn_range_.end_scn_.set_base();
  major_key.table_type_ = ObITable::TableType::META_MAJOR_SSTABLE;

  ObSSTable *new_major = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(major_key, new_major));

  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_ERR_UNEXPECTED, new_table_store.init(allocator_, *tablet, new_major));

  new_major->key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  ASSERT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, new_major));
}

TEST_F(TestTableStore, test_basic_add_new_major)
{
  ObTablet *tablet = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            1          1          1        \n"
              "11            1            150        150        150      \n"
              "11            150          200        200        200      \n";
  ObTabletTableStore old_table_store;
  ASSERT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.minor_tables_.count());

  ObSSTable *new_major = nullptr;
  ObITable::TableKey major_key;
  major_key.tablet_id_ = tablet_id_;
  major_key.scn_range_.start_scn_.set_min();
  major_key.scn_range_.end_scn_.convert_for_tx(200);
  major_key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(major_key, new_major));

  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 150;
  param.sstable_ = new_major;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::MAJOR_MERGE;
  param.compaction_info_.need_report_ = true;

  ObTabletTableStore new_table_store;
  ASSERT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store, nullptr));
  EXPECT_EQ(2, new_table_store.major_tables_.count());
  ASSERT_EQ(2, new_table_store.minor_tables_.count());

  new_table_store.reset();
  param.multi_version_start_ = 200;
  ASSERT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store, nullptr));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(0, new_table_store.minor_tables_.count());
}

TEST_F(TestTableStore, test_basic_add_new_minor)
{
  ObTablet *tablet = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(300);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            1          1          1        \n"
              "11            100          150        150        150      \n"
              "11            150          200        200        200      \n";
  ObTabletTableStore old_table_store;
  ASSERT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.minor_tables_.count());

  ObSSTable *new_minor = nullptr;
  ObITable::TableKey minor_key;
  minor_key.tablet_id_ = tablet_id_;
  minor_key.scn_range_.start_scn_.convert_for_tx(200);
  minor_key.scn_range_.end_scn_.convert_for_tx(300);
  minor_key.table_type_ = ObITable::TableType::MINI_SSTABLE;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(minor_key, new_minor));

  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 50;
  param.sstable_ = new_minor;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::MINI_MERGE;
  param.compaction_info_.need_report_ = false;

  // test add new mini sstable after mini merge
  ObTabletTableStore new_table_store;
  ASSERT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store, nullptr));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(3, new_table_store.minor_tables_.count());

  // test add a mini sstable which end scn not equals to the clog checkpoint scn
  new_table_store.reset();
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  EXPECT_EQ(OB_ERR_UNEXPECTED, new_table_store.init(allocator_, *tablet, param, old_table_store, nullptr));

  // test add a new minor which cover all mini sstables in old store
  new_table_store.reset();
  new_minor->key_.scn_range_.start_scn_.convert_for_tx(100);
  new_minor->key_.scn_range_.end_scn_.convert_for_tx(200);
  new_minor->meta_cache_.upper_trans_version_ = 200;
  new_minor->meta_->basic_meta_.upper_trans_version_ = 200;
  param.compaction_info_.merge_type_ = ObMergeType::MINOR_MERGE;
  param.ha_info_.need_check_sstable_ = true;
  ASSERT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store, nullptr));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(1, new_table_store.minor_tables_.count());

  // test add a range crossed sstable
  new_table_store.reset();
  new_minor->key_.scn_range_.start_scn_.convert_for_tx(50);
  new_minor->key_.scn_range_.end_scn_.convert_for_tx(180);
  EXPECT_EQ(OB_MINOR_SSTABLE_RANGE_CROSS, new_table_store.init(allocator_, *tablet, param, old_table_store, nullptr));

  // test add a new table which not cover any mini sstable in minor merge
  new_table_store.reset();
  new_minor->key_.scn_range_.start_scn_.convert_for_tx(50);
  new_minor->key_.scn_range_.end_scn_.convert_for_tx(60);
  EXPECT_EQ(OB_NO_NEED_MERGE, new_table_store.init(allocator_, *tablet, param, old_table_store, nullptr));

  new_table_store.reset();
  new_minor->key_.scn_range_.start_scn_.convert_for_tx(250);
  new_minor->key_.scn_range_.end_scn_.convert_for_tx(300);
  EXPECT_EQ(OB_NO_NEED_MERGE, new_table_store.init(allocator_, *tablet, param, old_table_store, nullptr));
}

TEST_F(TestTableStore, test_replace_table_store)
{
  ObTablet *tablet = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            50         50         50       \n"
              "11            50           150        150        150      \n"
              "11            150          200        200        200      \n";
  ObTabletTableStore old_table_store;
  ASSERT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.minor_tables_.count());

  // When replace sstable array is null, should assign old store to new store
  ObTabletTableStore new_table_store;
  ASSERT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  ASSERT_EQ(2, new_table_store.minor_tables_.count());

  // An empty replacement array should preserve all old sstables.
  ObArray<ObITable *> replace_tables;
  new_table_store.reset();
  ASSERT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, replace_tables, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  ASSERT_EQ(2, new_table_store.minor_tables_.count());

  // init table store with valid replaced sstable
  ObITable::TableKey table_key;
  table_key.tablet_id_ = tablet_id_;
  table_key.table_type_ = TYPE_MAP[11];
  table_key.scn_range_.start_scn_.convert_for_tx(50);
  table_key.scn_range_.end_scn_.convert_for_tx(150);
  ObSSTable *sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(table_key, sstable));
  sstable->meta_cache_.nested_offset_ = 2048;
  replace_tables.push_back(sstable);

  new_table_store.reset();
  ASSERT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, replace_tables, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  ASSERT_EQ(2, new_table_store.minor_tables_.count());
  EXPECT_EQ(2048, new_table_store.minor_tables_[0]->meta_cache_.nested_offset_);

  // init table store with wrong replaced sstable, should retrun err
  replace_tables.reset();
  sstable->key_.scn_range_.end_scn_.convert_for_tx(180);
  replace_tables.push_back(sstable);

  new_table_store.reset();
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, new_table_store.init(allocator_, *tablet, replace_tables, old_table_store));
}

TEST_F(TestTableStore, test_add_meta_table)
{
  ObTablet *tablet = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            100        100        100      \n";
  ObTabletTableStore old_table_store;
  ASSERT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());

  ObITable::TableKey table_key;
  table_key.tablet_id_ = tablet_id_;
  table_key.table_type_ = TYPE_MAP[13];
  table_key.scn_range_.start_scn_.convert_for_tx(0);
  table_key.scn_range_.end_scn_.convert_for_tx(150);
  ObSSTable *sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(table_key, sstable));
  sstable->meta_cache_.max_merged_trans_version_ = 150;

  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 50;
  param.sstable_ = sstable;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::META_MAJOR_MERGE;
  param.compaction_info_.need_report_ = false;

  // add new meta major
  ObTabletTableStore new_table_store;
  ASSERT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store, nullptr));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  ASSERT_EQ(1, new_table_store.meta_major_tables_.count());

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "13            0            150        150        150      \n"
              "10            0            100        100        100      \n";
  old_table_store.reset();
  ASSERT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(1, old_table_store.meta_major_tables_.count());

  // update with new meta major
  sstable->key_.scn_range_.end_scn_.convert_for_tx(200);
  new_table_store.reset();
  ASSERT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store, nullptr));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  ASSERT_EQ(1, new_table_store.meta_major_tables_.count());
  EXPECT_EQ(200, new_table_store.meta_major_tables_[0]->key_.scn_range_.end_scn_.get_val_for_tx());

  // update with wrong meta major, expect fail
  sstable->key_.scn_range_.end_scn_.convert_for_tx(50);
  new_table_store.reset();
  EXPECT_EQ(OB_MINOR_SSTABLE_RANGE_CROSS, new_table_store.init(allocator_, *tablet, param, old_table_store, nullptr));

  // update with a old major, should not recycle meta major
  sstable->key_.table_type_ = ObITable::MAJOR_SSTABLE;
  sstable->key_.scn_range_.end_scn_.convert_for_tx(120);
  param.compaction_info_.merge_type_ = ObMergeType::MAJOR_MERGE;
  new_table_store.reset();
  ASSERT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store, nullptr));
  ASSERT_EQ(1, new_table_store.meta_major_tables_.count());
}

TEST_F(TestTableStore, test_major_recycle_not_triggered_by_table_count)
{
  // total_table_count < MAJOR_RECYCLE_TRIGGER_THRESHOLD (89), should not recycle
  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            100        100        100      \n"
              "10            0            200        200        200      \n"
              "10            0            300        300        300      \n"
              "11            300          400        400        400      \n";
  ObTabletTableStore table_store;
  ASSERT_EQ(OB_SUCCESS, mock_old_table_store(&table_store));
  EXPECT_EQ(3, table_store.major_tables_.count());
  EXPECT_EQ(1, table_store.minor_tables_.count());

  // total = 4, far below threshold (89), should not trigger recycle
  const int64_t original_major_count = table_store.major_tables_.count();
  ASSERT_EQ(OB_SUCCESS, table_store.try_recycle_major_tables_(allocator_));
  EXPECT_EQ(original_major_count, table_store.major_tables_.count());
}

// Test major sstable recycle - not triggered when major count <= min_keep_cnt
TEST_F(TestTableStore, test_major_recycle_not_triggered_by_major_count)
{
  // Even if we could exceed threshold, major_count <= MAJOR_RECYCLE_MIN_KEEP_CNT (8) should not recycle
  // This test verifies the major_count check logic
  ObTabletTableStore table_store;

  // Create only 5 majors (< min_keep_cnt = 8)
  ObArray<ObITable *> major_tables;
  for (int64_t i = 1; i <= 5; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    key.scn_range_.start_scn_.set_min();
    key.scn_range_.end_scn_.convert_for_tx(i * 100);
    ObSSTable *sstable = nullptr;
    ASSERT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
    sstable->meta_->basic_meta_.max_merged_trans_version_ = i * 100;
    sstable->meta_cache_.max_merged_trans_version_ = i * 100;
    ASSERT_EQ(OB_SUCCESS, major_tables.push_back(sstable));
  }

  // Synthetic input isolates the minimum-major guard; it deliberately exceeds the minor-table limit.
  ObArray<ObITable *> minor_tables;
  for (int64_t i = 1; i <= 84; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::TableType::MINOR_SSTABLE;
    key.scn_range_.start_scn_.convert_for_tx(500 + i);
    key.scn_range_.end_scn_.convert_for_tx(500 + i + 1);
    ObSSTable *sstable = nullptr;
    ASSERT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
    sstable->meta_->basic_meta_.upper_trans_version_ = 500 + i + 1;
    sstable->meta_cache_.upper_trans_version_ = 500 + i + 1;
    ASSERT_EQ(OB_SUCCESS, minor_tables.push_back(sstable));
  }

  ASSERT_EQ(OB_SUCCESS, table_store.major_tables_.init(allocator_, major_tables));
  ASSERT_EQ(OB_SUCCESS, table_store.minor_tables_.init(allocator_, minor_tables));
  table_store.is_inited_ = true;

  // total = 89 reaches threshold (89), but major_count = 5 <= min_keep_cnt (8)
  EXPECT_EQ(89, table_store.get_table_count());
  EXPECT_EQ(5, table_store.major_tables_.count());

  const int64_t original_major_count = table_store.major_tables_.count();
  ASSERT_EQ(OB_SUCCESS, table_store.try_recycle_major_tables_(allocator_));
  // Should NOT recycle because major_count <= min_keep_cnt
  EXPECT_EQ(original_major_count, table_store.major_tables_.count());
}

// Test major sstable recycle - basic recycle scenario
TEST_F(TestTableStore, test_major_recycle_basic)
{
  ObTabletTableStore table_store;

  // Create 50 major sstables
  ObArray<ObITable *> major_tables;
  for (int64_t i = 1; i <= 50; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    key.scn_range_.start_scn_.set_min();
    key.scn_range_.end_scn_.convert_for_tx(i * 100);
    ObSSTable *sstable = nullptr;
    ASSERT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
    sstable->meta_->basic_meta_.max_merged_trans_version_ = i * 100;
    sstable->meta_cache_.max_merged_trans_version_ = i * 100;
    ASSERT_EQ(OB_SUCCESS, major_tables.push_back(sstable));
  }

  // Create 40 minor tables to make total = 90 > threshold (89)
  ObArray<ObITable *> minor_tables;
  for (int64_t i = 1; i <= 40; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::TableType::MINOR_SSTABLE;
    key.scn_range_.start_scn_.convert_for_tx(5000 + i);
    key.scn_range_.end_scn_.convert_for_tx(5000 + i + 1);
    ObSSTable *sstable = nullptr;
    ASSERT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
    sstable->meta_->basic_meta_.upper_trans_version_ = 5000 + i + 1;
    sstable->meta_cache_.upper_trans_version_ = 5000 + i + 1;
    ASSERT_EQ(OB_SUCCESS, minor_tables.push_back(sstable));
  }

  ASSERT_EQ(OB_SUCCESS, table_store.major_tables_.init(allocator_, major_tables));
  ASSERT_EQ(OB_SUCCESS, table_store.minor_tables_.init(allocator_, minor_tables));
  table_store.is_inited_ = true;

  EXPECT_EQ(90, table_store.get_table_count());
  EXPECT_EQ(50, table_store.major_tables_.count());

  ASSERT_EQ(OB_SUCCESS, table_store.try_recycle_major_tables_(allocator_));

  // After recycle:
  // - other_table_count = 40
  // - target_threshold = 38
  // - keep_count = MAX(38 - 40, 8) = 8 (capped by min_keep_cnt)
  // So should keep 8 majors
  ASSERT_EQ(8, table_store.major_tables_.count());

  // Verify oldest is kept (end_scn = 100)
  EXPECT_EQ(100, table_store.major_tables_[0]->get_key().scn_range_.end_scn_.get_val_for_tx());

  // Verify newest is kept (end_scn = 5000)
  const int64_t new_count = table_store.major_tables_.count();
  EXPECT_EQ(5000, table_store.major_tables_[new_count - 1]->get_key().scn_range_.end_scn_.get_val_for_tx());
}

// Test major sstable recycle - verify oldest and newest are always kept
TEST_F(TestTableStore, test_major_recycle_keeps_oldest_and_newest)
{
  ObTabletTableStore table_store;

  // Create 40 major sstables with specific versions
  ObArray<ObITable *> major_tables;
  for (int64_t i = 1; i <= 40; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    key.scn_range_.start_scn_.set_min();
    key.scn_range_.end_scn_.convert_for_tx(i * 1000);  // 1000, 2000, ..., 40000
    ObSSTable *sstable = nullptr;
    ASSERT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
    sstable->meta_->basic_meta_.max_merged_trans_version_ = i * 1000;
    sstable->meta_cache_.max_merged_trans_version_ = i * 1000;
    ASSERT_EQ(OB_SUCCESS, major_tables.push_back(sstable));
  }

  // Create 50 minor tables
  ObArray<ObITable *> minor_tables;
  for (int64_t i = 1; i <= 50; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::TableType::MINOR_SSTABLE;
    key.scn_range_.start_scn_.convert_for_tx(40000 + i);
    key.scn_range_.end_scn_.convert_for_tx(40000 + i + 1);
    ObSSTable *sstable = nullptr;
    ASSERT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
    sstable->meta_->basic_meta_.upper_trans_version_ = 40000 + i + 1;
    sstable->meta_cache_.upper_trans_version_ = 40000 + i + 1;
    ASSERT_EQ(OB_SUCCESS, minor_tables.push_back(sstable));
  }

  ASSERT_EQ(OB_SUCCESS, table_store.major_tables_.init(allocator_, major_tables));
  ASSERT_EQ(OB_SUCCESS, table_store.minor_tables_.init(allocator_, minor_tables));
  table_store.is_inited_ = true;

  // total = 90 > threshold (89)
  EXPECT_EQ(90, table_store.get_table_count());

  const int64_t oldest_version = table_store.major_tables_[0]->get_key().scn_range_.end_scn_.get_val_for_tx();
  const int64_t newest_version = table_store.major_tables_[table_store.major_tables_.count() - 1]->get_key().scn_range_.end_scn_.get_val_for_tx();

  ASSERT_EQ(OB_SUCCESS, table_store.try_recycle_major_tables_(allocator_));

  const int64_t min_keep_count = ObTabletTableStore::MAJOR_RECYCLE_MIN_KEEP_CNT;
  ASSERT_EQ(min_keep_count, table_store.major_tables_.count());

  // Verify oldest is still kept
  EXPECT_EQ(oldest_version, table_store.major_tables_[0]->get_key().scn_range_.end_scn_.get_val_for_tx());

  // Verify newest is still kept
  const int64_t new_count = table_store.major_tables_.count();
  EXPECT_EQ(newest_version, table_store.major_tables_[new_count - 1]->get_key().scn_range_.end_scn_.get_val_for_tx());
}

// Test major sstable recycle - verify newest K majors are kept consecutively
TEST_F(TestTableStore, test_major_recycle_keeps_newest_consecutive)
{
  ObTabletTableStore table_store;

  // Create 30 major sstables
  ObArray<ObITable *> major_tables;
  for (int64_t i = 1; i <= 30; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    key.scn_range_.start_scn_.set_min();
    key.scn_range_.end_scn_.convert_for_tx(i * 100);
    ObSSTable *sstable = nullptr;
    ASSERT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
    sstable->meta_->basic_meta_.max_merged_trans_version_ = i * 100;
    sstable->meta_cache_.max_merged_trans_version_ = i * 100;
    ASSERT_EQ(OB_SUCCESS, major_tables.push_back(sstable));
  }

  // Create 60 minor tables to make total = 90
  ObArray<ObITable *> minor_tables;
  for (int64_t i = 1; i <= 60; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::TableType::MINOR_SSTABLE;
    key.scn_range_.start_scn_.convert_for_tx(3000 + i);
    key.scn_range_.end_scn_.convert_for_tx(3000 + i + 1);
    ObSSTable *sstable = nullptr;
    ASSERT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
    sstable->meta_->basic_meta_.upper_trans_version_ = 3000 + i + 1;
    sstable->meta_cache_.upper_trans_version_ = 3000 + i + 1;
    ASSERT_EQ(OB_SUCCESS, minor_tables.push_back(sstable));
  }

  ASSERT_EQ(OB_SUCCESS, table_store.major_tables_.init(allocator_, major_tables));
  ASSERT_EQ(OB_SUCCESS, table_store.minor_tables_.init(allocator_, minor_tables));
  table_store.is_inited_ = true;

  ASSERT_EQ(OB_SUCCESS, table_store.try_recycle_major_tables_(allocator_));

  const int64_t new_count = table_store.major_tables_.count();
  // keep_count = max(38 - 60, 8) = 8
  ASSERT_EQ(8, new_count);

  // MAJOR_RECYCLE_KEEP_NEWEST_CNT = 4, keep_count = 8, so keep_newest_cnt = min(4, max(8-1, 1)) = 4
  // So newest 4 should be consecutive (2700, 2800, 2900, 3000)

  // Check that the newest majors are consecutive
  const int64_t keep_newest_cnt = MIN(ObTabletTableStore::MAJOR_RECYCLE_KEEP_NEWEST_CNT, MAX(new_count - 1, 1));
  for (int64_t i = 0; i < keep_newest_cnt; ++i) {
    const int64_t expected_version = (30 - keep_newest_cnt + 1 + i) * 100;
    EXPECT_EQ(expected_version, table_store.major_tables_[new_count - keep_newest_cnt + i]->get_key().scn_range_.end_scn_.get_val_for_tx());
  }
}

// Check the threshold boundary through the public table-store update path.
TEST_F(TestTableStore, test_major_recycle_during_init)
{
  ObTablet *tablet = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(tablet));
  const int64_t trigger = ObTabletTableStore::MAJOR_RECYCLE_TRIGGER_THRESHOLD;
  ObArray<ObITable *> majors;
  for (int64_t i = 1; i < trigger; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::MAJOR_SSTABLE;
    key.version_range_.snapshot_version_ = i * 100;
    ObSSTable *sstable = nullptr;
    ASSERT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
    ASSERT_EQ(OB_SUCCESS, majors.push_back(sstable));
  }
  ObTabletTableStore old_store;
  ASSERT_EQ(OB_SUCCESS, old_store.major_tables_.init(allocator_, majors));
  old_store.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, old_store.try_recycle_major_tables_(allocator_));
  ASSERT_EQ(trigger - 1, old_store.major_tables_.count());

  ObITable::TableKey key;
  key.tablet_id_ = tablet_id_;
  key.table_type_ = ObITable::MAJOR_SSTABLE;
  key.version_range_.snapshot_version_ = trigger * 100;
  ObSSTable *new_major = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(key, new_major));
  ObUpdateTableStoreParam param;
  param.sstable_ = new_major;
  param.multi_version_start_ = 100;
  param.compaction_info_.merge_type_ = MAJOR_MERGE;

  ObTabletTableStore new_store;
  ASSERT_EQ(OB_SUCCESS, new_store.init(allocator_, *tablet, param, old_store, nullptr));
  const ObSSTableArray &kept = new_store.major_tables_;
  const int64_t min_keep_count = ObTabletTableStore::MAJOR_RECYCLE_MIN_KEEP_CNT;
  const int64_t target_count = ObTabletTableStore::MAJOR_RECYCLE_TARGET_THRESHOLD;
  ASSERT_GE(kept.count(), min_keep_count);
  EXPECT_LE(kept.count(), target_count);
  EXPECT_EQ(100, kept[0]->get_snapshot_version());
  const int64_t newest_count = ObTabletTableStore::MAJOR_RECYCLE_KEEP_NEWEST_CNT;
  for (int64_t i = 0; i < newest_count; ++i) {
    EXPECT_EQ((trigger - i) * 100, kept[kept.count() - i - 1]->get_snapshot_version());
  }
  EXPECT_EQ(trigger - 1, old_store.major_tables_.count());
  const int64_t version = ObTabletTableStore::TABLE_STORE_VERSION_V4;
  EXPECT_EQ(version, new_store.version_);
  EXPECT_TRUE(new_store.is_ready_for_read_);
}

} // unittest
} // oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_tablet_table_store.log");
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_tablet_table_store");
  ::testing::InitGoogleTest(&argc, argv);
  const int ret = RUN_ALL_TESTS();
  return ret;
}
