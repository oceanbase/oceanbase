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

#define UNITTEST_DEBUG
#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#define private public
#define protected public

#include "storage/ob_i_table.h"
#include "storage/schema_utils.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_table_store_flag.h"
#include "storage/test_dml_common.h"
#include "storage/mockcontainer/mock_ob_iterator.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/ob_storage_schema.h"
#include "share/scn.h"
#include "storage/test_schema_prepare.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace memtable;
using namespace share::schema;
using namespace share;
using namespace compaction;
namespace storage
{
namespace mds
{
void *MdsAllocator::alloc(const int64_t size)
{
  void *ptr = ob_malloc(size, "MDS");
  ATOMIC_INC(&alloc_times_);
  MDS_LOG(DEBUG, "alloc obj", KP(ptr), K(lbt()));
  return ptr;
}
void MdsAllocator::free(void *ptr) {
  ATOMIC_INC(&free_times_);
  MDS_LOG(DEBUG, "free obj", KP(ptr), K(lbt()));
  ob_free(ptr);
}
}
}
namespace storage
{

  bool ObITabletMemtable::can_be_minor_merged()
  {
    return get_is_tablet_freeze();
  }

}

namespace unittest
{

class TestCompactionPolicy : public ::testing::Test
{
public:
  static void generate_table_key(
    const ObITable::TableType &type,
    const int64_t start_scn,
    const int64_t end_scn,
    ObITable::TableKey &table_key);
  static int mock_sstable(
    common::ObArenaAllocator &allocator,
    const ObITable::TableType &type,
    const int64_t start_scn,
    const int64_t end_scn,
    const int64_t max_merged_trans_version,
    const int64_t upper_trans_version,
    ObTableHandleV2 &table_handle);
  static int mock_sstable_meta(
    const int64_t row_count,
    ObTableHandleV2 &table_handle);
  static int mock_memtable(
    const int64_t start_log_ts,
    const int64_t end_log_ts,
    const int64_t snapshot_version,
    ObTablet &tablet,
    ObTableHandleV2 &table_handle);
  static int mock_tablet(
    common::ObArenaAllocator &allocator,
    const int64_t clog_checkpoint_ts,
    const int64_t snapshot_version,
    ObTabletHandle &tablet_handle);
  static int mock_table_store(
    common::ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle,
    common::ObIArray<ObTableHandleV2> &major_tables,
    common::ObIArray<ObTableHandleV2> &minor_tables);
  static int batch_mock_sstables(
    common::ObArenaAllocator &allocator,
    const char *key_data,
    common::ObIArray<ObTableHandleV2> &major_tables,
    common::ObIArray<ObTableHandleV2> &minor_tables);
  static int batch_mock_memtables(
    const char *key_data,
    ObTabletHandle &tablet_handle,
    common::ObIArray<ObTableHandleV2> &memtables);
  static int batch_mock_tables(
    common::ObArenaAllocator &allocator,
    const char *key_data,
    const bool have_row_cnt,
    common::ObIArray<ObTableHandleV2> &major_tables,
    common::ObIArray<ObTableHandleV2> &minor_tables,
    common::ObIArray<ObTableHandleV2> &memtables,
    ObTabletHandle &tablet_handle);
  static int prepare_freeze_info(
    const int64_t snapshot_gc_ts,
    common::ObIArray<share::ObFreezeInfo> &freeze_infos);

  int construct_array(
      const char *snapshot_list,
      ObIArray<int64_t> &array);
  int check_result_tables_handle(const char *end_log_ts_list, const ObGetMergeTablesResult &result);

public:
  TestCompactionPolicy();
  ~TestCompactionPolicy() = default;
  int prepare_tablet(
    const char *key_data,
    const int64_t clog_checkpoint_ts,
    const int64_t snapshot_version,
    const bool have_row_cnt = false);
public:
  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
public:
  static constexpr int64_t TEST_TENANT_ID = 1;
  static constexpr int64_t TEST_LS_ID = 9001;
  static constexpr int64_t TEST_TABLET_ID = 2323233;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObTenantFreezeInfoMgr *freeze_info_mgr_;
  ObTabletHandle tablet_handle_;
  ObSEArray<ObTableHandleV2, 4> major_tables_;
  ObSEArray<ObTableHandleV2, 4> minor_tables_;
  ObSEArray<ObTableHandleV2, 4> memtables_;
  ObMediumCompactionInfo medium_info_;
  ObSEArray<int64_t, 10> array_;
  common::ObArenaAllocator allocator_;
};

TestCompactionPolicy::TestCompactionPolicy()
  : ls_id_(TEST_LS_ID),
    tablet_id_(TEST_TABLET_ID),
    freeze_info_mgr_(nullptr),
    tablet_handle_(),
    major_tables_(),
    minor_tables_(),
    memtables_()
{
}

void TestCompactionPolicy::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  int ret = OB_SUCCESS;

  share::schema::ObTableSchema table_schema;
  TestSchemaPrepare::prepare_schema(table_schema);

  medium_info_.compaction_type_ = ObMediumCompactionInfo::MEDIUM_COMPACTION;
  medium_info_.medium_snapshot_ = 100;
  medium_info_.data_version_ = 100;

  medium_info_.storage_schema_.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL);
}

void TestCompactionPolicy::TearDown()
{
  tablet_handle_.reset();
  major_tables_.reset();
  minor_tables_.reset();
  memtables_.reset();

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  bool all_released = false;
  t3m->check_all_meta_mem_released(all_released, "TestCompactionPolicy");

  ObTenantFreezeInfoMgr *freeze_info_mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != freeze_info_mgr);
  freeze_info_mgr->freeze_info_mgr_.freeze_info_.reset();
  freeze_info_mgr->snapshots_[0].reset();
  freeze_info_mgr->snapshots_[1].reset();
}

void TestCompactionPolicy::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  // ls service cannot service before ObServerCheckpointSlogHandler starts running
  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
  // create ls
  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(TEST_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle);
  if (OB_SUCCESS != ret) {
    LOG_ERROR("[FATAL ERROR] failed to create ls", K(ret));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestCompactionPolicy::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID));
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLSID ls_id = ObLSID(TEST_LS_ID);
  ObTabletID tablet_id = ObTabletID(TEST_TABLET_ID);
  ObTabletMapKey key(ls_id, tablet_id);
  ASSERT_EQ(OB_SUCCESS, t3m->del_tablet(key));

  MockTenantModuleEnv::get_instance().destroy();
}

void TestCompactionPolicy::generate_table_key(
    const ObITable::TableType &type,
    const int64_t start_scn,
    const int64_t end_scn,
    ObITable::TableKey &table_key)
{
  table_key.reset();
  table_key.tablet_id_ = TEST_TABLET_ID;
  table_key.table_type_ = type;
  if (type == ObITable::TableType::MAJOR_SSTABLE) {
    table_key.version_range_.base_version_ = start_scn;
    table_key.version_range_.snapshot_version_ = end_scn;
  } else {
    table_key.scn_range_.start_scn_.convert_for_tx(start_scn);
    table_key.scn_range_.end_scn_.convert_for_tx(end_scn);
  }
}

int TestCompactionPolicy::mock_sstable(
  common::ObArenaAllocator &allocator,
  const ObITable::TableType &type,
  const int64_t start_scn,
  const int64_t end_scn,
  const int64_t max_merged_trans_version,
  const int64_t upper_trans_version,
  ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;

  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);

  ObITable::TableKey table_key;
  generate_table_key(type, start_scn, end_scn, table_key);

  ObTabletID tablet_id;
  tablet_id = TEST_TABLET_ID;
  ObTabletCreateSSTableParam param;
  ObStorageSchema storage_schema;
  if (OB_FAIL(storage_schema.init(allocator, table_schema, lib::Worker::CompatMode::MYSQL))) {
    LOG_WARN("failed to init storage schema", K(ret));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::build_create_sstable_param(storage_schema, tablet_id, 100, param))) {
    LOG_WARN("failed to build create sstable param", K(ret), K(table_key));
  } else {
    param.table_key_ = table_key;
    param.max_merged_trans_version_ = max_merged_trans_version;
  }

  void *buf = nullptr;
  ObSSTable *sstable = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObSSTable)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate sstable memory", K(ret));
  } else if (OB_ISNULL(sstable = new (buf)ObSSTable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table", K(ret));
  } else if (OB_FAIL(sstable->init(param, &allocator))) {
    LOG_WARN("fail to init sstable", K(ret), K(param));
  } else if (OB_FAIL(table_handle.set_sstable(sstable, &allocator))) {
    LOG_WARN("failed to set table handle", K(ret), KPC(sstable));
  } else {
    sstable->meta_->basic_meta_.max_merged_trans_version_ = max_merged_trans_version;
    sstable->meta_->basic_meta_.upper_trans_version_ = upper_trans_version;
    sstable->meta_cache_.max_merged_trans_version_ = max_merged_trans_version;
    sstable->meta_cache_.upper_trans_version_ = upper_trans_version;
    sstable->meta_cache_.nested_size_ = 0;
    sstable->meta_cache_.nested_offset_ = 0;
  }
  return ret;
}

int TestCompactionPolicy::mock_sstable_meta(
    const int64_t row_count,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = nullptr;
  if (OB_UNLIKELY(row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row count", KR(ret), K(row_count));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", KR(ret), K(table_handle));
  } else {
    sstable->meta_->basic_meta_.row_count_ = row_count;
    sstable->meta_cache_.row_count_ = row_count;
  }
  return ret;
}

int TestCompactionPolicy::mock_memtable(
    const int64_t start_scn,
    const int64_t end_scn,
    const int64_t snapshot_version,
    ObTablet &tablet,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  SCN clog_checkpoint_scn;
  ObProtectedMemtableMgrHandle *protected_handle = NULL;

  ObITable::TableKey table_key;
  int64_t end_border = -1;
  if (0 == end_scn || INT64_MAX == end_scn) {
    end_border = OB_MAX_SCN_TS_NS;
  } else {
    end_border = end_scn;
  }

  generate_table_key(ObITable::DATA_MEMTABLE, start_scn, end_border, table_key);
  ObMemtable *memtable = nullptr;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = nullptr;

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);

  if (OB_FAIL(tablet.get_protected_memtable_mgr_handle(protected_handle))) {
    LOG_WARN("failed to get_protected_memtable_mgr_handle", K(ret), K(tablet));
  }
  // if memtable_mgr not exist, create it
  else if (OB_FAIL(protected_handle->create_tablet_memtable_mgr_(
          tablet.get_tablet_meta().ls_id_, tablet.get_tablet_meta().tablet_id_, lib::Worker::CompatMode::MYSQL))) {
    LOG_WARN("failed to get_protected_memtable_mgr_handle", K(ret));
  }
  ObTabletMemtableMgr *mt_mgr = static_cast<ObTabletMemtableMgr *>(protected_handle->memtable_mgr_handle_.get_memtable_mgr());
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(t3m->acquire_data_memtable(table_handle))) {
    LOG_WARN("failed to acquire memtable", K(ret));
  } else if (OB_ISNULL(memtable = static_cast<ObMemtable*>(table_handle.get_table()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get memtable", K(ret));
  } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls_svr->get_ls(mt_mgr->ls_->get_ls_id(), ls_handle, ObLSGetMod::DATA_MEMTABLE_MOD))) {
    LOG_WARN("failed to get ls handle", K(ret));
  } else if (OB_FAIL(memtable->init(table_key, ls_handle, mt_mgr->freezer_, mt_mgr, 0, mt_mgr->freezer_->get_freeze_clock()))) {
    LOG_WARN("failed to init memtable", K(ret));
  } else if (OB_FAIL(mt_mgr->add_memtable_(table_handle))) {
    LOG_WARN("failed to add memtable to mgr", K(ret));
  } else if (OB_FAIL(memtable->add_to_data_checkpoint(mt_mgr->freezer_->get_ls_data_checkpoint()))) {
    LOG_WARN("add to data_checkpoint failed", K(ret), KPC(memtable));
    mt_mgr->clean_tail_memtable_();
  } else if (OB_MAX_SCN_TS_NS != end_border) { // frozen memtable
    SCN snapshot_scn;
    snapshot_scn.convert_for_tx(snapshot_version);
    memtable->snapshot_version_ = snapshot_scn;
    memtable->write_ref_cnt_ = 0;
    memtable->unsubmitted_cnt_ = 0;
    memtable->set_is_tablet_freeze();
    memtable->set_resolved_active_memtable_left_boundary();
    memtable->set_frozen();
    memtable->location_ = storage::checkpoint::ObFreezeCheckpointLocation::PREPARE;
  }

  return ret;
}

int TestCompactionPolicy::mock_tablet(
    common::ObArenaAllocator &allocator,
    const int64_t clog_checkpoint_ts,
    const int64_t snapshot_version,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id = ObLSID(TEST_LS_ID);
  ObTabletID tablet_id = ObTabletID(TEST_TABLET_ID);
  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  const lib::Worker::CompatMode &compat_mode = lib::Worker::CompatMode::MYSQL;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

  const ObTabletMapKey key(ls_id, tablet_id);
  ObTablet *tablet = nullptr;

  ObTableHandleV2 table_handle;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = nullptr;

  ObArenaAllocator arena_allocator;
  ObCreateTabletSchema create_tablet_schema;
  bool need_empty_major_table = false;

  if (OB_ISNULL(t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null t3m", K(ret));
  } else if (OB_FAIL(t3m->del_tablet(key))) {
    LOG_WARN("failed to del tablet", K(ret));
  } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls handle", K(ret));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_tmp_tablet(key, allocator, tablet_handle))) {
    LOG_WARN("failed to acquire tablet", K(ret), K(key));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(create_tablet_schema.init(arena_allocator, table_schema, compat_mode,
         false/*skip_column_info*/, ObCreateTabletSchema::STORAGE_SCHEMA_VERSION_V3))) {
    LOG_WARN("failed to init storage schema", KR(ret), K(table_schema));
  } else if (OB_FAIL(tablet->init_for_first_time_creation(allocator, ls_id, tablet_id, tablet_id,
      SCN::min_scn(), snapshot_version, create_tablet_schema, need_empty_major_table, ls_handle.get_ls()->get_freezer()))) {
    LOG_WARN("failed to init tablet", K(ret), K(ls_id), K(tablet_id), K(snapshot_version),
              K(table_schema), K(compat_mode));
  } else {
    tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_logservice(clog_checkpoint_ts);
    tablet->tablet_meta_.snapshot_version_ = snapshot_version;
  }
  return ret;
}

int TestCompactionPolicy::construct_array(
    const char *snapshot_list,
    ObIArray<int64_t> &array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(snapshot_list)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(snapshot_list));
  } else {
    array.reset();
    std::string copy(snapshot_list);
    char *org = const_cast<char *>(copy.c_str());
    static const char *delim = " ";
    char *s = std::strtok(org, delim);
    if (NULL != s) {
      array.push_back(atoi(s));
      while (NULL != (s= strtok(NULL, delim))) {
        array.push_back(atoi(s));
      }
    }
  }
  return ret;
}

int TestCompactionPolicy::check_result_tables_handle(
    const char *end_log_ts_list,
    const ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  construct_array(end_log_ts_list, array_);
  if (array_.count() != result.handle_.get_count()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "table count is not equal", K(ret), K(array_), K(result.handle_));
  }
  for (int i = 0; OB_SUCC(ret) && i < array_.count(); ++i) {
    if (array_.at(i) != result.handle_.get_table(i)->get_end_scn().get_val_for_tx()) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "table is not equal", K(ret), K(i), K(array_.at(i)), KPC(result.handle_.get_table(i)));
    }
  }
  return ret;
}

int TestCompactionPolicy::mock_table_store(
    common::ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle,
    common::ObIArray<ObTableHandleV2> &major_table_handles,
    common::ObIArray<ObTableHandleV2> &minor_table_handles)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable *, 8> major_tables;
  for (int64_t i = 0; OB_SUCC(ret) && i < major_table_handles.count(); ++i) {
    if (OB_FAIL(major_tables.push_back(major_table_handles.at(i).get_table()))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }

  ObSEArray<ObITable *, 8> minor_tables;
  for (int64_t i = 0; OB_SUCC(ret) && i < minor_table_handles.count(); ++i) {
    if (OB_FAIL(minor_tables.push_back(minor_table_handles.at(i).get_table()))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }

  ObTablet *tablet = tablet_handle.get_obj();
  ObTabletTableStore &table_store = *tablet->table_store_addr_.get_ptr();
  if (OB_SUCC(ret) && major_tables.count() > 0) {
    if (OB_FAIL(table_store.major_tables_.init(allocator, major_tables))) {
      LOG_WARN("failed to init major tables", K(ret));
    }
  }

  if (OB_SUCC(ret) && minor_tables.count() > 0) {
    if (OB_FAIL(table_store.minor_tables_.init(allocator, minor_tables))) {
      LOG_WARN("failed to init major tables", K(ret));
    }
  }
  return ret;
}

int TestCompactionPolicy::batch_mock_sstables(
  common::ObArenaAllocator &allocator,
  const char *key_data,
  common::ObIArray<ObTableHandleV2> &major_tables,
  common::ObIArray<ObTableHandleV2> &minor_tables)
{
  int ret = OB_SUCCESS;
  ObMockIterator key_iter;
  key_iter.from(key_data);

  const ObStoreRow *row = nullptr;
  const ObObj *cells = nullptr;
  for (int64_t i = 0; i < key_iter.count(); ++i) {
    key_iter.get_row(i, row);
    cells = row->row_val_.cells_;

    ObTableHandleV2 table_handle;
    const int64_t type = cells[0].get_int();
    ObITable::TableType table_type = (type == 10) ? ObITable::MAJOR_SSTABLE : ((type == 11) ? ObITable::MINOR_SSTABLE : ObITable::MINI_SSTABLE);
    if (OB_FAIL(mock_sstable(allocator, table_type, cells[1].get_int(), cells[2].get_int(), cells[3].get_int(), cells[4].get_int(), table_handle))) {
      LOG_WARN("failed to mock sstable", K(ret));
    } else if (ObITable::MAJOR_SSTABLE == table_type) {
      if (OB_FAIL(major_tables.push_back(table_handle))) {
        LOG_WARN("failed to add table", K(ret));
      }
    } else if (OB_FAIL(minor_tables.push_back(table_handle))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }
  return ret;
}

int TestCompactionPolicy::batch_mock_memtables(
  const char *key_data,
  ObTabletHandle &tablet_handle,
  common::ObIArray<ObTableHandleV2> &memtables)
{
  int ret = OB_SUCCESS;
  ObMockIterator key_iter;
  key_iter.from(key_data);

  const ObStoreRow *row = nullptr;
  const ObObj *cells = nullptr;
  for (int64_t i = 0; i < key_iter.count(); ++i) {
    key_iter.get_row(i, row);
    cells = row->row_val_.cells_;

    ObTableHandleV2 table_handle;
    const int64_t type = cells[0].get_int();
    assert(0 == type);
    if (OB_FAIL(mock_memtable(cells[1].get_int(), cells[2].get_int(), cells[3].get_int(), *tablet_handle.get_obj(), table_handle))) {
      LOG_WARN("failed to mock memtable", K(ret));
    } else if (OB_FAIL(memtables.push_back(table_handle))) {
      LOG_WARN("failed to add memtable", K(ret));
    }
  }
  return ret;
}

int TestCompactionPolicy::batch_mock_tables(
  common::ObArenaAllocator &allocator,
  const char *key_data,
  const bool have_row_cnt,
  common::ObIArray<ObTableHandleV2> &major_tables,
  common::ObIArray<ObTableHandleV2> &minor_tables,
  common::ObIArray<ObTableHandleV2> &memtables,
  ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObMockIterator key_iter;
  key_iter.from(key_data);

  const ObStoreRow *row = nullptr;
  const ObObj *cells = nullptr;
  for (int64_t i = 0; i < key_iter.count(); ++i) {
    key_iter.get_row(i, row);
    cells = row->row_val_.cells_;

    ObTableHandleV2 table_handle;
    const int64_t type = cells[0].get_int();
    if (0 == type) {
      if (OB_FAIL(mock_memtable(cells[1].get_int(), cells[2].get_int(), cells[3].get_int(), *tablet_handle.get_obj(), table_handle))) {
        LOG_WARN("failed to mock memtable", K(ret));
      } else if (OB_FAIL(memtables.push_back(table_handle))) {
        LOG_WARN("failed to add table", K(ret));
      }
    } else {
      ObITable::TableType table_type = (type == 10) ? ObITable::MAJOR_SSTABLE : ((type == 11) ? ObITable::MINOR_SSTABLE : ObITable::MINI_SSTABLE);
      if (OB_FAIL(mock_sstable(allocator, table_type, cells[1].get_int(), cells[2].get_int(), cells[3].get_int(), cells[4].get_int(), table_handle))) {
        LOG_WARN("failed to mock sstable", K(ret));
      } else if (have_row_cnt && OB_FAIL(mock_sstable_meta(cells[5].get_int(), table_handle))) {
        LOG_WARN("failed to mock sstable meta", KR(ret));
      } else if (ObITable::MAJOR_SSTABLE == table_type) {
        if (OB_FAIL(major_tables.push_back(table_handle))) {
          LOG_WARN("failed to add table", K(ret));
        }
      } else if (OB_FAIL(minor_tables.push_back(table_handle))) {
        LOG_WARN("failed to add table", K(ret));
      }
    }
  }
  return ret;
}

int TestCompactionPolicy::prepare_tablet(
    const char *key_data,
    const int64_t clog_checkpoint_ts,
    const int64_t snapshot_version,
    const bool have_row_cnt)
{
  int ret = OB_SUCCESS;
  tablet_handle_.reset();
  major_tables_.reset();
  minor_tables_.reset();
  memtables_.reset();

  if (OB_UNLIKELY(clog_checkpoint_ts <= 0 || snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(snapshot_version));
  } else if (OB_FAIL(mock_tablet(allocator_, clog_checkpoint_ts, snapshot_version, tablet_handle_))) {
    LOG_WARN("failed to mock tablet", K(ret));
  } else if (OB_ISNULL(key_data)) {
  } else if (OB_FAIL(batch_mock_tables(allocator_, key_data, have_row_cnt, major_tables_, minor_tables_, memtables_, tablet_handle_))) {
    LOG_WARN("failed to batch mock tables", K(ret));
  } else if (OB_FAIL(mock_table_store(allocator_, tablet_handle_, major_tables_, minor_tables_))) {
    LOG_WARN("failed to mock table store", K(ret));
  }
  return ret;
}

int TestCompactionPolicy::prepare_freeze_info(
  const int64_t snapshot_gc_ts,
  common::ObIArray<share::ObFreezeInfo> &freeze_infos)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);

  if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mgr is unexpected null", K(ret));
  } else {
    share::ObFreezeInfoList &info_list = mgr->freeze_info_mgr_.freeze_info_;
    info_list.reset();

    info_list.frozen_statuses_.assign(freeze_infos);
    info_list.latest_snapshot_gc_scn_.val_ = snapshot_gc_ts;
  }
  return ret;
}

class FakeLS : public storage::ObLS
{
public:
  FakeLS() {
    ls_meta_.tenant_id_ = 1001;
    ls_meta_.ls_id_ = ObLSID(100);
  }
  int64_t get_min_reserved_snapshot() { return 10; }
};


static const int64_t TENANT_ID = 1;
static const int64_t TABLE_ID = 7777;
static const int64_t TEST_ROWKEY_COLUMN_CNT = 3;
static const int64_t TEST_COLUMN_CNT = 6;

TEST_F(TestCompactionPolicy, basic_create_sstable)
{
  int ret = OB_SUCCESS;

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ASSERT_NE(nullptr, t3m);

  ObTableHandleV2 major_table_handle;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MAJOR_SSTABLE, 0, 100, 100, 100, major_table_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTableHandleV2 mini_table_handle;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MINI_SSTABLE, 100, 120, 120, 120, mini_table_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTableHandleV2 minor_table_handle;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MINOR_SSTABLE, 120, 180, 180, INT64_MAX, minor_table_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestCompactionPolicy, basic_create_tablet)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ASSERT_NE(nullptr, t3m);

  ObTabletHandle tablet_handle;
  ret = TestCompactionPolicy::mock_tablet(allocator_, 100, 100, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, tablet_handle.is_valid());

  ObTablet *tablet = tablet_handle.get_obj();
  ObTabletTableStore &table_store = *tablet->table_store_addr_.get_ptr();
  ASSERT_EQ(true, table_store.is_valid());
}

TEST_F(TestCompactionPolicy, basic_create_memtable)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ret = TestCompactionPolicy::mock_tablet(allocator_, 100, 100, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, tablet_handle.is_valid());

  SCN clog_checkpoint_scn;
  ObProtectedMemtableMgrHandle *protected_handle = NULL;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_protected_memtable_mgr_handle(protected_handle));
  // if memtable_mgr not exist, create it
  const ObTabletMeta &tablet_meta = tablet_handle.get_obj()->get_tablet_meta();
  ASSERT_EQ(OB_SUCCESS, protected_handle->create_tablet_memtable_mgr_(tablet_meta.ls_id_, tablet_meta.tablet_id_, lib::Worker::CompatMode::MYSQL));
  ObTabletMemtableMgr *mt_mgr = static_cast<ObTabletMemtableMgr *>(protected_handle->memtable_mgr_handle_.get_memtable_mgr());
  ASSERT_EQ(0, mt_mgr->get_memtable_count_());
  ObTableHandleV2 frozen_memtable;
  ret = TestCompactionPolicy::mock_memtable(1, 100, 100, *tablet_handle.get_obj(), frozen_memtable);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, mt_mgr->get_memtable_count_());
  ObMemtable *frozen_mt = static_cast<ObMemtable *>(frozen_memtable.get_table());
  ASSERT_EQ(true, frozen_mt->is_in_prepare_list_of_data_checkpoint());

  ASSERT_EQ(true, frozen_mt->can_be_minor_merged());

  ObTableHandleV2 active_memtable;
  ret = TestCompactionPolicy::mock_memtable(100, INT64_MAX, INT64_MAX, *tablet_handle.get_obj(), active_memtable);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, mt_mgr->get_memtable_count_());
  ObMemtable *active_mt = static_cast<ObMemtable *>(active_memtable.get_table());
  ASSERT_EQ(false, active_mt->can_be_minor_merged());
}

TEST_F(TestCompactionPolicy, basic_create_table_store)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ret = TestCompactionPolicy::mock_tablet(allocator_, 100, 100, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSEArray<ObTableHandleV2, 4> major_tables;
  ObTableHandleV2 major_table_handle_1;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MAJOR_SSTABLE, 0, 1, 1, 1, major_table_handle_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, major_tables.push_back(major_table_handle_1));

  ObTableHandleV2 major_table_handle_2;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MAJOR_SSTABLE, 0, 100, 100, 100, major_table_handle_2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, major_tables.push_back(major_table_handle_2));

  ObTableHandleV2 major_table_handle_3;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MAJOR_SSTABLE, 0, 150, 150, 150, major_table_handle_3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, major_tables.push_back(major_table_handle_3));


  ObSEArray<ObTableHandleV2, 4> minor_tables;
  ObTableHandleV2 minor_table_handle_1;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MINI_SSTABLE, 100, 150, 150, 160, minor_table_handle_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, minor_tables.push_back(minor_table_handle_1));

  ObTableHandleV2 minor_table_handle_2;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MINI_SSTABLE, 150, 200, 190, 200, minor_table_handle_2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, minor_tables.push_back(minor_table_handle_2));

  ObTableHandleV2 minor_table_handle_3;
  ret = TestCompactionPolicy::mock_sstable(allocator_, ObITable::MINI_SSTABLE, 200, 350, 350, INT64_MAX, minor_table_handle_3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, minor_tables.push_back(minor_table_handle_3));

  ret = TestCompactionPolicy::mock_table_store(allocator_, tablet_handle, major_tables, minor_tables);
  ASSERT_EQ(OB_SUCCESS, ret);

  LOG_INFO("Print tablet", KPC(tablet_handle.get_obj()));
}

TEST_F(TestCompactionPolicy, basic_batch_create_sstable)
{
  int ret = OB_SUCCESS;
  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "10            0            100        100        100      \n"
      "11            1            80         80         120      \n"
      "11            80           150        150        500      \n";

  ObArray<ObTableHandleV2> major_tables;
  ObArray<ObTableHandleV2> minor_tables;
  ret = TestCompactionPolicy::batch_mock_sstables(allocator_, key_data, major_tables, minor_tables);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, major_tables.count());
  ASSERT_EQ(2, minor_tables.count());
}

TEST_F(TestCompactionPolicy, basic_prepare_tablet)
{
  int ret = OB_SUCCESS;
  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "10            0            100        100        100      \n"
      "11            1            80         80         120      \n"
      "11            80           150        150        500      \n"
      "0             150          200        180        180      \n"
      "0             200          0          0          0        \n";

  ret = prepare_tablet(key_data, 150, 150);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletTableStore &table_store = *tablet_handle_.get_obj()->table_store_addr_.get_ptr();
  ASSERT_EQ(2, table_store.major_tables_.count());
  ASSERT_EQ(2, table_store.minor_tables_.count());

  ObProtectedMemtableMgrHandle *protected_handle = NULL;
  ASSERT_EQ(OB_SUCCESS, tablet_handle_.get_obj()->get_protected_memtable_mgr_handle(protected_handle));
  ObTabletMemtableMgr *mt_mgr = static_cast<ObTabletMemtableMgr *>(protected_handle->memtable_mgr_handle_.get_memtable_mgr());
  ASSERT_EQ(2, mt_mgr->get_memtable_count_());
}

TEST_F(TestCompactionPolicy, check_mini_merge_basic)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            1            160        150        150      \n"
      "11            160          300        300        300      \n"
      "0             1            160        150        150      \n"
      "0             160          200        210        210      \n"
      "0             200          300        300        300      \n"
      "0             300          0          0          0        \n";

  ret = prepare_tablet(key_data, 150, 150);
  ASSERT_EQ(OB_SUCCESS, ret);

  FakeLS ls;
  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINI_MERGE;
  ObGetMergeTablesResult result;
  tablet_handle_.get_obj()->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(300);
  tablet_handle_.get_obj()->tablet_meta_.snapshot_version_ = 300;
  ret = ObPartitionMergePolicy::get_mini_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(result.update_tablet_directly_, true);
}

TEST_F(TestCompactionPolicy, check_minor_merge_basic)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            1            150        150        150      \n"
      "11            150          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            300          350        350        350      \n";

  ret = prepare_tablet(key_data, 350, 350);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(5, result.handle_.get_count());
}

TEST_F(TestCompactionPolicy, check_no_need_minor_merge)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 320;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 400;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            1            150        150        150      \n"
      "11            150          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            300          310        310        310      \n"
      "11            310          375        375        375      \n";

  ret = prepare_tablet(key_data, 375, 375);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_NO_NEED_MERGE, ret);
}

TEST_F(TestCompactionPolicy, check_major_merge_basic)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 340;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            1            150        150        150      \n"
      "11            150          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            300          350        350        350      \n";

  ret = prepare_tablet(key_data, 350, 350);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MAJOR_MERGE;
  param.merge_version_ = 350;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_medium_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(6, result.handle_.get_count());
}

TEST_F(TestCompactionPolicy, check_no_need_major_merge)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
  frozen_val.val_ = 340;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "10            0            340        340        340      \n"
      "11            150          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            300          340        340        340      \n";

  ret = prepare_tablet(key_data, 340, 340);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MAJOR_MERGE;
  param.merge_version_ = 340;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_medium_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_NO_NEED_MERGE, ret);
}

TEST_F(TestCompactionPolicy, test_medium_info_serialize)
{
  int ret = OB_SUCCESS;
  // prepare parallel_rowkey_list
  const int64_t concurrent_cnt = 5;
  ObArenaAllocator allocator;
  ObDatumRowkey datum_rowkey_list[concurrent_cnt];
  ObDatumRowkey tmp_datum_rowkey;
  ObStorageDatum datums[OB_INNER_MAX_ROWKEY_COLUMN_NUMBER];
  tmp_datum_rowkey.assign(datums, OB_INNER_MAX_ROWKEY_COLUMN_NUMBER);

  medium_info_.contain_parallel_range_ = true;
  medium_info_.parallel_merge_info_.list_size_ = concurrent_cnt;
  medium_info_.parallel_merge_info_.parallel_datum_rowkey_list_ = datum_rowkey_list;

  for (int64_t idx = 0; idx < concurrent_cnt; ++idx) {
    tmp_datum_rowkey.datums_[0].set_string("aaaaa");
    tmp_datum_rowkey.datums_[1].set_int(idx);
    tmp_datum_rowkey.datum_cnt_ = 2;
    if (OB_FAIL(tmp_datum_rowkey.deep_copy(
      medium_info_.parallel_merge_info_.parallel_datum_rowkey_list_[idx] /*dst*/, allocator))) {
      LOG_WARN("failed to deep copy datum rowkey", KR(ret), K(idx), K(tmp_datum_rowkey));
    }
  }

  const int64_t buf_len = ObParallelMergeInfo::MAX_PARALLEL_RANGE_SERIALIZE_LEN;
  char medium_info_buf[buf_len];
  int64_t write_pos = 0;
  ret = medium_info_.serialize(medium_info_buf, buf_len, write_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(medium_info_.get_serialize_size(), write_pos);

  ObMediumCompactionInfo deserialize_medium_info;
  int64_t pos = 0;
  ret = deserialize_medium_info.deserialize(allocator, medium_info_buf, write_pos, pos);
  ASSERT_EQ(pos, write_pos);

  ASSERT_EQ(deserialize_medium_info.contain_parallel_range_, true);
  ASSERT_EQ(deserialize_medium_info.parallel_merge_info_.list_size_, concurrent_cnt);
  for (int64_t idx = 0; idx < concurrent_cnt; ++idx) {
    ASSERT_TRUE(
      medium_info_.parallel_merge_info_.parallel_datum_rowkey_list_[idx].datums_[0]
       == deserialize_medium_info.parallel_merge_info_.parallel_datum_rowkey_list_[idx].datums_[0]);
    ASSERT_TRUE(idx == deserialize_medium_info.parallel_merge_info_.parallel_datum_rowkey_list_[idx].datums_[1].get_int());
  }
}

TEST_F(TestCompactionPolicy, test_minor_dag_intersect)
{
  ObTabletMergeExecuteDag dag1;
  dag1.merge_type_ = MINOR_MERGE;
  dag1.ls_id_ = ObLSID(1);
  dag1.tablet_id_ = ObTabletID(1);

  ObTabletMergeExecuteDag dag2;
  dag2.merge_type_ = MINOR_MERGE;
  dag2.ls_id_ = ObLSID(1);
  dag2.tablet_id_ = ObTabletID(1);

  dag1.result_.scn_range_.start_scn_.val_ = 10;
  dag1.result_.scn_range_.end_scn_.val_ = 30;

  dag2.result_.scn_range_.start_scn_.val_ = 25;
  dag2.result_.scn_range_.end_scn_.val_ = 30;
  // if scn_range cross, means dag equal
  ASSERT_EQ(true, (dag1 == dag2));

  dag2.result_.scn_range_.start_scn_.val_ = 5;
  dag2.result_.scn_range_.end_scn_.val_ = 15;
  ASSERT_EQ(true, (dag1 == dag2));

  dag2.result_.scn_range_.start_scn_.val_ = 30;
  dag2.result_.scn_range_.end_scn_.val_ = 35;
  ASSERT_EQ(false, (dag1 == dag2));

  dag2.result_.scn_range_.start_scn_.val_ = 5;
  dag2.result_.scn_range_.end_scn_.val_ = 10;
  ASSERT_EQ(false, (dag1 == dag2));
}


TEST_F(TestCompactionPolicy, check_sstable_continue_failed)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);

  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            1            150        150        150      \n"
      "11            150          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            900          1000       1000       1000      \n";

  ret = prepare_tablet(key_data, 1000, 1000);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle_.get_obj();
  ASSERT_TRUE(nullptr != tablet);
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ret = tablet->fetch_table_store(table_store_wrapper);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table_store_wrapper.get_member()->check_continuous();
  ASSERT_EQ(OB_ERR_SYS, ret);
}

TEST_F(TestCompactionPolicy, check_minor_merge_policy_with_large_minor)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver    row_cnt\n"
      "10            0            1          1          1             1\n"
      "11            1            150        150        150           3000000\n"
      "12            150          200        200        200           100\n"
      "12            200          350        350        350           10000\n";

  ret = prepare_tablet(key_data, 350, 350, true/*have row cnt*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  // small mini sstable count = 2, should not schedule minor merge
  ASSERT_EQ(OB_NO_NEED_MERGE, ret);
}

TEST_F(TestCompactionPolicy, check_minor_merge_policy_with_large_minor2)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver    row_cnt\n"
      "10            0            1          1          1             1\n"
      "11            1            150        150        150           3000000\n"
      "12            150          300        200        200           100\n"
      "12            300          350        350        350           750000\n";

  ret = prepare_tablet(key_data, 350, 350, true/*have row cnt*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  // reach size_amplification_factor, need minor
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, result.handle_.get_count());
  ASSERT_EQ(1, result.scn_range_.start_scn_.get_val_for_tx());
  ASSERT_EQ(350, result.scn_range_.end_scn_.get_val_for_tx());
}

TEST_F(TestCompactionPolicy, check_minor_merge_policy_with_large_minor3)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<share::ObFreezeInfo> freeze_info;
  share::SCN frozen_val;
  frozen_val.val_ = 1;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver    row_cnt\n"
      "10            0            1          1          1             1\n"
      "11            1            150        150        150           3000000\n"
      "12            150          250        200        200           100\n"
      "12            250          300        200        200           100\n"
      "12            300          350        350        350           600\n";

  ret = prepare_tablet(key_data, 350, 350, true/*have row cnt*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  // small mini sstable count reach minor_compact_trigger, all small mini sstables should schedule mini minor merge
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, result.handle_.get_count());
  ASSERT_EQ(150, result.scn_range_.start_scn_.get_val_for_tx());
  ASSERT_EQ(350, result.scn_range_.end_scn_.get_val_for_tx());
}

} //unittest
} //oceanbase


int main(int argc, char **argv)
{
  system("rm -rf test_compaction_policy.log*");
  OB_LOGGER.set_file_name("test_compaction_policy.log");
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_compaction_policy");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
