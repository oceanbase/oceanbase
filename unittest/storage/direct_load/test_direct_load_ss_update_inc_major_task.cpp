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
 */

#ifdef OB_BUILD_SHARED_STORAGE

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define private public
#define protected public

#include "mtlenv/mock_tenant_module_env.h"
#include "storage/direct_load/ob_direct_load_ss_update_inc_major_task.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/compaction/ob_uncommit_tx_info.h"
#include "share/schema/ob_table_schema.h"
#include "unittest/storage/test_tablet_helper.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace share::schema;
using namespace share;

namespace unittest
{

class MockDirectLoadSSUpdateIncMajorTask : public ObDirectLoadSSUpdateIncMajorITask
{
public:
  MockDirectLoadSSUpdateIncMajorTask(ObLS &ls,
                                     const ObTablet &tablet,
                                     const share::ObLSID &ls_id,
                                     const ObTabletID &tablet_id)
    : ObDirectLoadSSUpdateIncMajorITask(ls, tablet, ls_id, tablet_id)
  {}
  virtual ~MockDirectLoadSSUpdateIncMajorTask() = default;

  int update_tablet_table_store(const blocksstable::ObSSTable *new_inc_major) override
  {
    return OB_SUCCESS;
  }

  int test_prepare_new_committed_inc_major(
      ObSSTable &old_inc_major,
      const int64_t commit_version,
      ObSSTable *&new_inc_major)
  {
    return prepare_new_committed_inc_major(old_inc_major, commit_version, new_inc_major);
  }
};

class TestDirectLoadSSUpdateIncMajorTask : public ::testing::Test
{
public:
  TestDirectLoadSSUpdateIncMajorTask();
  virtual ~TestDirectLoadSSUpdateIncMajorTask();
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

  int mock_row_store_inc_major_sstable(
      const int64_t upper_trans_version,
      ObSSTable *&sstable);
  int mock_co_inc_major_sstable(
      const int64_t upper_trans_version,
      const int64_t column_group_cnt,
      ObCOSSTableV2 *&co_sstable);
  int mock_cg_sstable(
      const int64_t cg_idx,
      const int64_t upper_trans_version,
      ObSSTable *&cg_sstable);

protected:
  static ObArenaAllocator allocator_;
  ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObLS *ls_;
  ObTablet *tablet_;
  static const int64_t TEST_COMMIT_VERSION = 1000;
  static const int64_t TEST_UPPER_TRANS_VERSION = INT64_MAX;
};

ObArenaAllocator TestDirectLoadSSUpdateIncMajorTask::allocator_("TestIncMajor", OB_MALLOC_NORMAL_BLOCK_SIZE, 500);
const int64_t TestDirectLoadSSUpdateIncMajorTask::TEST_COMMIT_VERSION;
const int64_t TestDirectLoadSSUpdateIncMajorTask::TEST_UPPER_TRANS_VERSION;

TestDirectLoadSSUpdateIncMajorTask::TestDirectLoadSSUpdateIncMajorTask()
  : ls_id_(1001),
    tablet_id_(200001),
    ls_(nullptr),
    tablet_(nullptr)
{
}

TestDirectLoadSSUpdateIncMajorTask::~TestDirectLoadSSUpdateIncMajorTask()
{
}

void TestDirectLoadSSUpdateIncMajorTask::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestDirectLoadSSUpdateIncMajorTask::TearDownTestCase()
{
  allocator_.reset();
  MockTenantModuleEnv::get_instance().destroy();
}

void TestDirectLoadSSUpdateIncMajorTask::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());

  void *ls_buf = allocator_.alloc(sizeof(ObLS));
  ASSERT_NE(nullptr, ls_buf);
  ls_ = new (ls_buf) ObLS();

  void *tablet_buf = allocator_.alloc(sizeof(ObTablet));
  ASSERT_NE(nullptr, tablet_buf);
  tablet_ = new (tablet_buf) ObTablet();
}

void TestDirectLoadSSUpdateIncMajorTask::TearDown()
{
  if (nullptr != tablet_) {
    tablet_->~ObTablet();
    tablet_ = nullptr;
  }
  if (nullptr != ls_) {
    ls_->~ObLS();
    ls_ = nullptr;
  }
}

int TestDirectLoadSSUpdateIncMajorTask::mock_row_store_inc_major_sstable(
    const int64_t upper_trans_version,
    ObSSTable *&sstable)
{
  int ret = OB_SUCCESS;
  sstable = nullptr;

  ObTabletCreateSSTableParam param;
  ObITable::TableKey table_key;
  table_key.table_type_ = ObITable::TableType::INC_MAJOR_SSTABLE;
  table_key.tablet_id_ = tablet_id_;
  table_key.scn_range_.start_scn_.convert_for_tx(100);
  table_key.scn_range_.end_scn_.convert_for_tx(200);

  param.table_key_ = table_key;
  param.schema_version_ = 100;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = 1;
  param.progressive_merge_step_ = 5;
  param.max_merged_trans_version_ = 200;
  param.index_type_ = ObIndexType::INDEX_TYPE_IS_NOT;
  param.table_mode_.mode_flag_ = ObTableModeFlag::TABLE_MODE_NORMAL;
  param.rowkey_column_cnt_ = 3;
  param.column_cnt_ = 5;
  param.root_block_addr_.set_none_addr();
  param.data_block_macro_meta_addr_.set_none_addr();
  param.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.data_index_tree_height_ = 0;
  param.index_blocks_cnt_ = 0;
  param.data_blocks_cnt_ = 1;
  param.micro_block_cnt_ = 1;
  param.use_old_macro_block_count_ = 0;
  param.data_checksum_ = 0;
  param.occupy_size_ = 1024;
  param.original_size_ = 1024;
  param.ddl_scn_.set_base();
  param.filled_tx_scn_ = table_key.scn_range_.end_scn_;
  param.tx_data_recycle_scn_.set_min();
  param.rec_scn_ = SCN::plus(table_key.get_start_scn(), 1);
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.encrypt_id_ = 0;
  param.master_key_id_ = 0;
  param.table_backup_flag_.reset();
  param.table_shared_flag_.reset();
  param.sstable_logic_seq_ = 0;
  param.row_count_ = 100;
  param.recycle_version_ = 0;
  param.root_macro_seq_ = 0;
  param.nested_size_ = 0;
  param.nested_offset_ = 0;
  param.column_group_cnt_ = 1;
  param.full_column_cnt_ = param.column_cnt_;
  param.co_base_snapshot_version_ = 0;

  compaction::ObUncommitTxDesc tx_desc(100, 200);
  if (OB_FAIL(param.column_checksums_.reserve(param.column_cnt_))) {
    LOG_WARN("failed to reserve column checksums", K(ret), K(param.column_cnt_));
  } else if (OB_FAIL(param.uncommit_tx_info_.push_back(tx_desc))) {
    LOG_WARN("failed to push back tx desc", K(ret));
  } else if (FALSE_IT(param.uncommit_tx_info_.set_info_status_record_seq())) {
  } else if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_))) {
    LOG_WARN("failed to fill column checksum", K(ret));
  } else {
    void *buf = allocator_.alloc(sizeof(ObSSTable));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else if (OB_ISNULL(sstable = new (buf) ObSSTable())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to construct sstable", K(ret));
    } else if (OB_FAIL(sstable->init(param, &allocator_))) {
      LOG_WARN("failed to init sstable", K(ret), K(param));
    } else {
      sstable->meta_->basic_meta_.upper_trans_version_ = upper_trans_version;
      sstable->meta_cache_.upper_trans_version_ = upper_trans_version;
      sstable->meta_->basic_meta_.data_macro_block_count_ = 1;
      sstable->meta_cache_.data_macro_block_count_ = 1;
      sstable->meta_->basic_meta_.status_ = SSTABLE_READY_FOR_READ;
      sstable->valid_for_reading_ = true;
    }
  }
  return ret;
}

int TestDirectLoadSSUpdateIncMajorTask::mock_cg_sstable(
    const int64_t cg_idx,
    const int64_t upper_trans_version,
    ObSSTable *&cg_sstable)
{
  int ret = OB_SUCCESS;
  cg_sstable = nullptr;

  ObTabletCreateSSTableParam param;
  ObITable::TableKey table_key;
  table_key.table_type_ = ObITable::TableType::INC_NORMAL_COLUMN_GROUP_SSTABLE;
  table_key.tablet_id_ = tablet_id_;
  table_key.column_group_idx_ = cg_idx;
  table_key.scn_range_.start_scn_.convert_for_tx(100);
  table_key.scn_range_.end_scn_.convert_for_tx(200);

  param.table_key_ = table_key;
  param.schema_version_ = 100;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = 1;
  param.progressive_merge_step_ = 5;
  param.max_merged_trans_version_ = 200;
  param.index_type_ = ObIndexType::INDEX_TYPE_IS_NOT;
  param.table_mode_.mode_flag_ = ObTableModeFlag::TABLE_MODE_NORMAL;
  param.rowkey_column_cnt_ = 3;
  param.column_cnt_ = 5;
  param.root_block_addr_.set_none_addr();
  param.data_block_macro_meta_addr_.set_none_addr();
  param.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.data_index_tree_height_ = 0;
  param.index_blocks_cnt_ = 0;
  param.data_blocks_cnt_ = 1;
  param.micro_block_cnt_ = 1;
  param.use_old_macro_block_count_ = 0;
  param.data_checksum_ = 0;
  param.occupy_size_ = 1024;
  param.original_size_ = 1024;
  param.ddl_scn_.set_base();
  param.filled_tx_scn_ = table_key.scn_range_.end_scn_;
  param.tx_data_recycle_scn_.set_min();
  param.rec_scn_ = SCN::plus(table_key.get_start_scn(), 1);
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.encrypt_id_ = 0;
  param.master_key_id_ = 0;
  param.table_backup_flag_.reset();
  param.table_shared_flag_.reset();
  param.sstable_logic_seq_ = 0;
  param.row_count_ = 100;
  param.recycle_version_ = 0;
  param.root_macro_seq_ = 0;
  param.nested_size_ = 0;
  param.nested_offset_ = 0;
  param.column_group_cnt_ = 1;
  param.full_column_cnt_ = param.column_cnt_;
  param.co_base_snapshot_version_ = 0;

  compaction::ObUncommitTxDesc tx_desc(100, 200);
  if (OB_FAIL(param.column_checksums_.reserve(param.column_cnt_))) {
    LOG_WARN("failed to reserve column checksums", K(ret), K(param.column_cnt_));
  } else if (OB_FAIL(param.uncommit_tx_info_.push_back(tx_desc))) {
    LOG_WARN("failed to push back tx desc", K(ret));
  } else if (FALSE_IT(param.uncommit_tx_info_.set_info_status_record_seq())) {
  } else if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_))) {
    LOG_WARN("failed to fill column checksum", K(ret));
  } else {
    void *buf = allocator_.alloc(sizeof(ObSSTable));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for cg ObSSTable", K(ret));
    } else if (OB_ISNULL(cg_sstable = new (buf) ObSSTable())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to construct cg ObSSTable", K(ret));
    } else if (OB_FAIL(cg_sstable->init(param, &allocator_))) {
      LOG_WARN("failed to init cg sstable", K(ret), K(param));
    } else {
      cg_sstable->meta_->basic_meta_.upper_trans_version_ = upper_trans_version;
      cg_sstable->meta_cache_.upper_trans_version_ = upper_trans_version;
      cg_sstable->meta_->basic_meta_.data_macro_block_count_ = 1;
      cg_sstable->meta_cache_.data_macro_block_count_ = 1;
      cg_sstable->meta_->basic_meta_.status_ = SSTABLE_READY_FOR_READ;
      cg_sstable->valid_for_reading_ = true;
    }
  }
  return ret;
}

int TestDirectLoadSSUpdateIncMajorTask::mock_co_inc_major_sstable(
    const int64_t upper_trans_version,
    const int64_t column_group_cnt,
    ObCOSSTableV2 *&co_sstable)
{
  int ret = OB_SUCCESS;
  co_sstable = nullptr;

  ObTabletCreateSSTableParam param;
  ObITable::TableKey table_key;
  table_key.table_type_ = ObITable::TableType::INC_COLUMN_ORIENTED_SSTABLE;
  table_key.tablet_id_ = tablet_id_;
  table_key.column_group_idx_ = 0;
  table_key.scn_range_.start_scn_.convert_for_tx(100);
  table_key.scn_range_.end_scn_.convert_for_tx(200);

  param.table_key_ = table_key;
  param.schema_version_ = 100;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = 1;
  param.progressive_merge_step_ = 5;
  param.max_merged_trans_version_ = 200;
  param.index_type_ = ObIndexType::INDEX_TYPE_IS_NOT;
  param.table_mode_.mode_flag_ = ObTableModeFlag::TABLE_MODE_NORMAL;
  param.rowkey_column_cnt_ = 3;
  param.column_cnt_ = 5;
  param.root_block_addr_.set_none_addr();
  param.data_block_macro_meta_addr_.set_none_addr();
  param.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.data_index_tree_height_ = 0;
  param.index_blocks_cnt_ = 0;
  param.data_blocks_cnt_ = 1;
  param.micro_block_cnt_ = 1;
  param.use_old_macro_block_count_ = 0;
  param.data_checksum_ = 0;
  param.occupy_size_ = 2048;
  param.original_size_ = 2048;
  param.ddl_scn_.set_base();
  param.filled_tx_scn_ = table_key.scn_range_.end_scn_;
  param.tx_data_recycle_scn_.set_min();
  param.rec_scn_ = SCN::plus(table_key.get_start_scn(), 1);
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.encrypt_id_ = 0;
  param.master_key_id_ = 0;
  param.table_backup_flag_.reset();
  param.table_shared_flag_.reset();
  param.sstable_logic_seq_ = 0;
  param.row_count_ = 100;
  param.recycle_version_ = 0;
  param.root_macro_seq_ = 0;
  param.nested_size_ = 0;
  param.nested_offset_ = 0;
  param.column_group_cnt_ = column_group_cnt;
  param.co_base_type_ = ObCOSSTableBaseType::ROWKEY_CG_TYPE;
  param.full_column_cnt_ = 10;
  param.is_co_table_without_cgs_ = false;
  param.co_base_snapshot_version_ = 0;

  compaction::ObUncommitTxDesc tx_desc(100, 200);
  if (OB_FAIL(param.column_checksums_.reserve(param.column_cnt_))) {
    LOG_WARN("failed to reserve column checksums", K(ret), K(param.column_cnt_));
  } else if (OB_FAIL(param.uncommit_tx_info_.push_back(tx_desc))) {
    LOG_WARN("failed to push back tx desc", K(ret));
  } else if (FALSE_IT(param.uncommit_tx_info_.set_info_status_record_seq())) {
  } else if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_))) {
    LOG_WARN("failed to fill column checksum", K(ret));
  } else {
    void *co_buf = allocator_.alloc(sizeof(ObCOSSTableV2));
    if (OB_ISNULL(co_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ObCOSSTableV2", K(ret));
    } else if (OB_ISNULL(co_sstable = new (co_buf) ObCOSSTableV2())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to construct ObCOSSTableV2", K(ret));
    } else if (OB_FAIL(co_sstable->init(param, &allocator_))) {
      LOG_WARN("failed to init co sstable", K(ret), K(param));
    } else {
      co_sstable->meta_->basic_meta_.upper_trans_version_ = upper_trans_version;
      co_sstable->meta_cache_.upper_trans_version_ = upper_trans_version;
      co_sstable->meta_->basic_meta_.data_macro_block_count_ = 1;
      co_sstable->meta_cache_.data_macro_block_count_ = 1;
      co_sstable->meta_->basic_meta_.status_ = SSTABLE_READY_FOR_READ;
      co_sstable->valid_for_reading_ = true;

      ObArray<ObITable *> cg_tables;
      for (int64_t i = 1; OB_SUCC(ret) && i < column_group_cnt; ++i) {
        ObSSTable *cg_sstable = nullptr;
        if (OB_FAIL(mock_cg_sstable(i, upper_trans_version, cg_sstable))) {
          LOG_WARN("failed to mock cg sstable", K(ret), K(i));
        } else if (OB_FAIL(cg_tables.push_back(cg_sstable))) {
          LOG_WARN("failed to push back cg sstable", K(ret));
        }
      }

      if (OB_SUCC(ret) && cg_tables.count() > 0) {
        if (OB_FAIL(co_sstable->fill_cg_sstables(cg_tables, param.progressive_merge_step_))) {
          LOG_WARN("failed to fill cg sstables", K(ret));
        }
      }
    }
  }
  return ret;
}

TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_prepare_new_committed_inc_major_row_store)
{
  int ret = OB_SUCCESS;
  ObSSTable *old_inc_major = nullptr;
  ObSSTable *new_inc_major = nullptr;

  ASSERT_EQ(OB_SUCCESS, mock_row_store_inc_major_sstable(TEST_UPPER_TRANS_VERSION, old_inc_major));
  ASSERT_NE(nullptr, old_inc_major);
  ASSERT_FALSE(old_inc_major->is_co_sstable());
  ASSERT_EQ(TEST_UPPER_TRANS_VERSION, old_inc_major->get_upper_trans_version());

  MockDirectLoadSSUpdateIncMajorTask task(*ls_, *tablet_, ls_id_, tablet_id_);

  ret = task.test_prepare_new_committed_inc_major(*old_inc_major, TEST_COMMIT_VERSION, new_inc_major);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, new_inc_major);
  ASSERT_FALSE(new_inc_major->is_co_sstable());
  ASSERT_EQ(TEST_COMMIT_VERSION, new_inc_major->get_upper_trans_version());
  ASSERT_NE(old_inc_major, new_inc_major);

  ObSSTableMetaHandle old_meta_handle;
  ObSSTableMetaHandle new_meta_handle;
  ASSERT_EQ(OB_SUCCESS, old_inc_major->get_meta(old_meta_handle));
  ASSERT_EQ(OB_SUCCESS, new_inc_major->get_meta(new_meta_handle));
  ASSERT_EQ(old_meta_handle.get_sstable_meta().get_progressive_merge_step(),
            new_meta_handle.get_sstable_meta().get_progressive_merge_step());

  LOG_INFO("test_prepare_new_committed_inc_major_row_store passed",
           KPC(old_inc_major), KPC(new_inc_major));
}

TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_prepare_new_committed_inc_major_column_store)
{
  int ret = OB_SUCCESS;
  ObCOSSTableV2 *old_co_sstable = nullptr;
  ObSSTable *new_inc_major = nullptr;
  const int64_t column_group_cnt = 3;

  ASSERT_EQ(OB_SUCCESS, mock_co_inc_major_sstable(TEST_UPPER_TRANS_VERSION, column_group_cnt, old_co_sstable));
  ASSERT_NE(nullptr, old_co_sstable);
  ASSERT_TRUE(old_co_sstable->is_co_sstable());
  ASSERT_EQ(TEST_UPPER_TRANS_VERSION, old_co_sstable->get_upper_trans_version());
  ASSERT_TRUE(old_co_sstable->is_cs_valid());

  MockDirectLoadSSUpdateIncMajorTask task(*ls_, *tablet_, ls_id_, tablet_id_);

  ret = task.test_prepare_new_committed_inc_major(*old_co_sstable, TEST_COMMIT_VERSION, new_inc_major);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, new_inc_major);
  ASSERT_TRUE(new_inc_major->is_co_sstable());
  ASSERT_EQ(TEST_COMMIT_VERSION, new_inc_major->get_upper_trans_version());
  ASSERT_NE(static_cast<ObSSTable*>(old_co_sstable), new_inc_major);

  ObCOSSTableV2 *new_co_sstable = static_cast<ObCOSSTableV2 *>(new_inc_major);
  ASSERT_TRUE(new_co_sstable->is_cs_valid());

  ObArray<ObSSTableWrapper> cg_wrappers;
  ASSERT_EQ(OB_SUCCESS, new_co_sstable->get_all_tables(cg_wrappers));
  ASSERT_EQ(column_group_cnt, cg_wrappers.count());

  for (int64_t i = 0; i < cg_wrappers.count(); ++i) {
    ObSSTable *cg_sstable = nullptr;
    ASSERT_EQ(OB_SUCCESS, cg_wrappers.at(i).get_loaded_column_store_sstable(cg_sstable));
    ASSERT_NE(nullptr, cg_sstable);
    ASSERT_EQ(TEST_COMMIT_VERSION, cg_sstable->get_upper_trans_version());
    LOG_INFO("check cg sstable", K(i), KPC(cg_sstable));
  }

  ObSSTableMetaHandle old_meta_handle;
  ObSSTableMetaHandle new_meta_handle;
  ASSERT_EQ(OB_SUCCESS, old_co_sstable->get_meta(old_meta_handle));
  ASSERT_EQ(OB_SUCCESS, new_co_sstable->get_meta(new_meta_handle));
  ASSERT_EQ(old_meta_handle.get_sstable_meta().get_progressive_merge_step(),
            new_meta_handle.get_sstable_meta().get_progressive_merge_step());

  LOG_INFO("test_prepare_new_committed_inc_major_column_store passed",
           KPC(old_co_sstable), KPC(new_co_sstable));
}

TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_prepare_new_committed_inc_major_invalid_params)
{
  int ret = OB_SUCCESS;
  ObSSTable *old_inc_major = nullptr;
  ObSSTable *new_inc_major = nullptr;

  ASSERT_EQ(OB_SUCCESS, mock_row_store_inc_major_sstable(TEST_COMMIT_VERSION, old_inc_major));
  ASSERT_NE(nullptr, old_inc_major);

  MockDirectLoadSSUpdateIncMajorTask task(*ls_, *tablet_, ls_id_, tablet_id_);

  ret = task.test_prepare_new_committed_inc_major(*old_inc_major, TEST_COMMIT_VERSION, new_inc_major);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  LOG_INFO("test_prepare_new_committed_inc_major_invalid_params passed");
}

TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_progressive_merge_step_preserved)
{
  int ret = OB_SUCCESS;
  ObCOSSTableV2 *old_co_sstable = nullptr;
  ObSSTable *new_inc_major = nullptr;
  const int64_t column_group_cnt = 2;
  const int64_t expected_step = 5;

  ASSERT_EQ(OB_SUCCESS, mock_co_inc_major_sstable(TEST_UPPER_TRANS_VERSION, column_group_cnt, old_co_sstable));
  ASSERT_NE(nullptr, old_co_sstable);

  ObSSTableMetaHandle old_meta_handle;
  ASSERT_EQ(OB_SUCCESS, old_co_sstable->get_meta(old_meta_handle));
  ASSERT_EQ(expected_step, old_meta_handle.get_sstable_meta().get_progressive_merge_step());

  MockDirectLoadSSUpdateIncMajorTask task(*ls_, *tablet_, ls_id_, tablet_id_);

  ret = task.test_prepare_new_committed_inc_major(*old_co_sstable, TEST_COMMIT_VERSION, new_inc_major);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, new_inc_major);

  ObCOSSTableV2 *new_co_sstable = static_cast<ObCOSSTableV2 *>(new_inc_major);
  ObSSTableMetaHandle new_meta_handle;
  ASSERT_EQ(OB_SUCCESS, new_co_sstable->get_meta(new_meta_handle));
  ASSERT_EQ(expected_step, new_meta_handle.get_sstable_meta().get_progressive_merge_step());

  LOG_INFO("test_progressive_merge_step_preserved passed",
           K(expected_step),
           "old_step", old_meta_handle.get_sstable_meta().get_progressive_merge_step(),
           "new_step", new_meta_handle.get_sstable_meta().get_progressive_merge_step());
}

// ==================== ObCOSSTableV2::set_upper_trans_version tests ====================

// Test: set_upper_trans_version on CO sstable updates CO base and all embedded CGs
TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_co_set_upper_trans_version_updates_all_cgs)
{
  int ret = OB_SUCCESS;
  const int64_t column_group_cnt = 4;
  ObCOSSTableV2 *co_sstable = nullptr;

  ASSERT_EQ(OB_SUCCESS, mock_co_inc_major_sstable(TEST_UPPER_TRANS_VERSION, column_group_cnt, co_sstable));
  ASSERT_NE(nullptr, co_sstable);
  ASSERT_TRUE(co_sstable->is_cs_valid());

  // Before: all tables have INT64_MAX
  ObArray<ObSSTableWrapper> old_cg_wrappers;
  ASSERT_EQ(OB_SUCCESS, co_sstable->get_all_tables(old_cg_wrappers));
  ASSERT_EQ(column_group_cnt, old_cg_wrappers.count());
  for (int64_t i = 0; i < old_cg_wrappers.count(); ++i) {
    ObSSTable *cg = nullptr;
    ASSERT_EQ(OB_SUCCESS, old_cg_wrappers.at(i).get_loaded_column_store_sstable(cg));
    ASSERT_EQ(TEST_UPPER_TRANS_VERSION, cg->get_upper_trans_version());
  }

  // Call set_upper_trans_version via virtual dispatch
  ObArenaAllocator tmp_alloc("TestSetUTV", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ret = co_sstable->set_upper_trans_version(tmp_alloc, TEST_COMMIT_VERSION);
  ASSERT_EQ(OB_SUCCESS, ret);

  // After: CO base and all embedded CGs should have TEST_COMMIT_VERSION
  ASSERT_EQ(TEST_COMMIT_VERSION, co_sstable->get_upper_trans_version());
  ASSERT_TRUE(co_sstable->is_cs_valid());

  ObArray<ObSSTableWrapper> new_cg_wrappers;
  ASSERT_EQ(OB_SUCCESS, co_sstable->get_all_tables(new_cg_wrappers));
  ASSERT_EQ(column_group_cnt, new_cg_wrappers.count());
  for (int64_t i = 0; i < new_cg_wrappers.count(); ++i) {
    ObSSTable *cg = nullptr;
    ASSERT_EQ(OB_SUCCESS, new_cg_wrappers.at(i).get_loaded_column_store_sstable(cg));
    ASSERT_NE(nullptr, cg);
    ASSERT_EQ(TEST_COMMIT_VERSION, cg->get_upper_trans_version());
    LOG_INFO("check cg after set_upper_trans_version", K(i), KPC(cg));
  }

  LOG_INFO("test_co_set_upper_trans_version_updates_all_cgs passed", KPC(co_sstable));
}

// Test: set_upper_trans_version on row store sstable only updates itself
TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_row_store_set_upper_trans_version)
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = nullptr;

  ASSERT_EQ(OB_SUCCESS, mock_row_store_inc_major_sstable(TEST_UPPER_TRANS_VERSION, sstable));
  ASSERT_NE(nullptr, sstable);
  ASSERT_EQ(TEST_UPPER_TRANS_VERSION, sstable->get_upper_trans_version());

  ObArenaAllocator tmp_alloc("TestSetUTV", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ret = sstable->set_upper_trans_version(tmp_alloc, TEST_COMMIT_VERSION);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(TEST_COMMIT_VERSION, sstable->get_upper_trans_version());

  LOG_INFO("test_row_store_set_upper_trans_version passed", KPC(sstable));
}

// Test: virtual dispatch works through ObSSTable pointer
TEST_F(TestDirectLoadSSUpdateIncMajorTask, test_set_upper_trans_version_virtual_dispatch)
{
  int ret = OB_SUCCESS;
  const int64_t column_group_cnt = 3;
  ObCOSSTableV2 *co_sstable = nullptr;

  ASSERT_EQ(OB_SUCCESS, mock_co_inc_major_sstable(TEST_UPPER_TRANS_VERSION, column_group_cnt, co_sstable));
  ASSERT_NE(nullptr, co_sstable);

  // Call through ObSSTable* pointer to verify virtual dispatch
  ObSSTable *base_ptr = static_cast<ObSSTable *>(co_sstable);
  ObArenaAllocator tmp_alloc("TestSetUTV", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ret = base_ptr->set_upper_trans_version(tmp_alloc, TEST_COMMIT_VERSION);
  ASSERT_EQ(OB_SUCCESS, ret);

  // CO base updated
  ASSERT_EQ(TEST_COMMIT_VERSION, co_sstable->get_upper_trans_version());

  // Embedded CGs also updated
  const ObSSTableArray &cg_sstables = co_sstable->meta_->get_cg_sstables();
  for (int64_t i = 0; i < cg_sstables.count(); ++i) {
    ASSERT_NE(nullptr, cg_sstables[i]);
    ASSERT_EQ(TEST_COMMIT_VERSION, cg_sstables[i]->get_upper_trans_version());
  }

  LOG_INFO("test_set_upper_trans_version_virtual_dispatch passed", KPC(co_sstable));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_direct_load_ss_update_inc_major_task.log*");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_direct_load_ss_update_inc_major_task.log", true, false);
  logger.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else // OB_BUILD_SHARED_STORAGE

int main(int argc, char **argv)
{
  return 0;
}

#endif // OB_BUILD_SHARED_STORAGE