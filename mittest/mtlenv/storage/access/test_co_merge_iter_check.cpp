/**
 * Copyright (c) 2026 OceanBase
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
#include <string>

#define private public
#define protected public

#include "test_co_merge_iter_check.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/test_tablet_helper.h"
#include "storage/column_store/ob_co_merge_dag.h"
#include "storage/column_store/ob_column_oriented_merger.h"
#include "storage/column_store/ob_co_merge_writer.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/compaction/ob_partition_rows_merger.h"
#include "test_merge_basic.h"
#include "unittest/storage/test_schema_prepare.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace share;
using namespace blocksstable;
using namespace compaction;
using namespace memtable;
using namespace observer;
using namespace unittest;
using namespace transaction;

namespace storage
{

static ObArenaAllocator allocator_;
static ObLocalArena merger_allocator_("TestCOMergeIter", OB_MALLOC_NORMAL_BLOCK_SIZE);

// ======== helpers copied & trimmed from test_co_merge.cpp ========
static void close_builder_and_prepare_sstable(
    const blocksstable::ObDataStoreDesc &data_store_desc,
    const ObITable::TableKey &table_key,
    const ObTableSchema &table_schema,
    const int64_t column_group_cnt,
    ObSSTableIndexBuilder &index_builder,
    ObTableHandleV2 &table_handle,
    const bool is_all_cg_base = true,
    const bool is_co_table_without_cgs = false)
{
  ObSSTableMergeRes res;
  OK(index_builder.close(res));
  ObIndexTreeRootBlockDesc root_desc = res.root_desc_;
  ASSERT_TRUE(root_desc.is_valid());

  ObTabletCreateSSTableParam param;
  param.table_key_ = table_key;
  param.co_base_type_ = is_all_cg_base ? ObCOSSTableBaseType::ALL_CG_TYPE : ObCOSSTableBaseType::ROWKEY_CG_TYPE;
  param.rec_scn_.set_min();
  param.column_group_cnt_ = column_group_cnt;
  param.schema_version_ = ObMultiVersionSSTableTest::SCHEMA_VERSION;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = 0;
  param.progressive_merge_step_ = 0;
  param.table_mode_ = table_schema.get_table_mode_struct();
  param.index_type_ = table_schema.get_index_type();
  param.rowkey_column_cnt_ = data_store_desc.is_cg() ? 0
    : table_schema.get_rowkey_column_num() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();

  ObSSTableMergeRes::fill_addr_and_data(res.root_desc_, param.root_block_addr_, param.root_block_data_);
  ObSSTableMergeRes::fill_addr_and_data(res.data_root_desc_, param.data_block_macro_meta_addr_, param.data_block_macro_meta_);
  param.is_meta_root_ = res.data_root_desc_.is_meta_root_;
  param.root_row_store_type_ = res.root_row_store_type_;
  param.latest_row_store_type_ = res.root_row_store_type_;
  param.data_index_tree_height_ = res.root_desc_.height_;
  param.index_blocks_cnt_ = res.index_blocks_cnt_;
  param.data_blocks_cnt_ = res.data_blocks_cnt_;
  param.micro_block_cnt_ = res.micro_block_cnt_;
  param.use_old_macro_block_count_ = res.use_old_macro_block_count_;
  param.row_count_ = res.row_count_;
  param.column_cnt_ = res.data_column_cnt_;
  param.data_checksum_ = res.data_checksum_;
  param.occupy_size_ = res.occupy_size_;
  param.original_size_ = res.original_size_;
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.max_merged_trans_version_ = res.max_merged_trans_version_;
  param.contain_uncommitted_row_ = res.contain_uncommitted_row_;
  param.compressor_type_ = res.compressor_type_;
  param.encrypt_id_ = res.encrypt_id_;
  param.master_key_id_ = res.master_key_id_;
  param.nested_size_ = res.nested_size_;
  param.nested_offset_ = res.nested_offset_;
  ASSERT_EQ(OB_SUCCESS, param.data_block_ids_.assign(res.data_block_ids_));
  ASSERT_EQ(OB_SUCCESS, param.other_block_ids_.assign(res.other_block_ids_));
  param.table_backup_flag_.reset();
  param.table_shared_flag_.reset();
  param.tx_data_recycle_scn_.set_min();
  param.sstable_logic_seq_ = 0;
  param.recycle_version_ = 0;
  param.root_macro_seq_ = 0;
  param.full_column_cnt_ = table_schema.get_column_count()
      + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  param.is_co_table_without_cgs_ = is_co_table_without_cgs;
  param.co_base_snapshot_version_ = 0;
  param.ddl_scn_.set_min();
  param.filled_tx_scn_ = table_key.is_major_sstable() ? SCN::min_scn() : table_key.get_end_scn();

  if (is_major_merge_type(data_store_desc.get_merge_type())) {
    ASSERT_EQ(OB_SUCCESS, ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_));
  }
  if (data_store_desc.is_cg()) {
    OK(ObTabletCreateDeleteHelper::create_sstable(param, allocator_, table_handle));
  } else {
    OK(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(param, allocator_, table_handle));
  }
}

static int get_col_ids(const ObTableSchema &table_schema, ObIArray<ObColDesc> &col_ids)
{
  return table_schema.get_store_column_ids(col_ids);
}

class TestCOMergeIterType : public TestMergeBasic
                       , public ::testing::WithParamInterface<CaseParam>
{
public:
  TestCOMergeIterType() : TestMergeBasic("test_co_merge_iter_type") {}
  virtual ~TestCOMergeIterType() = default;

  static void SetUpTestCase()
  {
    ObMultiVersionSSTableTest::SetUpTestCase();
    ObClockGenerator::init();

    ObLSID ls_id(ls_id_);
    ObTabletID tablet_id(tablet_id_);
    ObLSHandle ls_handle;
    ObLSService *ls_svr = MTL(ObLSService*);
    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

    // create tablet
    obrpc::ObBatchCreateTabletArg create_tablet_arg;
    share::schema::ObTableSchema table_schema;
    ASSERT_EQ(OB_SUCCESS, gen_create_tablet_arg(tenant_id_, ls_id, tablet_id, create_tablet_arg, 1, &table_schema));
    ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_));

    ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
    ASSERT_TRUE(nullptr != mgr);
    share::ObFreezeInfoList &info_list = mgr->freeze_info_mgr_.freeze_info_;
    info_list.reset();
    share::SCN frozen_val;
    frozen_val.val_ = 1;
    ASSERT_EQ(OB_SUCCESS, info_list.frozen_statuses_.push_back(share::ObFreezeInfo(frozen_val, 1, 0)));
    info_list.latest_snapshot_gc_scn_.val_ = 2;
  }

  static void TearDownTestCase()
  {
    ObMultiVersionSSTableTest::TearDownTestCase();
    ObClockGenerator::destroy();
  }

  void SetUp() override
  {
    ObMultiVersionSSTableTest::SetUp();
    param_.compat_mode_ = lib::Worker::CompatMode::MYSQL;
  }
  void TearDown() override
  {
    ObMultiVersionSSTableTest::TearDown();
  }

  void add_all_and_each_column_group()
  {
    unittest::TestSchemaPrepare::add_all_and_each_column_group(allocator_, table_schema_);
  }
  void add_rowkey_and_each_column_group()
  {
    unittest::TestSchemaPrepare::add_rowkey_and_each_column_group(allocator_, table_schema_);
  }

  void prepare_data(
      const int64_t micro_row_cnt,
      const int64_t macro_row_cnt,
      const int64_t column_count,
      ObMockIterator &data_iter,
      ObMacroBlockWriter &macro_writer,
      ObCOMergeProjector *projector)
  {
    const ObStoreRow *row = nullptr;
    ObDatumRow datum_row;
    datum_row.init(allocator_, column_count);
    for (int64_t i = 0; i < data_iter.count(); i++) {
      OK(data_iter.get_row(i, row));
      ASSERT_TRUE(nullptr != row);
      datum_row.from_store_row(*row);
      if (nullptr != projector) {
        projector->project(datum_row);
      }
      const ObDatumRow &append_row = projector == nullptr ? datum_row : projector->get_project_row();
      ASSERT_EQ(OB_SUCCESS, macro_writer.append_row(append_row));
      if (macro_writer.micro_writer_->get_row_count() >= micro_row_cnt) {
        OK(macro_writer.build_micro_block());
      }
      if (macro_writer.macro_blocks_[macro_writer.current_index_].get_row_count() >= macro_row_cnt) {
        OK(macro_writer.try_switch_macro_block());
      }
    }
    OK(macro_writer.try_switch_macro_block());
  }

  void prepare_co_sstable(
      const ObTableSchema &table_schema,
      const ObMergeType &merge_type,
      const int64_t snapshot_version,
      ObMockIterator &data_iter,
      ObTableHandleV2 &co_table_handle,
      const ObScnRange &scn_range,
      const bool is_all_cg_base,
      const bool is_co_table_without_cgs = false)
  {
    ObStorageSchema storage_schema;
    common::ObArray<ObITable *> cg_tables;
    ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
    const common::ObIArray<ObStorageColumnGroupSchema> &cg_array = storage_schema.get_column_groups();

    for (int64_t i = 0; i < cg_array.count(); i++) {
      const ObStorageColumnGroupSchema &cg_schema = cg_array.at(i);
      if (is_co_table_without_cgs && !cg_schema.has_multi_version_column()) {
        // Build an "CO table without CGs": only create the base CO sstable, and skip all normal CG sstables.
        continue;
      }
      // For schema "ROWKEY_CG + each_cg" (no ALL_CG), we may still need to construct an "ALL_CG only" base major
      // (i.e. PURE_COL_ONLY_ALL). In that case, we must write full rows into the base table; otherwise using
      // rowkey cg schema would only write rowkey + mv columns.
      ObStorageColumnGroupSchema mocked_row_store_cg;
      const ObStorageColumnGroupSchema *use_cg_schema = &cg_schema;
      if (is_co_table_without_cgs
          && cg_schema.has_multi_version_column()
          && is_all_cg_base
          && !storage_schema.has_all_column_group()) {
        OK(storage_schema.mock_row_store_cg(mocked_row_store_cg));
        ASSERT_TRUE(mocked_row_store_cg.is_all_column_group());
        use_cg_schema = &mocked_row_store_cg;
      }
      ObCOMergeProjector projector;
      blocksstable::ObWholeDataStoreDesc data_store_desc;
      ObMacroBlockWriter macro_writer;
      ObSSTableIndexBuilder root_index_builder(false /*not need writer buffer*/);
      ObCOMergeProjector *row_project = nullptr;
      ObTableHandleV2 *table_handle = nullptr;

      OK(data_store_desc.init(false /*is_ddl*/, table_schema,
          ObLSID(ls_id_), ObTabletID(tablet_id_), merge_type, snapshot_version, DATA_CURRENT_VERSION,
          table_schema.get_micro_index_clustered(),
          0 /*tablet_transfer_seq*/, 0 /*concurrent_cnt*/,
          share::SCN::min_scn() /*reorganization_scn*/, share::SCN::invalid_scn() /*end_scn*/,
          use_cg_schema, i));
      ASSERT_EQ(OB_SUCCESS, root_index_builder.init(data_store_desc.get_desc()));
      data_store_desc.get_desc().sstable_index_builder_ = &root_index_builder;
      if (!use_cg_schema->is_all_column_group()) {
        OK(projector.init(*use_cg_schema, allocator_));
        row_project = &projector;
      }
      ObMacroSeqParam seq_param;
      seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
      seq_param.start_ = 0;
      ObPreWarmerParam pre_warm_param(MEM_PRE_WARM);
      ObSSTablePrivateObjectCleaner cleaner;
      OK(macro_writer.open(data_store_desc.get_desc(), 0 /*parallel_idx*/, seq_param /*start_seq*/, pre_warm_param, cleaner));
      // 6 rows with micro_row_cnt=1, macro_row_cnt=2 → 3 macro blocks (2 micros each)
      // This ensures the SSTable is NOT small (multiple macro blocks).
      prepare_data(1 /*micro_row_cnt*/, 2 /*macro_row_cnt*/, table_schema.get_column_count(), data_iter, macro_writer, row_project);
      OK(macro_writer.close());
      ASSERT_EQ(macro_writer.get_macro_block_write_ctx().get_macro_block_count(), 0);

      ObITable::TableKey table_key;
      table_key.tablet_id_ = ObTabletID(tablet_id_);
      table_key.table_type_ = cg_schema.has_multi_version_column()
        ? ObITable::COLUMN_ORIENTED_SSTABLE
        : ObITable::NORMAL_COLUMN_GROUP_SSTABLE;
      table_key.scn_range_ = scn_range;
      if (cg_schema.has_multi_version_column()) {
        table_key.column_group_idx_ = i;
      }
      ASSERT_NE(nullptr, table_handle = OB_NEWx(ObTableHandleV2, (&allocator_)));
      const int64_t cg_cnt = cg_schema.has_multi_version_column() ? cg_array.count() : 1;
      close_builder_and_prepare_sstable(data_store_desc.get_desc(), table_key, table_schema, cg_cnt,
          root_index_builder, *table_handle, is_all_cg_base,
          is_co_table_without_cgs && cg_schema.has_multi_version_column() /*is_co_table_without_cgs*/);
      if (cg_schema.has_multi_version_column()) {
        co_table_handle.set_sstable(table_handle->get_table(), &allocator_);
      } else {
        OK(cg_tables.push_back(table_handle->get_table()));
      }
    }

    ASSERT_NE(nullptr, co_table_handle.get_table());
    if (!is_co_table_without_cgs) {
      ASSERT_EQ(OB_SUCCESS, static_cast<ObCOSSTableV2 *>(co_table_handle.get_table())->fill_cg_sstables(cg_tables));
    } else {
      // In this mode, CO sstable is created with param.is_co_table_without_cgs_ = true,
      // and fill_cg_sstables() must not be called (it will return OB_STATE_NOT_MATCH).
      ASSERT_TRUE(static_cast<ObCOSSTableV2 *>(co_table_handle.get_table())->is_cgs_empty_co_table());
    }
  }

  void prepare_merge_context(
      const CaseParam &p,
      const ObMergeType &merge_type,
      const ObVersionRange &trans_version_range,
      ObCOTabletMergeCtx &merge_context)
  {
    int32_t base_cg_idx = -1;
    TestMergeBasic::prepare_merge_context(merge_type, false /*is_full_merge*/, trans_version_range, merge_context);
    merge_context.static_param_.data_version_ = p.data_version_;
    merge_context.static_param_.dag_param_.merge_version_ = trans_version_range.snapshot_version_;

    ASSERT_EQ(OB_SUCCESS, merge_context.init_major_sstable_status());
    // decide/set status + merge_type (must be before cal_merge_param which needs co_major_merge_type_)
    ObITable *base_major_table = merge_context.static_param_.tables_handle_.get_table(0);
    ASSERT_NE(nullptr, base_major_table);
    ObMajorSSTableStatus decided_status = merge_context.static_param_.major_merge_sstable_status_array_.at(0).co_major_sstable_status_;
    if (base_major_table->is_co_sstable()) {

      ObSEArray<ObITable *, 8> tables;
      for (int64_t i = 0; i < merge_context.static_param_.tables_handle_.get_count(); ++i) {
        ObITable *t = merge_context.static_param_.tables_handle_.get_table(i);
        if (nullptr != t) { OK(tables.push_back(t)); }
      }
      ObCOMajorMergeStrategy merge_strategy;
      ASSERT_EQ(OB_SUCCESS, ObCOMajorMergePolicy::decide_merge_strategy(
          decided_status,
          *merge_context.get_schema(),
          tables,
          merge_strategy));
      merge_context.static_param_.co_static_param_.co_major_merge_strategy_ = merge_strategy;
    } else {
      // row major under column schema => delayed transform
      merge_context.static_param_.co_static_param_.co_major_merge_strategy_.set(false/*build_all_cg_only*/, true/*only_use_row_store*/);
    }

    merge_context.need_replay_base_directly_ = 1; // TODO: support replay_base_directly=false

    ASSERT_EQ(OB_SUCCESS, merge_context.cal_merge_param());
    ASSERT_EQ(OB_SUCCESS, merge_context.init_parallel_merge_ctx());
    ASSERT_EQ(OB_SUCCESS, merge_context.static_param_.init_static_info(merge_context.tablet_handle_));
    ASSERT_EQ(OB_SUCCESS, merge_context.init_static_desc());
    ASSERT_EQ(OB_SUCCESS, merge_context.init_read_info());
    ASSERT_EQ(OB_SUCCESS, merge_context.init_tablet_merge_info());
    ASSERT_EQ(OB_SUCCESS, merge_context.get_schema()->get_base_rowkey_column_group_index(base_cg_idx));
    merge_context.base_rowkey_cg_idx_ = base_cg_idx;
    ObCOMergeDagParam *dag_param = static_cast<ObCOMergeDagParam *>(&merge_context.static_param_.dag_param_);
    dag_param->compat_mode_ = lib::Worker::CompatMode::MYSQL;

    if (merge_context.should_mock_row_store_cg_schema()) {
      ASSERT_EQ(OB_SUCCESS, merge_context.prepare_mocked_row_store_cg_schema());
    }
    if (merge_context.is_build_all_cg_from_each_cg()) {
      ASSERT_EQ(OB_SUCCESS, merge_context.mock_row_store_table_read_info());
    }
    ASSERT_EQ(OB_SUCCESS, merge_context.check_prefer_reuse_macro_block());
  }

  static void force_row_count(blocksstable::ObSSTable &sstable, const int64_t row_cnt)
  {
    sstable.meta_cache_.row_count_ = row_cnt;
  }

  // Assert that the concrete type of `iter` matches the expected `IterKind`.
  // Uses dynamic_cast to distinguish:
  //   ROW   = ObPartitionRowMergeIter  (but NOT Macro/Micro)
  //   MACRO = ObPartitionMacroMergeIter (but NOT Micro)
  //   MICRO = ObPartitionMicroMergeIter
  static void assert_iter_kind(const IterKind expected, ObMergeIter *iter)
  {
    ASSERT_NE(nullptr, iter) << "iter is null";
    switch (expected) {
      case IterKind::ROW: {
        ASSERT_NE(nullptr, dynamic_cast<ObPartitionRowMergeIter *>(iter))
            << "expected ObPartitionRowMergeIter, got other type";
        ASSERT_EQ(nullptr, dynamic_cast<ObPartitionMacroMergeIter *>(iter))
            << "expected ROW iter but got MACRO/MICRO iter";
        break;
      }
      case IterKind::MACRO: {
        ASSERT_NE(nullptr, dynamic_cast<ObPartitionMacroMergeIter *>(iter))
            << "expected ObPartitionMacroMergeIter, got other type";
        ASSERT_EQ(nullptr, dynamic_cast<ObPartitionMicroMergeIter *>(iter))
            << "expected MACRO iter but got MICRO iter";
        break;
      }
      case IterKind::MICRO: {
        ASSERT_NE(nullptr, dynamic_cast<ObPartitionMicroMergeIter *>(iter))
            << "expected ObPartitionMicroMergeIter, got other type";
        break;
      }
      default:
        FAIL() << "unknown IterKind value: " << static_cast<int>(expected);
    }
  }

public:
  ObCOMergeDagParam param_;
  ObCOMergeDagNet dag_net_;
};

TEST_P(TestCOMergeIterType, test_co_merge_major_iter_type)
{
  const CaseParam &p = GetParam();

  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var1  -8       0        1        1    T_DML_INSERT  EXIST   CLF\n"
      "1        var1  -8       0        1        1    T_DML_INSERT  EXIST   CLF\n"
      "2        var1  -8       0        1        1    T_DML_INSERT  EXIST   CLF\n"
      "3        var1  -8       0        1        1    T_DML_INSERT  EXIST   CLF\n"
      "4        var1  -8       0        1        1    T_DML_INSERT  EXIST   CLF\n"
      "5        var1  -8       0        1        1    T_DML_INSERT  EXIST   CLF\n";

  const int schema_rowkey_cnt = 2;
  const int64_t snapshot_version = 8;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);

  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  if (p.schema_has_all_cg()) {
    add_all_and_each_column_group();
  } else {
    add_rowkey_and_each_column_group();
  }
  init_tablet();
  // Fix up table_store_cache_ so that it matches the real schema built from micro_data.
  // SetUpTestCase creates the tablet with build_test_schema which has only 1 user column
  // (stored count = 3), but the test schema has 4 user columns (stored count = 6).
  // Also align row_store_type and compressor_type to avoid spurious schema-change detection.
  {
    ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(ObLSID(ls_id_), ls_handle, ObLSGetMod::STORAGE_MOD));
    ObTabletHandle tablet_handle;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(ObTabletID(tablet_id_), tablet_handle));
    ObTableStoreCache &cache = tablet_handle.get_obj()->table_store_cache_;
    cache.last_major_column_count_ = 6;
    cache.last_major_latest_row_store_type_ = table_schema_.get_row_store_type();
    cache.last_major_compressor_type_ = table_schema_.get_compressor_type();
  }

  // base major table
  ObTableHandleV2 base_major_handle;
  ObITable *base_major_table = nullptr;
  ObCOSSTableV2 *base_major_co = nullptr;

  if (p.base_major_kind_ == BaseMajorKind::ROW_MAJOR) {
    ObMockIterator data_iter;
    OK(data_iter.from(micro_data[0]));
    reset_writer(snapshot_version);
    // 6 rows with micro_row_cnt=1, macro_row_cnt=2 → 3 macro blocks (2 micros each)
    prepare_data(1 /*micro_row_cnt*/, 2 /*macro_row_cnt*/,
        table_schema_.get_column_count(), data_iter, macro_writer_, nullptr);
    prepare_data_end(base_major_handle, ObITable::MAJOR_SSTABLE);
    base_major_table = base_major_handle.get_table();
    ASSERT_NE(nullptr, base_major_table);
    ASSERT_FALSE(base_major_table->is_co_sstable());
  } else {
    ObMockIterator data_iter;
    OK(data_iter.from(micro_data[0]));
    prepare_co_sstable(table_schema_, MAJOR_MERGE, snapshot_version,
        data_iter, base_major_handle, scn_range, p.is_all_cg_base(), p.is_co_table_without_cgs());
    base_major_table = base_major_handle.get_table();
    ASSERT_NE(nullptr, base_major_table);
    ASSERT_TRUE(base_major_table->is_co_sstable());
    base_major_co = static_cast<ObCOSSTableV2 *>(base_major_table);
    ASSERT_NE(nullptr, base_major_co);

    // enforce shape
    if (p.base_major_kind_ == BaseMajorKind::CO_ROW_STORE_ONLY) {
      ASSERT_TRUE(base_major_co->is_all_cg_base());
      ASSERT_TRUE(base_major_co->is_cgs_empty_co_table());
    } else if (p.base_major_kind_ == BaseMajorKind::CO_ROWKEY_CG_BASE) {
      ASSERT_TRUE(base_major_co->is_rowkey_cg_base());
      ASSERT_FALSE(base_major_co->is_cgs_empty_co_table());
    } else {
      ASSERT_TRUE(base_major_co->is_all_cg_base());
      ASSERT_FALSE(base_major_co->is_cgs_empty_co_table());
    }
  }

  // With 3+ macro blocks the SSTable should naturally NOT be small
  ASSERT_TRUE(base_major_table->is_sstable());
  ASSERT_FALSE(static_cast<ObSSTable *>(base_major_table)->is_small_sstable());

  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);
  merge_context.static_param_.tables_handle_.add_table(base_major_handle);

  // drive physical row cnt for policy
  force_row_count(*static_cast<ObSSTable *>(base_major_table), p.major_row_cnt_);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  trans_version_range.base_version_ = 7;

  prepare_merge_context(p, MAJOR_MERGE, trans_version_range, merge_context);

  int64_t start_cg_idx = 0;
  int64_t end_cg_idx = 2; // test 2 cgs only
  if (merge_context.is_build_all_cg_only()) {
    start_cg_idx = merge_context.base_rowkey_cg_idx_;
    end_cg_idx = start_cg_idx + 1;
  }

  ASSERT_EQ(OB_SUCCESS, merge_context.prepare_index_builder(start_cg_idx, end_cg_idx));
  ObCOMergeLogReplayer replayer(merger_allocator_, merge_context.static_param_,
    start_cg_idx, end_cg_idx, merge_context.prefer_reuse_macro_block_ ? false : merge_context.only_use_row_store());
  ASSERT_EQ(OB_SUCCESS, replayer.init(merge_context, 0 /*task_id*/));
  ASSERT_FALSE(replayer.merge_writers_.empty());

  // ===== Check builder's major iters via expected_major_iter_kinds_ =====
  if (!p.expected_major_iter_kinds_.empty()) {
    ASSERT_NE(nullptr, replayer.mergelog_iter_);
    common::ObSEArray<ObPartitionMergeIter *, 8> builder_iters;
    ASSERT_EQ(OB_SUCCESS, replayer.mergelog_iter_->get_major_sstable_merge_iters_for_check(builder_iters));
    ASSERT_EQ(static_cast<int64_t>(p.expected_major_iter_kinds_.size()), builder_iters.count())
        << "builder major iter count mismatch";
    for (int64_t i = 0; i < builder_iters.count(); ++i) {
      assert_iter_kind(p.expected_major_iter_kinds_[i], builder_iters.at(i));
    }
  }

  // ===== Check each writer's iters via expected_writer_iter_kinds_ =====
  if (!p.expected_writer_iter_kinds_.empty()) {
    ASSERT_EQ(static_cast<int64_t>(p.expected_writer_iter_kinds_.size()), replayer.merge_writers_.count())
        << "writer count mismatch";
    for (int64_t wi = 0; wi < replayer.merge_writers_.count(); ++wi) {
      const std::vector<IterKind> &expected_iters = p.expected_writer_iter_kinds_[wi];
      ObCOMergeWriter *writer = replayer.merge_writers_.at(wi);
      if (expected_iters.empty()) {
        ASSERT_EQ(nullptr, writer) << "writer " << wi << " is not null but expected iters are empty";
        // Allow null/skipped writer when no expected iters (e.g., base_cg writer)
        continue;
      }
      ASSERT_NE(nullptr, writer) << "writer " << wi << " is null but expected iters";
      ASSERT_EQ(static_cast<int64_t>(expected_iters.size()), writer->iters_.count())
          << "writer " << wi << " iter count mismatch";
      for (int64_t ii = 0; ii < writer->iters_.count(); ++ii) {
        ObCOMajorMergeIter *merge_iter = writer->iters_.at(ii);
        ASSERT_NE(nullptr, merge_iter) << "writer " << wi << " iter " << ii << " is null";
        assert_iter_kind(expected_iters[ii], merge_iter->iter_);
      }
    }
  }

  replayer.reset();
}

INSTANTIATE_TEST_CASE_P(
    COMergeIterTypeSuite,
    TestCOMergeIterType,
    ::testing::ValuesIn(get_all_co_merge_iter_type_configs())
);

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_co_merge_iter_check.log*");
  OB_LOGGER.set_file_name("test_co_merge_iter_check.log");
  OB_LOGGER.set_log_level("DEBUG");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}