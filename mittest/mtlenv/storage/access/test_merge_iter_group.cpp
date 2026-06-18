/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#define UNITTEST
#include "storage/compaction/ob_partition_merger.h"
#include "storage/compaction/ob_partition_merge_iter_group.h"
#include "storage/ob_storage_schema.h"
#include "storage/test_tablet_helper.h"
#include "mtlenv/storage/access/test_merge_basic.h"
#include "share/schema/ob_schema_utils.h"
#include "unittest/storage/test_schema_prepare.h"
#include "storage/column_store/ob_co_merge_dag.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace compaction;
using namespace unittest;

namespace storage
{

// ---------------------------------------------------------------------------
// Helper: close an SSTableIndexBuilder and create a CO SSTable or plain SSTable.
// ---------------------------------------------------------------------------
static void close_and_make_sstable(
    const blocksstable::ObDataStoreDesc &desc,
    const ObITable::TableKey        &table_key,
    const ObTableSchema             &table_schema,
    const int64_t                    column_group_cnt,
    ObSSTableIndexBuilder           &index_builder,
    ObTableHandleV2                 &handle,
    ObArenaAllocator                &allocator,
    bool                             is_co_base = false)
{
  ObSSTableMergeRes res;
  OK(index_builder.close(res));

  ObTabletCreateSSTableParam param;
  param.set_init_value_for_column_store_();
  param.table_key_            = table_key;
  param.co_base_type_         = is_co_base ? ObCOSSTableBaseType::ROWKEY_CG_TYPE
                                           : ObCOSSTableBaseType::INVALID_TYPE;
  param.rec_scn_.set_min();
  param.column_group_cnt_     = column_group_cnt;
  param.schema_version_       = ObMultiVersionSSTableTest::SCHEMA_VERSION;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = 0;
  param.progressive_merge_step_  = 0;
  param.table_mode_           = table_schema.get_table_mode_struct();
  param.index_type_           = table_schema.get_index_type();
  param.rowkey_column_cnt_    = desc.is_cg()
      ? 0
      : (table_schema.get_rowkey_column_num()
         + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt());

  OK(ObSSTableMergeRes::fill_addr_and_data(res.root_desc_,
      param.root_block_addr_, param.root_block_data_));
  OK(ObSSTableMergeRes::fill_addr_and_data(res.data_root_desc_,
      param.data_block_macro_meta_addr_, param.data_block_macro_meta_));
  param.is_meta_root_            = res.data_root_desc_.is_meta_root_;
  param.root_row_store_type_     = res.root_row_store_type_;
  param.latest_row_store_type_   = res.root_row_store_type_;
  param.data_index_tree_height_  = res.root_desc_.height_;
  param.index_blocks_cnt_        = res.index_blocks_cnt_;
  param.data_blocks_cnt_         = res.data_blocks_cnt_;
  param.micro_block_cnt_         = res.micro_block_cnt_;
  param.use_old_macro_block_count_ = res.use_old_macro_block_count_;
  param.row_count_               = res.row_count_;
  param.column_cnt_              = res.data_column_cnt_;
  param.data_checksum_           = res.data_checksum_;
  param.occupy_size_             = res.occupy_size_;
  param.original_size_           = res.original_size_;
  param.compressor_type_         = ObCompressorType::NONE_COMPRESSOR;
  param.max_merged_trans_version_ = res.max_merged_trans_version_;
  param.contain_uncommitted_row_ = res.contain_uncommitted_row_;
  param.nested_size_             = res.nested_size_;
  param.nested_offset_           = res.nested_offset_;
  param.table_backup_flag_.reset();
  param.table_shared_flag_.reset();
  param.tx_data_recycle_scn_.set_min();
  param.sstable_logic_seq_       = 0;
  param.recycle_version_         = 0;
  param.root_macro_seq_          = 0;
  param.ddl_scn_.set_min();
  param.filled_tx_scn_ = table_key.is_major_sstable() ? SCN::min_scn()
                                                        : table_key.get_end_scn();
  OK(param.data_block_ids_.assign(res.data_block_ids_));
  OK(param.other_block_ids_.assign(res.other_block_ids_));

  if (compaction::is_major_merge_type(desc.get_merge_type())) {
    OK(ObSSTableMergeRes::fill_column_checksum_for_empty_major(
        param.column_cnt_, param.column_checksums_));
  }

  if (is_co_base) {
    OK(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(param, allocator, handle));
  } else {
    OK(ObTabletCreateDeleteHelper::create_sstable(param, allocator, handle));
  }
}

// ---------------------------------------------------------------------------
// TestMergeIterGroup
// ---------------------------------------------------------------------------
class TestMergeIterGroup : public TestMergeBasic
{
public:
  TestMergeIterGroup()
    : TestMergeBasic("test_merge_iter_group"),
      local_arena_("TestMIGArena", OB_MALLOC_NORMAL_BLOCK_SIZE),
      schema_rowkey_cnt_(1),
      row_id_seed_(1)
  {}
  virtual ~TestMergeIterGroup() {}

  static void SetUpTestCase()
  {
    ObMultiVersionSSTableTest::SetUpTestCase();
    ObClockGenerator::init();
    TestMergeBasic::create_tablet();
  }

  static void TearDownTestCase()
  {
    ObMultiVersionSSTableTest::TearDownTestCase();
    ObClockGenerator::destroy();
  }

  void SetUp() override { ObMultiVersionSSTableTest::SetUp(); row_id_seed_ = 1; }
  void TearDown() override { ObMultiVersionSSTableTest::TearDown(); }

  void fill_merge_context(
      const ObVersionRange &version_range,
      ObTabletMajorMergeCtx &ctx)
  {
    TestMergeBasic::prepare_merge_context(
        MAJOR_MERGE,
        false,
        version_range,
        &merge_dag_,
        ctx,
        ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  }
  void prepare_ttl_filter(
    const int64_t filter_max_version,
    ObBasicTabletMergeCtx &merge_context)
  {
    const int64_t filter_col = schema_rowkey_cnt_ + 2;
    ASSERT_EQ(OB_SUCCESS, TestMergeBasic::prepare_ttl_filter(filter_max_version, filter_col, schema_rowkey_cnt_, merge_context));
    ASSERT_NE(nullptr, merge_context.filter_ctx_.compaction_filter_);
    merge_context.filter_ctx_.filter_col_idxs_.init_for_unittest(filter_col, 2);
  }

  // -----------------------------------------------------------------------
  // Generate row data string for single value column schema.
  // Format: "pk  trans_version  sql_seq  value  flag  multi_version_row_flag"
  // -----------------------------------------------------------------------
  std::string generate_single_col_data(const int64_t row_cnt,
                                       const int64_t snapshot_version = 5)
  {
    std::stringstream ss;
    ss << "bigint   bigint   bigint   bigint   flag    multi_version_row_flag\n";
    for (int64_t i = 0; i < row_cnt; ++i) {
      int64_t pk = row_id_seed_++;
      ss << pk << "  -" << snapshot_version << "  0  " << (pk * 100) << "  EXIST  CLF\n";
    }
    return ss.str();
  }

  // -----------------------------------------------------------------------
  // Generate row data string for two value columns schema.
  // Format: "pk  trans_version  sql_seq  v1  v2  flag  multi_version_row_flag"
  // -----------------------------------------------------------------------
  std::string generate_two_col_data(const int64_t row_cnt,
                                    const int64_t snapshot_version = 5)
  {
    std::stringstream ss;
    ss << "bigint   bigint   bigint   bigint   bigint   flag    multi_version_row_flag\n";
    for (int64_t i = 0; i < row_cnt; ++i) {
      int64_t pk = row_id_seed_++;
      ss << pk << "  -" << snapshot_version << "  0  " << (pk * 10) << "  " << (pk * 100) << "  EXIST  CLF\n";
    }
    return ss.str();
  }

  // -----------------------------------------------------------------------
  // Prepare schema, CO SSTable, merge context, and merger.
  // Returns the merger and merge context ready for creating iter groups.
  // -----------------------------------------------------------------------
  void setup_co_env(
      const char *row_data_str,
      const int64_t schema_rowkey_cnt,
      const int64_t snapshot_version,
      const int64_t *micro_row_cnt,
      const int64_t *macro_row_cnt,
      ObTabletMajorMergeCtx &merge_ctx,
      ObPartitionMajorMerger &merger,
      ObTableHandleV2 &co_handle,
      ObIArray<ObTableHandleV2> &cg_handle_arr)
  {
    ObScnRange scn_range;
    scn_range.start_scn_.set_min();
    scn_range.end_scn_.convert_for_tx(snapshot_version);

    const char *tmp_data[1] = { row_data_str };
    prepare_table_schema(tmp_data, schema_rowkey_cnt, scn_range, snapshot_version);
    unittest::TestSchemaPrepare::add_rowkey_and_each_column_group(allocator_, table_schema_);

    ObMockIterator data_iter;
    OK(data_iter.from(row_data_str));

    const int64_t cg_cnt = table_schema_.get_column_group_count();
    ObSEArray<int64_t, 8> micro_arr, macro_arr;
    for (int64_t i = 0; i < cg_cnt; ++i) {
      OK(micro_arr.push_back(micro_row_cnt[i]));
      OK(macro_arr.push_back(macro_row_cnt[i]));
    }

    prepare_pure_co_sstable(scn_range, snapshot_version, data_iter,
                            micro_arr.get_data(), macro_arr.get_data(),
                            co_handle, cg_handle_arr);

    merge_ctx.static_param_.tables_handle_.add_table(co_handle);

    ObVersionRange version_range;
    version_range.snapshot_version_   = snapshot_version;
    version_range.multi_version_start_ = 1;
    version_range.base_version_        = 0;
    fill_merge_context(version_range, merge_ctx);
    prepare_ttl_filter(10, merge_ctx);
    ASSERT_EQ(OB_SUCCESS, merger.prepare_merge(merge_ctx, 0));
    ASSERT_NE(nullptr, merger.merge_helper_);
  }

  // -----------------------------------------------------------------------
  // Build an ObRowkeyReadInfo for the rowkey CG (pk + trans_version + sql_seq).
  //
  // The rowkey CG SSTable only stores rowkey columns (including multi-version
  // extra columns). In production the CO merge uses a CG-specific read_info
  // with schema_column_count = schema_rowkey_cnt (just pk). ObRowkeyReadInfo
  // automatically adds the 2 extra multi-version columns, yielding a 3-column
  // read_info that matches the rowkey CG exactly.
  // -----------------------------------------------------------------------
  void build_rowkey_cg_read_info(
      const ObMergeParameter &merge_param,
      ObRowkeyReadInfo &rowkey_read_info)
  {
    const ObIArray<ObColDesc> &all_descs = merge_param.static_param_.multi_version_column_descs_;
    // Only the original rowkey column(s) — trans_version and sql_seq are added by init.
    ObSEArray<ObColDesc, 4> rowkey_col_descs;
    for (int64_t i = 0; i < schema_rowkey_cnt_; ++i) {
      OK(rowkey_col_descs.push_back(all_descs.at(i)));
    }
    OK(rowkey_read_info.init(
        allocator_,
        schema_rowkey_cnt_,   // schema_column_count: only pk (trans/sql auto-added)
        schema_rowkey_cnt_,   // schema_rowkey_cnt
        false,                // is_oracle_mode
        rowkey_col_descs));   // rowkey_col_descs: [pk]
  }

  // Cleanup handles and merger.
  void cleanup_co_env(
      ObPartitionMajorMerger &merger,
      ObTableHandleV2 &co_handle,
      ObIArray<ObTableHandleV2> &cg_handle_arr)
  {
    co_handle.reset();
    for (int64_t i = 0; i < cg_handle_arr.count(); ++i) {
      cg_handle_arr.at(i).reset();
    }
    merger.reset();
  }

  // Prepare a pure-column-store CO SSTable.
  void prepare_pure_co_sstable(
      const ObScnRange    &scn_range,
      const int64_t        snapshot_version,
      ObMockIterator      &data_iter,
      const int64_t       *micro_row_cnt,
      const int64_t       *macro_row_cnt,
      ObTableHandleV2     &co_handle,
      ObIArray<ObTableHandleV2> &cg_handles)
  {
    ObStorageSchema storage_schema;
    OK(storage_schema.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
    common::ObArray<ObITable *> cg_tables;
    ObStorageCGSchemaIterator cg_iter(storage_schema);
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      const ObStorageColumnGroupSchema *cg_schema = nullptr;
      int64_t cg_idx = -1;
      int64_t iter_idx = -1;
      if (OB_FAIL(cg_iter.next(cg_schema, cg_idx, iter_idx))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get next cg schema", K(ret));
        }
        break;
      }

      ObWholeDataStoreDesc whole_desc;
      OK(whole_desc.init(
          false /*is_ddl*/, table_schema_,
          ObLSID(ls_id_), ObTabletID(tablet_id_),
          MAJOR_MERGE, snapshot_version, DATA_CURRENT_VERSION,
          table_schema_.get_micro_index_clustered(),
          0 /*transfer_seq*/, 0 /*concurrent_cnt*/,
          SCN::min_scn() /*reorganization_scn*/,
          SCN::invalid_scn() /*end_scn*/,
          cg_schema, cg_idx));

      ObSSTableIndexBuilder idx_builder(false);
      OK(idx_builder.init(whole_desc.get_desc()));
      whole_desc.get_desc().sstable_index_builder_ = &idx_builder;

      ObMacroBlockWriter cg_writer;
      ObMacroSeqParam seq_param;
      seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
      seq_param.start_    = 0;
      ObPreWarmerParam pre_warm(MEM_PRE_WARM);
      ObSSTablePrivateObjectCleaner cleaner;
      OK(cg_writer.open(whole_desc.get_desc(), 0, seq_param, pre_warm, cleaner));

      ObCOMergeProjector projector;
      ObCOMergeProjector *proj_ptr = nullptr;
      if (!cg_schema->is_all_column_group()) {
        OK(projector.init(*cg_schema, allocator_));
        proj_ptr = &projector;
      }

      ObDatumRow datum_row;
      datum_row.init(allocator_, table_schema_.get_column_count());
      const ObStoreRow *row = nullptr;
      for (int64_t i = 0; i < data_iter.count(); ++i) {
        OK(data_iter.get_row(i, row));
        ASSERT_NE(nullptr, row);
        datum_row.from_store_row(*row);
        if (proj_ptr != nullptr) {
          proj_ptr->project(datum_row);
        }
        const ObDatumRow &append_row = (proj_ptr == nullptr)
            ? datum_row : proj_ptr->get_project_row();
        OK(cg_writer.append_row(append_row));

        if (cg_writer.micro_writer_->get_row_count() >= micro_row_cnt[iter_idx]) {
          OK(cg_writer.build_micro_block());
        }
        if (cg_writer.macro_blocks_[cg_writer.current_index_].get_row_count()
            >= macro_row_cnt[iter_idx]) {
          OK(cg_writer.try_switch_macro_block());
        }
      }
      OK(cg_writer.try_switch_macro_block());
      OK(cg_writer.close());

      ObITable::TableKey tkey;
      tkey.tablet_id_  = ObTabletID(tablet_id_);
      tkey.table_type_ = cg_schema->has_multi_version_column()
          ? ObITable::COLUMN_ORIENTED_SSTABLE
          : ObITable::NORMAL_COLUMN_GROUP_SSTABLE;
      tkey.scn_range_  = scn_range;
      if (cg_schema->has_multi_version_column()) {
        tkey.column_group_idx_ = cg_idx;
      }

      const int64_t cg_cnt_param = cg_schema->has_multi_version_column() ? cg_iter.get_column_group_count() : 1;
      const bool    is_co_base   = cg_schema->has_multi_version_column();

      ObTableHandleV2 tmp_hdl;

      close_and_make_sstable(whole_desc.get_desc(), tkey, table_schema_,
                             cg_cnt_param, idx_builder, tmp_hdl,
                             static_cast<ObArenaAllocator &>(allocator_),
                             is_co_base);

      if (is_co_base) {
        co_handle.set_sstable(tmp_hdl.get_table(), &allocator_);
      } else {
        OK(cg_handles.push_back(tmp_hdl));
        OK(cg_tables.push_back(tmp_hdl.get_table()));
      }
    }

    ASSERT_EQ(OB_SUCCESS,
        static_cast<ObCOSSTableV2 *>(co_handle.get_table())->fill_cg_sstables(cg_tables));
  }

public:
  compaction::ObLocalArena local_arena_;
  ObTabletMergeExecuteDag merge_dag_;
  int64_t schema_rowkey_cnt_;
  int64_t row_id_seed_;
};

// ===========================================================================
// Test 1: basic row alignment with small data (original test, kept for smoke)
// ===========================================================================
TEST_F(TestMergeIterGroup, test_basic_row_alignment)
{
  const int64_t snapshot_version = 10;
  const int64_t row_cnt = 4;

  std::string row_data = generate_single_col_data(row_cnt, snapshot_version);

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_ctx(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_ctx.static_param_);

  // All rows in one block per CG.
  const int64_t micro_limits[] = {INT64_MAX, INT64_MAX, INT64_MAX};
  const int64_t macro_limits[] = {INT64_MAX, INT64_MAX, INT64_MAX};

  ObTableHandleV2 co_handle;
  ObSEArray<ObTableHandleV2, 4> cg_handle_arr;
  setup_co_env(row_data.c_str(), 1, snapshot_version,
               micro_limits, macro_limits,
               merge_ctx, merger, co_handle, cg_handle_arr);

  ObPartitionMergeIterGroup<ObPartitionRowMergeIter> *group =
      OB_NEWx(ObPartitionMergeIterGroup<ObPartitionRowMergeIter>, (&allocator_), allocator_, merger.filter_handle_);
  ASSERT_NE(nullptr, group);
  ObRowkeyReadInfo rowkey_cg_read_info;
  build_rowkey_cg_read_info(merger.merge_param_, rowkey_cg_read_info);
  ASSERT_EQ(OB_SUCCESS, group->init(merger.merge_param_, 0, &rowkey_cg_read_info));

  ObPartitionMergeIter *col_iter = group->iter_group_items_.at(0).iter_;
  ASSERT_NE(nullptr, col_iter);

  for (int64_t i = 0; i < row_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, group->next());
    const ObDatumRow *check_row = group->get_filter_check_row();
    ASSERT_NE(nullptr, check_row);
    ASSERT_EQ(group->iter_row_id_, col_iter->iter_row_id_)
        << "Row " << i << ": group_row_id=" << group->iter_row_id_
        << " col_row_id=" << col_iter->iter_row_id_;
  }
  ASSERT_EQ(OB_ITER_END, group->next());

  group->reset();
  allocator_.free(group);
  cleanup_co_env(merger, co_handle, cg_handle_arr);
}

// ===========================================================================
// Test 2: column CG iter catchup after primary iter advances ahead
// ===========================================================================
TEST_F(TestMergeIterGroup, test_group_item_catchup)
{
  const int64_t snapshot_version = 10;
  const int64_t row_cnt = 5;

  std::string row_data = generate_single_col_data(row_cnt, snapshot_version);

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_ctx(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_ctx.static_param_);

  const int64_t micro_limits[] = {INT64_MAX, INT64_MAX, INT64_MAX};
  const int64_t macro_limits[] = {INT64_MAX, INT64_MAX, INT64_MAX};

  ObTableHandleV2 co_handle;
  ObSEArray<ObTableHandleV2, 4> cg_handle_arr;
  setup_co_env(row_data.c_str(), 1, snapshot_version,
               micro_limits, macro_limits,
               merge_ctx, merger, co_handle, cg_handle_arr);

  ObPartitionMergeIterGroup<ObPartitionRowMergeIter> *group =
      OB_NEWx(ObPartitionMergeIterGroup<ObPartitionRowMergeIter>, (&allocator_), allocator_, merger.filter_handle_);
  ASSERT_NE(nullptr, group);
  ObRowkeyReadInfo rowkey_cg_read_info;
  build_rowkey_cg_read_info(merger.merge_param_, rowkey_cg_read_info);
  ASSERT_EQ(OB_SUCCESS, group->init(merger.merge_param_, 0, &rowkey_cg_read_info));
  ASSERT_EQ(1, group->get_group_item_count());
  ObPartitionMergeIter *col_iter = group->iter_group_items_.at(0).iter_;
  ASSERT_NE(nullptr, col_iter);

  // Prime both to row 0.
  ASSERT_EQ(OB_SUCCESS, group->next());
  const ObDatumRow *check_row = group->get_filter_check_row();
  ASSERT_NE(nullptr, check_row);
  ASSERT_EQ(group->iter_row_id_, col_iter->iter_row_id_);

  // Advance primary iter TWO more steps (rows 1, 2) without checking col_iter.
  ASSERT_EQ(OB_SUCCESS, group->next());
  ASSERT_EQ(OB_SUCCESS, group->next());

  // Now get_filter_check_row should drive col_iter to catch up.
  ASSERT_EQ(OB_SUCCESS, group->next());
  check_row = group->get_filter_check_row();
  ASSERT_NE(nullptr, check_row);
  ASSERT_EQ(group->iter_row_id_, col_iter->iter_row_id_)
      << "After catchup: group_row_id=" << group->iter_row_id_
      << " col_row_id=" << col_iter->iter_row_id_;

  ASSERT_EQ(OB_SUCCESS, group->next());
  ASSERT_EQ(OB_ITER_END, group->next());

  group->reset();
  allocator_.free(group);
  cleanup_co_env(merger, co_handle, cg_handle_arr);
}

// ===========================================================================
// Test 3: multiple group items (two value columns → two CG iters)
// ===========================================================================
TEST_F(TestMergeIterGroup, test_multiple_group_items)
{
  const int64_t snapshot_version = 10;
  const int64_t row_cnt = 3;

  std::string row_data = generate_two_col_data(row_cnt, snapshot_version);

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_ctx(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_ctx.static_param_);

  // Vary micro block sizes per CG to exercise different block distributions.
  const int64_t micro_limits[] = {1, 1, 1, 1};
  const int64_t macro_limits[] = {1, 1, 1, 1};

  ObTableHandleV2 co_handle;
  ObSEArray<ObTableHandleV2, 8> cg_handle_arr;
  setup_co_env(row_data.c_str(), 1, snapshot_version,
               micro_limits, macro_limits,
               merge_ctx, merger, co_handle, cg_handle_arr);
  ASSERT_GE(cg_handle_arr.count(), 2);

  ObPartitionMergeIterGroup<ObPartitionRowMergeIter> *group =
      OB_NEWx(ObPartitionMergeIterGroup<ObPartitionRowMergeIter>, (&allocator_), allocator_, merger.filter_handle_);
  ASSERT_NE(nullptr, group);
  ObRowkeyReadInfo rowkey_cg_read_info;
  build_rowkey_cg_read_info(merger.merge_param_, rowkey_cg_read_info);
  ASSERT_EQ(OB_SUCCESS, group->init(merger.merge_param_, 0, &rowkey_cg_read_info));

  ObPartitionMergeIter *col_iter = group->iter_group_items_.at(0).iter_;
  ASSERT_NE(nullptr, col_iter);

  ASSERT_EQ(OB_SUCCESS, group->next());
  ASSERT_EQ(1, group->get_group_item_count());

  for (int64_t row = 1; row < row_cnt; ++row) {
    ASSERT_EQ(OB_SUCCESS, group->next());
    const ObDatumRow *check_row = group->get_filter_check_row();
    ASSERT_NE(nullptr, check_row);
    ASSERT_EQ(group->iter_row_id_, col_iter->iter_row_id_)
        << "row=" << row << " col_iter not aligned";
  }

  ASSERT_EQ(OB_ITER_END, group->next());

  group->reset();
  allocator_.free(group);
  cleanup_co_env(merger, co_handle, cg_handle_arr);
}

// ===========================================================================
// Test 4: large data volume – 100 rows, single block
//
// Verifies correctness at scale: every row_id must match between primary
// and group item iters through the entire iteration.
// ===========================================================================
TEST_F(TestMergeIterGroup, test_large_data_single_block)
{
  const int64_t snapshot_version = 10;
  const int64_t row_cnt = 100;

  std::string row_data = generate_single_col_data(row_cnt, snapshot_version);

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_ctx(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_ctx.static_param_);

  const int64_t micro_limits[] = {INT64_MAX, INT64_MAX, INT64_MAX};
  const int64_t macro_limits[] = {INT64_MAX, INT64_MAX, INT64_MAX};

  ObTableHandleV2 co_handle;
  ObSEArray<ObTableHandleV2, 4> cg_handle_arr;
  setup_co_env(row_data.c_str(), 1, snapshot_version,
               micro_limits, macro_limits,
               merge_ctx, merger, co_handle, cg_handle_arr);

  ObPartitionMergeIterGroup<ObPartitionRowMergeIter> *group =
      OB_NEWx(ObPartitionMergeIterGroup<ObPartitionRowMergeIter>, (&allocator_), allocator_, merger.filter_handle_);
  ASSERT_NE(nullptr, group);
  ObRowkeyReadInfo rowkey_cg_read_info;
  build_rowkey_cg_read_info(merger.merge_param_, rowkey_cg_read_info);
  ASSERT_EQ(OB_SUCCESS, group->init(merger.merge_param_, 0, &rowkey_cg_read_info));

  ObPartitionMergeIter *col_iter = group->iter_group_items_.at(0).iter_;
  ASSERT_NE(nullptr, col_iter);

  int64_t actual_row_cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = group->next())) {
    const ObDatumRow *check_row = group->get_filter_check_row();
    ASSERT_NE(nullptr, check_row);
    ASSERT_EQ(group->iter_row_id_, col_iter->iter_row_id_)
        << "Row " << actual_row_cnt;
    ++actual_row_cnt;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(row_cnt, actual_row_cnt);

  group->reset();
  allocator_.free(group);
  cleanup_co_env(merger, co_handle, cg_handle_arr);
}

// ===========================================================================
// Test 5: large data with multiple micro blocks per CG
//
// 50 rows, 5 rows per micro block → 10 micro blocks.
// The rowkey CG and value CG may have different micro block boundaries;
// the group must sync correctly across all of them.
// ===========================================================================
TEST_F(TestMergeIterGroup, test_large_data_multi_micro)
{
  const int64_t snapshot_version = 10;
  const int64_t row_cnt = 50;

  std::string row_data = generate_single_col_data(row_cnt, snapshot_version);

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_ctx(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_ctx.static_param_);

  // 5 rows per micro block, all in one macro block.
  const int64_t micro_limits[] = {5, 5, 5};
  const int64_t macro_limits[] = {INT64_MAX, INT64_MAX, INT64_MAX};

  ObTableHandleV2 co_handle;
  ObSEArray<ObTableHandleV2, 4> cg_handle_arr;
  setup_co_env(row_data.c_str(), 1, snapshot_version,
               micro_limits, macro_limits,
               merge_ctx, merger, co_handle, cg_handle_arr);

  ObPartitionMergeIterGroup<ObPartitionRowMergeIter> *group =
      OB_NEWx(ObPartitionMergeIterGroup<ObPartitionRowMergeIter>, (&allocator_), allocator_, merger.filter_handle_);
  ASSERT_NE(nullptr, group);
  ObRowkeyReadInfo rowkey_cg_read_info;
  build_rowkey_cg_read_info(merger.merge_param_, rowkey_cg_read_info);
  ASSERT_EQ(OB_SUCCESS, group->init(merger.merge_param_, 0, &rowkey_cg_read_info));

  ObPartitionMergeIter *col_iter = group->iter_group_items_.at(0).iter_;

  int64_t actual_row_cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = group->next())) {
    const ObDatumRow *check_row = group->get_filter_check_row();
    ASSERT_NE(nullptr, check_row);
    ASSERT_EQ(group->iter_row_id_, col_iter->iter_row_id_)
        << "Row " << actual_row_cnt;
    ++actual_row_cnt;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(row_cnt, actual_row_cnt);

  group->reset();
  allocator_.free(group);
  cleanup_co_env(merger, co_handle, cg_handle_arr);
}

// ===========================================================================
// Test 6: large data with multiple macro blocks
//
// 100 rows, 10 rows per micro, 25 per macro → 4 macro blocks.
// Exercises macro block boundary crossing during row-level iteration.
// ===========================================================================
TEST_F(TestMergeIterGroup, test_large_data_multi_macro)
{
  const int64_t snapshot_version = 10;
  const int64_t row_cnt = 100;

  std::string row_data = generate_single_col_data(row_cnt, snapshot_version);

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_ctx(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_ctx.static_param_);

  // 10 rows per micro, 25 per macro → 4 macro blocks of 25 rows each.
  const int64_t micro_limits[] = {10, 10, 10};
  const int64_t macro_limits[] = {25, 25, 25};

  ObTableHandleV2 co_handle;
  ObSEArray<ObTableHandleV2, 4> cg_handle_arr;
  setup_co_env(row_data.c_str(), 1, snapshot_version,
               micro_limits, macro_limits,
               merge_ctx, merger, co_handle, cg_handle_arr);

  ObPartitionMergeIterGroup<ObPartitionRowMergeIter> *group =
      OB_NEWx(ObPartitionMergeIterGroup<ObPartitionRowMergeIter>, (&allocator_), allocator_, merger.filter_handle_);
  ASSERT_NE(nullptr, group);
  ObRowkeyReadInfo rowkey_cg_read_info;
  build_rowkey_cg_read_info(merger.merge_param_, rowkey_cg_read_info);
  ASSERT_EQ(OB_SUCCESS, group->init(merger.merge_param_, 0, &rowkey_cg_read_info));

  ObPartitionMergeIter *col_iter = group->iter_group_items_.at(0).iter_;

  int64_t actual_row_cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = group->next())) {
    const ObDatumRow *check_row = group->get_filter_check_row();
    ASSERT_NE(nullptr, check_row);
    ASSERT_EQ(group->iter_row_id_, col_iter->iter_row_id_)
        << "Row " << actual_row_cnt;
    ++actual_row_cnt;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(row_cnt, actual_row_cnt);

  group->reset();
  allocator_.free(group);
  cleanup_co_env(merger, co_handle, cg_handle_arr);
}

// ===========================================================================
// Test 7: asymmetric block distribution between CGs
//
// Rowkey CG has large blocks (all rows in one micro), but the value CG has
// tiny blocks (2 rows per micro, 6 per macro).  This stresses the sync logic
// when group items cross many more block boundaries than the primary iter.
// ===========================================================================
TEST_F(TestMergeIterGroup, test_asymmetric_block_distribution)
{
  const int64_t snapshot_version = 10;
  const int64_t row_cnt = 30;

  std::string row_data = generate_single_col_data(row_cnt, snapshot_version);

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_ctx(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_ctx.static_param_);

  // CG 0 (rowkey): large blocks; CG 1 (all-col): large; CG 2 (value): tiny blocks
  const int64_t micro_limits[] = {INT64_MAX, INT64_MAX, 2};
  const int64_t macro_limits[] = {INT64_MAX, INT64_MAX, 6};

  ObTableHandleV2 co_handle;
  ObSEArray<ObTableHandleV2, 4> cg_handle_arr;
  setup_co_env(row_data.c_str(), 1, snapshot_version,
               micro_limits, macro_limits,
               merge_ctx, merger, co_handle, cg_handle_arr);

  ObPartitionMergeIterGroup<ObPartitionRowMergeIter> *group =
      OB_NEWx(ObPartitionMergeIterGroup<ObPartitionRowMergeIter>, (&allocator_), allocator_, merger.filter_handle_);
  ASSERT_NE(nullptr, group);
  ObRowkeyReadInfo rowkey_cg_read_info;
  build_rowkey_cg_read_info(merger.merge_param_, rowkey_cg_read_info);
  ASSERT_EQ(OB_SUCCESS, group->init(merger.merge_param_, 0, &rowkey_cg_read_info));

  ObPartitionMergeIter *col_iter = group->iter_group_items_.at(0).iter_;

  int64_t actual_row_cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = group->next())) {
    const ObDatumRow *check_row = group->get_filter_check_row();
    ASSERT_NE(nullptr, check_row);
    ASSERT_EQ(group->iter_row_id_, col_iter->iter_row_id_)
        << "Row " << actual_row_cnt;
    ++actual_row_cnt;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(row_cnt, actual_row_cnt);

  group->reset();
  allocator_.free(group);
  cleanup_co_env(merger, co_handle, cg_handle_arr);
}

// ===========================================================================
// Test 8: catchup across multiple micro/macro block boundaries
//
// Skip several get_filter_check_row() calls so that the group item iters
// fall behind by many rows spanning multiple blocks, then verify they
// catch up correctly on the next get_filter_check_row().
// ===========================================================================
TEST_F(TestMergeIterGroup, test_catchup_across_block_boundaries)
{
  const int64_t snapshot_version = 10;
  const int64_t row_cnt = 40;

  std::string row_data = generate_single_col_data(row_cnt, snapshot_version);

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_ctx(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_ctx.static_param_);

  // 4 rows per micro, 12 per macro → group items traverse many blocks
  const int64_t micro_limits[] = {4, 4, 4};
  const int64_t macro_limits[] = {12, 12, 12};

  ObTableHandleV2 co_handle;
  ObSEArray<ObTableHandleV2, 4> cg_handle_arr;
  setup_co_env(row_data.c_str(), 1, snapshot_version,
               micro_limits, macro_limits,
               merge_ctx, merger, co_handle, cg_handle_arr);

  ObPartitionMergeIterGroup<ObPartitionRowMergeIter> *group =
      OB_NEWx(ObPartitionMergeIterGroup<ObPartitionRowMergeIter>, (&allocator_), allocator_, merger.filter_handle_);
  ASSERT_NE(nullptr, group);
  ObRowkeyReadInfo rowkey_cg_read_info;
  build_rowkey_cg_read_info(merger.merge_param_, rowkey_cg_read_info);
  ASSERT_EQ(OB_SUCCESS, group->init(merger.merge_param_, 0, &rowkey_cg_read_info));

  ObPartitionMergeIter *col_iter = group->iter_group_items_.at(0).iter_;

  // Advance to row 0, sync group items.
  ASSERT_EQ(OB_SUCCESS, group->next());
  const ObDatumRow *check_row = group->get_filter_check_row();
  ASSERT_NE(nullptr, check_row);
  ASSERT_EQ(group->iter_row_id_, col_iter->iter_row_id_);

  // Advance 15 rows WITHOUT calling get_filter_check_row,
  // so group items fall behind across multiple block boundaries.
  for (int64_t i = 0; i < 15; ++i) {
    ASSERT_EQ(OB_SUCCESS, group->next());
  }

  // Now sync — group items must catch up across multiple blocks.
  check_row = group->get_filter_check_row();
  ASSERT_NE(nullptr, check_row);
  ASSERT_EQ(group->iter_row_id_, col_iter->iter_row_id_)
      << "After 15-row skip: group=" << group->iter_row_id_
      << " col=" << col_iter->iter_row_id_;

  // Continue and check every remaining row.
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = group->next())) {
    check_row = group->get_filter_check_row();
    ASSERT_NE(nullptr, check_row);
    ASSERT_EQ(group->iter_row_id_, col_iter->iter_row_id_);
  }
  ASSERT_EQ(OB_ITER_END, ret);

  group->reset();
  allocator_.free(group);
  cleanup_co_env(merger, co_handle, cg_handle_arr);
}

// ===========================================================================
// Test 9: large data with two columns, multi-block
//
// Two value columns → two group items.  Both must stay aligned with the
// primary iter across many block boundaries.
// ===========================================================================
TEST_F(TestMergeIterGroup, test_two_columns_large_data_multi_block)
{
  const int64_t snapshot_version = 10;
  const int64_t row_cnt = 60;

  std::string row_data = generate_two_col_data(row_cnt, snapshot_version);

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_ctx(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_ctx.static_param_);

  // CG 0 (rowkey): 10 per micro; CG 1 (all-col): 8 per micro;
  // CG 2 (v1): 5 per micro; CG 3 (v2): 3 per micro
  const int64_t micro_limits[] = {10, 8, 5, 3};
  const int64_t macro_limits[] = {30, 24, 15, 9};

  ObTableHandleV2 co_handle;
  ObSEArray<ObTableHandleV2, 8> cg_handle_arr;
  setup_co_env(row_data.c_str(), 1, snapshot_version,
               micro_limits, macro_limits,
               merge_ctx, merger, co_handle, cg_handle_arr);
  ASSERT_GE(cg_handle_arr.count(), 2);

  ObPartitionMergeIterGroup<ObPartitionRowMergeIter> *group =
      OB_NEWx(ObPartitionMergeIterGroup<ObPartitionRowMergeIter>, (&allocator_), allocator_, merger.filter_handle_);
  ASSERT_NE(nullptr, group);
  ObRowkeyReadInfo rowkey_cg_read_info;
  build_rowkey_cg_read_info(merger.merge_param_, rowkey_cg_read_info);
  ASSERT_EQ(OB_SUCCESS, group->init(merger.merge_param_, 0, &rowkey_cg_read_info));

  ObPartitionMergeIter *col_iter = group->iter_group_items_.at(0).iter_;

  int64_t actual_row_cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = group->next())) {
    const ObDatumRow *check_row = group->get_filter_check_row();
    ASSERT_NE(nullptr, check_row);
    ASSERT_EQ(group->iter_row_id_, col_iter->iter_row_id_)
        << "Row " << actual_row_cnt;
    ++actual_row_cnt;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(row_cnt, actual_row_cnt);

  group->reset();
  allocator_.free(group);
  cleanup_co_env(merger, co_handle, cg_handle_arr);
}

// ===========================================================================
// Test 10: ObPartitionMacroMergeIter as primary iter
//
// The primary iter operates at macro-block granularity.  After each next()
// that yields a macro block (not opened), we open_curr_range to read rows.
// Group items must stay aligned for every row.
// ===========================================================================
TEST_F(TestMergeIterGroup, test_macro_merge_iter_group)
{
  const int64_t snapshot_version = 10;
  const int64_t row_cnt = 40;

  std::string row_data = generate_single_col_data(row_cnt, snapshot_version);

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_ctx(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_ctx.static_param_);

  // 5 rows per micro, 10 per macro → 4 macro blocks.
  const int64_t micro_limits[] = {5, 5, 5};
  const int64_t macro_limits[] = {10, 10, 10};

  ObTableHandleV2 co_handle;
  ObSEArray<ObTableHandleV2, 4> cg_handle_arr;
  setup_co_env(row_data.c_str(), 1, snapshot_version,
               micro_limits, macro_limits,
               merge_ctx, merger, co_handle, cg_handle_arr);

  ObPartitionMergeIterGroup<ObPartitionMacroMergeIter> *group =
      OB_NEWx(ObPartitionMergeIterGroup<ObPartitionMacroMergeIter>, (&allocator_), allocator_, merger.filter_handle_);
  ASSERT_NE(nullptr, group);
  ObRowkeyReadInfo rowkey_cg_read_info;
  build_rowkey_cg_read_info(merger.merge_param_, rowkey_cg_read_info);
  ASSERT_EQ(OB_SUCCESS, group->init(merger.merge_param_, 0, &rowkey_cg_read_info));

  ObPartitionMergeIter *col_iter = group->iter_group_items_.at(0).iter_;
  ASSERT_NE(nullptr, col_iter);

  // For macro/micro iter, next() at block boundaries returns OK with curr_row=null
  // (the block is not yet opened).  After open_curr_range(), curr_row may still be
  // null — the first row is read on the subsequent next() call.  So we verify via
  // the last iter_row_id rather than counting rows.
  int64_t actual_row_cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = group->next())) {
    if (!group->is_macro_block_opened()) {
      ASSERT_EQ(OB_SUCCESS, group->open_curr_range(false, false));
    }
    if (nullptr != group->get_curr_row()) {
      const ObDatumRow *check_row = group->get_filter_check_row();
      ASSERT_NE(nullptr, check_row);
      ASSERT_EQ(group->iter_row_id_, col_iter->iter_row_id_)
          << "Row " << actual_row_cnt;
      ++actual_row_cnt;
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  // Macro iter has transition iterations at block boundaries where curr_row is null.
  // Verify all rows were traversed by checking the last row_id.
  ASSERT_EQ(row_cnt - 1, group->get_last_row_id());
  ASSERT_GT(actual_row_cnt, 0);

  group->reset();
  allocator_.free(group);
  cleanup_co_env(merger, co_handle, cg_handle_arr);
}

// ===========================================================================
// Test 11: ObPartitionMicroMergeIter as primary iter
//
// The primary iter operates at micro-block granularity.  After next() yields
// a micro block, we open it and verify row alignment for each row.
// ===========================================================================
TEST_F(TestMergeIterGroup, test_micro_merge_iter_group)
{
  const int64_t snapshot_version = 10;
  const int64_t row_cnt = 40;

  std::string row_data = generate_single_col_data(row_cnt, snapshot_version);

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_ctx(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_ctx.static_param_);

  // 5 rows per micro, 20 per macro → 2 macro blocks, 4 micro per macro.
  const int64_t micro_limits[] = {5, 5, 5};
  const int64_t macro_limits[] = {20, 20, 20};

  ObTableHandleV2 co_handle;
  ObSEArray<ObTableHandleV2, 4> cg_handle_arr;
  setup_co_env(row_data.c_str(), 1, snapshot_version,
               micro_limits, macro_limits,
               merge_ctx, merger, co_handle, cg_handle_arr);

  ObPartitionMergeIterGroup<ObPartitionMicroMergeIter> *group =
      OB_NEWx(ObPartitionMergeIterGroup<ObPartitionMicroMergeIter>, (&allocator_), allocator_, merger.filter_handle_);
  ASSERT_NE(nullptr, group);
  ObRowkeyReadInfo rowkey_cg_read_info;
  build_rowkey_cg_read_info(merger.merge_param_, rowkey_cg_read_info);
  ASSERT_EQ(OB_SUCCESS, group->init(merger.merge_param_, 0, &rowkey_cg_read_info));

  ObPartitionMergeIter *col_iter = group->iter_group_items_.at(0).iter_;
  ASSERT_NE(nullptr, col_iter);
  ASSERT_EQ(OB_SUCCESS, group->next());

  int64_t actual_row_cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (nullptr != group->get_curr_row()) {
      const ObDatumRow *check_row = group->get_filter_check_row();
      ASSERT_NE(nullptr, check_row);
      ASSERT_EQ(group->iter_row_id_, col_iter->iter_row_id_)
          << "Row " << actual_row_cnt;
      ++actual_row_cnt;
      ret = group->next();
    } else {
      ASSERT_EQ(OB_SUCCESS, group->open_curr_range(false, false));
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  // Micro iter has transition iterations at micro/macro block boundaries.
  ASSERT_EQ(row_cnt - 1, group->get_last_row_id());
  ASSERT_GT(actual_row_cnt, 0);

  group->reset();
  allocator_.free(group);
  cleanup_co_env(merger, co_handle, cg_handle_arr);
}

// ===========================================================================
// Test 12: ObPartitionMacroMergeIter with large data and asymmetric blocks
//
// Primary (macro iter) has 20 rows per macro; value CG has 3 rows per micro,
// 6 per macro.  This tests heavy catchup when the group items traverse many
// more blocks than the primary.
// ===========================================================================
TEST_F(TestMergeIterGroup, test_macro_iter_group_asymmetric)
{
  const int64_t snapshot_version = 10;
  const int64_t row_cnt = 60;

  std::string row_data = generate_single_col_data(row_cnt, snapshot_version);

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_ctx(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_ctx.static_param_);

  // Rowkey CG: 10 per micro, 20 per macro.  Value CG: 3 per micro, 6 per macro.
  const int64_t micro_limits[] = {10, 10, 3};
  const int64_t macro_limits[] = {20, 20, 6};

  ObTableHandleV2 co_handle;
  ObSEArray<ObTableHandleV2, 4> cg_handle_arr;
  setup_co_env(row_data.c_str(), 1, snapshot_version,
               micro_limits, macro_limits,
               merge_ctx, merger, co_handle, cg_handle_arr);

  ObPartitionMergeIterGroup<ObPartitionMacroMergeIter> *group =
      OB_NEWx(ObPartitionMergeIterGroup<ObPartitionMacroMergeIter>, (&allocator_), allocator_, merger.filter_handle_);
  ASSERT_NE(nullptr, group);
  ObRowkeyReadInfo rowkey_cg_read_info;
  build_rowkey_cg_read_info(merger.merge_param_, rowkey_cg_read_info);
  ASSERT_EQ(OB_SUCCESS, group->init(merger.merge_param_, 0, &rowkey_cg_read_info));

  ObPartitionMergeIter *col_iter = group->iter_group_items_.at(0).iter_;
  ASSERT_EQ(OB_SUCCESS, group->next());

  int64_t actual_row_cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (nullptr != group->get_curr_row()) {
      const ObDatumRow *check_row = group->get_filter_check_row();
      ASSERT_NE(nullptr, check_row);
      ASSERT_EQ(group->iter_row_id_, col_iter->iter_row_id_)
          << "Row " << actual_row_cnt;
      ++actual_row_cnt;
      ret = group->next();
    } else {
      ASSERT_EQ(OB_SUCCESS, group->open_curr_range(false, false));
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(row_cnt - 1, group->get_last_row_id());
  ASSERT_GT(actual_row_cnt, 0);

  group->reset();
  allocator_.free(group);
  cleanup_co_env(merger, co_handle, cg_handle_arr);
}

// ===========================================================================
// Test 13: ObPartitionMicroMergeIter with two columns, multi-block
//
// Primary is micro iter; schema has two value columns.
// Tests micro-level primary iter with multiple group items.
// ===========================================================================
TEST_F(TestMergeIterGroup, test_micro_iter_group_two_columns)
{
  const int64_t snapshot_version = 10;
  const int64_t row_cnt = 30;

  std::string row_data = generate_two_col_data(row_cnt, snapshot_version);

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_ctx(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_ctx.static_param_);

  const int64_t micro_limits[] = {6, 6, 4, 3};
  const int64_t macro_limits[] = {18, 18, 12, 9};

  ObTableHandleV2 co_handle;
  ObSEArray<ObTableHandleV2, 8> cg_handle_arr;
  setup_co_env(row_data.c_str(), 1, snapshot_version,
               micro_limits, macro_limits,
               merge_ctx, merger, co_handle, cg_handle_arr);
  ASSERT_GE(cg_handle_arr.count(), 2);

  ObPartitionMergeIterGroup<ObPartitionMicroMergeIter> *group =
      OB_NEWx(ObPartitionMergeIterGroup<ObPartitionMicroMergeIter>, (&allocator_), allocator_, merger.filter_handle_);
  ASSERT_NE(nullptr, group);
  ObRowkeyReadInfo rowkey_cg_read_info;
  build_rowkey_cg_read_info(merger.merge_param_, rowkey_cg_read_info);
  ASSERT_EQ(OB_SUCCESS, group->init(merger.merge_param_, 0, &rowkey_cg_read_info));

  ObPartitionMergeIter *col_iter = group->iter_group_items_.at(0).iter_;
  ASSERT_EQ(OB_SUCCESS, group->next());

  int64_t actual_row_cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (nullptr != group->get_curr_row()) {
      const ObDatumRow *check_row = group->get_filter_check_row();
      ASSERT_NE(nullptr, check_row);
      ASSERT_EQ(group->iter_row_id_, col_iter->iter_row_id_)
          << "Row " << actual_row_cnt;
      ++actual_row_cnt;
      ret = group->next();
    } else {
      ASSERT_EQ(OB_SUCCESS, group->open_curr_range(false, false));
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(row_cnt - 1, group->get_last_row_id());
  ASSERT_GT(actual_row_cnt, 0);

  group->reset();
  allocator_.free(group);
  cleanup_co_env(merger, co_handle, cg_handle_arr);
}

// ===========================================================================
// Test 14: duplicated filter col idx must be deduped to a single group item
//
// Multiple TTL rules can target the same column. When their (value,
// commit_version) are interleaved, the MDS distinct mgr cannot let one replace
// the other, so it keeps BOTH as separate ObTTLFilterInfo and filter_col_idxs_
// ends up with two entries carrying the SAME col_idx.
//
// ObPartitionMergeIterGroup::init must dedup by col_idx and build only ONE
// group item. Without the dedup, init builds two items for the same col_idx,
// and the second fuse_group_item_row double-writes the same filter_check_row_
// slot -> OB_ERR_UNEXPECTED(-4016) (the production crash this test guards).
// ===========================================================================
TEST_F(TestMergeIterGroup, test_duplicate_filter_col_idx_dedup)
{
  const int64_t snapshot_version = 10;
  const int64_t row_cnt = 8;

  std::string row_data = generate_single_col_data(row_cnt, snapshot_version);

  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_ctx(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_ctx.static_param_);

  const int64_t micro_limits[] = {INT64_MAX, INT64_MAX, INT64_MAX};
  const int64_t macro_limits[] = {INT64_MAX, INT64_MAX, INT64_MAX};

  ObTableHandleV2 co_handle;
  ObSEArray<ObTableHandleV2, 4> cg_handle_arr;
  setup_co_env(row_data.c_str(), 1, snapshot_version,
               micro_limits, macro_limits,
               merge_ctx, merger, co_handle, cg_handle_arr);

  // setup_co_env already registered one filter col (filter_col, cg 2). Inject a
  // second entry with the SAME col_idx to simulate two interleaved TTL rules on
  // the same column. filter_handle_ holds a pointer to the live filter_col_idxs_,
  // so the duplicate is visible at group->init() time.
  const int64_t filter_col = schema_rowkey_cnt_ + 2;
  ASSERT_EQ(OB_SUCCESS, merge_ctx.filter_ctx_.filter_col_idxs_.init_for_unittest(filter_col, 2));
  ASSERT_EQ(2, merge_ctx.filter_ctx_.filter_col_idxs_.count());

  ObPartitionMergeIterGroup<ObPartitionRowMergeIter> *group =
      OB_NEWx(ObPartitionMergeIterGroup<ObPartitionRowMergeIter>, (&allocator_), allocator_, merger.filter_handle_);
  ASSERT_NE(nullptr, group);
  ObRowkeyReadInfo rowkey_cg_read_info;
  build_rowkey_cg_read_info(merger.merge_param_, rowkey_cg_read_info);

  // init must succeed and dedup the duplicated col_idx down to ONE group item.
  ASSERT_EQ(OB_SUCCESS, group->init(merger.merge_param_, 0, &rowkey_cg_read_info));
  ASSERT_EQ(1, group->get_group_item_count())
      << "duplicated filter col idx must be deduped to a single group item";

  ObPartitionMergeIter *col_iter = group->iter_group_items_.at(0).iter_;
  ASSERT_NE(nullptr, col_iter);

  // Every row must fuse without OB_ERR_UNEXPECTED (the original -4016 crash):
  // get_filter_check_row returns null when fuse_group_item_row fails.
  int64_t actual_row_cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = group->next())) {
    const ObDatumRow *check_row = group->get_filter_check_row();
    ASSERT_NE(nullptr, check_row)
        << "get_filter_check_row returned null (fuse failed) at row " << actual_row_cnt;
    ASSERT_EQ(group->iter_row_id_, col_iter->iter_row_id_);
    ++actual_row_cnt;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(row_cnt, actual_row_cnt);

  group->reset();
  allocator_.free(group);
  cleanup_co_env(merger, co_handle, cg_handle_arr);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_merge_iter_group.log*");
  OB_LOGGER.set_file_name("test_merge_iter_group.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
